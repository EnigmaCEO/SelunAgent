import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

/**
 * Selun AI's question allowance, kept on the server so clearing a browser can't reset it.
 *
 * A guest gets a few free substantive answers. Each paid analysis adds more, and so does each paid
 * subscription month. Questions about Selun's services, prices, and purchases never use one. Every
 * turn's real model cost is recorded, so the allowances can be set from evidence rather than guesses.
 */

export type CreditConfig = {
  guestQuestions: number;
  purchaseGrant: number;
  subscriptionGrant: number;
  /** New guest allowances one client can open per UTC day, so clearing storage doesn't mean unlimited questions. */
  guestsPerClientPerDay: number;
  /** The most one answer may cost before Selun stops working on it, by whether the customer has paid. */
  freeTurnCostUsd: number;
  earnedTurnCostUsd: number;
  /** Free answers are kept shorter. */
  freeOutputTokens: number;
  /** List prices per million tokens, for the cost record only. */
  rates: { input: number; cached: number; output: number };
};

function number(raw: string | undefined, fallback: number): number {
  const value = Number(raw);
  return raw !== undefined && raw.trim() !== "" && Number.isFinite(value) && value >= 0 ? value : fallback;
}

export function readCreditConfig(env: NodeJS.ProcessEnv = process.env): CreditConfig {
  return {
    guestQuestions: number(env.SELUN_AI_GUEST_QUESTIONS, 5),
    purchaseGrant: number(env.SELUN_AI_PURCHASE_QUESTIONS, 10),
    subscriptionGrant: number(env.SELUN_AI_SUBSCRIPTION_QUESTIONS, 100),
    guestsPerClientPerDay: number(env.SELUN_AI_GUESTS_PER_CLIENT_PER_DAY, 3),
    freeTurnCostUsd: number(env.SELUN_AI_FREE_TURN_COST_USD, 0.02),
    earnedTurnCostUsd: number(env.SELUN_AI_EARNED_TURN_COST_USD, 0.05),
    freeOutputTokens: number(env.SELUN_AI_FREE_OUTPUT_TOKENS, 700),
    // gpt-5.4-mini list prices.
    rates: {
      input: number(env.SELUN_AI_RATE_INPUT_PER_M, 0.75),
      cached: number(env.SELUN_AI_RATE_CACHED_PER_M, 0.075),
      output: number(env.SELUN_AI_RATE_OUTPUT_PER_M, 4.5),
    },
  };
}

type Account = {
  id: string;
  createdAt: string;
  /** Free questions left. */
  free: number;
  /** Questions added by purchases, left. */
  earned: number;
  /** One entry per purchase, so a repeated completion can't add twice. */
  grants: Record<string, { amount: number; at: string }>;
};

export type TurnKind = "service" | "metered";

export type UsageRecord = {
  at: string;
  kind: TurnKind;
  /** Whether the answer used a question. */
  charged: boolean;
  /** Out of questions: Selun only helped with its services. */
  limited: boolean;
  /** Held back by the grounding checks, or stopped by the cost ceiling. Never charged. */
  suppressed: boolean;
  capped: boolean;
  requests: number;
  inputTokens: number;
  cachedTokens: number;
  outputTokens: number;
  costUsd: number;
};

type Store = {
  accounts: Record<string, Account>;
  /** UTC day, then a hashed client key, then how many guest allowances it opened. */
  issued: Record<string, Record<string, number>>;
  usage: UsageRecord[];
};

export type CreditView = { remaining: number; free: number; earned: number; guestQuestions: number };

const MAX_USAGE_RECORDS = 20_000;

export class AiCredits {
  private store: Store;

  constructor(
    readonly config: CreditConfig,
    private readonly file: string | null,
    private readonly now: () => Date = () => new Date(),
  ) {
    this.store = { accounts: {}, issued: {}, usage: [] };
    if (file && fs.existsSync(file)) {
      try {
        const parsed = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<Store>;
        this.store = { accounts: parsed.accounts ?? {}, issued: parsed.issued ?? {}, usage: parsed.usage ?? [] };
      } catch (error) {
        console.error("AI credit records couldn't be read, so they start empty:", error instanceof Error ? error.message : error);
      }
    }
  }

  private save() {
    if (!this.file) return;
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const temporary = `${this.file}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify(this.store));
    fs.renameSync(temporary, this.file);
  }

  /**
   * The account a guest token names, or a new one. Null once this client has opened its daily share of
   * new allowances: it can still ask about Selun's services, but gets no more free questions today.
   */
  account(token: string | null | undefined, clientKey: string): Account | null {
    const known = token ? this.store.accounts[token] : undefined;
    if (known) return known;
    const day = this.now().toISOString().slice(0, 10);
    // Only today's counts matter.
    for (const other of Object.keys(this.store.issued)) if (other !== day) delete this.store.issued[other];
    const today = (this.store.issued[day] ??= {});
    if ((today[clientKey] ?? 0) >= this.config.guestsPerClientPerDay) return null;
    today[clientKey] = (today[clientKey] ?? 0) + 1;
    const account: Account = { id: randomBytes(24).toString("base64url"), createdAt: this.now().toISOString(), free: this.config.guestQuestions, earned: 0, grants: {} };
    this.store.accounts[account.id] = account;
    this.save();
    return account;
  }

  find(id: string | null | undefined): Account | null {
    return (id && this.store.accounts[id]) || null;
  }

  remaining(account: Account | null): number {
    return account ? account.free + account.earned : 0;
  }

  view(account: Account | null): CreditView {
    return { remaining: this.remaining(account), free: account?.free ?? 0, earned: account?.earned ?? 0, guestQuestions: this.config.guestQuestions };
  }

  /** Uses one question, free ones first. False if there were none. */
  charge(account: Account): boolean {
    if (account.free > 0) account.free -= 1;
    else if (account.earned > 0) account.earned -= 1;
    else return false;
    this.save();
    return true;
  }

  /** Adds questions for a completed purchase. A repeat for the same purchase adds nothing. */
  grant(id: string | null | undefined, purchaseKey: string, amount: number): boolean {
    const account = this.find(id);
    if (!account || amount <= 0 || account.grants[purchaseKey]) return false;
    account.grants[purchaseKey] = { amount, at: this.now().toISOString() };
    account.earned += amount;
    this.save();
    return true;
  }

  costOf(usage: { inputTokens: number; cachedTokens: number; outputTokens: number }): number {
    const { rates } = this.config;
    return ((usage.inputTokens - usage.cachedTokens) * rates.input + usage.cachedTokens * rates.cached + usage.outputTokens * rates.output) / 1_000_000;
  }

  record(entry: Omit<UsageRecord, "at">) {
    this.store.usage.push({ at: this.now().toISOString(), ...entry });
    if (this.store.usage.length > MAX_USAGE_RECORDS) this.store.usage.splice(0, this.store.usage.length - MAX_USAGE_RECORDS);
    this.save();
  }

  /** For staff: what answers cost, so allowances and credit packs can be priced from evidence. */
  summary() {
    const group = (rows: UsageRecord[]) => {
      const costs = rows.map((row) => row.costUsd).sort((left, right) => left - right);
      const at = (fraction: number) => (costs.length ? costs[Math.min(costs.length - 1, Math.ceil(fraction * costs.length) - 1)] : 0);
      const total = costs.reduce((sum, cost) => sum + cost, 0);
      return {
        answers: rows.length,
        totalUsd: Number(total.toFixed(4)),
        averageUsd: Number((costs.length ? total / costs.length : 0).toFixed(5)),
        p95Usd: Number(at(0.95).toFixed(5)),
        maxUsd: Number((costs.at(-1) ?? 0).toFixed(5)),
      };
    };
    const usage = this.store.usage;
    const accounts = Object.values(this.store.accounts);
    return {
      since: usage[0]?.at ?? null,
      metered: group(usage.filter((row) => row.kind === "metered" && !row.limited)),
      service: group(usage.filter((row) => row.kind === "service")),
      outOfQuestions: group(usage.filter((row) => row.limited)),
      charged: usage.filter((row) => row.charged).length,
      notChargedSuppressed: usage.filter((row) => row.suppressed || row.capped).length,
      accounts: accounts.length,
      accountsWithPurchases: accounts.filter((account) => Object.keys(account.grants).length > 0).length,
    };
  }
}
