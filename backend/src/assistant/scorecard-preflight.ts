import { createHash } from "node:crypto";
import { SCORECARD_METHODOLOGY_VERSION, type PriceHistoryMeasure } from "../services/asset-scorecard-v2";
import { resolveAsset } from "./asset-resolver";

/**
 * Decides whether an Asset Scorecard can deliver its overall score before anyone is asked to pay, and
 * locks that decision to the purchase.
 *
 * A score needs two things: the asset must be in the segment's screen, and its price history must be
 * good enough to measure. Eligibility moves with the market and the customer's profile, so this runs the
 * same screen the paid analysis uses rather than a rule that approximates it. Anything it can't confirm
 * counts as not confirmed: the offer is withheld, not sold with a warning.
 *
 * A deliverable check produces a snapshot. The paid run scores from that snapshot rather than screening
 * again, so a payment can't buy a different eligibility decision from the one it was quoted. Price history
 * isn't frozen: the paid run measures it fresh, so a feed can still fail, which the free retry handles.
 */
export type ScreenProfile = { riskTolerance: string; timeframe: string; portfolioSegment: string };

/** One asset as the screen scored it. The paid run's cards are built from these. */
export type ScreenCard = {
  asset: string;
  name: string;
  coingeckoId: string;
  marketCapRank: number | null;
  tokenCategory: string;
  role: string;
  riskClass: string;
  selectionBucket: string;
  selected: boolean;
  qualityScore: number;
  riskScore: number;
  liquidityScore: number;
  structuralScore: number;
  compositeScore: number;
  selectionReasons: string[];
};

export type ScreenCounts = { screened: number; qualified: number; selected: number };
export type ScreenResult = { screenedAt: string; counts: ScreenCounts; cards: ScreenCard[] };

/** The screening decision a purchase is bound to, kept on the purchase record for its runs and retries. */
export type ScreeningSnapshot = {
  /** A hash of everything below, so a changed record can't pass as the one that was quoted. */
  id: string;
  boundAt: string;
  /** Payment must be approved by this time. Once it's confirmed, the snapshot stays good for every retry. */
  validUntil: string;
  screenedAt: string;
  methodologyVersion: string;
  profile: ScreenProfile;
  assets: string[];
  /** Every symbol the screen scored. Kept as evidence of what was eligible. */
  universe: string[];
  counts: ScreenCounts;
  cards: ScreenCard[];
};

export type AssetVerdict = {
  symbol: string;
  name: string;
  outcome: "eligible" | "outside_segment" | "insufficient_history" | "check_unavailable" | "unrecognized";
  reason?: string;
  /** A segment this asset can be scored in, for this customer's risk and time horizon. */
  eligibleSegment?: string;
};
export type PreflightResult = { deliverable: boolean; assets: AssetVerdict[]; snapshot?: ScreeningSnapshot | null };
export type PreflightOptions = {
  /** Only names another segment if it's already screened. Public routes use this so a probe can't start extra screens. */
  cachedAlternativesOnly?: boolean;
};

export type PreflightDeps = {
  runScreen: (profile: ScreenProfile) => Promise<ScreenResult>;
  measure: (asset: { coingeckoId: string; symbol: string; marketCapRank: number | null }) => Promise<PriceHistoryMeasure>;
  segments: readonly string[];
  now?: () => number;
  ttlMs?: number;
  /** How long a customer's turn waits. A screen still running after this keeps going, for the next try. */
  waitMs?: number;
  /** Looking for another segment only starts if this much of the wait is left. */
  alternativeBudgetMs?: number;
  snapshotValidMs?: number;
};

const SCREEN_TTL_MS = 2 * 60 * 60 * 1000;
const WAIT_MS = 32_000;
const ALTERNATIVE_BUDGET_MS = 12_000;
const SNAPSHOT_VALID_MS = 2 * 60 * 60 * 1000;
const MAX_NAMED_CARDS = 12;
const MAX_UNNAMED_CARDS = 8;

export class ScorecardPreflight {
  private readonly screens = new Map<string, { at: number; result: Promise<ScreenResult> }>();
  /** One screen at a time, so preflight can't crowd the market-data providers' rate limits. */
  private queue: Promise<unknown> = Promise.resolve();
  private readonly now: () => number;
  private readonly ttlMs: number;
  private readonly waitMs: number;
  private readonly alternativeBudgetMs: number;
  private readonly snapshotValidMs: number;

  constructor(private readonly deps: PreflightDeps) {
    this.now = deps.now ?? Date.now;
    this.ttlMs = deps.ttlMs ?? SCREEN_TTL_MS;
    this.waitMs = deps.waitMs ?? WAIT_MS;
    this.alternativeBudgetMs = deps.alternativeBudgetMs ?? ALTERNATIVE_BUDGET_MS;
    this.snapshotValidMs = deps.snapshotValidMs ?? SNAPSHOT_VALID_MS;
  }

  /** Runs a screen ahead of time so the next customer on this profile doesn't wait for it. */
  warm(profile: ScreenProfile): Promise<ScreenResult> {
    return this.screen(profile);
  }

  private screen(profile: ScreenProfile): Promise<ScreenResult> {
    const key = `${profile.riskTolerance}|${profile.timeframe}|${profile.portfolioSegment}`;
    const cached = this.screens.get(key);
    if (cached && this.now() - cached.at < this.ttlMs) return cached.result;
    const result = this.queue.then(() => this.deps.runScreen(profile));
    this.queue = result.catch(() => undefined);
    const entry = { at: this.now(), result };
    this.screens.set(key, entry);
    // A failed screen isn't an answer, so the next customer tries again.
    result.catch(() => {
      if (this.screens.get(key) === entry) this.screens.delete(key);
    });
    return result;
  }

  /** The screen's answer, or null if it failed or isn't back within the time left. */
  private async screenWithin(profile: ScreenProfile, deadline: number): Promise<ScreenResult | null> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<null>((resolve) => {
      timer = setTimeout(() => resolve(null), Math.max(0, deadline - this.now()));
    });
    try {
      return await Promise.race([this.screen(profile).catch(() => null), late]);
    } finally {
      clearTimeout(timer);
    }
  }

  /** Takes what a request named: a symbol, an ID, or a known name. Naming nothing means the screen's top assets. */
  async check(requested: string[], profile: ScreenProfile, options: PreflightOptions = {}): Promise<PreflightResult> {
    const deadline = this.now() + this.waitMs;
    const screen = await this.screenWithin(profile, deadline);
    if (!screen) {
      const reason = "Selun couldn't finish checking the market screen just now.";
      return { deliverable: false, assets: requested.map((query) => ({ ...labelOf(query), outcome: "check_unavailable", reason })) };
    }
    if (requested.length === 0) return this.checkTopAssets(screen, profile);

    const verdicts: AssetVerdict[] = [];
    const chosen = new Set<ScreenCard>();
    for (const query of requested) {
      const cards = matchCards(screen, query);
      const label = cards[0] ? { symbol: cards[0].asset, name: cards[0].name } : labelOf(query);
      if (verdicts.some((verdict) => verdict.symbol === label.symbol)) continue;
      if (cards.length === 0) {
        const known = resolveAsset(query);
        verdicts.push(known.status === "resolved"
          ? { ...label, outcome: "outside_segment", reason: "It isn't in the group of assets screened for this segment and profile right now." }
          : { ...label, outcome: "unrecognized", reason: "Selun doesn't recognize this asset." });
        continue;
      }
      const verdict = await this.judge(cards[0]);
      verdicts.push(verdict);
      if (verdict.outcome === "eligible") cards.forEach((card) => chosen.add(card));
    }
    for (const verdict of verdicts) {
      if (verdict.outcome !== "outside_segment") continue;
      const eligibleSegment = await this.findSegment(verdict.symbol, profile, deadline, options);
      if (eligibleSegment) verdict.eligibleSegment = eligibleSegment;
    }
    const deliverable = verdicts.every((verdict) => verdict.outcome === "eligible");
    return { deliverable, assets: verdicts, snapshot: deliverable ? this.bind(screen, requested, profile, [...chosen]) : null };
  }

  /** With nothing named there's nothing to refuse, but every card in the purchase must still be able to score. */
  private async checkTopAssets(screen: ScreenResult, profile: ScreenProfile): Promise<PreflightResult> {
    const verdicts = await Promise.all(selectScreenCards(screen.cards, []).map(async (card) => ({ card, verdict: await this.judge(card) })));
    if (verdicts.some(({ verdict }) => verdict.outcome === "check_unavailable")) {
      return { deliverable: false, assets: verdicts.map(({ verdict }) => verdict) };
    }
    const scoreable = verdicts.filter(({ verdict }) => verdict.outcome === "eligible").map(({ card }) => card);
    if (scoreable.length === 0) return { deliverable: false, assets: verdicts.map(({ verdict }) => verdict) };
    return { deliverable: true, assets: [], snapshot: this.bind(screen, [], profile, scoreable) };
  }

  private bind(screen: ScreenResult, assets: string[], profile: ScreenProfile, cards: ScreenCard[]): ScreeningSnapshot {
    const now = this.now();
    const content = {
      boundAt: new Date(now).toISOString(),
      validUntil: new Date(now + this.snapshotValidMs).toISOString(),
      screenedAt: screen.screenedAt,
      methodologyVersion: SCORECARD_METHODOLOGY_VERSION,
      profile: { riskTolerance: profile.riskTolerance, timeframe: profile.timeframe, portfolioSegment: profile.portfolioSegment },
      assets: [...assets],
      universe: [...new Set(screen.cards.map((card) => card.asset))],
      counts: screen.counts,
      cards: selectScreenCards(cards, []),
    };
    return { id: hashOf(content), ...content };
  }

  private async judge(card: ScreenCard): Promise<AssetVerdict> {
    const base = { symbol: card.asset, name: card.name };
    const measure = await this.deps.measure({ coingeckoId: card.coingeckoId, symbol: card.asset, marketCapRank: card.marketCapRank }).catch(() => null);
    if (!measure || measure.status === "unavailable") return { ...base, outcome: "check_unavailable", reason: "The price history feed didn't respond." };
    if (measure.status === "insufficient") return { ...base, outcome: "insufficient_history", reason: measure.reason };
    return { ...base, outcome: "eligible" };
  }

  private async findSegment(symbol: string, profile: ScreenProfile, deadline: number, options: PreflightOptions): Promise<string | undefined> {
    for (const segment of this.deps.segments) {
      if (segment === profile.portfolioSegment) continue;
      const alternative = { ...profile, portfolioSegment: segment };
      const cached = this.isCached(alternative);
      if (!cached && (options.cachedAlternativesOnly || deadline - this.now() < this.alternativeBudgetMs)) continue;
      const screen = await this.screenWithin(alternative, deadline);
      const card = screen ? matchCards(screen, symbol)[0] : undefined;
      if (card && (await this.judge(card)).outcome === "eligible") return segment;
    }
    return undefined;
  }

  private isCached(profile: ScreenProfile): boolean {
    const cached = this.screens.get(`${profile.riskTolerance}|${profile.timeframe}|${profile.portfolioSegment}`);
    return Boolean(cached && this.now() - cached.at < this.ttlMs);
  }
}

function hashOf(content: unknown): string {
  return createHash("sha256").update(JSON.stringify(content)).digest("hex");
}

function labelOf(query: string): { symbol: string; name: string } {
  const known = resolveAsset(query);
  return known.status === "resolved" ? { symbol: known.asset.symbol, name: known.asset.name } : { symbol: query.toUpperCase(), name: query };
}

/** Matches a symbol or ID directly, as requests may name either, and then anything the resolver knows it as. */
function matchCards(screen: ScreenResult, query: string): ScreenCard[] {
  const wanted = query.toUpperCase();
  const direct = screen.cards.filter((card) => card.asset.toUpperCase() === wanted || card.coingeckoId.toUpperCase() === wanted);
  if (direct.length > 0) return direct;
  const known = resolveAsset(query);
  if (known.status !== "resolved") return [];
  const symbol = known.asset.symbol.toUpperCase();
  return screen.cards.filter((card) => card.asset.toUpperCase() === symbol);
}

/**
 * The screen can list one asset twice when a provider fallback gives it another ID, such as coinlore-90
 * beside bitcoin. A customer buys one score per asset, so this keeps one card, preferring a real CoinGecko
 * ID because price history is looked up by it.
 */
export function uniqueCards<T extends { asset: string; coingeckoId: string; compositeScore: number }>(cards: T[]): T[] {
  const best = new Map<string, T>();
  const isFallbackId = (card: T) => card.coingeckoId.startsWith("coinlore-");
  for (const card of cards) {
    const key = card.asset.toUpperCase();
    const held = best.get(key);
    const better = !held || (isFallbackId(held) && !isFallbackId(card)) || (isFallbackId(held) === isFallbackId(card) && card.compositeScore > held.compositeScore);
    if (better) best.set(key, card);
  }
  return [...best.values()];
}

/** Highest screen score first, capped as the paid analysis caps it. Naming nothing means the top assets. */
export function selectScreenCards<T extends { asset: string; coingeckoId: string; compositeScore: number; qualityScore: number }>(cards: T[], assets: string[]): T[] {
  const wanted = new Set(assets.map((asset) => asset.toUpperCase()));
  return uniqueCards(cards.filter((card) => wanted.size === 0 || wanted.has(card.asset.toUpperCase()) || wanted.has(card.coingeckoId.toUpperCase())))
    .sort((left, right) => right.compositeScore - left.compositeScore || right.qualityScore - left.qualityScore)
    .slice(0, wanted.size > 0 ? MAX_NAMED_CARDS : MAX_UNNAMED_CARDS);
}

const sameAssets = (left: string[], right: string[]) => {
  const normalize = (assets: string[]) => assets.map((asset) => asset.toUpperCase()).sort().join("|");
  return normalize(left) === normalize(right);
};

/**
 * Why a snapshot can't be used for this exact purchase, or null if it can. Pass `now` before payment,
 * when an old quote must be checked again. After payment the snapshot is kept whatever its age, so only
 * its integrity and its match to the purchase are tested.
 */
export function snapshotProblem(
  snapshot: ScreeningSnapshot,
  purchase: { assets: string[]; profile: ScreenProfile },
  now?: number,
): "altered" | "different_purchase" | "methodology_changed" | "expired" | null {
  const { id, ...content } = snapshot;
  if (id !== hashOf(content)) return "altered";
  const { profile } = purchase;
  if (
    !sameAssets(snapshot.assets, purchase.assets) ||
    snapshot.profile.riskTolerance !== profile.riskTolerance ||
    snapshot.profile.timeframe !== profile.timeframe ||
    snapshot.profile.portfolioSegment !== profile.portfolioSegment
  ) return "different_purchase";
  if (snapshot.methodologyVersion !== SCORECARD_METHODOLOGY_VERSION) return "methodology_changed";
  if (now !== undefined && now >= Date.parse(snapshot.validUntil)) return "expired";
  return null;
}

/** The screening fields of a stored purchase input. */
export function purchaseOf(input: Record<string, unknown>): { assets: string[]; profile: ScreenProfile } {
  return {
    assets: Array.isArray(input.assets) ? (input.assets as string[]) : [],
    profile: { riskTolerance: String(input.riskTolerance), timeframe: String(input.timeframe), portfolioSegment: String(input.portfolioSegment) },
  };
}

export type ScreeningDecision = { snapshot: ScreeningSnapshot | null; refusal: PreflightResult | null };

/**
 * What the wallet route does before it issues a payment request or settles one, for chat and direct API
 * buyers alike. Nothing here runs once a payment is on record: that purchase keeps its decision.
 */
export async function decideScreening(params: {
  existing: { payment?: unknown; screeningSnapshot?: ScreeningSnapshot } | undefined;
  input: Record<string, unknown>;
  paying: boolean;
  preflight: Pick<ScorecardPreflight, "check">;
  now?: number;
}): Promise<ScreeningDecision> {
  const { existing, input, paying, preflight } = params;
  if (existing?.payment) return { snapshot: existing.screeningSnapshot ?? null, refusal: null };
  const purchase = purchaseOf(input);
  // A discovery probe names nothing and pays nothing, so there's no purchase yet to check.
  if (!paying && purchase.assets.length === 0) return { snapshot: null, refusal: null };
  const held = existing?.screeningSnapshot;
  if (held && !snapshotProblem(held, purchase, params.now ?? Date.now())) return { snapshot: held, refusal: null };
  const result = await preflight.check(purchase.assets, purchase.profile, { cachedAlternativesOnly: true });
  return result.deliverable && result.snapshot ? { snapshot: result.snapshot, refusal: null } : { snapshot: null, refusal: result };
}
