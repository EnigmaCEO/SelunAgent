import { createHmac, randomUUID } from "node:crypto";
import { Pool, type QueryResultRow } from "pg";
import { verifyMessage } from "viem";

export type AgentReferrerType = "human" | "agent";
export type AgentAllocationStatus = "pending" | "confirmed";
export type AgentPayoutStatus = "pending" | "paid";
export type AgentReferralTrackStatus = "tracked" | "attribution_locked";
export type AgentWithdrawalStatus = "paid" | "threshold_not_met" | "nothing_to_withdraw";

export type AgentProgramConfig = {
  fixedPayoutUsdc: number;
  minimumWithdrawalUsdc: number;
  payoutAsset: string;
  payoutNetwork: string;
  publicSiteUrl: string;
  withdrawalCooldownHours: number;
  requireWithdrawalSignature: boolean;
  withdrawalSignatureMaxAgeMinutes: number;
};

export type RegisterAgentReferrerInput = {
  type?: AgentReferrerType;
  walletAddress?: string | null;
  webhookUrl?: string | null;
};

export type AgentReferrerRegistration = {
  referrerId: string;
  type: AgentReferrerType;
  walletAddress: string | null;
  webhookUrl: string | null;
  createdAt: string;
  referralUrl: string;
};

export type TrackAgentReferralInput = {
  referrerId: string;
  userId: string;
  event: string;
  source?: string | null;
};

export type TrackAgentReferralResult = {
  status: AgentReferralTrackStatus;
  referrerId: string;
  attributedReferrerId: string;
  userId: string;
  event: string;
  source: string | null;
  recorded: boolean;
  createdAttribution: boolean;
  timestamp: string;
};

export type ConfirmAgentAllocationInput = {
  referrerId: string;
  userId: string;
  allocationId: string;
  amountUsd?: number | null;
  allocationAmountUsd?: number | null;
  reportAmountUsd?: number | null;
  totalPaidUsd?: number | null;
  paymentReference?: string | null;
  source?: string | null;
};

export type ConfirmAgentAllocationResult = {
  status: "confirmed";
  payout: number;
  currency: string;
  network: string;
  referrerId: string;
  allocationId: string;
  userId: string;
  amountUsd: number | null;
  paymentReference: string | null;
  payoutStatus: AgentPayoutStatus;
  confirmedAt: string;
  duplicate: boolean;
};

export type AgentReferrerStats = {
  referrerId: string;
  clicks: number;
  allocations: number;
  conversionRate: number;
  totalPayout: number;
  pendingPayout: number;
  paidPayout: number;
  minimumWithdrawal: number;
  currency: string;
  network: string;
  walletAddress: string | null;
};

export type RequestAgentWithdrawalInput = {
  referrerId: string;
  walletAddress?: string | null;
  signature?: string | null;
  signedAt?: string | null;
};

export type RequestAgentWithdrawalResult = {
  status: AgentWithdrawalStatus;
  referrerId: string;
  amount: number;
  currency: string;
  network: string;
  walletAddress: string | null;
  payoutCount: number;
  minimumWithdrawal: number;
  withdrawalId: string | null;
  requestedAt: string;
};

export type AgentActivityEntry = {
  referrerId: string;
  userId: string;
  allocationId: string;
  paymentReference: string | null;
  allocationAmountUsd: number;
  reportAmountUsd: number;
  totalPaidUsd: number;
  payoutAmountUsd: number;
  payoutStatus: AgentPayoutStatus;
  source: string | null;
  createdAt: string;
  confirmedAt: string | null;
  paidAt: string | null;
};

export type AgentConversionWebhookPayload = {
  event: "allocation_confirmed";
  referrer_id: string;
  user_id: string;
  allocation_id: string;
  payout: number;
  currency: string;
  network: string;
  amount_usd: number | null;
  confirmed_at: string;
};

type AgentReferrerRecord = {
  id: string;
  type: AgentReferrerType;
  walletAddress: string | null;
  webhookUrl: string | null;
  createdAt: string;
};

const REFERRER_ID_REGEX = /^(human|agent)_[a-z0-9][a-z0-9_-]{2,63}$/;
const EVM_WALLET_ADDRESS_REGEX = /^0x[a-fA-F0-9]{40}$/;
const ALLOCATION_STATUSES: ReadonlyArray<AgentAllocationStatus> = ["pending", "confirmed"];
const WEBHOOK_TIMEOUT_MS = 4_000;

let agentProgramPool: Pool | null = null;
let agentProgramSchemaReadyPromise: Promise<void> | null = null;

function getDatabaseUrl(): string | null {
  const raw =
    process.env.DATABASE_URL?.trim() ||
    process.env.POSTGRES_DSN?.trim() ||
    "";
  return raw || null;
}

function shouldUseSsl(connectionString: string): boolean {
  const sslMode = process.env.PGSSLMODE?.trim().toLowerCase();
  if (sslMode === "disable") return false;
  if (sslMode === "require") return true;
  return /sslmode=require/i.test(connectionString);
}

function getAgentProgramPool(): Pool {
  if (agentProgramPool) {
    return agentProgramPool;
  }

  const connectionString = getDatabaseUrl();
  if (!connectionString) {
    throw new Error("DATABASE_URL is required for agent referral persistence.");
  }

  agentProgramPool = new Pool({
    connectionString,
    ...(shouldUseSsl(connectionString)
      ? {
        ssl: {
          rejectUnauthorized: false,
        },
      }
      : {}),
  });

  return agentProgramPool;
}

async function ensureAgentProgramSchemaReady(): Promise<void> {
  if (!agentProgramSchemaReadyPromise) {
    agentProgramSchemaReadyPromise = (async () => {
      const pool = getAgentProgramPool();
      const client = await pool.connect();
      try {
        await client.query(`
          CREATE TABLE IF NOT EXISTS agent_referrers (
            id text PRIMARY KEY,
            type text NOT NULL CHECK (type IN ('human', 'agent')),
            wallet_address text,
            webhook_url text,
            created_at timestamptz NOT NULL DEFAULT NOW()
          );
        `);
        await client.query(`
          CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_referrers_wallet_address
          ON agent_referrers (wallet_address)
          WHERE wallet_address IS NOT NULL;
        `);
        await client.query(`
          CREATE TABLE IF NOT EXISTS agent_referral_users (
            id text PRIMARY KEY,
            referred_by text NOT NULL REFERENCES agent_referrers(id) ON DELETE RESTRICT,
            source text,
            attributed_at timestamptz NOT NULL DEFAULT NOW(),
            created_at timestamptz NOT NULL DEFAULT NOW()
          );
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_agent_referral_users_referred_by
          ON agent_referral_users (referred_by);
        `);
        await client.query(`
          CREATE TABLE IF NOT EXISTS agent_referral_events (
            id uuid PRIMARY KEY,
            dedupe_key text NOT NULL UNIQUE,
            referrer_id text NOT NULL REFERENCES agent_referrers(id) ON DELETE CASCADE,
            user_id text NOT NULL REFERENCES agent_referral_users(id) ON DELETE CASCADE,
            event_type text NOT NULL,
            source text,
            event_timestamp timestamptz NOT NULL DEFAULT NOW(),
            created_at timestamptz NOT NULL DEFAULT NOW()
          );
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_agent_referral_events_referrer_id_event_type
          ON agent_referral_events (referrer_id, event_type);
        `);
        await client.query(`
          CREATE TABLE IF NOT EXISTS agent_allocations (
            id text PRIMARY KEY,
            user_id text NOT NULL REFERENCES agent_referral_users(id) ON DELETE RESTRICT,
            referrer_id text NOT NULL REFERENCES agent_referrers(id) ON DELETE RESTRICT,
            amount_usd numeric(12, 2),
            allocation_amount_usd numeric(12, 2),
            report_amount_usd numeric(12, 2),
            total_paid_usd numeric(12, 2),
            payment_reference text,
            status text NOT NULL CHECK (status IN ('pending', 'confirmed')),
            source text,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            confirmed_at timestamptz
          );
        `);
        await client.query(`ALTER TABLE agent_allocations ADD COLUMN IF NOT EXISTS allocation_amount_usd numeric(12, 2);`);
        await client.query(`ALTER TABLE agent_allocations ADD COLUMN IF NOT EXISTS report_amount_usd numeric(12, 2);`);
        await client.query(`ALTER TABLE agent_allocations ADD COLUMN IF NOT EXISTS total_paid_usd numeric(12, 2);`);
        await client.query(`ALTER TABLE agent_allocations ADD COLUMN IF NOT EXISTS payment_reference text;`);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_agent_allocations_referrer_id_status
          ON agent_allocations (referrer_id, status);
        `);
        await client.query(`
          CREATE TABLE IF NOT EXISTS agent_withdrawals (
            id uuid PRIMARY KEY,
            referrer_id text NOT NULL REFERENCES agent_referrers(id) ON DELETE RESTRICT,
            wallet_address text,
            amount numeric(12, 2) NOT NULL CHECK (amount >= 0),
            currency text NOT NULL,
            network text NOT NULL,
            status text NOT NULL CHECK (status IN ('paid')),
            created_at timestamptz NOT NULL DEFAULT NOW(),
            processed_at timestamptz NOT NULL DEFAULT NOW()
          );
        `);
        await client.query(`
          CREATE TABLE IF NOT EXISTS agent_payouts (
            id uuid PRIMARY KEY,
            referrer_id text NOT NULL REFERENCES agent_referrers(id) ON DELETE RESTRICT,
            allocation_id text NOT NULL UNIQUE REFERENCES agent_allocations(id) ON DELETE CASCADE,
            amount numeric(12, 2) NOT NULL CHECK (amount >= 0),
            payout_rule_type text NOT NULL DEFAULT 'fixed',
            payout_rule_value numeric(12, 2),
            currency text NOT NULL,
            network text NOT NULL,
            wallet_address text,
            status text NOT NULL CHECK (status IN ('pending', 'paid')),
            payout_request_id uuid REFERENCES agent_withdrawals(id) ON DELETE SET NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            paid_at timestamptz
          );
        `);
        await client.query(`ALTER TABLE agent_payouts ADD COLUMN IF NOT EXISTS payout_rule_type text NOT NULL DEFAULT 'fixed';`);
        await client.query(`ALTER TABLE agent_payouts ADD COLUMN IF NOT EXISTS payout_rule_value numeric(12, 2);`);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_agent_payouts_referrer_id_status
          ON agent_payouts (referrer_id, status);
        `);
      } finally {
        client.release();
      }
    })().catch((error) => {
      agentProgramSchemaReadyPromise = null;
      throw error;
    });
  }

  await agentProgramSchemaReadyPromise;
}

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function parseNumericCell(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function toIsoString(value: unknown): string {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "string" && value.trim()) {
    return value;
  }
  return new Date(0).toISOString();
}

function buildEventDedupeKey(referrerId: string, userId: string, eventType: string, source: string | null): string {
  return createHmac("sha256", "selun-agent-referrals")
    .update(`${referrerId}|${userId}|${eventType}|${source ?? ""}`)
    .digest("hex");
}

function normalizeWalletAddress(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) return null;
  return EVM_WALLET_ADDRESS_REGEX.test(trimmed) ? trimmed.toLowerCase() : null;
}

function normalizeWebhookUrl(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) return null;

  try {
    const url = new URL(trimmed);
    return url.toString();
  } catch {
    return null;
  }
}

function normalizeFreeText(value: string | null | undefined, maxLength: number): string | null {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) return null;
  return trimmed.slice(0, maxLength);
}

function normalizeRequiredId(value: string | null | undefined, maxLength: number): string {
  return (value?.trim() ?? "").slice(0, maxLength);
}

function normalizeEventType(value: string | null | undefined): string {
  const trimmed = value?.trim().toLowerCase() ?? "";
  return trimmed ? trimmed.slice(0, 64) : "click";
}

function normalizeReferrerType(value: string | null | undefined): AgentReferrerType {
  return value?.trim().toLowerCase() === "human" ? "human" : "agent";
}

export function normalizeAgentProgramReferrerId(value: string | null | undefined): string | null {
  const trimmed = value?.trim().toLowerCase() ?? "";
  if (!trimmed) return null;
  return REFERRER_ID_REGEX.test(trimmed) ? trimmed : null;
}

export function isAgentProgramReferrerId(value: string | null | undefined): boolean {
  return Boolean(normalizeAgentProgramReferrerId(value));
}

function normalizeAllocationStatus(value: unknown): AgentAllocationStatus | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  return ALLOCATION_STATUSES.includes(normalized as AgentAllocationStatus)
    ? (normalized as AgentAllocationStatus)
    : null;
}

function buildGeneratedReferrerId(type: AgentReferrerType): string {
  return `${type}_${randomUUID().replace(/-/g, "").slice(0, 16)}`;
}

export function getAgentProgramConfig(): AgentProgramConfig {
  const fixedPayoutUsdc = roundCurrency(
    Number.isFinite(Number.parseFloat(process.env.AGENT_REFERRAL_FIXED_PAYOUT_USDC ?? ""))
      ? Number.parseFloat(process.env.AGENT_REFERRAL_FIXED_PAYOUT_USDC ?? "")
      : 20,
  );
  const minimumWithdrawalUsdc = roundCurrency(
    Number.isFinite(Number.parseFloat(process.env.AGENT_REFERRAL_MIN_WITHDRAWAL_USDC ?? ""))
      ? Number.parseFloat(process.env.AGENT_REFERRAL_MIN_WITHDRAWAL_USDC ?? "")
      : 50,
  );
  const payoutAsset = process.env.AGENT_REFERRAL_PAYOUT_ASSET?.trim() || "USDC";
  const payoutNetwork = process.env.AGENT_REFERRAL_PAYOUT_NETWORK?.trim() || "Base";
  const publicSiteUrl = process.env.NEXT_PUBLIC_SITE_URL?.trim().replace(/\/+$/, "") || "https://selun.sagitta.systems";
  const withdrawalCooldownHours = Math.max(
    0,
    Number.isFinite(Number.parseFloat(process.env.AGENT_REFERRAL_WITHDRAWAL_COOLDOWN_HOURS ?? ""))
      ? Number.parseFloat(process.env.AGENT_REFERRAL_WITHDRAWAL_COOLDOWN_HOURS ?? "")
      : 24,
  );
  const requireWithdrawalSignature = process.env.AGENT_REFERRAL_REQUIRE_WITHDRAWAL_SIGNATURE?.trim() === "1";
  const withdrawalSignatureMaxAgeMinutes = Math.max(
    1,
    Number.isFinite(Number.parseFloat(process.env.AGENT_REFERRAL_WITHDRAWAL_SIGNATURE_MAX_AGE_MINUTES ?? ""))
      ? Number.parseFloat(process.env.AGENT_REFERRAL_WITHDRAWAL_SIGNATURE_MAX_AGE_MINUTES ?? "")
      : 15,
  );

  return {
    fixedPayoutUsdc,
    minimumWithdrawalUsdc,
    payoutAsset,
    payoutNetwork,
    publicSiteUrl,
    withdrawalCooldownHours,
    requireWithdrawalSignature,
    withdrawalSignatureMaxAgeMinutes,
  };
}

function buildReferralUrl(referrerId: string): string {
  const { publicSiteUrl } = getAgentProgramConfig();
  return `${publicSiteUrl}/?ref=${encodeURIComponent(referrerId)}`;
}

function buildWithdrawalSignatureMessage(referrerId: string, walletAddress: string, signedAt: string): string {
  return `Selun withdrawal authorization\nreferrer_id=${referrerId}\nwallet_address=${walletAddress}\nsigned_at=${signedAt}`;
}

function mapReferrerRow(row: QueryResultRow): AgentReferrerRecord {
  return {
    id: typeof row.id === "string" ? row.id : "",
    type: normalizeReferrerType(typeof row.type === "string" ? row.type : "agent"),
    walletAddress: typeof row.wallet_address === "string" ? row.wallet_address : null,
    webhookUrl: typeof row.webhook_url === "string" ? row.webhook_url : null,
    createdAt: toIsoString(row.created_at),
  };
}

type Queryable = Pick<Pool, "query">;

async function getReferrerById(referrerId: string, executor: Queryable): Promise<AgentReferrerRecord | null> {
  await ensureAgentProgramSchemaReady();
  const result = await executor.query(
    `
      SELECT id, type, wallet_address, webhook_url, created_at
      FROM agent_referrers
      WHERE id = $1
      LIMIT 1;
    `,
    [referrerId],
  );
  const row = result.rows[0];
  return row ? mapReferrerRow(row) : null;
}

async function ensureUserAttribution(params: {
  executor: Queryable;
  userId: string;
  referrerId: string;
  source: string | null;
}): Promise<{ attributedReferrerId: string; createdAttribution: boolean }> {
  const before = await params.executor.query(
    `
      SELECT referred_by
      FROM agent_referral_users
      WHERE id = $1
      LIMIT 1;
    `,
    [params.userId],
  );

  await params.executor.query(
    `
      INSERT INTO agent_referral_users (id, referred_by, source, attributed_at, created_at)
      VALUES ($1, $2, $3, NOW(), NOW())
      ON CONFLICT (id) DO NOTHING;
    `,
    [params.userId, params.referrerId, params.source],
  );

  const result = await params.executor.query(
    `
      SELECT referred_by
      FROM agent_referral_users
      WHERE id = $1
      LIMIT 1;
    `,
    [params.userId],
  );

  const row = result.rows[0];
  if (!row || typeof row.referred_by !== "string") {
    throw new Error("Failed to resolve user attribution.");
  }

  return {
    attributedReferrerId: row.referred_by,
    createdAttribution: before.rowCount === 0,
  };
}

function buildSyntheticClickSource(source: string | null): string {
  return source ? `${source}:confirm` : "allocation_confirm";
}

async function ensureClickEvent(params: {
  executor: Queryable;
  referrerId: string;
  userId: string;
  source: string | null;
}): Promise<void> {
  const eventType = "click";
  const dedupeKey = buildEventDedupeKey(params.referrerId, params.userId, eventType, params.source);
  await params.executor.query(
    `
      INSERT INTO agent_referral_events (
        id,
        dedupe_key,
        referrer_id,
        user_id,
        event_type,
        source,
        event_timestamp,
        created_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
      ON CONFLICT (dedupe_key) DO NOTHING;
    `,
    [randomUUID(), dedupeKey, params.referrerId, params.userId, eventType, params.source],
  );
}

export function isAgentProgramDatabaseConfigured(): boolean {
  return Boolean(getDatabaseUrl());
}

export async function initializeAgentProgramPersistence(): Promise<void> {
  await ensureAgentProgramSchemaReady();
}

export async function registerAgentReferrer(input: RegisterAgentReferrerInput): Promise<AgentReferrerRegistration> {
  await ensureAgentProgramSchemaReady();
  const pool = getAgentProgramPool();
  const type = normalizeReferrerType(input.type);
  const walletAddress = normalizeWalletAddress(input.walletAddress);
  const webhookUrl =
    input.webhookUrl === undefined || input.webhookUrl === null || input.webhookUrl === ""
      ? null
      : normalizeWebhookUrl(input.webhookUrl);

  if (input.walletAddress && !walletAddress) {
    throw new Error("wallet_address must be a valid Base-compatible wallet address.");
  }

  if (input.webhookUrl && !webhookUrl) {
    throw new Error("webhook_url must be a valid absolute URL.");
  }

  if (walletAddress) {
    const existing = await pool.query(
      `
        SELECT id, type, wallet_address, webhook_url, created_at
        FROM agent_referrers
        WHERE wallet_address = $1
        LIMIT 1;
      `,
      [walletAddress],
    );
    const existingRow = existing.rows[0];
    if (existingRow) {
      const mapped = mapReferrerRow(existingRow);
      return {
        referrerId: mapped.id,
        type: mapped.type,
        walletAddress: mapped.walletAddress,
        webhookUrl: mapped.webhookUrl,
        createdAt: mapped.createdAt,
        referralUrl: buildReferralUrl(mapped.id),
      };
    }
  }

  for (let attempt = 0; attempt < 12; attempt += 1) {
    const referrerId = buildGeneratedReferrerId(type);
    try {
      const result = await pool.query(
        `
          INSERT INTO agent_referrers (id, type, wallet_address, webhook_url, created_at)
          VALUES ($1, $2, $3, $4, NOW())
          RETURNING id, type, wallet_address, webhook_url, created_at;
        `,
        [referrerId, type, walletAddress, webhookUrl],
      );
      const row = mapReferrerRow(result.rows[0] ?? {});
      return {
        referrerId: row.id,
        type: row.type,
        walletAddress: row.walletAddress,
        webhookUrl: row.webhookUrl,
        createdAt: row.createdAt,
        referralUrl: buildReferralUrl(row.id),
      };
    } catch (error) {
      const duplicate =
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        (error as { code?: string }).code === "23505";
      if (!duplicate) {
        throw error;
      }
    }
  }

  throw new Error("Unable to generate a unique referrer_id.");
}

export async function trackAgentReferral(input: TrackAgentReferralInput): Promise<TrackAgentReferralResult> {
  const referrerId = normalizeAgentProgramReferrerId(input.referrerId);
  const userId = normalizeRequiredId(input.userId, 128);
  const event = normalizeEventType(input.event);
  const source = normalizeFreeText(input.source ?? null, 128);

  if (!referrerId) {
    throw new Error("referrer_id is invalid.");
  }
  if (!userId) {
    throw new Error("user_id is required.");
  }

  await ensureAgentProgramSchemaReady();
  const pool = getAgentProgramPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const referrer = await getReferrerById(referrerId, client);
    if (!referrer) {
      throw new Error("Referrer not found.");
    }

    const attribution = await ensureUserAttribution({
      executor: client,
      userId,
      referrerId,
      source,
    });

    if (attribution.attributedReferrerId !== referrerId) {
      await client.query("COMMIT");
      return {
        status: "attribution_locked",
        referrerId,
        attributedReferrerId: attribution.attributedReferrerId,
        userId,
        event,
        source,
        recorded: false,
        createdAttribution: false,
        timestamp: new Date().toISOString(),
      };
    }

    const dedupeKey = buildEventDedupeKey(referrerId, userId, event, source);
    const result = await client.query(
      `
        INSERT INTO agent_referral_events (
          id,
          dedupe_key,
          referrer_id,
          user_id,
          event_type,
          source,
          event_timestamp,
          created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        ON CONFLICT (dedupe_key) DO NOTHING
        RETURNING event_timestamp;
      `,
      [randomUUID(), dedupeKey, referrerId, userId, event, source],
    );

    await client.query("COMMIT");
    return {
      status: "tracked",
      referrerId,
      attributedReferrerId: referrerId,
      userId,
      event,
      source,
      recorded: (result.rowCount ?? 0) > 0,
      createdAttribution: attribution.createdAttribution,
      timestamp: toIsoString(result.rows[0]?.event_timestamp ?? new Date()),
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function confirmAgentAllocation(input: ConfirmAgentAllocationInput): Promise<{
  result: ConfirmAgentAllocationResult;
  webhookUrl: string | null;
}> {
  const referrerId = normalizeAgentProgramReferrerId(input.referrerId);
  const userId = normalizeRequiredId(input.userId, 128);
  const allocationId = normalizeRequiredId(input.allocationId, 128);
  const amountUsd =
    typeof input.amountUsd === "number" && Number.isFinite(input.amountUsd)
      ? roundCurrency(input.amountUsd)
      : null;
  const allocationAmountUsd =
    typeof input.allocationAmountUsd === "number" && Number.isFinite(input.allocationAmountUsd)
      ? roundCurrency(input.allocationAmountUsd)
      : null;
  const reportAmountUsd =
    typeof input.reportAmountUsd === "number" && Number.isFinite(input.reportAmountUsd)
      ? roundCurrency(input.reportAmountUsd)
      : null;
  const totalPaidUsd =
    typeof input.totalPaidUsd === "number" && Number.isFinite(input.totalPaidUsd)
      ? roundCurrency(input.totalPaidUsd)
      : amountUsd;
  const paymentReference = normalizeFreeText(input.paymentReference ?? null, 128);
  const source = normalizeFreeText(input.source ?? null, 128);

  if (!referrerId) {
    throw new Error("referrer_id is invalid.");
  }
  if (!userId) {
    throw new Error("user_id is required.");
  }
  if (!allocationId) {
    throw new Error("allocation_id is required.");
  }
  if (amountUsd !== null && amountUsd < 0) {
    throw new Error("amount must be a non-negative number.");
  }

  await ensureAgentProgramSchemaReady();
  const pool = getAgentProgramPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const referrer = await getReferrerById(referrerId, client);
    if (!referrer) {
      throw new Error("Referrer not found.");
    }

    const attribution = await ensureUserAttribution({
      executor: client,
      userId,
      referrerId,
      source,
    });
    if (attribution.attributedReferrerId !== referrerId) {
      throw new Error(`User attribution is locked to ${attribution.attributedReferrerId}.`);
    }

    await ensureClickEvent({
      executor: client,
      referrerId,
      userId,
      source: buildSyntheticClickSource(source),
    });

    const existingAllocation = await client.query(
      `
        SELECT id, user_id, referrer_id, status, confirmed_at
        FROM agent_allocations
        WHERE id = $1
        LIMIT 1
        FOR UPDATE;
      `,
      [allocationId],
    );

    const existingAllocationRow = existingAllocation.rows[0];
    if (existingAllocationRow) {
      const existingUserId = typeof existingAllocationRow.user_id === "string" ? existingAllocationRow.user_id : "";
      const existingReferrerId =
        typeof existingAllocationRow.referrer_id === "string" ? existingAllocationRow.referrer_id : "";
      if (existingUserId !== userId || existingReferrerId !== referrerId) {
        throw new Error("allocation_id is already attributed to a different user or referrer.");
      }
    }

    const normalizedStatus = normalizeAllocationStatus(existingAllocationRow?.status) ?? "pending";
    await client.query(
      `
        INSERT INTO agent_allocations (
          id,
          user_id,
          referrer_id,
          amount_usd,
          allocation_amount_usd,
          report_amount_usd,
          total_paid_usd,
          payment_reference,
          status,
          source,
          created_at,
          confirmed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'confirmed', $9, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE
        SET
          amount_usd = COALESCE(EXCLUDED.amount_usd, agent_allocations.amount_usd),
          allocation_amount_usd = COALESCE(EXCLUDED.allocation_amount_usd, agent_allocations.allocation_amount_usd),
          report_amount_usd = COALESCE(EXCLUDED.report_amount_usd, agent_allocations.report_amount_usd),
          total_paid_usd = COALESCE(EXCLUDED.total_paid_usd, agent_allocations.total_paid_usd),
          payment_reference = COALESCE(EXCLUDED.payment_reference, agent_allocations.payment_reference),
          status = 'confirmed',
          source = COALESCE(agent_allocations.source, EXCLUDED.source),
          confirmed_at = COALESCE(agent_allocations.confirmed_at, NOW());
      `,
      [allocationId, userId, referrerId, amountUsd, allocationAmountUsd, reportAmountUsd, totalPaidUsd, paymentReference, source],
    );

    const { fixedPayoutUsdc, payoutAsset, payoutNetwork } = getAgentProgramConfig();
    const payoutInsert = await client.query(
      `
        INSERT INTO agent_payouts (
          id,
          referrer_id,
          allocation_id,
          amount,
          payout_rule_type,
          payout_rule_value,
          currency,
          network,
          wallet_address,
          status,
          created_at
        )
        VALUES ($1, $2, $3, $4, 'fixed', $5, $6, $7, $8, 'pending', NOW())
        ON CONFLICT (allocation_id) DO NOTHING
        RETURNING status;
      `,
      [randomUUID(), referrerId, allocationId, fixedPayoutUsdc, fixedPayoutUsdc, payoutAsset, payoutNetwork, referrer.walletAddress],
    );

    const payoutRowResult = await client.query(
      `
        SELECT status, created_at
        FROM agent_payouts
        WHERE allocation_id = $1
        LIMIT 1;
      `,
      [allocationId],
    );
    const payoutRow = payoutRowResult.rows[0];
    const payoutStatus = (typeof payoutRow?.status === "string" ? payoutRow.status : "pending") as AgentPayoutStatus;

    await client.query("COMMIT");

    return {
      result: {
        status: "confirmed",
        payout: fixedPayoutUsdc,
        currency: payoutAsset,
        network: payoutNetwork,
        referrerId,
        allocationId,
        userId,
        amountUsd,
        paymentReference,
        payoutStatus,
        confirmedAt: toIsoString(existingAllocationRow?.confirmed_at ?? payoutRow?.created_at ?? new Date()),
        duplicate: normalizedStatus === "confirmed" || (payoutInsert.rowCount ?? 0) === 0,
      },
      webhookUrl: referrer.webhookUrl,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function getAgentReferrerStats(referrerIdValue: string): Promise<AgentReferrerStats | null> {
  const referrerId = normalizeAgentProgramReferrerId(referrerIdValue);
  if (!referrerId) {
    return null;
  }

  await ensureAgentProgramSchemaReady();
  const pool = getAgentProgramPool();
  const referrer = await getReferrerById(referrerId, pool);
  if (!referrer) {
    return null;
  }

  const { minimumWithdrawalUsdc, payoutAsset, payoutNetwork } = getAgentProgramConfig();
  const result = await pool.query(
    `
      SELECT
        (
          SELECT COUNT(*)::int
          FROM agent_referral_events e
          WHERE e.referrer_id = $1
            AND e.event_type = 'click'
        ) AS clicks,
        (
          SELECT COUNT(*)::int
          FROM agent_allocations a
          WHERE a.referrer_id = $1
            AND a.status = 'confirmed'
        ) AS allocations,
        (
          SELECT COALESCE(SUM(p.amount), 0)::numeric(12, 2)
          FROM agent_payouts p
          WHERE p.referrer_id = $1
        ) AS total_payout,
        (
          SELECT COALESCE(SUM(p.amount), 0)::numeric(12, 2)
          FROM agent_payouts p
          WHERE p.referrer_id = $1
            AND p.status = 'pending'
        ) AS pending_payout,
        (
          SELECT COALESCE(SUM(p.amount), 0)::numeric(12, 2)
          FROM agent_payouts p
          WHERE p.referrer_id = $1
            AND p.status = 'paid'
        ) AS paid_payout;
    `,
    [referrerId],
  );

  const row = result.rows[0] ?? {};
  const clicks = typeof row.clicks === "number" ? row.clicks : 0;
  const allocations = typeof row.allocations === "number" ? row.allocations : 0;
  const totalPayout = roundCurrency(parseNumericCell(row.total_payout));
  const pendingPayout = roundCurrency(parseNumericCell(row.pending_payout));
  const paidPayout = roundCurrency(parseNumericCell(row.paid_payout));

  return {
    referrerId,
    clicks,
    allocations,
    conversionRate: clicks > 0 ? Number((allocations / clicks).toFixed(6)) : 0,
    totalPayout,
    pendingPayout,
    paidPayout,
    minimumWithdrawal: minimumWithdrawalUsdc,
    currency: payoutAsset,
    network: payoutNetwork,
    walletAddress: referrer.walletAddress,
  };
}

export async function getAgentReferrerActivity(referrerIdValue: string, limit = 100): Promise<AgentActivityEntry[] | null> {
  const referrerId = normalizeAgentProgramReferrerId(referrerIdValue);
  if (!referrerId) {
    return null;
  }

  await ensureAgentProgramSchemaReady();
  const pool = getAgentProgramPool();
  const referrer = await getReferrerById(referrerId, pool);
  if (!referrer) {
    return null;
  }

  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 250) : 100;
  const result = await pool.query(
    `
      SELECT
        a.referrer_id,
        a.user_id,
        a.id AS allocation_id,
        a.payment_reference,
        a.allocation_amount_usd,
        a.report_amount_usd,
        COALESCE(a.total_paid_usd, a.amount_usd, 0) AS total_paid_usd,
        p.amount AS payout_amount_usd,
        p.status AS payout_status,
        a.source,
        a.created_at,
        a.confirmed_at,
        p.paid_at
      FROM agent_allocations a
      LEFT JOIN agent_payouts p
        ON p.allocation_id = a.id
      WHERE a.referrer_id = $1
      ORDER BY COALESCE(a.confirmed_at, a.created_at) DESC
      LIMIT $2;
    `,
    [referrerId, safeLimit],
  );

  return result.rows.map((row) => ({
    referrerId: typeof row.referrer_id === "string" ? row.referrer_id : referrerId,
    userId: typeof row.user_id === "string" ? row.user_id : "",
    allocationId: typeof row.allocation_id === "string" ? row.allocation_id : "",
    paymentReference: typeof row.payment_reference === "string" ? row.payment_reference : null,
    allocationAmountUsd: roundCurrency(parseNumericCell(row.allocation_amount_usd)),
    reportAmountUsd: roundCurrency(parseNumericCell(row.report_amount_usd)),
    totalPaidUsd: roundCurrency(parseNumericCell(row.total_paid_usd)),
    payoutAmountUsd: roundCurrency(parseNumericCell(row.payout_amount_usd)),
    payoutStatus: typeof row.payout_status === "string" && row.payout_status === "paid" ? "paid" : "pending",
    source: typeof row.source === "string" ? row.source : null,
    createdAt: toIsoString(row.created_at),
    confirmedAt: row.confirmed_at ? toIsoString(row.confirmed_at) : null,
    paidAt: row.paid_at ? toIsoString(row.paid_at) : null,
  }));
}

export async function requestAgentWithdrawal(input: RequestAgentWithdrawalInput): Promise<RequestAgentWithdrawalResult> {
  const referrerId = normalizeAgentProgramReferrerId(input.referrerId);
  if (!referrerId) {
    throw new Error("referrer_id is invalid.");
  }

  const overrideWallet =
    input.walletAddress === undefined || input.walletAddress === null || input.walletAddress === ""
      ? null
      : normalizeWalletAddress(input.walletAddress);
  if (input.walletAddress && !overrideWallet) {
    throw new Error("wallet_address must be a valid Base-compatible wallet address.");
  }

  await ensureAgentProgramSchemaReady();
  const pool = getAgentProgramPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const referrer = await getReferrerById(referrerId, client);
    if (!referrer) {
      throw new Error("Referrer not found.");
    }

    const payoutWallet = overrideWallet ?? referrer.walletAddress;
    if (!payoutWallet) {
      throw new Error("A payout wallet is required before withdrawal.");
    }

    if (overrideWallet && overrideWallet !== referrer.walletAddress) {
      await client.query(
        `
          UPDATE agent_referrers
          SET wallet_address = $2
          WHERE id = $1;
        `,
        [referrerId, overrideWallet],
      );
    }

    const {
      minimumWithdrawalUsdc,
      payoutAsset,
      payoutNetwork,
      withdrawalCooldownHours,
      requireWithdrawalSignature,
      withdrawalSignatureMaxAgeMinutes,
    } = getAgentProgramConfig();

    if (requireWithdrawalSignature) {
      const signature = input.signature?.trim() ?? "";
      const signedAt = input.signedAt?.trim() ?? "";
      if (!signature || !signedAt) {
        throw new Error("wallet signature is required for withdrawals.");
      }

      const signedAtMs = Date.parse(signedAt);
      if (!Number.isFinite(signedAtMs)) {
        throw new Error("signed_at must be a valid ISO timestamp.");
      }

      const maxAgeMs = withdrawalSignatureMaxAgeMinutes * 60 * 1_000;
      if (Math.abs(Date.now() - signedAtMs) > maxAgeMs) {
        throw new Error("wallet signature has expired.");
      }

      const isValidSignature = await verifyMessage({
        address: payoutWallet as `0x${string}`,
        message: buildWithdrawalSignatureMessage(referrerId, payoutWallet, signedAt),
        signature: signature as `0x${string}`,
      });

      if (!isValidSignature) {
        throw new Error("wallet signature verification failed.");
      }
    }

    if (withdrawalCooldownHours > 0) {
      const lastWithdrawal = await client.query(
        `
          SELECT processed_at
          FROM agent_withdrawals
          WHERE referrer_id = $1
          ORDER BY processed_at DESC
          LIMIT 1;
        `,
        [referrerId],
      );
      const processedAtRaw = lastWithdrawal.rows[0]?.processed_at;
      const processedAtMs = processedAtRaw ? Date.parse(String(processedAtRaw)) : Number.NaN;
      if (Number.isFinite(processedAtMs)) {
        const cooldownMs = withdrawalCooldownHours * 60 * 60 * 1_000;
        if (Date.now() - processedAtMs < cooldownMs) {
          throw new Error("withdrawal cooldown is active.");
        }
      }
    }

    const pendingResult = await client.query(
      `
        SELECT amount
        FROM agent_payouts
        WHERE referrer_id = $1
          AND status = 'pending'
        ORDER BY created_at ASC
        FOR UPDATE;
      `,
      [referrerId],
    );

    const pendingRows = pendingResult.rows;
    const amount = roundCurrency(
      pendingRows.reduce((sum, row) => sum + parseNumericCell(row.amount), 0),
    );
    const payoutCount = pendingRows.length;
    const timestamp = new Date().toISOString();

    if (payoutCount === 0 || amount <= 0) {
      await client.query("COMMIT");
      return {
        status: "nothing_to_withdraw",
        referrerId,
        amount: 0,
        currency: payoutAsset,
        network: payoutNetwork,
        walletAddress: payoutWallet,
        payoutCount: 0,
        minimumWithdrawal: minimumWithdrawalUsdc,
        withdrawalId: null,
        requestedAt: timestamp,
      };
    }

    if (amount < minimumWithdrawalUsdc) {
      await client.query("COMMIT");
      return {
        status: "threshold_not_met",
        referrerId,
        amount,
        currency: payoutAsset,
        network: payoutNetwork,
        walletAddress: payoutWallet,
        payoutCount,
        minimumWithdrawal: minimumWithdrawalUsdc,
        withdrawalId: null,
        requestedAt: timestamp,
      };
    }

    const withdrawalId = randomUUID();
    await client.query(
      `
        INSERT INTO agent_withdrawals (
          id,
          referrer_id,
          wallet_address,
          amount,
          currency,
          network,
          status,
          created_at,
          processed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'paid', NOW(), NOW());
      `,
      [withdrawalId, referrerId, payoutWallet, amount, payoutAsset, payoutNetwork],
    );

    await client.query(
      `
        UPDATE agent_payouts
        SET
          status = 'paid',
          paid_at = NOW(),
          wallet_address = $3,
          payout_request_id = $2
        WHERE referrer_id = $1
          AND status = 'pending';
      `,
      [referrerId, withdrawalId, payoutWallet],
    );

    await client.query("COMMIT");
    return {
      status: "paid",
      referrerId,
      amount,
      currency: payoutAsset,
      network: payoutNetwork,
      walletAddress: payoutWallet,
      payoutCount,
      minimumWithdrawal: minimumWithdrawalUsdc,
      withdrawalId,
      requestedAt: timestamp,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function sendAgentConversionWebhook(
  webhookUrl: string,
  payload: AgentConversionWebhookPayload,
): Promise<void> {
  const signature = signAgentProgramPayload(payload);

  try {
    await fetch(webhookUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(signature ? { "X-Selun-Agent-Signature": signature } : {}),
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(WEBHOOK_TIMEOUT_MS),
    });
  } catch (error) {
    console.error("Agent conversion webhook delivery failed:", error);
  }
}

function stableSerialize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => stableSerialize(entry));
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  const record = value as Record<string, unknown>;
  const normalized: Record<string, unknown> = {};
  for (const key of Object.keys(record).sort()) {
    normalized[key] = stableSerialize(record[key]);
  }
  return normalized;
}

export function signAgentProgramPayload(payload: unknown): string | null {
  const secret = process.env.AGENT_REFERRAL_RESPONSE_SIGNING_SECRET?.trim();
  if (!secret) return null;
  const canonical = JSON.stringify(stableSerialize(payload));
  return createHmac("sha256", secret).update(canonical).digest("hex");
}
