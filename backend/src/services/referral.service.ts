import { randomUUID } from "node:crypto";
import { Pool, type QueryResultRow } from "pg";

export type ReferralLeaderboardEntry = {
  referralCode: string;
  totalEarnings: number;
  totalConversions: number;
};

export type ReferralStats = {
  code: string;
  earnings: number;
  conversions: number;
};

export type ReferralTransactionStatus = "pending" | "confirmed" | "paid" | "rejected";

export type ReferralEvent = {
  id: string;
  referralCode: string;
  transactionId: string;
  amountUsd: number;
  commissionUsd: number;
  status: ReferralTransactionStatus;
  createdAt: string;
  statusUpdatedAt: string;
};

export type RecordReferralConversionInput = {
  referralCode: string;
  transactionId: string;
  amountUsd: number;
  commissionUsd?: number;
  status?: ReferralTransactionStatus;
};

export type RecordReferralConversionResult = {
  recorded: boolean;
};

const REFERRAL_CODE_REGEX = /^[A-Z0-9]{6,8}$/;
const EVM_WALLET_ADDRESS_REGEX = /^0x[a-fA-F0-9]{40}$/;
const REFERRAL_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const REFERRAL_CODE_LENGTH = 6;
const DEFAULT_REFERRAL_COMMISSION_RATE = 0.5;
const EARNING_ELIGIBLE_STATUSES: ReadonlyArray<ReferralTransactionStatus> = ["confirmed", "paid"];
const REFERRAL_TRANSACTION_STATUSES: ReadonlyArray<ReferralTransactionStatus> = ["pending", "confirmed", "paid", "rejected"];

let referralPool: Pool | null = null;
let referralSchemaReadyPromise: Promise<void> | null = null;

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

function getReferralPool(): Pool {
  if (referralPool) {
    return referralPool;
  }

  const connectionString = getDatabaseUrl();
  if (!connectionString) {
    throw new Error("DATABASE_URL is required for referral persistence.");
  }

  referralPool = new Pool({
    connectionString,
    ...(shouldUseSsl(connectionString)
      ? {
        ssl: {
          rejectUnauthorized: false,
        },
      }
      : {}),
  });

  return referralPool;
}

async function ensureReferralSchemaReady(): Promise<void> {
  if (!referralSchemaReadyPromise) {
    referralSchemaReadyPromise = (async () => {
      const pool = getReferralPool();
      const client = await pool.connect();
      try {
        await client.query(`
          CREATE TABLE IF NOT EXISTS referrals (
            id uuid PRIMARY KEY,
            code text NOT NULL UNIQUE,
            created_at timestamptz NOT NULL DEFAULT NOW()
          );
        `);
        await client.query(`
          CREATE TABLE IF NOT EXISTS referral_events (
            id uuid PRIMARY KEY,
            referral_code text NOT NULL REFERENCES referrals(code) ON DELETE CASCADE,
            transaction_id text NOT NULL UNIQUE,
            amount_usd numeric(12, 2) NOT NULL CHECK (amount_usd >= 0),
            commission_usd numeric(12, 2) NOT NULL CHECK (commission_usd >= 0),
            status text NOT NULL DEFAULT 'confirmed',
            status_updated_at timestamptz NOT NULL DEFAULT NOW(),
            created_at timestamptz NOT NULL DEFAULT NOW()
          );
        `);
        await client.query(`ALTER TABLE referral_events ADD COLUMN IF NOT EXISTS status text;`);
        await client.query(`ALTER TABLE referral_events ALTER COLUMN status SET DEFAULT 'confirmed';`);
        await client.query(`
          UPDATE referral_events
          SET status = 'confirmed'
          WHERE status IS NULL OR btrim(status) = '';
        `);
        await client.query(`ALTER TABLE referral_events ALTER COLUMN status SET NOT NULL;`);
        await client.query(`ALTER TABLE referral_events ADD COLUMN IF NOT EXISTS status_updated_at timestamptz;`);
        await client.query(`ALTER TABLE referral_events ALTER COLUMN status_updated_at SET DEFAULT NOW();`);
        await client.query(`
          UPDATE referral_events
          SET status_updated_at = COALESCE(status_updated_at, created_at, NOW())
          WHERE status_updated_at IS NULL;
        `);
        await client.query(`ALTER TABLE referral_events ALTER COLUMN status_updated_at SET NOT NULL;`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_referrals_code ON referrals (code);`);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_referral_events_referral_code
          ON referral_events (referral_code);
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_referral_events_created_at
          ON referral_events (created_at DESC);
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_referral_events_referral_code_status_updated_at
          ON referral_events (referral_code, status_updated_at DESC);
        `);
      } finally {
        client.release();
      }
    })().catch((error) => {
      referralSchemaReadyPromise = null;
      throw error;
    });
  }

  await referralSchemaReadyPromise;
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

function roundCurrency(value: number): number {
  return Math.round(value * 100) / 100;
}

function getReferralCommissionRate(): number {
  const raw = process.env.SELUN_REFERRAL_COMMISSION_RATE?.trim();
  if (!raw) {
    return DEFAULT_REFERRAL_COMMISSION_RATE;
  }

  const parsed = Number.parseFloat(raw);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
    return DEFAULT_REFERRAL_COMMISSION_RATE;
  }

  return parsed;
}

export function getDefaultReferralCommissionUsd(
  amountUsd: number,
  status: ReferralTransactionStatus,
): number {
  if (status === "rejected") {
    return 0;
  }

  return roundCurrency(amountUsd * getReferralCommissionRate());
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

export function normalizeReferralTransactionStatus(value: unknown): ReferralTransactionStatus | null {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  return REFERRAL_TRANSACTION_STATUSES.includes(normalized as ReferralTransactionStatus)
    ? (normalized as ReferralTransactionStatus)
    : null;
}

function mapLeaderboardRow(row: QueryResultRow): ReferralLeaderboardEntry {
  return {
    referralCode: typeof row.referral_code === "string" ? row.referral_code : "",
    totalEarnings: roundCurrency(parseNumericCell(row.total_earnings)),
    totalConversions: typeof row.total_conversions === "number" ? row.total_conversions : 0,
  };
}

function mapReferralEventRow(row: QueryResultRow): ReferralEvent {
  return {
    id: typeof row.id === "string" ? row.id : "",
    referralCode: typeof row.referral_code === "string" ? row.referral_code : "",
    transactionId: typeof row.transaction_id === "string" ? row.transaction_id : "",
    amountUsd: roundCurrency(parseNumericCell(row.amount_usd)),
    commissionUsd: roundCurrency(parseNumericCell(row.commission_usd)),
    status: normalizeReferralTransactionStatus(row.status) ?? "confirmed",
    createdAt: toIsoString(row.created_at),
    statusUpdatedAt: toIsoString(row.status_updated_at),
  };
}

function generateReferralCodeCandidate(length = REFERRAL_CODE_LENGTH): string {
  let code = "";
  for (let index = 0; index < length; index += 1) {
    const alphabetIndex = Math.floor(Math.random() * REFERRAL_CODE_ALPHABET.length);
    code += REFERRAL_CODE_ALPHABET[alphabetIndex];
  }
  return code;
}

export function normalizeReferralCode(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) {
    return null;
  }

  if (EVM_WALLET_ADDRESS_REGEX.test(trimmed)) {
    return trimmed.toLowerCase();
  }

  const normalized = trimmed.toUpperCase();
  return REFERRAL_CODE_REGEX.test(normalized) ? normalized : null;
}

export function isReferralDatabaseConfigured(): boolean {
  return Boolean(getDatabaseUrl());
}

export async function initializeReferralPersistence(): Promise<void> {
  await ensureReferralSchemaReady();
}

export async function createReferralCode(): Promise<string> {
  await ensureReferralSchemaReady();
  const pool = getReferralPool();

  for (let attempt = 0; attempt < 16; attempt += 1) {
    const code = generateReferralCodeCandidate();
    try {
      const result = await pool.query<{ code: string }>(
        `
          INSERT INTO referrals (id, code, created_at)
          VALUES ($1, $2, NOW())
          RETURNING code;
        `,
        [randomUUID(), code],
      );
      return result.rows[0]?.code ?? code;
    } catch (error) {
      const duplicateCode =
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        (error as { code?: string }).code === "23505";
      if (!duplicateCode) {
        throw error;
      }
    }
  }

  throw new Error("Unable to generate a unique referral code. Retry the request.");
}

export async function getReferralLeaderboard(limit = 10): Promise<ReferralLeaderboardEntry[]> {
  await ensureReferralSchemaReady();
  const pool = getReferralPool();
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 100) : 10;
  const result = await pool.query(
    `
      SELECT
        r.code AS referral_code,
        COALESCE(SUM(e.commission_usd), 0)::numeric(12, 2) AS total_earnings,
        COUNT(e.id)::int AS total_conversions
      FROM referrals r
      LEFT JOIN referral_events e
        ON e.referral_code = r.code
       AND e.status = ANY($2::text[])
      GROUP BY r.code
      HAVING COUNT(e.id) > 0
      ORDER BY
        COALESCE(SUM(e.commission_usd), 0) DESC,
        COUNT(e.id) DESC,
        r.code ASC
      LIMIT $1;
    `,
    [safeLimit, EARNING_ELIGIBLE_STATUSES],
  );

  return result.rows.map((row) => mapLeaderboardRow(row));
}

export async function getReferralStatsByCode(code: string): Promise<ReferralStats | null> {
  const normalizedCode = normalizeReferralCode(code);
  if (!normalizedCode) {
    return null;
  }

  await ensureReferralSchemaReady();
  const pool = getReferralPool();
  const result = await pool.query(
    `
      SELECT
        r.code AS referral_code,
        COALESCE(SUM(e.commission_usd), 0)::numeric(12, 2) AS total_earnings,
        COUNT(e.id)::int AS total_conversions
      FROM referrals r
      LEFT JOIN referral_events e
        ON e.referral_code = r.code
       AND e.status = ANY($2::text[])
      WHERE r.code = $1
      GROUP BY r.code;
    `,
    [normalizedCode, EARNING_ELIGIBLE_STATUSES],
  );

  const row = result.rows[0];
  if (!row) {
    return {
      code: normalizedCode,
      earnings: 0,
      conversions: 0,
    };
  }

  return {
    code: typeof row.referral_code === "string" ? row.referral_code : normalizedCode,
    earnings: roundCurrency(parseNumericCell(row.total_earnings)),
    conversions: typeof row.total_conversions === "number" ? row.total_conversions : 0,
  };
}

export async function getReferralEventsByCode(code: string, limit = 100): Promise<ReferralEvent[] | null> {
  const normalizedCode = normalizeReferralCode(code);
  if (!normalizedCode) {
    return null;
  }

  await ensureReferralSchemaReady();
  const pool = getReferralPool();
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(Math.floor(limit), 250) : 100;
  const result = await pool.query(
    `
      SELECT
        e.id,
        e.referral_code,
        e.transaction_id,
        e.amount_usd,
        e.commission_usd,
        e.status,
        e.created_at,
        e.status_updated_at
      FROM referral_events e
      WHERE e.referral_code = $1
      ORDER BY
        COALESCE(e.status_updated_at, e.created_at) DESC,
        e.created_at DESC
      LIMIT $2;
    `,
    [normalizedCode, safeLimit],
  );

  return result.rows.map((row) => mapReferralEventRow(row));
}

export async function recordReferralConversion(
  input: RecordReferralConversionInput,
): Promise<RecordReferralConversionResult> {
  const normalizedCode = normalizeReferralCode(input.referralCode);
  const transactionId = input.transactionId.trim();
  const amountUsd = roundCurrency(input.amountUsd);
  const status = normalizeReferralTransactionStatus(input.status) ?? "confirmed";
  const commissionUsd = roundCurrency(input.commissionUsd ?? getDefaultReferralCommissionUsd(amountUsd, status));

  if (!normalizedCode || !transactionId || !Number.isFinite(amountUsd) || amountUsd < 0) {
    return { recorded: false };
  }

  await ensureReferralSchemaReady();
  const pool = getReferralPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `
        INSERT INTO referrals (id, code, created_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (code) DO NOTHING;
      `,
      [randomUUID(), normalizedCode],
    );
    const result = await client.query(
      `
        INSERT INTO referral_events (
          id,
          referral_code,
          transaction_id,
          amount_usd,
          commission_usd,
          status,
          status_updated_at,
          created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
        ON CONFLICT (transaction_id) DO UPDATE
        SET
          amount_usd = EXCLUDED.amount_usd,
          commission_usd = EXCLUDED.commission_usd,
          status = EXCLUDED.status,
          status_updated_at = CASE
            WHEN referral_events.status IS DISTINCT FROM EXCLUDED.status THEN NOW()
            ELSE COALESCE(referral_events.status_updated_at, referral_events.created_at, NOW())
          END
        RETURNING id;
      `,
      [randomUUID(), normalizedCode, transactionId, amountUsd, commissionUsd, status],
    );
    await client.query("COMMIT");

    return {
      recorded: (result.rowCount ?? 0) > 0,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
