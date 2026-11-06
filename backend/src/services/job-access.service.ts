import { createHmac, timingSafeEqual } from "node:crypto";

/**
 * Signed access for the wizard's job routes and execution status.
 *
 * - A decision ticket records the server's price for a wizard decision, so payment
 *   verification doesn't rely on an amount supplied by the caller.
 * - A run grant is the signed purchase receipt. It allows starting jobs for that decision
 *   and delivering its results, so those routes never rely on the caller's payment details.
 * - A job token allows advancing and reading one job.
 *
 * The frontend signs Stripe grants with the same secret: see app/lib/job-access.ts.
 */
const DECISION_TICKET_TTL_MS = 2 * 60 * 60_000;
const RUN_GRANT_TTL_MS = 24 * 60 * 60_000;
const REPORT_TOKEN_TTL_MS = 365 * 24 * 60 * 60_000;
const INTERNAL_REQUEST_MAX_AGE_MS = 60_000;
const MAX_DELIVERIES_PER_DECISION = 5;
const MAX_JOBS_PER_DECISION = 6;
const MAX_TRACKED_DECISIONS = 10_000;

export const JOB_TOKEN_HEADER = "x-selun-job-token";
export const INTERNAL_TIMESTAMP_HEADER = "x-selun-internal-timestamp";
export const INTERNAL_SIGNATURE_HEADER = "x-selun-internal-signature";

type DecisionTicket = { typ: "ticket"; d: string; w: string; a: string; r: boolean; iat: number; exp: number };
type RunGrant = { typ: "grant"; d: string; src: string; r: boolean; a: string; tx: string; w: string; exp: number };
type ReportToken = { typ: "report"; d: string; c: string; exp: number };
type PurchaseToken = { typ: "purchase"; p: string; k: "one_time" | "subscription"; exp: number };

export type VerifiedPurchase = {
  decisionId: string;
  source: string;
  includesReport: boolean;
  chargedAmountUsdc: string;
  transactionId: string;
  walletAddress: string;
};

function signingKey(): Buffer | null {
  const secret = process.env.SELUN_REFERRAL_INTERNAL_TOKEN?.trim() || process.env.SELUN_ADMIN_API_TOKEN?.trim();
  return secret ? createHmac("sha256", secret).update("selun-job-access-v1").digest() : null;
}

function requireKey(): Buffer {
  const key = signingKey();
  if (!key) throw new Error("Job access signing is not configured.");
  return key;
}

export function isJobAccessConfigured(): boolean {
  return signingKey() !== null;
}

function hmacHex(key: Buffer, value: string): string {
  return createHmac("sha256", key).update(value).digest("hex");
}

function equalText(a: string, b: string): boolean {
  const left = Buffer.from(a, "utf8");
  const right = Buffer.from(b, "utf8");
  return left.length === right.length && timingSafeEqual(left, right);
}

function seal(payload: DecisionTicket | RunGrant | ReportToken | PurchaseToken): string {
  const body = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  return `${body}.${hmacHex(requireKey(), body)}`;
}

function open(token: unknown, now: number): DecisionTicket | RunGrant | ReportToken | PurchaseToken | null {
  const key = signingKey();
  if (!key || typeof token !== "string" || token.length > 2_000) return null;
  const [body, signature, extra] = token.split(".");
  if (!body || !signature || extra !== undefined || !equalText(hmacHex(key, body), signature)) return null;
  try {
    const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8")) as DecisionTicket | RunGrant | ReportToken | PurchaseToken;
    return typeof payload.exp === "number" && payload.exp > now ? payload : null;
  } catch {
    return null;
  }
}

export function issueDecisionTicket(
  input: { decisionId: string; walletAddress: string; chargedAmountUsdc: string; includesReport: boolean },
  now = Date.now(),
): string {
  return seal({
    typ: "ticket",
    d: input.decisionId,
    w: input.walletAddress.toLowerCase(),
    a: input.chargedAmountUsdc,
    r: input.includesReport,
    iat: now,
    exp: now + DECISION_TICKET_TTL_MS,
  });
}

export function readDecisionTicket(
  token: unknown,
  now = Date.now(),
): { decisionId: string; walletAddress: string; chargedAmountUsdc: string; includesReport: boolean; issuedAt: number } | null {
  const payload = open(token, now);
  if (payload?.typ !== "ticket") return null;
  return { decisionId: payload.d, walletAddress: payload.w, chargedAmountUsdc: payload.a, includesReport: payload.r, issuedAt: payload.iat };
}

export function issueRunGrant(purchase: VerifiedPurchase, now = Date.now()): string {
  return seal({
    typ: "grant",
    d: purchase.decisionId,
    src: purchase.source,
    r: purchase.includesReport,
    a: purchase.chargedAmountUsdc,
    tx: purchase.transactionId,
    w: purchase.walletAddress,
    exp: now + RUN_GRANT_TTL_MS,
  });
}

export function readRunGrant(token: unknown, now = Date.now()): VerifiedPurchase | null {
  const payload = open(token, now);
  if (payload?.typ !== "grant") return null;
  return {
    decisionId: payload.d,
    source: payload.src,
    includesReport: payload.r === true,
    chargedAmountUsdc: payload.a,
    transactionId: payload.tx,
    walletAddress: payload.w,
  };
}

/** Lets its holder read the report for one completed paid request. It can't run or pay for anything. */
export function issueReportToken(capabilityId: string, decisionId: string, now = Date.now()): string {
  return seal({ typ: "report", d: decisionId, c: capabilityId, exp: now + REPORT_TOKEN_TTL_MS });
}

export function readReportToken(token: unknown, now = Date.now()): { capabilityId: string; decisionId: string } | null {
  const payload = open(token, now);
  return payload?.typ === "report" ? { capabilityId: payload.c, decisionId: payload.d } : null;
}

/** Held by the browser that started a PayPal checkout. It can only read and act on that one purchase. */
export function issuePurchaseToken(id: string, kind: "one_time" | "subscription", now = Date.now()): string {
  return seal({ typ: "purchase", p: id, k: kind, exp: now + REPORT_TOKEN_TTL_MS });
}

export function readPurchaseToken(token: unknown, now = Date.now()): { id: string; kind: "one_time" | "subscription" } | null {
  const payload = open(token, now);
  return payload?.typ === "purchase" ? { id: payload.p, kind: payload.k } : null;
}

/** True when a request was signed by our own frontend server for this exact route. */
export function verifyInternalRequest(
  input: { method: string; path: string; timestamp: unknown; signature: unknown },
  now = Date.now(),
): boolean {
  const key = signingKey();
  const { method, path, timestamp, signature } = input;
  if (!key || typeof timestamp !== "string" || typeof signature !== "string" || !/^\d+$/.test(timestamp)) return false;
  if (Math.abs(now - Number(timestamp)) > INTERNAL_REQUEST_MAX_AGE_MS) return false;
  return equalText(hmacHex(key, `internal\n${timestamp}\n${method}\n${path}`), signature);
}

const deliveriesByKey = new Map<string, number>();

/** Limits repeat emails and on-chain writes for one purchase. False once the limit is reached. */
export function reserveDelivery(kind: string, decisionId: string): boolean {
  const key = `${kind}:${decisionId}`;
  const used = deliveriesByKey.get(key) ?? 0;
  if (used >= MAX_DELIVERIES_PER_DECISION) return false;
  if (deliveriesByKey.size >= MAX_TRACKED_DECISIONS) {
    const oldest = deliveriesByKey.keys().next().value;
    if (oldest !== undefined) deliveriesByKey.delete(oldest);
  }
  deliveriesByKey.set(key, used + 1);
  return true;
}

export function jobAccessToken(jobId: string): string {
  return hmacHex(requireKey(), `job:${jobId}`);
}

export function verifyJobAccessToken(jobId: string, token: unknown): boolean {
  const key = signingKey();
  return Boolean(key && typeof token === "string" && equalText(hmacHex(key, `job:${jobId}`), token));
}

/** Polling path returned to x402 buyers. It carries the token so buyers can follow it as-is. */
export function buildStatusPath(jobId: string): string {
  return `/execution-status/${encodeURIComponent(jobId)}?accessToken=${jobAccessToken(jobId)}`;
}

function jobIdMatchesDecision(jobId: string, decisionId: string): boolean {
  const base = `selun-phase1-${decisionId}`.replace(/[^A-Za-z0-9-_]/g, "-");
  return /^[A-Za-z0-9-_]{1,200}$/.test(jobId) && (jobId === base || jobId.startsWith(`${base}-`));
}

const jobsByDecision = new Map<string, Set<string>>();

export type JobStartDecision =
  | { ok: true; jobToken: string }
  | { ok: false; status: 401 | 403 | 429 | 503; error: string };

/** Decides whether a grant may start this job. Counts each new job against the decision. */
export function authorizeJobStart(grant: unknown, jobId: string, now = Date.now()): JobStartDecision {
  if (!isJobAccessConfigured()) return { ok: false, status: 503, error: "Job access is not configured." };
  const payload = open(grant, now);
  if (payload?.typ !== "grant") {
    return { ok: false, status: 401, error: "A verified payment is required to start this job." };
  }
  if (!jobIdMatchesDecision(jobId, payload.d)) {
    return { ok: false, status: 403, error: "This payment does not cover that job." };
  }

  const jobs = jobsByDecision.get(payload.d) ?? new Set<string>();
  if (!jobs.has(jobId)) {
    if (jobs.size >= MAX_JOBS_PER_DECISION) {
      return { ok: false, status: 429, error: "This purchase has reached its retry limit. Please contact support." };
    }
    jobs.add(jobId);
    if (jobsByDecision.size >= MAX_TRACKED_DECISIONS) {
      const oldest = jobsByDecision.keys().next().value;
      if (oldest !== undefined) jobsByDecision.delete(oldest);
    }
    jobsByDecision.set(payload.d, jobs);
  }
  return { ok: true, jobToken: jobAccessToken(jobId) };
}
