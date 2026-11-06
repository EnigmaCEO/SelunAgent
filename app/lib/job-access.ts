import { createHmac, timingSafeEqual } from "node:crypto";

// Server-only. Signs the same formats as backend/src/services/job-access.service.ts.
const RUN_GRANT_TTL_MS = 24 * 60 * 60_000;
export const JOB_TOKEN_HEADER = "x-selun-job-token";

function signingKey(): Buffer {
  const secret = process.env.SELUN_REFERRAL_INTERNAL_TOKEN?.trim() || process.env.SELUN_ADMIN_API_TOKEN?.trim();
  if (!secret) throw new Error("Job access signing is not configured.");
  return createHmac("sha256", secret).update("selun-job-access-v1").digest();
}

export type VerifiedPurchase = {
  decisionId: string;
  source: string;
  includesReport: boolean;
  chargedAmountUsdc: string;
  transactionId: string;
  walletAddress: string;
};

/** Call only after Stripe has confirmed payment. */
export function issueCardRunGrant(
  decisionId: string,
  purchase: { includesReport: boolean; chargedAmountUsdc: string; transactionId: string } = {
    includesReport: false,
    chargedAmountUsdc: "0",
    transactionId: "",
  },
): string {
  const body = Buffer.from(
    JSON.stringify({
      typ: "grant",
      d: decisionId,
      src: "card",
      r: purchase.includesReport,
      a: purchase.chargedAmountUsdc,
      tx: purchase.transactionId,
      w: "",
      exp: Date.now() + RUN_GRANT_TTL_MS,
    }),
    "utf8",
  ).toString("base64url");
  return `${body}.${createHmac("sha256", signingKey()).update(body).digest("hex")}`;
}

export function readRunGrant(token: unknown): VerifiedPurchase | null {
  if (typeof token !== "string" || token.length > 2_000) return null;
  const [body, signature, extra] = token.split(".");
  if (!body || !signature || extra !== undefined) return null;
  const expected = Buffer.from(createHmac("sha256", signingKey()).update(body).digest("hex"), "utf8");
  const provided = Buffer.from(signature, "utf8");
  if (expected.length !== provided.length || !timingSafeEqual(expected, provided)) return null;
  try {
    const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
    if (payload.typ !== "grant" || typeof payload.exp !== "number" || payload.exp <= Date.now()) return null;
    return {
      decisionId: payload.d,
      source: payload.src,
      includesReport: payload.r === true,
      chargedAmountUsdc: payload.a,
      transactionId: payload.tx,
      walletAddress: payload.w,
    };
  } catch {
    return null;
  }
}

/** Proves to the backend that this request comes from this server, for this route. */
export function internalServiceHeaders(path: string, method = "POST"): Record<string, string> {
  const timestamp = String(Date.now());
  return {
    "x-selun-internal-timestamp": timestamp,
    "x-selun-internal-signature": createHmac("sha256", signingKey()).update(`internal\n${timestamp}\n${method}\n${path}`).digest("hex"),
  };
}

export function jobAccessToken(jobId: string): string {
  return createHmac("sha256", signingKey()).update(`job:${jobId}`).digest("hex");
}

/** Passes the browser's job token through a proxy to the backend. */
export function forwardJobToken(req: Request): Record<string, string> {
  const token = req.headers.get(JOB_TOKEN_HEADER);
  return token ? { [JOB_TOKEN_HEADER]: token } : {};
}
