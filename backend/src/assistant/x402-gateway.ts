import { decodePaymentRequiredHeader } from "@x402/core/http";
import type { PaymentRequired } from "@x402/core/types";

/**
 * Calls Selun's existing x402 routes over HTTP, exactly as an outside buyer
 * would. Verification, settlement, idempotency, and replay protection all stay
 * in those routes. Nothing here can mark a payment as made.
 */

export type ChallengeOutcome =
  | { kind: "challenge"; paymentRequired: PaymentRequired }
  | { kind: "rate_limited"; retryAfterSeconds: number }
  | { kind: "error"; status: number; message: string };

export type PaidOutcome =
  | { kind: "completed"; data: Record<string, unknown> }
  /** The route refused the payment before settling, so nothing was charged. */
  | { kind: "payment_rejected"; message: string }
  | { kind: "rate_limited"; retryAfterSeconds: number; reason?: string }
  /** Failed after the payment step may have run. A retry reuses the same decisionId. */
  | { kind: "failed"; status: number; message: string; mayHaveCharged: boolean };

export interface X402Gateway {
  requestChallenge(endpoint: string, body: Record<string, unknown>): Promise<ChallengeOutcome>;
  submitPaid(endpoint: string, body: Record<string, unknown>, paymentSignature: string, timeoutMs: number): Promise<PaidOutcome>;
}

function retryAfter(response: Response): number {
  const parsed = Number.parseInt(response.headers.get("retry-after") ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 30;
}

async function readJson(response: Response): Promise<Record<string, unknown>> {
  try {
    const value = await response.json();
    return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

function errorText(body: Record<string, unknown>, fallback: string): string {
  return typeof body.error === "string" && body.error ? body.error : fallback;
}

export function classifyPaidFailure(status: number, body: Record<string, unknown>): PaidOutcome {
  const message = errorText(body, `The service returned HTTP ${status}.`);
  // These routes reply this way before calling processSettlement, or when
  // settlement failed with no transaction, so no funds moved.
  const beforeSettlement =
    status === 400 ||
    status === 409 ||
    (status === 502 && (message.includes("verification") || message.includes("settlement failed") || message.includes("payer address")));
  if (beforeSettlement) return { kind: "payment_rejected", message };
  return { kind: "failed", status, message, mayHaveCharged: true };
}

export class HttpX402Gateway implements X402Gateway {
  constructor(
    private readonly baseUrl: string,
    private readonly fetchImpl: typeof fetch = fetch,
  ) {}

  private post(endpoint: string, body: Record<string, unknown>, headers: Record<string, string>, timeoutMs: number) {
    return this.fetchImpl(`${this.baseUrl}${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    });
  }

  async requestChallenge(endpoint: string, body: Record<string, unknown>): Promise<ChallengeOutcome> {
    let response: Response;
    try {
      response = await this.post(endpoint, body, {}, 20_000);
    } catch {
      return { kind: "error", status: 503, message: "The service could not be reached." };
    }
    if (response.status === 429) return { kind: "rate_limited", retryAfterSeconds: retryAfter(response) };
    const header = response.headers.get("payment-required");
    if (response.status === 402 && header) {
      try {
        return { kind: "challenge", paymentRequired: decodePaymentRequiredHeader(header) };
      } catch {
        return { kind: "error", status: 502, message: "The service returned an unreadable payment request." };
      }
    }
    const responseBody = await readJson(response);
    return { kind: "error", status: response.status, message: errorText(responseBody, `Unexpected HTTP ${response.status}.`) };
  }

  async submitPaid(
    endpoint: string,
    body: Record<string, unknown>,
    paymentSignature: string,
    timeoutMs: number,
  ): Promise<PaidOutcome> {
    let response: Response;
    try {
      response = await this.post(endpoint, body, { "PAYMENT-SIGNATURE": paymentSignature }, timeoutMs);
    } catch {
      return { kind: "failed", status: 504, message: "The service did not respond in time.", mayHaveCharged: true };
    }
    const responseBody = await readJson(response);
    if (response.status === 429) {
      return { kind: "rate_limited", retryAfterSeconds: retryAfter(response), reason: typeof responseBody.reason === "string" ? responseBody.reason : undefined };
    }
    if (response.status === 402) {
      let reason = errorText(responseBody, "The payment was not accepted.");
      const header = response.headers.get("payment-required");
      if (header) {
        try {
          reason = decodePaymentRequiredHeader(header).error || reason;
        } catch {
          // Keep the body-derived reason.
        }
      }
      return { kind: "payment_rejected", message: reason };
    }
    if (response.status === 200 && responseBody.success === true && responseBody.data && typeof responseBody.data === "object") {
      return { kind: "completed", data: responseBody.data as Record<string, unknown> };
    }
    return classifyPaidFailure(response.status, responseBody);
  }
}
