import { isAgentProgramReferrerId, normalizeReferralCode } from "@/app/lib/referral";

type TrackReferralConversionInput = {
  referralCode: string | null | undefined;
  transactionId: string | null | undefined;
  amountUsd: number | string | null | undefined;
  allocationAmountUsd?: number | string | null | undefined;
  reportAmountUsd?: number | string | null | undefined;
  paymentReference?: string | null | undefined;
  userId?: string | null | undefined;
  source?: string | null | undefined;
  status?: "pending" | "confirmed" | "paid" | "rejected";
};

let missingReferralTrackTokenLogged = false;

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

function getReferralTrackToken() {
  return process.env.SELUN_REFERRAL_INTERNAL_TOKEN?.trim() || process.env.SELUN_ADMIN_API_TOKEN?.trim() || "";
}

async function postBackendJson(path: string, payload: Record<string, unknown>) {
  const response = await fetch(`${getBackendBaseUrl()}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    cache: "no-store",
    signal: AbortSignal.timeout(2_500),
  });

  if (!response.ok) {
    const responseText = await response.text().catch(() => "");
    console.error("Agent referral backend request failed:", response.status, responseText || "<empty>");
  }
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export async function trackReferralConversionOnBackend(input: TrackReferralConversionInput): Promise<void> {
  const referralCode = normalizeReferralCode(input.referralCode);
  const transactionId = input.transactionId?.trim() ?? "";
  const amountUsd = toFiniteNumber(input.amountUsd);
  const allocationAmountUsd = toFiniteNumber(input.allocationAmountUsd);
  const reportAmountUsd = toFiniteNumber(input.reportAmountUsd);
  const userId = input.userId?.trim() ?? "";
  const source = input.source?.trim() ?? "";
  const paymentReference = input.paymentReference?.trim() ?? "";

  if (!referralCode || !transactionId || amountUsd === null || amountUsd < 0) {
    return;
  }

  if (isAgentProgramReferrerId(referralCode)) {
    try {
      if ((input.status === "confirmed" || input.status === "paid")) {
        await postBackendJson("/api/allocation/confirm", {
          referrer_id: referralCode,
          user_id: userId || transactionId,
          allocation_id: transactionId,
          amount: amountUsd,
          ...(allocationAmountUsd !== null ? { allocation_amount: allocationAmountUsd } : {}),
          ...(reportAmountUsd !== null ? { report_amount: reportAmountUsd } : {}),
          ...(paymentReference ? { payment_reference: paymentReference } : {}),
          ...(source ? { source } : {}),
        });
      } else if (input.status === "pending" && userId) {
        await postBackendJson("/api/referral/track", {
          referrer_id: referralCode,
          user_id: userId,
          event: "click",
          ...(source ? { source } : {}),
        });
      }
    } catch (error) {
      console.error("Agent referral program request error:", error);
    }
    return;
  }

  const token = getReferralTrackToken();
  if (!token) {
    if (!missingReferralTrackTokenLogged) {
      missingReferralTrackTokenLogged = true;
      console.warn("Referral tracking token missing. Stripe referral conversions will not be recorded.");
    }
    return;
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/referral/track`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Selun-Referral-Token": token,
      },
      body: JSON.stringify({
        referralCode,
        transactionId,
        amountUsd,
        status: input.status,
      }),
      cache: "no-store",
      signal: AbortSignal.timeout(2_500),
    });

    if (!response.ok) {
      const responseText = await response.text().catch(() => "");
      console.error("Referral tracking request failed:", response.status, responseText || "<empty>");
    }
  } catch (error) {
    console.error("Referral tracking request error:", error);
  }
}
