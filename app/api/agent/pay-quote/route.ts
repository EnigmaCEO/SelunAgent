import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type PaymentQuoteRequest = {
  walletAddress?: string;
  includeCertifiedDecisionRecord?: boolean;
  promoCode?: string;
};

type BackendQuoteResponse = {
  success?: boolean;
  error?: string;
  data?: {
    totalBeforeDiscountUsdc?: string;
    chargedAmountUsdc?: string;
    discountAmountUsdc?: string;
    discountPercent?: number;
    promoCodeApplied?: boolean;
    promoCode?: string;
    certifiedDecisionRecordPurchased?: boolean;
    paymentMethod?: "onchain" | "free_code";
    message?: string;
  };
};

const isHexAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function POST(req: Request): Promise<NextResponse> {
  let payload: PaymentQuoteRequest;

  try {
    payload = (await req.json()) as PaymentQuoteRequest;
  } catch {
    return NextResponse.json(
      {
        success: false,
        error: "Invalid pay-quote payload.",
      },
      { status: 400 },
    );
  }

  if (!payload.walletAddress || !isHexAddress(payload.walletAddress)) {
    return NextResponse.json(
      {
        success: false,
        error: "Valid walletAddress is required before promo quote.",
      },
      { status: 400 },
    );
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/agent/pay-quote`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        walletAddress: payload.walletAddress,
        includeCertifiedDecisionRecord: payload.includeCertifiedDecisionRecord,
        promoCode: payload.promoCode,
      }),
      cache: "no-store",
    });

    const rawText = await response.text();
    let backendResult: BackendQuoteResponse | null = null;
    try {
      backendResult = rawText ? (JSON.parse(rawText) as BackendQuoteResponse) : null;
    } catch {
      backendResult = null;
    }

    if (!response.ok || !backendResult?.success || !backendResult.data) {
      const trimmed = rawText.trim();
      const looksLikeHtml =
        trimmed.startsWith("<!DOCTYPE") ||
        trimmed.startsWith("<html") ||
        (response.headers.get("content-type") || "").includes("text/html");
      const fallbackError = looksLikeHtml
        ? "Backend promo quote returned HTML instead of JSON. Verify SELUN_BACKEND_URL and backend deployment."
        : `Promo quote failed (HTTP ${response.status}).`;
      return NextResponse.json(
        {
          success: false,
          error: backendResult?.error || fallbackError,
        },
        { status: response.status >= 400 ? response.status : 502 },
      );
    }

    return NextResponse.json({
      success: true,
      ...backendResult.data,
    });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Promo quote failed.",
      },
      { status: 502 },
    );
  }
}
