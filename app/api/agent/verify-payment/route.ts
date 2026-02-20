import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type VerifyPaymentRequest = {
  fromAddress?: string;
  expectedAmountUSDC?: number | string;
  transactionHash?: string;
};

const isHexAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);
const isTransactionHash = (value: string) => /^0x[a-fA-F0-9]{64}$/.test(value);

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function POST(req: Request) {
  let payload: VerifyPaymentRequest;

  try {
    payload = (await req.json()) as VerifyPaymentRequest;
  } catch {
    return NextResponse.json(
      {
        success: false,
        error: "Invalid verify-payment payload.",
      },
      { status: 400 },
    );
  }

  if (!payload.fromAddress || !isHexAddress(payload.fromAddress)) {
    return NextResponse.json(
      {
        success: false,
        error: "Valid fromAddress is required.",
      },
      { status: 400 },
    );
  }

  const expectedAmount =
    typeof payload.expectedAmountUSDC === "number"
      ? payload.expectedAmountUSDC
      : Number.parseFloat(payload.expectedAmountUSDC ?? "");
  if (!Number.isFinite(expectedAmount) || expectedAmount <= 0) {
    return NextResponse.json(
      {
        success: false,
        error: "expectedAmountUSDC must be greater than zero.",
      },
      { status: 400 },
    );
  }

  if (payload.transactionHash && !isTransactionHash(payload.transactionHash)) {
    return NextResponse.json(
      {
        success: false,
        error: "transactionHash must be a valid transaction hash.",
      },
      { status: 400 },
    );
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/agent/verify-payment`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        fromAddress: payload.fromAddress,
        expectedAmountUSDC: expectedAmount,
        transactionHash: payload.transactionHash,
      }),
      cache: "no-store",
    });

    const result = (await response.json()) as unknown;
    return NextResponse.json(result, { status: response.status });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to verify payment with backend.",
      },
      { status: 502 },
    );
  }
}
