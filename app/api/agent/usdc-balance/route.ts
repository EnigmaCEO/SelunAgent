import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type UsdcBalanceRequest = {
  walletAddress?: string;
};

const isHexAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function POST(req: Request) {
  let payload: UsdcBalanceRequest;

  try {
    payload = (await req.json()) as UsdcBalanceRequest;
  } catch {
    return NextResponse.json(
      {
        success: false,
        error: "Invalid JSON payload.",
      },
      { status: 400 },
    );
  }

  if (!payload.walletAddress || !isHexAddress(payload.walletAddress)) {
    return NextResponse.json(
      {
        success: false,
        error: "Valid walletAddress is required.",
      },
      { status: 400 },
    );
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/agent/usdc-balance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ walletAddress: payload.walletAddress }),
      cache: "no-store",
    });

    const result = (await response.json()) as unknown;
    return NextResponse.json(result, { status: response.status });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to query backend USDC balance.",
      },
      { status: 502 },
    );
  }
}
