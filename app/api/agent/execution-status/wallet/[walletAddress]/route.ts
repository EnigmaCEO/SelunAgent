import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function GET(
  _req: Request,
  context: { params: Promise<{ walletAddress: string }> },
) {
  const { walletAddress } = await context.params;

  if (!walletAddress?.trim()) {
    return NextResponse.json(
      {
        success: false,
        error: "walletAddress is required.",
      },
      { status: 400 },
    );
  }

  try {
    const response = await fetch(
      `${getBackendBaseUrl()}/execution-status/wallet/${encodeURIComponent(walletAddress.trim())}`,
      {
        method: "GET",
        cache: "no-store",
      },
    );

    const result = (await response.json()) as unknown;
    return NextResponse.json(result, { status: response.status });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to fetch wallet execution status.",
      },
      { status: 502 },
    );
  }
}
