import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type UsdcBalanceRequest = {
  walletAddress?: string;
};

const isHexAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);
const BACKEND_TIMEOUT_MS = 20_000;

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
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort("Backend USDC balance request timed out."), BACKEND_TIMEOUT_MS);

    try {
      const response = await fetch(`${getBackendBaseUrl()}/agent/usdc-balance`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ walletAddress: payload.walletAddress }),
        cache: "no-store",
        signal: controller.signal,
      });

      const contentType = response.headers.get("content-type") ?? "";
      if (contentType.includes("application/json")) {
        const result = (await response.json()) as unknown;
        return NextResponse.json(result, { status: response.status });
      }

      const textBody = await response.text();
      return NextResponse.json(
        {
          success: response.ok,
          error: textBody || "Backend returned a non-JSON response.",
        },
        { status: response.status },
      );
    } finally {
      clearTimeout(timeoutId);
    }
  } catch (error) {
    const timeoutError = error instanceof Error && error.name === "AbortError";
    return NextResponse.json(
      {
        success: false,
        error: timeoutError
          ? "USDC balance check timed out while contacting backend."
          : (error instanceof Error ? error.message : "Failed to query backend USDC balance."),
      },
      { status: timeoutError ? 504 : 502 },
    );
  }
}
