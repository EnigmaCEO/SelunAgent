import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type Phase1RunRequest = {
  jobId?: string;
  executionTimestamp?: string;
  riskMode?: string;
  riskTolerance?: string;
  investmentTimeframe?: string;
  timeWindow?: string;
  walletAddress?: string;
};

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function POST(req: Request) {
  let payload: Phase1RunRequest;

  try {
    payload = (await req.json()) as Phase1RunRequest;
  } catch {
    return NextResponse.json(
      {
        success: false,
        error: "Invalid phase1 payload.",
      },
      { status: 400 },
    );
  }

  if (!payload.jobId || typeof payload.jobId !== "string" || !payload.jobId.trim()) {
    return NextResponse.json(
      {
        success: false,
        error: "jobId is required.",
      },
      { status: 400 },
    );
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/agent/phase1/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jobId: payload.jobId,
        executionTimestamp: payload.executionTimestamp,
        riskMode: payload.riskMode,
        riskTolerance: payload.riskTolerance,
        investmentTimeframe: payload.investmentTimeframe,
        timeWindow: payload.timeWindow,
        walletAddress: payload.walletAddress,
      }),
      cache: "no-store",
    });

    const result = (await response.json()) as unknown;
    return NextResponse.json(result, { status: response.status });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to start phase 1.",
      },
      { status: 502 },
    );
  }
}
