import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type Phase4RunRequest = {
  jobId?: string;
};

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function POST(req: Request) {
  let payload: Phase4RunRequest;

  try {
    payload = (await req.json()) as Phase4RunRequest;
  } catch {
    return NextResponse.json(
      {
        success: false,
        error: "Invalid phase4 payload.",
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
    const response = await fetch(`${getBackendBaseUrl()}/agent/phase4/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jobId: payload.jobId,
      }),
      cache: "no-store",
    });

    const result = (await response.json()) as unknown;
    return NextResponse.json(result, { status: response.status });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to start phase 4.",
      },
      { status: 502 },
    );
  }
}
