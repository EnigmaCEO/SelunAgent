import { NextResponse } from "next/server";
import { legacyClientHeaders } from "@/app/lib/legacy-client-headers";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function POST(req: Request): Promise<Response> {
  let payload: unknown;

  try {
    payload = await req.json();
  } catch {
    return NextResponse.json({ success: false, error: "Invalid request payload." }, { status: 400 });
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/agent/result-email`, {
      method: "POST",
      headers: legacyClientHeaders(req, "/agent/result-email"),
      body: JSON.stringify(payload),
      cache: "no-store",
    });

    const result = (await response.json().catch(() => ({}))) as {
      success?: boolean;
      error?: string;
      data?: {
        status?: "sent" | "skipped" | "failed";
        error?: string | null;
      };
    };

    return NextResponse.json(
      {
        success: Boolean(result.success),
        status: result.data?.status,
        error: result.data?.error ?? result.error,
      },
      { status: response.status, headers: response.headers.has("Retry-After") ? { "Retry-After": response.headers.get("Retry-After")! } : {} },
    );
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to send result email via backend.",
      },
      { status: 502 },
    );
  }
}
