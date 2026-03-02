import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

function getAdminHeaders(req: Request): HeadersInit {
  const token =
    req.headers.get("x-selun-admin-token")?.trim() ||
    req.headers.get("authorization")?.trim();

  if (!token) {
    return {};
  }

  if (token.toLowerCase().startsWith("bearer ")) {
    return { Authorization: token };
  }

  return { "X-Selun-Admin-Token": token };
}

export async function POST(req: Request) {
  try {
    const response = await fetch(`${getBackendBaseUrl()}/agent/admin/treasury-smart-account`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        ...getAdminHeaders(req),
      },
      cache: "no-store",
    });

    const result = (await response.json().catch(() => ({}))) as unknown;
    return NextResponse.json(result, { status: response.status });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to initialize treasury smart account.",
      },
      { status: 502 },
    );
  }
}
