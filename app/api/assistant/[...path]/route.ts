import { NextResponse } from "next/server";
import { legacyClientHeaders } from "@/app/lib/legacy-client-headers";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

const CONVERSATION_HEADER = "x-selun-conversation";

const ALLOWED_GET = [/^status$/, /^capabilities$/, /^actions\/[0-9a-f-]{36}$/];
const ALLOWED_POST = [/^chat$/, /^paypal\/subscription\/assets$/, /^actions\/[0-9a-f-]{36}\/(challenge|execute|decline|paypal-checkout)$/, /^report\/(download|email)$/, /^paypal\/(capture|status|cancel)$/];

function backendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

type RouteContext = { params: Promise<{ path: string[] }> };

async function relay(req: Request, path: string, init: { method: "GET" | "POST"; body?: string }) {
  const backendPath = `/assistant/${path}`;
  // legacyClientHeaders signs the customer's IP so per-client limits work behind
  // this proxy. Its signature is POST-only, and GETs have no client limit.
  const headers: Record<string, string> = init.method === "POST"
    ? legacyClientHeaders(req, backendPath)
    : { Accept: "application/json" };
  const conversation = req.headers.get(CONVERSATION_HEADER);
  if (conversation) headers[CONVERSATION_HEADER] = conversation;

  try {
    const response = await fetch(`${backendBaseUrl()}${backendPath}`, {
      method: init.method,
      headers,
      body: init.body,
      cache: "no-store",
    });
    if (response.ok && response.headers.get("content-type")?.includes("application/pdf")) {
      return new Response(await response.arrayBuffer(), {
        headers: {
          "Cache-Control": "no-store",
          "Content-Type": "application/pdf",
          "Content-Disposition": response.headers.get("content-disposition") ?? "attachment",
          "X-Selun-Record-Hash": response.headers.get("x-selun-record-hash") ?? "",
        },
      });
    }
    const result = (await response.json()) as unknown;
    const retryAfter = response.headers.get("Retry-After");
    return NextResponse.json(result, {
      status: response.status,
      headers: { "Cache-Control": "no-store", ...(retryAfter ? { "Retry-After": retryAfter } : {}) },
    });
  } catch {
    return NextResponse.json(
      { success: false, code: "service_unavailable", error: "Selun can't be reached right now. Please try again shortly." },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }
}

export async function GET(req: Request, context: RouteContext) {
  const path = (await context.params).path.join("/");
  if (!ALLOWED_GET.some((pattern) => pattern.test(path))) {
    return NextResponse.json({ success: false, error: "Not found." }, { status: 404 });
  }
  return relay(req, path, { method: "GET" });
}

export async function POST(req: Request, context: RouteContext) {
  const path = (await context.params).path.join("/");
  if (!ALLOWED_POST.some((pattern) => pattern.test(path))) {
    return NextResponse.json({ success: false, error: "Not found." }, { status: 404 });
  }
  const body = await req.text();
  if (body.length > 32_000) {
    return NextResponse.json({ success: false, code: "invalid_request", error: "That request is too large." }, { status: 413 });
  }
  return relay(req, path, { method: "POST", body: body || "{}" });
}
