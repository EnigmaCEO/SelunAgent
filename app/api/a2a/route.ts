import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// A2A JSON-RPC 2.0 endpoint. Agenstry probes this with a no-op message/send heartbeat.
// Spec: https://google.github.io/A2A/specification/

type JsonRpcRequest = {
  jsonrpc: string;
  id: string | number | null;
  method: string;
  params?: unknown;
};

function jsonRpcError(id: string | number | null, code: number, message: string) {
  return NextResponse.json(
    { jsonrpc: "2.0", id, error: { code, message } },
    { status: 200, headers: { "Content-Type": "application/json" } },
  );
}

function jsonRpcResult(id: string | number | null, result: unknown) {
  return NextResponse.json(
    { jsonrpc: "2.0", id, result },
    { status: 200, headers: { "Content-Type": "application/json" } },
  );
}

export async function POST(req: NextRequest) {
  let body: JsonRpcRequest;
  try {
    body = await req.json();
  } catch {
    return jsonRpcError(null, -32700, "Parse error");
  }

  if (body.jsonrpc !== "2.0" || !body.method) {
    return jsonRpcError(body.id ?? null, -32600, "Invalid Request");
  }

  const id = body.id ?? null;

  // A2A v1.0: message/send
  // A2A v0.x: tasks/send
  if (body.method === "message/send" || body.method === "tasks/send") {
    return jsonRpcResult(id, {
      kind: "message",
      messageId: `selun-${Date.now()}`,
      role: "agent",
      parts: [
        {
          kind: "text",
          text: "Selun | Sagitta AAA is online. Submit a portfolio allocation request via the x402 payment-gated endpoints. See https://selun.sagitta.systems/for-developers for integration details.",
        },
      ],
      metadata: {
        agent: "selun-sagitta-aaa",
        protocolVersion: "1.0",
      },
    });
  }

  // A2A v1.0: message/stream (we don't stream, return error)
  if (body.method === "message/stream") {
    return jsonRpcError(id, -32601, "Streaming not supported. Use message/send.");
  }

  // tasks/get, tasks/cancel, etc. — stub responses
  if (body.method === "tasks/get") {
    return jsonRpcError(id, -32001, "Task not found");
  }

  if (body.method === "tasks/cancel") {
    return jsonRpcError(id, -32001, "Task not found");
  }

  return jsonRpcError(id, -32601, `Method not found: ${body.method}`);
}

// Agenstry may also GET the endpoint to check liveness
export async function GET() {
  return NextResponse.json({
    jsonrpc: "2.0",
    result: {
      agent: "selun-sagitta-aaa",
      protocolVersion: "1.0",
      status: "online",
    },
  });
}
