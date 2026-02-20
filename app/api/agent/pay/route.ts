import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type AgentPaymentRequest = {
  walletAddress?: string;
  includeCertifiedDecisionRecord?: boolean;
  riskMode?: string;
  investmentHorizon?: string;
};

type BackendAgentResponse = {
  success?: boolean;
  error?: string;
  data?: {
    status?: "paid";
    transactionId?: string;
    decisionId?: string;
    agentNote?: string;
    chargedAmountUsdc?: string;
    certifiedDecisionRecordPurchased?: boolean;
  };
};

const isHexAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

export async function POST(req: Request): Promise<NextResponse> {
  let payload: AgentPaymentRequest;

  try {
    payload = (await req.json()) as AgentPaymentRequest;
  } catch {
    return NextResponse.json(
      {
        success: false,
        error: "Invalid payment payload.",
      },
      { status: 400 },
    );
  }

  if (!payload.walletAddress || !isHexAddress(payload.walletAddress)) {
    return NextResponse.json(
      {
        success: false,
        error: "Valid walletAddress is required before payment.",
      },
      { status: 400 },
    );
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/agent/pay`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        walletAddress: payload.walletAddress,
        includeCertifiedDecisionRecord: payload.includeCertifiedDecisionRecord,
        riskMode: payload.riskMode,
        investmentHorizon: payload.investmentHorizon,
      }),
      cache: "no-store",
    });

    const backendResult = (await response.json()) as BackendAgentResponse;
    if (!response.ok || !backendResult.success || !backendResult.data) {
      return NextResponse.json(
        {
          success: false,
          error: backendResult.error || "Agent payment failed.",
        },
        { status: response.status || 500 },
      );
    }

    return NextResponse.json({
      success: true,
      status: backendResult.data.status || "paid",
      transactionId: backendResult.data.transactionId,
      decisionId: backendResult.data.decisionId,
      agentNote: backendResult.data.agentNote,
      chargedAmountUsdc: backendResult.data.chargedAmountUsdc,
      certifiedDecisionRecordPurchased: Boolean(backendResult.data.certifiedDecisionRecordPurchased),
    });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Agent payment failed.",
      },
      { status: 502 },
    );
  }
}
