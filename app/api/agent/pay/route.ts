import { generateText } from "ai";
import { NextResponse } from "next/server";
import { createAgent } from "../create-agent";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type AgentPaymentRequest = {
  walletAddress?: string;
  totalPriceUsdc?: number;
  includeCertifiedDecisionRecord?: boolean;
  riskMode?: string;
  investmentHorizon?: string;
};

type AgentPaymentResponse = {
  success: boolean;
  status?: "paid";
  transactionId?: string;
  decisionId?: string;
  agentNote?: string;
  error?: string;
};

const isHexAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);

function buildTransactionId() {
  const body = crypto.randomUUID().replace(/-/g, "").padEnd(64, "0").slice(0, 64);
  return `0x${body}`;
}

async function generateAgentPaymentNote(payload: AgentPaymentRequest): Promise<string> {
  if (!process.env.OPENAI_API_KEY) {
    return "Selun agent approved payment via fallback policy check for this allocation run.";
  }

  const agent = await createAgent();
  const { text } = await generateText({
    model: agent.model,
    system: `${agent.system}
You are approving a wizard payment checkpoint. Do not request additional tools.
Keep response to one concise sentence.`,
    prompt: `Confirm payment approval:
- Wallet: ${payload.walletAddress}
- Amount: ${payload.totalPriceUsdc} USDC
- Certified decision record: ${payload.includeCertifiedDecisionRecord ? "Enabled" : "Disabled"}
- Risk mode: ${payload.riskMode || "N/A"}
- Horizon: ${payload.investmentHorizon || "N/A"}`,
    maxSteps: 1,
  });

  return text.trim();
}

export async function POST(req: Request): Promise<NextResponse<AgentPaymentResponse>> {
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

  if (!payload.totalPriceUsdc || payload.totalPriceUsdc <= 0) {
    return NextResponse.json(
      {
        success: false,
        error: "totalPriceUsdc must be greater than zero.",
      },
      { status: 400 },
    );
  }

  try {
    const agentNote = await generateAgentPaymentNote(payload);
    await new Promise((resolve) => setTimeout(resolve, 900));

    return NextResponse.json({
      success: true,
      status: "paid",
      transactionId: buildTransactionId(),
      decisionId: `SELUN-DEC-${Date.now()}`,
      agentNote,
    });
  } catch (error) {
    console.error("Selun agent payment error:", error);
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Agent payment failed.",
      },
      { status: 500 },
    );
  }
}
