import { generateText } from "ai";
import { NextResponse } from "next/server";
import { createAgent } from "./create-agent";
import type { AgentHistoryMessage, AgentRequest, AgentResponse, WizardContext } from "@/app/types/agent";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const MAX_HISTORY_ITEMS = 8;

function formatHistory(history?: AgentHistoryMessage[]): string {
  if (!history || history.length === 0) return "None";

  return history
    .slice(-MAX_HISTORY_ITEMS)
    .map((entry) => `${entry.role === "assistant" ? "Selun" : "User"}: ${entry.content}`)
    .join("\n");
}

function formatContext(context?: WizardContext): string {
  if (!context) return "No wizard context provided.";

  return [
    `Risk tolerance: ${context.riskTolerance || "Not provided"}`,
    `Investment horizon: ${context.investmentHorizon || "Not provided"}`,
    `Portfolio summary: ${context.portfolioSummary || "Not provided"}`,
    `Objective: ${context.objective || "Not provided"}`,
    `Wallet: ${context.walletAddress || "Not connected yet"}`,
  ].join("\n");
}

function buildPrompt(payload: AgentRequest) {
  return `
Current wizard context:
${formatContext(payload.context)}

Recent conversation:
${formatHistory(payload.history)}

User request:
${payload.userMessage}

Respond with:
1) a short headline decision
2) top 3 reasons
3) one caution
4) one concrete next action.
  `.trim();
}

export async function POST(req: Request): Promise<NextResponse<AgentResponse>> {
  let payload: AgentRequest;

  try {
    payload = (await req.json()) as AgentRequest;
  } catch {
    return NextResponse.json(
      {
        success: false,
        error: "Invalid JSON payload.",
      },
      { status: 400 },
    );
  }

  if (!payload.userMessage?.trim()) {
    return NextResponse.json(
      {
        success: false,
        error: "userMessage is required.",
      },
      { status: 400 },
    );
  }

  try {
    const agent = await createAgent();
    const { text } = await generateText({
      ...agent,
      prompt: buildPrompt(payload),
    });

    return NextResponse.json({
      success: true,
      response: text,
    });
  } catch (error) {
    console.error("Selun agent route error:", error);
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Selun agent failed to respond.",
      },
      { status: 500 },
    );
  }
}
