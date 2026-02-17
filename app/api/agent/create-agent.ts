import { openai } from "@ai-sdk/openai";
import { getVercelAITools } from "@coinbase/agentkit-vercel-ai-sdk";
import { prepareAgentkitAndWalletProvider } from "./prepare-agentkit";

type SelunAgent = {
  tools: ReturnType<typeof getVercelAITools>;
  system: string;
  model: ReturnType<typeof openai>;
  maxSteps: number;
};

let cachedAgent: SelunAgent | null = null;

function buildSystemPrompt(networkId: string) {
  return `
You are Selun, an autonomous crypto allocation agent for retail investors.
You operate on ${networkId} and can use AgentKit tools for onchain context.
Rules:
- Keep recommendations concise, structured, and practical.
- Explain risk concentration, diversification, and liquidity implications.
- Ask one clarifying question only when required for safety.
- Never claim guaranteed returns and avoid overconfident language.
- If a requested action is unavailable, say it clearly and suggest a nearby supported action.
  `.trim();
}

export async function createAgent(): Promise<SelunAgent> {
  if (cachedAgent) return cachedAgent;

  if (!process.env.OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY is required to run Selun Agent.");
  }

  const { agentkit, walletProvider } = await prepareAgentkitAndWalletProvider();
  const modelName = process.env.SELUN_AGENT_MODEL ?? "gpt-4o-mini";
  const networkId = walletProvider.getNetwork().networkId ?? "unknown-network";

  cachedAgent = {
    tools: getVercelAITools(agentkit),
    system: buildSystemPrompt(networkId),
    model: openai(modelName),
    maxSteps: 8,
  };

  return cachedAgent;
}
