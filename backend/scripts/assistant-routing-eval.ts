/**
 * Checks that the configured model picks the right Selun service for typical requests.
 *
 *   npm run assistant:routing-eval -- --runs=3
 *
 * Uses the real model and costs its tokens. Nothing is paid for or run: the gateway below
 * refuses every call, and the assistant can only show quotes without a wallet signature.
 */
import path from "node:path";
import dotenv from "dotenv";
import { AssistantService, type Block } from "../src/assistant/assistant.service";
import { OpenAiClient, readOpenAiConfig } from "../src/assistant/openai-client";
import { getSiteKnowledge } from "../src/assistant/sagitta-sites.runtime";
import { getScorecardPreflight } from "../src/assistant/scorecard-preflight.runtime";

dotenv.config({ quiet: true });
dotenv.config({ path: path.join("backend", ".env"), override: true, quiet: true });

type Outcome = "capabilities" | "handoff" | "asset_choice" | "sagitta_directory" | "sagitta_system" | "sagitta_sources" | string;
type Case = { say: string; must?: Outcome[]; may?: Outcome[] };

const SCORECARD = "asset_scorecard";
const COMPREHENSIVE = "comprehensive_report";
const MONTHLY = "monthly_intelligence";
const REGIME = "market_regime";

const CASES: Case[] = [
  { say: "Give me a market update on Bitcoin.", must: [SCORECARD], may: [REGIME] },
  { say: "Give me a Bitcoin market update", must: [SCORECARD], may: [REGIME] },
  { say: "Tell me about BTC", must: [SCORECARD] },
  { say: "What can you tell me about Solana?", must: [SCORECARD] },
  { say: "Analyze the risks of Ethereum", must: [SCORECARD] },
  { say: "Compare BTC and ETH", must: [SCORECARD] },
  { say: "How good a token is AAVE as an asset?", must: [SCORECARD] },
  { say: "How is the crypto market right now?", must: [REGIME] },
  { say: "Give me a market update", must: [REGIME] },
  { say: "Is it safe to use DeFi right now?", must: ["sce_continuity_mode"] },
  { say: "I'm about to deposit into the Aave protocol. Is it safe to use?", must: ["sce_risk_evaluate"] },
  { say: "Are there any known incidents or threats involving Uniswap?", must: ["sce_case_relevance"] },
  { say: "What exposure limits fit a conservative investor with a 3+ year horizon?", must: ["policy_envelope"] },
  { say: "I hold $4000 of BTC and $1000 of USDC. How should I rebalance?", must: ["rebalance"] },
  { say: "Should I rebalance my portfolio?" },
  { say: "Help me build a crypto portfolio", must: ["handoff"] },
  { say: "What services can you provide?", must: ["capabilities"] },
  { say: "Analyze polygon", must: ["asset_choice"] },
  { say: "Analyze notarealcoin" },
  { say: "What is a stablecoin?" },
  // No service gives these. Offering the closest paid analysis is fine; anything else isn't.
  { say: "What is the price of Bitcoin right now?", may: [SCORECARD, REGIME] },
  { say: "Buy me 1 BTC", may: [SCORECARD, REGIME] },
  // Questions about Sagitta are free and never sell anything.
  { say: "What is Sagitta Radar?", must: ["sagitta_system"], may: ["sagitta_sources"] },
  { say: "Tell me about the Sagitta Continuity Engine", must: ["sagitta_system"], may: ["sagitta_sources"] },
  { say: "What's the difference between AAA and Selun?", must: ["sagitta_sources"], may: ["sagitta_system"] },
  { say: "What does Sagitta Systems do?", may: ["sagitta_directory", "sagitta_sources"] },
  { say: "What products does Sagitta have?", may: ["sagitta_directory", "sagitta_sources", "capabilities"] },
  { say: "Is Sagitta Wallet available yet?", must: ["sagitta_system"], may: ["sagitta_sources"] },
  { say: "Can I use Sagitta Banking today?", must: ["sagitta_system"], may: ["sagitta_sources"] },
  { say: "How much does Radar cost?", must: ["sagitta_sources"], may: ["sagitta_system"] },
  { say: "Who invests in Sagitta and what's its revenue?", may: ["sagitta_sources", "sagitta_directory"] },
  { say: "Who is the developer of Sagitta?", must: ["sagitta_sources"], may: ["sagitta_system", "sagitta_directory"] },
  { say: "Who founded Sagitta?", must: ["sagitta_sources"], may: ["sagitta_directory"] },
  { say: "What's on Sagitta's roadmap?", must: ["sagitta_sources"], may: ["sagitta_directory"] },
  { say: "Is Sagitta hiring?", must: ["sagitta_sources"], may: ["sagitta_directory"] },
  { say: "What does the Sagitta whitepaper say about the Reserve?", must: ["sagitta_sources"], may: ["sagitta_system"] },
  // Sold through PayPal only.
  { say: "I want a comprehensive report on Bitcoin", must: [COMPREHENSIVE] },
  { say: "Can I get a full report on ETH and SOL? I'd rather pay with PayPal", must: [COMPREHENSIVE] },
  { say: "I don't have a crypto wallet. Can I pay by card for a report on Bitcoin?", must: [COMPREHENSIVE] },
  { say: "I want a monthly report on Bitcoin", must: [MONTHLY] },
  { say: "Sign me up for a subscription covering BTC and ETH", must: [MONTHLY] },
  // A named asset is needed first, so a question comes back rather than a card.
  { say: "I'd like a comprehensive report" },
  { say: "I want a monthly subscription" },
];

function outcomes(blocks: Block[]): Outcome[] {
  return blocks.flatMap((block) => {
    if (block.type === "action") return [block.action.capabilityId];
    return block.type === "notice" ? [] : [block.type];
  });
}

function problem(testCase: Case, seen: Outcome[]): string | null {
  const allowed = new Set([...(testCase.must ?? []), ...(testCase.may ?? [])]);
  const missing = (testCase.must ?? []).filter((outcome) => !seen.includes(outcome));
  const extra = seen.filter((outcome) => !allowed.has(outcome));
  if (missing.length === 0 && extra.length === 0) return null;
  return [missing.length ? `missing ${missing.join(", ")}` : "", extra.length ? `unexpected ${extra.join(", ")}` : ""].filter(Boolean).join("; ");
}

async function main() {
  const config = readOpenAiConfig();
  if (!config) throw new Error("Set OPENAI_API_KEY and OPENAI_MODEL in backend/.env first.");
  const runsArgument = process.argv.find((argument) => argument.startsWith("--runs="));
  const runs = Math.max(1, Number.parseInt(runsArgument?.slice("--runs=".length) ?? "1", 10) || 1);

  const refuse = async () => {
    throw new Error("The routing eval never calls a paid route.");
  };
  const prices: Record<string, string> = {
    market_regime: "0.25", policy_envelope: "0.25", asset_scorecard: "0.5", rebalance: "1",
    sce_continuity_mode: "0.01", sce_case_relevance: "0.05", sce_risk_evaluate: "0.25", allocate: "19", allocate_with_report: "34",
  };
  const service = new AssistantService({
    llm: new OpenAiClient(config),
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    sites: getSiteKnowledge(),
    preflight: getScorecardPreflight(),
    paypal: { prices: { reportUsd: "4.99", subscriptionUsd: "9.99" }, createCheckout: refuse, createSubscription: refuse },
    loadCatalog: async () => ({
      resources: Object.entries(prices).map(([productId, amountUsdc]) => ({
        productId,
        endpoint: `/agent/x402/${productId.replaceAll("_", "-")}`,
        pricing: { amountUsdc },
      })),
    }),
    limits: { dailyTurnLimit: 100_000 },
  });

  console.log(`Model: ${config.model}. Each request runs ${runs} time(s).\n`);
  let failures = 0;
  for (const testCase of CASES) {
    const seenInRuns: string[] = [];
    let failedRuns = 0;
    for (let run = 0; run < runs; run += 1) {
      try {
        const reply = await service.chat({ message: testCase.say });
        const seen = outcomes(reply.blocks);
        const issue = problem(testCase, seen);
        if (issue) failedRuns += 1;
        seenInRuns.push(issue ? `[${seen.join(", ") || "no card"}] ${issue}` : `[${seen.join(", ") || "no card"}]`);
      } catch (error) {
        failedRuns += 1;
        seenInRuns.push(`error: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    failures += failedRuns;
    console.log(`${failedRuns ? "FAIL" : "PASS"}  "${testCase.say}"\n      ${[...new Set(seenInRuns)].join("\n      ")}`);
  }
  const total = CASES.length * runs;
  console.log(`\n${total - failures}/${total} runs chose correctly.`);
  process.exit(failures ? 1 : 0);
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(2);
});
