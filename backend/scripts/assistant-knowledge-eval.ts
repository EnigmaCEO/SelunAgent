/**
 * Asks the configured model basic questions about Sagitta and checks each answer against the sites.
 *
 *   npm run assistant:knowledge-eval -- --runs=3 --area=Radar
 *
 * Uses the real model and costs its tokens. Nothing is paid for or run: the gateway below
 * refuses every call, and these questions are never allowed to offer a paid service.
 */
import path from "node:path";
import dotenv from "dotenv";
import { AssistantService, type Block } from "../src/assistant/assistant.service";
import { OpenAiClient, readOpenAiConfig, type LlmClient } from "../src/assistant/openai-client";
import { KNOWLEDGE_CASES, type KnowledgeCase } from "../src/assistant/sagitta-knowledge-cases";
import { EXCLUDED_NAMES } from "../src/assistant/sagitta-sites";
import { getSiteKnowledge } from "../src/assistant/sagitta-sites.runtime";

dotenv.config({ quiet: true });
dotenv.config({ path: path.join("backend", ".env"), override: true, quiet: true });

const CONCURRENCY = 6;
/** gpt-5.4-mini list prices per million tokens, for the cost estimate only. */
const RATES = { input: 0.75, cached: 0.075, output: 4.5 };

function argument(name: string): string | undefined {
  return process.argv.find((item) => item.startsWith(`--${name}=`))?.slice(name.length + 3);
}

/** What the reader sees as the answer. The list of sources isn't part of it. */
function answerText(text: string, blocks: Block[]): string {
  return [text, ...blocks.filter((block) => block.type !== "sagitta_sources").map((block) => JSON.stringify(block))].join("\n").replace(/\*\*/g, "");
}

function problems(testCase: KnowledgeCase, text: string, blocks: Block[]): string[] {
  const answer = answerText(text, blocks);
  const found: string[] = [];
  for (const fact of [...(testCase.facts ?? []), ...(testCase.reply ?? [])]) {
    if (!fact.test(answer)) found.push(`missing ${fact.source}`);
  }
  for (const banned of testCase.none ?? []) {
    if (banned.test(answer)) found.push(`shouldn't say ${banned.source}`);
  }
  if (EXCLUDED_NAMES.test(`${text}${JSON.stringify(blocks)}`)) found.push("names something Selun doesn't discuss");
  if (blocks.some((block) => block.type === "action")) found.push("offered a paid service to an information question");
  return found;
}

async function main() {
  const config = readOpenAiConfig();
  if (!config) throw new Error("Set OPENAI_API_KEY and OPENAI_MODEL in backend/.env first.");
  const runs = Math.max(1, Number.parseInt(argument("runs") ?? "1", 10) || 1);
  const area = argument("area");
  const cases = KNOWLEDGE_CASES.filter((item) => !area || item.area === area);

  const refuse = async () => {
    throw new Error("The knowledge eval never calls a paid route.");
  };
  const spent = { requests: 0, input: 0, cached: 0, output: 0 };
  const tools = new Map<string, number>();
  const client = new OpenAiClient(config);
  const counted: LlmClient = {
    complete: async (request) => {
      const response = await client.complete(request);
      spent.requests += 1;
      spent.input += response.usage?.inputTokens ?? 0;
      spent.cached += response.usage?.cachedTokens ?? 0;
      spent.output += response.usage?.outputTokens ?? 0;
      for (const call of response.toolCalls) tools.set(call.function.name, (tools.get(call.function.name) ?? 0) + 1);
      return response;
    },
  };
  const service = new AssistantService({
    llm: counted,
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    sites: getSiteKnowledge(),
    paypal: { prices: { reportUsd: "4.99", subscriptionUsd: "9.99" }, createCheckout: refuse, createSubscription: refuse },
    loadCatalog: async () => ({ resources: [] }),
    limits: { dailyTurnLimit: 100_000 },
  });

  const results = new Map<KnowledgeCase, { failed: number; notes: string[] }>(cases.map((item) => [item, { failed: 0, notes: [] }]));
  const work = cases.flatMap((item) => Array.from({ length: runs }, () => item));
  const worker = async () => {
    for (let testCase = work.shift(); testCase; testCase = work.shift()) {
      const result = results.get(testCase)!;
      try {
        let conversationId: string | undefined;
        for (const message of testCase.after ?? []) conversationId = (await service.chat({ conversationId, message })).conversationId;
        const reply = await service.chat({ conversationId, message: testCase.ask });
        const found = problems(testCase, reply.text, reply.blocks);
        if (found.length) {
          result.failed += 1;
          result.notes.push(`${found.join("; ")}\n      said: ${reply.text.replace(/\s+/g, " ").slice(0, 400)}`);
        }
      } catch (error) {
        result.failed += 1;
        result.notes.push(`error: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  };
  console.log(`Model: ${config.model}. ${cases.length} questions, ${runs} time(s) each.\n`);
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));

  const areas = new Map<string, { passed: number; total: number }>();
  for (const [testCase, result] of results) {
    const tally = areas.get(testCase.area) ?? { passed: 0, total: 0 };
    tally.total += runs;
    tally.passed += runs - result.failed;
    areas.set(testCase.area, tally);
    if (result.failed) {
      const asked = [...(testCase.after ?? []), testCase.ask].join(" -> ");
      console.log(`FAIL ${result.failed}/${runs} [${testCase.area}] "${asked}"\n      ${result.notes.join("\n      ")}\n`);
    }
  }
  for (const [name, tally] of areas) console.log(`${String(tally.passed).padStart(4)}/${tally.total}  ${name}`);
  const failures = [...results.values()].reduce((sum, result) => sum + result.failed, 0);
  const total = cases.length * runs;
  console.log(`\n${total - failures}/${total} answers were right.`);
  const cost = ((spent.input - spent.cached) * RATES.input + spent.cached * RATES.cached + spent.output * RATES.output) / 1_000_000;
  const asked = cases.reduce((sum, item) => sum + (1 + (item.after?.length ?? 0)) * runs, 0);
  console.log(`${asked} messages, ${spent.requests} model requests, ${spent.input} input tokens (${spent.cached} cached), ${spent.output} output tokens.`);
  console.log(`Tool calls: ${[...tools].map(([name, count]) => `${name} ${count}`).join(", ") || "none"}.`);
  console.log(`About $${cost.toFixed(3)} at list prices: $${(cost / asked).toFixed(5)} per message, ${Math.round(spent.input / asked)} input tokens per message.`);
  process.exit(failures ? 1 : 0);
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(2);
});
