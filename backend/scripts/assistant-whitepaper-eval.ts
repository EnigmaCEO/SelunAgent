/**
 * Asks the configured model how the Sagitta Protocol works and judges each real answer on four things:
 * facts, whether it read the right page, whether it showed the customer that page, and unsupported claims.
 *
 *   npm run assistant:whitepaper-eval -- --runs=3 --out=baseline.json
 *   npm run assistant:whitepaper-eval -- --rescore=baseline.json
 *
 * Uses the real model and costs its tokens, except --rescore, which re-judges saved answers for free.
 * Nothing is paid for or run: the gateway below refuses every call.
 */
import fs from "node:fs";
import path from "node:path";
import dotenv from "dotenv";
import { AssistantService, type Block } from "../src/assistant/assistant.service";
import { OpenAiClient, readOpenAiConfig, type LlmClient } from "../src/assistant/openai-client";
import { WHITEPAPER_CASES, type WhitepaperCase } from "../src/assistant/sagitta-knowledge-cases";
import { EXCLUDED_NAMES, type SiteKnowledge } from "../src/assistant/sagitta-sites";
import { getSiteKnowledge } from "../src/assistant/sagitta-sites.runtime";

dotenv.config({ quiet: true });
dotenv.config({ path: path.join("backend", ".env"), override: true, quiet: true });

const CONCURRENCY = 6;
/** gpt-5.4-mini list prices per million tokens, for the cost estimate only. */
const RATES = { input: 0.75, cached: 0.075, output: 4.5 };

type Answer = { ask: string; text: string; cards: string; cited: string[]; read: string[]; searches: string[] };
const JUDGES = ["facts", "source", "citation", "claims"] as const;
type Judge = (typeof JUDGES)[number];

function argument(name: string): string | undefined {
  return process.argv.find((item) => item.startsWith(`--${name}=`))?.slice(name.length + 3);
}

const samePage = (left: string, right: string) => left.replace(/\/$/, "") === right.replace(/\/$/, "");

function judge(testCase: WhitepaperCase, answer: Answer): Record<Judge, string[]> {
  const visible = `${answer.text}\n${answer.cards}`.replace(/\*\*/g, "");
  const shown = (urls: string[]) => testCase.pages.some((page) => urls.some((url) => samePage(url, page)));
  return {
    facts: [...(testCase.facts ?? []), ...(testCase.reply ?? [])].filter((fact) => !fact.test(visible)).map((fact) => `missing ${fact.source}`),
    source: shown([...answer.read, ...answer.cited]) ? [] : ["never read the right page"],
    // The first source is the one a reader will take as the authority.
    citation: shown(answer.cited.slice(0, 1)) ? [] : ["didn't lead with the right page"],
    claims: [
      ...(testCase.none ?? []).filter((banned) => banned.test(visible)).map((banned) => `said ${banned.source}`),
      ...(EXCLUDED_NAMES.test(visible) ? ["names something Selun doesn't discuss"] : []),
    ],
  };
}

function report(answers: Answer[]): number {
  const tallies = Object.fromEntries(JUDGES.map((name) => [name, 0])) as Record<Judge, number>;
  let clean = 0;
  for (const testCase of WHITEPAPER_CASES) {
    for (const answer of answers.filter((item) => item.ask === testCase.ask)) {
      const verdict = judge(testCase, answer);
      for (const name of JUDGES) if (!verdict[name].length) tallies[name] += 1;
      if (JUDGES.every((name) => !verdict[name].length)) {
        clean += 1;
        continue;
      }
      const notes = JUDGES.flatMap((name) => verdict[name].map((note) => `${name}: ${note}`));
      console.log(`FAIL [${testCase.area}] "${testCase.ask}"\n      ${notes.join("\n      ")}\n      cited: ${answer.cited.join(", ") || "nothing"}\n      said: ${answer.text.replace(/\s+/g, " ").slice(0, 300)}\n`);
    }
  }
  const total = answers.length;
  for (const name of JUDGES) console.log(`${String(tallies[name]).padStart(4)}/${total}  ${name}`);
  console.log(`\n${clean}/${total} answers passed all four.`);
  return total - clean;
}

async function main() {
  const rescore = argument("rescore");
  if (rescore) {
    process.exit(report((JSON.parse(fs.readFileSync(rescore, "utf8")) as { answers: Answer[] }).answers) ? 1 : 0);
  }
  const config = readOpenAiConfig();
  if (!config) throw new Error("Set OPENAI_API_KEY and OPENAI_MODEL in backend/.env first.");
  const runs = Math.max(1, Number.parseInt(argument("runs") ?? "1", 10) || 1);
  const refuse = async () => {
    throw new Error("The whitepaper eval never calls a paid route.");
  };
  const spent = { requests: 0, input: 0, cached: 0, output: 0 };
  const client = new OpenAiClient(config);
  const counted: LlmClient = {
    complete: async (request) => {
      const response = await client.complete(request);
      spent.requests += 1;
      spent.input += response.usage?.inputTokens ?? 0;
      spent.cached += response.usage?.cachedTokens ?? 0;
      spent.output += response.usage?.outputTokens ?? 0;
      return response;
    },
  };

  const ask = async (testCase: WhitepaperCase): Promise<Answer> => {
    const read = new Set<string>();
    const searches: string[] = [];
    // One view per conversation, so answers running side by side don't share what they read.
    const sites = new Proxy(getSiteKnowledge(), {
      get(target, property) {
        if (property === "search") {
          return (query: string, question?: string) => {
            searches.push(query);
            const matches = target.search(query, question);
            for (const match of matches) read.add(match.url);
            return matches;
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? value.bind(target) : value;
      },
    }) as SiteKnowledge;
    const service = new AssistantService({
      llm: counted,
      gateway: { requestChallenge: refuse, submitPaid: refuse },
      sites,
      paypal: { prices: { reportUsd: "4.99", subscriptionUsd: "9.99" }, createCheckout: refuse, createSubscription: refuse },
      loadCatalog: async () => ({ resources: [] }),
      limits: { dailyTurnLimit: 100_000 },
    });
    const reply = await service.chat({ message: testCase.ask });
    const blocks: Block[] = reply.blocks;
    const cited = blocks.flatMap((block) => (block.type === "sagitta_sources" ? block.sources.map((source) => source.url) : []));
    return { ask: testCase.ask, text: reply.text, cards: JSON.stringify(blocks.filter((block) => block.type !== "sagitta_sources")), cited, read: [...read], searches };
  };

  const answers: Answer[] = [];
  const work = WHITEPAPER_CASES.flatMap((item) => Array.from({ length: runs }, () => item));
  const worker = async () => {
    for (let testCase = work.shift(); testCase; testCase = work.shift()) {
      try {
        answers.push(await ask(testCase));
      } catch (error) {
        answers.push({ ask: testCase.ask, text: `error: ${error instanceof Error ? error.message : String(error)}`, cards: "", cited: [], read: [], searches: [] });
      }
    }
  };
  console.log(`Model: ${config.model}. ${WHITEPAPER_CASES.length} questions, ${runs} time(s) each.\n`);
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));

  const out = argument("out");
  if (out) fs.writeFileSync(out, JSON.stringify({ model: config.model, answers }, null, 2));
  const failures = report(answers);
  const cost = ((spent.input - spent.cached) * RATES.input + spent.cached * RATES.cached + spent.output * RATES.output) / 1_000_000;
  console.log(`${answers.length} messages, ${spent.requests} model requests, ${answers.reduce((sum, answer) => sum + answer.searches.length, 0)} searches, ${spent.input} input tokens (${spent.cached} cached), ${spent.output} output tokens.`);
  console.log(`About $${cost.toFixed(3)} at list prices: $${(cost / Math.max(answers.length, 1)).toFixed(5)} per message.`);
  process.exit(failures ? 1 : 0);
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(2);
});
