/**
 * Deterministic checks on what the model wrote, run before a customer sees it.
 *
 * The prompt asks for the same things, but a prompt can be ignored. These checks
 * catch recommendation language, predictions, and figures with no source.
 */

import { findAssetMentions } from "./asset-resolver";
import { EXCLUDED_NAMES } from "./sagitta-sites";

const RECOMMENDATION_PATTERNS: RegExp[] = [
  /\b(strong|good|great|solid|excellent|perfect|ideal|best)\s+(fit|buy|investment|pick|choice|bet|entry)\b/i,
  /\byou\s+(should|must|ought to)\s+(buy|sell|hold|invest|add|reduce|allocate)\b/i,
  /\b(i|we|selun)\s+(would\s+|'d\s+)?recommends?\s+(buying|selling|holding|investing|adding|reducing|you)\b/i,
  /\b(worth|time to)\s+(buying|buy|selling|sell|investing|invest)\b/i,
  /\bsafe\s+(bet|investment)\b/i,
  /\bguaranteed?\b/i,
  /\b(will|is going to|going to)\s+(rise|go up|fall|drop|moon|crash|outperform)\b/i,
];

/**
 * The Vault keeps deposits, and the Treasury allocates its own Stability Units in a matching amount.
 * The whitepaper's shorthand ("converts deposits") reads otherwise, so the model repeats it despite being told not to.
 */
export const DEPOSITS_MOVE = /deposits? (are|is|get) (automatically |directly )?(transferred|moved|sent|routed)\b|\b(takes|uses|converts?|routes?|turns?|turning)\s+(the\s+|those\s+|eligible\s+|vault\s+|depositor\s+)*(deposits|capital into)\b|\bconverts? them\b/i;
/** Not naming a regulator is all the sources say. Having none is a legal conclusion they don't reach. */
export const REGULATOR_INFERENCE = /not by a (\w+ )?regulator|\bno regulator\b(?! (is )?named)|\b(not|isn['’]t) regulated\b|\bunregulated\b/i;

/** A summary reports what a service returned, so it shouldn't tell anyone what to do. */
const SUMMARY_ADVICE_PATTERNS: RegExp[] = [
  /\bshould\b/i,
  /\bsays to\b/i,
  /\bnot proceed\b/i,
  /\b(strong|good|great|solid|excellent|safe)\s+(asset|option|holding|protocol)\b/i,
];

/** A sentence refusing to do these things may name them: "I can't say if it will go up." */
const NEGATION = /\b(can't|cannot|can not|won't|not|never|don't|doesn't|isn't|aren't|no|unable)\b/i;

/** "If a portfolio worth $1,000 falls to $600..." explains a term. It isn't a figure about anything real. */
const ILLUSTRATION = /\b(for example|for instance|e\.g\.|as an example|imagine|suppose|say you|let'?s say|hypothetical(ly)?|if (you|your|an?|it|the value|the price)\b)/i;
/** What an illustration can never state: Selun's prices, a live price, or a figure about Sagitta or a named asset. */
const REAL_FIGURE = /\b(costs?|priced?|prices|pricing|fees?|charged?|pay|paid|usdc|selun|sagitta|radar|treasury|vault|escrow|today|currently|right now|trading at|market cap)\b/i;

const DOLLAR_AMOUNT = /\$\s?(\d[\d,]*(?:\.\d+)?)|(\d[\d,]*(?:\.\d+)?)\s?(?:usd|usdc|dollars)\b/gi;

function normalizeNumber(raw: string): string {
  const plain = raw.replaceAll(",", "");
  return plain.includes(".") ? plain.replace(/\.?0+$/, "") : plain;
}

export function numbersIn(text: string): Set<string> {
  return new Set([...text.matchAll(/\d[\d,]*(?:\.\d+)?/g)].map((match) => normalizeNumber(match[0])));
}

function recommendationIssues(text: string): string[] {
  const normalized = text.replaceAll("’", "'");
  const issues: string[] = [];
  for (const sentence of normalized.split(/(?<=[.!?\n])\s+/)) {
    if (NEGATION.test(sentence)) continue;
    for (const pattern of RECOMMENDATION_PATTERNS) {
      const match = pattern.exec(sentence);
      if (match) issues.push(`recommendation or prediction: "${match[0]}"`);
    }
  }
  return issues;
}

/** For a summary of a paid result: every number must come from that result. */
export function explanationIssues(text: string, sourceText: string): string[] {
  const allowed = numbersIn(sourceText);
  const unsupported = [...numbersIn(text)].filter((value) => !allowed.has(value));
  return [
    ...recommendationIssues(text),
    ...SUMMARY_ADVICE_PATTERNS.flatMap((pattern) => {
      const match = pattern.exec(text);
      return match ? [`advice wording: "${match[0]}"`] : [];
    }),
    ...unsupported.map((value) => `number with no source in the result: ${value}`),
  ];
}

function protocolClaimIssues(text: string): string[] {
  const issues: string[] = [];
  if (/\b(sagitta|protocol)\b/i.test(text) && REGULATOR_INFERENCE.test(text)) {
    issues.push("states a conclusion about regulation. Say only that the whitepaper and sites don't name a regulator");
  }
  const movesDeposits = /\btreasury\b/i.test(text) && text.split(/(?<=[.!?\n])\s+/).some((sentence) => DEPOSITS_MOVE.test(sentence) && !NEGATION.test(sentence.replaceAll("’", "'")));
  if (movesDeposits) {
    issues.push("says the Treasury takes or moves deposits. The Vault keeps the deposits, and the Treasury allocates its own Stability Units in a matching amount, then routes that capital through the Escrow. Describe it that way");
  }
  return issues;
}

/**
 * The text a dollar figure needs a source for. A worked example that a paragraph introduces as one is left out,
 * one sentence at a time, unless that sentence touches a price, Sagitta, or a named asset.
 */
function statedText(text: string): string {
  const kept: string[] = [];
  for (const paragraph of text.split(/\n\s*\n/)) {
    let example = false;
    for (const sentence of paragraph.split(/(?<=[.!?\n])\s+/)) {
      example ||= ILLUSTRATION.test(sentence);
      const real = REAL_FIGURE.test(sentence) || findAssetMentions(sentence).length > 0;
      if (!example || real) kept.push(sentence);
    }
  }
  return kept.join(" ");
}

/**
 * For an ordinary reply: dollar figures must come from a tool or from the customer.
 * `apiPrices` are what agents pay for per-use services through the API, which the websites list. No tool gave them
 * in this conversation, and the chat charges more, so they're never a chat price.
 */
export function replyIssues(text: string, sourceText: string, apiPrices: readonly string[] = []): string[] {
  const excluded = EXCLUDED_NAMES.exec(text);
  if (excluded) return [`mentions a name Selun doesn't discuss: "${excluded[0]}". Don't name it. Say you can only help with Sagitta Systems`];
  const allowed = numbersIn(sourceText);
  const apiOnly = numbersIn(apiPrices.join(" "));
  const stated = [...statedText(text).matchAll(DOLLAR_AMOUNT)].map((match) => normalizeNumber(match[1] ?? match[2]));
  return [
    ...recommendationIssues(text),
    ...protocolClaimIssues(text),
    ...stated.filter((value) => !allowed.has(value)).map((value) => `dollar amount with no source: ${value}`),
    ...stated.filter((value) => allowed.has(value) && apiOnly.has(value)).map((value) => `${value} is an API price from the websites. Bought in this chat, Selun's per-use services cost more, so state only the price a tool gave you`),
  ];
}
