/**
 * Deterministic checks on what the model wrote, run before a customer sees it.
 *
 * The prompt asks for the same things, but a prompt can be ignored. These checks
 * catch recommendation language, predictions, and figures with no source.
 */

const RECOMMENDATION_PATTERNS: RegExp[] = [
  /\b(strong|good|great|solid|excellent|perfect|ideal|best)\s+(fit|buy|investment|pick|choice|bet|entry)\b/i,
  /\byou\s+(should|must|ought to)\s+(buy|sell|hold|invest|add|reduce|allocate)\b/i,
  /\b(i|we|selun)\s+(would\s+|'d\s+)?recommends?\s+(buying|selling|holding|investing|adding|reducing|you)\b/i,
  /\b(worth|time to)\s+(buying|buy|selling|sell|investing|invest)\b/i,
  /\bsafe\s+(bet|investment)\b/i,
  /\bguaranteed?\b/i,
  /\b(will|is going to|going to)\s+(rise|go up|fall|drop|moon|crash|outperform)\b/i,
];

/** A summary reports what a service returned, so it shouldn't tell anyone what to do. */
const SUMMARY_ADVICE_PATTERNS: RegExp[] = [
  /\bshould\b/i,
  /\bsays to\b/i,
  /\bnot proceed\b/i,
  /\b(strong|good|great|solid|excellent|safe)\s+(asset|option|holding|protocol)\b/i,
];

/** A sentence refusing to do these things may name them: "I can't say if it will go up." */
const NEGATION = /\b(can't|cannot|can not|won't|not|never|don't|doesn't|isn't|aren't|no|unable)\b/i;

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

/** For an ordinary reply: dollar figures must come from a tool or from the customer. */
export function replyIssues(text: string, sourceText: string): string[] {
  const allowed = numbersIn(sourceText);
  const unsupported = [...text.matchAll(DOLLAR_AMOUNT)]
    .map((match) => normalizeNumber(match[1] ?? match[2]))
    .filter((value) => !allowed.has(value));
  return [
    ...recommendationIssues(text),
    ...unsupported.map((value) => `dollar amount with no source: ${value}`),
  ];
}
