import { CAPITAL_FLOW_PAGE, CAPITAL_FLOW_PASSAGES } from "./sagitta-capital-flow";
import fs from "node:fs";
import path from "node:path";
import { buildFactSheet, type FactSheet } from "./sagitta-facts";

/**
 * What Sagitta's own websites say, as the source of truth for questions about Sagitta.
 *
 * Only the hosts below are ever read, over HTTPS, and a redirect off them is dropped, so
 * nothing from elsewhere on the web can enter. Account, payment, and API paths are skipped,
 * and each site's robots.txt is respected. Page text is treated as data: the model is told
 * never to follow instructions found in it, and it can't trigger payments or services anyway.
 */

export const SAGITTA_HOSTS = [
  "www.sagitta.systems",
  "aaa.sagitta.systems",
  "sce.sagitta.systems",
  "selun.sagitta.systems",
  "defense.sagitta.systems",
  "radar.sagitta.systems",
  "protocol.sagitta.systems",
  "banking.sagitta.systems",
  "wallet.sagitta.systems",
  "sagitta-protocol.gitbook.io",
] as const;

const SKIPPED_PATH = /^\/(api|app|account|admin|auth|billing|checkout|dashboard|login|logout|signin|signup|register|portal|settings|wizard|plan|_next|static)(\/|$)/i;
const SKIPPED_FILE = /\.(css|js|mjs|map|png|jpe?g|gif|svg|ico|webp|avif|woff2?|ttf|mp4|webm|xml|txt|json|pdf|zip|jfif)$/i;
/**
 * Pages whose contents change by the hour, such as Radar's public alert feed. A saved copy would be read back
 * as if it were current, so they are never kept. Selun gives their address instead.
 */
const LIVE_PAGE = /^https:\/\/radar\.sagitta\.systems\/alerts(\/|$)/i;
const MAX_PAGES_PER_HOST = 120;
const MAX_PAGE_BYTES = 2_000_000;
const CHUNK_CHARS = 900;
/** A block on this share of a site's pages is navigation or a footer. */
const BOILERPLATE_SHARE = 0.4;

/** Selun doesn't mention these, even where a page does. */
export const EXCLUDED_NAMES = /sagitta\s*labs|sagittalabs|alexander\s+(demetrius\s+)?roth|orion\s+gray/i;

export type SiteChunk = { url: string; site: string; title: string; heading: string; text: string };
export type SiteCorpus = { fetchedAt: string; pages: number; chunks: SiteChunk[]; hosts: Record<string, number> };
export type Fetcher = (url: string) => Promise<{ status: number; url: string; contentType: string; text: string }>;

const defaultFetcher: Fetcher = async (url) => {
  const response = await fetch(url, { headers: { "User-Agent": "selun-agent", Accept: "text/html,application/xml,text/plain" }, redirect: "follow", signal: AbortSignal.timeout(20_000) });
  const text = await response.text();
  return { status: response.status, url: response.url, contentType: response.headers.get("content-type") ?? "", text: text.slice(0, MAX_PAGE_BYTES) };
};

const ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " ", mdash: "-", ndash: "-", hellip: "...", rsquo: "'", lsquo: "'", ldquo: '"', rdquo: '"', middot: "·", copy: "(c)" };

function decode(value: string): string {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (_, hex: string) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, code: string) => String.fromCodePoint(Number(code)))
    .replace(/&([a-z]+);/gi, (match, name: string) => ENTITIES[name.toLowerCase()] ?? match);
}

function clean(value: string): string {
  return decode(value.replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").replace(/\s([.,;:!?])/g, "$1").trim();
}

export type PageBlock = { heading: boolean; text: string };

const HEADING_MARK = "\u0001";

/** Readable blocks in page order. Scripts, styles, and page chrome are removed first. */
export function pageBlocks(html: string): { title: string; blocks: PageBlock[] } {
  const title = clean(/<title[^>]*>([\s\S]*?)<\/title>/i.exec(html)?.[1] ?? "");
  const body = (/<body[\s\S]*<\/body>/i.exec(html)?.[0] ?? html)
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<(script|style|noscript|svg|template|iframe|head|nav|footer)\b[\s\S]*?<\/\1>/gi, " ");
  // Every visible piece of text counts: prices and plan names often sit in plain divs and spans.
  // Block-level tags become line breaks, so what's visually separate stays separate.
  const marked = body
    .replace(/<h[1-6]\b[^>]*>([\s\S]*?)<\/h[1-6]>/gi, (_, inner: string) => `\n${HEADING_MARK}${inner.replace(/<[^>]+>/g, " ")}\n`)
    .replace(/<\/?(div|p|li|ul|ol|tr|td|th|dt|dd|dl|table|thead|tbody|section|article|main|aside|blockquote|figcaption|figure|summary|details|header|form|label|button|br|hr|pre)\b[^>]*>/gi, "\n");
  const blocks: PageBlock[] = [];
  for (const line of marked.split("\n")) {
    const heading = line.includes(HEADING_MARK);
    const text = clean(line.replace(HEADING_MARK, ""));
    if (/[a-z0-9]/i.test(text)) blocks.push({ heading, text });
  }
  return { title, blocks };
}

/**
 * Accounts Sagitta names as its own. These sit in footers, icons, and page metadata, so the page
 * text never shows them. Only profile addresses match: a link to one post isn't an account.
 */
const OFFICIAL_ACCOUNTS: [label: string, pattern: RegExp][] = [
  ["X (Twitter)", /https:\/\/(?:www\.)?(?:x|twitter)\.com\/\w*sagitta\w*(?![\w/])/gi],
  ["Discord", /https:\/\/discord\.gg\/\w+/gi],
  ["Telegram", /https:\/\/t\.me\/\+?[\w-]+/gi],
  ["GitHub", /https:\/\/github\.com\/EnigmaCEO\/[\w.-]+/gi],
  ["Paragraph publication", /https:\/\/paragraph\.com\/@\w+(?![\w/-])/gi],
];
/** Confirmed by Sagitta's owner, for accounts the sites don't link to yet. Listed under that site. */
const OWNER_CONFIRMED_LINKS: Record<string, string[]> = {
  "www.sagitta.systems": ["YouTube: https://www.youtube.com/@sagittasystems"],
};
const OFFICIAL_EMAIL = /mailto:([\w.+-]+@sagitta\.systems)/gi;
export const OFFICIAL_HEADING = "Official links, social media accounts, and contact email";

/** One readable line for each account and address found, or nothing if a page has none. */
export function officialLinks(html: string): string[] {
  const found = new Set<string>();
  for (const [label, pattern] of OFFICIAL_ACCOUNTS) {
    for (const match of html.matchAll(pattern)) found.add(`${label}: ${match[0]}`);
  }
  for (const match of html.matchAll(OFFICIAL_EMAIL)) found.add(`Email: ${match[1].toLowerCase()}`);
  return [...found].filter((line) => !EXCLUDED_NAMES.test(line));
}

function parseDisallowed(robots: string): string[] {
  const rules: string[] = [];
  let applies = false;
  for (const line of robots.split(/\r?\n/)) {
    const [field, ...rest] = line.split(":");
    const value = rest.join(":").trim();
    if (/^user-agent$/i.test(field.trim())) applies = value === "*";
    else if (applies && /^disallow$/i.test(field.trim()) && value) rules.push(value);
  }
  return rules;
}

function allowedUrl(raw: string, host: string, disallowed: string[]): string | null {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return null;
  }
  if (url.protocol !== "https:" || url.hostname !== host) return null;
  if (SKIPPED_PATH.test(url.pathname) || SKIPPED_FILE.test(url.pathname)) return null;
  if (disallowed.some((rule) => url.pathname.startsWith(rule))) return null;
  const address = `https://${host}${url.pathname.replace(/\/+$/, "") || "/"}`;
  return LIVE_PAGE.test(address) ? null : address;
}

function chunkPage(url: string, site: string, title: string, blocks: PageBlock[]): SiteChunk[] {
  const chunks: SiteChunk[] = [];
  let heading = "";
  let parts: string[] = [];
  let size = 0;
  const flush = () => {
    if (parts.length) chunks.push({ url, site, title, heading, text: parts.join(" ") });
    parts = [];
    size = 0;
  };
  for (const block of blocks) {
    if (block.heading) {
      flush();
      heading = block.text;
      continue;
    }
    if (size + block.text.length > CHUNK_CHARS) flush();
    parts.push(block.text);
    size += block.text.length;
  }
  flush();
  return chunks;
}

/** Reads one site through its sitemap and internal links. A page that fails is simply skipped. */
async function crawlHost(host: string, fetcher: Fetcher) {
  const base = `https://${host}`;
  const disallowed = await fetcher(`${base}/robots.txt`).then((res) => (res.status === 200 ? parseDisallowed(res.text) : [])).catch(() => []);
  const queue: string[] = [`${base}/`];
  const sitemap = await fetcher(`${base}/sitemap.xml`).then((res) => (res.status === 200 ? res.text : "")).catch(() => "");
  queue.push(...[...sitemap.matchAll(/<loc>\s*([^<\s]+)/g)].map((match) => match[1]));

  const seen = new Set<string>();
  const pages: { url: string; title: string; blocks: PageBlock[] }[] = [];
  const links = new Set<string>();
  let linksPage = "";
  while (queue.length && pages.length < MAX_PAGES_PER_HOST) {
    const next = allowedUrl(queue.shift()!, host, disallowed);
    if (!next || seen.has(next)) continue;
    seen.add(next);
    const response = await fetcher(next).catch(() => null);
    if (!response || response.status !== 200 || !response.contentType.includes("html")) continue;
    // A redirect off Sagitta's own host is dropped, not followed into the corpus.
    if (new URL(response.url).hostname !== host) continue;
    const { title, blocks } = pageBlocks(response.text);
    if (blocks.length) pages.push({ url: next, title, blocks });
    const pageLinks = officialLinks(response.text);
    if (pageLinks.length && !linksPage) linksPage = next;
    for (const line of pageLinks) links.add(line);
    for (const match of response.text.matchAll(/href="([^"#?]+)/g)) {
      const link = match[1].startsWith("/") ? `${base}${match[1]}` : match[1];
      if (link.startsWith(base)) queue.push(link);
    }
  }
  return { pages, links: [...links], linksPage };
}

/** Text repeated across much of a site is navigation or a footer, and would crowd out real answers. */
function withoutBoilerplate<T extends { blocks: PageBlock[] }>(pages: T[]): T[] {
  if (pages.length < 5) return pages;
  const counts = new Map<string, number>();
  for (const page of pages) for (const text of new Set(page.blocks.map((block) => block.text))) counts.set(text, (counts.get(text) ?? 0) + 1);
  const limit = pages.length * BOILERPLATE_SHARE;
  return pages.map((page) => ({ ...page, blocks: page.blocks.filter((block) => (counts.get(block.text) ?? 0) <= limit) }));
}

export async function crawlSagittaSites(options: { fetcher?: Fetcher; now?: () => Date; hosts?: readonly string[] } = {}): Promise<SiteCorpus> {
  const fetcher = options.fetcher ?? defaultFetcher;
  const chunks: SiteChunk[] = [];
  const hosts: Record<string, number> = {};
  let pageCount = 0;
  for (const host of options.hosts ?? SAGITTA_HOSTS) {
    const crawled = await crawlHost(host, fetcher).catch(() => ({ pages: [], links: [] as string[], linksPage: "" }));
    const pages = withoutBoilerplate(crawled.pages);
    hosts[host] = pages.length;
    pageCount += pages.length;
    for (const page of pages) {
      chunks.push(...chunkPage(page.url, host, page.title, page.blocks.filter((block) => !EXCLUDED_NAMES.test(block.text))));
    }
    const links = [...(OWNER_CONFIRMED_LINKS[host] ?? []), ...crawled.links];
    if (links.length) {
      chunks.push({ url: crawled.linksPage || `https://${host}/`, site: host, title: `${OFFICIAL_HEADING} on ${host}`, heading: OFFICIAL_HEADING, text: `${links.join(". ")}.` });
    }
  }
  return { fetchedAt: (options.now ?? (() => new Date()))().toISOString(), pages: pageCount, chunks, hosts };
}

// ---- Search ----

const STOP_WORDS = new Set("a an and are as at be but by can do does for from has have how i in is it its me my of on or our that the their them this to was we what when where which who why will with you your".split(" "));

/** Similar words people use for the same thing on these sites. */
const SYNONYMS: Record<string, string[]> = {
  backs: ["composition", "composed", "anchored", "gold"],
  backed: ["composition", "composed", "anchored", "gold"],
  backing: ["composition", "composed", "anchored", "gold"],
  developer: ["founder", "built", "building", "development", "leadership"],
  developers: ["founder", "built", "building", "development", "leadership"],
  creator: ["founder", "built", "leadership"],
  founder: ["founded", "leadership"],
  founded: ["founder", "leadership"],
  built: ["founder", "builds", "leadership"],
  builds: ["founder", "built", "leadership"],
  owner: ["founder", "leadership"],
  ceo: ["founder", "leadership"],
  team: ["founder", "leadership", "people", "roles"],
  price: ["pricing", "fee", "plan", "plans", "cost"],
  cost: ["pricing", "fee", "plan", "plans", "price"],
  jobs: ["careers", "roles", "hiring"],
  roadmap: ["next", "planned", "status"],
};

function stem(word: string): string {
  if (word.length > 4 && word.endsWith("ies")) return `${word.slice(0, -3)}y`;
  if (word.length > 3 && word.endsWith("s") && !word.endsWith("ss")) return word.slice(0, -1);
  return word;
}

function tokens(text: string): string[] {
  return (text.toLowerCase().match(/[a-z0-9$][a-z0-9.$-]*/g) ?? [])
    .map((word) => word.replace(/[.-]+$/, ""))
    .filter((word) => word && !STOP_WORDS.has(word))
    .map(stem);
}

export type SiteMatch = SiteChunk & { score: number };

function matchKey(chunk: SiteChunk): string {
  return `${chunk.url}\u0000${chunk.heading}\u0000${chunk.text}`;
}

const PRICE_QUESTION = /\b(price|prices|pricing|cost|costs|fee|fees|how much|plans?)\b/i;
const PRICE_BOOST = 1.6;

/** Also applied to anything read from disk, so a copy from before an exclusion can't bring it back. Live pages go too. */
function withoutExcluded(corpus: SiteCorpus): SiteCorpus {
  const hosts = Object.fromEntries(Object.entries(corpus.hosts).filter(([host]) => !EXCLUDED_NAMES.test(host)));
  const chunks = corpus.chunks.filter((chunk) => !LIVE_PAGE.test(chunk.url) && !EXCLUDED_NAMES.test(`${chunk.url} ${chunk.title} ${chunk.heading} ${chunk.text}`));
  // Dropped live pages leave the page count too, so a refresh without them doesn't look like a site shrinking.
  const live = new Set(corpus.chunks.filter((chunk) => LIVE_PAGE.test(chunk.url)).map((chunk) => chunk.url));
  for (const url of live) {
    const host = new URL(url).hostname;
    if (hosts[host]) hosts[host] = Math.max(hosts[host] - 1, 0);
  }
  return { ...corpus, hosts, chunks };
}

export type SiteKnowledgeOptions = {
  bundled: SiteCorpus;
  /** Where the latest read of the sites is kept across restarts. Null keeps it in memory. */
  file?: string | null;
  crawl?: () => Promise<SiteCorpus>;
  now?: () => Date;
  maxAgeMs?: number;
};

/**
 * Serves the latest good read of the sites. A refresh merges by host: a site that's down keeps
 * its last good pages, and a read that lost most of the pages is refused rather than served.
 */
export class SiteKnowledge {
  private corpus: SiteCorpus;
  private cachedIndex: SiteIndex | null = null;
  private factSheet: FactSheet | null = null;
  private refreshing: Promise<void> | null = null;
  private readonly now: () => Date;
  private readonly maxAgeMs: number;

  constructor(private readonly options: SiteKnowledgeOptions) {
    this.now = options.now ?? (() => new Date());
    this.maxAgeMs = options.maxAgeMs ?? 24 * 60 * 60_000;
    this.corpus = withoutExcluded(options.bundled);
    const stored = this.readStored();
    if (stored && Date.parse(stored.fetchedAt) > Date.parse(options.bundled.fetchedAt)) this.corpus = withoutExcluded(stored);
    this.factSheet = buildFactSheet(this.corpus) ?? buildFactSheet(withoutExcluded(options.bundled));
  }

  private readStored(): SiteCorpus | null {
    if (!this.options.file) return null;
    try {
      const parsed = JSON.parse(fs.readFileSync(this.options.file, "utf8")) as SiteCorpus;
      return Array.isArray(parsed.chunks) && typeof parsed.fetchedAt === "string" ? parsed : null;
    } catch {
      return null;
    }
  }

  get fetchedAt(): string {
    return this.corpus.fetchedAt;
  }

  /** The last sheet that could be read in full. */
  get facts(): FactSheet | null {
    return this.factSheet;
  }

  /** `question` is what the customer actually asked, since the query is the model's rewording. */
  search(query: string, question = ""): SiteMatch[] {
    this.cachedIndex ??= new SiteIndex(this.corpus);
    return this.cachedIndex.search(query, undefined, undefined, question);
  }

  get isStale(): boolean {
    return this.now().getTime() - Date.parse(this.corpus.fetchedAt) >= this.maxAgeMs;
  }

  merge(next: SiteCorpus): SiteCorpus | null {
    const hosts: Record<string, number> = {};
    const chunks: SiteChunk[] = [];
    for (const host of new Set([...Object.keys(this.corpus.hosts), ...Object.keys(next.hosts)])) {
      const fresh = next.hosts[host] ?? 0;
      const previous = this.corpus.hosts[host] ?? 0;
      const useFresh = fresh > 0 && fresh >= previous * 0.5;
      hosts[host] = useFresh ? fresh : previous;
      chunks.push(...(useFresh ? next : this.corpus).chunks.filter((chunk) => chunk.site === host));
    }
    const pages = Object.values(hosts).reduce((sum, count) => sum + count, 0);
    return chunks.length ? { fetchedAt: next.fetchedAt, pages, chunks, hosts } : null;
  }

  async refresh(): Promise<void> {
    if (this.refreshing) return this.refreshing;
    this.refreshing = (async () => {
      const merged = this.merge(withoutExcluded(await (this.options.crawl ?? crawlSagittaSites)()));
      if (!merged) return;
      this.corpus = merged;
      this.cachedIndex = null;
      const sheet = buildFactSheet(merged);
      if (sheet) this.factSheet = sheet;
      else console.warn("Sagitta facts weren't updated: a price, status, or link couldn't be read.");
      if (this.options.file) {
        fs.mkdirSync(path.dirname(this.options.file), { recursive: true });
        const temp = `${this.options.file}.tmp`;
        fs.writeFileSync(temp, JSON.stringify(merged), "utf8");
        fs.renameSync(temp, this.options.file);
      }
      console.log(`[sagitta-sites] read ${merged.pages} pages from ${Object.keys(merged.hosts).length} sites`);
    })()
      .catch((error) => console.warn("Sagitta sites refresh failed:", error instanceof Error ? error.message : error))
      .finally(() => {
        this.refreshing = null;
      });
    return this.refreshing;
  }
}

const WWW = "https://www.sagitta.systems";
const MAX_PASSAGES = 24;
const PINNED_PASSAGES = 6;
const PINNED_PRICE_PASSAGES = 14;
const TITLE_MATCH_TERMS = 3;
/** Thin or legal pages that share a product's name but rarely answer a question about it. */
const LOW_VALUE_PAGE = /\/(privacy|terms|alerts\/RADAR-)/i;
const LOW_VALUE_WANTED = /\b(privacy|terms|legal|alerts?|incidents?)\b/i;
const LOW_VALUE_WEIGHT = 0.4;

/** Each system's own record, its product page, and where its prices are published. */
const SYSTEM_PAGES: { name: RegExp; pages: string[]; pricing?: string }[] = [
  { name: /\bradar\b/i, pages: [`${WWW}/systems/sagitta-radar`], pricing: "https://radar.sagitta.systems/pricing" },
  // SCE's priced endpoints are sold through Selun.
  { name: /\b(sce|continuity engine)\b/i, pages: [`${WWW}/systems/sagitta-continuity-engine`, "https://sce.sagitta.systems/"], pricing: "https://selun.sagitta.systems/pricing" },
  { name: /\bdefen[cs]e\b/i, pages: [`${WWW}/systems/sagitta-defense`, "https://defense.sagitta.systems/"], pricing: "https://defense.sagitta.systems/" },
  { name: /\b(aaa|allocation agent)\b/i, pages: [`${WWW}/systems/aaa`, "https://aaa.sagitta.systems/what-is-aaa"], pricing: "https://aaa.sagitta.systems/pricing" },
  { name: /\bselun\b/i, pages: [`${WWW}/systems/selun`, "https://selun.sagitta.systems/what-is-selun"], pricing: "https://selun.sagitta.systems/pricing" },
  { name: /\bprotocol\b/i, pages: [`${WWW}/systems/sagitta-protocol`] },
  { name: /\bbanking\b/i, pages: [`${WWW}/systems/sagitta-banking`, "https://banking.sagitta.systems/"] },
  { name: /\bwallet\b/i, pages: [`${WWW}/systems/sagitta-wallet`] },
];

/** The one page that answers a kind of question, whatever words happen to be on it. */
const TOPIC_PAGES: { about: RegExp; pages: string[]; only?: RegExp }[] = [
  { about: /\b(company|incorporated|families|family|foundations?|organi[sz]ing|thesis|founder|founded|developer|who (is|are|runs|built|builds|makes)|what (is|does) sagitta)\b/i, pages: [`${WWW}/about`] },
  // The founder's profile sits low on a long page, and "who builds it" means them too.
  { about: /\b(founder|founded|founders|developer|developers|creator|ceo|owner|leadership|who (is|are|runs|built|builds|makes|created))\b/i, pages: [`${WWW}/about`], only: /Xavier/ },
  { about: /\bradar\b.*\blaunch|\blaunch.*\bradar\b/i, pages: [`${WWW}/newsroom/sagitta-radar-launched`] },
  { about: /\bmoonbase\b/i, pages: [`${WWW}/newsroom/sagitta-protocol-launched-on-moonbase-alpha-testnet`] },
  { about: /\barc\b/i, pages: [`${WWW}/newsroom/sagitta-protocol-launched-on-arc-testnet`] },
  { about: /\b(products|systems|everything sagitta)\b/i, pages: [`${WWW}/systems`] },
  { about: /\b(live|status|statuses|available|availability|mainnet|testnets?|launched|today|waitlist|pilot|real (funds|money)|operating|uptime|public test|in development|research horizon|covers?|coverage)\b/i, pages: [`${WWW}/status`] },
  { about: /\b(roadmap|how long|delivery|delivered|duration|timeline|turnaround|endpoints?)\b/i, pages: [`${WWW}/roadmap`] },
  { about: /\b(hiring|jobs?|careers?|roles?|openings?|work (for|at))\b/i, pages: [`${WWW}/careers`] },
];

export const WHITEPAPER_URL = "https://sagitta-protocol.gitbook.io";
const WP = `${WHITEPAPER_URL}/sagitta-whitepaper`;
const WHITEPAPER_PASSAGES = 6;
const GOVERNING_LEAD = 2;
const WORD_ROOT_LENGTH = 5;
/** A question about another product's own treasury or terms isn't about the Protocol's. */
const OTHER_SUBJECT = /\b(radar|selun|aaa|defense review|treasury operators?|(my|our|your|dao|dao['’]s)\s+(\w+\s+)?(treasury|vault|reserve))\b/i;
const ABOUT_THE_PROTOCOL = /\bsagitta\b|\bthe protocol\b|\bwhitepaper\b|\bdepositors?\b|\bmy (deposits?|money|funds|principal|capital)\b/i;
const PROTOCOL_PART = (part: string) => new RegExp(`\\b(the|sagitta|protocol['’]?s?)\\s+${part}\\b`, "i");

/**
 * Questions the whitepaper governs, and the pages that answer them. Word matching alone lets marketing
 * pages outrank it. Each topic has to be about the Protocol, so "treasury" in another sense finds its own pages.
 */
const WHITEPAPER_TOPICS: { about: RegExp; pages: string[]; needsContext?: boolean; whole?: boolean }[] = [
  { about: /\bbanks?\b|banking institution|\bfdic\b|\binsur(ed|ance|er)\b|\bregulat\w*|\blicen[sc]\w*|\bcustodial\b|\bcustodian\b|trust company|deposit[- ]taking|\bguarantee\w*|\blegal(ly)?\b/i, pages: [`${WP}/scope-and-non-banking-clarification`], needsContext: true, whole: true },
  { about: /\bprincipal\b.*\b(protect|preserv|safe|lose|loss)|\b(protect|preserv|lose|loss)\w*\b.*\bprincipal\b/i, pages: [`${WP}/scope-and-non-banking-clarification`, `${WP}/system-invariants`], needsContext: true },
  { about: new RegExp(`${PROTOCOL_PART("treasury").source}|treasury tokens?|stability units?|reserve ratio|allocation batch`, "i"), pages: [`${WP}/treasury`] },
  { about: new RegExp(`${PROTOCOL_PART("vault").source}|deposit receipts?|commitment period`, "i"), pages: [`${WP}/vault`] },
  { about: PROTOCOL_PART("escrow"), pages: [`${WP}/escrow`] },
  { about: new RegExp(`${PROTOCOL_PART("reserve").source}|\\bxaut\\b|tokenized gold|reserve[- ]relative|reserve assets`, "i"), pages: [`${WP}/reserve`] },
  { about: /\binvariants?\b/i, pages: [`${WP}/system-invariants`], whole: true },
  { about: /\bevacuat\w*|\breconstitut\w*|continuity (states?|doctrine)|substitution doctrine/i, pages: [`${WP}/continuity-engine`] },
  { about: /\bprotocol\b.*\bcontinuity engine\b|\bcontinuity engine\b.*\bprotocol\b/i, pages: [`${WP}/continuity-engine`] },
  { about: /\bdepeg\w*|\boracles?\b.*\b(fail|down|break|outage)|threat matrix|failure matrix|failure modes?|chain halt|governance (capture|deadlock)|\bwhat happens (if|when)\b/i, pages: [`${WP}/failure-threat-matrix`, `${WP}/continuity-engine`], needsContext: true },
  { about: /capital flow|\bhow (does |do )?(capital|money|deposits?|funds) (flows?|moves?)\b|\bdo (vault )?deposits (go|move)\b/i, pages: [CAPITAL_FLOW_PAGE], whole: true },
  { about: /\b(fiduciary|loss absorption|protocol[- ]level loss)\b/i, pages: [`${WP}/fiduciary-alignment`] },
  { about: /\bwhitepaper\b/i, pages: [WP, `${WP}/readme`] },
  { about: /\b(what is|what's|how does|how do|explain)\b.*\bsagitta protocol\b/i, pages: [`${WP}/readme`] },
];

/** The whitepaper topics a customer's question is about, if any. */
export function whitepaperTopics(question: string) {
  if (OTHER_SUBJECT.test(question) && !/\bprotocol\b/i.test(question)) return [];
  return WHITEPAPER_TOPICS.filter((topic) => topic.about.test(question) && (!topic.needsContext || ABOUT_THE_PROTOCOL.test(question)));
}

/** What kind of source a page is, so the model can tell a specification from a sales page. */
export function sourceType(url: string): "whitepaper" | "status" | "website" {
  if (url.startsWith(WHITEPAPER_URL)) return "whitepaper";
  return url === `${WWW}/status` ? "status" : "website";
}

/** Whether a message is about Sagitta or one of its systems by name. */
export function namesSagitta(text: string): boolean {
  return /\b(sagitta|selun|radar|sce|continuity engine|defense review|aaa|allocation agent)\b/i.test(text);
}

/** Plan names and features are answered by the pricing page, even when nobody says "price". */
const ASKS_ABOUT_PLANS = /\b(plans?|tiers?|intel|signal|watch|desk|observer|sandbox|production authority|doctrine authority|signed receipts|event history)\b/i;

const ASKS_FOR_LINKS = /\b(youtube|channel|videos?|twitter|tweet|x (account|page|profile|handle)|social|follow|contact|e-?mail|discord|telegram|github|blog|newsletter|reach|support|community|paragraph)\b/i;

/** Ranks passages by term relevance (BM25), weighting titles and headings. Same input, same order. */
export class SiteIndex {
  private readonly docs: { chunk: SiteChunk; terms: Map<string, number>; length: number }[];
  private readonly documentFrequency = new Map<string, number>();
  private readonly averageLength: number;

  constructor(readonly corpus: SiteCorpus) {
    // Added here so a refresh can't lose it, and only where its page is still being read.
    const approved = corpus.chunks.some((chunk) => chunk.url === CAPITAL_FLOW_PAGE) ? CAPITAL_FLOW_PASSAGES : [];
    this.docs = [...corpus.chunks, ...approved].map((chunk) => {
      const terms = new Map<string, number>();
      const all = tokens(chunk.text);
      for (const term of all) terms.set(term, (terms.get(term) ?? 0) + 1);
      // Where a passage lives says what it's about: a price card on /pricing may never say "price".
      const place = chunk.url.replace(/^https:\/\//, "").replace(/\.(sagitta\.systems|gitbook\.io)|^www\.|\.com/g, "").replace(/[/._-]+/g, " ");
      for (const term of tokens(`${chunk.title} ${chunk.heading} ${place}`)) terms.set(term, (terms.get(term) ?? 0) + 3);
      return { chunk, terms, length: Math.max(all.length, 1) };
    });
    for (const doc of this.docs) for (const term of doc.terms.keys()) this.documentFrequency.set(term, (this.documentFrequency.get(term) ?? 0) + 1);
    this.averageLength = this.docs.reduce((sum, doc) => sum + doc.length, 0) / Math.max(this.docs.length, 1);
  }

  search(query: string, limit = 8, perPage = 5, question = ""): SiteMatch[] {
    const direct = tokens(query);
    if (!direct.length) return [];
    const weights = new Map<string, number>(direct.map((term) => [term, 1]));
    for (const word of query.toLowerCase().match(/[a-z0-9]+/g) ?? []) {
      for (const similar of SYNONYMS[word] ?? []) if (!weights.has(stem(similar))) weights.set(stem(similar), 0.6);
    }
    const count = this.docs.length;
    const wantsPrice = PRICE_QUESTION.test(`${query} ${question}`);
    const lowValueOk = LOW_VALUE_WANTED.test(query);
    const scored: SiteMatch[] = [];
    for (const doc of this.docs) {
      let score = 0;
      for (const [term, weight] of weights) {
        const frequency = doc.terms.get(term);
        if (!frequency) continue;
        const df = this.documentFrequency.get(term) ?? 0;
        const idf = Math.log(1 + (count - df + 0.5) / (df + 0.5));
        score += weight * idf * (frequency * 2.2) / (frequency + 1.2 * (0.25 + 0.75 * (doc.length / this.averageLength)));
      }
      // A price card usually never says "price", so for these questions a real figure counts.
      if (wantsPrice && /\$\s?\d/.test(doc.chunk.text)) score *= PRICE_BOOST;
      if (!lowValueOk && LOW_VALUE_PAGE.test(doc.chunk.url)) score *= LOW_VALUE_WEIGHT;
      if (score > 0) scored.push({ ...doc.chunk, score: Math.round(score * 1000) / 1000 });
    }
    scored.sort((left, right) => right.score - left.score || left.url.localeCompare(right.url) || left.text.localeCompare(right.text));
    const perUrl = new Map<string, number>();
    const results: SiteMatch[] = [];
    for (const match of scored) {
      const used = perUrl.get(match.url) ?? 0;
      if (used >= perPage) continue;
      perUrl.set(match.url, used + 1);
      results.push(match);
      if (results.length >= limit) break;
    }
    return this.withPinned(`${query} ${question}`, [direct, tokens(question)], results, new Map(scored.map((match) => [matchKey(match), match.score])), wantsPrice, question || query);
  }

  /** Word matching alone misses pages that answer basic questions, so those always come along. */
  private withPinned(query: string, wordings: string[][], ranked: SiteMatch[], scores: Map<string, number>, wantsPrice: boolean, asked: string): SiteMatch[] {
    const pinned: SiteChunk[] = [];
    const fromPage = (url: string) => this.docs.filter((doc) => doc.chunk.url === url).map((doc) => doc.chunk);
    const whitepaper = this.whitepaperPassages(asked);
    const governing = new Set(whitepaper.map(matchKey));
    pinned.push(...whitepaper);
    if (ASKS_FOR_LINKS.test(query)) pinned.push(...this.docs.filter((doc) => doc.chunk.heading === OFFICIAL_HEADING).map((doc) => doc.chunk));
    for (const system of SYSTEM_PAGES) {
      if (!system.name.test(query)) continue;
      if ((wantsPrice || ASKS_ABOUT_PLANS.test(query)) && system.pricing) pinned.push(...fromPage(system.pricing).filter((chunk) => /\$\s?\d|\bfree\b|\bcustom\b/i.test(chunk.text)).slice(0, PINNED_PRICE_PASSAGES));
      for (const url of system.pages) pinned.push(...fromPage(url).slice(0, PINNED_PASSAGES));
    }
    for (const topic of TOPIC_PAGES) {
      if (!topic.about.test(query)) continue;
      for (const url of topic.pages) pinned.push(...fromPage(url).filter((chunk) => !topic.only || topic.only.test(chunk.text)).slice(0, PINNED_PASSAGES));
    }
    // A question naming a page's title wants that page's body, not just its shortest passages.
    for (const terms of wordings) {
      if (terms.length < TITLE_MATCH_TERMS) continue;
      const titled = this.docs.find((doc) => {
        const title = new Set(tokens(doc.chunk.title));
        return terms.every((term) => title.has(term));
      });
      if (titled) pinned.push(...fromPage(titled.chunk.url).slice(0, PINNED_PASSAGES));
    }
    const merged = new Map<string, SiteMatch>();
    // The page that governs the question leads, in topic order, and is the only one strong enough to be listed as a source.
    const lead = Math.max(ranked[0]?.score ?? 0, 1) * GOVERNING_LEAD;
    const order = [...governing];
    for (const chunk of pinned) merged.set(matchKey(chunk), { ...chunk, score: governing.has(matchKey(chunk)) ? lead - order.indexOf(matchKey(chunk)) / 1000 : scores.get(matchKey(chunk)) ?? 0 });
    for (const match of ranked) if (!governing.has(matchKey(match))) merged.set(matchKey(match), match);
    const kept = [...merged.values()].slice(0, MAX_PASSAGES);
    return kept.sort((left, right) => right.score - left.score || left.url.localeCompare(right.url));
  }

  /** The opening of each page that governs the question, then the passages closest to what was asked. */
  private whitepaperPassages(asked: string): SiteChunk[] {
    const terms = tokens(asked);
    // "Withdraw" should find "withdrawal", which stemming doesn't do.
    const mentions = (doc: SiteIndex["docs"][number], term: string) =>
      doc.terms.has(term) || [...doc.terms.keys()].some((other) => Math.min(term.length, other.length) >= WORD_ROOT_LENGTH && (other.startsWith(term) || term.startsWith(other)));
    const passages: SiteChunk[] = [];
    for (const topic of whitepaperTopics(asked)) {
      for (const url of topic.pages) {
        const docs = this.docs.filter((doc) => doc.chunk.url === url);
        if (topic.whole) {
          passages.push(...docs.map((doc) => doc.chunk));
          continue;
        }
        // On the Vault's page every passage says "Vault", so the words rare on this page are what pick a passage.
        const weigh = (doc: SiteIndex["docs"][number]) => terms.reduce((sum, term) => (mentions(doc, term) ? sum + Math.log(1 + docs.length / docs.filter((other) => mentions(other, term)).length) : sum), 0);
        const closest = docs.slice(1).map((doc) => ({ doc, weight: weigh(doc) })).sort((left, right) => right.weight - left.weight).slice(0, WHITEPAPER_PASSAGES - 1);
        passages.push(...docs.slice(0, 1).map((doc) => doc.chunk), ...closest.map(({ doc }) => doc.chunk));
      }
    }
    return passages.filter((passage, index) => passages.findIndex((other) => matchKey(other) === matchKey(passage)) === index);
  }
}
