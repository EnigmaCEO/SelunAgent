import { findAssetMentions } from "./asset-resolver";

/**
 * Read-only access to what Sagitta's Continuity Engine (SCE) publishes now: Radar's public alerts and the
 * security advisories in SCE's case library. Every read says whether it worked. A failed read is "unavailable",
 * never an empty list, so Selun can't mistake an outage for "nothing to report".
 */

export const RADAR_ALERTS_URL = "https://radar.sagitta.systems/alerts";
export const ADVISORY_WINDOW_DAYS = 90;
const DEFAULT_BASE_URL = "https://continuityengineserver.fly.dev";
const ALERT_TTL_MS = 60_000;
const ADVISORY_TTL_MS = 15 * 60_000;
const TIMEOUT_MS = 8_000;
const MAX_ADVISORIES = 3;

export type LiveAlert = {
  id: string;
  severity: string;
  status: string;
  monitorType: string;
  summary: string;
  whatHappened: string | null;
  asset: string | null;
  chain: string | null;
  createdAt: string;
  resolvedAt: string | null;
  url: string;
};

export type Scope = {
  /** What the customer asked about, for display, such as "Ethereum". Null means everything Radar monitors. */
  label: string | null;
  /** Lowercase names and symbols an alert must mention to count. Empty means every alert counts. */
  terms: string[];
  /** Search terms for crypto security advisories. */
  advisoryTerms: string[];
};

export type AlertRead =
  | { state: "ok"; fetchedAt: string; scope: string | null; active: LiveAlert[]; latest: LiveAlert | null }
  | { state: "unavailable"; fetchedAt: string; scope: string | null };

export type Advisory = {
  id: string;
  title: string;
  severity: string;
  source: string;
  cve: string | null;
  updatedAt: string;
  url: string | null;
};

export type AdvisoryRead =
  | { state: "ok"; fetchedAt: string; scope: string | null; windowDays: number; total: number; items: Advisory[] }
  | { state: "unavailable"; fetchedAt: string; scope: string | null; windowDays: number };

export interface LiveIntel {
  alerts(scope: Scope): Promise<AlertRead>;
  advisories(scope: Scope): Promise<AdvisoryRead>;
}

/** Networks, protocols, oracles, and stablecoins a customer may ask Radar about. The value is how it's written. */
const SUBJECTS: Record<string, string> = {
  ethereum: "Ethereum", base: "Base", arbitrum: "Arbitrum", optimism: "Optimism", polygon: "Polygon", avalanche: "Avalanche",
  bsc: "BSC", solana: "Solana", aave: "Aave", uniswap: "Uniswap", curve: "Curve", lido: "Lido", chainlink: "Chainlink",
  compound: "Compound", maker: "Maker", ethena: "Ethena", gho: "GHO", susde: "sUSDe", usde: "USDe", usdc: "USDC",
  usdt: "USDT", dai: "DAI", pyth: "Pyth", redstone: "RedStone", wormhole: "Wormhole", layerzero: "LayerZero",
  stargate: "Stargate", across: "Across", balancer: "Balancer", pendle: "Pendle", morpho: "Morpho", eigenlayer: "EigenLayer",
};
/** "Across" is usually just a word. */
const AMBIGUOUS = new Set(["across", "base", "curve", "compound", "maker"]);

/** Software whose advisories bear on holding or using each asset. */
const ASSET_SOFTWARE: Record<string, string[]> = {
  BTC: ["bitcoin", "electrum", "bip39", "bitcoinjs"],
  ETH: ["ethereum", "web3", "metamask", "openzeppelin", "ethers"],
  SOL: ["solana"],
};
const GENERAL_SOFTWARE = ["wallet", "ethereum", "bitcoin", "web3", "solana", "electrum", "metamask", "openzeppelin"];

export function scopeOf(message: string): Scope {
  const terms = new Set<string>();
  const advisoryTerms = new Set<string>();
  let label: string | null = null;
  for (const asset of findAssetMentions(message)) {
    label ??= asset.name;
    terms.add(asset.symbol.toLowerCase());
    terms.add(asset.name.toLowerCase());
    for (const term of ASSET_SOFTWARE[asset.symbol] ?? [asset.name.toLowerCase()]) advisoryTerms.add(term);
  }
  for (const word of message.toLowerCase().match(/[a-z0-9]+/g) ?? []) {
    const display = SUBJECTS[word];
    if (!display) continue;
    // "Base" or "Curve" count only written as a name.
    if (AMBIGUOUS.has(word) && !new RegExp(`\\b${display}\\b`).test(message)) continue;
    label ??= display;
    terms.add(word);
    if (word === "ethereum") ASSET_SOFTWARE.ETH.forEach((term) => advisoryTerms.add(term));
    else if (word === "solana") advisoryTerms.add("solana");
    else advisoryTerms.add(word);
  }
  return { label, terms: [...terms], advisoryTerms: advisoryTerms.size ? [...advisoryTerms] : GENERAL_SOFTWARE };
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function wordsOf(...values: unknown[]): Set<string> {
  return new Set(values.flatMap((value) => (typeof value === "string" ? value.toLowerCase().match(/[a-z0-9]+/g) ?? [] : [])));
}

export function toLiveAlert(raw: Record<string, unknown>): LiveAlert | null {
  const id = text(raw.id);
  const createdAt = text(raw.createdAt);
  const summary = text(raw.summary);
  if (!id || !createdAt || !summary) return null;
  return {
    id,
    severity: text(raw.severity) ?? "info",
    status: text(raw.status) ?? "active",
    monitorType: text(raw.monitorType) ?? "monitor",
    summary,
    whatHappened: text(raw.whatHappened),
    asset: text(raw.asset),
    chain: text(raw.chain),
    createdAt,
    resolvedAt: text(raw.resolvedAt),
    url: `${RADAR_ALERTS_URL}/${encodeURIComponent(id)}`,
  };
}

export function isResolved(alert: { status: string }): boolean {
  return alert.status.toLowerCase() === "resolved";
}

const SEVERITY_RANK: Record<string, number> = { critical: 0, high: 1, warning: 2, medium: 2, low: 3, info: 4 };
const rank = (severity: string) => SEVERITY_RANK[severity.toLowerCase()] ?? 5;
const newestFirst = (a: string, b: string) => Date.parse(b) - Date.parse(a);

/** The alerts in scope: active ones, most severe first, and the latest one either way. */
export function selectAlerts(rawAlerts: Record<string, unknown>[], scope: Scope, fetchedAt: string): AlertRead {
  const inScope = rawAlerts.filter((raw) => {
    if (raw.visibility !== undefined && raw.visibility !== "public") return false;
    if (!scope.terms.length) return true;
    const words = wordsOf(raw.asset, raw.chain, raw.oracle, raw.bridge, raw.affectedProtocol, raw.poolName, raw.route, raw.assetPair);
    return scope.terms.some((term) => words.has(term));
  });
  const alerts = inScope.map(toLiveAlert).filter((alert): alert is LiveAlert => alert !== null);
  const active = alerts.filter((alert) => !isResolved(alert)).sort((a, b) => rank(a.severity) - rank(b.severity) || newestFirst(a.createdAt, b.createdAt));
  const latest = [...alerts].sort((a, b) => newestFirst(a.createdAt, b.createdAt))[0] ?? null;
  return { state: "ok", fetchedAt, scope: scope.label, active, latest };
}

function advisoryUrl(raw: Record<string, unknown>, cve: string | null): string | null {
  const refs = Array.isArray(raw.sourceRefs) ? (raw.sourceRefs as Record<string, unknown>[]) : [];
  const urls = refs.flatMap((ref) => (Array.isArray(ref.referenceUrls) ? ref.referenceUrls.filter((url): url is string => typeof url === "string") : []));
  const safe = urls.filter((url) => /^https:\/\//.test(url));
  const github = safe.find((url) => url.startsWith("https://github.com/advisories/"));
  if (github) return github;
  if (cve) return `https://nvd.nist.gov/vuln/detail/${encodeURIComponent(cve)}`;
  const osvId = refs.map((ref) => text(ref.externalId)).find((id) => id && refs.some((ref) => ref.source === "OSV"));
  if (osvId) return `https://osv.dev/vulnerability/${encodeURIComponent(osvId)}`;
  return safe[0] ?? null;
}

/** Records that name one of the terms as a word, in the title, an affected package, or the summary. */
export function selectAdvisories(rawItems: Record<string, unknown>[], terms: string[]): Advisory[] {
  const patterns = terms.map((term) => new RegExp(`(^|[^a-z0-9])${term.replace(/[^a-z0-9]/gi, "\\$&")}([^a-z0-9]|$)`, "i"));
  const found = new Map<string, Advisory>();
  for (const raw of rawItems) {
    const id = text(raw.caseId);
    const title = text(raw.title);
    const updatedAt = text(raw.updatedAt);
    const source = text(raw.source) ?? "";
    // Exploit history belongs to Threat Case Relevance, not current advisories.
    if (!id || !title || !updatedAt || /rekt/i.test(source)) continue;
    const packages = Array.isArray(raw.affectedPackages)
      ? (raw.affectedPackages as Record<string, unknown>[]).map((pkg) => text(pkg.package_name) ?? text(pkg.name) ?? "").join(" ")
      : "";
    const haystack = `${title}\n${packages}\n${(text(raw.summary) ?? "").slice(0, 1500)}`;
    if (!patterns.some((pattern) => pattern.test(haystack))) continue;
    const cveRefs = Array.isArray(raw.cveRefs) ? raw.cveRefs.filter((ref): ref is string => typeof ref === "string" && /^CVE-\d{4}-\d+$/.test(ref)) : [];
    const cve = cveRefs[0] ?? null;
    found.set(id, { id, title: title.slice(0, 160), severity: text(raw.severity) ?? "unknown", source, cve, updatedAt, url: advisoryUrl(raw, cve) });
  }
  return [...found.values()].sort((a, b) => rank(a.severity) - rank(b.severity) || newestFirst(a.updatedAt, b.updatedAt));
}

type Fetch = (url: string, init?: { signal?: AbortSignal; headers?: Record<string, string> }) => Promise<{ ok: boolean; json(): Promise<unknown> }>;

export class SceLiveIntel implements LiveIntel {
  private readonly baseUrl: string;
  private readonly fetchFn: Fetch;
  private readonly now: () => Date;
  private feed: { at: number; alerts: Record<string, unknown>[] } | null = null;
  private advisoryCache = new Map<string, { at: number; read: AdvisoryRead }>();

  constructor(options: { baseUrl?: string; fetch?: Fetch; now?: () => Date } = {}) {
    this.baseUrl = (options.baseUrl?.trim() || DEFAULT_BASE_URL).replace(/\/+$/, "");
    this.fetchFn = options.fetch ?? ((url, init) => fetch(url, init));
    this.now = options.now ?? (() => new Date());
  }

  private async getJson(path: string): Promise<unknown> {
    const response = await this.fetchFn(`${this.baseUrl}${path}`, { signal: AbortSignal.timeout(TIMEOUT_MS), headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error("SCE read failed");
    return response.json();
  }

  async alerts(scope: Scope): Promise<AlertRead> {
    const now = this.now();
    try {
      if (!this.feed || now.getTime() - this.feed.at >= ALERT_TTL_MS) {
        const data = await this.getJson("/v1/sce/radar/alerts?limit=200");
        if (!Array.isArray(data)) throw new Error("SCE returned an unexpected alert feed");
        this.feed = { at: now.getTime(), alerts: data.filter((item): item is Record<string, unknown> => typeof item === "object" && item !== null) };
      }
      return selectAlerts(this.feed.alerts, scope, new Date(this.feed.at).toISOString());
    } catch {
      return { state: "unavailable", fetchedAt: now.toISOString(), scope: scope.label };
    }
  }

  async advisories(scope: Scope): Promise<AdvisoryRead> {
    const now = this.now();
    const key = [...scope.advisoryTerms].sort().join(",");
    const cached = this.advisoryCache.get(key);
    if (cached && now.getTime() - cached.at < ADVISORY_TTL_MS) return { ...cached.read, scope: scope.label };
    const since = new Date(now.getTime() - ADVISORY_WINDOW_DAYS * 86_400_000).toISOString().slice(0, 10);
    const reads = await Promise.allSettled(scope.advisoryTerms.map(async (term) => {
      const data = await this.getJson(`/case-library?search=${encodeURIComponent(term)}&pageSize=10&sortBy=updatedAt&sortDir=desc&updatedFrom=${since}`);
      const items = (data as { items?: unknown })?.items;
      if (!Array.isArray(items)) throw new Error("SCE returned an unexpected case list");
      return items.filter((item): item is Record<string, unknown> => typeof item === "object" && item !== null);
    }));
    const fetchedAt = now.toISOString();
    // Only when every search answered is "none found" true.
    if (reads.some((read) => read.status === "rejected")) {
      return { state: "unavailable", fetchedAt, scope: scope.label, windowDays: ADVISORY_WINDOW_DAYS };
    }
    const items = selectAdvisories(reads.flatMap((read) => (read.status === "fulfilled" ? read.value : [])), scope.advisoryTerms);
    const read: AdvisoryRead = { state: "ok", fetchedAt, scope: scope.label, windowDays: ADVISORY_WINDOW_DAYS, total: items.length, items: items.slice(0, MAX_ADVISORIES) };
    if (this.advisoryCache.size > 200) this.advisoryCache.clear();
    this.advisoryCache.set(key, { at: now.getTime(), read });
    return read;
  }
}

// ---- Wording. Every sentence states only what the read returned, with its time.

export function asOf(iso: string): string {
  const date = new Date(iso);
  const time = date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", timeZone: "UTC" });
  const day = date.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
  return `${time} UTC on ${day}`;
}

function day(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

function sentence(value: string): string {
  const trimmed = value.trim();
  return /[.!?]$/.test(trimmed) ? trimmed : `${trimmed}.`;
}

export function alertsText(read: AlertRead): string {
  const scope = read.scope ? ` involving ${read.scope}` : "";
  if (read.state === "unavailable") {
    return `I couldn't reach Radar's alert feed just now, so I can't say whether there are active alerts${scope}. Radar's alerts page has the latest.`;
  }
  const time = asOf(read.fetchedAt);
  if (read.active.length) {
    const top = read.active[0];
    const count = read.active.length === 1 ? "1 active alert" : `${read.active.length} active alerts`;
    return `Radar shows ${count}${scope} as of ${time}. The most severe is a ${top.severity} ${top.monitorType} alert: ${sentence(top.summary)} The card lists ${read.active.length === 1 ? "it" : "them"} with links to the details.`;
  }
  const latest = read.latest
    ? ` The most recent, a ${read.latest.severity} ${read.latest.monitorType} alert from ${day(read.latest.createdAt)}, ${read.latest.resolvedAt ? `was resolved on ${day(read.latest.resolvedAt)}` : "is resolved"}.`
    : "";
  return `Radar shows no active alerts${scope} as of ${time}.${latest} Radar watches specific oracles, bridges, and liquidity pools, so no alert doesn't mean no risk.`;
}

export function advisoriesText(read: AdvisoryRead): string {
  const scope = read.scope ? ` relevant to ${read.scope}` : "";
  if (read.state === "unavailable") {
    return `I couldn't reach SCE's case library just now, so I can't list recent security advisories${scope}. Please try again shortly.`;
  }
  if (!read.items.length) {
    return `SCE's case library has no crypto-related security advisories${scope} added or updated in the past ${read.windowDays} days, as of ${asOf(read.fetchedAt)}.`;
  }
  const top = read.items[0];
  const count = read.total === 1 ? "1 crypto-related security advisory" : `${read.total} crypto-related security advisories`;
  return `SCE's case library lists ${count}${scope} added or updated in the past ${read.windowDays} days, as of ${asOf(read.fetchedAt)}. The most severe is rated ${top.severity}${top.cve ? ` (${top.cve})` : ""}: ${sentence(top.title)} The card links to each one's source. For whether a known incident type affects a specific protocol, I can prepare a Threat Case Relevance check.`;
}

/** What the model is given: the facts, nothing to embellish. */
export function liveFacts(alerts: AlertRead, advisories: AdvisoryRead | null) {
  return {
    shown_to_customer: true,
    radar_alerts: alerts.state === "unavailable"
      ? { state: "unavailable", note: "The feed couldn't be read. Say so; never say there are no alerts." }
      : {
        state: "ok",
        as_of: asOf(alerts.fetchedAt),
        scope: alerts.scope ?? "everything Radar monitors",
        active_count: alerts.active.length,
        active: alerts.active.slice(0, 3).map((alert) => ({ severity: alert.severity, type: alert.monitorType, summary: alert.summary, since: day(alert.createdAt) })),
        most_recent: alerts.latest ? { severity: alerts.latest.severity, summary: alerts.latest.summary, created: day(alerts.latest.createdAt), resolved: alerts.latest.resolvedAt ? day(alerts.latest.resolvedAt) : null } : null,
        coverage: "Radar watches specific oracles, bridges, and liquidity pools. No alert doesn't mean no risk.",
      },
    security_advisories: !advisories ? undefined : advisories.state === "unavailable"
      ? { state: "unavailable", note: "The case library couldn't be read. Say so." }
      : {
        state: "ok",
        window: `added or updated in the past ${advisories.windowDays} days`,
        count: advisories.total,
        top: advisories.items.map((item) => ({ title: item.title, severity: item.severity, cve: item.cve, updated: day(item.updatedAt) })),
      },
  };
}
