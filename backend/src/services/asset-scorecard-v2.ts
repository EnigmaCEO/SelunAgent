/**
 * Asset Scorecard methodology v2: measured price history on top of the portfolio
 * model's market-structure scores.
 *
 * Phase 5 scoring feeds portfolio plans, and its "volatility" and "drawdown"
 * inputs are estimates from rank, liquidity, and structure. Those saturate for
 * large assets. This module doesn't change them, so plans are unaffected. It adds
 * real measurements for the scorecard only.
 *
 * The scales are fixed, not relative to other assets, so a score never depends on
 * what else is on the card, and two assets with similar histories score alike.
 */

export const SCORECARD_METHODOLOGY_VERSION = "asset-scorecard-v2";

const DAY_MS = 86_400_000;
const WINDOW_CLOSES = 365;
/** Below this, volatility and drawdown say too little to score. */
export const MIN_CLOSES = 180;
const FULL_WINDOW_CLOSES = 360;
const MAX_STALE_DAYS = 2;
const MAX_GAP_DAYS = 5;
/** A one-day jump this large that fully reverses the next day is a feed error, not a price. */
const BAD_PRINT_LOG_MOVE = Math.log(1.5);
const BAD_PRINT_REVERSAL_LOG_TOLERANCE = 0.1;
/** At this many extreme days a year, the series can't be told apart from a faulty feed, so it isn't scored. */
const MAX_EXTREME_MOVES = 5;

export const VOLATILITY_CEILING_PCT = 150;
export const DRAWDOWN_CEILING_PCT = 90;
const VOLATILITY_WEIGHT = 0.6;
const DRAWDOWN_WEIGHT = 0.4;
/** The same risk discount the portfolio model applies to quality. */
const RISK_DISCOUNT = 0.72;
/** A symbol lookup on an exchange is only trusted for assets large enough that the symbol is unambiguous. */
const SYMBOL_FALLBACK_MAX_RANK = 200;

export const SCORECARD_METHODOLOGY = {
  version: SCORECARD_METHODOLOGY_VERSION,
  priceRiskScore: `${VOLATILITY_WEIGHT} x min(annualized volatility / ${VOLATILITY_CEILING_PCT}%, 1) + ${DRAWDOWN_WEIGHT} x min(largest drawdown / ${DRAWDOWN_CEILING_PCT}%, 1)`,
  overallScore: `qualityScore x (1 - ${RISK_DISCOUNT} x priceRiskScore)`,
  priceHistory: `Up to ${WINDOW_CLOSES} complete UTC daily closes. Volatility is the standard deviation of daily log returns x sqrt(365). No score below ${MIN_CLOSES} closes.`,
  portfolioModelFields: "qualityScore, riskScore, compositeScore, liquidityScore, structuralScore, and selectionReasons come from the portfolio model. Its volatility and drawdown inputs are estimates from rank, liquidity, and structure, not price history.",
} as const;

export type DailyClose = { timestamp: number; close: number };
export type PriceHistorySource = "coingecko" | "coinbase";

export type PriceHistoryMeasure =
  | {
      status: "measured" | "partial";
      source: PriceHistorySource;
      closes: number;
      windowStart: string;
      windowEnd: string;
      annualizedVolatilityPct: number;
      maxDrawdownPct: number;
      currentDrawdownPct: number;
      badPrintsExcluded: number;
    }
  | { status: "insufficient"; source: PriceHistorySource; closes: number; reason: string }
  | { status: "unavailable"; reason: string };

function round(value: number, digits: number): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function isoDate(timestamp: number): string {
  return new Date(timestamp).toISOString().slice(0, 10);
}

function startOfUtcDay(timestamp: number): number {
  return Math.floor(timestamp / DAY_MS) * DAY_MS;
}

/** Keeps complete days only, which also drops a feed's running price for today. */
function usable(rows: DailyClose[], now: number): DailyClose[] {
  const today = startOfUtcDay(now);
  const byDay = new Map<number, number>();
  for (const { timestamp, close } of rows) {
    if (timestamp % DAY_MS === 0 && timestamp <= today && Number.isFinite(close) && close > 0) byDay.set(timestamp, close);
  }
  return [...byDay.entries()].sort((left, right) => left[0] - right[0]).map(([timestamp, close]) => ({ timestamp, close }));
}

/** CoinGecko stamps each daily price at 00:00 UTC, which is the previous day's close. */
export function parseCoinGeckoCloses(payload: unknown): DailyClose[] {
  const prices = (payload as { prices?: unknown } | null)?.prices;
  if (!Array.isArray(prices)) return [];
  return prices.flatMap((row) => (Array.isArray(row) ? [{ timestamp: Number(row[0]), close: Number(row[1]) }] : []));
}

/** Coinbase stamps a candle at its open, in seconds. Shifted to its close so it lines up with CoinGecko. */
export function parseCoinbaseCloses(payload: unknown): DailyClose[] {
  if (!Array.isArray(payload)) return [];
  return payload.flatMap((row) => (Array.isArray(row) ? [{ timestamp: Number(row[0]) * 1000 + DAY_MS, close: Number(row[4]) }] : []));
}

function withoutBadPrints(rows: DailyClose[]): { rows: DailyClose[]; excluded: number } {
  const kept: DailyClose[] = [];
  let excluded = 0;
  rows.forEach((row, index) => {
    const previous = rows[index - 1];
    const next = rows[index + 1];
    if (previous && next) {
      const fromPrevious = Math.log(row.close / previous.close);
      const fromNext = Math.log(row.close / next.close);
      const reversed = Math.abs(Math.log(next.close / previous.close)) <= BAD_PRINT_REVERSAL_LOG_TOLERANCE;
      if (Math.abs(fromPrevious) >= BAD_PRINT_LOG_MOVE && Math.abs(fromNext) >= BAD_PRINT_LOG_MOVE && fromPrevious * fromNext > 0 && reversed) {
        excluded += 1;
        return;
      }
    }
    kept.push(row);
  });
  return { rows: kept, excluded };
}

export function measurePriceHistory(rows: DailyClose[], source: PriceHistorySource, now: number): PriceHistoryMeasure {
  const clean = withoutBadPrints(usable(rows, now));
  const window = clean.rows.slice(-WINDOW_CLOSES);
  if (window.length < MIN_CLOSES) {
    return { status: "insufficient", source, closes: window.length, reason: `Only ${window.length} daily closes were available, and ${MIN_CLOSES} are needed.` };
  }
  const last = window[window.length - 1];
  if (startOfUtcDay(now) - last.timestamp > MAX_STALE_DAYS * DAY_MS) {
    return { status: "insufficient", source, closes: window.length, reason: `The price history stops at ${isoDate(last.timestamp)}.` };
  }
  const closes = window.map((row) => row.close);
  const extremeMoves = closes.slice(1).filter((close, index) => Math.abs(Math.log(close / closes[index])) >= BAD_PRINT_LOG_MOVE).length;
  if (extremeMoves >= MAX_EXTREME_MOVES) {
    return { status: "insufficient", source, closes: window.length, reason: `The price feed shows ${extremeMoves} extreme one-day moves, which is too erratic to measure reliably.` };
  }
  const returns: number[] = [];
  let peak = 0;
  let maxDrawdown = 0;
  for (const [index, row] of window.entries()) {
    const previous = window[index - 1];
    if (previous) {
      if (row.timestamp - previous.timestamp > MAX_GAP_DAYS * DAY_MS) {
        return { status: "insufficient", source, closes: window.length, reason: `The price history has a gap after ${isoDate(previous.timestamp)}.` };
      }
      returns.push(Math.log(row.close / previous.close));
    }
    peak = Math.max(peak, row.close);
    maxDrawdown = Math.max(maxDrawdown, 1 - row.close / peak);
  }
  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance = returns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (returns.length - 1);
  return {
    status: window.length >= FULL_WINDOW_CLOSES ? "measured" : "partial",
    source,
    closes: window.length,
    windowStart: isoDate(window[0].timestamp),
    windowEnd: isoDate(last.timestamp),
    annualizedVolatilityPct: round(Math.sqrt(variance * 365) * 100, 1),
    maxDrawdownPct: round(maxDrawdown * 100, 1),
    currentDrawdownPct: round((1 - last.close / peak) * 100, 1),
    badPrintsExcluded: clean.excluded,
  };
}

/** Null without enough history. An estimate is never substituted for a measurement. */
export function priceRiskScore(measure: PriceHistoryMeasure): number | null {
  if (measure.status !== "measured" && measure.status !== "partial") return null;
  const score =
    VOLATILITY_WEIGHT * Math.min(measure.annualizedVolatilityPct / VOLATILITY_CEILING_PCT, 1) +
    DRAWDOWN_WEIGHT * Math.min(measure.maxDrawdownPct / DRAWDOWN_CEILING_PCT, 1);
  return round(score, 6);
}

export function overallScore(qualityScore: number, priceRisk: number | null): number | null {
  return priceRisk === null ? null : round(qualityScore * (1 - RISK_DISCOUNT * priceRisk), 6);
}

type AssetRef = { coingeckoId: string; symbol: string; marketCapRank: number | null };

type HistoryDeps = {
  fetchJson?: (url: string) => Promise<unknown>;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
};

async function defaultFetchJson(url: string): Promise<unknown> {
  const response = await fetch(url, { headers: { Accept: "application/json", "User-Agent": "selun-agent" }, signal: AbortSignal.timeout(12_000) });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

const MIN_INTERVAL_MS = 2_500;
const RATE_LIMIT_BACKOFF_MS = [8_000, 16_000];
const RATE_LIMIT_COOLDOWN_MS = 60_000;

/** Fetches one asset at a time, because free feeds rate limit, and keeps results for the UTC day so reruns agree. */
/** Shared so the check before purchase and the paid run use one cache and one provider rate limit. */
export const scorecardPriceHistory = (): PriceHistoryService => (sharedPriceHistory ??= new PriceHistoryService());
let sharedPriceHistory: PriceHistoryService | undefined;

export class PriceHistoryService {
  private readonly fetchJson: (url: string) => Promise<unknown>;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly now: () => number;
  private readonly cache = new Map<string, PriceHistoryMeasure>();
  private queue: Promise<unknown> = Promise.resolve();
  private coinGeckoLimitedUntil = 0;

  constructor(deps: HistoryDeps = {}) {
    this.fetchJson = deps.fetchJson ?? defaultFetchJson;
    this.sleep = deps.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    this.now = deps.now ?? Date.now;
  }

  measure(asset: AssetRef): Promise<PriceHistoryMeasure> {
    const next = this.queue.then(() => this.measureNow(asset));
    this.queue = next.catch(() => undefined);
    return next;
  }

  private async measureNow(asset: AssetRef): Promise<PriceHistoryMeasure> {
    const key = `${isoDate(this.now())}:${asset.coingeckoId}`;
    const cached = this.cache.get(key);
    if (cached) return cached;

    const primary = await this.fromCoinGecko(asset);
    const measure = primary.status === "measured" || primary.status === "partial" ? primary : await this.fromCoinbase(asset, primary);
    if (measure.status !== "unavailable") this.cache.set(key, measure);
    return measure;
  }

  private async fromCoinGecko(asset: AssetRef): Promise<PriceHistoryMeasure> {
    const url = `https://api.coingecko.com/api/v3/coins/${encodeURIComponent(asset.coingeckoId)}/market_chart?vs_currency=usd&days=${WINDOW_CLOSES}&interval=daily`;
    const backoffs = this.now() < this.coinGeckoLimitedUntil ? [] : RATE_LIMIT_BACKOFF_MS;
    for (let attempt = 0; ; attempt += 1) {
      try {
        const payload = await this.fetchJson(url);
        await this.sleep(MIN_INTERVAL_MS);
        return measurePriceHistory(parseCoinGeckoCloses(payload), "coingecko", this.now());
      } catch (error) {
        const limited = String(error).includes("HTTP 429");
        if (limited && attempt < backoffs.length) {
          await this.sleep(backoffs[attempt]);
          continue;
        }
        // Later assets in the batch shouldn't each wait out the same limit.
        if (limited) this.coinGeckoLimitedUntil = this.now() + RATE_LIMIT_COOLDOWN_MS;
        return { status: "unavailable", reason: "The price history feed did not respond." };
      }
    }
  }

  private async fromCoinbase(asset: AssetRef, primary: PriceHistoryMeasure): Promise<PriceHistoryMeasure> {
    if (asset.marketCapRank === null || asset.marketCapRank > SYMBOL_FALLBACK_MAX_RANK) return primary;
    const today = startOfUtcDay(this.now());
    const rows: DailyClose[] = [];
    try {
      // Coinbase returns at most 300 candles per request.
      for (const [start, end] of [[today - 300 * DAY_MS, today], [today - 600 * DAY_MS, today - 300 * DAY_MS]]) {
        const url = `https://api.exchange.coinbase.com/products/${encodeURIComponent(asset.symbol.toUpperCase())}-USD/candles?granularity=86400&start=${new Date(start).toISOString()}&end=${new Date(end).toISOString()}`;
        rows.push(...parseCoinbaseCloses(await this.fetchJson(url)));
      }
    } catch {
      if (rows.length === 0) return primary;
    }
    const fallback = measurePriceHistory(rows, "coinbase", this.now());
    return fallback.status === "measured" || fallback.status === "partial" ? fallback : primary;
  }
}

/** A requested asset the segment's screen left out: it can be measured, but has no market standing to score. */
export type UnscreenedAsset = { asset: string; name: string; coingeckoId: string };

type AssetLookup = (query: string) => { status: string; asset?: { symbol: string; name: string; coingeckoId: string } };

/** Requests can name a symbol or an ID, so a scored asset is matched on both. Each asset is listed once. */
export function findUnscreenedAssets(
  requested: string[],
  scored: { asset: string; coingeckoId: string }[],
  lookup: AssetLookup,
): UnscreenedAsset[] {
  const seen = new Set(scored.flatMap((card) => [card.asset.toUpperCase(), card.coingeckoId.toUpperCase()]));
  return requested.flatMap((query) => {
    if (seen.has(query.toUpperCase())) return [];
    const found = lookup(query);
    if (found.status !== "resolved" || !found.asset || seen.has(found.asset.symbol.toUpperCase())) return [];
    seen.add(found.asset.symbol.toUpperCase());
    return [{ asset: found.asset.symbol, name: found.asset.name, coingeckoId: found.asset.coingeckoId }];
  });
}

type BaseCard = { asset: string; coingeckoId: string; marketCapRank: number | null; qualityScore: number };

/**
 * Adds measurements to the portfolio model's cards. If the feeds were down for
 * every asset, this throws: the x402 route records the failure, and the payer can
 * retry without paying again, rather than getting a card with nothing measured.
 */
export async function applyScorecardMethodology<T extends BaseCard>(
  cards: T[],
  unscreened: UnscreenedAsset[],
  history: Pick<PriceHistoryService, "measure">,
  options: { requireEveryAsset?: boolean; requireOverallScore?: boolean } = {},
) {
  const measures = await Promise.all(
    cards.map((card) => history.measure({ coingeckoId: card.coingeckoId, symbol: card.asset, marketCapRank: card.marketCapRank })),
  );
  // The screen supplied no rank for these, so only the ID lookup is used.
  const unscreenedMeasures = await Promise.all(
    unscreened.map((asset) => history.measure({ coingeckoId: asset.coingeckoId, symbol: asset.asset, marketCapRank: null })),
  );
  const all = [...measures, ...unscreenedMeasures];
  const failed = options.requireEveryAsset ? all.some((measure) => measure.status === "unavailable") : all.every((measure) => measure.status === "unavailable");
  if (all.length > 0 && failed) {
    throw new Error("Price history is temporarily unavailable.");
  }
  const scorecards = cards
    .map((card, index) => {
      const measure = measures[index];
      const priceRisk = priceRiskScore(measure);
      return { ...card, overallScore: overallScore(card.qualityScore, priceRisk), priceRiskScore: priceRisk, priceHistory: measure };
    })
    .sort((left, right) => (right.overallScore ?? -1) - (left.overallScore ?? -1));
  // A purchase locked to a screening decision was sold a score, so a card without one is a failed run, not a result.
  if (options.requireOverallScore && (scorecards.length === 0 || scorecards.some((card) => card.overallScore === null))) {
    throw new Error("Price history no longer supports an overall score for every purchased asset.");
  }
  return {
    scorecards,
    unscreenedAssets: unscreened.map((asset, index) => ({
      ...asset,
      priceRiskScore: priceRiskScore(unscreenedMeasures[index]),
      priceHistory: unscreenedMeasures[index],
    })),
  };
}
