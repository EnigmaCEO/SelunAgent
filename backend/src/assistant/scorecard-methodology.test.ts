import assert from "node:assert/strict";
import test from "node:test";
import {
  MIN_CLOSES,
  PriceHistoryService,
  SCORECARD_METHODOLOGY,
  SCORECARD_METHODOLOGY_VERSION,
  applyScorecardMethodology,
  findUnscreenedAssets,
  measurePriceHistory,
  overallScore,
  parseCoinbaseCloses,
  parseCoinGeckoCloses,
  priceRiskScore,
  type DailyClose,
  type PriceHistoryMeasure,
} from "../services/asset-scorecard-v2";
import { resolveAsset } from "./asset-resolver";
import { getToolSpec } from "./capability-registry";
import { presentResult } from "./result-presenter";

const DAY = 86_400_000;
const NOW = Date.UTC(2026, 10, 5, 21, 35);
const TODAY = Date.UTC(2026, 10, 5);

/** The latest close is stamped today at 00:00. Oldest first. */
function series(closes: number[], end = TODAY): DailyClose[] {
  return closes.map((close, index) => ({ timestamp: end - (closes.length - 1 - index) * DAY, close }));
}

function alternating(count: number, dailyMove: number): number[] {
  return Array.from({ length: count }, (_, index) => 100 * (index % 2 === 0 ? 1 : 1 + dailyMove));
}

function measured(annualizedVolatilityPct: number, maxDrawdownPct: number): PriceHistoryMeasure {
  return { status: "measured", source: "coingecko", closes: 365, windowStart: "2025-11-05", windowEnd: "2026-11-05", annualizedVolatilityPct, maxDrawdownPct, currentDrawdownPct: 10, badPrintsExcluded: 0 };
}

test("volatility and drawdown match values worked out independently", () => {
  const flat = measurePriceHistory(series(Array(365).fill(100)), "coingecko", NOW);
  if (flat.status !== "measured") return assert.fail("expected a measurement");
  assert.equal(flat.annualizedVolatilityPct, 0);
  assert.equal(flat.maxDrawdownPct, 0);

  const closes = Array.from({ length: 365 }, (_, index) => (index < 100 ? 100 : index < 200 ? 40 : 80));
  const drop = measurePriceHistory(series(closes), "coingecko", NOW);
  if (drop.status !== "measured") return assert.fail("expected a measurement");
  assert.equal(drop.maxDrawdownPct, 60, "100 to 40");
  assert.equal(drop.currentDrawdownPct, 20, "100 to 80");

  // Two moves of ln(0.4) and ln(2) among 364 returns.
  const logs = closes.slice(1).map((value, index) => Math.log(value / closes[index]));
  const mean = logs.reduce((sum, value) => sum + value, 0) / logs.length;
  const sd = Math.sqrt(logs.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (logs.length - 1));
  assert.equal(drop.annualizedVolatilityPct, Math.round(sd * Math.sqrt(365) * 1000) / 10);
});

test("the score moves in the right direction and in proportion", () => {
  const risk = (vol: number, drawdown: number) => priceRiskScore(measured(vol, drawdown))!;
  assert.ok(risk(60, 50) > risk(40, 50), "more volatile is riskier");
  assert.ok(risk(50, 70) > risk(50, 40), "deeper drawdown is riskier");
  assert.ok(Math.abs(risk(50.1, 50) - risk(50, 50)) < 0.001, "a tiny input change makes a tiny score change");
  assert.ok(overallScore(0.9, risk(40, 40))! > overallScore(0.9, risk(90, 70))!, "same standing, higher risk, lower overall");
  assert.ok(overallScore(0.9, 0.5)! > overallScore(0.5, 0.5)!, "same risk, lower standing, lower overall");
});

test("identical histories score identically, so no difference is manufactured", () => {
  const history = series(alternating(365, 0.03));
  const first = measurePriceHistory(history, "coingecko", NOW);
  const second = measurePriceHistory(history, "coinbase", NOW);
  assert.equal(priceRiskScore(first), priceRiskScore(second));
  assert.equal(overallScore(0.91, priceRiskScore(first)), overallScore(0.91, priceRiskScore(second)));
});

test("a score doesn't depend on which other assets are on the card", async () => {
  const history = { measure: async (asset: { symbol: string }) => (asset.symbol === "BTC" ? measured(44, 45) : measured(97, 68)) };
  const card = (asset: string) => ({ asset, coingeckoId: asset.toLowerCase(), marketCapRank: 1, qualityScore: 0.91 });
  const alone = await applyScorecardMethodology([card("BTC")], [], history);
  const together = await applyScorecardMethodology([card("PEPE"), card("BTC")], [], history);
  assert.equal(alone.scorecards[0].overallScore, together.scorecards.find((entry) => entry.asset === "BTC")!.overallScore);
  assert.equal(together.scorecards[0].asset, "BTC", "sorted by overall score");
});

test("scoring boundaries: fixed ceilings clamp, and the score stays within 0 to 1", () => {
  assert.equal(priceRiskScore(measured(0, 0)), 0);
  assert.equal(priceRiskScore(measured(150, 90)), 1);
  assert.equal(priceRiskScore(measured(400, 99)), 1, "beyond both ceilings");
  assert.equal(priceRiskScore(measured(150, 0)), 0.6);
  assert.equal(priceRiskScore(measured(0, 90)), 0.4);
  assert.equal(overallScore(1, 0), 1);
  assert.equal(overallScore(0, 1), 0);
  assert.equal(overallScore(1, 1), 0.28);
});

test("too little, stale, or gappy history gets no score rather than an estimate", () => {
  const short = measurePriceHistory(series(Array(MIN_CLOSES - 1).fill(100)), "coingecko", NOW);
  assert.equal(short.status, "insufficient");
  assert.equal(priceRiskScore(short), null);
  assert.equal(overallScore(0.9, priceRiskScore(short)), null);

  assert.equal(measurePriceHistory(series(Array(MIN_CLOSES).fill(100)), "coingecko", NOW).status, "partial");
  assert.equal(measurePriceHistory(series(Array(359).fill(100)), "coingecko", NOW).status, "partial");
  assert.equal(measurePriceHistory(series(Array(360).fill(100)), "coingecko", NOW).status, "measured");

  assert.equal(measurePriceHistory(series(Array(365).fill(100), TODAY - 3 * DAY), "coingecko", NOW).status, "insufficient", "stale");
  assert.equal(measurePriceHistory(series(Array(365).fill(100), TODAY - 2 * DAY), "coingecko", NOW).status, "measured");

  const gappy = series(Array(365).fill(100)).filter((_, index) => index < 200 || index > 206);
  assert.equal(measurePriceHistory(gappy, "coingecko", NOW).status, "insufficient", "week-long gap");
  assert.equal(measurePriceHistory(series(Array(365).fill(100)).filter((_, index) => index !== 200), "coingecko", NOW).status, "measured", "one missing day is fine");

  assert.equal(measurePriceHistory([], "coingecko", NOW).status, "insufficient");
});

test("incomplete and invalid points are ignored", () => {
  const closes = series(Array(365).fill(100));
  const dirty = [
    ...closes,
    { timestamp: NOW, close: 1 },
    { timestamp: TODAY + DAY, close: 1 },
    { timestamp: TODAY - 400 * DAY, close: 0 },
    { timestamp: TODAY - 401 * DAY, close: Number.NaN },
    { timestamp: TODAY - 402 * DAY, close: -5 },
  ];
  assert.deepEqual(measurePriceHistory(dirty, "coingecko", NOW), measurePriceHistory(closes, "coingecko", NOW));
});

test("the provider parsers line up the same day and survive malformed payloads", () => {
  assert.deepEqual(parseCoinGeckoCloses({ prices: [[TODAY, 95], "bad"] }), [{ timestamp: TODAY, close: 95 }]);
  assert.deepEqual(parseCoinbaseCloses([[(TODAY - DAY) / 1000, 90, 97, 96, 95, 1000]]), [{ timestamp: TODAY, close: 95 }]);
  for (const payload of [null, {}, "x", { prices: "x" }, 7]) {
    assert.deepEqual(parseCoinGeckoCloses(payload), []);
    assert.deepEqual(parseCoinbaseCloses(payload), []);
  }
});

test("only a large, instantly reversed one-day print is treated as a feed error", () => {
  const base = Array(365).fill(100);
  const spike = [...base];
  spike[200] = 1000;
  const cleaned = measurePriceHistory(series(spike), "coingecko", NOW);
  if (cleaned.status !== "measured") return assert.fail("expected a measurement");
  assert.equal(cleaned.badPrintsExcluded, 1);
  assert.equal(cleaned.maxDrawdownPct, 0);

  // A real crash doesn't fully reverse the next day, so it stays in.
  const crash = base.map((value, index) => (index === 200 ? 40 : index > 200 ? 60 : value));
  const kept = measurePriceHistory(series(crash), "coingecko", NOW);
  if (kept.status !== "measured") return assert.fail("expected a measurement");
  assert.equal(kept.badPrintsExcluded, 0);
  assert.equal(kept.maxDrawdownPct, 60);

  // An 8% reversal is ordinary market noise.
  const small = [...base];
  small[200] = 108;
  const normal = measurePriceHistory(series(small), "coingecko", NOW);
  if (normal.status !== "measured") return assert.fail("expected a measurement");
  assert.equal(normal.badPrintsExcluded, 0);
});

test("a feed that flips between prices for weeks isn't scored", () => {
  // Shaped like CoinGecko's GALA series in March 2026: it jumps between two levels, days apart.
  const closes = Array.from({ length: 365 }, (_, index) => (index > 200 && index < 240 && Math.floor(index / 3) % 2 === 0 ? 200 : 100));
  const flipping = measurePriceHistory(series(closes), "coingecko", NOW);
  assert.equal(flipping.status, "insufficient");
  assert.equal(priceRiskScore(flipping), null);
  assert.ok(flipping.status === "insufficient" && flipping.reason.includes("too erratic"));

  // Four real extreme days stay measured.
  const wild = Array.from({ length: 365 }, (_, index) => 100 * 2 ** [50, 100, 150, 200].filter((day) => index >= day).length);
  const kept = measurePriceHistory(series(wild), "coingecko", NOW);
  assert.equal(kept.status, "measured");
});

function feed(handler: (url: string) => unknown) {
  const calls: string[] = [];
  const slept: number[] = [];
  const service = new PriceHistoryService({
    fetchJson: async (url) => {
      calls.push(url);
      return handler(url);
    },
    sleep: async (ms) => {
      slept.push(ms);
    },
    now: () => NOW,
  });
  return { service, calls, slept };
}

const geckoPayload = { prices: series(alternating(365, 0.02)).map((row) => [row.timestamp, row.close]) };
const candlePayload = (url: string) => {
  const start = Date.parse(new URL(url).searchParams.get("start")!);
  return Array.from({ length: 300 }, (_, index) => [(start + index * DAY) / 1000, 0, 0, 0, 100 + (index % 2), 0]);
};
const BTC = { coingeckoId: "bitcoin", symbol: "BTC", marketCapRank: 1 };

test("reruns on the same UTC day reuse the measurement and make no new requests", async () => {
  const { service, calls } = feed(() => geckoPayload);
  const first = await service.measure(BTC);
  const second = await service.measure(BTC);
  assert.deepEqual(first, second);
  assert.equal(calls.length, 1);
});

test("requests run one at a time, however many arrive together", async () => {
  let active = 0;
  let peak = 0;
  const service = new PriceHistoryService({
    fetchJson: async () => {
      active += 1;
      peak = Math.max(peak, active);
      await new Promise((resolve) => setTimeout(resolve, 2));
      active -= 1;
      return geckoPayload;
    },
    sleep: async () => undefined,
    now: () => NOW,
  });
  await Promise.all(["a", "b", "c"].map((id) => service.measure({ coingeckoId: id, symbol: id, marketCapRank: 1 })));
  assert.equal(peak, 1);
});

test("a rate limit is retried, then falls back to the exchange feed, and later assets don't wait again", async () => {
  const { service, calls, slept } = feed((url) => {
    if (url.includes("coingecko")) throw new Error("HTTP 429");
    return candlePayload(url);
  });
  const btc = await service.measure(BTC);
  assert.equal(btc.status, "measured");
  assert.equal(btc.status === "measured" && btc.source, "coinbase");
  assert.deepEqual(slept, [8000, 16000]);
  assert.equal(calls.filter((url) => url.includes("coingecko")).length, 3);
  assert.equal(calls.filter((url) => url.includes("coinbase")).length, 2);

  await service.measure({ coingeckoId: "ethereum", symbol: "ETH", marketCapRank: 2 });
  assert.deepEqual(slept, [8000, 16000], "the second asset doesn't back off again");
});

test("the symbol fallback isn't trusted for small or unranked assets", async () => {
  const { service, calls } = feed(() => {
    throw new Error("HTTP 500");
  });
  const small = await service.measure({ coingeckoId: "tiny", symbol: "BTC", marketCapRank: 900 });
  const unranked = await service.measure({ coingeckoId: "tiny2", symbol: "BTC", marketCapRank: null });
  assert.equal(small.status, "unavailable");
  assert.equal(unranked.status, "unavailable");
  assert.ok(calls.every((url) => url.includes("coingecko")));
});

test("if every feed is down the paid run fails so the payer can retry, and failures aren't cached", async () => {
  let down = true;
  const { service } = feed(() => {
    if (down) throw new Error("HTTP 503");
    return geckoPayload;
  });
  const card = { asset: "BTC", coingeckoId: "bitcoin", marketCapRank: 1, qualityScore: 0.91 };
  await assert.rejects(() => applyScorecardMethodology([card], [], service), /temporarily unavailable/);
  down = false;
  const { scorecards: [retried] } = await applyScorecardMethodology([card], [], service);
  assert.equal(retried.priceHistory.status, "measured");
});

test("a missing asset doesn't sink the others, and sorts last without a score", async () => {
  const history = { measure: async (asset: { symbol: string }): Promise<PriceHistoryMeasure> => (asset.symbol === "NEW" ? { status: "insufficient", source: "coingecko", closes: 40, reason: "Only 40 daily closes were available, and 180 are needed." } : measured(44, 45)) };
  const { scorecards: cards } = await applyScorecardMethodology(
    [{ asset: "NEW", coingeckoId: "new", marketCapRank: 90, qualityScore: 0.99 }, { asset: "BTC", coingeckoId: "bitcoin", marketCapRank: 1, qualityScore: 0.5 }],
    [],
    history,
  );
  assert.deepEqual(cards.map((card) => card.asset), ["BTC", "NEW"]);
  assert.equal(cards[1].overallScore, null);
  assert.equal(cards[1].priceRiskScore, null);
});

test("an asset outside the segment's screen is measured by ID, without an overall score", async () => {
  const seen: unknown[] = [];
  const history = {
    measure: async (asset: { coingeckoId: string; marketCapRank: number | null }) => {
      seen.push(asset);
      return measured(65, 62);
    },
  };
  const { scorecards, unscreenedAssets } = await applyScorecardMethodology([], [{ asset: "DOGE", name: "Dogecoin", coingeckoId: "dogecoin" }], history);
  assert.equal(scorecards.length, 0);
  assert.equal(unscreenedAssets[0].priceRiskScore, priceRiskScore(measured(65, 62)));
  assert.equal("overallScore" in unscreenedAssets[0], false);
  assert.deepEqual(seen, [{ coingeckoId: "dogecoin", symbol: "DOGE", marketCapRank: null }], "no rank, so no symbol lookup");

  const down = { measure: async (): Promise<PriceHistoryMeasure> => ({ status: "unavailable", reason: "down" }) };
  await assert.rejects(() => applyScorecardMethodology([], [{ asset: "DOGE", name: "Dogecoin", coingeckoId: "dogecoin" }], down), /temporarily unavailable/);
  const none = await applyScorecardMethodology([], [], down);
  assert.deepEqual(none, { scorecards: [], unscreenedAssets: [] }, "nothing recognized is not an outage");
});

test("a requested asset is only listed as outside the screen when it really is", () => {
  const scored = [{ asset: "BTC", coingeckoId: "bitcoin" }];
  const symbols = (requested: string[]) => findUnscreenedAssets(requested, scored, resolveAsset).map((entry) => entry.asset);
  assert.deepEqual(symbols(["BTC", "btc", "bitcoin"]), [], "matched by symbol or ID");
  assert.deepEqual(symbols(["DOGE", "doge", "dogecoin"]), ["DOGE"], "listed once");
  assert.deepEqual(symbols(["NOTACOIN"]), [], "never guessed");
});

function paid(result: Record<string, unknown>) {
  return {
    status: "completed",
    endpoint: "/agent/x402/asset-scorecard",
    decisionId: "d",
    payment: { verified: true, transactionHash: `0x${"c".repeat(64)}`, chargedAmountUsdc: "0.5", network: "eip155:84532", fromAddress: "0xabc" },
    result,
  };
}

const LEGACY_CARD = { asset: "BTC", name: "Bitcoin", role: "core", riskClass: "large_cap_crypto", selected: true, selectionReasons: ["lower_volatility_proxy"], qualityScore: 0.91, riskScore: 0.17, liquidityScore: 1, structuralScore: 0.94, compositeScore: 0.81 };

test("a result sold under the first method keeps its labels, notes, and coverage", () => {
  const document = presentResult("asset_scorecard", "Asset Scorecard", paid({ assetsRequested: ["BTC"], scorecards: [LEGACY_CARD] }))!;
  const serialized = JSON.stringify(document);
  assert.ok(serialized.includes("Risk (higher is riskier)"));
  assert.ok(serialized.includes("Structural stability"));
  assert.ok(serialized.includes("Lower volatility proxy"));
  assert.ok(serialized.includes("the largest assets can score the same"));
  assert.ok(!serialized.includes("Price history"), "it didn't use any");
  assert.ok(!serialized.includes(SCORECARD_METHODOLOGY_VERSION));
});

function v2Result(cards: unknown[]) {
  return paid({ assetsRequested: ["BTC"], scoringMethodology: SCORECARD_METHODOLOGY, qualifiedCandidatesCount: 37, screenedCandidatesCount: 300, scorecards: cards });
}

const V2_CARD = { ...LEGACY_CARD, tokenCategory: "core", overallScore: 0.66, priceRiskScore: 0.37, priceHistory: measured(43.8, 44.7) };

test("a new result shows measurements, drops the proxy claims, and names its method", () => {
  const serialized = JSON.stringify(presentResult("asset_scorecard", "Asset Scorecard", v2Result([V2_CARD]))!);
  for (const expected of ["Annualized volatility", "43.8%", "44.7%", "Largest drawdown", "66 / 100", "37 / 100", "Market standing", "Market structure", SCORECARD_METHODOLOGY_VERSION, "don't predict future prices", "Price history"]) {
    assert.ok(serialized.includes(expected), expected);
  }
  for (const removed of ["proxy", "Structural stability", "Risk class", "the largest assets can score the same", "81 / 100"]) {
    assert.ok(!serialized.toLowerCase().includes(removed.toLowerCase()), removed);
  }
});

test("a new result says plainly what it could not measure", () => {
  const noHistory = { ...V2_CARD, asset: "NEW", name: "New", overallScore: null, priceRiskScore: null, priceHistory: { status: "insufficient", source: "coingecko", closes: 40, reason: "Only 40 daily closes were available, and 180 are needed." } };
  const partial = { ...V2_CARD, asset: "YNG", name: "Young", priceHistory: { ...measured(80, 60), status: "partial", closes: 200 } };
  const stable = { ...V2_CARD, asset: "USDC", name: "USDC", tokenCategory: "stablecoin" };
  const serialized = JSON.stringify(presentResult("asset_scorecard", "Asset Scorecard", v2Result([noHistory, partial, stable]))!);
  assert.ok(serialized.includes("NEW has no overall score. Only 40 daily closes"));
  assert.ok(serialized.includes("Not scored"));
  assert.ok(serialized.includes("Not measured"));
  assert.ok(serialized.includes("YNG has under a year of price history"));
  assert.ok(serialized.includes("doesn't assess its reserves"));
});

test("assets outside the screen and unknown assets are each explained", () => {
  const response = paid({ assetsRequested: ["DOGE", "NOPE"], portfolioSegment: "Bluechips", scoringMethodology: SCORECARD_METHODOLOGY, scorecards: [], unscreenedAssets: [{ asset: "DOGE", name: "Dogecoin", coingeckoId: "dogecoin", priceRiskScore: 0.54, priceHistory: measured(65.2, 62) }] });
  const serialized = JSON.stringify(presentResult("asset_scorecard", "Asset Scorecard", response)!);
  for (const expected of ["Dogecoin (DOGE)", "65.2%", "54 / 100", "Not scored", "DOGE is outside the universe screened for the Bluechips segment", "NOPE could not be identified"]) {
    assert.ok(serialized.includes(expected), expected);
  }
});

test("a result carries the card's figures as numbers for comparing, and nothing it didn't measure", () => {
  const noHistory = { ...V2_CARD, asset: "NEW", name: "New", overallScore: null, priceRiskScore: null, priceHistory: { status: "insufficient", source: "coingecko", closes: 40, reason: "Too few closes." } };
  const response = paid({ ...v2Result([V2_CARD, noHistory]).result, unscreenedAssets: [{ asset: "DOGE", name: "Dogecoin", priceRiskScore: 0.54, priceHistory: measured(65.2, 62) }] });
  const { assets } = presentResult("asset_scorecard", "Asset Scorecard", response)!;
  assert.deepEqual(assets?.[0], {
    asset: "BTC", name: "Bitcoin", overallScore: 66, priceRisk: 37, marketStanding: 91, liquidity: 100, structure: 94,
    volatilityPct: 43.8, maxDrawdownPct: 44.7, belowHighPct: 10, category: "Large cap crypto", measuredThrough: "2026-11-05",
  });
  assert.equal(assets?.[1].overallScore, null, "no history, no overall score");
  assert.equal(assets?.[1].priceRisk, null);
  assert.equal(assets?.[1].volatilityPct, null);
  assert.deepEqual([assets?.[2].asset, assets?.[2].overallScore, assets?.[2].marketStanding, assets?.[2].priceRisk], ["DOGE", null, null, 54], "outside the screen: price risk only");

  const report = presentResult("comprehensive_report", "Comprehensive Intelligence Report", paid({ marketRegime: {}, policyEnvelope: {}, assetScorecard: v2Result([V2_CARD]).result }))!;
  assert.equal(report.assets?.[0].overallScore, 66, "a report's asset scores compare too");
  assert.equal(presentResult("asset_scorecard", "Asset Scorecard", paid({ assetsRequested: ["BTC"], scorecards: [LEGACY_CARD] }))!.assets, undefined, "the first method's scores aren't comparable");
});

test("the quote discloses the price history before anyone pays", () => {
  const spec = getToolSpec("asset_scorecard")!;
  assert.deepEqual(spec.dataCoverage.map((item) => [item.label, item.included]), [["Price history", true], ["Live prices", false], ["Current news", false], ["Real-time market data", false]]);
  assert.ok(spec.limitations?.includes("less than 180 days"));
  assert.ok(!spec.summary.toLowerCase().includes("structural stability"));
});

test("when the paid scorecard requires every asset, one feed outage is a failed run with a free retry", async () => {
  const eth = { asset: "ETH", coingeckoId: "ethereum", marketCapRank: 2, qualityScore: 0.5 };
  const up = { measure: async () => ({ status: "insufficient" as const, source: "coingecko" as const, closes: 10, reason: "short" }) };
  const mixed = { measure: async (asset: { symbol: string }) => (asset.symbol === "ETH" ? { status: "unavailable" as const, reason: "down" } : up.measure()) };
  await assert.rejects(() => applyScorecardMethodology([eth, { ...eth, asset: "BTC", coingeckoId: "bitcoin" }], [], mixed, { requireEveryAsset: true }), /temporarily unavailable/);
  const lenient = await applyScorecardMethodology([eth, { ...eth, asset: "BTC", coingeckoId: "bitcoin" }], [], mixed);
  assert.equal(lenient.scorecards.length, 2, "the comprehensive report's behavior is unchanged");
});

test("a purchase locked to a screening decision fails rather than delivering a card with no score", async () => {
  const card = { asset: "BTC", name: "Bitcoin", coingeckoId: "bitcoin", marketCapRank: 1, qualityScore: 0.9 };
  const thin = { measure: async () => ({ status: "insufficient" as const, source: "coingecko" as const, closes: 90, reason: "Too few." }) };
  await assert.rejects(() => applyScorecardMethodology([card], [], thin as never, { requireEveryAsset: true, requireOverallScore: true }), /no longer supports/);
  await assert.rejects(() => applyScorecardMethodology([], [], thin as never, { requireOverallScore: true }), /no longer supports/);
  const lenient = await applyScorecardMethodology([card], [], thin as never, { requireEveryAsset: true });
  assert.equal(lenient.scorecards[0].overallScore, null);
});

test("the window names the days its closes ended, not the stamps on them", () => {
  const history = measurePriceHistory(series(Array(365).fill(100)), "coingecko", NOW) as { windowStart: string; windowEnd: string };
  assert.equal(history.windowEnd, new Date(TODAY - DAY).toISOString().slice(0, 10));
  assert.equal(history.windowStart, new Date(TODAY - 365 * DAY).toISOString().slice(0, 10));
});
