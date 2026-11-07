import assert from "node:assert/strict";
import test from "node:test";
import { AssistantError, AssistantService } from "./assistant.service";
import { ScorecardPreflight, decideScreening, snapshotProblem, type ScreenCard, type ScreenProfile, type ScreenResult, type ScreeningSnapshot } from "./scorecard-preflight";

const BTC = "BTC";
const DOGE = "DOGE";
const PROFILE: ScreenProfile = { riskTolerance: "Balanced", timeframe: "1-3_years", portfolioSegment: "Bluechips" };
const SEGMENTS = ["Bluechips", "Memecoins", "Gaming", "Yield Farm"];
const MEASURED = { status: "measured" } as never;

const IDS: Record<string, string> = { BTC: "bitcoin", ETH: "ethereum", DOGE: "dogecoin" };

function cardOf(symbol: string, compositeScore = 0.5): ScreenCard {
  return { asset: symbol, name: symbol, coingeckoId: IDS[symbol] ?? symbol.toLowerCase(), marketCapRank: 1, tokenCategory: "core", role: "core", riskClass: "large_cap_crypto", selectionBucket: "core", selected: true, qualityScore: 0.9, riskScore: 0.1, liquidityScore: 1, structuralScore: 0.9, compositeScore, selectionReasons: [] };
}

function screenOf(...symbols: string[]): ScreenResult {
  return { screenedAt: "2026-11-07T12:00:00.000Z", counts: { screened: 60, qualified: 40, selected: 10 }, cards: symbols.map((symbol) => cardOf(symbol)) };
}

function preflight(bySegment: Record<string, string[]>, overrides: Partial<ConstructorParameters<typeof ScorecardPreflight>[0]> = {}) {
  const runs: string[] = [];
  const instance = new ScorecardPreflight({
    runScreen: async (profile) => {
      runs.push(profile.portfolioSegment);
      return screenOf(...(bySegment[profile.portfolioSegment] ?? []));
    },
    measure: async () => MEASURED,
    segments: SEGMENTS,
    ...overrides,
  });
  return { instance, runs };
}

test("an asset in the screen with good price history can be offered", async () => {
  const { instance } = preflight({ Bluechips: ["BTC"] });
  const result = await instance.check([BTC], PROFILE);
  assert.equal(result.deliverable, true);
  assert.equal(result.assets[0].outcome, "eligible");
});

test("an asset outside the segment isn't offered, and the segment that scores it is named", async () => {
  const { instance } = preflight({ Bluechips: ["BTC"], Memecoins: ["DOGE"] });
  const result = await instance.check([DOGE], PROFILE);
  assert.equal(result.deliverable, false);
  assert.equal(result.assets[0].outcome, "outside_segment");
  assert.equal(result.assets[0].eligibleSegment, "Memecoins");
});

test("no segment is suggested when none scores the asset", async () => {
  const { instance } = preflight({ Bluechips: ["BTC"] });
  const result = await instance.check([DOGE], PROFILE);
  assert.equal(result.assets[0].eligibleSegment, undefined);
});

test("too little price history stops the offer, with the reason", async () => {
  const { instance } = preflight({ Bluechips: ["BTC"] }, { measure: async () => ({ status: "insufficient", source: "coingecko", closes: 90, reason: "Only 90 daily closes were available, and 180 are needed." }) });
  const result = await instance.check([BTC], PROFILE);
  assert.equal(result.deliverable, false);
  assert.equal(result.assets[0].outcome, "insufficient_history");
  assert.match(result.assets[0].reason ?? "", /180/);
});

test("anything that can't be confirmed withholds the offer", async () => {
  const feedDown = preflight({ Bluechips: ["BTC"] }, { measure: async () => ({ status: "unavailable", reason: "down" }) });
  assert.equal((await feedDown.instance.check([BTC], PROFILE)).assets[0].outcome, "check_unavailable");
  const feedThrows = preflight({ Bluechips: ["BTC"] }, { measure: async () => { throw new Error("boom"); } });
  assert.equal((await feedThrows.instance.check([BTC], PROFILE)).deliverable, false);
  const screenFails = preflight({}, { runScreen: async () => { throw new Error("boom"); } });
  const failed = await screenFails.instance.check([BTC], PROFILE);
  assert.equal(failed.deliverable, false);
  assert.equal(failed.assets[0].outcome, "check_unavailable");
});

test("a slow screen withholds the offer, then answers the next customer without running twice", async () => {
  let release: (result: ScreenResult) => void = () => undefined;
  let runs = 0;
  const instance = new ScorecardPreflight({
    runScreen: () => {
      runs += 1;
      return new Promise((resolve) => { release = resolve; });
    },
    measure: async () => MEASURED,
    segments: SEGMENTS,
    waitMs: 5,
  });
  assert.equal((await instance.check([BTC], PROFILE)).assets[0].outcome, "check_unavailable");
  release(screenOf("BTC"));
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal((await instance.check([BTC], PROFILE)).deliverable, true);
  assert.equal(runs, 1);
});

test("screens are cached per profile, run one at a time, expire, and a failure isn't kept", async () => {
  let time = 0;
  let running = 0;
  let most = 0;
  const { instance, runs } = preflight({}, { now: () => time, ttlMs: 100 });
  const counted = new ScorecardPreflight({
    runScreen: async () => {
      running += 1;
      most = Math.max(most, running);
      await new Promise((resolve) => setTimeout(resolve, 2));
      running -= 1;
      return screenOf("BTC");
    },
    measure: async () => MEASURED,
    segments: SEGMENTS,
  });
  await Promise.all(SEGMENTS.map((portfolioSegment) => counted.warm({ ...PROFILE, portfolioSegment })));
  assert.equal(most, 1, "one at a time");

  await instance.warm(PROFILE);
  await instance.warm(PROFILE);
  assert.equal(runs.length, 1, "cached");
  await instance.warm({ ...PROFILE, riskTolerance: "Conservative" });
  assert.equal(runs.length, 2, "another profile is its own screen");
  time = 101;
  await instance.warm(PROFILE);
  assert.equal(runs.length, 3, "expired");

  let attempts = 0;
  const flaky = new ScorecardPreflight({
    runScreen: async () => {
      attempts += 1;
      if (attempts === 1) throw new Error("boom");
      return screenOf("BTC");
    },
    measure: async () => MEASURED,
    segments: SEGMENTS,
  });
  await flaky.warm(PROFILE).catch(() => undefined);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal((await flaky.check([BTC], PROFILE)).deliverable, true);
});

const CATALOG = { resources: [
  { productId: "asset_scorecard", endpoint: "/agent/x402/asset-scorecard", pricing: { amountUsdc: "0.5" } },
  { productId: "market_regime", endpoint: "/agent/x402/market-regime", pricing: { amountUsdc: "0.25" } },
] };
const call = (name: string, args: unknown) => ({ id: `call_${name}`, type: "function" as const, function: { name, arguments: JSON.stringify(args) } });

function service(check: ScorecardPreflight["check"], script: { content: string | null; toolCalls: ReturnType<typeof call>[] }[]) {
  const toolResults: string[] = [];
  const challenges: string[] = [];
  const instance = new AssistantService({
    llm: {
      complete: async (request) => {
        toolResults.push(...request.messages.filter((message) => message.role === "tool").map((message) => message.content as string));
        return script.shift() ?? { content: "Done.", toolCalls: [] };
      },
    },
    gateway: {
      requestChallenge: async (endpoint: string) => {
        challenges.push(endpoint);
        return { kind: "challenge", paymentRequired: { x402Version: 2, resource: { url: endpoint, description: "", mimeType: "application/json" }, accepts: [{ scheme: "exact", network: "eip155:8453", amount: "500000", asset: "0xusdc", payTo: "0xseller", maxTimeoutSeconds: 60, extra: {} }] } as never };
      },
      submitPaid: async () => { throw new Error("Nothing paid runs here."); },
    },
    loadCatalog: async () => CATALOG as never,
    preflight: { check },
  });
  return { instance, toolResults, challenges };
}

test("a deliverable check is bound to exactly what was asked, and holds only those assets", async () => {
  const { instance } = preflight({ Bluechips: ["BTC", "ETH", "DOGE"] });
  const { snapshot, deliverable } = await instance.check(["ETH", "bitcoin"], PROFILE);
  assert.equal(deliverable, true);
  assert.deepEqual(snapshot?.cards.map((card) => card.asset).sort(), ["BTC", "ETH"]);
  assert.deepEqual(snapshot?.universe, ["BTC", "ETH", "DOGE"]);
  assert.deepEqual(snapshot?.profile, PROFILE);
  assert.equal(snapshot?.methodologyVersion, "asset-scorecard-v2");
  assert.equal(snapshot?.screenedAt, "2026-11-07T12:00:00.000Z");
});

test("every asset in a request must qualify, and one that doesn't produces no snapshot", async () => {
  const { instance } = preflight({ Bluechips: ["BTC"], Memecoins: ["DOGE"] });
  const result = await instance.check(["BTC", "DOGE"], PROFILE);
  assert.equal(result.deliverable, false);
  assert.equal(result.snapshot, null);
  assert.deepEqual(result.assets.map((asset) => asset.outcome), ["eligible", "outside_segment"]);
});

test("an asset nobody recognizes is refused, not silently dropped from what was paid for", async () => {
  const { instance } = preflight({ Bluechips: ["BTC"] });
  const result = await instance.check(["BTC", "NOTACOIN"], PROFILE);
  assert.equal(result.deliverable, false);
  assert.equal(result.assets[1].outcome, "unrecognized");
});

test("an asset the screen scores can be bought even if the chat's asset list doesn't know it", async () => {
  const { instance } = preflight({ Gaming: ["AXS"] });
  const result = await instance.check(["axs"], { ...PROFILE, portfolioSegment: "Gaming" });
  assert.equal(result.deliverable, true);
});

test("a public request doesn't start extra screens looking for another segment", async () => {
  const { instance, runs } = preflight({ Bluechips: ["BTC"], Memecoins: ["DOGE"] });
  const result = await instance.check([DOGE], PROFILE, { cachedAlternativesOnly: true });
  assert.deepEqual(runs, ["Bluechips"]);
  assert.equal(result.assets[0].eligibleSegment, undefined);
  await instance.warm({ ...PROFILE, portfolioSegment: "Memecoins" });
  assert.equal((await instance.check([DOGE], PROFILE, { cachedAlternativesOnly: true })).assets[0].eligibleSegment, "Memecoins");
});

test("naming no assets locks the top assets, leaving out any that can't score", async () => {
  const { instance } = preflight({ Bluechips: ["BTC", "ETH"] }, {
    measure: async (asset) => (asset.symbol === "ETH" ? { status: "insufficient", source: "coingecko", closes: 90, reason: "Too few." } : MEASURED),
  });
  const { deliverable, snapshot } = await instance.check([], PROFILE);
  assert.equal(deliverable, true);
  assert.deepEqual(snapshot?.cards.map((card) => card.asset), ["BTC"]);
});

async function snapshotFor(overrides: Partial<ConstructorParameters<typeof ScorecardPreflight>[0]> = {}): Promise<ScreeningSnapshot> {
  const { snapshot } = await preflight({ Bluechips: ["BTC"] }, overrides).instance.check([BTC], PROFILE);
  return snapshot!;
}

test("a snapshot only fits the purchase it was made for, and can't be altered", async () => {
  const snapshot = await snapshotFor();
  const purchase = { assets: ["BTC"], profile: PROFILE };
  assert.equal(snapshotProblem(snapshot, purchase), null);
  assert.equal(snapshotProblem(snapshot, { assets: ["BTC", "ETH"], profile: PROFILE }), "different_purchase");
  for (const change of [{ riskTolerance: "Aggressive" }, { timeframe: "<1_year" }, { portfolioSegment: "Memecoins" }]) {
    assert.equal(snapshotProblem(snapshot, { assets: ["BTC"], profile: { ...PROFILE, ...change } }), "different_purchase");
  }
  assert.equal(snapshotProblem({ ...snapshot, universe: [...snapshot.universe, "DOGE"] }, purchase), "altered");
  assert.equal(snapshotProblem({ ...snapshot, cards: [] }, purchase), "altered");
});

test("a quote expires for payment, but a paid snapshot is good at any age", async () => {
  const snapshot = await snapshotFor({ now: () => Date.parse("2026-11-07T12:00:00.000Z"), snapshotValidMs: 1000 });
  const purchase = { assets: ["BTC"], profile: PROFILE };
  assert.equal(snapshotProblem(snapshot, purchase, Date.parse("2026-11-07T12:00:00.500Z")), null);
  assert.equal(snapshotProblem(snapshot, purchase, Date.parse("2026-11-07T12:00:01.000Z")), "expired");
  assert.equal(snapshotProblem(snapshot, purchase), null);
});

const SCREEN_INPUT = { ...PROFILE, assets: ["BTC"] };
const unusedCheck = async () => { throw new Error("Eligibility must not be decided again."); };

test("the route never re-decides eligibility once a payment is on record", async () => {
  const snapshot = await snapshotFor();
  const decided = await decideScreening({ existing: { payment: { transactionHash: "0xabc" }, screeningSnapshot: snapshot }, input: SCREEN_INPUT, paying: true, preflight: { check: unusedCheck } });
  assert.equal(decided.snapshot, snapshot);
  assert.equal(decided.refusal, null);
});

test("the route refuses a direct API buyer before payment, whether probing or paying", async () => {
  const { instance } = preflight({ Bluechips: ["BTC"], Memecoins: ["DOGE"] });
  for (const paying of [false, true]) {
    const decided = await decideScreening({ existing: undefined, input: { ...PROFILE, assets: ["DOGE"] }, paying, preflight: instance });
    assert.equal(decided.snapshot, null);
    assert.equal(decided.refusal?.assets[0].outcome, "outside_segment");
  }
});

test("a discovery probe naming nothing costs no screen, but paying for that does", async () => {
  const { instance, runs } = preflight({ Bluechips: ["BTC"] });
  assert.deepEqual(await decideScreening({ existing: undefined, input: { ...PROFILE, assets: [] }, paying: false, preflight: instance }), { snapshot: null, refusal: null });
  assert.deepEqual(runs, []);
  const paying = await decideScreening({ existing: undefined, input: { ...PROFILE, assets: [] }, paying: true, preflight: instance });
  assert.deepEqual(paying.snapshot?.cards.map((card) => card.asset), ["BTC"]);
});

test("the route keeps a current quote's snapshot, and checks again once it expires", async () => {
  const snapshot = await snapshotFor();
  const kept = await decideScreening({ existing: { screeningSnapshot: snapshot }, input: SCREEN_INPUT, paying: true, preflight: { check: unusedCheck } });
  assert.equal(kept.snapshot, snapshot);
  const { instance } = preflight({});
  const later = await decideScreening({ existing: { screeningSnapshot: snapshot }, input: SCREEN_INPUT, paying: true, preflight: instance, now: Date.parse(snapshot.validUntil) });
  assert.equal(later.snapshot, null);
  assert.equal(later.refusal?.assets[0].outcome, "outside_segment");
});

test("no quote card is shown for an asset that can't get a score, and the model is told why", async () => {
  const { instance: check } = preflight({ Bluechips: ["BTC"], Memecoins: ["DOGE"] });
  const { instance, toolResults } = service(check.check.bind(check), [{ content: null, toolCalls: [call("propose_asset_scorecard", { assets: ["DOGE"] })] }, { content: "Dogecoin isn't in Bluechips.", toolCalls: [] }]);
  const reply = await instance.chat({ message: "Analyze Dogecoin" });
  assert.equal(reply.blocks.filter((block) => block.type === "action").length, 0);
  const told = toolResults.find((result) => result.includes("not_offered")) ?? "";
  assert.match(told, /"eligibleSegment":"Memecoins"/);
  assert.match(told, /Never offer a price-only report/);
});

test("the segment the customer agreed to is the one checked and quoted", async () => {
  const { instance: check } = preflight({ Bluechips: ["BTC"], Memecoins: ["DOGE"] });
  const { instance } = service(check.check.bind(check), [{ content: null, toolCalls: [call("propose_asset_scorecard", { assets: ["DOGE"], portfolioSegment: "Memecoins" })] }, { content: "Here it is.", toolCalls: [] }]);
  const reply = await instance.chat({ message: "Yes, use Memecoins for Dogecoin" });
  const card = reply.blocks.find((block) => block.type === "action");
  assert.ok(card?.type === "action");
  assert.equal(card.action.eligibilityChecked, true);
  assert.ok(card.action.details.some((detail) => detail.label === "Segment" && detail.value === "Memecoins"));
});

test("the market report is still offered when the named asset's scorecard isn't", async () => {
  const { instance: check } = preflight({ Bluechips: ["BTC"] });
  const { instance, toolResults } = service(check.check.bind(check), [{ content: null, toolCalls: [call("propose_market_regime", {})] }, { content: "Here's the market report.", toolCalls: [] }]);
  const reply = await instance.chat({ message: "Give me a Dogecoin market update" });
  const cards = reply.blocks.flatMap((block) => (block.type === "action" ? [block.action.capabilityId] : []));
  assert.deepEqual(cards, ["market_regime"]);
  assert.ok(toolResults.some((result) => result.includes("not_offered")));
});

test("payment isn't requested if a quote stopped being deliverable, and nothing is charged", async () => {
  let eligible = true;
  const check: ScorecardPreflight["check"] = async () => ({ deliverable: eligible, assets: [{ symbol: "BTC", name: "Bitcoin", outcome: eligible ? "eligible" : "outside_segment" }] });
  const { instance, challenges } = service(check, [{ content: null, toolCalls: [call("propose_asset_scorecard", { assets: ["BTC"] })] }, { content: "Here it is.", toolCalls: [] }]);
  const reply = await instance.chat({ message: "Analyze Bitcoin" });
  const card = reply.blocks.find((block) => block.type === "action");
  assert.ok(card?.type === "action");
  eligible = false;
  await assert.rejects(instance.prepareApproval(reply.conversationId, card.action.id), (error: unknown) => error instanceof AssistantError && error.code === "scorecard_unavailable" && /nothing was charged/.test(error.message));
  assert.deepEqual(challenges, []);
  eligible = true;
  await instance.prepareApproval(reply.conversationId, card.action.id);
  assert.equal(challenges.length, 1);
});

const PAYPAL_PRICES = { reportUsd: "4.99", subscriptionUsd: "9.99" };

function paypalService(check: ScorecardPreflight["check"], script: { content: string | null; toolCalls: ReturnType<typeof call>[] }[]) {
  const checkouts: { screening?: ScreeningSnapshot | null }[] = [];
  const subscriptions: { screening?: ScreeningSnapshot | null }[] = [];
  const instance = new AssistantService({
    llm: { complete: async () => script.shift() ?? { content: "Done.", toolCalls: [] } },
    gateway: { requestChallenge: async () => { throw new Error("PayPal products don't use the wallet route."); }, submitPaid: async () => { throw new Error("Nothing paid runs here."); } },
    loadCatalog: async () => CATALOG as never,
    preflight: { check },
    paypal: {
      prices: PAYPAL_PRICES,
      createCheckout: async (input) => { checkouts.push(input); return { purchaseId: "pp-1", approveUrl: "https://paypal.test/approve" }; },
      createSubscription: async (input) => { subscriptions.push(input); return { subscriptionLocalId: "ps-1", approveUrl: "https://paypal.test/sub" }; },
    },
  });
  return { instance, checkouts, subscriptions };
}

for (const tool of ["propose_comprehensive_report", "propose_monthly_intelligence"]) {
  test(`${tool}: no card unless every named asset can get a score`, async () => {
    const { instance: check } = preflight({ Bluechips: ["BTC"], Memecoins: ["DOGE"] });
    const { instance } = paypalService(check.check.bind(check), [{ content: null, toolCalls: [call(tool, { assets: ["BTC", "DOGE"] })] }, { content: "Dogecoin isn't in Bluechips.", toolCalls: [] }]);
    const reply = await instance.chat({ message: "Full report on Bitcoin and Dogecoin" });
    assert.equal(reply.blocks.filter((block) => block.type === "action").length, 0);
  });
}

async function quotedReport(check: ScorecardPreflight["check"], tool = "propose_comprehensive_report") {
  const service = paypalService(check, [{ content: null, toolCalls: [call(tool, { assets: ["BTC"] })] }, { content: "Here it is.", toolCalls: [] }]);
  const reply = await service.instance.chat({ message: "Full report on Bitcoin" });
  const card = reply.blocks.find((block) => block.type === "action");
  assert.ok(card?.type === "action");
  assert.equal(card.action.eligibilityChecked, true);
  const approve = () => service.instance.startPayPalCheckout(reply.conversationId, card.action.id, { approved: true, approvedPrice: card.action.priceUsdc });
  return { ...service, approve };
}

test("PayPal checkout carries the snapshot its quote was checked against", async () => {
  const { instance: check, runs } = preflight({ Bluechips: ["BTC"] });
  const { approve, checkouts } = await quotedReport(check.check.bind(check));
  await approve();
  assert.equal(checkouts[0].screening?.cards[0].asset, "BTC");
  assert.deepEqual(checkouts[0].screening?.assets, ["BTC"]);
  assert.equal(runs.length, 1, "the cached screen answers both");
});

test("a subscription checkout carries one too", async () => {
  const { instance: check } = preflight({ Bluechips: ["BTC"] });
  const { approve, subscriptions } = await quotedReport(check.check.bind(check), "propose_monthly_intelligence");
  await approve();
  assert.equal(subscriptions[0].screening?.cards[0].asset, "BTC");
});

test("an expired report quote is checked again, and stops before PayPal if it no longer qualifies", async () => {
  let eligible = true;
  const expired = { ...(await snapshotFor()), validUntil: "2000-01-01T00:00:00.000Z" };
  const check: ScorecardPreflight["check"] = async () => ({ deliverable: eligible, assets: [{ symbol: "BTC", name: "Bitcoin", outcome: eligible ? "eligible" : "outside_segment" }], snapshot: eligible ? expired : null });
  const { approve, checkouts } = await quotedReport(check);
  eligible = false;
  await assert.rejects(approve(), (error: unknown) => error instanceof AssistantError && error.code === "scorecard_unavailable" && /nothing was charged/.test(error.message));
  assert.equal(checkouts.length, 0);
});

test("the model is told a report is PayPal in US dollars, not a wallet purchase in USDC", async () => {
  const { instance: check } = preflight({ Bluechips: ["BTC"] });
  const told: string[] = [];
  const script = [{ content: null, toolCalls: [call("propose_comprehensive_report", { assets: ["BTC"] })] }, { content: "Here it is.", toolCalls: [] }];
  const instance = new AssistantService({
    llm: { complete: async (request) => { told.push(...request.messages.filter((message) => message.role === "tool").map((message) => message.content as string)); return script.shift() ?? { content: "Done.", toolCalls: [] }; } },
    gateway: { requestChallenge: async () => { throw new Error("unused"); }, submitPaid: async () => { throw new Error("unused"); } },
    loadCatalog: async () => CATALOG as never,
    preflight: { check: check.check.bind(check) },
    paypal: { prices: PAYPAL_PRICES, createCheckout: async () => { throw new Error("unused"); }, createSubscription: async () => { throw new Error("unused"); } },
  });
  await instance.chat({ message: "Full report on Bitcoin" });
  const quote = told.find((result) => result.includes("quote_shown_to_customer")) ?? "";
  assert.match(quote, /"price_usd":"4.99"/);
  assert.match(quote, /through PayPal/);
  assert.doesNotMatch(quote, /price_usdc/);
});
