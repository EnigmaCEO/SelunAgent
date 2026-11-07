import { createHash } from "node:crypto";
import { ScorecardPreflight, type ScreeningSnapshot } from "./scorecard-preflight";
import { ScoreUnavailableError, UndeliverableError } from "../services/fulfillment-errors";
import { withSandboxFaults } from "./sandbox-faults";
import type { CheckoutInput, CheckoutProvider } from "./commerce";

import assert from "node:assert/strict";
import fs from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import express from "express";
import { issuePurchaseToken, issueReportToken, readPurchaseToken } from "../services/job-access.service";
import { createAssistantRouter } from "./assistant.routes";
import { AssistantError, AssistantService } from "./assistant.service";
import { buildRegistry, getToolSpec } from "./capability-registry";
import { COMPREHENSIVE_REPORT_ID } from "./commerce";
import { PayPalApiError, parseOrder, readPayPalConfig, type PayPalApi, type PayPalOrder, type PayPalSaleTransaction, type PayPalSubscription, type WebhookHeaders } from "./paypal-client";
import { PayPalCommerceService, purchaseIdFor, type CoverageNotice, type PayPalCommerceDeps } from "./paypal-commerce.service";
import { renderResultPdf, resultRecordHash } from "./result-report";
import { presentResult } from "./result-presenter";
import { ReportService } from "./report.service";

process.env.SELUN_REFERRAL_INTERNAL_TOKEN = "test-paypal-secret";

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "selun-paypal-"));
test.after(() => fs.rmSync(dir, { recursive: true, force: true }));
let fileCounter = 0;
const stateFile = () => path.join(dir, `state-${(fileCounter += 1)}.json`);

const DECISION = "selun-ai-11111111-1111-4111-8111-111111111111";
const INPUT = { riskTolerance: "Balanced", timeframe: "1-3_years", portfolioSegment: "Bluechips", assets: ["BTC"] };
const HEADERS: WebhookHeaders = { authAlgo: "a", certUrl: "c", transmissionId: "t", transmissionSig: "s", transmissionTime: "n" };

const RESULT = {
  snapshotAt: "2026-11-05T22:04:00.000Z",
  marketRegime: { snapshotAt: "2026-11-05T22:04:00.000Z", regime: "balanced", marketCondition: { volatility_state: "moderate", liquidity_state: "stable", risk_appetite: "neutral" }, authorization: { status: "AUTHORIZED" }, sourceSummary: { sourceCount: 4, missingDomains: [] } },
  policyEnvelope: { snapshotAt: "2026-11-05T22:04:00.000Z", policyEnvelope: { risk_budget: 0.5, stablecoin_minimum: 0.2, exposure_caps: { max_single_asset_exposure: 0.3, high_volatility_asset_cap: 0.1 }, portfolio_volatility_target: 0.4, liquidity_floor_requirement: "high" } },
  assetScorecard: {
    snapshotAt: "2026-11-05T22:04:00.000Z",
    assetsRequested: ["BTC"],
    portfolioSegment: "Bluechips",
    scoringMethodology: { version: "asset-scorecard-v2" },
    qualifiedCandidatesCount: 40,
    screenedCandidatesCount: 300,
    scorecards: [{ asset: "BTC", name: "Bitcoin", role: "core", riskClass: "large_cap_crypto", selected: true, qualityScore: 0.911, liquidityScore: 1, structuralScore: 0.938, overallScore: 0.666, priceRiskScore: 0.374, tokenCategory: "core", priceHistory: { status: "measured", source: "coingecko", closes: 365, windowStart: "2025-11-06", windowEnd: "2026-11-05", annualizedVolatilityPct: 43.8, maxDrawdownPct: 44.7, currentDrawdownPct: 24.1 } }],
  },
};

/** A PayPal that behaves as documented, with knobs for the awkward cases. */
class FakePayPal implements PayPalApi {
  orders = new Map<string, PayPalOrder>();
  captureStatus = "COMPLETED";
  captureCalls: { orderId: string; requestId: string }[] = [];
  refunds: { id: string; requestId: string }[] = [];
  remoteCapture = new Map<string, string>();
  refundFails = false;
  verified = true;
  verifyCalls = 0;
  plans: string[] = [];
  subscriptions = new Map<string, PayPalSubscription>();
  sales: PayPalSaleTransaction[] = [];
  cancelled: string[] = [];
  getCaptureFails = false;
  private next = 0;

  async createOrder(input: { purchaseId: string; amount: string }) {
    const orderId = `ORDER-${(this.next += 1)}`;
    this.orders.set(orderId, { orderId, status: "CREATED", customId: input.purchaseId, amount: input.amount, currency: "USD", payerId: null, captureId: null, captureStatus: null, captureAmount: null, captureCurrency: null });
    return { orderId, approveUrl: `https://sandbox.paypal.test/approve/${orderId}` };
  }
  approve(orderId = "ORDER-1") {
    this.orders.get(orderId)!.status = "APPROVED";
  }
  async getOrder(orderId: string) {
    return { ...this.orders.get(orderId)! };
  }
  async captureOrder(orderId: string, requestId: string) {
    this.captureCalls.push({ orderId, requestId });
    const order = this.orders.get(orderId)!;
    order.status = "COMPLETED";
    order.payerId = "PAYER123";
    order.captureId = `CAP-${orderId}`;
    order.captureStatus = this.captureStatus;
    order.captureAmount = order.amount;
    order.captureCurrency = order.currency;
    this.remoteCapture.set(order.captureId, this.captureStatus);
    return { ...order };
  }
  async getCapture(captureId: string) {
    if (this.getCaptureFails) throw new Error("PayPal is down");
    return { captureId, status: this.remoteCapture.get(captureId) ?? "UNKNOWN", amount: "4.99", currency: "USD" };
  }
  async refundCapture(captureId: string, requestId: string) {
    if (this.refundFails) throw new Error("refund refused");
    this.refunds.push({ id: captureId, requestId });
    this.remoteCapture.set(captureId, "REFUNDED");
    return { refundId: `REF-${captureId}`, status: "COMPLETED" };
  }
  async createPlan(priceUsd: string) {
    this.plans.push(priceUsd);
    return `PLAN-${priceUsd}`;
  }
  async createSubscription(input: { planId: string; customId: string }) {
    const subscriptionId = `I-${(this.next += 1)}`;
    this.subscriptions.set(subscriptionId, { subscriptionId, status: "APPROVAL_PENDING", customId: input.customId, planId: input.planId, payerId: null, subscriberEmail: null, nextBillingTime: null });
    return { subscriptionId, approveUrl: `https://sandbox.paypal.test/sub/${subscriptionId}` };
  }
  activate(subscriptionId: string) {
    Object.assign(this.subscriptions.get(subscriptionId)!, { status: "ACTIVE", payerId: "PAYER123", subscriberEmail: "subscriber@example.com", nextBillingTime: "2026-12-05T00:00:00Z" });
  }
  async getSubscription(subscriptionId: string) {
    return { ...this.subscriptions.get(subscriptionId)! };
  }
  async listSubscriptionTransactions() {
    return this.sales.map((sale) => ({ ...sale }));
  }
  async cancelSubscription(subscriptionId: string) {
    this.cancelled.push(subscriptionId);
    this.subscriptions.get(subscriptionId)!.status = "CANCELLED";
  }
  async refundSale(saleId: string, requestId: string) {
    if (this.refundFails) throw new Error("refund refused");
    this.refunds.push({ id: saleId, requestId });
    return { refundId: `REF-${saleId}`, status: "COMPLETED" };
  }
  disputes = new Map<string, { disputeId: string; status: string; outcome: string | null; transactionIds: string[] }>();
  disputeReadFails: number | null = 404;
  async getDispute(disputeId: string) {
    const known = this.disputes.get(disputeId);
    if (known) return known;
    throw new PayPalApiError(this.disputeReadFails ?? 500, "NOT_FOUND", "no such dispute");
  }
  async verifyWebhook() {
    this.verifyCalls += 1;
    return this.verified;
  }
}

function harness(overrides: Partial<PayPalCommerceDeps> & { file?: string } = {}) {
  const api = new FakePayPal();
  const runs: { decisionId: string; input: Record<string, unknown> }[] = [];
  const alerts: string[] = [];
  const emails: { token: string; email: string }[] = [];
  const service = new PayPalCommerceService({
    api,
    config: { env: "sandbox", returnBaseUrl: "http://localhost:3000", reportPriceUsd: "4.99", subscriptionPriceUsd: "9.99", webhookId: "WH-1" },
    runReport: async (decisionId, input) => {
      runs.push({ decisionId, input });
      return RESULT;
    },
    deliverReport: async (token, email) => {
      emails.push({ token, email });
    },
    stateFile: overrides.file ?? stateFile(),
    alert: (message) => alerts.push(message),
    eventRetryMs: 0,
    retryDelayMs: 0,
    nothingScoredRetryMs: 0,
    ...overrides,
  });
  return { api, service, runs, alerts, emails };
}

const flush = async () => {
  for (let index = 0; index < 20; index += 1) await new Promise((resolve) => setTimeout(resolve, 3));
};

async function paidPurchase(h: ReturnType<typeof harness>) {
  const { purchaseId } = await h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "Comprehensive Intelligence Report", body: INPUT, conversationId: "c", actionId: "a" });
  h.api.approve();
  const view = await h.service.capture(purchaseId);
  await flush();
  return { purchaseId, view };
}

function webhook(id: string, eventType: string, resource: Record<string, unknown>) {
  return JSON.stringify({ id, event_type: eventType, resource });
}

const CAPTURE = "CAP-ORDER-1";

/** Delivery plus the processing that follows it. */
async function deliver(h: ReturnType<typeof harness>, body: string) {
  const result = await h.service.handleWebhook(HEADERS, body);
  await h.service.drainEvents();
  await flush();
  return result;
}

const SNAPSHOT_AT = Date.parse("2026-11-07T12:00:00.000Z");

async function lockedSnapshot(assets = ["BTC"]) {
  const card = { asset: "BTC", name: "Bitcoin", coingeckoId: "bitcoin", marketCapRank: 1, tokenCategory: "core", role: "core", riskClass: "large_cap_crypto", selectionBucket: "core", selected: true, qualityScore: 0.9, riskScore: 0.1, liquidityScore: 1, structuralScore: 0.9, compositeScore: 0.9, selectionReasons: [] };
  const preflight = new ScorecardPreflight({
    runScreen: async () => ({ screenedAt: "2026-11-07T11:30:00.000Z", counts: { screened: 60, qualified: 40, selected: 10 }, cards: [card] }),
    measure: async () => ({ status: "measured" }) as never,
    segments: ["Bluechips"],
    now: () => SNAPSHOT_AT,
    snapshotValidMs: 60_000,
  });
  return (await preflight.check(assets, { riskTolerance: "Balanced", timeframe: "1-3_years", portfolioSegment: "Bluechips" })).snapshot!;
}

const ACCEPTED_AT = new Date(SNAPSHOT_AT + 30_000);
const LATE = new Date(SNAPSHOT_AT + 61_000);
const lockedCheckout = (h: ReturnType<typeof harness>, screening: ScreeningSnapshot) => h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null, screening });

test("every run of a paid report uses the snapshot it was bought with, even after the quote window", async () => {
  const screening = await lockedSnapshot();
  const seen: unknown[] = [];
  let attempts = 0;
  let clock = ACCEPTED_AT;
  const h = harness({
    now: () => clock,
    runReport: async (_decisionId, _input, locked) => {
      seen.push(locked);
      if ((attempts += 1) === 1) throw new Error("Price history is temporarily unavailable.");
      return RESULT;
    },
  });
  const { purchaseId } = await lockedCheckout(h, screening);
  h.api.approve();
  await h.service.capture(purchaseId);
  clock = LATE;
  await flush();
  assert.deepEqual(seen, [screening, screening]);
  assert.equal((await h.service.status(purchaseId)).phase, "completed");
  assert.equal(h.api.refunds.length, 0);
});

test("a locked asset that can't score refunds after a few tries, without waiting out the provider window", async () => {
  const screening = await lockedSnapshot();
  let tries = 0;
  const h = harness({ now: () => ACCEPTED_AT, runReport: async () => { tries += 1; throw new ScoreUnavailableError(); } });
  const { purchaseId } = await lockedCheckout(h, screening);
  h.api.approve();
  await h.service.capture(purchaseId);
  await flush();
  assert.equal((await h.service.status(purchaseId)).phase, "refunded");
  assert.equal(h.api.refunds.length, 1);
  assert.equal(tries, 3);
});

const hour = 60 * 60_000;

test("each product lists its own payment methods, and billing doesn't depend on the provider", () => {
  const catalog = { resources: [{ productId: "asset_scorecard", endpoint: "/x", pricing: { amountUsdc: "0.50" } }] } as unknown as Parameters<typeof buildRegistry>[0];
  const methods = (paypal: boolean) => buildRegistry(catalog, paypal ? { reportUsd: "4.99", subscriptionUsd: "9.99" } : null)
    .map((entry) => [entry.id, entry.paymentMethods, entry.billing]);
  assert.deepEqual(methods(false), [["asset_scorecard", ["x402_wallet"], "once"]]);
  assert.deepEqual(methods(true), [
    ["asset_scorecard", ["x402_wallet"], "once"],
    ["comprehensive_report", ["paypal"], "once"],
    ["monthly_intelligence", ["paypal"], "monthly"],
  ]);
  assert.deepEqual(buildRegistry(catalog, { reportUsd: "4.99", subscriptionUsd: "9.99" }, ["paypal", "stripe"]).find((entry) => entry.id === "comprehensive_report")?.paymentMethods, ["paypal", "stripe"]);
});

test("the staging fault switch can't touch a live purchase, and does nothing without its file", async () => {
  const file = path.join(dir, "fault.json");
  fs.writeFileSync(file, JSON.stringify({ fault: "provider" }));
  const run = async () => RESULT;
  assert.equal(withSandboxFaults("live", file, run), run);
  await assert.rejects(() => withSandboxFaults("sandbox", file, run)(), /Sandbox test/);
  assert.equal(await withSandboxFaults("sandbox", path.join(dir, "absent.json"), run)(), RESULT);
});

test("a provider outage doesn't refund in minutes: the customer is told, and it recovers on the same payment", async () => {
  const start = Date.parse("2026-11-07T12:00:00.000Z");
  let clock = start;
  let feedUp = false;
  let tries = 0;
  const h = harness({
    now: () => new Date(clock),
    retryDelayMs: 60_000,
    runReport: async () => {
      tries += 1;
      if (!feedUp) throw new Error("Price history is temporarily unavailable.");
      return RESULT;
    },
  });
  const { purchaseId } = await paidPurchase(h);
  assert.equal(tries, 1);
  const delayed = await h.service.status(purchaseId);
  assert.equal(delayed.phase, "delayed");
  assert.match(delayed.message ?? "", /up to 24 hours/);
  assert.match(delayed.message ?? "", /won't be charged again/);
  assert.equal(h.api.refunds.length, 0);

  // Not due yet, so checking the status doesn't start another run.
  await h.service.status(purchaseId);
  await flush();
  assert.equal(tries, 1);

  clock = start + 23 * hour;
  feedUp = true;
  assert.equal((await h.service.status(purchaseId)).phase, "running");
  await flush();
  assert.equal(tries, 2);
  assert.equal((await h.service.status(purchaseId)).phase, "completed");
  assert.equal(h.api.captureCalls.length, 1);
  assert.equal(h.api.refunds.length, 0);
});

test("an outage that outlasts the window refunds, once", async () => {
  const start = Date.parse("2026-11-07T12:00:00.000Z");
  let clock = start;
  const h = harness({ now: () => new Date(clock), retryDelayMs: 60_000, runReport: async () => { throw new Error("feed down"); } });
  const { purchaseId } = await paidPurchase(h);
  assert.equal((await h.service.status(purchaseId)).phase, "delayed");
  clock = start + 24 * hour;
  await h.service.status(purchaseId);
  await flush();
  assert.equal((await h.service.status(purchaseId)).phase, "refunded");
  assert.equal(h.api.refunds.length, 1);
});

test("the retry schedule survives a restart, and isn't brought forward by it", async () => {
  const start = Date.parse("2026-11-07T12:00:00.000Z");
  let clock = start;
  let tries = 0;
  const file = stateFile();
  const failing = async () => { tries += 1; throw new Error("feed down"); };
  const first = harness({ file, now: () => new Date(clock), retryDelayMs: hour, runReport: failing });
  const { purchaseId } = await paidPurchase(first);
  assert.equal(tries, 1);

  clock = start + 30 * 60_000;
  const second = harness({ file, now: () => new Date(clock), retryDelayMs: hour, runReport: failing });
  second.service.resume();
  await flush();
  assert.equal(tries, 1, "not due yet");
  assert.equal((await second.service.status(purchaseId)).phase, "delayed");

  clock = start + 2 * hour;
  const third = harness({ file, now: () => new Date(clock), retryDelayMs: hour, runReport: async () => { tries += 1; return RESULT; } });
  third.service.resume();
  await flush();
  assert.equal(tries, 2);
  assert.equal((await third.service.status(purchaseId)).phase, "completed");
  assert.equal(third.api.refunds.length + first.api.refunds.length, 0);
});

test("a result that can't ever be delivered refunds straight away", async () => {
  let tries = 0;
  const h = harness({ runReport: async () => { tries += 1; throw new UndeliverableError("The screening decision for this purchase can't be used: altered."); } });
  const { purchaseId } = await paidPurchase(h);
  assert.equal(tries, 1);
  assert.equal((await h.service.status(purchaseId)).phase, "refunded");
});

const SCORED_AND_LOST = {
  ...RESULT,
  assetScorecard: { ...RESULT.assetScorecard, scorecards: [...RESULT.assetScorecard.scorecards, { ...RESULT.assetScorecard.scorecards[0], asset: "ETH", overallScore: null, priceRiskScore: null, priceHistory: { status: "insufficient", reason: "Only 90 daily closes were available, and 180 are needed." } }], unscreenedAssets: [{ asset: "DOGE", name: "Dogecoin", coingeckoId: "dogecoin", priceHistory: { status: "measured" } }] },
};
const renewalSale = (id: string) => ({ id, status: "COMPLETED", amount: "9.99", currency: "USD", time: "2026-11-07T12:00:00Z" });

async function activeSubscription(overrides: Parameters<typeof harness>[0]) {
  const notices: { email: string; notice: CoverageNotice }[] = [];
  const h = harness({ notifyCoverage: async (email, notice) => { notices.push({ email, notice }); }, ...overrides });
  const { subscriptionLocalId } = await h.service.createSubscription({ actionKey: "renewal", capabilityId: COMPREHENSIVE_REPORT_ID, title: "Report", body: { ...INPUT, assets: ["BTC", "ETH", "DOGE"] }, conversationId: null, actionId: null });
  h.api.activate("I-1");
  return { h, notices, subscriptionLocalId };
}

test("a renewal losing some assets is delivered, says which and why, and notifies once", async () => {
  const { h, notices, subscriptionLocalId } = await activeSubscription({ runReport: async () => SCORED_AND_LOST });
  h.api.sales.push(renewalSale("R-1"));
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  const view = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(view.phase, "completed");
  assert.equal(h.api.refunds.length, 0);
  assert.equal(notices.length, 1);
  assert.equal(notices[0].email, "subscriber@example.com");
  assert.equal(notices[0].notice.chooseBy, null);
  assert.deepEqual(notices[0].notice.missing.map((entry) => entry.asset), ["ETH", "DOGE"]);
  assert.match(notices[0].notice.missing[0].reason, /180/);
  const text = JSON.stringify(view.document);
  assert.match(text, /ETH has no overall score/);
  assert.match(text, /DOGE is outside/);
});

test("a renewal with nothing to score waits for new assets, and they deliver it on the same payment", async () => {
  const start = Date.parse("2026-11-07T12:00:00.000Z");
  let clock = start;
  const inputs: Record<string, unknown>[] = [];
  const locks: unknown[] = [];
  let checked: string[] = [];
  const snapshot = await lockedSnapshot();
  const { h, notices, subscriptionLocalId } = await activeSubscription({
    now: () => new Date(clock),
    nothingScoredRetryMs: 6 * hour,
    checkAssets: async (assets) => { checked = assets; return { deliverable: true, assets: [], snapshot }; },
    runReport: async (_decisionId, input, locked) => {
      inputs.push(input);
      locks.push(locked);
      if ((input.assets as string[]).includes("DOGE")) throw new ScoreUnavailableError("None of the requested assets can get an overall score.");
      return RESULT;
    },
  });
  h.api.sales.push(renewalSale("R-1"));
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  const waiting = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(waiting.phase, "needs_selection");
  assert.match(waiting.message ?? "", /Choose different assets/);
  assert.equal(notices.length, 1);
  assert.ok(notices[0].notice.chooseBy?.startsWith("2026-11-10"));
  assert.equal(h.api.refunds.length, 0);

  clock = start + hour;
  await h.service.updateSubscriptionAssets(subscriptionLocalId, ["BTC"]);
  await flush();
  assert.deepEqual(checked, ["BTC"]);
  const done = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(done.phase, "completed");
  assert.deepEqual(inputs.at(-1)?.assets, ["BTC"]);
  assert.equal(locks.at(-1), snapshot);
  assert.equal(h.api.refunds.length, 0);
  assert.equal(h.runs.length, 0);
});

test("a renewal nobody updates is refunded for that month when the window ends", async () => {
  const start = Date.parse("2026-11-07T12:00:00.000Z");
  let clock = start;
  const { h, subscriptionLocalId } = await activeSubscription({ now: () => new Date(clock), nothingScoredRetryMs: 6 * hour, runReport: async () => { throw new ScoreUnavailableError(); } });
  h.api.sales.push(renewalSale("R-1"));
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  clock = start + 73 * hour;
  await h.service.sweep();
  await flush();
  assert.deepEqual(h.api.refunds.map((refund) => refund.id), ["R-1"]);
  assert.equal((await h.service.syncSubscription(subscriptionLocalId)).phase, "refunded");
});

test("a renewal's provider outage retries instead of asking the customer to change anything", async () => {
  const { h, notices, subscriptionLocalId } = await activeSubscription({ retryDelayMs: hour, runReport: async () => { throw new Error("feed down"); } });
  h.api.sales.push(renewalSale("R-1"));
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  assert.equal((await h.service.syncSubscription(subscriptionLocalId)).phase, "delayed");
  assert.equal(notices.length, 0);
});

test("new subscription assets are refused unless they can be scored, and nothing changes", async () => {
  const { h, subscriptionLocalId } = await activeSubscription({
    checkAssets: async () => ({ deliverable: false, assets: [{ symbol: "DOT", name: "Polkadot", outcome: "outside_segment" }] }),
    runReport: async (_decisionId, input) => { h2Runs.push(input); return RESULT; },
  });
  h.api.sales.push(renewalSale("R-1"));
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  await assert.rejects(() => h.service.updateSubscriptionAssets(subscriptionLocalId, ["DOT"]), (error: AssistantError) => error.code === "scorecard_unavailable" && /wasn't changed/.test(error.message));
  h.api.sales.push(renewalSale("R-2"));
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  assert.deepEqual(h2Runs.at(-1)?.assets, ["BTC", "ETH", "DOGE"]);
});
const h2Runs: Record<string, unknown>[] = [];

test("PayPal isn't captured on an expired quote, and a fresh quote recovers the same order", async () => {
  const screening = await lockedSnapshot();
  let clock = LATE;
  const h = harness({ now: () => clock });
  const { purchaseId } = await lockedCheckout(h, screening);
  h.api.approve();
  await assert.rejects(() => h.service.capture(purchaseId), (error: AssistantError) => error.code === "quote_expired" && /nothing was charged/.test(error.message));
  assert.equal(h.api.captureCalls.length, 0);
  assert.equal(h.runs.length, 0);

  const { id: _id, ...content } = { ...screening, validUntil: new Date(LATE.getTime() + 60_000).toISOString() };
  const rebound = { ...content, id: createHash("sha256").update(JSON.stringify(content)).digest("hex") };
  await lockedCheckout(h, rebound);
  clock = new Date(LATE.getTime() + 1000);
  await h.service.capture(purchaseId);
  await flush();
  assert.equal(h.api.captureCalls.length, 1);
  assert.equal(h.runs.length, 1);
});

test("PayPal isn't captured on a quote made for different assets", async () => {
  const screening = await lockedSnapshot(["BTC", "bitcoin"]);
  const h = harness({ now: () => ACCEPTED_AT });
  const { purchaseId } = await lockedCheckout(h, screening);
  h.api.approve();
  await assert.rejects(() => h.service.capture(purchaseId), (error: AssistantError) => error.code === "quote_expired");
  assert.equal(h.api.captureCalls.length, 0);
});

test("a webhook doesn't capture on an expired quote, and doesn't keep retrying it", async () => {
  const screening = await lockedSnapshot();
  const h = harness({ now: () => LATE });
  await lockedCheckout(h, screening);
  h.api.approve();
  await deliver(h, webhook("EV-expired", "CHECKOUT.ORDER.APPROVED", { id: "ORDER-1" }));
  assert.equal(h.api.captureCalls.length, 0);
  assert.equal(h.runs.length, 0);
  assert.equal(h.alerts.length, 0);
});

test("a saved snapshot survives a restart", async () => {
  const screening = await lockedSnapshot();
  const file = stateFile();
  const seen: unknown[] = [];
  const first = harness({ file, now: () => ACCEPTED_AT, runReport: () => new Promise(() => undefined) });
  const { purchaseId } = await lockedCheckout(first, screening);
  first.api.approve();
  await first.service.capture(purchaseId);
  await flush();
  const second = harness({ file, now: () => LATE, runReport: async (_decisionId, _input, locked) => { seen.push(locked); return RESULT; } });
  second.service.resume();
  await flush();
  assert.deepEqual(seen, [screening]);
});

test("a subscription's first month uses the checked snapshot, and later months screen when they run", async () => {
  const screening = await lockedSnapshot();
  const seen: unknown[] = [];
  const h = harness({ now: () => ACCEPTED_AT, runReport: async (_decisionId, _input, locked) => { seen.push(locked); return RESULT; } });
  const { subscriptionLocalId } = await h.service.createSubscription({ actionKey: "k", capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null, screening });
  h.api.activate("I-1");
  h.api.sales.push({ id: "SALE-1", status: "COMPLETED", amount: "9.99", currency: "USD", time: "2026-11-07T12:00:00Z" });
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  h.api.sales.push({ id: "SALE-2", status: "COMPLETED", amount: "9.99", currency: "USD", time: "2026-11-07T12:00:00Z" });
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  assert.deepEqual(seen, [screening, null]);
});

test("nothing runs until PayPal itself reports a completed capture", async () => {
  const h = harness();
  const { purchaseId } = await h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null });

  // The customer came back without approving, or forged the return URL.
  await assert.rejects(() => h.service.capture(purchaseId), (error: AssistantError) => error.code === "payment_not_approved");
  await flush();
  assert.equal(h.runs.length, 0);
  assert.equal(h.api.captureCalls.length, 0);
  assert.equal((await h.service.status(purchaseId)).phase, "awaiting_payment");

  h.api.approve();
  await h.service.capture(purchaseId);
  await flush();
  assert.equal(h.runs.length, 1);
  assert.equal(h.api.captureCalls.length, 1);
});

test("a pending capture waits, and starts only once PayPal completes it", async () => {
  const h = harness();
  h.api.captureStatus = "PENDING";
  const { purchaseId, view } = await paidPurchase(h);
  assert.equal(view.phase, "payment_pending");
  assert.equal(h.runs.length, 0);

  // A webhook repeat while it's still pending changes nothing.
  await deliver(h, webhook("e1", "PAYMENT.CAPTURE.PENDING", { id: CAPTURE }));
  await flush();
  assert.equal(h.runs.length, 0);

  h.api.remoteCapture.set(CAPTURE, "COMPLETED");
  await deliver(h, webhook("e2", "PAYMENT.CAPTURE.COMPLETED", { id: CAPTURE }));
  await flush();
  assert.equal(h.runs.length, 1);
  assert.equal((await h.service.status(purchaseId)).phase, "completed");
});

test("a declined capture doesn't run and tells the customer", async () => {
  const h = harness();
  h.api.captureStatus = "DECLINED";
  const { view } = await paidPurchase(h);
  assert.equal(view.phase, "failed");
  assert.match(view.message ?? "", /declined/);
  assert.equal(h.runs.length, 0);
});

test("repeats, double clicks, and restarts never capture or run twice", async () => {
  const file = stateFile();
  const h = harness({ file });
  const { purchaseId } = await h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null });
  const again = await h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null });
  assert.equal(again.purchaseId, purchaseId);
  assert.equal(h.api.orders.size, 1, "same order, not a second one");

  h.api.approve();
  await Promise.all([h.service.capture(purchaseId), h.service.capture(purchaseId), h.service.capture(purchaseId)]);
  await flush();
  await h.service.capture(purchaseId);
  await flush();
  assert.equal(h.api.captureCalls.length, 1);
  assert.equal(h.runs.length, 1);
  assert.equal(h.api.captureCalls[0].requestId, `selun-capture-${purchaseId}`, "PayPal's own idempotency key is stable");

  await assert.rejects(
    () => h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null }),
    (error: AssistantError) => error.code === "action_not_payable",
  );

  const restarted = harness({ file });
  restarted.service.resume();
  await flush();
  assert.equal(restarted.runs.length, 0, "a finished purchase isn't rerun");
  assert.equal((await restarted.service.status(purchaseId)).phase, "completed");
});

test("an order whose amount or reference doesn't match is refused, before any capture", async () => {
  for (const change of [{ amount: "0.01" }, { currency: "EUR" }, { customId: "pp-someone-else" }]) {
    const h = harness();
    const { purchaseId } = await h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null });
    h.api.approve();
    Object.assign(h.api.orders.get("ORDER-1")!, change);
    await assert.rejects(() => h.service.capture(purchaseId), (error: AssistantError) => error.code === "payment_not_verified");
    await flush();
    assert.equal(h.api.captureCalls.length, 0);
    assert.equal(h.runs.length, 0);
    assert.equal(h.alerts.length, 1);
  }
});

test("a failed run retries on the same payment, without a second capture", async () => {
  let attempts = 0;
  const h = harness({
    runReport: async () => {
      attempts += 1;
      if (attempts < 3) throw new Error("feed down");
      return RESULT;
    },
  });
  const { purchaseId } = await paidPurchase(h);
  assert.equal(attempts, 3);
  assert.equal(h.api.captureCalls.length, 1);
  assert.equal(h.api.refunds.length, 0);
  assert.equal((await h.service.status(purchaseId)).phase, "completed");
});

test("if it can't be delivered by the end of the window, the full payment is refunded once", async () => {
  const h = harness({
    retryWindowMs: 0,
    runReport: async () => {
      throw new Error("feed down");
    },
  });
  const { purchaseId } = await paidPurchase(h);
  const view = await h.service.status(purchaseId);
  assert.equal(view.phase, "refunded");
  assert.match(view.message ?? "", /refunded in full/);
  assert.deepEqual(h.api.refunds, [{ id: CAPTURE, requestId: `selun-refund-${purchaseId}` }]);
  assert.equal(view.document, null);
  assert.equal(view.reportToken, null);

  await h.service.status(purchaseId);
  await flush();
  assert.equal(h.api.refunds.length, 1);
});

test("a result that can't become a report counts as undelivered", async () => {
  const h = harness({ runReport: async () => null as unknown as Record<string, unknown> });
  const { purchaseId } = await paidPurchase(h);
  assert.equal((await h.service.status(purchaseId)).phase, "refunded");
});

test("a refund that fails alerts a person and tells the customer plainly", async () => {
  const h = harness({ retryWindowMs: 0,
    runReport: async () => {
      throw new Error("feed down");
    },
  });
  h.api.refundFails = true;
  const { purchaseId } = await paidPurchase(h);
  const view = await h.service.status(purchaseId);
  assert.equal(view.phase, "failed");
  assert.match(view.message ?? "", /team has been alerted/);
  assert.equal(h.alerts.some((message) => message.includes("Refund it by hand")), true);
});

test("a purchase interrupted by a restart runs again on the same payment", async () => {
  const file = stateFile();
  let release!: () => void;
  const first = harness({ file, runReport: () => new Promise((resolve) => { release = () => resolve(RESULT); }) });
  const { purchaseId } = await paidPurchase(first);
  assert.equal((await first.service.status(purchaseId)).phase, "running");

  const second = harness({ file });
  second.service.resume();
  await flush();
  assert.equal(second.runs.length, 1);
  assert.equal(second.api.captureCalls.length, 0);
  assert.equal((await second.service.status(purchaseId)).phase, "completed");
  release();
});

function reportsFor(service: PayPalCommerceService) {
  return new ReportService({
    loadPaidResponse: (capabilityId, decisionId) => service.loadPaidResponse(capabilityId, decisionId),
    assertAccess: (capabilityId, decisionId) => service.assertReportAccess(capabilityId, decisionId),
    sendEmail: async () => ({ status: "test_recorded" }),
    stateFile: stateFile(),
  });
}

test("the PDF is real, carries PayPal's references, and invents no wallet or transaction hash", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  const view = await h.service.status(purchaseId);
  const document = view.document!;
  assert.equal(document.provenance.paypal?.captureId, CAPTURE);
  assert.equal(document.provenance.paypal?.amount, "4.99");
  assert.equal(document.provenance.transactionHash, null);
  assert.equal(document.provenance.payer, null);
  assert.equal(document.provenance.network, null);
  assert.equal(document.provenance.amountUsdc, null);
  assert.ok(!JSON.stringify(document).includes("0x"), "no fabricated on-chain value");
  for (const heading of ["Market conditions", "Risk limits for this profile", "Asset scores", "43.8%", "asset-scorecard-v2"]) {
    assert.ok(JSON.stringify(document).includes(heading), heading);
  }

  const reports = reportsFor(h.service);
  const first = await reports.download(view.reportToken);
  const second = await reports.download(view.reportToken);
  assert.equal(Buffer.from(first.bytes).subarray(0, 5).toString(), "%PDF-");
  assert.deepEqual(Buffer.from(first.bytes), Buffer.from(second.bytes), "a repeat download is identical");
  assert.equal(first.recordHash, resultRecordHash(document));
  assert.equal(h.runs.length, 1, "downloading doesn't run the service");
});

test("a confirmed refund revokes that purchase, keeps its evidence, and leaves others alone", async () => {
  const h = harness();
  const { purchaseId, view: _view } = await paidPurchase(h);
  const reports = reportsFor(h.service);
  const token = issueReportToken(COMPREHENSIVE_REPORT_ID, DECISION);
  const { recordHash } = await reports.download(token);

  const otherDecision = "selun-ai-22222222-2222-4222-8222-222222222222";
  const other = await h.service.createCheckout({ decisionId: otherDecision, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null });
  h.api.approve("ORDER-2");
  await h.service.capture(other.purchaseId);
  await flush();

  h.api.remoteCapture.set(CAPTURE, "REFUNDED");
  await deliver(h, webhook("e-refund", "PAYMENT.CAPTURE.REFUNDED", { id: "REF-1", links: [{ rel: "up", href: `https://api/v2/payments/captures/${CAPTURE}` }] }));

  await assert.rejects(() => reports.download(token), (error: AssistantError) => error.code === "report_revoked");
  await assert.rejects(() => reports.email(token, { email: "a@example.com", approved: true }), (error: AssistantError) => error.code === "report_revoked");
  const revoked = await h.service.status(purchaseId);
  assert.equal(revoked.phase, "revoked");
  assert.equal(revoked.document, null);
  assert.equal(revoked.reportToken, null);

  // The evidence stays on record.
  const stored = JSON.parse(fs.readFileSync((h.service as unknown as { stateFile: string }).stateFile, "utf8"));
  assert.ok(stored.purchases[purchaseId].responseData, "analysis result retained");
  assert.equal(stored.purchases[purchaseId].payment.captureId, CAPTURE);
  assert.ok(stored.purchases[purchaseId].audit.some((entry: { event: string }) => entry.event === "capture_refunded"));
  assert.ok(recordHash);

  const untouched = await reports.download(issueReportToken(COMPREHENSIVE_REPORT_ID, otherDecision));
  assert.ok(untouched.bytes.length > 0, "another purchase is unaffected");
});

test("a partial refund doesn't revoke, because the capture says what stands", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  h.api.remoteCapture.set(CAPTURE, "PARTIALLY_REFUNDED");
  await deliver(h, webhook("e-partial", "PAYMENT.CAPTURE.REFUNDED", { links: [{ rel: "up", href: `https://api/v2/payments/captures/${CAPTURE}` }] }));
  assert.equal((await h.service.status(purchaseId)).phase, "completed");
});

test("a reversal revokes, and stops a run that hasn't finished", async () => {
  let finish!: (value: Record<string, unknown>) => void;
  const h = harness({ runReport: () => new Promise((resolve) => { finish = resolve; }) });
  const { purchaseId } = await paidPurchase(h);
  assert.equal((await h.service.status(purchaseId)).phase, "running");

  h.api.remoteCapture.set(CAPTURE, "REVERSED");
  await deliver(h, webhook("e-rev", "PAYMENT.CAPTURE.REVERSED", { id: CAPTURE }));
  finish(RESULT);
  await flush();

  const view = await h.service.status(purchaseId);
  assert.equal(view.phase, "revoked");
  assert.equal(view.document, null);
  assert.equal(h.service.loadPaidResponse(COMPREHENSIVE_REPORT_ID, DECISION), null);
  assert.equal(h.api.refunds.length, 0, "already reversed, so no second refund");
});

test("a missed webhook is caught when the report is next used", async () => {
  const h = harness({ reconcileAfterMs: 0 });
  await paidPurchase(h);
  const reports = reportsFor(h.service);
  const token = issueReportToken(COMPREHENSIVE_REPORT_ID, DECISION);
  assert.ok((await reports.download(token)).bytes.length > 0);

  h.api.remoteCapture.set(CAPTURE, "REFUNDED");
  await assert.rejects(() => reports.download(token), (error: AssistantError) => error.code === "report_revoked");
});

test("a PayPal outage during the check keeps the last known state", async () => {
  const h = harness({ reconcileAfterMs: 0 });
  await paidPurchase(h);
  h.api.getCaptureFails = true;
  const reports = reportsFor(h.service);
  assert.ok((await reports.download(issueReportToken(COMPREHENSIVE_REPORT_ID, DECISION))).bytes.length > 0);
});

function dispute(status: string, outcome?: string) {
  return { dispute_id: "PP-D-1", status, disputed_transactions: [{ seller_transaction_id: CAPTURE }], ...(outcome ? { dispute_outcome: { outcome_code: outcome } } : {}) };
}

test("an open dispute holds access, and it returns if the payment stands", async () => {
  for (const outcome of ["RESOLVED_SELLER_FAVOUR", "CANCELED_BY_BUYER"]) {
    const h = harness();
    const { purchaseId } = await paidPurchase(h);
    const reports = reportsFor(h.service);
    const token = issueReportToken(COMPREHENSIVE_REPORT_ID, DECISION);

    await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.CREATED", dispute("OPEN")));
    assert.equal((await h.service.status(purchaseId)).phase, "restricted");
    await assert.rejects(() => reports.download(token), (error: AssistantError) => error.code === "report_restricted");

    await deliver(h, webhook("d2", "CUSTOMER.DISPUTE.UPDATED", dispute("WAITING_FOR_SELLER_RESPONSE")));
    assert.equal((await h.service.status(purchaseId)).phase, "restricted");

    await deliver(h, webhook("d3", "CUSTOMER.DISPUTE.RESOLVED", dispute("RESOLVED", outcome)));
    assert.equal((await h.service.status(purchaseId)).phase, "completed", outcome);
    assert.ok((await reports.download(token)).bytes.length > 0);
  }
});

test("a dispute the buyer wins returns their money, so access ends", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.CREATED", dispute("OPEN")));
  await deliver(h, webhook("d2", "CUSTOMER.DISPUTE.RESOLVED", dispute("RESOLVED", "RESOLVED_BUYER_FAVOUR")));
  assert.equal((await h.service.status(purchaseId)).phase, "revoked");
});

test("a dispute outcome we don't recognize stays on hold and alerts a person", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.CREATED", dispute("OPEN")));
  await deliver(h, webhook("d2", "CUSTOMER.DISPUTE.RESOLVED", dispute("RESOLVED", "SOMETHING_NEW")));
  assert.equal((await h.service.status(purchaseId)).phase, "restricted");
  assert.equal(h.alerts.length, 1);
});

test("a webhook with a bad signature changes nothing, and a repeat is ignored", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  h.api.remoteCapture.set(CAPTURE, "REFUNDED");

  h.api.verified = false;
  await assert.rejects(() => h.service.handleWebhook(HEADERS, webhook("forged", "PAYMENT.CAPTURE.REVERSED", { id: CAPTURE })), (error: AssistantError) => error.status === 401);
  await h.service.drainEvents();
  const stored = () => JSON.parse(fs.readFileSync((h.service as unknown as { stateFile: string }).stateFile, "utf8"));
  assert.equal(stored().webhookEvents.forged, undefined, "a forged event isn't even stored");
  h.api.remoteCapture.set(CAPTURE, "COMPLETED");
  assert.equal((await h.service.status(purchaseId)).phase, "completed", "forged event had no effect");

  h.api.verified = true;
  await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.CREATED", dispute("OPEN")));
  await deliver(h, webhook("d2", "CUSTOMER.DISPUTE.RESOLVED", dispute("RESOLVED", "RESOLVED_SELLER_FAVOUR")));
  const repeat = await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.CREATED", dispute("OPEN")));
  assert.deepEqual(repeat, { accepted: true, duplicate: true });
  assert.equal((await h.service.status(purchaseId)).phase, "completed", "an old event can't re-restrict");
});

test("a verified event is on disk before it's acknowledged, and survives a crash", async () => {
  const file = stateFile();
  const first = harness({ file });
  await paidPurchase(first);
  first.api.remoteCapture.set(CAPTURE, "REFUNDED");
  const body = webhook("e-crash", "PAYMENT.CAPTURE.REVERSED", { id: CAPTURE });
  const result = await first.service.handleWebhook(HEADERS, body);
  assert.deepEqual(result, { accepted: true });
  const saved = JSON.parse(fs.readFileSync(file, "utf8")).webhookEvents["e-crash"];
  assert.equal(saved.status, "pending", "saved, not processed yet");
  assert.equal(saved.type, "PAYMENT.CAPTURE.REVERSED");

  // The process dies here, before processing. PayPal won't resend what it saw acknowledged.
  const second = harness({ file });
  second.api.remoteCapture.set(CAPTURE, "REVERSED");
  second.service.resume();
  await new Promise((resolve) => setTimeout(resolve, 10));
  await second.service.drainEvents();
  assert.equal(JSON.parse(fs.readFileSync(file, "utf8")).webhookEvents["e-crash"].status, "processed");
  assert.equal(second.service.loadPaidResponse(COMPREHENSIVE_REPORT_ID, DECISION) !== null, true, "evidence kept");
  await assert.rejects(() => second.service.assertReportAccess(COMPREHENSIVE_REPORT_ID, DECISION), (error: AssistantError) => error.code === "report_revoked");
});

test("a failed event retries without PayPal resending it, then alerts a person", async () => {
  const h = harness({ maxEventAttempts: 3 });
  await paidPurchase(h);
  h.api.getCaptureFails = true;
  await deliver(h, webhook("e-fail", "PAYMENT.CAPTURE.COMPLETED", { id: CAPTURE }));
  const event = () => JSON.parse(fs.readFileSync((h.service as unknown as { stateFile: string }).stateFile, "utf8")).webhookEvents["e-fail"];
  assert.ok(event().attempts >= 1 && event().attempts < 3, "tried, and not yet given up");
  assert.equal(event().status, "pending");
  for (let index = 0; index < 5 && event().status === "pending"; index += 1) {
    await h.service.drainEvents();
    await flush();
  }
  assert.equal(event().status, "failed");
  assert.equal(event().attempts, 3);
  assert.equal(h.alerts.some((message) => message.includes("could not be processed")), true);
  assert.equal(h.runs.length, 1, "no extra run");

  const recovered = harness();
  await paidPurchase(recovered);
  recovered.api.getCaptureFails = true;
  await deliver(recovered, webhook("e-ok", "PAYMENT.CAPTURE.COMPLETED", { id: CAPTURE }));
  recovered.api.getCaptureFails = false;
  await recovered.service.drainEvents();
  const stored = JSON.parse(fs.readFileSync((recovered.service as unknown as { stateFile: string }).stateFile, "utf8"));
  assert.equal(stored.webhookEvents["e-ok"].status, "processed");
});

test("a webhook about an unknown payment is kept and does nothing", async () => {
  const h = harness();
  await deliver(h, webhook("e", "PAYMENT.CAPTURE.COMPLETED", { id: "CAP-NOT-OURS" }));
  const stored = JSON.parse(fs.readFileSync((h.service as unknown as { stateFile: string }).stateFile, "utf8"));
  assert.equal(stored.webhookEvents.e.status, "unmatched");
  assert.equal(h.runs.length, 0);
});

test("a buyer who approves and never comes back is still captured and fulfilled, once", async () => {
  const h = harness();
  const { purchaseId } = await h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null });
  h.api.approve();
  await deliver(h, webhook("a1", "CHECKOUT.ORDER.APPROVED", { id: "ORDER-1" }));
  assert.equal(h.api.captureCalls.length, 1);
  assert.equal(h.runs.length, 1);

  // Now they do come back, and the capture webhook arrives too.
  await h.service.capture(purchaseId);
  await deliver(h, webhook("a2", "PAYMENT.CAPTURE.COMPLETED", { id: CAPTURE, custom_id: purchaseId }));
  await deliver(h, webhook("a1", "CHECKOUT.ORDER.APPROVED", { id: "ORDER-1" }));
  assert.equal(h.api.captureCalls.length, 1, "one capture");
  assert.equal(h.runs.length, 1, "one report");
  assert.equal((await h.service.status(purchaseId)).phase, "completed");
});

test("events arriving out of order settle on what PayPal says now", async () => {
  // The capture event arrives before this server recorded the capture.
  const h = harness();
  const { purchaseId } = await h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null });
  h.api.approve();
  await h.api.captureOrder("ORDER-1", "already-captured-elsewhere");
  await deliver(h, webhook("c1", "PAYMENT.CAPTURE.COMPLETED", { id: CAPTURE, custom_id: purchaseId }));
  assert.equal(h.runs.length, 1);
  assert.equal(h.api.captureCalls.length, 1, "no second capture");

  // A refund event arrives before the completed one. The late one can't restore access.
  const r = harness();
  const paid = await paidPurchase(r);
  r.api.remoteCapture.set(CAPTURE, "REFUNDED");
  await deliver(r, webhook("r1", "PAYMENT.CAPTURE.REFUNDED", { id: "REF", links: [{ rel: "up", href: `https://api/captures/${CAPTURE}` }] }));
  await deliver(r, webhook("r2", "PAYMENT.CAPTURE.COMPLETED", { id: CAPTURE }));
  assert.equal((await r.service.status(paid.purchaseId)).phase, "revoked");
  assert.equal(r.runs.length, 1);
});

test("a reversal event identifying the capture through its link revokes, even if the capture still reads complete", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  await deliver(h, webhook("rv", "PAYMENT.CAPTURE.REVERSED", { id: "REV-1", links: [{ rel: "up", href: `https://api/captures/${CAPTURE}` }] }));
  assert.equal((await h.service.status(purchaseId)).phase, "revoked");
});

test("a dispute is read from PayPal when it can be, not only from the event", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  h.api.disputes.set("PP-D-1", { disputeId: "PP-D-1", status: "RESOLVED", outcome: "RESOLVED_BUYER_FAVOUR", transactionIds: [CAPTURE] });
  // The event body is stale and says it's still open.
  await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.UPDATED", dispute("OPEN")));
  assert.equal((await h.service.status(purchaseId)).phase, "revoked");
});

test("a dispute that can't be read for a server error retries instead of guessing", async () => {
  const h = harness();
  const { purchaseId } = await paidPurchase(h);
  h.api.disputeReadFails = 500;
  await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.CREATED", dispute("OPEN")));
  assert.equal((await h.service.status(purchaseId)).phase, "completed", "not applied yet");
  h.api.disputeReadFails = 404;
  await h.service.drainEvents();
  await flush();
  assert.equal((await h.service.status(purchaseId)).phase, "restricted");
});

test("a report held by a dispute finishes once the merchant wins", async () => {
  const h = harness();
  h.api.captureStatus = "PENDING";
  const { purchaseId } = await paidPurchase(h);
  await deliver(h, webhook("d1", "CUSTOMER.DISPUTE.CREATED", dispute("OPEN")));
  h.api.remoteCapture.set(CAPTURE, "COMPLETED");
  await deliver(h, webhook("c1", "PAYMENT.CAPTURE.COMPLETED", { id: CAPTURE }));
  assert.equal(h.runs.length, 0, "no fulfillment while the dispute is open");
  await deliver(h, webhook("d2", "CUSTOMER.DISPUTE.RESOLVED", dispute("RESOLVED", "RESOLVED_SELLER_FAVOUR")));
  assert.equal(h.runs.length, 1);
  assert.equal((await h.service.status(purchaseId)).phase, "completed");
});

async function subscribed(h: ReturnType<typeof harness>) {
  const { subscriptionLocalId } = await h.service.createSubscription({ actionKey: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "Comprehensive Intelligence Report", body: INPUT, conversationId: null, actionId: null });
  const [remote] = [...h.api.subscriptions.keys()];
  return { subscriptionLocalId, remote };
}

const sale = (id: string, status = "COMPLETED", amount = "9.99") => ({ id, status, amount, currency: "USD", time: "2026-11-05T00:00:00Z" });

test("a subscription runs nothing until PayPal shows a completed payment", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  assert.deepEqual(h.api.plans, ["9.99"]);

  let view = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(view.status, "created");
  h.api.activate(remote);
  view = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(view.status, "active");
  assert.equal(view.phase, "payment_pending");
  await flush();
  assert.equal(h.runs.length, 0, "active but unpaid");

  h.api.sales = [sale("S-1", "PENDING")];
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  assert.equal(h.runs.length, 0, "payment not complete");
});

test("each paid month produces exactly one report, delivered to PayPal's address", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  h.api.sales = [sale("S-1")];

  await Promise.all([h.service.syncSubscription(subscriptionLocalId), h.service.syncSubscription(subscriptionLocalId)]);
  await deliver(h, webhook("p1", "PAYMENT.SALE.COMPLETED", { billing_agreement_id: remote }));
  await flush();
  assert.equal(h.runs.length, 1, "one payment, one report");
  assert.deepEqual(h.emails.map((entry) => entry.email), ["subscriber@example.com"]);

  let view = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(view.phase, "completed");
  assert.equal(view.reports, 1);
  assert.equal(view.document?.provenance.paypal?.saleId, "S-1");
  assert.equal(view.document?.provenance.paypal?.subscriptionId, remote);
  assert.equal(view.document?.provenance.transactionHash, null);

  h.api.sales = [sale("S-1"), sale("S-2")];
  await deliver(h, webhook("p2", "PAYMENT.SALE.COMPLETED", { billing_agreement_id: remote }));
  await flush();
  view = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(h.runs.length, 2);
  assert.equal(view.reports, 2);
  assert.notEqual(h.runs[0].decisionId, h.runs[1].decisionId, "each cycle has its own record");
});

test("a payment of the wrong amount isn't honored", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  h.api.sales = [sale("S-1", "COMPLETED", "0.01")];
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  assert.equal(h.runs.length, 0);
  assert.equal(h.alerts.length, 1);
});

test("activation and cancellation events never create a report; only a completed payment does", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  await deliver(h, webhook("s1", "BILLING.SUBSCRIPTION.ACTIVATED", { id: remote }));
  assert.equal(h.runs.length, 0);
  assert.equal((await h.service.syncSubscription(subscriptionLocalId)).status, "active");

  h.api.sales = [sale("S-1")];
  await deliver(h, webhook("s2", "PAYMENT.SALE.COMPLETED", { id: "S-1", billing_agreement_id: remote }));
  assert.equal(h.runs.length, 1);

  await h.service.cancelSubscription(subscriptionLocalId);
  await deliver(h, webhook("s3", "BILLING.SUBSCRIPTION.CANCELLED", { id: remote }));
  assert.equal(h.runs.length, 1, "cancelling doesn't produce a report");
  const view = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(view.status, "cancelled");
  assert.ok(view.document, "the paid period stays available");
});

test("a sale event before PayPal lists the payment retries rather than being lost", async () => {
  const h = harness();
  const { remote } = await subscribed(h);
  h.api.activate(remote);
  await deliver(h, webhook("s1", "PAYMENT.SALE.COMPLETED", { id: "S-1", billing_agreement_id: remote }));
  assert.equal(h.runs.length, 0);
  h.api.sales = [sale("S-1")];
  await h.service.drainEvents();
  await flush();
  assert.equal(h.runs.length, 1);
});

test("a refund or reversal event finds the month it belongs to", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  h.api.sales = [sale("S-1")];
  await deliver(h, webhook("s1", "PAYMENT.SALE.COMPLETED", { id: "S-1", billing_agreement_id: remote }));

  // A sale refund carries sale_id, and may not name the subscription.
  h.api.sales = [sale("S-1", "REFUNDED")];
  await deliver(h, webhook("s2", "PAYMENT.SALE.REFUNDED", { id: "RF-1", sale_id: "S-1" }));
  assert.equal((await h.service.syncSubscription(subscriptionLocalId)).phase, "revoked");

  const r = harness();
  const again = await subscribed(r);
  r.api.activate(again.remote);
  r.api.sales = [sale("S-9")];
  await deliver(r, webhook("s1", "PAYMENT.SALE.COMPLETED", { id: "S-9", billing_agreement_id: again.remote }));
  await deliver(r, webhook("s2", "PAYMENT.SALE.REVERSED", { id: "S-9", billing_agreement_id: again.remote }));
  assert.equal((await r.service.syncSubscription(again.subscriptionLocalId)).phase, "revoked");
});

test("a failed subscription payment is recorded and earns nothing", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  await deliver(h, webhook("f1", "BILLING.SUBSCRIPTION.PAYMENT.FAILED", { id: remote }));
  assert.equal(h.runs.length, 0);
  const stored = JSON.parse(fs.readFileSync((h.service as unknown as { stateFile: string }).stateFile, "utf8"));
  assert.ok(stored.subscriptions[subscriptionLocalId].audit.some((entry: { event: string }) => entry.event === "subscription_payment_failed"));
});

test("a cycle that can't be delivered refunds that month's payment only", async () => {
  let calls = 0;
  const h = harness({ retryWindowMs: 0,
    runReport: async () => {
      calls += 1;
      if (calls === 1) return RESULT;
      throw new Error("feed down");
    },
  });
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  h.api.sales = [sale("S-1")];
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  h.api.sales = [sale("S-1"), sale("S-2")];
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  assert.deepEqual(h.api.refunds.map((refund) => refund.id), ["S-2"]);
  const view = await h.service.syncSubscription(subscriptionLocalId);
  assert.equal(view.reports, 1, "the first month's report remains");
  assert.equal(view.phase, "refunded");
});

test("a refunded month loses that report and keeps the others", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  h.api.sales = [sale("S-1"), sale("S-2")];
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();
  const reports = reportsFor(h.service);
  const decisions = h.runs.map((run) => run.decisionId);

  h.api.sales = [sale("S-1", "REFUNDED"), sale("S-2")];
  await h.service.syncSubscription(subscriptionLocalId);
  await assert.rejects(() => reports.download(issueReportToken(COMPREHENSIVE_REPORT_ID, decisions[0])), (error: AssistantError) => error.code === "report_revoked");
  assert.ok((await reports.download(issueReportToken(COMPREHENSIVE_REPORT_ID, decisions[1]))).bytes.length > 0);
});

test("cancelling stops billing and keeps paid reports", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  h.api.sales = [sale("S-1")];
  await h.service.syncSubscription(subscriptionLocalId);
  await flush();

  const view = await h.service.cancelSubscription(subscriptionLocalId);
  assert.equal(view.status, "cancelled");
  assert.deepEqual(h.api.cancelled, [remote]);
  assert.ok(view.document, "the paid report stays");
  await h.service.cancelSubscription(subscriptionLocalId);
  assert.equal(h.api.cancelled.length, 1, "a repeat doesn't call PayPal again");
});

test("a subscription that doesn't match its record is refused", async () => {
  const h = harness();
  const { subscriptionLocalId, remote } = await subscribed(h);
  h.api.activate(remote);
  h.api.subscriptions.get(remote)!.customId = "someone-else";
  await assert.rejects(() => h.service.syncSubscription(subscriptionLocalId), (error: AssistantError) => error.code === "payment_not_verified");
});

test("a damaged record file stops sales rather than starting empty", async () => {
  const file = stateFile();
  fs.writeFileSync(file, "{ not json");
  const h = harness({ file });
  await assert.rejects(() => h.service.createCheckout({ decisionId: DECISION, capabilityId: COMPREHENSIVE_REPORT_ID, title: "T", body: INPUT, conversationId: null, actionId: null }), (error: AssistantError) => error.code === "service_unavailable");
});

test("configuration: prices are configurable, and PayPal stays off without credentials", () => {
  assert.equal(readPayPalConfig({}), null);
  assert.equal(readPayPalConfig({ PAYPAL_ENV: "sandbox", PAYPAL_CLIENT_ID: "a" }), null);
  assert.equal(readPayPalConfig({ PAYPAL_ENV: "typo", PAYPAL_CLIENT_ID: "a", PAYPAL_CLIENT_SECRET: "b" }), null);
  const defaults = readPayPalConfig({ PAYPAL_ENV: "sandbox", PAYPAL_CLIENT_ID: "a", PAYPAL_CLIENT_SECRET: "b" })!;
  assert.equal(defaults.reportPriceUsd, "4.99");
  assert.equal(defaults.subscriptionPriceUsd, "9.99");
  assert.match(defaults.baseUrl, /sandbox/);
  const custom = readPayPalConfig({ PAYPAL_ENV: "live", PAYPAL_CLIENT_ID: "a", PAYPAL_CLIENT_SECRET: "b", PAYPAL_REPORT_PRICE_USD: "6.50", PAYPAL_SUBSCRIPTION_PRICE_USD: "free" })!;
  assert.equal(custom.reportPriceUsd, "6.50");
  assert.equal(custom.subscriptionPriceUsd, "9.99", "a bad value falls back");
  assert.doesNotMatch(custom.baseUrl, /sandbox/);
});

test("PayPal's order shape is read correctly", () => {
  const order = parseOrder({
    id: "O1",
    status: "COMPLETED",
    payer: { payer_id: "P1" },
    purchase_units: [{ amount: { value: "4.99", currency_code: "USD" }, payments: { captures: [{ id: "C1", status: "COMPLETED", custom_id: "pp-1", amount: { value: "4.99", currency_code: "USD" } }] } }],
  });
  assert.deepEqual(order, { orderId: "O1", status: "COMPLETED", customId: "pp-1", amount: "4.99", currency: "USD", payerId: "P1", captureId: "C1", captureStatus: "COMPLETED", captureAmount: "4.99", captureCurrency: "USD" });
  assert.equal(parseOrder({}).captureId, null);
});

test("evidence without PayPal's reference isn't a completed purchase", () => {
  const base = { status: "completed", endpoint: "paypal:x", decisionId: "d", result: RESULT };
  assert.equal(presentResult(COMPREHENSIVE_REPORT_ID, "T", { ...base, payment: { verified: true, provider: "paypal", amount: "4.99", currency: "USD" } }), null, "no capture or sale");
  assert.equal(presentResult(COMPREHENSIVE_REPORT_ID, "T", { ...base, payment: { verified: false, provider: "paypal", amount: "4.99", currency: "USD", captureId: "C" } }), null);
  assert.ok(presentResult(COMPREHENSIVE_REPORT_ID, "T", { ...base, payment: { verified: true, provider: "paypal", amount: "4.99", currency: "USD", captureId: "C" } }));
});

test("an x402 result's record is unchanged by the PayPal work", async () => {
  const response = { status: "completed", endpoint: "/agent/x402/asset-scorecard", decisionId: "d", payment: { verified: true, transactionHash: `0x${"a".repeat(64)}`, chargedAmountUsdc: "0.5", network: "eip155:84532", fromAddress: "0xabc" }, result: RESULT.assetScorecard };
  const document = presentResult("asset_scorecard", "Asset Scorecard", response)!;
  assert.equal("paypal" in document.provenance, false, "so its fingerprint doesn't change");
  const pdf = Buffer.from(await renderResultPdf(document, "2026-11-05T00:00:00.000Z")).toString("latin1");
  assert.ok(!pdf.includes("PayPal"));
});

test("an internal policy code never reaches the customer", () => {
  const withReason = (reason: string) => JSON.stringify(presentResult("policy_envelope", "Policy Envelope", {
    status: "completed",
    decisionId: "d",
    payment: { verified: true, transactionHash: `0x${"a".repeat(64)}`, chargedAmountUsdc: "0.25" },
    result: { policyMode: "capital_preservation", authorization: { status: "RESTRICTED", reason }, policyEnvelope: {} },
  }));
  assert.ok(!withReason("PHASE1_PROHIBITION_DOWNGRADED_TO_RESTRICTED").includes("PHASE1"));
  assert.ok(withReason("PHASE1_PROHIBITION_DOWNGRADED_TO_RESTRICTED").includes("treated as restricted"));
  assert.ok(!withReason("SOME_NEW_RULE_CODE").includes("SOME_NEW"));
  assert.ok(withReason("Conditions are stressed.").includes("Conditions are stressed."));
});

test("a report issued on the first layout keeps its exact bytes", async () => {
  const response = { status: "completed", endpoint: "/agent/x402/asset-scorecard", decisionId: "d", payment: { verified: true, transactionHash: `0x${"a".repeat(64)}`, chargedAmountUsdc: "0.5", network: "eip155:84532", fromAddress: "0xabc" }, result: RESULT.assetScorecard };
  const document = presentResult("asset_scorecard", "Asset Scorecard", response)!;
  const at = "2026-11-05T00:00:00.000Z";
  const earliest = Buffer.from(await renderResultPdf(document, at));
  assert.deepEqual(Buffer.from(await renderResultPdf(document, at, undefined)), earliest, "no recorded layout means the first one");
  assert.deepEqual(Buffer.from(await renderResultPdf(document, at, 1)), earliest);
  assert.notDeepEqual(Buffer.from(await renderResultPdf(document, at, 2)), earliest, "new reports use the new layout");
});

test("purchase tokens only open purchases, and tampering fails", () => {
  const token = issuePurchaseToken("pp-1", "one_time");
  assert.deepEqual(readPurchaseToken(token), { id: "pp-1", kind: "one_time" });
  assert.equal(readPurchaseToken(`${token}x`), null);
  assert.equal(readPurchaseToken(issueReportToken("a", "b")), null, "a report token isn't a purchase token");
  assert.equal(readPurchaseToken(undefined), null);
});

test("PayPal products are offered only when PayPal is configured", () => {
  const catalog = { resources: [{ productId: "asset_scorecard", endpoint: "/x", pricing: { amountUsdc: "0.5" } }] };
  assert.equal(buildRegistry(catalog).some((entry) => entry.currency === "USD"), false);
  const withPayPal = buildRegistry(catalog, { reportUsd: "4.99", subscriptionUsd: "9.99" });
  assert.deepEqual(withPayPal.filter((entry) => entry.currency === "USD").map((entry) => [entry.id, entry.priceUsdc, entry.execution]), [
    ["comprehensive_report", "4.99", "hosted_checkout"],
    ["monthly_intelligence", "9.99", "hosted_checkout"],
  ]);
  assert.equal(withPayPal.find((entry) => entry.id === "asset_scorecard")?.execution, "x402_tool", "individual services stay on x402");
  assert.equal(withPayPal.some((entry) => entry.id.startsWith("allocate") && entry.currency === "USD"), false, "allocations aren't sold through PayPal");
  assert.ok(getToolSpec(COMPREHENSIVE_REPORT_ID)?.normalize({ assets: ["bitcoin"] }).ok);
  assert.equal(getToolSpec(COMPREHENSIVE_REPORT_ID)?.normalize({ assets: [] }).ok, false);
});

// ---- Through the conversation and the HTTP routes ----

type RouteHarness = ReturnType<typeof harness> & { url: string; server: Server; deps: ConstructorParameters<typeof AssistantService>[0] };

async function routeHarness(checkoutProviders: CheckoutProvider[] = []): Promise<RouteHarness> {
  const h = harness();
  const script = [
    { content: null, toolCalls: [{ id: "1", type: "function" as const, function: { name: "propose_comprehensive_report", arguments: JSON.stringify({ assets: ["BTC"] }) } }] },
    { content: "Here's the quote.", toolCalls: [] },
  ];
  const deps: ConstructorParameters<typeof AssistantService>[0] = {
    checkoutProviders,
    llm: { complete: async () => script.shift() ?? { content: "Here is a summary.", toolCalls: [] } },
    gateway: {
      requestChallenge: async () => { throw new Error("no wallet path should run"); },
      submitPaid: async () => { throw new Error("no wallet path should run"); },
    },
    loadCatalog: async () => ({ resources: [] }),
    paypal: { prices: { reportUsd: "4.99", subscriptionUsd: "9.99" }, createCheckout: (input) => h.service.createCheckout(input), createSubscription: (input) => h.service.createSubscription(input) },
  };
  const app = express();
  app.use(express.json({ verify: (req, _res, buffer) => { (req as typeof req & { rawBody?: string }).rawBody = buffer.toString("utf8"); } }));
  app.use("/assistant", createAssistantRouter(deps, {
    loadPaidResponse: (capabilityId, decisionId) => h.service.loadPaidResponse(capabilityId, decisionId),
    assertAccess: (capabilityId, decisionId) => h.service.assertReportAccess(capabilityId, decisionId),
    stateFile: stateFile(),
  }, h.service));
  const server = createServer(app);
  await new Promise<void>((resolve) => server.listen(0, resolve));
  return { ...h, deps, server, url: `http://127.0.0.1:${(server.address() as AddressInfo).port}/assistant` };
}

const post = (url: string, body: unknown, headers: Record<string, string> = {}) =>
  fetch(url, { method: "POST", headers: { "content-type": "application/json", ...headers }, body: JSON.stringify(body) });

test("a registered provider appears on the card and starts its own checkout, with nothing else changed", async () => {
  const started: CheckoutInput[] = [];
  const h = await routeHarness([{
    id: "newpay",
    label: "NewPay",
    note: () => "You'll go to NewPay to approve.",
    start: async (input) => { started.push(input); return { approveUrl: "https://newpay.test/go", kind: "one_time", id: "np-1" }; },
  }]);
  try {
    const chat = (await (await post(`${h.url}/chat`, { message: "A full report on Bitcoin" })).json()) as { conversationId: string; blocks: { type: string; action: { id: string; decisionId: string; paymentOptions: { id: string; label: string; flow: string; note: string }[] } }[] };
    const action = chat.blocks.find((block) => block.type === "action")!.action;
    assert.deepEqual(action.paymentOptions.map((option) => [option.id, option.flow]), [["paypal", "hosted_checkout"], ["newpay", "hosted_checkout"]]);
    assert.equal(action.paymentOptions[1].note, "You'll go to NewPay to approve.");
    const conversation = { "x-selun-conversation": chat.conversationId };

    const refused = await post(`${h.url}/actions/${action.id}/checkout`, { method: "unlisted", approved: true, approvedPrice: "4.99" }, conversation);
    assert.equal(refused.status, 400);
    assert.equal(started.length, 0);

    const reply = await post(`${h.url}/actions/${action.id}/checkout`, { method: "newpay", approved: true, approvedPrice: "4.99" }, conversation);
    assert.equal(reply.status, 200);
    const body = await reply.json() as { approveUrl: string; method: string; kind: string };
    assert.deepEqual([body.approveUrl, body.method, body.kind], ["https://newpay.test/go", "newpay", "one_time"]);
    assert.equal(started[0].purchaseKey, action.decisionId);
    assert.equal(h.api.orders.size, 0, "PayPal wasn't touched");
  } finally {
    await new Promise((resolve) => h.server.close(resolve));
  }
});

test("the full purchase through the routes: quote, checkout, return, result, PDF", async () => {
  const h = await routeHarness();
  try {
    const chat = (await (await post(`${h.url}/chat`, { message: "I want a full report on Bitcoin, I'll pay with PayPal" })).json()) as { conversationId: string; blocks: { type: string; action: { id: string; priceUsdc: string; currency: string; execution: string; decisionId: string; paymentOptions: { id: string }[]; billing: string } }[] };
    const action = chat.blocks.find((block) => block.type === "action")!.action;
    assert.equal(action.priceUsdc, "4.99");
    assert.equal(action.currency, "USD");
    assert.equal(action.execution, "hosted_checkout");
    assert.deepEqual(action.paymentOptions.map((option) => option.id), ["paypal"]);
    assert.equal(action.billing, "once");
    const conversation = { "x-selun-conversation": chat.conversationId };

    // The wallet path is closed for this product, and approval is required.
    assert.equal((await post(`${h.url}/actions/${action.id}/challenge`, {}, conversation)).status, 400);
    assert.equal((await post(`${h.url}/actions/${action.id}/checkout`, {}, conversation)).status, 400);
    assert.equal((await post(`${h.url}/actions/${action.id}/checkout`, { method: "paypal", approved: true, approvedPrice: "0.01" }, conversation)).status, 409, "a lower price is refused");
    assert.equal(h.api.orders.size, 0);

    const started = (await (await post(`${h.url}/actions/${action.id}/checkout`, { method: "paypal", approved: true, approvedPrice: "4.99" }, conversation)).json()) as { approveUrl: string; purchaseToken: string };
    assert.match(started.approveUrl, /approve\/ORDER-1/);
    assert.equal(h.runs.length, 0, "starting checkout runs nothing");
    assert.equal(h.api.orders.get("ORDER-1")?.amount, "4.99", "the server's price, not the browser's");

    assert.equal((await post(`${h.url}/paypal/capture`, { purchaseToken: "forged" })).status, 403);
    assert.equal((await post(`${h.url}/paypal/capture`, { purchaseToken: started.purchaseToken })).status, 409, "returning without approving charges nothing");

    h.api.approve();
    const captured = (await (await post(`${h.url}/paypal/capture`, { purchaseToken: started.purchaseToken })).json()) as { phase: string };
    assert.ok(captured.phase === "running" || captured.phase === "completed");
    await flush();
    await new Promise((resolve) => setTimeout(resolve, 20));

    const done = (await (await post(`${h.url}/paypal/status`, { purchaseToken: started.purchaseToken })).json()) as { phase: string; reportToken: string; document: { provenance: { decisionId: string } }; explanation: string | null };
    assert.equal(done.phase, "completed");
    assert.equal(done.document.provenance.decisionId, action.decisionId);
    assert.equal(h.runs.length, 1);
    assert.deepEqual(h.runs[0].input, INPUT, "the server's normalized input");

    const pdf = await post(`${h.url}/report/download`, { reportToken: done.reportToken });
    assert.equal(pdf.status, 200);
    assert.equal(pdf.headers.get("content-type"), "application/pdf");
    assert.equal(Buffer.from(await pdf.arrayBuffer()).subarray(0, 5).toString(), "%PDF-");

    // Checkout can't be restarted once paid.
    assert.equal((await post(`${h.url}/actions/${action.id}/checkout`, { method: "paypal", approved: true, approvedPrice: "4.99" }, conversation)).status, 409);

    h.api.remoteCapture.set(CAPTURE, "REFUNDED");
    const webhookResponse = await fetch(`${h.url}/paypal/webhook`, { method: "POST", headers: { "content-type": "application/json" }, body: webhook("r1", "PAYMENT.CAPTURE.REVERSED", { id: CAPTURE }) });
    assert.equal(webhookResponse.status, 200);
    await h.service.drainEvents();
    assert.equal((await post(`${h.url}/report/download`, { reportToken: done.reportToken })).status, 403);
    assert.equal(purchaseIdFor(action.decisionId).startsWith("pp-"), true);
  } finally {
    await new Promise((resolve) => h.server.close(resolve));
  }
});

test("a forged webhook is refused at the route", async () => {
  const h = await routeHarness();
  try {
    h.api.verified = false;
    const response = await fetch(`${h.url}/paypal/webhook`, { method: "POST", headers: { "content-type": "application/json" }, body: webhook("x", "PAYMENT.CAPTURE.REVERSED", { id: CAPTURE }) });
    assert.equal(response.status, 401);
    assert.equal(h.api.verifyCalls, 1);
  } finally {
    await new Promise((resolve) => h.server.close(resolve));
  }
});
