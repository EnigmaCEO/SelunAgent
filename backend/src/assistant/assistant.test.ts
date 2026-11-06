import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import test from "node:test";
import express from "express";
import { encodePaymentRequiredHeader, encodePaymentSignatureHeader } from "@x402/core/http";
import { createAssistantRouter, WindowRateLimiter } from "./assistant.routes";
import { AssistantError, AssistantService, buildSystemPrompt, friendlyPaymentRejection, type ActionView, type AssistantLimits } from "./assistant.service";
import { findAssetMentions, resolveAsset } from "./asset-resolver";
import { buildRegistry, getToolSpec, TOOL_CAPABILITY_IDS, type CapabilityCatalog } from "./capability-registry";
import { SpendLedger } from "./commerce";
import { LlmError, OpenAiClient, readOpenAiConfig, type ChatMessage, type LlmClient, type LlmRequest, type LlmResponse } from "./openai-client";
import { presentResult } from "./result-presenter";
import { classifyPaidFailure, HttpX402Gateway, type ChallengeOutcome, type PaidOutcome, type X402Gateway } from "./x402-gateway";

const PAYER = "0xfe2d5E9c5aE6E48B7F8b0b82AC4dE8B423bA0557";
const TX = `0x${"a".repeat(64)}`;

function catalog(prices: Partial<Record<string, string>> = {}): CapabilityCatalog {
  const base: Record<string, string> = {
    market_regime: "0.25",
    policy_envelope: "0.25",
    asset_scorecard: "0.5",
    rebalance: "1",
    sce_continuity_mode: "0.01",
    sce_case_relevance: "0.05",
    sce_risk_evaluate: "0.25",
    allocate: "19",
    allocate_with_report: "34",
    ...prices,
  } as Record<string, string>;
  return {
    resources: Object.entries(base).map(([productId, amountUsdc]) => ({
      productId,
      endpoint: `/agent/x402/${productId.replaceAll("_", "-")}`,
      pricing: { amountUsdc },
    })),
  };
}

function toolCall(name: string, args: unknown, id = `call_${name}`) {
  return { id, type: "function" as const, function: { name, arguments: JSON.stringify(args) } };
}

/** Scripted model. Each entry answers one model call and sees the request. */
class ScriptedLlm implements LlmClient {
  readonly requests: LlmRequest[] = [];
  constructor(private readonly script: (LlmResponse | Error)[]) {}
  async complete(request: LlmRequest): Promise<LlmResponse> {
    this.requests.push(request);
    const next = this.script.shift() ?? { content: "Done.", toolCalls: [] };
    if (next instanceof Error) throw next;
    return next;
  }
}

function completedData(capabilityId: string, result: Record<string, unknown>, verified = true): PaidOutcome {
  return {
    kind: "completed",
    data: {
      status: "completed",
      endpoint: `/agent/x402/${capabilityId}`,
      decisionId: "selun-ai-test",
      productId: capabilityId,
      payment: { required: true, chargedAmountUsdc: "0.25", verified, fromAddress: PAYER, transactionHash: TX, network: "eip155:84532" },
      result,
    },
  };
}

const MARKET_RESULT = {
  snapshotAt: "2026-11-04T10:00:00.000Z",
  regime: "defensive",
  marketCondition: { volatility_state: "elevated", liquidity_state: "stable", risk_appetite: "defensive", sentiment_direction: -0.3, confidence: 0.74, uncertainty: 0.26 },
  authorization: { status: "DEFERRED", confidence: 0.6, justification: ["market_regime:NEUTRAL_TRANSITION", "allocation_activity_deferred_pending_phase_3_asset_checks"] },
  policyMode: "balanced_defensive",
  sourceSummary: { sourceCount: 5, dataFreshness: "2026-11-04T22:15:46.479Z", missingDomains: ["asset_evaluation_deferred_to_phase_3", "missing_btcusdt_binance_klines"] },
};

class FakeGateway implements X402Gateway {
  challenges: { endpoint: string; body: Record<string, unknown> }[] = [];
  paid: { endpoint: string; body: Record<string, unknown>; signature: string }[] = [];
  challengeAmount: string | null = null;
  challengeOutcome: ChallengeOutcome | null = null;
  paidOutcome: PaidOutcome | (() => PaidOutcome) = completedData("market_regime", MARKET_RESULT);

  async requestChallenge(endpoint: string, body: Record<string, unknown>): Promise<ChallengeOutcome> {
    this.challenges.push({ endpoint, body });
    if (this.challengeOutcome) return this.challengeOutcome;
    return {
      kind: "challenge",
      paymentRequired: {
        x402Version: 2,
        resource: { url: endpoint, description: "", mimeType: "application/json" },
        accepts: [
          { scheme: "exact", network: "eip155:84532", amount: this.challengeAmount ?? "250000", asset: "0xusdc", payTo: "0xseller", maxTimeoutSeconds: 60, extra: {} },
          { scheme: "exact", network: "solana:mainnet", amount: "250000", asset: "usdc", payTo: "sol", maxTimeoutSeconds: 60, extra: {} },
        ],
      } as never,
    };
  }

  async submitPaid(endpoint: string, body: Record<string, unknown>, signature: string): Promise<PaidOutcome> {
    this.paid.push({ endpoint, body, signature });
    return typeof this.paidOutcome === "function" ? this.paidOutcome() : this.paidOutcome;
  }
}

function signature(amount = "250000", payer = PAYER): string {
  return encodePaymentSignatureHeader({
    x402Version: 2,
    accepted: { scheme: "exact", network: "eip155:84532", amount, asset: "0xusdc", payTo: "0xseller", maxTimeoutSeconds: 60, extra: {} },
    payload: { authorization: { from: payer }, signature: "0xsig" },
  } as never);
}

function makeService(script: (LlmResponse | Error)[], options: { limits?: Partial<AssistantLimits>; catalog?: () => Promise<CapabilityCatalog> } = {}) {
  const llm = new ScriptedLlm(script);
  const gateway = new FakeGateway();
  const service = new AssistantService({
    llm,
    gateway,
    loadCatalog: options.catalog ?? (async () => catalog()),
    limits: { catalogTtlMs: 0, ...options.limits },
  });
  return { llm, gateway, service };
}

async function proposeMarketRegime(service: AssistantService) {
  const reply = await service.chat({ message: "Give me a market update" });
  const block = reply.blocks.find((entry) => entry.type === "action");
  assert.ok(block && block.type === "action");
  return { conversationId: reply.conversationId, action: block.action };
}

const MARKET_SCRIPT = (): LlmResponse[] => [
  { content: null, toolCalls: [toolCall("propose_market_regime", {})] },
  { content: "This covers the whole market. The card shows the price.", toolCalls: [] },
];

async function waitForState(service: AssistantService, conversationId: string, actionId: string, check: (view: ActionView) => boolean) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const view = service.getAction(conversationId, actionId);
    if (check(view)) return view;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("Timed out waiting for action state.");
}

// ---- Asset resolution ----

test("resolves common names and symbols to the correct API parameter", () => {
  for (const [query, symbol] of [["Bitcoin", "BTC"], ["BTC", "BTC"], ["btc", "BTC"], ["$BTC", "BTC"], ["bitcoin price", "BTC"], ["Ethereum", "ETH"], ["ETH", "ETH"], ["ether", "ETH"], ["the ethereum token", "ETH"]] as const) {
    const resolution = resolveAsset(query);
    assert.equal(resolution.status, "resolved", query);
    assert.equal(resolution.status === "resolved" && resolution.asset.symbol, symbol, query);
  }
});

test("longer asset names are not collapsed into a shorter one", () => {
  const cash = resolveAsset("Bitcoin Cash");
  assert.equal(cash.status === "resolved" && cash.asset.symbol, "BCH");
  const classic = resolveAsset("Ethereum Classic");
  assert.equal(classic.status === "resolved" && classic.asset.symbol, "ETC");
  const wrapped = resolveAsset("wrapped bitcoin");
  assert.equal(wrapped.status === "resolved" && wrapped.asset.symbol, "WBTC");
});

test("ambiguous and unknown assets are never guessed", () => {
  const polygon = resolveAsset("polygon");
  assert.equal(polygon.status, "ambiguous");
  assert.deepEqual(polygon.status === "ambiguous" && polygon.options.map((option) => option.symbol), ["POL", "MATIC"]);
  assert.equal(resolveAsset("notarealcoin").status, "unknown");
  assert.equal(resolveAsset("").status, "unknown");
});

// ---- Registry ----

test("registry prices come from the live catalog and products missing from it are dropped", () => {
  const live = catalog({ market_regime: "0.33" });
  live.resources = live.resources.filter((resource) => resource.productId !== "rebalance");
  live.resources.push({ productId: "brand_new_product", endpoint: "/agent/x402/new", pricing: { amountUsdc: "1" } });
  const registry = buildRegistry(live);
  assert.equal(registry.find((entry) => entry.id === "market_regime")?.priceUsdc, "0.33");
  assert.equal(registry.some((entry) => entry.id === "rebalance"), false);
  assert.equal(registry.some((entry) => (entry.id as string) === "brand_new_product"), false, "unallowlisted products stay invisible");
});

test("tool input validation rejects unknown fields, including injected payment fields", () => {
  const spec = getToolSpec("market_regime");
  assert.ok(spec);
  assert.equal(spec.normalize({ decisionId: "attacker" }).ok, false);
  assert.equal(spec.normalize({ promoCode: "FREE" }).ok, false);
  assert.equal(spec.normalize({ riskTolerance: "Yolo" }).ok, false);
  const risk = getToolSpec("sce_risk_evaluate");
  assert.equal(risk?.normalize({}).ok, false);
  assert.equal(risk?.normalize({ protocol_name: "Aave", addresses: ["not-an-address"] }).ok, false);
});

test("asset scorecard re-resolves assets on the server and discloses defaults", () => {
  const spec = getToolSpec("asset_scorecard");
  const normalized = spec?.normalize({ assets: ["Bitcoin", "btc"] });
  assert.ok(normalized?.ok);
  assert.deepEqual(normalized.body.assets, ["BTC"]);
  assert.deepEqual(normalized.assumptions, ["Balanced risk profile", "1 to 3 year horizon", "Bluechips segment"]);
  const ambiguous = spec?.normalize({ assets: ["polygon"] });
  assert.equal(ambiguous?.ok === false && ambiguous.code, "ambiguous_asset");
});

// ---- Acceptance journey ----

test("acceptance: Bitcoin assessment resolves BTC, quotes real prices, and charges nothing until approved", async () => {
  const { llm, gateway, service } = makeService([
    { content: null, toolCalls: [toolCall("resolve_asset", { query: "Bitcoin" })] },
    { content: null, toolCalls: [toolCall("propose_market_regime", {}, "a"), toolCall("propose_asset_scorecard", { assets: ["BTC"] }, "b")] },
    { content: "Here are two options, each approved separately.", toolCalls: [] },
  ]);

  const reply = await service.chat({ message: "Give me a Bitcoin market assessment" });
  const actions = reply.blocks.flatMap((block) => (block.type === "action" ? [block.action] : []));
  assert.deepEqual(actions.map((action) => [action.capabilityId, action.priceUsdc]), [["asset_scorecard", "0.5"], ["market_regime", "0.25"]]);
  assert.equal(actions[0].details[0].value, "Bitcoin (BTC)");
  assert.ok(actions.every((action) => action.state === "awaiting_approval" && action.document === null));
  assert.equal(gateway.paid.length, 0, "no paid call without approval");
  assert.equal(gateway.challenges.length, 0, "proposing does not even touch the paid route");

  const toolMessage = llm.requests[1].messages.find((message) => message.role === "tool");
  assert.ok(toolMessage && toolMessage.role === "tool");
  assert.deepEqual(JSON.parse(toolMessage.content), { status: "resolved", symbol: "BTC", name: "Bitcoin" });
});

test("approved action runs through the x402 route and the result comes from the route, not the model", async () => {
  const { llm, gateway, service } = makeService([
    ...MARKET_SCRIPT(),
    { content: "The market looks cautious right now.", toolCalls: [] },
  ]);
  const { conversationId, action } = await proposeMarketRegime(service);

  const prepared = await service.prepareApproval(conversationId, action.id);
  assert.equal(gateway.challenges[0].endpoint, "/agent/x402/market-regime");
  assert.equal(gateway.challenges[0].body.decisionId, action.decisionId);
  assert.deepEqual(prepared.paymentRequired.accepts.map((accept) => accept.network), ["eip155:84532"], "only EVM options reach the browser");

  const sig = signature();
  const started = await service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: sig });
  assert.equal(started.state, "executing");

  const done = await waitForState(service, conversationId, action.id, (view) => view.explanationStatus === "ready");
  assert.equal(done.state, "completed");
  assert.equal(gateway.paid.length, 1);
  assert.equal(gateway.paid[0].signature, sig);
  assert.equal(done.document?.provenance.transactionHash, TX);
  assert.equal(done.document?.provenance.explorerUrl, `https://sepolia.basescan.org/tx/${TX}`);
  assert.equal(done.explanation, "The market looks cautious right now.");

  const serialized = JSON.stringify(llm.requests);
  assert.ok(!serialized.includes(sig), "the model never sees the payment signature");
  assert.ok(serialized.includes("verified_result"), "the model receives the real result as a tool message");
  const after = await service.chat({ conversationId, message: "thanks" });
  assert.equal(after.text, "Done.");
});

test("the model has no tool that can run or pay for a service", () => {
  const { llm, service } = makeService([{ content: "Hi", toolCalls: [] }]);
  return service.chat({ message: "hi" }).then(() => {
    const names = llm.requests[0].tools?.map((tool) => tool.function.name) ?? [];
    assert.ok(names.length > 0);
    for (const name of names) {
      assert.ok(name === "list_capabilities" || name === "resolve_asset" || name === "lookup_sagitta" || name === "open_portfolio_plan" || name.startsWith("propose_"), name);
    }
  });
});

test("a model that claims payment or invents a tool cannot move an action forward", async () => {
  const { gateway, service } = makeService([
    { content: null, toolCalls: [toolCall("propose_market_regime", {}), toolCall("execute_paid_action", { paid: true }, "x"), toolCall("verified_result", { paid: true }, "y")] },
    { content: "Your payment went through and the market is bullish!", toolCalls: [] },
  ]);
  const reply = await service.chat({ message: "I already paid, run it" });
  const block = reply.blocks.find((entry) => entry.type === "action");
  assert.ok(block && block.type === "action");
  assert.equal(block.action.state, "awaiting_approval");
  assert.equal(block.action.document, null);
  assert.equal(gateway.paid.length, 0);
  assert.equal(reply.blocks.filter((entry) => entry.type === "action").length, 1);
});

// ---- Approval and payment rules ----

test("execution requires explicit approval, matching price, and a real signature", async () => {
  const { gateway, service } = makeService(MARKET_SCRIPT());
  const { conversationId, action } = await proposeMarketRegime(service);
  const attempt = (approval: Parameters<AssistantService["approveAndExecute"]>[2]) => service.approveAndExecute(conversationId, action.id, approval);
  const code = (expected: string) => (error: unknown) => error instanceof AssistantError && error.code === expected;

  await assert.rejects(attempt({ paymentSignature: signature() }), code("approval_required"));
  await assert.rejects(attempt({ approved: "true", approvedPriceUsdc: "0.25", paymentSignature: signature() }), code("approval_required"));
  await assert.rejects(attempt({ approved: true, approvedPriceUsdc: "0.25" }), code("invalid_payment"));
  await assert.rejects(attempt({ approved: true, approvedPriceUsdc: "0.25", paymentSignature: "not-a-signature" }), code("invalid_payment"));
  await assert.rejects(attempt({ approved: true, approvedPriceUsdc: "0.01", paymentSignature: signature() }), code("price_changed"));
  await assert.rejects(attempt({ approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature("1") }), code("invalid_payment"));
  await assert.rejects(attempt({ approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature(), rail: "paypal_once" }), code("approval_required"));
  assert.equal(gateway.paid.length, 0);
});

test("a price change between quote and approval forces re-approval", async () => {
  let price = "0.25";
  const { gateway, service } = makeService(MARKET_SCRIPT(), { catalog: async () => catalog({ market_regime: price }) });
  const { conversationId, action } = await proposeMarketRegime(service);
  price = "0.75";
  await assert.rejects(service.prepareApproval(conversationId, action.id), (error: unknown) => {
    assert.ok(error instanceof AssistantError);
    assert.equal(error.code, "price_changed");
    assert.equal((error.extra.action as ActionView).priceUsdc, "0.75");
    return true;
  });
  await assert.rejects(
    service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() }),
    (error: unknown) => error instanceof AssistantError && error.code === "price_changed",
  );
  assert.equal(gateway.challenges.length, 0);
  assert.equal(gateway.paid.length, 0);
});

test("a challenge that disagrees with the catalog price fails closed", async () => {
  const { gateway, service } = makeService(MARKET_SCRIPT());
  const { conversationId, action } = await proposeMarketRegime(service);
  gateway.challengeAmount = "999000000";
  gateway.challengeOutcome = null;
  await assert.rejects(service.prepareApproval(conversationId, action.id), (error: unknown) => error instanceof AssistantError && error.code === "service_unavailable");
});

test("an action can only execute once while running or after completion", async () => {
  const { gateway, service } = makeService([...MARKET_SCRIPT(), { content: "Explained.", toolCalls: [] }]);
  const { conversationId, action } = await proposeMarketRegime(service);
  const approval = { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() };
  await service.approveAndExecute(conversationId, action.id, approval);
  await assert.rejects(service.approveAndExecute(conversationId, action.id, approval), (error: unknown) => error instanceof AssistantError && error.code === "action_not_payable");
  await waitForState(service, conversationId, action.id, (view) => view.explanationStatus === "ready");
  await assert.rejects(service.approveAndExecute(conversationId, action.id, approval), (error: unknown) => error instanceof AssistantError && error.code === "action_not_payable");
  assert.equal(gateway.paid.length, 1);
});

test("declined actions cannot be paid, and other conversations cannot reach an action", async () => {
  const { service } = makeService([...MARKET_SCRIPT(), { content: "Hello", toolCalls: [] }, { content: "OK", toolCalls: [] }]);
  const { conversationId, action } = await proposeMarketRegime(service);
  const other = await service.chat({ message: "hello" });
  assert.throws(() => service.getAction(other.conversationId, action.id), (error: unknown) => error instanceof AssistantError && error.code === "action_not_found");
  assert.throws(() => service.getAction(undefined, action.id), (error: unknown) => error instanceof AssistantError && error.code === "conversation_not_found");

  assert.equal(service.decline(conversationId, action.id).state, "declined");
  await assert.rejects(service.prepareApproval(conversationId, action.id), (error: unknown) => error instanceof AssistantError && error.code === "action_not_payable");
  await service.chat({ conversationId, message: "never mind" });
});

// ---- Payment outcomes ----

test("a route response without a verified receipt is not shown as a result", async () => {
  const { gateway, service } = makeService(MARKET_SCRIPT());
  gateway.paidOutcome = completedData("market_regime", MARKET_RESULT, false);
  const { conversationId, action } = await proposeMarketRegime(service);
  await service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() });
  const view = await waitForState(service, conversationId, action.id, (entry) => entry.state === "failed");
  assert.equal(view.document, null);
  assert.equal(view.failure?.mayHaveCharged, true);
});

test("a rejected payment says not charged, releases the spend, and can be retried", async () => {
  const { gateway, service } = makeService([...MARKET_SCRIPT(), { content: "Explained.", toolCalls: [] }], { limits: { sessionLimitUsdc: "0.25" } });
  gateway.paidOutcome = { kind: "payment_rejected", message: "invalid_exact_evm_insufficient_funds" };
  const { conversationId, action } = await proposeMarketRegime(service);
  await service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() });
  const failed = await waitForState(service, conversationId, action.id, (entry) => entry.state === "failed");
  assert.equal(failed.failure?.mayHaveCharged, false);
  assert.match(failed.failure?.message ?? "", /enough USDC.*not been charged/);

  gateway.paidOutcome = completedData("market_regime", MARKET_RESULT);
  await service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() });
  const done = await waitForState(service, conversationId, action.id, (entry) => entry.explanationStatus === "ready");
  assert.equal(done.state, "completed");
  assert.equal(gateway.paid[0].body.decisionId, gateway.paid[1].body.decisionId, "retries reuse the decisionId, which prevents double charges");
});

test("after a possible charge, later failures never claim 'not charged'", async () => {
  const { gateway, service } = makeService(MARKET_SCRIPT());
  gateway.paidOutcome = { kind: "failed", status: 500, message: "execute failed", mayHaveCharged: true };
  const { conversationId, action } = await proposeMarketRegime(service);
  const approval = { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() };
  await service.approveAndExecute(conversationId, action.id, approval);
  const failed = await waitForState(service, conversationId, action.id, (entry) => entry.state === "failed");
  assert.equal(failed.failure?.mayHaveCharged, true);
  assert.equal(failed.failure?.retryable, true);

  gateway.paidOutcome = { kind: "payment_rejected", message: "quote expired" };
  await service.approveAndExecute(conversationId, action.id, approval);
  const second = await waitForState(service, conversationId, action.id, (entry) => entry.state === "failed");
  assert.equal(second.failure?.mayHaveCharged, true);
  assert.doesNotMatch(second.failure?.message ?? "", /not been charged/);
});

test("classifies route failures by whether money could have moved", () => {
  assert.equal(classifyPaidFailure(502, { error: "x402 verification mismatch: bad" }).kind, "payment_rejected");
  assert.equal(classifyPaidFailure(502, { error: "x402 settlement failed: nope" }).kind, "payment_rejected");
  assert.equal(classifyPaidFailure(500, { error: "sce_upstream_timeout" }).kind, "failed");
  assert.equal(classifyPaidFailure(502, { error: "x402 settlement completed without a transaction hash." }).kind, "failed");
  assert.match(friendlyPaymentRejection("whatever"), /not been charged/);
  assert.doesNotMatch(friendlyPaymentRejection("x402 settlement failed: Missing or invalid parameters. validBefore: 123"), /expired/, "parameter dumps don't imply expiry");
  assert.match(friendlyPaymentRejection("invalid_exact_evm_payload_authorization_valid_before"), /expired/);
});

// ---- Spending limits ----

test("spending limits are enforced per action, per conversation, and per wallet", async () => {
  const code = (expected: string) => (error: unknown) => error instanceof AssistantError && error.code === "spending_limit" && error.extra.reason === expected;

  const tooBig = makeService(MARKET_SCRIPT(), { limits: { maxActionUsdc: "0.10" } });
  const first = await proposeMarketRegime(tooBig.service);
  await assert.rejects(tooBig.service.prepareApproval(first.conversationId, first.action.id), code("action_limit"));

  const ledger = new SpendLedger({ maxActionUsdc: "5", sessionLimitUsdc: "0.5", walletDailyLimitUsdc: "0.6" });
  assert.equal(ledger.reserve("s1", "a1", "0.25", PAYER), null);
  assert.equal(ledger.reserve("s1", "a2", "0.25", PAYER), null);
  assert.equal(ledger.reserve("s1", "a3", "0.25", PAYER), "session_limit");
  assert.equal(ledger.reserve("s2", "a4", "0.25", PAYER), "wallet_daily_limit", "a new conversation can't evade the wallet limit");
  assert.equal(ledger.reserve("s1", "a2", "0.25", PAYER), null, "re-reserving the same action doesn't double count");
  assert.equal(ledger.sessionSpent("s1"), "0.5");
  ledger.release("s1", "a2", PAYER);
  assert.equal(ledger.sessionSpent("s1"), "0.25");
});

test("quotes disclose when a limit would block approval", async () => {
  const { llm, service } = makeService(MARKET_SCRIPT(), { limits: { sessionLimitUsdc: "0.10" } });
  await proposeMarketRegime(service);
  const tool = llm.requests[1].messages.find((message): message is Extract<ChatMessage, { role: "tool" }> => message.role === "tool");
  assert.match(tool?.content ?? "", /spending_limit_note/);
});

// ---- Graceful failure ----

test("unsupported requests produce no action, and unavailable catalogs produce no price", async () => {
  const unsupported = makeService([{ content: "Selun can't place trades.", toolCalls: [] }]);
  const reply = await unsupported.service.chat({ message: "buy me 1 BTC" });
  assert.deepEqual(reply.blocks, []);

  const down = makeService(
    [{ content: null, toolCalls: [toolCall("propose_market_regime", {})] }, { content: "It's unavailable right now.", toolCalls: [] }],
    { catalog: async () => { throw new Error("cdp down"); } },
  );
  const downReply = await down.service.chat({ message: "market update" });
  assert.equal(downReply.blocks.some((block) => block.type === "action"), false);
  assert.ok(downReply.blocks.some((block) => block.type === "notice"));
  const tool = down.llm.requests[1].messages.find((message): message is Extract<ChatMessage, { role: "tool" }> => message.role === "tool");
  assert.match(tool?.content ?? "", /service_unavailable/);
});

test("ambiguous assets offer choices and create no quote", async () => {
  const { service } = makeService([
    { content: null, toolCalls: [toolCall("propose_asset_scorecard", { assets: ["polygon"] })] },
    { content: "Which Polygon token do you mean?", toolCalls: [] },
  ]);
  const reply = await service.chat({ message: "analyze polygon" });
  assert.equal(reply.blocks.some((block) => block.type === "action"), false);
  const choice = reply.blocks.find((block) => block.type === "asset_choice");
  assert.ok(choice && choice.type === "asset_choice");
  assert.deepEqual(choice.options.map((option) => option.symbol), ["POL", "MATIC"]);
});

test("a model failure rolls back the turn and returns a friendly error", async () => {
  const { service } = makeService([
    new LlmError("rate_limited", "429"),
    { content: "Back online.", toolCalls: [] },
  ]);
  await assert.rejects(service.chat({ message: "hello" }), (error: unknown) => error instanceof AssistantError && error.code === "assistant_busy");
  const reply = await service.chat({ message: "hello again" });
  assert.equal(reply.text, "Back online.");
});

test("a runaway tool loop stops at the round limit", async () => {
  const looping = Array.from({ length: 10 }, (_, index) => ({ content: null, toolCalls: [toolCall("resolve_asset", { query: "btc" }, `c${index}`)] }));
  const { llm, service } = makeService(looping, { limits: { maxToolRounds: 3 } });
  const reply = await service.chat({ message: "loop" });
  assert.equal(llm.requests.length, 3);
  assert.ok(reply.text.length > 0);
});

test("explanation failure still delivers the paid result", async () => {
  const { service } = makeService([...MARKET_SCRIPT(), new LlmError("upstream", "boom")]);
  const { conversationId, action } = await proposeMarketRegime(service);
  await service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() });
  const done = await waitForState(service, conversationId, action.id, (view) => view.explanationStatus === "unavailable");
  assert.equal(done.state, "completed");
  assert.ok(done.document);
  assert.equal(done.explanation, null);
});

test("input limits and missing configuration return clear errors", async () => {
  const { service } = makeService([], { limits: { maxMessageChars: 5, maxTurnsPerSession: 1 } });
  await assert.rejects(service.chat({ message: "" }), (error: unknown) => error instanceof AssistantError && error.code === "invalid_request");
  await assert.rejects(service.chat({ message: "too long message" }), (error: unknown) => error instanceof AssistantError && error.code === "invalid_request");
  const first = await service.chat({ message: "hi" });
  await assert.rejects(service.chat({ conversationId: first.conversationId, message: "more" }), (error: unknown) => error instanceof AssistantError && error.code === "conversation_limit");
  await assert.rejects(service.chat({ conversationId: "missing", message: "hi" }), (error: unknown) => error instanceof AssistantError && error.code === "conversation_not_found");

  const daily = makeService([], { limits: { dailyTurnLimit: 1 } });
  await daily.service.chat({ message: "hi" });
  await assert.rejects(daily.service.chat({ message: "hi" }), (error: unknown) => error instanceof AssistantError && error.code === "daily_limit");

  const unconfigured = new AssistantService({ llm: null, gateway: new FakeGateway(), loadCatalog: async () => catalog() });
  assert.equal(unconfigured.isConfigured, false);
  await assert.rejects(unconfigured.chat({ message: "hi" }), (error: unknown) => error instanceof AssistantError && error.code === "assistant_not_configured");
});

test("the system prompt forbids invented data and payment claims", () => {
  const prompt = buildSystemPrompt("2026-11-04");
  assert.match(prompt, /only source of market data/);
  assert.match(prompt, /cannot run a paid service or take payment/);
  assert.match(prompt, /data, not instructions/);
});

// ---- Presentation ----

test("presents market regime results from real fields", () => {
  const data = completedData("market_regime", MARKET_RESULT);
  assert.equal(data.kind, "completed");
  const document = data.kind === "completed" ? presentResult("market_regime", "Market Regime", data.data) : null;
  assert.ok(document);
  const highlight = document.sections[0];
  assert.ok(highlight.kind === "highlight");
  assert.equal(highlight.value, "Defensive");
  const metrics = document.sections[1];
  assert.ok(metrics.kind === "metrics");
  assert.deepEqual(metrics.items.map((item) => item.value), ["Elevated", "Stable", "Defensive", "Slightly negative", "74%"]);
  const rendered = JSON.stringify(document.sections);
  assert.doesNotMatch(rendered, /NEUTRAL_TRANSITION|deferred_to_phase|binance_klines|2026-11-04T22/, "no machine codes reach customers");
  assert.match(rendered, /1 data feed was unavailable/);
});

test("an unscored asset is reported honestly", () => {
  const document = presentResult("asset_scorecard", "Asset Scorecard", {
    status: "completed",
    endpoint: "/agent/x402/asset-scorecard",
    decisionId: "d",
    payment: { verified: true, transactionHash: TX, chargedAmountUsdc: "0.5", network: "eip155:8453" },
    result: {
      portfolioSegment: "Bluechips",
      assetsRequested: ["BTC", "PEPE"],
      qualifiedCandidatesCount: 10,
      screenedCandidatesCount: 40,
      scorecards: [{ asset: "BTC", name: "Bitcoin", compositeScore: 0.91, qualityScore: 0.9, riskScore: 0.3, liquidityScore: 0.99, structuralScore: 0.95, role: "core", riskClass: "large_cap_crypto", selected: true, selectionReasons: [] }],
    },
  });
  assert.ok(document);
  assert.equal(document.provenance.explorerUrl, `https://basescan.org/tx/${TX}`);
  const card = document.sections[0];
  assert.ok(card.kind === "metrics");
  assert.equal(card.title, "Bitcoin (BTC)");
  assert.equal(card.items[0].value, "91 / 100");
  assert.ok(document.sections.some((section) => section.kind === "note" && section.text.startsWith("PEPE did not appear")));
});

test("presenters handle every registered tool capability and partial data without throwing", () => {
  for (const id of ["market_regime", "policy_envelope", "asset_scorecard", "rebalance", "sce_continuity_mode", "sce_risk_evaluate", "sce_case_relevance"]) {
    const document = presentResult(id, id, {
      status: "completed",
      payment: { verified: true, transactionHash: "0xabc" },
      result: {},
    });
    assert.ok(document, id);
    assert.equal(document.provenance.explorerUrl, null, "malformed hashes get no explorer link");
  }
  assert.equal(presentResult("unknown", "x", { status: "completed", payment: { verified: true, transactionHash: TX }, result: {} }), null);
});

// ---- OpenAI client ----

test("OpenAI client sends the configured model and server-side key, and maps failures", async () => {
  const config = readOpenAiConfig({ OPENAI_API_KEY: "sk-test", OPENAI_MODEL: "test-model", OPENAI_BASE_URL: "https://llm.example/v1/" } as NodeJS.ProcessEnv);
  assert.ok(config);
  assert.equal(readOpenAiConfig({ OPENAI_API_KEY: "sk-test" } as NodeJS.ProcessEnv), null, "the model must come from configuration");

  let seen: { url: string; authorization: string | null; body: Record<string, unknown> } | undefined;
  const ok = new OpenAiClient(config, (async (url: string, init: RequestInit) => {
    seen = { url, authorization: new Headers(init.headers).get("authorization"), body: JSON.parse(String(init.body)) };
    return new Response(JSON.stringify({ choices: [{ message: { content: "Hello", tool_calls: [toolCall("resolve_asset", {})] } }] }), { status: 200 });
  }) as typeof fetch);
  const response = await ok.complete({ messages: [{ role: "user", content: "hi" }] });
  assert.equal(response.content, "Hello");
  assert.equal(response.toolCalls[0].function.name, "resolve_asset");
  assert.equal(seen?.url, "https://llm.example/v1/chat/completions");
  assert.equal(seen?.authorization, "Bearer sk-test");
  assert.equal(seen?.body.model, "test-model");
  assert.equal(seen?.body.store, false);

  for (const [status, kind] of [[401, "auth"], [429, "rate_limited"], [500, "upstream"]] as const) {
    const client = new OpenAiClient(config, (async () => new Response("{}", { status })) as typeof fetch);
    await assert.rejects(client.complete({ messages: [] }), (error: unknown) => {
      assert.ok(error instanceof LlmError);
      assert.equal(error.kind, kind);
      assert.doesNotMatch(error.message, /sk-test/);
      return true;
    });
  }
  const empty = new OpenAiClient(config, (async () => new Response(JSON.stringify({ choices: [{ message: { content: "" } }] }))) as typeof fetch);
  await assert.rejects(empty.complete({ messages: [] }), (error: unknown) => error instanceof LlmError && error.kind === "empty");
});

// ---- HTTP gateway and routes ----

async function withServer(app: express.Express, run: (baseUrl: string) => Promise<void>) {
  const server: Server = createServer(app);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    await run(`http://127.0.0.1:${(server.address() as AddressInfo).port}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test("gateway decodes a real 402 and forwards PAYMENT-SIGNATURE unchanged", async () => {
  const seen: (string | undefined)[] = [];
  const app = express();
  app.use(express.json());
  app.post("/agent/x402/market-regime", (req, res) => {
    const sig = req.get("payment-signature");
    seen.push(sig);
    if (!sig) {
      res.setHeader("PAYMENT-REQUIRED", encodePaymentRequiredHeader({
        x402Version: 2,
        resource: { url: "x", description: "", mimeType: "application/json" },
        accepts: [{ scheme: "exact", network: "eip155:84532", amount: "250000", asset: "0xusdc", payTo: "0xseller", maxTimeoutSeconds: 60, extra: {} }],
      } as never));
      return res.status(402).json({ success: false });
    }
    if (sig === "bad") {
      return res.status(502).json({ success: false, error: "x402 verification mismatch: nope" });
    }
    return res.status(200).json({ success: true, data: { status: "completed" } });
  });

  await withServer(app, async (baseUrl) => {
    const gateway = new HttpX402Gateway(baseUrl);
    const challenge = await gateway.requestChallenge("/agent/x402/market-regime", { decisionId: "d" });
    assert.equal(challenge.kind === "challenge" && challenge.paymentRequired.accepts[0].amount, "250000");
    assert.equal((await gateway.submitPaid("/agent/x402/market-regime", {}, "good", 5_000)).kind, "completed");
    assert.equal((await gateway.submitPaid("/agent/x402/market-regime", {}, "bad", 5_000)).kind, "payment_rejected");
    assert.deepEqual(seen, [undefined, "good", "bad"]);
    const missing = await gateway.requestChallenge("/agent/x402/unknown", {});
    assert.equal(missing.kind, "error");
  });
});

test("HTTP routes: full journey with status codes and no secret leakage", async () => {
  const llm = new ScriptedLlm([...MARKET_SCRIPT(), { content: "Explained.", toolCalls: [] }]);
  const gateway = new FakeGateway();
  const app = express();
  app.use(express.json());
  app.use("/assistant", createAssistantRouter({ llm, gateway, loadCatalog: async () => catalog() }));

  await withServer(app, async (baseUrl) => {
    const status = await (await fetch(`${baseUrl}/assistant/status`)).json() as { available: boolean; rails: { id: string; status: string }[] };
    assert.equal(status.available, true);
    assert.deepEqual(status.rails.map((rail) => [rail.id, rail.status]), [["x402_wallet", "available"], ["paypal_once", "coming_soon"], ["paypal_subscription", "coming_soon"]]);

    const capabilities = await (await fetch(`${baseUrl}/assistant/capabilities`)).json() as { capabilities: { id: string; priceUsdc: string }[] };
    assert.equal(capabilities.capabilities.length, 9);
    assert.equal(capabilities.capabilities.find((entry) => entry.id === "allocate")?.priceUsdc, "19");

    const post = (path: string, body: unknown, conversation?: string) => fetch(`${baseUrl}/assistant/${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...(conversation ? { "x-selun-conversation": conversation } : {}) },
      body: JSON.stringify(body),
    });

    const chat = await post("chat", { message: "market update" });
    assert.equal(chat.status, 200);
    const reply = await chat.json() as { conversationId: string; blocks: { type: string; action: ActionView }[] };
    const action = reply.blocks[0].action;

    assert.equal((await post(`actions/${action.id}/challenge`, {})).status, 404, "no conversation header");
    const challenge = await post(`actions/${action.id}/challenge`, {}, reply.conversationId);
    assert.equal(challenge.status, 200);

    const unapproved = await post(`actions/${action.id}/execute`, { paymentSignature: signature() }, reply.conversationId);
    assert.equal(unapproved.status, 400);
    assert.equal(((await unapproved.json()) as { code: string }).code, "approval_required");

    const executed = await post(`actions/${action.id}/execute`, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() }, reply.conversationId);
    assert.equal(executed.status, 202);
    const executedBody = await executed.text();
    assert.ok(!executedBody.includes(signature()), "the signature is never echoed");

    let final: ActionView | undefined;
    for (let attempt = 0; attempt < 100 && final?.explanationStatus !== "ready"; attempt += 1) {
      const polled = await fetch(`${baseUrl}/assistant/actions/${action.id}`, { headers: { "x-selun-conversation": reply.conversationId } });
      final = ((await polled.json()) as { action: ActionView }).action;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(final?.state, "completed");
    assert.equal(final?.document?.provenance.transactionHash, TX);
  });
});

test("rate limiter blocks after the limit and recovers after the window", () => {
  let now = 0;
  const limiter = new WindowRateLimiter(2, 1_000, () => now);
  assert.equal(limiter.take("ip"), 0);
  assert.equal(limiter.take("ip"), 0);
  assert.ok(limiter.take("ip") > 0);
  assert.equal(limiter.take("other"), 0);
  now = 1_001;
  assert.equal(limiter.take("ip"), 0);
});

// ---- Routing ----

const quotedIds = (blocks: { type: string; action?: ActionView }[]) =>
  blocks.flatMap((block) => (block.type === "action" && block.action ? [block.action.capabilityId] : []));

test("finds assets a message names, without mistaking ordinary words for them", () => {
  const symbols = (text: string) => findAssetMentions(text).map((asset) => asset.symbol);
  assert.deepEqual(symbols("Give me a market update on Bitcoin."), ["BTC"]);
  assert.deepEqual(symbols("btc and $ETH please"), ["BTC", "ETH"]);
  assert.deepEqual(symbols("bitcoin cash outlook"), ["BCH"]);
  assert.deepEqual(symbols("wrapped bitcoin"), ["WBTC"]);
  assert.deepEqual(symbols("is the market near the highs? send a link to a ton of dot points"), []);
  assert.deepEqual(symbols("update on NEAR"), ["NEAR"]);
  assert.deepEqual(symbols("how is polygon doing"), [], "ambiguous names go through the customer's choice");
  assert.deepEqual(symbols("how is the market right now"), []);
});

test("a named token gets its scorecard quote first, even if the model only picks Market Regime", async () => {
  const { service } = makeService([{ content: null, toolCalls: [toolCall("propose_market_regime", {})] }, { content: "Here you go.", toolCalls: [] }]);
  const reply = await service.chat({ message: "Give me a market update on Bitcoin." });
  assert.deepEqual(quotedIds(reply.blocks), ["asset_scorecard", "market_regime"]);
  const scorecard = reply.blocks[0];
  assert.ok(scorecard.type === "action");
  assert.equal(scorecard.action.priceUsdc, "0.5", "the price comes from the catalog");
  assert.ok(scorecard.action.details.some((detail) => detail.value === "Bitcoin (BTC)"));
  assert.equal(scorecard.action.state, "awaiting_approval", "still only a quote");
});

test("the model is told about a scorecard the server added, including its real price", async () => {
  const { service, llm } = makeService([{ content: null, toolCalls: [toolCall("propose_market_regime", {})] }, { content: "Here you go.", toolCalls: [] }]);
  await service.chat({ message: "market update on BTC" });
  const toolMessage = llm.requests[1].messages.at(-1);
  assert.ok(toolMessage?.role === "tool");
  const result = JSON.parse(toolMessage.content);
  assert.deepEqual(result.also_quoted.assets, ["BTC"]);
  assert.equal(result.also_quoted.price_usdc, "0.5");
});

test("an asset the model resolved counts, even when the message doesn't spell it out", async () => {
  const { service } = makeService([
    { content: null, toolCalls: [toolCall("resolve_asset", { query: "xbt" })] },
    { content: null, toolCalls: [toolCall("propose_market_regime", {})] },
    { content: "Done.", toolCalls: [] },
  ]);
  const reply = await service.chat({ message: "market update on the orange coin" });
  assert.deepEqual(quotedIds(reply.blocks), ["asset_scorecard", "market_regime"]);
});

test("a model that proposes both, in either order, shows each card once", async () => {
  for (const calls of [
    [toolCall("propose_market_regime", {}), toolCall("propose_asset_scorecard", { assets: ["BTC"] })],
    [toolCall("propose_asset_scorecard", { assets: ["BTC"] }), toolCall("propose_market_regime", {})],
  ]) {
    const { service } = makeService([{ content: null, toolCalls: calls }, { content: "Done.", toolCalls: [] }]);
    const reply = await service.chat({ message: "market update on Bitcoin" });
    assert.deepEqual(quotedIds(reply.blocks).sort(), ["asset_scorecard", "market_regime"]);
  }
});

test("a general market update stays Market Regime only", async () => {
  const { service } = makeService([{ content: null, toolCalls: [toolCall("propose_market_regime", {})] }, { content: "Done.", toolCalls: [] }]);
  const reply = await service.chat({ message: "How is the market right now?" });
  assert.deepEqual(quotedIds(reply.blocks), ["market_regime"]);
});

test("asking for Market Regime later doesn't quote the same token's scorecard again", async () => {
  const { service } = makeService([
    { content: null, toolCalls: [toolCall("propose_asset_scorecard", { assets: ["BTC"] })] },
    { content: "Done.", toolCalls: [] },
    { content: null, toolCalls: [toolCall("propose_market_regime", {})] },
    { content: "Done.", toolCalls: [] },
  ]);
  const first = await service.chat({ message: "Tell me about BTC" });
  const second = await service.chat({ conversationId: first.conversationId, message: "Now the wider market, with BTC in mind" });
  assert.deepEqual(quotedIds(second.blocks), ["market_regime"]);
});

test("every service the model can propose is named in the routing instructions", () => {
  const prompt = buildSystemPrompt("2026-11-05");
  for (const id of TOOL_CAPABILITY_IDS) assert.ok(prompt.includes(`propose_${id}`), `${id} has no routing rule`);
});

// ---- What the model writes ----

async function paidAction(script: (LlmResponse | Error)[]) {
  process.env.SELUN_REFERRAL_INTERNAL_TOKEN = "test-assistant-secret";
  const made = makeService([...MARKET_SCRIPT(), ...script]);
  const { conversationId, action } = await proposeMarketRegime(made.service);
  await made.service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() });
  const done = await waitForState(made.service, conversationId, action.id, (view) => view.state === "completed" && view.explanationStatus !== "pending");
  return { ...made, done, conversationId };
}

test("a summary with recommendation language is rewritten before anyone sees it", async () => {
  const { done, llm } = await paidAction([
    { content: "Conditions make this a strong fit for you.", toolCalls: [] },
    { content: "The service reports a defensive market.", toolCalls: [] },
  ]);
  assert.equal(done.explanation, "The service reports a defensive market.");
  assert.match(JSON.stringify(llm.requests.at(-1)), /rejected/);
});

test("a summary that stays unsupported isn't shown, and the result is still complete", async () => {
  const { done } = await paidAction([
    { content: "Confidence is 99%.", toolCalls: [] },
    { content: "Confidence is still 99%.", toolCalls: [] },
  ]);
  assert.equal(done.explanation, null);
  assert.equal(done.explanationStatus, "unavailable");
  assert.ok(done.document);
});

test("a completed action gets a report token, which the model never sees", async () => {
  const { done, llm } = await paidAction([{ content: "The service reports a defensive market.", toolCalls: [] }]);
  assert.ok(done.reportToken);
  assert.ok(done.coverage.some((item) => item.label === "Live prices" && !item.included));
  assert.ok(!JSON.stringify(llm.requests).includes(done.reportToken));
});

test("a quote has disclosures but no report token", async () => {
  const { service } = makeService(MARKET_SCRIPT());
  const { action } = await proposeMarketRegime(service);
  assert.equal(action.coverage.length, 3);
  assert.equal(action.reportToken, null);
});

test("a reply stating an unsourced price is rewritten, then replaced if it persists", async () => {
  const fixed = makeService([
    { content: "Bitcoin is trading at $68,000.", toolCalls: [] },
    { content: "I can't give live prices.", toolCalls: [] },
  ]);
  assert.equal((await fixed.service.chat({ message: "price of btc?" })).text, "I can't give live prices.");

  const stubborn = makeService([
    { content: "Bitcoin is trading at $68,000.", toolCalls: [] },
    { content: "It's about $68,100.", toolCalls: [] },
  ]);
  const reply = await stubborn.service.chat({ message: "price of btc?" });
  assert.ok(!reply.text.includes("68"));
  const transcript = JSON.stringify(stubborn.llm.requests.at(-1)?.messages.filter((message) => message.role === "assistant"));
  assert.ok(!transcript.includes("68,"), "a rejected reply doesn't stay in the transcript");
});

test("a held-back reply keeps the quote cards already shown", async () => {
  const { service } = makeService([
    { content: null, toolCalls: [toolCall("propose_market_regime", {})] },
    { content: "This is a strong fit.", toolCalls: [] },
    { content: "It's still a strong fit.", toolCalls: [] },
  ]);
  const reply = await service.chat({ message: "market update" });
  assert.equal(reply.blocks.length, 1);
  assert.ok(!reply.text.includes("strong fit"));
});

test("a wallet's daily cap says so, instead of suggesting a retry in a moment", async () => {
  const made = makeService(MARKET_SCRIPT());
  made.gateway.paidOutcome = { kind: "rate_limited", retryAfterSeconds: 3600, reason: "from_address_daily_cap" };
  const { conversationId, action } = await proposeMarketRegime(made.service);
  await made.service.approveAndExecute(conversationId, action.id, { approved: true, approvedPriceUsdc: "0.25", paymentSignature: signature() });
  const done = await waitForState(made.service, conversationId, action.id, (view) => view.state === "failed");
  assert.match(done.failure?.message ?? "", /today's limit.*You have not been charged/);
  assert.equal(done.failure?.mayHaveCharged, false);
});
