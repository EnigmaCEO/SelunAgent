import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeSceInputForContinuityMode,
  normalizeSceInputForCaseRelevance,
  normalizeSceInputForRiskEvaluate,
  buildSceWrappedResult,
  buildSceCacheKey,
  getSceContinuityModeFromCache,
  setSceContinuityModeInCache,
  clearSceContinuityModeCache,
  _sceContinuityModeCacheForTest,
} from "./agent.routes";

// --- normalizeSceInputForContinuityMode ---

test("normalizeSceInputForContinuityMode extracts only allowed fields", () => {
  const body = {
    scope: "protocol",
    chain_id: "eip155:8453",
    threat_family: "reentrancy",
    doctrine_tag: "defi",
    requested_action: "evaluate",
    walletAddress: "0xdeadbeef",
    transactionHash: "0xabc",
    decisionId: "d-001",
    resultEmail: "user@example.com",
  };
  const result = normalizeSceInputForContinuityMode(body);
  assert.deepEqual(result, {
    scope: "protocol",
    chain_id: "eip155:8453",
    threat_family: "reentrancy",
    doctrine_tag: "defi",
    requested_action: "evaluate",
  });
});

test("normalizeSceInputForContinuityMode drops payment/wallet fields", () => {
  const body = {
    walletAddress: "0xdeadbeef",
    transactionHash: "0xabc",
    decisionId: "d-001",
    "PAYMENT-SIGNATURE": "sig",
    resultEmail: "user@example.com",
  };
  const result = normalizeSceInputForContinuityMode(body);
  assert.deepEqual(result, {});
});

test("normalizeSceInputForContinuityMode returns empty object for null/non-object body", () => {
  assert.deepEqual(normalizeSceInputForContinuityMode(null), {});
  assert.deepEqual(normalizeSceInputForContinuityMode("string"), {});
  assert.deepEqual(normalizeSceInputForContinuityMode(42), {});
  assert.deepEqual(normalizeSceInputForContinuityMode(undefined), {});
});

test("normalizeSceInputForContinuityMode omits undefined fields", () => {
  const body = { scope: "protocol" };
  const result = normalizeSceInputForContinuityMode(body);
  assert.ok(!Object.prototype.hasOwnProperty.call(result, "chain_id"));
  assert.ok(!Object.prototype.hasOwnProperty.call(result, "threat_family"));
  assert.equal(result.scope, "protocol");
});

// --- normalizeSceInputForCaseRelevance ---

test("normalizeSceInputForCaseRelevance extracts only allowed fields", () => {
  const body = {
    protocol_name: "Uniswap",
    chain_id: "eip155:8453",
    asset_types: ["ERC20"],
    threat_families: ["flash-loan"],
    doctrine_tags: ["defi"],
    requested_action: "assess",
    walletAddress: "0xdeadbeef",
    decisionId: "d-002",
  };
  const result = normalizeSceInputForCaseRelevance(body);
  assert.deepEqual(result, {
    protocol_name: "Uniswap",
    chain_id: "eip155:8453",
    asset_types: ["ERC20"],
    threat_families: ["flash-loan"],
    doctrine_tags: ["defi"],
    requested_action: "assess",
  });
});

test("normalizeSceInputForCaseRelevance drops wallet/payment fields", () => {
  const body = { walletAddress: "0x1", transactionHash: "0x2", decisionId: "d-x" };
  assert.deepEqual(normalizeSceInputForCaseRelevance(body), {});
});

// --- normalizeSceInputForRiskEvaluate ---

test("normalizeSceInputForRiskEvaluate extracts only allowed fields", () => {
  const body = {
    protocol_name: "Aave",
    chain_id: "eip155:8453",
    addresses: ["0xabc"],
    asset_types: ["ERC20"],
    threat_families: ["oracle-manipulation"],
    doctrine_tags: ["lending"],
    requested_action: "evaluate",
    risk_tolerance: "low",
    walletAddress: "0xdeadbeef",
    decisionId: "d-003",
    transactionHash: "0xfff",
  };
  const result = normalizeSceInputForRiskEvaluate(body);
  assert.deepEqual(result, {
    protocol_name: "Aave",
    chain_id: "eip155:8453",
    addresses: ["0xabc"],
    asset_types: ["ERC20"],
    threat_families: ["oracle-manipulation"],
    doctrine_tags: ["lending"],
    requested_action: "evaluate",
    risk_tolerance: "low",
  });
});

test("normalizeSceInputForRiskEvaluate drops payment/wallet fields", () => {
  const body = { walletAddress: "0x1", decisionId: "d-x", "x402-header": "val" };
  assert.deepEqual(normalizeSceInputForRiskEvaluate(body), {});
});

// --- buildSceWrappedResult ---

test("buildSceWrappedResult puts full SCE response under .sce", () => {
  const sceResponse = { status: "ok", score: 0.8 };
  const result = buildSceWrappedResult(sceResponse, "/v1/sce/continuity-mode");
  assert.deepEqual(result.sce, sceResponse);
});

test("buildSceWrappedResult sets sceSourceEndpoint", () => {
  const result = buildSceWrappedResult({}, "/v1/sce/case-relevance");
  assert.equal(result.sceSourceEndpoint, "/v1/sce/case-relevance");
});

test("buildSceWrappedResult sets selunWrapper: true", () => {
  const result = buildSceWrappedResult({}, "/v1/sce/risk/evaluate");
  assert.equal(result.selunWrapper, true);
});

test("buildSceWrappedResult extracts evaluation_id as sceEvaluationId", () => {
  const sceResponse = { evaluation_id: "eval-abc-123", status: "ok" };
  const result = buildSceWrappedResult(sceResponse, "/v1/sce/continuity-mode");
  assert.equal(result.sceEvaluationId, "eval-abc-123");
});

test("buildSceWrappedResult sets sceEvaluationId null when absent", () => {
  const result = buildSceWrappedResult({ status: "ok" }, "/v1/sce/continuity-mode");
  assert.equal(result.sceEvaluationId, null);
});

test("buildSceWrappedResult extracts valid_until as validUntil", () => {
  const sceResponse = { valid_until: "2026-06-16T12:00:00Z" };
  const result = buildSceWrappedResult(sceResponse, "/v1/sce/continuity-mode");
  assert.equal(result.validUntil, "2026-06-16T12:00:00Z");
});

test("buildSceWrappedResult sets validUntil null when absent", () => {
  const result = buildSceWrappedResult({ status: "ok" }, "/v1/sce/continuity-mode");
  assert.equal(result.validUntil, null);
});

test("buildSceWrappedResult ignores non-string evaluation_id", () => {
  const result = buildSceWrappedResult({ evaluation_id: 42 }, "/v1/sce/continuity-mode");
  assert.equal(result.sceEvaluationId, null);
});

test("buildSceWrappedResult ignores non-string valid_until", () => {
  const result = buildSceWrappedResult({ valid_until: null }, "/v1/sce/continuity-mode");
  assert.equal(result.validUntil, null);
});

// --- buildSceCacheKey ---

test("buildSceCacheKey produces stable output for same input", () => {
  const key1 = buildSceCacheKey({ scope: "chain", chain_id: 8453 });
  const key2 = buildSceCacheKey({ scope: "chain", chain_id: 8453 });
  assert.equal(key1, key2);
});

test("buildSceCacheKey is key-order independent", () => {
  const key1 = buildSceCacheKey({ chain_id: 8453, scope: "chain" });
  const key2 = buildSceCacheKey({ scope: "chain", chain_id: 8453 });
  assert.equal(key1, key2);
});

test("buildSceCacheKey produces different keys for different field values", () => {
  const key1 = buildSceCacheKey({ scope: "chain", chain_id: 8453 });
  const key2 = buildSceCacheKey({ scope: "global", chain_id: 8453 });
  assert.notEqual(key1, key2);
});

test("buildSceCacheKey treats different chain_ids as different keys", () => {
  const key1 = buildSceCacheKey({ scope: "chain", chain_id: 8453 });
  const key2 = buildSceCacheKey({ scope: "chain", chain_id: 1 });
  assert.notEqual(key1, key2);
});

test("buildSceCacheKey treats empty input and partial input as different", () => {
  const keyEmpty = buildSceCacheKey({});
  const keyPartial = buildSceCacheKey({ scope: "global" });
  assert.notEqual(keyEmpty, keyPartial);
});

// --- SCE continuity-mode cache get/set/clear ---

test("setSceContinuityModeInCache and getSceContinuityModeFromCache roundtrip", () => {
  clearSceContinuityModeCache();
  const key = buildSceCacheKey({ scope: "chain", chain_id: 8453 });
  const result = { sce: { continuity_mode: "active" }, selunWrapper: true, validUntil: null, sceEvaluationId: null, sceSourceEndpoint: "/v1/sce/continuity-mode" };
  setSceContinuityModeInCache(key, result);
  assert.deepEqual(getSceContinuityModeFromCache(key), result);
  clearSceContinuityModeCache();
});

test("getSceContinuityModeFromCache returns null for missing key", () => {
  clearSceContinuityModeCache();
  assert.equal(getSceContinuityModeFromCache("no-such-key"), null);
});

test("clearSceContinuityModeCache removes all entries", () => {
  const key = buildSceCacheKey({ scope: "global" });
  setSceContinuityModeInCache(key, { selunWrapper: true, validUntil: null, sceEvaluationId: null, sce: {}, sceSourceEndpoint: "" });
  clearSceContinuityModeCache();
  assert.equal(getSceContinuityModeFromCache(key), null);
});

test("getSceContinuityModeFromCache returns null for expired entry", () => {
  clearSceContinuityModeCache();
  const key = buildSceCacheKey({ scope: "threat_family" });
  _sceContinuityModeCacheForTest.set(key, { result: { sce: { mode: "active" } }, expiresAt: Date.now() - 1 });
  assert.equal(getSceContinuityModeFromCache(key), null);
  clearSceContinuityModeCache();
});

test("setSceContinuityModeInCache uses valid_until as TTL when in the future", () => {
  clearSceContinuityModeCache();
  const key = buildSceCacheKey({ scope: "chain" });
  const futureTime = new Date(Date.now() + 30_000).toISOString();
  const result = { sce: {}, selunWrapper: true, validUntil: futureTime, sceEvaluationId: null, sceSourceEndpoint: "" };
  setSceContinuityModeInCache(key, result);
  const entry = _sceContinuityModeCacheForTest.get(key);
  assert.ok(entry !== undefined);
  assert.ok(entry.expiresAt <= Date.now() + 30_000 + 100);
  assert.ok(entry.expiresAt > Date.now());
  clearSceContinuityModeCache();
});

test("setSceContinuityModeInCache uses max TTL when valid_until is absent", () => {
  clearSceContinuityModeCache();
  const key = buildSceCacheKey({ scope: "doctrine_tag" });
  const before = Date.now();
  setSceContinuityModeInCache(key, { sce: {}, selunWrapper: true, validUntil: null, sceEvaluationId: null, sceSourceEndpoint: "" });
  const entry = _sceContinuityModeCacheForTest.get(key);
  assert.ok(entry !== undefined);
  assert.ok(entry.expiresAt >= before + 59_000);
  assert.ok(entry.expiresAt <= before + 61_000);
  clearSceContinuityModeCache();
});
