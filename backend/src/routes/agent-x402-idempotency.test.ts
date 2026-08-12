import assert from "node:assert/strict";
import test from "node:test";
import { BUILDER_CODE } from "@x402/extensions/builder-code";
import { BASE_BUILDER_CODE } from "../builder-code";
import type { X402ToolRecord, X402ToolProductId } from "../services/x402-state.types";
import {
  X402_ALLOCATE_EXAMPLE_DECISION_ID,
  X402_ALLOCATE_WITH_REPORT_EXAMPLE_DECISION_ID,
  authorizeStoredToolReplay,
  buildX402RouteExtensions,
  isStoredToolReplayFresh,
  paymentPayersMatch,
} from "./agent.routes";
import fs from "node:fs";
import path from "node:path";

const EVM_PAYER = "0xfe2d5E9c5aE6E48B7F8b0b82AC4dE8B423bA0557";
const OTHER_EVM_PAYER = "0x4A0Bff202c7Bdb3B49002fBD21ea8c821350f665";
const NOW_MS = Date.parse("2026-08-09T12:00:00Z");

function acceptedToolRecord(
  productId: X402ToolProductId,
  validUntil?: string | null,
): X402ToolRecord {
  return {
    decisionId: "decision-001",
    productId,
    inputFingerprint: "fingerprint",
    requestBody: {},
    chargedAmountUsdc: "0.01",
    quoteIssuedAt: "2026-08-09T11:50:00Z",
    quoteExpiresAt: "2026-08-09T12:10:00Z",
    state: "accepted",
    createdAt: "2026-08-09T11:50:00Z",
    updatedAt: "2026-08-09T11:51:00Z",
    payment: {
      fromAddress: EVM_PAYER,
      transactionHash: `0x${"a".repeat(64)}`,
      network: "eip155:8453",
      verifiedAt: "2026-08-09T11:51:00Z",
    },
    responseData: validUntil === undefined ? { result: "ok" } : { result: "ok", validUntil },
  };
}

test("allocation discovery examples use distinct decision IDs", () => {
  assert.notEqual(
    X402_ALLOCATE_EXAMPLE_DECISION_ID,
    X402_ALLOCATE_WITH_REPORT_EXAMPLE_DECISION_ID,
  );
});

test("x402 routes advertise Selun's Builder Code without dropping discovery metadata", () => {
  const discovery = { bazaar: { info: { input: { type: "http" } } } };
  const extensions = buildX402RouteExtensions(discovery);
  const builderCode = extensions[BUILDER_CODE] as {
    info?: { a?: string };
    schema?: { properties?: { a?: { pattern?: string } } };
  };

  assert.equal(extensions.bazaar, discovery.bazaar);
  assert.equal(builderCode.info?.a, BASE_BUILDER_CODE);
  assert.equal(builderCode.schema?.properties?.a?.pattern, "^[a-z0-9_]{1,32}$");
});

test("paymentPayersMatch compares EVM addresses case-insensitively", () => {
  assert.equal(paymentPayersMatch(EVM_PAYER, EVM_PAYER.toLowerCase()), true);
  assert.equal(paymentPayersMatch(EVM_PAYER, OTHER_EVM_PAYER), false);
});

test("paymentPayersMatch keeps non-EVM payer identifiers case-sensitive", () => {
  const solanaPayer = "Cc8prGdx5kbEKuGvSRb2wg8DN9tiXMjAKmXeqeFigu5L";
  assert.equal(paymentPayersMatch(solanaPayer, solanaPayer), true);
  assert.equal(paymentPayersMatch(solanaPayer, solanaPayer.toLowerCase()), false);
});

test("authorizeStoredToolReplay permits only the stored payer", () => {
  const record = acceptedToolRecord("market_regime");
  assert.equal(authorizeStoredToolReplay(record, EVM_PAYER.toLowerCase(), NOW_MS), "authorized");
  assert.equal(authorizeStoredToolReplay(record, OTHER_EVM_PAYER, NOW_MS), "payer_mismatch");
});

test("authorizeStoredToolReplay rejects records without payment ownership", () => {
  const record = acceptedToolRecord("market_regime");
  delete record.payment;
  assert.equal(authorizeStoredToolReplay(record, EVM_PAYER, NOW_MS), "payer_mismatch");
});

test("expired SCE results require a paid refresh", () => {
  const record = acceptedToolRecord("sce_continuity_mode", "2026-08-09T11:59:59Z");
  assert.equal(isStoredToolReplayFresh(record, NOW_MS), false);
  assert.equal(authorizeStoredToolReplay(record, EVM_PAYER, NOW_MS), "stale");
});

test("unexpired SCE results remain replayable by the original payer", () => {
  const record = acceptedToolRecord("sce_continuity_mode", "2026-08-09T12:00:01Z");
  assert.equal(isStoredToolReplayFresh(record, NOW_MS), true);
  assert.equal(authorizeStoredToolReplay(record, EVM_PAYER, NOW_MS), "authorized");
});

test("malformed SCE validity timestamps are not replayed", () => {
  const record = acceptedToolRecord("sce_risk_evaluate", "not-a-timestamp");
  assert.equal(authorizeStoredToolReplay(record, EVM_PAYER, NOW_MS), "stale");
});

test("non-SCE tool replays are not freshness-limited", () => {
  const record = acceptedToolRecord("market_regime", "2026-08-09T11:00:00Z");
  assert.equal(authorizeStoredToolReplay(record, EVM_PAYER, NOW_MS), "authorized");
});

test("fresh tool settlement headers are applied before the paid response is sent", () => {
  const source = fs.readFileSync(path.join(__dirname, "agent.routes.ts"), "utf8");
  const transactionGuard = source.indexOf('if (!transactionHash)');
  const applyHeaders = source.indexOf("applySettlementResponseHeaders(res, settlement);", transactionGuard);
  const executePaid = source.indexOf("const response = await executePaidToolRequest({", transactionGuard);

  assert.ok(transactionGuard >= 0, "settlement transaction guard is present");
  assert.ok(applyHeaders > transactionGuard, "settlement headers are applied after settlement validation");
  assert.ok(applyHeaders < executePaid, "settlement headers are applied before executePaidToolRequest sends JSON");
});
