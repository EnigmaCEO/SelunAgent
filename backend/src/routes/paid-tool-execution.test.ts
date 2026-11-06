import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { decideAllocateReplay, getPaidToolResponse, runAndRecordPaidTool, selectAllocateQuoteWindow, selectToolQuoteWindow, type PaidToolExecution } from "./agent.routes";
import type { X402AllocateRecord, X402ToolRecord } from "../services/x402-state.types";
import { getX402StateStore } from "../services/x402-state.service";

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "selun-paid-tool-"));
process.env.X402_STATE_FILE = path.join(dir, "x402-state.json");
process.env.SELUN_ADMIN_USAGE_EMAILS_ENABLED = "0";
process.env.SELUN_RESULT_EMAILS_ENABLED = "0";
delete process.env.RESEND_API_KEY;
// Placeholders so the price config loads. Nothing here reaches a chain or Coinbase.
process.env.COINBASE_API_KEY = "test";
process.env.COINBASE_API_SECRET = "test";
process.env.NETWORK_ID = "base-sepolia";
process.env.BASE_SEPOLIA_RPC = "http://127.0.0.1:1";
process.env.USDC_CONTRACT_ADDRESS = "0x036CbD53842c5426634e7929541eC2318f3dCF7e";
test.after(() => fs.rmSync(dir, { recursive: true, force: true }));

const PAYER = "0x436B52e5E76e3bfa124215376e4a294c7d89f8D1";
const TX = `0x${"d".repeat(64)}`;

function execution(decisionId: string, execute: PaidToolExecution["execute"]): PaidToolExecution {
  return {
    productId: "market_regime",
    normalizedInput: { riskTolerance: "Balanced", timeframe: "1-3_years" },
    inputFingerprint: "fingerprint",
    decisionId,
    execute,
    skipPhase1: true,
    payer: PAYER,
    transactionHash: TX,
    network: "eip155:84532",
    quoteIssuedAt: "2026-11-05T00:00:00.000Z",
    quoteExpiresAt: "2026-11-05T00:10:00.000Z",
    createdAt: "2026-11-05T00:00:00.000Z",
  };
}

test("a paid run records the result, and reports read back the same response", async () => {
  const { record, responseData, jobId } = await runAndRecordPaidTool(execution("paid-ok", async () => ({ snapshotAt: "2026-11-05T00:00:00.000Z", regime: "balanced" })));
  assert.ok(jobId.length > 0);
  assert.equal(record.state, "accepted");
  assert.equal(record.payment?.fromAddress, PAYER);
  assert.equal(record.payment?.transactionHash, TX);
  assert.deepEqual(responseData.result, { snapshotAt: "2026-11-05T00:00:00.000Z", regime: "balanced" });
  assert.deepEqual(responseData.payment, { required: true, chargedAmountUsdc: responseData.payment.chargedAmountUsdc, verified: true, fromAddress: PAYER, transactionHash: TX, network: "eip155:84532" });

  assert.deepEqual(getX402StateStore().getToolRecord("market_regime", "paid-ok"), record);
  const stored = getPaidToolResponse("market_regime", "paid-ok");
  assert.deepEqual(stored?.result, responseData.result);
  assert.equal((stored?.payment as { transactionHash: string }).transactionHash, TX);
});

test("a failed run keeps the payment, so the same payer can retry without paying again", async () => {
  await assert.rejects(
    () => runAndRecordPaidTool(execution("paid-fail", async () => {
      throw new Error("feed down");
    })),
    /feed down/,
  );
  const failed = getX402StateStore().getToolRecord("market_regime", "paid-fail");
  assert.equal(failed?.state, "failed_post_settlement");
  assert.equal(failed?.payment?.transactionHash, TX);
  assert.equal(failed?.failure?.message, "feed down");
  assert.equal(failed?.responseData, undefined);
  assert.equal(getPaidToolResponse("market_regime", "paid-fail"), null, "no result to report on");

  const { record } = await runAndRecordPaidTool(execution("paid-fail", async () => ({ regime: "balanced" })));
  assert.equal(record.state, "accepted");
  assert.equal(record.payment?.transactionHash, TX);
});

test("a paid decision keeps its quote window, so a retry's signature can match", () => {
  const stored = { issuedAt: "2026-01-01T00:00:00.000Z", expiresAt: "2026-01-01T00:10:00.000Z" };
  const record = (fields: Partial<X402ToolRecord>) => ({
    decisionId: "d",
    productId: "market_regime",
    inputFingerprint: "f",
    requestBody: {},
    chargedAmountUsdc: "0.25",
    quoteIssuedAt: stored.issuedAt,
    quoteExpiresAt: stored.expiresAt,
    createdAt: stored.issuedAt,
    updatedAt: stored.issuedAt,
    ...fields,
  }) as X402ToolRecord;
  const payment = { fromAddress: PAYER, transactionHash: TX, verifiedAt: stored.issuedAt };

  assert.equal(selectToolQuoteWindow(undefined), null, "new request gets a new window");
  assert.equal(selectToolQuoteWindow(record({ state: "quoted" })), null, "expired quote gets a new window");
  const future = new Date(Date.now() + 60_000).toISOString();
  assert.deepEqual(selectToolQuoteWindow(record({ state: "quoted", quoteExpiresAt: future })), { issuedAt: stored.issuedAt, expiresAt: future }, "open quote is reused");
  assert.deepEqual(selectToolQuoteWindow(record({ state: "accepted", payment })), stored, "paid: long expired, still reused");
  assert.deepEqual(selectToolQuoteWindow(record({ state: "failed_post_settlement", payment })), stored, "failed after payment: reused");
  assert.equal(selectToolQuoteWindow(record({ state: "failed_post_settlement" })), null, "failed with no payment on record isn't recoverable");
});

function allocation(fields: Partial<X402AllocateRecord>): X402AllocateRecord {
  return {
    decisionId: "alloc-1",
    inputFingerprint: "f",
    inputs: { riskTolerance: "Balanced", timeframe: "1-3_years", portfolioSegment: "Bluechips", withReport: true },
    chargedAmountUsdc: "34",
    quoteIssuedAt: "2026-01-01T00:00:00.000Z",
    quoteExpiresAt: "2026-01-01T00:10:00.000Z",
    state: "accepted",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    jobId: "job-1",
    payment: { fromAddress: PAYER, transactionHash: TX, verifiedAt: "2026-01-01T00:00:00.000Z" },
    ...fields,
  };
}

test("a paid allocation keeps its quote window, however old, for both routes", () => {
  const stored = { issuedAt: "2026-01-01T00:00:00.000Z", expiresAt: "2026-01-01T00:10:00.000Z" };
  assert.deepEqual(selectAllocateQuoteWindow(allocation({})), stored);
  assert.deepEqual(selectAllocateQuoteWindow(allocation({ inputs: { riskTolerance: "Balanced", timeframe: "1-3_years", portfolioSegment: "Bluechips", withReport: false } })), stored);
  assert.equal(selectAllocateQuoteWindow(undefined), null);
  assert.equal(selectAllocateQuoteWindow(allocation({ state: "quoted", jobId: undefined, payment: undefined })), null, "expired, unpaid quote gets a new window");
  const future = new Date(Date.now() + 60_000).toISOString();
  assert.equal(selectAllocateQuoteWindow(allocation({ state: "quoted", quoteExpiresAt: future, jobId: undefined }))?.expiresAt, future, "open quote is reused");
});

test("a repeat allocation request never pays twice, and only its payer can restart it", () => {
  const healthy = { found: true, failed: false, running: false };
  assert.equal(decideAllocateReplay(allocation({}), PAYER, healthy), "replay", "finished job returns the stored one");
  assert.equal(decideAllocateReplay(allocation({}), PAYER.toLowerCase(), healthy), "replay", "EVM case doesn't matter");
  assert.equal(decideAllocateReplay(allocation({}), PAYER, { ...healthy, running: true }), "replay", "duplicate while running starts nothing");
  assert.equal(decideAllocateReplay(allocation({}), PAYER, { ...healthy, running: true, failed: true }), "replay", "still orchestrating");
  assert.equal(decideAllocateReplay(allocation({}), PAYER, { ...healthy, failed: true }), "restart", "failed job reruns on the same payment");
  assert.equal(decideAllocateReplay(allocation({}), PAYER, { found: false, failed: false, running: false }), "restart", "job lost in a restart reruns");

  const other = "0x4A0Bff202c7Bdb3B49002fBD21ea8c821350f665";
  assert.equal(decideAllocateReplay(allocation({}), other, { ...healthy, failed: true }), "payer_mismatch", "another wallet can't take over a failed job");
  assert.equal(decideAllocateReplay(allocation({ payment: undefined }), PAYER, healthy), "payer_mismatch", "free-route record has no owner to match");
});

test("the execution step needs no HTTP request, response, or wallet signature", async () => {
  const seen: string[] = [];
  await runAndRecordPaidTool(execution("paid-plain", async (jobId) => {
    seen.push(jobId);
    return {};
  }));
  assert.equal(seen.length, 1);
});
