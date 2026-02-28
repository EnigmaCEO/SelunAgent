import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { X402StateStore } from "./x402-state.service";
import type { X402AllocateRecord, X402ToolRecord } from "./x402-state.types";

function withTempStateFile(run: (stateFilePath: string) => void) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "selun-x402-state-"));
  const stateFilePath = path.join(tempDir, "state.json");
  try {
    run(stateFilePath);
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

function buildAcceptedRecord(decisionId: string, transactionHash: string): X402AllocateRecord {
  const timestamp = "2026-02-26T00:00:00.000Z";
  return {
    decisionId,
    inputFingerprint: `fp-${decisionId}`,
    inputs: {
      riskTolerance: "Balanced",
      timeframe: "1-3_years",
      withReport: false,
    },
    chargedAmountUsdc: "19",
    quoteIssuedAt: timestamp,
    quoteExpiresAt: "2026-02-26T00:10:00.000Z",
    state: "accepted",
    createdAt: timestamp,
    updatedAt: timestamp,
    jobId: `job-${decisionId}`,
    payment: {
      fromAddress: "0x1234567890123456789012345678901234567890",
      transactionHash,
      network: "eip155:8453",
      verifiedAt: timestamp,
    },
  };
}

function buildToolRecord(decisionId: string, transactionHash: string): X402ToolRecord {
  const timestamp = "2026-02-26T00:00:00.000Z";
  return {
    decisionId,
    productId: "rebalance",
    inputFingerprint: `fp-tool-${decisionId}`,
    requestBody: {
      riskTolerance: "Balanced",
      timeframe: "1-3_years",
      holdings: [{ asset: "BTC", usdValue: 5000 }],
    },
    chargedAmountUsdc: "1",
    quoteIssuedAt: timestamp,
    quoteExpiresAt: "2026-02-26T00:10:00.000Z",
    state: "accepted",
    createdAt: timestamp,
    updatedAt: timestamp,
    responseData: {
      currentPortfolioUsd: 5000,
      recommendations: [{ asset: "BTC", action: "hold" }],
    },
    payment: {
      fromAddress: "0x1234567890123456789012345678901234567890",
      transactionHash,
      network: "eip155:8453",
      verifiedAt: timestamp,
    },
  };
}

test("transaction hash reservation is single-use across decision IDs", () => {
  withTempStateFile((stateFilePath) => {
    const store = new X402StateStore(stateFilePath, 3);

    const first = store.reserveTransactionHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "decision-1");
    assert.deepEqual(first, { accepted: true, reused: false });

    const reusedSameDecision = store.reserveTransactionHash(
      "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "decision-1",
    );
    assert.deepEqual(reusedSameDecision, { accepted: true, reused: true });

    const rejectedDifferentDecision = store.reserveTransactionHash(
      "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "decision-2",
    );
    assert.deepEqual(rejectedDifferentDecision, { accepted: false, existingDecisionId: "decision-1" });
  });
});

test("accepted records persist and backfill transaction ownership on restart", () => {
  withTempStateFile((stateFilePath) => {
    const txHash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const record = buildAcceptedRecord("decision-42", txHash);

    const store = new X402StateStore(stateFilePath, 3);
    store.setAllocateRecord(record.decisionId, record);

    const restarted = new X402StateStore(stateFilePath, 3);
    assert.equal(restarted.getTransactionOwner(txHash), "decision-42");
    assert.equal(restarted.getDecisionIdForJob("job-decision-42"), "decision-42");
    assert.equal(restarted.getAllocateRecord("decision-42")?.payment?.transactionHash, txHash);
    assert.equal(restarted.getAllocateRecord("decision-42")?.payment?.network, "eip155:8453");
  });
});

test("daily usage keys older than retention window are pruned on load", () => {
  withTempStateFile((stateFilePath) => {
    const today = new Date();
    today.setUTCHours(0, 0, 0, 0);
    const oldDay = new Date(today);
    oldDay.setUTCDate(oldDay.getUTCDate() - 10);
    const oldDayKey = oldDay.toISOString().slice(0, 10);
    const currentDayKey = today.toISOString().slice(0, 10);

    const payload = {
      version: 1,
      updatedAt: today.toISOString(),
      allocateByDecisionId: {},
      decisionIdByJobId: {},
      addressDailyUsage: {
        [`${oldDayKey}:0xaaa`]: 2,
        [`${currentDayKey}:0xbbb`]: 5,
      },
      consumedTransactionByHash: {},
    };

    fs.mkdirSync(path.dirname(stateFilePath), { recursive: true });
    fs.writeFileSync(stateFilePath, JSON.stringify(payload, null, 2), "utf8");

    const store = new X402StateStore(stateFilePath, 3);
    assert.equal(store.getAddressDailyUsage(`${oldDayKey}:0xaaa`), 0);
    assert.equal(store.getAddressDailyUsage(`${currentDayKey}:0xbbb`), 5);
  });
});

test("tool records persist and survive restart", () => {
  withTempStateFile((stateFilePath) => {
    const txHash = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const record = buildToolRecord("decision-tool-7", txHash);

    const store = new X402StateStore(stateFilePath, 3);
    store.setToolRecord(record.productId, record.decisionId, record);

    const restarted = new X402StateStore(stateFilePath, 3);
    const restored = restarted.getToolRecord("rebalance", "decision-tool-7");
    assert.equal(restored?.payment?.transactionHash, txHash);
    assert.equal(restored?.responseData?.currentPortfolioUsd, 5000);
    assert.equal(restarted.getTransactionOwner(txHash), "rebalance:decision-tool-7");
  });
});
