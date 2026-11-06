import assert from "node:assert/strict";
import { test } from "node:test";
import { isFacilitatorNonceCollision, settleWithRetry } from "./x402-settlement-retry";

const noSleep = async () => {};
const collision = { success: false, transaction: "", errorMessage: "Missing or invalid parameters.\nDouble check you have provided the correct parameters." };
const lowNonce = { success: false, transaction: "", errorMessage: "Nonce provided for the transaction is lower than the current nonce of the account." };
const settled = { success: true, transaction: "0xabc" };

function sequence<T>(...results: T[]) {
  const calls = { count: 0 };
  return { calls, settle: async () => results[Math.min(calls.count++, results.length - 1)] };
}

test("retries nonce collisions until the settlement lands", async () => {
  const { calls, settle } = sequence<typeof collision | typeof settled>(collision, lowNonce, settled);
  assert.equal(await settleWithRetry(settle, noSleep), settled);
  assert.equal(calls.count, 3);
});

test("a first-time success isn't retried", async () => {
  const { calls, settle } = sequence(settled);
  await settleWithRetry(settle, noSleep);
  assert.equal(calls.count, 1);
});

test("gives up after a bounded number of attempts and returns the failure", async () => {
  const { calls, settle } = sequence(collision);
  assert.equal(await settleWithRetry(settle, noSleep), collision);
  assert.equal(calls.count, 6);
});

test("doesn't retry failures that aren't collisions", async () => {
  for (const failure of [
    { success: false, transaction: "", errorMessage: "The contract function reverted: FiatTokenV2: authorization is used or canceled" },
    { success: false, transaction: "", errorMessage: "insufficient funds" },
    { success: false, transaction: "" },
  ]) {
    const { calls, settle } = sequence(failure);
    await settleWithRetry(settle, noSleep);
    assert.equal(calls.count, 1, failure.errorMessage);
  }
});

test("doesn't retry when a transaction exists, because it may have charged", () => {
  assert.equal(isFacilitatorNonceCollision({ ...collision, transaction: "0xabc" }), false);
});
