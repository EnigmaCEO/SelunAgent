import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

// The redemption file lives under <cwd>/data when cwd is named "backend", so keep it in a scratch folder.
const root = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "selun-promo-")), "backend");
fs.mkdirSync(root);
process.chdir(root);
Object.assign(process.env, {
  COINBASE_API_KEY: "test",
  COINBASE_API_SECRET: "test",
  BASE_SEPOLIA_RPC: "http://127.0.0.1:1",
  BASE_MAINNET_RPC: "http://127.0.0.1:1",
  USDC_CONTRACT_ADDRESS: "0x036CbD53842c5426634e7929541eC2318f3dCF7e",
  SELUN_FREE_CODES_JSON: JSON.stringify([
    { code: "ONEUSE", maxUses: 1, includeCertifiedDecisionRecord: true, discountPercent: 20 },
    { code: "FREEONE", maxUses: 1, includeCertifiedDecisionRecord: true, discountPercent: 100 },
  ]),
});

const wallet = (digit: string) => `0x${digit.repeat(40)}`;
const stored = () => JSON.parse(fs.readFileSync(path.join(root, "data", "free-code-redemptions.json"), "utf8")).redemptions as Array<{ decisionId: string; pendingUntil?: string }>;

test("a discount code counts as used only once its checkout is paid", async () => {
  const { authorizeWizardPayment, settlePromoCodeRedemption } = await import("./selun-agent.service.js");

  // Unpaid checkouts don't use the single use, by the same wallet or another.
  const abandoned = await authorizeWizardPayment({ walletAddress: wallet("1"), promoCode: "ONEUSE" });
  assert.equal(abandoned.chargedAmountUsdc, "15.2");
  await authorizeWizardPayment({ walletAddress: wallet("1"), promoCode: "ONEUSE" });
  await new Promise((resolve) => setTimeout(resolve, 2)); // distinct decision IDs
  const paid = await authorizeWizardPayment({ walletAddress: wallet("2"), promoCode: "ONEUSE" });
  assert.ok(stored().every((entry) => entry.pendingUntil));

  // Paying settles that checkout, and the code is now used up.
  assert.equal(settlePromoCodeRedemption(paid.decisionId), "ONEUSE");
  assert.equal(stored().find((entry) => entry.decisionId === paid.decisionId)?.pendingUntil, undefined);
  await assert.rejects(authorizeWizardPayment({ walletAddress: wallet("3"), promoCode: "ONEUSE" }), /usage limit reached/);
  assert.equal(settlePromoCodeRedemption("SELUN-DEC-unknown"), null);
});

test("a free code is used when the checkout is granted", async () => {
  const { authorizeWizardPayment } = await import("./selun-agent.service.js");
  const granted = await authorizeWizardPayment({ walletAddress: wallet("4"), promoCode: "FREEONE" });
  assert.equal(granted.paymentMethod, "free_code");
  assert.equal(stored().find((entry) => entry.decisionId === granted.decisionId)?.pendingUntil, undefined);
  await assert.rejects(authorizeWizardPayment({ walletAddress: wallet("5"), promoCode: "FREEONE" }), /usage limit reached/);
});
