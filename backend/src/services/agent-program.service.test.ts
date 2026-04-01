import assert from "node:assert/strict";
import test from "node:test";
import {
  getAgentProgramConfig,
  normalizeAgentProgramReferrerId,
  signAgentProgramPayload,
} from "./agent-program.service";

test("normalizeAgentProgramReferrerId accepts agent and human ids", () => {
  assert.equal(normalizeAgentProgramReferrerId("agent_123"), "agent_123");
  assert.equal(normalizeAgentProgramReferrerId("HUMAN_alpha-42"), "human_alpha-42");
  assert.equal(normalizeAgentProgramReferrerId("0x8ba1f109551bd432803012645ac136ddd64dba72"), null);
  assert.equal(normalizeAgentProgramReferrerId("ASH123"), null);
});

test("getAgentProgramConfig falls back to deterministic defaults", () => {
  const previousPayout = process.env.AGENT_REFERRAL_FIXED_PAYOUT_USDC;
  const previousMinimum = process.env.AGENT_REFERRAL_MIN_WITHDRAWAL_USDC;
  delete process.env.AGENT_REFERRAL_FIXED_PAYOUT_USDC;
  delete process.env.AGENT_REFERRAL_MIN_WITHDRAWAL_USDC;

  try {
    const config = getAgentProgramConfig();
    assert.equal(config.fixedPayoutUsdc, 20);
    assert.equal(config.minimumWithdrawalUsdc, 50);
    assert.equal(config.payoutAsset, "USDC");
    assert.equal(config.payoutNetwork, "Base");
    assert.equal(config.withdrawalCooldownHours, 24);
    assert.equal(config.requireWithdrawalSignature, false);
    assert.equal(config.withdrawalSignatureMaxAgeMinutes, 15);
  } finally {
    if (previousPayout === undefined) {
      delete process.env.AGENT_REFERRAL_FIXED_PAYOUT_USDC;
    } else {
      process.env.AGENT_REFERRAL_FIXED_PAYOUT_USDC = previousPayout;
    }
    if (previousMinimum === undefined) {
      delete process.env.AGENT_REFERRAL_MIN_WITHDRAWAL_USDC;
    } else {
      process.env.AGENT_REFERRAL_MIN_WITHDRAWAL_USDC = previousMinimum;
    }
  }
});

test("signAgentProgramPayload only signs when a secret is configured", () => {
  const previousSecret = process.env.AGENT_REFERRAL_RESPONSE_SIGNING_SECRET;
  delete process.env.AGENT_REFERRAL_RESPONSE_SIGNING_SECRET;
  assert.equal(signAgentProgramPayload({ ok: true }), null);

  process.env.AGENT_REFERRAL_RESPONSE_SIGNING_SECRET = "test-secret";
  const signature = signAgentProgramPayload({ ok: true });
  assert.equal(typeof signature, "string");
  assert.equal(signature?.length, 64);

  if (previousSecret === undefined) {
    delete process.env.AGENT_REFERRAL_RESPONSE_SIGNING_SECRET;
  } else {
    process.env.AGENT_REFERRAL_RESPONSE_SIGNING_SECRET = previousSecret;
  }
});
