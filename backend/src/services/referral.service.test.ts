import assert from "node:assert/strict";
import test from "node:test";
import { getDefaultReferralCommissionUsd } from "./referral.service";

test("getDefaultReferralCommissionUsd defaults to 50 percent of the paid amount", () => {
  const previousRate = process.env.SELUN_REFERRAL_COMMISSION_RATE;
  delete process.env.SELUN_REFERRAL_COMMISSION_RATE;

  try {
    assert.equal(getDefaultReferralCommissionUsd(34, "confirmed"), 17);
    assert.equal(getDefaultReferralCommissionUsd(19, "paid"), 9.5);
    assert.equal(getDefaultReferralCommissionUsd(34, "pending"), 17);
  } finally {
    if (previousRate === undefined) {
      delete process.env.SELUN_REFERRAL_COMMISSION_RATE;
    } else {
      process.env.SELUN_REFERRAL_COMMISSION_RATE = previousRate;
    }
  }
});

test("getDefaultReferralCommissionUsd always returns zero for rejected transactions", () => {
  const previousRate = process.env.SELUN_REFERRAL_COMMISSION_RATE;
  process.env.SELUN_REFERRAL_COMMISSION_RATE = "0.8";

  try {
    assert.equal(getDefaultReferralCommissionUsd(34, "rejected"), 0);
  } finally {
    if (previousRate === undefined) {
      delete process.env.SELUN_REFERRAL_COMMISSION_RATE;
    } else {
      process.env.SELUN_REFERRAL_COMMISSION_RATE = previousRate;
    }
  }
});

test("getDefaultReferralCommissionUsd respects a configured commission rate", () => {
  const previousRate = process.env.SELUN_REFERRAL_COMMISSION_RATE;
  process.env.SELUN_REFERRAL_COMMISSION_RATE = "0.25";

  try {
    assert.equal(getDefaultReferralCommissionUsd(34, "confirmed"), 8.5);
  } finally {
    if (previousRate === undefined) {
      delete process.env.SELUN_REFERRAL_COMMISSION_RATE;
    } else {
      process.env.SELUN_REFERRAL_COMMISSION_RATE = previousRate;
    }
  }
});
