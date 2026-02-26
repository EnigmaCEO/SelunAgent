import assert from "node:assert/strict";
import test from "node:test";
import { isExpiredIsoTimestamp, normalizeOptionalBoolean } from "./x402-utils";

test("normalizeOptionalBoolean handles boolean-like inputs safely", () => {
  assert.deepEqual(normalizeOptionalBoolean(undefined), { value: false, valid: true });
  assert.deepEqual(normalizeOptionalBoolean(true), { value: true, valid: true });
  assert.deepEqual(normalizeOptionalBoolean(false), { value: false, valid: true });
  assert.deepEqual(normalizeOptionalBoolean("true"), { value: true, valid: true });
  assert.deepEqual(normalizeOptionalBoolean(" false "), { value: false, valid: true });
  assert.deepEqual(normalizeOptionalBoolean("0"), { value: false, valid: false });
});

test("isExpiredIsoTimestamp evaluates timestamp expiry deterministically", () => {
  const now = Date.parse("2026-02-26T12:00:00.000Z");
  assert.equal(isExpiredIsoTimestamp("2026-02-26T11:59:59.999Z", now), true);
  assert.equal(isExpiredIsoTimestamp("2026-02-26T12:00:00.000Z", now), true);
  assert.equal(isExpiredIsoTimestamp("2026-02-26T12:00:00.001Z", now), false);
  assert.equal(isExpiredIsoTimestamp("not-a-date", now), true);
});
