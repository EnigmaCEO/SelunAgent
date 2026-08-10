import assert from "node:assert/strict";
import test from "node:test";
import { Attribution } from "ox/erc8021";
import { BASE_BUILDER_CODE, BUILDER_CODE_DATA_SUFFIX, appendBuilderCode } from "./builder-code";

// The frontend hardcodes this same value in `app/lib/builder-code.ts` to keep
// `ox` out of the client bundle. If this assertion fails, update that file too.
const FRONTEND_DATA_SUFFIX = "0x62635f733571673531646f0b0080218021802180218021802180218021";

test("data suffix matches the frontend constant", () => {
  assert.equal(BUILDER_CODE_DATA_SUFFIX, FRONTEND_DATA_SUFFIX);
});

test("data suffix decodes back to the builder code", () => {
  const calldata = `0xa9059cbb${"00".repeat(64)}` as const;
  const decoded = Attribution.fromData(`${calldata}${BUILDER_CODE_DATA_SUFFIX.slice(2)}` as `0x${string}`);
  assert.deepEqual(decoded?.codes, [BASE_BUILDER_CODE]);
});

test("appendBuilderCode preserves the original calldata", () => {
  const calldata = `0xa9059cbb${"00".repeat(64)}`;
  const attributed = appendBuilderCode(calldata as `0x${string}`);
  assert.ok(attributed.startsWith(calldata));
  assert.equal(attributed, `${calldata}${BUILDER_CODE_DATA_SUFFIX.slice(2)}`);
});

test("appendBuilderCode leaves bare value transfers untouched", () => {
  // Attaching calldata would invoke the fallback of a smart-account recipient.
  assert.equal(appendBuilderCode("0x"), "0x");
  assert.equal(appendBuilderCode(undefined), "0x");
});
