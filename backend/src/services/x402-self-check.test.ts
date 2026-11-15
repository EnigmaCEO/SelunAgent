import assert from "node:assert/strict";
import test from "node:test";
import { createCachedCheck, isPaymentChallenge, runX402SelfCheck } from "./x402-self-check";

const BASE = "http://127.0.0.1:8787";
const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const challenge = () => json(402, { success: false, error: "x402_payment_required" });
const manifest = (...paths: string[]) => json(200, { version: 1, resources: paths.map((p) => `https://selun.sagitta.systems${p}`) });

test("isPaymentChallenge accepts only a 402 x402 challenge", () => {
  assert.equal(isPaymentChallenge(402, { error: "x402_payment_required" }), true);
  assert.equal(isPaymentChallenge(402, { error: "something_else" }), false);
  assert.equal(isPaymentChallenge(400, { error: "x402_payment_required" }), false);
  assert.equal(isPaymentChallenge(402, null), false);
});

test("probes every route in the discovery document over the base URL", async () => {
  const calls: string[] = [];
  const result = await runX402SelfCheck({
    baseUrl: BASE,
    fetchImpl: async (url, init) => {
      calls.push(`${init?.method ?? "GET"} ${url}`);
      return url.endsWith("/.well-known/x402") ? manifest("/agent/x402/a", "/agent/x402/b") : challenge();
    },
  });
  assert.equal(result.status, "ok");
  assert.equal(result.detail, "2/2 routes answer unpaid calls with 402");
  assert.deepEqual(calls, [
    `GET ${BASE}/.well-known/x402`,
    `POST ${BASE}/agent/x402/a`,
    `POST ${BASE}/agent/x402/b`,
  ]);
});

test("marks a route that does not return a challenge as failing", async () => {
  const result = await runX402SelfCheck({
    baseUrl: BASE,
    fetchImpl: async (url) => {
      if (url.endsWith("/.well-known/x402")) return manifest("/agent/x402/a", "/agent/x402/b", "/agent/x402/c");
      if (url.endsWith("/b")) return json(400, { error: "riskTolerance must be one of ..." });
      if (url.endsWith("/c")) return new Response("boom", { status: 500 });
      return challenge();
    },
  });
  assert.equal(result.status, "degraded");
  assert.equal(result.detail, "1/3 routes answer unpaid calls with 402");
  assert.deepEqual(
    result.routes.map((r) => [r.path, r.status, r.ok, r.error]),
    [
      ["/agent/x402/a", 402, true, undefined],
      ["/agent/x402/b", 400, false, "riskTolerance must be one of ..."],
      ["/agent/x402/c", 500, false, "unexpected HTTP 500"],
    ],
  );
});

test("reports a network failure without throwing", async () => {
  const result = await runX402SelfCheck({
    baseUrl: BASE,
    fetchImpl: async (url) => {
      if (url.endsWith("/.well-known/x402")) return manifest("/agent/x402/a");
      throw new TypeError("fetch failed");
    },
  });
  assert.equal(result.status, "degraded");
  assert.equal(result.routes[0]?.error, "request failed");
});

test("is degraded when the discovery document is unreadable", async () => {
  const result = await runX402SelfCheck({ baseUrl: BASE, fetchImpl: async () => new Response("nope", { status: 503 }) });
  assert.equal(result.status, "degraded");
  assert.equal(result.routes.length, 0);
});

test("createCachedCheck serves the cache within the TTL and shares one in-flight run", async () => {
  let runs = 0;
  let clock = 0;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const check = createCachedCheck(async () => { runs += 1; await gate; return runs; }, 1_000, () => clock);

  const first = check();
  const second = check();
  release();
  assert.equal(await first, 1);
  assert.equal(await second, 1);
  assert.equal(runs, 1);

  clock = 999;
  assert.equal(await check(), 1);
  clock = 1_000;
  assert.equal(await check(), 2);
});
