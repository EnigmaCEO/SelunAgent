import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import test from "node:test";
import type { Request, Response } from "express";
import { CooldownStore, createLegacyCooldown, legacyClientIp } from "./legacy-cooldown";

const policy = { limit: 3, windowMs: 900_000, cooldownMs: 1_800_000 };

test("rotating wallets cannot bypass the IP cooldown; blocked requests do not extend it", () => {
  const store = new CooldownStore();
  for (let i = 0; i < 3; i++) assert.equal(store.check(["ip:a", `wallet:${i}`], policy, 100_000 + i), 0);
  assert.equal(store.check(["ip:a", "wallet:new"], policy, 100_003), 1800);
  assert.equal(store.check(["ip:a", "wallet:another"], policy, 101_003), 1799);
  assert.equal(store.check(["ip:a", "wallet:new"], policy, 1_900_003), 0);
});

test("rotating IPs cannot bypass a wallet cooldown; unrelated clients remain allowed", () => {
  const store = new CooldownStore();
  for (let i = 0; i < 3; i++) assert.equal(store.check([`ip:${i}`, "wallet:a"], policy, 100_000 + i), 0);
  assert.equal(store.check(["ip:next", "wallet:a"], policy, 100_003), 1800);
  assert.equal(store.check(["ip:other", "wallet:b"], policy, 100_003), 0);
});

test("old attempts expire and full stores reject new clients without evicting locks", () => {
  const store = new CooldownStore(2);
  assert.equal(store.check(["ip:a", "wallet:a"], policy, 100_000), 0);
  assert.equal(store.check(["ip:b"], policy, 100_001), 60);
  assert.equal(store.check(["ip:b"], policy, 1_000_000), 0);
});

function request(path: string, headers: Record<string, string> = {}, body = {}): Request {
  return { method: "POST", path, originalUrl: `/agent${path}`, socket: { remoteAddress: "127.0.0.1" },
    body, get: (name: string) => headers[name.toLowerCase()] } as unknown as Request;
}

test("client IP assertions require a valid signature, fresh timestamp, and matching route", () => {
  const previous = process.env.SELUN_REFERRAL_INTERNAL_TOKEN;
  const fly = process.env.FLY_APP_NAME;
  process.env.SELUN_REFERRAL_INTERNAL_TOKEN = "offline-test";
  delete process.env.FLY_APP_NAME;
  try {
    const timestamp = String(Date.now());
    const ip = "203.0.113.10";
    const signature = createHmac("sha256", "offline-test").update(`${timestamp}\nPOST\n/agent/pay\n${ip}`).digest("hex");
    const headers = { "x-selun-client-ip": ip, "x-selun-client-timestamp": timestamp, "x-selun-client-signature": signature };
    assert.equal(legacyClientIp(request("/pay", headers)), ip);
    assert.equal(legacyClientIp(request("/result-email", headers)), "127.0.0.1");
    assert.equal(legacyClientIp(request("/pay", { ...headers, "x-selun-client-ip": "203.0.113.11" })), "127.0.0.1");
    assert.equal(legacyClientIp(request("/pay", { ...headers, "x-selun-client-timestamp": "1" })), "127.0.0.1");
    assert.equal(legacyClientIp(request("/pay", { "x-forwarded-for": ip, "fly-client-ip": ip })), "127.0.0.1");
    process.env.FLY_APP_NAME = "offline-test";
    assert.equal(legacyClientIp(request("/pay", { "fly-client-ip": ip })), ip);
  } finally {
    if (previous === undefined) delete process.env.SELUN_REFERRAL_INTERNAL_TOKEN;
    else process.env.SELUN_REFERRAL_INTERNAL_TOKEN = previous;
    if (fly === undefined) delete process.env.FLY_APP_NAME;
    else process.env.FLY_APP_NAME = fly;
  }
});

test("middleware rejects before downstream work with HTTP 429 and Retry-After", () => {
  const middleware = createLegacyCooldown();
  let downstreamCalls = 0;
  let status = 0;
  const headers: Record<string, string> = {};
  let payload: any;
  const res = { set: (name: string, value: string) => { headers[name] = value; },
    status: (code: number) => { status = code; return res; }, json: (body: unknown) => { payload = body; } } as unknown as Response;
  for (let i = 0; i < 4; i++) middleware(request(i === 3 ? "/PAY/" : "/pay", {}, { walletAddress: `0x${String(i + 1).repeat(40)}` }), res, () => { downstreamCalls++; });
  assert.equal(downstreamCalls, 3);
  assert.equal(status, 429);
  assert.equal(headers["Retry-After"], "1800");
  assert.equal(payload.reason, "security_cooldown");
  middleware(request("/verify-payment"), res, () => { downstreamCalls++; });
  assert.equal(downstreamCalls, 4); // a customer can still confirm an already submitted transfer
});

test("summary and report requests share the email cooldown", () => {
  const middleware = createLegacyCooldown();
  let downstreamCalls = 0;
  let status = 0;
  const res = { set: () => {}, status: (code: number) => { status = code; return res; }, json: () => {} } as unknown as Response;
  for (let i = 0; i < 7; i++) middleware(request(i % 2 ? "/report-email" : "/result-email", {}, { resultEmail: `test${i}@example.com` }), res, () => { downstreamCalls++; });
  assert.equal(downstreamCalls, 6);
  assert.equal(status, 429);
});
