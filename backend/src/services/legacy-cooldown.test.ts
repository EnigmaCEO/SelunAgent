import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import test, { type TestContext } from "node:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
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

function fixture(t: TestContext): string {
  const root = fs.realpathSync(os.tmpdir());
  const directory = fs.mkdtempSync(path.join(root, "selun-cooldown-test-"));
  t.after(() => {
    assert.equal(path.dirname(fs.realpathSync(directory)), root);
    assert.ok(path.basename(directory).startsWith("selun-cooldown-test-"));
    fs.rmSync(directory, { recursive: true });
  });
  return path.join(directory, "state.json");
}

test("locks escalate to 30 minutes, 2 hours, and 24 hours across restarts", t => {
  const file = fixture(t);
  let now = 1_000_000;
  for (const [index, seconds] of [1800, 7200, 86400, 86400].entries()) {
    let store = new CooldownStore(100, file);
    for (let i = 0; i < 3; i++) assert.equal(store.check(["ip:a", `wallet:${index}`], policy, now + i), 0);
    assert.equal(store.check(["ip:a", `wallet:${index}`], policy, now + 3), seconds);
    store = new CooldownStore(100, file);
    assert.equal(store.strikeLevel(["ip:a"]), Math.min(index + 1, 3));
    assert.equal(store.check(["ip:a"], policy, now + 1003), seconds - 1);
    now += seconds * 1000 + 3;
  }
});

test("a persisted lock applies across route groups without escalating on blocked requests", t => {
  const file = fixture(t);
  let store = new CooldownStore(100, file);
  for (let i = 0; i < 3; i++) assert.equal(store.check(["ip:a"], policy, 100_000 + i, "checkout"), 0);
  assert.equal(store.check(["ip:a"], policy, 100_003, "checkout"), 1800);
  store = new CooldownStore(100, file);
  assert.equal(store.check(["ip:a"], policy, 101_003, "email"), 1799);
  assert.equal(store.strikeLevel(["ip:a"]), 1);
  assert.equal(store.check(["ip:b"], policy, 101_003, "email"), 0);
});

test("strike history resets after seven quiet days, not when the short counter expires", t => {
  const file = fixture(t);
  let store = new CooldownStore(100, file);
  for (let i = 0; i < 4; i++) store.check(["ip:a"], policy, 100_000 + i);
  store = new CooldownStore(100, file);
  assert.equal(store.check(["ip:a"], policy, 2_000_000), 0);
  assert.equal(store.strikeLevel(["ip:a"]), 1);
  const afterQuietPeriod = 100_003 + 7 * 24 * 60 * 60_000;
  store = new CooldownStore(100, file);
  for (let i = 0; i < 3; i++) assert.equal(store.check(["ip:a"], policy, afterQuietPeriod + i), 0);
  assert.equal(store.check(["ip:a"], policy, afterQuietPeriod + 3), 1800);
});

test("attempt counters survive a restart before a lock is triggered", t => {
  const file = fixture(t);
  for (let i = 0; i < 3; i++) assert.equal(new CooldownStore(100, file).check(["ip:a"], policy, 100_000 + i), 0);
  assert.equal(new CooldownStore(100, file).check(["ip:a"], policy, 100_003), 1800);
});

test("corrupt state fails closed rather than granting protected work", t => {
  const file = fixture(t);
  fs.writeFileSync(file, "not valid json");
  const middleware = createLegacyCooldown({ stateFile: file });
  let downstreamCalls = 0;
  let status = 0;
  const res = { set: () => {}, status: (code: number) => { status = code; return res; }, json: () => {} } as unknown as Response;
  middleware(request("/pay"), res, () => { downstreamCalls++; });
  assert.equal(status, 503);
  assert.equal(downstreamCalls, 0);
});

test("failed snapshot writes cannot authorize protected work", t => {
  const directory = path.dirname(fixture(t));
  const parentFile = path.join(directory, "not-a-directory");
  fs.writeFileSync(parentFile, "test");
  const middleware = createLegacyCooldown({ stateFile: path.join(parentFile, "state.json") });
  let downstreamCalls = 0;
  let status = 0;
  const res = { set: () => {}, status: (code: number) => { status = code; return res; }, json: () => {} } as unknown as Response;
  middleware(request("/pay"), res, () => { downstreamCalls++; });
  assert.equal(status, 503);
  assert.equal(downstreamCalls, 0);
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
  const middleware = createLegacyCooldown({ stateFile: false });
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

test("summary, report, and escalation probes share the email cooldown", () => {
  const middleware = createLegacyCooldown({ stateFile: false });
  let downstreamCalls = 0;
  let status = 0;
  const res = { set: () => {}, status: (code: number) => { status = code; return res; }, json: () => {} } as unknown as Response;
  const routes = ["/result-email", "/report-email", "/x402/sce/escalation-brief"];
  for (let i = 0; i < 7; i++) middleware(request(routes[i % routes.length], {}, { resultEmail: `test${i}@example.com` }), res, () => { downstreamCalls++; });
  assert.equal(downstreamCalls, 6);
  assert.equal(status, 429);
});
