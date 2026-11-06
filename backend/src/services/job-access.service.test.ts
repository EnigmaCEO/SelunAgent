import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import express from "express";

import { isSolanaPaymentEnabled } from "../config";
import { createAgentRouter } from "../routes/agent.routes";
import {
  authorizeJobStart,
  buildStatusPath,
  issueDecisionTicket,
  issueRunGrant,
  jobAccessToken,
  readDecisionTicket,
  readRunGrant,
  reserveDelivery,
  verifyInternalRequest,
  verifyJobAccessToken,
} from "./job-access.service";

// The secret and the cooldown's state file are both read on first use.
process.env.SELUN_REFERRAL_INTERNAL_TOKEN = "test-job-access-secret";
process.env.SELUN_RESULT_EMAILS_ENABLED = "0";
process.env.SELUN_ADMIN_USAGE_EMAILS_ENABLED = "0";
process.env.SELUN_LEGACY_COOLDOWN_STATE_FILE = `${process.env.TEMP ?? "/tmp"}/selun-job-access-test-${process.pid}.json`;

const wallet = "0x1111111111111111111111111111111111111111";

const purchase = (decisionId: string, includesReport = false) => ({
  decisionId,
  source: "onchain",
  includesReport,
  chargedAmountUsdc: "19",
  transactionId: "0xabc",
  walletAddress: wallet,
});

test("a run grant starts only jobs for its own decision", () => {
  const grant = issueRunGrant(purchase("SELUN-DEC-1"));
  const allowed = authorizeJobStart(grant, "selun-phase1-SELUN-DEC-1-1700");
  assert.equal(allowed.ok, true);
  assert.equal(allowed.ok && allowed.jobToken, jobAccessToken("selun-phase1-SELUN-DEC-1-1700"));

  for (const other of ["selun-phase1-SELUN-DEC-2-1700", "selun-allocate-SELUN-DEC-1", "selun-phase1-SELUN-DEC-11-1700"]) {
    const denied = authorizeJobStart(grant, other);
    assert.equal(denied.ok, false, other);
    assert.equal(!denied.ok && denied.status, 403);
  }
});

test("missing, forged, tampered and expired grants are refused", () => {
  const grant = issueRunGrant(purchase("SELUN-DEC-2"));
  const [body, signature] = grant.split(".");
  const forgedBody = Buffer.from(JSON.stringify({ typ: "grant", d: "SELUN-DEC-2", src: "onchain", exp: Date.now() + 60_000 })).toString("base64url");
  const jobId = "selun-phase1-SELUN-DEC-2-1";
  for (const bad of [
    undefined,
    "",
    "nonsense",
    `${forgedBody}.${signature}`,
    `${forgedBody}.${createHmac("sha256", "wrong-secret").update(forgedBody).digest("hex")}`,
    `${body}.${signature}.extra`,
    issueRunGrant(purchase("SELUN-DEC-2"), Date.now() - 25 * 60 * 60_000),
  ]) {
    const denied = authorizeJobStart(bad, jobId);
    assert.equal(denied.ok, false);
    assert.equal(!denied.ok && denied.status, 401);
  }
});

test("a decision ticket can't stand in for a run grant", () => {
  const ticket = issueDecisionTicket({ decisionId: "SELUN-DEC-3", walletAddress: wallet, chargedAmountUsdc: "19", includesReport: false });
  assert.equal(authorizeJobStart(ticket, "selun-phase1-SELUN-DEC-3").ok, false);
});

test("a decision ticket keeps the server's price and expires", () => {
  const now = Date.now();
  const ticket = issueDecisionTicket({ decisionId: "D", walletAddress: wallet.toUpperCase().replace("0X", "0x"), chargedAmountUsdc: "19", includesReport: true }, now);
  assert.deepEqual(readDecisionTicket(ticket, now), { decisionId: "D", walletAddress: wallet, chargedAmountUsdc: "19", includesReport: true, issuedAt: now });
  assert.equal(readDecisionTicket(ticket, now + 3 * 60 * 60_000), null);
  assert.equal(readDecisionTicket(issueRunGrant(purchase("D"))), null);
});

test("one purchase can only start a limited number of different jobs", () => {
  const grant = issueRunGrant(purchase("SELUN-DEC-4"));
  for (let i = 0; i < 6; i++) assert.equal(authorizeJobStart(grant, `selun-phase1-SELUN-DEC-4-${i}`).ok, true);
  assert.equal(authorizeJobStart(grant, "selun-phase1-SELUN-DEC-4-0").ok, true, "repeating a known job isn't a new start");
  const denied = authorizeJobStart(grant, "selun-phase1-SELUN-DEC-4-99");
  assert.equal(!denied.ok && denied.status, 429);
});

test("job tokens are tied to one job, and status paths carry a working token", () => {
  const token = jobAccessToken("job-a");
  assert.equal(verifyJobAccessToken("job-a", token), true);
  assert.equal(verifyJobAccessToken("job-b", token), false);
  assert.equal(verifyJobAccessToken("job-a", undefined), false);
  assert.equal(verifyJobAccessToken("job-a", "short"), false);
  const url = new URL(buildStatusPath("job a"), "http://x");
  assert.equal(url.pathname, "/execution-status/job%20a");
  assert.equal(verifyJobAccessToken("job a", url.searchParams.get("accessToken")), true);
});

test("a grant carries the purchase, and a ticket isn't a purchase", () => {
  const signed = purchase("SELUN-DEC-5", true);
  assert.deepEqual(readRunGrant(issueRunGrant(signed)), signed);
  assert.equal(readRunGrant(issueDecisionTicket({ decisionId: "D", walletAddress: wallet, chargedAmountUsdc: "19", includesReport: true })), null);
  assert.equal(readRunGrant("forged.signature"), null);
});

function internalHeaders(path: string, now = Date.now()) {
  const key = createHmac("sha256", "test-job-access-secret").update("selun-job-access-v1").digest();
  return {
    "x-selun-internal-timestamp": String(now),
    "x-selun-internal-signature": createHmac("sha256", key).update(`internal\n${now}\nPOST\n${path}`).digest("hex"),
  };
}

test("internal requests must be fresh and signed for the exact route", () => {
  const now = Date.now();
  const headers = internalHeaders("/agent/store-hash", now);
  const request = { method: "POST", path: "/agent/store-hash", timestamp: headers["x-selun-internal-timestamp"], signature: headers["x-selun-internal-signature"] };
  assert.equal(verifyInternalRequest(request, now), true);
  assert.equal(verifyInternalRequest({ ...request, path: "/agent/report-email" }, now), false);
  assert.equal(verifyInternalRequest({ ...request, method: "GET" }, now), false);
  assert.equal(verifyInternalRequest(request, now + 61_000), false);
  assert.equal(verifyInternalRequest({ ...request, signature: undefined }, now), false);
});

test("deliveries for one purchase are capped per kind", () => {
  for (let i = 0; i < 5; i++) assert.equal(reserveDelivery("store-hash", "SELUN-DEC-6"), true);
  assert.equal(reserveDelivery("store-hash", "SELUN-DEC-6"), false);
  assert.equal(reserveDelivery("result-email", "SELUN-DEC-6"), true);
  assert.equal(reserveDelivery("store-hash", "SELUN-DEC-7"), true);
});

test("Solana is only offered where it can settle", () => {
  const live = { networkId: "base-mainnet", treasurySolanaAddress: "sol", payAiFacilitatorUrl: "https://payai" };
  assert.equal(isSolanaPaymentEnabled(live), true);
  assert.equal(isSolanaPaymentEnabled({ ...live, networkId: "base-sepolia" }), false, "no mainnet Solana in a test setup");
  assert.equal(isSolanaPaymentEnabled({ ...live, payAiFacilitatorUrl: undefined }), false);
  assert.equal(isSolanaPaymentEnabled({ ...live, payAiFacilitatorUrl: "  " }), false);
  assert.equal(isSolanaPaymentEnabled({ ...live, treasurySolanaAddress: null }), false);
});

// ---- The routes themselves

let base = "";
let server: ReturnType<express.Express["listen"]>;

before(async () => {
  const app = express();
  app.use(express.json());
  app.use("/agent", createAgentRouter());
  await new Promise<void>((resolve) => {
    server = app.listen(0, "127.0.0.1", () => resolve());
  });
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

after(async () => {
  await new Promise((resolve) => server.close(resolve));
});

const post = (path: string, body: unknown, headers: Record<string, string> = {}) =>
  fetch(`${base}/agent${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...headers },
    body: JSON.stringify(body),
  });

test("POST /phase1/run refuses to start without a verified payment", async () => {
  const response = await post("/phase1/run", { jobId: "selun-phase1-anything" });
  assert.equal(response.status, 401);
});

for (const phase of [3, 4, 5, 6]) {
  test(`POST /phase${phase}/run needs the job's token`, async () => {
    const jobId = `selun-phase1-gate-test-${phase}`;
    assert.equal((await post(`/phase${phase}/run`, { jobId })).status, 401);
    assert.equal((await post(`/phase${phase}/run`, { jobId }, { "x-selun-job-token": jobAccessToken("another-job") })).status, 401);

    // Past the gate, the job's own state decides: this job has no completed earlier phase.
    const allowed = await post(`/phase${phase}/run`, { jobId }, { "x-selun-job-token": jobAccessToken(jobId) });
    assert.equal(allowed.status, 400);
    assert.match(((await allowed.json()) as { error: string }).error, /can only start after/);
  });
}

test("POST /verify-payment rejects an invalid ticket before checking the chain", async () => {
  const forged = await post("/verify-payment", { fromAddress: wallet, expectedAmountUSDC: "0.01", decisionTicket: "forged" });
  assert.equal(forged.status, 400);

  const someoneElses = issueDecisionTicket({ decisionId: "D", walletAddress: "0x2222222222222222222222222222222222222222", chargedAmountUsdc: "19", includesReport: false });
  const wrongWallet = await post("/verify-payment", { fromAddress: wallet, expectedAmountUSDC: "0.01", decisionTicket: someoneElses });
  assert.equal(wrongWallet.status, 403);
});

// The email routes share a limit of 6 requests per IP, so these two tests make exactly 6.
test("POST /result-email ignores claimed payment details and needs a signed purchase", async () => {
  const forged = await post("/result-email", {
    resultEmail: "victim@example.com",
    payment: { status: "paid", paymentMethod: "card", decisionId: "D", transactionId: "T", chargedAmountUsdc: 0 },
  });
  assert.equal(forged.status, 402);

  const allowed = await post("/result-email", { resultEmail: "buyer@example.com", runGrant: issueRunGrant(purchase("SELUN-DEC-8")) });
  assert.equal(allowed.status, 200);
  assert.equal(((await allowed.json()) as { data: { status: string } }).data.status, "skipped", "email is off in tests");
});

test("POST /report-email is server-only and needs a report purchase", async () => {
  const path = "/agent/report-email";
  const body = { resultEmail: "buyer@example.com", filename: "r.pdf", pdfBase64: "AAAA" };
  const forged = { ...body, payment: { status: "paid", paymentMethod: "card", decisionId: "D", transactionId: "T", chargedAmountUsdc: 0 } };
  assert.equal((await post("/report-email", forged)).status, 401);
  assert.equal((await post("/report-email", forged, internalHeaders("/agent/store-hash"))).status, 401, "signed for another route");
  assert.equal((await post("/report-email", { ...body, runGrant: issueRunGrant(purchase("SELUN-DEC-9", false)) }, internalHeaders(path))).status, 402);
  assert.equal((await post("/report-email", { ...body, runGrant: issueRunGrant(purchase("SELUN-DEC-9", true)) }, internalHeaders(path))).status, 200);
});

test("POST /store-hash is server-only and needs a report purchase for that decision", async () => {
  const path = "/agent/store-hash";
  const body = { decisionId: "SELUN-DEC-10", pdfHash: "abc" };
  assert.equal((await post("/store-hash", body)).status, 401);
  assert.equal((await post("/store-hash", body, internalHeaders(path, Date.now() - 120_000))).status, 401, "stale signature");
  assert.equal((await post("/store-hash", body, internalHeaders(path))).status, 402);
  assert.equal((await post("/store-hash", { ...body, runGrant: issueRunGrant(purchase("SELUN-DEC-10", false)) }, internalHeaders(path))).status, 402);
  assert.equal((await post("/store-hash", { ...body, runGrant: issueRunGrant(purchase("SELUN-DEC-11", true)) }, internalHeaders(path))).status, 402);
});
