import assert from "node:assert/strict";
import fs from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { inflateSync } from "node:zlib";
import express from "express";
import { PDFDocument } from "pdf-lib";
import { issueDecisionTicket, issueReportToken, issueRunGrant } from "../services/job-access.service";
import { isValidEmail, sendResultDocumentEmail } from "../services/email.service";
import { createAssistantRouter } from "./assistant.routes";
import { TOOL_CAPABILITY_IDS } from "./capability-registry";
import { explanationIssues, replyIssues } from "./claim-guard";
import { presentResult } from "./result-presenter";
import { resultRecordHash } from "./result-report";
import { ReportService } from "./report.service";

process.env.SELUN_REFERRAL_INTERNAL_TOKEN = "test-report-secret";

const TX = `0x${"b".repeat(64)}`;
const PAYER = "0x436B52e5E76e3bfa124215376e4a294c7d89f8D1";
const DECISION = "selun-ai-11111111-1111-4111-8111-111111111111";

function paidResponse(decisionId = DECISION, compositeScore = 0.811) {
  return {
    status: "completed",
    endpoint: "/agent/x402/asset-scorecard",
    decisionId,
    productId: "asset_scorecard",
    payment: { required: true, chargedAmountUsdc: "0.5", verified: true, fromAddress: PAYER, transactionHash: TX, network: "eip155:84532" },
    result: {
      snapshotAt: "2026-11-05T07:25:00.000Z",
      assetsRequested: ["BTC"],
      portfolioSegment: "Bluechips",
      qualifiedCandidatesCount: 37,
      screenedCandidatesCount: 300,
      scorecards: [{ asset: "BTC", name: "Bitcoin", role: "core", riskClass: "large_cap_crypto", selected: true, selectionReasons: ["controlled_risk_profile", "high_quality_signal", "lower_drawdown_proxy", "lower_volatility_proxy", "profile_match_alignment"], qualityScore: 0.91, riskScore: 0.17, liquidityScore: 1, structuralScore: 0.94, compositeScore }],
    },
  };
}

function setup(options: { stored?: () => Record<string, unknown> | null; sendEmail?: ConstructorParameters<typeof ReportService>[0]["sendEmail"] } = {}) {
  const stateFile = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "selun-report-")), "fulfillment.json");
  const loads: string[] = [];
  const deps = {
    stateFile,
    now: () => new Date("2026-11-05T08:00:00.000Z"),
    sendEmail: options.sendEmail,
    loadPaidResponse: (capabilityId: string, decisionId: string) => {
      loads.push(`${capabilityId}:${decisionId}`);
      return options.stored ? options.stored() : decisionId === DECISION ? paidResponse() : null;
    },
  };
  return { deps, loads, stateFile, service: new ReportService(deps), token: issueReportToken("asset_scorecard", DECISION) };
}

/** Reads the words actually drawn in the PDF. */
function pdfText(bytes: Uint8Array): string {
  const raw = Buffer.from(bytes).toString("latin1");
  const parts: string[] = [];
  for (const match of raw.matchAll(/stream\r?\n([\s\S]*?)\r?\n?endstream/g)) {
    let content: string;
    try {
      content = inflateSync(Buffer.from(match[1], "latin1")).toString("latin1");
    } catch {
      content = match[1];
    }
    for (const hex of content.matchAll(/<([0-9A-Fa-f]+)>\s*Tj/g)) parts.push(Buffer.from(hex[1], "hex").toString("latin1"));
  }
  return parts.join(" ");
}

// ---- Disclosures ----

test("every service states what it includes on the quote and the result", () => {
  const document = presentResult("asset_scorecard", "Asset Scorecard", paidResponse());
  assert.deepEqual(document?.coverage.map((item) => [item.label, item.included]), [["Live prices", false], ["Current news", false], ["Real-time market data", false]]);
  assert.match(document?.notice ?? "", /not a recommendation to buy, sell, or hold/);
  assert.equal(TOOL_CAPABILITY_IDS.length, 7);
});

test("the result no longer uses recommendation wording of its own", () => {
  const serialized = JSON.stringify(presentResult("asset_scorecard", "Asset Scorecard", paidResponse())?.sections);
  assert.ok(!serialized.includes("Selected for plan"));
  assert.ok(serialized.includes("In Selun's model selection"));
});

// ---- Claim checks ----

test("flags recommendation language and predictions", () => {
  for (const text of ["Bitcoin is a strong fit.", "It's a good investment.", "You should buy BTC.", "I recommend buying it.", "Returns are guaranteed.", "BTC will go up soon."]) {
    assert.ok(replyIssues(text, "").length > 0, text);
  }
});

test("doesn't flag a refusal or a plain definition", () => {
  for (const text of ["I can't say whether Bitcoin will go up.", "Nothing is guaranteed, and this is not a recommendation.", "A stablecoin aims to hold a steady value.", "Liquidity means how easily an asset can be traded."]) {
    assert.deepEqual(replyIssues(text, ""), [], text);
  }
});

test("a summary may only use numbers from the result", () => {
  const source = JSON.stringify(presentResult("asset_scorecard", "Asset Scorecard", paidResponse()));
  assert.deepEqual(explanationIssues("Bitcoin scored 81/100, with 37 of 300 assets qualifying.", source), []);
  assert.ok(explanationIssues("Bitcoin scored 85/100.", source).some((issue) => issue.includes("85")));
});

test("a summary can't tell the customer what should happen", () => {
  const source = '"Minimum in stablecoins","32%" "Block" "Pause"';
  assert.ok(explanationIssues("At least 32% should stay in stablecoins.", source).length > 0);
  assert.ok(explanationIssues("The checker says to stop and not proceed.", source).length > 0);
  assert.deepEqual(explanationIssues("The service's stablecoin minimum for this profile is 32%.", source), []);
});

test("the scorecard says why the largest assets can score the same", () => {
  const serialized = JSON.stringify(presentResult("asset_scorecard", "Asset Scorecard", paidResponse())?.sections);
  assert.ok(serialized.includes("the largest assets can score the same"));
});

test("a reply may only state dollar amounts from a tool or the customer", () => {
  assert.deepEqual(replyIssues("The scorecard costs $0.50.", '{"price_usdc":"0.5"}'), []);
  // What the Protocol's sources don't support, even where the whitepaper's shorthand invites it.
  assert.ok(replyIssues("The Treasury converts Vault deposits into allocation batches.", "").length > 0);
  assert.ok(replyIssues("The Treasury turns those deposits into batches.", "").length > 0);
  assert.deepEqual(replyIssues("The Treasury doesn't convert deposits. It allocates a matching amount of Stability Units and routes that capital through the Escrow.", ""), []);
  assert.deepEqual(replyIssues("Aave uses deposits to fund loans.", ""), []);
  assert.ok(replyIssues("The Sagitta Protocol is governed by protocol law, not by a banking regulator.", "").length > 0);
  assert.ok(replyIssues("So the Sagitta Protocol isn't regulated.", "").length > 0);
  assert.deepEqual(replyIssues("The whitepaper and sites don't name a regulator for the Sagitta Protocol.", ""), []);
  assert.deepEqual(replyIssues("USDT isn't regulated in that sense.", ""), []);
  assert.deepEqual(replyIssues("You hold $4,000 of BTC.", "I hold $4000 of BTC"), []);
  assert.ok(replyIssues("Bitcoin is trading at $68,000.", '{"price_usdc":"0.5"}').length > 0);
  assert.ok(replyIssues("That is about 68000 USD.", "").length > 0);
});

// ---- PDF ----

test("the PDF shows the server's values, disclosure, and purchase record", async () => {
  const { service, token } = setup();
  const report = await service.download(token);
  assert.equal(Buffer.from(report.bytes).subarray(0, 5).toString(), "%PDF-");
  await PDFDocument.load(report.bytes);

  const text = pdfText(report.bytes);
  for (const expected of ["Asset Scorecard", "81 / 100", "91 / 100", "17 / 100", "37 of 300 screened assets", "Live prices", "not a recommendation to buy, sell", "0.5 USDC", "Base Sepolia (test network)", PAYER, DECISION]) {
    assert.ok(text.includes(expected), `missing "${expected}"`);
  }
  assert.ok(text.replace(/\s/g, "").includes(TX), "the transaction hash may wrap across lines");
  assert.ok(text.replace(/\s/g, "").includes(report.recordHash));
  assert.match(report.filename, /^selun-asset-scorecard-selun-ai-[0-9a-f-]+\.pdf$/);
  assert.equal((await PDFDocument.load(report.bytes)).getPageCount(), 1, "a single result fits on one page");
});

test("repeat downloads are byte-identical and don't create anything new", async () => {
  const { service, token, loads } = setup();
  const first = await service.download(token);
  const second = await service.download(token);
  assert.deepEqual(Buffer.from(first.bytes), Buffer.from(second.bytes));
  assert.equal(loads.length, 1, "the paid record is read once, then the frozen copy is used");
});

test("an issued report survives a restart, and later changes can't alter it", async () => {
  let stored: Record<string, unknown> | null = paidResponse();
  const { deps, service, token } = setup({ stored: () => stored });
  const first = await service.download(token);

  stored = paidResponse(DECISION, 0.01);
  const restarted = new ReportService(deps);
  const again = await restarted.download(token);
  assert.deepEqual(Buffer.from(again.bytes), Buffer.from(first.bytes));
  assert.ok(pdfText(again.bytes).includes("81 / 100"));

  stored = null;
  assert.equal((await new ReportService(deps).download(token)).recordHash, first.recordHash);
});

test("the fingerprint changes if any certified value changes", () => {
  const original = presentResult("asset_scorecard", "Asset Scorecard", paidResponse())!;
  const altered = presentResult("asset_scorecard", "Asset Scorecard", paidResponse(DECISION, 0.99))!;
  assert.equal(resultRecordHash(original), resultRecordHash(structuredClone(original)));
  assert.notEqual(resultRecordHash(original), resultRecordHash(altered));
});

test("no report without a signed token for a real paid result", async () => {
  const { service, token } = setup();
  const forged = `${token.slice(0, -1)}${token.endsWith("0") ? "1" : "0"}`;
  for (const bad of [undefined, "", "forged.signature", forged, issueRunGrant({ decisionId: DECISION, source: "onchain", includesReport: true, chargedAmountUsdc: "0.5", transactionId: TX, walletAddress: PAYER }), issueDecisionTicket({ decisionId: DECISION, walletAddress: PAYER, chargedAmountUsdc: "0.5", includesReport: true })]) {
    await assert.rejects(service.download(bad), (error: { code?: string }) => error.code === "report_not_authorized");
  }
  await assert.rejects(service.download(issueReportToken("asset_scorecard", "selun-ai-no-such-decision")), (error: { code?: string }) => error.code === "report_not_found");
  await assert.rejects(service.download(issueReportToken("not_a_service", DECISION)), (error: { code?: string }) => error.code === "report_not_found");
});

test("a stored response without a verified receipt gives no report", async () => {
  const unverified = paidResponse();
  (unverified.payment as { verified: boolean }).verified = false;
  const { service, token } = setup({ stored: () => unverified });
  await assert.rejects(service.download(token), (error: { code?: string }) => error.code === "report_not_found");
});

test("a token can't be reused for a different service on the same decision", async () => {
  const { service, token } = setup();
  await service.download(token);
  await assert.rejects(service.download(issueReportToken("market_regime", DECISION)), (error: { code?: string }) => error.code === "report_not_authorized");
});

// ---- Email ----

function emailSpy(outcomes: Awaited<ReturnType<typeof sendResultDocumentEmail>>[] = []) {
  const sent: Parameters<typeof sendResultDocumentEmail>[0][] = [];
  const send: typeof sendResultDocumentEmail = async (input) => {
    sent.push(input);
    return outcomes.shift() ?? { status: "sent" };
  };
  return { sent, send };
}

test("emails the frozen PDF to the approved address and stores only a hash of it", async () => {
  const spy = emailSpy();
  const { service, token, stateFile } = setup({ sendEmail: spy.send });
  const downloaded = await service.download(token);
  const result = await service.email(token, { email: " Buyer@Example.com ", approved: true });

  assert.deepEqual(result, { status: "sent", recipientsRemaining: 2 });
  assert.equal(spy.sent[0].toEmail, "Buyer@Example.com");
  assert.equal(spy.sent[0].pdfBase64, Buffer.from(downloaded.bytes).toString("base64"), "the attachment is the same PDF");
  assert.ok(!fs.readFileSync(stateFile, "utf8").toLowerCase().includes("buyer@example.com"));
});

test("an email needs explicit approval and a valid address", async () => {
  const spy = emailSpy();
  const { service, token } = setup({ sendEmail: spy.send });
  await assert.rejects(service.email(token, { email: "buyer@example.com" }), (error: { code?: string }) => error.code === "approval_required");
  for (const email of ["", "nope", "a@b", 7, "x".repeat(250) + "@example.com"]) {
    await assert.rejects(service.email(token, { email, approved: true }), (error: { code?: string }) => error.code === "invalid_email");
  }
  assert.equal(spy.sent.length, 0);
});

test("a repeat to the same address sends nothing, even in another case or after a restart", async () => {
  const spy = emailSpy();
  const { deps, service, token } = setup({ sendEmail: spy.send });
  await service.email(token, { email: "buyer@example.com", approved: true });
  assert.equal((await service.email(token, { email: "BUYER@example.com", approved: true })).status, "already_sent");
  assert.equal((await new ReportService(deps).email(token, { email: "buyer@example.com", approved: true })).status, "already_sent");
  assert.equal(spy.sent.length, 1);
});

test("a double click sends one email", async () => {
  const spy = emailSpy();
  const { service, token } = setup({ sendEmail: spy.send });
  const results = await Promise.all([1, 2, 3].map(() => service.email(token, { email: "buyer@example.com", approved: true })));
  assert.equal(spy.sent.length, 1);
  assert.deepEqual(results.map((result) => result.status).sort(), ["already_sent", "already_sent", "sent"]);
});

test("a failed send can be retried and doesn't use up a recipient", async () => {
  const spy = emailSpy([{ status: "failed", error: "Resend email failed with HTTP 401: API key is invalid" }, { status: "sent" }]);
  const { service, token } = setup({ sendEmail: spy.send });
  await assert.rejects(service.email(token, { email: "buyer@example.com", approved: true }), (error: { code?: string; message?: string }) => error.code === "report_email_failed" && !error.message?.includes("401"));
  assert.deepEqual(await service.email(token, { email: "buyer@example.com", approved: true }), { status: "sent", recipientsRemaining: 2 });
  assert.equal(spy.sent.length, 2);
});

test("disabled email says so and records nothing", async () => {
  const spy = emailSpy([{ status: "skipped", error: "Result email delivery is disabled." }]);
  const { service, token, stateFile } = setup({ sendEmail: spy.send });
  await assert.rejects(service.email(token, { email: "buyer@example.com", approved: true }), (error: { code?: string }) => error.code === "report_email_failed");
  assert.equal((JSON.parse(fs.readFileSync(stateFile, "utf8")) as Record<string, { emails: unknown[] }>)[DECISION].emails.length, 0);
});

test("a report goes to at most three different addresses", async () => {
  const spy = emailSpy();
  const { service, token } = setup({ sendEmail: spy.send });
  for (const email of ["a@example.com", "b@example.com", "c@example.com"]) await service.email(token, { email, approved: true });
  await assert.rejects(service.email(token, { email: "d@example.com", approved: true }), (error: { code?: string }) => error.code === "report_email_limit");
  assert.equal(spy.sent.length, 3);
});

test("the mock provider records instead of sending, and refuses production", async () => {
  const folder = fs.mkdtempSync(path.join(os.tmpdir(), "selun-mock-email-"));
  const saved = { ...process.env };
  const fetchBefore = globalThis.fetch;
  globalThis.fetch = (async () => { throw new Error("the mock provider must not call the network"); }) as typeof fetch;
  Object.assign(process.env, { SELUN_EMAIL_PROVIDER: "mock", SELUN_MOCK_EMAIL_DIR: folder, SELUN_RESULT_EMAILS_ENABLED: "1", NODE_ENV: "test" });
  const input = { toEmail: "buyer@example.com", decisionId: DECISION, title: "Asset Scorecard", filename: "r.pdf", pdfBase64: "QUJD", amountPaid: "0.5 USDC", paymentReference: { label: "Payment transaction", value: TX }, recordHash: "abc" };
  try {
    assert.deepEqual(await sendResultDocumentEmail(input), { status: "test_recorded" });
    await sendResultDocumentEmail(input);
    const files = fs.readdirSync(folder);
    assert.equal(files.length, 1, "repeats share an idempotency key");
    const recorded = JSON.parse(fs.readFileSync(path.join(folder, files[0]), "utf8"));
    assert.deepEqual(recorded.to, ["buyer@example.com"]);
    assert.equal(recorded.attachments[0].content, "QUJD");

    process.env.NODE_ENV = "production";
    const outcome = await sendResultDocumentEmail(input);
    assert.equal(outcome.status, "failed");
  } finally {
    globalThis.fetch = fetchBefore;
    process.env = saved;
  }
  assert.ok(isValidEmail("buyer@example.com"));
});

// ---- HTTP ----

function withServer(app: express.Express, run: (baseUrl: string) => Promise<void>) {
  return new Promise<void>((resolve, reject) => {
    const server: Server = createServer(app).listen(0, "127.0.0.1", async () => {
      try {
        await run(`http://127.0.0.1:${(server.address() as AddressInfo).port}`);
        server.close(() => resolve());
      } catch (error) {
        server.close(() => reject(error));
      }
    });
  });
}

test("HTTP: the download route ignores anything but the token", async () => {
  const spy = emailSpy();
  const { deps, token } = setup({ sendEmail: spy.send });
  const app = express();
  app.use(express.json());
  app.use("/assistant", createAssistantRouter({ llm: null, gateway: { requestChallenge: async () => { throw new Error("no"); }, submitPaid: async () => { throw new Error("no"); } }, loadCatalog: async () => ({ resources: [] }) }, deps));

  await withServer(app, async (baseUrl) => {
    const post = (route: string, body: unknown) => fetch(`${baseUrl}/assistant/report/${route}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });

    const tampered = await post("download", {
      reportToken: token,
      document: { title: "Tampered" },
      sections: [{ kind: "note", text: "100 / 100" }],
      result: { scorecards: [{ compositeScore: 1 }] },
      payment: { transactionHash: "0xfake", chargedAmountUsdc: "0" },
    });
    assert.equal(tampered.status, 200);
    assert.equal(tampered.headers.get("content-type"), "application/pdf");
    assert.match(tampered.headers.get("content-disposition") ?? "", /attachment; filename="selun-asset-scorecard-/);
    const bytes = new Uint8Array(await tampered.arrayBuffer());
    const text = pdfText(bytes);
    assert.ok(text.includes("81 / 100") && !text.includes("Tampered") && !text.includes("0xfake"));

    assert.equal((await post("download", { decisionId: DECISION })).status, 403, "a decision id alone isn't enough");
    assert.equal((await post("email", { reportToken: token, email: "buyer@example.com" })).status, 400);
    const emailed = await post("email", { reportToken: token, email: "buyer@example.com", approved: true });
    assert.deepEqual(await emailed.json(), { success: true, status: "sent", recipientsRemaining: 2 });
  });
});
