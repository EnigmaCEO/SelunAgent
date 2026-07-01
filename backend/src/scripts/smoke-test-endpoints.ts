/**
 * Smoke test for Selun x402 endpoints.
 *
 * Calls each endpoint in the recommended workflow order:
 *   market-regime → policy-envelope → asset-scorecard → allocate
 *   sce/risk-evaluate → sce/case-relevance → sce/continuity-mode
 *
 * For paid endpoints this script only verifies the 402 challenge is
 * well-formed (correct status, PAYMENT-REQUIRED header, x402 version 2).
 * It does NOT submit payment — use your x402 client to pay and retry.
 *
 * Usage:
 *   npx ts-node src/scripts/smoke-test-endpoints.ts [base=https://selun.sagitta.systems]
 */

const DEFAULT_BASE_URL = "https://selun.sagitta.systems";

function readArg(name: string, fallback: string): string {
  const match = process.argv.find((a) => a.startsWith(`${name}=`));
  return match ? match.slice(name.length + 1) : fallback;
}

const BASE = readArg("base", DEFAULT_BASE_URL).replace(/\/$/, "");
const DECISION_PREFIX = `smoke-${Date.now()}`;

type CheckResult = {
  name: string;
  endpoint: string;
  status: "pass" | "fail" | "warn";
  httpStatus?: number;
  note: string;
};

const results: CheckResult[] = [];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function readJsonObject(res: Response): Promise<Record<string, unknown>> {
  const body = await res.json().catch(() => ({}));
  return isRecord(body) ? body : {};
}

function pass(name: string, endpoint: string, httpStatus: number, note: string) {
  results.push({ name, endpoint, status: "pass", httpStatus, note });
  console.log(`  ✓ ${name} (${httpStatus}) — ${note}`);
}

function fail(name: string, endpoint: string, httpStatus: number | undefined, note: string) {
  results.push({ name, endpoint, status: "fail", httpStatus, note });
  console.error(`  ✗ ${name}${httpStatus !== undefined ? ` (${httpStatus})` : ""} — ${note}`);
}

function warn(name: string, endpoint: string, note: string) {
  results.push({ name, endpoint, status: "warn", note });
  console.warn(`  ⚠ ${name} — ${note}`);
}

async function checkHealth() {
  const endpoint = "/health";
  const url = `${BASE}${endpoint}`;
  console.log(`\n[health] GET ${url}`);
  try {
    const res = await fetch(url);
    const body = await readJsonObject(res);
    if (res.status === 200 && body.status === "ok") {
      pass("Health", endpoint, res.status, `executionModelVersion=${body.executionModelVersion}`);
    } else {
      fail("Health", endpoint, res.status, JSON.stringify(body));
    }
  } catch (e) {
    fail("Health", endpoint, undefined, String(e));
  }
}

async function checkCapabilities() {
  const endpoint = "/agent/x402/capabilities";
  const url = `${BASE}${endpoint}`;
  console.log(`\n[capabilities] GET ${url}`);
  try {
    const res = await fetch(url);
    const body = await readJsonObject(res);
    if (res.status === 200 && Array.isArray(body.resources)) {
      pass("Capabilities", endpoint, res.status, `${body.resources.length} resources, x402Version=${body.x402Version}`);
    } else {
      fail("Capabilities", endpoint, res.status, JSON.stringify(body).slice(0, 200));
    }
  } catch (e) {
    fail("Capabilities", endpoint, undefined, String(e));
  }
}

async function checkAgentCard() {
  const endpoint = "/.well-known/agent-card.json";
  const url = `${BASE}${endpoint}`;
  console.log(`\n[agent-card] GET ${url}`);
  try {
    const res = await fetch(url);
    const body = await readJsonObject(res);
    if (res.status === 200 && body.protocolVersion && body.skills?.length) {
      pass("Agent card", endpoint, res.status, `protocolVersion=${body.protocolVersion}, skills=${body.skills.length}`);
    } else {
      fail("Agent card", endpoint, res.status, JSON.stringify(body).slice(0, 200));
    }
  } catch (e) {
    fail("Agent card", endpoint, undefined, String(e));
  }
}

async function checkSkillFile() {
  const endpoint = "/SKILL.md";
  const url = `${BASE}${endpoint}`;
  console.log(`\n[skill-file] GET ${url}`);
  try {
    const res = await fetch(url);
    const text = await res.text().catch(() => "");
    if (res.status === 200 && text.includes("market-regime")) {
      pass("SKILL.md", endpoint, res.status, `${text.length} chars`);
    } else {
      fail("SKILL.md", endpoint, res.status, text.slice(0, 100));
    }
  } catch (e) {
    fail("SKILL.md", endpoint, undefined, String(e));
  }
}

async function checkA2A() {
  const endpoint = "/api/a2a";
  const url = `${BASE}${endpoint}`;
  console.log(`\n[a2a] POST ${url}`);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "message/send", params: {} }),
    });
    const body = await readJsonObject(res);
    if (res.status === 200 && body.jsonrpc === "2.0" && body.result) {
      const result = isRecord(body.result) ? body.result : {};
      pass("A2A JSON-RPC", endpoint, res.status, `role=${result.role}`);
    } else {
      fail("A2A JSON-RPC", endpoint, res.status, JSON.stringify(body).slice(0, 200));
    }
  } catch (e) {
    fail("A2A JSON-RPC", endpoint, undefined, String(e));
  }
}

async function check402(name: string, endpoint: string, body: unknown) {
  const url = `${BASE}${endpoint}`;
  console.log(`\n[${name}] POST ${url}`);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const paymentRequiredHeader = res.headers.get("PAYMENT-REQUIRED");
    const resBody = await readJsonObject(res);

    if (res.status !== 402) {
      fail(name, endpoint, res.status, `Expected 402, got ${res.status}: ${JSON.stringify(resBody).slice(0, 150)}`);
      return;
    }

    if (paymentRequiredHeader) {
      pass(name, endpoint, 402, `PAYMENT-REQUIRED header present (${paymentRequiredHeader.length} chars)`);
    } else {
      // Some versions embed in body — check for x402 field
      const x402Body = resBody as Record<string, unknown>;
      if (x402Body.x402 || x402Body.error === "x402_payment_required") {
        warn(name, endpoint, `402 returned but PAYMENT-REQUIRED header missing — payment info in body. x402Version=${(x402Body.x402 as Record<string,unknown>)?.transport}`);
      } else {
        fail(name, endpoint, 402, `402 returned but no PAYMENT-REQUIRED header and no x402 body field`);
      }
    }
  } catch (e) {
    fail(name, endpoint, undefined, String(e));
  }
}

async function main() {
  console.log(`\nSelun Smoke Test`);
  console.log(`Base: ${BASE}`);
  console.log(`Decision prefix: ${DECISION_PREFIX}`);
  console.log("=".repeat(60));

  // Free / discovery checks
  await checkHealth();
  await checkCapabilities();
  await checkAgentCard();
  await checkSkillFile();
  await checkA2A();

  // x402-gated endpoint checks (verify 402 challenge, no payment submitted)
  const decisionId = (suffix: string) => `${DECISION_PREFIX}-${suffix}`;
  const base402 = { riskTolerance: "Balanced", timeframe: "1-3_years", portfolioSegment: "Bluechips" };

  await check402("Market Regime", "/agent/x402/market-regime", { ...base402, decisionId: decisionId("market-regime") });
  await check402("Policy Envelope", "/agent/x402/policy-envelope", { ...base402, decisionId: decisionId("policy-envelope") });
  await check402("Asset Scorecard", "/agent/x402/asset-scorecard", { ...base402, decisionId: decisionId("asset-scorecard") });
  await check402("Allocate", "/agent/x402/allocate", { ...base402, decisionId: decisionId("allocate") });
  await check402("Rebalance", "/agent/x402/rebalance", { ...base402, decisionId: decisionId("rebalance"), holdings: [] });
  await check402("SCE Risk Evaluate", "/agent/x402/sce/risk-evaluate", { decisionId: decisionId("sce-risk-evaluate") });
  await check402("SCE Case Relevance", "/agent/x402/sce/case-relevance", { decisionId: decisionId("sce-case-relevance") });
  await check402("SCE Continuity Mode", "/agent/x402/sce/continuity-mode", { decisionId: decisionId("sce-continuity-mode") });

  // Summary
  console.log("\n" + "=".repeat(60));
  const passed = results.filter((r) => r.status === "pass").length;
  const warned = results.filter((r) => r.status === "warn").length;
  const failed = results.filter((r) => r.status === "fail").length;
  console.log(`Results: ${passed} passed, ${warned} warned, ${failed} failed / ${results.length} total`);

  if (failed > 0) {
    console.log("\nFailed:");
    results.filter((r) => r.status === "fail").forEach((r) => console.log(`  ✗ ${r.name}: ${r.note}`));
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
