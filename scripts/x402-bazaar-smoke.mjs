import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { decodePaymentResponseHeader, wrapFetchWithPaymentFromConfig } from "@x402/fetch";
import { decodePaymentRequiredHeader } from "@x402/core/http";
import { ExactEvmScheme } from "@x402/evm";
import { privateKeyToAccount } from "viem/accounts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");

loadEnvFiles([
  path.join(projectRoot, ".env"),
  path.join(projectRoot, ".env.local"),
  path.join(projectRoot, "backend", ".env"),
  path.join(projectRoot, "backend", ".env.local"),
]);

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  printHelp();
  process.exit(0);
}

const privateKey = readRequiredEnv(["SELUN_X402_SMOKE_PRIVATE_KEY", "EVM_PRIVATE_KEY"]);
const account = privateKeyToAccount(privateKey);
const withReport = readBooleanEnv("SELUN_X402_SMOKE_WITH_REPORT", false);
const targetEndpoint = resolveTargetEndpoint(withReport);
const targetUrl = resolveTargetUrl(targetEndpoint);
const targetPathname = new URL(targetUrl).pathname;
const shouldPoll = readBooleanEnv("SELUN_X402_SMOKE_POLL", true);
const pollIntervalMs = readPositiveIntEnv("SELUN_X402_SMOKE_POLL_INTERVAL_MS", 5000);
const pollTimeoutMs = readPositiveIntEnv("SELUN_X402_SMOKE_POLL_TIMEOUT_MS", 10 * 60 * 1000);
const decisionId = process.env.SELUN_X402_SMOKE_DECISION_ID?.trim() || `bazaar-smoke-${Date.now()}`;
const requestBody = buildRequestBody({
  decisionId,
  targetPathname,
  withReport,
});

const fetchWithPayment = wrapFetchWithPaymentFromConfig(fetch, {
  schemes: [
    {
      network: process.env.SELUN_X402_SMOKE_NETWORK?.trim() || "eip155:*",
      client: new ExactEvmScheme(account),
    },
  ],
});

await main().catch((error) => {
  console.error(`[smoke] failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});

async function main() {
  console.log(`[smoke] buyer wallet: ${account.address}`);
  console.log(`[smoke] target: ${targetUrl}`);
  console.log(`[smoke] decisionId: ${requestBody.decisionId}`);

  const probeResponse = await fetch(targetUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(requestBody),
  });

  const probeBody = await readJsonBody(probeResponse);
  const paymentRequiredHeader = probeResponse.headers.get("payment-required");

  if (probeResponse.status !== 402 || !paymentRequiredHeader) {
    throw new Error(
      `Expected a 402 probe with PAYMENT-REQUIRED, got ${probeResponse.status}. Body: ${stringify(probeBody)}`,
    );
  }

  const selectedOptionId = probeBody?.x402?.selectedOptionId ?? "unknown";
  const selectedAmountUsdc = probeBody?.x402?.amountUsdc ?? "unknown";
  console.log(`[smoke] probe ok: 402 PAYMENT-REQUIRED (${selectedOptionId}, ${selectedAmountUsdc} USDC)`);

  const paidResponse = await fetchWithPayment(targetUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(requestBody),
  });

  const paidBody = await readJsonBody(paidResponse);
  if (paidResponse.status !== 202 && paidResponse.status !== 200) {
    const failureDetails = decodePaymentRequiredFailure(paidResponse);
    const paymentError = failureDetails?.error ?? paidBody?.error ?? paidBody?.message ?? "unknown payment error";
    throw new Error(`Paid request failed with HTTP ${paidResponse.status}: ${paymentError}`);
  }

  const paymentResponseHeader = paidResponse.headers.get("payment-response");
  const decodedPaymentResponse = paymentResponseHeader
    ? decodePaymentResponseHeader(paymentResponseHeader)
    : paidBody?.data?.payment?.transactionHash
      ? {
        network: paidBody.data.payment.network ?? "unknown",
        transaction: paidBody.data.payment.transactionHash,
      }
      : null;
  if (!decodedPaymentResponse) {
    throw new Error("Paid response did not include a PAYMENT-RESPONSE header or payment receipt in the response body.");
  }
  const statusPath = paidBody?.data?.statusPath;

  console.log(`[smoke] payment accepted: HTTP ${paidResponse.status}`);
  console.log(`[smoke] settlement: ${decodedPaymentResponse.network} ${decodedPaymentResponse.transaction}`);
  if (statusPath) {
    console.log(`[smoke] statusPath: ${statusPath}`);
  }

  if (!shouldPoll || !statusPath) {
    if (paidResponse.status === 200 && paidBody?.data?.result) {
      console.log(`[smoke] result: ${stringify(paidBody.data.result)}`);
    }
    return;
  }

  const statusUrl = new URL(statusPath, targetUrl).toString();
  console.log(`[smoke] polling: ${statusUrl}`);

  const finalStatus = await pollExecutionStatus(statusUrl, pollIntervalMs, pollTimeoutMs);
  const phase6Status = finalStatus?.jobContext?.phase6?.status ?? "unknown";
  console.log(`[smoke] final status: ${finalStatus?.status ?? "unknown"} (phase6=${phase6Status})`);

  if (phase6Status !== "complete") {
    throw new Error(`Execution did not complete successfully. Body: ${stringify(finalStatus)}`);
  }

  if (finalStatus?.agentContract?.decisionHash) {
    console.log(`[smoke] decisionHash: ${finalStatus.agentContract.decisionHash}`);
  }
}

async function pollExecutionStatus(statusUrl, intervalMs, timeoutMs) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const response = await fetch(statusUrl, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });
    const body = await readJsonBody(response);
    if (!response.ok) {
      throw new Error(`Execution status request failed with ${response.status}. Body: ${stringify(body)}`);
    }

    const rootStatus = typeof body?.status === "string" ? body.status : "unknown";
    const phase6Status = body?.jobContext?.phase6?.status;
    console.log(`[smoke] status=${rootStatus} phase6=${phase6Status ?? "unknown"}`);

    if (phase6Status === "complete") {
      return body;
    }
    if (phase6Status === "failed" || rootStatus === "failed") {
      return body;
    }

    await sleep(intervalMs);
  }

  throw new Error(`Timed out waiting for execution completion after ${timeoutMs}ms.`);
}

function loadEnvFiles(paths) {
  for (const envPath of paths) {
    if (fs.existsSync(envPath)) {
      dotenv.config({ path: envPath, quiet: true });
    }
  }
}

function resolveTargetEndpoint(withReport) {
  const explicit = process.env.SELUN_X402_SMOKE_ENDPOINT?.trim().toLowerCase();
  if (explicit) {
    const normalized = explicit
      .replace(/^\/+/, "")
      .replace(/^agent\/x402\//, "")
      .replace(/^x402\//, "");
    if (normalized) return normalized;
  }
  return withReport ? "allocate-with-report" : "allocate";
}

function resolveTargetUrl(targetEndpoint) {
  const direct = process.env.SELUN_X402_SMOKE_URL?.trim();
  if (direct) return direct;

  const backendBaseUrl = process.env.SELUN_BACKEND_URL?.trim();
  if (!backendBaseUrl) {
    throw new Error(
      "Missing SELUN_X402_SMOKE_URL or SELUN_BACKEND_URL. Set one of them before running the smoke script.",
    );
  }

  const routePath = `/agent/x402/${targetEndpoint}`;
  return new URL(routePath, ensureTrailingSlash(backendBaseUrl)).toString();
}

function buildRequestBody({ decisionId, targetPathname, withReport }) {
  const riskTolerance = process.env.SELUN_X402_SMOKE_RISK_TOLERANCE?.trim() || "Balanced";
  const timeframe = process.env.SELUN_X402_SMOKE_TIMEFRAME?.trim() || "1-3_years";
  const base = {
    decisionId,
    riskTolerance,
    timeframe,
    ...(process.env.SELUN_X402_SMOKE_RESULT_EMAIL?.trim()
      ? { resultEmail: process.env.SELUN_X402_SMOKE_RESULT_EMAIL.trim() }
      : {}),
    ...(process.env.SELUN_X402_SMOKE_PROMO_CODE?.trim()
      ? { promoCode: process.env.SELUN_X402_SMOKE_PROMO_CODE.trim() }
      : {}),
  };

  if (targetPathname.endsWith("/allocate-with-report")) {
    return { ...base, withReport: true };
  }

  if (targetPathname.endsWith("/allocate")) {
    return { ...base, withReport };
  }

  if (targetPathname.endsWith("/rebalance")) {
    return {
      ...base,
      holdings: parseJsonEnv("SELUN_X402_SMOKE_HOLDINGS_JSON", defaultHoldings()),
    };
  }

  if (targetPathname.endsWith("/asset-scorecard")) {
    return {
      ...base,
      assets: parseJsonEnv("SELUN_X402_SMOKE_ASSETS_JSON", ["BTC", "ETH", "SOL"]),
    };
  }

  if (targetPathname.endsWith("/sce/continuity-mode")) {
    return {
      decisionId,
      scope: process.env.SELUN_X402_SMOKE_SCE_SCOPE?.trim() || "global",
      ...(process.env.SELUN_X402_SMOKE_SCE_CHAIN_ID ? { chain_id: Number(process.env.SELUN_X402_SMOKE_SCE_CHAIN_ID) } : {}),
      ...(process.env.SELUN_X402_SMOKE_SCE_THREAT_FAMILY?.trim() ? { threat_family: process.env.SELUN_X402_SMOKE_SCE_THREAT_FAMILY.trim() } : {}),
      ...(process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION?.trim() ? { requested_action: process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION.trim() } : {}),
    };
  }

  if (targetPathname.endsWith("/sce/case-relevance")) {
    return {
      decisionId,
      protocol_name: process.env.SELUN_X402_SMOKE_SCE_PROTOCOL_NAME?.trim() || "Uniswap",
      chain_id: process.env.SELUN_X402_SMOKE_SCE_CHAIN_ID ? Number(process.env.SELUN_X402_SMOKE_SCE_CHAIN_ID) : 8453,
      threat_families: parseJsonEnv("SELUN_X402_SMOKE_SCE_THREAT_FAMILIES_JSON", ["DeFi Protocol Incident", "Admin Key / Access Control"]),
      ...(process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION?.trim() ? { requested_action: process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION.trim() } : { requested_action: "swap" }),
    };
  }

  if (targetPathname.endsWith("/sce/risk-evaluate")) {
    return {
      decisionId,
      protocol_name: process.env.SELUN_X402_SMOKE_SCE_PROTOCOL_NAME?.trim() || "Aave",
      chain_id: process.env.SELUN_X402_SMOKE_SCE_CHAIN_ID ? Number(process.env.SELUN_X402_SMOKE_SCE_CHAIN_ID) : 8453,
      ...(process.env.SELUN_X402_SMOKE_SCE_ADDRESSES_JSON ? { addresses: parseJsonEnv("SELUN_X402_SMOKE_SCE_ADDRESSES_JSON", []) } : {}),
      ...(process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION?.trim() ? { requested_action: process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION.trim() } : { requested_action: "deposit" }),
    };
  }

  return base;
}

function parseJsonEnv(name, fallback) {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  try {
    return JSON.parse(raw);
  } catch (error) {
    throw new Error(`${name} must be valid JSON. ${error instanceof Error ? error.message : "Parse failed."}`);
  }
}

function defaultHoldings() {
  return [
    { asset: "BTC", usdValue: 4300 },
    { asset: "ETH", usdValue: 3700 },
    { asset: "USDC", usdValue: 2000 },
  ];
}

function ensureTrailingSlash(value) {
  return value.endsWith("/") ? value : `${value}/`;
}

function readRequiredEnv(names) {
  for (const name of names) {
    const value = process.env[name]?.trim();
    if (value) {
      return value;
    }
  }
  throw new Error(`Missing required environment variable. Set one of: ${names.join(", ")}`);
}

function readBooleanEnv(name, fallback) {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function readPositiveIntEnv(name, fallback) {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

async function readJsonBody(response) {
  const text = await response.text();
  if (!text.trim()) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

function stringify(value) {
  return JSON.stringify(value, null, 2);
}

function decodePaymentRequiredFailure(response) {
  const encoded = response.headers.get("payment-required");
  if (!encoded) return null;
  try {
    return decodePaymentRequiredHeader(encoded);
  } catch {
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function printHelp() {
  console.log(`Usage: node scripts/x402-bazaar-smoke.mjs

Required env:
  SELUN_X402_SMOKE_URL or SELUN_BACKEND_URL
  SELUN_X402_SMOKE_PRIVATE_KEY or EVM_PRIVATE_KEY

Optional env (portfolio endpoints):
  SELUN_X402_SMOKE_ENDPOINT (allocate | allocate-with-report | market-regime | policy-envelope | asset-scorecard | rebalance | sce/continuity-mode | sce/case-relevance | sce/risk-evaluate)
  SELUN_X402_SMOKE_DECISION_ID
  SELUN_X402_SMOKE_RISK_TOLERANCE (default: Balanced)
  SELUN_X402_SMOKE_TIMEFRAME (default: 1-3_years)
  SELUN_X402_SMOKE_WITH_REPORT (default: false)
  SELUN_X402_SMOKE_ASSETS_JSON (JSON array for /asset-scorecard)
  SELUN_X402_SMOKE_HOLDINGS_JSON (JSON array for /rebalance)
  SELUN_X402_SMOKE_RESULT_EMAIL
  SELUN_X402_SMOKE_PROMO_CODE
  SELUN_X402_SMOKE_NETWORK (default: eip155:*)
  SELUN_X402_SMOKE_POLL (default: true)
  SELUN_X402_SMOKE_POLL_INTERVAL_MS (default: 5000)
  SELUN_X402_SMOKE_POLL_TIMEOUT_MS (default: 600000)

Optional env (SCE endpoints):
  SELUN_X402_SMOKE_SCE_SCOPE          scope for continuity-mode (default: global)
  SELUN_X402_SMOKE_SCE_CHAIN_ID       chain id (default: 8453 for case-relevance/risk-evaluate)
  SELUN_X402_SMOKE_SCE_THREAT_FAMILY  single threat_family for continuity-mode
  SELUN_X402_SMOKE_SCE_PROTOCOL_NAME  protocol name (default: Uniswap/case-relevance, Aave/risk-evaluate)
  SELUN_X402_SMOKE_SCE_THREAT_FAMILIES_JSON  JSON array for case-relevance (default: ["DeFi Protocol Incident","Admin Key / Access Control"])
  SELUN_X402_SMOKE_SCE_ADDRESSES_JSON JSON array of addresses for risk-evaluate
  SELUN_X402_SMOKE_SCE_REQUESTED_ACTION  action string (default: swap/deposit)

Examples:
  node scripts/x402-bazaar-smoke.mjs
  $env:SELUN_X402_SMOKE_URL="https://selun.sagitta.systems/agent/x402/rebalance"
  $env:SELUN_X402_SMOKE_HOLDINGS_JSON='[{"asset":"BTC","usdValue":4300},{"asset":"ETH","usdValue":3700},{"asset":"USDC","usdValue":2000}]'
  node scripts/x402-bazaar-smoke.mjs

  $env:SELUN_BACKEND_URL="https://selun.sagitta.systems"
  npm run x402:smoke:sce:continuity-mode

  $env:SELUN_X402_SMOKE_SCE_PROTOCOL_NAME="Uniswap"
  npm run x402:smoke:sce:case-relevance

  $env:SELUN_X402_SMOKE_SCE_PROTOCOL_NAME="Aave"
  npm run x402:smoke:sce:risk-evaluate
`);
}
