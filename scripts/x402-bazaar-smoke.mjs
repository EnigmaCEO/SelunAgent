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
const allocateUrl = resolveAllocateUrl(withReport);
const shouldPoll = readBooleanEnv("SELUN_X402_SMOKE_POLL", true);
const pollIntervalMs = readPositiveIntEnv("SELUN_X402_SMOKE_POLL_INTERVAL_MS", 5000);
const pollTimeoutMs = readPositiveIntEnv("SELUN_X402_SMOKE_POLL_TIMEOUT_MS", 10 * 60 * 1000);
const decisionId = process.env.SELUN_X402_SMOKE_DECISION_ID?.trim() || `bazaar-smoke-${Date.now()}`;
const requestBody = {
  decisionId,
  riskTolerance: process.env.SELUN_X402_SMOKE_RISK_TOLERANCE?.trim() || "Balanced",
  timeframe: process.env.SELUN_X402_SMOKE_TIMEFRAME?.trim() || "1-3_years",
  withReport,
  ...(process.env.SELUN_X402_SMOKE_RESULT_EMAIL?.trim()
    ? { resultEmail: process.env.SELUN_X402_SMOKE_RESULT_EMAIL.trim() }
    : {}),
  ...(process.env.SELUN_X402_SMOKE_PROMO_CODE?.trim()
    ? { promoCode: process.env.SELUN_X402_SMOKE_PROMO_CODE.trim() }
    : {}),
};

const fetchWithPayment = wrapFetchWithPaymentFromConfig(fetch, {
  schemes: [
    {
      network: process.env.SELUN_X402_SMOKE_NETWORK?.trim() || "eip155:*",
      client: new ExactEvmScheme(account),
    },
  ],
});

await main();

async function main() {
  console.log(`[smoke] buyer wallet: ${account.address}`);
  console.log(`[smoke] target: ${allocateUrl}`);
  console.log(`[smoke] decisionId: ${requestBody.decisionId}`);

  const probeResponse = await fetch(allocateUrl, {
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

  const paidResponse = await fetchWithPayment(allocateUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(requestBody),
  });

  const paidBody = await readJsonBody(paidResponse);
  if (paidResponse.status !== 202 && paidResponse.status !== 200) {
    const failureDetails = decodePaymentRequiredFailure(paidResponse);
    const reasonSuffix = failureDetails ? ` x402 error: ${failureDetails.error}.` : "";
    throw new Error(`Paid request failed with ${paidResponse.status}.${reasonSuffix} Body: ${stringify(paidBody)}`);
  }

  const paymentResponseHeader = paidResponse.headers.get("payment-response");
  if (!paymentResponseHeader) {
    throw new Error("Paid response did not include PAYMENT-RESPONSE.");
  }

  const decodedPaymentResponse = decodePaymentResponseHeader(paymentResponseHeader);
  const statusPath = paidBody?.data?.statusPath;

  console.log(`[smoke] payment accepted: HTTP ${paidResponse.status}`);
  console.log(`[smoke] settlement: ${decodedPaymentResponse.network} ${decodedPaymentResponse.transaction}`);
  if (statusPath) {
    console.log(`[smoke] statusPath: ${statusPath}`);
  }

  if (!shouldPoll || !statusPath) {
    return;
  }

  const statusUrl = new URL(statusPath, allocateUrl).toString();
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

function resolveAllocateUrl(withReport) {
  const direct = process.env.SELUN_X402_SMOKE_URL?.trim();
  if (direct) return direct;

  const backendBaseUrl = process.env.SELUN_BACKEND_URL?.trim();
  if (!backendBaseUrl) {
    throw new Error(
      "Missing SELUN_X402_SMOKE_URL or SELUN_BACKEND_URL. Set one of them before running the smoke script.",
    );
  }

  const routePath = withReport ? "/agent/x402/allocate-with-report" : "/agent/x402/allocate";
  return new URL(routePath, ensureTrailingSlash(backendBaseUrl)).toString();
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

Optional env:
  SELUN_X402_SMOKE_DECISION_ID
  SELUN_X402_SMOKE_RISK_TOLERANCE (default: Balanced)
  SELUN_X402_SMOKE_TIMEFRAME (default: 1-3_years)
  SELUN_X402_SMOKE_WITH_REPORT (default: false)
  SELUN_X402_SMOKE_RESULT_EMAIL
  SELUN_X402_SMOKE_PROMO_CODE
  SELUN_X402_SMOKE_NETWORK (default: eip155:*)
  SELUN_X402_SMOKE_POLL (default: true)
  SELUN_X402_SMOKE_POLL_INTERVAL_MS (default: 5000)
  SELUN_X402_SMOKE_POLL_TIMEOUT_MS (default: 600000)
`);
}
