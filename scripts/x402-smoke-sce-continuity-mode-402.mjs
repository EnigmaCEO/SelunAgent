import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

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

await main().catch((error) => {
  console.error(`[402-smoke] failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});

async function main() {
  const targetUrl = resolveTargetUrl();
  const requestBody = buildRequestBody();

  const response = await fetch(targetUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(requestBody),
  });

  const responseText = await response.text();
  const responseBody = parseResponseBody(responseText);
  const paymentRequiredHeader = response.headers.get("payment-required");

  if (response.status !== 402) {
    throw new Error(`Expected HTTP 402, received ${response.status}. Body: ${stringify(responseBody)}`);
  }
  if (!paymentRequiredHeader) {
    throw new Error("HTTP 402 response did not include a PAYMENT-REQUIRED header.");
  }

  console.log(stringify({
    status: response.status,
    statusText: response.statusText,
    error: responseBody?.error ?? "x402_payment_required",
    message: responseBody?.message ?? "Payment required",
    x402: {
      endpoint: responseBody?.x402?.endpoint ?? new URL(targetUrl).pathname,
      productId: responseBody?.x402?.productId ?? "sce_continuity_mode",
      amountUsdc: responseBody?.x402?.amountUsdc,
      paymentRequirementsV2: responseBody?.x402?.paymentRequirementsV2,
      decisionId: responseBody?.x402?.decisionId ?? requestBody.decisionId,
      quoteIssuedAt: responseBody?.x402?.quoteIssuedAt,
      quoteExpiresAt: responseBody?.x402?.quoteExpiresAt,
    },
  }));
}

function resolveTargetUrl() {
  const backendBaseUrl = process.env.SELUN_BACKEND_URL?.trim();
  if (!backendBaseUrl) {
    throw new Error("Set SELUN_BACKEND_URL before running this smoke.");
  }

  return new URL("/agent/x402/sce/continuity-mode", ensureTrailingSlash(backendBaseUrl)).toString();
}

function buildRequestBody() {
  return {
    decisionId: process.env.SELUN_X402_SMOKE_DECISION_ID?.trim() || `continuity-mode-402-smoke-${Date.now()}`,
    scope: process.env.SELUN_X402_SMOKE_SCE_SCOPE?.trim() || "global",
    ...(process.env.SELUN_X402_SMOKE_SCE_CHAIN_ID
      ? { chain_id: readNumberEnv("SELUN_X402_SMOKE_SCE_CHAIN_ID") }
      : {}),
    ...(process.env.SELUN_X402_SMOKE_SCE_THREAT_FAMILY?.trim()
      ? { threat_family: process.env.SELUN_X402_SMOKE_SCE_THREAT_FAMILY.trim() }
      : {}),
    ...(process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION?.trim()
      ? { requested_action: process.env.SELUN_X402_SMOKE_SCE_REQUESTED_ACTION.trim() }
      : {}),
  };
}

function readNumberEnv(name) {
  const value = Number(process.env[name]);
  if (!Number.isFinite(value)) {
    throw new Error(`${name} must be a valid number.`);
  }
  return value;
}

function parseResponseBody(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function stringify(value) {
  return JSON.stringify(value, (_key, item) => (typeof item === "bigint" ? item.toString() : item), 2);
}

function ensureTrailingSlash(value) {
  return value.endsWith("/") ? value : `${value}/`;
}

function loadEnvFiles(paths) {
  for (const envPath of paths) {
    if (fs.existsSync(envPath)) {
      dotenv.config({ path: envPath, quiet: true });
    }
  }
}

function printHelp() {
  console.log(`Usage: npm run x402:smoke:sce:continuity-mode:402

Sends an unpaid request to /agent/x402/sce/continuity-mode and prints the
compact HTTP 402 error and payment requirements. It omits headers, Bazaar
metadata, and server logs. No wallet or private key is used.

Required env:
  SELUN_BACKEND_URL

Optional env:
  SELUN_X402_SMOKE_DECISION_ID
  SELUN_X402_SMOKE_SCE_SCOPE
  SELUN_X402_SMOKE_SCE_CHAIN_ID
  SELUN_X402_SMOKE_SCE_THREAT_FAMILY
  SELUN_X402_SMOKE_SCE_REQUESTED_ACTION

Example:
  $env:SELUN_BACKEND_URL="https://selun.sagitta.systems"
  npm run x402:smoke:sce:continuity-mode:402
`);
}
