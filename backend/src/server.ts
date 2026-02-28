import fs from "node:fs";
import { createHash } from "node:crypto";
import dotenv from "dotenv";
import express from "express";
import { createAgentRouter, getX402AllocateMetadataByJobId, getX402CapabilitiesData } from "./routes/agent.routes";
import { EXECUTION_MODEL_VERSION, getConfig } from "./config";
import { resolveBackendPath } from "./runtime-paths";
import { getExecutionStatus, getExecutionStatusByWallet } from "./services/phase1-execution.service";

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" ? (value as JsonRecord) : null;
}

function stableSerialize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => stableSerialize(entry));
  }

  if (!value || typeof value !== "object") {
    return value;
  }

  const record = value as JsonRecord;
  const normalized: JsonRecord = {};
  for (const key of Object.keys(record).sort()) {
    normalized[key] = stableSerialize(record[key]);
  }
  return normalized;
}

function hashDecisionPayload(value: unknown): string {
  const canonical = JSON.stringify(stableSerialize(value));
  return `sha256:${createHash("sha256").update(canonical).digest("hex")}`;
}

function buildAllocationMap(phase6Output: JsonRecord): Record<string, number> {
  const allocation = asRecord(phase6Output.allocation);
  const rows = allocation?.allocations;
  if (!Array.isArray(rows)) return {};

  const weights: Record<string, number> = {};
  for (const row of rows) {
    const rowRecord = asRecord(row);
    if (!rowRecord) continue;
    const symbol = typeof rowRecord.symbol === "string" ? rowRecord.symbol : null;
    const weight = typeof rowRecord.allocation_weight === "number" ? rowRecord.allocation_weight : null;
    if (!symbol || weight === null || !Number.isFinite(weight)) continue;
    weights[symbol] = weight;
  }

  return weights;
}

function buildAgentContract(payload: JsonRecord): JsonRecord | null {
  const jobId = typeof payload.jobId === "string" ? payload.jobId : null;
  const context = asRecord(payload.jobContext);
  if (!jobId || !context) return null;

  const phase1 = asRecord(context.phase1);
  const phase1Output = asRecord(phase1?.output);
  const phase1Market = asRecord(phase1Output?.market_condition);
  const phase2 = asRecord(context.phase2);
  const phase2Output = asRecord(phase2?.output);
  const phase2Inputs = asRecord(phase2Output?.inputs);
  const phase2UserProfile = asRecord(phase2Inputs?.user_profile);
  const phase6 = asRecord(context.phase6);
  const phase6Output = asRecord(phase6?.output);

  if (!phase6Output) return null;

  const x402Meta = getX402AllocateMetadataByJobId(jobId);
  const allocation = buildAllocationMap(phase6Output);
  const timestamp = typeof phase6Output.timestamp === "string" ? phase6Output.timestamp : null;
  const doctrineVersion = typeof phase6Output.doctrine_version === "string" ? phase6Output.doctrine_version : null;
  const executionModelVersion =
    typeof phase6Output.execution_model_version === "string"
      ? phase6Output.execution_model_version
      : EXECUTION_MODEL_VERSION;
  const confidenceScore =
    typeof phase1Market?.confidence === "number" && Number.isFinite(phase1Market.confidence)
      ? phase1Market.confidence
      : null;

  const aaaAllocate = asRecord(phase6?.aaaAllocate);
  const aaaResponse = asRecord(aaaAllocate?.response);
  const allocationResult = asRecord(aaaResponse?.allocation_result);
  const allocationMeta = asRecord(allocationResult?.meta);
  const allocatorVersion =
    (typeof allocationMeta?.allocator_version_effective === "string" && allocationMeta.allocator_version_effective) ||
    (typeof allocationMeta?.allocator === "string" && allocationMeta.allocator) ||
    null;

  const derivedInputs = {
    riskTolerance:
      x402Meta?.inputs.riskTolerance ??
      (typeof phase2UserProfile?.risk_tolerance === "string" ? phase2UserProfile.risk_tolerance : null),
    timeframe:
      x402Meta?.inputs.timeframe ??
      (typeof phase2UserProfile?.investment_timeframe === "string" ? phase2UserProfile.investment_timeframe : null),
    withReport: x402Meta?.inputs.withReport ?? null,
  };

  const paymentEcho = x402Meta?.payment
    ? {
      required: x402Meta.payment.required,
      chargedAmountUsdc: x402Meta.payment.chargedAmountUsdc,
      verified: x402Meta.payment.verified,
      fromAddress: x402Meta.payment.fromAddress,
      transactionHash: x402Meta.payment.transactionHash,
    }
    : null;

  const decisionId = x402Meta?.decisionId ?? null;
  const createdAt = typeof context.createdAt === "string" ? context.createdAt : null;
  const finishedAt = typeof phase6?.completedAt === "string" ? phase6.completedAt : null;
  const status = typeof payload.status === "string" ? payload.status : null;

  const decisionHash = hashDecisionPayload({
    decisionId,
    jobId,
    status,
    inputs: derivedInputs,
    allocation,
    allocatorVersion,
    executionModelVersion,
    doctrineVersion,
    timestamp,
    createdAt,
    finishedAt,
    paymentTransactionHash: paymentEcho?.transactionHash ?? null,
  });

  return {
    decisionId,
    jobId,
    status,
    allocation,
    decisionHash,
    allocatorVersion,
    executionModelVersion,
    doctrineVersion,
    timestamp,
    confidenceScore,
    createdAt,
    finishedAt,
    inputs: derivedInputs,
    payment: paymentEcho,
  };
}

function sanitizeExecutionStatusForAllocator<T>(payload: T): T {
  try {
    const cloned = JSON.parse(JSON.stringify(payload)) as JsonRecord;
    const jobContext = cloned.jobContext;
    if (!jobContext || typeof jobContext !== "object") {
      return cloned as T;
    }

    const context = jobContext as JsonRecord;

    const phase2 = context.phase2;
    const phase2Output =
      phase2 && typeof phase2 === "object" ? (phase2 as JsonRecord).output : undefined;
    const policyEnvelope =
      phase2Output && typeof phase2Output === "object"
        ? (phase2Output as JsonRecord).policy_envelope
        : undefined;
    if (policyEnvelope && typeof policyEnvelope === "object") {
      delete (policyEnvelope as JsonRecord).stablecoin_minimum;
      delete (policyEnvelope as JsonRecord).stablecoinMinimum;
    }

    const phase5 = context.phase5;
    const phase5Output =
      phase5 && typeof phase5 === "object" ? (phase5 as JsonRecord).output : undefined;
    const phase5Inputs =
      phase5Output && typeof phase5Output === "object" ? (phase5Output as JsonRecord).inputs : undefined;
    const portfolioConstraints =
      phase5Inputs && typeof phase5Inputs === "object"
        ? (phase5Inputs as JsonRecord).portfolio_constraints
        : undefined;
    if (portfolioConstraints && typeof portfolioConstraints === "object") {
      delete (portfolioConstraints as JsonRecord).stablecoin_minimum;
      delete (portfolioConstraints as JsonRecord).stablecoinMinimum;
    }

    return cloned as T;
  } catch {
    return payload;
  }
}

function resolveTrustProxySetting(value: string | undefined): boolean | number | string {
  const raw = value?.trim();
  if (!raw) return false;

  const normalized = raw.toLowerCase();
  if (normalized === "true") return true;
  if (normalized === "false") return false;

  const numericHops = Number.parseInt(raw, 10);
  if (Number.isFinite(numericHops) && numericHops >= 0) {
    return numericHops;
  }

  return raw;
}

const backendEnvPath = resolveBackendPath(".env");
const backendEnvLocalPath = resolveBackendPath(".env.local");

dotenv.config();

if (fs.existsSync(backendEnvPath)) {
  dotenv.config({ path: backendEnvPath, override: true });
}

if (fs.existsSync(backendEnvLocalPath)) {
  dotenv.config({ path: backendEnvLocalPath, override: true });
}

const app = express();
app.set("trust proxy", resolveTrustProxySetting(process.env.TRUST_PROXY));

app.use(express.json({ limit: "1mb" }));

function parseCsv(value: string | undefined): string[] {
  if (!value?.trim()) return [];
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function buildAbsoluteUrl(req: express.Request, routePath: string): string {
  const forwardedProto = req.header("x-forwarded-proto")?.split(",")[0]?.trim();
  const forwardedHost = req.header("x-forwarded-host")?.split(",")[0]?.trim();
  const proto = forwardedProto || req.protocol || "https";
  const host = forwardedHost || req.get("host");
  if (!host) return routePath;
  return `${proto}://${host}${routePath}`;
}

async function buildWellKnownX402Document(req: express.Request) {
  const capabilities = await getX402CapabilitiesData();
  const resources = capabilities.resources.map((resource) => buildAbsoluteUrl(req, resource.endpoint));
  const ownershipProofs = parseCsv(process.env.X402_DISCOVERY_OWNERSHIP_PROOFS);
  const instructions = process.env.X402_DISCOVERY_INSTRUCTIONS?.trim();
  return {
    version: 1,
    resources,
    ...(ownershipProofs.length > 0 ? { ownershipProofs } : {}),
    ...(instructions ? { instructions } : {}),
  };
}

app.get("/health", (_req, res) => {
  res.status(200).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    status: "ok",
  });
});

app.get("/.well-known/x402", async (req, res) => {
  try {
    const document = await buildWellKnownX402Document(req);
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    return res.status(200).json(document);
  } catch (error) {
    return res.status(500).json({
      success: false,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      error: error instanceof Error ? error.message : "Failed to build x402 discovery document.",
    });
  }
});

app.get("/.well-known/x402.json", async (req, res) => {
  try {
    const document = await buildWellKnownX402Document(req);
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    return res.status(200).json(document);
  } catch (error) {
    return res.status(500).json({
      success: false,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      error: error instanceof Error ? error.message : "Failed to build x402 discovery document.",
    });
  }
});

app.use("/agent", createAgentRouter());

app.get("/execution-status/wallet/:walletAddress", (req, res) => {
  const walletAddress = req.params.walletAddress?.trim();
  if (!walletAddress) {
    return res.status(400).json({
      success: false,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      error: "walletAddress is required.",
    });
  }

  const status = getExecutionStatusByWallet(walletAddress);
  const sanitizedStatus = sanitizeExecutionStatusForAllocator(status);
  const agentContract = buildAgentContract(sanitizedStatus as JsonRecord);
  return res.status(status.found ? 200 : 404).json({
    success: status.found,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    ...sanitizedStatus,
    ...(agentContract ? { agentContract } : {}),
  });
});

app.get("/execution-status/:jobId", (req, res) => {
  const jobId = req.params.jobId?.trim();
  if (!jobId) {
    return res.status(400).json({
      success: false,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      error: "jobId is required.",
    });
  }

  const status = getExecutionStatus(jobId);
  const sanitizedStatus = sanitizeExecutionStatusForAllocator(status);
  const agentContract = buildAgentContract(sanitizedStatus as JsonRecord);
  return res.status(status.found ? 200 : 404).json({
    success: status.found,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    ...sanitizedStatus,
    ...(agentContract ? { agentContract } : {}),
  });
});

const port = Number.parseInt(process.env.PORT ?? "8787", 10);

try {
  getConfig();
} catch (error) {
  console.error(error instanceof Error ? error.message : "Invalid configuration.");
  process.exit(1);
}

app.listen(port, () => {
  console.log(`Selun Express backend listening on port ${port}`);
});
