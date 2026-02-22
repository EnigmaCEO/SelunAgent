import fs from "node:fs";
import dotenv from "dotenv";
import express from "express";
import { createAgentRouter } from "./routes/agent.routes";
import { EXECUTION_MODEL_VERSION, getConfig } from "./config";
import { resolveBackendPath } from "./runtime-paths";
import { getExecutionStatus, getExecutionStatusByWallet } from "./services/phase1-execution.service";

type JsonRecord = Record<string, unknown>;

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

app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req, res) => {
  res.status(200).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    status: "ok",
  });
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
  return res.status(status.found ? 200 : 404).json({
    success: status.found,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    ...sanitizedStatus,
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
  return res.status(status.found ? 200 : 404).json({
    success: status.found,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    ...sanitizedStatus,
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
