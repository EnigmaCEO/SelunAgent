import { Router, type Request, type Response } from "express";
import { createHash } from "node:crypto";
import { declareDiscoveryExtension } from "@x402/extensions/bazaar";
import { isAddress } from "viem";
import { EXECUTION_MODEL_VERSION, getConfig } from "../config";
import { getExecutionLogs } from "../logging/execution-logs";
import { getX402StateStore } from "../services/x402-state.service";
import { isExpiredIsoTimestamp, normalizeOptionalBoolean } from "../services/x402-utils";
import { isValidEmail, sendAdminUsageEmail, sendUserReportEmail, sendUserSummaryEmail } from "../services/email.service";
import {
  authorizeWizardPayment,
  getAgentAddress,
  getUSDCBalanceForAddress,
  getWizardPricing,
  initializeAgent,
  quoteWizardPayment,
  storeDecisionHashOnChain,
  verifyIncomingPayment,
} from "../services/selun-agent.service";
import {
  CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
  EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
  EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
  REVIEW_MARKET_CONDITIONS_PHASE,
  SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
  getExecutionStatus,
  runPhase1,
  runPhase3,
  runPhase4,
  runPhase5,
  runPhase6,
} from "../services/phase1-execution.service";
import type {
  AllocateInputShape,
  AllocateRiskTolerance,
  AllocateTimeframe,
  X402AllocateRecord,
} from "../services/x402-state.types";

const router = Router();

type AllocateProofSource = "headers" | "body_payment" | "body_legacy" | "none";
type AllocatePaymentProof = {
  fromAddress?: string;
  transactionHash?: string;
  source: AllocateProofSource;
  deprecatedBodyProof: boolean;
};
type X402AllocateAccept = {
  optionId: "allocation_only" | "allocation_with_report";
  scheme: "exact";
  amountUsdc: string;
  price: string;
  withReport: boolean;
  network: string;
  caip2Network: string;
  usdcContractAddress: string;
  payToAddress: string;
  requiredHeaders: readonly ["x402-from-address", "x402-transaction-hash"];
};
type X402PaymentRequirementV2 = {
  scheme: "exact";
  network: string;
  amount: string;
  asset: string;
  payTo: string;
  maxTimeoutSeconds: number;
  extra: {
    optionId: "allocation_only" | "allocation_with_report";
    withReport: boolean;
    requiredHeaders: readonly ["x402-from-address", "x402-transaction-hash"];
  };
};

type AllocationEmailRow = {
  asset: string;
  name: string;
  category: string;
  riskClass: string;
  allocationPct: number;
};

const ALLOCATE_PHASE_POLL_INTERVAL_MS = 2_000;
const ALLOCATE_PHASE_TIMEOUT_MS = 20 * 60 * 1_000;
const runningAllocateOrchestration = new Set<string>();
const x402IpBurstState = new Map<string, number[]>();

function nowIso() {
  return new Date().toISOString();
}

function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function getX402QuoteTtlMs() {
  return readPositiveIntEnv("X402_QUOTE_TTL_MS", 10 * 60 * 1_000);
}

function getX402IpBurstWindowMs() {
  return readPositiveIntEnv("X402_IP_BURST_WINDOW_MS", 60_000);
}

function getX402IpBurstLimit() {
  return readPositiveIntEnv("X402_IP_BURST_LIMIT", 60);
}

function getX402FromAddressDailyCap() {
  return readPositiveIntEnv("X402_FROM_ADDRESS_DAILY_CAP", 20);
}

function getX402GlobalConcurrencyCap() {
  return readPositiveIntEnv("X402_GLOBAL_CONCURRENCY_CAP", 8);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toText(value: unknown, fallback = ""): string {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : fallback;
  }
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  return fallback;
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function toTitleCase(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
    .join(" ");
}

function mapStrategyLabel(value: unknown): string {
  const normalized = toText(value, "").toLowerCase();
  if (normalized === "capital_preservation") return "Capital Preservation";
  if (normalized === "balanced_defensive") return "Balanced Defensive";
  if (normalized === "balanced_growth") return "Balanced Growth";
  if (normalized === "offensive_growth") return "Growth-Focused";
  return toTitleCase(toText(value, "Unspecified"));
}

function mapRiskClassLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (!normalized || normalized === "unknown") return "Unspecified";
  if (normalized === "large_cap_crypto") return "Large Cap Crypto";
  if (normalized === "stablecoin") return "Stablecoin";
  return toTitleCase(normalized);
}

function collectAllocationRows(value: unknown): AllocationEmailRow[] {
  if (!Array.isArray(value)) return [];
  const rows: AllocationEmailRow[] = [];

  for (const row of value) {
    if (!isRecord(row)) continue;
    const asset = toText(row.asset, "");
    const name = toText(row.name, asset);
    const category = toText(row.category, "Unknown");
    const riskClass = toText(row.riskClass, "Unknown");
    const allocationPct = toFiniteNumber(row.allocationPct);
    if (!asset || allocationPct === null) continue;

    rows.push({
      asset,
      name,
      category,
      riskClass: mapRiskClassLabel(riskClass),
      allocationPct: Math.max(0, Math.min(100, allocationPct)),
    });
  }

  return rows.sort((left, right) => right.allocationPct - left.allocationPct || left.asset.localeCompare(right.asset));
}

function normalizeRiskTolerance(value: unknown): AllocateRiskTolerance | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  if (normalized === "conservative") return "Conservative";
  if (normalized === "balanced") return "Balanced";
  if (normalized === "growth") return "Growth";
  if (normalized === "aggressive") return "Aggressive";
  return null;
}

function normalizeTimeframe(value: unknown): AllocateTimeframe | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  if (normalized === "<1_year" || normalized === "< 1 year" || normalized === "lt_1_year") return "<1_year";
  if (normalized === "1-3_years" || normalized === "1-3 years" || normalized === "1_3_years") return "1-3_years";
  if (normalized === "3+_years" || normalized === "3+ years" || normalized === "gt_3_years") return "3+_years";
  return null;
}

function deriveRiskMode(riskTolerance: AllocateRiskTolerance): "conservative" | "balanced" | "growth" | "aggressive" {
  if (riskTolerance === "Conservative") return "conservative";
  if (riskTolerance === "Growth") return "growth";
  if (riskTolerance === "Aggressive") return "aggressive";
  return "balanced";
}

function formatUsdcAmount(value: number): string {
  const rounded = Number(value.toFixed(6));
  return rounded.toString();
}

function getAllocateChargeAmountUsdc(withReport: boolean): string {
  const pricing = getWizardPricing();
  const total = pricing.structuredAllocationPriceUsdc + (withReport ? pricing.certifiedDecisionRecordFeeUsdc : 0);
  return formatUsdcAmount(total);
}

function normalizeDecisionId(value: string | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed) return null;
  if (trimmed.length < 6 || trimmed.length > 128) return null;
  if (!/^[A-Za-z0-9:_\-.]+$/.test(trimmed)) return null;
  return trimmed;
}

function resolveDecisionId(req: Request): { decisionId?: string; error?: string } {
  const bodyDecisionId = normalizeDecisionId(typeof req.body?.decisionId === "string" ? req.body.decisionId : undefined);
  const headerDecisionId = normalizeDecisionId(req.header("idempotency-key") ?? undefined);

  if (bodyDecisionId && headerDecisionId && bodyDecisionId !== headerDecisionId) {
    return {
      error: "decisionId and Idempotency-Key header must match when both are provided.",
    };
  }

  const resolved = bodyDecisionId ?? headerDecisionId;
  if (!resolved) {
    return {
      error: "decisionId is required for agent calls (body decisionId or Idempotency-Key header).",
    };
  }

  return { decisionId: resolved };
}

function buildAllocateJobId(decisionId: string): string {
  return `selun-allocate-${decisionId}-${Date.now()}`.replace(/[^a-zA-Z0-9-_]/g, "-");
}

function computeAllocateInputFingerprint(input: AllocateInputShape): string {
  return createHash("sha256").update(JSON.stringify(input)).digest("hex");
}

function createAllocateQuoteWindow() {
  const issuedAt = nowIso();
  const expiresAt = new Date(Date.now() + getX402QuoteTtlMs()).toISOString();
  return { issuedAt, expiresAt };
}

function resolveQuoteExpiresAt(existingRecord: X402AllocateRecord | undefined): string {
  if (existingRecord?.quoteExpiresAt && !isExpiredIsoTimestamp(existingRecord.quoteExpiresAt)) {
    return existingRecord.quoteExpiresAt;
  }
  return createAllocateQuoteWindow().expiresAt;
}

function getClientIp(req: Request): string {
  return req.ip || req.socket.remoteAddress || "unknown";
}

function enforceIpBurstLimit(req: Request): { limited: boolean; retryAfterSeconds: number } {
  const burstWindowMs = getX402IpBurstWindowMs();
  const burstLimit = getX402IpBurstLimit();
  const ip = getClientIp(req);
  const now = Date.now();
  const windowStart = now - burstWindowMs;
  const bucket = (x402IpBurstState.get(ip) ?? []).filter((timestamp) => timestamp >= windowStart);

  if (bucket.length >= burstLimit) {
    const oldest = bucket[0] ?? now;
    const retryAfterMs = Math.max(1_000, burstWindowMs - (now - oldest));
    return {
      limited: true,
      retryAfterSeconds: Math.ceil(retryAfterMs / 1000),
    };
  }

  bucket.push(now);
  x402IpBurstState.set(ip, bucket);
  return {
    limited: false,
    retryAfterSeconds: 0,
  };
}

function utcDayKey(value = new Date()): string {
  return value.toISOString().slice(0, 10);
}

function makeDailyUsageKey(fromAddress: string, day = utcDayKey()): string {
  return `${day}:${fromAddress.toLowerCase()}`;
}

function getAddressUsageCount(fromAddress: string): number {
  return getX402StateStore().getAddressDailyUsage(makeDailyUsageKey(fromAddress));
}

function incrementAddressUsage(fromAddress: string) {
  const key = makeDailyUsageKey(fromAddress);
  getX402StateStore().incrementAddressDailyUsage(key);
}

function secondsUntilNextUtcDay(): number {
  const now = new Date();
  const next = new Date(now);
  next.setUTCDate(now.getUTCDate() + 1);
  next.setUTCHours(0, 0, 0, 0);
  return Math.max(1, Math.ceil((next.getTime() - now.getTime()) / 1000));
}

function readAllocatePaymentProof(req: Request): AllocatePaymentProof {
  const headerFrom = req.header("x402-from-address")?.trim();
  const headerTx = req.header("x402-transaction-hash")?.trim();
  if (headerFrom || headerTx) {
    return {
      fromAddress: headerFrom,
      transactionHash: headerTx,
      source: "headers",
      deprecatedBodyProof: false,
    };
  }

  const bodyPayment = typeof req.body?.payment === "object" && req.body?.payment !== null
    ? req.body.payment as Record<string, unknown>
    : null;

  const bodyPaymentFrom = typeof bodyPayment?.fromAddress === "string" ? bodyPayment.fromAddress.trim() : undefined;
  const bodyPaymentTx = typeof bodyPayment?.transactionHash === "string" ? bodyPayment.transactionHash.trim() : undefined;
  if (bodyPaymentFrom || bodyPaymentTx) {
    return {
      fromAddress: bodyPaymentFrom,
      transactionHash: bodyPaymentTx,
      source: "body_payment",
      deprecatedBodyProof: true,
    };
  }

  const legacyFrom = typeof req.body?.fromAddress === "string" ? req.body.fromAddress.trim() : undefined;
  const legacyTx = typeof req.body?.transactionHash === "string" ? req.body.transactionHash.trim() : undefined;
  if (legacyFrom || legacyTx) {
    return {
      fromAddress: legacyFrom,
      transactionHash: legacyTx,
      source: "body_legacy",
      deprecatedBodyProof: true,
    };
  }

  return {
    source: "none",
    deprecatedBodyProof: false,
  };
}

function phaseStatus(jobId: string, phase: "phase2" | "phase3" | "phase4" | "phase5" | "phase6"): "idle" | "in_progress" | "complete" | "failed" | "not_found" {
  const status = getExecutionStatus(jobId);
  if (!status.found || !status.jobContext) return "not_found";
  return status.jobContext[phase].status;
}

function phaseError(jobId: string, phase: "phase2" | "phase3" | "phase4" | "phase5" | "phase6"): string | undefined {
  const status = getExecutionStatus(jobId);
  if (!status.found || !status.jobContext) return "job not found";
  return status.jobContext[phase].error;
}

async function waitForPhaseResult(jobId: string, phase: "phase2" | "phase3" | "phase4" | "phase5" | "phase6"): Promise<void> {
  const deadline = Date.now() + ALLOCATE_PHASE_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const status = phaseStatus(jobId, phase);
    if (status === "complete") return;
    if (status === "failed") {
      throw new Error(phaseError(jobId, phase) ?? `${phase} failed.`);
    }
    if (status === "not_found") {
      throw new Error("job not found.");
    }
    await sleep(ALLOCATE_PHASE_POLL_INTERVAL_MS);
  }

  throw new Error(`${phase} timed out.`);
}

async function orchestrateAllocateJob(jobId: string): Promise<void> {
  if (runningAllocateOrchestration.has(jobId)) return;
  runningAllocateOrchestration.add(jobId);

  try {
    await waitForPhaseResult(jobId, "phase2");
    runPhase3(jobId);
    await waitForPhaseResult(jobId, "phase3");
    runPhase4(jobId);
    await waitForPhaseResult(jobId, "phase4");
    runPhase5(jobId);
    await waitForPhaseResult(jobId, "phase5");
    runPhase6(jobId);
    await waitForPhaseResult(jobId, "phase6");
  } finally {
    runningAllocateOrchestration.delete(jobId);
  }
}

function computePriceContract() {
  const pricing = getWizardPricing();
  return {
    allocationOnlyUsdc: formatUsdcAmount(pricing.structuredAllocationPriceUsdc),
    allocationWithReportUsdc: formatUsdcAmount(
      pricing.structuredAllocationPriceUsdc + pricing.certifiedDecisionRecordFeeUsdc,
    ),
    formula: "chargedAmountUsdc = structuredAllocationPriceUsdc + (withReport ? certifiedDecisionRecordFeeUsdc : 0)",
    callerProvidedAmountAccepted: false,
  };
}

function toUsdPrice(amountUsdc: string): string {
  const amount = Number.parseFloat(amountUsdc);
  if (!Number.isFinite(amount)) return `$${amountUsdc}`;
  return `$${amount.toFixed(2)}`;
}

function toCaip2Network(networkId: string): string {
  if (networkId === "base-mainnet") return "eip155:8453";
  if (networkId === "base-sepolia") return "eip155:84532";
  return networkId;
}

function buildAllocateAccepts(payToAddress: string): X402AllocateAccept[] {
  const config = getConfig();
  const pricing = computePriceContract();
  const common = {
    scheme: "exact" as const,
    network: config.networkId,
    caip2Network: toCaip2Network(config.networkId),
    usdcContractAddress: config.usdcContractAddress,
    payToAddress,
    requiredHeaders: ["x402-from-address", "x402-transaction-hash"] as const,
  };

  return [
    {
      optionId: "allocation_only",
      amountUsdc: pricing.allocationOnlyUsdc,
      price: toUsdPrice(pricing.allocationOnlyUsdc),
      withReport: false,
      ...common,
    },
    {
      optionId: "allocation_with_report",
      amountUsdc: pricing.allocationWithReportUsdc,
      price: toUsdPrice(pricing.allocationWithReportUsdc),
      withReport: true,
      ...common,
    },
  ];
}

function buildPaymentRequirementsV2(accepts: X402AllocateAccept[]): X402PaymentRequirementV2[] {
  const maxTimeoutSeconds = Math.max(30, Math.ceil(getConfig().paymentTimeoutMs / 1000));
  return accepts.map((option) => ({
    scheme: "exact",
    network: option.caip2Network,
    amount: option.amountUsdc,
    asset: option.usdcContractAddress,
    payTo: option.payToAddress,
    maxTimeoutSeconds,
    extra: {
      optionId: option.optionId,
      withReport: option.withReport,
      requiredHeaders: option.requiredHeaders,
    },
  }));
}

function buildAllocateDiscoveryExtension() {
  return declareDiscoveryExtension({
    description:
      "Selun performs deterministic crypto allocation construction with optional certified decision record, payable via x402 (USDC) on Base.",
    bodyType: "json",
    input: {
      decisionId: "agent-run-001",
      riskTolerance: "Balanced",
      timeframe: "1-3_years",
      withReport: true,
    },
    inputSchema: {
      type: "object",
      properties: {
        decisionId: { type: "string" },
        riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
        timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
        withReport: { type: "boolean" },
      },
      required: ["decisionId", "riskTolerance", "timeframe", "withReport"],
    },
    output: {
      example: {
        success: true,
        executionModelVersion: EXECUTION_MODEL_VERSION,
        data: {
          status: "accepted",
          jobId: "selun-allocate-agent-run-001-1700000000000",
          decisionId: "agent-run-001",
          statusPath: "/execution-status/selun-allocate-agent-run-001-1700000000000",
        },
      },
    },
  });
}

// NEW: attach discovery extension as a response header (does not alter JSON bodies)
function attachBazaarDiscovery(res: Response) {
  try {
    const ext = buildAllocateDiscoveryExtension();
    // Header name is intentionally prefixed; consumers treat it as opaque JSON.
    res.setHeader("X-X402-Bazaar-Discovery", JSON.stringify(ext));
  } catch {
    // best-effort; do not break endpoint if metadata fails
  }
}

function buildAbsoluteResourceUrl(req: Request, routePath: string): string {
  const forwardedProto = req.header("x-forwarded-proto")?.split(",")[0]?.trim();
  const forwardedHost = req.header("x-forwarded-host")?.split(",")[0]?.trim();
  const proto = forwardedProto || req.protocol || "https";
  const host = forwardedHost || req.get("host");
  if (!host) return routePath;
  return `${proto}://${host}${routePath}`;
}

function sendRateLimited(
  res: Response,
  reason: "ip_burst_limit" | "from_address_daily_cap" | "global_concurrency_cap",
  retryAfterSeconds: number,
) {
  res.setHeader("Retry-After", String(retryAfterSeconds));
  return res.status(429).json({
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: "rate_limited",
    reason,
    retryAfterSeconds,
    logs: getExecutionLogs(120),
  });
}

function sendAllocateConflict(
  res: Response,
  decisionId: string,
  existingInputs: AllocateInputShape,
  requestedInputs: AllocateInputShape,
) {
  return res.status(409).json({
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: "idempotency_conflict",
    decisionId,
    message: "decisionId already exists with different inputs.",
    existingInputs,
    requestedInputs,
    logs: getExecutionLogs(120),
  });
}

function idempotentResponse(res: Response, record: X402AllocateRecord) {
  const statusPath = record.jobId ? `/execution-status/${encodeURIComponent(record.jobId)}` : null;
  const jobStatus = record.jobId ? getExecutionStatus(record.jobId) : null;
  const phase6Status = jobStatus?.jobContext?.phase6.status;
  const complete = phase6Status === "complete";

  return res.status(complete ? 200 : 202).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    data: {
      status: complete ? "already_complete" : "already_accepted",
      idempotentReplay: true,
      endpoint: "/agent/x402/allocate",
      jobId: record.jobId ?? null,
      decisionId: record.decisionId,
      inputs: record.inputs,
      payment: {
        required: Number.parseFloat(record.chargedAmountUsdc) > 0,
        chargedAmountUsdc: record.chargedAmountUsdc,
        verified: record.state === "accepted",
        fromAddress: record.payment?.fromAddress ?? null,
        transactionHash: record.payment?.transactionHash ?? null,
      },
      quoteExpiresAt: record.quoteExpiresAt,
      statusPath,
    },
    logs: getExecutionLogs(120),
  });
}

function getRecordByJobId(jobId: string): X402AllocateRecord | null {
  const decisionId = getX402StateStore().getDecisionIdForJob(jobId);
  if (!decisionId) return null;
  return getX402StateStore().getAllocateRecord(decisionId) ?? null;
}

export function getX402AllocateMetadataByJobId(jobId: string): {
  decisionId: string;
  inputs: AllocateInputShape;
  payment: {
    required: boolean;
    chargedAmountUsdc: string;
    verified: boolean;
    fromAddress: string | null;
    transactionHash: string | null;
  };
  quoteExpiresAt: string;
} | null {
  const record = getRecordByJobId(jobId);
  if (!record) return null;
  return {
    decisionId: record.decisionId,
    inputs: record.inputs,
    payment: {
      required: Number.parseFloat(record.chargedAmountUsdc) > 0,
      chargedAmountUsdc: record.chargedAmountUsdc,
      verified: record.state === "accepted",
      fromAddress: record.payment?.fromAddress ?? null,
      transactionHash: record.payment?.transactionHash ?? null,
    },
    quoteExpiresAt: record.quoteExpiresAt,
  };
}

async function sendAllocatePaymentRequired(
  res: Response,
  params: {
    inputs: AllocateInputShape;
    withReport: boolean;
    chargeAmountUsdc: string;
    decisionId: string;
    quoteExpiresAt: string;
    deprecationWarning?: string;
    persistQuote?: boolean;
  },
) {
  const shouldPersistQuote = params.persistQuote ?? true;
  const stateStore = getX402StateStore();
  const existing = shouldPersistQuote ? stateStore.getAllocateRecord(params.decisionId) : undefined;
  const inputFingerprint = computeAllocateInputFingerprint(params.inputs);
  const timestamp = nowIso();
  const record: X402AllocateRecord = existing
    ? {
      ...existing,
      inputFingerprint,
      inputs: params.inputs,
      chargedAmountUsdc: params.chargeAmountUsdc,
      quoteIssuedAt: timestamp,
      quoteExpiresAt: params.quoteExpiresAt,
      updatedAt: timestamp,
      state: existing.state === "accepted" ? "accepted" : "quoted",
    }
    : {
      decisionId: params.decisionId,
      inputFingerprint,
      inputs: params.inputs,
      chargedAmountUsdc: params.chargeAmountUsdc,
      quoteIssuedAt: timestamp,
      quoteExpiresAt: params.quoteExpiresAt,
      state: "quoted",
      createdAt: timestamp,
      updatedAt: timestamp,
    };
  if (shouldPersistQuote) {
    stateStore.setAllocateRecord(params.decisionId, record);
  }

  if (params.deprecationWarning) {
    res.setHeader("X-Deprecation-Notice", params.deprecationWarning);
  }

  res.setHeader("X-X402-Quote-Expires-At", params.quoteExpiresAt);
  res.setHeader("X-X402-Proof-Fields-Deprecated", "body.payment.*,body.fromAddress,body.transactionHash");
  res.setHeader("X-X402-Proof-Canonical", "x402-from-address,x402-transaction-hash");

  const identity = await getAgentAddress();
  const config = getConfig();
  const accepts = buildAllocateAccepts(identity.walletAddress);
  const paymentRequirementsV2 = buildPaymentRequirementsV2(accepts);
  const selectedAccept = accepts.find((option) => option.withReport === params.withReport) ?? accepts[0];

  res.setHeader("WWW-Authenticate", "x402");
  res.setHeader("X-X402-Network", config.networkId);
  res.setHeader("X-X402-Asset", config.usdcContractAddress);
  res.setHeader("X-X402-Amount", selectedAccept.amountUsdc);
  res.setHeader("X-X402-Payee", identity.walletAddress);

  attachBazaarDiscovery(res);

  return res.status(402).json({
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: "x402_payment_required",
    x402: {
      endpoint: "/agent/x402/allocate",
      amountUsdc: params.chargeAmountUsdc,
      accepts,
      paymentRequirementsV2,
      selectedAccept,
      network: config.networkId,
      caip2Network: toCaip2Network(config.networkId),
      usdcContractAddress: config.usdcContractAddress,
      payToAddress: identity.walletAddress,
      decisionId: params.decisionId,
      withReport: params.withReport,
      quoteExpiresAt: params.quoteExpiresAt,
      requiredHeaders: [
        "x402-from-address",
        "x402-transaction-hash",
      ],
      deprecatedProofFields: [
        "payment.fromAddress",
        "payment.transactionHash",
        "fromAddress",
        "transactionHash",
      ],
    },
    message: "Payment required. Submit a USDC transfer proof and retry the same request.",
    logs: getExecutionLogs(120),
  });
}

function success<T>(res: Response, data: T) {
  return res.status(200).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    data,
    logs: getExecutionLogs(120),
  });
}

function failure(res: Response, error: unknown, status = 500) {
  const message = error instanceof Error ? error.message : "Unexpected error.";
  return res.status(status).json({
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: message,
    logs: getExecutionLogs(120),
  });
}

async function verifyResultEmailPayment(params: {
  fromAddress: string;
  expectedAmountUSDC: number;
  transactionHash: string;
  decisionId: string;
}) {
  return verifyIncomingPayment({
    fromAddress: params.fromAddress,
    expectedAmountUSDC: params.expectedAmountUSDC,
    transactionHash: params.transactionHash,
    decisionId: params.decisionId,
  });
}

router.post("/init", async (_req: Request, res: Response) => {
  try {
    const identity = await initializeAgent();
    return success(res, identity);
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.get("/wallet", async (_req: Request, res: Response) => {
  try {
    const identity = await getAgentAddress();
    const balance = await getUSDCBalanceForAddress(identity.walletAddress);
    return success(res, {
      ...identity,
      usdc: {
        contractAddress: balance.usdcContractAddress,
        balance: balance.usdcBalance,
        balanceBaseUnits: balance.usdcBalanceBaseUnits,
      },
    });
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.get("/pricing", (_req: Request, res: Response) => {
  try {
    return success(res, getWizardPricing());
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.get("/x402/capabilities", async (_req: Request, res: Response) => {
  try {
    attachBazaarDiscovery(res);
    const identity = await getAgentAddress();
    const config = getConfig();
    const accepts = buildAllocateAccepts(identity.walletAddress);
    const paymentRequirementsV2 = buildPaymentRequirementsV2(accepts);
    return res.status(200).json({
      success: true,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      data: {
        endpoint: "/agent/x402/allocate",
        method: "POST",
        discoverable: true,
        versions: {
          executionModelVersion: EXECUTION_MODEL_VERSION,
        },
        supportedInputs: {
          riskTolerance: ["Conservative", "Balanced", "Growth", "Aggressive"],
          timeframe: ["<1_year", "1-3_years", "3+_years"],
          withReport: "boolean",
        },
        pricing: computePriceContract(),
        accepts,
        paymentRequirementsV2,
        paymentProof: {
          canonical: {
            headers: ["x402-from-address", "x402-transaction-hash"],
          },
          deprecatedBodyFields: [
            "payment.fromAddress",
            "payment.transactionHash",
            "fromAddress",
            "transactionHash",
          ],
        },
        limits: {
          ipBurstLimit: {
            requests: getX402IpBurstLimit(),
            windowMs: getX402IpBurstWindowMs(),
          },
          fromAddressDailyCap: getX402FromAddressDailyCap(),
          globalConcurrencyCap: getX402GlobalConcurrencyCap(),
        },
        idempotency: {
          required: true,
          acceptedKeys: ["decisionId", "Idempotency-Key"],
          conflictBehavior: "same decisionId + different inputs => 409",
        },
        discovery: {
          type: "http",
          facilitator: "coinbase_cdp_compatible",
          network: config.networkId,
          caip2Network: toCaip2Network(config.networkId),
          metadataRefreshCadenceHours: 6,
          category: "finance:allocation",
          tags: ["portfolio", "allocation", "risk", "x402", "audit"],
        },
        inputSchema: {
          type: "object",
          properties: {
            decisionId: { type: "string" },
            riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
            timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
            withReport: { type: "boolean" },
          },
          required: ["decisionId", "riskTolerance", "timeframe", "withReport"],
        },
        outputSchema: {
          acceptedResponse: {
            type: "object",
            properties: {
              success: { type: "boolean" },
              data: {
                type: "object",
                properties: {
                  status: { type: "string" },
                  jobId: { type: "string" },
                  decisionId: { type: "string" },
                  statusPath: { type: "string" },
                },
              },
            },
          },
        },
        examples: {
          request: {
            decisionId: "agent-run-001",
            riskTolerance: "Balanced",
            timeframe: "1-3_years",
            withReport: true,
          },
        },
      },
      logs: getExecutionLogs(120),
    });
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.get("/x402/discovery", async (_req: Request, res: Response) => {
  // Alias for Bazaar-style discovery crawlers; keep payload identical to /x402/capabilities.
  // Express Router is a callable request handler; use it to re-dispatch the request.
  const forwardedReq = { ..._req, url: "/x402/capabilities", method: "GET" } as Request;
  return new Promise<void>((resolve, reject) => {
    router(forwardedReq, res, (err?: unknown) => {
      if (err) reject(err);
      else resolve();
    });
  });
});

router.post("/usdc-balance", async (req: Request, res: Response) => {
  const walletAddress = req.body?.walletAddress;

  if (typeof walletAddress !== "string") {
    return failure(res, new Error("walletAddress is required."), 400);
  }
  if (!isAddress(walletAddress)) {
    return failure(res, new Error("walletAddress must be a valid EVM address."), 400);
  }

  try {
    const balance = await getUSDCBalanceForAddress(walletAddress);
    return success(res, balance);
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.post("/verify-payment", async (req: Request, res: Response) => {
  const fromAddress = req.body?.fromAddress;
  const expectedAmountUSDC = req.body?.expectedAmountUSDC;
  const transactionHash = req.body?.transactionHash;
  const decisionId = req.body?.decisionId;

  if (typeof fromAddress !== "string") {
    return failure(res, new Error("fromAddress is required."), 400);
  }
  if (!(typeof expectedAmountUSDC === "string" || typeof expectedAmountUSDC === "number")) {
    return failure(res, new Error("expectedAmountUSDC is required."), 400);
  }
  if (transactionHash !== undefined && typeof transactionHash !== "string") {
    return failure(res, new Error("transactionHash must be a string when provided."), 400);
  }
  if (decisionId !== undefined && typeof decisionId !== "string") {
    return failure(res, new Error("decisionId must be a string when provided."), 400);
  }

  try {
    const receipt = await verifyIncomingPayment({
      fromAddress,
      expectedAmountUSDC,
      transactionHash,
      decisionId,
    });
    return success(res, receipt);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/result-email", async (req: Request, res: Response) => {
  const resultEmail = toText(req.body?.resultEmail, "").toLowerCase();
  if (!resultEmail || !isValidEmail(resultEmail)) {
    return failure(res, new Error("resultEmail must be a valid email address."), 400);
  }

  const payment = isRecord(req.body?.payment) ? req.body.payment : null;
  const paymentStatus = toText(payment?.status, "").toLowerCase();
  const decisionId = toText(payment?.decisionId, "");
  const transactionId = toText(payment?.transactionId, "");
  const chargedAmountUsdc = toFiniteNumber(payment?.chargedAmountUsdc ?? payment?.amountUsdc);
  const walletAddress = toText(req.body?.walletAddress, "");
  if (paymentStatus !== "paid" || !decisionId || !transactionId) {
    return failure(res, new Error("Paid purchase record is required before result email delivery."), 402);
  }
  if (chargedAmountUsdc === null || chargedAmountUsdc < 0) {
    return failure(res, new Error("Charged amount is required before result email delivery."), 400);
  }
  if (!walletAddress || !isAddress(walletAddress)) {
    return failure(res, new Error("Valid wallet address is required before result email delivery."), 400);
  }

  try {
    await verifyResultEmailPayment({
      fromAddress: walletAddress,
      expectedAmountUSDC: chargedAmountUsdc,
      transactionHash: transactionId,
      decisionId,
    });
  } catch (error) {
    return failure(res, new Error(`Payment verification required: ${error instanceof Error ? error.message : "verification failed."}`), 402);
  }

  const phase2Artifact = isRecord(req.body?.phase2Artifact) ? req.body.phase2Artifact : null;
  const allocationPolicy = phase2Artifact && isRecord(phase2Artifact.allocation_policy)
    ? phase2Artifact.allocation_policy
    : null;
  const result = await sendUserSummaryEmail({
    toEmail: resultEmail,
    decisionId,
    riskMode: toText(req.body?.riskMode, "n/a"),
    investmentHorizon: toText(req.body?.investmentHorizon, "n/a"),
    regimeDetected: toText(req.body?.regimeDetected, "Unknown"),
    strategyLabel: mapStrategyLabel(allocationPolicy?.mode),
    walletAddress,
    chargedAmountUsdc,
    transactionHash: transactionId,
    certifiedDecisionRecordPurchased: Boolean(payment?.certifiedDecisionRecordPurchased),
    allocations: collectAllocationRows(req.body?.allocations).slice(0, 12),
  });

  if (result.status === "failed") {
    return failure(res, new Error(result.error || "Result email delivery failed."), 502);
  }

  return success(res, {
    status: result.status,
    error: result.error ?? null,
  });
});

router.post("/report-email", async (req: Request, res: Response) => {
  const resultEmail = toText(req.body?.resultEmail, "").toLowerCase();
  if (!resultEmail || !isValidEmail(resultEmail)) {
    return failure(res, new Error("resultEmail must be a valid email address."), 400);
  }

  const filename = toText(req.body?.filename, "");
  const pdfBase64 = toText(req.body?.pdfBase64, "");
  if (!filename) {
    return failure(res, new Error("filename is required for report email delivery."), 400);
  }
  if (!pdfBase64) {
    return failure(res, new Error("pdfBase64 is required for report email delivery."), 400);
  }

  const payment = isRecord(req.body?.payment) ? req.body.payment : null;
  const paymentStatus = toText(payment?.status, "").toLowerCase();
  const decisionId = toText(payment?.decisionId, "");
  const transactionId = toText(payment?.transactionId, "");
  const chargedAmountUsdc = toFiniteNumber(payment?.chargedAmountUsdc ?? payment?.amountUsdc);
  const walletAddress = toText(req.body?.walletAddress, "");
  if (paymentStatus !== "paid" || !decisionId || !transactionId) {
    return failure(res, new Error("Paid purchase record is required before report email delivery."), 402);
  }
  if (chargedAmountUsdc === null || chargedAmountUsdc < 0) {
    return failure(res, new Error("Charged amount is required before report email delivery."), 400);
  }
  if (!walletAddress || !isAddress(walletAddress)) {
    return failure(res, new Error("Valid wallet address is required before report email delivery."), 400);
  }

  try {
    await verifyResultEmailPayment({
      fromAddress: walletAddress,
      expectedAmountUSDC: chargedAmountUsdc,
      transactionHash: transactionId,
      decisionId,
    });
  } catch (error) {
    return failure(res, new Error(`Payment verification required: ${error instanceof Error ? error.message : "verification failed."}`), 402);
  }

  const result = await sendUserReportEmail({
    toEmail: resultEmail,
    decisionId,
    filename,
    pdfBase64,
    riskMode: toText(req.body?.riskMode, "n/a"),
    investmentHorizon: toText(req.body?.investmentHorizon, "n/a"),
    includeCertified: Boolean(req.body?.includeCertifiedDecisionRecord),
    walletAddress,
    amountUsdc: chargedAmountUsdc,
    transactionHash: transactionId,
  });

  if (result.status === "failed") {
    return failure(res, new Error(result.error || "Report email delivery failed."), 502);
  }

  return success(res, {
    status: result.status,
    error: result.error ?? null,
  });
});

router.post("/pay", async (req: Request, res: Response) => {
  const walletAddress = req.body?.walletAddress;

  if (typeof walletAddress !== "string") {
    return failure(res, new Error("walletAddress is required."), 400);
  }

  try {
    const receipt = await authorizeWizardPayment({
      walletAddress,
      includeCertifiedDecisionRecord: req.body?.includeCertifiedDecisionRecord,
      riskMode: req.body?.riskMode,
      investmentHorizon: req.body?.investmentHorizon,
      promoCode: req.body?.promoCode,
    });

    void sendAdminUsageEmail({
      channel: "legacy_pay",
      decisionId: receipt.decisionId,
      walletAddress,
      resultEmail:
        typeof req.body?.resultEmail === "string" && isValidEmail(req.body.resultEmail.trim().toLowerCase())
          ? req.body.resultEmail.trim().toLowerCase()
          : null,
      promoCode: typeof req.body?.promoCode === "string" && req.body.promoCode.trim()
        ? req.body.promoCode.trim()
        : null,
      chargedAmountUsdc: receipt.chargedAmountUsdc,
      transactionHash: receipt.transactionId,
      paymentMethod: receipt.paymentMethod ?? "onchain",
      includeCertifiedDecisionRecord: receipt.certifiedDecisionRecordPurchased,
      riskTolerance: typeof req.body?.riskMode === "string" ? req.body.riskMode : null,
      timeframe: typeof req.body?.investmentHorizon === "string" ? req.body.investmentHorizon : null,
      jobId: null,
    }).catch((error) => {
      console.error("Failed to send Selun admin usage email (legacy_pay):", error);
    });

    return success(res, receipt);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/pay-quote", async (req: Request, res: Response) => {
  const walletAddress = req.body?.walletAddress;

  if (typeof walletAddress !== "string") {
    return failure(res, new Error("walletAddress is required."), 400);
  }

  try {
    const quote = await quoteWizardPayment({
      walletAddress,
      includeCertifiedDecisionRecord: req.body?.includeCertifiedDecisionRecord,
      promoCode: req.body?.promoCode,
    });
    return success(res, quote);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/store-hash", async (req: Request, res: Response) => {
  const decisionId = req.body?.decisionId;
  const pdfHash = req.body?.pdfHash;

  if (typeof decisionId !== "string" || !decisionId.trim()) {
    return failure(res, new Error("decisionId is required."), 400);
  }
  if (typeof pdfHash !== "string" || !pdfHash.trim()) {
    return failure(res, new Error("pdfHash is required."), 400);
  }

  try {
    const receipt = await storeDecisionHashOnChain({ decisionId, pdfHash });
    return success(res, receipt);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/x402/allocate", async (req: Request, res: Response) => {
  const burst = enforceIpBurstLimit(req);
  if (burst.limited) {
    return sendRateLimited(res, "ip_burst_limit", burst.retryAfterSeconds);
  }

  const riskTolerance = normalizeRiskTolerance(req.body?.riskTolerance);
  const timeframe = normalizeTimeframe(req.body?.timeframe);
  const withReportParsed = normalizeOptionalBoolean(req.body?.withReport);
  if (!withReportParsed.valid) {
    return failure(res, new Error("withReport must be a boolean (true or false)."), 400);
  }
  const withReport = withReportParsed.value;
  const chargeAmountUsdc = getAllocateChargeAmountUsdc(withReport);
  const requiresPayment = Number.parseFloat(chargeAmountUsdc) > 0;
  const decisionResolution = resolveDecisionId(req);
  const decisionId = decisionResolution.decisionId;
  const missingDecisionIdOnlyError = !decisionId && decisionResolution.error?.includes("decisionId is required");
  if (decisionResolution.error && !missingDecisionIdOnlyError) {
    return failure(res, new Error(decisionResolution.error), 400);
  }
  const stateStore = getX402StateStore();
  const existingRecord = decisionId ? stateStore.getAllocateRecord(decisionId) : undefined;
  const proof = readAllocatePaymentProof(req);
  const missingProof = requiresPayment && (!proof.fromAddress || !proof.transactionHash);

  if (existingRecord?.state === "accepted" && existingRecord.jobId) {
    return idempotentResponse(res, existingRecord);
  }

  if (proof.fromAddress && !isAddress(proof.fromAddress)) {
    return failure(res, new Error("x402-from-address must be a valid EVM address."), 400);
  }

  if (missingProof) {
    try {
      const quoteDecisionId = decisionId ?? `quote-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
      const quoteInputs: AllocateInputShape = {
        riskTolerance: riskTolerance ?? "Balanced",
        timeframe: timeframe ?? "1-3_years",
        withReport,
      };
      const persistQuote = Boolean(decisionId && riskTolerance && timeframe);

      return await sendAllocatePaymentRequired(res, {
        inputs: quoteInputs,
        withReport,
        chargeAmountUsdc,
        decisionId: quoteDecisionId,
        quoteExpiresAt: resolveQuoteExpiresAt(existingRecord),
        deprecationWarning: proof.deprecatedBodyProof
          ? "Body payment proof fields are deprecated; use x402-from-address and x402-transaction-hash headers."
          : undefined,
        persistQuote,
      });
    } catch (error) {
      return failure(res, error, 500);
    }
  }

  if (!decisionId) {
    return failure(res, new Error(decisionResolution.error ?? "decisionId is required."), 400);
  }

  if (!riskTolerance) {
    return failure(
      res,
      new Error("riskTolerance is required. Allowed: Conservative | Balanced | Growth | Aggressive."),
      400,
    );
  }

  if (!timeframe) {
    return failure(
      res,
      new Error("timeframe is required. Allowed: <1_year | 1-3_years | 3+_years."),
      400,
    );
  }

  const inputShape: AllocateInputShape = {
    riskTolerance,
    timeframe,
    withReport,
  };
  const inputFingerprint = computeAllocateInputFingerprint(inputShape);
  const decisionScopedRecord = stateStore.getAllocateRecord(decisionId);

  if (decisionScopedRecord && decisionScopedRecord.inputFingerprint !== inputFingerprint) {
    return sendAllocateConflict(res, decisionId, decisionScopedRecord.inputs, inputShape);
  }

  if (decisionScopedRecord?.state === "accepted" && decisionScopedRecord.jobId) {
    return idempotentResponse(res, decisionScopedRecord);
  }

  if (decisionScopedRecord?.state === "quoted" && isExpiredIsoTimestamp(decisionScopedRecord.quoteExpiresAt)) {
    try {
      return await sendAllocatePaymentRequired(res, {
        inputs: inputShape,
        withReport,
        chargeAmountUsdc,
        decisionId,
        quoteExpiresAt: createAllocateQuoteWindow().expiresAt,
        deprecationWarning: proof.deprecatedBodyProof
          ? "Body payment proof fields are deprecated; use x402-from-address and x402-transaction-hash headers."
          : undefined,
      });
    } catch (error) {
      return failure(res, error, 500);
    }
  }

  if (proof.transactionHash) {
    const transactionOwner = stateStore.getTransactionOwner(proof.transactionHash);
    if (transactionOwner && transactionOwner !== decisionId) {
      return failure(
        res,
        new Error("x402-transaction-hash was already consumed for a different decisionId."),
        409,
      );
    }
  }

  if (runningAllocateOrchestration.size >= getX402GlobalConcurrencyCap()) {
    return sendRateLimited(res, "global_concurrency_cap", 10);
  }

  const quoteExpiresAt = resolveQuoteExpiresAt(decisionScopedRecord);

  if (requiresPayment && (!proof.fromAddress || !proof.transactionHash)) {
    try {
      return await sendAllocatePaymentRequired(res, {
        inputs: inputShape,
        withReport,
        chargeAmountUsdc,
        decisionId,
        quoteExpiresAt,
        deprecationWarning: proof.deprecatedBodyProof
          ? "Body payment proof fields are deprecated; use x402-from-address and x402-transaction-hash headers."
          : undefined,
      });
    } catch (error) {
      return failure(res, error, 500);
    }
  }

  if (proof.fromAddress && getAddressUsageCount(proof.fromAddress) >= getX402FromAddressDailyCap()) {
    return sendRateLimited(res, "from_address_daily_cap", secondsUntilNextUtcDay());
  }

  if (requiresPayment) {
    try {
      await verifyIncomingPayment({
        fromAddress: proof.fromAddress as string,
        expectedAmountUSDC: chargeAmountUsdc,
        transactionHash: proof.transactionHash,
        decisionId,
      });
    } catch (error) {
      try {
        return await sendAllocatePaymentRequired(res, {
          inputs: inputShape,
          withReport,
          chargeAmountUsdc,
          decisionId,
          quoteExpiresAt: createAllocateQuoteWindow().expiresAt,
          deprecationWarning: proof.deprecatedBodyProof
            ? "Body payment proof fields are deprecated; use x402-from-address and x402-transaction-hash headers."
            : undefined,
        });
      } catch {
        return failure(res, error, 402);
      }
    }

    if (proof.transactionHash) {
      const reservation = stateStore.reserveTransactionHash(proof.transactionHash, decisionId);
      if (!reservation.accepted) {
        return failure(
          res,
          new Error("x402-transaction-hash was already consumed for a different decisionId."),
          409,
        );
      }
    }
  }

  const jobId = decisionScopedRecord?.jobId ?? buildAllocateJobId(decisionId);

  try {
    if (!decisionScopedRecord?.jobId) {
      runPhase1({
        jobId,
        executionTimestamp: nowIso(),
        riskMode: deriveRiskMode(riskTolerance),
        riskTolerance,
        investmentTimeframe: timeframe,
        walletAddress: proof.fromAddress,
      });

      void orchestrateAllocateJob(jobId).catch((error) => {
        console.error(`Allocate orchestration failed for ${jobId}:`, error);
      });
    }

    const acceptedAt = nowIso();
    const persistedRecord: X402AllocateRecord = {
      decisionId,
      inputFingerprint,
      inputs: inputShape,
      chargedAmountUsdc: chargeAmountUsdc,
      quoteIssuedAt: decisionScopedRecord?.quoteIssuedAt ?? acceptedAt,
      quoteExpiresAt,
      state: "accepted",
      createdAt: decisionScopedRecord?.createdAt ?? acceptedAt,
      updatedAt: acceptedAt,
      jobId,
      payment: proof.fromAddress && proof.transactionHash
        ? {
          fromAddress: proof.fromAddress,
          transactionHash: proof.transactionHash,
          verifiedAt: acceptedAt,
        }
        : undefined,
    };

    stateStore.setAllocateRecord(decisionId, persistedRecord);
    if (!decisionScopedRecord?.jobId && proof.fromAddress) {
      incrementAddressUsage(proof.fromAddress);
    }

    void sendAdminUsageEmail({
      channel: "x402_allocate",
      decisionId,
      walletAddress: proof.fromAddress ?? null,
      resultEmail:
        typeof req.body?.resultEmail === "string" && isValidEmail(req.body.resultEmail.trim().toLowerCase())
          ? req.body.resultEmail.trim().toLowerCase()
          : null,
      promoCode: typeof req.body?.promoCode === "string" && req.body.promoCode.trim()
        ? req.body.promoCode.trim()
        : null,
      chargedAmountUsdc: chargeAmountUsdc,
      transactionHash: proof.transactionHash ?? null,
      paymentMethod: "x402",
      includeCertifiedDecisionRecord: withReport,
      riskTolerance,
      timeframe,
      jobId,
    }).catch((error) => {
      console.error("Failed to send Selun admin usage email (x402_allocate):", error);
    });

    if (proof.deprecatedBodyProof) {
      res.setHeader(
        "X-Deprecation-Notice",
        "Body payment proof fields are deprecated; use x402-from-address and x402-transaction-hash headers.",
      );
    }

    const statusPath = `/execution-status/${encodeURIComponent(jobId)}`;
    return res.status(202).json({
      success: true,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      data: {
        status: decisionScopedRecord?.jobId ? "already_accepted" : "accepted",
        idempotentReplay: Boolean(decisionScopedRecord?.jobId),
        endpoint: "/agent/x402/allocate",
        jobId,
        decisionId,
        inputs: inputShape,
        payment: {
          required: requiresPayment,
          chargedAmountUsdc: chargeAmountUsdc,
          verified: requiresPayment ? true : false,
          fromAddress: proof.fromAddress ?? null,
          transactionHash: proof.transactionHash ?? null,
        },
        quoteExpiresAt,
        statusPath,
      },
      logs: getExecutionLogs(120),
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase1/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  const riskMode = typeof req.body?.riskMode === "string" ? req.body.riskMode : undefined;
  const timeWindow = typeof req.body?.timeWindow === "string" ? req.body.timeWindow : undefined;
  const executionTimestamp = typeof req.body?.executionTimestamp === "string"
    ? req.body.executionTimestamp
    : undefined;
  const riskTolerance = typeof req.body?.riskTolerance === "string" ? req.body.riskTolerance : undefined;
  const investmentTimeframe = typeof req.body?.investmentTimeframe === "string"
    ? req.body.investmentTimeframe
    : undefined;
  const walletAddress = typeof req.body?.walletAddress === "string" ? req.body.walletAddress : undefined;

  try {
    runPhase1({
      jobId,
      executionTimestamp,
      riskMode,
      riskTolerance,
      investmentTimeframe,
      timeWindow,
      walletAddress,
    });
    return res.status(202).json({
      status: "started",
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase3/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase3(jobId);
    return res.status(202).json({
      status: "started",
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase4/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase4(jobId);
    return res.status(202).json({
      status: "started",
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase5/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase5(jobId);
    return res.status(202).json({
      status: "started",
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase6/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase6(jobId);
    return res.status(202).json({
      status: "started",
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

export function createAgentRouter() {
  return router;
}
