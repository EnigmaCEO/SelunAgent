import { Router, type Request, type Response } from "express";
import { createHash, timingSafeEqual } from "node:crypto";
import { createFacilitatorConfig } from "@coinbase/x402";
import { encodePaymentResponseHeader } from "@x402/core/http";
import { HTTPFacilitatorClient, type HTTPRequestContext, type ProcessSettleSuccessResponse, type RouteConfig, x402ResourceServer } from "@x402/core/server";
import type { Network, PaymentRequirements } from "@x402/core/types";
import { ExactEvmScheme } from "@x402/evm/exact/server";
import { SOLANA_MAINNET_CAIP2, USDC_MAINNET_ADDRESS as USDC_SOLANA_MAINNET_ADDRESS, validateSvmAddress } from "@x402/svm";
import { ExpressAdapter, x402HTTPResourceServer } from "@x402/express";
import { bazaarResourceServerExtension, declareDiscoveryExtension } from "@x402/extensions/bazaar";
import { formatUnits, isAddress, parseUnits } from "viem";
import { EXECUTION_MODEL_VERSION, getConfig } from "../config";
import { getExecutionLogs } from "../logging/execution-logs";
import { getX402StateStore } from "../services/x402-state.service";
import { isExpiredIsoTimestamp, normalizeOptionalBoolean } from "../services/x402-utils";
import {
  normalizePortfolioSegment,
  portfolioSegmentKey,
  PORTFOLIO_SEGMENTS,
} from "../services/portfolio-segments";
import type { AdminUsageChannel } from "../services/email.service";
import { isValidEmail, sendAdminErrorEmail, sendAdminInterestEmail, sendAdminUsageEmail, sendUserReportEmail, sendUserSummaryEmail } from "../services/email.service";
import { normalizeReferralCode, recordReferralConversion } from "../services/referral.service";
import { callSceContinuityMode, callSceCaseRelevance, callSceRiskEvaluate } from "../services/sce-client";
import { confirmAgentAllocation, isAgentProgramReferrerId, normalizeAgentProgramReferrerId } from "../services/agent-program.service";
import {
  authorizeWizardPayment,
  getAgentAddress,
  getAdminFundingWalletSnapshot,
  getAgentWalletSnapshot,
  getTreasuryWalletSetupSnapshot,
  getUSDCBalanceForAddress,
  getWizardPricing,
  initializeAgent,
  listProjectSellerWalletSnapshots,
  ensureTreasurySmartAccount,
  quoteWizardPayment,
  storeDecisionHashOnChain,
  transferNativeFromAdminWallet,
  transferSellerNative,
  transferSellerUsdc,
  swapSellerUsdcToEth,
  transferTreasuryUsdc,
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
  PortfolioSegment,
  X402AllocateRecord,
  X402ToolRecord,
  X402ToolProductId,
} from "../services/x402-state.types";

const router = Router();

type X402AllocateOptionId = "allocation_only" | "allocation_with_report";

type X402AllocateAccept = {
  optionId: X402AllocateOptionId;
  scheme: "exact";
  amountUsdc: string;
  amount: string;
  price: string;
  withReport: boolean;
  network: string;
  caip2Network: Network;
  asset: string;
  payTo: string;
  maxTimeoutSeconds: number;
};
type X402PaymentRequirementExtra = {
  optionId: X402AllocateOptionId;
  withReport: boolean;
  name: string;
  version: string;
  decisionId?: string;
  inputFingerprint?: string;
  quoteIssuedAt?: string;
  quoteExpiresAt?: string;
  executionModelVersion?: string;
};
type X402PaymentRequirementV2 = PaymentRequirements & {
  scheme: "exact";
  network: Network;
  extra: X402PaymentRequirementExtra;
};

type AllocationEmailRow = {
  asset: string;
  name: string;
  category: string;
  riskClass: string;
  allocationPct: number;
};

type X402ToolPaymentRequirementV2 = PaymentRequirements & {
  scheme: "exact";
  network: Network;
  extra: Record<string, unknown>;
};

type X402ToolDefinition = {
  productId: X402ToolProductId;
  routePath: string;
  title: string;
  description: string;
  category: string;
  tags: string[];
  amountUsdc: () => string;
  inputSchema: Record<string, unknown>;
  exampleInput: Record<string, unknown>;
  exampleOutput: Record<string, unknown>;
};

type X402ToolBaseInput = {
  riskTolerance: AllocateRiskTolerance;
  timeframe: AllocateTimeframe;
  portfolioSegment: PortfolioSegment;
};

type AssetScorecardInput = X402ToolBaseInput & {
  assets: string[];
};

type RebalanceHolding = {
  asset: string;
  name: string | null;
  usdValue: number;
  allocationPct: number | null;
};

type RebalanceInput = X402ToolBaseInput & {
  holdings: RebalanceHolding[];
};

type StoredToolResponseData = {
  status: "completed";
  endpoint: string;
  decisionId: string;
  productId: X402ToolProductId;
  payment: {
    required: true;
    chargedAmountUsdc: string;
    verified: true;
    fromAddress: string;
    transactionHash: string;
    network: string | null;
  };
  result: Record<string, unknown>;
};

type AdminPurchaseRow = {
  purchaseKey: string;
  kind: "allocate" | "tool";
  endpoint: string;
  title: string;
  decisionId: string;
  productId: string;
  chargedAmountUsdc: string;
  fromAddress: string;
  paymentTransactionHash: string;
  paymentNetwork: string | null;
  purchasedAt: string;
  refunded: boolean;
  refund: {
    refundedAt: string;
    transactionHash: string;
    toAddress: string;
    amountUsdc: string;
    note: string | null;
  } | null;
};

type AdminNativeTopUpSummary = {
  transactionHash: string;
  amountEth: string;
  toAddress: string;
  fromWalletAddress: string;
  nativeBalanceBefore: string;
  nativeBalanceAfter: string;
};

type AdminRefundReference = {
  kind: "allocate" | "tool";
  decisionId: string;
  productId?: X402ToolProductId;
};

const ALLOCATE_PHASE_POLL_INTERVAL_MS = 2_000;
const ALLOCATE_PHASE_TIMEOUT_MS = 20 * 60 * 1_000;
const runningAllocateOrchestration = new Set<string>();
const x402IpBurstState = new Map<string, number[]>();
let x402SellerServer: x402ResourceServer | null = null;
let x402SellerServerInitPromise: Promise<x402ResourceServer> | null = null;
const KNOWN_X402_EIP712_DOMAINS: Record<string, { name: string; version: string }> = {
  "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": { name: "USD Coin", version: "2" },
  "0x036cbd53842c5426634e7929541ec2318f3dcf7e": { name: "USDC", version: "2" },
};

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

function extractReferralCode(req: Request): string | null {
  const queryRef = Array.isArray(req.query?.ref)
    ? req.query.ref[0]
    : typeof req.query?.ref === "string"
      ? req.query.ref
      : undefined;
  const bodyRef = typeof req.body?.referralCode === "string" ? req.body.referralCode : undefined;
  const raw = (queryRef ?? bodyRef)?.trim() ?? "";
  if (!raw) {
    return null;
  }

  const agentProgramReferrerId = normalizeAgentProgramReferrerId(raw);
  if (agentProgramReferrerId) {
    return agentProgramReferrerId;
  }

  return normalizeReferralCode(raw);
}

async function recordPaidAllocationReferral(params: {
  referralCode: string | null;
  transactionId: string;
  amountUsd: number;
  routePath: string;
  userId: string;
  allocationAmountUsd?: number;
  reportAmountUsd?: number;
  paymentReference?: string;
}) {
  if (!params.referralCode) {
    return;
  }

  try {
    if (isAgentProgramReferrerId(params.referralCode)) {
      await confirmAgentAllocation({
        referrerId: params.referralCode,
        userId: params.userId,
        allocationId: params.transactionId,
        amountUsd: params.amountUsd,
        allocationAmountUsd: params.allocationAmountUsd,
        reportAmountUsd: params.reportAmountUsd,
        totalPaidUsd: params.amountUsd,
        paymentReference: params.paymentReference ?? params.transactionId,
        source: params.routePath,
      });
      return;
    }

    await recordReferralConversion({
      referralCode: params.referralCode,
      transactionId: params.transactionId,
      amountUsd: params.amountUsd,
    });
  } catch (error) {
    console.error(`Failed to record referral conversion for ${params.routePath}:`, error);
  }
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

function getAdminApiToken(): string | null {
  const token = process.env.SELUN_ADMIN_API_TOKEN?.trim() || process.env.SELUN_ADMIN_TOKEN?.trim();
  return token || null;
}

function getAdminWithdrawAddress(): string {
  const candidate =
    process.env.SELUN_ADMIN_WITHDRAW_ADDRESS?.trim() ||
    process.env.SELUN_ADMIN_REFUND_ADDRESS?.trim() ||
    process.env.SELUN_ADMIN_WALLET_ADDRESS?.trim();
  if (!candidate || !isAddress(candidate)) {
    throw new Error("SELUN_ADMIN_WITHDRAW_ADDRESS must be a valid EVM address.");
  }
  return candidate;
}

function getAdminGasTopUpAmountEth(): string {
  const candidate = process.env.SELUN_ADMIN_GAS_TOP_UP_ETH?.trim();
  if (!candidate) return "0.00005";
  const parsed = Number.parseFloat(candidate);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error("SELUN_ADMIN_GAS_TOP_UP_ETH must be greater than zero.");
  }
  return candidate;
}

function extractAdminToken(req: Request): string {
  const headerToken =
    req.header("x-selun-admin-token")?.trim() ||
    req.header("X-Selun-Admin-Token")?.trim();
  if (headerToken) return headerToken;

  const authorization = req.header("authorization")?.trim() || req.header("Authorization")?.trim();
  if (!authorization) return "";
  const match = authorization.match(/^Bearer\s+(.+)$/i);
  return match?.[1]?.trim() || "";
}

function isAuthorizedAdminRequest(req: Request): boolean {
  const expected = getAdminApiToken();
  if (!expected) return false;
  const provided = extractAdminToken(req);
  if (!provided) return false;

  const expectedBuffer = Buffer.from(expected);
  const providedBuffer = Buffer.from(provided);
  if (expectedBuffer.length !== providedBuffer.length) return false;
  return timingSafeEqual(expectedBuffer, providedBuffer);
}

function requireAdminRequest(req: Request, res: Response): boolean {
  const expected = getAdminApiToken();
  if (!expected) {
    failure(res, new Error("SELUN_ADMIN_API_TOKEN is not configured."), 503);
    return false;
  }
  if (!isAuthorizedAdminRequest(req)) {
    failure(res, new Error("Unauthorized admin request."), 401);
    return false;
  }
  return true;
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
  const rounded2 = Number(amount.toFixed(2));
  return rounded2 === amount ? `$${amount.toFixed(2)}` : `$${amountUsdc}`;
}

function toCaip2Network(networkId: string): string {
  if (networkId === "base-mainnet") return "eip155:8453";
  if (networkId === "base-sepolia") return "eip155:84532";
  return networkId;
}

function getX402FacilitatorUrl() {
  const configured = process.env.X402_FACILITATOR_URL?.trim();
  if (configured) return configured;
  return getConfig().networkId === "base-mainnet"
    ? "https://api.cdp.coinbase.com/platform/v2/x402"
    : "https://www.x402.org/facilitator";
}

function normalizeSecret(value: string): string {
  const withoutWrappingQuotes =
    (value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))
      ? value.slice(1, -1)
      : value;

  return withoutWrappingQuotes.replace(/\\n/g, "\n");
}

function getX402MainnetFacilitatorApiKeyId() {
  return process.env.CDP_API_KEY_ID?.trim() || getConfig().coinbaseApiKey;
}

function getX402MainnetFacilitatorApiKeySecret() {
  const raw = process.env.CDP_API_KEY_SECRET?.trim();
  return raw ? normalizeSecret(raw) : getConfig().coinbaseApiSecret;
}

function createX402FacilitatorClient() {
  if (getConfig().networkId === "base-mainnet") {
    return new HTTPFacilitatorClient(
      createFacilitatorConfig(
        getX402MainnetFacilitatorApiKeyId(),
        getX402MainnetFacilitatorApiKeySecret(),
      ),
    );
  }

  return new HTTPFacilitatorClient({
    url: getX402FacilitatorUrl(),
  });
}

function getX402MaxTimeoutSeconds() {
  return Math.max(30, Math.ceil(getConfig().paymentTimeoutMs / 1000));
}

function getDefaultX402Eip712Domain() {
  const asset = getConfig().usdcContractAddress.toLowerCase();
  return KNOWN_X402_EIP712_DOMAINS[asset] || { name: "USD Coin", version: "2" };
}

function getX402Eip712DomainName() {
  return process.env.X402_EIP712_DOMAIN_NAME?.trim() || getDefaultX402Eip712Domain().name;
}

function getX402Eip712DomainVersion() {
  return process.env.X402_EIP712_DOMAIN_VERSION?.trim() || getDefaultX402Eip712Domain().version;
}

function toUsdcBaseUnits(amountUsdc: string): string {
  return parseUnits(amountUsdc, 6).toString();
}

function buildAllocateAmountExtra(withReport: boolean, optionId: X402AllocateOptionId): X402PaymentRequirementExtra {
  return {
    optionId,
    withReport,
    name: getX402Eip712DomainName(),
    version: getX402Eip712DomainVersion(),
    executionModelVersion: EXECUTION_MODEL_VERSION,
  };
}

function getX402ToolPriceUsdc(productId: X402ToolProductId): string {
  const config = getConfig();
  switch (productId) {
    case "market_regime":
      return formatUsdcAmount(config.x402MarketRegimePriceUsdc);
    case "policy_envelope":
      return formatUsdcAmount(config.x402PolicyEnvelopePriceUsdc);
    case "asset_scorecard":
      return formatUsdcAmount(config.x402AssetScorecardPriceUsdc);
    case "rebalance":
      return formatUsdcAmount(config.x402RebalancePriceUsdc);
    case "sce_continuity_mode":
      return formatUsdcAmount(config.x402SceContinuityModePriceUsdc);
    case "sce_case_relevance":
      return formatUsdcAmount(config.x402SceCaseRelevancePriceUsdc);
    case "sce_risk_evaluate":
      return formatUsdcAmount(config.x402SceRiskEvaluatePriceUsdc);
  }
}

const X402_PUBLIC_DESCRIPTIONS = {
  allocate:
    "Portfolio allocation engine. Call this when an automated portfolio workflow needs a target crypto allocation for a specified risk tolerance, timeframe, and optional portfolio segment. Returns an accepted allocation job with a statusPath for asynchronous completion.",
  allocateWithReport:
    "Portfolio allocation and report engine. Call this when an automated portfolio workflow needs the target allocation plus a certified decision record for a specified risk tolerance, timeframe, and optional portfolio segment. Returns an accepted allocation job with a statusPath for asynchronous completion.",
  marketRegime:
    "Market regime classifier. Call this before allocation or rebalancing. Returns volatility, liquidity, sentiment, and allocation authorization inputs for automated portfolio workflows, plus the requested portfolio segment context.",
  policyEnvelope:
    "Risk policy engine. Call this when portfolio constraints and segment filters must be computed for a supplied risk tolerance, timeframe, and optional portfolio segment. Returns exposure caps, stablecoin floor, risk budget, authorization constraints, and segment-aware universe guidance.",
  assetScorecard:
    "Asset scoring engine. Call this before asset selection or allocation. Returns liquidity, structural stability, role classification, and composite quality scores for candidate assets, shaped by the requested portfolio segment.",
  rebalance:
    "Portfolio rebalance engine. Call this after allocation or on a monitoring schedule with current holdings. Returns target-vs-current drift and the adjustments required to rebalance within policy constraints for the requested portfolio segment.",
} as const;

const X402_SERVER_METADATA = {
  name: "Selun | Sagitta AAA Portfolio Infrastructure",
  description:
    "Payment-gated x402 endpoints for Sagitta AAA portfolio intelligence and SCE execution preflight. Includes allocation, market regime, policy envelope, asset scoring, rebalance, continuity mode, case relevance, and doctrine-based risk evaluation for autonomous agents.",
  provider: {
    name: "Sagitta AAA",
    url: "https://selun.sagitta.systems",
  },
  discoveryTags: [
    "portfolio",
    "allocation",
    "market-regime",
    "policy-engine",
    "asset-scoring",
    "rebalance",
    "sce",
    "active-intelligence",
    "agent-safety",
    "execution-preflight",
    "defi-safety",
    "doctrine-action",
    "protocol-risk",
    "case-library",
    "x402",
  ],
} as const;

function getX402ToolDefinitions(): X402ToolDefinition[] {
  return [
    {
      productId: "market_regime",
      routePath: "/agent/x402/market-regime",
      title: "Selun Market Regime",
      description: X402_PUBLIC_DESCRIPTIONS.marketRegime,
      category: "finance:market-regime",
      tags: ["portfolio", "market-regime", "volatility", "sentiment", "x402"],
      amountUsdc: () => getX402ToolPriceUsdc("market_regime"),
      inputSchema: {
        type: "object",
        properties: {
          decisionId: { type: "string" },
          riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
          timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
          portfolioSegment: { enum: [...PORTFOLIO_SEGMENTS] },
        },
        required: ["decisionId", "riskTolerance", "timeframe"],
      },
      exampleInput: {
        decisionId: "market-regime-001",
        riskTolerance: "Balanced",
        timeframe: "1-3_years",
        portfolioSegment: "Bluechips",
      },
      exampleOutput: {
        status: "completed",
        endpoint: "/agent/x402/market-regime",
        decisionId: "market-regime-001",
        productId: "market_regime",
        result: {
          regime: "defensive",
          marketCondition: {
            volatility_state: "elevated",
            liquidity_state: "stable",
            risk_appetite: "defensive",
            confidence: 0.74,
          },
        },
      },
    },
    {
      productId: "policy_envelope",
      routePath: "/agent/x402/policy-envelope",
      title: "Selun Policy Envelope",
      description: X402_PUBLIC_DESCRIPTIONS.policyEnvelope,
      category: "finance:policy-envelope",
      tags: ["portfolio", "policy-engine", "risk-budget", "exposure-caps", "x402"],
      amountUsdc: () => getX402ToolPriceUsdc("policy_envelope"),
      inputSchema: {
        type: "object",
        properties: {
          decisionId: { type: "string" },
          riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
          timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
          portfolioSegment: { enum: [...PORTFOLIO_SEGMENTS] },
        },
        required: ["decisionId", "riskTolerance", "timeframe"],
      },
      exampleInput: {
        decisionId: "policy-envelope-001",
        riskTolerance: "Growth",
        timeframe: "3+_years",
        portfolioSegment: "Yield Farm",
      },
      exampleOutput: {
        status: "completed",
        endpoint: "/agent/x402/policy-envelope",
        decisionId: "policy-envelope-001",
        productId: "policy_envelope",
        result: {
          policyMode: "balanced_growth",
          policyEnvelope: {
            risk_budget: 0.62,
            stablecoin_minimum: 0.18,
          },
        },
      },
    },
    {
      productId: "asset_scorecard",
      routePath: "/agent/x402/asset-scorecard",
      title: "Selun Asset Scorecard",
      description: X402_PUBLIC_DESCRIPTIONS.assetScorecard,
      category: "finance:asset-scorecard",
      tags: ["portfolio", "asset-scoring", "liquidity", "quality", "x402"],
      amountUsdc: () => getX402ToolPriceUsdc("asset_scorecard"),
      inputSchema: {
        type: "object",
        properties: {
          decisionId: { type: "string" },
          riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
          timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
          portfolioSegment: { enum: [...PORTFOLIO_SEGMENTS] },
          assets: {
            type: "array",
            items: { type: "string" },
            maxItems: 12,
          },
        },
        required: ["decisionId", "riskTolerance", "timeframe"],
      },
      exampleInput: {
        decisionId: "scorecard-001",
        riskTolerance: "Balanced",
        timeframe: "1-3_years",
        portfolioSegment: "Gaming",
        assets: ["BTC", "ETH", "SOL"],
      },
      exampleOutput: {
        status: "completed",
        endpoint: "/agent/x402/asset-scorecard",
        decisionId: "scorecard-001",
        productId: "asset_scorecard",
        result: {
          scorecards: [
            {
              asset: "BTC",
              role: "core",
              riskClass: "large_cap_crypto",
              compositeScore: 0.91,
            },
          ],
        },
      },
    },
    {
      productId: "rebalance",
      routePath: "/agent/x402/rebalance",
      title: "Selun Rebalance",
      description: X402_PUBLIC_DESCRIPTIONS.rebalance,
      category: "finance:rebalance",
      tags: ["portfolio", "rebalance", "drift-analysis", "allocation-target", "x402"],
      amountUsdc: () => getX402ToolPriceUsdc("rebalance"),
      inputSchema: {
        type: "object",
        properties: {
          decisionId: { type: "string" },
          riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
          timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
          portfolioSegment: { enum: [...PORTFOLIO_SEGMENTS] },
          holdings: {
            type: "array",
            minItems: 1,
            items: {
              type: "object",
              properties: {
                asset: { type: "string" },
                name: { type: "string" },
                usdValue: { type: "number" },
                allocationPct: { type: "number" },
              },
              required: ["asset", "usdValue"],
            },
          },
        },
        required: ["decisionId", "riskTolerance", "timeframe", "holdings"],
      },
      exampleInput: {
        decisionId: "rebalance-001",
        riskTolerance: "Balanced",
        timeframe: "1-3_years",
        portfolioSegment: "Bluechips",
        holdings: [
          { asset: "BTC", usdValue: 4300 },
          { asset: "ETH", usdValue: 3700 },
          { asset: "USDC", usdValue: 2000 },
        ],
      },
      exampleOutput: {
        status: "completed",
        endpoint: "/agent/x402/rebalance",
        decisionId: "rebalance-001",
        productId: "rebalance",
        result: {
          currentPortfolioUsd: 10000,
          recommendations: [
            {
              asset: "BTC",
              currentAllocationPct: 43,
              targetAllocationPct: 36,
              action: "reduce",
            },
          ],
        },
      },
    },
    {
      productId: "sce_continuity_mode",
      routePath: "/agent/x402/sce/continuity-mode",
      title: "Selun SCE Continuity Mode",
      description:
        "Pre-execution safety check. Returns `mode` (NORMAL | INCIDENT | DEGRADED), `recommended_posture`, `reason_codes`, and per-domain `risk_domains`. If mode !== NORMAL or recommended_posture is require_review or pause, call /agent/x402/sce/risk-evaluate before proceeding. Call this before any protocol interaction, routing, rebalance, or capital movement.",
      category: "intelligence:continuity-mode",
      tags: ["sce", "continuity-mode", "active-intelligence", "agent-safety", "execution-preflight", "continuity-intelligence", "doctrine-action", "protocol-risk", "x402"],
      amountUsdc: () => getX402ToolPriceUsdc("sce_continuity_mode"),
      inputSchema: {
        type: "object",
        properties: {
          decisionId: { type: "string" },
          scope: { type: "string", enum: ["global", "chain", "threat_family", "doctrine_tag"] },
          chain_id: { type: "integer" },
          threat_family: { type: "string" },
          doctrine_tag: { type: "string" },
          requested_action: { type: "string" },
        },
        required: ["decisionId"],
      },
      exampleInput: {
        decisionId: "sce-continuity-001",
        scope: "global",
      },
      exampleOutput: {
        status: "completed",
        endpoint: "/agent/x402/sce/continuity-mode",
        decisionId: "sce-continuity-001",
        productId: "sce_continuity_mode",
        result: {
          sce: {
            schema_version: "sce.continuity_mode.v1",
            mode: "INCIDENT",
            recommended_posture: "require_review",
            confidence: { score: 0.85, label: "high", basis: "Pipeline healthy with adequate corpus coverage" },
            reason_codes: ["critical_cases_present", "high_severity_cases_present", "threat_matrix_pressure_elevated"],
            risk_domains: { authority: "warning", governance: "warning", oracle: "normal", bridge: "warning", dependency: "warning", treasury: "unknown", frontend: "warning", keeper: "normal", stablecoin: "warning" },
            recommended_next: { sce_endpoint: "/v1/sce/risk/evaluate", selun_wrapper_path: "/agent/x402/sce/risk-evaluate", reason: "Runtime risk evaluation recommended before protocol-specific execution.", estimated_price_usdc_min: "0.10", estimated_price_usdc_max: "0.50" },
            valid_until: "2026-06-17T11:41:37Z",
          },
          sceSourceEndpoint: "/v1/sce/continuity-mode",
          sceEvaluationId: "sce_continuity_31f0ba501e1849c5",
          validUntil: "2026-06-17T11:41:37Z",
          selunWrapper: true,
        },
      },
    },
    {
      productId: "sce_case_relevance",
      routePath: "/agent/x402/sce/case-relevance",
      title: "Selun SCE Case Relevance",
      description:
        "Classifies whether a protocol or threat family is active in the SCE corpus. Returns `relevance_level` (NONE | LOW | MEDIUM | HIGH), `relevance_score`, `matched_threat_families`, and `matched_doctrine_tags`. Only call when you already have specific threat context to evaluate — not a default preflight. Pass `threat_families`, `doctrine_tags`, or `protocol_name` to get a meaningful result.",
      category: "intelligence:case-relevance",
      tags: ["sce", "case-relevance", "active-intelligence", "agent-safety", "execution-preflight", "case-library", "protocol-risk", "x402"],
      amountUsdc: () => getX402ToolPriceUsdc("sce_case_relevance"),
      inputSchema: {
        type: "object",
        properties: {
          decisionId: { type: "string" },
          protocol_name: { type: "string" },
          chain_id: { type: "integer" },
          asset_types: { type: "array", items: { type: "string" } },
          threat_families: { type: "array", items: { type: "string" } },
          doctrine_tags: { type: "array", items: { type: "string" } },
          requested_action: { type: "string" },
        },
        required: ["decisionId"],
      },
      exampleInput: {
        decisionId: "sce-case-001",
        protocol_name: "Uniswap",
        chain_id: 8453,
        asset_types: ["ERC20", "oracle"],
        threat_families: ["DeFi Protocol Incident", "Admin Key / Access Control"],
        requested_action: "swap",
      },
      exampleOutput: {
        status: "completed",
        endpoint: "/agent/x402/sce/case-relevance",
        decisionId: "sce-case-001",
        productId: "sce_case_relevance",
        result: {
          sce: {
            schema_version: "sce.case_relevance.v1",
            relevance_score: 82,
            relevance_level: "HIGH",
            confidence: { score: 0.78, label: "high", basis: "Matched indexed threat families and doctrine tags" },
            matched_threat_families: ["DeFi Protocol Incident", "Admin Key / Access Control"],
            matched_doctrine_tags: ["emergency_rotation_policy", "user_warning_policy"],
            reason_codes: ["family_match", "doctrine_tag_match", "high_severity_cases_present"],
            valid_until: "2026-06-17T11:56:44Z",
          },
          sceSourceEndpoint: "/v1/sce/case-relevance",
          sceEvaluationId: "sce_relevance_8a66e19f06184e1e",
          validUntil: "2026-06-17T11:56:44Z",
          selunWrapper: true,
        },
      },
    },
    {
      productId: "sce_risk_evaluate",
      routePath: "/agent/x402/sce/risk-evaluate",
      title: "Selun SCE Risk Evaluate",
      description:
        "Protocol risk gate. Returns `doctrine_action` (BLOCK | ALLOW | ESCALATE), `risk_level`, `risk_score`, and per-domain `risk_domains`. If doctrine_action === BLOCK, halt the action and return `reason_codes` and `recommended_next` to the user. Recheck after `valid_until`. Call this when continuity-mode returns mode !== NORMAL or recommended_posture is require_review or pause.",
      category: "intelligence:risk-evaluate",
      tags: ["sce", "risk-evaluate", "active-intelligence", "agent-safety", "execution-preflight", "doctrine-action", "case-library", "protocol-risk", "x402"],
      amountUsdc: () => getX402ToolPriceUsdc("sce_risk_evaluate"),
      inputSchema: {
        type: "object",
        properties: {
          decisionId: { type: "string" },
          protocol_name: { type: "string" },
          chain_id: { type: "integer" },
          addresses: { type: "array", items: { type: "string" } },
          asset_types: { type: "array", items: { type: "string" } },
          threat_families: { type: "array", items: { type: "string" } },
          doctrine_tags: { type: "array", items: { type: "string" } },
          requested_action: { type: "string" },
          risk_tolerance: { type: "string" },
        },
        required: ["decisionId"],
      },
      exampleInput: {
        decisionId: "sce-risk-001",
        protocol_name: "Aave",
        chain_id: 8453,
        addresses: ["0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8"],
        requested_action: "deposit",
      },
      exampleOutput: {
        status: "completed",
        endpoint: "/agent/x402/sce/risk-evaluate",
        decisionId: "sce-risk-001",
        productId: "sce_risk_evaluate",
        result: {
          sce: {
            schema_version: "sce.risk_evaluate.v1",
            doctrine_action: "BLOCK",
            recommended_posture: "pause",
            risk_score: 60,
            risk_level: "HIGH",
            confidence: { score: 0.82, label: "high", basis: "Pipeline healthy; composite evaluation complete" },
            reason_codes: ["critical_cases_present", "high_severity_cases_present", "threat_matrix_pressure_elevated", "doctrine_action_block", "risk_level_high"],
            risk_domains: { authority: "warning", governance: "warning", oracle: "normal", bridge: "warning", dependency: "warning", treasury: "unknown", frontend: "warning", keeper: "normal", stablecoin: "warning" },
            continuity_mode: { mode: "INCIDENT", recommended_posture: "require_review" },
            recommended_next: { sce_endpoint: "/v1/sce/escalation-brief", selun_wrapper_path: "/agent/x402/sce/escalation-brief", reason: "Action blocked. Escalation brief recommended before any protocol interaction.", estimated_price_usdc_min: "1.00", estimated_price_usdc_max: "10.00" },
            valid_until: "2026-06-17T11:41:52Z",
          },
          sceSourceEndpoint: "/v1/sce/risk/evaluate",
          sceEvaluationId: "sce_risk_a1ace5b8f5144d88",
          validUntil: "2026-06-17T11:41:52Z",
          selunWrapper: true,
        },
      },
    },
  ];
}

function getX402ToolDefinition(productId: X402ToolProductId): X402ToolDefinition {
  const definition = getX402ToolDefinitions().find((entry) => entry.productId === productId);
  if (!definition) {
    throw new Error(`Unknown x402 product: ${productId}`);
  }
  return definition;
}

function buildAllocateAccepts(payToAddress: string): X402AllocateAccept[] {
  const config = getConfig();
  const pricing = computePriceContract();
  const maxTimeoutSeconds = getX402MaxTimeoutSeconds();
  const common = {
    scheme: "exact" as const,
    network: config.networkId,
    caip2Network: toCaip2Network(config.networkId) as Network,
    asset: config.usdcContractAddress,
    payTo: payToAddress,
    maxTimeoutSeconds,
  };

  return [
    {
      optionId: "allocation_only",
      amountUsdc: pricing.allocationOnlyUsdc,
      amount: toUsdcBaseUnits(pricing.allocationOnlyUsdc),
      price: toUsdPrice(pricing.allocationOnlyUsdc),
      withReport: false,
      ...common,
    },
    {
      optionId: "allocation_with_report",
      amountUsdc: pricing.allocationWithReportUsdc,
      amount: toUsdcBaseUnits(pricing.allocationWithReportUsdc),
      price: toUsdPrice(pricing.allocationWithReportUsdc),
      withReport: true,
      ...common,
    },
  ];
}

function buildPaymentRequirementsV2(accepts: X402AllocateAccept[]): X402PaymentRequirementV2[] {
  return accepts.map((option) => ({
    scheme: "exact",
    network: option.caip2Network,
    amount: option.amount,
    asset: option.asset,
    payTo: option.payTo,
    maxTimeoutSeconds: option.maxTimeoutSeconds,
    extra: buildAllocateAmountExtra(option.withReport, option.optionId),
  }));
}

function getAllocateAcceptByReport(accepts: X402AllocateAccept[], withReport: boolean): X402AllocateAccept {
  return accepts.find((option) => option.withReport === withReport) ?? accepts[0];
}

function buildRequestScopedPaymentRequirement(params: {
  accept: X402AllocateAccept;
  decisionId: string;
  inputFingerprint: string;
  quoteIssuedAt: string;
  quoteExpiresAt: string;
}): X402PaymentRequirementV2 {
  return {
    scheme: "exact",
    network: params.accept.caip2Network,
    amount: params.accept.amount,
    asset: params.accept.asset,
    payTo: params.accept.payTo,
    maxTimeoutSeconds: params.accept.maxTimeoutSeconds,
    extra: {
      ...buildAllocateAmountExtra(params.accept.withReport, params.accept.optionId),
      decisionId: params.decisionId,
      inputFingerprint: params.inputFingerprint,
      quoteIssuedAt: params.quoteIssuedAt,
      quoteExpiresAt: params.quoteExpiresAt,
    },
  };
}

function buildAllocateDiscoveryExtension() {
  return declareDiscoveryExtension({
    description: X402_PUBLIC_DESCRIPTIONS.allocate,
    bodyType: "json",
    input: {
      decisionId: "agent-run-001",
      riskTolerance: "Balanced",
      timeframe: "1-3_years",
      portfolioSegment: "Bluechips",
      referralCode: "ASH123",
    },
    inputSchema: {
      type: "object",
      properties: {
        decisionId: { type: "string" },
        riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
        timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
        portfolioSegment: { enum: [...PORTFOLIO_SEGMENTS] },
        referralCode: { type: "string", description: "Optional affiliate/referral code. You can also send this as ?ref=CODE." },
      },
      required: ["decisionId", "riskTolerance", "timeframe"],
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

function buildAllocateWithReportDiscoveryExtension() {
  return declareDiscoveryExtension({
    description: X402_PUBLIC_DESCRIPTIONS.allocateWithReport,
    bodyType: "json",
    input: {
      decisionId: "agent-run-001",
      riskTolerance: "Balanced",
      timeframe: "1-3_years",
      portfolioSegment: "Bluechips",
      referralCode: "ASH123",
    },
    inputSchema: {
      type: "object",
      properties: {
        decisionId: { type: "string" },
        riskTolerance: { enum: ["Conservative", "Balanced", "Growth", "Aggressive"] },
        timeframe: { enum: ["<1_year", "1-3_years", "3+_years"] },
        portfolioSegment: { enum: [...PORTFOLIO_SEGMENTS] },
        referralCode: { type: "string", description: "Optional affiliate/referral code. You can also send this as ?ref=CODE." },
      },
      required: ["decisionId", "riskTolerance", "timeframe"],
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

function buildToolDiscoveryExtension(definition: X402ToolDefinition, exampleOutput: Record<string, unknown>) {
  return declareDiscoveryExtension({
    description: definition.description,
    bodyType: "json",
    input: definition.exampleInput,
    inputSchema: definition.inputSchema,
    output: {
      example: {
        success: true,
        executionModelVersion: EXECUTION_MODEL_VERSION,
        data: exampleOutput,
      },
    },
  });
}

function attachBazaarDiscovery(res: Response, extension?: Record<string, unknown>) {
  try {
    const ext = extension ?? buildAllocateDiscoveryExtension();
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

function isEmptyObject(value: unknown): boolean {
  return isRecord(value) && Object.keys(value).length === 0;
}

function buildAllocateChallengeBody(params: {
  inputs: AllocateInputShape;
  routePath: string;
  decisionId: string;
  chargeAmountUsdc: string;
  accepts: X402AllocateAccept[];
  selectedAccept: X402AllocateAccept;
  selectedRequirement: X402PaymentRequirementV2;
  quoteExpiresAt: string;
  quoteIssuedAt: string;
}): Record<string, unknown> {
  return {
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: "x402_payment_required",
    x402: {
      endpoint: params.routePath,
      transport: {
        version: 2,
        paymentRequiredHeader: "PAYMENT-REQUIRED",
        paymentSignatureHeader: "PAYMENT-SIGNATURE",
        paymentResponseHeader: "PAYMENT-RESPONSE",
        facilitatorUrl: getX402FacilitatorUrl(),
      },
      amountUsdc: params.chargeAmountUsdc,
      selectedOptionId: params.selectedAccept.optionId,
      accepts: params.accepts,
      paymentRequirementsV2: [params.selectedRequirement],
      decisionId: params.decisionId,
      withReport: params.inputs.withReport,
      quoteIssuedAt: params.quoteIssuedAt,
      quoteExpiresAt: params.quoteExpiresAt,
    },
    message: "Payment required. Retry the same request with a PAYMENT-SIGNATURE header created from PAYMENT-REQUIRED.",
    logs: getExecutionLogs(120),
  };
}

function buildAllocateRouteConfig(
  req: Request,
  requirement: X402PaymentRequirementV2,
  challengeBody: Record<string, unknown>,
  routePath: string,
  description: string,
  discoveryExtension: Record<string, unknown>,
): RouteConfig {
  return {
    accepts: {
      scheme: requirement.scheme,
      payTo: requirement.payTo,
      price: {
        amount: requirement.amount,
        asset: requirement.asset,
      },
      network: requirement.network,
      maxTimeoutSeconds: requirement.maxTimeoutSeconds,
      extra: requirement.extra,
    },
    resource: buildAbsoluteResourceUrl(req, routePath),
    description,
    mimeType: "application/json",
    extensions: {
      ...discoveryExtension,
    },
    unpaidResponseBody: async () => ({
      contentType: "application/json",
      body: challengeBody,
    }),
  };
}

function buildAllocateHttpContext(req: Request): HTTPRequestContext {
  return {
    adapter: new ExpressAdapter(req),
    path: `${req.baseUrl || ""}${req.path || ""}` || "/agent/x402/allocate",
    method: req.method,
  };
}

function applyResponseHeaders(res: Response, headers: Record<string, string>) {
  for (const [name, value] of Object.entries(headers)) {
    res.setHeader(name, value);
  }
}

function sendX402HttpResponse(
  res: Response,
  response: {
    status: number;
    headers: Record<string, string>;
    body?: unknown;
    isHtml?: boolean;
  },
  fallbackBody?: Record<string, unknown>,
) {
  applyResponseHeaders(res, response.headers);
  if (response.isHtml) {
    return res.status(response.status).send(typeof response.body === "string" ? response.body : "");
  }

  const body = response.body === undefined || isEmptyObject(response.body)
    ? (fallbackBody ?? response.body ?? {})
    : response.body;
  return res.status(response.status).json(body);
}

async function getX402SellerServer(): Promise<x402ResourceServer> {
  if (x402SellerServer) return x402SellerServer;
  if (!x402SellerServerInitPromise) {
    x402SellerServerInitPromise = (async () => {
      const server = new x402ResourceServer(createX402FacilitatorClient());
      server.register(toCaip2Network(getConfig().networkId) as Network, new ExactEvmScheme());
      server.registerExtension(bazaarResourceServerExtension);
      await server.initialize();
      x402SellerServer = server;
      return server;
    })().catch((error) => {
      x402SellerServerInitPromise = null;
      throw error;
    });
  }
  return x402SellerServerInitPromise;
}

async function createAllocateX402HttpServer(
  req: Request,
  requirement: X402PaymentRequirementV2,
  challengeBody: Record<string, unknown>,
  routePath: string,
  description: string,
  discoveryExtension: Record<string, unknown>,
) {
  const server = await getX402SellerServer();
  return new x402HTTPResourceServer(
    server,
    {
      [`POST ${routePath}`]: buildAllocateRouteConfig(req, requirement, challengeBody, routePath, description, discoveryExtension),
    },
  );
}

function buildToolRequirement(productId: X402ToolProductId, payToAddress: string, decisionId: string, inputFingerprint: string, quoteIssuedAt: string, quoteExpiresAt: string): X402ToolPaymentRequirementV2 {
  return {
    scheme: "exact",
    network: toCaip2Network(getConfig().networkId) as Network,
    amount: toUsdcBaseUnits(getX402ToolPriceUsdc(productId)),
    asset: getConfig().usdcContractAddress,
    payTo: payToAddress,
    maxTimeoutSeconds: getX402MaxTimeoutSeconds(),
    extra: {
      productId,
      name: getX402Eip712DomainName(),
      version: getX402Eip712DomainVersion(),
      executionModelVersion: EXECUTION_MODEL_VERSION,
      decisionId,
      inputFingerprint,
      quoteIssuedAt,
      quoteExpiresAt,
    },
  };
}

function buildToolPreviewRequirement(productId: X402ToolProductId, payToAddress: string): X402ToolPaymentRequirementV2 {
  return {
    scheme: "exact",
    network: toCaip2Network(getConfig().networkId) as Network,
    amount: toUsdcBaseUnits(getX402ToolPriceUsdc(productId)),
    asset: getConfig().usdcContractAddress,
    payTo: payToAddress,
    maxTimeoutSeconds: getX402MaxTimeoutSeconds(),
    extra: {
      productId,
      name: getX402Eip712DomainName(),
      version: getX402Eip712DomainVersion(),
      executionModelVersion: EXECUTION_MODEL_VERSION,
    },
  };
}

function buildToolChallengeBody(params: {
  definition: X402ToolDefinition;
  decisionId: string;
  requirement: X402ToolPaymentRequirementV2;
  quoteIssuedAt: string;
  quoteExpiresAt: string;
}): Record<string, unknown> {
  const priceUsdc = getX402ToolPriceUsdc(params.definition.productId);
  return {
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: "x402_payment_required",
    x402: {
      endpoint: params.definition.routePath,
      productId: params.definition.productId,
      transport: {
        version: 2,
        paymentRequiredHeader: "PAYMENT-REQUIRED",
        paymentSignatureHeader: "PAYMENT-SIGNATURE",
        paymentResponseHeader: "PAYMENT-RESPONSE",
        facilitatorUrl: getX402FacilitatorUrl(),
      },
      amountUsdc: priceUsdc,
      paymentRequirementsV2: [params.requirement],
      decisionId: params.decisionId,
      quoteIssuedAt: params.quoteIssuedAt,
      quoteExpiresAt: params.quoteExpiresAt,
    },
    message: "Payment required. Retry the same request with a PAYMENT-SIGNATURE header created from PAYMENT-REQUIRED.",
    logs: getExecutionLogs(120),
  };
}

function buildSolanaToolRequirement(productId: X402ToolProductId): { scheme: string; payTo: string; price: { amount: string; asset: string }; network: Network; maxTimeoutSeconds: number } | null {
  const solanaAddress = getConfig().treasurySolanaAddress;
  if (!solanaAddress || !validateSvmAddress(solanaAddress)) return null;
  return {
    scheme: "exact",
    payTo: solanaAddress,
    price: {
      amount: toUsdcBaseUnits(getX402ToolPriceUsdc(productId)),
      asset: USDC_SOLANA_MAINNET_ADDRESS,
    },
    network: SOLANA_MAINNET_CAIP2 as Network,
    maxTimeoutSeconds: getX402MaxTimeoutSeconds(),
  };
}

function buildToolRouteConfig(
  req: Request,
  definition: X402ToolDefinition,
  requirement: X402ToolPaymentRequirementV2,
  challengeBody: Record<string, unknown>,
  discoveryExtension: Record<string, unknown>,
): RouteConfig {
  const evmOption = {
    scheme: requirement.scheme,
    payTo: requirement.payTo,
    price: {
      amount: requirement.amount,
      asset: requirement.asset,
    },
    network: requirement.network,
    maxTimeoutSeconds: requirement.maxTimeoutSeconds,
    extra: requirement.extra,
  };
  const solanaOption = buildSolanaToolRequirement(definition.productId);
  return {
    accepts: solanaOption ? [evmOption, solanaOption] : evmOption,
    resource: buildAbsoluteResourceUrl(req, definition.routePath),
    description: definition.description,
    mimeType: "application/json",
    extensions: {
      ...discoveryExtension,
    },
    unpaidResponseBody: async () => ({
      contentType: "application/json",
      body: challengeBody,
    }),
  };
}

async function createToolX402HttpServer(
  req: Request,
  definition: X402ToolDefinition,
  requirement: X402ToolPaymentRequirementV2,
  challengeBody: Record<string, unknown>,
  discoveryExtension: Record<string, unknown>,
) {
  const server = await getX402SellerServer();
  return new x402HTTPResourceServer(
    server,
    {
      [`POST ${definition.routePath}`]: buildToolRouteConfig(req, definition, requirement, challengeBody, discoveryExtension),
    },
  );
}

function buildToolStateKey(productId: X402ToolProductId, decisionId: string): string {
  return `${productId}:${decisionId}`;
}

function buildToolJobId(productId: X402ToolProductId, decisionId: string): string {
  return `selun-${productId}-${decisionId}-${Date.now()}`.replace(/[^a-zA-Z0-9-_]/g, "-");
}

function computeToolInputFingerprint(value: Record<string, unknown>): string {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function getStoredToolResponse(productId: X402ToolProductId, decisionId: string): StoredToolResponseData | null {
  const record = getX402StateStore().getToolRecord(productId, decisionId);
  if (!record?.responseData) return null;
  const payment = record.payment;
  if (!payment) return null;
  return {
    status: "completed",
    endpoint: getX402ToolDefinition(productId).routePath,
    decisionId,
    productId,
    payment: {
      required: true,
      chargedAmountUsdc: record.chargedAmountUsdc,
      verified: true,
      fromAddress: payment.fromAddress,
      transactionHash: payment.transactionHash,
      network: payment.network ?? null,
    },
    result: record.responseData,
  };
}

function idempotentToolResponse(res: Response, productId: X402ToolProductId, decisionId: string) {
  const stored = getStoredToolResponse(productId, decisionId);
  if (!stored) {
    return failure(res, new Error("Stored x402 result is unavailable."), 500);
  }

  return res.status(200).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    data: {
      ...stored,
      idempotentReplay: true,
    },
    logs: getExecutionLogs(120),
  });
}

function normalizeStringArray(value: unknown, maxItems: number): string[] {
  if (!Array.isArray(value)) return [];
  const values: string[] = [];
  for (const entry of value) {
    const normalized = toText(entry, "").toUpperCase();
    if (!normalized) continue;
    if (!values.includes(normalized)) values.push(normalized);
    if (values.length >= maxItems) break;
  }
  return values;
}

function normalizeRebalanceHoldings(value: unknown): RebalanceHolding[] | null {
  if (!Array.isArray(value) || value.length === 0) return null;
  const holdings: RebalanceHolding[] = [];
  for (const entry of value) {
    if (!isRecord(entry)) return null;
    const asset = toText(entry.asset, "").toUpperCase();
    const name = toText(entry.name, "");
    const usdValue = toFiniteNumber(entry.usdValue);
    const allocationPct = toFiniteNumber(entry.allocationPct);
    if (!asset || usdValue === null || usdValue < 0) return null;
    holdings.push({
      asset,
      name: name || null,
      usdValue,
      allocationPct: allocationPct === null ? null : Math.max(0, Math.min(100, allocationPct)),
    });
  }
  return holdings;
}

function applyStoredPaymentResponseHeader(res: Response, record: X402AllocateRecord) {
  if (!record.payment?.transactionHash) return;

  res.setHeader(
    "PAYMENT-RESPONSE",
    encodePaymentResponseHeader({
      success: true,
      payer: record.payment.fromAddress,
      transaction: record.payment.transactionHash,
      network: (record.payment.network || toCaip2Network(getConfig().networkId)) as Network,
    }),
  );
}

function applySettlementResponseHeaders(res: Response, settlement: ProcessSettleSuccessResponse) {
  applyResponseHeaders(res, settlement.headers);
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
  const endpoint = record.inputs.withReport ? "/agent/x402/allocate-with-report" : "/agent/x402/allocate";
  applyStoredPaymentResponseHeader(res, record);

  return res.status(complete ? 200 : 202).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    data: {
      status: complete ? "already_complete" : "already_accepted",
      idempotentReplay: true,
      endpoint,
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

function requireJobContext(jobId: string) {
  const status = getExecutionStatus(jobId);
  if (!status.found || !status.jobContext) {
    throw new Error("job not found.");
  }
  return status.jobContext;
}

async function ensurePhase2Outputs(jobId: string): Promise<ReturnType<typeof requireJobContext>> {
  await waitForPhaseResult(jobId, "phase2");
  const context = requireJobContext(jobId);
  if (!context.phase1.output || !context.phase2.output) {
    throw new Error("Phase 2 outputs are unavailable.");
  }
  return context;
}

async function ensurePhase5Outputs(jobId: string): Promise<ReturnType<typeof requireJobContext>> {
  await waitForPhaseResult(jobId, "phase2");
  runPhase3(jobId);
  await waitForPhaseResult(jobId, "phase3");
  runPhase4(jobId);
  await waitForPhaseResult(jobId, "phase4");
  runPhase5(jobId);
  await waitForPhaseResult(jobId, "phase5");
  const context = requireJobContext(jobId);
  if (!context.phase5.output) {
    throw new Error("Phase 5 outputs are unavailable.");
  }
  return context;
}

async function ensurePhase6Outputs(jobId: string): Promise<ReturnType<typeof requireJobContext>> {
  const context = await ensurePhase5Outputs(jobId);
  runPhase6(jobId);
  await waitForPhaseResult(jobId, "phase6");
  const latest = requireJobContext(jobId);
  if (!latest.phase6.output) {
    throw new Error("Phase 6 outputs are unavailable.");
  }
  return latest;
}

function mapMarketRegimeLabel(context: ReturnType<typeof requireJobContext>): string {
  const phase1 = context.phase1.output;
  const phase2 = context.phase2.output;
  if (!phase1 || !phase2) return "unavailable";
  const appetite = phase1.market_condition.risk_appetite;
  const mode = phase2.allocation_policy.mode;
  if (appetite === "defensive" || mode === "capital_preservation" || mode === "balanced_defensive") return "defensive";
  if (appetite === "expansionary" || mode === "offensive_growth") return "risk_on";
  return "balanced";
}

async function buildMarketRegimeResult(jobId: string): Promise<Record<string, unknown>> {
  const context = await ensurePhase2Outputs(jobId);
  const phase1 = context.phase1.output!;
  const phase2 = context.phase2.output!;
  return {
    snapshotAt: phase1.timestamp,
    regime: mapMarketRegimeLabel(context),
    portfolioSegment: phase2.inputs.user_profile.portfolio_segment,
    marketCondition: phase1.market_condition,
    authorization: phase1.allocation_authorization,
    policyMode: phase2.allocation_policy.mode,
    sourceSummary: {
      sourceCount: phase1.audit.sources.length,
      dataFreshness: phase1.audit.data_freshness,
      missingDomains: phase1.audit.missing_domains,
    },
  };
}

async function buildPolicyEnvelopeResult(jobId: string): Promise<Record<string, unknown>> {
  const context = await ensurePhase2Outputs(jobId);
  const phase2 = context.phase2.output!;
  return {
    snapshotAt: phase2.timestamp,
    portfolioSegment: phase2.inputs.user_profile.portfolio_segment,
    policyMode: phase2.allocation_policy.mode,
    defensiveBiasAdjustment: phase2.allocation_policy.defensive_bias_adjustment,
    policyEnvelope: phase2.policy_envelope,
    authorization: phase2.allocation_authorization,
    audit: {
      phase1TimestampRef: phase2.audit.phase1_timestamp_ref,
      policyRulesApplied: phase2.audit.policy_rules_applied,
      agentJudgement: phase2.audit.agent_judgement,
    },
  };
}

async function buildAssetScorecardResult(jobId: string, input: AssetScorecardInput): Promise<Record<string, unknown>> {
  const context = await ensurePhase5Outputs(jobId);
  const phase5 = context.phase5.output!;
  const filterAssets = new Set(input.assets.map((asset) => asset.toUpperCase()));
  const scored = phase5.evaluation.tokens
    .filter((token) => filterAssets.size === 0 || filterAssets.has(token.symbol.toUpperCase()) || filterAssets.has(token.coingecko_id.toUpperCase()))
    .sort((left, right) => right.composite_score - left.composite_score || right.quality_score - left.quality_score)
    .slice(0, filterAssets.size > 0 ? 12 : 8)
    .map((token) => ({
      asset: token.symbol,
      name: token.name,
      coingeckoId: token.coingecko_id,
      role: token.role,
      riskClass: token.risk_class,
      selectionBucket: token.selection_bucket,
      selected: token.selected,
      qualityScore: token.quality_score,
      riskScore: token.risk_score,
      liquidityScore: token.liquidity_score,
      structuralScore: token.structural_score,
      compositeScore: token.composite_score,
      selectionReasons: token.selection_reasons,
    }));

  return {
    snapshotAt: phase5.timestamp,
    portfolioSegment: phase5.inputs.user_profile.portfolio_segment,
    screenedCandidatesCount: phase5.evaluation.screened_candidates_count,
    qualifiedCandidatesCount: phase5.evaluation.qualified_candidates_count,
    selectedCandidatesCount: phase5.evaluation.selected_candidates_count,
    assetsRequested: input.assets,
    scorecards: scored,
  };
}

async function buildRebalanceResult(jobId: string, input: RebalanceInput): Promise<Record<string, unknown>> {
  const context = await ensurePhase6Outputs(jobId);
  const phase2 = context.phase2.output!;
  const phase6 = context.phase6.output!;
  const holdings = input.holdings.map((holding) => ({ ...holding }));
  const totalUsd = holdings.reduce((sum, holding) => sum + holding.usdValue, 0);
  const normalizedHoldings = holdings.map((holding) => ({
    ...holding,
    allocationPct: holding.allocationPct ?? (totalUsd > 0 ? (holding.usdValue / totalUsd) * 100 : 0),
  }));

  const currentByAsset = new Map(normalizedHoldings.map((holding) => [holding.asset.toUpperCase(), holding]));
  const recommendations = phase6.allocation.allocations.map((allocation) => {
    const current = currentByAsset.get(allocation.symbol.toUpperCase());
    const targetPct = Number((allocation.allocation_weight * 100).toFixed(2));
    const currentPct = Number(((current?.allocationPct ?? 0)).toFixed(2));
    const deltaPct = Number((targetPct - currentPct).toFixed(2));
    const action = deltaPct > 1 ? "increase" : deltaPct < -1 ? "reduce" : "hold";
    return {
      asset: allocation.symbol,
      name: allocation.name,
      bucket: allocation.bucket,
      currentAllocationPct: currentPct,
      targetAllocationPct: targetPct,
      deltaPct,
      action,
    };
  });

  const exits = normalizedHoldings
    .filter((holding) => !phase6.allocation.allocations.some((allocation) => allocation.symbol.toUpperCase() === holding.asset.toUpperCase()))
    .map((holding) => ({
      asset: holding.asset,
      name: holding.name,
      currentAllocationPct: Number((holding.allocationPct ?? 0).toFixed(2)),
      targetAllocationPct: 0,
      deltaPct: Number((0 - (holding.allocationPct ?? 0)).toFixed(2)),
      action: "reduce",
    }));

  return {
    snapshotAt: phase6.timestamp,
    currentPortfolioUsd: Number(totalUsd.toFixed(2)),
    portfolioSegment: phase6.inputs.user_profile.portfolio_segment,
    policyMode: phase2.allocation_policy.mode,
    policyEnvelope: phase2.policy_envelope,
    targetPortfolio: {
      stablecoinAllocation: phase6.allocation.stablecoin_allocation,
      expectedPortfolioVolatility: phase6.allocation.expected_portfolio_volatility,
      concentrationIndex: phase6.allocation.concentration_index,
    },
    recommendations: [...recommendations, ...exits],
  };
}

function success<T>(res: Response, data: T) {
  return res.status(200).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    data,
    logs: getExecutionLogs(120),
  });
}

function failure(res: Response, error: unknown, status = 500, details?: Record<string, unknown>) {
  const message = error instanceof Error ? error.message : "Unexpected error.";
  return res.status(status).json({
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: message,
    ...(details ? { details } : {}),
    logs: getExecutionLogs(120),
  });
}

async function buildX402CapabilitiesData() {
  const identity = await getAgentAddress();
  const config = getConfig();
  const accepts = buildAllocateAccepts(identity.walletAddress);
  const allocationOnlyAccept = getAllocateAcceptByReport(accepts, false);
  const allocationWithReportAccept = getAllocateAcceptByReport(accepts, true);
  const toolDefinitions = getX402ToolDefinitions();
  const resources = [
    {
      endpoint: "/agent/x402/allocate",
      method: "POST",
      productId: "allocate",
      title: "Selun Allocation",
      description: X402_PUBLIC_DESCRIPTIONS.allocate,
      pricing: {
        amountUsdc: allocationOnlyAccept.amountUsdc,
        price: allocationOnlyAccept.price,
      },
      accepts: [allocationOnlyAccept],
      paymentRequirementsV2: buildPaymentRequirementsV2([allocationOnlyAccept]),
      inputSchema: buildAllocateDiscoveryExtension().inputSchema,
    },
    {
      endpoint: "/agent/x402/allocate-with-report",
      method: "POST",
      productId: "allocate_with_report",
      title: "Selun Allocation With Report",
      description: X402_PUBLIC_DESCRIPTIONS.allocateWithReport,
      pricing: {
        amountUsdc: allocationWithReportAccept.amountUsdc,
        price: allocationWithReportAccept.price,
      },
      accepts: [allocationWithReportAccept],
      paymentRequirementsV2: buildPaymentRequirementsV2([allocationWithReportAccept]),
      inputSchema: buildAllocateWithReportDiscoveryExtension().inputSchema,
    },
    ...toolDefinitions.map((definition) => {
      const requirement = buildToolPreviewRequirement(definition.productId, identity.walletAddress);
      return {
        endpoint: definition.routePath,
        method: "POST",
        productId: definition.productId,
        title: definition.title,
        description: definition.description,
        category: definition.category,
        tags: definition.tags,
        pricing: {
          amountUsdc: definition.amountUsdc(),
          price: toUsdPrice(definition.amountUsdc()),
        },
        paymentRequirementsV2: [requirement],
        inputSchema: definition.inputSchema,
        ...(definition.exampleInput !== undefined ? { exampleInput: definition.exampleInput } : {}),
        ...(definition.exampleOutput !== undefined ? { exampleOutput: definition.exampleOutput } : {}),
      };
    }),
  ];

  return {
    discoverable: true,
    name: X402_SERVER_METADATA.name,
    description: X402_SERVER_METADATA.description,
    provider: X402_SERVER_METADATA.provider,
    x402Version: 2,
    versions: {
      executionModelVersion: EXECUTION_MODEL_VERSION,
    },
    pricing: {
      ...computePriceContract(),
      marketRegimeUsdc: getX402ToolPriceUsdc("market_regime"),
      policyEnvelopeUsdc: getX402ToolPriceUsdc("policy_envelope"),
      assetScorecardUsdc: getX402ToolPriceUsdc("asset_scorecard"),
      rebalanceUsdc: getX402ToolPriceUsdc("rebalance"),
      sceContinuityModeUsdc: getX402ToolPriceUsdc("sce_continuity_mode"),
      sceCaseRelevanceUsdc: getX402ToolPriceUsdc("sce_case_relevance"),
      sceRiskEvaluateUsdc: getX402ToolPriceUsdc("sce_risk_evaluate"),
    },
    resources,
    paymentTransport: {
      facilitatorUrl: getX402FacilitatorUrl(),
      headers: {
        paymentRequired: "PAYMENT-REQUIRED",
        paymentSignature: "PAYMENT-SIGNATURE",
        paymentResponse: "PAYMENT-RESPONSE",
      },
      requestScopedRequirements: {
        boundFields: ["decisionId", "quoteExpiresAt", "request input fingerprint"],
        mismatchBehavior: "server returns 402 with a fresh PAYMENT-REQUIRED challenge",
      },
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
      facilitator: getX402FacilitatorUrl(),
      transportVersion: 2,
      network: config.networkId,
      caip2Network: toCaip2Network(config.networkId),
      metadataRefreshCadenceHours: 6,
      category: "agent-safety:defi-preflight",
      categories: ["agent-safety:defi-preflight", "finance:portfolio-agent"],
      tags: [...X402_SERVER_METADATA.discoveryTags],
    },
    agentPolicy: {
      name: "SCE Preflight Policy",
      intent: "Before autonomous capital movement or protocol interaction, perform a continuity check and gate execution on doctrine_action.",
      defaultFirstCall: "/agent/x402/sce/continuity-mode",
      escalationCall: "/agent/x402/sce/risk-evaluate",
      haltCondition: "doctrine_action === 'BLOCK'",
      recheckRule: "Recheck after valid_until.",
      agent_preflight_policy: "SCE continuity-mode is the default pre-execution check. SCE risk-evaluate is the action gate.",
      recommended_call_order: [
        {
          step: 1,
          endpoint: "/agent/x402/sce/continuity-mode",
          productId: "sce_continuity_mode",
          when: "Before any protocol interaction, routing, rebalance, allocation, or capital movement.",
          escalate_if: "mode !== 'NORMAL' || recommended_posture === 'require_review' || recommended_posture === 'pause'",
        },
        {
          step: 2,
          endpoint: "/agent/x402/sce/risk-evaluate",
          productId: "sce_risk_evaluate",
          when: "When continuity-mode returns mode !== NORMAL or recommended_posture is require_review or pause.",
          halt_if: "doctrine_action === 'BLOCK'",
          return_on_halt: ["reason_codes", "risk_domains", "recommended_next", "valid_until"],
        },
        {
          step: 3,
          endpoint: "/agent/x402/sce/case-relevance",
          productId: "sce_case_relevance",
          when: "Only when the agent already has threat context: asset categories, doctrine tags, or specific protocol context to evaluate.",
        },
      ],
      sce_policy_snippet: [
        "Before any protocol interaction, routing, rebalance, allocation, or capital movement:",
        "1. Call /agent/x402/sce/continuity-mode.",
        "   If mode !== 'NORMAL' or recommended_posture is 'require_review' or 'pause', proceed to step 2.",
        "2. Call /agent/x402/sce/risk-evaluate.",
        "   If doctrine_action === 'BLOCK', halt the action.",
        "   Return reason_codes, risk_domains, recommended_next, and valid_until to the user.",
        "   Recheck after valid_until.",
        "Use /agent/x402/sce/case-relevance only when you already have threat context,",
        "asset categories, doctrine tags, or protocol context to evaluate.",
      ].join("\n"),
      failure_handling: {
        sce_upstream_error: "Log the error. Do not block execution solely on SCE unavailability unless policy requires it.",
        sce_upstream_timeout: "Treat as degraded. Proceed with caution or surface to user.",
        valid_until_expired: "Recheck: re-call the same endpoint before acting.",
      },
    },
  };
}

export async function getX402CapabilitiesData() {
  return buildX402CapabilitiesData();
}

function getToolTitle(productId: X402ToolProductId): string {
  return getX402ToolDefinitions().find((definition) => definition.productId === productId)?.title ?? toTitleCase(productId);
}

function getToolRoutePath(productId: X402ToolProductId): string {
  return getX402ToolDefinitions().find((definition) => definition.productId === productId)?.routePath ?? `/agent/x402/${productId.replaceAll("_", "-")}`;
}

function buildAllocatePurchaseRow(record: X402AllocateRecord): AdminPurchaseRow | null {
  if (record.state !== "accepted" || !record.payment?.transactionHash || !record.payment.fromAddress) {
    return null;
  }

  const endpoint = record.inputs.withReport ? "/agent/x402/allocate-with-report" : "/agent/x402/allocate";
  return {
    purchaseKey: `allocate:${record.decisionId}`,
    kind: "allocate",
    endpoint,
    title: record.inputs.withReport ? "Selun Allocation With Report" : "Selun Allocation",
    decisionId: record.decisionId,
    productId: record.inputs.withReport ? "allocate_with_report" : "allocate",
    chargedAmountUsdc: record.chargedAmountUsdc,
    fromAddress: record.payment.fromAddress,
    paymentTransactionHash: record.payment.transactionHash,
    paymentNetwork: record.payment.network ?? null,
    purchasedAt: record.payment.verifiedAt,
    refunded: Boolean(record.refund),
    refund: record.refund
      ? {
        refundedAt: record.refund.refundedAt,
        transactionHash: record.refund.transactionHash,
        toAddress: record.refund.toAddress,
        amountUsdc: record.refund.amountUsdc,
        note: record.refund.note ?? null,
      }
      : null,
  };
}

function buildToolPurchaseRow(record: X402ToolRecord): AdminPurchaseRow | null {
  if (record.state !== "accepted" || !record.payment?.transactionHash || !record.payment.fromAddress) {
    return null;
  }

  return {
    purchaseKey: `tool:${record.productId}:${record.decisionId}`,
    kind: "tool",
    endpoint: getToolRoutePath(record.productId),
    title: getToolTitle(record.productId),
    decisionId: record.decisionId,
    productId: record.productId,
    chargedAmountUsdc: record.chargedAmountUsdc,
    fromAddress: record.payment.fromAddress,
    paymentTransactionHash: record.payment.transactionHash,
    paymentNetwork: record.payment.network ?? null,
    purchasedAt: record.payment.verifiedAt,
    refunded: Boolean(record.refund),
    refund: record.refund
      ? {
        refundedAt: record.refund.refundedAt,
        transactionHash: record.refund.transactionHash,
        toAddress: record.refund.toAddress,
        amountUsdc: record.refund.amountUsdc,
        note: record.refund.note ?? null,
      }
      : null,
  };
}

function listAdminPurchases(): AdminPurchaseRow[] {
  const stateStore = getX402StateStore();
  const purchases = [
    ...stateStore.listAllocateRecords().map(buildAllocatePurchaseRow),
    ...stateStore.listToolRecords().map(buildToolPurchaseRow),
  ].filter((row): row is AdminPurchaseRow => Boolean(row));

  return purchases.sort((left, right) => right.purchasedAt.localeCompare(left.purchasedAt));
}

function normalizeRefundReferences(value: unknown): AdminRefundReference[] {
  if (!Array.isArray(value)) return [];
  const refs: AdminRefundReference[] = [];

  for (const entry of value) {
    if (!isRecord(entry)) continue;
    const kind = entry.kind === "allocate" || entry.kind === "tool" ? entry.kind : null;
    const decisionId = normalizeDecisionId(typeof entry.decisionId === "string" ? entry.decisionId : undefined);
    const productId =
      entry.productId === "market_regime" ||
      entry.productId === "policy_envelope" ||
      entry.productId === "asset_scorecard" ||
      entry.productId === "rebalance"
        ? entry.productId
        : undefined;

    if (!kind || !decisionId) continue;
    if (kind === "tool" && !productId) continue;

    const duplicate = refs.some((ref) =>
      ref.kind === kind &&
      ref.decisionId === decisionId &&
      (kind === "allocate" || ref.productId === productId)
    );
    if (duplicate) continue;

    refs.push({
      kind,
      decisionId,
      ...(productId ? { productId } : {}),
    });
  }

  return refs;
}

function sumPurchaseAmountBaseUnits(purchases: AdminPurchaseRow[]): bigint {
  return purchases.reduce((sum, purchase) => sum + parseUnits(purchase.chargedAmountUsdc, 6), 0n);
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

function sendToolConflict(
  res: Response,
  definition: X402ToolDefinition,
  decisionId: string,
) {
  return res.status(409).json({
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: "decisionId already exists for this x402 product with different inputs.",
    data: {
      endpoint: definition.routePath,
      decisionId,
      productId: definition.productId,
    },
    logs: getExecutionLogs(120),
  });
}

async function executeToolProduct(productId: X402ToolProductId, jobId: string, input: X402ToolBaseInput | AssetScorecardInput | RebalanceInput): Promise<Record<string, unknown>> {
  if (productId === "market_regime") {
    return buildMarketRegimeResult(jobId);
  }
  if (productId === "policy_envelope") {
    return buildPolicyEnvelopeResult(jobId);
  }
  if (productId === "asset_scorecard") {
    return buildAssetScorecardResult(jobId, input as AssetScorecardInput);
  }
  return buildRebalanceResult(jobId, input as RebalanceInput);
}

type X402ToolRequestOptions = {
  skipPhase1?: boolean;
  usageChannel?: AdminUsageChannel;
};

async function handleX402ToolRequest(
  req: Request,
  res: Response,
  productId: X402ToolProductId,
  normalizedInput: Record<string, unknown>,
  execute: (jobId: string) => Promise<Record<string, unknown>>,
  options?: X402ToolRequestOptions,
) {
  const burst = enforceIpBurstLimit(req);
  if (burst.limited) {
    return sendRateLimited(res, "ip_burst_limit", burst.retryAfterSeconds);
  }

  const definition = getX402ToolDefinition(productId);
  const paymentSignatureHeader = req.header("payment-signature")?.trim() || req.header("PAYMENT-SIGNATURE")?.trim();
  const hasPaymentSignature = Boolean(paymentSignatureHeader);
  const decisionResolution = resolveDecisionId(req);
  const decisionId = decisionResolution.decisionId;
  const missingDecisionIdOnlyError = !decisionId && decisionResolution.error?.includes("decisionId is required");
  if (decisionResolution.error && !missingDecisionIdOnlyError) {
    return failure(res, new Error(decisionResolution.error ?? "decisionId is required."), 400);
  }
  if (hasPaymentSignature && !decisionId) {
    return failure(res, new Error("decisionId is required."), 400);
  }

  const inputFingerprint = computeToolInputFingerprint(normalizedInput);
  const stateStore = getX402StateStore();
  const existingRecord = decisionId ? stateStore.getToolRecord(productId, decisionId) : undefined;
  if (existingRecord?.state === "accepted" && existingRecord.responseData) {
    return idempotentToolResponse(res, productId, decisionId as string);
  }
  if (existingRecord && existingRecord.inputFingerprint !== inputFingerprint) {
    return sendToolConflict(res, definition, decisionId as string);
  }

  if (runningAllocateOrchestration.size >= getX402GlobalConcurrencyCap()) {
    return sendRateLimited(res, "global_concurrency_cap", 10);
  }

  const orchestrationKey = buildToolStateKey(productId, decisionId ?? `probe-${Date.now()}`);
  if (runningAllocateOrchestration.has(orchestrationKey)) {
    return sendRateLimited(res, "global_concurrency_cap", 10);
  }
  runningAllocateOrchestration.add(orchestrationKey);

  try {
    const quoteDecisionId = decisionId ?? `quote-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const activeQuoteWindow = existingRecord?.state === "quoted" && !isExpiredIsoTimestamp(existingRecord.quoteExpiresAt)
      ? {
        issuedAt: existingRecord.quoteIssuedAt,
        expiresAt: existingRecord.quoteExpiresAt,
      }
      : createAllocateQuoteWindow();

    const identity = await getAgentAddress();
    const requirement = buildToolRequirement(
      productId,
      identity.walletAddress,
      quoteDecisionId,
      inputFingerprint,
      activeQuoteWindow.issuedAt,
      activeQuoteWindow.expiresAt,
    );
    const discoveryExtension = buildToolDiscoveryExtension(definition, definition.exampleOutput);
    const challengeBody = buildToolChallengeBody({
      definition,
      decisionId: quoteDecisionId,
      requirement,
      quoteIssuedAt: activeQuoteWindow.issuedAt,
      quoteExpiresAt: activeQuoteWindow.expiresAt,
    });

    if (decisionId) {
      stateStore.setToolRecord(productId, decisionId, {
        decisionId,
        productId,
        inputFingerprint,
        requestBody: normalizedInput,
        chargedAmountUsdc: getX402ToolPriceUsdc(productId),
        quoteIssuedAt: activeQuoteWindow.issuedAt,
        quoteExpiresAt: activeQuoteWindow.expiresAt,
        state: "quoted",
        createdAt: existingRecord?.createdAt ?? activeQuoteWindow.issuedAt,
        updatedAt: nowIso(),
      });
    }

    const httpServer = await createToolX402HttpServer(req, definition, requirement, challengeBody, discoveryExtension);
    const httpContext = buildAllocateHttpContext(req);
    const processResult = await httpServer.processHTTPRequest(httpContext);

    if (processResult.type === "payment-error") {
      attachBazaarDiscovery(res, discoveryExtension);
      return sendX402HttpResponse(res, processResult.response, challengeBody);
    }

    if (processResult.type !== "payment-verified") {
      return failure(res, new Error("x402 verification did not complete."), 500);
    }

    const verifyResult = await httpServer.server.verifyPayment(
      processResult.paymentPayload,
      processResult.paymentRequirements,
    );
    if (!verifyResult.isValid) {
      return failure(
        res,
        new Error(`x402 verification mismatch: ${verifyResult.invalidMessage || verifyResult.invalidReason || "invalid payment"}`),
        502,
      );
    }

    const payer = verifyResult.payer?.trim();
    if (!payer || (!isAddress(payer) && !validateSvmAddress(payer))) {
      return failure(res, new Error("x402 facilitator did not return a valid payer address."), 502);
    }

    if (getAddressUsageCount(payer) >= getX402FromAddressDailyCap()) {
      return sendRateLimited(res, "from_address_daily_cap", secondsUntilNextUtcDay());
    }

    const settlement = await httpServer.processSettlement(
      processResult.paymentPayload,
      processResult.paymentRequirements,
      processResult.declaredExtensions,
      { request: httpContext },
    );

    if (!settlement.success) {
      if (settlement.transaction?.trim()) {
        // Facilitator timed out internally but the tx confirmed on-chain.
        // The buyer was charged — proceed with delivery rather than returning 502.
        console.warn("[x402] settlement timeout with on-chain tx — recovering and proceeding", {
          errorReason: settlement.errorReason,
          transaction: settlement.transaction,
          payer: settlement.payer,
          productId,
          decisionId,
        });
      } else {
        void sendAdminErrorEmail({
          decisionId: decisionId ?? "unknown",
          endpoint: definition.routePath,
          productId,
          errorReason: settlement.errorReason ?? null,
          errorMessage: settlement.errorMessage ?? null,
          walletAddress: payer ?? null,
          transactionHash: null,
          network: settlement.network ?? null,
          chargedAmountUsdc: getX402ToolPriceUsdc(productId),
          requestInput: normalizedInput as Record<string, unknown>,
        });
        return failure(
          res,
          new Error(`x402 settlement failed: ${settlement.errorMessage || settlement.errorReason || "unknown error"}`),
          502,
          {
            errorReason: settlement.errorReason || null,
            errorMessage: settlement.errorMessage || null,
            network: settlement.network || null,
            payer: settlement.payer || null,
            transaction: settlement.transaction || null,
            facilitatorUrl: getX402FacilitatorUrl(),
          },
        );
      }
    }

    const transactionHash = settlement.transaction?.trim();
    if (!transactionHash) {
      return failure(res, new Error("x402 settlement completed without a transaction hash."), 502);
    }

    const confirmedDecisionId = decisionId;
    if (!confirmedDecisionId) {
      return failure(res, new Error("decisionId is required."), 400);
    }

    const reservation = stateStore.reserveTransactionHash(transactionHash, buildToolStateKey(productId, confirmedDecisionId));
    if (!reservation.accepted) {
      return failure(
        res,
        new Error("PAYMENT-RESPONSE transaction was already consumed for a different decisionId."),
        409,
      );
    }

    const jobId = buildToolJobId(productId, confirmedDecisionId);
    if (!options?.skipPhase1) {
      runPhase1({
        jobId,
        executionTimestamp: nowIso(),
        riskMode: deriveRiskMode(normalizedInput.riskTolerance as AllocateRiskTolerance),
        riskTolerance: normalizedInput.riskTolerance as string,
        investmentTimeframe: normalizedInput.timeframe as string,
        portfolioSegment: normalizedInput.portfolioSegment as string,
        walletAddress: payer,
      });
    }

    let result: Awaited<ReturnType<typeof execute>>;
    try {
      result = await execute(jobId);
    } catch (executeErr) {
      void sendAdminErrorEmail({
        decisionId: confirmedDecisionId,
        endpoint: definition.routePath,
        productId,
        errorReason: "execute_failed_post_settlement",
        errorMessage: executeErr instanceof Error ? executeErr.message : String(executeErr),
        walletAddress: payer,
        transactionHash,
        network: settlement.network ?? null,
        chargedAmountUsdc: getX402ToolPriceUsdc(productId),
        requestInput: normalizedInput as Record<string, unknown>,
      });
      throw executeErr;
    }
    const acceptedAt = nowIso();
    const responseData: StoredToolResponseData = {
      status: "completed",
      endpoint: definition.routePath,
      decisionId: confirmedDecisionId,
      productId,
      payment: {
        required: true,
        chargedAmountUsdc: getX402ToolPriceUsdc(productId),
        verified: true,
        fromAddress: payer,
        transactionHash,
        network: settlement.network ?? null,
      },
      result,
    };

    stateStore.setToolRecord(productId, confirmedDecisionId, {
      decisionId: confirmedDecisionId,
      productId,
      inputFingerprint,
      requestBody: normalizedInput,
      chargedAmountUsdc: getX402ToolPriceUsdc(productId),
      quoteIssuedAt: activeQuoteWindow.issuedAt,
      quoteExpiresAt: activeQuoteWindow.expiresAt,
      state: "accepted",
      createdAt: existingRecord?.createdAt ?? acceptedAt,
      updatedAt: acceptedAt,
      payment: {
        fromAddress: payer,
        transactionHash,
        network: settlement.network,
        verifiedAt: acceptedAt,
      },
      responseData: result,
    });
    incrementAddressUsage(payer);

    void sendAdminUsageEmail({
      channel: options?.usageChannel ?? "x402_allocate",
      decisionId: confirmedDecisionId,
      endpoint: definition.routePath,
      productId,
      walletAddress: payer,
      resultEmail:
        typeof req.body?.resultEmail === "string" && isValidEmail(req.body.resultEmail.trim().toLowerCase())
          ? req.body.resultEmail.trim().toLowerCase()
          : null,
      promoCode: typeof req.body?.promoCode === "string" && req.body.promoCode.trim()
        ? req.body.promoCode.trim()
        : null,
      chargedAmountUsdc: getX402ToolPriceUsdc(productId),
      transactionHash,
      paymentMethod: "x402",
      paymentNetwork: settlement.network ?? null,
      includeCertifiedDecisionRecord: false,
      riskTolerance: normalizedInput.riskTolerance as string,
      timeframe: normalizedInput.timeframe as string,
      jobId,
      requestInput: normalizedInput,
      responseOutput: {
        success: true,
        executionModelVersion: EXECUTION_MODEL_VERSION,
        data: responseData,
      },
    }).catch((error) => {
      console.error(`Failed to send Selun admin usage email (${productId}):`, error);
    });

    attachBazaarDiscovery(res, discoveryExtension);
    if (settlement.success) {
      applySettlementResponseHeaders(res, settlement);
    }
    return res.status(200).json({
      success: true,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      data: responseData,
      logs: getExecutionLogs(120),
    });
  } catch (error) {
    return failure(res, error, 500);
  } finally {
    runningAllocateOrchestration.delete(orchestrationKey);
  }
}

router.get("/admin/overview", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const [wallet, wallets, purchases, adminFundingWallet, treasury] = await Promise.all([
      getAgentWalletSnapshot(),
      listProjectSellerWalletSnapshots(),
      Promise.resolve(listAdminPurchases()),
      getAdminFundingWalletSnapshot(),
      getTreasuryWalletSetupSnapshot(),
    ]);

    return success(res, {
      wallet,
      wallets,
      adminRefundAddress: getAdminWithdrawAddress(),
      defaultGasTopUpAmountEth: getAdminGasTopUpAmountEth(),
      transferMode: {
        kind: "cdp_server_account_v2",
        gaslessUsdc: false,
        note: "CDP v2 server-account transfers currently execute as standard EVM sends in Selun. Gas is auto-funded when needed.",
      },
      treasury,
      adminFundingWallet,
      purchases,
    });
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.post("/admin/treasury-smart-account", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const treasury = await ensureTreasurySmartAccount();
    return success(res, {
      treasury,
      message: "Treasury smart account is ready.",
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/admin/rollup-usdc", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const sellerAddress =
      typeof req.body?.sellerAddress === "string" && isAddress(req.body.sellerAddress)
        ? req.body.sellerAddress
        : undefined;
    if (!sellerAddress) {
      return failure(res, new Error("sellerAddress is required and must be a valid EVM address."), 400);
    }

    const rollupAllNormalized = normalizeOptionalBoolean(req.body?.rollupAll);
    if (!rollupAllNormalized.valid) {
      return failure(res, new Error("rollupAll must be a boolean when provided."), 400);
    }
    const rollupAll = req.body?.rollupAll === undefined ? true : rollupAllNormalized.value;
    const ensureNativeTopUpNormalized = normalizeOptionalBoolean(req.body?.ensureNativeTopUp);
    if (!ensureNativeTopUpNormalized.valid) {
      return failure(res, new Error("ensureNativeTopUp must be a boolean when provided."), 400);
    }
    const ensureNativeTopUp = req.body?.ensureNativeTopUp === undefined ? true : ensureNativeTopUpNormalized.value;
    const requestedAmount = req.body?.amountUsdc;
    const topUpFromSellerAddress =
      typeof req.body?.nativeTopUpFromSellerAddress === "string" && isAddress(req.body.nativeTopUpFromSellerAddress)
        ? req.body.nativeTopUpFromSellerAddress
        : undefined;
    const topUpAmountEth =
      typeof req.body?.nativeTopUpAmountEth === "string" || typeof req.body?.nativeTopUpAmountEth === "number"
        ? String(req.body.nativeTopUpAmountEth)
        : getAdminGasTopUpAmountEth();
    const note = toText(req.body?.note, "");

    const [activeWallet, wallets] = await Promise.all([
      getAgentWalletSnapshot(),
      listProjectSellerWalletSnapshots(),
    ]);

    if (sellerAddress.toLowerCase() === activeWallet.walletAddress.toLowerCase()) {
      return failure(res, new Error("Selected seller wallet is already the active wallet."), 400);
    }

    const sourceWallet = wallets.find(
      (wallet) => wallet.walletAddress.toLowerCase() === sellerAddress.toLowerCase(),
    );
    if (!sourceWallet) {
      return failure(res, new Error("Selected seller wallet was not found in the configured CDP project."), 404);
    }

    const availableUsdcBaseUnits = BigInt(sourceWallet.usdcBalanceBaseUnits);
    if (availableUsdcBaseUnits <= 0n) {
      return failure(res, new Error("Selected seller wallet has no USDC balance available to roll up."), 400);
    }

    let amountUsdc: string;
    if (typeof requestedAmount === "string" || typeof requestedAmount === "number") {
      amountUsdc = formatUsdcAmount(Number(formatUnits(parseUnits(String(requestedAmount), 6), 6)));
    } else if (rollupAll) {
      amountUsdc = formatUnits(availableUsdcBaseUnits, 6);
    } else {
      return failure(res, new Error("Provide amountUsdc or set rollupAll=true."), 400);
    }

    let nativeTopUp: AdminNativeTopUpSummary | null = null;
    if (ensureNativeTopUp && BigInt(sourceWallet.nativeBalanceBaseUnits) === 0n) {
      const adminFundingWallet = await getAdminFundingWalletSnapshot();
      const topUpTransfer =
        adminFundingWallet && BigInt(adminFundingWallet.nativeBalanceBaseUnits) > 0n
          ? await transferNativeFromAdminWallet({
            toAddress: sellerAddress,
            amountEth: topUpAmountEth,
            note: note ? `${note} | native top-up for rollup` : "admin native top-up for rollup",
          })
          : await (async () => {
            const fundingWallet =
              wallets.find((wallet) =>
                wallet.walletAddress.toLowerCase() === (topUpFromSellerAddress ?? "").toLowerCase(),
              ) ??
              wallets.find((wallet) =>
                wallet.walletAddress.toLowerCase() !== sellerAddress.toLowerCase() &&
                BigInt(wallet.nativeBalanceBaseUnits) > 0n,
              );

            if (!fundingWallet) {
              throw new Error("No admin funding wallet or project seller wallet with native balance is available to top up the selected rollup wallet.");
            }

            if (fundingWallet.walletAddress.toLowerCase() === sellerAddress.toLowerCase()) {
              throw new Error("nativeTopUpFromSellerAddress must differ from the rollup source wallet.");
            }

            return transferSellerNative({
              fromSellerAddress: fundingWallet.walletAddress,
              toAddress: sellerAddress,
              amountEth: topUpAmountEth,
              note: note ? `${note} | native top-up for rollup` : "admin native top-up for rollup",
            });
          })();

      nativeTopUp = {
        transactionHash: topUpTransfer.transactionHash,
        amountEth: topUpTransfer.amountEth,
        toAddress: topUpTransfer.toAddress,
        fromWalletAddress: topUpTransfer.fromWalletAddress,
        nativeBalanceBefore: topUpTransfer.nativeBalanceBefore,
        nativeBalanceAfter: topUpTransfer.nativeBalanceAfter,
      };
    }

    const transfer = await transferSellerUsdc({
      sellerAddress,
      toAddress: activeWallet.walletAddress,
      amountUsdc,
      note: note || undefined,
    });

    return success(res, {
      ...(nativeTopUp ? { nativeTopUp } : {}),
      sourceWallet: {
        walletAddress: sourceWallet.walletAddress,
        usdcBalanceBefore: sourceWallet.usdcBalance,
        nativeBalanceBefore: sourceWallet.nativeBalance,
      },
      destinationWallet: {
        walletAddress: activeWallet.walletAddress,
        usdcBalanceBefore: activeWallet.usdcBalance,
        nativeBalanceBefore: activeWallet.nativeBalance,
      },
      transfer,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/admin/rollup-usdc-to-treasury", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const sellerAddress =
      typeof req.body?.sellerAddress === "string" && isAddress(req.body.sellerAddress)
        ? req.body.sellerAddress
        : undefined;
    if (!sellerAddress) {
      return failure(res, new Error("sellerAddress is required and must be a valid EVM address."), 400);
    }

    const rollupAllNormalized = normalizeOptionalBoolean(req.body?.rollupAll);
    if (!rollupAllNormalized.valid) {
      return failure(res, new Error("rollupAll must be a boolean when provided."), 400);
    }
    const rollupAll = req.body?.rollupAll === undefined ? true : rollupAllNormalized.value;
    const ensureNativeTopUpNormalized = normalizeOptionalBoolean(req.body?.ensureNativeTopUp);
    if (!ensureNativeTopUpNormalized.valid) {
      return failure(res, new Error("ensureNativeTopUp must be a boolean when provided."), 400);
    }
    const ensureNativeTopUp = req.body?.ensureNativeTopUp === undefined ? true : ensureNativeTopUpNormalized.value;
    const requestedAmount = req.body?.amountUsdc;
    const topUpFromSellerAddress =
      typeof req.body?.nativeTopUpFromSellerAddress === "string" && isAddress(req.body.nativeTopUpFromSellerAddress)
        ? req.body.nativeTopUpFromSellerAddress
        : undefined;
    const topUpAmountEth =
      typeof req.body?.nativeTopUpAmountEth === "string" || typeof req.body?.nativeTopUpAmountEth === "number"
        ? String(req.body.nativeTopUpAmountEth)
        : getAdminGasTopUpAmountEth();
    const note = toText(req.body?.note, "");

    const [treasury, wallets] = await Promise.all([
      ensureTreasurySmartAccount(),
      listProjectSellerWalletSnapshots(),
    ]);

    if (!treasury.smartAccount) {
      return failure(res, new Error("Treasury smart account is not available."), 400);
    }

    if (sellerAddress.toLowerCase() === treasury.smartAccount.walletAddress.toLowerCase()) {
      return failure(res, new Error("Selected wallet is already the treasury smart account."), 400);
    }

    const sourceWallet = wallets.find(
      (wallet) => wallet.walletAddress.toLowerCase() === sellerAddress.toLowerCase(),
    );
    if (!sourceWallet) {
      return failure(res, new Error("Selected seller wallet was not found in the configured CDP project."), 404);
    }

    const availableUsdcBaseUnits = BigInt(sourceWallet.usdcBalanceBaseUnits);
    if (availableUsdcBaseUnits <= 0n) {
      return failure(res, new Error("Selected seller wallet has no USDC balance available to roll up."), 400);
    }

    let amountUsdc: string;
    if (typeof requestedAmount === "string" || typeof requestedAmount === "number") {
      amountUsdc = formatUsdcAmount(Number(formatUnits(parseUnits(String(requestedAmount), 6), 6)));
    } else if (rollupAll) {
      amountUsdc = formatUnits(availableUsdcBaseUnits, 6);
    } else {
      return failure(res, new Error("Provide amountUsdc or set rollupAll=true."), 400);
    }

    let nativeTopUp: AdminNativeTopUpSummary | null = null;
    if (ensureNativeTopUp && BigInt(sourceWallet.nativeBalanceBaseUnits) === 0n) {
      const adminFundingWallet = await getAdminFundingWalletSnapshot();
      const topUpTransfer =
        adminFundingWallet && BigInt(adminFundingWallet.nativeBalanceBaseUnits) > 0n
          ? await transferNativeFromAdminWallet({
            toAddress: sellerAddress,
            amountEth: topUpAmountEth,
            note: note ? `${note} | native top-up for treasury rollup` : "admin native top-up for treasury rollup",
          })
          : await (async () => {
            const fundingWallet =
              wallets.find((wallet) =>
                wallet.walletAddress.toLowerCase() === (topUpFromSellerAddress ?? "").toLowerCase(),
              ) ??
              wallets.find((wallet) =>
                wallet.walletAddress.toLowerCase() !== sellerAddress.toLowerCase() &&
                BigInt(wallet.nativeBalanceBaseUnits) > 0n,
              );

            if (!fundingWallet) {
              throw new Error("No admin funding wallet or project seller wallet with native balance is available to top up the selected treasury rollup wallet.");
            }

            if (fundingWallet.walletAddress.toLowerCase() === sellerAddress.toLowerCase()) {
              throw new Error("nativeTopUpFromSellerAddress must differ from the treasury rollup source wallet.");
            }

            return transferSellerNative({
              fromSellerAddress: fundingWallet.walletAddress,
              toAddress: sellerAddress,
              amountEth: topUpAmountEth,
              note: note ? `${note} | native top-up for treasury rollup` : "admin native top-up for treasury rollup",
            });
          })();

      nativeTopUp = {
        transactionHash: topUpTransfer.transactionHash,
        amountEth: topUpTransfer.amountEth,
        toAddress: topUpTransfer.toAddress,
        fromWalletAddress: topUpTransfer.fromWalletAddress,
        nativeBalanceBefore: topUpTransfer.nativeBalanceBefore,
        nativeBalanceAfter: topUpTransfer.nativeBalanceAfter,
      };
    }

    const transfer = await transferSellerUsdc({
      sellerAddress,
      toAddress: treasury.smartAccount.walletAddress,
      amountUsdc,
      note: note || undefined,
    });

    return success(res, {
      ...(nativeTopUp ? { nativeTopUp } : {}),
      sourceWallet: {
        walletAddress: sourceWallet.walletAddress,
        usdcBalanceBefore: sourceWallet.usdcBalance,
        nativeBalanceBefore: sourceWallet.nativeBalance,
      },
      destinationWallet: {
        walletAddress: treasury.smartAccount.walletAddress,
        usdcBalanceBefore: treasury.smartAccount.usdcBalance,
        nativeBalanceBefore: treasury.smartAccount.nativeBalance,
      },
      transfer,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/admin/swap-usdc-to-eth", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const config = getConfig();
    if (config.networkId !== "base-mainnet") {
      return failure(res, new Error("USDC-to-ETH seller wallet swaps are only available on base-mainnet."), 400);
    }

    const sellerAddress =
      typeof req.body?.sellerAddress === "string" && isAddress(req.body.sellerAddress)
        ? req.body.sellerAddress
        : undefined;
    if (!sellerAddress) {
      return failure(res, new Error("sellerAddress is required and must be a valid EVM address."), 400);
    }

    const amountUsdc = req.body?.amountUsdc;
    if (typeof amountUsdc !== "string" && typeof amountUsdc !== "number") {
      return failure(res, new Error("amountUsdc is required."), 400);
    }

    const slippageBps =
      typeof req.body?.slippageBps === "string" || typeof req.body?.slippageBps === "number"
        ? Number(req.body.slippageBps)
        : undefined;
    const note = toText(req.body?.note, "");

    const wallets = await listProjectSellerWalletSnapshots();
    const sourceWallet = wallets.find(
      (wallet) => wallet.walletAddress.toLowerCase() === sellerAddress.toLowerCase(),
    );
    if (!sourceWallet) {
      return failure(res, new Error("Selected seller wallet was not found in the configured CDP project."), 404);
    }

    const transfer = await swapSellerUsdcToEth({
      sellerAddress,
      amountUsdc,
      ...(slippageBps !== undefined ? { slippageBps } : {}),
      ...(note ? { note } : {}),
    });

    return success(res, {
      sourceWallet: {
        walletAddress: sourceWallet.walletAddress,
        usdcBalanceBefore: sourceWallet.usdcBalance,
        nativeBalanceBefore: sourceWallet.nativeBalance,
      },
      transfer,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/admin/withdraw", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const adminWithdrawAddress = getAdminWithdrawAddress();
    const sellerAddress =
      typeof req.body?.sellerAddress === "string" && isAddress(req.body.sellerAddress)
        ? req.body.sellerAddress
        : undefined;
    if (!sellerAddress) {
      return failure(res, new Error("sellerAddress is required and must be a valid EVM address."), 400);
    }

    const withdrawAllNormalized = normalizeOptionalBoolean(req.body?.withdrawAll);
    if (!withdrawAllNormalized.valid) {
      return failure(res, new Error("withdrawAll must be a boolean when provided."), 400);
    }
    const withdrawAll = req.body?.withdrawAll === undefined ? true : withdrawAllNormalized.value;

    const ensureNativeTopUpNormalized = normalizeOptionalBoolean(req.body?.ensureNativeTopUp);
    if (!ensureNativeTopUpNormalized.valid) {
      return failure(res, new Error("ensureNativeTopUp must be a boolean when provided."), 400);
    }
    const ensureNativeTopUp = req.body?.ensureNativeTopUp === undefined ? true : ensureNativeTopUpNormalized.value;

    const requestedAmount = req.body?.amountUsdc;
    const topUpFromSellerAddress =
      typeof req.body?.nativeTopUpFromSellerAddress === "string" && isAddress(req.body.nativeTopUpFromSellerAddress)
        ? req.body.nativeTopUpFromSellerAddress
        : undefined;
    const topUpAmountEth =
      typeof req.body?.nativeTopUpAmountEth === "string" || typeof req.body?.nativeTopUpAmountEth === "number"
        ? String(req.body.nativeTopUpAmountEth)
        : getAdminGasTopUpAmountEth();
    const note = toText(req.body?.note, "");

    const wallets = await listProjectSellerWalletSnapshots();
    const targetWallet = wallets.find(
      (wallet) => wallet.walletAddress.toLowerCase() === sellerAddress.toLowerCase(),
    );

    if (!targetWallet) {
      return failure(res, new Error("Selected seller wallet was not found in the configured CDP project."), 404);
    }

    const availableUsdcBaseUnits = BigInt(targetWallet.usdcBalanceBaseUnits);
    if (availableUsdcBaseUnits <= 0n) {
      return failure(res, new Error("Selected seller wallet has no USDC balance available to withdraw."), 400);
    }

    let amountUsdc: string;
    if (typeof requestedAmount === "string" || typeof requestedAmount === "number") {
      const normalized = formatUsdcAmount(Number(formatUnits(parseUnits(String(requestedAmount), 6), 6)));
      amountUsdc = normalized;
    } else if (withdrawAll) {
      amountUsdc = formatUnits(availableUsdcBaseUnits, 6);
    } else {
      return failure(res, new Error("Provide amountUsdc or set withdrawAll=true."), 400);
    }

    let nativeTopUp: AdminNativeTopUpSummary | null = null;
    if (ensureNativeTopUp && BigInt(targetWallet.nativeBalanceBaseUnits) === 0n) {
      const adminFundingWallet = await getAdminFundingWalletSnapshot();
      const topUpTransfer =
        adminFundingWallet && BigInt(adminFundingWallet.nativeBalanceBaseUnits) > 0n
          ? await transferNativeFromAdminWallet({
            toAddress: sellerAddress,
            amountEth: topUpAmountEth,
            note: note ? `${note} | native top-up` : "admin native top-up",
          })
          : await (async () => {
            const fundingWallet =
              wallets.find((wallet) =>
                wallet.walletAddress.toLowerCase() === (topUpFromSellerAddress ?? "").toLowerCase(),
              ) ??
              wallets.find((wallet) =>
                wallet.walletAddress.toLowerCase() !== sellerAddress.toLowerCase() &&
                BigInt(wallet.nativeBalanceBaseUnits) > 0n,
              );

            if (!fundingWallet) {
              throw new Error("No admin funding wallet or project seller wallet with native balance is available to top up the selected withdrawal wallet.");
            }

            if (fundingWallet.walletAddress.toLowerCase() === sellerAddress.toLowerCase()) {
              throw new Error("nativeTopUpFromSellerAddress must differ from the withdrawal seller wallet.");
            }

            return transferSellerNative({
              fromSellerAddress: fundingWallet.walletAddress,
              toAddress: sellerAddress,
              amountEth: topUpAmountEth,
              note: note ? `${note} | native top-up` : "admin native top-up",
            });
          })();

      nativeTopUp = {
        transactionHash: topUpTransfer.transactionHash,
        amountEth: topUpTransfer.amountEth,
        toAddress: topUpTransfer.toAddress,
        fromWalletAddress: topUpTransfer.fromWalletAddress,
        nativeBalanceBefore: topUpTransfer.nativeBalanceBefore,
        nativeBalanceAfter: topUpTransfer.nativeBalanceAfter,
      };
    }

    const transfer = await transferSellerUsdc({
      sellerAddress,
      toAddress: adminWithdrawAddress,
      amountUsdc,
      note: note || undefined,
    });

    return success(res, {
      ...(nativeTopUp ? { nativeTopUp } : {}),
      transfer,
      sourceWallet: {
        walletAddress: targetWallet.walletAddress,
        usdcBalanceBefore: targetWallet.usdcBalance,
        nativeBalanceBefore: targetWallet.nativeBalance,
      },
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/admin/withdraw-treasury", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const adminWithdrawAddress = getAdminWithdrawAddress();
    const withdrawAllNormalized = normalizeOptionalBoolean(req.body?.withdrawAll);
    if (!withdrawAllNormalized.valid) {
      return failure(res, new Error("withdrawAll must be a boolean when provided."), 400);
    }
    const withdrawAll = req.body?.withdrawAll === undefined ? true : withdrawAllNormalized.value;
    const requestedAmount = req.body?.amountUsdc;
    const note = toText(req.body?.note, "");

    const treasury = await ensureTreasurySmartAccount();
    if (!treasury.smartAccount) {
      return failure(res, new Error("Treasury smart account is not available."), 400);
    }

    const availableUsdcBaseUnits = BigInt(treasury.smartAccount.usdcBalanceBaseUnits);
    if (availableUsdcBaseUnits <= 0n) {
      return failure(res, new Error("Treasury smart account has no USDC balance available to withdraw."), 400);
    }

    let amountUsdc: string;
    if (typeof requestedAmount === "string" || typeof requestedAmount === "number") {
      amountUsdc = formatUsdcAmount(Number(formatUnits(parseUnits(String(requestedAmount), 6), 6)));
    } else if (withdrawAll) {
      amountUsdc = formatUnits(availableUsdcBaseUnits, 6);
    } else {
      return failure(res, new Error("Provide amountUsdc or set withdrawAll=true."), 400);
    }

    const transfer = await transferTreasuryUsdc({
      toAddress: adminWithdrawAddress,
      amountUsdc,
      note: note || undefined,
    });

    return success(res, {
      sourceWallet: {
        walletAddress: treasury.smartAccount.walletAddress,
        usdcBalanceBefore: treasury.smartAccount.usdcBalance,
        nativeBalanceBefore: treasury.smartAccount.nativeBalance,
      },
      destinationWallet: {
        walletAddress: adminWithdrawAddress,
      },
      transfer,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/admin/refund", async (req: Request, res: Response) => {
  if (!requireAdminRequest(req, res)) return;

  try {
    const adminRefundAddress = getAdminWithdrawAddress();
    const ensureNativeTopUpNormalized = normalizeOptionalBoolean(req.body?.ensureNativeTopUp);
    if (!ensureNativeTopUpNormalized.valid) {
      return failure(res, new Error("ensureNativeTopUp must be a boolean when provided."), 400);
    }
    const ensureNativeTopUp = req.body?.ensureNativeTopUp === undefined ? true : ensureNativeTopUpNormalized.value;
    const requestedAmount = req.body?.amountUsdc;
    const sellerAddress =
      typeof req.body?.sellerAddress === "string" && isAddress(req.body.sellerAddress)
        ? req.body.sellerAddress
        : undefined;
    const topUpFromSellerAddress =
      typeof req.body?.nativeTopUpFromSellerAddress === "string" && isAddress(req.body.nativeTopUpFromSellerAddress)
        ? req.body.nativeTopUpFromSellerAddress
        : undefined;
    const topUpAmountEth =
      typeof req.body?.nativeTopUpAmountEth === "string" || typeof req.body?.nativeTopUpAmountEth === "number"
        ? String(req.body.nativeTopUpAmountEth)
        : getAdminGasTopUpAmountEth();
    const refs = normalizeRefundReferences(req.body?.refunds);
    const note = toText(req.body?.note, "");
    const purchases = listAdminPurchases();

    const selectedPurchases = refs.length > 0
      ? refs.map((ref) => {
        const purchaseKey = ref.kind === "allocate"
          ? `allocate:${ref.decisionId}`
          : `tool:${ref.productId}:${ref.decisionId}`;
        return purchases.find((purchase) => purchase.purchaseKey === purchaseKey) ?? null;
      }).filter((purchase): purchase is AdminPurchaseRow => Boolean(purchase))
      : [];

    if (refs.length > 0 && selectedPurchases.length !== refs.length) {
      return failure(res, new Error("One or more selected purchases could not be found."), 404);
    }

    if (selectedPurchases.some((purchase) => purchase.refunded)) {
      return failure(res, new Error("One or more selected purchases were already refunded."), 409);
    }

    let amountUsdc: string;
    if (typeof requestedAmount === "string" || typeof requestedAmount === "number") {
      const normalized = formatUsdcAmount(Number(formatUnits(parseUnits(String(requestedAmount), 6), 6)));
      amountUsdc = normalized;
    } else if (selectedPurchases.length > 0) {
      amountUsdc = formatUnits(sumPurchaseAmountBaseUnits(selectedPurchases), 6);
    } else {
      return failure(res, new Error("Provide amountUsdc or select purchases to refund."), 400);
    }

    if (selectedPurchases.length > 0) {
      const selectedTotal = formatUnits(sumPurchaseAmountBaseUnits(selectedPurchases), 6);
      if (amountUsdc !== selectedTotal) {
        return failure(res, new Error("amountUsdc must match the total of selected purchases."), 400);
      }
    }

    let nativeTopUp: AdminNativeTopUpSummary | null = null;
    if (ensureNativeTopUp) {
      const [activeWallet, wallets] = await Promise.all([
        getAgentWalletSnapshot(),
        listProjectSellerWalletSnapshots(),
      ]);
      const refundSellerAddress = sellerAddress ?? activeWallet.walletAddress;
      const targetWallet = wallets.find(
        (wallet) => wallet.walletAddress.toLowerCase() === refundSellerAddress.toLowerCase(),
      );

      if (!targetWallet) {
        return failure(res, new Error("Selected seller wallet was not found in the configured CDP project."), 404);
      }

      const targetNativeBalance = BigInt(targetWallet.nativeBalanceBaseUnits);
      if (targetNativeBalance === 0n) {
        const fundingWallet =
          wallets.find((wallet) =>
            wallet.walletAddress.toLowerCase() === (topUpFromSellerAddress ?? "").toLowerCase(),
          ) ??
          wallets.find((wallet) =>
            wallet.walletAddress.toLowerCase() !== refundSellerAddress.toLowerCase() &&
            BigInt(wallet.nativeBalanceBaseUnits) > 0n,
          );

        if (!fundingWallet) {
          return failure(
            res,
            new Error("No project seller wallet with native balance is available to top up the selected refund wallet."),
            400,
          );
        }

        if (fundingWallet.walletAddress.toLowerCase() === refundSellerAddress.toLowerCase()) {
          return failure(res, new Error("nativeTopUpFromSellerAddress must differ from the refund seller wallet."), 400);
        }

        const topUpTransfer = await transferSellerNative({
          fromSellerAddress: fundingWallet.walletAddress,
          toAddress: refundSellerAddress,
          amountEth: topUpAmountEth,
          note: note ? `${note} | native top-up` : "admin native top-up",
        });

        nativeTopUp = {
          transactionHash: topUpTransfer.transactionHash,
          amountEth: topUpTransfer.amountEth,
          toAddress: topUpTransfer.toAddress,
          fromWalletAddress: topUpTransfer.fromWalletAddress,
          nativeBalanceBefore: topUpTransfer.nativeBalanceBefore,
          nativeBalanceAfter: topUpTransfer.nativeBalanceAfter,
        };
      }
    }

    const transfer = await transferSellerUsdc({
      ...(sellerAddress ? { sellerAddress } : {}),
      toAddress: adminRefundAddress,
      amountUsdc,
      note: note || undefined,
    });

    if (selectedPurchases.length > 0) {
      const stateStore = getX402StateStore();
      for (const purchase of selectedPurchases) {
        const refund = {
          refundedAt: nowIso(),
          transactionHash: transfer.transactionHash,
          toAddress: adminRefundAddress,
          amountUsdc: purchase.chargedAmountUsdc,
          ...(note ? { note } : {}),
        };

        if (purchase.kind === "allocate") {
          stateStore.markAllocateRefund(purchase.decisionId, refund);
        } else {
          stateStore.markToolRefund(purchase.productId as X402ToolProductId, purchase.decisionId, refund);
        }
      }
    }

    return success(res, {
      ...(nativeTopUp ? { nativeTopUp } : {}),
      transfer,
      refundedPurchases: selectedPurchases.map((purchase) => ({
        purchaseKey: purchase.purchaseKey,
        decisionId: purchase.decisionId,
        productId: purchase.productId,
        chargedAmountUsdc: purchase.chargedAmountUsdc,
      })),
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

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
    const data = await buildX402CapabilitiesData();
    return res.status(200).json({
      success: true,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      data,
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
  const paymentMethod = toText(payment?.paymentMethod, "onchain").toLowerCase();
  const decisionId = toText(payment?.decisionId, "");
  const transactionId = toText(payment?.transactionId, "");
  const chargedAmountUsdc = toFiniteNumber(payment?.chargedAmountUsdc ?? payment?.amountUsdc);
  const walletAddressRaw = toText(req.body?.walletAddress, "");
  const hasValidWalletAddress = Boolean(walletAddressRaw && isAddress(walletAddressRaw));
  const requiresOnchainVerification = paymentMethod !== "card";
  if (paymentStatus !== "paid" || !decisionId || !transactionId) {
    return failure(res, new Error("Paid purchase record is required before result email delivery."), 402);
  }
  if (chargedAmountUsdc === null || chargedAmountUsdc < 0) {
    return failure(res, new Error("Charged amount is required before result email delivery."), 400);
  }
  if (requiresOnchainVerification && !hasValidWalletAddress) {
    return failure(res, new Error("Valid wallet address is required before result email delivery."), 400);
  }

  if (requiresOnchainVerification) {
    try {
      await verifyResultEmailPayment({
        fromAddress: walletAddressRaw,
        expectedAmountUSDC: chargedAmountUsdc,
        transactionHash: transactionId,
        decisionId,
      });
    } catch (error) {
      return failure(res, new Error(`Payment verification required: ${error instanceof Error ? error.message : "verification failed."}`), 402);
    }
  }

  const walletAddress = hasValidWalletAddress
    ? walletAddressRaw
    : paymentMethod === "card"
      ? "Stripe card checkout"
      : "n/a";

  const phase2Artifact = isRecord(req.body?.phase2Artifact) ? req.body.phase2Artifact : null;
  const allocationPolicy = phase2Artifact && isRecord(phase2Artifact.allocation_policy)
    ? phase2Artifact.allocation_policy
    : null;

  if (paymentMethod === "card") {
    void sendAdminUsageEmail({
      channel: "legacy_pay",
      decisionId,
      walletAddress: hasValidWalletAddress ? walletAddressRaw : null,
      resultEmail,
      promoCode: typeof req.body?.promoCode === "string" && req.body.promoCode.trim()
        ? req.body.promoCode.trim()
        : null,
      chargedAmountUsdc,
      transactionHash: transactionId,
      paymentMethod: "card",
      includeCertifiedDecisionRecord: Boolean(payment?.certifiedDecisionRecordPurchased),
      riskTolerance: toText(req.body?.riskMode, "") || null,
      timeframe: toText(req.body?.investmentHorizon, "") || null,
      jobId: null,
    }).catch((error) => {
      console.error("Failed to send Selun admin usage email (card/result-email):", error);
    });
  }

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
  const paymentMethod = toText(payment?.paymentMethod, "onchain").toLowerCase();
  const decisionId = toText(payment?.decisionId, "");
  const transactionId = toText(payment?.transactionId, "");
  const chargedAmountUsdc = toFiniteNumber(payment?.chargedAmountUsdc ?? payment?.amountUsdc);
  const walletAddressRaw = toText(req.body?.walletAddress, "");
  const hasValidWalletAddress = Boolean(walletAddressRaw && isAddress(walletAddressRaw));
  const requiresOnchainVerification = paymentMethod !== "card";
  if (paymentStatus !== "paid" || !decisionId || !transactionId) {
    return failure(res, new Error("Paid purchase record is required before report email delivery."), 402);
  }
  if (chargedAmountUsdc === null || chargedAmountUsdc < 0) {
    return failure(res, new Error("Charged amount is required before report email delivery."), 400);
  }
  if (requiresOnchainVerification && !hasValidWalletAddress) {
    return failure(res, new Error("Valid wallet address is required before report email delivery."), 400);
  }

  if (requiresOnchainVerification) {
    try {
      await verifyResultEmailPayment({
        fromAddress: walletAddressRaw,
        expectedAmountUSDC: chargedAmountUsdc,
        transactionHash: transactionId,
        decisionId,
      });
    } catch (error) {
      return failure(res, new Error(`Payment verification required: ${error instanceof Error ? error.message : "verification failed."}`), 402);
    }
  }

  const walletAddress = hasValidWalletAddress
    ? walletAddressRaw
    : paymentMethod === "card"
      ? "Stripe card checkout"
      : "n/a";

  if (paymentMethod === "card") {
    void sendAdminUsageEmail({
      channel: "legacy_pay",
      decisionId,
      walletAddress: hasValidWalletAddress ? walletAddressRaw : null,
      resultEmail,
      promoCode: typeof req.body?.promoCode === "string" && req.body.promoCode.trim()
        ? req.body.promoCode.trim()
        : null,
      chargedAmountUsdc,
      transactionHash: transactionId,
      paymentMethod: "card",
      includeCertifiedDecisionRecord: Boolean(req.body?.includeCertifiedDecisionRecord),
      riskTolerance: toText(req.body?.riskMode, "") || null,
      timeframe: toText(req.body?.investmentHorizon, "") || null,
      jobId: null,
    }).catch((error) => {
      console.error("Failed to send Selun admin usage email (card/report-email):", error);
    });
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

async function handleX402AllocateRequest(
  req: Request,
  res: Response,
  options: {
    routePath: "/agent/x402/allocate" | "/agent/x402/allocate-with-report";
    withReport: boolean;
    description: string;
    discoveryExtension: Record<string, unknown>;
  },
) {
  const burst = enforceIpBurstLimit(req);
  if (burst.limited) {
    return sendRateLimited(res, "ip_burst_limit", burst.retryAfterSeconds);
  }

  const referralCode = extractReferralCode(req);
  const paymentSignatureHeader = req.header("payment-signature")?.trim() || req.header("PAYMENT-SIGNATURE")?.trim();
  const hasPaymentSignature = Boolean(paymentSignatureHeader);
  const riskTolerance = normalizeRiskTolerance(req.body?.riskTolerance);
  const timeframe = normalizeTimeframe(req.body?.timeframe);
  const portfolioSegment = normalizePortfolioSegment(req.body?.portfolioSegment);
  const riskToleranceProvided = req.body?.riskTolerance !== undefined;
  const timeframeProvided = req.body?.timeframe !== undefined;
  const portfolioSegmentProvided = req.body?.portfolioSegment !== undefined;
  if (riskToleranceProvided && !riskTolerance) {
    return failure(
      res,
      new Error("riskTolerance must be one of Conservative | Balanced | Growth | Aggressive."),
      400,
    );
  }
  if (timeframeProvided && !timeframe) {
    return failure(
      res,
      new Error("timeframe must be one of <1_year | 1-3_years | 3+_years."),
      400,
    );
  }
  if (portfolioSegmentProvided && !portfolioSegment) {
    return failure(
      res,
      new Error(`portfolioSegment must be one of ${PORTFOLIO_SEGMENTS.join(" | ")}.`),
      400,
    );
  }
  const withReportParsed = normalizeOptionalBoolean(req.body?.withReport);
  if (!withReportParsed.valid) {
    return failure(res, new Error("withReport must be a boolean (true or false)."), 400);
  }
  if (req.body?.withReport !== undefined && withReportParsed.value !== options.withReport) {
    return failure(
      res,
      new Error(
        options.withReport
          ? "Use /agent/x402/allocate-with-report for certified decision record purchases."
          : "Use /agent/x402/allocate for allocation-only purchases.",
      ),
      400,
    );
  }
  const withReport = options.withReport;
  const chargeAmountUsdc = getAllocateChargeAmountUsdc(withReport);
  const requiresPayment = Number.parseFloat(chargeAmountUsdc) > 0;
  const decisionResolution = resolveDecisionId(req);
  const decisionId = decisionResolution.decisionId;
  const missingDecisionIdOnlyError = !decisionId && decisionResolution.error?.includes("decisionId is required");
  if (decisionResolution.error && !missingDecisionIdOnlyError) {
    return failure(res, new Error(decisionResolution.error), 400);
  }
  if (hasPaymentSignature && !decisionId) {
    return failure(res, new Error(decisionResolution.error ?? "decisionId is required."), 400);
  }

  const stateStore = getX402StateStore();
  const existingRecord = decisionId ? stateStore.getAllocateRecord(decisionId) : undefined;

  if (existingRecord?.state === "accepted" && existingRecord.jobId) {
    return idempotentResponse(res, existingRecord);
  }

  if (decisionId && !riskTolerance) {
    return failure(
      res,
      new Error("riskTolerance is required. Allowed: Conservative | Balanced | Growth | Aggressive."),
      400,
    );
  }

  if (decisionId && !timeframe) {
    return failure(
      res,
      new Error("timeframe is required. Allowed: <1_year | 1-3_years | 3+_years."),
      400,
    );
  }

  const inputShape: AllocateInputShape = {
    riskTolerance: riskTolerance ?? "Balanced",
    timeframe: timeframe ?? "1-3_years",
    portfolioSegment: portfolioSegment ?? "Bluechips",
    withReport,
  };
  const inputFingerprint = computeAllocateInputFingerprint(inputShape);
  const decisionScopedRecord = decisionId ? stateStore.getAllocateRecord(decisionId) : undefined;

  if (decisionId && decisionScopedRecord && decisionScopedRecord.inputFingerprint !== inputFingerprint) {
    return sendAllocateConflict(res, decisionId, decisionScopedRecord.inputs, inputShape);
  }

  if (decisionScopedRecord?.state === "accepted" && decisionScopedRecord.jobId) {
    return idempotentResponse(res, decisionScopedRecord);
  }

  if (runningAllocateOrchestration.size >= getX402GlobalConcurrencyCap()) {
    return sendRateLimited(res, "global_concurrency_cap", 10);
  }

  try {
    const quoteDecisionId = decisionId ?? `quote-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const activeQuoteWindow = decisionScopedRecord?.state === "quoted" && !isExpiredIsoTimestamp(decisionScopedRecord.quoteExpiresAt)
      ? {
        issuedAt: decisionScopedRecord.quoteIssuedAt,
        expiresAt: decisionScopedRecord.quoteExpiresAt,
      }
      : createAllocateQuoteWindow();

    const identity = await getAgentAddress();
    const accepts = buildAllocateAccepts(identity.walletAddress).filter((option) => option.withReport === withReport);
    const selectedAccept = getAllocateAcceptByReport(accepts, withReport);
    const selectedRequirement = buildRequestScopedPaymentRequirement({
      accept: selectedAccept,
      decisionId: quoteDecisionId,
      inputFingerprint,
      quoteIssuedAt: activeQuoteWindow.issuedAt,
      quoteExpiresAt: activeQuoteWindow.expiresAt,
    });
    const challengeBody = buildAllocateChallengeBody({
      inputs: inputShape,
      routePath: options.routePath,
      decisionId: quoteDecisionId,
      chargeAmountUsdc,
      accepts,
      selectedAccept,
      selectedRequirement,
      quoteIssuedAt: activeQuoteWindow.issuedAt,
      quoteExpiresAt: activeQuoteWindow.expiresAt,
    });

    if (decisionId) {
      stateStore.setAllocateRecord(decisionId, {
        decisionId,
        inputFingerprint,
        inputs: inputShape,
        chargedAmountUsdc: chargeAmountUsdc,
        quoteIssuedAt: activeQuoteWindow.issuedAt,
        quoteExpiresAt: activeQuoteWindow.expiresAt,
        state: "quoted",
        createdAt: decisionScopedRecord?.createdAt ?? activeQuoteWindow.issuedAt,
        updatedAt: nowIso(),
      });
    }

    if (!requiresPayment) {
      if (!decisionId) {
        return failure(res, new Error("decisionId is required."), 400);
      }

      const jobId = decisionScopedRecord?.jobId ?? buildAllocateJobId(decisionId);
      if (!decisionScopedRecord?.jobId) {
        runPhase1({
          jobId,
          executionTimestamp: nowIso(),
          riskMode: deriveRiskMode(inputShape.riskTolerance),
          riskTolerance: inputShape.riskTolerance,
          investmentTimeframe: inputShape.timeframe,
          portfolioSegment: inputShape.portfolioSegment,
        });

        void orchestrateAllocateJob(jobId).catch((error) => {
          console.error(`Allocate orchestration failed for ${jobId}:`, error);
        });
      }

      const acceptedAt = nowIso();
      stateStore.setAllocateRecord(decisionId, {
        decisionId,
        inputFingerprint,
        inputs: inputShape,
        chargedAmountUsdc: chargeAmountUsdc,
        quoteIssuedAt: activeQuoteWindow.issuedAt,
        quoteExpiresAt: activeQuoteWindow.expiresAt,
        state: "accepted",
        createdAt: decisionScopedRecord?.createdAt ?? acceptedAt,
        updatedAt: acceptedAt,
        jobId,
      });

      const statusPath = `/execution-status/${encodeURIComponent(jobId)}`;
      return res.status(202).json({
        success: true,
        executionModelVersion: EXECUTION_MODEL_VERSION,
        data: {
          status: decisionScopedRecord?.jobId ? "already_accepted" : "accepted",
          idempotentReplay: Boolean(decisionScopedRecord?.jobId),
          endpoint: options.routePath,
          jobId,
          decisionId,
          inputs: inputShape,
          payment: {
            required: false,
            chargedAmountUsdc: chargeAmountUsdc,
            verified: true,
            fromAddress: null,
            transactionHash: null,
          },
          quoteExpiresAt: activeQuoteWindow.expiresAt,
          statusPath,
        },
        logs: getExecutionLogs(120),
      });
    }

    const httpServer = await createAllocateX402HttpServer(
      req,
      selectedRequirement,
      challengeBody,
      options.routePath,
      options.description,
      options.discoveryExtension,
    );
    const httpContext = buildAllocateHttpContext(req);
    const processResult = await httpServer.processHTTPRequest(httpContext);

    if (processResult.type === "payment-error") {
      attachBazaarDiscovery(res, options.discoveryExtension);
      return sendX402HttpResponse(res, processResult.response, challengeBody);
    }

    if (processResult.type !== "payment-verified") {
      return failure(res, new Error("x402 verification did not complete."), 500);
    }

    const verifyResult = await httpServer.server.verifyPayment(
      processResult.paymentPayload,
      processResult.paymentRequirements,
    );
    if (!verifyResult.isValid) {
      return failure(
        res,
        new Error(`x402 verification mismatch: ${verifyResult.invalidMessage || verifyResult.invalidReason || "invalid payment"}`),
        502,
      );
    }

    const payer = verifyResult.payer?.trim();
    if (!payer || (!isAddress(payer) && !validateSvmAddress(payer))) {
      return failure(res, new Error("x402 facilitator did not return a valid payer address."), 502);
    }

    if (getAddressUsageCount(payer) >= getX402FromAddressDailyCap()) {
      return sendRateLimited(res, "from_address_daily_cap", secondsUntilNextUtcDay());
    }

    const settlement = await httpServer.processSettlement(
      processResult.paymentPayload,
      processResult.paymentRequirements,
      processResult.declaredExtensions,
      { request: httpContext },
    );

    if (!settlement.success) {
      console.error("x402 settlement failed", {
        errorReason: settlement.errorReason,
        errorMessage: settlement.errorMessage,
        network: settlement.network,
        payer: settlement.payer,
        transaction: settlement.transaction,
      });
      return failure(
        res,
        new Error(`x402 settlement failed: ${settlement.errorMessage || settlement.errorReason || "unknown error"}`),
        502,
        {
          errorReason: settlement.errorReason || null,
          errorMessage: settlement.errorMessage || null,
          network: settlement.network || null,
          payer: settlement.payer || null,
          transaction: settlement.transaction || null,
          facilitatorUrl: getX402FacilitatorUrl(),
        },
      );
    }

    const transactionHash = settlement.transaction?.trim();
    if (!transactionHash) {
      return failure(res, new Error("x402 settlement completed without a transaction hash."), 502);
    }

    const reservation = stateStore.reserveTransactionHash(transactionHash, decisionId as string);
    if (!reservation.accepted) {
      return failure(
        res,
        new Error("PAYMENT-RESPONSE transaction was already consumed for a different decisionId."),
        409,
      );
    }

    const jobId = decisionScopedRecord?.jobId ?? buildAllocateJobId(decisionId as string);
    if (!decisionScopedRecord?.jobId) {
      runPhase1({
        jobId,
        executionTimestamp: nowIso(),
        riskMode: deriveRiskMode(inputShape.riskTolerance),
        riskTolerance: inputShape.riskTolerance,
        investmentTimeframe: inputShape.timeframe,
        portfolioSegment: inputShape.portfolioSegment,
        walletAddress: payer,
      });

      void orchestrateAllocateJob(jobId).catch((error) => {
        console.error(`Allocate orchestration failed for ${jobId}:`, error);
      });
    }

    const acceptedAt = nowIso();
    stateStore.setAllocateRecord(decisionId as string, {
      decisionId: decisionId as string,
      inputFingerprint,
      inputs: inputShape,
      chargedAmountUsdc: chargeAmountUsdc,
      quoteIssuedAt: activeQuoteWindow.issuedAt,
      quoteExpiresAt: activeQuoteWindow.expiresAt,
      state: "accepted",
      createdAt: decisionScopedRecord?.createdAt ?? acceptedAt,
      updatedAt: acceptedAt,
      jobId,
      payment: {
        fromAddress: payer,
        transactionHash,
        network: settlement.network,
        verifiedAt: acceptedAt,
      },
    });
    if (!decisionScopedRecord?.jobId) {
      incrementAddressUsage(payer);
    }

    await recordPaidAllocationReferral({
      referralCode,
      transactionId: transactionHash,
      amountUsd: Number.parseFloat(chargeAmountUsdc),
      routePath: options.routePath,
      userId: payer,
      allocationAmountUsd: withReport ? Number.parseFloat(getAllocateChargeAmountUsdc(false)) : Number.parseFloat(chargeAmountUsdc),
      reportAmountUsd: withReport ? Number.parseFloat(getAllocateChargeAmountUsdc(true)) - Number.parseFloat(getAllocateChargeAmountUsdc(false)) : 0,
      paymentReference: transactionHash,
    });

    void sendAdminUsageEmail({
      channel: "x402_allocate",
      decisionId: decisionId as string,
      endpoint: options.routePath,
      productId: withReport ? "allocation_with_report" : "allocation_only",
      walletAddress: payer,
      resultEmail:
        typeof req.body?.resultEmail === "string" && isValidEmail(req.body.resultEmail.trim().toLowerCase())
          ? req.body.resultEmail.trim().toLowerCase()
          : null,
      promoCode: typeof req.body?.promoCode === "string" && req.body.promoCode.trim()
        ? req.body.promoCode.trim()
        : null,
      chargedAmountUsdc: chargeAmountUsdc,
      transactionHash,
      paymentMethod: "x402",
      paymentNetwork: settlement.network ?? null,
      includeCertifiedDecisionRecord: withReport,
      riskTolerance: inputShape.riskTolerance,
      timeframe: inputShape.timeframe,
      jobId,
      requestInput: inputShape,
      responseOutput: {
        success: true,
        executionModelVersion: EXECUTION_MODEL_VERSION,
        data: {
          status: decisionScopedRecord?.jobId ? "already_accepted" : "accepted",
          idempotentReplay: Boolean(decisionScopedRecord?.jobId),
          endpoint: options.routePath,
          jobId,
          decisionId,
          inputs: inputShape,
          payment: {
            required: true,
            chargedAmountUsdc: chargeAmountUsdc,
            verified: true,
            fromAddress: payer,
            transactionHash,
            network: settlement.network,
          },
          quoteExpiresAt: activeQuoteWindow.expiresAt,
          statusPath: `/execution-status/${encodeURIComponent(jobId)}`,
        },
      },
    }).catch((error) => {
      console.error("Failed to send Selun admin usage email (x402_allocate):", error);
    });

    attachBazaarDiscovery(res, options.discoveryExtension);
    applySettlementResponseHeaders(res, settlement);

    const statusPath = `/execution-status/${encodeURIComponent(jobId)}`;
    return res.status(202).json({
      success: true,
      executionModelVersion: EXECUTION_MODEL_VERSION,
      data: {
        status: decisionScopedRecord?.jobId ? "already_accepted" : "accepted",
        idempotentReplay: Boolean(decisionScopedRecord?.jobId),
        endpoint: options.routePath,
        jobId,
        decisionId,
        inputs: inputShape,
        payment: {
          required: true,
          chargedAmountUsdc: chargeAmountUsdc,
          verified: true,
          fromAddress: payer,
          transactionHash,
          network: settlement.network,
        },
        quoteExpiresAt: activeQuoteWindow.expiresAt,
        statusPath,
      },
      logs: getExecutionLogs(120),
    });
  } catch (error) {
    return failure(res, error, 500);
  }
}

router.post("/x402/allocate", async (req: Request, res: Response) =>
  handleX402AllocateRequest(req, res, {
    routePath: "/agent/x402/allocate",
    withReport: false,
    description: X402_PUBLIC_DESCRIPTIONS.allocate,
    discoveryExtension: buildAllocateDiscoveryExtension(),
  })
);

router.post("/x402/allocate-with-report", async (req: Request, res: Response) =>
  handleX402AllocateRequest(req, res, {
    routePath: "/agent/x402/allocate-with-report",
    withReport: true,
    description: X402_PUBLIC_DESCRIPTIONS.allocateWithReport,
    discoveryExtension: buildAllocateWithReportDiscoveryExtension(),
  })
);

router.post("/x402/market-regime", async (req: Request, res: Response) => {
  const paymentSignatureHeader = req.header("payment-signature")?.trim() || req.header("PAYMENT-SIGNATURE")?.trim();
  const isProbe = !paymentSignatureHeader;
  const riskTolerance = normalizeRiskTolerance(req.body?.riskTolerance) ?? (isProbe ? "Balanced" : null);
  const timeframe = normalizeTimeframe(req.body?.timeframe) ?? (isProbe ? "1-3_years" : null);
  const portfolioSegmentProvided = req.body?.portfolioSegment !== undefined;
  const portfolioSegment = normalizePortfolioSegment(req.body?.portfolioSegment) ?? "Bluechips";
  if (!riskTolerance) {
    return failure(res, new Error("riskTolerance must be one of Conservative | Balanced | Growth | Aggressive."), 400);
  }
  if (!timeframe) {
    return failure(res, new Error("timeframe must be one of <1_year | 1-3_years | 3+_years."), 400);
  }
  if (portfolioSegmentProvided && !normalizePortfolioSegment(req.body?.portfolioSegment)) {
    return failure(res, new Error(`portfolioSegment must be one of ${PORTFOLIO_SEGMENTS.join(" | ")}.`), 400);
  }

  const input: X402ToolBaseInput = { riskTolerance, timeframe, portfolioSegment };
  return handleX402ToolRequest(req, res, "market_regime", input, (jobId) =>
    executeToolProduct("market_regime", jobId, input)
  );
});

router.post("/x402/policy-envelope", async (req: Request, res: Response) => {
  const paymentSignatureHeader = req.header("payment-signature")?.trim() || req.header("PAYMENT-SIGNATURE")?.trim();
  const isProbe = !paymentSignatureHeader;
  const riskTolerance = normalizeRiskTolerance(req.body?.riskTolerance) ?? (isProbe ? "Balanced" : null);
  const timeframe = normalizeTimeframe(req.body?.timeframe) ?? (isProbe ? "1-3_years" : null);
  const portfolioSegmentProvided = req.body?.portfolioSegment !== undefined;
  const portfolioSegment = normalizePortfolioSegment(req.body?.portfolioSegment) ?? "Bluechips";
  if (!riskTolerance) {
    return failure(res, new Error("riskTolerance must be one of Conservative | Balanced | Growth | Aggressive."), 400);
  }
  if (!timeframe) {
    return failure(res, new Error("timeframe must be one of <1_year | 1-3_years | 3+_years."), 400);
  }
  if (portfolioSegmentProvided && !normalizePortfolioSegment(req.body?.portfolioSegment)) {
    return failure(res, new Error(`portfolioSegment must be one of ${PORTFOLIO_SEGMENTS.join(" | ")}.`), 400);
  }

  const input: X402ToolBaseInput = { riskTolerance, timeframe, portfolioSegment };
  return handleX402ToolRequest(req, res, "policy_envelope", input, (jobId) =>
    executeToolProduct("policy_envelope", jobId, input)
  );
});

router.post("/x402/asset-scorecard", async (req: Request, res: Response) => {
  const paymentSignatureHeader = req.header("payment-signature")?.trim() || req.header("PAYMENT-SIGNATURE")?.trim();
  const isProbe = !paymentSignatureHeader;
  const riskTolerance = normalizeRiskTolerance(req.body?.riskTolerance) ?? (isProbe ? "Balanced" : null);
  const timeframe = normalizeTimeframe(req.body?.timeframe) ?? (isProbe ? "1-3_years" : null);
  const portfolioSegmentProvided = req.body?.portfolioSegment !== undefined;
  const portfolioSegment = normalizePortfolioSegment(req.body?.portfolioSegment) ?? "Bluechips";
  if (!riskTolerance) {
    return failure(res, new Error("riskTolerance must be one of Conservative | Balanced | Growth | Aggressive."), 400);
  }
  if (!timeframe) {
    return failure(res, new Error("timeframe must be one of <1_year | 1-3_years | 3+_years."), 400);
  }
  if (portfolioSegmentProvided && !normalizePortfolioSegment(req.body?.portfolioSegment)) {
    return failure(res, new Error(`portfolioSegment must be one of ${PORTFOLIO_SEGMENTS.join(" | ")}.`), 400);
  }

  const input: AssetScorecardInput = {
    riskTolerance,
    timeframe,
    portfolioSegment,
    assets: normalizeStringArray(req.body?.assets, 12),
  };
  return handleX402ToolRequest(req, res, "asset_scorecard", input, (jobId) =>
    executeToolProduct("asset_scorecard", jobId, input)
  );
});

router.post("/x402/rebalance", async (req: Request, res: Response) => {
  const paymentSignatureHeader = req.header("payment-signature")?.trim() || req.header("PAYMENT-SIGNATURE")?.trim();
  const isProbe = !paymentSignatureHeader;
  const riskTolerance = normalizeRiskTolerance(req.body?.riskTolerance) ?? (isProbe ? "Balanced" : null);
  const timeframe = normalizeTimeframe(req.body?.timeframe) ?? (isProbe ? "1-3_years" : null);
  const portfolioSegmentProvided = req.body?.portfolioSegment !== undefined;
  const portfolioSegment = normalizePortfolioSegment(req.body?.portfolioSegment) ?? "Bluechips";
  if (!riskTolerance) {
    return failure(res, new Error("riskTolerance must be one of Conservative | Balanced | Growth | Aggressive."), 400);
  }
  if (!timeframe) {
    return failure(res, new Error("timeframe must be one of <1_year | 1-3_years | 3+_years."), 400);
  }
  if (portfolioSegmentProvided && !normalizePortfolioSegment(req.body?.portfolioSegment)) {
    return failure(res, new Error(`portfolioSegment must be one of ${PORTFOLIO_SEGMENTS.join(" | ")}.`), 400);
  }

  const holdings = normalizeRebalanceHoldings(req.body?.holdings) ?? (isProbe ? [{ asset: "BTC", name: "Bitcoin", usdValue: 1000, allocationPct: 100 }] : null);
  if (!holdings) {
    return failure(
      res,
      new Error("holdings must be a non-empty array of { asset, usdValue, optional name, optional allocationPct }."),
      400,
    );
  }

  const input: RebalanceInput = {
    riskTolerance,
    timeframe,
    portfolioSegment,
    holdings,
  };
  return handleX402ToolRequest(req, res, "rebalance", input, (jobId) =>
    executeToolProduct("rebalance", jobId, input)
  );
});

// --- SCE wrapper helpers ---

export function normalizeSceInputForContinuityMode(body: unknown): Record<string, unknown> {
  if (typeof body !== "object" || body === null) return {};
  const b = body as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of ["scope", "chain_id", "threat_family", "doctrine_tag", "requested_action"]) {
    if (b[key] !== undefined) out[key] = b[key];
  }
  return out;
}

export function normalizeSceInputForCaseRelevance(body: unknown): Record<string, unknown> {
  if (typeof body !== "object" || body === null) return {};
  const b = body as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of ["protocol_name", "chain_id", "asset_types", "threat_families", "doctrine_tags", "requested_action"]) {
    if (b[key] !== undefined) out[key] = b[key];
  }
  return out;
}

export function normalizeSceInputForRiskEvaluate(body: unknown): Record<string, unknown> {
  if (typeof body !== "object" || body === null) return {};
  const b = body as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of ["protocol_name", "chain_id", "addresses", "asset_types", "threat_families", "doctrine_tags", "requested_action", "risk_tolerance"]) {
    if (b[key] !== undefined) out[key] = b[key];
  }
  return out;
}

export function buildSceWrappedResult(
  sceResponse: Record<string, unknown>,
  sceSourceEndpoint: string,
): Record<string, unknown> {
  return {
    sce: sceResponse,
    sceSourceEndpoint,
    sceEvaluationId: typeof sceResponse.evaluation_id === "string" ? sceResponse.evaluation_id : null,
    validUntil: typeof sceResponse.valid_until === "string" ? sceResponse.valid_until : null,
    selunWrapper: true,
  };
}

function sanitizeSceError(error: unknown): Error {
  if (error instanceof Error) {
    if (error.message.startsWith("SCE upstream returned HTTP")) return new Error("sce_upstream_error");
    if (error.message.startsWith("SCE upstream timed out")) return new Error("sce_upstream_timeout");
    if (error.message.includes("non-JSON")) return new Error("sce_upstream_error");
  }
  return new Error("sce_upstream_unavailable");
}

// --- SCE continuity-mode in-memory result cache ---
// Payments are always verified. Only the upstream SCE call is cached.

const SCE_CONTINUITY_MODE_CACHE_MAX_ENTRIES = 1_000;
const SCE_CONTINUITY_MODE_CACHE_MAX_TTL_MS = 60_000;

type SceContinuityModeCacheEntry = {
  result: Record<string, unknown>;
  expiresAt: number;
};

const sceContinuityModeCache = new Map<string, SceContinuityModeCacheEntry>();

// Exported for test setup and teardown only.
export const _sceContinuityModeCacheForTest = sceContinuityModeCache;

export function buildSceCacheKey(input: Record<string, unknown>): string {
  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(input).sort()) {
    sorted[key] = input[key];
  }
  return JSON.stringify(sorted);
}

export function getSceContinuityModeFromCache(key: string): Record<string, unknown> | null {
  const entry = sceContinuityModeCache.get(key);
  if (!entry) return null;
  if (Date.now() >= entry.expiresAt) {
    sceContinuityModeCache.delete(key);
    return null;
  }
  return entry.result;
}

export function setSceContinuityModeInCache(key: string, result: Record<string, unknown>): void {
  if (sceContinuityModeCache.size >= SCE_CONTINUITY_MODE_CACHE_MAX_ENTRIES) {
    const oldest = sceContinuityModeCache.keys().next().value;
    if (oldest !== undefined) sceContinuityModeCache.delete(oldest);
  }
  const validUntilMs = typeof result.validUntil === "string" ? Date.parse(result.validUntil) : NaN;
  const ttlFromValidUntil = Number.isFinite(validUntilMs) && validUntilMs > Date.now()
    ? validUntilMs
    : NaN;
  const expiresAt = Number.isFinite(ttlFromValidUntil)
    ? Math.min(ttlFromValidUntil, Date.now() + SCE_CONTINUITY_MODE_CACHE_MAX_TTL_MS)
    : Date.now() + SCE_CONTINUITY_MODE_CACHE_MAX_TTL_MS;
  sceContinuityModeCache.set(key, { result, expiresAt });
}

export function clearSceContinuityModeCache(): void {
  sceContinuityModeCache.clear();
}

// --- SCE x402 wrapper routes ---

router.post("/x402/sce/continuity-mode", async (req: Request, res: Response) => {
  const sceInput = normalizeSceInputForContinuityMode(req.body);
  return handleX402ToolRequest(
    req,
    res,
    "sce_continuity_mode",
    sceInput,
    async (_jobId) => {
      const cacheKey = buildSceCacheKey(sceInput);
      const cached = getSceContinuityModeFromCache(cacheKey);
      if (cached) return cached;
      try {
        const sceResponse = await callSceContinuityMode(sceInput);
        const result = buildSceWrappedResult(sceResponse, "/v1/sce/continuity-mode");
        setSceContinuityModeInCache(cacheKey, result);
        return result;
      } catch (error) {
        console.error("SCE continuity-mode upstream error:", error instanceof Error ? error.message : error);
        throw sanitizeSceError(error);
      }
    },
    { skipPhase1: true, usageChannel: "x402_sce" },
  );
});

router.post("/x402/sce/case-relevance", async (req: Request, res: Response) => {
  const sceInput = normalizeSceInputForCaseRelevance(req.body);
  return handleX402ToolRequest(
    req,
    res,
    "sce_case_relevance",
    sceInput,
    async (_jobId) => {
      try {
        const sceResponse = await callSceCaseRelevance(sceInput);
        return buildSceWrappedResult(sceResponse, "/v1/sce/case-relevance");
      } catch (error) {
        console.error("SCE case-relevance upstream error:", error instanceof Error ? error.message : error);
        throw sanitizeSceError(error);
      }
    },
    { skipPhase1: true, usageChannel: "x402_sce" },
  );
});

router.post("/x402/sce/risk-evaluate", async (req: Request, res: Response) => {
  const sceInput = normalizeSceInputForRiskEvaluate(req.body);
  return handleX402ToolRequest(
    req,
    res,
    "sce_risk_evaluate",
    sceInput,
    async (_jobId) => {
      try {
        const sceResponse = await callSceRiskEvaluate(sceInput);
        return buildSceWrappedResult(sceResponse, "/v1/sce/risk/evaluate");
      } catch (error) {
        console.error("SCE risk-evaluate upstream error:", error instanceof Error ? error.message : error);
        throw sanitizeSceError(error);
      }
    },
    { skipPhase1: true, usageChannel: "x402_sce" },
  );
});

router.post("/x402/sce/escalation-brief", (req: Request, res: Response) => {
  void sendAdminInterestEmail("/agent/x402/sce/escalation-brief", req.body);
  return res.status(501).json({
    success: false,
    error: "endpoint_not_yet_available",
    message:
      "SCE Escalation Brief is not yet available as a Selun x402 endpoint. " +
      "Your interest has been recorded. Check back for updates.",
    recommended_action: "proceed_with_caution",
    selunWrapper: true,
  });
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
  const portfolioSegment = typeof req.body?.portfolioSegment === "string" ? req.body.portfolioSegment : undefined;
  const walletAddress = typeof req.body?.walletAddress === "string" ? req.body.walletAddress : undefined;

  try {
    runPhase1({
      jobId,
      executionTimestamp,
      riskMode,
      riskTolerance,
      investmentTimeframe,
      portfolioSegment,
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
