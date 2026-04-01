export type StripeWizardRiskMode = "Conservative" | "Balanced" | "Growth" | "Aggressive";
export type StripeWizardInvestmentHorizon = "< 1 Year" | "1-3 Years" | "3+ Years";
export type StripeWizardPortfolioSegment = "Bluechips" | "Memecoins" | "Gaming" | "Yield Farm";

type ExecutionStatusPayload = {
  success?: boolean;
  status?: "idle" | "starting" | "in_progress" | "complete" | "failed" | "not_found";
  error?: string;
};

type Phase1StartPayload = {
  status?: string;
  error?: string;
};

export type EnsureStripeWizardPhase1StartedInput = {
  decisionId: string;
  riskMode: string | null | undefined;
  investmentHorizon: string | null | undefined;
  portfolioSegment: string | null | undefined;
};

export type EnsureStripeWizardPhase1StartedResult = {
  jobId: string;
  statusPath: string;
  executionStatus: "idle" | "starting" | "in_progress" | "complete" | "failed" | null;
  started: boolean;
};

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

function isRiskMode(value: string | null | undefined): value is StripeWizardRiskMode {
  return value === "Conservative" || value === "Balanced" || value === "Growth" || value === "Aggressive";
}

function isInvestmentHorizon(value: string | null | undefined): value is StripeWizardInvestmentHorizon {
  return value === "< 1 Year" || value === "1-3 Years" || value === "3+ Years";
}

function isPortfolioSegment(value: string | null | undefined): value is StripeWizardPortfolioSegment {
  return value === "Bluechips" || value === "Memecoins" || value === "Gaming" || value === "Yield Farm";
}

function toBackendRiskMode(mode: StripeWizardRiskMode): "conservative" | "neutral" | "aggressive" {
  if (mode === "Conservative") return "conservative";
  if (mode === "Aggressive" || mode === "Growth") return "aggressive";
  return "neutral";
}

function toBackendRiskTolerance(mode: StripeWizardRiskMode): "conservative" | "balanced" | "growth" | "aggressive" {
  if (mode === "Conservative") return "conservative";
  if (mode === "Growth") return "growth";
  if (mode === "Aggressive") return "aggressive";
  return "balanced";
}

function toBackendTimeWindow(horizon: StripeWizardInvestmentHorizon): "7d" | "30d" {
  if (horizon === "< 1 Year") return "7d";
  return "30d";
}

function toBackendInvestmentTimeframe(
  horizon: StripeWizardInvestmentHorizon,
): "<1_year" | "1-3_years" | "3+_years" {
  if (horizon === "< 1 Year") return "<1_year";
  if (horizon === "3+ Years") return "3+_years";
  return "1-3_years";
}

function normalizeDecisionId(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error("decisionId is required to start Stripe-backed fulfillment.");
  }
  return trimmed;
}

export function buildStripeWizardJobId(decisionId: string): string {
  return `selun-phase1-${normalizeDecisionId(decisionId)}`.replace(/[^A-Za-z0-9-_]/g, "-");
}

async function fetchExecutionStatus(jobId: string): Promise<ExecutionStatusPayload | null> {
  const response = await fetch(`${getBackendBaseUrl()}/execution-status/${encodeURIComponent(jobId)}`, {
    method: "GET",
    cache: "no-store",
  });

  if (response.status === 404) {
    return null;
  }

  const payload = (await response.json().catch(() => ({}))) as ExecutionStatusPayload;
  if (!response.ok) {
    throw new Error(payload.error || "Unable to read execution status.");
  }

  return payload;
}

async function startPhase1Job(params: {
  jobId: string;
  riskMode: StripeWizardRiskMode;
  investmentHorizon: StripeWizardInvestmentHorizon;
  portfolioSegment: StripeWizardPortfolioSegment;
}): Promise<void> {
  const response = await fetch(`${getBackendBaseUrl()}/agent/phase1/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      jobId: params.jobId,
      executionTimestamp: new Date().toISOString(),
      riskMode: toBackendRiskMode(params.riskMode),
      riskTolerance: toBackendRiskTolerance(params.riskMode),
      investmentTimeframe: toBackendInvestmentTimeframe(params.investmentHorizon),
      portfolioSegment: params.portfolioSegment,
      timeWindow: toBackendTimeWindow(params.investmentHorizon),
    }),
    cache: "no-store",
  });

  const payload = (await response.json().catch(() => ({}))) as Phase1StartPayload;
  if (!response.ok || payload.status !== "started") {
    throw new Error(payload.error || "Unable to start Stripe-backed Phase 1.");
  }
}

export async function ensureStripeWizardPhase1Started(
  input: EnsureStripeWizardPhase1StartedInput,
): Promise<EnsureStripeWizardPhase1StartedResult> {
  const decisionId = normalizeDecisionId(input.decisionId);
  if (!isRiskMode(input.riskMode)) {
    throw new Error("Stripe fulfillment is missing a valid risk mode.");
  }
  if (!isInvestmentHorizon(input.investmentHorizon)) {
    throw new Error("Stripe fulfillment is missing a valid investment horizon.");
  }
  if (!isPortfolioSegment(input.portfolioSegment)) {
    throw new Error("Stripe fulfillment is missing a valid portfolio segment.");
  }

  const jobId = buildStripeWizardJobId(decisionId);
  const statusPath = `/api/agent/execution-status/${encodeURIComponent(jobId)}`;
  const existingStatus = await fetchExecutionStatus(jobId);

  if (existingStatus && existingStatus.status && existingStatus.status !== "not_found" && existingStatus.status !== "idle") {
    return {
      jobId,
      statusPath,
      executionStatus: existingStatus.status,
      started: false,
    };
  }

  await startPhase1Job({
    jobId,
    riskMode: input.riskMode,
    investmentHorizon: input.investmentHorizon,
    portfolioSegment: input.portfolioSegment,
  });

  return {
    jobId,
    statusPath,
    executionStatus: existingStatus?.status === "idle" ? "starting" : "starting",
    started: true,
  };
}
