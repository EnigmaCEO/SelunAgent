"use client";

import Image from "next/image";
import Link from "next/link";
import { Fragment, useCallback, useEffect, useRef, useState } from "react";

type WizardState = "CONFIGURE" | "REVIEW" | "PROCESSING" | "COMPLETE";

type ProcessingStep =
  | "SIGNAL_PULL"
  | "REGIME_CLASSIFICATION"
  | "ASSET_EXPANSION"
  | "ASSET_SCREENING"
  | "ASSET_SELECTION"
  | "ALLOCATION_CONSTRUCTION"
  | "REPORT_GENERATION";

type RiskMode = "Conservative" | "Balanced" | "Growth" | "Aggressive";

type InvestmentHorizon = "< 1 Year" | "1-3 Years" | "3+ Years";

type AllocationRow = {
  asset: string;
  name: string;
  category: string;
  riskClass: string;
  allocationPct: number;
};

type ProcessingStepMeta = {
  key: ProcessingStep;
  label: string;
};

type AgentPaymentReceipt = {
  transactionId: string;
  decisionId: string;
  agentNote: string;
  chargedAmountUsdc: number;
  certifiedDecisionRecordPurchased: boolean;
  paymentMethod: "onchain" | "free_code";
  freeCodeApplied: boolean;
};

type AgentPaymentResponse = {
  success: boolean;
  status?: "paid";
  transactionId?: string;
  decisionId?: string;
  agentNote?: string;
  chargedAmountUsdc?: string;
  certifiedDecisionRecordPurchased?: boolean;
  paymentMethod?: "onchain" | "free_code";
  freeCodeApplied?: boolean;
  error?: string;
};

type PaymentQuoteResponse = {
  success: boolean;
  totalBeforeDiscountUsdc?: string;
  chargedAmountUsdc?: string;
  discountAmountUsdc?: string;
  discountPercent?: number;
  promoCodeApplied?: boolean;
  promoCode?: string;
  certifiedDecisionRecordPurchased?: boolean;
  paymentMethod?: "onchain" | "free_code";
  message?: string;
  error?: string;
};

type PromoQuoteResult = {
  totalBeforeDiscountUsdc: number;
  chargedAmountUsdc: number;
  discountAmountUsdc: number;
  discountPercent: number;
  promoCodeApplied: boolean;
  promoCode?: string;
  certifiedDecisionRecordPurchased: boolean;
  paymentMethod: "onchain" | "free_code";
  message: string;
};

type UsdcBalanceResponse = {
  success?: boolean;
  error?: string;
  data?: {
    walletAddress?: string;
    network?: string;
    usdcContractAddress?: string;
    usdcBalance?: string;
    usdcBalanceBaseUnits?: string;
  };
};

type PricingResponse = {
  success?: boolean;
  error?: string;
  data?: {
    structuredAllocationPriceUsdc?: number;
    certifiedDecisionRecordFeeUsdc?: number;
  };
};

type AgentWalletResponse = {
  success?: boolean;
  error?: string;
  data?: {
    walletAddress?: string;
    network?: string;
    usdc?: {
      contractAddress?: string;
      balance?: string;
      balanceBaseUnits?: string;
    };
  };
};

type VerifyPaymentResponse = {
  success?: boolean;
  error?: string;
  data?: {
    transactionHash?: string;
    amount?: string;
    confirmed?: boolean;
    blockNumber?: number;
  };
};

type Phase1RunStartResponse = {
  status?: string;
  phase?: string;
  success?: boolean;
  error?: string;
};

type ExecutionStatusLog = {
  phase: string;
  subPhase?: string;
  status: "in_progress" | "complete" | "failed";
  timestamp: string;
  startedAt?: string;
  completedAt?: string;
  error?: string;
};

type Phase1Output = {
  timestamp: string;
  execution_model_version: "Selun-1.0.0";
  doctrine_version: "SELUN-SIGNAL-1.0";
  market_condition: {
    volatility_state: "low" | "moderate" | "elevated" | "extreme";
    liquidity_state: "weak" | "stable" | "strong";
    risk_appetite: "defensive" | "neutral" | "expansionary";
    sentiment_direction: number;
    sentiment_alignment: number;
    public_echo_strength: number;
    confidence: number;
    uncertainty: number;
  };
  evidence: {
    volatility_metrics: {
      btc_volatility_24h: number;
      eth_volatility_24h: number;
      volatility_zscore: number;
    };
    liquidity_metrics: {
      total_volume_24h: number;
      volume_deviation_zscore: number;
      avg_spread: number;
      stablecoin_dominance: number;
    };
    sentiment_metrics: {
      headline_count: number;
      aggregate_sentiment_score: number;
      engagement_deviation: number;
      fear_greed_index: number;
      fear_greed_available: boolean;
    };
  };
  allocation_authorization: {
    status: "AUTHORIZED" | "DEFERRED" | "PROHIBITED";
    confidence: number;
    justification: string[];
  };
  phase_boundaries: {
    asset_evaluation: "PHASE_3";
    portfolio_construction: "PHASE_4";
  };
  audit: {
    sources: Array<{
      id: string;
      provider: string;
      endpoint: string;
      url: string;
      fetched_at: string;
    }>;
    data_freshness: string;
    missing_domains: string[];
    assumptions: string[];
    source_credibility: Array<{
      domain: "volatility" | "liquidity" | "sentiment" | "market_metrics";
      provider: string;
      score: number;
      successes: number;
      failures: number;
      last_success_at: string | null;
      last_failure_at: string | null;
      avg_latency_ms: number;
    }>;
    source_selection: Array<{
      domain: "volatility" | "liquidity" | "sentiment" | "market_metrics";
      selected: string[];
      rejected: Array<{ id: string; reason: string }>;
      rationale: string[];
    }>;
  };
};

type Phase2Output = {
  timestamp: string;
  execution_model_version: "Selun-1.0.0";
  doctrine_version: "SELUN-POLICY-1.0";
  inputs: {
    market_condition_ref: string;
    user_profile: {
      risk_tolerance: "Conservative" | "Balanced" | "Growth" | "Aggressive";
      investment_timeframe: "<1_year" | "1-3_years" | "3+_years";
    };
  };
  allocation_policy: {
    mode: "capital_preservation" | "balanced_defensive" | "balanced_growth" | "offensive_growth";
    defensive_bias_adjustment: number;
  };
  policy_envelope: {
    risk_budget: number;
    risk_scaling_factor: number;
    exposure_caps: {
      max_single_asset_exposure: number;
      high_volatility_asset_cap: number;
    };
    stablecoin_minimum: number;
    portfolio_volatility_target: number;
    liquidity_floor_requirement: "tier_1_only" | "tier_1_plus_tier_2" | "broad_liquidity_ok";
    volatility_ceiling: number;
    capital_preservation_bias: number;
    defensive_adjustment_applied: boolean;
  };
  allocation_authorization: {
    status: "AUTHORIZED" | "RESTRICTED" | "PROHIBITED";
    reason: string;
    confidence: number;
  };
  phase_boundaries: {
    asset_universe_expansion: "PHASE_3";
    portfolio_construction: "PHASE_4";
  };
  audit: {
    phase1_timestamp_ref: string;
    policy_rules_applied: string[];
    agent_judgement: {
      used: boolean;
      model: string | null;
      posture: "more_defensive" | "neutral" | "selective_risk_on" | null;
      authorization_hint: "NO_CHANGE" | "TIGHTEN" | "RELAX" | null;
      reason_codes: string[];
      skipped_reason: string | null;
    };
  };
};

type Phase3Output = {
  timestamp: string;
  execution_model_version: "Selun-1.0.0";
  doctrine_version: "SELUN-UNIVERSE-1.0";
  inputs: {
    phase2_policy_ref: string;
    user_profile: {
      risk_tolerance: "Conservative" | "Balanced" | "Growth" | "Aggressive";
      investment_timeframe: "<1_year" | "1-3_years" | "3+_years";
    };
    top_volume_target: number;
    volume_window_days: [7, 30];
  };
  universe: {
    top_volume_candidates_count: number;
    profile_match_candidates_count: number;
    total_candidates_count: number;
    tokens: Array<{
      coingecko_id: string;
      symbol: string;
      name: string;
      market_cap_rank: number | null;
      volume_24h_usd: number;
      volume_7d_estimated_usd: number;
      volume_30d_estimated_usd: number;
      source_tags: string[];
      profile_match_reasons: string[];
    }>;
  };
  phase_boundaries: {
    asset_screening: "PHASE_4";
    portfolio_construction: "PHASE_4";
  };
  audit: {
    sources: Array<{
      id: string;
      provider: string;
      endpoint: string;
      url: string;
      fetched_at: string;
    }>;
    selection_rules: string[];
    missing_domains: string[];
    agent_profile_match: {
      used: boolean;
      model: string | null;
      reason_codes: string[];
      skipped_reason: string | null;
    };
  };
};

type Phase4Output = {
  timestamp: string;
  execution_model_version: "Selun-1.0.0";
  doctrine_version: "SELUN-SCREENING-1.0";
  inputs: {
    phase3_universe_ref: string;
    phase2_policy_ref: string;
    user_profile: {
      risk_tolerance: "Conservative" | "Balanced" | "Growth" | "Aggressive";
      investment_timeframe: "<1_year" | "1-3_years" | "3+_years";
    };
    screening_thresholds: {
      min_liquidity_score: number;
      min_structural_score: number;
      min_screening_score: number;
      min_volume_24h_usd: number;
      target_eligible_count: number;
      allow_low_depth: boolean;
      rank_sanity_threshold: number;
    };
  };
  screening: {
    total_candidates_count: number;
    excluded_by_phase3_count: number;
    evaluated_candidates_count: number;
    eligible_candidates_count: number;
    tokens: Array<{
      coingecko_id: string;
      symbol: string;
      name: string;
      market_cap_rank: number | null;
      token_category: "core" | "stablecoin" | "meme" | "proxy_or_wrapped" | "alt" | "unknown";
      rank_bucket: "top_100" | "top_500" | "long_tail" | "unknown";
      exchange_depth_proxy: "high" | "medium" | "low" | "unknown";
      stablecoin_validation_state: "not_stablecoin" | "trusted_stablecoin" | "unverified_stablecoin";
      liquidity_score: number;
      structural_score: number;
      screening_score: number;
      eligible: boolean;
      exclusion_reasons: string[];
      profile_match_reasons: string[];
    }>;
  };
  phase_boundaries: {
    risk_quality_evaluation: "PHASE_5";
    portfolio_construction: "PHASE_5";
  };
  audit: {
    selection_rules: string[];
    missing_domains: string[];
    agent_screening: {
      used: boolean;
      model: string | null;
      reason_codes: string[];
      skipped_reason: string | null;
    };
  };
};

type Phase5Output = {
  timestamp: string;
  execution_model_version: "Selun-1.0.0";
  doctrine_version: "SELUN-QUALITY-1.0";
  inputs: {
    phase4_screening_ref: string;
    phase2_policy_ref: string;
    user_profile: {
      risk_tolerance: "Conservative" | "Balanced" | "Growth" | "Aggressive";
      investment_timeframe: "<1_year" | "1-3_years" | "3+_years";
    };
    portfolio_constraints: {
      risk_budget: number;
      stablecoin_minimum: number;
      max_single_asset_exposure: number;
      high_volatility_asset_cap: number;
    };
  };
  evaluation: {
    screened_candidates_count: number;
    qualified_candidates_count: number;
    selected_candidates_count: number;
    tokens: Array<{
      coingecko_id: string;
      symbol: string;
      name: string;
      market_cap_rank: number | null;
      token_category: "core" | "stablecoin" | "meme" | "proxy_or_wrapped" | "alt" | "unknown";
      rank_bucket: "top_100" | "top_500" | "long_tail" | "unknown";
      exchange_depth_proxy: "high" | "medium" | "low" | "unknown";
      stablecoin_validation_state: "not_stablecoin" | "trusted_stablecoin" | "unverified_stablecoin";
      profile_match_reasons: string[];
      liquidity_score: number;
      structural_score: number;
      quality_score: number;
      risk_score: number;
      risk_class:
        | "stablecoin"
        | "large_cap_crypto"
        | "defi_bluechip"
        | "large_cap_equity_core"
        | "defensive_equity"
        | "growth_high_beta_equity"
        | "high_risk"
        | "equity_fund"
        | "fixed_income"
        | "commodities"
        | "real_estate"
        | "cash_equivalent"
        | "speculative"
        | "traditional_asset"
        | "alternative"
        | "balanced_fund"
        | "emerging_market"
        | "frontier_market"
        | "esoteric"
        | "unclassified"
        | "wealth_management"
        | "fund_of_funds"
        | "index_fund";
      role: "core" | "satellite" | "defensive" | "liquidity" | "carry" | "speculative";
      profitability: number;
      volatility: number;
      volatility_proxy_score: number;
      drawdown_proxy_score: number;
      stablecoin_risk_modifier: number;
      composite_score: number;
      selection_bucket: "stablecoin" | "core" | "satellite" | "high_volatility";
      selected: boolean;
      selection_reasons: string[];
    }>;
  };
  phase_boundaries: {
    portfolio_construction: "PHASE_6";
    decision_report: "POST_PHASE_6";
  };
};

type Phase6Output = {
  timestamp: string;
  execution_model_version: "Selun-1.0.0";
  doctrine_version: "SELUN-ALLOCATION-1.0";
  allocation: {
    shortlisted_candidates_count: number;
    selected_candidates_count: number;
    allocations: Array<{
      coingecko_id: string;
      symbol: string;
      name: string;
      bucket: "stablecoin" | "core" | "satellite" | "high_volatility";
      allocation_weight: number;
    }>;
    total_allocation_weight: number;
    stablecoin_allocation: number;
    expected_portfolio_volatility: number;
    concentration_index: number;
  };
};

type AaaAllocateDispatch = {
  status: "idle" | "in_progress" | "complete" | "failed";
  requestedAt?: string;
  completedAt?: string;
  endpoint?: string;
  httpStatus?: number;
  response?: unknown;
  error?: string;
};

type ResultEmailDeliveryStatus = "idle" | "sending" | "sent" | "failed";

type ResultEmailDeliveryResponse = {
  success?: boolean;
  status?: "sent" | "skipped" | "failed";
  error?: string;
};

type ExecutionStatusResponse = {
  success?: boolean;
  error?: string;
  found?: boolean;
  status?: string;
  logs?: ExecutionStatusLog[];
  jobContext?: {
    phase1?: {
      status?: string;
      output?: Phase1Output;
      error?: string;
    };
    phase2?: {
      status?: "idle" | "in_progress" | "complete" | "failed";
      triggeredAt?: string;
      completedAt?: string;
      output?: Phase2Output;
      error?: string;
    };
    phase3?: {
      status?: "idle" | "in_progress" | "complete" | "failed";
      triggeredAt?: string;
      completedAt?: string;
      output?: Phase3Output;
      error?: string;
    };
    phase4?: {
      status?: "idle" | "in_progress" | "complete" | "failed";
      triggeredAt?: string;
      completedAt?: string;
      output?: Phase4Output;
      error?: string;
    };
    phase5?: {
      status?: "idle" | "in_progress" | "complete" | "failed";
      triggeredAt?: string;
      completedAt?: string;
      output?: Phase5Output;
      error?: string;
    };
    phase6?: {
      status?: "idle" | "in_progress" | "complete" | "failed";
      triggeredAt?: string;
      completedAt?: string;
      output?: Phase6Output;
      error?: string;
      aaaAllocate?: AaaAllocateDispatch;
    };
  };
};

type ChainConfig = {
  chainIdHex: string;
  chainName: string;
  nativeCurrency: { name: string; symbol: string; decimals: number };
  rpcUrls: string[];
  blockExplorerUrls: string[];
};

type ProviderRpcError = {
  code?: number;
  message?: string;
  data?: unknown;
  cause?: unknown;
};

type EthereumProvider = {
  request: (args: { method: string; params?: unknown[] | object }) => Promise<unknown>;
};

declare global {
  interface Window {
    ethereum?: EthereumProvider;
  }
}

const RISK_MODES: RiskMode[] = ["Conservative", "Balanced", "Growth", "Aggressive"];
const HORIZONS: InvestmentHorizon[] = ["< 1 Year", "1-3 Years", "3+ Years"];
const DEFAULT_RISK_MODE: RiskMode = "Balanced";
const DEFAULT_HORIZON: InvestmentHorizon = "1-3 Years";
const PROCESSING_POLL_INTERVAL_MS = 1500;
const WIZARD_FLOW: WizardState[] = ["CONFIGURE", "REVIEW", "PROCESSING", "COMPLETE"];
const FALLBACK_BASE_PRICE_USDC = 19;
const FALLBACK_CERTIFIED_DECISION_RECORD_FEE_USDC = 15;

const NETWORK_LABELS: Record<string, string> = {
  "base-mainnet": "Base Mainnet",
  "base-sepolia": "Base Sepolia",
};
const CHAIN_CONFIGS: Record<string, ChainConfig> = {
  "base-mainnet": {
    chainIdHex: "0x2105",
    chainName: "Base",
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: ["https://mainnet.base.org"],
    blockExplorerUrls: ["https://basescan.org"],
  },
  "base-sepolia": {
    chainIdHex: "0x14a34",
    chainName: "Base Sepolia",
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: ["https://sepolia.base.org"],
    blockExplorerUrls: ["https://sepolia.basescan.org"],
  },
};

const PROCESSING_STEPS: ProcessingStepMeta[] = [
  { key: "SIGNAL_PULL", label: "Reviewing Market Condition" },
  { key: "REGIME_CLASSIFICATION", label: "Determining Allocation Policy" },
  { key: "ASSET_EXPANSION", label: "Expanding Eligible Asset Universe"},
  { key: "ASSET_SCREENING", label: "Screening for Liquidity & Structural Stability" },
  { key: "ASSET_SELECTION", label: "Evaluating Asset Risk & Quality" },
  { key: "ALLOCATION_CONSTRUCTION", label: "Constructing Portfolio Allocation" },
  { key: "REPORT_GENERATION", label: "Preparing Certified Decision Report" },
];

const REGIME_DETECTED = "Late-cycle risk-on with selective defensive ballast";

const PHASE1_SUB_PHASE_LABELS: Record<string, string> = {
  collecting_market_volatility_data: "Collecting market volatility data",
  collecting_liquidity_metrics: "Collecting liquidity metrics",
  collecting_macro_sentiment_data: "Collecting macro sentiment data",
  evaluating_market_alignment: "Evaluating market alignment",
  finalizing_market_snapshot: "Finalizing market snapshot",
};
const PHASE3_SUB_PHASE_LABELS: Record<string, string> = {
  engaging_selun_agent_for_universe: "Engaging Selun agent for profile context",
  collecting_top_volume_universe: "Collecting top-volume token universe",
  discovering_profile_match_candidates: "Discovering profile-matching tokens",
  finalizing_universe_snapshot: "Finalizing eligible universe snapshot",
};

const BUCKET_LABELS: Record<Phase6Output["allocation"]["allocations"][number]["bucket"], string> = {
  stablecoin: "Stability",
  core: "Core",
  satellite: "Satellite",
  high_volatility: "High Volatility",
};

const STABILITY_RISK_CLASSES = new Set(["stablecoin", "cash_equivalent", "defensive_stablecoin", "treasury"]);
const CORE_SYMBOLS = new Set(["BTC", "ETH"]);
const PORTFOLIO_ROLE_TOOLTIPS: Record<string, string> = {
  Core: "Main long-term holdings that anchor the portfolio.",
  Carry: "Income-oriented positions designed for steady return carry.",
  "Income Position": "Income-oriented positions designed for steady return carry.",
  Defensive: "Lower-volatility positions focused on protecting capital.",
  "Stable Holding": "Lower-volatility positions focused on protecting capital.",
  "Stable Holdings": "Lower-volatility positions focused on protecting capital.",
  Liquidity: "Cash-like reserves kept for flexibility and rebalancing.",
  "Liquidity Reserve": "Cash-like reserves kept for flexibility and rebalancing.",
  Satellite: "Smaller supporting positions with higher upside and risk.",
  "Growth Position": "Smaller supporting positions with higher upside and risk.",
  "Growth Positions": "Smaller supporting positions with higher upside and risk.",
  Speculative: "Highest-risk positions kept small with tighter limits.",
  "High-Risk Position": "Highest-risk positions kept small with tighter limits.",
  "High-Risk Positions": "Highest-risk positions kept small with tighter limits.",
  Stability: "Stable-value holdings meant to reduce drawdowns.",
  "High Volatility": "High-volatility positions kept under strict limits.",
  Allocation: "Fallback label used when role data is not available.",
};

type Phase1Status = "idle" | "starting" | "in_progress" | "complete" | "failed";
type SubPhaseStatus = "pending" | "in_progress" | "complete" | "failed";

const createPhase1SubPhaseStates = (): Record<string, SubPhaseStatus> => ({
  collecting_market_volatility_data: "pending",
  collecting_liquidity_metrics: "pending",
  collecting_macro_sentiment_data: "pending",
  evaluating_market_alignment: "pending",
  finalizing_market_snapshot: "pending",
});

const createPhase3SubPhaseStates = (): Record<string, SubPhaseStatus> => ({
  engaging_selun_agent_for_universe: "pending",
  collecting_top_volume_universe: "pending",
  discovering_profile_match_candidates: "pending",
  finalizing_universe_snapshot: "pending",
});

const shortenAddress = (address: string) => `${address.slice(0, 6)}...${address.slice(-4)}`;
const formatUsdcValue = (value: number) =>
  new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  }).format(value);

function formatMarketRegimeLabel(phase1Output: Phase1Output | null): string {
  if (!phase1Output) return REGIME_DETECTED;
  const condition = phase1Output.market_condition;
  const appetiteLabel =
    condition.risk_appetite === "expansionary"
      ? "Expansionary"
      : condition.risk_appetite === "defensive"
        ? "Defensive"
        : "Neutral";

  return `${appetiteLabel} market condition | Volatility ${condition.volatility_state} | Liquidity ${condition.liquidity_state}`;
}

type AllocationEntry = {
  asset: string;
  name: string;
  category: string;
  riskClass: string;
  weight: number;
};

type AllocationAssetMetadata = {
  name: string;
  category: string;
  riskClass: string;
};

function normalizeAssetSymbol(value: unknown): string {
  return typeof value === "string" ? value.trim().toUpperCase() : "";
}

function normalizeAssetName(value: unknown, fallback: string): string {
  if (typeof value === "string" && value.trim()) return value.trim();
  return fallback;
}

function normalizeRiskClass(value: unknown): string {
  if (typeof value === "string" && value.trim()) return value.trim().toLowerCase();
  return "unknown";
}

function parseNumericWeight(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

function formatPortfolioRoleLabel(
  role: unknown,
  options?: {
    symbol?: string;
    riskClass?: string;
    bucket?: Phase6Output["allocation"]["allocations"][number]["bucket"];
  },
): string {
  const normalizedRole = typeof role === "string" ? role.trim().toLowerCase() : "";
  if (normalizedRole === "core") return "Core";
  if (normalizedRole === "carry") return "Carry";
  if (normalizedRole === "defensive") return "Defensive";
  if (normalizedRole === "liquidity") return "Liquidity";
  if (normalizedRole === "satellite") return "Satellite";
  if (normalizedRole === "speculative") return "Speculative";
  if (normalizedRole === "stability") return "Stability";
  if (normalizedRole === "high_volatility" || normalizedRole === "high volatility") return "High Volatility";

  if (options?.bucket) {
    return BUCKET_LABELS[options.bucket] ?? "Allocation";
  }

  const riskClass = normalizeRiskClass(options?.riskClass);
  const symbol = normalizeAssetSymbol(options?.symbol);
  if (STABILITY_RISK_CLASSES.has(riskClass)) return "Defensive";
  if (CORE_SYMBOLS.has(symbol)) return "Core";
  return "Allocation";
}

function aggregateAllocationEntries(entries: AllocationEntry[]): AllocationRow[] {
  const byAsset = new Map<string, AllocationEntry>();

  for (const entry of entries) {
    const symbol = normalizeAssetSymbol(entry.asset);
    if (!symbol || entry.weight <= 0) continue;
    const existing = byAsset.get(symbol);
    if (existing) {
      existing.weight += entry.weight;
      if (existing.category === "Allocation" && entry.category !== "Allocation") {
        existing.category = entry.category;
      }
      if ((!existing.name || existing.name === existing.asset) && entry.name && entry.name !== entry.asset) {
        existing.name = entry.name;
      }
      if ((existing.riskClass === "unknown" || !existing.riskClass) && entry.riskClass && entry.riskClass !== "unknown") {
        existing.riskClass = entry.riskClass;
      }
      continue;
    }
    byAsset.set(symbol, {
      asset: symbol,
      name: normalizeAssetName(entry.name, symbol),
      category: entry.category || "Allocation",
      riskClass: normalizeRiskClass(entry.riskClass),
      weight: entry.weight,
    });
  }

  const roundedRows = [...byAsset.values()]
    .map((entry) => ({
      asset: entry.asset,
      name: entry.name,
      category: entry.category,
      riskClass: entry.riskClass,
      allocationPct: roundPercentage(entry.weight),
    }))
    .sort((left, right) => right.allocationPct - left.allocationPct || left.asset.localeCompare(right.asset));

  return normalizeRoundedAllocationRows(roundedRows);
}

function getAaaPayloadCandidates(phase6AaaAllocate: AaaAllocateDispatch | null): Record<string, unknown>[] {
  if (!phase6AaaAllocate || !isObjectRecord(phase6AaaAllocate.response)) {
    return [];
  }

  const payloadCandidates: Record<string, unknown>[] = [phase6AaaAllocate.response];
  if (isObjectRecord(phase6AaaAllocate.response.data)) {
    payloadCandidates.push(phase6AaaAllocate.response.data);
  }
  return payloadCandidates;
}

function upsertAllocationAssetMetadata(
  metadataByAsset: Map<string, AllocationAssetMetadata>,
  input: {
    symbol: string;
    name: unknown;
    category: unknown;
    riskClass: unknown;
  },
  prefer: boolean = false,
): void {
  const symbol = normalizeAssetSymbol(input.symbol);
  if (!symbol) return;

  const name = normalizeAssetName(input.name, symbol);
  const category = typeof input.category === "string" && input.category.trim() ? input.category.trim() : "Allocation";
  const riskClass = normalizeRiskClass(input.riskClass);

  const existing = metadataByAsset.get(symbol);
  if (!existing) {
    metadataByAsset.set(symbol, { name, category, riskClass });
    return;
  }

  if (name !== symbol && (prefer || existing.name === symbol)) {
    existing.name = name;
  }
  if (category !== "Allocation" && (prefer || existing.category === "Allocation")) {
    existing.category = category;
  }
  if (riskClass !== "unknown" && (prefer || existing.riskClass === "unknown")) {
    existing.riskClass = riskClass;
  }
}

function buildAllocationMetadata(
  phase5Output: Phase5Output | null,
  phase6Output: Phase6Output | null,
  phase6AaaAllocate: AaaAllocateDispatch | null,
): Map<string, AllocationAssetMetadata> {
  const metadataByAsset = new Map<string, AllocationAssetMetadata>();

  if (phase5Output && Array.isArray(phase5Output.evaluation.tokens)) {
    for (const token of phase5Output.evaluation.tokens) {
      const symbol = normalizeAssetSymbol(token.symbol);
      if (!symbol) continue;
      const riskClass = normalizeRiskClass(token.risk_class);
      upsertAllocationAssetMetadata(
        metadataByAsset,
        {
          symbol,
          name: token.name,
          category: formatPortfolioRoleLabel(token.role, { symbol, riskClass, bucket: token.selection_bucket }),
          riskClass,
        },
        token.selected === true,
      );
    }
  }

  if (phase6Output && Array.isArray(phase6Output.allocation.allocations)) {
    for (const allocation of phase6Output.allocation.allocations) {
      const symbol = normalizeAssetSymbol(allocation.symbol);
      if (!symbol) continue;
      upsertAllocationAssetMetadata(
        metadataByAsset,
        {
          symbol,
          name: allocation.name,
          category: BUCKET_LABELS[allocation.bucket] ?? "Allocation",
          riskClass: metadataByAsset.get(symbol)?.riskClass ?? "unknown",
        },
        false,
      );
    }
  }

  const payloadCandidates = getAaaPayloadCandidates(phase6AaaAllocate);
  for (const payload of payloadCandidates) {
    const allocatorRequest = isObjectRecord(payload.allocator_request) ? payload.allocator_request : null;
    const portfolio = allocatorRequest && isObjectRecord(allocatorRequest.portfolio) ? allocatorRequest.portfolio : null;
    const assets = portfolio && Array.isArray(portfolio.assets) ? portfolio.assets : [];

    for (const asset of assets) {
      if (!isObjectRecord(asset)) continue;
      const symbol = normalizeAssetSymbol(asset.id ?? asset.symbol);
      if (!symbol) continue;
      const riskClass = normalizeRiskClass(asset.risk_class);
      const role = typeof asset.role === "string" ? asset.role.toLowerCase() : "";
      upsertAllocationAssetMetadata(
        metadataByAsset,
        {
          symbol,
          name: asset.name,
          category: formatPortfolioRoleLabel(role, { symbol, riskClass }),
          riskClass,
        },
        true,
      );
    }
  }

  return metadataByAsset;
}

function parseAaaAllocationEntries(
  phase6AaaAllocate: AaaAllocateDispatch | null,
  metadataByAsset: Map<string, AllocationAssetMetadata>,
): AllocationEntry[] {
  const payloadCandidates = getAaaPayloadCandidates(phase6AaaAllocate);
  if (payloadCandidates.length === 0) {
    return [];
  }

  for (const payload of payloadCandidates) {
    const allocationResult = isObjectRecord(payload.allocation_result) ? payload.allocation_result : null;
    if (!allocationResult) continue;

    const weightMap = isObjectRecord(allocationResult.next_allocation_weights)
      ? allocationResult.next_allocation_weights
      : isObjectRecord(allocationResult.target_weights)
        ? allocationResult.target_weights
        : null;
    if (!weightMap) continue;

    const entries: AllocationEntry[] = [];
    for (const [asset, rawWeight] of Object.entries(weightMap)) {
      const symbol = normalizeAssetSymbol(asset);
      const weight = parseNumericWeight(rawWeight);
      if (!symbol || weight === null) continue;
      const metadata = metadataByAsset.get(symbol);
      entries.push({
        asset: symbol,
        name: metadata?.name ?? symbol,
        category: metadata?.category ?? "Allocation",
        riskClass: metadata?.riskClass ?? "unknown",
        weight,
      });
    }
    if (entries.length > 0) return entries;
  }

  return [];
}

function parsePhase6AllocationEntries(
  phase6Output: Phase6Output | null,
  metadataByAsset: Map<string, AllocationAssetMetadata>,
): AllocationEntry[] {
  if (!phase6Output || !Array.isArray(phase6Output.allocation.allocations)) {
    return [];
  }

  const entries: AllocationEntry[] = [];
  for (const allocation of phase6Output.allocation.allocations) {
    const symbol = normalizeAssetSymbol(allocation.symbol);
    const weight = parseNumericWeight(allocation.allocation_weight);
    if (!symbol || weight === null) continue;
    const metadata = metadataByAsset.get(symbol);
    entries.push({
      asset: symbol,
      name: normalizeAssetName(allocation.name, metadata?.name ?? symbol),
      category: metadata?.category ?? BUCKET_LABELS[allocation.bucket] ?? "Allocation",
      riskClass: metadata?.riskClass ?? "unknown",
      weight,
    });
  }
  return entries;
}

function toAllocationRows(
  phase5Output: Phase5Output | null,
  phase6Output: Phase6Output | null,
  phase6AaaAllocate: AaaAllocateDispatch | null,
): AllocationRow[] {
  const metadataByAsset = buildAllocationMetadata(phase5Output, phase6Output, phase6AaaAllocate);
  const aaaRows = aggregateAllocationEntries(parseAaaAllocationEntries(phase6AaaAllocate, metadataByAsset));
  if (aaaRows.length > 0) {
    return aaaRows;
  }

  return aggregateAllocationEntries(parsePhase6AllocationEntries(phase6Output, metadataByAsset));
}

function roundPercentage(weight: number): number {
  return Math.round(weight * 10_000) / 100;
}

function normalizeRoundedAllocationRows(rows: AllocationRow[]): AllocationRow[] {
  if (rows.length === 0) return rows;

  const targetBasisPoints = 10_000;
  const maxAdjustmentBasisPoints = 10;
  const basisPoints = rows.map((row) => Math.max(0, Math.round(row.allocationPct * 100)));
  let remainingAdjustment = targetBasisPoints - basisPoints.reduce((sum, value) => sum + value, 0);
  if (remainingAdjustment === 0 || Math.abs(remainingAdjustment) > maxAdjustmentBasisPoints) {
    return rows;
  }

  const adjustmentOrder = rows
    .map((row, index) => ({ index, allocationPct: row.allocationPct, asset: row.asset }))
    .sort((left, right) => right.allocationPct - left.allocationPct || left.asset.localeCompare(right.asset))
    .map((entry) => entry.index);

  while (remainingAdjustment !== 0) {
    let adjustedInPass = false;

    for (const index of adjustmentOrder) {
      if (remainingAdjustment === 0) break;

      if (remainingAdjustment > 0) {
        basisPoints[index] += 1;
        remainingAdjustment -= 1;
        adjustedInPass = true;
        continue;
      }

      if (basisPoints[index] > 0) {
        basisPoints[index] -= 1;
        remainingAdjustment += 1;
        adjustedInPass = true;
      }
    }

    if (!adjustedInPass) {
      return rows;
    }
  }

  return rows.map((row, index) => ({
    ...row,
    allocationPct: basisPoints[index] / 100,
  }));
}

function formatRiskClassLabel(riskClass: string): string {
  const normalized = normalizeRiskClass(riskClass);
  if (normalized === "unknown") return "Unspecified";
  return normalized
    .split("_")
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(" ");
}

const isHexAddress = (value: string) => /^0x[a-fA-F0-9]{40}$/.test(value);
const getNetworkLabel = (network: string) => NETWORK_LABELS[network] || network;

async function queryUsdcBalance(walletAddress: string): Promise<{
  usdcBalance: number;
  networkId: string;
  networkLabel: string;
}> {
  const response = await fetch("/api/agent/usdc-balance", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ walletAddress }),
  });

  const payload = (await response.json()) as UsdcBalanceResponse;
  if (!response.ok || !payload.success || !payload.data) {
    throw new Error(payload.error || "Unable to read USDC balance.");
  }

  const networkId = payload.data.network || "unknown-network";
  const parsedBalance = Number.parseFloat(payload.data.usdcBalance || "");
  if (!Number.isFinite(parsedBalance)) {
    throw new Error("Backend returned an invalid USDC balance.");
  }

  return {
    usdcBalance: parsedBalance,
    networkId,
    networkLabel: getNetworkLabel(networkId),
  };
}

async function queryPricing(): Promise<{
  structuredAllocationPriceUsdc: number;
  certifiedDecisionRecordFeeUsdc: number;
}> {
  const response = await fetch("/api/agent/pricing", {
    method: "GET",
    cache: "no-store",
  });

  const payload = (await response.json()) as PricingResponse;
  if (!response.ok || !payload.success || !payload.data) {
    throw new Error(payload.error || "Unable to load pricing.");
  }

  const basePrice = Number(payload.data.structuredAllocationPriceUsdc);
  const decisionFee = Number(payload.data.certifiedDecisionRecordFeeUsdc);
  if (!Number.isFinite(basePrice) || basePrice < 0 || !Number.isFinite(decisionFee) || decisionFee < 0) {
    throw new Error("Backend returned invalid pricing.");
  }

  return {
    structuredAllocationPriceUsdc: basePrice,
    certifiedDecisionRecordFeeUsdc: decisionFee,
  };
}

async function queryPaymentQuote(params: {
  walletAddress: string;
  includeCertifiedDecisionRecord: boolean;
  promoCode: string;
}): Promise<PromoQuoteResult> {
  const response = await fetch("/api/agent/pay-quote", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });

  const rawText = await response.text();
  let payload: PaymentQuoteResponse | null = null;
  try {
    payload = rawText ? (JSON.parse(rawText) as PaymentQuoteResponse) : null;
  } catch {
    payload = null;
  }

  if (!response.ok || !payload?.success) {
    if (payload?.error) {
      throw new Error(payload.error);
    }

    const trimmed = rawText.trim();
    const looksLikeHtml =
      trimmed.startsWith("<!DOCTYPE") ||
      trimmed.startsWith("<html") ||
      (response.headers.get("content-type") || "").includes("text/html");
    if (looksLikeHtml) {
      throw new Error("Promo quote endpoint returned HTML instead of JSON. Redeploy latest frontend/backend and try again.");
    }

    throw new Error(`Unable to validate promo code (HTTP ${response.status}).`);
  }

  const totalBeforeDiscountUsdc = Number.parseFloat(payload.totalBeforeDiscountUsdc ?? "");
  const chargedAmountUsdc = Number.parseFloat(payload.chargedAmountUsdc ?? "");
  const discountAmountUsdc = Number.parseFloat(payload.discountAmountUsdc ?? "");
  const discountPercent = Number(payload.discountPercent ?? 0);
  if (
    !Number.isFinite(totalBeforeDiscountUsdc) ||
    totalBeforeDiscountUsdc < 0 ||
    !Number.isFinite(chargedAmountUsdc) ||
    chargedAmountUsdc < 0 ||
    !Number.isFinite(discountAmountUsdc) ||
    discountAmountUsdc < 0 ||
    !Number.isFinite(discountPercent) ||
    discountPercent < 0 ||
    discountPercent > 100
  ) {
    throw new Error("Backend returned invalid promo quote.");
  }

  return {
    totalBeforeDiscountUsdc,
    chargedAmountUsdc,
    discountAmountUsdc,
    discountPercent,
    promoCodeApplied: Boolean(payload.promoCodeApplied),
    promoCode: payload.promoCode,
    certifiedDecisionRecordPurchased: Boolean(payload.certifiedDecisionRecordPurchased),
    paymentMethod: payload.paymentMethod ?? "onchain",
    message: payload.message || "Promo code applied.",
  };
}

async function queryAgentWallet(): Promise<{
  walletAddress: string;
  networkId: string;
  usdcContractAddress: string;
}> {
  const response = await fetch("/api/agent/wallet", {
    method: "GET",
    cache: "no-store",
  });

  const payload = (await response.json()) as AgentWalletResponse;
  if (!response.ok || !payload.success || !payload.data) {
    throw new Error(payload.error || "Unable to load agent wallet.");
  }

  const walletAddress = payload.data.walletAddress ?? "";
  const networkId = payload.data.network ?? "";
  const usdcContractAddress = payload.data.usdc?.contractAddress ?? "";

  if (!isHexAddress(walletAddress)) {
    throw new Error("Backend returned invalid agent wallet address.");
  }
  if (!networkId || !CHAIN_CONFIGS[networkId]) {
    throw new Error(`Unsupported backend network for payment: ${networkId || "unknown"}.`);
  }
  if (!isHexAddress(usdcContractAddress)) {
    throw new Error("Backend returned invalid USDC contract address.");
  }

  return {
    walletAddress,
    networkId,
    usdcContractAddress,
  };
}

async function verifyPaymentOnBackend(params: {
  fromAddress: string;
  expectedAmountUSDC: number | string;
  transactionHash?: string;
  decisionId?: string;
}, options?: {
  timeoutMs?: number;
}): Promise<{
  transactionHash: string;
  amount: string;
  confirmed: boolean;
}> {
  const timeoutMs = options?.timeoutMs;
  const controller = typeof AbortController !== "undefined" && typeof timeoutMs === "number" && timeoutMs > 0
    ? new AbortController()
    : undefined;
  const timeoutHandle =
    controller && typeof timeoutMs === "number" && timeoutMs > 0
      ? setTimeout(() => controller.abort(), timeoutMs)
      : undefined;
  try {
    const response = await fetch("/api/agent/verify-payment", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
      signal: controller?.signal,
    });

    const payload = (await response.json()) as VerifyPaymentResponse;
    if (!response.ok || !payload.success || !payload.data) {
      throw new Error(payload.error || "Payment verification failed.");
    }

    const transactionHash = payload.data.transactionHash ?? "";
    const amount = payload.data.amount ?? "";
    const confirmed = Boolean(payload.data.confirmed);

    if (!/^0x[a-fA-F0-9]{64}$/.test(transactionHash)) {
      throw new Error("Backend returned invalid transaction hash.");
    }
    if (!confirmed) {
      throw new Error("Payment was not confirmed on-chain.");
    }

    return {
      transactionHash,
      amount,
      confirmed,
    };
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error("Payment verification timed out.");
    }
    throw error;
  } finally {
    if (typeof timeoutHandle === "number") {
      clearTimeout(timeoutHandle);
    }
  }
}

async function startPhase1Execution(params: {
  jobId: string;
  executionTimestamp: string;
  riskMode: string;
  riskTolerance: string;
  investmentTimeframe: string;
  timeWindow: string;
  walletAddress?: string;
}): Promise<void> {
  const response = await fetch("/api/agent/phase1/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });

  const payload = (await response.json()) as Phase1RunStartResponse;
  if (!response.ok || payload.status !== "started") {
    throw new Error(payload.error || "Unable to start Phase 1.");
  }
}

async function startPhase3Execution(params: { jobId: string }): Promise<void> {
  const response = await fetch("/api/agent/phase3/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });

  const payload = (await response.json()) as Phase1RunStartResponse;
  if (!response.ok || payload.status !== "started") {
    throw new Error(payload.error || "Unable to start Phase 3.");
  }
}

async function startPhase4Execution(params: { jobId: string }): Promise<void> {
  const response = await fetch("/api/agent/phase4/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });

  const payload = (await response.json()) as Phase1RunStartResponse;
  if (!response.ok || payload.status !== "started") {
    throw new Error(payload.error || "Unable to start Phase 4.");
  }
}

async function startPhase5Execution(params: { jobId: string }): Promise<void> {
  const response = await fetch("/api/agent/phase5/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });

  const payload = (await response.json()) as Phase1RunStartResponse;
  if (!response.ok || payload.status !== "started") {
    throw new Error(payload.error || "Unable to start Phase 5.");
  }
}

async function startPhase6Execution(params: { jobId: string }): Promise<void> {
  const response = await fetch("/api/agent/phase6/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });

  const payload = (await response.json()) as Phase1RunStartResponse;
  if (!response.ok || payload.status !== "started") {
    throw new Error(payload.error || "Unable to start Phase 6.");
  }
}

async function queryExecutionStatus(jobId: string): Promise<ExecutionStatusResponse> {
  const response = await fetch(`/api/agent/execution-status/${encodeURIComponent(jobId)}`, {
    method: "GET",
    cache: "no-store",
  });

  const payload = (await response.json()) as ExecutionStatusResponse;
  if (!response.ok) {
    throw new Error(payload.error || "Unable to poll execution status.");
  }
  return payload;
}

function toBackendRiskMode(mode: RiskMode): "conservative" | "neutral" | "aggressive" {
  if (mode === "Conservative") return "conservative";
  if (mode === "Aggressive" || mode === "Growth") return "aggressive";
  return "neutral";
}

function toBackendRiskTolerance(mode: RiskMode): "conservative" | "balanced" | "growth" | "aggressive" {
  if (mode === "Conservative") return "conservative";
  if (mode === "Growth") return "growth";
  if (mode === "Aggressive") return "aggressive";
  return "balanced";
}

function toBackendTimeWindow(horizon: InvestmentHorizon): "7d" | "30d" {
  if (horizon === "< 1 Year") return "7d";
  return "30d";
}

function toBackendInvestmentTimeframe(horizon: InvestmentHorizon): "<1_year" | "1-3_years" | "3+_years" {
  if (horizon === "< 1 Year") return "<1_year";
  if (horizon === "3+ Years") return "3+_years";
  return "1-3_years";
}

function parseUsdcToBaseUnits(value: string): bigint {
  const trimmed = value.trim();
  if (!/^\d+(\.\d+)?$/.test(trimmed)) {
    throw new Error(`Invalid USDC amount: ${value}`);
  }

  const [wholePart, fractionalRaw = ""] = trimmed.split(".");
  if (fractionalRaw.length > 6) {
    throw new Error(`USDC amount has too many decimal places: ${value}`);
  }

  const fractionalPart = fractionalRaw.padEnd(6, "0");
  return BigInt(wholePart) * 1_000_000n + BigInt(fractionalPart || "0");
}

function encodeUsdcTransferCall(toAddress: string, amountBaseUnits: bigint): string {
  const methodSelector = "a9059cbb";
  const encodedTo = toAddress.toLowerCase().replace(/^0x/, "").padStart(64, "0");
  const encodedAmount = amountBaseUnits.toString(16).padStart(64, "0");
  return `0x${methodSelector}${encodedTo}${encodedAmount}`;
}

function extractWalletRpcErrorDetails(error: unknown): {
  message: string;
  code?: number;
  transactionHash?: string;
} {
  const transactionHashPattern = /0x[a-fA-F0-9]{64}/;
  const seen = new Set<unknown>();
  const queue: unknown[] = [error];
  const messages: string[] = [];
  let code: number | undefined;
  let transactionHash: string | undefined;

  while (queue.length > 0) {
    const current = queue.shift();
    if (current == null || seen.has(current)) continue;

    if (typeof current === "string") {
      const trimmed = current.trim();
      if (trimmed) messages.push(trimmed);
      const match = trimmed.match(transactionHashPattern);
      if (!transactionHash && match) {
        transactionHash = match[0];
      }
      continue;
    }

    if (typeof current === "number" || typeof current === "boolean") {
      continue;
    }

    if (current instanceof Error) {
      seen.add(current);
      if (current.message) messages.push(current.message);
      queue.push((current as { cause?: unknown }).cause);
      continue;
    }

    if (typeof current === "object") {
      seen.add(current);
      const record = current as Record<string, unknown>;
      if (typeof record.code === "number" && code === undefined) {
        code = record.code;
      }
      if (typeof record.message === "string") {
        messages.push(record.message);
      }

      const hashValue = record.transactionHash ?? record.txHash ?? record.hash;
      if (!transactionHash && typeof hashValue === "string" && /^0x[a-fA-F0-9]{64}$/.test(hashValue)) {
        transactionHash = hashValue;
      }

      for (const value of Object.values(record)) {
        queue.push(value);
      }
    }
  }

  return {
    message: [...new Set(messages.map((value) => value.trim()).filter(Boolean))].join(" | ") || "Wallet transfer failed.",
    code,
    transactionHash,
  };
}

function parseDuplicateBroadcastWalletError(error: unknown): {
  isDuplicateBroadcast: boolean;
  message: string;
  transactionHash?: string;
} {
  const details = extractWalletRpcErrorDetails(error);
  const normalizedMessage = details.message.toLowerCase();
  const duplicateBroadcastMarkers = [
    "already known",
    "known transaction",
    "already imported",
    "nonce too low",
    "replacement transaction underpriced",
  ];

  return {
    isDuplicateBroadcast:
      duplicateBroadcastMarkers.some((marker) => normalizedMessage.includes(marker)) ||
      (details.code === -32000 && normalizedMessage.includes("known")),
    message: details.message,
    transactionHash: details.transactionHash,
  };
}

function isUserRejectedWalletError(error: unknown): boolean {
  const details = extractWalletRpcErrorDetails(error);
  const normalizedMessage = details.message.toLowerCase();
  return (
    details.code === 4001 ||
    normalizedMessage.includes("user rejected") ||
    normalizedMessage.includes("user denied") ||
    normalizedMessage.includes("rejected the request")
  );
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseHexQuantityToBigInt(value: unknown): bigint | null {
  if (typeof value !== "string") return null;
  if (!/^0x[a-fA-F0-9]+$/.test(value)) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

async function hasPendingTransactions(provider: EthereumProvider, walletAddress: string): Promise<boolean> {
  try {
    const [latestRaw, pendingRaw] = await Promise.all([
      provider.request({
        method: "eth_getTransactionCount",
        params: [walletAddress, "latest"],
      }),
      provider.request({
        method: "eth_getTransactionCount",
        params: [walletAddress, "pending"],
      }),
    ]);

    const latest = parseHexQuantityToBigInt(latestRaw);
    const pending = parseHexQuantityToBigInt(pendingRaw);
    if (latest === null || pending === null) return false;
    return pending > latest;
  } catch {
    return false;
  }
}

async function hasPendingTransactionsOnRpc(networkId: string, walletAddress: string): Promise<boolean> {
  const chainConfig = CHAIN_CONFIGS[networkId];
  const rpcUrl = chainConfig?.rpcUrls?.[0];
  if (!rpcUrl) return false;

  const sendRpc = async (method: string, params: unknown[]): Promise<unknown> => {
    const response = await fetch(rpcUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: Date.now(),
        method,
        params,
      }),
    });
    if (!response.ok) {
      throw new Error(`RPC request failed: ${response.status}`);
    }
    const payload = (await response.json()) as {
      result?: unknown;
      error?: { message?: string };
    };
    if (payload.error) {
      throw new Error(payload.error.message || "RPC returned an error.");
    }
    return payload.result;
  };

  try {
    const [latestRaw, pendingRaw] = await Promise.all([
      sendRpc("eth_getTransactionCount", [walletAddress, "latest"]),
      sendRpc("eth_getTransactionCount", [walletAddress, "pending"]),
    ]);
    const latest = parseHexQuantityToBigInt(latestRaw);
    const pending = parseHexQuantityToBigInt(pendingRaw);
    if (latest === null || pending === null) return false;
    return pending > latest;
  } catch {
    return false;
  }
}

async function tryRecoverExistingPayment(params: {
  fromAddress: string;
  expectedAmountUSDC: string;
  decisionId?: string;
}, timeoutMs: number): Promise<Awaited<ReturnType<typeof verifyPaymentOnBackend>> | null> {
  try {
    return await verifyPaymentOnBackend(
      {
        fromAddress: params.fromAddress,
        expectedAmountUSDC: params.expectedAmountUSDC,
      },
      { timeoutMs },
    );
  } catch {
    return null;
  }
}

async function getWalletChainIdHex(): Promise<string | null> {
  const provider = window.ethereum;
  if (!provider?.request) return null;
  try {
    const raw = await provider.request({ method: "eth_chainId" });
    return typeof raw === "string" ? raw : null;
  } catch {
    return null;
  }
}

// CHANGED: make switching more robust:
// - if already on chain, return immediately
// - use longer timeout (wallet UI may require user attention)
// - keep original add-chain fallback
async function ensureWalletOnChain(networkId: string) {
  const provider = window.ethereum;
  if (!provider?.request) {
    throw new Error("No Ethereum wallet detected.");
  }

  const config = CHAIN_CONFIGS[networkId];
  if (!config) {
    throw new Error(`Unsupported chain requested: ${networkId}`);
  }

  const currentChainId = await getWalletChainIdHex();
  if (currentChainId && currentChainId.toLowerCase() === config.chainIdHex.toLowerCase()) {
    return;
  }

  try {
    await provider.request({
      method: "wallet_switchEthereumChain",
      params: [{ chainId: config.chainIdHex }],
    });
    return;
  } catch (error) {
    const rpcError = error as ProviderRpcError;

    // If switch request was shown but not completed, wallets may effectively "hang".
    // Surface a more actionable hint.
    if (rpcError?.code === 4001) {
      throw new Error(`Network switch rejected. Please switch your wallet to ${config.chainName} and retry.`);
    }

    if (rpcError.code !== 4902) {
      throw new Error(rpcError.message || `Failed to switch wallet network to ${config.chainName}.`);
    }
  }

  await provider.request({
    method: "wallet_addEthereumChain",
    params: [
      {
        chainId: config.chainIdHex,
        chainName: config.chainName,
        nativeCurrency: config.nativeCurrency,
        rpcUrls: config.rpcUrls,
        blockExplorerUrls: config.blockExplorerUrls,
      },
    ],
  });
}

// Helper function to check if an amount is effectively zero
function isZeroAmount(value: unknown): boolean {
  const n = typeof value === "number" ? value : typeof value === "string" ? Number(value) : NaN;
  return Number.isFinite(n) && Math.abs(n) < 1e-9;
}

type ConfigureStepProps = {
  riskMode: RiskMode | null;
  investmentHorizon: InvestmentHorizon | null;
  onRiskModeSelect: (mode: RiskMode) => void;
  onInvestmentHorizonSelect: (horizon: InvestmentHorizon) => void;
  onContinue: () => void;
};

type SliderSelectorProps<T extends string> = {
  title: string;
  options: readonly T[];
  value: T | null;
  onSelect: (value: T) => void;
};

function SliderSelector<T extends string>({ title, options, value, onSelect }: SliderSelectorProps<T>) {
  const maxIndex = Math.max(options.length - 1, 1);

  return (
    <div className="rounded-xl border border-slate-300/70 bg-slate-50/80 p-4">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">{title}</p>
        <span className="text-sm font-semibold text-cyan-800">{value ?? "Select"}</span>
      </div>

      <div className="mt-3 grid gap-2" style={{ gridTemplateColumns: `repeat(${options.length}, minmax(0,1fr))` }}>
        {options.map((option) => {
          const selected = value === option;
          return (
            <button
              key={option}
              type="button"
              onClick={() => onSelect(option)}
              className={`rounded-full border px-3 py-1.5 text-xs font-semibold transition ${
                selected
                  ? "border-cyan-700 bg-cyan-900 text-cyan-50"
                  : "border-slate-300 bg-white text-slate-600 hover:border-cyan-400"
              }`}
            >
              {option}
            </button>
          );
        })}
      </div>

      <div className="relative mt-4 h-9">
        <div className="absolute left-0 right-0 top-1/2 h-1.5 -translate-y-1/2 rounded-full border border-slate-300 bg-slate-200" />

        {options.map((option, index) => {
          const selected = value === option;
          const left = `${(index / maxIndex) * 100}%`;

          return (
            <button
              key={`${option}-stop`}
              type="button"
              onClick={() => onSelect(option)}
              className="absolute top-1/2 h-9 w-9 -translate-y-1/2 -translate-x-1/2"
              style={{ left }}
              aria-label={`Set ${title} to ${option}`}
            >
              <span
                className={`mx-auto block transition-all ${
                  selected
                    ? "h-5 w-5 rounded-full border-2 border-cyan-700 bg-cyan-300 shadow-[0_0_0_4px_rgba(6,182,212,0.18)]"
                    : "h-4 w-4 rounded-full border border-slate-500 bg-slate-300"
                }`}
              />
            </button>
          );
        })}
      </div>
    </div>
  );
}

function ConfigureStep({
  riskMode,
  investmentHorizon,
  onRiskModeSelect,
  onInvestmentHorizonSelect,
  onContinue,
}: ConfigureStepProps) {
  const canContinue = Boolean(riskMode && investmentHorizon);

  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 1. Set Your Allocation Profile</h2>
      <p className="mt-2 text-slate-600">Choose your risk tolerance and investment timeframe.</p>

      <div className="mt-6 grid gap-6 md:grid-cols-2">
        <SliderSelector title="Risk Tolerance" options={RISK_MODES} value={riskMode} onSelect={onRiskModeSelect} />
        <SliderSelector
          title="Investment Timeframe"
          options={HORIZONS}
          value={investmentHorizon}
          onSelect={onInvestmentHorizonSelect}
        />
      </div>

      <div className="mt-6 flex justify-end">
        <button
          type="button"
          onClick={onContinue}
          disabled={!canContinue}
          className="rounded-full bg-cyan-500 px-5 py-2.5 text-sm font-semibold text-slate-950 shadow-sm transition hover:bg-cyan-400 disabled:cursor-not-allowed disabled:bg-slate-300 disabled:text-slate-500"
        >
          Continue
        </button>
      </div>
    </section>
  );
}

type ReviewStepProps = {
  riskMode: RiskMode;
  investmentHorizon: InvestmentHorizon;
  basePriceUsdc: number;
  certifiedDecisionRecordFeeUsdc: number;
  resultEmail: string;
  includeCertifiedDecisionRecord: boolean;
  promoCode: string;
  promoQuote: PromoQuoteResult | null;
  promoQuoteError: string | null;
  isApplyingPromoCode: boolean;
  requiresPromoApply: boolean;
  requiredAmountUsdc: number;
  totalPriceUsdc: number;
  isLoadingPricing: boolean;
  pricingError: string | null;
  walletAddress: string | null;
  usdcNetworkLabel: string;
  usdcBalance: number | null;
  isLoadingUsdcBalance: boolean;
  usdcBalanceError: string | null;
  isBalanceLow: boolean;
  isConnectingWallet: boolean;
  paymentError: string | null;
  isPaying: boolean;
  onResultEmailChange: (value: string) => void;
  onToggleCertifiedDecisionRecord: (nextValue: boolean) => void;
  onPromoCodeChange: (value: string) => void;
  onApplyPromoCode: () => Promise<void>;
  onConnectWallet: () => Promise<void>;
  onRefreshUsdcBalance: () => void;
  onRefreshPricing: () => Promise<void>;
  onBack: () => void;
  onGenerate: () => void;
};

function ReviewStep({
  riskMode,
  investmentHorizon,
  basePriceUsdc,
  certifiedDecisionRecordFeeUsdc,
  resultEmail,
  includeCertifiedDecisionRecord,
  promoCode,
  promoQuote,
  promoQuoteError,
  isApplyingPromoCode,
  requiresPromoApply,
  requiredAmountUsdc,
  totalPriceUsdc,
  isLoadingPricing,
  pricingError,
  walletAddress,
  usdcNetworkLabel,
  usdcBalance,
  isLoadingUsdcBalance,
  usdcBalanceError,
  isBalanceLow,
  isConnectingWallet,
  paymentError,
  isPaying,
  onResultEmailChange,
  onToggleCertifiedDecisionRecord,
  onPromoCodeChange,
  onApplyPromoCode,
  onConnectWallet,
  onRefreshUsdcBalance,
  onRefreshPricing,
  onBack,
  onGenerate,
}: ReviewStepProps) {
  const hasPromoCode = promoCode.trim().length > 0;
  const promoApplied = Boolean(promoQuote?.promoCodeApplied);
  const normalizedResultEmail = resultEmail.trim();
  const hasResultEmail = normalizedResultEmail.length > 0;
  const invalidResultEmail = hasResultEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizedResultEmail);
  const requiresUsdcBalance = requiredAmountUsdc > 0;
  const canGenerate =
    Boolean(walletAddress) &&
    !isPaying &&
    !isApplyingPromoCode &&
    !invalidResultEmail &&
    !requiresPromoApply &&
    (
      !requiresUsdcBalance ||
      (
        !isLoadingUsdcBalance &&
        !usdcBalanceError &&
        !isLoadingPricing &&
        !pricingError &&
        usdcBalance !== null &&
        usdcBalance >= requiredAmountUsdc
      )
    );

  const shouldLeadWithConnect = !walletAddress;

  const authorizeLabel = isPaying
    ? "Agent processing payment..."
    : isLoadingPricing
      ? "Loading pricing..."
    : pricingError
      ? "Pricing unavailable. Refresh required."
    : invalidResultEmail
      ? "Enter a valid results email or leave blank"
    : requiresUsdcBalance && isLoadingUsdcBalance && walletAddress
      ? "Checking USDC balance..."
    : requiresPromoApply
      ? "Apply promo code to confirm final price"
    : isBalanceLow
      ? "Insufficient USDC balance"
    : !requiresUsdcBalance
      ? "Generate Allocation"
    : `Authorize ${formatUsdcValue(requiredAmountUsdc)} USDC & Generate Allocation`;

  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 2. Review & Authorize</h2>
      <p className="mt-2 text-slate-600">Review your allocation profile and authorize execution on-chain.</p>

      <div className="mt-6 grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Risk Tolerance</p>
          <p className="mt-2 text-lg font-semibold text-slate-800">{riskMode}</p>
        </div>
        <div className="rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Investment Timeframe</p>
          <p className="mt-2 text-lg font-semibold text-slate-800">{investmentHorizon}</p>
        </div>
        <div className="rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Structured Allocation</p>
          <p className="mt-2 text-lg font-semibold text-slate-800">${formatUsdcValue(basePriceUsdc)} USDC</p>
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
        <label className="flex cursor-pointer items-start gap-3">
          <input
            type="checkbox"
            checked={includeCertifiedDecisionRecord}
            onChange={(event) => onToggleCertifiedDecisionRecord(event.target.checked)}
            disabled={isPaying}
            className="mt-1 h-4 w-4 accent-cyan-700"
          />
          <div className="space-y-1">
            <p className="text-sm font-semibold text-slate-800">Add Certified Decision Report</p>
            <p className="text-sm font-semibold text-slate-700">${formatUsdcValue(certifiedDecisionRecordFeeUsdc)} USDC</p>
            <p className="text-xs text-slate-600">
            Exhaustive report detailing current market conditions, allocation rationale, and token selection logic.
            <br /><strong>Recommended for in-depth analysis and documentation.</strong>
            </p>
          </div>
        </label>
      </div>

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
        <label htmlFor="review-result-email" className="block text-xs font-bold uppercase tracking-[0.12em] text-slate-500">
          Optional Results Email
        </label>
        <input
          id="review-result-email"
          type="email"
          value={resultEmail}
          onChange={(event) => onResultEmailChange(event.target.value)}
          placeholder="you@example.com"
          className={`mt-2 w-full rounded-lg border bg-white px-3 py-2 text-sm text-slate-800 focus:outline-none ${
            invalidResultEmail ? "border-rose-300 focus:border-rose-400" : "border-slate-300 focus:border-cyan-400"
          }`}
        />
        <p className="mt-1 text-xs text-slate-500">
          If provided, Selun will email your allocation summary after generation. If you later download the certified
          record, the same address will be used for PDF delivery.
        </p>
        {invalidResultEmail && (
          <p className="mt-2 text-xs font-medium text-rose-700">Enter a valid email address or leave this field blank.</p>
        )}
      </div>

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-white/80 p-4">
        <div className="flex items-center justify-between text-sm text-slate-700">
          <span>Structured Allocation</span>
          <span>${formatUsdcValue(basePriceUsdc)} USDC</span>
        </div>
        <div className="mt-1 flex items-center justify-between text-sm text-slate-700">
          <span>Certified Decision Report</span>
          <span>
            {includeCertifiedDecisionRecord ? `$${formatUsdcValue(certifiedDecisionRecordFeeUsdc)} USDC` : "$0 USDC"}
          </span>
        </div>
        {promoApplied && promoQuote && (
          <div className="mt-1 flex items-center justify-between text-sm text-emerald-700">
            <span>Promo Discount ({formatUsdcValue(promoQuote.discountPercent)}%)</span>
            <span>- ${formatUsdcValue(promoQuote.discountAmountUsdc)} USDC</span>
          </div>
        )}
        <div className="mt-3 flex items-center justify-between border-t border-slate-200 pt-3 text-base font-bold text-slate-950">
          <span>Final Total</span>
          <span>${formatUsdcValue(requiredAmountUsdc)} USDC</span>
        </div>
        {requiresPromoApply && (
          <p className="mt-2 text-xs font-medium text-amber-700">
            Apply your code to confirm the final checkout price before purchase.
          </p>
        )}
        {promoApplied && promoQuote?.message && (
          <p className="mt-2 text-xs font-medium text-emerald-700">{promoQuote.message}</p>
        )}
        <p className="mt-2 text-xs font-medium text-slate-500">
          Allocation executed using Sagitta AAA v4 market-aware allocator.
        </p>
      </div>

      {pricingError && (
        <div className="mt-3 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2">
          <p className="text-sm font-medium text-amber-700">{pricingError}</p>
          <button
            type="button"
            onClick={() => void onRefreshPricing()}
            disabled={isLoadingPricing || isPaying}
            className="rounded-full border border-amber-300 bg-white px-3 py-1.5 text-xs font-semibold text-amber-800 transition hover:border-amber-400 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {isLoadingPricing ? "Refreshing..." : "Refresh Pricing"}
          </button>
        </div>
      )}

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Wallet</p>
            <p className="mt-1 text-sm font-semibold text-slate-800">
              {walletAddress ? `Connected: ${shortenAddress(walletAddress)}` : "Not connected"}
            </p>
            <p className="mt-1 text-xs text-slate-600">
              Payment will be executed by the Selun agent after wallet verification.
            </p>
          </div>

          <button
            type="button"
            onClick={onConnectWallet}
            disabled={isConnectingWallet || isPaying}
            className={`rounded-full px-4 py-2 text-sm font-semibold transition disabled:cursor-not-allowed disabled:opacity-50 ${
              shouldLeadWithConnect
                ? "border border-slate-400 bg-slate-200 text-slate-800 shadow-[0_0_0_3px_rgba(34,211,238,0.2),0_8px_20px_rgba(34,211,238,0.18)] hover:border-cyan-500 hover:bg-slate-100 hover:shadow-[0_0_0_4px_rgba(34,211,238,0.28),0_10px_24px_rgba(34,211,238,0.24)]"
                : "border border-slate-400 bg-white text-slate-700 hover:border-cyan-400"
            }`}
          >
            {isConnectingWallet ? "Connecting..." : walletAddress ? "Reconnect Wallet" : "Connect Wallet"}
          </button>
        </div>

        <div className="mt-4 grid gap-3 md:grid-cols-[minmax(0,1fr)_auto]">
          <div className="rounded-lg border border-slate-300/60 bg-white/80 px-3 py-2">
            <p className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500">USDC Network</p>
            <p className="mt-1 text-sm font-semibold text-slate-800">{usdcNetworkLabel}</p>
          </div>

          <button
            type="button"
            onClick={onRefreshUsdcBalance}
            disabled={!walletAddress || isLoadingUsdcBalance || isPaying}
            className="self-end rounded-full border border-slate-400 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-cyan-400 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {isLoadingUsdcBalance ? "Checking..." : "Refresh Balance"}
          </button>
        </div>

        <div className="mt-3 rounded-lg border border-slate-300/60 bg-white/80 px-3 py-2">
          <p className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500">USDC Available</p>
          <p className="mt-1 text-lg font-semibold text-slate-900">
            {walletAddress
              ? isLoadingUsdcBalance
                ? "Checking..."
                : usdcBalance !== null
                  ? `${formatUsdcValue(usdcBalance)} USDC`
                  : "Unavailable"
              : "Connect wallet to check"}
          </p>
          <p className="text-xs text-slate-600">Required: {formatUsdcValue(requiredAmountUsdc)} USDC</p>
        </div>

        {usdcBalanceError && (
          <p className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm font-medium text-amber-700">
            {usdcBalanceError}
          </p>
        )}

        {walletAddress && requiresUsdcBalance && isBalanceLow && !usdcBalanceError && !requiresPromoApply && (
          <p className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm font-medium text-amber-700">
            Low USDC balance on {usdcNetworkLabel}. Add funds before continuing.
          </p>
        )}
      </div>

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
        <label className="block text-xs font-bold uppercase tracking-[0.16em] text-slate-500" htmlFor="promo-code-input">
          Promo Code (Optional)
        </label>
        <div className="mt-2 grid gap-2 md:grid-cols-[minmax(0,1fr)_auto]">
          <input
            id="promo-code-input"
            type="text"
            value={promoCode}
            onChange={(event) => onPromoCodeChange(event.target.value)}
            placeholder="Enter promo code"
            disabled={isPaying || isApplyingPromoCode}
            className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-800 outline-none ring-cyan-500/30 transition placeholder:text-slate-400 focus:border-cyan-400 focus:ring-4 disabled:cursor-not-allowed disabled:bg-slate-100"
          />
          <button
            type="button"
            onClick={() => void onApplyPromoCode()}
            disabled={!walletAddress || !hasPromoCode || isPaying || isApplyingPromoCode}
            className="rounded-full border border-cyan-500 bg-cyan-50 px-4 py-2 text-sm font-semibold text-cyan-800 transition hover:bg-cyan-100 disabled:cursor-not-allowed disabled:border-slate-300 disabled:bg-slate-100 disabled:text-slate-400"
          >
            {isApplyingPromoCode ? "Applying..." : promoApplied ? "Code Applied" : "Apply Code"}
          </button>
        </div>
        {!walletAddress && (
          <p className="mt-2 text-xs font-medium text-amber-700">Connect wallet first, then apply promo code.</p>
        )}
        {walletAddress && (
          <p className="mt-2 text-xs text-slate-600">
            Apply code first to lock preview pricing before purchase.
          </p>
        )}
        {promoQuoteError && (
          <p className="mt-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
            {promoQuoteError}
          </p>
        )}
      </div>

      {paymentError && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {paymentError}
        </p>
      )}

      <div className="mt-6 flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={onBack}
          disabled={isPaying}
          className="rounded-full border border-slate-400 bg-white px-5 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-slate-500 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Back
        </button>
        <button
          type="button"
          onClick={onGenerate}
          disabled={!canGenerate}
          className="rounded-full bg-cyan-500 px-5 py-2.5 text-sm font-semibold text-slate-950 shadow-sm transition hover:bg-cyan-400 disabled:cursor-not-allowed disabled:bg-slate-300 disabled:text-slate-500 disabled:shadow-none"
        >
          {authorizeLabel}
        </button>
      </div>
    </section>
  );
}

type ProcessingStepViewProps = {
  steps: ProcessingStepMeta[];
  phase1Status: Phase1Status;
  phase1SubPhaseStates: Record<string, SubPhaseStatus>;
  phase1Error: string | null;
  phase2Status: "idle" | "in_progress" | "complete" | "failed";
  phase2Output: Phase2Output | null;
  phase2Error: string | null;
  phase3Status: "idle" | "in_progress" | "complete" | "failed";
  phase3SubPhaseStates: Record<string, SubPhaseStatus>;
  phase3Output: Phase3Output | null;
  phase3Error: string | null;
  phase4Status: "idle" | "in_progress" | "complete" | "failed";
  phase4Output: Phase4Output | null;
  phase4Error: string | null;
  phase5Status: "idle" | "in_progress" | "complete" | "failed";
  phase5Output: Phase5Output | null;
  phase5Error: string | null;
  phase6Status: "idle" | "in_progress" | "complete" | "failed";
  phase6Output: Phase6Output | null;
  phase6AaaAllocate: AaaAllocateDispatch | null;
  phase6Error: string | null;
  onRetryPhase1: () => void;
  onRetryPhase3: () => void;
  onRetryPhase4: () => void;
  onRetryPhase5: () => void;
  onRetryPhase6: () => void;
};

function ProcessingStepView({
  steps,
  phase1Status,
  phase1SubPhaseStates,
  phase1Error,
  phase2Status,
  phase2Output,
  phase2Error,
  phase3Status,
  phase3SubPhaseStates,
  phase3Output,
  phase3Error,
  phase4Status,
  phase4Output,
  phase4Error,
  phase5Status,
  phase5Output,
  phase5Error,
  phase6Status,
  phase6Output,
  phase6AaaAllocate,
  phase6Error,
  onRetryPhase1,
  onRetryPhase3,
  onRetryPhase4,
  onRetryPhase5,
  onRetryPhase6,
}: ProcessingStepViewProps) {
  const hasReportGenerationStep = steps.some((step) => step.key === "REPORT_GENERATION");
  const completedUnits =
    (phase1Status === "complete" ? 1 : 0) +
    (phase2Status === "complete" ? 1 : 0) +
    (phase3Status === "complete" ? 1 : 0) +
    (phase4Status === "complete" ? 1 : 0) +
    (phase5Status === "complete" ? 1 : 0) +
    (phase6Status === "complete" ? (hasReportGenerationStep ? 2 : 1) : 0);
  const partialUnits =
    phase1Status === "in_progress" || phase1Status === "starting"
      ? 0.35
      : phase1Status === "complete" && phase2Status === "in_progress"
        ? 0.4
      : phase2Status === "complete" && phase3Status === "in_progress"
          ? 0.4
      : phase3Status === "complete" && phase4Status === "in_progress"
          ? 0.4
      : phase4Status === "complete" && phase5Status === "in_progress"
          ? 0.4
      : phase5Status === "complete" && phase6Status === "in_progress"
          ? 0.4
          : 0;
  const progressPercent = Math.min(100, ((completedUnits + partialUnits) / steps.length) * 100);
  const hasAaaPhase6Surface = Boolean(phase6AaaAllocate && phase6AaaAllocate.status !== "idle");

  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 3. Agent Execution</h2>
      <p className="mt-2 text-slate-600">Selun is analyzing market conditions and constructing your allocation.</p>

      <div className="mt-5 h-2 w-full overflow-hidden rounded-full bg-slate-200">
        <div
          className="h-full rounded-full bg-cyan-500 transition-all duration-500"
          style={{ width: `${progressPercent}%` }}
        />
      </div>

      <div className="mt-6 space-y-3">
        {steps.map((step, index) => {
          let statusText = "Pending";
          let isDone = false;
          let isCurrent = false;
          let isFailed = false;

          if (index === 0) {
            isDone = phase1Status === "complete";
            isCurrent = phase1Status === "in_progress" || phase1Status === "starting";
            isFailed = phase1Status === "failed";
          } else if (index === 1) {
            isDone = phase2Status === "complete";
            isCurrent = phase1Status === "complete" && phase2Status === "in_progress";
            isFailed = phase2Status === "failed";
          } else if (index === 2) {
            isDone = phase3Status === "complete";
            isCurrent = phase2Status === "complete" && phase3Status === "in_progress";
            isFailed = phase3Status === "failed";
          } else if (index === 3) {
            isDone = phase4Status === "complete";
            isCurrent = phase3Status === "complete" && phase4Status === "in_progress";
            isFailed = phase4Status === "failed";
          } else if (index === 4) {
            isDone = phase5Status === "complete";
            isCurrent = phase4Status === "complete" && phase5Status === "in_progress";
            isFailed = phase5Status === "failed";
          } else if (index === 5) {
            isDone = phase6Status === "complete";
            isCurrent = phase5Status === "complete" && phase6Status === "in_progress";
            isFailed = phase6Status === "failed";
          } else if (index === 6 && hasReportGenerationStep) {
            isDone = phase6Status === "complete";
            isCurrent = false;
            isFailed = false;
          } else {
            isDone = false;
            isCurrent = false;
            isFailed = false;
          }
          statusText = isDone ? "Done" : isCurrent ? "In Progress" : isFailed ? "Failed" : "Pending";

          return (
            <div
              key={step.key}
              className={`flex items-center justify-between rounded-xl border px-4 py-3 transition ${
                isFailed
                  ? "border-rose-300 bg-rose-50"
                  : isCurrent
                  ? "border-cyan-300 bg-cyan-50"
                  : isDone
                    ? "border-emerald-200 bg-emerald-50"
                    : "border-slate-300 bg-white"
              }`}
            >
              <p className="text-sm font-medium text-slate-700">{step.label}</p>
              <span
                className={`rounded-full px-2.5 py-1 text-xs font-semibold uppercase tracking-[0.12em] ${
                  isFailed
                    ? "bg-rose-600 text-rose-50"
                    : isCurrent
                    ? "bg-cyan-600 text-cyan-50"
                    : isDone
                      ? "bg-emerald-600 text-emerald-50"
                      : "bg-slate-400 text-slate-50"
                }`}
              >
                {statusText}
              </span>
            </div>
          );
        })}
      </div>

      {phase1Status !== "idle" && (
        <div className="mt-5 rounded-xl border border-slate-300/70 bg-white/80 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Phase 1 Sub-Phases</p>
          <div className="mt-3 grid gap-2 md:grid-cols-2">
            {Object.entries(PHASE1_SUB_PHASE_LABELS).map(([subPhaseKey, label]) => {
              const subPhaseStatus = phase1SubPhaseStates[subPhaseKey] ?? "pending";
              const statusClass =
                subPhaseStatus === "complete"
                  ? "bg-emerald-600 text-emerald-50"
                  : subPhaseStatus === "in_progress"
                    ? "bg-cyan-600 text-cyan-50"
                    : subPhaseStatus === "failed"
                      ? "bg-rose-600 text-rose-50"
                      : "bg-slate-400 text-slate-50";

              return (
                <div
                  key={subPhaseKey}
                  className="flex items-center justify-between rounded-lg border border-slate-200 bg-slate-50/80 px-3 py-2"
                >
                  <span className="text-sm text-slate-700">{label}</span>
                  <span className={`rounded-full px-2 py-0.5 text-[11px] font-semibold uppercase ${statusClass}`}>
                    {subPhaseStatus === "in_progress"
                      ? "In Progress"
                      : subPhaseStatus === "complete"
                        ? "Done"
                        : subPhaseStatus === "failed"
                          ? "Failed"
                          : "Pending"}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {phase1Error && (
        <p className="mt-4 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {phase1Error}
        </p>
      )}

      {phase2Error && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {phase2Error}
        </p>
      )}

      {phase3Error && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {phase3Error}
        </p>
      )}

      {phase4Error && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {phase4Error}
        </p>
      )}

      {phase5Error && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {phase5Error}
        </p>
      )}

      {phase6Error && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {phase6Error}
        </p>
      )}

      {phase2Status === "in_progress" && !phase2Output && (
        <p className="mt-4 rounded-lg border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm font-medium text-cyan-800">
          Phase 2 policy envelope is running...
        </p>
      )}

      {phase2Status === "complete" && (
        <div className="mt-4">
          <p className="rounded-lg border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm font-medium text-cyan-800">
            Phase 2 complete. Phase 3 auto-starts to expand the eligible universe.
          </p>
        </div>
      )}

      {phase3Status !== "idle" && (
        <div className="mt-5 rounded-xl border border-slate-300/70 bg-white/80 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Phase 3 Sub-Phases</p>
          <div className="mt-3 grid gap-2 md:grid-cols-2">
            {Object.entries(PHASE3_SUB_PHASE_LABELS).map(([subPhaseKey, label]) => {
              const subPhaseStatus = phase3SubPhaseStates[subPhaseKey] ?? "pending";
              const statusClass =
                subPhaseStatus === "complete"
                  ? "bg-emerald-600 text-emerald-50"
                  : subPhaseStatus === "in_progress"
                    ? "bg-cyan-600 text-cyan-50"
                    : subPhaseStatus === "failed"
                      ? "bg-rose-600 text-rose-50"
                      : "bg-slate-400 text-slate-50";

              return (
                <div
                  key={subPhaseKey}
                  className="flex items-center justify-between rounded-lg border border-slate-200 bg-slate-50/80 px-3 py-2"
                >
                  <span className="text-sm text-slate-700">{label}</span>
                  <span className={`rounded-full px-2 py-0.5 text-[11px] font-semibold uppercase ${statusClass}`}>
                    {subPhaseStatus === "in_progress"
                      ? "In Progress"
                      : subPhaseStatus === "complete"
                        ? "Done"
                        : subPhaseStatus === "failed"
                          ? "Failed"
                          : "Pending"}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {phase3Status === "in_progress" && !phase3Output && (
        <p className="mt-4 rounded-lg border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm font-medium text-cyan-800">
          Phase 3 universe expansion is running...
        </p>
      )}

      {phase3Status === "complete" && (
        <div className="mt-4">
          <p className="rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2 text-sm font-medium text-indigo-800">
            Phase 3 complete. Eligible asset universe is ready for Phase 4 structural screening.
          </p>
        </div>
      )}

      {phase4Status === "in_progress" && !phase4Output && (
        <p className="mt-4 rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2 text-sm font-medium text-indigo-800">
          Phase 4 liquidity and structural screening is running...
        </p>
      )}

      {phase4Status === "complete" && (
        <div className="mt-4">
          <p className="rounded-lg border border-violet-200 bg-violet-50 px-3 py-2 text-sm font-medium text-violet-800">
            Phase 4 complete. Liquidity and structural stability filters are applied.
          </p>
        </div>
      )}

      {phase5Status === "in_progress" && !phase5Output && (
        <p className="mt-4 rounded-lg border border-teal-200 bg-teal-50 px-3 py-2 text-sm font-medium text-teal-800">
          Phase 5 risk and quality evaluation is running...
        </p>
      )}

      {phase5Status === "complete" && (
        <div className="mt-4">
          <p className="rounded-lg border border-teal-200 bg-teal-50 px-3 py-2 text-sm font-medium text-teal-800">
            Phase 5 complete. Quality shortlist is ready for Phase 6 allocation.
          </p>
        </div>
      )}

      {phase6Status === "in_progress" && !phase6Output && (
        <p className="mt-4 rounded-lg border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm font-medium text-cyan-800">
          Phase 6 allocation construction is running...
        </p>
      )}

      {phase6Output && !hasAaaPhase6Surface && (
        <p className="mt-4 rounded-lg border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm font-medium text-cyan-800">
          Phase 6 allocation payload received. Preparing final output...
        </p>
      )}

      {hasAaaPhase6Surface && phase6AaaAllocate && (
        <div className="mt-5 rounded-xl border border-sky-200 bg-sky-50/50 p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-sm font-semibold text-slate-800">Phase 6 Allocation Response</p>
            <span
              className={`rounded-full px-2 py-0.5 text-[11px] font-semibold uppercase tracking-[0.12em] ${
                phase6AaaAllocate.status === "complete"
                  ? "bg-emerald-600 text-emerald-50"
                  : phase6AaaAllocate.status === "in_progress"
                    ? "bg-cyan-600 text-cyan-50"
                    : phase6AaaAllocate.status === "failed"
                      ? "bg-rose-600 text-rose-50"
                      : "bg-slate-400 text-slate-50"
              }`}
            >
              {phase6AaaAllocate.status}
            </span>
          </div>
          {(phase6AaaAllocate.endpoint || phase6AaaAllocate.httpStatus !== undefined) && (
            <p className="mt-2 text-xs text-slate-600">
              Endpoint: {phase6AaaAllocate.endpoint ?? "n/a"}{" "}
              {phase6AaaAllocate.httpStatus !== undefined ? `| HTTP ${phase6AaaAllocate.httpStatus}` : ""}
            </p>
          )}
          {phase6AaaAllocate.error && (
            <p className="mt-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
              {phase6AaaAllocate.error}
            </p>
          )}
          {phase6AaaAllocate.status === "complete" && !phase6AaaAllocate.error && (
            <p className="mt-2 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm font-medium text-emerald-800">
              AAA allocation accepted. Loading allocation output...
            </p>
          )}
        </div>
      )}

      {(phase1Status === "failed" || phase2Status === "failed") && (
        <div className="mt-4">
          <button
            type="button"
            onClick={onRetryPhase1}
            className="rounded-full border border-rose-300 bg-white px-4 py-2 text-sm font-semibold text-rose-700 transition hover:border-rose-400"
          >
            Retry Phase 1
          </button>
        </div>
      )}

      {phase3Status === "failed" && (
        <div className="mt-3">
          <button
            type="button"
            onClick={onRetryPhase3}
            className="rounded-full border border-rose-300 bg-white px-4 py-2 text-sm font-semibold text-rose-700 transition hover:border-rose-400"
          >
            Retry Phase 3
          </button>
        </div>
      )}

      {phase4Status === "failed" && (
        <div className="mt-3">
          <button
            type="button"
            onClick={onRetryPhase4}
            className="rounded-full border border-rose-300 bg-white px-4 py-2 text-sm font-semibold text-rose-700 transition hover:border-rose-400"
          >
            Retry Phase 4
          </button>
        </div>
      )}

      {phase5Status === "failed" && (
        <div className="mt-3">
          <button
            type="button"
            onClick={onRetryPhase5}
            className="rounded-full border border-rose-300 bg-white px-4 py-2 text-sm font-semibold text-rose-700 transition hover:border-rose-400"
          >
            Retry Phase 5
          </button>
        </div>
      )}

      {phase6Status === "failed" && (
        <div className="mt-3">
          <button
            type="button"
            onClick={onRetryPhase6}
            className="rounded-full border border-rose-300 bg-white px-4 py-2 text-sm font-semibold text-rose-700 transition hover:border-rose-400"
          >
            Retry Phase 6
          </button>
        </div>
      )}
    </section>
  );
}

type CompleteStepProps = {
  regimeDetected: string;
  allocations: AllocationRow[];
  phase7Enabled: boolean;
  walletAddress: string | null;
  agentPaymentReceipt: AgentPaymentReceipt | null;
  resultEmail: string;
  resultEmailDeliveryStatus: ResultEmailDeliveryStatus;
  resultEmailDeliveryMessage: string | null;
  phase1Output: Phase1Output | null;
  phase2Output: Phase2Output | null;
  phase6AaaAllocate: AaaAllocateDispatch | null;
  downloadError: string | null;
  isDownloading: boolean;
  onRetryResultEmail: () => void;
  onDownloadReport: () => void;
  onStartOver: () => void;
};

function CompleteStep({
  regimeDetected,
  allocations,
  phase7Enabled,
  walletAddress,
  agentPaymentReceipt,
  resultEmail,
  resultEmailDeliveryStatus,
  resultEmailDeliveryMessage,
  phase1Output,
  phase2Output,
  phase6AaaAllocate,
  downloadError,
  isDownloading,
  onRetryResultEmail,
  onDownloadReport,
  onStartOver,
}: CompleteStepProps) {
  const [portfolioTotalUsdInput, setPortfolioTotalUsdInput] = useState("10000");

  const parseUsdInput = (value: string | undefined): number => {
    if (!value) return 0;
    const parsed = Number.parseFloat(value);
    if (!Number.isFinite(parsed) || parsed < 0) return 0;
    return parsed;
  };

  const formatUsd = (value: number): string =>
    new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  const formatPct = (value: number): string => `${value.toFixed(2).replace(/\.?0+$/, "")}%`;
  const toTitleCase = (value: string): string => {
    const normalized = value.replace(/[_-]+/g, " ").trim();
    if (!normalized) return "Unknown";
    return normalized
      .split(/\s+/)
      .map((token) => token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
      .join(" ");
  };
  const extractAaaResponsePayload = (dispatch: AaaAllocateDispatch | null): Record<string, unknown> | null => {
    if (!dispatch || !isObjectRecord(dispatch.response)) return null;
    if (isObjectRecord(dispatch.response.data)) return dispatch.response.data;
    return dispatch.response;
  };
  const readObjectPath = (root: unknown, path: string[]): unknown => {
    let cursor: unknown = root;
    for (const segment of path) {
      if (!isObjectRecord(cursor)) return undefined;
      cursor = cursor[segment];
    }
    return cursor;
  };
  const asFiniteNumber = (value: unknown): number | null => {
    const parsed = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
    return Number.isFinite(parsed) ? parsed : null;
  };
  const mapPolicyModeLabel = (mode: Phase2Output["allocation_policy"]["mode"] | null): string => {
    if (!mode) return "Unspecified";
    if (mode === "capital_preservation") return "Capital Preservation";
    if (mode === "balanced_defensive") return "Balanced Defensive";
    if (mode === "balanced_growth") return "Balanced Growth";
    if (mode === "offensive_growth") return "Growth-Focused";
    return "Unspecified";
  };
  const mapLiquidityLabel = (value: string): string => {
    const normalized = value.trim().toLowerCase();
    if (normalized === "stable" || normalized === "normal") return "Healthy";
    if (normalized === "weak" || normalized === "tight") return "Tight";
    return toTitleCase(value);
  };
  const ROLE_GROUP_CONFIG = [
    { key: "Defensive", label: "Stable Holdings" },
    { key: "Core", label: "Core Holdings" },
    { key: "Carry", label: "Income Position" },
    { key: "Satellite", label: "Growth Positions" },
    { key: "Liquidity", label: "Liquidity Reserve" },
    { key: "Speculative", label: "High-Risk Positions" },
  ] as const;
  type RoleGroupKey = (typeof ROLE_GROUP_CONFIG)[number]["key"];
  const mapRoleGroupLabel = (value: RoleGroupKey): string => {
    const match = ROLE_GROUP_CONFIG.find((entry) => entry.key === value);
    return match?.label ?? value;
  };
  const mapRoleDisplayLabel = (value: string): string => {
    const normalized = value.trim().toLowerCase();
    if (normalized === "carry") return "Income Position";
    if (normalized === "satellite") return "Growth Position";
    if (normalized === "liquidity") return "Liquidity Reserve";
    if (normalized === "speculative" || normalized === "high volatility" || normalized === "high_volatility") {
      return "High-Risk Position";
    }
    if (normalized === "stability") return "Stable Holding";
    return toTitleCase(value);
  };
  const normalizeRoleGroupLabel = (value: string, riskClass: string): RoleGroupKey => {
    const normalized = value.trim().toLowerCase();
    if (normalized === "defensive" || normalized === "stability" || normalized === "stable holdings" || normalized === "stable holding") {
      return "Defensive";
    }
    if (normalized === "core" || normalized === "core holdings") return "Core";
    if (normalized === "carry" || normalized === "income position") return "Carry";
    if (normalized === "satellite" || normalized === "growth positions" || normalized === "growth position") {
      return "Satellite";
    }
    if (normalized === "liquidity" || normalized === "liquidity reserve") return "Liquidity";
    if (
      normalized === "speculative" ||
      normalized === "high volatility" ||
      normalized === "high_volatility" ||
      normalized === "high-risk positions" ||
      normalized === "high-risk position"
    ) {
      return "Speculative";
    }
    if (STABILITY_RISK_CLASSES.has(riskClass)) return "Defensive";
    return "Satellite";
  };

  const requestedTotal = parseUsdInput(portfolioTotalUsdInput);
  const portfolioTotalUsd = requestedTotal > 0 ? requestedTotal : 10_000;
  const roundUsd = (value: number): number => Math.round(value * 100) / 100;
  const portfolioUsdStep = portfolioTotalUsd >= 10_000 ? 1_000 : portfolioTotalUsd >= 5_000 ? 500 : 100;
  const aaaPayload = extractAaaResponsePayload(phase6AaaAllocate);
  const policyRiskBudget =
    phase2Output?.policy_envelope.risk_budget ??
    asFiniteNumber(readObjectPath(aaaPayload, ["policy_snapshot", "policy_envelope", "risk_budget"]));
  const riskBudgetUsed =
    asFiniteNumber(
      readObjectPath(aaaPayload, ["allocation_result", "meta", "constraints_effective", "risk_budget"]),
    ) ?? policyRiskBudget;
  const riskCapacityRemaining =
    riskBudgetUsed !== null && policyRiskBudget !== null ? Math.max(0, policyRiskBudget - riskBudgetUsed) : null;
  const strategyLabel = mapPolicyModeLabel(phase2Output?.allocation_policy.mode ?? null);
  const regimeLabel = phase1Output
    ? toTitleCase(phase1Output.market_condition.risk_appetite)
    : regimeDetected.split("|")[0]?.replace(/posture|market outlook|outlook|market condition|condition/gi, "").trim() || "Unknown";
  const volatilityLabel = phase1Output ? toTitleCase(phase1Output.market_condition.volatility_state) : "Unknown";
  const liquidityLabel = phase1Output ? mapLiquidityLabel(phase1Output.market_condition.liquidity_state) : "Unknown";
  const userRiskLevel = phase2Output?.inputs.user_profile.risk_tolerance ?? "n/a";
  const marketConfidenceScore =
    asFiniteNumber(readObjectPath(aaaPayload, ["allocation_result", "meta", "market_regime_confidence"])) ??
    (phase1Output ? phase1Output.market_condition.confidence : null);
  const confidencePctLabel =
    marketConfidenceScore !== null ? `${Math.round(marketConfidenceScore * 100)}%` : "n/a";
  const fearGreedScoreRaw =
    phase1Output?.evidence.sentiment_metrics.fear_greed_available === true
      ? phase1Output.evidence.sentiment_metrics.fear_greed_index
      : null;
  const fearGreedScore =
    fearGreedScoreRaw !== null && Number.isFinite(fearGreedScoreRaw)
      ? Math.max(0, Math.min(100, fearGreedScoreRaw))
      : null;
  const fearGreedView = (() => {
    if (fearGreedScore === null) {
      return {
        label: "Unavailable",
        chipClass: "border-slate-300 bg-slate-100 text-slate-700",
        barClass: "bg-slate-400",
        rationale: "Sentiment gauge unavailable for this run.",
      };
    }
    if (fearGreedScore <= 24) {
      return {
        label: "Extreme Fear",
        chipClass: "border-rose-300 bg-rose-100 text-rose-800",
        barClass: "bg-rose-500",
        rationale: "High fear supports a more defensive allocation mix.",
      };
    }
    if (fearGreedScore <= 44) {
      return {
        label: "Fear",
        chipClass: "border-amber-300 bg-amber-100 text-amber-800",
        barClass: "bg-amber-500",
        rationale: "Elevated caution supports defensive tilts.",
      };
    }
    if (fearGreedScore <= 54) {
      return {
        label: "Neutral",
        chipClass: "border-slate-300 bg-slate-100 text-slate-800",
        barClass: "bg-slate-500",
        rationale: "Balanced sentiment supports neutral positioning.",
      };
    }
    if (fearGreedScore <= 74) {
      return {
        label: "Greed",
        chipClass: "border-emerald-300 bg-emerald-100 text-emerald-800",
        barClass: "bg-emerald-500",
        rationale: "Positive sentiment can support measured risk-taking.",
      };
    }
    return {
      label: "Extreme Greed",
      chipClass: "border-cyan-300 bg-cyan-100 text-cyan-800",
      barClass: "bg-cyan-500",
      rationale: "Very strong sentiment may raise overheating risk.",
    };
  })();
  const riskExposureRatio =
    riskBudgetUsed !== null
      ? policyRiskBudget !== null && policyRiskBudget > 0
        ? riskBudgetUsed / policyRiskBudget
        : riskBudgetUsed
      : null;
  const riskExposureRatioClamped =
    riskExposureRatio !== null ? Math.max(0, Math.min(1, riskExposureRatio)) : null;
  const riskExposureText = riskExposureRatio !== null ? `${formatPct(riskExposureRatio * 100)} of allowed range` : "n/a";
  const portfolioRiskLevel =
    riskExposureRatio === null ? "n/a" : riskExposureRatio >= 0.75 ? "High" : riskExposureRatio >= 0.4 ? "Moderate" : "Low";
  const regimeTone = (() => {
    const normalized = regimeLabel.toLowerCase();
    if (normalized.includes("defensive") || normalized.includes("risk_off")) {
      return {
        headlineClass: "text-cyan-900",
        badgeClass: "border-cyan-300 bg-cyan-100/80 text-cyan-900",
      };
    }
    if (normalized.includes("expansionary") || normalized.includes("aggressive") || normalized.includes("risk_on")) {
      return {
        headlineClass: "text-amber-900",
        badgeClass: "border-amber-300 bg-amber-100/80 text-amber-900",
      };
    }
    return {
      headlineClass: "text-slate-900",
      badgeClass: "border-slate-300 bg-slate-100/80 text-slate-900",
    };
  })();

  const handlePortfolioTotalChange = (value: string) => {
    if (!/^\d*\.?\d{0,2}$/.test(value)) return;
    setPortfolioTotalUsdInput(value);
  };
  const handleAdjustPortfolioTotal = (direction: -1 | 1) => {
    const current = requestedTotal > 0 ? requestedTotal : 10_000;
    const next = Math.max(0, roundUsd(current + direction * portfolioUsdStep));
    setPortfolioTotalUsdInput(next.toFixed(2));
  };

  const allocationBasisPoints = allocations.map((row) => Math.max(0, Math.round(row.allocationPct * 100)));
  const totalAllocationBasisPoints = allocationBasisPoints.reduce((sum, value) => sum + value, 0);
  const portfolioTotalCents = Math.max(0, Math.round(portfolioTotalUsd * 100));
  const rawUsdAllocations = allocationBasisPoints.map((basisPoints, index) => {
    const divisor = totalAllocationBasisPoints > 0 ? totalAllocationBasisPoints : 1;
    const rawCents = (portfolioTotalCents * basisPoints) / divisor;
    return {
      index,
      cents: Math.floor(rawCents),
      remainder: rawCents - Math.floor(rawCents),
    };
  });
  let remainingUsdCents = portfolioTotalCents - rawUsdAllocations.reduce((sum, entry) => sum + entry.cents, 0);
  const usdAdjustmentOrder = rawUsdAllocations
    .slice()
    .sort((left, right) => right.remainder - left.remainder || allocations[left.index].asset.localeCompare(allocations[right.index].asset));
  for (let orderIndex = 0; remainingUsdCents > 0 && usdAdjustmentOrder.length > 0; orderIndex += 1) {
    const target = usdAdjustmentOrder[orderIndex % usdAdjustmentOrder.length];
    rawUsdAllocations[target.index].cents += 1;
    remainingUsdCents -= 1;
  }

  const rowsForDisplay = allocations.map((row, index) => ({
    ...row,
    usdAmount: rawUsdAllocations[index].cents / 100,
    roleTooltip: PORTFOLIO_ROLE_TOOLTIPS[row.category] ?? PORTFOLIO_ROLE_TOOLTIPS.Allocation,
    roleGroup: normalizeRoleGroupLabel(row.category, row.riskClass),
  }));
  const rowsTotalUsd = portfolioTotalCents / 100;
  const totalAllocationPct = totalAllocationBasisPoints / 100;
  const roleGroupOrder = ROLE_GROUP_CONFIG.map((entry) => entry.key);
  const roleGroupTotals = new Map<RoleGroupKey, number>();
  for (const group of roleGroupOrder) roleGroupTotals.set(group, 0);
  for (const [index, row] of rowsForDisplay.entries()) {
    roleGroupTotals.set(row.roleGroup, (roleGroupTotals.get(row.roleGroup) ?? 0) + allocationBasisPoints[index]);
  }
  const groupedRows = roleGroupOrder
    .map((group) => ({
      group,
      rows: rowsForDisplay
        .filter((row) => row.roleGroup === group)
        .sort((left, right) => right.allocationPct - left.allocationPct || left.asset.localeCompare(right.asset)),
    }))
    .filter((entry) => entry.rows.length > 0);
  const roleOverviewStats = ROLE_GROUP_CONFIG.map((group) => ({
    label: group.label,
    value: formatPct((roleGroupTotals.get(group.key) ?? 0) / 100),
  }));
  const hasResultEmail = resultEmail.trim().length > 0;
  const resultEmailTone =
    resultEmailDeliveryStatus === "sent"
      ? "border-emerald-300 bg-emerald-50 text-emerald-900"
      : resultEmailDeliveryStatus === "failed"
        ? "border-rose-300 bg-rose-50 text-rose-900"
        : resultEmailDeliveryStatus === "sending"
          ? "border-cyan-300 bg-cyan-50 text-cyan-900"
          : "border-slate-300 bg-slate-50 text-slate-800";
  const resultEmailHeadline =
    resultEmailDeliveryStatus === "sent"
      ? "Allocation summary emailed"
      : resultEmailDeliveryStatus === "failed"
        ? "Allocation summary email failed"
        : resultEmailDeliveryStatus === "sending"
          ? "Sending allocation summary"
          : hasResultEmail
            ? "Allocation summary email queued"
            : "No result email requested";
  const resultEmailBody = hasResultEmail
    ? resultEmailDeliveryMessage ??
      (resultEmailDeliveryStatus === "sent"
        ? `A summary of this run was sent to ${resultEmail}.`
        : resultEmailDeliveryStatus === "failed"
          ? `Selun could not deliver the summary to ${resultEmail}.`
          : resultEmailDeliveryStatus === "sending"
            ? `Sending this run summary to ${resultEmail}.`
            : `Selun will keep using ${resultEmail} for any report email delivery from this run.`)
    : "No summary email was requested for this run.";

  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 4. Allocation Complete</h2>
      <p className="mt-2 text-slate-600">
        {phase7Enabled
          ? "Allocation executed successfully. Certified Decision Report is ready."
          : "Allocation executed successfully. Structured Decision Report not included in this run."}
      </p>

      {(walletAddress || agentPaymentReceipt) && (
        <div className="mt-4 rounded-xl border border-emerald-300/80 bg-emerald-50 px-4 py-3">
          <p className="text-sm font-semibold text-emerald-900">Allocation executed via Selun Agent</p>
          <p className="mt-1 text-xs text-emerald-900/80">
            Wallet: {walletAddress ? shortenAddress(walletAddress) : "n/a"} | Decision ID{" "}
            {agentPaymentReceipt?.decisionId ?? "n/a"}
          </p>
        </div>
      )}

      <div className="mt-6 grid gap-4 lg:grid-cols-12">
        <div className="rounded-xl border border-slate-300/70 bg-slate-50/70 p-4 lg:col-span-5">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Tier 1 - Outcome</p>
          <p className="mt-3 text-sm font-medium text-slate-600">Market Condition</p>
          <p className={`text-3xl font-black tracking-[0.08em] ${regimeTone.headlineClass}`}>{`${regimeLabel.toUpperCase()}`}</p>
          <p className="mt-2 text-sm text-slate-700">Strategy: {strategyLabel}</p>
          <div className="mt-3 flex flex-wrap gap-2">
            <span className={`rounded-full border px-2.5 py-1 text-xs font-semibold ${regimeTone.badgeClass}`}>
              Condition: {regimeLabel}
            </span>
            <span className="rounded-full border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700">
              Volatility: {volatilityLabel}
            </span>
            <span className="rounded-full border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700">
              Liquidity: {liquidityLabel}
            </span>
            <span className="rounded-full border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700">
              Confidence: {confidencePctLabel}
            </span>
            <span className={`rounded-full border px-2.5 py-1 text-xs font-medium ${fearGreedView.chipClass}`}>
              Fear & Greed: {fearGreedView.label}
              {fearGreedScore !== null ? ` (${Math.round(fearGreedScore)})` : ""}
            </span>
          </div>
          <div className="mt-3 rounded-lg border border-slate-300/70 bg-white px-3 py-2">
            <div className="flex items-center justify-between gap-3">
              <p className="text-[11px] font-bold uppercase tracking-[0.12em] text-slate-500">Fear & Greed</p>
              <p className="text-xs font-semibold text-slate-800">
                {fearGreedScore !== null ? `${Math.round(fearGreedScore)} / 100` : "n/a"}
              </p>
            </div>
            <div className="mt-1 h-2.5 rounded-full bg-slate-200">
              <div
                className={`h-2.5 rounded-full ${fearGreedView.barClass}`}
                style={{ width: fearGreedScore !== null ? `${Math.max(2, fearGreedScore)}%` : "2%" }}
              />
            </div>
            <p className="mt-1 text-xs text-slate-600">{fearGreedView.rationale}</p>
          </div>
          <div className="mt-4 rounded-lg border border-slate-300/70 bg-white px-3 py-2">
            <p className="text-[11px] font-bold uppercase tracking-[0.12em] text-slate-500">How This Was Built</p>
            <div className="mt-2 grid gap-2 text-xs text-slate-700">
              <p>
                <span className="font-semibold text-slate-800">Strategy:</span> {strategyLabel}
              </p>
              <p>
                <span className="font-semibold text-slate-800">Your Risk Tolerance:</span> {userRiskLevel}
              </p>
              <p>
                <span className="font-semibold text-slate-800">Confidence:</span> {confidencePctLabel}
              </p>
            </div>
          </div>
        </div>

        <div className="rounded-xl border border-slate-300/70 bg-slate-50/70 p-4 lg:col-span-7">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Tier 2 - Portfolio Summary</p>
          <div className="mt-3 flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="text-sm font-medium text-slate-600">Portfolio USD</p>
              <div className="mt-1 inline-flex items-center rounded-lg border border-slate-300 bg-white">
                <button
                  type="button"
                  onClick={() => handleAdjustPortfolioTotal(-1)}
                  className="h-9 w-9 border-r border-slate-200 text-lg font-semibold text-slate-700 transition hover:bg-slate-50"
                  aria-label="Decrease portfolio total"
                >
                  -
                </button>
                <input
                  type="text"
                  inputMode="decimal"
                  value={portfolioTotalUsdInput}
                  onChange={(event) => handlePortfolioTotalChange(event.target.value)}
                  className="h-9 w-36 bg-transparent px-2 text-right text-sm font-semibold text-slate-900 focus:outline-none"
                />
                <button
                  type="button"
                  onClick={() => handleAdjustPortfolioTotal(1)}
                  className="h-9 w-9 border-l border-slate-200 text-lg font-semibold text-slate-700 transition hover:bg-slate-50"
                  aria-label="Increase portfolio total"
                >
                  +
                </button>
              </div>
              <p className="mt-1 text-xs text-slate-500">Step: {formatUsd(portfolioUsdStep)}</p>
            </div>
            <div className="text-right">
              <p className="text-sm font-medium text-slate-600/80">Total Portfolio Assigned</p>
              <p className="text-xl font-bold tabular-nums text-slate-900">{formatPct(totalAllocationPct)}</p>
              <p className="text-xs text-slate-500">{rowsForDisplay.length} assets</p>
            </div>
          </div>

          <div className="mt-4 rounded-lg border border-slate-300/70 bg-white p-3">
            <p className="text-xs font-bold uppercase tracking-[0.14em] text-slate-500">
              Portfolio Mix Overview
            </p>
            <div className="mt-2 grid gap-2 [grid-template-columns:repeat(auto-fit,minmax(132px,1fr))]">
              {roleOverviewStats.map((stat) => (
                <div key={stat.label} className="rounded-md border border-slate-200 px-3 py-2">
                  <p className="text-[10px] uppercase tracking-[0.1em] text-slate-500/75">{stat.label}</p>
                  <p className="text-lg font-bold tabular-nums text-slate-900">{stat.value}</p>
                </div>
              ))}
            </div>
            <div className="mt-2 rounded-md border border-slate-200 px-3 py-2">
              <div className="flex items-center justify-between gap-3">
                <p className="text-[10px] uppercase tracking-[0.1em] text-slate-500/75">Risk Exposure</p>
                <p className="text-sm font-semibold text-slate-800">Portfolio Risk Level: {portfolioRiskLevel}</p>
              </div>
              <p className="mt-1 text-sm font-bold tabular-nums text-slate-900">{riskExposureText}</p>
              <div className="mt-1 h-2.5 rounded-full bg-slate-300/55">
                <div
                  className="h-2.5 rounded-full bg-cyan-500"
                  style={{
                    width:
                      riskExposureRatioClamped !== null
                        ? `${Math.max(2, Math.min(100, riskExposureRatioClamped * 100))}%`
                        : "0%",
                  }}
                />
              </div>
              <p className="mt-1 text-[11px] text-slate-600">
                Remaining room: {riskCapacityRemaining !== null ? formatPct(riskCapacityRemaining * 100) : "n/a"}
              </p>
            </div>
            <p className="mt-2 text-xs text-slate-500">Current total from row values: {formatUsd(rowsTotalUsd)}</p>
          </div>
        </div>
      </div>

      <div className="mt-6 overflow-auto rounded-xl border border-slate-300/70 bg-white">
        <table className="min-w-full border-collapse">
          <thead>
            <tr className="bg-slate-100">
              <th className="px-4 py-3 text-left text-xs font-bold uppercase tracking-[0.14em] text-slate-500">Asset</th>
              <th className="px-4 py-3 text-left text-xs font-bold uppercase tracking-[0.14em] text-slate-500">Role</th>
              <th className="px-4 py-3 text-left text-xs font-bold uppercase tracking-[0.14em] text-slate-500">Risk Class</th>
              <th className="px-4 py-3 text-right text-xs font-bold uppercase tracking-[0.14em] text-slate-500">Allocation %</th>
              <th className="px-4 py-3 text-right text-xs font-bold uppercase tracking-[0.14em] text-slate-500">USD Value</th>
            </tr>
          </thead>
          <tbody>
            {groupedRows.map((group) => (
              <Fragment key={group.group}>
                <tr className="border-t border-slate-300 bg-slate-50/80">
                  <td className="px-4 py-2" colSpan={5}>
                    <div className="flex items-center gap-3 text-xs font-bold uppercase tracking-[0.12em] text-slate-600">
                      <span className="h-px flex-1 bg-slate-300" />
                      <span>{mapRoleGroupLabel(group.group)} - {formatPct((roleGroupTotals.get(group.group) ?? 0) / 100)}</span>
                      <span className="h-px flex-1 bg-slate-300" />
                    </div>
                  </td>
                </tr>
                {group.rows.map((row) => {
                  const clampedPct = Math.max(0, Math.min(100, row.allocationPct));
                  return (
                    <tr key={row.asset} className="border-t border-slate-200">
                      <td className="px-4 py-4 text-sm text-slate-800">
                        <p className="font-semibold">{row.asset}</p>
                        <p className="text-xs text-slate-500">{row.name}</p>
                      </td>
                      <td className="px-4 py-4 text-sm text-slate-700">
                        <span className="inline-flex items-center gap-2">
                          <span>{mapRoleDisplayLabel(row.category)}</span>
                          <span
                            title={row.roleTooltip}
                            aria-label={`${row.category} explanation`}
                            className="inline-flex h-4 w-4 cursor-help items-center justify-center rounded-full border border-slate-300 text-[10px] font-bold text-slate-500"
                          >
                            ?
                          </span>
                        </span>
                      </td>
                      <td className="px-4 py-4 text-sm text-slate-500">{formatRiskClassLabel(row.riskClass)}</td>
                      <td className="px-4 py-4 text-right">
                        <p className="text-sm font-bold tabular-nums text-slate-900">{formatPct(row.allocationPct)}</p>
                        <div className="mt-1 ml-auto h-2.5 w-32 rounded-full bg-slate-300/55">
                          <div
                            className="h-2.5 rounded-full bg-cyan-500"
                            style={{ width: `${Math.max(2, clampedPct)}%` }}
                          />
                        </div>
                      </td>
                      <td className="px-4 py-4 text-right">
                        <span className="inline-block min-w-[8rem] rounded-md border border-slate-300 bg-slate-100 px-2 py-1 text-right text-sm font-semibold text-slate-900 shadow-inner">
                          {formatUsd(row.usdAmount)}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-6 flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={onStartOver}
          disabled={isDownloading}
          className="rounded-full border border-slate-400 bg-white px-5 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-slate-500 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Start Over
        </button>
        {phase7Enabled && (
          <button
            type="button"
            onClick={onDownloadReport}
            disabled={isDownloading}
            className="rounded-full bg-cyan-500 px-5 py-2.5 text-sm font-semibold text-slate-950 shadow-sm transition hover:bg-cyan-400 disabled:cursor-wait disabled:bg-cyan-300"
          >
            {isDownloading ? "Preparing Download..." : "Download Certified Decision Record"}
          </button>
        )}
      </div>

      <div className={`mt-4 rounded-xl border p-3 ${resultEmailTone}`}>
        <p className="text-xs font-bold uppercase tracking-[0.12em]">Result Email Delivery</p>
        <p className="mt-2 text-sm font-semibold">{resultEmailHeadline}</p>
        <p className="mt-1 text-sm">{resultEmailBody}</p>
        {phase7Enabled && hasResultEmail && (
          <p className="mt-2 text-xs">
            Downloading the certified record will also attempt PDF delivery to {resultEmail}.
          </p>
        )}
        {hasResultEmail && resultEmailDeliveryStatus === "failed" && (
          <button
            type="button"
            onClick={onRetryResultEmail}
            disabled={isDownloading}
            className="mt-3 rounded-full border border-current bg-white/80 px-4 py-2 text-sm font-semibold transition hover:bg-white disabled:cursor-not-allowed disabled:opacity-60"
          >
            Retry Summary Email
          </button>
        )}
      </div>

      {downloadError && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {downloadError}
        </p>
      )}
    </section>
  );
}

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  if (!Number.isFinite(ms) || ms <= 0) return promise;
  return new Promise<T>((resolve, reject) => {
    const id = window.setTimeout(() => {
      if (label.toLowerCase().includes("wallet network switch")) {
        reject(
          new Error(
            "Wallet network switch is taking too long. Open your wallet extension/app and confirm the network change, then click Generate again.",
          ),
        );
        return;
      }
      reject(new Error(`${label} timed out after ${ms}ms.`));
    }, ms);

    promise
      .then((v) => resolve(v))
      .catch(reject)
      .finally(() => window.clearTimeout(id));
  });
}

type PaymentStage =
  | "idle"
  | "preflight"
  | "agent_pay"
  | "wallet_tx_prompt"
  | "verifying"
  | "starting_execution";

function SelunAllocationWizard() {
  const pageTopRef = useRef<HTMLDivElement | null>(null);
  const resultEmailAttemptKeyRef = useRef<string | null>(null);
  const [wizardState, setWizardState] = useState<WizardState>("CONFIGURE");
  const [riskMode, setRiskMode] = useState<RiskMode | null>(DEFAULT_RISK_MODE);
  const [investmentHorizon, setInvestmentHorizon] = useState<InvestmentHorizon | null>(DEFAULT_HORIZON);
  const [resultEmail, setResultEmail] = useState("");
  const [resultEmailDeliveryStatus, setResultEmailDeliveryStatus] = useState<ResultEmailDeliveryStatus>("idle");
  const [resultEmailDeliveryMessage, setResultEmailDeliveryMessage] = useState<string | null>(null);
  const [includeCertifiedDecisionRecord, setIncludeCertifiedDecisionRecord] = useState(false);
  const [promoCode, setPromoCode] = useState("");
  const [promoQuote, setPromoQuote] = useState<PromoQuoteResult | null>(null);
  const [promoQuoteError, setPromoQuoteError] = useState<string | null>(null);
  const [isApplyingPromoCode, setIsApplyingPromoCode] = useState(false);
  const [basePriceUsdc, setBasePriceUsdc] = useState<number>(FALLBACK_BASE_PRICE_USDC);
  const [certifiedDecisionRecordFeeUsdc, setCertifiedDecisionRecordFeeUsdc] = useState<number>(
    FALLBACK_CERTIFIED_DECISION_RECORD_FEE_USDC,
  );
  const [isLoadingPricing, setIsLoadingPricing] = useState(true);
  const [pricingError, setPricingError] = useState<string | null>(null);
  const [walletAddress, setWalletAddress] = useState<string | null>(null);
  const [usdcNetworkId, setUsdcNetworkId] = useState("backend-configured");
  const [usdcNetworkLabel, setUsdcNetworkLabel] = useState("Configured in backend");
  const [usdcBalance, setUsdcBalance] = useState<number | null>(null);
  const [isLoadingUsdcBalance, setIsLoadingUsdcBalance] = useState(false);
  const [usdcBalanceError, setUsdcBalanceError] = useState<string | null>(null);
  const [usdcBalanceRefreshNonce, setUsdcBalanceRefreshNonce] = useState(0);
  const [isConnectingWallet, setIsConnectingWallet] = useState(false);
  const [paymentError, setPaymentError] = useState<string | null>(null);
  const [agentPaymentReceipt, setAgentPaymentReceipt] = useState<AgentPaymentReceipt | null>(null);
  const [isPaying, setIsPaying] = useState(false);
  const [paymentStage, setPaymentStage] = useState<PaymentStage>("idle");
  const [phase1JobId, setPhase1JobId] = useState<string | null>(null);
  const [phase1Status, setPhase1Status] = useState<Phase1Status>("idle");
  const [phase1Error, setPhase1Error] = useState<string | null>(null);
  const [phase1Output, setPhase1Output] = useState<Phase1Output | null>(null);
  const [phase2Status, setPhase2Status] = useState<"idle" | "in_progress" | "complete" | "failed">("idle");
  const [phase2Output, setPhase2Output] = useState<Phase2Output | null>(null);
  const [phase2Error, setPhase2Error] = useState<string | null>(null);
  const [phase3Status, setPhase3Status] = useState<"idle" | "in_progress" | "complete" | "failed">("idle");
  const [phase3Output, setPhase3Output] = useState<Phase3Output | null>(null);
  const [phase3Error, setPhase3Error] = useState<string | null>(null);
  const [isStartingPhase3, setIsStartingPhase3] = useState(false);
  const [phase4Status, setPhase4Status] = useState<"idle" | "in_progress" | "complete" | "failed">("idle");
  const [phase4Output, setPhase4Output] = useState<Phase4Output | null>(null);
  const [phase4Error, setPhase4Error] = useState<string | null>(null);
  const [isStartingPhase4, setIsStartingPhase4] = useState(false);
  const [phase5Status, setPhase5Status] = useState<"idle" | "in_progress" | "complete" | "failed">("idle");
  const [phase5Output, setPhase5Output] = useState<Phase5Output | null>(null);
  const [phase5Error, setPhase5Error] = useState<string | null>(null);
  const [isStartingPhase5, setIsStartingPhase5] = useState(false);
  const [phase6Status, setPhase6Status] = useState<"idle" | "in_progress" | "complete" | "failed">("idle");
  const [phase6Output, setPhase6Output] = useState<Phase6Output | null>(null);
  const [phase6AaaAllocate, setPhase6AaaAllocate] = useState<AaaAllocateDispatch | null>(null);
  const [phase6Error, setPhase6Error] = useState<string | null>(null);
  const [isStartingPhase6, setIsStartingPhase6] = useState(false);
  const [phase1SubPhaseStates, setPhase1SubPhaseStates] = useState<Record<string, SubPhaseStatus>>(
    createPhase1SubPhaseStates(),
  );
  const [phase3SubPhaseStates, setPhase3SubPhaseStates] = useState<Record<string, SubPhaseStatus>>(
    createPhase3SubPhaseStates(),
  );
  const [isDownloading, setIsDownloading] = useState(false);
  const totalPriceUsdc =
    basePriceUsdc + (includeCertifiedDecisionRecord ? certifiedDecisionRecordFeeUsdc : 0);
  const requiredAmountUsdc = promoQuote?.chargedAmountUsdc ?? totalPriceUsdc;
  const requiresPromoApply = promoCode.trim().length > 0 && !promoQuote;
  const regimeDetected = formatMarketRegimeLabel(phase1Output);
  const allocationRows = toAllocationRows(phase5Output, phase6Output, phase6AaaAllocate);
  const phase7Enabled = Boolean(agentPaymentReceipt?.certifiedDecisionRecordPurchased);
  const processingSteps = phase7Enabled
    ? PROCESSING_STEPS
    : PROCESSING_STEPS.filter((step) => step.key !== "REPORT_GENERATION");
  const isBalanceLow = Boolean(
    !isLoadingPricing &&
      !pricingError &&
      walletAddress &&
      usdcBalance !== null &&
      usdcBalance < requiredAmountUsdc,
  );

  const resetPaymentUi = useCallback(() => {
    setIsPaying(false);
    setPaymentStage("idle");
    setPaymentError(null);
  }, []);

  useEffect(() => {
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    pageTopRef.current?.focus();
  }, [wizardState]);

  const refreshPricing = useCallback(async () => {
    setIsLoadingPricing(true);
    setPricingError(null);
    try {
      const pricing = await queryPricing();
      setBasePriceUsdc(pricing.structuredAllocationPriceUsdc);
      setCertifiedDecisionRecordFeeUsdc(pricing.certifiedDecisionRecordFeeUsdc);
      setPromoQuote(null);
      setPromoQuoteError(null);
    } catch (error: unknown) {
      setPricingError(error instanceof Error ? error.message : "Unable to load pricing from backend.");
    } finally {
      setIsLoadingPricing(false);
    }
  }, []);

  useEffect(() => {
    void refreshPricing();
  }, [refreshPricing]);

  const handleToggleCertifiedDecisionRecord = useCallback((nextValue: boolean) => {
    setIncludeCertifiedDecisionRecord(nextValue);
    setPromoQuote(null);
    setPromoQuoteError(null);
  }, []);

  const handlePromoCodeChange = useCallback((value: string) => {
    setPromoCode(value);
    setPromoQuote(null);
    setPromoQuoteError(null);
  }, []);

  const handleApplyPromoCode = useCallback(async () => {
    const normalizedCode = promoCode.trim();
    if (!normalizedCode) {
      setPromoQuote(null);
      setPromoQuoteError("Enter a promo code first.");
      return;
    }
    if (!walletAddress) {
      setPromoQuote(null);
      setPromoQuoteError("Connect wallet first to validate promo code pricing.");
      return;
    }
    if (isLoadingPricing) {
      setPromoQuote(null);
      setPromoQuoteError("Pricing is still loading. Please wait and retry.");
      return;
    }
    if (pricingError) {
      setPromoQuote(null);
      setPromoQuoteError("Pricing is unavailable. Refresh pricing before applying a code.");
      return;
    }

    setIsApplyingPromoCode(true);
    setPromoQuoteError(null);
    try {
      const quote = await queryPaymentQuote({
        walletAddress,
        includeCertifiedDecisionRecord,
        promoCode: normalizedCode,
      });
      setPromoQuote(quote);
    } catch (error) {
      setPromoQuote(null);
      setPromoQuoteError(error instanceof Error ? error.message : "Unable to apply promo code.");
    } finally {
      setIsApplyingPromoCode(false);
    }
  }, [promoCode, walletAddress, includeCertifiedDecisionRecord, isLoadingPricing, pricingError]);

  useEffect(() => {
    if (wizardState !== "PROCESSING") return;
    if (!phase1JobId) return;

    let cancelled = false;

    const syncPhase1Status = async () => {
      try {
        const statusPayload = await queryExecutionStatus(phase1JobId);
        if (cancelled) return;

        const phaseStatusRaw = statusPayload.status ?? statusPayload.jobContext?.phase1?.status ?? "in_progress";
        const phase2StatusRaw =
          statusPayload.jobContext?.phase2?.status === "in_progress" ||
          statusPayload.jobContext?.phase2?.status === "complete" ||
          statusPayload.jobContext?.phase2?.status === "failed"
            ? statusPayload.jobContext.phase2.status
            : "idle";
        const phase3StatusRaw =
          statusPayload.jobContext?.phase3?.status === "in_progress" ||
          statusPayload.jobContext?.phase3?.status === "complete" ||
          statusPayload.jobContext?.phase3?.status === "failed"
            ? statusPayload.jobContext.phase3.status
            : "idle";
        const phase4StatusRaw =
          statusPayload.jobContext?.phase4?.status === "in_progress" ||
          statusPayload.jobContext?.phase4?.status === "complete" ||
          statusPayload.jobContext?.phase4?.status === "failed"
            ? statusPayload.jobContext.phase4.status
            : "idle";
        const phase5StatusRaw =
          statusPayload.jobContext?.phase5?.status === "in_progress" ||
          statusPayload.jobContext?.phase5?.status === "complete" ||
          statusPayload.jobContext?.phase5?.status === "failed"
            ? statusPayload.jobContext.phase5.status
            : "idle";
        const phase6StatusRaw =
          statusPayload.jobContext?.phase6?.status === "in_progress" ||
          statusPayload.jobContext?.phase6?.status === "complete" ||
          statusPayload.jobContext?.phase6?.status === "failed"
            ? statusPayload.jobContext.phase6.status
            : "idle";
        const logs = statusPayload.logs ?? [];

        const phase1SubPhaseStateUpdates = createPhase1SubPhaseStates();
        const phase3SubPhaseStateUpdates = createPhase3SubPhaseStates();

        for (const log of logs) {
          if (!log.subPhase) continue;
          const nextStatus = log.status;

          if (log.phase === "review_market_conditions" && log.subPhase in phase1SubPhaseStateUpdates) {
            if (nextStatus === "failed") {
              phase1SubPhaseStateUpdates[log.subPhase] = "failed";
              continue;
            }
            if (nextStatus === "complete") {
              phase1SubPhaseStateUpdates[log.subPhase] = "complete";
              continue;
            }
            if (nextStatus === "in_progress" && phase1SubPhaseStateUpdates[log.subPhase] !== "complete") {
              phase1SubPhaseStateUpdates[log.subPhase] = "in_progress";
            }
          }

          if (log.phase === "expand_eligible_asset_universe" && log.subPhase in phase3SubPhaseStateUpdates) {
            if (nextStatus === "failed") {
              phase3SubPhaseStateUpdates[log.subPhase] = "failed";
              continue;
            }
            if (nextStatus === "complete") {
              phase3SubPhaseStateUpdates[log.subPhase] = "complete";
              continue;
            }
            if (nextStatus === "in_progress" && phase3SubPhaseStateUpdates[log.subPhase] !== "complete") {
              phase3SubPhaseStateUpdates[log.subPhase] = "in_progress";
            }
          }
        }

        setPhase1SubPhaseStates(phase1SubPhaseStateUpdates);
        setPhase2Output(statusPayload.jobContext?.phase2?.output ?? null);
        setPhase2Error(statusPayload.jobContext?.phase2?.error ?? null);
        setPhase2Status(phase2StatusRaw);
        setPhase3SubPhaseStates(phase3SubPhaseStateUpdates);
        setPhase3Output(statusPayload.jobContext?.phase3?.output ?? null);
        setPhase3Error(statusPayload.jobContext?.phase3?.error ?? null);
        setPhase3Status(phase3StatusRaw);
        setPhase4Output(statusPayload.jobContext?.phase4?.output ?? null);
        setPhase4Error(statusPayload.jobContext?.phase4?.error ?? null);
        setPhase4Status(phase4StatusRaw);
        setPhase5Output(statusPayload.jobContext?.phase5?.output ?? null);
        setPhase5Error(statusPayload.jobContext?.phase5?.error ?? null);
        setPhase5Status(phase5StatusRaw);
        setPhase6Output(statusPayload.jobContext?.phase6?.output ?? null);
        setPhase6AaaAllocate(statusPayload.jobContext?.phase6?.aaaAllocate ?? null);
        setPhase6Error(statusPayload.jobContext?.phase6?.error ?? null);
        setPhase6Status(phase6StatusRaw);

        if (phaseStatusRaw === "complete") {
          setPhase1Status("complete");
          setPhase1Output(statusPayload.jobContext?.phase1?.output ?? null);
          setPhase1Error(null);
        } else if (phaseStatusRaw === "failed") {
          setPhase1Status("failed");
          setPhase1Error(
            statusPayload.jobContext?.phase1?.error ||
              statusPayload.error ||
              "Phase 1 failed in backend execution.",
          );
        } else {
          setPhase1Status("in_progress");
        }

      } catch (error) {
        if (cancelled) return;
        setPhase1Status("failed");
        setPhase1Error(error instanceof Error ? error.message : "Unable to poll Phase 1 execution.");
      }
    };

    void syncPhase1Status();
    const intervalId = window.setInterval(() => {
      void syncPhase1Status();
    }, PROCESSING_POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [wizardState, phase1JobId]);

  useEffect(() => {
    if (!walletAddress) {
      setUsdcNetworkId("backend-configured");
      setUsdcNetworkLabel("Configured in backend");
      setUsdcBalance(null);
      setUsdcBalanceError(null);
      setIsLoadingUsdcBalance(false);
      return;
    }

    if (!isHexAddress(walletAddress)) {
      setUsdcBalance(null);
      setUsdcBalanceError("Connected wallet address is invalid.");
      setIsLoadingUsdcBalance(false);
      return;
    }

    let cancelled = false;
    setIsLoadingUsdcBalance(true);
    setUsdcBalanceError(null);

    void queryUsdcBalance(walletAddress)
      .then((result) => {
        if (cancelled) return;
        setUsdcNetworkId(result.networkId);
        setUsdcNetworkLabel(result.networkLabel);
        setUsdcBalance(Number.isFinite(result.usdcBalance) ? result.usdcBalance : 0);
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        setUsdcBalance(null);
        setUsdcBalanceError(error instanceof Error ? error.message : "Unable to read USDC balance from backend.");
      })
      .finally(() => {
        if (!cancelled) setIsLoadingUsdcBalance(false);
      });

    return () => {
      cancelled = true;
    };
  }, [walletAddress, usdcBalanceRefreshNonce]);

  const canContinueFromConfigure = Boolean(riskMode && investmentHorizon);

  const handleContinueFromConfigure = () => {
    if (!canContinueFromConfigure) return;
    setWizardState("REVIEW");
  };

  const handleBackToConfigure = () => {
    if (isPaying) return;
    setPaymentError(null);
    setWizardState("CONFIGURE");
  };

  const handleConnectWallet = async () => {
    if (!window.ethereum?.request) {
      setPaymentError("No Ethereum wallet detected. Install MetaMask or another EVM wallet.");
      return;
    }

    setIsConnectingWallet(true);
    setPaymentError(null);

    try {
      const accountsResult = await window.ethereum.request({ method: "eth_requestAccounts" });

      if (!Array.isArray(accountsResult) || accountsResult.length === 0 || typeof accountsResult[0] !== "string") {
        throw new Error("Wallet connection did not return an account.");
      }

      setWalletAddress(accountsResult[0]);
      setAgentPaymentReceipt(null);
      setPromoQuote(null);
      setPromoQuoteError(null);
    } catch (error) {
      setPaymentError(error instanceof Error ? error.message : "Wallet connection failed.");
    } finally {
      setIsConnectingWallet(false);
    }
  };

  const handleRefreshUsdcBalance = () => {
    if (!walletAddress) return;
    setUsdcBalanceRefreshNonce((current) => current + 1);
    setPaymentError(null);
  };

  const startPhase1Flow = async (jobId: string) => {
    if (!riskMode || !investmentHorizon) {
      throw new Error("Risk mode and investment horizon are required to run Phase 1.");
    }

    setPhase1JobId(jobId);
    setPhase1Status("starting");
    setPhase1Error(null);
    setPhase1Output(null);
    setPhase2Status("idle");
    setPhase2Output(null);
    setPhase2Error(null);
    setPhase3Status("idle");
    setPhase3Output(null);
    setPhase3Error(null);
    setIsStartingPhase3(false);
    setPhase4Status("idle");
    setPhase4Output(null);
    setPhase4Error(null);
    setIsStartingPhase4(false);
    setPhase5Status("idle");
    setPhase5Output(null);
    setPhase5Error(null);
    setIsStartingPhase5(false);
    setPhase6Status("idle");
    setPhase6Output(null);
    setPhase6AaaAllocate(null);
    setPhase6Error(null);
    setIsStartingPhase6(false);
    setPhase1SubPhaseStates(createPhase1SubPhaseStates());
    setPhase3SubPhaseStates(createPhase3SubPhaseStates());

    await startPhase1Execution({
      jobId,
      executionTimestamp: new Date().toISOString(),
      riskMode: toBackendRiskMode(riskMode),
      riskTolerance: toBackendRiskTolerance(riskMode),
      investmentTimeframe: toBackendInvestmentTimeframe(investmentHorizon),
      timeWindow: toBackendTimeWindow(investmentHorizon),
      walletAddress: walletAddress ?? undefined,
    });

    setPhase1Status("in_progress");
    setWizardState("PROCESSING");
  };

  const handleRetryPhase1 = async () => {
    if (wizardState !== "PROCESSING") return;
    if (!riskMode || !investmentHorizon) return;

    try {
      const retryJobId = phase1JobId
        ? `${phase1JobId}-retry-${Date.now()}`
        : `selun-phase1-${Date.now()}`;
      await startPhase1Flow(retryJobId);
    } catch (error) {
      setPhase1Status("failed");
      setPhase1Error(error instanceof Error ? error.message : "Failed to restart Phase 1.");
    }
  };

  const handleStartPhase3 = useCallback(async () => {
    if (wizardState !== "PROCESSING") return;
    if (!phase1JobId) {
      setPhase3Status("failed");
      setPhase3Error("Missing job id for Phase 3. Retry Phase 1.");
      return;
    }
    if (phase1Status !== "complete" || phase2Status !== "complete") {
      return;
    }

    setIsStartingPhase3(true);
    setPhase3Error(null);
    setPhase3Output(null);
    setPhase3SubPhaseStates(createPhase3SubPhaseStates());
    setPhase4Status("idle");
    setPhase4Output(null);
    setPhase4Error(null);
    setPhase5Status("idle");
    setPhase5Output(null);
    setPhase5Error(null);
    setPhase6Status("idle");
    setPhase6Output(null);
    setPhase6AaaAllocate(null);
    setPhase6Error(null);

    try {
      await startPhase3Execution({ jobId: phase1JobId });
      setPhase3Status("in_progress");
    } catch (error) {
      setPhase3Status("failed");
      setPhase3Error(error instanceof Error ? error.message : "Failed to start Phase 3.");
    } finally {
      setIsStartingPhase3(false);
    }
  }, [phase1JobId, phase1Status, phase2Status, wizardState]);

  const handleRetryPhase3 = async () => {
    if (wizardState !== "PROCESSING") return;
    await handleStartPhase3();
  };

  const handleStartPhase4 = useCallback(async () => {
    if (wizardState !== "PROCESSING") return;
    if (!phase1JobId) {
      setPhase4Status("failed");
      setPhase4Error("Missing job id for Phase 4. Retry Phase 1.");
      return;
    }
    if (phase3Status !== "complete") {
      return;
    }

    setIsStartingPhase4(true);
    setPhase4Error(null);
    setPhase4Output(null);
    setPhase5Status("idle");
    setPhase5Output(null);
    setPhase5Error(null);
    setPhase6Status("idle");
    setPhase6Output(null);
    setPhase6AaaAllocate(null);
    setPhase6Error(null);

    try {
      await startPhase4Execution({ jobId: phase1JobId });
      setPhase4Status("in_progress");
    } catch (error) {
      setPhase4Status("failed");
      setPhase4Error(error instanceof Error ? error.message : "Failed to start Phase 4.");
    } finally {
      setIsStartingPhase4(false);
    }
  }, [phase1JobId, phase3Status, wizardState]);

  const handleRetryPhase4 = async () => {
    if (wizardState !== "PROCESSING") return;
    await handleStartPhase4();
  };

  const handleStartPhase5 = useCallback(async () => {
    if (wizardState !== "PROCESSING") return;
    if (!phase1JobId) {
      setPhase5Status("failed");
      setPhase5Error("Missing job id for Phase 5. Retry Phase 1.");
      return;
    }
    if (phase4Status !== "complete") {
      return;
    }

    setIsStartingPhase5(true);
    setPhase5Error(null);
    setPhase5Output(null);
    setPhase6Status("idle");
    setPhase6Output(null);
    setPhase6AaaAllocate(null);
    setPhase6Error(null);

    try {
      await startPhase5Execution({ jobId: phase1JobId });
      setPhase5Status("in_progress");
    } catch (error) {
      setPhase5Status("failed");
      setPhase5Error(error instanceof Error ? error.message : "Failed to start Phase 5.");
    } finally {
      setIsStartingPhase5(false);
    }
  }, [phase1JobId, phase4Status, wizardState]);

  const handleRetryPhase5 = async () => {
    if (wizardState !== "PROCESSING") return;
    await handleStartPhase5();
  };

  const handleStartPhase6 = useCallback(async () => {
    if (wizardState !== "PROCESSING") return;
    if (!phase1JobId) {
      setPhase6Status("failed");
      setPhase6Error("Missing job id for Phase 6. Retry Phase 1.");
      return;
    }
    if (phase5Status !== "complete") {
      return;
    }

    setIsStartingPhase6(true);
    setPhase6Error(null);
    setPhase6Output(null);
    setPhase6AaaAllocate(null);

    try {
      await startPhase6Execution({ jobId: phase1JobId });
      setPhase6Status("in_progress");
    } catch (error) {
      setPhase6Status("failed");
      setPhase6Error(error instanceof Error ? error.message : "Failed to start Phase 6.");
    } finally {
      setIsStartingPhase6(false);
    }
  }, [phase1JobId, phase5Status, wizardState]);

  const handleRetryPhase6 = async () => {
    if (wizardState !== "PROCESSING") return;
    await handleStartPhase6();
  };

  useEffect(() => {
    if (wizardState !== "PROCESSING") return;
    if (phase1Status !== "complete" || phase2Status !== "complete") return;
    if (phase3Status !== "idle") return;
    if (isStartingPhase3) return;
    void handleStartPhase3();
  }, [wizardState, phase1Status, phase2Status, phase3Status, isStartingPhase3, handleStartPhase3]);

  useEffect(() => {
    if (wizardState !== "PROCESSING") return;
    if (phase3Status !== "complete") return;
    if (phase4Status !== "idle") return;
    if (isStartingPhase4) return;
    void handleStartPhase4();
  }, [wizardState, phase3Status, phase4Status, isStartingPhase4, handleStartPhase4]);

  useEffect(() => {
    if (wizardState !== "PROCESSING") return;
    if (phase4Status !== "complete") return;
    if (phase5Status !== "idle") return;
    if (isStartingPhase5) return;
    void handleStartPhase5();
  }, [wizardState, phase4Status, phase5Status, isStartingPhase5, handleStartPhase5]);

  useEffect(() => {
    if (wizardState !== "PROCESSING") return;
    if (phase5Status !== "complete") return;
    if (phase6Status !== "idle") return;
    if (isStartingPhase6) return;
    void handleStartPhase6();
  }, [wizardState, phase5Status, phase6Status, isStartingPhase6, handleStartPhase6]);

  useEffect(() => {
    if (wizardState !== "PROCESSING") return;
    if (phase6Status !== "complete") return;
    setWizardState("COMPLETE");
  }, [wizardState, phase6Status]);

  const buildResultDeliveryPayload = useCallback(() => {
    if (!agentPaymentReceipt) return null;

    return {
      riskMode,
      investmentHorizon,
      includeCertifiedDecisionRecord: agentPaymentReceipt.certifiedDecisionRecordPurchased,
      totalPriceUsdc,
      walletAddress,
      usdcNetworkId,
      usdcNetworkLabel,
      observedUsdcBalance: usdcBalance,
      payment: {
        status: "paid" as const,
        transactionId: agentPaymentReceipt.transactionId,
        decisionId: agentPaymentReceipt.decisionId,
        amountUsdc: agentPaymentReceipt.chargedAmountUsdc,
        chargedAmountUsdc: agentPaymentReceipt.chargedAmountUsdc,
        agentNote: agentPaymentReceipt.agentNote,
        certifiedDecisionRecordPurchased: agentPaymentReceipt.certifiedDecisionRecordPurchased,
        paymentMethod: agentPaymentReceipt.paymentMethod,
        freeCodeApplied: agentPaymentReceipt.freeCodeApplied,
      },
      regimeDetected,
      phase1Artifact: phase1Output,
      phase2Artifact: phase2Output,
      phase3Artifact: phase3Output,
      phase4Artifact: phase4Output,
      phase5Artifact: phase5Output,
      phase6Artifact: phase6Output,
      aaaAllocateDispatch: phase6AaaAllocate,
      allocations: toAllocationRows(phase5Output, phase6Output, phase6AaaAllocate),
    };
  }, [
    agentPaymentReceipt,
    investmentHorizon,
    phase1Output,
    phase2Output,
    phase3Output,
    phase4Output,
    phase5Output,
    phase6AaaAllocate,
    phase6Output,
    regimeDetected,
    riskMode,
    totalPriceUsdc,
    usdcBalance,
    usdcNetworkId,
    usdcNetworkLabel,
    walletAddress,
  ]);

  const sendResultSummaryEmail = useCallback(async () => {
    const trimmedResultEmail = resultEmail.trim().toLowerCase();
    if (!trimmedResultEmail) {
      setResultEmailDeliveryStatus("idle");
      setResultEmailDeliveryMessage(null);
      return;
    }
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(trimmedResultEmail)) {
      setResultEmailDeliveryStatus("failed");
      setResultEmailDeliveryMessage("Enter a valid email address on Step 2 to receive summary delivery.");
      return;
    }

    const payload = buildResultDeliveryPayload();
    if (!payload) {
      setResultEmailDeliveryStatus("failed");
      setResultEmailDeliveryMessage("Payment confirmation is required before result email delivery.");
      return;
    }
    if (payload.payment.decisionId) {
      resultEmailAttemptKeyRef.current = `${payload.payment.decisionId}:${trimmedResultEmail}`;
    }

    setResultEmailDeliveryStatus("sending");
    setResultEmailDeliveryMessage(`Sending allocation summary to ${trimmedResultEmail}.`);

    try {
      const response = await fetch("/api/result-email", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...payload,
          resultEmail: trimmedResultEmail,
        }),
      });

      const body = (await response.json().catch(() => ({}))) as ResultEmailDeliveryResponse;
      if (!response.ok || body.status !== "sent") {
        throw new Error(body.error || `Result email delivery failed (HTTP ${response.status}).`);
      }

      setResultEmailDeliveryStatus("sent");
      setResultEmailDeliveryMessage(`Allocation summary sent to ${trimmedResultEmail}.`);
    } catch (error) {
      setResultEmailDeliveryStatus("failed");
      setResultEmailDeliveryMessage(
        error instanceof Error ? error.message : "Result email delivery failed.",
      );
    }
  }, [buildResultDeliveryPayload, resultEmail]);

  useEffect(() => {
    if (wizardState !== "COMPLETE") return;
    const trimmedResultEmail = resultEmail.trim().toLowerCase();
    const decisionId = agentPaymentReceipt?.decisionId ?? "";
    if (!trimmedResultEmail || !decisionId) return;

    const attemptKey = `${decisionId}:${trimmedResultEmail}`;
    if (resultEmailAttemptKeyRef.current === attemptKey) return;
    resultEmailAttemptKeyRef.current = attemptKey;
    void sendResultSummaryEmail();
  }, [agentPaymentReceipt?.decisionId, resultEmail, sendResultSummaryEmail, wizardState]);

  const handleGenerateAllocation = async () => {
    if (!riskMode || !investmentHorizon || isPaying) return;

    setIsPaying(true);
    setPaymentStage("preflight");
    setPaymentError(null);
    setAgentPaymentReceipt(null);

    try {
      const trimmedResultEmail = resultEmail.trim();
      if (trimmedResultEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(trimmedResultEmail)) {
        throw new Error("Enter a valid results email or leave the field blank before generating.");
      }

      if (isLoadingPricing) {
        throw new Error("Loading backend pricing. Please wait.");
      }
      if (pricingError) {
        throw new Error("Pricing unavailable. Retry after backend pricing is restored.");
      }
      if (!walletAddress) {
        throw new Error("Connect wallet to authorize Selun agent payment.");
      }
      if (isApplyingPromoCode) {
        throw new Error("Applying promo code. Please wait.");
      }
      if (requiresPromoApply) {
        throw new Error("Apply promo code first to confirm final price before purchase.");
      }
      if (requiredAmountUsdc > 0 && isLoadingUsdcBalance) {
        throw new Error("Checking USDC balance. Please wait.");
      }
      if (requiredAmountUsdc > 0 && (usdcBalanceError || usdcBalance === null)) {
        throw new Error("USDC balance unavailable. Refresh balance and try again.");
      }
      const availableUsdc = usdcBalance ?? 0;
      if (requiredAmountUsdc > 0 && availableUsdc < requiredAmountUsdc) {
        throw new Error(
          `Insufficient USDC on ${usdcNetworkLabel}. Required ${formatUsdcValue(requiredAmountUsdc)} USDC.`,
        );
      }

      const agentWallet = await withTimeout(queryAgentWallet(), 20_000, "Agent wallet lookup");

      // CHANGED: 30s -> 120s (wallet UI often needs user attention)
      await withTimeout(ensureWalletOnChain(agentWallet.networkId), 120_000, "Wallet network switch");

      setPaymentStage("agent_pay");
      resultEmailAttemptKeyRef.current = null;
      setResultEmailDeliveryStatus("idle");
      setResultEmailDeliveryMessage(null);
      const preAuthorizeResponse = await withTimeout(
        fetch("/api/agent/pay", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            walletAddress,
            includeCertifiedDecisionRecord,
            riskMode,
            investmentHorizon,
            promoCode: promoCode.trim() || undefined,
            resultEmail: trimmedResultEmail || undefined,
          }),
        }),
        30_000,
        "Agent pay request",
      );

      const paymentResult = (await preAuthorizeResponse.json()) as AgentPaymentResponse;
      if (!preAuthorizeResponse.ok || !paymentResult.success) {
        throw new Error(paymentResult.error || "Agent payment failed.");
      }

      if (
        !paymentResult.transactionId ||
        !paymentResult.decisionId ||
        !paymentResult.agentNote ||
        typeof paymentResult.certifiedDecisionRecordPurchased !== "boolean"
      ) {
        throw new Error("Incomplete agent payment response.");
      }
      const chargedAmountUsdcString = paymentResult.chargedAmountUsdc?.trim() ?? "";
      const chargedAmountUsdc = Number.parseFloat(chargedAmountUsdcString);
      if (!Number.isFinite(chargedAmountUsdc) || chargedAmountUsdc < 0) {
        throw new Error("Invalid charged amount received from backend.");
      }

      const paymentMethod = paymentResult.paymentMethod ?? "onchain";
      const freeCodeApplied = Boolean(paymentResult.freeCodeApplied);
      const isFreeCodeCheckout = paymentMethod === "free_code" || freeCodeApplied || chargedAmountUsdc === 0;

      if (isFreeCodeCheckout) {
        setPaymentStage("starting_execution");
        setAgentPaymentReceipt({
          transactionId: paymentResult.transactionId,
          decisionId: paymentResult.decisionId,
          agentNote: paymentResult.agentNote,
          chargedAmountUsdc,
          certifiedDecisionRecordPurchased: paymentResult.certifiedDecisionRecordPurchased,
          paymentMethod: "free_code",
          freeCodeApplied: true,
        });

        const phase1JobId = `selun-phase1-${paymentResult.decisionId}-${Date.now()}`.replace(/[^a-zA-Z0-9-_]/g, "-");
        await withTimeout(startPhase1Flow(phase1JobId), 20_000, "Phase 1 start");
        return;
      }

      const provider = window.ethereum;
      if (!provider?.request) throw new Error("No Ethereum wallet detected.");

      const pendingLocal = await hasPendingTransactions(provider, walletAddress);
      const pendingRpc = await hasPendingTransactionsOnRpc(agentWallet.networkId, walletAddress);
      if (pendingLocal || pendingRpc) {
        const recovered = await tryRecoverExistingPayment(
          { fromAddress: walletAddress, expectedAmountUSDC: chargedAmountUsdcString, decisionId: paymentResult.decisionId },
          30_000,
        );
        if (recovered) {
          setPaymentStage("starting_execution");
          setAgentPaymentReceipt({
            transactionId: recovered.transactionHash,
            decisionId: paymentResult.decisionId,
            agentNote: paymentResult.agentNote!,
            chargedAmountUsdc,
            certifiedDecisionRecordPurchased: Boolean(paymentResult.certifiedDecisionRecordPurchased),
            paymentMethod: "onchain",
            freeCodeApplied: false,
          });
          const phase1JobId = `selun-phase1-${paymentResult.decisionId}-${Date.now()}`.replace(/[^a-zA-Z0-9-_]/g, "-");
          await withTimeout(startPhase1Flow(phase1JobId), 20_000, "Phase 1 start");
          return;
        }
      }

      setPaymentStage("wallet_tx_prompt");
      const transferData = encodeUsdcTransferCall(agentWallet.walletAddress, parseUsdcToBaseUnits(chargedAmountUsdcString));

      const transferHashRaw = await withTimeout(
        provider.request({
          method: "eth_sendTransaction",
          params: [
            {
              from: walletAddress,
              to: agentWallet.usdcContractAddress,
              data: transferData,
              value: "0x0",
            },
          ],
        }) as Promise<unknown>,
        120_000,
        "Wallet transaction approval",
      );

      if (typeof transferHashRaw !== "string" || !/^0x[a-fA-F0-9]{64}$/.test(transferHashRaw)) {
        throw new Error("Wallet did not return a valid transfer transaction hash.");
      }

      setPaymentStage("verifying");
      const verification = await withTimeout(
        verifyPaymentOnBackend({
          fromAddress: walletAddress,
          expectedAmountUSDC: chargedAmountUsdcString,
          transactionHash: transferHashRaw,
          decisionId: paymentResult.decisionId,
        }),
        120_000,
        "Payment verification",
      );

      setAgentPaymentReceipt({
        transactionId: verification.transactionHash,
        decisionId: paymentResult.decisionId,
        agentNote: paymentResult.agentNote!,
        chargedAmountUsdc,
        certifiedDecisionRecordPurchased: Boolean(paymentResult.certifiedDecisionRecordPurchased),
        paymentMethod: "onchain",
        freeCodeApplied: false,
      });

      setPaymentStage("starting_execution");
      const phase1JobId = `selun-phase1-${paymentResult.decisionId}-${Date.now()}`.replace(/[^a-zA-Z0-9-_]/g, "-");
      await withTimeout(startPhase1Flow(phase1JobId), 20_000, "Phase 1 start");
    } catch (error) {
      setPaymentError(error instanceof Error ? error.message : "Agent payment failed.");
    } finally {
      setIsPaying(false);
      setPaymentStage("idle");
    }
  };

  const handleDownloadReport = async () => {
    if (!riskMode || !investmentHorizon || isDownloading) return;
    if (!agentPaymentReceipt) {
      setPaymentError("Payment confirmation is required before report download.");
      return;
    }
    if (!agentPaymentReceipt.certifiedDecisionRecordPurchased) {
      setPaymentError("Phase 7 is disabled for this run because the certified report was not purchased.");
      return;
    }

    const trimmedResultEmail = resultEmail.trim();
    if (trimmedResultEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(trimmedResultEmail)) {
      setPaymentError("Please enter a valid email address to send report results.");
      return;
    }

    try {
      setIsDownloading(true);
      setPaymentError(null);
      const payload = buildResultDeliveryPayload();
      if (!payload) {
        throw new Error("Payment confirmation is required before report download.");
      }

      const response = await fetch("/api/report/download", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...payload,
          decisionRecord: agentPaymentReceipt.certifiedDecisionRecordPurchased
            ? {
                decisionId: agentPaymentReceipt?.decisionId ?? `SELUN-DEC-${Date.now()}`,
                generatedAt: new Date().toISOString(),
                rationaleSummary: "Structured rationale generated from deterministic pipeline outputs.",
                format: "formal-pdf-export-mock",
              }
            : null,
          resultEmail: trimmedResultEmail || undefined,
        }),
      });

      if (!response.ok) {
        let errorMessage = `Report generation failed (HTTP ${response.status}).`;
        try {
          const errorPayload = (await response.json()) as { error?: string };
          if (errorPayload?.error) {
            errorMessage = errorPayload.error;
          }
        } catch {
          // keep default
        }
        throw new Error(errorMessage);
      }

      const blob = await response.blob();
      const objectUrl = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      const contentDisposition = response.headers.get("content-disposition") || "";
      const filenameMatch = contentDisposition.match(/filename\*?=(?:UTF-8''|")?([^\";]+)/i);
      const suggestedNameRaw = filenameMatch?.[1] || "";
      const suggestedName = decodeURIComponent(suggestedNameRaw).replace(/^["']|["']$/g, "");
      const defaultName = agentPaymentReceipt.certifiedDecisionRecordPurchased
        ? "selun-certified-decision-record.pdf"
        : "selun-structured-allocation-report.pdf";
      anchor.href = objectUrl;
      anchor.download = suggestedName || defaultName;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      URL.revokeObjectURL(objectUrl);
    } catch (error) {
      setPaymentError(error instanceof Error ? error.message : "Failed to generate report.");
    } finally {
      setIsDownloading(false);
    }
  };

  const handleRetryResultEmail = () => {
    resultEmailAttemptKeyRef.current = null;
    void sendResultSummaryEmail();
  };

  const handleStartOver = () => {
    if (isDownloading) return;
    setWizardState("CONFIGURE");
    setRiskMode(DEFAULT_RISK_MODE);
    setInvestmentHorizon(DEFAULT_HORIZON);
    setResultEmail("");
    setResultEmailDeliveryStatus("idle");
    setResultEmailDeliveryMessage(null);
    resultEmailAttemptKeyRef.current = null;
    setIncludeCertifiedDecisionRecord(false);
    setPromoCode("");
    setPromoQuote(null);
    setPromoQuoteError(null);
    setIsApplyingPromoCode(false);
    setWalletAddress(null);
    setUsdcNetworkId("backend-configured");
    setUsdcNetworkLabel("Configured in backend");
    setUsdcBalance(null);
    setIsLoadingUsdcBalance(false);
    setUsdcBalanceError(null);
    setUsdcBalanceRefreshNonce(0);
    setIsConnectingWallet(false);
    setPaymentError(null);
    setAgentPaymentReceipt(null);
    setIsPaying(false);
    setPaymentStage("idle");
    setPhase1JobId(null);
    setPhase1Status("idle");
    setPhase1Error(null);
    setPhase1Output(null);
    setPhase2Status("idle");
    setPhase2Output(null);
    setPhase2Error(null);
    setPhase3Status("idle");
    setPhase3Output(null);
    setPhase3Error(null);
    setIsStartingPhase3(false);
    setPhase4Status("idle");
    setPhase4Output(null);
    setPhase4Error(null);
    setIsStartingPhase4(false);
    setPhase5Status("idle");
    setPhase5Output(null);
    setPhase5Error(null);
    setIsStartingPhase5(false);
    setPhase6Status("idle");
    setPhase6Output(null);
    setPhase6AaaAllocate(null);
    setPhase6Error(null);
    setIsStartingPhase6(false);
    setPhase1SubPhaseStates(createPhase1SubPhaseStates());
    setPhase3SubPhaseStates(createPhase3SubPhaseStates());
    setIsDownloading(false);
  };

  return (
    <div ref={pageTopRef} tabIndex={-1} className="mx-auto w-full max-w-5xl p-5 md:p-8 focus:outline-none">
      <header className="mb-6 rounded-2xl border border-slate-300/70 bg-white/65 p-5 backdrop-blur">
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div className="flex items-start gap-4">
            <Link
              href="/"
              className="rounded-xl border border-cyan-200/70 bg-white/80 p-2 shadow-sm transition hover:border-cyan-300 hover:shadow"
              aria-label="Go to Selun home page"
            >
              <Image
                src="/selun-mark.svg"
                alt="Selun mark"
                width={80}
                height={80}
                className="h-[58px] w-[80px] md:h-[80px] md:w-[80px]"
                priority
              />
            </Link>

            <div>
              <p className="text-xs font-bold uppercase tracking-[0.24em] text-cyan-700">SELUN AGENT</p>
              <h1 className="mt-2 text-3xl font-semibold tracking-tight text-slate-900 md:text-5xl">
                Crypto Allocation Agent
              </h1>
              <p className="mt-2 text-slate-600">
              Smart allocation designed for current market conditions.
              </p>
            </div>
          </div>

          <Link
            href="/"
            className="inline-flex h-fit items-center justify-center rounded-full border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-cyan-400 hover:text-cyan-700"
          >
            Back to Home
          </Link>
        </div>
      </header>

      <div className="mb-6 grid grid-cols-2 gap-2 md:grid-cols-4">
        {WIZARD_FLOW.map((state, index) => {
          const isActive = wizardState === state;
          const isComplete = WIZARD_FLOW.indexOf(wizardState) > index;
          const stateLabel = state.charAt(0) + state.slice(1).toLowerCase();

          return (
            <div
              key={state}
              className={`flex items-center gap-2 rounded-lg border px-3 py-2 text-xs font-bold uppercase tracking-[0.12em] ${
                isActive
                  ? "border-cyan-700 bg-gradient-to-r from-cyan-900 to-cyan-700 text-cyan-50 shadow-sm"
                  : isComplete
                    ? "border-emerald-300 bg-emerald-50 text-emerald-700"
                    : "border-dashed border-slate-300 bg-slate-100/80 text-slate-500"
              }`}
            >
              <span
                className={`inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-md text-[10px] font-extrabold ${
                  isActive
                    ? "bg-cyan-200/20 text-cyan-50"
                    : isComplete
                      ? "bg-emerald-100 text-emerald-700"
                      : "bg-slate-200 text-slate-500"
                }`}
              >
                {index + 1}
              </span>
              <span className="truncate">{stateLabel}</span>
            </div>
          );
        })}
      </div>

      {wizardState === "CONFIGURE" && (
        <ConfigureStep
          riskMode={riskMode}
          investmentHorizon={investmentHorizon}
          onRiskModeSelect={setRiskMode}
          onInvestmentHorizonSelect={setInvestmentHorizon}
          onContinue={handleContinueFromConfigure}
        />
      )}

      {wizardState === "REVIEW" && riskMode && investmentHorizon && (
        <ReviewStep
          riskMode={riskMode}
          investmentHorizon={investmentHorizon}
          basePriceUsdc={basePriceUsdc}
          certifiedDecisionRecordFeeUsdc={certifiedDecisionRecordFeeUsdc}
          resultEmail={resultEmail}
          includeCertifiedDecisionRecord={includeCertifiedDecisionRecord}
          promoCode={promoCode}
          promoQuote={promoQuote}
          promoQuoteError={promoQuoteError}
          isApplyingPromoCode={isApplyingPromoCode}
          requiresPromoApply={requiresPromoApply}
          requiredAmountUsdc={requiredAmountUsdc}
          totalPriceUsdc={totalPriceUsdc}
          isLoadingPricing={isLoadingPricing}
          pricingError={pricingError}
          walletAddress={walletAddress}
          usdcNetworkLabel={usdcNetworkLabel}
          usdcBalance={usdcBalance}
          isLoadingUsdcBalance={isLoadingUsdcBalance}
          usdcBalanceError={usdcBalanceError}
          isBalanceLow={isBalanceLow}
          isConnectingWallet={isConnectingWallet}
          paymentError={paymentError}
          isPaying={isPaying}
          onResultEmailChange={setResultEmail}
          onToggleCertifiedDecisionRecord={handleToggleCertifiedDecisionRecord}
          onPromoCodeChange={handlePromoCodeChange}
          onApplyPromoCode={handleApplyPromoCode}
          onConnectWallet={handleConnectWallet}
          onRefreshUsdcBalance={handleRefreshUsdcBalance}
          onRefreshPricing={refreshPricing}
          onBack={handleBackToConfigure}
          onGenerate={handleGenerateAllocation}
        />
      )}

      {wizardState === "PROCESSING" && (
        <ProcessingStepView
          steps={processingSteps}
          phase1Status={phase1Status}
          phase1SubPhaseStates={phase1SubPhaseStates}
          phase1Error={phase1Error}
          phase2Status={phase2Status}
          phase2Output={phase2Output}
          phase2Error={phase2Error}
          phase3Status={phase3Status}
          phase3SubPhaseStates={phase3SubPhaseStates}
          phase3Output={phase3Output}
          phase3Error={phase3Error}
          phase4Status={phase4Status}
          phase4Output={phase4Output}
          phase4Error={phase4Error}
          phase5Status={phase5Status}
          phase5Output={phase5Output}
          phase5Error={phase5Error}
          phase6Status={phase6Status}
          phase6Output={phase6Output}
          phase6AaaAllocate={phase6AaaAllocate}
          phase6Error={phase6Error}
          onRetryPhase1={handleRetryPhase1}
          onRetryPhase3={handleRetryPhase3}
          onRetryPhase4={handleRetryPhase4}
          onRetryPhase5={handleRetryPhase5}
          onRetryPhase6={handleRetryPhase6}
        />
      )}

      {wizardState === "COMPLETE" && (
        <CompleteStep
          regimeDetected={regimeDetected}
          allocations={allocationRows}
          phase7Enabled={phase7Enabled}
          walletAddress={walletAddress}
          agentPaymentReceipt={agentPaymentReceipt}
          resultEmail={resultEmail}
          resultEmailDeliveryStatus={resultEmailDeliveryStatus}
          resultEmailDeliveryMessage={resultEmailDeliveryMessage}
          phase1Output={phase1Output}
          phase2Output={phase2Output}
          phase6AaaAllocate={phase6AaaAllocate}
          downloadError={paymentError}
          isDownloading={isDownloading}
          onRetryResultEmail={handleRetryResultEmail}
          onDownloadReport={handleDownloadReport}
          onStartOver={handleStartOver}
        />
      )}
    </div>
  );
}

export default function WizardPage() {
  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_12%_10%,rgba(125,211,252,0.22),transparent_38%),radial-gradient(circle_at_85%_8%,rgba(96,165,250,0.2),transparent_32%),linear-gradient(165deg,#eff6ff,#dbeafe,#e0e7ff)]">
      <SelunAllocationWizard />
    </main>
  );
}
