import fs from "node:fs";
import path from "node:path";
import { createHash, createHmac } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";
import { EXECUTION_MODEL_VERSION } from "../config";
import { emitExecutionLog } from "../logging/execution-logs";
import { initializeAgent } from "./selun-agent.service";

export const REVIEW_MARKET_CONDITIONS_PHASE = "review_market_conditions" as const;
export const PHASE_1_LABEL = "PHASE_1_MARKET_REVIEW" as const;
export const DOCTRINE_VERSION = "SELUN-SIGNAL-1.0" as const;
export const PHASE_2_LABEL = "PHASE_2_POLICY_ENVELOPE" as const;
export const PHASE2_DOCTRINE_VERSION = "SELUN-POLICY-1.0" as const;
export const EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE = "expand_eligible_asset_universe" as const;
export const PHASE_3_LABEL = "PHASE_3_ELIGIBLE_ASSET_UNIVERSE" as const;
export const PHASE3_DOCTRINE_VERSION = "SELUN-UNIVERSE-1.0" as const;
export const SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE =
  "screen_liquidity_and_structural_stability" as const;
export const PHASE_4_LABEL = "PHASE_4_LIQUIDITY_AND_STRUCTURAL_STABILITY" as const;
export const PHASE4_DOCTRINE_VERSION = "SELUN-SCREENING-1.0" as const;
export const EVALUATE_ASSET_RISK_AND_QUALITY_PHASE = "evaluate_asset_risk_and_quality" as const;
export const PHASE_5_LABEL = "PHASE_5_RISK_AND_QUALITY" as const;
export const PHASE5_DOCTRINE_VERSION = "SELUN-QUALITY-1.0" as const;
export const CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE = "construct_portfolio_allocation" as const;
export const PHASE_6_LABEL = "PHASE_6_PORTFOLIO_ALLOCATION" as const;
export const PHASE6_DOCTRINE_VERSION = "SELUN-ALLOCATION-1.0" as const;

const PHASE1_SUB_PHASES = [
  "collecting_market_volatility_data",
  "collecting_liquidity_metrics",
  "collecting_macro_sentiment_data",
  "evaluating_market_alignment",
  "finalizing_market_snapshot",
] as const;
const PHASE3_SUB_PHASES = [
  "engaging_selun_agent_for_universe",
  "collecting_top_volume_universe",
  "discovering_profile_match_candidates",
  "finalizing_universe_snapshot",
] as const;
const PHASE4_SUB_PHASES = [
  "engaging_selun_agent_for_screening",
  "computing_liquidity_signals",
  "applying_structural_stability_gates",
  "finalizing_screening_snapshot",
] as const;
const PHASE5_SUB_PHASES = [
  "engaging_selun_agent_for_quality",
  "scoring_asset_risk_quality",
  "building_quality_shortlist",
  "finalizing_quality_snapshot",
] as const;
const PHASE6_SUB_PHASES = [
  "engaging_selun_agent_for_allocation",
  "constructing_bucket_allocations",
  "applying_portfolio_constraints",
  "finalizing_allocation_snapshot",
] as const;

const SYSTEM_PROMPT_VERSION = "SELUN-PHASE1-PROMPT-1.0";
const MAX_JOB_LOGS = 300;
const readPositiveIntEnv = (name: string, fallback: number): number => {
  const raw = process.env[name]?.trim();
  const parsed = Number.parseInt(raw ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};
const readBooleanEnv = (name: string, fallback: boolean): boolean => {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
};
const readStringEnv = (name: string): string => process.env[name]?.trim() ?? "";
const DEFAULT_PHASE1_MAX_USABLE_DATA_ATTEMPTS = 12;
const DEFAULT_PHASE1_RETRY_DELAY_MS = 2000;
const DEFAULT_PHASE1_MAX_RETRY_DELAY_MS = 12000;
const DEFAULT_PHASE1_SNAPSHOT_MAX_AGE_MS = 6 * 60 * 60 * 1000;
const DEFAULT_PHASE1_VOLATILITY_SOURCE_TARGET = 2;
const DEFAULT_PHASE1_GLOBAL_METRICS_SOURCE_TARGET = 2;
const DEFAULT_PHASE1_SENTIMENT_SOURCE_TARGET = 3;
const PHASE1_SNAPSHOT_PATH = path.join(process.cwd(), "backend", "data", "phase1-market-snapshot.json");
const PHASE1_SOURCE_INTELLIGENCE_PATH = path.join(process.cwd(), "backend", "data", "source-intelligence.json");
const DEFAULT_VOLATILITY_SOURCE_ORDER = ["coingecko", "coinbase"] as const;
const DEFAULT_GLOBAL_METRICS_SOURCE_ORDER = ["coingecko", "coinpaprika"] as const;
const DEFAULT_SENTIMENT_SOURCE_ORDER = ["cryptocompare", "alternative_me"] as const;
const DEFAULT_VOLATILITY_DISCOVERY_POOL = ["binance", "kraken_ohlc"] as const;
const DEFAULT_GLOBAL_METRICS_DISCOVERY_POOL = ["coinlore", "coinmarketcap"] as const;
const DEFAULT_SENTIMENT_DISCOVERY_POOL = ["coindesk_rss"] as const;
const AAA_SELUN_ALLOCATE_PATH = "/selun/allocate";
const DEFAULT_AAA_ALLOCATE_TIMEOUT_MS = 15_000;
const SENTIMENT_PROVIDER_WEIGHT: Record<string, number> = {
  cryptocompare: 1,
  coindesk_rss: 0.9,
  alternative_me: 0.8,
};

const PHASE2_BASELINES: Record<UserRiskTolerance, Phase2PolicyBaseline> = {
  Conservative: {
    riskBudget: 0.28,
    maxSingleAssetExposure: 0.16,
    stablecoinMinimum: 0.35,
    highVolatilityAssetCap: 0.08,
    portfolioVolatilityTarget: 0.22,
    liquidityFloorRequirement: "tier_1_only",
    volatilityCeiling: 0.35,
    capitalPreservationBias: 0.82,
    mode: "capital_preservation",
  },
  Balanced: {
    riskBudget: 0.42,
    maxSingleAssetExposure: 0.25,
    stablecoinMinimum: 0.2,
    highVolatilityAssetCap: 0.15,
    portfolioVolatilityTarget: 0.38,
    liquidityFloorRequirement: "tier_1_only",
    volatilityCeiling: 0.5,
    capitalPreservationBias: 0.63,
    mode: "balanced_defensive",
  },
  Growth: {
    riskBudget: 0.56,
    maxSingleAssetExposure: 0.31,
    stablecoinMinimum: 0.12,
    highVolatilityAssetCap: 0.22,
    portfolioVolatilityTarget: 0.52,
    liquidityFloorRequirement: "tier_1_plus_tier_2",
    volatilityCeiling: 0.66,
    capitalPreservationBias: 0.46,
    mode: "balanced_growth",
  },
  Aggressive: {
    riskBudget: 0.68,
    maxSingleAssetExposure: 0.38,
    stablecoinMinimum: 0.08,
    highVolatilityAssetCap: 0.3,
    portfolioVolatilityTarget: 0.68,
    liquidityFloorRequirement: "tier_1_plus_tier_2",
    volatilityCeiling: 0.82,
    capitalPreservationBias: 0.3,
    mode: "offensive_growth",
  },
};

const PHASE2_TIMEFRAME_DELTAS: Record<UserInvestmentTimeframe, Phase2PolicyDeltas> = {
  "<1_year": {
    riskBudget: -0.08,
    maxSingleAssetExposure: -0.04,
    stablecoinMinimum: 0.08,
    highVolatilityAssetCap: -0.05,
    portfolioVolatilityTarget: -0.08,
    volatilityCeiling: -0.1,
    capitalPreservationBias: 0.1,
  },
  "1-3_years": {
    riskBudget: 0,
    maxSingleAssetExposure: 0,
    stablecoinMinimum: 0,
    highVolatilityAssetCap: 0,
    portfolioVolatilityTarget: 0,
    volatilityCeiling: 0,
    capitalPreservationBias: 0,
  },
  "3+_years": {
    riskBudget: 0.06,
    maxSingleAssetExposure: 0.03,
    stablecoinMinimum: -0.05,
    highVolatilityAssetCap: 0.05,
    portfolioVolatilityTarget: 0.08,
    volatilityCeiling: 0.08,
    capitalPreservationBias: -0.08,
  },
};

const PHASE2_AGENT_DELTA_PROFILE_CONFIG: Record<
  UserRiskTolerance,
  {
    tightenMultiplier: number;
    relaxMultiplier: number;
    neutralMultiplier: number;
    capitalPreservationBiasMin: number;
    capitalPreservationBiasMax: number;
  }
> = {
  Conservative: {
    tightenMultiplier: 0.45,
    relaxMultiplier: 0.55,
    neutralMultiplier: 0.5,
    capitalPreservationBiasMin: 0.55,
    capitalPreservationBiasMax: 1,
  },
  Balanced: {
    tightenMultiplier: 1,
    relaxMultiplier: 0.85,
    neutralMultiplier: 0.9,
    capitalPreservationBiasMin: 0.35,
    capitalPreservationBiasMax: 0.9,
  },
  Growth: {
    tightenMultiplier: 0.8,
    relaxMultiplier: 1,
    neutralMultiplier: 0.9,
    capitalPreservationBiasMin: 0.22,
    capitalPreservationBiasMax: 0.82,
  },
  Aggressive: {
    tightenMultiplier: 0.67,
    relaxMultiplier: 1.1,
    neutralMultiplier: 0.9,
    capitalPreservationBiasMin: 0.12,
    capitalPreservationBiasMax: 0.74,
  },
};
const AGGRESSIVE_SHORT_TERM_LOW_VOL_HIGH_VOL_CAP_FLOOR = 0.05;
const DEFAULT_PHASE3_TOP_VOLUME_TOKEN_TARGET = 300;
const DEFAULT_PHASE3_MAX_PROFILE_TOKEN_TARGET = 80;
const DEFAULT_PHASE3_COINGECKO_MIN_INTERVAL_MS = 1200;
const DEFAULT_PHASE3_TOP_VOLUME_SOURCE_ORDER = ["coingecko", "coinlore", "coinpaprika"] as const;
const DEFAULT_PHASE3_TOP_VOLUME_DISCOVERY_POOL = ["coinmarketcap"] as const;
const PHASE3_PROFILE_REASON_TARGETS: Record<UserRiskTolerance, number> = {
  Conservative: 14,
  Balanced: 18,
  Growth: 22,
  Aggressive: 26,
};

const PHASE3_POLICY_REASON_TARGETS: Record<AllocationPolicyMode, number> = {
  capital_preservation: 14,
  balanced_defensive: 16,
  balanced_growth: 18,
  offensive_growth: 20,
};

const PHASE3_PROFILE_TOKEN_ALIASES: Record<string, string> = {
  // Coingecko migration: Maker token ecosystem moved under Sky.
  maker: "sky",
};
const PHASE3_RUNTIME_PROFILE_TOKEN_ALIASES = new Map<string, string>();

const PHASE3_PROFILE_TOKEN_ALIASES_FROM_ENV: Record<string, string> = (() => {
  const raw = process.env.PHASE3_PROFILE_TOKEN_ALIASES_JSON?.trim();
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const entries = Object.entries(parsed as Record<string, unknown>);
    const aliases: Record<string, string> = {};
    for (const [sourceId, targetId] of entries) {
      if (typeof sourceId !== "string" || typeof targetId !== "string") continue;
      const normalizedSourceId = normalizeTokenId(sourceId);
      const normalizedTargetId = normalizeTokenId(targetId);
      if (!normalizedSourceId || !normalizedTargetId || normalizedSourceId === normalizedTargetId) continue;
      aliases[normalizedSourceId] = normalizedTargetId;
    }
    return aliases;
  } catch {
    return {};
  }
})();

const PHASE3_EMERGENCY_RETAIL_TOKENS: Array<{ coingeckoId: string; symbol: string; name: string }> = [
  { coingeckoId: "bitcoin", symbol: "BTC", name: "Bitcoin" },
  { coingeckoId: "ethereum", symbol: "ETH", name: "Ethereum" },
  { coingeckoId: "tether", symbol: "USDT", name: "Tether" },
  { coingeckoId: "usd-coin", symbol: "USDC", name: "USDC" },
  { coingeckoId: "solana", symbol: "SOL", name: "Solana" },
  { coingeckoId: "ripple", symbol: "XRP", name: "XRP" },
  { coingeckoId: "binancecoin", symbol: "BNB", name: "BNB" },
  { coingeckoId: "dogecoin", symbol: "DOGE", name: "Dogecoin" },
  { coingeckoId: "cardano", symbol: "ADA", name: "Cardano" },
  { coingeckoId: "chainlink", symbol: "LINK", name: "Chainlink" },
];

const SELUN_PHASE1_SYSTEM_PROMPT = `
SYSTEM ROLE
You are Selun, an autonomous allocation investigation agent operating under Sagitta doctrine.
You are not a chatbot. You are not a recommender.
You operate as a forensic financial analyst and investigative auditor.
Act as a sovereign market auditor. Your task is to determine whether allocation activity should occur at all, under what constraints, and with what risk posture.
Only structured evidence and scored conclusions are permitted.

EXECUTION CONTEXT
Produce a deterministic, auditable Phase 1 Market Review.
Do not speculate. Do not persuade. Do not narrate.
No human-opinion weighting.

REQUIRED TASKS
1) Market regime classification
2) Sentiment and signal synthesis
3) Allocation authorization determination
4) Output integrity checks

PROHIBITIONS
No buy or sell recommendations.
No price prediction.
No marketing language.
If data is insufficient, downgrade confidence or exclude.
`.trim();

const PHASE2_SYSTEM_PROMPT_VERSION = "SELUN-PHASE2-PROMPT-1.0";
const PHASE3_SYSTEM_PROMPT_VERSION = "SELUN-PHASE3-PROMPT-1.0";
const SELUN_PHASE2_SYSTEM_PROMPT = `
SYSTEM ROLE
You are Selun, an autonomous policy-envelope engine operating under Sagitta doctrine.
You are not a chatbot and you are not a recommender.
You must transform Phase 1 macro conditions plus user profile into deterministic allocation constraints.

OBJECTIVE
Translate:
Market State + User Risk Profile -> Policy Constraints for AAA.

MANDATORY OUTPUT FIELDS
allocation_policy,
risk_scaling_factor,
exposure_caps,
defensive_bias_adjustment,
liquidity_floor_requirement,
volatility_ceiling,
capital_preservation_bias,
allocation_authorization.status

PROHIBITIONS
No narrative.
No speculation.
No price prediction.
No recommendations.
Only deterministic rule-based transformations.
`.trim();
const PHASE2_AGENT_REASONING_PROVIDER = "coinbase-agentkit";

const POSITIVE_NEWS_TOKENS = [
  "surge",
  "rally",
  "gain",
  "adoption",
  "approval",
  "upgrade",
  "growth",
  "partnership",
  "launch",
  "integration",
];

const NEGATIVE_NEWS_TOKENS = [
  "hack",
  "lawsuit",
  "drop",
  "fall",
  "bear",
  "liquidation",
  "fraud",
  "ban",
  "exploit",
  "decline",
  "slump",
  "crash",
  "sanction",
  "investigation",
  "depeg",
];

type JobLogStatus = "in_progress" | "complete" | "failed";
type JobPhaseStatus = "idle" | "in_progress" | "complete" | "failed";
type Phase2Status = "idle" | "in_progress" | "complete" | "failed";
type Phase3Status = "idle" | "in_progress" | "complete" | "failed";
type Phase4Status = "idle" | "in_progress" | "complete" | "failed";
type Phase5Status = "idle" | "in_progress" | "complete" | "failed";
type Phase6Status = "idle" | "in_progress" | "complete" | "failed";
type SelectionDomain = "volatility" | "liquidity" | "sentiment" | "market_metrics";
type VolatilityState = "low" | "moderate" | "elevated" | "extreme";
type LiquidityState = "weak" | "stable" | "strong";
type RiskAppetite = "defensive" | "neutral" | "expansionary";
type RiskMode = "conservative" | "balanced" | "growth" | "aggressive" | "neutral";
type CorrelationState = "compression" | "expansion" | "stable";
type AllocationAuthorizationStatus = "AUTHORIZED" | "DEFERRED" | "PROHIBITED";
type UserRiskTolerance = "Conservative" | "Balanced" | "Growth" | "Aggressive";
type UserInvestmentTimeframe = "<1_year" | "1-3_years" | "3+_years";
type LiquidityFloorRequirement = "tier_1_only" | "tier_1_plus_tier_2" | "broad_liquidity_ok";
type AllocationPolicyMode = "capital_preservation" | "balanced_defensive" | "balanced_growth" | "offensive_growth";
type Phase2AuthorizationStatus = "AUTHORIZED" | "RESTRICTED" | "PROHIBITED";

export type Phase1RunInput = {
  jobId: string;
  executionTimestamp?: string;
  riskMode?: string;
  riskTolerance?: string;
  investmentTimeframe?: string;
  timeWindow?: string;
  walletAddress?: string;
};

type NormalizedPhase1Input = {
  jobId: string;
  executionTimestamp: string;
  riskMode: RiskMode;
  userProfile: {
    riskTolerance: UserRiskTolerance;
    investmentTimeframe: UserInvestmentTimeframe;
  };
  timeWindow: string;
  walletAddress?: string;
};

type JobProgressLog = {
  phase: string;
  subPhase?: string;
  status: JobLogStatus;
  timestamp: string;
  startedAt?: string;
  completedAt?: string;
  error?: string;
};

type SourceSelectionRecord = {
  domain: SelectionDomain;
  selected: string[];
  rejected: Array<{
    id: string;
    reason: string;
  }>;
  rationale: string[];
};

type SourceCredibilityRecord = {
  domain: SelectionDomain;
  provider: string;
  score: number;
  successes: number;
  failures: number;
  last_success_at: string | null;
  last_failure_at: string | null;
  avg_latency_ms: number;
};

type Phase2PolicyBaseline = {
  riskBudget: number;
  maxSingleAssetExposure: number;
  stablecoinMinimum: number;
  highVolatilityAssetCap: number;
  portfolioVolatilityTarget: number;
  liquidityFloorRequirement: LiquidityFloorRequirement;
  volatilityCeiling: number;
  capitalPreservationBias: number;
  mode: AllocationPolicyMode;
};

type Phase2PolicyDeltas = {
  riskBudget: number;
  maxSingleAssetExposure: number;
  stablecoinMinimum: number;
  highVolatilityAssetCap: number;
  portfolioVolatilityTarget: number;
  volatilityCeiling: number;
  capitalPreservationBias: number;
};

type Phase2AgentPosture = "more_defensive" | "neutral" | "selective_risk_on";
type Phase2AgentAuthorizationHint = "NO_CHANGE" | "TIGHTEN" | "RELAX";

type Phase2AgentJudgement = {
  posture: Phase2AgentPosture;
  authorization_hint: Phase2AgentAuthorizationHint;
  reason_codes: string[];
  envelope_adjustments: {
    risk_budget_delta: number;
    stablecoin_minimum_delta: number;
    high_volatility_asset_cap_delta: number;
    max_single_asset_exposure_delta: number;
    portfolio_volatility_target_delta: number;
    volatility_ceiling_delta: number;
    capital_preservation_bias_delta: number;
  };
};

type Phase2AgentInvocationResult = {
  used: true;
  model: string;
  judgement: Phase2AgentJudgement;
};

const phase1OutputSchema = z
  .object({
    timestamp: z.string(),
    execution_model_version: z.literal(EXECUTION_MODEL_VERSION as "Selun-1.0.0"),
    doctrine_version: z.literal(DOCTRINE_VERSION),
    market_condition: z
      .object({
        volatility_state: z.enum(["low", "moderate", "elevated", "extreme"]),
        liquidity_state: z.enum(["weak", "stable", "strong"]),
        risk_appetite: z.enum(["defensive", "neutral", "expansionary"]),
        sentiment_direction: z.number().min(-1).max(1),
        sentiment_alignment: z.number().min(0).max(1),
        public_echo_strength: z.number().min(0).max(1),
        confidence: z.number().min(0).max(1),
        uncertainty: z.number().min(0).max(1),
      })
      .strict(),
    evidence: z
      .object({
        volatility_metrics: z
          .object({
            btc_volatility_24h: z.number(),
            eth_volatility_24h: z.number(),
            volatility_zscore: z.number(),
          })
          .strict(),
        liquidity_metrics: z
          .object({
            total_volume_24h: z.number(),
            volume_deviation_zscore: z.number(),
            avg_spread: z.number(),
            stablecoin_dominance: z.number(),
          })
          .strict(),
        sentiment_metrics: z
          .object({
            headline_count: z.number(),
            aggregate_sentiment_score: z.number(),
            engagement_deviation: z.number(),
            fear_greed_index: z.number().min(-1).max(100),
            fear_greed_available: z.boolean(),
          })
          .strict(),
      })
      .strict(),
    allocation_authorization: z
      .object({
        status: z.enum(["AUTHORIZED", "DEFERRED", "PROHIBITED"]),
        confidence: z.number().min(0).max(1),
        justification: z.array(z.string()),
      })
      .strict(),
    phase_boundaries: z
      .object({
        asset_evaluation: z.literal("PHASE_3"),
        portfolio_construction: z.literal("PHASE_4"),
      })
      .strict(),
    audit: z
      .object({
        sources: z.array(
          z
            .object({
              id: z.string(),
              provider: z.string(),
              endpoint: z.string(),
              url: z.string(),
              fetched_at: z.string(),
            })
            .strict(),
        ),
        data_freshness: z.string(),
        missing_domains: z.array(z.string()),
        assumptions: z.array(z.string()),
        source_credibility: z.array(
          z
            .object({
              domain: z.enum(["volatility", "liquidity", "sentiment", "market_metrics"]),
              provider: z.string(),
              score: z.number().min(0).max(1),
              successes: z.number().int().nonnegative(),
              failures: z.number().int().nonnegative(),
              last_success_at: z.string().nullable(),
              last_failure_at: z.string().nullable(),
              avg_latency_ms: z.number().nonnegative(),
            })
            .strict(),
        ),
        source_selection: z.array(
          z
            .object({
              domain: z.enum(["volatility", "liquidity", "sentiment", "market_metrics"]),
              selected: z.array(z.string()),
              rejected: z.array(
                z
                  .object({
                    id: z.string(),
                    reason: z.string(),
                  })
                  .strict(),
              ),
              rationale: z.array(z.string()),
            })
            .strict(),
        ),
      })
      .strict(),
  })
  .strict();

export type Phase1Output = z.infer<typeof phase1OutputSchema>;

const phase2AgentJudgementSchema = z
  .object({
    posture: z.enum(["more_defensive", "neutral", "selective_risk_on"]),
    authorization_hint: z.enum(["NO_CHANGE", "TIGHTEN", "RELAX"]),
    reason_codes: z.array(z.string().min(1)).min(1).max(8),
    envelope_adjustments: z
      .object({
        risk_budget_delta: z.number().min(-0.05).max(0.05),
        stablecoin_minimum_delta: z.number().min(-0.05).max(0.05),
        high_volatility_asset_cap_delta: z.number().min(-0.05).max(0.05),
        max_single_asset_exposure_delta: z.number().min(-0.05).max(0.05),
        portfolio_volatility_target_delta: z.number().min(-0.05).max(0.05),
        volatility_ceiling_delta: z.number().min(-0.05).max(0.05),
        capital_preservation_bias_delta: z.number().min(-0.08).max(0.08),
      })
      .strict(),
  })
  .strict();

const phase2OutputSchema = z
  .object({
    timestamp: z.string(),
    execution_model_version: z.literal(EXECUTION_MODEL_VERSION as "Selun-1.0.0"),
    doctrine_version: z.literal(PHASE2_DOCTRINE_VERSION),
    inputs: z
      .object({
        market_condition_ref: z.string(),
        user_profile: z
          .object({
            risk_tolerance: z.enum(["Conservative", "Balanced", "Growth", "Aggressive"]),
            investment_timeframe: z.enum(["<1_year", "1-3_years", "3+_years"]),
          })
          .strict(),
      })
      .strict(),
    allocation_policy: z
      .object({
        mode: z.enum(["capital_preservation", "balanced_defensive", "balanced_growth", "offensive_growth"]),
        defensive_bias_adjustment: z.number().min(-1).max(1),
      })
      .strict(),
    policy_envelope: z
      .object({
        risk_budget: z.number().min(0).max(1),
        risk_scaling_factor: z.number().min(0).max(2),
        exposure_caps: z
          .object({
            max_single_asset_exposure: z.number().min(0).max(1),
            high_volatility_asset_cap: z.number().min(0).max(1),
          })
          .strict(),
        stablecoin_minimum: z.number().min(0).max(1),
        portfolio_volatility_target: z.number().min(0).max(1),
        liquidity_floor_requirement: z.enum(["tier_1_only", "tier_1_plus_tier_2", "broad_liquidity_ok"]),
        volatility_ceiling: z.number().min(0).max(1),
        capital_preservation_bias: z.number().min(0).max(1),
        defensive_adjustment_applied: z.boolean(),
      })
      .strict(),
    allocation_authorization: z
      .object({
        status: z.enum(["AUTHORIZED", "RESTRICTED", "PROHIBITED"]),
        reason: z.string(),
        confidence: z.number().min(0).max(1),
      })
      .strict(),
    phase_boundaries: z
      .object({
        asset_universe_expansion: z.literal("PHASE_3"),
        portfolio_construction: z.literal("PHASE_4"),
      })
      .strict(),
    audit: z
      .object({
        phase1_timestamp_ref: z.string(),
        policy_rules_applied: z.array(z.string()),
        agent_delta_applied: z
          .object({
            risk_budget_delta: z.number(),
            max_single_asset_exposure_delta: z.number(),
            stablecoin_minimum_delta: z.number(),
            high_volatility_asset_cap_delta: z.number(),
            portfolio_volatility_target_delta: z.number(),
            volatility_ceiling_delta: z.number(),
            capital_preservation_bias_delta: z.number(),
          })
          .strict(),
        agent_judgement: z
          .object({
            used: z.boolean(),
            model: z.string().nullable(),
            posture: z.enum(["more_defensive", "neutral", "selective_risk_on"]).nullable(),
            authorization_hint: z.enum(["NO_CHANGE", "TIGHTEN", "RELAX"]).nullable(),
            reason_codes: z.array(z.string()),
            skipped_reason: z.string().nullable(),
          })
          .strict(),
      })
      .strict(),
  })
  .strict();

export type Phase2Output = z.infer<typeof phase2OutputSchema>;

const phase3OutputSchema = z
  .object({
    timestamp: z.string(),
    execution_model_version: z.literal(EXECUTION_MODEL_VERSION as "Selun-1.0.0"),
    doctrine_version: z.literal(PHASE3_DOCTRINE_VERSION),
    inputs: z
      .object({
        phase2_policy_ref: z.string(),
        user_profile: z
          .object({
            risk_tolerance: z.enum(["Conservative", "Balanced", "Growth", "Aggressive"]),
            investment_timeframe: z.enum(["<1_year", "1-3_years", "3+_years"]),
          })
          .strict(),
        top_volume_target: z.number().int().min(50).max(500),
        volume_window_days: z.tuple([z.literal(7), z.literal(30)]),
      })
      .strict(),
    universe: z
      .object({
        top_volume_candidates_count: z.number().int().nonnegative(),
        profile_match_candidates_count: z.number().int().nonnegative(),
        total_candidates_count: z.number().int().nonnegative(),
        tokens: z.array(
          z
            .object({
              coingecko_id: z.string(),
              symbol: z.string(),
              name: z.string(),
              market_cap_rank: z.number().int().positive().nullable(),
              volume_24h_usd: z.number().nonnegative(),
              volume_7d_estimated_usd: z.number().nonnegative(),
              volume_30d_estimated_usd: z.number().nonnegative(),
              price_change_pct_7d: z.number().nullable().optional(),
              price_change_pct_30d: z.number().nullable().optional(),
              source_tags: z.array(z.string()),
              profile_match_reasons: z.array(z.string()),
              status: z.enum(["RESOLVED", "UNRESOLVED"]),
              exclude_from_phase4: z.boolean(),
              phase4_exclusion_reasons: z.array(z.string()),
              phase4_screening_hints: z
                .object({
                  token_category: z.enum(["core", "stablecoin", "meme", "proxy_or_wrapped", "alt", "unknown"]),
                  rank_bucket: z.enum(["top_100", "top_500", "long_tail", "unknown"]),
                  strict_rank_gate_required: z.boolean(),
                  exchange_depth_proxy: z.enum(["high", "medium", "low", "unknown"]),
                  stablecoin_validation_state: z.enum([
                    "not_stablecoin",
                    "trusted_stablecoin",
                    "unverified_stablecoin",
                  ]),
                  suspicious_volume_rank_mismatch: z.boolean(),
                  meme_token_detected: z.boolean(),
                  proxy_or_wrapped_detected: z.boolean(),
                })
                .strict(),
            })
            .strict(),
        ),
      })
      .strict(),
    phase_boundaries: z
      .object({
        asset_screening: z.literal("PHASE_4"),
        portfolio_construction: z.literal("PHASE_4"),
      })
      .strict(),
    audit: z
      .object({
        sources: z.array(
          z
            .object({
              id: z.string(),
              provider: z.string(),
              endpoint: z.string(),
              url: z.string(),
              fetched_at: z.string(),
            })
            .strict(),
        ),
        selection_rules: z.array(z.string()),
        missing_domains: z.array(z.string()),
        agent_profile_match: z
          .object({
            used: z.boolean(),
            model: z.string().nullable(),
            reason_codes: z.array(z.string()),
            skipped_reason: z.string().nullable(),
          })
          .strict(),
      })
      .strict(),
  })
  .strict();

export type Phase3Output = z.infer<typeof phase3OutputSchema>;

const phase4OutputSchema = z
  .object({
    timestamp: z.string(),
    execution_model_version: z.literal(EXECUTION_MODEL_VERSION as "Selun-1.0.0"),
    doctrine_version: z.literal(PHASE4_DOCTRINE_VERSION),
    inputs: z
      .object({
        phase3_universe_ref: z.string(),
        phase2_policy_ref: z.string(),
        user_profile: z
          .object({
            risk_tolerance: z.enum(["Conservative", "Balanced", "Growth", "Aggressive"]),
            investment_timeframe: z.enum(["<1_year", "1-3_years", "3+_years"]),
          })
          .strict(),
        screening_thresholds: z
          .object({
            min_liquidity_score: z.number().min(0).max(1),
            min_structural_score: z.number().min(0).max(1),
            min_screening_score: z.number().min(0).max(1),
            min_volume_24h_usd: z.number().nonnegative(),
            target_eligible_count: z.number().int().min(1).max(300),
            allow_low_depth: z.boolean(),
            rank_sanity_threshold: z.number().int().min(100).max(5000),
          })
          .strict(),
      })
      .strict(),
    screening: z
      .object({
        total_candidates_count: z.number().int().nonnegative(),
        excluded_by_phase3_count: z.number().int().nonnegative(),
        evaluated_candidates_count: z.number().int().nonnegative(),
        eligible_candidates_count: z.number().int().nonnegative(),
        tokens: z.array(
          z
            .object({
              coingecko_id: z.string(),
              symbol: z.string(),
              name: z.string(),
              market_cap_rank: z.number().int().positive().nullable(),
              volume_24h_usd: z.number().nonnegative(),
              volume_7d_estimated_usd: z.number().nonnegative(),
              volume_30d_estimated_usd: z.number().nonnegative(),
              price_change_pct_7d: z.number().nullable().optional(),
              price_change_pct_30d: z.number().nullable().optional(),
              source_tags: z.array(z.string()),
              profile_match_reasons: z.array(z.string()),
              token_category: z.enum(["core", "stablecoin", "meme", "proxy_or_wrapped", "alt", "unknown"]),
              rank_bucket: z.enum(["top_100", "top_500", "long_tail", "unknown"]),
              exchange_depth_proxy: z.enum(["high", "medium", "low", "unknown"]),
              stablecoin_validation_state: z.enum(["not_stablecoin", "trusted_stablecoin", "unverified_stablecoin"]),
              liquidity_score: z.number().min(0).max(1),
              structural_score: z.number().min(0).max(1),
              screening_score: z.number().min(0).max(1),
              eligible: z.boolean(),
              exclusion_reasons: z.array(z.string()),
            })
            .strict(),
        ),
      })
      .strict(),
    phase_boundaries: z
      .object({
        risk_quality_evaluation: z.literal("PHASE_5"),
        portfolio_construction: z.literal("PHASE_6"),
      })
      .strict(),
    audit: z
      .object({
        sources: z.array(
          z
            .object({
              id: z.string(),
              provider: z.string(),
              endpoint: z.string(),
              url: z.string(),
              fetched_at: z.string(),
            })
            .strict(),
        ),
        selection_rules: z.array(z.string()),
        missing_domains: z.array(z.string()),
        agent_screening: z
          .object({
            used: z.boolean(),
            model: z.string().nullable(),
            reason_codes: z.array(z.string()),
            skipped_reason: z.string().nullable(),
          })
          .strict(),
      })
      .strict(),
  })
  .strict();

export type Phase4Output = z.infer<typeof phase4OutputSchema>;

const PHASE5_RISK_CLASSES = [
  "stablecoin",
  "large_cap_crypto",
  "defi_bluechip",
  "large_cap_equity_core",
  "defensive_equity",
  "growth_high_beta_equity",
  "high_risk",
  "equity_fund",
  "fixed_income",
  "commodities",
  "real_estate",
  "cash_equivalent",
  "speculative",
  "traditional_asset",
  "alternative",
  "balanced_fund",
  "emerging_market",
  "frontier_market",
  "esoteric",
  "unclassified",
  "wealth_management",
  "fund_of_funds",
  "index_fund",
] as const;

const PHASE5_ROLES = ["core", "satellite", "defensive", "liquidity", "carry", "speculative"] as const;

const phase5OutputSchema = z
  .object({
    timestamp: z.string(),
    execution_model_version: z.literal(EXECUTION_MODEL_VERSION as "Selun-1.0.0"),
    doctrine_version: z.literal(PHASE5_DOCTRINE_VERSION),
    inputs: z
      .object({
        phase4_screening_ref: z.string(),
        phase2_policy_ref: z.string(),
        user_profile: z
          .object({
            risk_tolerance: z.enum(["Conservative", "Balanced", "Growth", "Aggressive"]),
            investment_timeframe: z.enum(["<1_year", "1-3_years", "3+_years"]),
          })
          .strict(),
        portfolio_constraints: z
          .object({
            risk_budget: z.number().min(0).max(1),
            stablecoin_minimum: z.number().min(0).max(1),
            max_single_asset_exposure: z.number().min(0).max(1),
            high_volatility_asset_cap: z.number().min(0).max(1),
          })
          .strict(),
      })
      .strict(),
    evaluation: z
      .object({
        screened_candidates_count: z.number().int().nonnegative(),
        qualified_candidates_count: z.number().int().nonnegative(),
        selected_candidates_count: z.number().int().nonnegative(),
        tokens: z.array(
          z
            .object({
              coingecko_id: z.string(),
              symbol: z.string(),
              name: z.string(),
              market_cap_rank: z.number().int().positive().nullable(),
              token_category: z.enum(["core", "stablecoin", "meme", "proxy_or_wrapped", "alt", "unknown"]),
              rank_bucket: z.enum(["top_100", "top_500", "long_tail", "unknown"]),
              exchange_depth_proxy: z.enum(["high", "medium", "low", "unknown"]),
              stablecoin_validation_state: z.enum(["not_stablecoin", "trusted_stablecoin", "unverified_stablecoin"]),
              profile_match_reasons: z.array(z.string()),
              liquidity_score: z.number().min(0).max(1),
              structural_score: z.number().min(0).max(1),
              quality_score: z.number().min(0).max(1),
              risk_score: z.number().min(0).max(1),
              risk_class: z.enum(PHASE5_RISK_CLASSES),
              role: z.enum(PHASE5_ROLES),
              profitability: z.number().min(0).max(1),
              volatility: z.number().min(0).max(1),
              volatility_proxy_score: z.number().min(0).max(1),
              drawdown_proxy_score: z.number().min(0).max(1),
              stablecoin_risk_modifier: z.number().min(-1).max(1),
              composite_score: z.number().min(0).max(1),
              selection_bucket: z.enum(["stablecoin", "core", "satellite", "high_volatility"]),
              selected: z.boolean(),
              selection_reasons: z.array(z.string()),
            })
            .strict(),
        ),
      })
      .strict(),
    phase_boundaries: z
      .object({
        portfolio_construction: z.literal("PHASE_6"),
        decision_report: z.literal("POST_PHASE_6"),
      })
      .strict(),
    audit: z
      .object({
        sources: z.array(
          z
            .object({
              id: z.string(),
              provider: z.string(),
              endpoint: z.string(),
              url: z.string(),
              fetched_at: z.string(),
            })
            .strict(),
        ),
        selection_rules: z.array(z.string()),
        missing_domains: z.array(z.string()),
        agent_quality_review: z
          .object({
            used: z.boolean(),
            model: z.string().nullable(),
            reason_codes: z.array(z.string()),
            skipped_reason: z.string().nullable(),
          })
          .strict(),
      })
      .strict(),
  })
  .strict();

export type Phase5Output = z.infer<typeof phase5OutputSchema>;

const phase6OutputSchema = z
  .object({
    timestamp: z.string(),
    execution_model_version: z.literal(EXECUTION_MODEL_VERSION as "Selun-1.0.0"),
    doctrine_version: z.literal(PHASE6_DOCTRINE_VERSION),
    inputs: z
      .object({
        phase5_quality_ref: z.string(),
        phase2_policy_ref: z.string(),
        user_profile: z
          .object({
            risk_tolerance: z.enum(["Conservative", "Balanced", "Growth", "Aggressive"]),
            investment_timeframe: z.enum(["<1_year", "1-3_years", "3+_years"]),
          })
          .strict(),
        portfolio_constraints: z
          .object({
            risk_budget: z.number().min(0).max(1),
            stablecoin_minimum: z.number().min(0).max(1),
            max_single_asset_exposure: z.number().min(0).max(1),
            high_volatility_asset_cap: z.number().min(0).max(1),
          })
          .strict(),
      })
      .strict(),
    allocation: z
      .object({
        shortlisted_candidates_count: z.number().int().nonnegative(),
        selected_candidates_count: z.number().int().nonnegative(),
        allocations: z.array(
          z
            .object({
              coingecko_id: z.string(),
              symbol: z.string(),
              name: z.string(),
              bucket: z.enum(["stablecoin", "core", "satellite", "high_volatility"]),
              allocation_weight: z.number().min(0).max(1),
            })
            .strict(),
        ),
        total_allocation_weight: z.number().min(0).max(1),
        stablecoin_allocation: z.number().min(0).max(1),
        expected_portfolio_volatility: z.number().min(0).max(1),
        concentration_index: z.number().min(0).max(1),
      })
      .strict(),
    phase_boundaries: z
      .object({
        decision_report: z.literal("POST_PHASE_6"),
      })
      .strict(),
    audit: z
      .object({
        sources: z.array(
          z
            .object({
              id: z.string(),
              provider: z.string(),
              endpoint: z.string(),
              url: z.string(),
              fetched_at: z.string(),
            })
            .strict(),
        ),
        selection_rules: z.array(z.string()),
        missing_domains: z.array(z.string()),
        agent_allocation_review: z
          .object({
            used: z.boolean(),
            model: z.string().nullable(),
            reason_codes: z.array(z.string()),
            skipped_reason: z.string().nullable(),
          })
          .strict(),
      })
      .strict(),
  })
  .strict();

export type Phase6Output = z.infer<typeof phase6OutputSchema>;

type Phase3UniverseToken = {
  coingeckoId: string;
  symbol: string;
  name: string;
  marketCapRank: number | null;
  volume24hUsd: number;
  volume7dEstimatedUsd: number;
  volume30dEstimatedUsd: number;
  priceChangePct7d: number | null;
  priceChangePct30d: number | null;
  sourceTags: Set<string>;
  profileMatchReasons: Set<string>;
};

type AaaAllocateDispatchStatus = "idle" | "in_progress" | "complete" | "failed";

type AaaAllocateDispatch = {
  status: AaaAllocateDispatchStatus;
  requestedAt?: string;
  completedAt?: string;
  endpoint?: string;
  httpStatus?: number;
  response?: unknown;
  error?: string;
};

type JobContext = {
  jobId: string;
  createdAt: string;
  updatedAt: string;
  phase1: {
    status: JobPhaseStatus;
    attempts: number;
    startedAt?: string;
    completedAt?: string;
    error?: string;
    input?: NormalizedPhase1Input;
    output?: Phase1Output;
    promptVersion: string;
    promptHash: string;
    agentIdentity?: {
      agentId: string;
      walletAddress: string;
      network: string;
    };
  };
  phase2: {
    status: Phase2Status;
    triggeredAt?: string;
    completedAt?: string;
    inputRef?: string;
    output?: Phase2Output;
    error?: string;
  };
  phase3: {
    status: Phase3Status;
    triggeredAt?: string;
    completedAt?: string;
    inputRef?: string;
    output?: Phase3Output;
    error?: string;
  };
  phase4: {
    status: Phase4Status;
    triggeredAt?: string;
    completedAt?: string;
    inputRef?: string;
    output?: Phase4Output;
    error?: string;
  };
  phase5: {
    status: Phase5Status;
    triggeredAt?: string;
    completedAt?: string;
    inputRef?: string;
    output?: Phase5Output;
    error?: string;
  };
  phase6: {
    status: Phase6Status;
    triggeredAt?: string;
    completedAt?: string;
    inputRef?: string;
    output?: Phase6Output;
    error?: string;
    aaaAllocate: AaaAllocateDispatch;
  };
  logs: JobProgressLog[];
};

type SharedMarketData = {
  btcPrices: number[];
  ethPrices: number[];
  btcVolumes: number[];
  ethVolumes: number[];
  totalVolume24hUsd: number;
  marketCapChangePct24h: number;
  stablecoinDominancePct: number;
  marketBreadthPositiveRatio: number;
  marketBreadthAbsMove24h: number;
  marketBreadthAssetCount: number;
  breadthMissing: boolean;
  missing: boolean;
};

type VolatilityDomain = {
  btcVolatility24h: number;
  ethVolatility24h: number;
  volatilityZScore: number;
  volatilityState: VolatilityState;
  missing: boolean;
};

type LiquidityDomain = {
  totalVolume24h: number;
  volumeDeviationZScore: number;
  avgSpread: number;
  stablecoinDominance: number;
  liquidityState: LiquidityState;
  missing: boolean;
};

type NewsItem = {
  title: string;
  body: string;
  source: string;
  publishedOn: number;
  upvotes: number;
  downvotes: number;
};

type SentimentDomain = {
  headlineCount: number;
  aggregateSentimentScore: number;
  engagementDeviation: number;
  sentimentDirection: number;
  sentimentAlignment: number;
  publicEchoStrength: number;
  sourceCount: number;
  sourceConsensus: number;
  fearGreedIncluded: boolean;
  fearGreedIndex: number | null;
  newsItems: NewsItem[];
  missing: boolean;
};

type MarketSeries = {
  prices: number[];
  volumes: number[];
};

type GlobalMetrics = {
  totalVolume24hUsd: number;
  marketCapChangePct24h: number;
  stablecoinDominancePct: number;
  usable: boolean;
};

type MarketBreadthMetrics = {
  positiveRatio: number;
  averageAbsMove24h: number;
  assetCount: number;
  usable: boolean;
};

type MarketSeriesCandidate = {
  provider: string;
  sourceId: string;
  series: MarketSeries;
};

type GlobalMetricsCandidate = {
  provider: string;
  sourceId: string;
  metrics: GlobalMetrics;
};

type MarketBreadthCandidate = {
  provider: string;
  sourceId: string;
  metrics: MarketBreadthMetrics;
};

type SentimentSignalCandidate = {
  provider: string;
  sourceId: string;
  signal: Omit<SentimentDomain, "missing">;
};

type AlignmentDomain = {
  riskAppetite: RiskAppetite;
  confidence: number;
  uncertainty: number;
};

type CorrelationDomain = {
  state: CorrelationState;
  correlation7d: number;
  correlation30d: number;
};

type MarketRegimeResult = {
  classification: string;
  confidence: number;
  signals: string[];
  systemicRisks: string[];
};

type AllocationAuthorizationResult = {
  status: AllocationAuthorizationStatus;
  confidence: number;
  justification: string[];
};

type SourceReference = {
  id: string;
  provider: string;
  endpoint: string;
  url: string;
  fetched_at: string;
};

type LastKnownGoodMacroSnapshot = {
  capturedAt: string;
  volatility: VolatilityDomain;
  liquidity: LiquidityDomain;
  sentiment: SentimentDomain;
  alignment: AlignmentDomain;
  sources: SourceReference[];
  sourceSelection: SourceSelectionRecord[];
};

const jobContextById = new Map<string, JobContext>();
const runningJobs = new Set<string>();
const runningPhase3Jobs = new Set<string>();
const runningPhase4Jobs = new Set<string>();
const runningPhase5Jobs = new Set<string>();
const runningPhase6Jobs = new Set<string>();
const latestJobIdByWallet = new Map<string, string>();
let lastKnownGoodMacroSnapshot: LastKnownGoodMacroSnapshot | null = null;
let macroSnapshotLoadedFromDisk = false;
let sourceIntelligenceLoadedFromDisk = false;
const sourceIntelligenceByKey = new Map<string, SourceCredibilityRecord>();

function nowIso() {
  return new Date().toISOString();
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function round(value: number, decimals = 6) {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function average(values: number[]) {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function standardDeviation(values: number[]) {
  if (values.length <= 1) return 0;
  const mean = average(values);
  const variance = average(values.map((value) => (value - mean) ** 2));
  return Math.sqrt(variance);
}

function zScore(value: number, values: number[]) {
  const sd = standardDeviation(values);
  if (sd === 0) return 0;
  return (value - average(values)) / sd;
}

function parsePositiveNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseNullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" && value.trim().length === 0) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeWalletKey(walletAddress?: string): string | null {
  const normalized = walletAddress?.trim().toLowerCase();
  if (!normalized) return null;
  if (!/^0x[a-f0-9]{40}$/.test(normalized)) return null;
  return normalized;
}

function parseSeries(data: unknown, key: "prices" | "total_volumes"): number[] {
  if (!data || typeof data !== "object") return [];
  const candidate = (data as Record<string, unknown>)[key];
  if (!Array.isArray(candidate)) return [];

  return candidate
    .map((row) => {
      if (!Array.isArray(row) || row.length < 2) return Number.NaN;
      return Number(row[1]);
    })
    .filter((value) => Number.isFinite(value));
}

function parseCoinbaseCandleSeries(data: unknown): MarketSeries {
  if (!Array.isArray(data)) {
    return { prices: [], volumes: [] };
  }

  const rows = data
    .map((row) => {
      if (!Array.isArray(row) || row.length < 6) return null;
      const timestamp = Number(row[0]);
      const close = Number(row[4]);
      const volume = Number(row[5]);
      if (!Number.isFinite(timestamp) || !Number.isFinite(close) || !Number.isFinite(volume)) return null;
      return { timestamp, close, volume };
    })
    .filter((row): row is { timestamp: number; close: number; volume: number } => row !== null)
    .sort((left, right) => left.timestamp - right.timestamp);

  return {
    prices: rows.map((row) => row.close),
    volumes: rows.map((row) => row.volume),
  };
}

async function fetchCoinbaseMarketSeries(
  productId: "BTC-USD" | "ETH-USD",
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  limitations: Set<string>,
): Promise<MarketSeries> {
  const end = new Date();
  const start = new Date(end.getTime() - 8 * 24 * 60 * 60 * 1000);
  const toolId = `coinbase_exchange:${productId.toLowerCase().replace("-", "_")}_candles_8d_hourly`;
  const url = `https://api.exchange.coinbase.com/products/${productId}/candles?granularity=3600&start=${encodeURIComponent(start.toISOString())}&end=${encodeURIComponent(end.toISOString())}`;

  try {
    const payload = await fetchJson<unknown>(url, toolCalls, toolId, sourceReferences);
    return parseCoinbaseCandleSeries(payload);
  } catch {
    limitations.add(`missing_${productId.toLowerCase().replace("-", "_")}_coinbase_candles`);
    return { prices: [], volumes: [] };
  }
}

function parseCoinGeckoGlobalMetrics(data: unknown): GlobalMetrics {
  const global = (data as { data?: Record<string, unknown> } | null)?.data ?? {};
  const totalVolumeObject = global.total_volume as Record<string, unknown> | undefined;
  const totalVolume24hUsd = parsePositiveNumber(totalVolumeObject?.usd);
  const marketCapChangePct24h = parsePositiveNumber(global.market_cap_change_percentage_24h_usd);
  const marketCapPercentages = (global.market_cap_percentage ?? {}) as Record<string, unknown>;
  const stablecoinDominancePct = [
    "usdt",
    "usdc",
    "dai",
    "busd",
    "tusd",
    "usde",
    "usdd",
    "fdusd",
    "pyusd",
    "usdp",
    "gusd",
    "frax",
  ].reduce((sum, symbol) => sum + parsePositiveNumber(marketCapPercentages[symbol]), 0);

  return {
    totalVolume24hUsd: round(totalVolume24hUsd, 6),
    marketCapChangePct24h: round(marketCapChangePct24h, 6),
    stablecoinDominancePct: round(stablecoinDominancePct, 6),
    usable: totalVolume24hUsd > 0 || stablecoinDominancePct > 0,
  };
}

function parseCoinPaprikaGlobalMetrics(data: unknown): GlobalMetrics {
  const payload = (data ?? {}) as Record<string, unknown>;
  const totalVolume24hUsd = parsePositiveNumber(
    payload.volume_24h_usd ??
    payload.volume_24h_usd_reported ??
    payload.total_volume_24h_usd,
  );
  const marketCapChangePct24h = parsePositiveNumber(
    payload.market_cap_change_24h ??
    payload.market_cap_change_24h_percent ??
    payload.market_cap_change_percentage_24h_usd,
  );
  const stablecoinDominancePct = parsePositiveNumber(
    payload.stablecoin_dominance ??
    payload.stablecoins_dominance ??
    payload.stablecoin_dominance_pct,
  );

  return {
    totalVolume24hUsd: round(totalVolume24hUsd, 6),
    marketCapChangePct24h: round(marketCapChangePct24h, 6),
    stablecoinDominancePct: round(stablecoinDominancePct, 6),
    usable: totalVolume24hUsd > 0 || stablecoinDominancePct > 0,
  };
}

function parseCoinMarketCapGlobalMetrics(data: unknown): GlobalMetrics {
  const payload = ((data as { data?: Record<string, unknown> } | null)?.data ?? {}) as Record<string, unknown>;
  const quote = ((payload.quote as Record<string, unknown> | undefined)?.USD ??
    (payload.quote as Record<string, unknown> | undefined)?.usd ??
    {}) as Record<string, unknown>;

  const totalVolume24hUsd = parsePositiveNumber(
    quote.total_volume_24h ??
      payload.total_volume_24h ??
      quote.total_volume_24h_reported ??
      payload.total_volume_24h_reported,
  );
  const marketCapChangePct24h = parsePositiveNumber(
    quote.total_market_cap_yesterday_percentage_change ??
      payload.total_market_cap_yesterday_percentage_change ??
      payload.market_cap_change_24h ??
      payload.market_cap_change_24h_percent,
  );
  const totalMarketCap = parsePositiveNumber(quote.total_market_cap ?? payload.total_market_cap);
  const stablecoinMarketCap = parsePositiveNumber(
    payload.stablecoin_market_cap ??
      quote.stablecoin_market_cap ??
      payload.stablecoins_market_cap,
  );
  const stablecoinDominancePct =
    totalMarketCap > 0 && stablecoinMarketCap > 0
      ? (stablecoinMarketCap / totalMarketCap) * 100
      : parsePositiveNumber(
          payload.stablecoin_dominance ??
            payload.stablecoin_dominance_pct,
        );

  return {
    totalVolume24hUsd: round(totalVolume24hUsd, 6),
    marketCapChangePct24h: round(marketCapChangePct24h, 6),
    stablecoinDominancePct: round(stablecoinDominancePct, 6),
    usable: totalVolume24hUsd > 0 || stablecoinDominancePct > 0 || totalMarketCap > 0,
  };
}

function parseCoinGeckoBreadthMetrics(data: unknown): MarketBreadthMetrics {
  if (!Array.isArray(data)) {
    return { positiveRatio: 0, averageAbsMove24h: 0, assetCount: 0, usable: false };
  }

  const changeValues: number[] = [];
  for (const row of data) {
    if (!row || typeof row !== "object") continue;
    const candidate = row as Record<string, unknown>;
    const change = Number(candidate.price_change_percentage_24h_in_currency ?? candidate.price_change_percentage_24h);
    if (!Number.isFinite(change)) continue;
    changeValues.push(change);
  }

  if (changeValues.length === 0) {
    return { positiveRatio: 0, averageAbsMove24h: 0, assetCount: 0, usable: false };
  }

  const positiveRatio = changeValues.filter((value) => value > 0).length / changeValues.length;
  const averageAbsMove24h = average(changeValues.map((value) => Math.abs(value)));

  return {
    positiveRatio: round(clamp(positiveRatio, 0, 1), 6),
    averageAbsMove24h: round(Math.max(0, averageAbsMove24h), 6),
    assetCount: changeValues.length,
    usable: true,
  };
}

function parseCoinLoreBreadthMetrics(data: unknown): MarketBreadthMetrics {
  const payload = data as { data?: unknown[] } | null;
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  const changeValues: number[] = [];

  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const candidate = row as Record<string, unknown>;
    const change = Number(candidate.percent_change_24h);
    if (!Number.isFinite(change)) continue;
    changeValues.push(change);
  }

  if (changeValues.length === 0) {
    return { positiveRatio: 0, averageAbsMove24h: 0, assetCount: 0, usable: false };
  }

  const positiveRatio = changeValues.filter((value) => value > 0).length / changeValues.length;
  const averageAbsMove24h = average(changeValues.map((value) => Math.abs(value)));

  return {
    positiveRatio: round(clamp(positiveRatio, 0, 1), 6),
    averageAbsMove24h: round(Math.max(0, averageAbsMove24h), 6),
    assetCount: changeValues.length,
    usable: true,
  };
}

async function fetchAlternativeSentimentIndex(
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
): Promise<{ normalized: number; index: number }> {
  const payload = await fetchJson<{ data?: Array<{ value?: string | number }> }>(
    "https://api.alternative.me/fng/?limit=1",
    toolCalls,
    "alternative_me:fng_latest",
    sourceReferences,
  );
  const valueRaw = payload.data?.[0]?.value;
  const value = parsePositiveNumber(valueRaw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error("Invalid alternative.me fear and greed value.");
  }

  return {
    normalized: clamp((value - 50) / 50, -1, 1),
    index: round(clamp(value, 0, 100), 3),
  };
}

async function fetchText(
  url: string,
  toolCalls: Set<string>,
  toolCallId: string,
  sourceReferences?: Map<string, SourceReference>,
): Promise<string> {
  toolCalls.add(toolCallId);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12_000);

  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "text/plain,application/xml,text/xml,*/*",
      },
      cache: "no-store",
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = await response.text();
    if (sourceReferences && !sourceReferences.has(toolCallId)) {
      const parsed = parseToolCallIdentifier(toolCallId);
      sourceReferences.set(toolCallId, {
        id: toolCallId,
        provider: parsed.provider,
        endpoint: parsed.endpoint,
        url,
        fetched_at: nowIso(),
      });
    }

    return payload;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchBinanceMarketSeries(
  symbol: "BTCUSDT" | "ETHUSDT",
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  limitations: Set<string>,
): Promise<MarketSeries> {
  const toolId = `binance:${symbol.toLowerCase()}_klines_8d_hourly`;
  const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1h&limit=220`;

  try {
    const payload = await fetchJson<unknown>(url, toolCalls, toolId, sourceReferences);
    if (!Array.isArray(payload)) return { prices: [], volumes: [] };
    const rows = payload
      .map((row) => {
        if (!Array.isArray(row) || row.length < 6) return null;
        const openTime = Number(row[0]);
        const close = Number(row[4]);
        const volume = Number(row[5]);
        if (!Number.isFinite(openTime) || !Number.isFinite(close) || !Number.isFinite(volume)) return null;
        return { openTime, close, volume };
      })
      .filter((row): row is { openTime: number; close: number; volume: number } => row !== null)
      .sort((left, right) => left.openTime - right.openTime);

    return {
      prices: rows.map((row) => row.close),
      volumes: rows.map((row) => row.volume),
    };
  } catch {
    limitations.add(`missing_${symbol.toLowerCase()}_binance_klines`);
    return { prices: [], volumes: [] };
  }
}

async function fetchKrakenOhlcMarketSeries(
  pair: "XBTUSD" | "ETHUSD",
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  limitations: Set<string>,
): Promise<MarketSeries> {
  const toolId = `kraken:${pair.toLowerCase()}_ohlc_8d_hourly`;
  const url = `https://api.kraken.com/0/public/OHLC?pair=${pair}&interval=60`;

  try {
    const payload = await fetchJson<Record<string, unknown>>(url, toolCalls, toolId, sourceReferences);
    const result = (payload.result ?? {}) as Record<string, unknown>;
    const key = Object.keys(result).find((item) => item !== "last");
    if (!key || !Array.isArray(result[key])) return { prices: [], volumes: [] };
    const rows = (result[key] as unknown[])
      .map((row) => {
        if (!Array.isArray(row) || row.length < 7) return null;
        const timestamp = Number(row[0]);
        const close = Number(row[4]);
        const volume = Number(row[6]);
        if (!Number.isFinite(timestamp) || !Number.isFinite(close) || !Number.isFinite(volume)) return null;
        return { timestamp, close, volume };
      })
      .filter((row): row is { timestamp: number; close: number; volume: number } => row !== null)
      .sort((left, right) => left.timestamp - right.timestamp);

    return {
      prices: rows.map((row) => row.close),
      volumes: rows.map((row) => row.volume),
    };
  } catch {
    limitations.add(`missing_${pair.toLowerCase()}_kraken_ohlc`);
    return { prices: [], volumes: [] };
  }
}

async function fetchCoinLoreGlobalMetrics(
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
): Promise<GlobalMetrics> {
  const payload = await fetchJson<Array<Record<string, unknown>>>(
    "https://api.coinlore.net/api/global/",
    toolCalls,
    "coinlore:global_market_metrics",
    sourceReferences,
  );
  const row = Array.isArray(payload) && payload.length > 0 ? payload[0] : {};
  const totalVolume24hUsd = parsePositiveNumber(
    row.total_volume ??
    row.total_volume24 ??
    row.total_volume_24h ??
    row.volume24a,
  );
  const marketCapChangePct24h = parsePositiveNumber(
    row.mcap_change ??
    row.market_cap_change_24h ??
    row.market_cap_change_24h_percent,
  );
  const stablecoinDominancePct = parsePositiveNumber(
    row.stablecoin_dominance ?? row.stablecoin_dominance_pct,
  );

  return {
    totalVolume24hUsd: round(totalVolume24hUsd, 6),
    marketCapChangePct24h: round(marketCapChangePct24h, 6),
    stablecoinDominancePct: round(stablecoinDominancePct, 6),
    usable: totalVolume24hUsd > 0 || stablecoinDominancePct > 0,
  };
}

function decodeXmlEntities(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .trim();
}

function parseRssNewsItems(xml: string, source: string): NewsItem[] {
  const itemBlocks = Array.from(xml.matchAll(/<item[\s\S]*?<\/item>/gi)).map((match) => match[0]);
  const nowSeconds = Math.floor(Date.now() / 1000);

  return itemBlocks
    .map((item, index) => {
      const cdataMatch = item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/i);
      const plainMatch = item.match(/<title>(.*?)<\/title>/i);
      const rawTitle = (cdataMatch?.[1] || plainMatch?.[1] || "").trim();
      if (!rawTitle) return null;

      const pubDateMatch = item.match(/<pubDate>(.*?)<\/pubDate>/i);
      const publishedMs = pubDateMatch ? Date.parse(pubDateMatch[1].trim()) : Number.NaN;
      const fallbackSeconds = nowSeconds - index * 60;

      return {
        title: decodeXmlEntities(rawTitle),
        body: "",
        source,
        publishedOn: Number.isFinite(publishedMs) ? Math.floor(publishedMs / 1000) : fallbackSeconds,
        upvotes: 0,
        downvotes: 0,
      } satisfies NewsItem;
    })
    .filter((item): item is NewsItem => item !== null);
}

function computeReturns(prices: number[]): number[] {
  const returns: number[] = [];
  for (let index = 1; index < prices.length; index += 1) {
    const previous = prices[index - 1];
    const current = prices[index];
    if (previous <= 0 || !Number.isFinite(previous) || !Number.isFinite(current)) continue;
    returns.push((current - previous) / previous);
  }
  return returns;
}

function computeVolatilityPercent(prices: number[]): number {
  const returns = computeReturns(prices);
  return round(standardDeviation(returns) * 100, 6);
}

function computeDailyWindowVolatilitySeries(prices: number[], dayCount = 8): number[] {
  const values: number[] = [];
  for (let offset = dayCount - 1; offset >= 0; offset -= 1) {
    const end = prices.length - offset * 24;
    const start = end - 25;
    if (start < 0 || end > prices.length) continue;
    values.push(computeVolatilityPercent(prices.slice(start, end)));
  }
  return values;
}

function computeDailyWindowVolumeSeries(volumes: number[], dayCount = 8): number[] {
  const values: number[] = [];
  for (let offset = dayCount - 1; offset >= 0; offset -= 1) {
    const index = volumes.length - 1 - offset * 24;
    if (index < 0 || index >= volumes.length) continue;
    values.push(volumes[index]);
  }
  return values;
}

function combineSeries(first: number[], second: number[]): number[] {
  const length = Math.min(first.length, second.length);
  const values: number[] = [];
  for (let index = 0; index < length; index += 1) {
    values.push((first[index] + second[index]) / 2);
  }
  return values;
}

function normalizeWeights(weights: number[]): number[] {
  if (weights.length === 0) return [];
  const sanitized = weights.map((weight) => (Number.isFinite(weight) && weight > 0 ? weight : 0));
  const total = sanitized.reduce((sum, weight) => sum + weight, 0);
  if (total <= 0) {
    const uniform = 1 / sanitized.length;
    return sanitized.map(() => uniform);
  }
  return sanitized.map((weight) => weight / total);
}

function weightedAverage(values: number[], weights: number[]): number {
  if (values.length === 0) return 0;
  const normalized = normalizeWeights(weights);
  let total = 0;
  for (let index = 0; index < values.length; index += 1) {
    const value = Number.isFinite(values[index]) ? values[index] : 0;
    total += value * (normalized[index] ?? 0);
  }
  return total;
}

function aggregateMarketSeriesCandidates(
  domain: SelectionDomain,
  candidates: MarketSeriesCandidate[],
): MarketSeries {
  if (candidates.length === 0) return { prices: [], volumes: [] };
  if (candidates.length === 1) return candidates[0].series;

  const candidateWeights = candidates.map((candidate) =>
    clamp(getSourceCredibility(domain, candidate.provider), 0.05, 1),
  );
  const normalizedWeights = normalizeWeights(candidateWeights);

  const minPriceLength = Math.min(...candidates.map((candidate) => candidate.series.prices.length));
  const minVolumeLength = Math.min(...candidates.map((candidate) => candidate.series.volumes.length));

  const prices =
    minPriceLength > 0
      ? Array.from({ length: minPriceLength }, (_, offset) => {
          const sampleValues = candidates.map((candidate) => {
            const values = candidate.series.prices;
            return values[values.length - minPriceLength + offset] ?? 0;
          });
          return weightedAverage(sampleValues, normalizedWeights);
        })
      : candidates[candidates.length - 1].series.prices;

  const volumes =
    minVolumeLength > 0
      ? Array.from({ length: minVolumeLength }, (_, offset) => {
          const sampleValues = candidates.map((candidate) => {
            const values = candidate.series.volumes;
            return values[values.length - minVolumeLength + offset] ?? 0;
          });
          return weightedAverage(sampleValues, normalizedWeights);
        })
      : candidates[candidates.length - 1].series.volumes;

  return {
    prices: prices.map((value) => round(value, 8)),
    volumes: volumes.map((value) => round(value, 8)),
  };
}

function aggregateGlobalMetricsCandidates(candidates: GlobalMetricsCandidate[]): GlobalMetrics {
  if (candidates.length === 0) {
    return {
      totalVolume24hUsd: 0,
      marketCapChangePct24h: 0,
      stablecoinDominancePct: 0,
      usable: false,
    };
  }
  if (candidates.length === 1) {
    return candidates[0].metrics;
  }

  const weights = normalizeWeights(
    candidates.map((candidate) => clamp(getSourceCredibility("market_metrics", candidate.provider), 0.05, 1)),
  );
  const totalVolume24hUsd = weightedAverage(
    candidates.map((candidate) => candidate.metrics.totalVolume24hUsd),
    weights,
  );
  const marketCapChangePct24h = weightedAverage(
    candidates.map((candidate) => candidate.metrics.marketCapChangePct24h),
    weights,
  );

  const stablecoinCandidates = candidates
    .map((candidate, index) => ({
      value: candidate.metrics.stablecoinDominancePct,
      weight: weights[index],
    }))
    .filter((candidate) => candidate.value > 0);
  const stablecoinDominancePct =
    stablecoinCandidates.length > 0
      ? weightedAverage(
          stablecoinCandidates.map((candidate) => candidate.value),
          stablecoinCandidates.map((candidate) => candidate.weight),
        )
      : 0;

  return {
    totalVolume24hUsd: round(totalVolume24hUsd, 6),
    marketCapChangePct24h: round(marketCapChangePct24h, 6),
    stablecoinDominancePct: round(stablecoinDominancePct, 6),
    usable: totalVolume24hUsd > 0 || stablecoinDominancePct > 0,
  };
}

function aggregateMarketBreadthCandidates(candidates: MarketBreadthCandidate[]): MarketBreadthMetrics {
  if (candidates.length === 0) {
    return { positiveRatio: 0, averageAbsMove24h: 0, assetCount: 0, usable: false };
  }
  if (candidates.length === 1) {
    return candidates[0].metrics;
  }

  const weights = normalizeWeights(
    candidates.map((candidate) => clamp(getSourceCredibility("market_metrics", candidate.provider), 0.05, 1)),
  );
  const positiveRatio = weightedAverage(
    candidates.map((candidate) => candidate.metrics.positiveRatio),
    weights,
  );
  const averageAbsMove24h = weightedAverage(
    candidates.map((candidate) => candidate.metrics.averageAbsMove24h),
    weights,
  );
  const assetCount = Math.round(weightedAverage(candidates.map((candidate) => candidate.metrics.assetCount), weights));

  return {
    positiveRatio: round(clamp(positiveRatio, 0, 1), 6),
    averageAbsMove24h: round(Math.max(0, averageAbsMove24h), 6),
    assetCount: Math.max(0, assetCount),
    usable: positiveRatio > 0 || averageAbsMove24h > 0,
  };
}

function computePearsonCorrelation(left: number[], right: number[]) {
  const length = Math.min(left.length, right.length);
  if (length < 3) return 0;

  const x = left.slice(-length);
  const y = right.slice(-length);
  const meanX = average(x);
  const meanY = average(y);
  let numerator = 0;
  let denominatorX = 0;
  let denominatorY = 0;

  for (let index = 0; index < length; index += 1) {
    const dx = x[index] - meanX;
    const dy = y[index] - meanY;
    numerator += dx * dy;
    denominatorX += dx * dx;
    denominatorY += dy * dy;
  }

  const denominator = Math.sqrt(denominatorX * denominatorY);
  if (denominator === 0) return 0;
  return clamp(numerator / denominator, -1, 1);
}

function computeLexiconSentimentScore(text: string) {
  const normalized = text.toLowerCase();
  let positive = 0;
  let negative = 0;

  for (const token of POSITIVE_NEWS_TOKENS) {
    if (normalized.includes(token)) positive += 1;
  }
  for (const token of NEGATIVE_NEWS_TOKENS) {
    if (normalized.includes(token)) negative += 1;
  }

  if (positive === 0 && negative === 0) return 0;
  return clamp((positive - negative) / (positive + negative), -1, 1);
}

function hashSystemPrompt() {
  return createHash("sha256").update(SELUN_PHASE1_SYSTEM_PROMPT).digest("hex");
}

function hashPhase2SystemPrompt() {
  return createHash("sha256").update(SELUN_PHASE2_SYSTEM_PROMPT).digest("hex");
}

function parseRiskMode(value: string | undefined): RiskMode {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "conservative") return "conservative";
  if (normalized === "balanced") return "balanced";
  if (normalized === "growth") return "growth";
  if (normalized === "aggressive") return "aggressive";
  if (normalized === "neutral") return "neutral";
  return "neutral";
}

function canonicalRiskToleranceFromRiskMode(riskMode: RiskMode): UserRiskTolerance {
  if (riskMode === "conservative") return "Conservative";
  if (riskMode === "aggressive") return "Aggressive";
  if (riskMode === "growth") return "Growth";
  return "Balanced";
}

function parseRiskTolerance(value: string | undefined, fallbackRiskMode: RiskMode): UserRiskTolerance {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "conservative") return "Conservative";
  if (normalized === "balanced") return "Balanced";
  if (normalized === "growth") return "Growth";
  if (normalized === "aggressive") return "Aggressive";
  return canonicalRiskToleranceFromRiskMode(fallbackRiskMode);
}

function parseInvestmentTimeframe(value: string | undefined, fallbackTimeWindow: string): UserInvestmentTimeframe {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "<1_year" || normalized === "< 1 year" || normalized === "lt_1_year") return "<1_year";
  if (normalized === "1-3_years" || normalized === "1-3 years" || normalized === "1_3_years") return "1-3_years";
  if (normalized === "3+_years" || normalized === "3+ years" || normalized === "gt_3_years") return "3+_years";
  if (fallbackTimeWindow === "7d") return "<1_year";
  return "1-3_years";
}

function parseTimeWindow(value: string | undefined): { label: string } {
  const normalized = value?.trim().toLowerCase() || "30d";
  if (normalized.includes("7")) return { label: "7d" };
  if (normalized.includes("30")) return { label: "30d" };
  if (normalized.includes("14")) return { label: "14d" };
  return { label: "30d" };
}

function normalizeRunInput(input: string | Phase1RunInput): NormalizedPhase1Input {
  if (typeof input === "string") {
    const { label } = parseTimeWindow(undefined);
    const riskMode = parseRiskMode(undefined);
    return {
      jobId: input.trim(),
      executionTimestamp: nowIso(),
      riskMode,
      userProfile: {
        riskTolerance: canonicalRiskToleranceFromRiskMode(riskMode),
        investmentTimeframe: parseInvestmentTimeframe(undefined, label),
      },
      timeWindow: label,
    };
  }

  const { label } = parseTimeWindow(input.timeWindow);
  const riskMode = parseRiskMode(input.riskMode);
  return {
    jobId: input.jobId.trim(),
    executionTimestamp: input.executionTimestamp?.trim() || nowIso(),
    riskMode,
    userProfile: {
      riskTolerance: parseRiskTolerance(input.riskTolerance, riskMode),
      investmentTimeframe: parseInvestmentTimeframe(input.investmentTimeframe, label),
    },
    timeWindow: label,
    walletAddress: normalizeWalletKey(input.walletAddress) ?? undefined,
  };
}

function parseOrderedSources(
  envName: string,
  defaults: readonly string[],
): string[] {
  const raw = process.env[envName]?.trim();
  if (!raw) return [...defaults];

  const parsed = raw
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
  return parsed.length > 0 ? Array.from(new Set(parsed)) : [...defaults];
}

function getPhase1ExecutionTuning() {
  return {
    maxUsableDataAttempts: readPositiveIntEnv(
      "PHASE1_MAX_USABLE_DATA_ATTEMPTS",
      DEFAULT_PHASE1_MAX_USABLE_DATA_ATTEMPTS,
    ),
    retryDelayMs: readPositiveIntEnv(
      "PHASE1_RETRY_DELAY_MS",
      DEFAULT_PHASE1_RETRY_DELAY_MS,
    ),
    maxRetryDelayMs: readPositiveIntEnv(
      "PHASE1_MAX_RETRY_DELAY_MS",
      DEFAULT_PHASE1_MAX_RETRY_DELAY_MS,
    ),
    snapshotMaxAgeMs: readPositiveIntEnv(
      "PHASE1_SNAPSHOT_MAX_AGE_MS",
      DEFAULT_PHASE1_SNAPSHOT_MAX_AGE_MS,
    ),
  };
}

function getPhase2AgentReasoningConfig() {
  return {
    provider: PHASE2_AGENT_REASONING_PROVIDER,
  };
}

function buildDeterministicPhase2AgentJudgement(
  phase1: Phase1Output,
  input: NormalizedPhase1Input,
): Phase2AgentJudgement {
  const market = phase1.market_condition;
  const fearGreed = phase1.evidence.sentiment_metrics.fear_greed_available
    ? phase1.evidence.sentiment_metrics.fear_greed_index
    : null;
  const reasonCodes = new Set<string>();

  let posture: Phase2AgentPosture = "neutral";
  if (
    market.risk_appetite === "defensive" ||
    market.liquidity_state === "weak" ||
    market.volatility_state === "extreme" ||
    market.uncertainty >= 0.65 ||
    (fearGreed !== null && fearGreed <= 30)
  ) {
    posture = "more_defensive";
    reasonCodes.add("macro_defensive_or_stress");
  } else if (
    market.risk_appetite === "expansionary" &&
    market.liquidity_state === "strong" &&
    market.confidence >= 0.62 &&
    market.uncertainty <= 0.52 &&
    market.sentiment_direction >= 0.1
  ) {
    posture = "selective_risk_on";
    reasonCodes.add("macro_expansionary_alignment");
  } else {
    reasonCodes.add("macro_neutral_mixed_signals");
  }

  let authorizationHint: Phase2AgentAuthorizationHint = "NO_CHANGE";
  if (
    market.volatility_state === "extreme" ||
    market.uncertainty >= 0.75 ||
    market.liquidity_state === "weak" ||
    (fearGreed !== null && fearGreed <= 20) ||
    phase1.allocation_authorization.status === "PROHIBITED"
  ) {
    authorizationHint = "TIGHTEN";
    reasonCodes.add("authorization_tighten_guardrails");
  } else if (
    posture === "selective_risk_on" &&
    market.confidence >= 0.68 &&
    market.uncertainty <= 0.5 &&
    (input.userProfile.riskTolerance === "Growth" || input.userProfile.riskTolerance === "Aggressive")
  ) {
    authorizationHint = "RELAX";
    reasonCodes.add("authorization_relax_within_bounds");
  } else {
    reasonCodes.add("authorization_no_change");
  }

  const envelope = {
    risk_budget_delta: 0,
    stablecoin_minimum_delta: 0,
    high_volatility_asset_cap_delta: 0,
    max_single_asset_exposure_delta: 0,
    portfolio_volatility_target_delta: 0,
    volatility_ceiling_delta: 0,
    capital_preservation_bias_delta: 0,
  };

  if (posture === "more_defensive") {
    envelope.risk_budget_delta -= 0.03;
    envelope.stablecoin_minimum_delta += 0.03;
    envelope.high_volatility_asset_cap_delta -= 0.03;
    envelope.max_single_asset_exposure_delta -= 0.02;
    envelope.portfolio_volatility_target_delta -= 0.02;
    envelope.volatility_ceiling_delta -= 0.03;
    envelope.capital_preservation_bias_delta += 0.05;
  } else if (posture === "selective_risk_on") {
    envelope.risk_budget_delta += 0.02;
    envelope.stablecoin_minimum_delta -= 0.02;
    envelope.high_volatility_asset_cap_delta += 0.02;
    envelope.max_single_asset_exposure_delta += 0.01;
    envelope.portfolio_volatility_target_delta += 0.02;
    envelope.volatility_ceiling_delta += 0.02;
    envelope.capital_preservation_bias_delta -= 0.04;
  }

  if (authorizationHint === "TIGHTEN") {
    envelope.risk_budget_delta -= 0.01;
    envelope.stablecoin_minimum_delta += 0.01;
    envelope.high_volatility_asset_cap_delta -= 0.01;
    envelope.capital_preservation_bias_delta += 0.02;
  } else if (authorizationHint === "RELAX") {
    envelope.risk_budget_delta += 0.01;
    envelope.stablecoin_minimum_delta -= 0.01;
    envelope.high_volatility_asset_cap_delta += 0.01;
    envelope.capital_preservation_bias_delta -= 0.02;
  }

  const result: Phase2AgentJudgement = {
    posture,
    authorization_hint: authorizationHint,
    reason_codes: Array.from(reasonCodes).slice(0, 8),
    envelope_adjustments: {
      risk_budget_delta: round(clamp(envelope.risk_budget_delta, -0.05, 0.05), 6),
      stablecoin_minimum_delta: round(clamp(envelope.stablecoin_minimum_delta, -0.05, 0.05), 6),
      high_volatility_asset_cap_delta: round(clamp(envelope.high_volatility_asset_cap_delta, -0.05, 0.05), 6),
      max_single_asset_exposure_delta: round(clamp(envelope.max_single_asset_exposure_delta, -0.05, 0.05), 6),
      portfolio_volatility_target_delta: round(clamp(envelope.portfolio_volatility_target_delta, -0.05, 0.05), 6),
      volatility_ceiling_delta: round(clamp(envelope.volatility_ceiling_delta, -0.05, 0.05), 6),
      capital_preservation_bias_delta: round(clamp(envelope.capital_preservation_bias_delta, -0.08, 0.08), 6),
    },
  };

  const parsed = phase2AgentJudgementSchema.safeParse(result);
  if (!parsed.success) {
    throw new Error("phase2_agent_judgement_schema_validation_failed");
  }
  return parsed.data;
}

async function runPhase2AgentJudgement(
  phase1: Phase1Output,
  input: NormalizedPhase1Input,
): Promise<Phase2AgentInvocationResult> {
  const config = getPhase2AgentReasoningConfig();

  emitExecutionLog({
    phase: "determine_allocation_policy",
    action: "phase2_agent_judgement",
    status: "started",
    transactionHash: null,
  });

  try {
    await initializeAgent();
    const judgement = buildDeterministicPhase2AgentJudgement(phase1, input);

    emitExecutionLog({
      phase: "determine_allocation_policy",
      action: "phase2_agent_judgement",
      status: "success",
      transactionHash: null,
    });

    return {
      used: true,
      model: config.provider,
      judgement,
    };
  } catch (error) {
    emitExecutionLog({
      phase: "determine_allocation_policy",
      action: "phase2_agent_judgement",
      status: "error",
      transactionHash: null,
    });
    const reason = error instanceof Error ? error.message : "unknown";
    throw new Error(`phase2_agent_required:${reason}`);
  }
}

function createSourceSelectionState(): Record<SelectionDomain, SourceSelectionRecord> {
  return {
    volatility: {
      domain: "volatility",
      selected: [],
      rejected: [],
      rationale: [],
    },
    liquidity: {
      domain: "liquidity",
      selected: [],
      rejected: [],
      rationale: [],
    },
    sentiment: {
      domain: "sentiment",
      selected: [],
      rejected: [],
      rationale: [],
    },
    market_metrics: {
      domain: "market_metrics",
      selected: [],
      rejected: [],
      rationale: [],
    },
  };
}

function addSelection(
  selections: Record<SelectionDomain, SourceSelectionRecord>,
  domain: SelectionDomain,
  sourceId: string,
  reason: string,
) {
  const record = selections[domain];
  if (!record.selected.includes(sourceId)) {
    record.selected.push(sourceId);
  }
  record.rationale.push(reason);
}

function addRejection(
  selections: Record<SelectionDomain, SourceSelectionRecord>,
  domain: SelectionDomain,
  sourceId: string,
  reason: string,
) {
  const record = selections[domain];
  const exists = record.rejected.some((item) => item.id === sourceId && item.reason === reason);
  if (!exists) {
    record.rejected.push({ id: sourceId, reason });
  }
}

function cloneSourceSelectionRecords(records: SourceSelectionRecord[]): SourceSelectionRecord[] {
  return records.map((record) => ({
    domain: record.domain,
    selected: [...record.selected],
    rejected: record.rejected.map((item) => ({ id: item.id, reason: item.reason })),
    rationale: [...record.rationale],
  }));
}

function cloneSourceReferences(records: SourceReference[]): SourceReference[] {
  return records.map((item) => ({
    id: item.id,
    provider: item.provider,
    endpoint: item.endpoint,
    url: item.url,
    fetched_at: item.fetched_at,
  }));
}

function mergeSnapshotSelection(
  target: Record<SelectionDomain, SourceSelectionRecord>,
  snapshotSelection: SourceSelectionRecord[],
) {
  for (const record of snapshotSelection) {
    const targetRecord = target[record.domain];
    for (const selected of record.selected) {
      if (!targetRecord.selected.includes(selected)) {
        targetRecord.selected.push(selected);
      }
    }
    for (const rejection of record.rejected) {
      const exists = targetRecord.rejected.some(
        (item) => item.id === rejection.id && item.reason === rejection.reason,
      );
      if (!exists) {
        targetRecord.rejected.push({ id: rejection.id, reason: rejection.reason });
      }
    }
    for (const rationale of record.rationale) {
      if (!targetRecord.rationale.includes(rationale)) {
        targetRecord.rationale.push(rationale);
      }
    }
  }
}

function cloneVolatilityDomain(domain: VolatilityDomain): VolatilityDomain {
  return {
    btcVolatility24h: domain.btcVolatility24h,
    ethVolatility24h: domain.ethVolatility24h,
    volatilityZScore: domain.volatilityZScore,
    volatilityState: domain.volatilityState,
    missing: domain.missing,
  };
}

function cloneLiquidityDomain(domain: LiquidityDomain): LiquidityDomain {
  return {
    totalVolume24h: domain.totalVolume24h,
    volumeDeviationZScore: domain.volumeDeviationZScore,
    avgSpread: domain.avgSpread,
    stablecoinDominance: domain.stablecoinDominance,
    liquidityState: domain.liquidityState,
    missing: domain.missing,
  };
}

function cloneSentimentDomain(domain: SentimentDomain): SentimentDomain {
  const fearGreedIndexRaw = (domain as Partial<SentimentDomain>).fearGreedIndex;
  const fearGreedIndex =
    fearGreedIndexRaw === null || fearGreedIndexRaw === undefined
      ? null
      : Number.isFinite(Number(fearGreedIndexRaw))
        ? clamp(Number(fearGreedIndexRaw), 0, 100)
        : null;

  return {
    headlineCount: domain.headlineCount,
    aggregateSentimentScore: domain.aggregateSentimentScore,
    engagementDeviation: domain.engagementDeviation,
    sentimentDirection: domain.sentimentDirection,
    sentimentAlignment: domain.sentimentAlignment,
    publicEchoStrength: domain.publicEchoStrength,
    sourceCount: Math.max(0, Math.floor((domain as Partial<SentimentDomain>).sourceCount ?? 0)),
    sourceConsensus: clamp((domain as Partial<SentimentDomain>).sourceConsensus ?? 0, 0, 1),
    fearGreedIncluded: Boolean((domain as Partial<SentimentDomain>).fearGreedIncluded),
    fearGreedIndex,
    newsItems: domain.newsItems.map((item) => ({
      title: item.title,
      body: item.body,
      source: item.source,
      publishedOn: item.publishedOn,
      upvotes: item.upvotes,
      downvotes: item.downvotes,
    })),
    missing: domain.missing,
  };
}

function cloneAlignmentDomain(domain: AlignmentDomain): AlignmentDomain {
  return {
    riskAppetite: domain.riskAppetite,
    confidence: domain.confidence,
    uncertainty: domain.uncertainty,
  };
}

function getSourceIntelligenceKey(domain: SelectionDomain, provider: string): string {
  return `${domain}:${provider}`;
}

function computeCredibilityScore(record: {
  successes: number;
  failures: number;
  avg_latency_ms: number;
  last_success_at: string | null;
}): number {
  const attempts = record.successes + record.failures;
  const successRate = attempts > 0 ? record.successes / attempts : 0.5;
  const latencyScore = clamp(1 - record.avg_latency_ms / 4000, 0, 1);
  const freshnessScore = (() => {
    if (!record.last_success_at) return 0.2;
    const ageMs = Date.now() - new Date(record.last_success_at).getTime();
    if (!Number.isFinite(ageMs) || ageMs < 0) return 0.2;
    const weekMs = 7 * 24 * 60 * 60 * 1000;
    return clamp(1 - ageMs / weekMs, 0, 1);
  })();
  return round(clamp(successRate * 0.6 + freshnessScore * 0.25 + latencyScore * 0.15, 0, 1), 6);
}

function restoreSourceIntelligenceFromDisk() {
  if (sourceIntelligenceLoadedFromDisk) return;
  sourceIntelligenceLoadedFromDisk = true;

  try {
    if (!fs.existsSync(PHASE1_SOURCE_INTELLIGENCE_PATH)) return;
    const raw = fs.readFileSync(PHASE1_SOURCE_INTELLIGENCE_PATH, "utf8");
    const parsed = JSON.parse(raw) as { records?: SourceCredibilityRecord[] };
    const records = Array.isArray(parsed.records) ? parsed.records : [];

    for (const record of records) {
      if (!record || typeof record !== "object") continue;
      if (!record.domain || !record.provider) continue;
      const normalized: SourceCredibilityRecord = {
        domain: record.domain,
        provider: String(record.provider),
        score: clamp(Number(record.score) || 0, 0, 1),
        successes: Math.max(0, Math.floor(Number(record.successes) || 0)),
        failures: Math.max(0, Math.floor(Number(record.failures) || 0)),
        last_success_at: record.last_success_at ?? null,
        last_failure_at: record.last_failure_at ?? null,
        avg_latency_ms: Math.max(0, Number(record.avg_latency_ms) || 0),
      };
      const key = getSourceIntelligenceKey(normalized.domain, normalized.provider);
      sourceIntelligenceByKey.set(key, normalized);
    }
  } catch {
    // Invalid credibility cache is ignored; runtime will rebuild.
  }
}

function persistSourceIntelligence() {
  try {
    fs.mkdirSync(path.dirname(PHASE1_SOURCE_INTELLIGENCE_PATH), { recursive: true });
    const records = Array.from(sourceIntelligenceByKey.values()).sort(
      (left, right) =>
        left.domain.localeCompare(right.domain) ||
        right.score - left.score ||
        left.provider.localeCompare(right.provider),
    );
    fs.writeFileSync(
      PHASE1_SOURCE_INTELLIGENCE_PATH,
      JSON.stringify({ updatedAt: nowIso(), records }, null, 2),
      "utf8",
    );
  } catch {
    // Best-effort only.
  }
}

function getSourceCredibility(domain: SelectionDomain, provider: string): number {
  const key = getSourceIntelligenceKey(domain, provider);
  return sourceIntelligenceByKey.get(key)?.score ?? 0.5;
}

function getSourceCredibilitySnapshot(): SourceCredibilityRecord[] {
  return Array.from(sourceIntelligenceByKey.values()).sort(
    (left, right) =>
      left.domain.localeCompare(right.domain) ||
      right.score - left.score ||
      left.provider.localeCompare(right.provider),
  );
}

function recordSourceOutcome(
  domain: SelectionDomain,
  provider: string,
  success: boolean,
  latencyMs: number,
) {
  const key = getSourceIntelligenceKey(domain, provider);
  const existing = sourceIntelligenceByKey.get(key) ?? {
    domain,
    provider,
    score: 0.5,
    successes: 0,
    failures: 0,
    last_success_at: null,
    last_failure_at: null,
    avg_latency_ms: Math.max(0, latencyMs),
  };

  if (success) {
    existing.successes += 1;
    existing.last_success_at = nowIso();
  } else {
    existing.failures += 1;
    existing.last_failure_at = nowIso();
  }

  const total = existing.successes + existing.failures;
  if (total <= 1) {
    existing.avg_latency_ms = Math.max(0, latencyMs);
  } else {
    existing.avg_latency_ms = round(
      clamp((existing.avg_latency_ms * (total - 1) + Math.max(0, latencyMs)) / total, 0, 60_000),
      3,
    );
  }
  existing.score = computeCredibilityScore(existing);
  sourceIntelligenceByKey.set(key, existing);
}

function buildProviderOrder(
  domain: SelectionDomain,
  configured: string[],
  discoveryPool: string[],
): string[] {
  const configuredUnique = Array.from(new Set(configured.map((provider) => provider.trim().toLowerCase()).filter(Boolean)));
  const discoveryUnique = Array.from(
    new Set(discoveryPool.map((provider) => provider.trim().toLowerCase()).filter(Boolean)),
  );
  const historicalUnique = Array.from(
    new Set(
      Array.from(sourceIntelligenceByKey.values())
        .filter((record) => record.domain === domain)
        .map((record) => record.provider.trim().toLowerCase())
        .filter(Boolean),
    ),
  );

  const configuredRank = new Map<string, number>();
  for (let index = 0; index < configuredUnique.length; index += 1) {
    configuredRank.set(configuredUnique[index], index);
  }

  const discoverySet = new Set(discoveryUnique);
  const candidateSet = new Set([...configuredUnique, ...discoveryUnique, ...historicalUnique]);

  return Array.from(candidateSet)
    .map((provider) => {
      const credibility = getSourceCredibility(domain, provider);
      const configuredIndex = configuredRank.get(provider);
      const configuredBoost = configuredIndex === undefined ? 0 : Math.max(0, 0.12 - configuredIndex * 0.02);
      const discoveryBoost = discoverySet.has(provider) ? 0.01 : 0;
      const historicalBoost = sourceIntelligenceByKey.has(getSourceIntelligenceKey(domain, provider)) ? 0.005 : 0;
      return {
        provider,
        score: credibility + configuredBoost + discoveryBoost + historicalBoost,
        credibility,
      };
    })
    .sort(
      (left, right) =>
        right.score - left.score ||
        right.credibility - left.credibility ||
        left.provider.localeCompare(right.provider),
    )
    .map((entry) => entry.provider);
}

function persistMacroSnapshot(snapshot: LastKnownGoodMacroSnapshot) {
  try {
    fs.mkdirSync(path.dirname(PHASE1_SNAPSHOT_PATH), { recursive: true });
    fs.writeFileSync(PHASE1_SNAPSHOT_PATH, JSON.stringify(snapshot, null, 2), "utf8");
  } catch {
    // Persistence is best-effort; in-memory snapshot remains active.
  }
}

function restoreMacroSnapshotFromDisk() {
  if (macroSnapshotLoadedFromDisk) return;
  macroSnapshotLoadedFromDisk = true;

  try {
    if (!fs.existsSync(PHASE1_SNAPSHOT_PATH)) return;
    const raw = fs.readFileSync(PHASE1_SNAPSHOT_PATH, "utf8");
    const parsed = JSON.parse(raw) as LastKnownGoodMacroSnapshot;
    if (!parsed || typeof parsed !== "object" || typeof parsed.capturedAt !== "string") return;
    if (
      !parsed.volatility ||
      !parsed.liquidity ||
      !parsed.sentiment ||
      !parsed.alignment ||
      !Array.isArray(parsed.sources) ||
      !Array.isArray(parsed.sourceSelection)
    ) {
      return;
    }

    lastKnownGoodMacroSnapshot = {
      capturedAt: parsed.capturedAt,
      volatility: cloneVolatilityDomain(parsed.volatility),
      liquidity: cloneLiquidityDomain(parsed.liquidity),
      sentiment: cloneSentimentDomain(parsed.sentiment),
      alignment: cloneAlignmentDomain(parsed.alignment),
      sources: cloneSourceReferences(parsed.sources),
      sourceSelection: cloneSourceSelectionRecords(parsed.sourceSelection),
    };
  } catch {
    // Invalid snapshot data is ignored and replaced by fresh runtime results.
  }
}

function parseToolCallIdentifier(toolCallId: string): { provider: string; endpoint: string } {
  const separatorIndex = toolCallId.indexOf(":");
  if (separatorIndex <= 0 || separatorIndex >= toolCallId.length - 1) {
    return { provider: "unknown", endpoint: toolCallId };
  }

  return {
    provider: toolCallId.slice(0, separatorIndex),
    endpoint: toolCallId.slice(separatorIndex + 1),
  };
}

async function fetchJson<T>(
  url: string,
  toolCalls: Set<string>,
  toolCallId: string,
  sourceReferences?: Map<string, SourceReference>,
  requestHeaders?: Record<string, string>,
): Promise<T> {
  toolCalls.add(toolCallId);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12_000);

  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        ...(requestHeaders ?? {}),
      },
      cache: "no-store",
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const payload = (await response.json()) as T;
    if (sourceReferences && !sourceReferences.has(toolCallId)) {
      const parsed = parseToolCallIdentifier(toolCallId);
      sourceReferences.set(toolCallId, {
        id: toolCallId,
        provider: parsed.provider,
        endpoint: parsed.endpoint,
        url,
        fetched_at: nowIso(),
      });
    }

    return payload;
  } finally {
    clearTimeout(timeout);
  }
}

function defaultAaaAllocateDispatch(): AaaAllocateDispatch {
  return { status: "idle" };
}

function resolveAaaApiBaseUrl(): string {
  return readStringEnv("AAA_API_BASE_URL") || readStringEnv("AAA_BASE_URL");
}

function resolveSelunExecutionBaseUrl(): string {
  const configured =
    readStringEnv("SELUN_EXECUTION_STATUS_BASE_URL") ||
    readStringEnv("SELUN_BACKEND_PUBLIC_URL") ||
    readStringEnv("SELUN_PUBLIC_BACKEND_URL");
  if (configured) return configured.replace(/\/+$/, "");
  const port = readPositiveIntEnv("PORT", 8787);
  return `http://localhost:${port}`;
}

function buildAaaAllocateHmacHeaders(bodyText: string, secret: string): Record<string, string> {
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const signature = createHmac("sha256", secret)
    .update(`${timestamp}.${bodyText}`)
    .digest("hex");
  return {
    "x-selun-timestamp": timestamp,
    "x-selun-signature": `sha256=${signature}`,
  };
}

async function forwardPhase6ToAaaAllocator(jobId: string, context: JobContext): Promise<void> {
  const aaaBaseUrl = resolveAaaApiBaseUrl();
  const enabled = readBooleanEnv("AAA_ALLOCATE_ENABLED", Boolean(aaaBaseUrl));
  if (!enabled || !aaaBaseUrl) {
    context.phase6.aaaAllocate = defaultAaaAllocateDispatch();
    return;
  }

  const endpoint = `${aaaBaseUrl.replace(/\/+$/, "")}${AAA_SELUN_ALLOCATE_PATH}`;
  const payload = {
    job_id: jobId,
    selun_base_url: resolveSelunExecutionBaseUrl(),
  };
  const bodyText = JSON.stringify(payload);
  const hmacSecret =
    readStringEnv("AAA_ALLOCATE_HMAC_SECRET") ||
    readStringEnv("SELUN_ALLOCATE_HMAC_SECRET");
  const timeoutMs = readPositiveIntEnv("AAA_ALLOCATE_TIMEOUT_MS", DEFAULT_AAA_ALLOCATE_TIMEOUT_MS);
  const headers: Record<string, string> = {
    "content-type": "application/json",
  };
  if (!hmacSecret) {
    context.phase6.aaaAllocate = {
      status: "failed",
      requestedAt: nowIso(),
      completedAt: nowIso(),
      endpoint,
      error: "AAA allocation forwarding requires AAA_ALLOCATE_HMAC_SECRET (or SELUN_ALLOCATE_HMAC_SECRET).",
    };
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: "forwarding_allocation_to_aaa",
      status: "failed",
      completedAt: nowIso(),
      error: context.phase6.aaaAllocate.error,
    });
    return;
  }
  Object.assign(headers, buildAaaAllocateHmacHeaders(bodyText, hmacSecret));

  const requestedAt = nowIso();
  context.phase6.aaaAllocate = {
    status: "in_progress",
    requestedAt,
    endpoint,
  };
  appendJobLog(jobId, {
    phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
    subPhase: "forwarding_allocation_to_aaa",
    status: "in_progress",
  });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers,
      body: bodyText,
      signal: controller.signal,
      cache: "no-store",
    });

    const responseText = await response.text();
    let responsePayload: unknown = responseText;
    if (responseText) {
      try {
        responsePayload = JSON.parse(responseText) as unknown;
      } catch {
        responsePayload = responseText;
      }
    }

    const completedAt = nowIso();
    if (!response.ok) {
      const detail =
        responsePayload &&
        typeof responsePayload === "object" &&
        "detail" in responsePayload
          ? String((responsePayload as { detail?: unknown }).detail ?? "")
          : `HTTP ${response.status}`;
      context.phase6.aaaAllocate = {
        status: "failed",
        requestedAt,
        completedAt,
        endpoint,
        httpStatus: response.status,
        response: responsePayload,
        error: `AAA allocation request failed: ${detail}`,
      };
      appendJobLog(jobId, {
        phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
        subPhase: "forwarding_allocation_to_aaa",
        status: "failed",
        completedAt,
        error: context.phase6.aaaAllocate.error,
      });
      return;
    }

    context.phase6.aaaAllocate = {
      status: "complete",
      requestedAt,
      completedAt,
      endpoint,
      httpStatus: response.status,
      response: responsePayload,
    };
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: "forwarding_allocation_to_aaa",
      status: "complete",
      completedAt,
    });
  } catch (error) {
    const completedAt = nowIso();
    const message = error instanceof Error ? error.message : "AAA allocation request failed.";
    context.phase6.aaaAllocate = {
      status: "failed",
      requestedAt,
      completedAt,
      endpoint,
      error: message,
    };
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: "forwarding_allocation_to_aaa",
      status: "failed",
      completedAt,
      error: message,
    });
  } finally {
    clearTimeout(timeout);
  }
}

function getOrCreateJobContext(jobId: string): JobContext {
  const existing = jobContextById.get(jobId);
  if (existing) return existing;

  const createdAt = nowIso();
  const context: JobContext = {
    jobId,
    createdAt,
    updatedAt: createdAt,
    phase1: {
      status: "idle",
      attempts: 0,
      promptVersion: SYSTEM_PROMPT_VERSION,
      promptHash: hashSystemPrompt(),
    },
    phase2: {
      status: "idle",
    },
    phase3: {
      status: "idle",
    },
    phase4: {
      status: "idle",
    },
    phase5: {
      status: "idle",
    },
    phase6: {
      status: "idle",
      aaaAllocate: defaultAaaAllocateDispatch(),
    },
    logs: [],
  };

  jobContextById.set(jobId, context);
  return context;
}

function appendJobLog(jobId: string, entry: Omit<JobProgressLog, "timestamp">) {
  const context = getOrCreateJobContext(jobId);
  const payload: JobProgressLog = {
    ...entry,
    timestamp: nowIso(),
  };

  context.logs.push(payload);
  if (context.logs.length > MAX_JOB_LOGS) {
    context.logs.shift();
  }
  context.updatedAt = payload.timestamp;

  emitExecutionLog({
    phase: entry.phase,
    action: entry.subPhase || "phase_status",
    status:
      entry.status === "in_progress"
        ? "pending"
        : entry.status === "complete"
          ? "success"
          : "error",
    transactionHash: null,
  });
}

function fallbackSharedMarketData(): SharedMarketData {
  return {
    btcPrices: [],
    ethPrices: [],
    btcVolumes: [],
    ethVolumes: [],
    totalVolume24hUsd: 0,
    marketCapChangePct24h: 0,
    stablecoinDominancePct: 0,
    marketBreadthPositiveRatio: 0,
    marketBreadthAbsMove24h: 0,
    marketBreadthAssetCount: 0,
    breadthMissing: true,
    missing: true,
  };
}

function fallbackVolatilityDomain(): VolatilityDomain {
  return {
    btcVolatility24h: 0,
    ethVolatility24h: 0,
    volatilityZScore: 0,
    volatilityState: "moderate",
    missing: true,
  };
}

function fallbackLiquidityDomain(): LiquidityDomain {
  return {
    totalVolume24h: 0,
    volumeDeviationZScore: 0,
    avgSpread: 0,
    stablecoinDominance: 0,
    liquidityState: "stable",
    missing: true,
  };
}

function fallbackSentimentDomain(): SentimentDomain {
  return {
    headlineCount: 0,
    aggregateSentimentScore: 0,
    engagementDeviation: 0,
    sentimentDirection: 0,
    sentimentAlignment: 0,
    publicEchoStrength: 0,
    sourceCount: 0,
    sourceConsensus: 0,
    fearGreedIncluded: false,
    fearGreedIndex: null,
    newsItems: [],
    missing: true,
  };
}

async function collectSharedMarketData(
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  limitations: Set<string>,
  sourceSelection: Record<SelectionDomain, SourceSelectionRecord>,
): Promise<SharedMarketData> {
  const configuredVolatilityOrder = parseOrderedSources(
    "PHASE1_VOLATILITY_SOURCE_ORDER",
    DEFAULT_VOLATILITY_SOURCE_ORDER,
  );
  const volatilityDiscoveryPool = parseOrderedSources(
    "PHASE1_DISCOVERY_VOLATILITY_POOL",
    DEFAULT_VOLATILITY_DISCOVERY_POOL,
  );
  const volatilityOrder = buildProviderOrder("volatility", configuredVolatilityOrder, volatilityDiscoveryPool);
  const volatilitySourceTarget = Math.min(
    4,
    Math.max(1, readPositiveIntEnv("PHASE1_VOLATILITY_SOURCE_TARGET", DEFAULT_PHASE1_VOLATILITY_SOURCE_TARGET)),
  );

  const configuredGlobalMetricsOrder = parseOrderedSources(
    "PHASE1_GLOBAL_METRICS_SOURCE_ORDER",
    DEFAULT_GLOBAL_METRICS_SOURCE_ORDER,
  );
  const globalMetricsDiscoveryPool = parseOrderedSources(
    "PHASE1_DISCOVERY_GLOBAL_METRICS_POOL",
    DEFAULT_GLOBAL_METRICS_DISCOVERY_POOL,
  );
  const globalMetricsOrder = buildProviderOrder(
    "market_metrics",
    configuredGlobalMetricsOrder,
    globalMetricsDiscoveryPool,
  );
  const globalMetricsSourceTarget = Math.min(
    3,
    Math.max(1, readPositiveIntEnv("PHASE1_GLOBAL_METRICS_SOURCE_TARGET", DEFAULT_PHASE1_GLOBAL_METRICS_SOURCE_TARGET)),
  );

  const resolveSeries = async (asset: "btc" | "eth"): Promise<MarketSeries> => {
    const coinGeckoConfig = asset === "btc"
      ? {
          url: "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=8&interval=hourly",
          toolId: "coingecko:bitcoin_market_chart_8d_hourly",
        }
      : {
          url: "https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days=8&interval=hourly",
          toolId: "coingecko:ethereum_market_chart_8d_hourly",
        };
    const coinbaseProduct = asset === "btc" ? "BTC-USD" : "ETH-USD";
    const binanceSymbol = asset === "btc" ? "BTCUSDT" : "ETHUSDT";
    const krakenPair = asset === "btc" ? "XBTUSD" : "ETHUSD";
    const candidates: MarketSeriesCandidate[] = [];

    for (const provider of volatilityOrder) {
      if (candidates.length >= volatilitySourceTarget) break;

      if (provider === "coingecko") {
        const startedAt = Date.now();
        try {
          const payload = await fetchJson<unknown>(
            coinGeckoConfig.url,
            toolCalls,
            coinGeckoConfig.toolId,
            sourceReferences,
          );
          const series = {
            prices: parseSeries(payload, "prices"),
            volumes: parseSeries(payload, "total_volumes"),
          };
          if (series.prices.length > 0) {
            recordSourceOutcome("volatility", provider, true, Date.now() - startedAt);
            candidates.push({
              provider,
              sourceId: coinGeckoConfig.toolId,
              series,
            });
            addSelection(
              sourceSelection,
              "volatility",
              `${provider}:${asset}`,
              `candidate_collected_provider_order:${volatilityOrder.join(">")};credibility:${getSourceCredibility("volatility", provider)}`,
            );
            continue;
          }
          recordSourceOutcome("volatility", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "volatility", `${provider}:${asset}`, "empty_series_payload");
        } catch (error) {
          recordSourceOutcome("volatility", provider, false, Date.now() - startedAt);
          addRejection(
            sourceSelection,
            "volatility",
            `${provider}:${asset}`,
            error instanceof Error ? error.message : "coingecko_fetch_failed",
          );
        }
        continue;
      }

      if (provider === "coinbase") {
        const startedAt = Date.now();
        const series = await fetchCoinbaseMarketSeries(coinbaseProduct, toolCalls, sourceReferences, limitations);
        if (series.prices.length > 0) {
          recordSourceOutcome("volatility", provider, true, Date.now() - startedAt);
          candidates.push({
            provider,
            sourceId: `coinbase_exchange:${coinbaseProduct.toLowerCase().replace("-", "_")}_candles_8d_hourly`,
            series,
          });
          addSelection(
            sourceSelection,
            "volatility",
            `${provider}:${asset}`,
            `candidate_collected_provider_order:${volatilityOrder.join(">")};credibility:${getSourceCredibility("volatility", provider)}`,
          );
          continue;
        }
        recordSourceOutcome("volatility", provider, false, Date.now() - startedAt);
        addRejection(sourceSelection, "volatility", `${provider}:${asset}`, "empty_series_payload");
        continue;
      }

      if (provider === "binance") {
        const startedAt = Date.now();
        const series = await fetchBinanceMarketSeries(binanceSymbol, toolCalls, sourceReferences, limitations);
        if (series.prices.length > 0) {
          recordSourceOutcome("volatility", provider, true, Date.now() - startedAt);
          candidates.push({
            provider,
            sourceId: `binance:${binanceSymbol.toLowerCase()}_klines_8d_hourly`,
            series,
          });
          addSelection(
            sourceSelection,
            "volatility",
            `${provider}:${asset}`,
            `candidate_collected_provider_order:${volatilityOrder.join(">")};credibility:${getSourceCredibility("volatility", provider)}`,
          );
          continue;
        }
        recordSourceOutcome("volatility", provider, false, Date.now() - startedAt);
        addRejection(sourceSelection, "volatility", `${provider}:${asset}`, "empty_series_payload");
        continue;
      }

      if (provider === "kraken_ohlc") {
        const startedAt = Date.now();
        const series = await fetchKrakenOhlcMarketSeries(krakenPair, toolCalls, sourceReferences, limitations);
        if (series.prices.length > 0) {
          recordSourceOutcome("volatility", provider, true, Date.now() - startedAt);
          candidates.push({
            provider,
            sourceId: `kraken:${krakenPair.toLowerCase()}_ohlc_8d_hourly`,
            series,
          });
          addSelection(
            sourceSelection,
            "volatility",
            `${provider}:${asset}`,
            `candidate_collected_provider_order:${volatilityOrder.join(">")};credibility:${getSourceCredibility("volatility", provider)}`,
          );
          continue;
        }
        recordSourceOutcome("volatility", provider, false, Date.now() - startedAt);
        addRejection(sourceSelection, "volatility", `${provider}:${asset}`, "empty_series_payload");
        continue;
      }

      recordSourceOutcome("volatility", provider, false, 0);
      addRejection(sourceSelection, "volatility", `${provider}:${asset}`, "unsupported_provider");
    }

    if (candidates.length > 1) {
      addSelection(
        sourceSelection,
        "volatility",
        `aggregated:${asset}`,
        `composite_series_sources:${candidates.map((candidate) => candidate.provider).join(">")};source_count:${candidates.length}`,
      );
      return aggregateMarketSeriesCandidates("volatility", candidates);
    }
    if (candidates.length === 1) {
      return candidates[0].series;
    }

    limitations.add(`missing_${asset}_market_series`);
    return { prices: [], volumes: [] };
  };

  const resolveGlobalMetrics = async (): Promise<GlobalMetrics> => {
    const candidates: GlobalMetricsCandidate[] = [];

    for (const provider of globalMetricsOrder) {
      if (candidates.length >= globalMetricsSourceTarget) break;

      if (provider === "coingecko") {
        const startedAt = Date.now();
        try {
          const payload = await fetchJson<unknown>(
            "https://api.coingecko.com/api/v3/global",
            toolCalls,
            "coingecko:global_market_metrics",
            sourceReferences,
          );
          const metrics = parseCoinGeckoGlobalMetrics(payload);
          if (metrics.usable) {
            recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
            candidates.push({
              provider,
              sourceId: "coingecko:global_market_metrics",
              metrics,
            });
            addSelection(
              sourceSelection,
              "market_metrics",
              "coingecko:global_market_metrics",
              `candidate_collected_provider_order:${globalMetricsOrder.join(">")};credibility:${getSourceCredibility("market_metrics", provider)}`,
            );
            continue;
          }
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "market_metrics", "coingecko:global_market_metrics", "unusable_payload");
        } catch (error) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(
            sourceSelection,
            "market_metrics",
            "coingecko:global_market_metrics",
            error instanceof Error ? error.message : "coingecko_global_fetch_failed",
          );
        }
        continue;
      }

      if (provider === "coinpaprika") {
        const startedAt = Date.now();
        try {
          const payload = await fetchJson<unknown>(
            "https://api.coinpaprika.com/v1/global",
            toolCalls,
            "coinpaprika:global_market_metrics",
            sourceReferences,
          );
          const metrics = parseCoinPaprikaGlobalMetrics(payload);
          if (metrics.usable) {
            recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
            candidates.push({
              provider,
              sourceId: "coinpaprika:global_market_metrics",
              metrics,
            });
            addSelection(
              sourceSelection,
              "market_metrics",
              "coinpaprika:global_market_metrics",
              `candidate_collected_provider_order:${globalMetricsOrder.join(">")};credibility:${getSourceCredibility("market_metrics", provider)}`,
            );
            continue;
          }
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "market_metrics", "coinpaprika:global_market_metrics", "unusable_payload");
        } catch (error) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(
            sourceSelection,
            "market_metrics",
            "coinpaprika:global_market_metrics",
            error instanceof Error ? error.message : "coinpaprika_global_fetch_failed",
          );
        }
        continue;
      }

      if (provider === "coinmarketcap") {
        const startedAt = Date.now();
        const apiKey = getCoinMarketCapApiKey();
        if (!apiKey) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "market_metrics", "coinmarketcap:global_market_metrics", "api_key_missing");
          continue;
        }
        try {
          const payload = await fetchJson<unknown>(
            "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest?convert=USD",
            toolCalls,
            "coinmarketcap:global_market_metrics",
            sourceReferences,
            {
              "X-CMC_PRO_API_KEY": apiKey,
            },
          );
          const metrics = parseCoinMarketCapGlobalMetrics(payload);
          if (metrics.usable) {
            recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
            candidates.push({
              provider,
              sourceId: "coinmarketcap:global_market_metrics",
              metrics,
            });
            addSelection(
              sourceSelection,
              "market_metrics",
              "coinmarketcap:global_market_metrics",
              `candidate_collected_provider_order:${globalMetricsOrder.join(">")};credibility:${getSourceCredibility("market_metrics", provider)}`,
            );
            continue;
          }
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "market_metrics", "coinmarketcap:global_market_metrics", "unusable_payload");
        } catch (error) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(
            sourceSelection,
            "market_metrics",
            "coinmarketcap:global_market_metrics",
            error instanceof Error ? error.message : "coinmarketcap_global_fetch_failed",
          );
        }
        continue;
      }

      if (provider === "coinlore") {
        const startedAt = Date.now();
        try {
          const metrics = await fetchCoinLoreGlobalMetrics(toolCalls, sourceReferences);
          if (metrics.usable) {
            recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
            candidates.push({
              provider,
              sourceId: "coinlore:global_market_metrics",
              metrics,
            });
            addSelection(
              sourceSelection,
              "market_metrics",
              "coinlore:global_market_metrics",
              `candidate_collected_provider_order:${globalMetricsOrder.join(">")};credibility:${getSourceCredibility("market_metrics", provider)}`,
            );
            continue;
          }
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "market_metrics", "coinlore:global_market_metrics", "unusable_payload");
        } catch (error) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(
            sourceSelection,
            "market_metrics",
            "coinlore:global_market_metrics",
            error instanceof Error ? error.message : "coinlore_global_fetch_failed",
          );
        }
        continue;
      }

      recordSourceOutcome("market_metrics", provider, false, 0);
      addRejection(sourceSelection, "market_metrics", provider, "unsupported_provider");
    }

    if (candidates.length === 0) {
      limitations.add("missing_global_market_metrics");
      return {
        totalVolume24hUsd: 0,
        marketCapChangePct24h: 0,
        stablecoinDominancePct: 0,
        usable: false,
      };
    }

    if (candidates.length > 1) {
      addSelection(
        sourceSelection,
        "market_metrics",
        "aggregated:global_market_metrics",
        `composite_global_metrics_sources:${candidates.map((candidate) => candidate.provider).join(">")};source_count:${candidates.length}`,
      );
    }

    const aggregated = aggregateGlobalMetricsCandidates(candidates);
    if (aggregated.stablecoinDominancePct <= 0) {
      limitations.add("stablecoin_dominance_unavailable_from_selected_global_source");
    }
    return aggregated;
  };

  const resolveMarketBreadth = async (): Promise<MarketBreadthMetrics> => {
    const breadthCandidates: MarketBreadthCandidate[] = [];
    const breadthOrder = buildProviderOrder("market_metrics", ["coingecko", "coinlore"], []).filter(
      (provider) => provider === "coingecko" || provider === "coinlore",
    );

    for (const provider of breadthOrder) {
      if (provider === "coingecko") {
        const startedAt = Date.now();
        try {
          const payload = await fetchJson<unknown>(
            "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=60&page=1&price_change_percentage=24h",
            toolCalls,
            "coingecko:top60_market_breadth",
            sourceReferences,
          );
          const metrics = parseCoinGeckoBreadthMetrics(payload);
          if (metrics.usable) {
            recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
            breadthCandidates.push({
              provider,
              sourceId: "coingecko:top60_market_breadth",
              metrics,
            });
            addSelection(
              sourceSelection,
              "market_metrics",
              "coingecko:top60_market_breadth",
              `market_breadth_candidate_collected;credibility:${getSourceCredibility("market_metrics", provider)}`,
            );
          } else {
            recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
            addRejection(sourceSelection, "market_metrics", "coingecko:top60_market_breadth", "unusable_payload");
          }
        } catch (error) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(
            sourceSelection,
            "market_metrics",
            "coingecko:top60_market_breadth",
            error instanceof Error ? error.message : "coingecko_market_breadth_fetch_failed",
          );
        }
        continue;
      }

      if (provider === "coinlore") {
        const startedAt = Date.now();
        try {
          const payload = await fetchJson<unknown>(
            "https://api.coinlore.net/api/tickers/?start=0&limit=60",
            toolCalls,
            "coinlore:top60_market_breadth",
            sourceReferences,
          );
          const metrics = parseCoinLoreBreadthMetrics(payload);
          if (metrics.usable) {
            recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
            breadthCandidates.push({
              provider,
              sourceId: "coinlore:top60_market_breadth",
              metrics,
            });
            addSelection(
              sourceSelection,
              "market_metrics",
              "coinlore:top60_market_breadth",
              `market_breadth_candidate_collected;credibility:${getSourceCredibility("market_metrics", provider)}`,
            );
          } else {
            recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
            addRejection(sourceSelection, "market_metrics", "coinlore:top60_market_breadth", "unusable_payload");
          }
        } catch (error) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          addRejection(
            sourceSelection,
            "market_metrics",
            "coinlore:top60_market_breadth",
            error instanceof Error ? error.message : "coinlore_market_breadth_fetch_failed",
          );
        }
        continue;
      }

      continue;
    }

    if (breadthCandidates.length === 0) {
      limitations.add("missing_market_breadth_data");
      return { positiveRatio: 0, averageAbsMove24h: 0, assetCount: 0, usable: false };
    }

    if (breadthCandidates.length > 1) {
      addSelection(
        sourceSelection,
        "market_metrics",
        "aggregated:market_breadth",
        `composite_market_breadth_sources:${breadthCandidates.map((candidate) => candidate.provider).join(">")};source_count:${breadthCandidates.length}`,
      );
    }

    return aggregateMarketBreadthCandidates(breadthCandidates);
  };

  const [btcSeries, ethSeries, globalMetrics, marketBreadth] = await Promise.all([
    resolveSeries("btc"),
    resolveSeries("eth"),
    resolveGlobalMetrics(),
    resolveMarketBreadth(),
  ]);

  const hasAnyMarketSeries =
    btcSeries.prices.length > 0 ||
    ethSeries.prices.length > 0 ||
    btcSeries.volumes.length > 0 ||
    ethSeries.volumes.length > 0;

  if (!hasAnyMarketSeries && !marketBreadth.usable) {
    limitations.add("missing_global_market_metrics");
    limitations.add("missing_macro_market_data");
    return fallbackSharedMarketData();
  }

  const result: SharedMarketData = {
    btcPrices: btcSeries.prices,
    ethPrices: ethSeries.prices,
    btcVolumes: btcSeries.volumes,
    ethVolumes: ethSeries.volumes,
    totalVolume24hUsd: globalMetrics.totalVolume24hUsd,
    marketCapChangePct24h: globalMetrics.marketCapChangePct24h,
    stablecoinDominancePct: globalMetrics.stablecoinDominancePct,
    marketBreadthPositiveRatio: marketBreadth.positiveRatio,
    marketBreadthAbsMove24h: marketBreadth.averageAbsMove24h,
    marketBreadthAssetCount: marketBreadth.assetCount,
    breadthMissing: !marketBreadth.usable,
    missing: !hasAnyMarketSeries && !marketBreadth.usable,
  };

  if (!marketBreadth.usable) {
    limitations.add("missing_market_breadth_data");
  }

  return result;
}

function deriveVolatilityState(currentVolatility: number, volatilityZScore: number, marketCapChangePct: number): VolatilityState {
  let pressure = 0;

  if (currentVolatility >= 8) pressure += 3;
  else if (currentVolatility >= 5) pressure += 2;
  else if (currentVolatility >= 2.5) pressure += 1;

  if (volatilityZScore >= 1.75) pressure += 2;
  else if (volatilityZScore >= 0.75) pressure += 1;

  if (marketCapChangePct <= -4) pressure += 1;

  if (pressure >= 4) return "extreme";
  if (pressure >= 2) return "elevated";
  if (pressure >= 1) return "moderate";
  return "low";
}

function deriveLiquidityState(volumeDeviationZScore: number, avgSpreadPct: number, stablecoinDominancePct: number): LiquidityState {
  if (volumeDeviationZScore < -0.75 || avgSpreadPct > 0.25 || stablecoinDominancePct > 12) return "weak";
  if (volumeDeviationZScore > 0.5 && avgSpreadPct < 0.12 && stablecoinDominancePct < 9) return "strong";
  return "stable";
}

async function collectVolatilityDomain(
  sharedData: SharedMarketData,
  limitations: Set<string>,
): Promise<VolatilityDomain> {
  try {
    const btcCurrentVol = sharedData.btcPrices.length >= 25
      ? computeVolatilityPercent(sharedData.btcPrices.slice(-25))
      : Number.NaN;
    const ethCurrentVol = sharedData.ethPrices.length >= 25
      ? computeVolatilityPercent(sharedData.ethPrices.slice(-25))
      : Number.NaN;
    const btcSeries = computeDailyWindowVolatilitySeries(sharedData.btcPrices, 8);
    const ethSeries = computeDailyWindowVolatilitySeries(sharedData.ethPrices, 8);
    const combinedSeries =
      btcSeries.length > 0 && ethSeries.length > 0
        ? combineSeries(btcSeries, ethSeries)
        : btcSeries.length > 0
          ? btcSeries
          : ethSeries;
    const breadthVolatilityProxy = sharedData.marketBreadthAbsMove24h > 0
      ? sharedData.marketBreadthAbsMove24h / 2.5
      : Number.NaN;
    const currentVolCandidates = [btcCurrentVol, ethCurrentVol, breadthVolatilityProxy]
      .filter((value) => Number.isFinite(value));
    const currentCombinedVol =
      combinedSeries[combinedSeries.length - 1] ??
      (currentVolCandidates.length > 0 ? average(currentVolCandidates) : Number.NaN);
    if (!Number.isFinite(currentCombinedVol)) {
      throw new Error("Volatility series unavailable.");
    }
    const historicalCombinedVol = combinedSeries.slice(0, -1);
    const volatilityZ = zScore(currentCombinedVol, historicalCombinedVol);
    const breadthMarketAdjustment = sharedData.marketBreadthPositiveRatio > 0
      ? (sharedData.marketBreadthPositiveRatio - 0.5) * 6
      : 0;
    const adjustedMarketCapChange = sharedData.marketCapChangePct24h + breadthMarketAdjustment;

    return {
      btcVolatility24h: round(Number.isFinite(btcCurrentVol) ? btcCurrentVol : 0, 6),
      ethVolatility24h: round(Number.isFinite(ethCurrentVol) ? ethCurrentVol : 0, 6),
      volatilityZScore: round(volatilityZ, 6),
      volatilityState: deriveVolatilityState(currentCombinedVol, volatilityZ, adjustedMarketCapChange),
      missing: false,
    };
  } catch {
    limitations.add("missing_volatility_domain");
    return fallbackVolatilityDomain();
  }
}

async function fetchAverageSpreadPercent(
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  limitations: Set<string>,
  sourceSelection: Record<SelectionDomain, SourceSelectionRecord>,
): Promise<number> {
  const spreads: number[] = [];

  const parseSpread = (
    sourceId: string,
    bidRaw: unknown,
    askRaw: unknown,
  ) => {
    const bid = parsePositiveNumber(bidRaw);
    const ask = parsePositiveNumber(askRaw);
    const mid = (bid + ask) / 2;
    if (bid <= 0 || ask <= 0 || mid <= 0) {
      addRejection(sourceSelection, "liquidity", sourceId, "invalid_bid_ask_values");
      return;
    }
    spreads.push(((ask - bid) / mid) * 100);
    addSelection(sourceSelection, "liquidity", sourceId, "used_for_orderbook_spread");
  };

  try {
    const coinbaseBtc = await fetchJson<Record<string, unknown>>(
      "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
      toolCalls,
      "coinbase_exchange:btc_usd_ticker",
      sourceReferences,
    );
    parseSpread("coinbase_exchange:btc_usd_ticker", coinbaseBtc.bid, coinbaseBtc.ask);
  } catch (error) {
    addRejection(
      sourceSelection,
      "liquidity",
      "coinbase_exchange:btc_usd_ticker",
      error instanceof Error ? error.message : "coinbase_btc_ticker_failed",
    );
  }

  try {
    const coinbaseEth = await fetchJson<Record<string, unknown>>(
      "https://api.exchange.coinbase.com/products/ETH-USD/ticker",
      toolCalls,
      "coinbase_exchange:eth_usd_ticker",
      sourceReferences,
    );
    parseSpread("coinbase_exchange:eth_usd_ticker", coinbaseEth.bid, coinbaseEth.ask);
  } catch (error) {
    addRejection(
      sourceSelection,
      "liquidity",
      "coinbase_exchange:eth_usd_ticker",
      error instanceof Error ? error.message : "coinbase_eth_ticker_failed",
    );
  }

  try {
    const kraken = await fetchJson<Record<string, unknown>>(
      "https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHUSD",
      toolCalls,
      "kraken:xbtusd_ethusd_ticker",
      sourceReferences,
    );
    const krakenResult = (kraken.result ?? {}) as Record<string, unknown>;
    let sawTicker = false;
    for (const [symbol, ticker] of Object.entries(krakenResult)) {
      if (!symbol.includes("USD") || (typeof ticker !== "object" || ticker === null)) continue;
      sawTicker = true;
      const bid = Array.isArray((ticker as { b?: unknown }).b) ? (ticker as { b: unknown[] }).b[0] : undefined;
      const ask = Array.isArray((ticker as { a?: unknown }).a) ? (ticker as { a: unknown[] }).a[0] : undefined;
      parseSpread(`kraken:${symbol.toLowerCase()}_ticker`, bid, ask);
    }
    if (!sawTicker) {
      addRejection(sourceSelection, "liquidity", "kraken:xbtusd_ethusd_ticker", "empty_ticker_payload");
    }
  } catch (error) {
    addRejection(
      sourceSelection,
      "liquidity",
      "kraken:xbtusd_ethusd_ticker",
      error instanceof Error ? error.message : "kraken_ticker_failed",
    );
  }

  if (spreads.length === 0) {
    limitations.add("missing_orderbook_spread_data");
    return 0;
  }
  return round(average(spreads), 6);
}

async function collectLiquidityDomain(
  sharedData: SharedMarketData,
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  limitations: Set<string>,
  sourceSelection: Record<SelectionDomain, SourceSelectionRecord>,
): Promise<LiquidityDomain> {
  try {
    const btcVolumeSeries = computeDailyWindowVolumeSeries(sharedData.btcVolumes, 8);
    const ethVolumeSeries = computeDailyWindowVolumeSeries(sharedData.ethVolumes, 8);
    const combinedVolumeSeries =
      btcVolumeSeries.length > 0 && ethVolumeSeries.length > 0
        ? combineSeries(btcVolumeSeries, ethVolumeSeries)
        : btcVolumeSeries.length > 0
          ? btcVolumeSeries
          : ethVolumeSeries;
    const currentCombinedVolume = combinedVolumeSeries[combinedVolumeSeries.length - 1] ?? Number.NaN;
    const historicalCombinedVolumes = combinedVolumeSeries.slice(0, -1);
    const volumeDeviation = Number.isFinite(currentCombinedVolume)
      ? zScore(currentCombinedVolume, historicalCombinedVolumes)
      : 0;
    const avgSpread = await fetchAverageSpreadPercent(
      toolCalls,
      sourceReferences,
      limitations,
      sourceSelection,
    );
    const hasDomainSignal =
      Number.isFinite(currentCombinedVolume) ||
      avgSpread > 0 ||
      sharedData.totalVolume24hUsd > 0 ||
      sharedData.stablecoinDominancePct > 0;
    if (!hasDomainSignal) {
      throw new Error("Liquidity domain unavailable.");
    }

    return {
      totalVolume24h: round(sharedData.totalVolume24hUsd, 6),
      volumeDeviationZScore: round(volumeDeviation, 6),
      avgSpread: round(avgSpread, 6),
      stablecoinDominance: round(sharedData.stablecoinDominancePct, 6),
      liquidityState: deriveLiquidityState(volumeDeviation, avgSpread, sharedData.stablecoinDominancePct),
      missing: false,
    };
  } catch {
    limitations.add("missing_liquidity_domain");
    return fallbackLiquidityDomain();
  }
}

function normalizeNewsRows(payload: { Data?: unknown[] }): NewsItem[] {
  const rows = Array.isArray(payload.Data) ? payload.Data : [];
  const result: NewsItem[] = [];

  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const data = row as Record<string, unknown>;
    const publishedOn = Number(data.published_on);
    if (!Number.isFinite(publishedOn)) continue;

    result.push({
      title: String(data.title ?? ""),
      body: String(data.body ?? ""),
      source: String(data.source ?? data.source_info ?? "unknown"),
      publishedOn: Math.floor(publishedOn),
      upvotes: parsePositiveNumber(data.upvotes),
      downvotes: parsePositiveNumber(data.downvotes),
    });
  }

  return result;
}

function splitNewsWindows(rows: NewsItem[]): { recentRows: NewsItem[]; historicalRows: NewsItem[] } {
  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  const currentWindowStart = now - dayMs;
  const historicalWindowStart = now - 7 * dayMs;

  const recentRows: NewsItem[] = [];
  const historicalRows: NewsItem[] = [];

  for (const row of rows) {
    const publishedMs = row.publishedOn * 1000;
    if (publishedMs >= currentWindowStart) {
      recentRows.push(row);
    } else if (publishedMs >= historicalWindowStart) {
      historicalRows.push(row);
    }
  }

  if (recentRows.length === 0 && rows.length > 0) {
    recentRows.push(...rows.slice(0, Math.min(rows.length, 40)));
  }

  return { recentRows, historicalRows };
}

function deriveSentimentFromNewsRows(
  recentRows: NewsItem[],
  historicalRows: NewsItem[],
): Omit<SentimentDomain, "newsItems" | "missing"> | null {
  const headlineCount = recentRows.length;
  if (headlineCount === 0) {
    return null;
  }

  const sentimentScores = recentRows.map((row) => computeLexiconSentimentScore(`${row.title} ${row.body}`.trim()));
  const aggregateSentimentScore = clamp(average(sentimentScores), -1, 1);
  const sentimentDispersion = standardDeviation(sentimentScores);

  const engagementValues = recentRows.map((row) => row.upvotes - row.downvotes);
  const historicalEngagementValues = historicalRows.map((row) => row.upvotes - row.downvotes);
  const recentEngagement = average(engagementValues);
  const hasEngagementHistory = historicalEngagementValues.length > 2;
  const engagementDeviation = hasEngagementHistory
    ? zScore(recentEngagement, historicalEngagementValues)
    : recentEngagement === 0
      ? 0
      : Math.sign(recentEngagement);

  const normalizedEngagement = clamp(engagementDeviation / 3, -1, 1);
  const sentimentDirection = hasEngagementHistory
    ? clamp(aggregateSentimentScore * 0.75 + normalizedEngagement * 0.25, -1, 1)
    : aggregateSentimentScore;
  const sentimentAlignment = hasEngagementHistory
    ? clamp(1 - Math.abs(aggregateSentimentScore - normalizedEngagement), 0, 1)
    : clamp(1 - sentimentDispersion, 0, 1);
  const publicEchoStrength = clamp(
    clamp(headlineCount / 40, 0, 1) * 0.6 +
      clamp(Math.abs(engagementDeviation) / 3, 0, 1) * (hasEngagementHistory ? 0.4 : 0.2) +
      clamp(Math.abs(aggregateSentimentScore), 0, 1) * (hasEngagementHistory ? 0 : 0.2),
    0,
    1,
  );

  return {
    headlineCount,
    aggregateSentimentScore: round(aggregateSentimentScore, 6),
    engagementDeviation: round(clamp(engagementDeviation, -4, 4), 6),
    sentimentDirection: round(sentimentDirection, 6),
    sentimentAlignment: round(sentimentAlignment, 6),
    publicEchoStrength: round(publicEchoStrength, 6),
    sourceCount: 1,
    sourceConsensus: 1,
    fearGreedIncluded: false,
    fearGreedIndex: null,
  };
}

function aggregateSentimentCandidates(
  candidates: SentimentSignalCandidate[],
  limitations: Set<string>,
): SentimentDomain {
  if (candidates.length === 0) {
    return fallbackSentimentDomain();
  }

  const weights = normalizeWeights(
    candidates.map((candidate) => {
      const credibility = clamp(getSourceCredibility("sentiment", candidate.provider), 0.05, 1);
      const providerWeight = SENTIMENT_PROVIDER_WEIGHT[candidate.provider] ?? 0.75;
      return credibility * providerWeight;
    }),
  );

  const sourceDirections = candidates.map((candidate) => candidate.signal.sentimentDirection);
  const directionStd = standardDeviation(sourceDirections);
  const sourceConsensus = clamp(1 - directionStd / 0.8, 0, 1);
  const sourceCoverage = clamp(candidates.length / 3, 0, 1);
  const fearGreedIncluded = candidates.some((candidate) => candidate.provider === "alternative_me");
  const fearGreedCandidates = candidates.filter(
    (candidate): candidate is SentimentSignalCandidate & { signal: Omit<SentimentDomain, "missing"> & { fearGreedIndex: number } } =>
      typeof candidate.signal.fearGreedIndex === "number" && Number.isFinite(candidate.signal.fearGreedIndex),
  );
  const fearGreedIndex = fearGreedCandidates.length > 0
    ? round(
        clamp(
          weightedAverage(
            fearGreedCandidates.map((candidate) => candidate.signal.fearGreedIndex),
            normalizeWeights(
              fearGreedCandidates.map((candidate) => clamp(getSourceCredibility("sentiment", candidate.provider), 0.05, 1)),
            ),
          ),
          0,
          100,
        ),
        3,
      )
    : null;
  if (!fearGreedIncluded) {
    limitations.add("fear_and_greed_signal_unavailable");
  }

  const weightedAlignment = weightedAverage(
    candidates.map((candidate) => candidate.signal.sentimentAlignment),
    weights,
  );
  const weightedEcho = weightedAverage(
    candidates.map((candidate) => candidate.signal.publicEchoStrength),
    weights,
  );
  const sentimentDirection = weightedAverage(
    candidates.map((candidate) => candidate.signal.sentimentDirection),
    weights,
  );
  const aggregateSentimentScore = weightedAverage(
    candidates.map((candidate) => candidate.signal.aggregateSentimentScore),
    weights,
  );

  const newsCandidates = candidates.filter((candidate) => candidate.provider !== "alternative_me");
  if (newsCandidates.length === 0) {
    limitations.add("sentiment_domain_index_proxy_without_headline_engagement");
  }
  const headlineCount = newsCandidates.length > 0
    ? newsCandidates.reduce((sum, candidate) => sum + candidate.signal.headlineCount, 0)
    : 1;
  const engagementDeviation = newsCandidates.length > 0
    ? weightedAverage(
        newsCandidates.map((candidate) => candidate.signal.engagementDeviation),
        normalizeWeights(
          newsCandidates.map((candidate) => clamp(getSourceCredibility("sentiment", candidate.provider), 0.05, 1)),
        ),
      )
    : 0;

  const newsItems = candidates
    .flatMap((candidate) => candidate.signal.newsItems)
    .sort((left, right) => right.publishedOn - left.publishedOn);
  const dedupedNewsItems: NewsItem[] = [];
  const seenTitles = new Set<string>();
  for (const item of newsItems) {
    const key = item.title.trim().toLowerCase();
    if (!key || seenTitles.has(key)) continue;
    seenTitles.add(key);
    dedupedNewsItems.push(item);
    if (dedupedNewsItems.length >= 80) break;
  }

  return {
    headlineCount: Math.max(0, Math.floor(headlineCount)),
    aggregateSentimentScore: round(clamp(aggregateSentimentScore, -1, 1), 6),
    engagementDeviation: round(clamp(engagementDeviation, -4, 4), 6),
    sentimentDirection: round(clamp(sentimentDirection, -1, 1), 6),
    sentimentAlignment: round(
      clamp(weightedAlignment * 0.55 + sourceConsensus * 0.3 + sourceCoverage * 0.15, 0, 1),
      6,
    ),
    publicEchoStrength: round(clamp(weightedEcho * 0.75 + sourceCoverage * 0.25, 0, 1), 6),
    sourceCount: candidates.length,
    sourceConsensus: round(sourceConsensus, 6),
    fearGreedIncluded,
    fearGreedIndex,
    newsItems: dedupedNewsItems,
    missing: false,
  };
}

async function collectSentimentDomain(
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  limitations: Set<string>,
  sourceSelection: Record<SelectionDomain, SourceSelectionRecord>,
): Promise<SentimentDomain> {
  const configuredSentimentOrder = parseOrderedSources(
    "PHASE1_SENTIMENT_SOURCE_ORDER",
    DEFAULT_SENTIMENT_SOURCE_ORDER,
  );
  const sentimentDiscoveryPool = parseOrderedSources(
    "PHASE1_DISCOVERY_SENTIMENT_POOL",
    DEFAULT_SENTIMENT_DISCOVERY_POOL,
  );
  const sentimentOrder = buildProviderOrder("sentiment", configuredSentimentOrder, sentimentDiscoveryPool);
  const sentimentSourceTarget = Math.min(
    4,
    Math.max(1, readPositiveIntEnv("PHASE1_SENTIMENT_SOURCE_TARGET", DEFAULT_PHASE1_SENTIMENT_SOURCE_TARGET)),
  );
  const candidates: SentimentSignalCandidate[] = [];

  for (const provider of sentimentOrder) {
    if (candidates.length >= sentimentSourceTarget && candidates.some((candidate) => candidate.provider === "alternative_me")) {
      break;
    }

    if (provider === "cryptocompare") {
      const startedAt = Date.now();
      try {
        const payload = await fetchJson<{ Data?: unknown[] }>(
          "https://min-api.cryptocompare.com/data/v2/news/?lang=EN",
          toolCalls,
          "cryptocompare:news_last_24h",
          sourceReferences,
        );
        const rows = normalizeNewsRows(payload);
        const { recentRows, historicalRows } = splitNewsWindows(rows);
        const sentiment = deriveSentimentFromNewsRows(recentRows, historicalRows);
        if (!sentiment) {
          recordSourceOutcome("sentiment", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "sentiment", "cryptocompare:news_last_24h", "no_recent_headlines");
          continue;
        }

        recordSourceOutcome("sentiment", provider, true, Date.now() - startedAt);
        candidates.push({
          provider,
          sourceId: "cryptocompare:news_last_24h",
          signal: {
            ...sentiment,
            sourceCount: 1,
            sourceConsensus: 1,
            fearGreedIncluded: false,
            newsItems: recentRows,
          },
        });
        addSelection(
          sourceSelection,
          "sentiment",
          "cryptocompare:news_last_24h",
          `sentiment_candidate_collected_order:${sentimentOrder.join(">")};credibility:${getSourceCredibility("sentiment", provider)}`,
        );
      } catch (error) {
        recordSourceOutcome("sentiment", provider, false, Date.now() - startedAt);
        addRejection(
          sourceSelection,
          "sentiment",
          "cryptocompare:news_last_24h",
          error instanceof Error ? error.message : "cryptocompare_fetch_failed",
        );
      }
      continue;
    }

    if (provider === "alternative_me") {
      const startedAt = Date.now();
      try {
        const fearGreedSignal = await fetchAlternativeSentimentIndex(toolCalls, sourceReferences);
        recordSourceOutcome("sentiment", provider, true, Date.now() - startedAt);
        candidates.push({
          provider,
          sourceId: "alternative_me:fng_latest",
          signal: {
            headlineCount: 1,
            aggregateSentimentScore: round(fearGreedSignal.normalized, 6),
            engagementDeviation: 0,
            sentimentDirection: round(fearGreedSignal.normalized, 6),
            sentimentAlignment: round(clamp(1 - Math.abs(fearGreedSignal.normalized), 0, 1), 6),
            publicEchoStrength: 0.2,
            sourceCount: 1,
            sourceConsensus: 1,
            fearGreedIncluded: true,
            fearGreedIndex: fearGreedSignal.index,
            newsItems: [],
          },
        });
        addSelection(
          sourceSelection,
          "sentiment",
          "alternative_me:fng_latest",
          `fear_greed_candidate_collected_order:${sentimentOrder.join(">")};credibility:${getSourceCredibility("sentiment", provider)}`,
        );
      } catch (error) {
        recordSourceOutcome("sentiment", provider, false, Date.now() - startedAt);
        addRejection(
          sourceSelection,
          "sentiment",
          "alternative_me:fng_latest",
          error instanceof Error ? error.message : "alternative_me_fetch_failed",
        );
      }
      continue;
    }

    if (provider === "coindesk_rss") {
      const startedAt = Date.now();
      try {
        const xml = await fetchText(
          "https://www.coindesk.com/arc/outboundfeeds/rss/",
          toolCalls,
          "coindesk:rss_latest",
          sourceReferences,
        );
        const rows = parseRssNewsItems(xml, "coindesk");
        const { recentRows, historicalRows } = splitNewsWindows(rows);
        const sentiment = deriveSentimentFromNewsRows(recentRows, historicalRows);
        if (!sentiment) {
          recordSourceOutcome("sentiment", provider, false, Date.now() - startedAt);
          addRejection(sourceSelection, "sentiment", "coindesk:rss_latest", "no_headlines_in_feed");
          continue;
        }

        recordSourceOutcome("sentiment", provider, true, Date.now() - startedAt);
        candidates.push({
          provider,
          sourceId: "coindesk:rss_latest",
          signal: {
            ...sentiment,
            sourceCount: 1,
            sourceConsensus: 1,
            fearGreedIncluded: false,
            newsItems: recentRows,
          },
        });
        addSelection(
          sourceSelection,
          "sentiment",
          "coindesk:rss_latest",
          `sentiment_candidate_collected_order:${sentimentOrder.join(">")};credibility:${getSourceCredibility("sentiment", provider)}`,
        );
      } catch (error) {
        recordSourceOutcome("sentiment", provider, false, Date.now() - startedAt);
        addRejection(
          sourceSelection,
          "sentiment",
          "coindesk:rss_latest",
          error instanceof Error ? error.message : "coindesk_rss_fetch_failed",
        );
      }
      continue;
    }

    recordSourceOutcome("sentiment", provider, false, 0);
    addRejection(sourceSelection, "sentiment", provider, "unsupported_provider");
  }

  if (candidates.length === 0) {
    limitations.add("missing_sentiment_domain");
    return fallbackSentimentDomain();
  }

  if (candidates.length > 1) {
    addSelection(
      sourceSelection,
      "sentiment",
      "aggregated:sentiment",
      `composite_sentiment_sources:${candidates.map((candidate) => candidate.provider).join(">")};source_count:${candidates.length}`,
    );
  }

  return aggregateSentimentCandidates(candidates, limitations);
}

function deriveRiskAppetite(
  volatilityState: VolatilityState,
  liquidityState: LiquidityState,
  sentimentDirection: number,
  marketBreadthPositiveRatio: number,
  marketBreadthAbsMove24h: number,
): RiskAppetite {
  if (
    volatilityState === "extreme" ||
    (volatilityState === "elevated" && liquidityState === "weak") ||
    (liquidityState === "weak" && sentimentDirection <= -0.2) ||
    (marketBreadthPositiveRatio > 0 && marketBreadthPositiveRatio <= 0.38 && sentimentDirection <= -0.05)
  ) {
    return "defensive";
  }

  if (
    (volatilityState === "low" || volatilityState === "moderate") &&
    (liquidityState === "strong" || liquidityState === "stable") &&
    sentimentDirection >= 0.08 &&
    marketBreadthPositiveRatio >= 0.62 &&
    marketBreadthAbsMove24h >= 3
  ) {
    return "expansionary";
  }

  return "neutral";
}

function deriveConfidenceAndUncertainty(
  volatility: VolatilityDomain,
  liquidity: LiquidityDomain,
  sentiment: SentimentDomain,
  sharedData: SharedMarketData,
): AlignmentDomain {
  const domainAvailable = [!volatility.missing, !liquidity.missing, !sentiment.missing];
  const availableCount = domainAvailable.filter(Boolean).length;
  const missingRatio = 1 - availableCount / 3;

  const volatilitySignal = volatility.volatilityState === "low" ? 1 : volatility.volatilityState === "moderate" ? 0 : -1;
  const liquiditySignal = liquidity.liquidityState === "strong" ? 1 : liquidity.liquidityState === "stable" ? 0 : -1;
  const sentimentSignal = sentiment.sentimentDirection >= 0.15 ? 1 : sentiment.sentimentDirection <= -0.15 ? -1 : 0;
  const breadthSignal =
    sharedData.marketBreadthPositiveRatio >= 0.62
      ? 1
      : sharedData.marketBreadthPositiveRatio <= 0.38 && sharedData.marketBreadthPositiveRatio > 0
        ? -1
        : 0;
  const directionalSignals = [volatilitySignal, liquiditySignal, sentimentSignal, breadthSignal];
  const nonZeroSignals = directionalSignals.filter((signal) => signal !== 0);
  const hasPositiveSignal = nonZeroSignals.some((signal) => signal > 0);
  const hasNegativeSignal = nonZeroSignals.some((signal) => signal < 0);
  const directionalConflict = hasPositiveSignal && hasNegativeSignal;
  const sourceDiversity = clamp(sentiment.sourceCount / 3, 0, 1);
  const sentimentSourceConsensus = clamp(sentiment.sourceConsensus, 0, 1);
  const breadthCoverage = clamp(sharedData.marketBreadthAssetCount / 60, 0, 1);

  let alignmentScore = 0.5;
  if (nonZeroSignals.length > 1) {
    let pairMatches = 0;
    let pairCount = 0;
    for (let index = 0; index < nonZeroSignals.length; index += 1) {
      for (let other = index + 1; other < nonZeroSignals.length; other += 1) {
        pairCount += 1;
        if (nonZeroSignals[index] === nonZeroSignals[other]) pairMatches += 1;
      }
    }
    alignmentScore = pairCount > 0 ? pairMatches / pairCount : 0.5;
  }

  const neutralityFactor = clamp(
    ((volatility.volatilityState === "moderate" ? 1 : 0) +
      (liquidity.liquidityState === "stable" ? 1 : 0) +
      (1 - Math.min(1, Math.abs(sentiment.sentimentDirection) / 0.35))) / 3,
    0,
    1,
  );

  let confidence = 0.25 + (availableCount / 3) * 0.35 + alignmentScore * 0.3 + Math.abs(sentiment.sentimentDirection) * 0.1;
  confidence -= missingRatio * 0.2;
  confidence += sourceDiversity * 0.08;
  confidence += sentimentSourceConsensus * 0.07;
  confidence += breadthCoverage * 0.05;
  confidence -= sentiment.sentimentAlignment < 0.5 ? 0.07 : 0;
  confidence -= Math.abs(sentiment.sentimentDirection) < 0.08 ? 0.05 : 0;
  confidence -= sentiment.fearGreedIncluded ? 0 : 0.05;
  confidence -= directionalConflict ? 0.1 : 0;

  let uncertainty = 0.3 + missingRatio * 0.35 + (1 - alignmentScore) * 0.2 + neutralityFactor * 0.2;
  uncertainty -= confidence * 0.15;
  uncertainty += (1 - sourceDiversity) * 0.08;
  uncertainty += (1 - sentimentSourceConsensus) * 0.08;
  uncertainty += (1 - breadthCoverage) * 0.05;
  uncertainty += sentiment.fearGreedIncluded ? 0 : 0.05;
  uncertainty += directionalConflict ? 0.1 : 0;

  confidence = clamp(confidence, 0, 1);
  uncertainty = clamp(uncertainty, 0, 1);

  const confidenceCap = clamp(
    0.55 + alignmentScore * 0.3 + sourceDiversity * 0.1 + sentimentSourceConsensus * 0.05 + breadthCoverage * 0.05,
    0,
    0.9,
  );
  confidence = Math.min(confidence, confidenceCap);

  if (volatility.volatilityState === "low" && liquidity.liquidityState === "weak") {
    confidence = Math.min(confidence, 0.82);
    uncertainty = Math.max(uncertainty, 0.42);
  }
  if (liquidity.liquidityState === "weak" && sentiment.sentimentDirection <= -0.15) {
    confidence = Math.min(confidence, 0.84);
    uncertainty = Math.max(uncertainty, 0.35);
  }
  if (liquidity.liquidityState === "weak" && volatility.volatilityState === "moderate") {
    confidence = Math.min(confidence, 0.86);
    uncertainty = Math.max(uncertainty, 0.33);
  }

  if (availableCount === 0) {
    confidence = 0.2;
    uncertainty = 0.9;
  }

  return {
    riskAppetite: deriveRiskAppetite(
      volatility.volatilityState,
      liquidity.liquidityState,
      sentiment.sentimentDirection,
      sharedData.marketBreadthPositiveRatio,
      sharedData.marketBreadthAbsMove24h,
    ),
    confidence: round(confidence, 6),
    uncertainty: round(uncertainty, 6),
  };
}

function applyConsistencyCalibration(
  alignment: AlignmentDomain,
  sentiment: SentimentDomain,
  assumptions: Set<string>,
  limitations: Set<string>,
): AlignmentDomain {
  let confidence = alignment.confidence;
  let uncertainty = alignment.uncertainty;
  const calibrationFlags: string[] = [];

  if (Math.abs(sentiment.sentimentDirection) < 0.08 && sentiment.sentimentAlignment < 0.55 && confidence > 0.62) {
    confidence = 0.62;
    uncertainty = Math.max(uncertainty, 0.52);
    calibrationFlags.push("low_sentiment_signal_confidence_cap_applied");
  }

  if (sentiment.sourceCount < 2 && confidence > 0.68) {
    confidence = 0.68;
    uncertainty = Math.max(uncertainty, 0.56);
    calibrationFlags.push("single_source_sentiment_confidence_cap_applied");
  }

  if (sentiment.sourceConsensus < 0.45) {
    confidence = round(clamp(confidence * 0.92, 0, 1), 6);
    uncertainty = round(clamp(uncertainty + 0.07, 0, 1), 6);
    calibrationFlags.push("low_sentiment_source_consensus_penalty_applied");
  }

  if (!sentiment.fearGreedIncluded) {
    confidence = Math.min(confidence, 0.72);
    uncertainty = Math.max(uncertainty, 0.58);
    calibrationFlags.push("fear_greed_missing_consistency_penalty_applied");
    limitations.add("fear_and_greed_signal_unavailable");
  }
  if (alignment.riskAppetite === "defensive" && sentiment.sentimentDirection <= -0.15) {
    confidence = Math.min(confidence, 0.84);
    uncertainty = Math.max(uncertainty, 0.36);
    calibrationFlags.push("defensive_regime_confidence_cap_applied");
  }
  if (alignment.riskAppetite === "defensive" && sentiment.sentimentAlignment < 0.6) {
    confidence = Math.min(confidence, 0.82);
    uncertainty = Math.max(uncertainty, 0.4);
    calibrationFlags.push("defensive_low_alignment_uncertainty_floor_applied");
  }
  if (confidence > 0.85 && uncertainty < 0.3) {
    uncertainty = 0.3;
    calibrationFlags.push("confidence_uncertainty_coherence_floor_applied");
  }

  if (calibrationFlags.length > 0) {
    for (const flag of calibrationFlags) {
      assumptions.add(`consistency_pass:${flag}`);
    }
  }

  return {
    riskAppetite: alignment.riskAppetite,
    confidence: round(clamp(confidence, 0, 1), 6),
    uncertainty: round(clamp(uncertainty, 0, 1), 6),
  };
}

function deriveCorrelationState(sharedData: SharedMarketData): CorrelationDomain {
  try {
    if (sharedData.btcPrices.length < 40 || sharedData.ethPrices.length < 40) {
      return { state: "stable", correlation7d: 0, correlation30d: 0 };
    }

    const btcReturns = computeReturns(sharedData.btcPrices);
    const ethReturns = computeReturns(sharedData.ethPrices);
    const length = Math.min(btcReturns.length, ethReturns.length);
    if (length < 30) return { state: "stable", correlation7d: 0, correlation30d: 0 };

    const alignedBtc = btcReturns.slice(-length);
    const alignedEth = ethReturns.slice(-length);
    const corr7d = computePearsonCorrelation(alignedBtc.slice(-7 * 24), alignedEth.slice(-7 * 24));
    const corr30d = computePearsonCorrelation(alignedBtc.slice(-30 * 24), alignedEth.slice(-30 * 24));
    const diff = corr7d - corr30d;

    if (diff >= 0.1) {
      return { state: "compression", correlation7d: round(corr7d, 6), correlation30d: round(corr30d, 6) };
    }
    if (diff <= -0.1) {
      return { state: "expansion", correlation7d: round(corr7d, 6), correlation30d: round(corr30d, 6) };
    }
    return { state: "stable", correlation7d: round(corr7d, 6), correlation30d: round(corr30d, 6) };
  } catch {
    return { state: "stable", correlation7d: 0, correlation30d: 0 };
  }
}

function hasUsableVolatilityData(volatility: VolatilityDomain): boolean {
  return (
    !volatility.missing &&
    (
      volatility.btcVolatility24h > 0 ||
      volatility.ethVolatility24h > 0 ||
      Math.abs(volatility.volatilityZScore) > 0 ||
      volatility.volatilityState !== "moderate"
    )
  );
}

function hasUsableLiquidityData(liquidity: LiquidityDomain): boolean {
  return (
    !liquidity.missing &&
    (liquidity.totalVolume24h > 0 || liquidity.stablecoinDominance > 0 || liquidity.avgSpread > 0)
  );
}

function hasUsableSentimentData(sentiment: SentimentDomain): boolean {
  return !sentiment.missing && sentiment.headlineCount > 0;
}

function getMacroUsabilityIssues(
  volatility: VolatilityDomain,
  liquidity: LiquidityDomain,
  sentiment: SentimentDomain,
  sharedData: SharedMarketData,
): string[] {
  const issues: string[] = [];
  if (!hasUsableVolatilityData(volatility)) issues.push("volatility_domain_unusable");
  if (!hasUsableLiquidityData(liquidity)) issues.push("liquidity_domain_unusable");
  if (!hasUsableSentimentData(sentiment)) issues.push("sentiment_domain_unusable");
  if (sharedData.marketBreadthAssetCount < 20) issues.push("market_breadth_unusable");
  return issues;
}

function deriveMarketRegime(
  volatility: VolatilityDomain,
  liquidity: LiquidityDomain,
  sentiment: SentimentDomain,
  alignment: AlignmentDomain,
  correlation: CorrelationDomain,
  sharedData: SharedMarketData,
): MarketRegimeResult {
  const riskAppetite = alignment.riskAppetite;
  const liquidityStress =
    liquidity.liquidityState === "weak" ? "high" : liquidity.liquidityState === "strong" ? "low" : "normal";
  const classification =
    riskAppetite === "defensive"
      ? "DEFENSIVE_STRESS"
      : riskAppetite === "expansionary"
        ? "EXPANSIONARY_RISK_ON"
        : "NEUTRAL_TRANSITION";

  const confidence = round(
    clamp(
      alignment.confidence * 0.7 +
        (1 - alignment.uncertainty) * 0.2 +
        (correlation.state === "stable" ? 0.1 : 0.05),
      0,
      1,
    ),
    6,
  );

  const systemicRisks: string[] = [];
  if (volatility.volatilityState === "elevated" || volatility.volatilityState === "extreme") {
    systemicRisks.push("elevated_market_volatility");
  }
  if (liquidity.liquidityState === "weak") {
    systemicRisks.push("liquidity_fragility");
  }
  if (sentiment.sentimentDirection < -0.2) {
    systemicRisks.push("negative_sentiment_pressure");
  }
  if (sharedData.marketBreadthPositiveRatio > 0 && sharedData.marketBreadthPositiveRatio <= 0.35) {
    systemicRisks.push("broad_market_weakness");
  }
  if (sharedData.marketBreadthPositiveRatio >= 0.65 && sharedData.marketBreadthAbsMove24h >= 3) {
    systemicRisks.push("broad_market_expansion");
  }
  if (correlation.state === "compression") {
    systemicRisks.push("correlation_compression");
  }
  if (systemicRisks.length === 0) {
    systemicRisks.push("no_systemic_alerts");
  }

  return {
    classification,
    confidence,
    signals: [
      `volatility_state:${volatility.volatilityState}`,
      `risk_appetite:${riskAppetite}`,
      `liquidity_stress:${liquidityStress}`,
      `correlation_state:${correlation.state}`,
      `sentiment_direction:${round(sentiment.sentimentDirection, 4)}`,
      `market_breadth_positive_ratio:${round(sharedData.marketBreadthPositiveRatio, 4)}`,
    ],
    systemicRisks,
  };
}

function deriveAllocationAuthorization(
  input: NormalizedPhase1Input,
  marketRegime: MarketRegimeResult,
  volatility: VolatilityDomain,
  liquidity: LiquidityDomain,
  sentiment: SentimentDomain,
): AllocationAuthorizationResult {
  const riskBias =
    input.riskMode === "conservative"
      ? -0.08
      : input.riskMode === "growth"
        ? 0.03
      : input.riskMode === "aggressive"
        ? 0.06
        : 0;

  const authorizationConfidence = round(clamp(marketRegime.confidence + riskBias, 0, 1), 6);
  let status: AllocationAuthorizationStatus = "DEFERRED";
  const justification: string[] = [
    `market_regime:${marketRegime.classification}`,
    `volatility_state:${volatility.volatilityState}`,
    `liquidity_state:${liquidity.liquidityState}`,
    `sentiment_direction:${round(sentiment.sentimentDirection, 4)}`,
    `risk_mode:${input.riskMode}`,
  ];

  if (
    marketRegime.classification === "DEFENSIVE_STRESS" &&
    (authorizationConfidence >= 0.45 || volatility.volatilityState === "extreme")
  ) {
    status = "PROHIBITED";
    justification.push("allocation_activity_blocked_by_macro_stress");
  } else if (
    marketRegime.classification === "EXPANSIONARY_RISK_ON" &&
    authorizationConfidence >= 0.55 &&
    liquidity.liquidityState !== "weak"
  ) {
    status = "AUTHORIZED";
    justification.push("allocation_activity_permitted_under_current_regime");
  } else {
    status = "DEFERRED";
    justification.push("allocation_activity_deferred_pending_phase_3_asset_checks");
  }

  return {
    status,
    confidence: authorizationConfidence,
    justification,
  };
}

function applyPolicyDelta(target: Phase2PolicyDeltas, next: Partial<Phase2PolicyDeltas>) {
  target.riskBudget += next.riskBudget ?? 0;
  target.maxSingleAssetExposure += next.maxSingleAssetExposure ?? 0;
  target.stablecoinMinimum += next.stablecoinMinimum ?? 0;
  target.highVolatilityAssetCap += next.highVolatilityAssetCap ?? 0;
  target.portfolioVolatilityTarget += next.portfolioVolatilityTarget ?? 0;
  target.volatilityCeiling += next.volatilityCeiling ?? 0;
  target.capitalPreservationBias += next.capitalPreservationBias ?? 0;
}

function createZeroPolicyDelta(): Phase2PolicyDeltas {
  return {
    riskBudget: 0,
    maxSingleAssetExposure: 0,
    stablecoinMinimum: 0,
    highVolatilityAssetCap: 0,
    portfolioVolatilityTarget: 0,
    volatilityCeiling: 0,
    capitalPreservationBias: 0,
  };
}

function derivePhase2PolicyEnvelope(
  phase1: Phase1Output,
  input: NormalizedPhase1Input,
  agentInvocation: Phase2AgentInvocationResult,
): {
  output: Phase2Output;
  policyRules: string[];
} {
  const market = phase1.market_condition;
  const fearGreed = phase1.evidence.sentiment_metrics.fear_greed_available
    ? phase1.evidence.sentiment_metrics.fear_greed_index
    : null;

  const baseline = PHASE2_BASELINES[input.userProfile.riskTolerance];
  const timeframeDelta = PHASE2_TIMEFRAME_DELTAS[input.userProfile.investmentTimeframe];
  const marketDelta = createZeroPolicyDelta();
  const policyRules: string[] = [
    "rule:phase2_agent_required:true",
    `phase2_prompt_version:${PHASE2_SYSTEM_PROMPT_VERSION}`,
    `phase2_prompt_hash:${hashPhase2SystemPrompt()}`,
    `baseline_risk_tolerance:${input.userProfile.riskTolerance}`,
    `baseline_investment_timeframe:${input.userProfile.investmentTimeframe}`,
    `phase1_authorization:${phase1.allocation_authorization.status}`,
    `market_volatility_state:${market.volatility_state}`,
    `market_liquidity_state:${market.liquidity_state}`,
    `market_risk_appetite:${market.risk_appetite}`,
  ];
  const agentJudgement = agentInvocation.judgement;
  if (agentInvocation.used) {
    policyRules.push("rule:phase2_agent_judgement_applied");
    policyRules.push(`rule:phase2_agent_posture:${agentJudgement.posture}`);
    policyRules.push(`rule:phase2_agent_hint:${agentJudgement.authorization_hint}`);
    for (const reasonCode of agentJudgement.reason_codes) {
      policyRules.push(`rule:phase2_agent_reason:${reasonCode}`);
    }
  }

  if (market.risk_appetite === "defensive") {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.1,
      maxSingleAssetExposure: -0.04,
      stablecoinMinimum: 0.08,
      highVolatilityAssetCap: -0.08,
      portfolioVolatilityTarget: -0.1,
      volatilityCeiling: -0.12,
      capitalPreservationBias: 0.12,
    });
    policyRules.push("rule:risk_appetite_defensive");
  } else if (market.risk_appetite === "expansionary") {
    applyPolicyDelta(marketDelta, {
      riskBudget: 0.06,
      maxSingleAssetExposure: 0.03,
      stablecoinMinimum: -0.04,
      highVolatilityAssetCap: 0.05,
      portfolioVolatilityTarget: 0.07,
      volatilityCeiling: 0.08,
      capitalPreservationBias: -0.08,
    });
    policyRules.push("rule:risk_appetite_expansionary");
  } else {
    policyRules.push("rule:risk_appetite_neutral");
  }

  if (market.volatility_state === "elevated") {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.07,
      maxSingleAssetExposure: -0.03,
      stablecoinMinimum: 0.04,
      highVolatilityAssetCap: -0.05,
      portfolioVolatilityTarget: -0.07,
      volatilityCeiling: -0.08,
      capitalPreservationBias: 0.08,
    });
    policyRules.push("rule:volatility_elevated");
  } else if (market.volatility_state === "extreme") {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.16,
      maxSingleAssetExposure: -0.07,
      stablecoinMinimum: 0.12,
      highVolatilityAssetCap: -0.12,
      portfolioVolatilityTarget: -0.14,
      volatilityCeiling: -0.16,
      capitalPreservationBias: 0.16,
    });
    policyRules.push("rule:volatility_extreme");
  } else if (market.volatility_state === "low") {
    applyPolicyDelta(marketDelta, {
      riskBudget: 0.02,
      stablecoinMinimum: -0.01,
      highVolatilityAssetCap: 0.01,
      portfolioVolatilityTarget: 0.02,
      volatilityCeiling: 0.02,
      capitalPreservationBias: -0.02,
    });
    policyRules.push("rule:volatility_low");
  } else {
    policyRules.push("rule:volatility_moderate");
  }

  if (market.liquidity_state === "weak") {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.1,
      maxSingleAssetExposure: -0.05,
      stablecoinMinimum: 0.08,
      highVolatilityAssetCap: -0.08,
      portfolioVolatilityTarget: -0.08,
      volatilityCeiling: -0.1,
      capitalPreservationBias: 0.1,
    });
    policyRules.push("rule:liquidity_weak");
  } else if (market.liquidity_state === "strong") {
    applyPolicyDelta(marketDelta, {
      riskBudget: 0.04,
      maxSingleAssetExposure: 0.02,
      stablecoinMinimum: -0.03,
      highVolatilityAssetCap: 0.03,
      portfolioVolatilityTarget: 0.03,
      volatilityCeiling: 0.03,
      capitalPreservationBias: -0.04,
    });
    policyRules.push("rule:liquidity_strong");
  } else {
    policyRules.push("rule:liquidity_stable");
  }

  if (market.sentiment_direction <= -0.25) {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.07,
      stablecoinMinimum: 0.05,
      highVolatilityAssetCap: -0.04,
      portfolioVolatilityTarget: -0.05,
      volatilityCeiling: -0.06,
      capitalPreservationBias: 0.07,
    });
    policyRules.push("rule:sentiment_negative");
  } else if (market.sentiment_direction >= 0.25) {
    applyPolicyDelta(marketDelta, {
      riskBudget: 0.05,
      stablecoinMinimum: -0.03,
      highVolatilityAssetCap: 0.04,
      portfolioVolatilityTarget: 0.04,
      volatilityCeiling: 0.05,
      capitalPreservationBias: -0.06,
    });
    policyRules.push("rule:sentiment_positive");
  } else if (Math.abs(market.sentiment_direction) < 0.08) {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.02,
      stablecoinMinimum: 0.02,
      capitalPreservationBias: 0.03,
    });
    policyRules.push("rule:sentiment_flat");
  }

  if (fearGreed !== null) {
    policyRules.push(`rule:fear_greed_present:${round(fearGreed, 3)}`);
    if (fearGreed <= 15) {
      applyPolicyDelta(marketDelta, {
        riskBudget: -0.08,
        stablecoinMinimum: 0.08,
        highVolatilityAssetCap: -0.05,
        capitalPreservationBias: 0.09,
      });
      policyRules.push("rule:fear_greed_extreme_fear");
    } else if (fearGreed <= 30) {
      applyPolicyDelta(marketDelta, {
        riskBudget: -0.04,
        stablecoinMinimum: 0.04,
        highVolatilityAssetCap: -0.02,
        capitalPreservationBias: 0.05,
      });
      policyRules.push("rule:fear_greed_fear");
    } else if (fearGreed >= 75) {
      applyPolicyDelta(marketDelta, {
        riskBudget: 0.05,
        stablecoinMinimum: -0.03,
        highVolatilityAssetCap: 0.03,
        capitalPreservationBias: -0.05,
      });
      policyRules.push("rule:fear_greed_extreme_greed");
    } else if (fearGreed >= 60) {
      applyPolicyDelta(marketDelta, {
        riskBudget: 0.03,
        stablecoinMinimum: -0.02,
        highVolatilityAssetCap: 0.02,
        capitalPreservationBias: -0.03,
      });
      policyRules.push("rule:fear_greed_greed");
    }
  } else {
    policyRules.push("rule:fear_greed_unavailable");
  }

  if (market.uncertainty >= 0.65) {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.08,
      maxSingleAssetExposure: -0.03,
      stablecoinMinimum: 0.05,
      highVolatilityAssetCap: -0.04,
      portfolioVolatilityTarget: -0.06,
      volatilityCeiling: -0.07,
      capitalPreservationBias: 0.09,
    });
    policyRules.push("rule:uncertainty_high");
  }
  if (market.confidence <= 0.4) {
    applyPolicyDelta(marketDelta, {
      riskBudget: -0.05,
      stablecoinMinimum: 0.03,
      portfolioVolatilityTarget: -0.04,
      volatilityCeiling: -0.04,
      capitalPreservationBias: 0.06,
    });
    policyRules.push("rule:confidence_low");
  } else if (market.confidence >= 0.75 && market.uncertainty <= 0.35) {
    applyPolicyDelta(marketDelta, {
      riskBudget: 0.03,
      maxSingleAssetExposure: 0.01,
      highVolatilityAssetCap: 0.02,
      portfolioVolatilityTarget: 0.02,
      volatilityCeiling: 0.02,
      capitalPreservationBias: -0.03,
    });
    policyRules.push("rule:confidence_high_uncertainty_low");
  }

  const profileDeltaConfig = PHASE2_AGENT_DELTA_PROFILE_CONFIG[input.userProfile.riskTolerance];
  const agentDeltaScale =
    !agentJudgement
      ? 0
      : agentJudgement.authorization_hint === "TIGHTEN" || agentJudgement.posture === "more_defensive"
        ? profileDeltaConfig.tightenMultiplier
        : agentJudgement.authorization_hint === "RELAX" || agentJudgement.posture === "selective_risk_on"
          ? profileDeltaConfig.relaxMultiplier
          : profileDeltaConfig.neutralMultiplier;
  if (agentJudgement) {
    policyRules.push(`rule:phase2_agent_delta_scale:${round(agentDeltaScale, 3)}`);
    policyRules.push(`rule:phase2_agent_delta_profile:${input.userProfile.riskTolerance.toLowerCase()}`);
  }

  const agentDeltaApplied = agentJudgement
    ? {
        risk_budget_delta: round(
          clamp(agentJudgement.envelope_adjustments.risk_budget_delta * agentDeltaScale, -0.05, 0.05),
          6,
        ),
        max_single_asset_exposure_delta: round(
          clamp(agentJudgement.envelope_adjustments.max_single_asset_exposure_delta * agentDeltaScale, -0.05, 0.05),
          6,
        ),
        stablecoin_minimum_delta: round(
          clamp(agentJudgement.envelope_adjustments.stablecoin_minimum_delta * agentDeltaScale, -0.05, 0.05),
          6,
        ),
        high_volatility_asset_cap_delta: round(
          clamp(agentJudgement.envelope_adjustments.high_volatility_asset_cap_delta * agentDeltaScale, -0.05, 0.05),
          6,
        ),
        portfolio_volatility_target_delta: round(
          clamp(agentJudgement.envelope_adjustments.portfolio_volatility_target_delta * agentDeltaScale, -0.05, 0.05),
          6,
        ),
        volatility_ceiling_delta: round(
          clamp(agentJudgement.envelope_adjustments.volatility_ceiling_delta * agentDeltaScale, -0.05, 0.05),
          6,
        ),
        capital_preservation_bias_delta: round(
          clamp(agentJudgement.envelope_adjustments.capital_preservation_bias_delta * agentDeltaScale, -0.08, 0.08),
          6,
        ),
      }
    : {
        risk_budget_delta: 0,
        max_single_asset_exposure_delta: 0,
        stablecoin_minimum_delta: 0,
        high_volatility_asset_cap_delta: 0,
        portfolio_volatility_target_delta: 0,
        volatility_ceiling_delta: 0,
        capital_preservation_bias_delta: 0,
      };
  if (agentJudgement) {
    applyPolicyDelta(marketDelta, {
      riskBudget: agentDeltaApplied.risk_budget_delta,
      maxSingleAssetExposure: agentDeltaApplied.max_single_asset_exposure_delta,
      stablecoinMinimum: agentDeltaApplied.stablecoin_minimum_delta,
      highVolatilityAssetCap: agentDeltaApplied.high_volatility_asset_cap_delta,
      portfolioVolatilityTarget: agentDeltaApplied.portfolio_volatility_target_delta,
      volatilityCeiling: agentDeltaApplied.volatility_ceiling_delta,
      capitalPreservationBias: agentDeltaApplied.capital_preservation_bias_delta,
    });
  }

  const riskBudget = clamp(
    baseline.riskBudget + timeframeDelta.riskBudget + marketDelta.riskBudget,
    0.08,
    0.9,
  );
  let maxSingleAssetExposure = clamp(
    baseline.maxSingleAssetExposure + timeframeDelta.maxSingleAssetExposure + marketDelta.maxSingleAssetExposure,
    0.05,
    0.45,
  );
  const stablecoinMinimum = clamp(
    baseline.stablecoinMinimum + timeframeDelta.stablecoinMinimum + marketDelta.stablecoinMinimum,
    0.03,
    0.75,
  );
  let highVolatilityAssetCap = clamp(
    baseline.highVolatilityAssetCap + timeframeDelta.highVolatilityAssetCap + marketDelta.highVolatilityAssetCap,
    0.02,
    0.45,
  );
  const portfolioVolatilityTarget = clamp(
    baseline.portfolioVolatilityTarget + timeframeDelta.portfolioVolatilityTarget + marketDelta.portfolioVolatilityTarget,
    0.1,
    0.9,
  );
  let volatilityCeiling = clamp(
    baseline.volatilityCeiling + timeframeDelta.volatilityCeiling + marketDelta.volatilityCeiling,
    0.15,
    0.95,
  );
  const rawCapitalPreservationBias =
    baseline.capitalPreservationBias + timeframeDelta.capitalPreservationBias + marketDelta.capitalPreservationBias;
    const capitalPreservationBias = clamp(
    rawCapitalPreservationBias,
    profileDeltaConfig.capitalPreservationBiasMin,
    profileDeltaConfig.capitalPreservationBiasMax,
  );
  if (rawCapitalPreservationBias > profileDeltaConfig.capitalPreservationBiasMax + 1e-9) {
    policyRules.push(
      `rule:capital_preservation_bias_profile_max:${round(profileDeltaConfig.capitalPreservationBiasMax, 3)}`,
    );
  } else if (rawCapitalPreservationBias < profileDeltaConfig.capitalPreservationBiasMin - 1e-9) {
    policyRules.push(
      `rule:capital_preservation_bias_profile_min:${round(profileDeltaConfig.capitalPreservationBiasMin, 3)}`,
    );
  }

  const maxSingleHardCap = clamp(riskBudget * 0.85 + 0.05, 0.08, 0.45);
  maxSingleAssetExposure = Math.min(maxSingleAssetExposure, maxSingleHardCap);
  highVolatilityAssetCap = Math.min(highVolatilityAssetCap, maxSingleAssetExposure);
  if (
    input.userProfile.riskTolerance === "Aggressive" &&
    input.userProfile.investmentTimeframe === "<1_year" &&
    market.volatility_state === "low"
  ) {
    const adjustedFloor = Math.min(AGGRESSIVE_SHORT_TERM_LOW_VOL_HIGH_VOL_CAP_FLOOR, maxSingleAssetExposure);
    highVolatilityAssetCap = Math.max(highVolatilityAssetCap, adjustedFloor);
    policyRules.push(`rule:high_volatility_cap_floor_aggressive_short_term_low_vol:${round(adjustedFloor, 3)}`);
  }
  volatilityCeiling = Math.max(volatilityCeiling, portfolioVolatilityTarget);

  let liquidityFloorRequirement: LiquidityFloorRequirement = baseline.liquidityFloorRequirement;
  if (
    market.liquidity_state === "weak" ||
    market.risk_appetite === "defensive" ||
    input.userProfile.riskTolerance === "Conservative" ||
    market.uncertainty >= 0.7
  ) {
    liquidityFloorRequirement = "tier_1_only";
    policyRules.push("rule:liquidity_floor_tier1_only");
  } else if (
    market.liquidity_state === "strong" &&
    market.risk_appetite === "expansionary" &&
    (input.userProfile.riskTolerance === "Growth" || input.userProfile.riskTolerance === "Aggressive") &&
    input.userProfile.investmentTimeframe === "3+_years"
  ) {
    liquidityFloorRequirement = "broad_liquidity_ok";
    policyRules.push("rule:liquidity_floor_broad");
  } else if (input.userProfile.riskTolerance === "Balanced") {
    liquidityFloorRequirement = "tier_1_only";
    policyRules.push("rule:liquidity_floor_tier1_balanced");
  } else {
    liquidityFloorRequirement = "tier_1_plus_tier_2";
    policyRules.push("rule:liquidity_floor_tier1_plus_tier2");
  }

  const riskScalingFactor = clamp(riskBudget / Math.max(0.0001, baseline.riskBudget), 0, 2);
  const defensiveBiasAdjustment = round(capitalPreservationBias - baseline.capitalPreservationBias, 6);
  const defensiveAdjustmentApplied =
    riskBudget < baseline.riskBudget - 0.000001 || capitalPreservationBias > baseline.capitalPreservationBias + 0.02;

  const severeMacroBlock = (
    market.volatility_state === "extreme" &&
    market.liquidity_state === "weak" &&
    (market.sentiment_direction <= -0.4 || market.uncertainty >= 0.8)
  ) || (
    market.uncertainty >= 0.9 &&
    market.confidence <= 0.2
  );

  const baseAuthorizationConfidence = clamp(
    market.confidence * 0.6 + (1 - market.uncertainty) * 0.4,
    0,
    1,
  );
  let authorizationStatus: Phase2AuthorizationStatus = "RESTRICTED";
  let authorizationReason = "BASELINE_POLICY_RESTRICTIONS";
  let authorizationConfidence = baseAuthorizationConfidence;
  const aggressiveGuardedAuthorizedEligible =
    !severeMacroBlock &&
    input.userProfile.riskTolerance === "Aggressive" &&
    riskScalingFactor >= 0.9 &&
    market.confidence >= 0.65 &&
    market.uncertainty <= 0.6 &&
    market.volatility_state !== "extreme" &&
    stablecoinMinimum >= 0.25 &&
    highVolatilityAssetCap <= 0.12 &&
    liquidityFloorRequirement === "tier_1_only";

  if (severeMacroBlock) {
    authorizationStatus = "PROHIBITED";
    authorizationReason = "MACRO_EMERGENCY_BLOCK";
    authorizationConfidence = clamp(baseAuthorizationConfidence + 0.06, 0, 1);
    policyRules.push("rule:authorization_prohibited_macro_emergency");
  } else if (aggressiveGuardedAuthorizedEligible) {
    authorizationStatus = "AUTHORIZED";
    authorizationReason = "AGGRESSIVE_GUARDED_ACTIVE";
    authorizationConfidence = clamp(baseAuthorizationConfidence + 0.04, 0, 1);
    policyRules.push("rule:authorization_aggressive_guarded_active");
  } else if (
    (phase1.allocation_authorization.status === "AUTHORIZED" && riskScalingFactor >= 0.95) ||
    (market.risk_appetite === "expansionary" &&
      market.liquidity_state !== "weak" &&
      market.confidence >= 0.55 &&
      market.uncertainty <= 0.55 &&
      riskScalingFactor >= 0.92)
  ) {
    authorizationStatus = "AUTHORIZED";
    authorizationReason = "MARKET_ALIGNED_POLICY_ACTIVE";
    authorizationConfidence = clamp(baseAuthorizationConfidence + 0.05, 0, 1);
    policyRules.push("rule:authorization_active");
  } else if (phase1.allocation_authorization.status === "PROHIBITED") {
    authorizationStatus = "RESTRICTED";
    authorizationReason = "PHASE1_PROHIBITION_DOWNGRADED_TO_RESTRICTED";
    authorizationConfidence = clamp(baseAuthorizationConfidence + 0.02, 0, 1);
    policyRules.push("rule:authorization_prohibited_to_restricted");
  } else if (market.liquidity_state === "weak") {
    authorizationStatus = "RESTRICTED";
    authorizationReason = "LIQUIDITY_GUARDRAILS_ENFORCED";
    authorizationConfidence = baseAuthorizationConfidence;
    policyRules.push("rule:authorization_liquidity_restricted");
  } else if (market.risk_appetite === "defensive" || market.uncertainty >= 0.6) {
    authorizationStatus = "RESTRICTED";
    authorizationReason = "DEFENSIVE_POSTURE_RESTRICTIONS";
    authorizationConfidence = baseAuthorizationConfidence;
    policyRules.push("rule:authorization_defensive_restricted");
  }

  if (
    agentJudgement?.authorization_hint === "RELAX" &&
    authorizationStatus === "RESTRICTED" &&
    phase1.allocation_authorization.status !== "PROHIBITED" &&
    !severeMacroBlock &&
    market.volatility_state !== "extreme" &&
    market.liquidity_state !== "weak" &&
    market.confidence >= 0.62 &&
    market.uncertainty <= 0.58 &&
    riskScalingFactor >= 0.88
  ) {
    authorizationStatus = "AUTHORIZED";
    authorizationReason = "AGENT_GUARDED_RELAX_WITHIN_BOUNDS";
    authorizationConfidence = clamp(baseAuthorizationConfidence + 0.03, 0, 1);
    policyRules.push("rule:authorization_agent_relax_within_bounds");
  }

  if (agentJudgement?.authorization_hint === "TIGHTEN" && authorizationStatus === "AUTHORIZED" && !severeMacroBlock) {
    authorizationStatus = "RESTRICTED";
    authorizationReason = "AGENT_TIGHTEN_WITHIN_BOUNDS";
    authorizationConfidence = clamp(baseAuthorizationConfidence + 0.01, 0, 1);
    policyRules.push("rule:authorization_agent_tighten_within_bounds");
  }

  const policyMode: AllocationPolicyMode =
    capitalPreservationBias >= 0.75
      ? "capital_preservation"
      : capitalPreservationBias >= 0.58
        ? "balanced_defensive"
        : capitalPreservationBias >= 0.4
          ? "balanced_growth"
          : "offensive_growth";

  const marketConditionRef = `sha256:${createHash("sha256").update(JSON.stringify(phase1.market_condition)).digest("hex")}`;

  const output: Phase2Output = {
    timestamp: nowIso(),
    execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
    doctrine_version: PHASE2_DOCTRINE_VERSION,
    inputs: {
      market_condition_ref: marketConditionRef,
      user_profile: {
        risk_tolerance: input.userProfile.riskTolerance,
        investment_timeframe: input.userProfile.investmentTimeframe,
      },
    },
    allocation_policy: {
      mode: policyMode,
      defensive_bias_adjustment: round(clamp(defensiveBiasAdjustment, -1, 1), 6),
    },
    policy_envelope: {
      risk_budget: round(riskBudget, 6),
      risk_scaling_factor: round(riskScalingFactor, 6),
      exposure_caps: {
        max_single_asset_exposure: round(maxSingleAssetExposure, 6),
        high_volatility_asset_cap: round(highVolatilityAssetCap, 6),
      },
      stablecoin_minimum: round(stablecoinMinimum, 6),
      portfolio_volatility_target: round(portfolioVolatilityTarget, 6),
      liquidity_floor_requirement: liquidityFloorRequirement,
      volatility_ceiling: round(volatilityCeiling, 6),
      capital_preservation_bias: round(capitalPreservationBias, 6),
      defensive_adjustment_applied: defensiveAdjustmentApplied,
    },
    allocation_authorization: {
      status: authorizationStatus,
      reason: authorizationReason,
      confidence: round(clamp(authorizationConfidence, 0, 1), 6),
    },
    phase_boundaries: {
      asset_universe_expansion: "PHASE_3",
      portfolio_construction: "PHASE_4",
    },
    audit: {
      phase1_timestamp_ref: phase1.timestamp,
      policy_rules_applied: Array.from(new Set(policyRules)),
      agent_delta_applied: agentDeltaApplied,
      agent_judgement: {
        used: agentInvocation.used,
        model: agentInvocation.model,
        posture: agentJudgement.posture,
        authorization_hint: agentJudgement.authorization_hint,
        reason_codes: agentJudgement.reason_codes,
        skipped_reason: null,
      },
    },
  };

  return { output, policyRules };
}

function sanitizePhase2ForSchema(output: Phase2Output): Phase2Output {
  return {
    ...output,
    timestamp: String(output.timestamp),
    execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
    doctrine_version: PHASE2_DOCTRINE_VERSION,
    inputs: {
      market_condition_ref: String(output.inputs.market_condition_ref),
      user_profile: {
        risk_tolerance: output.inputs.user_profile.risk_tolerance,
        investment_timeframe: output.inputs.user_profile.investment_timeframe,
      },
    },
    allocation_policy: {
      mode: output.allocation_policy.mode,
      defensive_bias_adjustment: round(clamp(output.allocation_policy.defensive_bias_adjustment, -1, 1), 6),
    },
    policy_envelope: {
      risk_budget: round(clamp(output.policy_envelope.risk_budget, 0, 1), 6),
      risk_scaling_factor: round(clamp(output.policy_envelope.risk_scaling_factor, 0, 2), 6),
      exposure_caps: {
        max_single_asset_exposure: round(clamp(output.policy_envelope.exposure_caps.max_single_asset_exposure, 0, 1), 6),
        high_volatility_asset_cap: round(clamp(output.policy_envelope.exposure_caps.high_volatility_asset_cap, 0, 1), 6),
      },
      stablecoin_minimum: round(clamp(output.policy_envelope.stablecoin_minimum, 0, 1), 6),
      portfolio_volatility_target: round(clamp(output.policy_envelope.portfolio_volatility_target, 0, 1), 6),
      liquidity_floor_requirement: output.policy_envelope.liquidity_floor_requirement,
      volatility_ceiling: round(clamp(output.policy_envelope.volatility_ceiling, 0, 1), 6),
      capital_preservation_bias: round(clamp(output.policy_envelope.capital_preservation_bias, 0, 1), 6),
      defensive_adjustment_applied: Boolean(output.policy_envelope.defensive_adjustment_applied),
    },
    allocation_authorization: {
      status: output.allocation_authorization.status,
      reason: String(output.allocation_authorization.reason),
      confidence: round(clamp(output.allocation_authorization.confidence, 0, 1), 6),
    },
    phase_boundaries: {
      asset_universe_expansion: "PHASE_3",
      portfolio_construction: "PHASE_4",
    },
    audit: {
      phase1_timestamp_ref: String(output.audit.phase1_timestamp_ref),
      policy_rules_applied: output.audit.policy_rules_applied.map((item) => String(item)),
      agent_delta_applied: {
        risk_budget_delta: round(output.audit.agent_delta_applied.risk_budget_delta, 6),
        max_single_asset_exposure_delta: round(output.audit.agent_delta_applied.max_single_asset_exposure_delta, 6),
        stablecoin_minimum_delta: round(output.audit.agent_delta_applied.stablecoin_minimum_delta, 6),
        high_volatility_asset_cap_delta: round(output.audit.agent_delta_applied.high_volatility_asset_cap_delta, 6),
        portfolio_volatility_target_delta: round(output.audit.agent_delta_applied.portfolio_volatility_target_delta, 6),
        volatility_ceiling_delta: round(output.audit.agent_delta_applied.volatility_ceiling_delta, 6),
        capital_preservation_bias_delta: round(output.audit.agent_delta_applied.capital_preservation_bias_delta, 6),
      },
      agent_judgement: {
        used: Boolean(output.audit.agent_judgement.used),
        model: output.audit.agent_judgement.model ? String(output.audit.agent_judgement.model) : null,
        posture: output.audit.agent_judgement.posture ?? null,
        authorization_hint: output.audit.agent_judgement.authorization_hint ?? null,
        reason_codes: output.audit.agent_judgement.reason_codes.map((item) => String(item)),
        skipped_reason: output.audit.agent_judgement.skipped_reason
          ? String(output.audit.agent_judgement.skipped_reason)
          : null,
      },
    },
  };
}

function validatePhase2OutputWithRetry(buildCandidate: () => Phase2Output): Phase2Output {
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    const candidate = attempt === 1 ? buildCandidate() : sanitizePhase2ForSchema(buildCandidate());
    const parsed = phase2OutputSchema.safeParse(candidate);
    if (parsed.success) return parsed.data;
  }

  throw new Error("Phase 2 output validation failed after one retry.");
}

function getPhase3TopVolumeTarget(): number {
  return Math.min(500, Math.max(50, readPositiveIntEnv("PHASE3_TOP_VOLUME_TOKEN_TARGET", DEFAULT_PHASE3_TOP_VOLUME_TOKEN_TARGET)));
}

function getPhase3CoinGeckoMinIntervalMs(): number {
  return Math.min(
    10_000,
    Math.max(0, readPositiveIntEnv("PHASE3_COINGECKO_MIN_INTERVAL_MS", DEFAULT_PHASE3_COINGECKO_MIN_INTERVAL_MS)),
  );
}

function getCoinMarketCapApiKey(): string | null {
  const key = process.env.COINMARKETCAP_API_KEY?.trim();
  return key ? key : null;
}

function normalizeTokenId(value: string): string {
  return value.trim().toLowerCase();
}

function canonicalizePhase3ProfileTokenId(value: string): string {
  const normalized = normalizeTokenId(value);
  return (
    PHASE3_RUNTIME_PROFILE_TOKEN_ALIASES.get(normalized) ??
    PHASE3_PROFILE_TOKEN_ALIASES_FROM_ENV[normalized] ??
    PHASE3_PROFILE_TOKEN_ALIASES[normalized] ??
    normalized
  );
}

function registerPhase3ProfileTokenAlias(sourceId: string, targetId: string, selectionRules?: Set<string>): void {
  const normalizedSourceId = normalizeTokenId(sourceId);
  const normalizedTargetId = normalizeTokenId(targetId);
  if (!normalizedSourceId || !normalizedTargetId || normalizedSourceId === normalizedTargetId) return;
  const existingAlias = canonicalizePhase3ProfileTokenId(normalizedSourceId);
  if (existingAlias === normalizedTargetId) return;

  PHASE3_RUNTIME_PROFILE_TOKEN_ALIASES.set(normalizedSourceId, normalizedTargetId);
  selectionRules?.add(`phase3_profile_token_alias_resolved:${normalizedSourceId}->${normalizedTargetId}`);
}

function normalizeTokenSymbol(value: string): string {
  return value.trim().toUpperCase();
}

function buildProfileFallbackSymbol(tokenId: string): string {
  const normalizedId = canonicalizePhase3ProfileTokenId(tokenId);
  const segments = normalizedId
    .split("-")
    .map((segment) => segment.replace(/[^a-z0-9]/gi, "").toUpperCase())
    .filter(Boolean);

  if (segments.length === 0) return "UNK";
  if (segments.length === 1) {
    return normalizeTokenSymbol(segments[0].slice(0, 6) || "UNK");
  }

  const acronym = segments.map((segment) => segment[0]).join("");
  if (acronym.length >= 2 && acronym.length <= 6) {
    return normalizeTokenSymbol(acronym);
  }

  return normalizeTokenSymbol(segments.join("").slice(0, 6) || "UNK");
}

function buildProfileFallbackName(tokenId: string): string {
  const normalizedId = canonicalizePhase3ProfileTokenId(tokenId);
  const display = normalizedId
    .replace(/-/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!display) return "Unknown Token";
  return display.replace(/\b\w/g, (char) => char.toUpperCase());
}

function buildProfileFallbackToken(tokenId: string, reasons: Set<string>): Phase3UniverseToken {
  const normalizedId = canonicalizePhase3ProfileTokenId(tokenId);
  const fallbackSymbol = buildProfileFallbackSymbol(normalizedId);
  const fallbackName = buildProfileFallbackName(normalizedId);

  return toUniverseToken({
    coingeckoId: normalizedId,
    symbol: fallbackSymbol,
    name: fallbackName,
    marketCapRank: null,
    volume24hUsd: 0,
    sourceTags: ["profile_match", "rate_limit_fallback"],
    profileMatchReasons: Array.from(reasons),
  });
}

function buildEmergencyRetailUniverseTokens(): Phase3UniverseToken[] {
  return PHASE3_EMERGENCY_RETAIL_TOKENS.map((token) =>
    toUniverseToken({
      coingeckoId: token.coingeckoId,
      symbol: token.symbol,
      name: token.name,
      marketCapRank: null,
      volume24hUsd: 0,
      sourceTags: ["emergency_universe_fallback"],
    }),
  );
}

function toUniverseToken(input: {
  coingeckoId: string;
  symbol: string;
  name: string;
  marketCapRank: number | null;
  volume24hUsd: number;
  priceChangePct7d?: number | null;
  priceChangePct30d?: number | null;
  sourceTags: string[];
  profileMatchReasons?: string[];
}): Phase3UniverseToken {
  const volume24hUsd = Math.max(0, round(input.volume24hUsd, 6));
  return {
    coingeckoId: normalizeTokenId(input.coingeckoId),
    symbol: normalizeTokenSymbol(input.symbol),
    name: input.name.trim() || normalizeTokenSymbol(input.symbol),
    marketCapRank:
      typeof input.marketCapRank === "number" && Number.isFinite(input.marketCapRank) && input.marketCapRank > 0
        ? Math.max(1, Math.floor(input.marketCapRank))
        : null,
    volume24hUsd,
    volume7dEstimatedUsd: round(volume24hUsd * 7, 6),
    volume30dEstimatedUsd: round(volume24hUsd * 30, 6),
    priceChangePct7d: input.priceChangePct7d === null || input.priceChangePct7d === undefined ? null : round(input.priceChangePct7d, 6),
    priceChangePct30d:
      input.priceChangePct30d === null || input.priceChangePct30d === undefined ? null : round(input.priceChangePct30d, 6),
    sourceTags: new Set(input.sourceTags.map((tag) => tag.trim()).filter(Boolean)),
    profileMatchReasons: new Set((input.profileMatchReasons ?? []).map((reason) => reason.trim()).filter(Boolean)),
  };
}

function parseCoinGeckoUniverseRows(payload: unknown, sourceTag: string): Phase3UniverseToken[] {
  if (!Array.isArray(payload)) return [];
  const tokens: Phase3UniverseToken[] = [];

  for (const row of payload) {
    if (!row || typeof row !== "object") continue;
    const candidate = row as Record<string, unknown>;
    const coingeckoId = typeof candidate.id === "string" ? candidate.id : "";
    const symbol = typeof candidate.symbol === "string" ? candidate.symbol : "";
    const name = typeof candidate.name === "string" ? candidate.name : symbol.toUpperCase();
    if (!coingeckoId || !symbol) continue;

    tokens.push(
      toUniverseToken({
        coingeckoId,
        symbol,
        name,
        marketCapRank: Number.isFinite(Number(candidate.market_cap_rank))
          ? Number(candidate.market_cap_rank)
          : null,
        volume24hUsd: parsePositiveNumber(candidate.total_volume),
        priceChangePct7d: parseNullableNumber(
          candidate.price_change_percentage_7d_in_currency ?? candidate.price_change_percentage_7d,
        ),
        priceChangePct30d: parseNullableNumber(
          candidate.price_change_percentage_30d_in_currency ?? candidate.price_change_percentage_30d,
        ),
        sourceTags: [sourceTag],
      }),
    );
  }

  return tokens;
}

function parseCoinLoreUniverseRows(payload: unknown, sourceTag: string): Phase3UniverseToken[] {
  if (!payload || typeof payload !== "object") return [];
  const rows = Array.isArray((payload as { data?: unknown[] }).data) ? (payload as { data: unknown[] }).data : [];
  const tokens: Phase3UniverseToken[] = [];

  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const candidate = row as Record<string, unknown>;
    const rawId = String(candidate.id ?? "").trim();
    const symbol = typeof candidate.symbol === "string" ? candidate.symbol : "";
    const name = typeof candidate.name === "string" ? candidate.name : symbol.toUpperCase();
    if (!rawId || !symbol) continue;

    tokens.push(
      toUniverseToken({
        coingeckoId: `coinlore-${rawId}`,
        symbol,
        name,
        marketCapRank: Number.isFinite(Number(candidate.rank)) ? Number(candidate.rank) : null,
        volume24hUsd: parsePositiveNumber(candidate.volume24),
        priceChangePct7d: parseNullableNumber(candidate.percent_change_7d),
        priceChangePct30d: parseNullableNumber(candidate.percent_change_30d),
        sourceTags: [sourceTag],
      }),
    );
  }

  return tokens;
}

function parseCoinPaprikaUniverseRows(payload: unknown, sourceTag: string): Phase3UniverseToken[] {
  if (!Array.isArray(payload)) return [];
  const tokens: Phase3UniverseToken[] = [];

  for (const row of payload) {
    if (!row || typeof row !== "object") continue;
    const candidate = row as Record<string, unknown>;
    const rawId = typeof candidate.id === "string" ? candidate.id.trim() : "";
    const symbol = typeof candidate.symbol === "string" ? candidate.symbol : "";
    const name = typeof candidate.name === "string" ? candidate.name : symbol.toUpperCase();
    if (!rawId || !symbol) continue;
    const normalizedId = rawId.includes("-") ? rawId.slice(rawId.indexOf("-") + 1) : `coinpaprika-${rawId}`;

    const quote =
      ((candidate.quotes as Record<string, unknown> | undefined)?.USD ??
        (candidate.quotes as Record<string, unknown> | undefined)?.usd ??
        {}) as Record<string, unknown>;
    tokens.push(
      toUniverseToken({
        coingeckoId: normalizedId,
        symbol,
        name,
        marketCapRank: Number.isFinite(Number(candidate.rank)) ? Number(candidate.rank) : null,
        volume24hUsd: parsePositiveNumber(quote.volume_24h ?? candidate.volume_24h_usd),
        priceChangePct7d: parseNullableNumber(quote.percent_change_7d ?? candidate.percent_change_7d),
        priceChangePct30d: parseNullableNumber(quote.percent_change_30d ?? candidate.percent_change_30d),
        sourceTags: [sourceTag],
      }),
    );
  }

  return tokens;
}

function parseCoinMarketCapUniverseRows(payload: unknown, sourceTag: string): Phase3UniverseToken[] {
  const rows =
    Array.isArray((payload as { data?: unknown[] } | null)?.data)
      ? ((payload as { data: unknown[] }).data)
      : [];
  const tokens: Phase3UniverseToken[] = [];

  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const candidate = row as Record<string, unknown>;
    const symbol = typeof candidate.symbol === "string" ? candidate.symbol : "";
    const name = typeof candidate.name === "string" ? candidate.name : symbol.toUpperCase();
    if (!symbol || !name) continue;

    const slug = typeof candidate.slug === "string" && candidate.slug.trim() ? candidate.slug : "";
    const id = slug || (typeof candidate.id === "number" ? `coinmarketcap-${candidate.id}` : "");
    if (!id) continue;
    const quote =
      ((candidate.quote as Record<string, unknown> | undefined)?.USD ??
        (candidate.quote as Record<string, unknown> | undefined)?.usd ??
        {}) as Record<string, unknown>;

    tokens.push(
      toUniverseToken({
        coingeckoId: id,
        symbol,
        name,
        marketCapRank: Number.isFinite(Number(candidate.cmc_rank)) ? Number(candidate.cmc_rank) : null,
        volume24hUsd: parsePositiveNumber(quote.volume_24h ?? candidate.volume_24h),
        priceChangePct7d: parseNullableNumber(quote.percent_change_7d),
        priceChangePct30d: parseNullableNumber(quote.percent_change_30d),
        sourceTags: [sourceTag],
      }),
    );
  }

  return tokens;
}

function mergeUniverseTokens(
  base: Map<string, Phase3UniverseToken>,
  token: Phase3UniverseToken,
): void {
  const existing = base.get(token.coingeckoId);
  if (!existing) {
    base.set(token.coingeckoId, token);
    return;
  }
  existing.marketCapRank =
    existing.marketCapRank === null
      ? token.marketCapRank
      : token.marketCapRank === null
        ? existing.marketCapRank
        : Math.min(existing.marketCapRank, token.marketCapRank);
  existing.volume24hUsd = Math.max(existing.volume24hUsd, token.volume24hUsd);
  existing.volume7dEstimatedUsd = Math.max(existing.volume7dEstimatedUsd, token.volume7dEstimatedUsd);
  existing.volume30dEstimatedUsd = Math.max(existing.volume30dEstimatedUsd, token.volume30dEstimatedUsd);
  existing.priceChangePct7d = existing.priceChangePct7d ?? token.priceChangePct7d;
  existing.priceChangePct30d = existing.priceChangePct30d ?? token.priceChangePct30d;
  for (const sourceTag of token.sourceTags) existing.sourceTags.add(sourceTag);
  for (const reason of token.profileMatchReasons) existing.profileMatchReasons.add(reason);
}

function getPhase3TokenRank(token: Phase3UniverseToken): number {
  return token.marketCapRank ?? Number.MAX_SAFE_INTEGER;
}

function pickPhase3ReasonCandidates(
  pool: Phase3UniverseToken[],
  limit: number,
  primaryPredicate: (token: Phase3UniverseToken) => boolean,
  secondaryPredicate?: (token: Phase3UniverseToken) => boolean,
): Phase3UniverseToken[] {
  if (limit <= 0 || pool.length === 0) return [];

  const selected: Phase3UniverseToken[] = [];
  const seenIds = new Set<string>();
  const appendMatches = (predicate: (token: Phase3UniverseToken) => boolean) => {
    for (const token of pool) {
      if (selected.length >= limit) break;
      if (seenIds.has(token.coingeckoId)) continue;
      if (!predicate(token)) continue;
      selected.push(token);
      seenIds.add(token.coingeckoId);
    }
  };

  appendMatches(primaryPredicate);
  if (selected.length < limit && secondaryPredicate) {
    appendMatches(secondaryPredicate);
  }
  if (selected.length < limit) {
    appendMatches(() => true);
  }

  return selected;
}

function buildPhase3ProfileReasonMap(
  phase2Output: Phase2Output,
  existingTokenMap: Map<string, Phase3UniverseToken>,
): Map<string, Set<string>> {
  const map = new Map<string, Set<string>>();
  const add = (tokenId: string, reason: string) => {
    const normalizedId = canonicalizePhase3ProfileTokenId(tokenId);
    if (!normalizedId) return;
    const reasons = map.get(normalizedId) ?? new Set<string>();
    reasons.add(reason);
    map.set(normalizedId, reasons);
  };
  const addMany = (tokens: Phase3UniverseToken[], reason: string) => {
    for (const token of tokens) {
      add(token.coingeckoId, reason);
    }
  };

  const sorted = sortUniverseTokens(Array.from(existingTokenMap.values()));
  const rankedPool = sorted.filter((token) => token.marketCapRank !== null && token.marketCapRank <= 500);
  const pool = rankedPool.length > 0 ? rankedPool : sorted;
  if (pool.length === 0) return map;

  const isStable = (token: Phase3UniverseToken) => isStablecoinToken(token);
  const isMeme = (token: Phase3UniverseToken) => isMemeToken(token);
  const isTopN = (token: Phase3UniverseToken, maxRank: number) => getPhase3TokenRank(token) <= maxRank;
  const isLargeLiquid = (token: Phase3UniverseToken) => isTopN(token, 120) && token.volume24hUsd > 0;
  const isDefensive = (token: Phase3UniverseToken) => !isMeme(token) && (isStable(token) || isTopN(token, 40));
  const isGrowth = (token: Phase3UniverseToken) => !isStable(token) && !isMeme(token) && isTopN(token, 220);
  const isHighVolatility = (token: Phase3UniverseToken) =>
    !isStable(token) &&
    (isMeme(token) || (isTopN(token, 250) && !isTopN(token, 20)));

  const riskTolerance = phase2Output.inputs.user_profile.risk_tolerance;
  const riskReason = `profile_risk_tolerance:${riskTolerance.toLowerCase()}`;
  const riskTarget = PHASE3_PROFILE_REASON_TARGETS[riskTolerance];
  if (riskTolerance === "Conservative") {
    addMany(
      pickPhase3ReasonCandidates(pool, riskTarget, (token) => isDefensive(token), (token) => isLargeLiquid(token) && !isMeme(token)),
      riskReason,
    );
  } else if (riskTolerance === "Balanced") {
    addMany(
      pickPhase3ReasonCandidates(pool, riskTarget, (token) => isLargeLiquid(token) && !isMeme(token), (token) => isGrowth(token)),
      riskReason,
    );
  } else if (riskTolerance === "Growth") {
    addMany(
      pickPhase3ReasonCandidates(pool, riskTarget, (token) => isGrowth(token), (token) => !isStable(token) && !isMeme(token)),
      riskReason,
    );
  } else {
    addMany(
      pickPhase3ReasonCandidates(pool, riskTarget, (token) => isHighVolatility(token), (token) => !isStable(token) && isTopN(token, 300)),
      riskReason,
    );
  }

  const policyMode = phase2Output.allocation_policy.mode;
  const policyReason = `policy_mode:${policyMode}`;
  const policyTarget = PHASE3_POLICY_REASON_TARGETS[policyMode];
  if (policyMode === "capital_preservation") {
    addMany(
      pickPhase3ReasonCandidates(pool, policyTarget, (token) => isDefensive(token), (token) => isLargeLiquid(token) && !isMeme(token)),
      policyReason,
    );
  } else if (policyMode === "balanced_defensive") {
    addMany(
      pickPhase3ReasonCandidates(pool, policyTarget, (token) => isDefensive(token), (token) => isGrowth(token)),
      policyReason,
    );
  } else if (policyMode === "balanced_growth") {
    addMany(
      pickPhase3ReasonCandidates(pool, policyTarget, (token) => isGrowth(token), (token) => !isStable(token) && isTopN(token, 280)),
      policyReason,
    );
  } else {
    addMany(
      pickPhase3ReasonCandidates(pool, policyTarget, (token) => isHighVolatility(token), (token) => !isStable(token) && isTopN(token, 320)),
      policyReason,
    );
  }

  if (phase2Output.policy_envelope.liquidity_floor_requirement === "tier_1_only") {
    addMany(
      pickPhase3ReasonCandidates(
        pool,
        12,
        (token) => !isMeme(token) && isTopN(token, 35),
        (token) => isDefensive(token),
      ),
      "liquidity_floor:tier_1_only",
    );
  }

  if (phase2Output.policy_envelope.stablecoin_minimum >= 0.35) {
    addMany(
      pickPhase3ReasonCandidates(
        pool,
        10,
        (token) => isStable(token),
        (token) => !isMeme(token) && isTopN(token, 80),
      ),
      "stablecoin_floor_requirement",
    );
  }

  if (phase2Output.policy_envelope.exposure_caps.high_volatility_asset_cap >= 0.15) {
    addMany(
      pickPhase3ReasonCandidates(
        pool,
        14,
        (token) => isHighVolatility(token),
        (token) => !isStable(token) && isTopN(token, 300),
      ),
      "high_volatility_sleeve_available",
    );
  }

  if (phase2Output.allocation_authorization.status === "RESTRICTED") {
    addMany(
      pickPhase3ReasonCandidates(
        pool,
        10,
        (token) => isDefensive(token),
        (token) => !isMeme(token) && isTopN(token, 80),
      ),
      "restricted_authorization_defensive_anchor",
    );
  }

  if (map.size === 0) {
    addMany(pickPhase3ReasonCandidates(pool, 12, (token) => !isMeme(token)), riskReason);
  }

  return map;
}

function sortUniverseTokens(tokens: Phase3UniverseToken[]): Phase3UniverseToken[] {
  return [...tokens].sort(
    (left, right) =>
      right.volume24hUsd - left.volume24hUsd ||
      (left.marketCapRank ?? Number.MAX_SAFE_INTEGER) - (right.marketCapRank ?? Number.MAX_SAFE_INTEGER) ||
      left.symbol.localeCompare(right.symbol),
  );
}

const PHASE3_WRAPPED_SYMBOLS = new Set([
  "WBTC",
  "WETH",
  "WSTETH",
  "STETH",
  "RETH",
  "CBETH",
  "WEETH",
  "WSOL",
  "WBNB",
]);

const PHASE3_NON_RETAIL_ID_PATTERNS = [
  /(^|-)wrapped(-|$)/,
  /xstock/,
  /tokenized-stock/,
  /treasury-fund/,
  /heloc/,
  /(^|-)fan-token(-|$)/,
];

const PHASE3_NON_RETAIL_NAME_PATTERNS = [
  /\bwrapped\b/,
  /xstock/i,
  /tokenized stock/i,
  /treasury fund/i,
  /heloc/i,
  /fan token/i,
];

const PHASE3_TRUSTED_STABLECOIN_IDS = new Set([
  "usd-coin",
  "tether",
  "dai",
  "first-digital-usd",
  "paypal-usd",
  "ethena-usde",
  "global-dollar",
  "gho",
  "euro-coin",
  "ripple-usd",
  "quantoz-usdq",
]);

const PHASE3_STABLECOIN_NAME_PATTERNS = [/usd/i, /stable/i, /dollar/i, /usdt/i, /usdc/i];
const PHASE3_MEME_PATTERNS = [
  /meme/i,
  /doge/i,
  /shib/i,
  /pepe/i,
  /bonk/i,
  /fart/i,
  /trump/i,
  /melania/i,
  /popcat/i,
  /dogwif/i,
];

function isPhase3RetailFilterEnabled(): boolean {
  return readBooleanEnv("PHASE3_RETAIL_FILTER_ENABLED", true);
}

function isPhase4MemeAllowed(): boolean {
  return readBooleanEnv("PHASE4_ALLOW_MEME_TOKENS", false);
}

function getPhase3NonRetailReason(token: Phase3UniverseToken): "wrapped_token" | "non_retail_proxy" | null {
  const id = token.coingeckoId.toLowerCase();
  const name = token.name.toLowerCase();
  const symbol = token.symbol.toUpperCase();

  if (PHASE3_WRAPPED_SYMBOLS.has(symbol)) return "wrapped_token";
  if (PHASE3_NON_RETAIL_ID_PATTERNS.some((pattern) => pattern.test(id))) {
    if (id.includes("wrapped")) return "wrapped_token";
    return "non_retail_proxy";
  }
  if (PHASE3_NON_RETAIL_NAME_PATTERNS.some((pattern) => pattern.test(name))) {
    if (name.includes("wrapped")) return "wrapped_token";
    return "non_retail_proxy";
  }
  return null;
}

function filterPhase3TokensForRetail(
  tokens: Phase3UniverseToken[],
  selectionRules: Set<string>,
  scope: "top_volume" | "profile_match",
): Phase3UniverseToken[] {
  const enabled = isPhase3RetailFilterEnabled();
  selectionRules.add(`phase3_retail_filter_enabled:${enabled}`);
  if (!enabled) return tokens;

  const kept: Phase3UniverseToken[] = [];
  const removedByReason = new Map<string, number>();

  for (const token of tokens) {
    const reason = getPhase3NonRetailReason(token);
    if (!reason) {
      kept.push(token);
      continue;
    }
    removedByReason.set(reason, (removedByReason.get(reason) ?? 0) + 1);
  }

  for (const [reason, count] of removedByReason.entries()) {
    selectionRules.add(`phase3_retail_filter_removed:${scope}:${reason}:${count}`);
  }

  return kept;
}

function isStablecoinToken(token: Phase3UniverseToken): boolean {
  if (PHASE3_TRUSTED_STABLECOIN_IDS.has(token.coingeckoId)) return true;
  const name = token.name.toLowerCase();
  const symbol = token.symbol.toLowerCase();
  return PHASE3_STABLECOIN_NAME_PATTERNS.some((pattern) => pattern.test(name) || pattern.test(symbol));
}

function isMemeToken(token: Phase3UniverseToken): boolean {
  const id = token.coingeckoId.toLowerCase();
  const name = token.name.toLowerCase();
  const symbol = token.symbol.toLowerCase();
  return PHASE3_MEME_PATTERNS.some((pattern) => pattern.test(id) || pattern.test(name) || pattern.test(symbol));
}

function getProfileReasonFallbackCandidates(
  reason: string,
  existingTokenMap: Map<string, Phase3UniverseToken>,
): Phase3UniverseToken[] {
  const candidates = sortUniverseTokens(Array.from(existingTokenMap.values())).filter(
    (token) => token.marketCapRank !== null && token.marketCapRank <= 500,
  );
  if (candidates.length === 0) return [];

  const reasonLower = reason.toLowerCase();
  const isStablecoinReason = reasonLower.includes("stablecoin");
  const isDefensiveReason =
    reasonLower.includes("capital_preservation") ||
    reasonLower.includes("liquidity_floor") ||
    reasonLower.includes("defensive_anchor");
  const isBalancedRiskReason = reasonLower.includes("profile_risk_tolerance:balanced");

  const rankFiltered = candidates.filter((token) => {
    if (isDefensiveReason || isStablecoinReason) return token.marketCapRank !== null && token.marketCapRank <= 100;
    if (isBalancedRiskReason) return token.marketCapRank !== null && token.marketCapRank <= 150;
    return true;
  });
  const pool = rankFiltered.length > 0 ? rankFiltered : candidates;

  const preferredStablecoins = pool.filter(
    (token) =>
      PHASE3_TRUSTED_STABLECOIN_IDS.has(token.coingeckoId) ||
      isStablecoinToken(token),
  );
  if (isStablecoinReason && preferredStablecoins.length > 0) {
    return preferredStablecoins.slice(0, 5);
  }

  if (isDefensiveReason) {
    const defensivePool = pool.filter((token) => !isMemeToken(token));
    if (defensivePool.length > 0) return defensivePool.slice(0, 8);
  }

  const nonMemePool = pool.filter((token) => !isMemeToken(token));
  if (nonMemePool.length > 0) return nonMemePool.slice(0, 8);
  return pool.slice(0, 8);
}

function rebindProfileReasonsToUniverseAnchors(
  unresolvedTokenId: string,
  reasons: Set<string>,
  existingTokenMap: Map<string, Phase3UniverseToken>,
  selectionRules: Set<string>,
): boolean {
  if (reasons.size === 0) return false;

  let reboundCount = 0;
  for (const reason of reasons) {
    const candidates = getProfileReasonFallbackCandidates(reason, existingTokenMap);
    if (candidates.length === 0) continue;

    const selected =
      candidates.find((token) => !token.profileMatchReasons.has(reason)) ??
      candidates[0];
    if (!selected) continue;

    selected.sourceTags.add("profile_match");
    selected.sourceTags.add("profile_reason_rebound");
    selected.profileMatchReasons.add(reason);
    reboundCount += 1;
  }

  if (reboundCount > 0) {
    selectionRules.add(`phase3_profile_token_rebound:${unresolvedTokenId}:${reboundCount}`);
    return true;
  }

  return false;
}

function deriveExchangeDepthProxy(token: Phase3UniverseToken): "high" | "medium" | "low" | "unknown" {
  if (token.volume24hUsd <= 0) return "unknown";
  if (token.marketCapRank !== null && token.marketCapRank <= 100 && token.volume24hUsd >= 5_000_000) return "high";
  if (token.marketCapRank !== null && token.marketCapRank <= 500 && token.volume24hUsd >= 1_000_000) return "medium";
  return "low";
}

function derivePhase3TokenOutput(
  token: Phase3UniverseToken,
): {
  coingecko_id: string;
  symbol: string;
  name: string;
  market_cap_rank: number | null;
  volume_24h_usd: number;
  volume_7d_estimated_usd: number;
  volume_30d_estimated_usd: number;
  price_change_pct_7d: number | null;
  price_change_pct_30d: number | null;
  source_tags: string[];
  profile_match_reasons: string[];
  status: "RESOLVED" | "UNRESOLVED";
  exclude_from_phase4: boolean;
  phase4_exclusion_reasons: string[];
  phase4_screening_hints: {
    token_category: "core" | "stablecoin" | "meme" | "proxy_or_wrapped" | "alt" | "unknown";
    rank_bucket: "top_100" | "top_500" | "long_tail" | "unknown";
    strict_rank_gate_required: boolean;
    exchange_depth_proxy: "high" | "medium" | "low" | "unknown";
    stablecoin_validation_state: "not_stablecoin" | "trusted_stablecoin" | "unverified_stablecoin";
    suspicious_volume_rank_mismatch: boolean;
    meme_token_detected: boolean;
    proxy_or_wrapped_detected: boolean;
  };
} {
  const unresolvedByRank = token.marketCapRank === null;
  const unresolvedByVolume = token.volume24hUsd <= 0;
  const unresolved = unresolvedByRank || unresolvedByVolume;
  const nonRetailReason = getPhase3NonRetailReason(token);
  const proxyOrWrappedDetected = Boolean(nonRetailReason);
  const memeTokenDetected = isMemeToken(token);
  const stablecoin = isStablecoinToken(token);
  const stablecoinValidationState: "not_stablecoin" | "trusted_stablecoin" | "unverified_stablecoin" = stablecoin
    ? PHASE3_TRUSTED_STABLECOIN_IDS.has(token.coingeckoId)
      ? "trusted_stablecoin"
      : "unverified_stablecoin"
    : "not_stablecoin";

  const rankBucket: "top_100" | "top_500" | "long_tail" | "unknown" =
    token.marketCapRank === null ? "unknown" : token.marketCapRank <= 100 ? "top_100" : token.marketCapRank <= 500 ? "top_500" : "long_tail";

  const strictRankGateRequired = rankBucket === "long_tail" || rankBucket === "unknown";
  const exchangeDepthProxy = deriveExchangeDepthProxy(token);
  const suspiciousVolumeRankMismatch = token.marketCapRank !== null && token.marketCapRank > 500 && token.volume24hUsd >= 50_000_000;
  const allowMemeTokens = isPhase4MemeAllowed();

  const phase4ExclusionReasons: string[] = [];
  if (unresolvedByRank) phase4ExclusionReasons.push("unresolved_market_cap_rank");
  if (unresolvedByVolume) phase4ExclusionReasons.push("unresolved_volume_24h");
  if (proxyOrWrappedDetected) phase4ExclusionReasons.push("non_retail_proxy_or_wrapped");
  if (memeTokenDetected && !allowMemeTokens) phase4ExclusionReasons.push("meme_token_not_allowed");
  if (stablecoinValidationState === "unverified_stablecoin") phase4ExclusionReasons.push("unverified_stablecoin");
  if (suspiciousVolumeRankMismatch) phase4ExclusionReasons.push("volume_rank_mismatch_flag");

  const tokenCategory: "core" | "stablecoin" | "meme" | "proxy_or_wrapped" | "alt" | "unknown" =
    proxyOrWrappedDetected
      ? "proxy_or_wrapped"
      : stablecoin
        ? "stablecoin"
        : memeTokenDetected
          ? "meme"
          : token.marketCapRank !== null && token.marketCapRank <= 20
            ? "core"
            : token.marketCapRank !== null
              ? "alt"
              : "unknown";

  const status: "RESOLVED" | "UNRESOLVED" = unresolved ? "UNRESOLVED" : "RESOLVED";
  const excludeFromPhase4 = phase4ExclusionReasons.length > 0;

  return {
    coingecko_id: token.coingeckoId,
    symbol: token.symbol,
    name: token.name,
    market_cap_rank: token.marketCapRank,
    volume_24h_usd: round(token.volume24hUsd, 6),
    volume_7d_estimated_usd: round(token.volume7dEstimatedUsd, 6),
    volume_30d_estimated_usd: round(token.volume30dEstimatedUsd, 6),
    price_change_pct_7d: token.priceChangePct7d === null ? null : round(token.priceChangePct7d, 6),
    price_change_pct_30d: token.priceChangePct30d === null ? null : round(token.priceChangePct30d, 6),
    source_tags: Array.from(token.sourceTags).sort(),
    profile_match_reasons: Array.from(token.profileMatchReasons).sort(),
    status,
    exclude_from_phase4: excludeFromPhase4,
    phase4_exclusion_reasons: phase4ExclusionReasons.sort(),
    phase4_screening_hints: {
      token_category: tokenCategory,
      rank_bucket: rankBucket,
      strict_rank_gate_required: strictRankGateRequired,
      exchange_depth_proxy: exchangeDepthProxy,
      stablecoin_validation_state: stablecoinValidationState,
      suspicious_volume_rank_mismatch: suspiciousVolumeRankMismatch,
      meme_token_detected: memeTokenDetected,
      proxy_or_wrapped_detected: proxyOrWrappedDetected,
    },
  };
}

async function collectPhase3TopVolumeTokens(
  topTarget: number,
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  selectionRules: Set<string>,
  missingDomains: Set<string>,
): Promise<Phase3UniverseToken[]> {
  const configuredTopVolumeOrder = parseOrderedSources(
    "PHASE3_TOP_VOLUME_SOURCE_ORDER",
    DEFAULT_PHASE3_TOP_VOLUME_SOURCE_ORDER,
  );
  const topVolumeDiscoveryPool = parseOrderedSources(
    "PHASE3_DISCOVERY_TOP_VOLUME_POOL",
    DEFAULT_PHASE3_TOP_VOLUME_DISCOVERY_POOL,
  );
  const providerOrder = buildProviderOrder("market_metrics", configuredTopVolumeOrder, topVolumeDiscoveryPool);
  selectionRules.add(`phase3_top_volume_provider_order:${providerOrder.join(">")}`);
  const merged = new Map<string, Phase3UniverseToken>();

  for (const provider of providerOrder) {
    if (merged.size >= topTarget) break;
    const startedAt = Date.now();

    try {
      if (provider === "coingecko") {
        const pageSize = 250;
        // Pull enough pages to fill the target and keep page size fixed for API consistency.
        const pageCount = Math.max(1, Math.ceil(topTarget / pageSize));
        let providerAdded = 0;

        for (let page = 1; page <= pageCount; page += 1) {
          const toolId = `coingecko:top_volume_page_${page}`;
          const url =
            `https://api.coingecko.com/api/v3/coins/markets` +
            `?vs_currency=usd&order=volume_desc&per_page=${pageSize}&page=${page}` +
            `&price_change_percentage=7d,30d`;
          const payload = await fetchJson<unknown>(url, toolCalls, toolId, sourceReferences);
          const parsedTokens = parseCoinGeckoUniverseRows(payload, "top_volume_7_30d_proxy");
          const beforeSize = merged.size;
          for (const token of parsedTokens) mergeUniverseTokens(merged, token);
          providerAdded += merged.size - beforeSize;
          if (parsedTokens.length < pageSize) break;
          if (merged.size >= topTarget) break;
        }

        if (providerAdded > 0) {
          recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
          selectionRules.add(`phase3_top_volume_provider_selected:${provider}`);
          selectionRules.add("phase3_top_volume_basis:coingecko_volume_desc_proxy_7_30d");
          selectionRules.add(`phase3_top_volume_provider_added:${provider}:${providerAdded}`);
          continue;
        }

        recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
        continue;
      }

      if (provider === "coinlore") {
        const needed = Math.max(1, topTarget - merged.size);
        const limit = Math.min(500, Math.max(topTarget, needed));
        const payload = await fetchJson<unknown>(
          `https://api.coinlore.net/api/tickers/?start=0&limit=${limit}`,
          toolCalls,
          "coinlore:top_volume_universe",
          sourceReferences,
        );
        const parsed = parseCoinLoreUniverseRows(payload, "top_volume_7_30d_proxy");
        const beforeSize = merged.size;
        for (const token of parsed) {
          mergeUniverseTokens(merged, token);
        }
        const providerAdded = merged.size - beforeSize;
        if (providerAdded > 0) {
          recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
          selectionRules.add(`phase3_top_volume_provider_selected:${provider}`);
          selectionRules.add("phase3_top_volume_basis:coinlore_volume24_proxy_7_30d");
          selectionRules.add(`phase3_top_volume_provider_added:${provider}:${providerAdded}`);
          continue;
        }

        recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
        continue;
      }

      if (provider === "coinpaprika") {
        const needed = Math.max(1, topTarget - merged.size);
        const limit = Math.min(500, Math.max(topTarget, needed));
        const payload = await fetchJson<unknown>(
          `https://api.coinpaprika.com/v1/tickers?quotes=USD&limit=${limit}`,
          toolCalls,
          "coinpaprika:top_volume_universe",
          sourceReferences,
        );
        const parsed = parseCoinPaprikaUniverseRows(payload, "top_volume_7_30d_proxy");
        const beforeSize = merged.size;
        for (const token of parsed) {
          mergeUniverseTokens(merged, token);
        }
        const providerAdded = merged.size - beforeSize;
        if (providerAdded > 0) {
          recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
          selectionRules.add(`phase3_top_volume_provider_selected:${provider}`);
          selectionRules.add("phase3_top_volume_basis:coinpaprika_volume24_proxy_7_30d");
          selectionRules.add(`phase3_top_volume_provider_added:${provider}:${providerAdded}`);
          continue;
        }

        recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
        continue;
      }

      if (provider === "coinmarketcap") {
        const apiKey = getCoinMarketCapApiKey();
        if (!apiKey) {
          recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
          selectionRules.add("phase3_top_volume_provider_skipped:coinmarketcap:api_key_missing");
          continue;
        }
        const needed = Math.max(1, topTarget - merged.size);
        const limit = Math.min(500, Math.max(topTarget, needed));
        const payload = await fetchJson<unknown>(
          `https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?start=1&limit=${limit}&convert=USD&sort=volume_24h&sort_dir=desc`,
          toolCalls,
          "coinmarketcap:top_volume_universe",
          sourceReferences,
          {
            "X-CMC_PRO_API_KEY": apiKey,
          },
        );
        const parsed = parseCoinMarketCapUniverseRows(payload, "top_volume_7_30d_proxy");
        const beforeSize = merged.size;
        for (const token of parsed) {
          mergeUniverseTokens(merged, token);
        }
        const providerAdded = merged.size - beforeSize;
        if (providerAdded > 0) {
          recordSourceOutcome("market_metrics", provider, true, Date.now() - startedAt);
          selectionRules.add(`phase3_top_volume_provider_selected:${provider}`);
          selectionRules.add("phase3_top_volume_basis:coinmarketcap_volume24_proxy_7_30d");
          selectionRules.add(`phase3_top_volume_provider_added:${provider}:${providerAdded}`);
          continue;
        }

        recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
        continue;
      }

      selectionRules.add(`phase3_top_volume_provider_unsupported:${provider}`);
    } catch (error) {
      recordSourceOutcome("market_metrics", provider, false, Date.now() - startedAt);
      selectionRules.add(
        `phase3_top_volume_provider_error:${provider}:${error instanceof Error ? error.message : "unknown_error"}`,
      );
    }
  }

  if (merged.size === 0) {
    missingDomains.add("phase3_top_volume_universe_unavailable");
    return [];
  }
  if (merged.size < topTarget) {
    missingDomains.add(`phase3_top_volume_under_target:${merged.size}/${topTarget}`);
  }

  return sortUniverseTokens(Array.from(merged.values()));
}

function chunkArray<T>(input: T[], size: number): T[][] {
  if (size <= 0) return [input];
  const chunks: T[][] = [];
  for (let index = 0; index < input.length; index += size) {
    chunks.push(input.slice(index, index + size));
  }
  return chunks;
}

async function fetchCoinGeckoProfileChunkWithRetry(
  ids: string[],
  chunkIndex: number,
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  selectionRules: Set<string>,
): Promise<Phase3UniverseToken[]> {
  const joinedIds = ids.map((id) => encodeURIComponent(id)).join(",");
  const maxAttempts = 3;
  const toolId = `coingecko:profile_match_tokens_${chunkIndex + 1}`;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const payload = await fetchJson<unknown>(
        `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=${joinedIds}&price_change_percentage=7d,30d`,
        toolCalls,
        toolId,
        sourceReferences,
      );
      if (attempt > 1) {
        selectionRules.add(`phase3_profile_fetch_retry_success_attempt:${attempt}`);
      }
      return parseCoinGeckoUniverseRows(payload, "profile_match");
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown_error";
      const shouldRetry = message.includes("HTTP 429") || message.includes("HTTP 5");
      if (!shouldRetry || attempt >= maxAttempts) {
        throw error;
      }
      const backoffMs = 1200 * attempt;
      selectionRules.add(`phase3_profile_fetch_retry:${toolId}:attempt_${attempt}:delay_${backoffMs}ms`);
      await sleep(backoffMs);
    }
  }

  return [];
}

type CoinGeckoSearchCandidate = {
  id: string;
  symbol: string;
  name: string;
  marketCapRank: number | null;
};

function parseCoinGeckoSearchCandidates(payload: unknown): CoinGeckoSearchCandidate[] {
  if (!payload || typeof payload !== "object") return [];
  const coins = (payload as Record<string, unknown>).coins;
  if (!Array.isArray(coins)) return [];

  const parsed: CoinGeckoSearchCandidate[] = [];
  for (const candidate of coins) {
    if (!candidate || typeof candidate !== "object") continue;
    const record = candidate as Record<string, unknown>;
    const id = typeof record.id === "string" ? normalizeTokenId(record.id) : "";
    if (!id) continue;
    const symbol = typeof record.symbol === "string" ? normalizeTokenSymbol(record.symbol) : "";
    const name = typeof record.name === "string" ? record.name.trim() : "";
    const marketCapRank = Number(record.market_cap_rank);
    parsed.push({
      id,
      symbol,
      name,
      marketCapRank: Number.isFinite(marketCapRank) && marketCapRank > 0 ? Math.floor(marketCapRank) : null,
    });
  }

  return parsed;
}

function scoreCoinGeckoSearchCandidate(
  candidate: CoinGeckoSearchCandidate,
  expectedId: string,
  expectedSymbol: string,
  expectedName: string,
): number {
  const candidateId = candidate.id;
  const candidateSymbol = candidate.symbol;
  const candidateName = candidate.name.toLowerCase();
  const expectedNameNormalized = expectedName.toLowerCase();
  let score = 0;

  if (candidateId === expectedId) score += 100;
  if (expectedSymbol && candidateSymbol === expectedSymbol) score += 90;
  if (expectedNameNormalized && candidateName === expectedNameNormalized) score += 70;
  if (expectedId && (candidateId.includes(expectedId) || expectedId.includes(candidateId))) score += 20;
  if (expectedSymbol && candidateId.includes(expectedSymbol.toLowerCase())) score += 10;
  if (candidate.marketCapRank !== null) score += Math.max(0, 25 - Math.min(candidate.marketCapRank, 25));

  return score;
}

async function discoverPhase3ProfileTokenAlias(
  tokenId: string,
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  selectionRules: Set<string>,
): Promise<string | null> {
  const normalizedTokenId = normalizeTokenId(tokenId);
  const expectedSymbol = buildProfileFallbackSymbol(normalizedTokenId);
  const expectedName = buildProfileFallbackName(normalizedTokenId);
  const queryCandidates = Array.from(
    new Set(
      [
        expectedSymbol,
        expectedName,
        normalizedTokenId.replace(/-/g, " "),
        normalizedTokenId,
      ]
        .map((value) => value.trim())
        .filter((value) => value.length >= 2),
    ),
  ).slice(0, 4);
  if (queryCandidates.length === 0) return null;

  let bestCandidate: { id: string; score: number } | null = null;
  for (let queryIndex = 0; queryIndex < queryCandidates.length; queryIndex += 1) {
    const query = queryCandidates[queryIndex];
    const toolId = `coingecko:profile_token_search_${normalizedTokenId}_${queryIndex + 1}`;

    try {
      const payload = await fetchJson<unknown>(
        `https://api.coingecko.com/api/v3/search?query=${encodeURIComponent(query)}`,
        toolCalls,
        toolId,
        sourceReferences,
      );
      const candidates = parseCoinGeckoSearchCandidates(payload);
      for (const candidate of candidates) {
        const score = scoreCoinGeckoSearchCandidate(candidate, normalizedTokenId, expectedSymbol, expectedName);
        if (!bestCandidate || score > bestCandidate.score) {
          bestCandidate = { id: candidate.id, score };
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown_error";
      selectionRules.add(`phase3_profile_token_search_error:${normalizedTokenId}:${message}`);
    }
  }

  if (!bestCandidate || bestCandidate.score < 80) {
    return null;
  }

  if (bestCandidate.id !== normalizedTokenId) {
    registerPhase3ProfileTokenAlias(normalizedTokenId, bestCandidate.id, selectionRules);
    selectionRules.add(`phase3_profile_token_search_alias:${normalizedTokenId}->${bestCandidate.id}`);
  }

  return bestCandidate.id;
}

async function fetchCoinGeckoProfileTokenFallback(
  tokenId: string,
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  selectionRules: Set<string>,
  allowAliasDiscovery = true,
): Promise<Phase3UniverseToken | null> {
  const normalizedTokenId = canonicalizePhase3ProfileTokenId(tokenId);
  const encodedTokenId = encodeURIComponent(normalizedTokenId);
  const toolId = `coingecko:profile_token_${tokenId}`;
  const maxAttempts = 2;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const payload = await fetchJson<unknown>(
        `https://api.coingecko.com/api/v3/coins/${encodedTokenId}`,
        toolCalls,
        toolId,
        sourceReferences,
      );
      if (!payload || typeof payload !== "object") return null;
      const record = payload as Record<string, unknown>;
      const canonicalRecordId = normalizeTokenId(
        typeof record.id === "string" && record.id.trim() ? record.id : normalizedTokenId,
      );
      if (canonicalRecordId && canonicalRecordId !== normalizedTokenId) {
        registerPhase3ProfileTokenAlias(normalizedTokenId, canonicalRecordId, selectionRules);
      }
      const effectiveTokenId = canonicalRecordId || normalizedTokenId;
      const marketData =
        record.market_data && typeof record.market_data === "object"
          ? (record.market_data as Record<string, unknown>)
          : {};
      const totalVolume =
        marketData.total_volume && typeof marketData.total_volume === "object"
          ? (marketData.total_volume as Record<string, unknown>)
          : {};
      const symbol =
        typeof record.symbol === "string" && record.symbol.trim()
          ? record.symbol
          : buildProfileFallbackSymbol(effectiveTokenId);
      const name =
        typeof record.name === "string" && record.name.trim()
          ? record.name
          : buildProfileFallbackName(effectiveTokenId);
      const marketCapRank = Number(record.market_cap_rank);
      const volume24hUsd = parsePositiveNumber(totalVolume.usd);
      const priceChangePct7d = parseNullableNumber(
        marketData.price_change_percentage_7d_in_currency ?? marketData.price_change_percentage_7d,
      );
      const priceChangePct30d = parseNullableNumber(
        marketData.price_change_percentage_30d_in_currency ?? marketData.price_change_percentage_30d,
      );

      if (attempt > 1) {
        selectionRules.add(`phase3_profile_token_retry_success:${tokenId}:attempt_${attempt}`);
      }

      return toUniverseToken({
        coingeckoId: effectiveTokenId,
        symbol,
        name,
        marketCapRank: Number.isFinite(marketCapRank) ? marketCapRank : null,
        volume24hUsd,
        priceChangePct7d,
        priceChangePct30d,
        sourceTags: ["profile_match"],
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown_error";
      const shouldRetry = message.includes("HTTP 429") || message.includes("HTTP 5");
      if (!shouldRetry || attempt >= maxAttempts) {
        if (message.includes("HTTP 429")) {
          selectionRules.add(`phase3_profile_token_rate_limited:${normalizedTokenId}`);
          return buildProfileFallbackToken(normalizedTokenId, new Set());
        }
        if (allowAliasDiscovery) {
          const discoveredTokenId = await discoverPhase3ProfileTokenAlias(
            normalizedTokenId,
            toolCalls,
            sourceReferences,
            selectionRules,
          );
          if (discoveredTokenId && discoveredTokenId !== normalizedTokenId) {
            const discoveredToken = await fetchCoinGeckoProfileTokenFallback(
              discoveredTokenId,
              toolCalls,
              sourceReferences,
              selectionRules,
              false,
            );
            if (discoveredToken) {
              selectionRules.add(`phase3_profile_token_resolved_after_search:${normalizedTokenId}->${discoveredTokenId}`);
              return discoveredToken;
            }
          }
        }
        selectionRules.add(`phase3_profile_token_fetch_error:${normalizedTokenId}:${message}`);
        return null;
      }
      const backoffMs = 1500 * attempt;
      selectionRules.add(`phase3_profile_token_retry:${normalizedTokenId}:attempt_${attempt}:delay_${backoffMs}ms`);
      await sleep(backoffMs);
    }
  }

  return null;
}

async function collectPhase3ProfileMatchTokens(
  phase2Output: Phase2Output,
  existingTokenMap: Map<string, Phase3UniverseToken>,
  toolCalls: Set<string>,
  sourceReferences: Map<string, SourceReference>,
  selectionRules: Set<string>,
  missingDomains: Set<string>,
): Promise<Phase3UniverseToken[]> {
  const reasonMap = buildPhase3ProfileReasonMap(phase2Output, existingTokenMap);
  const allProfileIds = Array.from(reasonMap.keys());
  const minCoinGeckoIntervalMs = getPhase3CoinGeckoMinIntervalMs();
  selectionRules.add("phase3_profile_reason_selection:dynamic_universe");
  selectionRules.add(`phase3_profile_token_reason_count:${allProfileIds.length}`);
  selectionRules.add(`phase3_coingecko_min_interval_ms:${minCoinGeckoIntervalMs}`);

  for (const [tokenId, reasons] of reasonMap.entries()) {
    const existing = existingTokenMap.get(tokenId);
    if (!existing) continue;
    existing.sourceTags.add("profile_match");
    for (const reason of reasons) existing.profileMatchReasons.add(reason);
  }

  const unresolvedIds = allProfileIds.filter((tokenId) => !existingTokenMap.has(tokenId));
  if (unresolvedIds.length === 0) {
    return [];
  }

  const limitedIds = unresolvedIds.slice(0, DEFAULT_PHASE3_MAX_PROFILE_TOKEN_TARGET);
  if (limitedIds.length < unresolvedIds.length) {
    selectionRules.add(`phase3_profile_token_limit_applied:${limitedIds.length}`);
  }
  const chunks = chunkArray(limitedIds, 120);
  const collected: Phase3UniverseToken[] = [];
  const resolvedIds = new Set<string>();

  for (let index = 0; index < chunks.length; index += 1) {
    const ids = chunks[index];
    const startedAt = Date.now();
    try {
      const tokens = await fetchCoinGeckoProfileChunkWithRetry(
        ids,
        index,
        toolCalls,
        sourceReferences,
        selectionRules,
      );
      for (const token of tokens) {
        const reasons = reasonMap.get(token.coingeckoId);
        if (reasons) {
          for (const reason of reasons) token.profileMatchReasons.add(reason);
        }
        token.sourceTags.add("profile_match");
        resolvedIds.add(token.coingeckoId);
        collected.push(token);
      }
      recordSourceOutcome("market_metrics", "coingecko", true, Date.now() - startedAt);
    } catch (error) {
      recordSourceOutcome("market_metrics", "coingecko", false, Date.now() - startedAt);
      const message = error instanceof Error ? error.message : "unknown_error";
      if (message.includes("HTTP 429")) {
        selectionRules.add(`phase3_profile_fetch_rate_limited:chunk_${index + 1}`);
      } else {
        selectionRules.add(`phase3_profile_fetch_error:${message}`);
      }
    } finally {
      if (minCoinGeckoIntervalMs > 0 && index < chunks.length - 1) {
        await sleep(minCoinGeckoIntervalMs);
      }
    }
  }

  for (const tokenId of limitedIds) {
    if (resolvedIds.has(tokenId)) continue;
    const fallbackToken = await fetchCoinGeckoProfileTokenFallback(
      tokenId,
      toolCalls,
      sourceReferences,
      selectionRules,
    );
    if (fallbackToken) {
      const reasons = reasonMap.get(tokenId);
      if (reasons) {
        for (const reason of reasons) fallbackToken.profileMatchReasons.add(reason);
      }
      fallbackToken.sourceTags.add("profile_match");
      collected.push(fallbackToken);
      resolvedIds.add(tokenId);
      continue;
    }

    const reasons = reasonMap.get(tokenId) ?? new Set<string>();
    if (rebindProfileReasonsToUniverseAnchors(tokenId, reasons, existingTokenMap, selectionRules)) {
      missingDomains.add(`phase3_profile_token_rebound:${tokenId}`);
      continue;
    }
    const placeholder = buildProfileFallbackToken(tokenId, reasons);
    const unresolvedReason = selectionRules.has(`phase3_profile_token_rate_limited:${tokenId}`)
      ? "rate_limited_placeholder"
      : "unresolved_placeholder";
    placeholder.sourceTags.add(unresolvedReason);
    collected.push(placeholder);
    missingDomains.add(`phase3_profile_token_unresolved:${tokenId}`);
    if (unresolvedReason === "rate_limited_placeholder") {
      missingDomains.add(`phase3_profile_token_rate_limited:${tokenId}`);
    }
  }

  return collected;
}

function sanitizePhase3ForSchema(output: Phase3Output): Phase3Output {
  return {
    timestamp: String(output.timestamp),
    execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
    doctrine_version: PHASE3_DOCTRINE_VERSION,
    inputs: {
      phase2_policy_ref: String(output.inputs.phase2_policy_ref),
      user_profile: {
        risk_tolerance: output.inputs.user_profile.risk_tolerance,
        investment_timeframe: output.inputs.user_profile.investment_timeframe,
      },
      top_volume_target: Math.min(500, Math.max(50, Math.floor(output.inputs.top_volume_target))),
      volume_window_days: [7, 30],
    },
    universe: {
      top_volume_candidates_count: Math.max(0, Math.floor(output.universe.top_volume_candidates_count)),
      profile_match_candidates_count: Math.max(0, Math.floor(output.universe.profile_match_candidates_count)),
      total_candidates_count: Math.max(0, Math.floor(output.universe.total_candidates_count)),
      tokens: output.universe.tokens.map((token) => ({
        coingecko_id: String(token.coingecko_id),
        symbol: normalizeTokenSymbol(String(token.symbol)),
        name: String(token.name),
        market_cap_rank:
          token.market_cap_rank === null
            ? null
            : Math.max(1, Math.floor(Number(token.market_cap_rank) || 1)),
        volume_24h_usd: round(Math.max(0, Number(token.volume_24h_usd) || 0), 6),
        volume_7d_estimated_usd: round(Math.max(0, Number(token.volume_7d_estimated_usd) || 0), 6),
        volume_30d_estimated_usd: round(Math.max(0, Number(token.volume_30d_estimated_usd) || 0), 6),
        price_change_pct_7d: (() => {
          const parsed = parseNullableNumber(token.price_change_pct_7d);
          return parsed === null ? null : round(parsed, 6);
        })(),
        price_change_pct_30d: (() => {
          const parsed = parseNullableNumber(token.price_change_pct_30d);
          return parsed === null ? null : round(parsed, 6);
        })(),
        source_tags: token.source_tags.map((tag) => String(tag)),
        profile_match_reasons: token.profile_match_reasons.map((reason) => String(reason)),
        status: token.status === "UNRESOLVED" ? "UNRESOLVED" : "RESOLVED",
        exclude_from_phase4: Boolean(token.exclude_from_phase4),
        phase4_exclusion_reasons: Array.isArray(token.phase4_exclusion_reasons)
          ? token.phase4_exclusion_reasons.map((reason) => String(reason))
          : [],
        phase4_screening_hints: {
          token_category:
            token.phase4_screening_hints?.token_category === "core" ||
            token.phase4_screening_hints?.token_category === "stablecoin" ||
            token.phase4_screening_hints?.token_category === "meme" ||
            token.phase4_screening_hints?.token_category === "proxy_or_wrapped" ||
            token.phase4_screening_hints?.token_category === "alt" ||
            token.phase4_screening_hints?.token_category === "unknown"
              ? token.phase4_screening_hints.token_category
              : "unknown",
          rank_bucket:
            token.phase4_screening_hints?.rank_bucket === "top_100" ||
            token.phase4_screening_hints?.rank_bucket === "top_500" ||
            token.phase4_screening_hints?.rank_bucket === "long_tail" ||
            token.phase4_screening_hints?.rank_bucket === "unknown"
              ? token.phase4_screening_hints.rank_bucket
              : "unknown",
          strict_rank_gate_required: Boolean(token.phase4_screening_hints?.strict_rank_gate_required),
          exchange_depth_proxy:
            token.phase4_screening_hints?.exchange_depth_proxy === "high" ||
            token.phase4_screening_hints?.exchange_depth_proxy === "medium" ||
            token.phase4_screening_hints?.exchange_depth_proxy === "low" ||
            token.phase4_screening_hints?.exchange_depth_proxy === "unknown"
              ? token.phase4_screening_hints.exchange_depth_proxy
              : "unknown",
          stablecoin_validation_state:
            token.phase4_screening_hints?.stablecoin_validation_state === "trusted_stablecoin" ||
            token.phase4_screening_hints?.stablecoin_validation_state === "unverified_stablecoin" ||
            token.phase4_screening_hints?.stablecoin_validation_state === "not_stablecoin"
              ? token.phase4_screening_hints.stablecoin_validation_state
              : "not_stablecoin",
          suspicious_volume_rank_mismatch: Boolean(token.phase4_screening_hints?.suspicious_volume_rank_mismatch),
          meme_token_detected: Boolean(token.phase4_screening_hints?.meme_token_detected),
          proxy_or_wrapped_detected: Boolean(token.phase4_screening_hints?.proxy_or_wrapped_detected),
        },
      })),
    },
    phase_boundaries: {
      asset_screening: "PHASE_4",
      portfolio_construction: "PHASE_4",
    },
    audit: {
      sources: output.audit.sources.map((source) => ({
        id: String(source.id),
        provider: String(source.provider),
        endpoint: String(source.endpoint),
        url: String(source.url),
        fetched_at: String(source.fetched_at),
      })),
      selection_rules: output.audit.selection_rules.map((rule) => String(rule)),
      missing_domains: output.audit.missing_domains.map((domain) => String(domain)),
      agent_profile_match: {
        used: Boolean(output.audit.agent_profile_match.used),
        model: output.audit.agent_profile_match.model ? String(output.audit.agent_profile_match.model) : null,
        reason_codes: output.audit.agent_profile_match.reason_codes.map((reason) => String(reason)),
        skipped_reason: output.audit.agent_profile_match.skipped_reason
          ? String(output.audit.agent_profile_match.skipped_reason)
          : null,
      },
    },
  };
}

function validatePhase3OutputWithRetry(buildCandidate: () => Phase3Output): Phase3Output {
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    const candidate = attempt === 1 ? buildCandidate() : sanitizePhase3ForSchema(buildCandidate());
    const parsed = phase3OutputSchema.safeParse(candidate);
    if (parsed.success) return parsed.data;
  }

  throw new Error("Phase 3 output validation failed after one retry.");
}

type Phase3OutputToken = Phase3Output["universe"]["tokens"][number];
type Phase4OutputToken = Phase4Output["screening"]["tokens"][number];
type Phase5PortfolioBucket = "stablecoin" | "core" | "satellite" | "high_volatility";
type Phase5RiskClass = (typeof PHASE5_RISK_CLASSES)[number];
type Phase5Role = (typeof PHASE5_ROLES)[number];
type StablecoinClassificationToken = {
  coingecko_id: string;
  symbol: string;
  name: string;
  rank_bucket?: Phase4OutputToken["rank_bucket"];
  exchange_depth_proxy?: Phase4OutputToken["exchange_depth_proxy"];
  stablecoin_validation_state?: Phase4OutputToken["stablecoin_validation_state"];
  liquidity_score?: number;
  structural_score?: number;
};

type Phase6AllocationToken = {
  coingecko_id: string;
  symbol: string;
  name: string;
  market_cap_rank: number | null;
  token_category: Phase4OutputToken["token_category"];
  rank_bucket: Phase4OutputToken["rank_bucket"];
  exchange_depth_proxy: Phase4OutputToken["exchange_depth_proxy"];
  stablecoin_validation_state: Phase4OutputToken["stablecoin_validation_state"];
  liquidity_score: number;
  structural_score: number;
};

type Phase6AllocationCandidate = {
  token: Phase6AllocationToken;
  qualityScore: number;
  riskScore: number;
  compositeScore: number;
  profileBoost: number;
  bucket: Phase5PortfolioBucket;
};

type Phase4Thresholds = {
  minLiquidityScore: number;
  minStructuralScore: number;
  minScreeningScore: number;
  minVolume24hUsd: number;
  allowLowDepth: boolean;
  targetEligibleCount: number;
  rankSanityThreshold: number;
};

type Phase4CoverageRecoveryStep = {
  minLiquidityDelta: number;
  minStructuralDelta: number;
  minScreeningDelta: number;
  minVolume24hUsdFloor: number;
};

type Phase4LaneDiagnostics = {
  coreEligibleCount: number;
  coverageFillEligibleCount: number;
  coverageFillCap: number;
  stablecoinCapCount: number;
  stablecoinMaxShare: number;
  demotedByCoverageFill: number;
  demotedByStablecoinCap: number;
  demotedByStablecoinIssuer: number;
  demotedByStablecoinCluster: number;
};

type Phase5ScoredCandidate = {
  token: Phase4OutputToken;
  qualityScore: number;
  riskScore: number;
  volatilityProxyScore: number;
  drawdownProxyScore: number;
  stablecoinRiskModifier: number;
  compositeScore: number;
  profitability: number;
  volatility: number;
  riskClass: Phase5RiskClass;
  role: Phase5Role;
  profileBoost: number;
  bucket: Phase5PortfolioBucket;
};

type Phase5AgentScoringConstraints = {
  riskBudget: number;
  stablecoinMinimum: number;
  maxSingleAssetExposure: number;
  highVolatilityAssetCap: number;
};

type Phase5AgentScoringInstructionPack = {
  version: string;
  objective: string;
  input_contract: string;
  gating_rules: string[];
  scoring_rules: string[];
  risk_class_rules: string[];
  role_rules: string[];
  ranking_rules: string[];
};

type Phase5AgentScoringRun = {
  instructionPack: Phase5AgentScoringInstructionPack;
  scoredCandidates: Phase5ScoredCandidate[];
  provider: string;
  transport: "deterministic_rules" | "model_json_schema";
};

const PHASE4_MIN_ELIGIBLE_COVERAGE = 25;
const PHASE4_TARGET_ELIGIBLE_BASELINE = 50;
const PHASE4_RECOVERY_MIN_LIQUIDITY_FLOOR = 0.4;
const PHASE4_RECOVERY_MIN_STRUCTURAL_FLOOR = 0.57;
const PHASE4_RECOVERY_MIN_SCREENING_FLOOR = 0.51;
const PHASE4_COVERAGE_FILL_MAX_SHARE = 0.4;
const PHASE4_STABLECOIN_MAX_SHARE_BUFFER = 0.22;
const PHASE4_STABLECOIN_MAX_SHARE_FLOOR = 0.25;
const PHASE4_STABLECOIN_MAX_SHARE_CEILING = 0.45;
const PHASE4_STABLECOIN_ISSUER_MAX_SHARE_OF_STABLE_ELIGIBLE = 0.6;
const PHASE4_STABLECOIN_CLUSTER_MAX_SHARE_OF_STABLE_ELIGIBLE = 0.75;
const PHASE4_COVERAGE_RECOVERY_STEPS: Phase4CoverageRecoveryStep[] = [
  {
    minLiquidityDelta: 0.04,
    minStructuralDelta: 0.02,
    minScreeningDelta: 0.03,
    minVolume24hUsdFloor: 12_000_000,
  },
  {
    minLiquidityDelta: 0.05,
    minStructuralDelta: 0.03,
    minScreeningDelta: 0.04,
    minVolume24hUsdFloor: 10_000_000,
  },
  {
    minLiquidityDelta: 0.06,
    minStructuralDelta: 0.03,
    minScreeningDelta: 0.05,
    minVolume24hUsdFloor: 8_000_000,
  },
  {
    minLiquidityDelta: 0.05,
    minStructuralDelta: 0.02,
    minScreeningDelta: 0.04,
    minVolume24hUsdFloor: 6_000_000,
  },
];

const PHASE5_AGENT_SCORING_RULEBOOK_VERSION = "SELUN-PHASE5-AGENT-SCORING-RULES-1.0";
const PHASE5_AGENT_SCORING_PROVIDER = "messari-chat-completions";
const PHASE5_AGENT_SCORING_ENDPOINT = "https://api.messari.io/ai/v1/chat/completions";
const PHASE5_AGENT_SCORING_DEFAULT_PROVIDER_MODE = "deterministic";
const PHASE5_MAX_SELECTED_STABLECOINS = Math.max(
  1,
  readPositiveIntEnv("PHASE5_MAX_SELECTED_STABLECOINS", 1),
);

function uniqueSorted(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => String(value)))).sort();
}

function hashReference(payload: unknown): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(payload)).digest("hex")}`;
}

function normalizeVolumeSignal(value: number, floor: number, ceiling: number): number {
  if (!Number.isFinite(value) || value <= 0) return 0;
  if (!Number.isFinite(floor) || floor <= 0) floor = 1;
  if (!Number.isFinite(ceiling) || ceiling <= floor) ceiling = floor * 2;
  const numerator = Math.log10(value + 1) - Math.log10(floor + 1);
  const denominator = Math.log10(ceiling + 1) - Math.log10(floor + 1);
  if (!Number.isFinite(denominator) || denominator <= 0) return 0;
  return clamp(numerator / denominator, 0, 1);
}

function depthScore(depth: Phase3OutputToken["phase4_screening_hints"]["exchange_depth_proxy"]): number {
  if (depth === "high") return 1;
  if (depth === "medium") return 0.78;
  if (depth === "low") return 0.42;
  return 0.2;
}

function rankBucketScore(rankBucket: Phase3OutputToken["phase4_screening_hints"]["rank_bucket"]): number {
  if (rankBucket === "top_100") return 1;
  if (rankBucket === "top_500") return 0.75;
  if (rankBucket === "long_tail") return 0.4;
  return 0.15;
}

function tokenCategoryScore(tokenCategory: Phase3OutputToken["phase4_screening_hints"]["token_category"]): number {
  if (tokenCategory === "core") return 0.95;
  if (tokenCategory === "stablecoin") return 0.92;
  if (tokenCategory === "alt") return 0.72;
  if (tokenCategory === "meme") return 0.22;
  if (tokenCategory === "proxy_or_wrapped") return 0.05;
  return 0.2;
}

function stablecoinValidationScore(
  state: Phase3OutputToken["phase4_screening_hints"]["stablecoin_validation_state"],
): number {
  if (state === "trusted_stablecoin") return 0.95;
  if (state === "unverified_stablecoin") return 0.2;
  return 0.82;
}

function derivePhase4Thresholds(
  riskTolerance: UserRiskTolerance,
  policyMode: AllocationPolicyMode,
): Phase4Thresholds {
  const baseByRisk: Record<UserRiskTolerance, Phase4Thresholds> = {
    Conservative: {
      minLiquidityScore: 0.62,
      minStructuralScore: 0.7,
      minScreeningScore: 0.7,
      minVolume24hUsd: 20_000_000,
      allowLowDepth: false,
      targetEligibleCount: 50,
      rankSanityThreshold: 500,
    },
    Balanced: {
      minLiquidityScore: 0.55,
      minStructuralScore: 0.62,
      minScreeningScore: 0.62,
      minVolume24hUsd: 10_000_000,
      allowLowDepth: false,
      targetEligibleCount: 50,
      rankSanityThreshold: 500,
    },
    Growth: {
      minLiquidityScore: 0.48,
      minStructuralScore: 0.52,
      minScreeningScore: 0.54,
      minVolume24hUsd: 5_000_000,
      allowLowDepth: true,
      targetEligibleCount: 56,
      rankSanityThreshold: 500,
    },
    Aggressive: {
      minLiquidityScore: 0.4,
      minStructuralScore: 0.45,
      minScreeningScore: 0.48,
      minVolume24hUsd: 2_500_000,
      allowLowDepth: true,
      targetEligibleCount: 64,
      rankSanityThreshold: 500,
    },
  };

  const thresholds = { ...baseByRisk[riskTolerance] };
  if (policyMode === "capital_preservation") {
    thresholds.minLiquidityScore = clamp(thresholds.minLiquidityScore + 0.05, 0, 1);
    thresholds.minStructuralScore = clamp(thresholds.minStructuralScore + 0.05, 0, 1);
    thresholds.minScreeningScore = clamp(thresholds.minScreeningScore + 0.05, 0, 1);
    thresholds.minVolume24hUsd = Math.max(thresholds.minVolume24hUsd, 15_000_000);
    thresholds.allowLowDepth = false;
    thresholds.targetEligibleCount = Math.max(PHASE4_TARGET_ELIGIBLE_BASELINE, thresholds.targetEligibleCount - 4);
  } else if (policyMode === "offensive_growth") {
    thresholds.minLiquidityScore = clamp(thresholds.minLiquidityScore - 0.03, 0, 1);
    thresholds.minStructuralScore = clamp(thresholds.minStructuralScore - 0.03, 0, 1);
    thresholds.minScreeningScore = clamp(thresholds.minScreeningScore - 0.03, 0, 1);
    thresholds.targetEligibleCount = Math.min(120, thresholds.targetEligibleCount + 6);
  }

  thresholds.targetEligibleCount = Math.max(
    PHASE4_TARGET_ELIGIBLE_BASELINE,
    thresholds.targetEligibleCount,
  );

  return thresholds;
}

function evaluatePhase4Token(token: Phase3OutputToken, thresholds: Phase4Thresholds): Phase4OutputToken {
  const hints = token.phase4_screening_hints;
  const volume24Score = normalizeVolumeSignal(token.volume_24h_usd, thresholds.minVolume24hUsd, 5_000_000_000);
  const volume7Score = normalizeVolumeSignal(token.volume_7d_estimated_usd, thresholds.minVolume24hUsd * 7, 35_000_000_000);
  const volume30Score = normalizeVolumeSignal(
    token.volume_30d_estimated_usd,
    thresholds.minVolume24hUsd * 30,
    150_000_000_000,
  );
  const liquidityScore = clamp(
    0.45 * volume24Score + 0.25 * volume7Score + 0.15 * volume30Score + 0.15 * depthScore(hints.exchange_depth_proxy),
    0,
    1,
  );

  const structuralBase =
    0.4 * rankBucketScore(hints.rank_bucket) +
    0.35 * tokenCategoryScore(hints.token_category) +
    0.25 * stablecoinValidationScore(hints.stablecoin_validation_state);
  const structuralPenalty =
    (hints.suspicious_volume_rank_mismatch ? 0.2 : 0) +
    (hints.strict_rank_gate_required ? 0.12 : 0) +
    (hints.proxy_or_wrapped_detected ? 0.25 : 0);
  const structuralScore = clamp(structuralBase - structuralPenalty, 0, 1);
  const profileBoost = Math.min(0.08, token.profile_match_reasons.length * 0.02);
  const screeningScore = clamp(0.58 * liquidityScore + 0.42 * structuralScore + profileBoost, 0, 1);

  const exclusionReasons = [...token.phase4_exclusion_reasons];
  if (token.volume_24h_usd < thresholds.minVolume24hUsd) {
    exclusionReasons.push("min_24h_volume_not_met");
  }
  if (!thresholds.allowLowDepth && hints.exchange_depth_proxy === "low") {
    exclusionReasons.push("exchange_depth_below_profile_threshold");
  }
  if (liquidityScore < thresholds.minLiquidityScore) {
    exclusionReasons.push("liquidity_score_below_threshold");
  }
  if (structuralScore < thresholds.minStructuralScore) {
    exclusionReasons.push("structural_score_below_threshold");
  }
  if (screeningScore < thresholds.minScreeningScore) {
    exclusionReasons.push("screening_score_below_threshold");
  }
  if (hints.strict_rank_gate_required && (token.market_cap_rank === null || token.market_cap_rank > thresholds.rankSanityThreshold)) {
    exclusionReasons.push("strict_rank_gate_violation");
  }

  const uniqueReasons = uniqueSorted(exclusionReasons);
  return {
    coingecko_id: token.coingecko_id,
    symbol: token.symbol,
    name: token.name,
    market_cap_rank: token.market_cap_rank,
    volume_24h_usd: round(Math.max(0, token.volume_24h_usd), 6),
    volume_7d_estimated_usd: round(Math.max(0, token.volume_7d_estimated_usd), 6),
    volume_30d_estimated_usd: round(Math.max(0, token.volume_30d_estimated_usd), 6),
    price_change_pct_7d:
      token.price_change_pct_7d === null || token.price_change_pct_7d === undefined
        ? null
        : round(token.price_change_pct_7d, 6),
    price_change_pct_30d:
      token.price_change_pct_30d === null || token.price_change_pct_30d === undefined
        ? null
        : round(token.price_change_pct_30d, 6),
    source_tags: uniqueSorted(token.source_tags),
    profile_match_reasons: uniqueSorted(token.profile_match_reasons),
    token_category: hints.token_category,
    rank_bucket: hints.rank_bucket,
    exchange_depth_proxy: hints.exchange_depth_proxy,
    stablecoin_validation_state: hints.stablecoin_validation_state,
    liquidity_score: round(liquidityScore, 6),
    structural_score: round(structuralScore, 6),
    screening_score: round(screeningScore, 6),
    eligible: uniqueReasons.length === 0,
    exclusion_reasons: uniqueReasons,
  };
}

function phase4ProfileRelevanceScore(token: Phase4OutputToken): number {
  const reasons = token.profile_match_reasons;
  if (reasons.length === 0 && !token.source_tags.includes("profile_match")) return 0;

  const hasRiskReason = reasons.some((reason) => reason.startsWith("profile_risk_tolerance:"));
  const hasPolicyReason = reasons.some((reason) => reason.startsWith("policy_mode:"));
  const hasStabilityReason =
    reasons.some((reason) => reason.includes("stablecoin")) ||
    reasons.some((reason) => reason.includes("liquidity_floor")) ||
    reasons.some((reason) => reason.includes("defensive_anchor"));

  return clamp(
    (token.source_tags.includes("profile_match") ? 0.24 : 0) +
      Math.min(0.48, reasons.length * 0.1) +
      (hasRiskReason ? 0.12 : 0) +
      (hasPolicyReason ? 0.1 : 0) +
      (hasStabilityReason ? 0.08 : 0),
    0,
    1,
  );
}

function phase4PriorityCompositeScore(token: Phase4OutputToken): number {
  return clamp(
    0.52 * token.screening_score +
      0.2 * token.liquidity_score +
      0.18 * token.structural_score +
      0.1 * phase4ProfileRelevanceScore(token),
    0,
    1,
  );
}

function phase4PrioritySort(left: Phase4OutputToken, right: Phase4OutputToken): number {
  return (
    phase4PriorityCompositeScore(right) - phase4PriorityCompositeScore(left) ||
    phase4ProfileRelevanceScore(right) - phase4ProfileRelevanceScore(left) ||
    right.screening_score - left.screening_score ||
    right.liquidity_score - left.liquidity_score ||
    right.structural_score - left.structural_score ||
    (left.market_cap_rank ?? Number.MAX_SAFE_INTEGER) - (right.market_cap_rank ?? Number.MAX_SAFE_INTEGER)
  );
}

function applyPhase4PriorityCutoff(tokens: Phase4OutputToken[], targetEligibleCount: number): Phase4OutputToken[] {
  const eligible = tokens.filter((token) => token.eligible);
  if (eligible.length <= targetEligibleCount) return tokens;

  const eligibleSorted = [...eligible].sort(phase4PrioritySort);
  const keepIds = new Set(eligibleSorted.slice(0, targetEligibleCount).map((token) => token.coingecko_id));

  return tokens.map((token) => {
    if (!token.eligible || keepIds.has(token.coingecko_id)) return token;
    return {
      ...token,
      eligible: false,
      exclusion_reasons: uniqueSorted([...token.exclusion_reasons, "screening_priority_cutoff"]),
    };
  });
}

function applyPhase4EligibilityLanesAndGuards(
  tokens: Phase4OutputToken[],
  hardEligibleIds: Set<string>,
  targetEligibleCount: number,
  minimumCoverageTarget: number,
  stablecoinMinimum: number,
): {
  tokens: Phase4OutputToken[];
  diagnostics: Phase4LaneDiagnostics;
} {
  const coverageFillCap = Math.max(0, Math.floor(targetEligibleCount * PHASE4_COVERAGE_FILL_MAX_SHARE));
  const stablecoinMaxShare = clamp(
    stablecoinMinimum + PHASE4_STABLECOIN_MAX_SHARE_BUFFER,
    PHASE4_STABLECOIN_MAX_SHARE_FLOOR,
    PHASE4_STABLECOIN_MAX_SHARE_CEILING,
  );
  const stablecoinCapCount = Math.max(1, Math.floor(targetEligibleCount * stablecoinMaxShare));
  const demotedReasonById = new Map<string, string>();
  let remainingDemotions = Math.max(0, tokens.filter((token) => token.eligible).length - minimumCoverageTarget);

  const demote = (token: Phase4OutputToken, reason: string): void => {
    if (!token.eligible) return;
    if (remainingDemotions <= 0) return;
    if (demotedReasonById.has(token.coingecko_id)) return;
    demotedReasonById.set(token.coingecko_id, reason);
    remainingDemotions -= 1;
  };

  const activeEligible = (): Phase4OutputToken[] =>
    tokens.filter((token) => token.eligible && !demotedReasonById.has(token.coingecko_id));

  const coverageFillCandidates = activeEligible()
    .filter((token) => !hardEligibleIds.has(token.coingecko_id))
    .sort(phase4PrioritySort);
  const coverageOverflow = Math.max(0, coverageFillCandidates.length - coverageFillCap);
  if (coverageOverflow > 0 && remainingDemotions > 0) {
    const demoteCount = Math.min(coverageOverflow, remainingDemotions);
    for (const token of coverageFillCandidates.slice(-demoteCount)) {
      demote(token, "coverage_fill_lane_quota_exceeded");
    }
  }

  const stableEligibleAfterCoverage = activeEligible()
    .filter((token) => token.token_category === "stablecoin")
    .sort(phase4PrioritySort);
  const stableOverflow = Math.max(0, stableEligibleAfterCoverage.length - stablecoinCapCount);
  if (stableOverflow > 0 && remainingDemotions > 0) {
    const demoteCount = Math.min(stableOverflow, remainingDemotions);
    for (const token of stableEligibleAfterCoverage.slice(-demoteCount)) {
      demote(token, "stablecoin_concentration_cap");
    }
  }

  const stableEligibleForDistribution = activeEligible()
    .filter((token) => token.token_category === "stablecoin")
    .sort(phase4PrioritySort);
  if (stableEligibleForDistribution.length > 1 && remainingDemotions > 0) {
    const maxIssuerCount = Math.max(
      1,
      Math.floor(stablecoinCapCount * PHASE4_STABLECOIN_ISSUER_MAX_SHARE_OF_STABLE_ELIGIBLE),
    );
    const maxClusterCount = Math.max(
      1,
      Math.floor(stablecoinCapCount * PHASE4_STABLECOIN_CLUSTER_MAX_SHARE_OF_STABLE_ELIGIBLE),
    );
    const issuerCounts = new Map<string, number>();
    const clusterCounts = new Map<string, number>();
    let stableKept = 0;

    for (const token of stableEligibleForDistribution) {
      const issuer = deriveStablecoinIssuer(token);
      const cluster = deriveStablecoinCorrelationCluster(token);
      const issuerCount = issuerCounts.get(issuer) ?? 0;
      const clusterCount = clusterCounts.get(cluster) ?? 0;
      const overStableCap = stableKept >= stablecoinCapCount;
      const overIssuerCap = issuerCount >= maxIssuerCount;
      const overClusterCap = clusterCount >= maxClusterCount;

      if ((overStableCap || overIssuerCap || overClusterCap) && remainingDemotions > 0) {
        const reason = overStableCap
          ? "stablecoin_concentration_cap"
          : overIssuerCap
            ? "stablecoin_issuer_concentration_guard"
            : "stablecoin_cluster_correlation_guard";
        demote(token, reason);
        continue;
      }

      stableKept += 1;
      issuerCounts.set(issuer, issuerCount + 1);
      clusterCounts.set(cluster, clusterCount + 1);
    }
  }

  const updatedTokens = tokens.map((token) => {
    const demotionReason = demotedReasonById.get(token.coingecko_id);
    if (demotionReason) {
      return {
        ...token,
        eligible: false,
        exclusion_reasons: uniqueSorted([...token.exclusion_reasons, demotionReason]),
      };
    }
    if (!token.eligible) return token;

    const laneTag = hardEligibleIds.has(token.coingecko_id)
      ? "phase4_core_eligible_lane"
      : "phase4_coverage_fill_lane";

    return {
      ...token,
      source_tags: uniqueSorted([...token.source_tags, laneTag]),
    };
  });

  const finalEligible = updatedTokens.filter((token) => token.eligible);
  const coreEligibleCount = finalEligible.filter((token) => hardEligibleIds.has(token.coingecko_id)).length;
  const coverageFillEligibleCount = Math.max(0, finalEligible.length - coreEligibleCount);
  const demotedReasons = [...demotedReasonById.values()];

  return {
    tokens: updatedTokens,
    diagnostics: {
      coreEligibleCount,
      coverageFillEligibleCount,
      coverageFillCap,
      stablecoinCapCount,
      stablecoinMaxShare: round(stablecoinMaxShare, 6),
      demotedByCoverageFill: demotedReasons.filter((reason) => reason === "coverage_fill_lane_quota_exceeded").length,
      demotedByStablecoinCap: demotedReasons.filter((reason) => reason === "stablecoin_concentration_cap").length,
      demotedByStablecoinIssuer: demotedReasons.filter((reason) => reason === "stablecoin_issuer_concentration_guard").length,
      demotedByStablecoinCluster: demotedReasons.filter((reason) => reason === "stablecoin_cluster_correlation_guard").length,
    },
  };
}

function relaxPhase4Thresholds(base: Phase4Thresholds, step: Phase4CoverageRecoveryStep): Phase4Thresholds {
  return {
    minLiquidityScore: clamp(base.minLiquidityScore - step.minLiquidityDelta, PHASE4_RECOVERY_MIN_LIQUIDITY_FLOOR, 1),
    minStructuralScore: clamp(base.minStructuralScore - step.minStructuralDelta, PHASE4_RECOVERY_MIN_STRUCTURAL_FLOOR, 1),
    minScreeningScore: clamp(base.minScreeningScore - step.minScreeningDelta, PHASE4_RECOVERY_MIN_SCREENING_FLOOR, 1),
    minVolume24hUsd: Math.max(step.minVolume24hUsdFloor, Math.floor(base.minVolume24hUsd * 0.8)),
    allowLowDepth: base.allowLowDepth,
    targetEligibleCount: Math.max(base.targetEligibleCount, PHASE4_MIN_ELIGIBLE_COVERAGE),
    rankSanityThreshold: base.rankSanityThreshold,
  };
}

function validatePhase4Output(output: Phase4Output): Phase4Output {
  const parsed = phase4OutputSchema.safeParse(output);
  if (parsed.success) return parsed.data;
  const firstError = parsed.error.issues[0];
  throw new Error(`Phase 4 output validation failed: ${firstError?.message ?? "unknown_error"}`);
}

function riskBucketScore(token: Phase4OutputToken): number {
  if (token.token_category === "stablecoin") return 0.15;
  if (token.token_category === "core") return 0.3;
  if (token.token_category === "alt") return 0.55;
  if (token.token_category === "meme") return 0.9;
  if (token.token_category === "proxy_or_wrapped") return 0.95;
  return 0.8;
}

function rankRiskScore(rankBucket: Phase4OutputToken["rank_bucket"]): number {
  if (rankBucket === "top_100") return 0.2;
  if (rankBucket === "top_500") return 0.45;
  if (rankBucket === "long_tail") return 0.75;
  return 0.85;
}

function rankPositionRiskScore(rank: number | null): number {
  if (!Number.isFinite(rank) || rank === null || rank <= 0) return 0.85;
  if (rank <= 10) return 0.08;
  if (rank <= 50) return 0.14 + ((rank - 10) / 40) * 0.1;
  if (rank <= 100) return 0.24 + ((rank - 50) / 50) * 0.08;
  if (rank <= 500) return 0.32 + ((rank - 100) / 400) * 0.28;
  return 0.7 + Math.min(0.2, Math.log10(rank - 499) * 0.08);
}

function depthRiskScore(depth: Phase4OutputToken["exchange_depth_proxy"]): number {
  if (depth === "high") return 0.2;
  if (depth === "medium") return 0.45;
  if (depth === "low") return 0.7;
  return 0.85;
}

function deriveVolatilityProxyScore(token: Phase4OutputToken): number {
  const liquidityFragility = clamp(1 - token.liquidity_score, 0, 1);
  const structuralFragility = clamp(1 - token.structural_score, 0, 1);
  const depthFragility = depthRiskScore(token.exchange_depth_proxy);
  const rankFragility = rankPositionRiskScore(token.market_cap_rank);
  return clamp(
    0.4 * liquidityFragility +
      0.25 * structuralFragility +
      0.2 * depthFragility +
      0.15 * rankFragility,
    0,
    1,
  );
}

function deriveDrawdownProxyScore(token: Phase4OutputToken, volatilityProxyScore: number): number {
  const rankFragility = rankPositionRiskScore(token.market_cap_rank);
  const liquidityFragility = clamp(1 - token.liquidity_score, 0, 1);
  const structuralFragility = clamp(1 - token.structural_score, 0, 1);
  const volumeShockPenalty =
    token.volume_24h_usd < 25_000_000 ? 0.15 : token.volume_24h_usd < 50_000_000 ? 0.08 : 0;
  return clamp(
    0.4 * rankFragility +
      0.3 * liquidityFragility +
      0.2 * structuralFragility +
      0.1 * volatilityProxyScore +
      volumeShockPenalty,
    0,
    1,
  );
}

const STABLECOIN_ISSUER_CLASSIFICATION_GUIDANCE = [
  "Extract issuer root from coingecko_id and token name rather than fixed symbol maps.",
  "Treat tokens with the same non-generic brand root as the same issuer concentration bucket.",
] as const;

const STABLECOIN_CLUSTER_CLASSIFICATION_GUIDANCE = [
  "fiat_custodial_or_cash_backed: trusted validation + strong structure + deep market depth.",
  "crypto_collateralized_or_defi_backed: solid structure/liquidity but not top custodial profile.",
  "synthetic_or_yield_structured: moderate structure or depth/liquidity fragility.",
  "emerging_or_unverified: unverified validation or weak structure/depth profile.",
] as const;

const STABLECOIN_GENERIC_IDENTITY_TOKENS = new Set<string>([
  "usd",
  "eur",
  "coin",
  "coins",
  "token",
  "tokens",
  "stable",
  "stablecoin",
  "dollar",
  "digital",
  "global",
  "first",
  "official",
  "network",
  "protocol",
  "finance",
  "financial",
  "payment",
  "payments",
]);

const STABLECOIN_ISSUER_MAX_SHARE_OF_STABLE_SLEEVE = 0.7;
const STABLECOIN_CLUSTER_MAX_SHARE_OF_STABLE_SLEEVE = 0.85;
const STABLECOIN_CLUSTER_CORRELATION_DAMPENING = 0.35;

function normalizeStablecoinIdentityValue(value: string): string {
  return value.trim().toLowerCase();
}

function extractStablecoinIdentityTokens(token: StablecoinClassificationToken): string[] {
  const identitySeed = `${token.coingecko_id} ${token.name} ${token.symbol}`.toLowerCase();
  const parts = identitySeed
    .split(/[^a-z0-9]+/)
    .map((part) => part.trim())
    .filter((part) => part.length >= 3);

  return parts.filter((part) => !STABLECOIN_GENERIC_IDENTITY_TOKENS.has(part));
}

function deriveStablecoinIssuer(token: StablecoinClassificationToken): string {
  const identityTokens = extractStablecoinIdentityTokens(token);
  const symbolRoot = normalizeStablecoinIdentityValue(token.symbol);
  const issuerRoot = identityTokens[0] ?? (symbolRoot || "unknown");
  return `issuer_${issuerRoot}`;
}

function deriveStablecoinCorrelationCluster(token: StablecoinClassificationToken): string {
  const rankBucket = token.rank_bucket ?? "unknown";
  const depth = token.exchange_depth_proxy ?? "unknown";
  const validationState = token.stablecoin_validation_state ?? "unverified_stablecoin";
  const liquidityScore = clamp(token.liquidity_score ?? 0.35, 0, 1);
  const structuralScore = clamp(token.structural_score ?? 0.65, 0, 1);
  const depthFragility = depthRiskScore(depth);
  const rankFragility = rankRiskScore(rankBucket);

  if (validationState === "unverified_stablecoin") return "emerging_or_unverified";
  if (
    validationState === "trusted_stablecoin" &&
    structuralScore >= 0.9 &&
    liquidityScore >= 0.45 &&
    depthFragility <= 0.35 &&
    rankFragility <= 0.35
  ) {
    return "fiat_custodial_or_cash_backed";
  }
  if (structuralScore >= 0.82 && liquidityScore >= 0.35 && depthFragility <= 0.55) {
    return "crypto_collateralized_or_defi_backed";
  }
  if (structuralScore >= 0.72 && liquidityScore >= 0.28 && depthFragility <= 0.7) {
    return "synthetic_or_yield_structured";
  }
  return "emerging_or_unverified";
}

type StablecoinRiskContext = {
  totalStableVolume24h: number;
  maxStableVolume24h: number;
  issuerCounts: Map<string, number>;
  clusterCounts: Map<string, number>;
};

function buildStablecoinRiskContext(tokens: Phase4OutputToken[]): StablecoinRiskContext {
  const stableTokens = tokens.filter((token) => token.token_category === "stablecoin");
  const issuerCounts = new Map<string, number>();
  const clusterCounts = new Map<string, number>();
  let totalStableVolume24h = 0;
  let maxStableVolume24h = 0;

  for (const token of stableTokens) {
    const issuer = deriveStablecoinIssuer(token);
    const cluster = deriveStablecoinCorrelationCluster(token);
    issuerCounts.set(issuer, (issuerCounts.get(issuer) ?? 0) + 1);
    clusterCounts.set(cluster, (clusterCounts.get(cluster) ?? 0) + 1);
    const volume = Math.max(0, token.volume_24h_usd);
    totalStableVolume24h += volume;
    if (volume > maxStableVolume24h) maxStableVolume24h = volume;
  }

  return {
    totalStableVolume24h,
    maxStableVolume24h,
    issuerCounts,
    clusterCounts,
  };
}

function deriveStablecoinDepegDriftScore(token: Phase4OutputToken): number {
  const pct7d = Math.abs(token.price_change_pct_7d ?? 0);
  const pct30d = Math.abs(token.price_change_pct_30d ?? 0);
  const weightedPct = 0.65 * pct7d + 0.35 * pct30d;
  return clamp(Math.tanh(weightedPct * 1.15), 0, 1);
}

function computeStablecoinGroupExposure(
  stableIds: string[],
  allocations: Map<string, number>,
  groupById: Map<string, string>,
): Map<string, number> {
  const exposure = new Map<string, number>();
  for (const id of stableIds) {
    const group = groupById.get(id) ?? "unknown";
    const value = allocations.get(id) ?? 0;
    exposure.set(group, (exposure.get(group) ?? 0) + value);
  }
  return exposure;
}

function rebalanceStablecoinGroupExposure(
  stableIds: string[],
  allocations: Map<string, number>,
  scoreById: Map<string, number>,
  groupById: Map<string, string>,
  groupCapAbsolute: number,
  capPerAsset: number,
): boolean {
  if (stableIds.length <= 1 || groupCapAbsolute <= 0) return false;
  const distinctGroups = new Set(stableIds.map((id) => groupById.get(id) ?? "unknown"));
  if (distinctGroups.size <= 1) return false;

  let anyAdjustment = false;
  for (let iteration = 0; iteration < 12; iteration += 1) {
    const groupExposure = computeStablecoinGroupExposure(stableIds, allocations, groupById);
    const overweightGroups = [...groupExposure.entries()]
      .filter(([, allocation]) => allocation > groupCapAbsolute + 1e-9)
      .map(([group]) => group);
    if (overweightGroups.length === 0) break;

    let released = 0;
    const reducedGroups = new Set<string>();
    for (const group of overweightGroups) {
      const groupIds = stableIds.filter((id) => (groupById.get(id) ?? "unknown") === group);
      const currentGroupExposure = groupExposure.get(group) ?? 0;
      const excess = currentGroupExposure - groupCapAbsolute;
      if (excess <= 1e-9) continue;
      const groupAllocationSum = groupIds.reduce((sum, id) => sum + (allocations.get(id) ?? 0), 0);
      if (groupAllocationSum <= 1e-12) continue;

      reducedGroups.add(group);
      for (const id of groupIds) {
        const current = allocations.get(id) ?? 0;
        if (current <= 0) continue;
        const cut = Math.min(current, excess * (current / groupAllocationSum));
        if (cut <= 0) continue;
        allocations.set(id, current - cut);
        released += cut;
      }
    }

    if (released <= 1e-12) break;
    anyAdjustment = true;

    const exposureAfterCuts = computeStablecoinGroupExposure(stableIds, allocations, groupById);
    let recipients = stableIds.filter((id) => {
      const current = allocations.get(id) ?? 0;
      if (current >= capPerAsset - 1e-9) return false;
      const group = groupById.get(id) ?? "unknown";
      if (reducedGroups.has(group)) return false;
      return (exposureAfterCuts.get(group) ?? 0) < groupCapAbsolute - 1e-9;
    });

    if (recipients.length === 0) {
      recipients = stableIds.filter((id) => {
        const current = allocations.get(id) ?? 0;
        const group = groupById.get(id) ?? "unknown";
        return current < capPerAsset - 1e-9 && !reducedGroups.has(group);
      });
    }
    if (recipients.length === 0) {
      recipients = stableIds.filter((id) => (allocations.get(id) ?? 0) < capPerAsset - 1e-9);
    }
    if (recipients.length === 0) break;

    const leftover = addProportionalWithCap(allocations, recipients, scoreById, released, capPerAsset);
    if (leftover > 1e-9) {
      const fallbackId = recipients[0] ?? stableIds[0];
      if (fallbackId) allocations.set(fallbackId, (allocations.get(fallbackId) ?? 0) + leftover);
    }
  }

  return anyAdjustment;
}

function buildStablecoinCorrelationAwareScores(stableCandidates: Phase6AllocationCandidate[]): Map<string, number> {
  const issuerCounts = new Map<string, number>();
  const clusterCounts = new Map<string, number>();

  for (const candidate of stableCandidates) {
    const issuer = deriveStablecoinIssuer(candidate.token);
    const cluster = deriveStablecoinCorrelationCluster(candidate.token);
    issuerCounts.set(issuer, (issuerCounts.get(issuer) ?? 0) + 1);
    clusterCounts.set(cluster, (clusterCounts.get(cluster) ?? 0) + 1);
  }

  const scoreById = new Map<string, number>();
  for (const candidate of stableCandidates) {
    const issuer = deriveStablecoinIssuer(candidate.token);
    const cluster = deriveStablecoinCorrelationCluster(candidate.token);
    const issuerCount = issuerCounts.get(issuer) ?? 1;
    const clusterCount = clusterCounts.get(cluster) ?? 1;

    const baseScore = Math.max(0.0001, candidate.compositeScore);
    const issuerDampening = 1 / (1 + 0.5 * Math.max(0, issuerCount - 1));
    const clusterDampening = 1 / (1 + STABLECOIN_CLUSTER_CORRELATION_DAMPENING * Math.max(0, clusterCount - 1));
    const riskDampening = clamp(1 - candidate.riskScore * 0.2, 0.55, 1);
    const adjustedScore = Math.max(0.0001, baseScore * issuerDampening * clusterDampening * riskDampening);

    scoreById.set(candidate.token.coingecko_id, adjustedScore);
  }

  return scoreById;
}

function pickStablecoinAnchorsWithDiversification(
  stableCandidates: Phase6AllocationCandidate[],
  minimumStableCount: number,
): Phase6AllocationCandidate[] {
  if (minimumStableCount <= 0 || stableCandidates.length === 0) return [];

  const anchors: Phase6AllocationCandidate[] = [];
  const selectedIds = new Set<string>();
  const selectedIssuers = new Set<string>();
  const selectedClusters = new Set<string>();

  for (const candidate of stableCandidates) {
    if (anchors.length >= minimumStableCount) break;
    const id = candidate.token.coingecko_id;
    if (selectedIds.has(id)) continue;
    const cluster = deriveStablecoinCorrelationCluster(candidate.token);
    if (selectedClusters.has(cluster)) continue;
    anchors.push(candidate);
    selectedIds.add(id);
    selectedIssuers.add(deriveStablecoinIssuer(candidate.token));
    selectedClusters.add(cluster);
  }

  for (const candidate of stableCandidates) {
    if (anchors.length >= minimumStableCount) break;
    const id = candidate.token.coingecko_id;
    if (selectedIds.has(id)) continue;
    const issuer = deriveStablecoinIssuer(candidate.token);
    if (selectedIssuers.has(issuer)) continue;
    anchors.push(candidate);
    selectedIds.add(id);
    selectedIssuers.add(issuer);
    selectedClusters.add(deriveStablecoinCorrelationCluster(candidate.token));
  }

  for (const candidate of stableCandidates) {
    if (anchors.length >= minimumStableCount) break;
    const id = candidate.token.coingecko_id;
    if (selectedIds.has(id)) continue;
    anchors.push(candidate);
    selectedIds.add(id);
    selectedIssuers.add(deriveStablecoinIssuer(candidate.token));
    selectedClusters.add(deriveStablecoinCorrelationCluster(candidate.token));
  }

  return anchors;
}

function deriveStablecoinRiskModifier(token: Phase4OutputToken, context: StablecoinRiskContext): number {
  if (token.token_category !== "stablecoin") return 0;
  const liquidityFragility = clamp(1 - token.liquidity_score, 0, 1);
  const structuralFragility = clamp(1 - token.structural_score, 0, 1);
  const depthFragility = depthRiskScore(token.exchange_depth_proxy);
  const rankFragility = rankPositionRiskScore(token.market_cap_rank);
  const issuer = deriveStablecoinIssuer(token);
  const cluster = deriveStablecoinCorrelationCluster(token);
  const issuerCount = context.issuerCounts.get(issuer) ?? 1;
  const clusterCount = context.clusterCounts.get(cluster) ?? 1;
  const volumeShare = context.totalStableVolume24h > 0
    ? clamp(token.volume_24h_usd / context.totalStableVolume24h, 0, 1)
    : 0;
  const normalizedLeadership = context.maxStableVolume24h > 0
    ? clamp(token.volume_24h_usd / context.maxStableVolume24h, 0, 1)
    : 0;
  const issuerConcentrationPenalty = clamp(Math.max(0, issuerCount - 1) * 0.018, 0, 0.08);
  const clusterConcentrationPenalty = clamp(Math.max(0, clusterCount - 1) * 0.014, 0, 0.06);
  const depegDriftPenalty = 0.04 * deriveStablecoinDepegDriftScore(token);
  const missingPerformancePenalty =
    token.price_change_pct_7d === null || token.price_change_pct_30d === null ? 0.006 : 0;
  const issuerUnknownPenalty = issuer === "issuer_unknown" ? 0.01 : 0;
  const volumeLeadershipRelief = 0.014 * Math.sqrt(normalizedLeadership);
  const marketShareRelief = 0.01 * Math.sqrt(volumeShare);
  let modifier =
    0.022 +
    0.024 * liquidityFragility +
    0.02 * structuralFragility +
    0.012 * depthFragility +
    0.01 * rankFragility +
    issuerConcentrationPenalty +
    clusterConcentrationPenalty +
    depegDriftPenalty +
    missingPerformancePenalty +
    issuerUnknownPenalty -
    volumeLeadershipRelief -
    marketShareRelief;
  if (cluster === "synthetic_or_yield_structured") modifier += 0.016;
  if (cluster === "emerging_or_unverified") modifier += 0.03;
  if (token.stablecoin_validation_state === "trusted_stablecoin") modifier -= 0.01;
  else if (token.stablecoin_validation_state === "unverified_stablecoin") modifier += 0.06;
  return clamp(modifier, -0.02, 0.24);
}

function buildPhase5AgentScoringInstructionPack(
  riskTolerance: UserRiskTolerance,
  investmentTimeframe: UserInvestmentTimeframe,
  constraints: Phase5AgentScoringConstraints,
): Phase5AgentScoringInstructionPack {
  return {
    version: PHASE5_AGENT_SCORING_RULEBOOK_VERSION,
    objective:
      "Score Phase 4 eligible assets for Phase 5 shortlist using bounded risk-quality rules and deterministic output constraints.",
    input_contract:
      "phase4_json.tokens where eligible=true; scoring must not include excluded candidates; all outputs clipped to schema bounds.",
    gating_rules: [
      "Use only Phase 4 eligible candidates as scoring universe.",
      "Respect policy envelope constraints (risk budget, stablecoin minimum, max single exposure, high-volatility cap) as shortlist context.",
      "Do not bypass structural gates already applied in Phase 4.",
      "Round numeric outputs to 6 decimals and clamp to [0,1] where applicable.",
    ],
    scoring_rules: [
      "Quality score must reward higher screening/liquidity/structural integrity and profile-match alignment.",
      "Risk score must penalize weaker depth/rank structure and higher volatility/drawdown proxies.",
      "Profitability must align to investment timeframe using prior performance windows (7d/30d price-change inputs); short-term favors 7d, longer horizons favor 30d.",
      "Stablecoin risk modifier must include issuer concentration, cluster concentration, and depeg-drift penalties with liquidity/volume leadership relief.",
      "Composite score must reward high quality and penalize elevated risk; rank candidates by composite quality-risk balance.",
      "Stablecoin scores must reflect issuer/structure fragility and validation state rather than treating all stablecoins as identical.",
      `Scoring context: risk_tolerance=${riskTolerance}; investment_timeframe=${investmentTimeframe}.`,
      `policy_context: risk_budget=${round(constraints.riskBudget, 6)}, stablecoin_minimum=${round(constraints.stablecoinMinimum, 6)}, max_single_asset_exposure=${round(constraints.maxSingleAssetExposure, 6)}, high_volatility_asset_cap=${round(constraints.highVolatilityAssetCap, 6)}.`,
    ],
    risk_class_rules: [
      "stablecoin => risk_class=stablecoin.",
      "meme/proxy_or_wrapped => risk_class=speculative.",
      "long_tail or very high risk/volatility proxies => risk_class=high_risk.",
      "commodity-linked alt signals => risk_class=commodities.",
      "high-quality DeFi identity + quality envelope => risk_class=defi_bluechip.",
      "core top-rank liquid assets => risk_class=large_cap_crypto; otherwise fallback=alternative/unclassified/speculative.",
    ],
    role_rules: [
      "role=defensive: trusted stablecoin with low risk profile; more frequent under Conservative/Balanced.",
      "role=liquidity: stablecoin not meeting defensive quality/risk criteria or tactical cash sleeve under Growth/Aggressive.",
      "role=core: reserved for sparse system anchors only (top-rank large-cap, high depth, strong structure/liquidity, low risk).",
      "role=carry: non-core assets with strong profitability+quality and moderate risk; thresholds depend on risk_tolerance.",
      "role=speculative: high-volatility bucket, high-risk classification, or risk score above tolerance-specific speculative threshold.",
      "role=satellite: default diversifier role for remaining eligible non-core/non-speculative assets.",
    ],
    ranking_rules: [
      "Primary sort: composite_score DESC.",
      "Tie-breakers: quality_score DESC, risk_score ASC, market_cap_rank ASC (nulls last).",
      "Selected set is top-N by target profile selection count.",
    ],
  };
}

const PHASE5_DETERMINISTIC_DEFI_KEYWORDS = [
  "defi",
  "swap",
  "dao",
  "amm",
  "lending",
  "staking",
  "yield",
  "dex",
] as const;

const PHASE5_DETERMINISTIC_COMMODITY_KEYWORDS = ["gold", "silver", "commodity"] as const;

function getPhase5AgentScoringProviderMode(): "deterministic" | "messari" {
  const raw = process.env.PHASE5_AGENT_SCORING_PROVIDER?.trim().toLowerCase();
  if (!raw || raw === PHASE5_AGENT_SCORING_DEFAULT_PROVIDER_MODE || raw === "ruleset") return "deterministic";
  if (raw === "messari" || raw === "model" || raw === "llm") return "messari";
  throw new Error(`phase5_agent_scoring_invalid_provider:${raw}`);
}

function hasKeywordSignal(token: Phase4OutputToken, keywords: readonly string[]): boolean {
  const text = `${token.coingecko_id} ${token.symbol} ${token.name}`.toLowerCase();
  return keywords.some((keyword) => text.includes(keyword));
}

function deriveDeterministicPhase5RiskClass(
  token: Phase4OutputToken,
  riskScore: number,
  volatilityProxyScore: number,
): Phase5RiskClass {
  if (token.token_category === "stablecoin") return "stablecoin";
  if (token.token_category === "unknown") return "unclassified";
  if (token.token_category === "meme" || token.token_category === "proxy_or_wrapped") return "speculative";
  if (token.rank_bucket === "long_tail" || riskScore >= 0.8 || volatilityProxyScore >= 0.82) return "high_risk";
  if (
    token.token_category === "alt" &&
    token.exchange_depth_proxy !== "low" &&
    hasKeywordSignal(token, PHASE5_DETERMINISTIC_COMMODITY_KEYWORDS)
  ) {
    return "commodities";
  }
  if (
    token.token_category === "alt" &&
    token.exchange_depth_proxy !== "low" &&
    token.structural_score >= 0.8 &&
    token.liquidity_score >= 0.45 &&
    riskScore <= 0.7 &&
    volatilityProxyScore <= 0.74 &&
    hasKeywordSignal(token, PHASE5_DETERMINISTIC_DEFI_KEYWORDS)
  ) {
    return "defi_bluechip";
  }
  if (
    token.token_category === "core" &&
    token.rank_bucket === "top_100" &&
    token.exchange_depth_proxy !== "low" &&
    riskScore <= 0.66
  ) {
    return "large_cap_crypto";
  }
  return "alternative";
}

function deriveDeterministicPhase5Bucket(
  token: Phase4OutputToken,
  riskClass: Phase5RiskClass,
  riskScore: number,
): Phase5PortfolioBucket {
  const isCoreAnchor =
    riskClass === "large_cap_crypto" &&
    token.rank_bucket === "top_100" &&
    token.exchange_depth_proxy === "high" &&
    token.market_cap_rank !== null &&
    token.market_cap_rank <= 3 &&
    token.liquidity_score >= 0.72 &&
    token.structural_score >= 0.9 &&
    riskScore <= 0.24;
  if (riskClass === "stablecoin") return "stablecoin";
  if (isCoreAnchor) return "core";
  if (riskClass === "high_risk" || riskClass === "speculative" || riskScore >= 0.62 || token.rank_bucket === "long_tail") {
    return "high_volatility";
  }
  return "satellite";
}

type Phase5RolePolicy = {
  defensiveStableRiskCeiling: number;
  speculativeRiskThreshold: number;
  carryMinProfitability: number;
  carryMinQuality: number;
  carryMaxRisk: number;
};

const PHASE5_ROLE_POLICY_BY_RISK: Record<UserRiskTolerance, Phase5RolePolicy> = {
  Conservative: {
    defensiveStableRiskCeiling: 0.34,
    speculativeRiskThreshold: 0.64,
    carryMinProfitability: 0.74,
    carryMinQuality: 0.72,
    carryMaxRisk: 0.34,
  },
  Balanced: {
    defensiveStableRiskCeiling: 0.32,
    speculativeRiskThreshold: 0.68,
    carryMinProfitability: 0.7,
    carryMinQuality: 0.67,
    carryMaxRisk: 0.42,
  },
  Growth: {
    defensiveStableRiskCeiling: 0.3,
    speculativeRiskThreshold: 0.66,
    carryMinProfitability: 0.66,
    carryMinQuality: 0.62,
    carryMaxRisk: 0.5,
  },
  Aggressive: {
    defensiveStableRiskCeiling: 0.29,
    speculativeRiskThreshold: 0.62,
    carryMinProfitability: 0.62,
    carryMinQuality: 0.58,
    carryMaxRisk: 0.56,
  },
};

function deriveDeterministicPhase5Role(
  token: Phase4OutputToken,
  riskClass: Phase5RiskClass,
  bucket: Phase5PortfolioBucket,
  riskScore: number,
  qualityScore: number,
  profitability: number,
  riskTolerance: UserRiskTolerance,
): Phase5Role {
  const policy = PHASE5_ROLE_POLICY_BY_RISK[riskTolerance];
  const isCoreAnchor =
    riskClass === "large_cap_crypto" &&
    bucket === "core" &&
    token.market_cap_rank !== null &&
    token.market_cap_rank <= 3 &&
    token.exchange_depth_proxy === "high" &&
    token.liquidity_score >= 0.72 &&
    token.structural_score >= 0.9 &&
    riskScore <= 0.24;
  if (riskClass === "stablecoin") {
    if (token.stablecoin_validation_state !== "trusted_stablecoin") return "liquidity";
    if (riskTolerance === "Growth" || riskTolerance === "Aggressive") {
      return riskScore <= policy.defensiveStableRiskCeiling ? "defensive" : "liquidity";
    }
    if (riskScore <= policy.defensiveStableRiskCeiling) return "defensive";
    return "liquidity";
  }
  if (isCoreAnchor) return "core";
  if (riskClass === "high_risk" || riskClass === "speculative" || bucket === "high_volatility" || riskScore >= policy.speculativeRiskThreshold) {
    return "speculative";
  }
  if (
    profitability >= policy.carryMinProfitability &&
    qualityScore >= policy.carryMinQuality &&
    riskScore <= policy.carryMaxRisk &&
    bucket !== "core"
  ) {
    return "carry";
  }
  return "satellite";
}

function deriveTimeframeWeightedPerformancePct(
  token: Phase4OutputToken,
  investmentTimeframe: UserInvestmentTimeframe,
): number {
  const change7d = token.price_change_pct_7d;
  const change30d = token.price_change_pct_30d;
  const has7d = typeof change7d === "number" && Number.isFinite(change7d);
  const has30d = typeof change30d === "number" && Number.isFinite(change30d);

  if (investmentTimeframe === "<1_year") {
    if (has7d) return change7d as number;
    if (has30d) return change30d as number;
    return 0;
  }

  if (investmentTimeframe === "1-3_years") {
    if (has7d && has30d) return 0.35 * (change7d as number) + 0.65 * (change30d as number);
    if (has30d) return change30d as number;
    if (has7d) return change7d as number;
    return 0;
  }

  if (has7d && has30d) return 0.2 * (change7d as number) + 0.8 * (change30d as number);
  if (has30d) return change30d as number;
  if (has7d) return change7d as number;
  return 0;
}

function performancePctToSignal(performancePct: number): number {
  const decimalReturn = clamp(performancePct / 100, -1.2, 2.5);
  return clamp(0.5 + Math.tanh(decimalReturn * 1.6) * 0.45, 0, 1);
}

function derivePhase5ProfitabilityScore(
  token: Phase4OutputToken,
  investmentTimeframe: UserInvestmentTimeframe,
  qualityScore: number,
  compositeScore: number,
  riskScore: number,
): number {
  const timeframePerformancePct = deriveTimeframeWeightedPerformancePct(token, investmentTimeframe);
  const performanceSignal = performancePctToSignal(timeframePerformancePct);
  return clamp(
    0.62 * performanceSignal + 0.2 * qualityScore + 0.12 * compositeScore + 0.06 * (1 - riskScore),
    0,
    1,
  );
}

function runPhase5DeterministicScoring(
  qualified: Phase4OutputToken[],
  riskTolerance: UserRiskTolerance,
  investmentTimeframe: UserInvestmentTimeframe,
): Phase5ScoredCandidate[] {
  const stablecoinRiskContext = buildStablecoinRiskContext(qualified);
  return qualified
    .map<Phase5ScoredCandidate>((token) => {
      const profileBoost = Math.min(0.1, token.profile_match_reasons.length * 0.03);
      const qualityScore = clamp(
        0.45 * token.screening_score +
          0.22 * token.liquidity_score +
          0.2 * token.structural_score +
          0.08 * profileBoost +
          0.05 * (1 - rankPositionRiskScore(token.market_cap_rank)),
        0,
        1,
      );
      const volatilityProxyScore = deriveVolatilityProxyScore(token);
      const drawdownProxyScore = deriveDrawdownProxyScore(token, volatilityProxyScore);
      const stablecoinRiskModifier = deriveStablecoinRiskModifier(token, stablecoinRiskContext);
      const baseRiskScore = clamp(
        0.25 * riskBucketScore(token) +
          0.15 * rankRiskScore(token.rank_bucket) +
          0.15 * rankPositionRiskScore(token.market_cap_rank) +
          0.15 * depthRiskScore(token.exchange_depth_proxy) +
          0.18 * volatilityProxyScore +
          0.12 * drawdownProxyScore,
        0,
        1,
      );
      const riskScore = clamp(baseRiskScore + stablecoinRiskModifier, 0, 1);
      const compositeScore = clamp(qualityScore * (1 - riskScore * 0.72) + profileBoost * 0.08, 0, 1);
      const profitability = derivePhase5ProfitabilityScore(
        token,
        investmentTimeframe,
        qualityScore,
        compositeScore,
        riskScore,
      );
      const volatilityBase = clamp(0.7 * volatilityProxyScore + 0.3 * drawdownProxyScore, 0, 1);
      const volatility = token.token_category === "stablecoin"
        ? clamp(volatilityBase * 0.6 + (token.stablecoin_validation_state === "trusted_stablecoin" ? -0.03 : 0.04), 0, 1)
        : volatilityBase;
      const riskClass = deriveDeterministicPhase5RiskClass(token, riskScore, volatilityProxyScore);
      const bucket = deriveDeterministicPhase5Bucket(token, riskClass, riskScore);
      const role = deriveDeterministicPhase5Role(
        token,
        riskClass,
        bucket,
        riskScore,
        qualityScore,
        profitability,
        riskTolerance,
      );
      return {
        token,
        qualityScore: round(qualityScore, 6),
        riskScore: round(riskScore, 6),
        volatilityProxyScore: round(volatilityProxyScore, 6),
        drawdownProxyScore: round(drawdownProxyScore, 6),
        stablecoinRiskModifier: round(stablecoinRiskModifier, 6),
        compositeScore: round(compositeScore, 6),
        profitability: round(profitability, 6),
        volatility: round(volatility, 6),
        riskClass,
        role,
        profileBoost: round(profileBoost, 6),
        bucket,
      };
    })
    .sort(
      (left, right) =>
        right.compositeScore - left.compositeScore ||
        right.qualityScore - left.qualityScore ||
        left.riskScore - right.riskScore ||
        (left.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER) - (right.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER),
    );
}

const phase5AgentModelTokenSchema = z
  .object({
    coingecko_id: z.string().min(1),
    quality_score: z.number().min(0).max(1),
    risk_score: z.number().min(0).max(1),
    volatility_proxy_score: z.number().min(0).max(1),
    drawdown_proxy_score: z.number().min(0).max(1),
    stablecoin_risk_modifier: z.number().min(-0.2).max(0.4),
    composite_score: z.number().min(0).max(1),
    profile_boost: z.number().min(0).max(0.2),
    profitability: z.number().min(0).max(1),
    volatility: z.number().min(0).max(1),
    risk_class: z.enum(PHASE5_RISK_CLASSES),
    role: z.enum(PHASE5_ROLES),
    selection_bucket: z.enum(["stablecoin", "core", "satellite", "high_volatility"]),
  })
  .strict();

const phase5AgentModelResponseSchema = z
  .object({
    tokens: z.array(phase5AgentModelTokenSchema),
  })
  .strict();

type Phase5AgentModelResponse = z.infer<typeof phase5AgentModelResponseSchema>;

function buildPhase5AgentModelCandidates(qualified: Phase4OutputToken[]): Array<Record<string, unknown>> {
  return qualified.map((token) => ({
    coingecko_id: token.coingecko_id,
    symbol: token.symbol,
    name: token.name,
    market_cap_rank: token.market_cap_rank,
    token_category: token.token_category,
    rank_bucket: token.rank_bucket,
    exchange_depth_proxy: token.exchange_depth_proxy,
    stablecoin_validation_state: token.stablecoin_validation_state,
    liquidity_score: token.liquidity_score,
    structural_score: token.structural_score,
    screening_score: token.screening_score,
    volume_24h_usd: token.volume_24h_usd,
    volume_7d_estimated_usd: token.volume_7d_estimated_usd,
    volume_30d_estimated_usd: token.volume_30d_estimated_usd,
    price_change_pct_7d: token.price_change_pct_7d ?? null,
    price_change_pct_30d: token.price_change_pct_30d ?? null,
    profile_match_reasons: token.profile_match_reasons,
  }));
}

function extractJsonObjectFromModelContent(content: string): unknown {
  const fencedMatch = content.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = (fencedMatch?.[1] ?? content).trim();
  try {
    return JSON.parse(candidate);
  } catch {
    const firstBrace = candidate.indexOf("{");
    const lastBrace = candidate.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      return JSON.parse(candidate.slice(firstBrace, lastBrace + 1));
    }
    throw new Error("phase5_agent_scoring_invalid_json_payload");
  }
}

function validatePhase5AgentModelResponse(
  response: unknown,
  expectedIds: Set<string>,
): Phase5AgentModelResponse {
  const parsed = phase5AgentModelResponseSchema.safeParse(response);
  if (!parsed.success) {
    const firstIssue = parsed.error.issues[0];
    throw new Error(`phase5_agent_scoring_schema_validation_failed:${firstIssue?.message ?? "unknown"}`);
  }

  const seen = new Set<string>();
  for (const token of parsed.data.tokens) {
    if (!expectedIds.has(token.coingecko_id)) {
      throw new Error(`phase5_agent_scoring_unknown_candidate:${token.coingecko_id}`);
    }
    if (seen.has(token.coingecko_id)) {
      throw new Error(`phase5_agent_scoring_duplicate_candidate:${token.coingecko_id}`);
    }
    seen.add(token.coingecko_id);
  }
  if (seen.size !== expectedIds.size) {
    throw new Error(`phase5_agent_scoring_missing_candidates:${expectedIds.size - seen.size}`);
  }

  return parsed.data;
}

async function runPhase5AgentScoringProcess(
  qualified: Phase4OutputToken[],
  riskTolerance: UserRiskTolerance,
  investmentTimeframe: UserInvestmentTimeframe,
  constraints: Phase5AgentScoringConstraints,
): Promise<Phase5AgentScoringRun> {
  const instructionPack = buildPhase5AgentScoringInstructionPack(
    riskTolerance,
    investmentTimeframe,
    constraints,
  );
  const providerMode = getPhase5AgentScoringProviderMode();

  if (providerMode === "deterministic") {
    return {
      instructionPack,
      scoredCandidates: runPhase5DeterministicScoring(qualified, riskTolerance, investmentTimeframe),
      provider: "deterministic-ruleset:v4",
      transport: "deterministic_rules",
    };
  }

  const apiKey = process.env.MESSARI_API_KEY?.trim();
  if (!apiKey) {
    throw new Error("phase5_agent_scoring_model_config_error:MESSARI_API_KEY_missing");
  }
  const configuredModel = process.env.PHASE5_AGENT_SCORING_MODEL?.trim() || "messari-default";
  const candidates = buildPhase5AgentModelCandidates(qualified);
  const expectedIds = new Set(qualified.map((token) => token.coingecko_id));
  const prompt = {
    task: "phase5_asset_scoring",
    objective: instructionPack.objective,
    instruction_pack: instructionPack,
    constraints: {
      risk_budget: constraints.riskBudget,
      stablecoin_minimum: constraints.stablecoinMinimum,
      max_single_asset_exposure: constraints.maxSingleAssetExposure,
      high_volatility_asset_cap: constraints.highVolatilityAssetCap,
    },
    allowed_enums: {
      risk_class: PHASE5_RISK_CLASSES,
      role: PHASE5_ROLES,
      selection_bucket: ["stablecoin", "core", "satellite", "high_volatility"],
    },
    output_schema: {
      tokens: [
        {
          coingecko_id: "string",
          quality_score: "number[0,1]",
          risk_score: "number[0,1]",
          volatility_proxy_score: "number[0,1]",
          drawdown_proxy_score: "number[0,1]",
          stablecoin_risk_modifier: "number[-0.2,0.4]",
          composite_score: "number[0,1]",
          profile_boost: "number[0,0.2]",
          profitability: "number[0,1]",
          volatility: "number[0,1]",
          risk_class: "enum",
          role: "enum",
          selection_bucket: "enum",
        },
      ],
    },
    hard_requirements: [
      "Return JSON only. No markdown. No prose.",
      "Include every candidate exactly once by coingecko_id.",
      "Do not invent candidates that are not in input.",
      "Follow instruction_pack rules for classification and ranking.",
    ],
    candidates,
  };

  const response = await fetch(PHASE5_AGENT_SCORING_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-messari-api-key": apiKey,
    },
    body: JSON.stringify({
      messages: [
        {
          role: "user",
          content: JSON.stringify(prompt),
        },
      ],
    }),
  });
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`phase5_agent_scoring_model_call_failed:${response.status}:${body.slice(0, 300)}`);
  }
  const payload = await response.json();
  const content =
    (typeof payload?.data?.messages?.[0]?.content === "string" ? payload.data.messages[0].content : "") ||
    (typeof payload?.choices?.[0]?.message?.content === "string" ? payload.choices[0].message.content : "");
  if (!content) {
    throw new Error("phase5_agent_scoring_model_response_empty");
  }

  const parsedJson = extractJsonObjectFromModelContent(content);
  const scoredPayload = validatePhase5AgentModelResponse(parsedJson, expectedIds);
  const scoreById = new Map(scoredPayload.tokens.map((token) => [token.coingecko_id, token]));
  const scoredCandidates = qualified
    .map<Phase5ScoredCandidate>((token) => {
      const score = scoreById.get(token.coingecko_id);
      if (!score) {
        throw new Error(`phase5_agent_scoring_missing_candidate_score:${token.coingecko_id}`);
      }
      return {
        token,
        qualityScore: round(score.quality_score, 6),
        riskScore: round(score.risk_score, 6),
        volatilityProxyScore: round(score.volatility_proxy_score, 6),
        drawdownProxyScore: round(score.drawdown_proxy_score, 6),
        stablecoinRiskModifier: round(score.stablecoin_risk_modifier, 6),
        compositeScore: round(score.composite_score, 6),
        profitability: round(score.profitability, 6),
        volatility: round(score.volatility, 6),
        riskClass: score.risk_class,
        role: score.role,
        profileBoost: round(score.profile_boost, 6),
        bucket: score.selection_bucket,
      };
    })
    .sort(
      (left, right) =>
        right.compositeScore - left.compositeScore ||
        right.qualityScore - left.qualityScore ||
        left.riskScore - right.riskScore ||
        (left.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER) - (right.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER),
    );

  return {
    instructionPack,
    scoredCandidates,
    provider: `${PHASE5_AGENT_SCORING_PROVIDER}:${configuredModel}`,
    transport: "model_json_schema",
  };
}

function derivePhase5SelectionTarget(riskTolerance: UserRiskTolerance): number {
  if (riskTolerance === "Conservative") return 6;
  if (riskTolerance === "Balanced") return 8;
  if (riskTolerance === "Growth") return 10;
  return 12;
}

function selectPhase5CandidatesWithStablecoinCap(
  shortlist: Phase5ScoredCandidate[],
  targetSelection: number,
  maxSelectedStablecoins: number,
): {
  selectedIds: Set<string>;
  selectedCount: number;
  selectedStablecoinCount: number;
  cappedOutStablecoins: number;
} {
  const desiredCount = Math.min(shortlist.length, Math.max(0, targetSelection));
  const selectedIds = new Set<string>();
  let selectedStablecoinCount = 0;
  let cappedOutStablecoins = 0;
  const isStablecoinCandidate = (candidate: Phase5ScoredCandidate): boolean =>
    candidate.token.token_category === "stablecoin" || candidate.bucket === "stablecoin";
  const stablecoinCandidates = shortlist
    .filter((candidate) => isStablecoinCandidate(candidate))
    .sort(
      (left, right) =>
        right.token.volume_24h_usd - left.token.volume_24h_usd ||
        right.token.liquidity_score - left.token.liquidity_score ||
        right.token.structural_score - left.token.structural_score ||
        right.token.screening_score - left.token.screening_score ||
        (left.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER) - (right.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER),
    );
  const preferredStablecoinIds = new Set(
    stablecoinCandidates.slice(0, Math.max(0, maxSelectedStablecoins)).map((candidate) => candidate.token.coingecko_id),
  );

  for (const candidate of stablecoinCandidates) {
    if (selectedIds.size >= desiredCount) break;
    if (!preferredStablecoinIds.has(candidate.token.coingecko_id)) continue;
    selectedIds.add(candidate.token.coingecko_id);
    selectedStablecoinCount += 1;
  }

  for (const candidate of shortlist) {
    if (selectedIds.size >= desiredCount) break;
    const isStablecoin = isStablecoinCandidate(candidate);
    if (isStablecoin && !preferredStablecoinIds.has(candidate.token.coingecko_id)) {
      cappedOutStablecoins += 1;
      continue;
    }
    if (isStablecoin && selectedStablecoinCount >= maxSelectedStablecoins) {
      cappedOutStablecoins += 1;
      continue;
    }
    selectedIds.add(candidate.token.coingecko_id);
    if (isStablecoin) selectedStablecoinCount += 1;
  }

  if (selectedIds.size < desiredCount) {
    for (const candidate of shortlist) {
      if (selectedIds.size >= desiredCount) break;
      if (selectedIds.has(candidate.token.coingecko_id)) continue;
      if (isStablecoinCandidate(candidate)) continue;
      selectedIds.add(candidate.token.coingecko_id);
    }
  }

  return {
    selectedIds,
    selectedCount: selectedIds.size,
    selectedStablecoinCount,
    cappedOutStablecoins,
  };
}

function deriveStablecoinBaseline(riskTolerance: UserRiskTolerance): number {
  if (riskTolerance === "Conservative") return 0.4;
  if (riskTolerance === "Balanced") return 0.24;
  if (riskTolerance === "Growth") return 0.14;
  return 0.08;
}

function allocateProportionalWithCap(
  ids: string[],
  scoreById: Map<string, number>,
  total: number,
  capPerAsset: number,
): Map<string, number> {
  const allocations = new Map<string, number>();
  for (const id of ids) allocations.set(id, 0);
  if (ids.length === 0 || total <= 0) return allocations;

  const feasibleCap = Math.max(capPerAsset, total / ids.length);
  let remainingIds = [...ids];
  let remainingTotal = total;

  while (remainingIds.length > 0 && remainingTotal > 1e-12) {
    const scores = remainingIds.map((id) => Math.max(0.0001, scoreById.get(id) ?? 0.0001));
    const scoreSum = scores.reduce((sum, value) => sum + value, 0);
    const cappedIds = new Set<string>();

    for (let index = 0; index < remainingIds.length; index += 1) {
      const id = remainingIds[index];
      const candidateShare = remainingTotal * (scores[index] / scoreSum);
      const current = allocations.get(id) ?? 0;
      if (current + candidateShare > feasibleCap + 1e-9) {
        const add = Math.max(0, feasibleCap - current);
        allocations.set(id, current + add);
        remainingTotal -= add;
        cappedIds.add(id);
      }
    }

    if (cappedIds.size === 0) {
      for (let index = 0; index < remainingIds.length; index += 1) {
        const id = remainingIds[index];
        const share = remainingTotal * (scores[index] / scoreSum);
        allocations.set(id, (allocations.get(id) ?? 0) + share);
      }
      remainingTotal = 0;
      break;
    }

    remainingIds = remainingIds.filter((id) => !cappedIds.has(id));
  }

  if (remainingTotal > 1e-9 && ids.length > 0) {
    const fallbackId = ids[0];
    allocations.set(fallbackId, (allocations.get(fallbackId) ?? 0) + remainingTotal);
  }

  return allocations;
}

function addProportionalWithCap(
  allocations: Map<string, number>,
  ids: string[],
  scoreById: Map<string, number>,
  amount: number,
  capPerAsset: number,
): number {
  let remainingAmount = amount;
  let activeIds = ids.filter((id) => (allocations.get(id) ?? 0) < capPerAsset - 1e-9);

  while (remainingAmount > 1e-12 && activeIds.length > 0) {
    const scoreSum = activeIds.reduce((sum, id) => sum + Math.max(0.0001, scoreById.get(id) ?? 0.0001), 0);
    if (scoreSum <= 0) break;
    let used = 0;

    for (const id of activeIds) {
      const score = Math.max(0.0001, scoreById.get(id) ?? 0.0001);
      const targetAdd = remainingAmount * (score / scoreSum);
      const current = allocations.get(id) ?? 0;
      const room = Math.max(0, capPerAsset - current);
      const add = Math.min(room, targetAdd);
      if (add <= 0) continue;
      allocations.set(id, current + add);
      used += add;
    }

    if (used <= 1e-12) break;
    remainingAmount -= used;
    activeIds = activeIds.filter((id) => (allocations.get(id) ?? 0) < capPerAsset - 1e-9);
  }

  return remainingAmount;
}

function finalizeAllocationMap(
  allocations: Map<string, number>,
  ids: string[],
  scoreById: Map<string, number>,
  capPerAsset: number,
): void {
  let total = Array.from(allocations.values()).reduce((sum, value) => sum + value, 0);
  if (total < 1 - 1e-9) {
    const deficit = 1 - total;
    const leftover = addProportionalWithCap(allocations, ids, scoreById, deficit, Math.max(capPerAsset, 1));
    if (leftover > 1e-9 && ids.length > 0) {
      const fallback = ids[0];
      allocations.set(fallback, (allocations.get(fallback) ?? 0) + leftover);
    }
  } else if (total > 1 + 1e-9) {
    const scale = 1 / total;
    for (const id of ids) {
      allocations.set(id, (allocations.get(id) ?? 0) * scale);
    }
  }

  for (const id of ids) {
    allocations.set(id, round(Math.max(0, allocations.get(id) ?? 0), 6));
  }

  total = Array.from(allocations.values()).reduce((sum, value) => sum + value, 0);
  const delta = round(1 - total, 6);
  if (Math.abs(delta) <= 0 || ids.length === 0) return;

  const targetId = [...ids].sort((left, right) => (allocations.get(right) ?? 0) - (allocations.get(left) ?? 0))[0];
  if (!targetId) return;
  allocations.set(targetId, round(Math.max(0, (allocations.get(targetId) ?? 0) + delta), 6));
}

function buildPhase6Portfolio(
  candidates: Phase6AllocationCandidate[],
  constraints: Phase6Output["inputs"]["portfolio_constraints"],
  riskTolerance: UserRiskTolerance,
): {
  allocations: Phase6Output["allocation"]["allocations"];
  selectedIds: Set<string>;
  stablecoinAllocation: number;
  expectedVolatility: number;
  concentrationIndex: number;
  stablecoinDiagnostics: {
    issuerCapShareOfStableSleeve: number;
    clusterCapShareOfStableSleeve: number;
    correlationDampening: number;
    distinctIssuers: number;
    distinctClusters: number;
    maxIssuerAllocation: number;
    maxClusterAllocation: number;
    topIssuer: string | null;
    topCluster: string | null;
  };
} {
  const sorted = [...candidates].sort(
    (left, right) =>
      right.compositeScore - left.compositeScore ||
      right.qualityScore - left.qualityScore ||
      left.riskScore - right.riskScore ||
      (left.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER) - (right.token.market_cap_rank ?? Number.MAX_SAFE_INTEGER),
  );
  if (sorted.length === 0) {
    return {
      allocations: [],
      selectedIds: new Set<string>(),
      stablecoinAllocation: 0,
      expectedVolatility: 0,
      concentrationIndex: 0,
      stablecoinDiagnostics: {
        issuerCapShareOfStableSleeve: STABLECOIN_ISSUER_MAX_SHARE_OF_STABLE_SLEEVE,
        clusterCapShareOfStableSleeve: STABLECOIN_CLUSTER_MAX_SHARE_OF_STABLE_SLEEVE,
        correlationDampening: STABLECOIN_CLUSTER_CORRELATION_DAMPENING,
        distinctIssuers: 0,
        distinctClusters: 0,
        maxIssuerAllocation: 0,
        maxClusterAllocation: 0,
        topIssuer: null,
        topCluster: null,
      },
    };
  }

  const targetCount = Math.min(sorted.length, Math.max(3, derivePhase5SelectionTarget(riskTolerance)));
  const selectedIds: string[] = [];
  const selectedIdSet = new Set<string>();

  const stableCandidates = sorted.filter((candidate) => candidate.bucket === "stablecoin");
  const coreCandidates = sorted.filter((candidate) => candidate.bucket === "core");

  const minimumStableCount =
    constraints.stablecoin_minimum >= 0.2 ? 2 : constraints.stablecoin_minimum > 0 ? 1 : 0;
  const stableAnchors = pickStablecoinAnchorsWithDiversification(stableCandidates, minimumStableCount);
  for (const candidate of stableAnchors) {
    if (selectedIdSet.has(candidate.token.coingecko_id)) continue;
    selectedIds.push(candidate.token.coingecko_id);
    selectedIdSet.add(candidate.token.coingecko_id);
  }

  for (const symbol of ["BTC", "ETH"]) {
    const anchor = coreCandidates.find(
      (candidate) => candidate.token.symbol.toUpperCase() === symbol && !selectedIdSet.has(candidate.token.coingecko_id),
    );
    if (!anchor) continue;
    selectedIds.push(anchor.token.coingecko_id);
    selectedIdSet.add(anchor.token.coingecko_id);
  }

  for (const candidate of sorted) {
    if (selectedIds.length >= targetCount) break;
    if (selectedIdSet.has(candidate.token.coingecko_id)) continue;
    selectedIds.push(candidate.token.coingecko_id);
    selectedIdSet.add(candidate.token.coingecko_id);
  }
  if (selectedIds.length === 0 && sorted.length > 0) {
    selectedIds.push(sorted[0].token.coingecko_id);
    selectedIdSet.add(sorted[0].token.coingecko_id);
  }

  const selected = sorted.filter((candidate) => selectedIdSet.has(candidate.token.coingecko_id));
  const selectedById = new Map(selected.map((candidate) => [candidate.token.coingecko_id, candidate]));
  const scoreById = new Map(selected.map((candidate) => [candidate.token.coingecko_id, Math.max(0.0001, candidate.compositeScore)]));
  const maxSingleCap = clamp(constraints.max_single_asset_exposure, 0.05, 1);
  const highVolCap = clamp(constraints.high_volatility_asset_cap, 0, 1);

  const selectedStableIds = selected
    .filter((candidate) => candidate.bucket === "stablecoin")
    .map((candidate) => candidate.token.coingecko_id);
  const selectedStableCandidates = selected.filter((candidate) => candidate.bucket === "stablecoin");
  const stableScoreById = buildStablecoinCorrelationAwareScores(selectedStableCandidates);
  const stableIssuerById = new Map(
    selectedStableCandidates.map((candidate) => [
      candidate.token.coingecko_id,
      deriveStablecoinIssuer(candidate.token),
    ]),
  );
  const stableClusterById = new Map(
    selectedStableCandidates.map((candidate) => [
      candidate.token.coingecko_id,
      deriveStablecoinCorrelationCluster(candidate.token),
    ]),
  );
  const selectedNonStableIds = selected
    .filter((candidate) => candidate.bucket !== "stablecoin")
    .map((candidate) => candidate.token.coingecko_id);

  let stablecoinTarget =
    selectedStableIds.length > 0
      ? clamp(Math.max(constraints.stablecoin_minimum, deriveStablecoinBaseline(riskTolerance)), 0, 0.65)
      : 0;
  if (selectedNonStableIds.length === 0 && selectedStableIds.length > 0) {
    stablecoinTarget = 1;
  }

  const allocations = new Map<string, number>();
  const stableAllocations = allocateProportionalWithCap(
    selectedStableIds,
    stableScoreById.size > 0 ? stableScoreById : scoreById,
    stablecoinTarget,
    maxSingleCap,
  );
  for (const [id, value] of stableAllocations.entries()) allocations.set(id, value);
  if (stablecoinTarget > 0 && selectedStableIds.length > 1) {
    const stableIssuerCapAbsolute = stablecoinTarget * STABLECOIN_ISSUER_MAX_SHARE_OF_STABLE_SLEEVE;
    rebalanceStablecoinGroupExposure(
      selectedStableIds,
      allocations,
      stableScoreById.size > 0 ? stableScoreById : scoreById,
      stableIssuerById,
      stableIssuerCapAbsolute,
      maxSingleCap,
    );
    const stableClusterCapAbsolute = stablecoinTarget * STABLECOIN_CLUSTER_MAX_SHARE_OF_STABLE_SLEEVE;
    rebalanceStablecoinGroupExposure(
      selectedStableIds,
      allocations,
      stableScoreById.size > 0 ? stableScoreById : scoreById,
      stableClusterById,
      stableClusterCapAbsolute,
      maxSingleCap,
    );
  }

  const nonStableTotal = clamp(1 - stablecoinTarget, 0, 1);
  const nonStableAllocations = allocateProportionalWithCap(selectedNonStableIds, scoreById, nonStableTotal, maxSingleCap);
  for (const [id, value] of nonStableAllocations.entries()) {
    allocations.set(id, (allocations.get(id) ?? 0) + value);
  }

  const highVolIds = selected
    .filter((candidate) => candidate.bucket === "high_volatility")
    .map((candidate) => candidate.token.coingecko_id);
  const highVolSum = highVolIds.reduce((sum, id) => sum + (allocations.get(id) ?? 0), 0);
  if (highVolIds.length > 0 && highVolSum > highVolCap) {
    const scale = highVolCap / highVolSum;
    let released = 0;
    for (const id of highVolIds) {
      const current = allocations.get(id) ?? 0;
      const adjusted = current * scale;
      allocations.set(id, adjusted);
      released += current - adjusted;
    }
    const redistributionIds = selected
      .filter((candidate) => candidate.bucket !== "high_volatility")
      .map((candidate) => candidate.token.coingecko_id);
    const leftover = addProportionalWithCap(allocations, redistributionIds, scoreById, released, Math.max(maxSingleCap, 1));
    if (leftover > 1e-9 && selectedIds.length > 0) {
      const fallbackId = selectedIds[0];
      allocations.set(fallbackId, (allocations.get(fallbackId) ?? 0) + leftover);
    }
  }

  finalizeAllocationMap(allocations, selectedIds, scoreById, maxSingleCap);

  const allocationRows: Phase6Output["allocation"]["allocations"] = selected
    .map((candidate) => ({
      coingecko_id: candidate.token.coingecko_id,
      symbol: candidate.token.symbol,
      name: candidate.token.name,
      bucket: candidate.bucket,
      allocation_weight: round(allocations.get(candidate.token.coingecko_id) ?? 0, 6),
    }))
    .filter((entry) => entry.allocation_weight > 0)
    .sort((left, right) => right.allocation_weight - left.allocation_weight);

  const stablecoinAllocation = round(
    allocationRows
      .filter((allocation) => allocation.bucket === "stablecoin")
      .reduce((sum, allocation) => sum + allocation.allocation_weight, 0),
    6,
  );
  const expectedVolatility = round(
    clamp(
      allocationRows.reduce((sum, allocation) => {
        const candidate = selectedById.get(allocation.coingecko_id);
        if (!candidate) return sum;
        return sum + allocation.allocation_weight * candidate.riskScore;
      }, 0),
      0,
      1,
    ),
    6,
  );
  const concentrationIndex = round(
    clamp(
      allocationRows.reduce((sum, allocation) => sum + allocation.allocation_weight ** 2, 0),
      0,
      1,
    ),
    6,
  );

  const stablecoinRows = allocationRows.filter((allocation) => allocation.bucket === "stablecoin");
  const stablecoinIssuerExposure = new Map<string, number>();
  const stablecoinClusterExposure = new Map<string, number>();
  for (const row of stablecoinRows) {
    const classificationToken: StablecoinClassificationToken =
      selectedById.get(row.coingecko_id)?.token ?? {
        coingecko_id: row.coingecko_id,
        symbol: row.symbol,
        name: row.name,
        rank_bucket: "unknown",
        exchange_depth_proxy: "unknown",
        stablecoin_validation_state: "unverified_stablecoin",
        liquidity_score: 0.35,
        structural_score: 0.65,
      };
    const issuer = deriveStablecoinIssuer(classificationToken);
    const cluster = deriveStablecoinCorrelationCluster(classificationToken);
    stablecoinIssuerExposure.set(issuer, (stablecoinIssuerExposure.get(issuer) ?? 0) + row.allocation_weight);
    stablecoinClusterExposure.set(cluster, (stablecoinClusterExposure.get(cluster) ?? 0) + row.allocation_weight);
  }
  const issuerExposureSorted = [...stablecoinIssuerExposure.entries()]
    .map(([issuer, allocation]) => ({ issuer, allocation: round(allocation, 6) }))
    .sort((left, right) => right.allocation - left.allocation || left.issuer.localeCompare(right.issuer));
  const clusterExposureSorted = [...stablecoinClusterExposure.entries()]
    .map(([cluster, allocation]) => ({ cluster, allocation: round(allocation, 6) }))
    .sort((left, right) => right.allocation - left.allocation || left.cluster.localeCompare(right.cluster));

  return {
    allocations: allocationRows,
    selectedIds: new Set(allocationRows.map((allocation) => allocation.coingecko_id)),
    stablecoinAllocation,
    expectedVolatility,
    concentrationIndex,
    stablecoinDiagnostics: {
      issuerCapShareOfStableSleeve: STABLECOIN_ISSUER_MAX_SHARE_OF_STABLE_SLEEVE,
      clusterCapShareOfStableSleeve: STABLECOIN_CLUSTER_MAX_SHARE_OF_STABLE_SLEEVE,
      correlationDampening: STABLECOIN_CLUSTER_CORRELATION_DAMPENING,
      distinctIssuers: stablecoinIssuerExposure.size,
      distinctClusters: stablecoinClusterExposure.size,
      maxIssuerAllocation: round(issuerExposureSorted[0]?.allocation ?? 0, 6),
      maxClusterAllocation: round(clusterExposureSorted[0]?.allocation ?? 0, 6),
      topIssuer: issuerExposureSorted[0]?.issuer ?? null,
      topCluster: clusterExposureSorted[0]?.cluster ?? null,
    },
  };
}

function validatePhase5Output(output: Phase5Output): Phase5Output {
  const parsed = phase5OutputSchema.safeParse(output);
  if (parsed.success) return parsed.data;
  const firstError = parsed.error.issues[0];
  throw new Error(`Phase 5 output validation failed: ${firstError?.message ?? "unknown_error"}`);
}

function validatePhase6Output(output: Phase6Output): Phase6Output {
  const parsed = phase6OutputSchema.safeParse(output);
  if (parsed.success) return parsed.data;
  const firstError = parsed.error.issues[0];
  throw new Error(`Phase 6 output validation failed: ${firstError?.message ?? "unknown_error"}`);
}

function buildPhase1Output(
  input: NormalizedPhase1Input,
  volatility: VolatilityDomain,
  liquidity: LiquidityDomain,
  sentiment: SentimentDomain,
  alignment: AlignmentDomain,
  allocationAuthorization: AllocationAuthorizationResult,
  sourceReferences: Map<string, SourceReference>,
  sourceSelection: Record<SelectionDomain, SourceSelectionRecord>,
  assumptions: Set<string>,
  limitations: Set<string>,
): Phase1Output {
  return {
    timestamp: input.executionTimestamp,
    execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
    doctrine_version: DOCTRINE_VERSION,
    market_condition: {
      volatility_state: volatility.volatilityState,
      liquidity_state: liquidity.liquidityState,
      risk_appetite: alignment.riskAppetite,
      sentiment_direction: round(clamp(sentiment.sentimentDirection, -1, 1), 6),
      sentiment_alignment: round(clamp(sentiment.sentimentAlignment, 0, 1), 6),
      public_echo_strength: round(clamp(sentiment.publicEchoStrength, 0, 1), 6),
      confidence: round(clamp(alignment.confidence, 0, 1), 6),
      uncertainty: round(clamp(alignment.uncertainty, 0, 1), 6),
    },
    evidence: {
      volatility_metrics: {
        btc_volatility_24h: round(volatility.btcVolatility24h, 6),
        eth_volatility_24h: round(volatility.ethVolatility24h, 6),
        volatility_zscore: round(volatility.volatilityZScore, 6),
      },
      liquidity_metrics: {
        total_volume_24h: round(liquidity.totalVolume24h, 6),
        volume_deviation_zscore: round(liquidity.volumeDeviationZScore, 6),
        avg_spread: round(liquidity.avgSpread, 6),
        stablecoin_dominance: round(liquidity.stablecoinDominance, 6),
      },
      sentiment_metrics: {
        headline_count: sentiment.headlineCount,
        aggregate_sentiment_score: round(clamp(sentiment.aggregateSentimentScore, -1, 1), 6),
        engagement_deviation: round(sentiment.engagementDeviation, 6),
        fear_greed_index:
          sentiment.fearGreedIndex === null ? -1 : round(clamp(sentiment.fearGreedIndex, 0, 100), 3),
        fear_greed_available: sentiment.fearGreedIndex !== null,
      },
    },
    allocation_authorization: {
      status: allocationAuthorization.status,
      confidence: round(clamp(allocationAuthorization.confidence, 0, 1), 6),
      justification: allocationAuthorization.justification,
    },
    phase_boundaries: {
      asset_evaluation: "PHASE_3",
      portfolio_construction: "PHASE_4",
    },
    audit: {
      sources: Array.from(sourceReferences.values()).sort((left, right) => left.id.localeCompare(right.id)),
      data_freshness: input.executionTimestamp,
      missing_domains: Array.from(limitations).sort(),
      assumptions: Array.from(assumptions).sort(),
      source_credibility: getSourceCredibilitySnapshot().map((record) => ({
        domain: record.domain,
        provider: record.provider,
        score: round(clamp(record.score, 0, 1), 6),
        successes: record.successes,
        failures: record.failures,
        last_success_at: record.last_success_at,
        last_failure_at: record.last_failure_at,
        avg_latency_ms: round(Math.max(0, record.avg_latency_ms), 3),
      })),
      source_selection: Object.values(sourceSelection).map((record) => ({
        domain: record.domain,
        selected: Array.from(new Set(record.selected)).sort(),
        rejected: record.rejected
          .map((item) => ({ id: String(item.id), reason: String(item.reason) }))
          .sort((left, right) => left.id.localeCompare(right.id) || left.reason.localeCompare(right.reason)),
        rationale: Array.from(new Set(record.rationale)),
      })),
    },
  };
}

function sanitizeForSchema(output: Phase1Output): Phase1Output {
  return {
    timestamp: String(output.timestamp),
    execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
    doctrine_version: DOCTRINE_VERSION,
    market_condition: {
      ...output.market_condition,
      sentiment_direction: round(clamp(output.market_condition.sentiment_direction, -1, 1), 6),
      sentiment_alignment: round(clamp(output.market_condition.sentiment_alignment, 0, 1), 6),
      public_echo_strength: round(clamp(output.market_condition.public_echo_strength, 0, 1), 6),
      confidence: round(clamp(output.market_condition.confidence, 0, 1), 6),
      uncertainty: round(clamp(output.market_condition.uncertainty, 0, 1), 6),
    },
    evidence: {
      volatility_metrics: {
        btc_volatility_24h: round(output.evidence.volatility_metrics.btc_volatility_24h, 6),
        eth_volatility_24h: round(output.evidence.volatility_metrics.eth_volatility_24h, 6),
        volatility_zscore: round(output.evidence.volatility_metrics.volatility_zscore, 6),
      },
      liquidity_metrics: {
        total_volume_24h: round(output.evidence.liquidity_metrics.total_volume_24h, 6),
        volume_deviation_zscore: round(output.evidence.liquidity_metrics.volume_deviation_zscore, 6),
        avg_spread: round(output.evidence.liquidity_metrics.avg_spread, 6),
        stablecoin_dominance: round(output.evidence.liquidity_metrics.stablecoin_dominance, 6),
      },
      sentiment_metrics: {
        headline_count: Math.max(0, Math.floor(output.evidence.sentiment_metrics.headline_count)),
        aggregate_sentiment_score: round(clamp(output.evidence.sentiment_metrics.aggregate_sentiment_score, -1, 1), 6),
        engagement_deviation: round(output.evidence.sentiment_metrics.engagement_deviation, 6),
        fear_greed_index: round(clamp(output.evidence.sentiment_metrics.fear_greed_index, -1, 100), 3),
        fear_greed_available: Boolean(output.evidence.sentiment_metrics.fear_greed_available),
      },
    },
    allocation_authorization: {
      ...output.allocation_authorization,
      confidence: round(clamp(output.allocation_authorization.confidence, 0, 1), 6),
      justification: output.allocation_authorization.justification.map((item) => String(item)),
    },
    phase_boundaries: {
      asset_evaluation: "PHASE_3",
      portfolio_construction: "PHASE_4",
    },
    audit: {
      sources: output.audit.sources.map((source) => ({
        id: String(source.id),
        provider: String(source.provider),
        endpoint: String(source.endpoint),
        url: String(source.url),
        fetched_at: String(source.fetched_at),
      })),
      data_freshness: String(output.audit.data_freshness),
      missing_domains: output.audit.missing_domains.map((item) => String(item)),
      assumptions: output.audit.assumptions.map((item) => String(item)),
      source_credibility: output.audit.source_credibility.map((record) => ({
        domain: record.domain,
        provider: String(record.provider),
        score: round(clamp(record.score, 0, 1), 6),
        successes: Math.max(0, Math.floor(record.successes)),
        failures: Math.max(0, Math.floor(record.failures)),
        last_success_at: record.last_success_at ? String(record.last_success_at) : null,
        last_failure_at: record.last_failure_at ? String(record.last_failure_at) : null,
        avg_latency_ms: round(Math.max(0, Number(record.avg_latency_ms) || 0), 3),
      })),
      source_selection: output.audit.source_selection.map((selection) => ({
        domain: selection.domain,
        selected: selection.selected.map((item) => String(item)),
        rejected: selection.rejected.map((item) => ({
          id: String(item.id),
          reason: String(item.reason),
        })),
        rationale: selection.rationale.map((item) => String(item)),
      })),
    },
  };
}

function validateOutputWithRetry(buildCandidate: () => Phase1Output): Phase1Output {
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    const candidate = attempt === 1 ? buildCandidate() : sanitizeForSchema(buildCandidate());
    const parsed = phase1OutputSchema.safeParse(candidate);
    if (parsed.success) return parsed.data;
  }

  throw new Error("Phase 1 output validation failed after one retry.");
}

function markPhase2Triggered(jobId: string) {
  const context = getOrCreateJobContext(jobId);
  context.phase2.status = "in_progress";
  context.phase2.triggeredAt = nowIso();
  context.phase2.completedAt = undefined;
  context.phase2.error = undefined;
  context.updatedAt = context.phase2.triggeredAt;
}

async function determineAllocationPolicy(jobId: string): Promise<void> {
  try {
    markPhase2Triggered(jobId);
    const context = getOrCreateJobContext(jobId);
    const phase1Output = context.phase1.output;
    const phase1Input = context.phase1.input;

    if (!phase1Output || !phase1Input) {
      throw new Error("Phase 2 requires completed Phase 1 output and input context.");
    }

    appendJobLog(jobId, {
      phase: "determine_allocation_policy",
      status: "in_progress",
      startedAt: nowIso(),
    });

    const agentInvocation = await runPhase2AgentJudgement(phase1Output, phase1Input);
    const { output: phase2Candidate, policyRules } = derivePhase2PolicyEnvelope(
      phase1Output,
      phase1Input,
      agentInvocation,
    );
    const phase2Output = validatePhase2OutputWithRetry(() => phase2Candidate);

    context.phase2.inputRef = phase2Output.inputs.market_condition_ref;
    context.phase2.output = phase2Output;
    context.phase2.error = undefined;
    await sleep(30);

    context.phase2.status = "complete";
    context.phase2.completedAt = nowIso();
    context.updatedAt = context.phase2.completedAt;
    appendJobLog(jobId, {
      phase: "determine_allocation_policy",
      status: "complete",
      completedAt: context.phase2.completedAt,
    });
    emitExecutionLog({
      phase: "determine_allocation_policy",
      action: "policy_envelope_generated",
      status: "success",
      transactionHash: null,
    });
    if (policyRules.length === 0) {
      emitExecutionLog({
        phase: "determine_allocation_policy",
        action: "policy_rule_audit",
        status: "pending",
        transactionHash: null,
      });
    }
  } catch (error) {
    const context = getOrCreateJobContext(jobId);
    context.phase2.status = "failed";
    context.phase2.error = error instanceof Error ? error.message : "Phase 2 execution failed.";
    context.phase2.completedAt = nowIso();
    context.updatedAt = context.phase2.completedAt;
    appendJobLog(jobId, {
      phase: "determine_allocation_policy",
      status: "failed",
      completedAt: context.phase2.completedAt,
      error: context.phase2.error,
    });
    throw error;
  }
}

function markPhase3Triggered(jobId: string) {
  const context = getOrCreateJobContext(jobId);
  context.phase3.status = "in_progress";
  context.phase3.triggeredAt = nowIso();
  context.phase3.completedAt = undefined;
  context.phase3.error = undefined;
  context.updatedAt = context.phase3.triggeredAt;
}

async function executePhase3(jobId: string): Promise<void> {
  const context = getOrCreateJobContext(jobId);
  const phase2Output = context.phase2.output;
  if (!phase2Output || context.phase2.status !== "complete") {
    throw new Error("Phase 3 requires completed Phase 2 output.");
  }

  markPhase3Triggered(jobId);
  appendJobLog(jobId, {
    phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
    status: "in_progress",
    startedAt: nowIso(),
  });

  const toolCalls = new Set<string>();
  const sourceReferences = new Map<string, SourceReference>();
  const selectionRules = new Set<string>([
    "phase3_agent_required:true",
    "phase3_scope:top_volume_7_30d_plus_profile_match",
    `phase3_prompt_version:${PHASE3_SYSTEM_PROMPT_VERSION}`,
    `execution_model_version:${EXECUTION_MODEL_VERSION}`,
    `doctrine_version:${PHASE3_DOCTRINE_VERSION}`,
    "phase4_rank_sanity_threshold:500",
    "phase4_structural_gates:rank_sanity|exchange_depth_proxy|stablecoin_validation|token_category",
    `phase4_allow_meme_tokens:${isPhase4MemeAllowed()}`,
  ]);
  const missingDomains = new Set<string>();
  const topVolumeTarget = getPhase3TopVolumeTarget();

  let agentProfileMatch = {
    used: false,
    model: null as string | null,
    reason_codes: [] as string[],
    skipped_reason: null as string | null,
  };

  try {
    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[0],
      status: "in_progress",
    });
    try {
      await initializeAgent();
      agentProfileMatch = {
        used: true,
        model: PHASE2_AGENT_REASONING_PROVIDER,
        reason_codes: [
          `risk_tolerance:${phase2Output.inputs.user_profile.risk_tolerance.toLowerCase()}`,
          `investment_timeframe:${phase2Output.inputs.user_profile.investment_timeframe}`,
          `policy_mode:${phase2Output.allocation_policy.mode}`,
          `authorization_status:${phase2Output.allocation_authorization.status.toLowerCase()}`,
        ],
        skipped_reason: null,
      };
      selectionRules.add("phase3_agent_engaged:coinbase-agentkit");
    } catch (error) {
      missingDomains.add("phase3_agent_identity_unavailable");
      agentProfileMatch = {
        used: false,
        model: PHASE2_AGENT_REASONING_PROVIDER,
        reason_codes: [],
        skipped_reason:
          error instanceof Error ? `phase3_agent_engagement_error:${error.message}` : "phase3_agent_engagement_error:unknown",
      };
      selectionRules.add(`phase3_agent_skipped:${agentProfileMatch.skipped_reason}`);
      throw new Error(agentProfileMatch.skipped_reason ?? "phase3_agent_required:unknown");
    }
    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[0],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[1],
      status: "in_progress",
    });
    const collectedTopVolumeTokens = await collectPhase3TopVolumeTokens(
      topVolumeTarget,
      toolCalls,
      sourceReferences,
      selectionRules,
      missingDomains,
    );
    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[1],
      status: "complete",
    });
    if (collectedTopVolumeTokens.length === 0) {
      throw new Error("Phase 3 could not collect top-volume token universe.");
    }

    let topVolumeTokens = sortUniverseTokens(
      filterPhase3TokensForRetail(collectedTopVolumeTokens, selectionRules, "top_volume"),
    ).slice(0, topVolumeTarget);
    if (topVolumeTokens.length === 0) {
      missingDomains.add("phase3_top_volume_retail_unavailable");
      selectionRules.add("phase3_top_volume_emergency_fallback_applied");
      topVolumeTokens = buildEmergencyRetailUniverseTokens();
    }
    if (topVolumeTokens.length < topVolumeTarget) {
      missingDomains.add(`phase3_top_volume_retail_under_target:${topVolumeTokens.length}/${topVolumeTarget}`);
    }

    const universeMap = new Map<string, Phase3UniverseToken>();
    for (const token of topVolumeTokens) {
      mergeUniverseTokens(universeMap, token);
    }
    selectionRules.add(`phase3_top_volume_collected:${topVolumeTokens.length}`);

    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[2],
      status: "in_progress",
    });
    const collectedProfileMatchTokens = await collectPhase3ProfileMatchTokens(
      phase2Output,
      universeMap,
      toolCalls,
      sourceReferences,
      selectionRules,
      missingDomains,
    );
    const profileMatchTokens = filterPhase3TokensForRetail(
      collectedProfileMatchTokens,
      selectionRules,
      "profile_match",
    );
    for (const token of profileMatchTokens) {
      mergeUniverseTokens(universeMap, token);
    }
    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[2],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[3],
      status: "in_progress",
    });
    const mergedTokens = sortUniverseTokens(Array.from(universeMap.values()));
    const profileMatchCount = mergedTokens.filter((token) => token.profileMatchReasons.size > 0).length;
    const phase3Tokens = mergedTokens.map((token) => derivePhase3TokenOutput(token));
    for (const token of phase3Tokens) {
      if (token.status === "UNRESOLVED") {
        missingDomains.add(`phase3_token_unresolved:${token.coingecko_id}`);
      }
    }
    const unresolvedCount = phase3Tokens.filter((token) => token.status === "UNRESOLVED").length;
    const excludedCount = phase3Tokens.filter((token) => token.exclude_from_phase4).length;
    const suspiciousCount = phase3Tokens.filter(
      (token) => token.phase4_screening_hints.suspicious_volume_rank_mismatch,
    ).length;
    selectionRules.add(`phase3_unresolved_count:${unresolvedCount}`);
    selectionRules.add(`phase3_excluded_from_phase4_count:${excludedCount}`);
    selectionRules.add(`phase3_volume_rank_mismatch_flags:${suspiciousCount}`);
    const phase2PolicyRef =
      `sha256:${createHash("sha256").update(JSON.stringify({
        allocation_policy: phase2Output.allocation_policy,
        policy_envelope: phase2Output.policy_envelope,
        allocation_authorization: phase2Output.allocation_authorization,
      })).digest("hex")}`;

    const output = validatePhase3OutputWithRetry(() => ({
      timestamp: nowIso(),
      execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
      doctrine_version: PHASE3_DOCTRINE_VERSION,
      inputs: {
        phase2_policy_ref: phase2PolicyRef,
        user_profile: {
          risk_tolerance: phase2Output.inputs.user_profile.risk_tolerance,
          investment_timeframe: phase2Output.inputs.user_profile.investment_timeframe,
        },
        top_volume_target: topVolumeTarget,
        volume_window_days: [7, 30] as const,
      },
      universe: {
        top_volume_candidates_count: topVolumeTokens.length,
        profile_match_candidates_count: profileMatchCount,
        total_candidates_count: mergedTokens.length,
        tokens: phase3Tokens,
      },
      phase_boundaries: {
        asset_screening: "PHASE_4" as const,
        portfolio_construction: "PHASE_4" as const,
      },
      audit: {
        sources: Array.from(sourceReferences.values()).sort((left, right) => left.id.localeCompare(right.id)),
        selection_rules: Array.from(selectionRules).sort(),
        missing_domains: Array.from(missingDomains).sort(),
        agent_profile_match: agentProfileMatch,
      },
    }));

    context.phase3.inputRef = phase2PolicyRef;
    context.phase3.output = output;
    context.phase3.error = undefined;
    context.phase3.status = "complete";
    context.phase3.completedAt = nowIso();
    context.updatedAt = context.phase3.completedAt;

    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      subPhase: PHASE3_SUB_PHASES[3],
      status: "complete",
    });
    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      status: "complete",
      completedAt: context.phase3.completedAt,
    });
  } catch (error) {
    context.phase3.status = "failed";
    context.phase3.error = error instanceof Error ? error.message : "Phase 3 execution failed.";
    context.phase3.completedAt = nowIso();
    context.updatedAt = context.phase3.completedAt;

    appendJobLog(jobId, {
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
      status: "failed",
      completedAt: context.phase3.completedAt,
      error: context.phase3.error,
    });
    throw error;
  } finally {
    runningPhase3Jobs.delete(jobId);
  }
}

function markPhase4Triggered(jobId: string) {
  const context = getOrCreateJobContext(jobId);
  context.phase4.status = "in_progress";
  context.phase4.triggeredAt = nowIso();
  context.phase4.completedAt = undefined;
  context.phase4.error = undefined;
  context.updatedAt = context.phase4.triggeredAt;
}

function markPhase5Triggered(jobId: string) {
  const context = getOrCreateJobContext(jobId);
  context.phase5.status = "in_progress";
  context.phase5.triggeredAt = nowIso();
  context.phase5.completedAt = undefined;
  context.phase5.error = undefined;
  context.updatedAt = context.phase5.triggeredAt;
}

function markPhase6Triggered(jobId: string) {
  const context = getOrCreateJobContext(jobId);
  context.phase6.status = "in_progress";
  context.phase6.triggeredAt = nowIso();
  context.phase6.completedAt = undefined;
  context.phase6.error = undefined;
  context.phase6.aaaAllocate = defaultAaaAllocateDispatch();
  context.updatedAt = context.phase6.triggeredAt;
}

async function executePhase4(jobId: string): Promise<void> {
  const context = getOrCreateJobContext(jobId);
  const phase2Output = context.phase2.output;
  const phase3Output = context.phase3.output;
  if (!phase2Output || context.phase2.status !== "complete") {
    throw new Error("Phase 4 requires completed Phase 2 output.");
  }
  if (!phase3Output || context.phase3.status !== "complete") {
    throw new Error("Phase 4 requires completed Phase 3 output.");
  }

  markPhase4Triggered(jobId);
  appendJobLog(jobId, {
    phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
    status: "in_progress",
    startedAt: nowIso(),
  });

  const selectionRules = new Set<string>([
    "phase4_agent_required:true",
    `execution_model_version:${EXECUTION_MODEL_VERSION}`,
    `doctrine_version:${PHASE4_DOCTRINE_VERSION}`,
    "phase4_scoring_method:liquidity_structural_composite",
    "phase4_rank_sanity_threshold:500",
    "phase4_structural_gates:rank_sanity|exchange_depth_proxy|stablecoin_validation|token_category",
  ]);
  const missingDomains = new Set<string>();
  const phase3UniverseRef = hashReference({
    inputs: phase3Output.inputs,
    universe: phase3Output.universe,
  });
  let thresholds = derivePhase4Thresholds(
    phase2Output.inputs.user_profile.risk_tolerance,
    phase2Output.allocation_policy.mode,
  );
  const agentScreening = {
    used: false,
    model: null as string | null,
    reason_codes: [] as string[],
    skipped_reason: null as string | null,
  };

  try {
    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[0],
      status: "in_progress",
    });
    try {
      await initializeAgent();
      agentScreening.used = true;
      agentScreening.model = PHASE2_AGENT_REASONING_PROVIDER;
      agentScreening.reason_codes = [
        `risk_tolerance:${phase2Output.inputs.user_profile.risk_tolerance.toLowerCase()}`,
        `policy_mode:${phase2Output.allocation_policy.mode}`,
        `stablecoin_minimum:${round(phase2Output.policy_envelope.stablecoin_minimum, 4)}`,
      ];
      selectionRules.add("phase4_agent_engaged:coinbase-agentkit");
    } catch (error) {
      const skippedReason =
        error instanceof Error ? `phase4_agent_engagement_error:${error.message}` : "phase4_agent_engagement_error:unknown";
      agentScreening.skipped_reason = skippedReason;
      missingDomains.add("phase4_agent_identity_unavailable");
      selectionRules.add(`phase4_agent_skipped:${skippedReason}`);
      throw new Error(skippedReason);
    }
    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[0],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[1],
      status: "in_progress",
    });
    const hardFloorThresholds = { ...thresholds };
    const hardLaneEligibleIds = new Set(
      phase3Output.universe.tokens
        .map((token) => evaluatePhase4Token(token, hardFloorThresholds))
        .filter((token) => token.eligible)
        .map((token) => token.coingecko_id),
    );
    selectionRules.add(`phase4_hard_floor_min_liquidity_score:${round(hardFloorThresholds.minLiquidityScore, 4)}`);
    selectionRules.add(`phase4_hard_floor_min_structural_score:${round(hardFloorThresholds.minStructuralScore, 4)}`);
    selectionRules.add(`phase4_hard_floor_min_screening_score:${round(hardFloorThresholds.minScreeningScore, 4)}`);
    selectionRules.add(`phase4_hard_floor_min_volume_24h_usd:${Math.floor(hardFloorThresholds.minVolume24hUsd)}`);
    const minimumCoverageTarget = Math.min(
      phase3Output.universe.tokens.length,
      Math.max(1, PHASE4_MIN_ELIGIBLE_COVERAGE),
    );
    const desiredCoverageTarget = Math.min(
      phase3Output.universe.tokens.length,
      Math.max(minimumCoverageTarget, thresholds.targetEligibleCount),
    );
    selectionRules.add(`phase4_minimum_eligible_coverage_target:${minimumCoverageTarget}`);
    selectionRules.add(`phase4_desired_eligible_coverage_target:${desiredCoverageTarget}`);
    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[1],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[2],
      status: "in_progress",
    });
    const buildScreenedSet = (
      activeThresholds: Phase4Thresholds,
    ): { tokens: Phase4OutputToken[]; diagnostics: Phase4LaneDiagnostics } => {
      const evaluatedTokens = phase3Output.universe.tokens
        .map((token) => evaluatePhase4Token(token, activeThresholds))
        .sort(phase4PrioritySort);
      const cutoffApplied = applyPhase4PriorityCutoff(evaluatedTokens, activeThresholds.targetEligibleCount);
      return applyPhase4EligibilityLanesAndGuards(
        cutoffApplied,
        hardLaneEligibleIds,
        activeThresholds.targetEligibleCount,
        minimumCoverageTarget,
        phase2Output.policy_envelope.stablecoin_minimum,
      );
    };

    const screenedSet = buildScreenedSet(thresholds);
    let screenedTokens = screenedSet.tokens;
    let laneDiagnostics = screenedSet.diagnostics;
    let eligibleCount = screenedTokens.filter((token) => token.eligible).length;
    if (eligibleCount === 0 && screenedTokens.length > 0) {
      const emergencyPassIds = new Set(
        screenedTokens
          .filter((token) => !token.exclusion_reasons.includes("unresolved_market_cap_rank"))
          .slice(0, Math.min(5, screenedTokens.length))
          .map((token) => token.coingecko_id),
      );
      if (emergencyPassIds.size > 0) {
        screenedTokens = screenedTokens.map((token) =>
          emergencyPassIds.has(token.coingecko_id)
            ? {
                ...token,
                eligible: true,
                exclusion_reasons: [],
              }
            : token,
        );
        const emergencyLaneSet = applyPhase4EligibilityLanesAndGuards(
          screenedTokens,
          hardLaneEligibleIds,
          thresholds.targetEligibleCount,
          minimumCoverageTarget,
          phase2Output.policy_envelope.stablecoin_minimum,
        );
        screenedTokens = emergencyLaneSet.tokens;
        laneDiagnostics = emergencyLaneSet.diagnostics;
        selectionRules.add(`phase4_emergency_pass_enabled:${emergencyPassIds.size}`);
      }
    }
    eligibleCount = screenedTokens.filter((token) => token.eligible).length;
    if (eligibleCount < desiredCoverageTarget && screenedTokens.length > eligibleCount) {
      for (let index = 0; index < PHASE4_COVERAGE_RECOVERY_STEPS.length; index += 1) {
        const recoveryThresholds = relaxPhase4Thresholds(thresholds, PHASE4_COVERAGE_RECOVERY_STEPS[index]);
        const recoveredSet = buildScreenedSet(recoveryThresholds);
        const recoveredTokens = recoveredSet.tokens;
        const recoveredEligibleCount = recoveredTokens.filter((token) => token.eligible).length;
        selectionRules.add(`phase4_coverage_recovery_step_${index + 1}:eligible_${recoveredEligibleCount}`);

        if (recoveredEligibleCount > eligibleCount) {
          thresholds = recoveryThresholds;
          screenedTokens = recoveredTokens;
          laneDiagnostics = recoveredSet.diagnostics;
          eligibleCount = recoveredEligibleCount;
          selectionRules.add(`phase4_coverage_recovery_applied:step_${index + 1}`);
        }
        if (eligibleCount >= desiredCoverageTarget) break;
      }
    }
    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[2],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[3],
      status: "in_progress",
    });
    selectionRules.add(`phase4_min_liquidity_score:${round(thresholds.minLiquidityScore, 4)}`);
    selectionRules.add(`phase4_min_structural_score:${round(thresholds.minStructuralScore, 4)}`);
    selectionRules.add(`phase4_min_screening_score:${round(thresholds.minScreeningScore, 4)}`);
    selectionRules.add(`phase4_min_volume_24h_usd:${Math.floor(thresholds.minVolume24hUsd)}`);
    selectionRules.add(`phase4_target_eligible_count:${thresholds.targetEligibleCount}`);
    selectionRules.add(`phase4_allow_low_depth:${thresholds.allowLowDepth}`);
    selectionRules.add(`phase4_total_candidates:${screenedTokens.length}`);
    selectionRules.add(`phase4_eligible_candidates:${eligibleCount}`);
    selectionRules.add(`phase4_core_eligible_candidates:${laneDiagnostics.coreEligibleCount}`);
    selectionRules.add(`phase4_coverage_fill_candidates:${laneDiagnostics.coverageFillEligibleCount}`);
    selectionRules.add(`phase4_coverage_fill_quota:${laneDiagnostics.coverageFillCap}`);
    selectionRules.add(`phase4_stablecoin_eligible_cap:${laneDiagnostics.stablecoinCapCount}`);
    selectionRules.add(`phase4_stablecoin_eligible_max_share:${round(laneDiagnostics.stablecoinMaxShare, 4)}`);
    if (laneDiagnostics.demotedByCoverageFill > 0) {
      selectionRules.add(`phase4_coverage_fill_demotions:${laneDiagnostics.demotedByCoverageFill}`);
    }
    if (laneDiagnostics.demotedByStablecoinCap > 0) {
      selectionRules.add(`phase4_stablecoin_cap_demotions:${laneDiagnostics.demotedByStablecoinCap}`);
    }
    if (laneDiagnostics.demotedByStablecoinIssuer > 0) {
      selectionRules.add(`phase4_stablecoin_issuer_demotions:${laneDiagnostics.demotedByStablecoinIssuer}`);
    }
    if (laneDiagnostics.demotedByStablecoinCluster > 0) {
      selectionRules.add(`phase4_stablecoin_cluster_demotions:${laneDiagnostics.demotedByStablecoinCluster}`);
    }
    if (eligibleCount < 5) {
      missingDomains.add(`phase4_eligible_universe_thin:${eligibleCount}`);
    }

    const output = validatePhase4Output({
      timestamp: nowIso(),
      execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
      doctrine_version: PHASE4_DOCTRINE_VERSION,
      inputs: {
        phase3_universe_ref: phase3UniverseRef,
        phase2_policy_ref: phase3Output.inputs.phase2_policy_ref,
        user_profile: {
          risk_tolerance: phase2Output.inputs.user_profile.risk_tolerance,
          investment_timeframe: phase2Output.inputs.user_profile.investment_timeframe,
        },
        screening_thresholds: {
          min_liquidity_score: round(thresholds.minLiquidityScore, 6),
          min_structural_score: round(thresholds.minStructuralScore, 6),
          min_screening_score: round(thresholds.minScreeningScore, 6),
          min_volume_24h_usd: Math.max(0, Math.floor(thresholds.minVolume24hUsd)),
          target_eligible_count: thresholds.targetEligibleCount,
          allow_low_depth: thresholds.allowLowDepth,
          rank_sanity_threshold: thresholds.rankSanityThreshold,
        },
      },
      screening: {
        total_candidates_count: phase3Output.universe.tokens.length,
        excluded_by_phase3_count: phase3Output.universe.tokens.filter((token) => token.exclude_from_phase4).length,
        evaluated_candidates_count: screenedTokens.length,
        eligible_candidates_count: eligibleCount,
        tokens: screenedTokens,
      },
      phase_boundaries: {
        risk_quality_evaluation: "PHASE_5",
        portfolio_construction: "PHASE_6",
      },
      audit: {
        sources: phase3Output.audit.sources,
        selection_rules: Array.from(selectionRules).sort(),
        missing_domains: Array.from(missingDomains).sort(),
        agent_screening: agentScreening,
      },
    });

    context.phase4.inputRef = phase3UniverseRef;
    context.phase4.output = output;
    context.phase4.error = undefined;
    context.phase4.status = "complete";
    context.phase4.completedAt = nowIso();
    context.updatedAt = context.phase4.completedAt;
    context.phase5 = {
      status: "idle",
    };
    context.phase6 = {
      status: "idle",
      aaaAllocate: defaultAaaAllocateDispatch(),
    };

    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      subPhase: PHASE4_SUB_PHASES[3],
      status: "complete",
    });
    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      status: "complete",
      completedAt: context.phase4.completedAt,
    });
  } catch (error) {
    context.phase4.status = "failed";
    context.phase4.error = error instanceof Error ? error.message : "Phase 4 execution failed.";
    context.phase4.completedAt = nowIso();
    context.updatedAt = context.phase4.completedAt;

    appendJobLog(jobId, {
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
      status: "failed",
      completedAt: context.phase4.completedAt,
      error: context.phase4.error,
    });
    throw error;
  } finally {
    runningPhase4Jobs.delete(jobId);
  }
}

async function executePhase5(jobId: string): Promise<void> {
  const context = getOrCreateJobContext(jobId);
  const phase2Output = context.phase2.output;
  const phase4Output = context.phase4.output;
  if (!phase2Output || context.phase2.status !== "complete") {
    throw new Error("Phase 5 requires completed Phase 2 output.");
  }
  if (!phase4Output || context.phase4.status !== "complete") {
    throw new Error("Phase 5 requires completed Phase 4 output.");
  }

  markPhase5Triggered(jobId);
  appendJobLog(jobId, {
    phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
    status: "in_progress",
    startedAt: nowIso(),
  });

  const selectionRules = new Set<string>([
    "phase5_agent_required:true",
    `execution_model_version:${EXECUTION_MODEL_VERSION}`,
    `doctrine_version:${PHASE5_DOCTRINE_VERSION}`,
    "phase5_scoring_method:agent_rules_engine",
    "phase5_output_mode:shortlist_only",
  ]);
  const missingDomains = new Set<string>();
  const phase4ScreeningRef = hashReference({
    inputs: phase4Output.inputs,
    screening: phase4Output.screening,
  });
  const agentQualityReview = {
    used: false,
    model: null as string | null,
    reason_codes: [] as string[],
    skipped_reason: null as string | null,
  };

  try {
    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[0],
      status: "in_progress",
    });
    try {
      await initializeAgent();
      agentQualityReview.used = true;
      agentQualityReview.model = PHASE2_AGENT_REASONING_PROVIDER;
      agentQualityReview.reason_codes = [
        `risk_tolerance:${phase2Output.inputs.user_profile.risk_tolerance.toLowerCase()}`,
        `investment_timeframe:${phase2Output.inputs.user_profile.investment_timeframe}`,
        `risk_budget:${round(phase2Output.policy_envelope.risk_budget, 4)}`,
      ];
      selectionRules.add("phase5_agent_engaged:coinbase-agentkit");
    } catch (error) {
      const skippedReason =
        error instanceof Error ? `phase5_agent_engagement_error:${error.message}` : "phase5_agent_engagement_error:unknown";
      agentQualityReview.skipped_reason = skippedReason;
      missingDomains.add("phase5_agent_identity_unavailable");
      selectionRules.add(`phase5_agent_skipped:${skippedReason}`);
      throw new Error(skippedReason);
    }
    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[0],
      status: "complete",
    });

    const qualified = phase4Output.screening.tokens.filter((token) => token.eligible);
    if (qualified.length === 0) {
      throw new Error("Phase 5 requires at least one Phase 4 eligible candidate.");
    }
    const portfolioConstraints = {
      risk_budget: round(clamp(phase2Output.policy_envelope.risk_budget, 0, 1), 6),
      stablecoin_minimum: round(clamp(phase2Output.policy_envelope.stablecoin_minimum, 0, 1), 6),
      max_single_asset_exposure: round(clamp(phase2Output.policy_envelope.exposure_caps.max_single_asset_exposure, 0, 1), 6),
      high_volatility_asset_cap: round(clamp(phase2Output.policy_envelope.exposure_caps.high_volatility_asset_cap, 0, 1), 6),
    };

    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[1],
      status: "in_progress",
    });
    const agentScoringRun = await runPhase5AgentScoringProcess(
      qualified,
      phase2Output.inputs.user_profile.risk_tolerance,
      phase2Output.inputs.user_profile.investment_timeframe,
      {
        riskBudget: portfolioConstraints.risk_budget,
        stablecoinMinimum: portfolioConstraints.stablecoin_minimum,
        maxSingleAssetExposure: portfolioConstraints.max_single_asset_exposure,
        highVolatilityAssetCap: portfolioConstraints.high_volatility_asset_cap,
      },
    );
    const scoredCandidates = agentScoringRun.scoredCandidates;
    selectionRules.add(`phase5_agent_scoring_provider:${agentScoringRun.provider}`);
    selectionRules.add(`phase5_agent_scoring_transport:${agentScoringRun.transport}`);
    selectionRules.add(`phase5_agent_scoring_rulebook:${agentScoringRun.instructionPack.version}`);
    selectionRules.add(`phase5_agent_scoring_input:${agentScoringRun.instructionPack.input_contract}`);
    selectionRules.add(`phase5_agent_scoring_gating_rules:${agentScoringRun.instructionPack.gating_rules.length}`);
    selectionRules.add(`phase5_agent_scoring_risk_class_rules:${agentScoringRun.instructionPack.risk_class_rules.length}`);
    selectionRules.add(`phase5_agent_scoring_role_rules:${agentScoringRun.instructionPack.role_rules.length}`);
    selectionRules.add("phase5_agent_scoring_rule:quality_signal_priority");
    selectionRules.add("phase5_agent_scoring_rule:risk_signal_priority");
    selectionRules.add("phase5_agent_scoring_rule:quality_risk_tradeoff_ranking");
    selectionRules.add("phase5_agent_scoring_rule:risk_class_enum_constrained");
    selectionRules.add("phase5_agent_scoring_rule:role_enum_constrained");
    selectionRules.add("phase5_agent_scoring_rule:deterministic_sort_tiebreak");
    agentQualityReview.reason_codes.push(
      `scoring_provider:${agentScoringRun.provider}`,
      `scoring_rulebook:${agentScoringRun.instructionPack.version}`,
      `scoring_input:phase4_eligible_candidates:${qualified.length}`,
    );
    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[1],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[2],
      status: "in_progress",
    });
    const targetSelection = derivePhase5SelectionTarget(phase2Output.inputs.user_profile.risk_tolerance);
    const shortlistCount = Math.min(scoredCandidates.length, Math.max(targetSelection * 4, 20));
    const shortlist = scoredCandidates.slice(0, shortlistCount);
    const selectionResult = selectPhase5CandidatesWithStablecoinCap(
      shortlist,
      targetSelection,
      PHASE5_MAX_SELECTED_STABLECOINS,
    );
    const selectedCount = selectionResult.selectedCount;
    const selectedIds = selectionResult.selectedIds;
    const selectedRoleCounts = new Map<Phase5Role, number>();
    for (const role of PHASE5_ROLES) selectedRoleCounts.set(role, 0);
    for (const candidate of shortlist) {
      if (!selectedIds.has(candidate.token.coingecko_id)) continue;
      selectedRoleCounts.set(candidate.role, (selectedRoleCounts.get(candidate.role) ?? 0) + 1);
    }
    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[2],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[3],
      status: "in_progress",
    });
    const rolePolicy = PHASE5_ROLE_POLICY_BY_RISK[phase2Output.inputs.user_profile.risk_tolerance];
    selectionRules.add(`phase5_qualified_candidates:${qualified.length}`);
    selectionRules.add(`phase5_shortlist_candidates:${shortlist.length}`);
    selectionRules.add(`phase5_selected_candidates:${selectedCount}`);
    selectionRules.add(`phase5_target_selection:${targetSelection}`);
    selectionRules.add("phase5_profitability_basis:timeframe_weighted_prior_performance");
    selectionRules.add(`phase5_profitability_timeframe:${phase2Output.inputs.user_profile.investment_timeframe}`);
    selectionRules.add("phase5_profitability_inputs:price_change_pct_7d|price_change_pct_30d");
    selectionRules.add(
      "phase5_stablecoin_risk_basis:liquidity_fragility|structure_fragility|issuer_concentration|cluster_concentration|depeg_drift|volume_leadership",
    );
    selectionRules.add("phase5_role_policy:core_sparse_system_anchor");
    selectionRules.add("phase5_role_policy:risk_tolerance_adaptive_roles_v1");
    selectionRules.add(`phase5_role_policy_risk_tolerance:${phase2Output.inputs.user_profile.risk_tolerance.toLowerCase()}`);
    selectionRules.add(
      `phase5_role_policy_thresholds:speculative>=${round(rolePolicy.speculativeRiskThreshold, 3)}|carry_profitability>=${round(rolePolicy.carryMinProfitability, 3)}|carry_quality>=${round(rolePolicy.carryMinQuality, 3)}|carry_risk<=${round(rolePolicy.carryMaxRisk, 3)}|defensive_stable_risk<=${round(rolePolicy.defensiveStableRiskCeiling, 3)}`,
    );
    for (const role of PHASE5_ROLES) {
      selectionRules.add(`phase5_selected_role_count_${role}:${selectedRoleCounts.get(role) ?? 0}`);
    }
    selectionRules.add(`phase5_selected_stablecoin_cap:${PHASE5_MAX_SELECTED_STABLECOINS}`);
    selectionRules.add(
      "phase5_selected_stablecoin_priority:volume_24h_usd_desc>liquidity_score_desc>structural_score_desc>screening_score_desc>market_cap_rank_asc",
    );
    selectionRules.add(`phase5_selected_stablecoin_count:${selectionResult.selectedStablecoinCount}`);
    if (selectionResult.cappedOutStablecoins > 0) {
      selectionRules.add(`phase5_selected_stablecoin_demotions:${selectionResult.cappedOutStablecoins}`);
    }
    selectionRules.add(`phase5_stablecoin_minimum:${portfolioConstraints.stablecoin_minimum}`);
    selectionRules.add(`phase5_risk_budget:${portfolioConstraints.risk_budget}`);
    selectionRules.add("phase5_transition:phase6_handoff");
    if (selectedCount < 3) {
      missingDomains.add(`phase5_selected_universe_thin:${selectedCount}`);
    }

    const output = validatePhase5Output({
      timestamp: nowIso(),
      execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
      doctrine_version: PHASE5_DOCTRINE_VERSION,
      inputs: {
        phase4_screening_ref: phase4ScreeningRef,
        phase2_policy_ref: phase4Output.inputs.phase2_policy_ref,
        user_profile: {
          risk_tolerance: phase2Output.inputs.user_profile.risk_tolerance,
          investment_timeframe: phase2Output.inputs.user_profile.investment_timeframe,
        },
        portfolio_constraints: portfolioConstraints,
      },
      evaluation: {
        screened_candidates_count: phase4Output.screening.tokens.length,
        qualified_candidates_count: qualified.length,
        selected_candidates_count: selectedCount,
        tokens: shortlist.map((candidate) => {
          const selected = selectedIds.has(candidate.token.coingecko_id);
          const profitability = candidate.profitability;
          const volatility = candidate.volatility;
          const riskClass = candidate.riskClass;
          const role = candidate.role;
          const selectionReasons: string[] = [];
          if (candidate.profileBoost > 0) selectionReasons.push("profile_match_alignment");
          if (candidate.qualityScore >= 0.7) selectionReasons.push("high_quality_signal");
          if (candidate.riskScore <= 0.35) selectionReasons.push("controlled_risk_profile");
          if (candidate.token.token_category === "stablecoin") selectionReasons.push("capital_stability_component");
          if (candidate.volatilityProxyScore <= 0.3) selectionReasons.push("lower_volatility_proxy");
          if (candidate.drawdownProxyScore <= 0.3) selectionReasons.push("lower_drawdown_proxy");
          selectionReasons.push(`risk_class:${riskClass}`);
          selectionReasons.push(`role:${role}`);
          if (selected) {
            selectionReasons.push("selected_in_phase5");
            selectionReasons.push(`selection_bucket:${candidate.bucket}`);
          } else {
            selectionReasons.push("below_shortlist_cutoff");
          }
          return {
            coingecko_id: candidate.token.coingecko_id,
            symbol: candidate.token.symbol,
            name: candidate.token.name,
            market_cap_rank: candidate.token.market_cap_rank,
            token_category: candidate.token.token_category,
            rank_bucket: candidate.token.rank_bucket,
            exchange_depth_proxy: candidate.token.exchange_depth_proxy,
            stablecoin_validation_state: candidate.token.stablecoin_validation_state,
            profile_match_reasons: candidate.token.profile_match_reasons,
            liquidity_score: candidate.token.liquidity_score,
            structural_score: candidate.token.structural_score,
            quality_score: candidate.qualityScore,
            risk_score: candidate.riskScore,
            risk_class: riskClass,
            role,
            profitability,
            volatility,
            volatility_proxy_score: candidate.volatilityProxyScore,
            drawdown_proxy_score: candidate.drawdownProxyScore,
            stablecoin_risk_modifier: candidate.stablecoinRiskModifier,
            composite_score: candidate.compositeScore,
            selection_bucket: candidate.bucket,
            selected,
            selection_reasons: uniqueSorted(selectionReasons),
          };
        }),
      },
      phase_boundaries: {
        portfolio_construction: "PHASE_6",
        decision_report: "POST_PHASE_6",
      },
      audit: {
        sources: phase4Output.audit.sources,
        selection_rules: Array.from(selectionRules).sort(),
        missing_domains: Array.from(missingDomains).sort(),
        agent_quality_review: agentQualityReview,
      },
    });

    context.phase5.inputRef = phase4ScreeningRef;
    context.phase5.output = output;
    context.phase5.error = undefined;
    context.phase5.status = "complete";
    context.phase5.completedAt = nowIso();
    context.updatedAt = context.phase5.completedAt;
    context.phase6 = {
      status: "idle",
      aaaAllocate: defaultAaaAllocateDispatch(),
    };

    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      subPhase: PHASE5_SUB_PHASES[3],
      status: "complete",
    });
    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      status: "complete",
      completedAt: context.phase5.completedAt,
    });
  } catch (error) {
    context.phase5.status = "failed";
    context.phase5.error = error instanceof Error ? error.message : "Phase 5 execution failed.";
    context.phase5.completedAt = nowIso();
    context.updatedAt = context.phase5.completedAt;

    appendJobLog(jobId, {
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
      status: "failed",
      completedAt: context.phase5.completedAt,
      error: context.phase5.error,
    });
    throw error;
  } finally {
    runningPhase5Jobs.delete(jobId);
  }
}

async function executePhase6(jobId: string): Promise<void> {
  const context = getOrCreateJobContext(jobId);
  const phase2Output = context.phase2.output;
  const phase5Output = context.phase5.output;
  if (!phase2Output || context.phase2.status !== "complete") {
    throw new Error("Phase 6 requires completed Phase 2 output.");
  }
  if (!phase5Output || context.phase5.status !== "complete") {
    throw new Error("Phase 6 requires completed Phase 5 output.");
  }

  markPhase6Triggered(jobId);
  appendJobLog(jobId, {
    phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
    status: "in_progress",
    startedAt: nowIso(),
  });

  const selectionRules = new Set<string>([
    "phase6_agent_required:true",
    `execution_model_version:${EXECUTION_MODEL_VERSION}`,
    `doctrine_version:${PHASE6_DOCTRINE_VERSION}`,
    "phase6_allocation_method:bucket_constrained_proportional_cap",
  ]);
  const missingDomains = new Set<string>();
  const phase5QualityRef = hashReference({
    inputs: phase5Output.inputs,
    evaluation: phase5Output.evaluation,
  });
  const agentAllocationReview = {
    used: false,
    model: null as string | null,
    reason_codes: [] as string[],
    skipped_reason: null as string | null,
  };

  try {
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[0],
      status: "in_progress",
    });
    try {
      await initializeAgent();
      agentAllocationReview.used = true;
      agentAllocationReview.model = PHASE2_AGENT_REASONING_PROVIDER;
      agentAllocationReview.reason_codes = [
        `risk_tolerance:${phase2Output.inputs.user_profile.risk_tolerance.toLowerCase()}`,
        `investment_timeframe:${phase2Output.inputs.user_profile.investment_timeframe}`,
        `risk_budget:${round(phase5Output.inputs.portfolio_constraints.risk_budget, 4)}`,
      ];
      selectionRules.add("phase6_agent_engaged:coinbase-agentkit");
    } catch (error) {
      const skippedReason =
        error instanceof Error ? `phase6_agent_engagement_error:${error.message}` : "phase6_agent_engagement_error:unknown";
      agentAllocationReview.skipped_reason = skippedReason;
      missingDomains.add("phase6_agent_identity_unavailable");
      selectionRules.add(`phase6_agent_skipped:${skippedReason}`);
      throw new Error(skippedReason);
    }
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[0],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[1],
      status: "in_progress",
    });
    const preferredShortlist = phase5Output.evaluation.tokens.filter((token) => token.selected);
    const fallbackShortlist =
      preferredShortlist.length > 0
        ? preferredShortlist
        : [...phase5Output.evaluation.tokens].sort(
            (left, right) =>
              right.composite_score - left.composite_score ||
              right.quality_score - left.quality_score ||
              left.risk_score - right.risk_score ||
              (left.market_cap_rank ?? Number.MAX_SAFE_INTEGER) - (right.market_cap_rank ?? Number.MAX_SAFE_INTEGER),
          );
    if (fallbackShortlist.length === 0) {
      throw new Error("Phase 6 requires at least one shortlisted candidate.");
    }
    const allocationCandidates = fallbackShortlist.map<Phase6AllocationCandidate>((token) => ({
      token: {
        coingecko_id: token.coingecko_id,
        symbol: token.symbol,
        name: token.name,
        market_cap_rank: token.market_cap_rank,
        token_category: token.token_category,
        rank_bucket: token.rank_bucket,
        exchange_depth_proxy: token.exchange_depth_proxy,
        stablecoin_validation_state: token.stablecoin_validation_state,
        liquidity_score: token.liquidity_score,
        structural_score: token.structural_score,
      },
      qualityScore: token.quality_score,
      riskScore: token.risk_score,
      compositeScore: token.composite_score,
      profileBoost: Math.min(0.1, token.profile_match_reasons.length * 0.03),
      bucket: token.selection_bucket,
    }));
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[1],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[2],
      status: "in_progress",
    });
    const portfolio = buildPhase6Portfolio(
      allocationCandidates,
      phase5Output.inputs.portfolio_constraints,
      phase2Output.inputs.user_profile.risk_tolerance,
    );
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[2],
      status: "complete",
    });

    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[3],
      status: "in_progress",
    });
    const selectedCount = portfolio.allocations.length;
    selectionRules.add(`phase6_shortlisted_candidates:${allocationCandidates.length}`);
    selectionRules.add(`phase6_selected_candidates:${selectedCount}`);
    selectionRules.add(`phase6_target_selection:${derivePhase5SelectionTarget(phase2Output.inputs.user_profile.risk_tolerance)}`);
    selectionRules.add(`phase6_stablecoin_minimum:${phase5Output.inputs.portfolio_constraints.stablecoin_minimum}`);
    selectionRules.add(`phase6_risk_budget:${phase5Output.inputs.portfolio_constraints.risk_budget}`);
    selectionRules.add("phase6_stablecoin_diversification_guard:issuer_concentration_and_cluster_correlation");
    selectionRules.add("phase6_stablecoin_correlation_model:clustered_not_independent");
    selectionRules.add(
      `phase6_stablecoin_issuer_cap_share:${round(portfolio.stablecoinDiagnostics.issuerCapShareOfStableSleeve, 4)}`,
    );
    selectionRules.add(
      `phase6_stablecoin_cluster_cap_share:${round(portfolio.stablecoinDiagnostics.clusterCapShareOfStableSleeve, 4)}`,
    );
    selectionRules.add(
      `phase6_stablecoin_correlation_dampening:${round(portfolio.stablecoinDiagnostics.correlationDampening, 4)}`,
    );
    selectionRules.add(`phase6_stablecoin_distinct_issuers:${portfolio.stablecoinDiagnostics.distinctIssuers}`);
    selectionRules.add(`phase6_stablecoin_distinct_clusters:${portfolio.stablecoinDiagnostics.distinctClusters}`);
    selectionRules.add(
      `phase6_stablecoin_max_issuer_allocation:${round(portfolio.stablecoinDiagnostics.maxIssuerAllocation, 6)}`,
    );
    selectionRules.add(
      `phase6_stablecoin_max_cluster_allocation:${round(portfolio.stablecoinDiagnostics.maxClusterAllocation, 6)}`,
    );
    if (portfolio.stablecoinDiagnostics.topIssuer) {
      selectionRules.add(
        `phase6_stablecoin_top_issuer:${portfolio.stablecoinDiagnostics.topIssuer.replace(/[^a-z0-9_-]/gi, "_").toLowerCase()}`,
      );
    }
    if (portfolio.stablecoinDiagnostics.topCluster) {
      selectionRules.add(
        `phase6_stablecoin_top_cluster:${portfolio.stablecoinDiagnostics.topCluster.replace(/[^a-z0-9_-]/gi, "_").toLowerCase()}`,
      );
    }
    if (portfolio.stablecoinAllocation > 0 && portfolio.stablecoinDiagnostics.distinctIssuers < 2) {
      missingDomains.add("phase6_stablecoin_issuer_diversification_limited");
    }
    if (portfolio.stablecoinAllocation > 0 && portfolio.stablecoinDiagnostics.distinctClusters < 2) {
      missingDomains.add("phase6_stablecoin_cluster_diversification_limited");
    }
    if (selectedCount < 3) {
      missingDomains.add(`phase6_selected_universe_thin:${selectedCount}`);
    }

    const output = validatePhase6Output({
      timestamp: nowIso(),
      execution_model_version: EXECUTION_MODEL_VERSION as "Selun-1.0.0",
      doctrine_version: PHASE6_DOCTRINE_VERSION,
      inputs: {
        phase5_quality_ref: phase5QualityRef,
        phase2_policy_ref: phase5Output.inputs.phase2_policy_ref,
        user_profile: {
          risk_tolerance: phase2Output.inputs.user_profile.risk_tolerance,
          investment_timeframe: phase2Output.inputs.user_profile.investment_timeframe,
        },
        portfolio_constraints: phase5Output.inputs.portfolio_constraints,
      },
      allocation: {
        shortlisted_candidates_count: allocationCandidates.length,
        selected_candidates_count: selectedCount,
        allocations: portfolio.allocations,
        total_allocation_weight: round(
          clamp(
            portfolio.allocations.reduce((sum, allocation) => sum + allocation.allocation_weight, 0),
            0,
            1,
          ),
          6,
        ),
        stablecoin_allocation: portfolio.stablecoinAllocation,
        expected_portfolio_volatility: portfolio.expectedVolatility,
        concentration_index: portfolio.concentrationIndex,
      },
      phase_boundaries: {
        decision_report: "POST_PHASE_6",
      },
      audit: {
        sources: phase5Output.audit.sources,
        selection_rules: Array.from(selectionRules).sort(),
        missing_domains: Array.from(missingDomains).sort(),
        agent_allocation_review: agentAllocationReview,
      },
    });

    context.phase6.inputRef = phase5QualityRef;
    context.phase6.output = output;
    context.phase6.error = undefined;
    context.phase6.status = "complete";
    context.phase6.completedAt = nowIso();
    context.updatedAt = context.phase6.completedAt;
    await forwardPhase6ToAaaAllocator(jobId, context);

    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      subPhase: PHASE6_SUB_PHASES[3],
      status: "complete",
    });
    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      status: "complete",
      completedAt: context.phase6.completedAt,
    });
  } catch (error) {
    context.phase6.status = "failed";
    context.phase6.error = error instanceof Error ? error.message : "Phase 6 execution failed.";
    context.phase6.completedAt = nowIso();
    context.updatedAt = context.phase6.completedAt;

    appendJobLog(jobId, {
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
      status: "failed",
      completedAt: context.phase6.completedAt,
      error: context.phase6.error,
    });
    throw error;
  } finally {
    runningPhase6Jobs.delete(jobId);
  }
}

async function engageSelunAgent(jobId: string, limitations: Set<string>): Promise<void> {
  emitExecutionLog({
    phase: REVIEW_MARKET_CONDITIONS_PHASE,
    action: "engage_selun_agent",
    status: "started",
    transactionHash: null,
  });

  try {
    const identity = await initializeAgent();
    const context = getOrCreateJobContext(jobId);
    context.phase1.agentIdentity = {
      agentId: identity.agentId,
      walletAddress: identity.walletAddress,
      network: identity.network,
    };

    emitExecutionLog({
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      action: "engage_selun_agent",
      status: "success",
      transactionHash: null,
    });
  } catch (error) {
    const reason = error instanceof Error ? error.message : "unknown";
    limitations.add("agent_identity_unavailable");
    emitExecutionLog({
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      action: "engage_selun_agent",
      status: "error",
      transactionHash: null,
    });
    throw new Error(`phase1_agent_required:${reason}`);
  }
}

async function executePhase1(input: NormalizedPhase1Input): Promise<void> {
  restoreMacroSnapshotFromDisk();
  restoreSourceIntelligenceFromDisk();
  const tuning = getPhase1ExecutionTuning();
  const context = getOrCreateJobContext(input.jobId);
  context.phase1.status = "in_progress";
  context.phase1.startedAt = nowIso();
  context.phase1.completedAt = undefined;
  context.phase1.error = undefined;
  context.phase1.attempts += 1;
  context.phase1.input = input;
  context.updatedAt = context.phase1.startedAt;
  if (input.walletAddress) {
    latestJobIdByWallet.set(input.walletAddress, input.jobId);
  }

  appendJobLog(input.jobId, {
    phase: REVIEW_MARKET_CONDITIONS_PHASE,
    status: "in_progress",
    startedAt: context.phase1.startedAt,
  });

  const toolCalls = new Set<string>();
  const sourceReferences = new Map<string, SourceReference>();
  const sourceSelection = createSourceSelectionState();
  const configuredVolatilityOrder = parseOrderedSources(
    "PHASE1_VOLATILITY_SOURCE_ORDER",
    DEFAULT_VOLATILITY_SOURCE_ORDER,
  );
  const volatilityDiscoveryPool = parseOrderedSources(
    "PHASE1_DISCOVERY_VOLATILITY_POOL",
    DEFAULT_VOLATILITY_DISCOVERY_POOL,
  );
  const configuredGlobalMetricsOrder = parseOrderedSources(
    "PHASE1_GLOBAL_METRICS_SOURCE_ORDER",
    DEFAULT_GLOBAL_METRICS_SOURCE_ORDER,
  );
  const globalMetricsDiscoveryPool = parseOrderedSources(
    "PHASE1_DISCOVERY_GLOBAL_METRICS_POOL",
    DEFAULT_GLOBAL_METRICS_DISCOVERY_POOL,
  );
  const configuredSentimentOrder = parseOrderedSources(
    "PHASE1_SENTIMENT_SOURCE_ORDER",
    DEFAULT_SENTIMENT_SOURCE_ORDER,
  );
  const sentimentDiscoveryPool = parseOrderedSources(
    "PHASE1_DISCOVERY_SENTIMENT_POOL",
    DEFAULT_SENTIMENT_DISCOVERY_POOL,
  );
  const assumptions = new Set<string>([
    "phase1_agent_required:true",
    "deterministic_thresholds_only",
    `time_window:${input.timeWindow}`,
    `risk_mode:${input.riskMode}`,
    "survivability_filter_enabled",
    `prompt_version:${SYSTEM_PROMPT_VERSION}`,
    `prompt_hash:${hashSystemPrompt()}`,
    `execution_model_version:${EXECUTION_MODEL_VERSION}`,
    `doctrine_version:${DOCTRINE_VERSION}`,
    "multi_source_consistency_pass_enabled",
    "fear_greed_weighting_enabled",
    "market_wide_breadth_scan_enabled",
    `volatility_source_order:${configuredVolatilityOrder.join(">")}`,
    `volatility_discovery_pool:${volatilityDiscoveryPool.join(">")}`,
    `global_metrics_source_order:${configuredGlobalMetricsOrder.join(">")}`,
    `global_metrics_discovery_pool:${globalMetricsDiscoveryPool.join(">")}`,
    `sentiment_source_order:${configuredSentimentOrder.join(">")}`,
    `sentiment_discovery_pool:${sentimentDiscoveryPool.join(">")}`,
  ]);
  const limitations = new Set<string>();

  try {
    await engageSelunAgent(input.jobId, limitations);

    let sharedData = fallbackSharedMarketData();
    let volatility = fallbackVolatilityDomain();
    let liquidity = fallbackLiquidityDomain();
    let sentiment = fallbackSentimentDomain();
    let usabilityIssues: string[] = [];
    let usedSnapshotRecovery = false;
    let snapshotRecoveryAgeMs: number | null = null;

    for (let attempt = 1; attempt <= tuning.maxUsableDataAttempts; attempt += 1) {
      assumptions.add(`macro_data_attempt:${attempt}`);

      appendJobLog(input.jobId, {
        phase: REVIEW_MARKET_CONDITIONS_PHASE,
        subPhase: "collecting_market_volatility_data",
        status: "in_progress",
      });
      sharedData = await collectSharedMarketData(
        toolCalls,
        sourceReferences,
        limitations,
        sourceSelection,
      );
      volatility = await collectVolatilityDomain(sharedData, limitations);
      appendJobLog(input.jobId, {
        phase: REVIEW_MARKET_CONDITIONS_PHASE,
        subPhase: "collecting_market_volatility_data",
        status: "complete",
      });

      appendJobLog(input.jobId, {
        phase: REVIEW_MARKET_CONDITIONS_PHASE,
        subPhase: "collecting_liquidity_metrics",
        status: "in_progress",
      });
      liquidity = await collectLiquidityDomain(
        sharedData,
        toolCalls,
        sourceReferences,
        limitations,
        sourceSelection,
      );
      appendJobLog(input.jobId, {
        phase: REVIEW_MARKET_CONDITIONS_PHASE,
        subPhase: "collecting_liquidity_metrics",
        status: "complete",
      });

      appendJobLog(input.jobId, {
        phase: REVIEW_MARKET_CONDITIONS_PHASE,
        subPhase: "collecting_macro_sentiment_data",
        status: "in_progress",
      });
      sentiment = await collectSentimentDomain(toolCalls, sourceReferences, limitations, sourceSelection);
      appendJobLog(input.jobId, {
        phase: REVIEW_MARKET_CONDITIONS_PHASE,
        subPhase: "collecting_macro_sentiment_data",
        status: "complete",
      });

      usabilityIssues = getMacroUsabilityIssues(volatility, liquidity, sentiment, sharedData);
      if (usabilityIssues.length === 0) {
        break;
      }

      limitations.add(`macro_data_unusable_attempt_${attempt}`);
      for (const issue of usabilityIssues) {
        limitations.add(issue);
      }

      if (attempt < tuning.maxUsableDataAttempts) {
        const delayMs = Math.min(tuning.retryDelayMs * attempt, tuning.maxRetryDelayMs);
        assumptions.add(`macro_retry_delay_ms:${delayMs}`);
        await sleep(delayMs);
      }
    }

    if (usabilityIssues.length > 0) {
      if (lastKnownGoodMacroSnapshot) {
        snapshotRecoveryAgeMs = Date.now() - new Date(lastKnownGoodMacroSnapshot.capturedAt).getTime();
      }

      if (
        lastKnownGoodMacroSnapshot &&
        snapshotRecoveryAgeMs !== null &&
        Number.isFinite(snapshotRecoveryAgeMs) &&
        snapshotRecoveryAgeMs <= tuning.snapshotMaxAgeMs
      ) {
        usedSnapshotRecovery = true;
        volatility = cloneVolatilityDomain(lastKnownGoodMacroSnapshot.volatility);
        liquidity = cloneLiquidityDomain(lastKnownGoodMacroSnapshot.liquidity);
        sentiment = cloneSentimentDomain(lastKnownGoodMacroSnapshot.sentiment);

        for (const source of lastKnownGoodMacroSnapshot.sources) {
          if (!sourceReferences.has(source.id)) {
            sourceReferences.set(source.id, {
              id: source.id,
              provider: source.provider,
              endpoint: source.endpoint,
              url: source.url,
              fetched_at: source.fetched_at,
            });
          }
        }
        mergeSnapshotSelection(sourceSelection, lastKnownGoodMacroSnapshot.sourceSelection);

        limitations.add("live_macro_unavailable_recovered_with_last_known_good_snapshot");
        limitations.add(`snapshot_recovery_age_ms:${Math.max(0, Math.floor(snapshotRecoveryAgeMs))}`);
        for (const issue of usabilityIssues) {
          limitations.add(`recovery_issue:${issue}`);
        }
        assumptions.add("recovery_mode:last_known_good_snapshot");
      } else {
        throw new Error(
          `Phase 1 could not obtain usable macro values after ${tuning.maxUsableDataAttempts} attempts: ${usabilityIssues.join(", ")}.`,
        );
      }
    }

    appendJobLog(input.jobId, {
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      subPhase: "evaluating_market_alignment",
      status: "in_progress",
    });
    const baseAlignment = usedSnapshotRecovery && lastKnownGoodMacroSnapshot
      ? cloneAlignmentDomain(lastKnownGoodMacroSnapshot.alignment)
      : deriveConfidenceAndUncertainty(volatility, liquidity, sentiment, sharedData);
    const alignment = applyConsistencyCalibration(baseAlignment, sentiment, assumptions, limitations);
    assumptions.add(`sentiment_source_count:${sentiment.sourceCount}`);
    assumptions.add(`sentiment_source_consensus:${round(sentiment.sourceConsensus, 4)}`);
    assumptions.add(`fear_greed_weighted:${sentiment.fearGreedIncluded}`);
    assumptions.add(
      `fear_greed_index:${sentiment.fearGreedIndex === null ? "unavailable" : round(sentiment.fearGreedIndex, 3)}`,
    );
    assumptions.add(`market_breadth_asset_count:${sharedData.marketBreadthAssetCount}`);
    assumptions.add(`market_breadth_positive_ratio:${round(sharedData.marketBreadthPositiveRatio, 4)}`);
    const correlation = deriveCorrelationState(sharedData);
    const marketRegime = deriveMarketRegime(volatility, liquidity, sentiment, alignment, correlation, sharedData);
    appendJobLog(input.jobId, {
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      subPhase: "evaluating_market_alignment",
      status: "complete",
    });

    appendJobLog(input.jobId, {
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      subPhase: "finalizing_market_snapshot",
      status: "in_progress",
    });
    limitations.add("asset_evaluation_deferred_to_phase_3");
    limitations.add("portfolio_construction_deferred_to_phase_4");
    assumptions.add("phase_boundary_asset_evaluation:PHASE_3");
    assumptions.add("phase_boundary_portfolio_construction:PHASE_4");
    const allocationAuthorization = deriveAllocationAuthorization(
      input,
      marketRegime,
      volatility,
      liquidity,
      sentiment,
    );

    const output = validateOutputWithRetry(() =>
      buildPhase1Output(
        input,
        volatility,
        liquidity,
        sentiment,
        alignment,
        allocationAuthorization,
        sourceReferences,
        sourceSelection,
        assumptions,
        limitations,
      ),
    );

    if (!usedSnapshotRecovery) {
      lastKnownGoodMacroSnapshot = {
        capturedAt: nowIso(),
        volatility: cloneVolatilityDomain(volatility),
        liquidity: cloneLiquidityDomain(liquidity),
        sentiment: cloneSentimentDomain(sentiment),
        alignment: cloneAlignmentDomain(alignment),
        sources: cloneSourceReferences(Array.from(sourceReferences.values())),
        sourceSelection: cloneSourceSelectionRecords(Object.values(sourceSelection)),
      };
      persistMacroSnapshot(lastKnownGoodMacroSnapshot);
    }

    context.phase1.output = output;
    appendJobLog(input.jobId, {
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      subPhase: "finalizing_market_snapshot",
      status: "complete",
    });

    context.phase1.status = "complete";
    context.phase1.completedAt = nowIso();
    context.updatedAt = context.phase1.completedAt;

    appendJobLog(input.jobId, {
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      status: "complete",
      completedAt: context.phase1.completedAt,
    });

    await determineAllocationPolicy(input.jobId);
  } catch (error) {
    context.phase1.status = "failed";
    context.phase1.error = error instanceof Error ? error.message : "Unknown phase execution failure.";
    context.phase1.completedAt = nowIso();
    context.updatedAt = context.phase1.completedAt;

    appendJobLog(input.jobId, {
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      status: "failed",
      completedAt: context.phase1.completedAt,
      error: context.phase1.error,
    });
  } finally {
    persistSourceIntelligence();
    runningJobs.delete(input.jobId);
  }
}

export function runPhase1(input: string | Phase1RunInput): void {
  const normalized = normalizeRunInput(input);
  if (!normalized.jobId) {
    throw new Error("jobId is required.");
  }

  getOrCreateJobContext(normalized.jobId);
  if (runningJobs.has(normalized.jobId)) {
    return;
  }

  runningJobs.add(normalized.jobId);
  void executePhase1(normalized);
}

export function runPhase3(jobId: string): void {
  const normalizedJobId = jobId.trim();
  if (!normalizedJobId) {
    throw new Error("jobId is required.");
  }

  const context = getOrCreateJobContext(normalizedJobId);
  if (context.phase2.status !== "complete" || !context.phase2.output) {
    throw new Error("Phase 3 can only start after Phase 2 completes.");
  }
  if (runningPhase3Jobs.has(normalizedJobId)) {
    return;
  }

  runningPhase3Jobs.add(normalizedJobId);
  void executePhase3(normalizedJobId);
}

export function runPhase4(jobId: string): void {
  const normalizedJobId = jobId.trim();
  if (!normalizedJobId) {
    throw new Error("jobId is required.");
  }

  const context = getOrCreateJobContext(normalizedJobId);
  if (context.phase3.status !== "complete" || !context.phase3.output) {
    throw new Error("Phase 4 can only start after Phase 3 completes.");
  }
  if (runningPhase4Jobs.has(normalizedJobId)) {
    return;
  }

  runningPhase4Jobs.add(normalizedJobId);
  void executePhase4(normalizedJobId);
}

export function runPhase5(jobId: string): void {
  const normalizedJobId = jobId.trim();
  if (!normalizedJobId) {
    throw new Error("jobId is required.");
  }

  const context = getOrCreateJobContext(normalizedJobId);
  if (context.phase4.status !== "complete" || !context.phase4.output) {
    throw new Error("Phase 5 can only start after Phase 4 completes.");
  }
  if (runningPhase5Jobs.has(normalizedJobId)) {
    return;
  }

  runningPhase5Jobs.add(normalizedJobId);
  void executePhase5(normalizedJobId);
}

export function runPhase6(jobId: string): void {
  const normalizedJobId = jobId.trim();
  if (!normalizedJobId) {
    throw new Error("jobId is required.");
  }

  const context = getOrCreateJobContext(normalizedJobId);
  if (context.phase5.status !== "complete" || !context.phase5.output) {
    throw new Error("Phase 6 can only start after Phase 5 completes.");
  }
  if (runningPhase6Jobs.has(normalizedJobId)) {
    return;
  }

  runningPhase6Jobs.add(normalizedJobId);
  void executePhase6(normalizedJobId);
}

export function getExecutionStatus(jobId: string): {
  found: boolean;
  jobId: string;
  phase: typeof REVIEW_MARKET_CONDITIONS_PHASE;
  status: JobPhaseStatus | "not_found";
  logs: JobProgressLog[];
  jobContext: JobContext | null;
} {
  const context = jobContextById.get(jobId);
  if (!context) {
    return {
      found: false,
      jobId,
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      status: "not_found",
      logs: [],
      jobContext: null,
    };
  }

  return {
    found: true,
    jobId,
    phase: REVIEW_MARKET_CONDITIONS_PHASE,
    status: context.phase1.status,
    logs: context.logs,
    jobContext: context,
  };
}

export function getExecutionStatusByWallet(walletAddress: string): {
  found: boolean;
  walletAddress: string;
  jobId: string | null;
  phase: typeof REVIEW_MARKET_CONDITIONS_PHASE;
  status: JobPhaseStatus | "not_found";
  logs: JobProgressLog[];
  jobContext: JobContext | null;
} {
  const walletKey = normalizeWalletKey(walletAddress);
  if (!walletKey) {
    return {
      found: false,
      walletAddress,
      jobId: null,
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      status: "not_found",
      logs: [],
      jobContext: null,
    };
  }

  const jobId = latestJobIdByWallet.get(walletKey);
  if (!jobId) {
    return {
      found: false,
      walletAddress: walletKey,
      jobId: null,
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
      status: "not_found",
      logs: [],
      jobContext: null,
    };
  }

  const status = getExecutionStatus(jobId);
  return {
    found: status.found,
    walletAddress: walletKey,
    jobId,
    phase: status.phase,
    status: status.status,
    logs: status.logs,
    jobContext: status.jobContext,
  };
}

export {
  PHASE1_SUB_PHASES,
  PHASE3_SUB_PHASES,
  PHASE4_SUB_PHASES,
  PHASE5_SUB_PHASES,
  PHASE6_SUB_PHASES,
  SELUN_PHASE1_SYSTEM_PROMPT,
  PHASE2_SYSTEM_PROMPT_VERSION,
  SELUN_PHASE2_SYSTEM_PROMPT,
};
