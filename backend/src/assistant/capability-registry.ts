import { z } from "zod";
import { assetLabel, resolveAsset, type KnownAsset } from "./asset-resolver";

/**
 * Capability registry: the allowlist of what the assistant may offer.
 *
 * Descriptions, schemas, and customer language live here. Prices and endpoints
 * do not: they are read from Selun's live x402 catalog, so the assistant can
 * never quote a figure the paid route would not charge. A catalog product with
 * no entry here is invisible to the assistant.
 */

export type ToolCapabilityId =
  | "market_regime"
  | "policy_envelope"
  | "asset_scorecard"
  | "rebalance"
  | "sce_continuity_mode"
  | "sce_case_relevance"
  | "sce_risk_evaluate";

export type HandoffCapabilityId = "allocate" | "allocate_with_report";
export type PayPalCapabilityId = "comprehensive_report" | "monthly_intelligence";
export type CapabilityId = ToolCapabilityId | HandoffCapabilityId | PayPalCapabilityId;

export type CatalogResource = {
  productId: string;
  endpoint: string;
  pricing: { amountUsdc: string };
};

export type CapabilityCatalog = { resources: CatalogResource[] };

export type Capability = {
  id: CapabilityId;
  title: string;
  summary: string;
  whenToUse: string;
  outputType: string;
  duration: string;
  limitations: string | null;
  execution: "x402_tool" | "wizard_handoff" | "paypal_checkout" | "paypal_subscription";
  endpoint: string;
  /** The amount in `currency`. PayPal products are priced in USD, everything else in USDC. */
  priceUsdc: string;
  currency: "USDC" | "USD";
};

export type PayPalPrices = { reportUsd: string; subscriptionUsd: string };

export type NormalizedInput =
  | {
    ok: true;
    body: Record<string, unknown>;
    /** Customer-readable description of what will be analyzed. */
    details: { label: string; value: string }[];
    /** Defaults the customer did not choose, disclosed before payment. */
    assumptions: string[];
  }
  | {
    ok: false;
    code: "ambiguous_asset" | "unknown_asset" | "invalid_input";
    message: string;
    options?: KnownAsset[];
  };

export type DataCoverageItem = { label: string; included: boolean; note: string };

/**
 * Fixed statements shown on every quote, result, and PDF. They describe what the service
 * returns, which is not something the model decides or can change.
 */
const MARKET_PIPELINE_COVERAGE: DataCoverageItem[] = [
  { label: "Live prices", included: false, note: "No prices are shown. Market feeds are only inputs to the analysis." },
  { label: "Current news", included: false, note: "No news is shown. Recent headlines only feed a sentiment reading." },
  { label: "Real-time market data", included: false, note: "It is a snapshot from when the analysis runs and does not update afterwards." },
];

const SCORECARD_COVERAGE: DataCoverageItem[] = [
  { label: "Price history", included: true, note: "Up to 365 daily closes through the last full UTC day, used to measure volatility and drawdown." },
  ...MARKET_PIPELINE_COVERAGE,
];

const CONTINUITY_ENGINE_COVERAGE: DataCoverageItem[] = [
  { label: "Live prices", included: false, note: "This check does not use or show market prices." },
  { label: "Current news", included: false, note: "No news is shown." },
  { label: "Real-time market data", included: false, note: "It reports Sagitta's continuity engine at the time of the check, and may be briefly cached." },
];

type ToolSpec = {
  id: ToolCapabilityId | PayPalCapabilityId;
  dataCoverage: DataCoverageItem[];
  title: string;
  summary: string;
  whenToUse: string;
  outputType: string;
  duration: string;
  limitations: string | null;
  parameters: Record<string, unknown>;
  normalize: (raw: unknown) => NormalizedInput;
};

const RISK = ["Conservative", "Balanced", "Growth", "Aggressive"] as const;
const TIMEFRAME = ["<1_year", "1-3_years", "3+_years"] as const;
const SEGMENT = ["Bluechips", "Memecoins", "Gaming", "Yield Farm"] as const;
const TIMEFRAME_LABEL: Record<(typeof TIMEFRAME)[number], string> = {
  "<1_year": "Under 1 year",
  "1-3_years": "1 to 3 years",
  "3+_years": "3+ years",
};

const profileProperties = {
  riskTolerance: { type: "string", enum: [...RISK], description: "Only set when the customer stated a risk preference." },
  timeframe: { type: "string", enum: [...TIMEFRAME], description: "Only set when the customer stated a time horizon." },
  portfolioSegment: { type: "string", enum: [...SEGMENT], description: "Only set when the customer named a segment." },
};

const profileShape = {
  riskTolerance: z.enum(RISK).optional(),
  timeframe: z.enum(TIMEFRAME).optional(),
  portfolioSegment: z.enum(SEGMENT).optional(),
};

type ProfileInput = {
  riskTolerance?: (typeof RISK)[number];
  timeframe?: (typeof TIMEFRAME)[number];
  portfolioSegment?: (typeof SEGMENT)[number];
};

function resolveProfile(input: ProfileInput) {
  const assumptions: string[] = [];
  if (!input.riskTolerance) assumptions.push("Balanced risk profile");
  if (!input.timeframe) assumptions.push("1 to 3 year horizon");
  if (!input.portfolioSegment) assumptions.push("Bluechips segment");
  const body = {
    riskTolerance: input.riskTolerance ?? "Balanced",
    timeframe: input.timeframe ?? "1-3_years",
    portfolioSegment: input.portfolioSegment ?? "Bluechips",
  };
  return {
    body,
    assumptions,
    details: [
      { label: "Risk profile", value: body.riskTolerance },
      { label: "Horizon", value: TIMEFRAME_LABEL[body.timeframe] },
      { label: "Segment", value: body.portfolioSegment },
    ],
  };
}

function invalid(error: z.ZodError): NormalizedInput {
  const issue = error.issues[0];
  const path = issue?.path.join(".") || "input";
  return { ok: false, code: "invalid_input", message: `${path}: ${issue?.message ?? "invalid value"}` };
}

function resolveAssetList(queries: string[]): { assets: KnownAsset[] } | { failure: NormalizedInput } {
  const assets: KnownAsset[] = [];
  for (const query of queries) {
    const resolution = resolveAsset(query);
    if (resolution.status === "ambiguous") {
      return {
        failure: {
          ok: false,
          code: "ambiguous_asset",
          message: `"${query}" is ambiguous. ${resolution.reason}`,
          options: resolution.options,
        },
      };
    }
    if (resolution.status === "unknown") {
      return {
        failure: { ok: false, code: "unknown_asset", message: `Selun could not confirm an asset named "${query}".` },
      };
    }
    if (!assets.some((asset) => asset.symbol === resolution.asset.symbol)) assets.push(resolution.asset);
  }
  return { assets };
}

const profileOnlySchema = z.object(profileShape).strict();

function normalizeProfileOnly(raw: unknown): NormalizedInput {
  const parsed = profileOnlySchema.safeParse(raw ?? {});
  if (!parsed.success) return invalid(parsed.error);
  const profile = resolveProfile(parsed.data);
  return { ok: true, body: profile.body, details: profile.details, assumptions: profile.assumptions };
}

const scorecardSchema = z
  .object({ ...profileShape, assets: z.array(z.string().min(1).max(64)).min(1).max(12) })
  .strict();

const rebalanceSchema = z
  .object({
    ...profileShape,
    holdings: z
      .array(z.object({ asset: z.string().min(1).max(64), usdValue: z.number().finite().nonnegative() }).strict())
      .min(1)
      .max(40),
  })
  .strict();

const chainId = z.number().int().positive().max(2 ** 53).optional();
const shortText = z.string().trim().min(1).max(120);
const shortTextList = z.array(shortText).max(12).optional();

const continuitySchema = z
  .object({
    scope: z.enum(["global", "chain", "threat_family", "doctrine_tag"]).optional(),
    chain_id: chainId,
    threat_family: shortText.optional(),
    doctrine_tag: shortText.optional(),
    requested_action: shortText.optional(),
  })
  .strict();

const caseRelevanceSchema = z
  .object({
    protocol_name: shortText.optional(),
    chain_id: chainId,
    asset_types: shortTextList,
    threat_families: shortTextList,
    doctrine_tags: shortTextList,
    requested_action: shortText.optional(),
  })
  .strict();

const riskEvaluateSchema = z
  .object({
    protocol_name: shortText,
    chain_id: chainId,
    addresses: z.array(z.string().regex(/^0x[0-9a-fA-F]{40}$/)).max(12).optional(),
    asset_types: shortTextList,
    threat_families: shortTextList,
    doctrine_tags: shortTextList,
    requested_action: shortText.optional(),
    risk_tolerance: z.string().trim().min(1).max(40).optional(),
  })
  .strict();

function describeSce(body: Record<string, unknown>): { label: string; value: string }[] {
  const details: { label: string; value: string }[] = [];
  if (typeof body.protocol_name === "string") details.push({ label: "Protocol", value: body.protocol_name });
  if (typeof body.scope === "string") details.push({ label: "Scope", value: body.scope.replaceAll("_", " ") });
  if (typeof body.chain_id === "number") details.push({ label: "Chain ID", value: String(body.chain_id) });
  if (typeof body.requested_action === "string") details.push({ label: "Planned action", value: body.requested_action });
  if (Array.isArray(body.threat_families)) details.push({ label: "Threat families", value: body.threat_families.join(", ") });
  return details;
}

const TOOL_SPECS: ToolSpec[] = [
  {
    id: "market_regime",
    dataCoverage: MARKET_PIPELINE_COVERAGE,
    title: "Market Regime",
    summary: "Reads the current crypto market across volatility, liquidity, and sentiment, then classifies the overall conditions.",
    whenToUse: "Questions about the whole crypto market, such as 'how is the market right now'. Never the only answer to a request that names a token: use Asset Scorecard for that token. It is not a price quote or a single-asset forecast.",
    outputType: "market_assessment",
    duration: "Usually a few minutes",
    limitations: "Covers the whole crypto market, not one asset.",
    parameters: { type: "object", properties: profileProperties, additionalProperties: false },
    normalize: normalizeProfileOnly,
  },
  {
    id: "asset_scorecard",
    dataCoverage: SCORECARD_COVERAGE,
    title: "Asset Scorecard",
    summary: "Measures each asset's volatility and drawdown from daily price history, scores its price risk and market standing, and shows the role it could play in a portfolio.",
    whenToUse: "Any request about specific named tokens, up to 12 at once, such as 'tell me about BTC', 'Bitcoin update', 'analyze the risks of Ethereum', or 'compare SOL and ADA'. It returns scores and measured price history, not live prices or news.",
    outputType: "asset_scores",
    duration: "Can take several minutes",
    limitations: "Volatility and drawdown describe the past year, not the future. Liquidity and market standing top out for the largest assets, so those can match. An asset with less than 180 days of price history, or outside the group screened for the chosen segment, can't get an overall score. Selun confirms both before offering this analysis and locks that decision to your purchase. If a price feed fails during the run, you can retry without paying again.",
    parameters: {
      type: "object",
      properties: {
        assets: {
          type: "array",
          items: { type: "string" },
          minItems: 1,
          maxItems: 12,
          description: "Asset names or symbols, for example [\"BTC\"]. Selun re-resolves each one on the server.",
        },
        ...profileProperties,
      },
      required: ["assets"],
      additionalProperties: false,
    },
    normalize(raw) {
      const parsed = scorecardSchema.safeParse(raw);
      if (!parsed.success) return invalid(parsed.error);
      const resolved = resolveAssetList(parsed.data.assets);
      if ("failure" in resolved) return resolved.failure;
      const profile = resolveProfile(parsed.data);
      return {
        ok: true,
        body: { ...profile.body, assets: resolved.assets.map((asset) => asset.symbol) },
        details: [{ label: "Assets", value: resolved.assets.map(assetLabel).join(", ") }, ...profile.details],
        assumptions: profile.assumptions,
      };
    },
  },
  {
    id: "policy_envelope",
    dataCoverage: MARKET_PIPELINE_COVERAGE,
    title: "Policy Envelope",
    summary: "Works out the risk limits for a profile: exposure caps, stablecoin floor, and risk budget.",
    whenToUse: "Questions about what limits should apply to a given risk level and time horizon.",
    outputType: "risk_policy",
    duration: "Usually a few minutes",
    limitations: null,
    parameters: { type: "object", properties: profileProperties, additionalProperties: false },
    normalize: normalizeProfileOnly,
  },
  {
    id: "rebalance",
    dataCoverage: MARKET_PIPELINE_COVERAGE,
    title: "Rebalance Check",
    summary: "Compares current holdings with Selun's target portfolio and shows what to increase, reduce, or hold.",
    whenToUse: "The customer shared their holdings with US dollar values and wants to know how to rebalance.",
    outputType: "rebalance_plan",
    duration: "Can take several minutes",
    limitations: "Selun shows suggested changes only. It never places trades.",
    parameters: {
      type: "object",
      properties: {
        holdings: {
          type: "array",
          minItems: 1,
          items: {
            type: "object",
            properties: { asset: { type: "string" }, usdValue: { type: "number", minimum: 0 } },
            required: ["asset", "usdValue"],
            additionalProperties: false,
          },
          description: "Only amounts the customer actually provided. Never estimate them.",
        },
        ...profileProperties,
      },
      required: ["holdings"],
      additionalProperties: false,
    },
    normalize(raw) {
      const parsed = rebalanceSchema.safeParse(raw);
      if (!parsed.success) return invalid(parsed.error);
      const holdings: { asset: string; name: string; usdValue: number }[] = [];
      for (const holding of parsed.data.holdings) {
        const resolved = resolveAssetList([holding.asset]);
        if ("failure" in resolved) return resolved.failure;
        const asset = resolved.assets[0];
        holdings.push({ asset: asset.symbol, name: asset.name, usdValue: holding.usdValue });
      }
      const profile = resolveProfile(parsed.data);
      return {
        ok: true,
        body: { ...profile.body, holdings },
        details: [
          { label: "Holdings", value: holdings.map((entry) => `${entry.asset} $${entry.usdValue.toLocaleString("en-US")}`).join(", ") },
          ...profile.details,
        ],
        assumptions: profile.assumptions,
      };
    },
  },
  {
    id: "sce_continuity_mode",
    dataCoverage: CONTINUITY_ENGINE_COVERAGE,
    title: "Safety Check (Continuity Mode)",
    summary: "Reports whether Sagitta's continuity engine sees normal, degraded, or incident conditions across DeFi risk domains.",
    whenToUse: "'Is it safe to use DeFi right now' or a safety check before moving funds.",
    outputType: "safety_status",
    duration: "Usually under a minute",
    limitations: null,
    parameters: {
      type: "object",
      properties: {
        scope: { type: "string", enum: ["global", "chain", "threat_family", "doctrine_tag"] },
        chain_id: { type: "integer" },
        threat_family: { type: "string" },
        doctrine_tag: { type: "string" },
        requested_action: { type: "string" },
      },
      additionalProperties: false,
    },
    normalize(raw) {
      const parsed = continuitySchema.safeParse(raw ?? {});
      if (!parsed.success) return invalid(parsed.error);
      const body: Record<string, unknown> = { scope: "global", ...parsed.data };
      return { ok: true, body, details: describeSce(body), assumptions: parsed.data.scope ? [] : ["Global scope"] };
    },
  },
  {
    id: "sce_risk_evaluate",
    dataCoverage: CONTINUITY_ENGINE_COVERAGE,
    title: "Protocol Risk Evaluation",
    summary: "Evaluates a specific protocol and returns a risk level, a risk score, and an allow, escalate, or block decision.",
    whenToUse: "A customer plans to use a named DeFi protocol such as Aave or Uniswap, or asks whether it is safe. It evaluates protocols, not tokens like BTC, ETH, or the AAVE token; use Asset Scorecard for those.",
    outputType: "protocol_risk",
    duration: "Usually under a minute",
    limitations: "Evaluates protocols, not tokens.",
    parameters: {
      type: "object",
      properties: {
        protocol_name: { type: "string" },
        chain_id: { type: "integer" },
        addresses: { type: "array", items: { type: "string" } },
        requested_action: { type: "string" },
        risk_tolerance: { type: "string" },
      },
      required: ["protocol_name"],
      additionalProperties: false,
    },
    normalize(raw) {
      const parsed = riskEvaluateSchema.safeParse(raw);
      if (!parsed.success) return invalid(parsed.error);
      return { ok: true, body: parsed.data, details: describeSce(parsed.data), assumptions: [] };
    },
  },
  {
    id: "sce_case_relevance",
    dataCoverage: CONTINUITY_ENGINE_COVERAGE,
    title: "Threat Case Relevance",
    summary: "Checks whether a protocol or threat type matches active cases in Sagitta's incident library.",
    whenToUse: "The customer asks about known incidents or threats affecting a specific protocol or threat type. It is not a general safety check or a risk rating.",
    outputType: "case_relevance",
    duration: "Usually under a minute",
    limitations: null,
    parameters: {
      type: "object",
      properties: {
        protocol_name: { type: "string" },
        chain_id: { type: "integer" },
        asset_types: { type: "array", items: { type: "string" } },
        threat_families: { type: "array", items: { type: "string" } },
        doctrine_tags: { type: "array", items: { type: "string" } },
        requested_action: { type: "string" },
      },
      additionalProperties: false,
    },
    normalize(raw) {
      const parsed = caseRelevanceSchema.safeParse(raw ?? {});
      if (!parsed.success) return invalid(parsed.error);
      if (!parsed.data.protocol_name && !parsed.data.threat_families?.length && !parsed.data.doctrine_tags?.length) {
        return { ok: false, code: "invalid_input", message: "A protocol name, threat family, or doctrine tag is required." };
      }
      return { ok: true, body: parsed.data, details: describeSce(parsed.data), assumptions: [] };
    },
  },
];

const COMPREHENSIVE_COVERAGE: DataCoverageItem[] = SCORECARD_COVERAGE;

const REPORT_INPUT = {
  parameters: {
    type: "object",
    properties: {
      assets: {
        type: "array",
        items: { type: "string" },
        minItems: 1,
        maxItems: 12,
        description: "Asset names or symbols, for example [\"BTC\"]. Selun re-resolves each one on the server.",
      },
      ...profileProperties,
    },
    required: ["assets"],
    additionalProperties: false,
  },
  normalize(raw: unknown): NormalizedInput {
    const parsed = scorecardSchema.safeParse(raw);
    if (!parsed.success) return invalid(parsed.error);
    const resolved = resolveAssetList(parsed.data.assets);
    if ("failure" in resolved) return resolved.failure;
    const profile = resolveProfile(parsed.data);
    return {
      ok: true,
      body: { ...profile.body, assets: resolved.assets.map((asset) => asset.symbol) },
      details: [{ label: "Assets", value: resolved.assets.map(assetLabel).join(", ") }, ...profile.details],
      assumptions: profile.assumptions,
    };
  },
};

const REPORT_LIMITATIONS = "It describes market conditions and past price behavior when it runs, and it doesn't include live prices or current news. It isn't a recommendation to buy, sell, or hold anything.";
const REPORT_ASSURANCE = "Selun confirms every asset you name can get an overall score before checkout and locks that decision to your purchase. If the report still can't be delivered after retries, your payment is refunded.";

/** Sold through PayPal only, built from the same services the x402 routes run. */
const PAYPAL_SPECS: ToolSpec[] = [
  {
    id: "comprehensive_report",
    dataCoverage: COMPREHENSIVE_COVERAGE,
    title: "Comprehensive Intelligence Report",
    summary: "One report covering current market conditions, risk limits for your profile, and measured price risk and scores for the assets you choose.",
    whenToUse: "The customer wants a full or complete report, a comprehensive analysis, or to pay with PayPal or a card rather than a crypto wallet. It needs at least one named asset.",
    outputType: "comprehensive_report",
    duration: "Can take several minutes",
    limitations: `${REPORT_LIMITATIONS} ${REPORT_ASSURANCE}`,
    ...REPORT_INPUT,
  },
  {
    id: "monthly_intelligence",
    dataCoverage: COMPREHENSIVE_COVERAGE,
    title: "Monthly Intelligence Subscription",
    summary: "One Comprehensive Intelligence Report for the assets you choose, each paid month, emailed to your PayPal address and available here. Cancel anytime.",
    whenToUse: "The customer wants ongoing, recurring, or monthly reports or a subscription. It needs at least one named asset.",
    outputType: "comprehensive_report_subscription",
    duration: "The first report starts once PayPal confirms your first payment",
    limitations: `${REPORT_LIMITATIONS} ${REPORT_ASSURANCE} That applies to your first report. Each later month is checked when it runs. If an asset no longer qualifies, the report says so and we'll let you know so you can change your assets. If none qualifies, you can choose new ones, or that month is refunded. Each paid month includes one report. Reports already paid for stay available if you cancel.`,
    ...REPORT_INPUT,
  },
];

const HANDOFF_SPECS: Omit<Capability, "endpoint" | "priceUsdc" | "currency">[] = [
  {
    id: "allocate",
    title: "Personal Portfolio Plan",
    summary: "A full target portfolio built for your risk level, time horizon, and current market conditions.",
    whenToUse: "The customer wants a recommended portfolio or asset mix.",
    outputType: "portfolio_plan",
    duration: "Several minutes",
    limitations: "Completed in Selun's guided plan builder, which also supports card checkout.",
    execution: "wizard_handoff",
  },
  {
    id: "allocate_with_report",
    title: "Portfolio Plan with Certified Report",
    summary: "The full portfolio plan plus a downloadable certified decision report.",
    whenToUse: "The customer wants a documented record of the decision.",
    outputType: "portfolio_plan_with_report",
    duration: "Several minutes",
    limitations: "Completed in Selun's guided plan builder, which also supports card checkout.",
    execution: "wizard_handoff",
  },
];

export const TOOL_CAPABILITY_IDS = TOOL_SPECS.map((spec) => spec.id);

export function getToolSpec(id: string): ToolSpec | undefined {
  return TOOL_SPECS.find((spec) => spec.id === id) ?? PAYPAL_SPECS.find((spec) => spec.id === id);
}

/** PayPal products are only offered when PayPal is configured. */
export function listToolSpecs(includePayPal = false): readonly ToolSpec[] {
  return includePayPal ? [...TOOL_SPECS, ...PAYPAL_SPECS] : TOOL_SPECS;
}

/** Joins the static allowlist to the live catalog. Products missing from the catalog are dropped. */
export function buildRegistry(catalog: CapabilityCatalog, paypal?: PayPalPrices | null): Capability[] {
  const capabilities: Capability[] = [];
  const priced = (id: string) => catalog.resources.find((resource) => resource.productId === id);

  for (const spec of TOOL_SPECS) {
    const resource = priced(spec.id);
    if (!resource) continue;
    capabilities.push({
      id: spec.id,
      title: spec.title,
      summary: spec.summary,
      whenToUse: spec.whenToUse,
      outputType: spec.outputType,
      duration: spec.duration,
      limitations: spec.limitations,
      execution: "x402_tool",
      endpoint: resource.endpoint,
      priceUsdc: resource.pricing.amountUsdc,
      currency: "USDC",
    });
  }
  for (const spec of HANDOFF_SPECS) {
    const resource = priced(spec.id);
    if (!resource) continue;
    capabilities.push({ ...spec, endpoint: resource.endpoint, priceUsdc: resource.pricing.amountUsdc, currency: "USDC" });
  }
  if (paypal) {
    for (const spec of PAYPAL_SPECS) {
      const subscription = spec.id === "monthly_intelligence";
      capabilities.push({
        id: spec.id,
        title: spec.title,
        summary: spec.summary,
        whenToUse: spec.whenToUse,
        outputType: spec.outputType,
        duration: spec.duration,
        limitations: spec.limitations,
        execution: subscription ? "paypal_subscription" : "paypal_checkout",
        endpoint: "",
        priceUsdc: subscription ? paypal.subscriptionUsd : paypal.reportUsd,
        currency: "USD",
      });
    }
  }
  return capabilities;
}
