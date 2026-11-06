/**
 * Turns real x402 route responses into a channel-neutral ResultDocument.
 *
 * This is deterministic code with no model involved, so every number the
 * customer sees comes from the service. The same structure is what the chat
 * renders today and what a PDF or email renderer should consume later.
 */

import { SCORECARD_METHODOLOGY_VERSION } from "../services/asset-scorecard-v2";
import { getToolSpec, type DataCoverageItem } from "./capability-registry";
import type { PayPalPaymentEvidence } from "./commerce";

export type Tone = "positive" | "neutral" | "caution" | "negative";

export const ANALYSIS_NOTICE = "This is Selun's analysis of the data described here. It is not a recommendation to buy, sell, or hold anything.";

export type ResultSection =
  | { kind: "highlight"; label: string; value: string; tone: Tone; detail?: string }
  | { kind: "metrics"; title?: string; items: { label: string; value: string; tone?: Tone; meter?: number }[] }
  | { kind: "table"; title?: string; columns: { key: string; label: string; numeric?: boolean }[]; rows: Record<string, string>[] }
  | { kind: "list"; title?: string; items: string[] }
  | { kind: "note"; text: string }
  | { kind: "heading"; text: string };

export type ResultDocument = {
  schemaVersion: "selun.result.v1";
  capabilityId: string;
  title: string;
  snapshotAt: string | null;
  sections: ResultSection[];
  coverage: DataCoverageItem[];
  notice: string;
  provenance: {
    source: string;
    endpoint: string;
    decisionId: string;
    /** x402 only. A PayPal purchase has none of these four, and they're never filled in with substitutes. */
    amountUsdc: string | null;
    transactionHash: string | null;
    network: string | null;
    explorerUrl: string | null;
    payer: string | null;
    /** PayPal purchases only. Absent on x402 results, so those records hash as they always did. */
    paypal?: PayPalPaymentEvidence;
  };
};

type Json = Record<string, unknown>;

function record(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Json) : {};
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function number(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function humanize(value: unknown): string {
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") return String(value);
  const raw = text(value);
  if (!raw) return "Not reported";
  const spaced = raw.replaceAll("_", " ").replaceAll("-", " ").trim().toLowerCase();
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

function percent(value: unknown): string {
  const parsed = number(value);
  return parsed === null ? "Not reported" : `${Math.round(parsed * 1000) / 10}%`;
}

function score(label: string, value: unknown): { label: string; value: string; meter?: number } {
  const parsed = number(value);
  if (parsed === null) return { label, value: "Not reported" };
  return { label, value: `${Math.round(parsed * 100)} / 100`, meter: Math.max(0, Math.min(1, parsed)) };
}

function toneFor(value: unknown): Tone {
  const normalized = String(value ?? "").toLowerCase();
  if (/(block|prohibit|incident|critical|high|pause|defensive|negative|bearish|fear|elevated|stress)/.test(normalized)) {
    return normalized.includes("defensive") || normalized.includes("elevated") ? "caution" : "negative";
  }
  if (/(restrict|deferred|degraded|escalate|medium|review|warning|caution|unknown)/.test(normalized)) return "caution";
  if (/(allow|authorized|normal|low|risk.on|expansion|positive|stable|healthy)/.test(normalized)) return "positive";
  return "neutral";
}

const REGIME_COPY: Record<string, { label: string; detail: string }> = {
  defensive: { label: "Defensive", detail: "Conditions favor protecting capital over adding risk." },
  balanced: { label: "Balanced", detail: "Conditions are mixed, with no strong lean toward risk or caution." },
  risk_on: { label: "Risk-on", detail: "Conditions are supportive of taking on more risk." },
};

const VOLATILITY_TONE: Record<string, Tone> = { low: "positive", moderate: "neutral", elevated: "caution", extreme: "negative" };
const LIQUIDITY_TONE: Record<string, Tone> = { weak: "negative", stable: "neutral", strong: "positive" };
const APPETITE_TONE: Record<string, Tone> = { defensive: "caution", neutral: "neutral", expansionary: "positive" };

function conditionTone(tones: Record<string, Tone>, value: unknown): Tone {
  return tones[String(value)] ?? "neutral";
}

function sentimentItem(value: unknown): { label: string; value: string; tone: Tone } {
  const parsed = number(value);
  if (parsed === null) return { label: "Sentiment", value: "Not reported", tone: "neutral" };
  if (parsed > 0.5) return { label: "Sentiment", value: "Positive", tone: "positive" };
  if (parsed > 0.15) return { label: "Sentiment", value: "Slightly positive", tone: "positive" };
  if (parsed < -0.5) return { label: "Sentiment", value: "Negative", tone: "negative" };
  if (parsed < -0.15) return { label: "Sentiment", value: "Slightly negative", tone: "caution" };
  return { label: "Sentiment", value: "Mixed", tone: "neutral" };
}

/** The pipeline's justification entries are machine codes. Only these outcomes reach customers. */
function allocationGuidance(justification: unknown): string | null {
  const codes = Array.isArray(justification) ? justification : [];
  if (codes.includes("allocation_activity_blocked_by_macro_stress")) return "Selun's rules block new allocations while the wider economy shows this much stress.";
  if (codes.includes("allocation_activity_permitted_under_current_regime")) return "Selun's rules permit new allocations under current conditions.";
  if (codes.includes("allocation_activity_deferred_pending_phase_3_asset_checks")) return "Selun's rules call for asset-level checks before any new allocation. This is not a call to buy or sell.";
  return null;
}

function presentMarketRegime(result: Json): ResultSection[] {
  const condition = record(result.marketCondition);
  const authorization = record(result.authorization);
  const summary = record(result.sourceSummary);
  const regime = text(result.regime) ?? "unavailable";
  const copy = REGIME_COPY[regime] ?? { label: humanize(regime), detail: "" };

  const conditionItems = [
    { label: "Volatility", value: humanize(condition.volatility_state), tone: conditionTone(VOLATILITY_TONE, condition.volatility_state) },
    { label: "Liquidity", value: humanize(condition.liquidity_state), tone: conditionTone(LIQUIDITY_TONE, condition.liquidity_state) },
    { label: "Risk appetite", value: humanize(condition.risk_appetite), tone: conditionTone(APPETITE_TONE, condition.risk_appetite) },
    sentimentItem(condition.sentiment_direction),
  ];

  const sections: ResultSection[] = [
    { kind: "highlight", label: "Market regime", value: copy.label, tone: toneFor(regime === "risk_on" ? "positive" : regime), detail: copy.detail },
    {
      kind: "metrics",
      title: "Current conditions",
      items: [...conditionItems, { label: "Confidence", value: percent(condition.confidence), meter: number(condition.confidence) ?? undefined }],
    },
    {
      kind: "metrics",
      title: "New allocation guidance",
      items: [
        { label: "Status", value: humanize(authorization.status), tone: toneFor(authorization.status) },
        { label: "Policy mode", value: humanize(result.policyMode) },
      ],
    },
  ];
  const guidance = allocationGuidance(authorization.justification);
  if (guidance) sections.push({ kind: "note", text: guidance });
  const sourceCount = number(summary.sourceCount);
  const missingFeeds = Array.isArray(summary.missingDomains)
    ? summary.missingDomains.filter((entry) => typeof entry === "string" && entry.startsWith("missing_")).length
    : 0;
  const dataNote = [
    sourceCount !== null ? `Based on ${sourceCount} data sources.` : null,
    missingFeeds > 0 ? `${missingFeeds} data ${missingFeeds === 1 ? "feed was" : "feeds were"} unavailable for this reading.` : null,
  ].filter(Boolean).join(" ");
  if (dataNote) sections.push({ kind: "note", text: dataNote });
  return sections;
}

function pct(value: unknown): string {
  const parsed = number(value);
  return parsed === null ? "Not measured" : `${parsed}%`;
}

const PRICE_SOURCE: Record<string, string> = { coingecko: "CoinGecko", coinbase: "Coinbase" };

function isV2Scorecard(result: Json): boolean {
  return record(result.scoringMethodology).version === SCORECARD_METHODOLOGY_VERSION;
}

function windowText(history: Json): string {
  return `${number(history.closes)} daily closes, ${text(history.windowStart)} to ${text(history.windowEnd)}, from ${PRICE_SOURCE[String(history.source)] ?? "a market feed"}`;
}

function presentScorecardV2(result: Json): ResultSection[] {
  const cards = Array.isArray(result.scorecards) ? result.scorecards.map(record) : [];
  const requested = Array.isArray(result.assetsRequested) ? result.assetsRequested.filter((entry): entry is string => typeof entry === "string") : [];
  const unscreened = Array.isArray(result.unscreenedAssets) ? result.unscreenedAssets.map(record) : [];
  const scored = new Set([...cards, ...unscreened].flatMap((card) => [String(card.asset ?? "").toUpperCase(), String(card.coingeckoId ?? "").toUpperCase()]));
  const unscored = requested.filter((asset) => !scored.has(asset.toUpperCase()));
  const segment = text(result.portfolioSegment) ?? "selected";
  const sections: ResultSection[] = [];
  const windows = new Set<string>();
  const noOverall: string[] = [];
  const partial: string[] = [];

  for (const card of cards) {
    const asset = text(card.asset) ?? "?";
    const history = record(card.priceHistory);
    const measured = history.status === "measured" || history.status === "partial";
    if (measured) {
      windows.add(windowText(history));
    } else {
      noOverall.push(`${asset} has no overall score. ${text(history.reason) ?? "Its price history was unavailable."}`);
    }
    if (history.status === "partial") partial.push(asset);
    sections.push({
      kind: "metrics",
      title: `${text(card.name) ?? asset} (${asset})`,
      items: [
        measured ? score("Overall score", card.overallScore) : { label: "Overall score", value: "Not scored" },
        measured ? score("Price risk (higher is riskier)", card.priceRiskScore) : { label: "Price risk", value: "Not measured" },
        { label: "Annualized volatility", value: pct(history.annualizedVolatilityPct) },
        { label: "Largest drawdown", value: pct(history.maxDrawdownPct) },
        { label: "Now below period high", value: pct(history.currentDrawdownPct) },
        score("Market standing", card.qualityScore),
        score("Liquidity", card.liquidityScore),
        score("Market structure", card.structuralScore),
        { label: "Role in Selun's portfolio model", value: humanize(card.role) },
        { label: "Market category", value: humanize(card.riskClass) },
        { label: "In Selun's model selection", value: humanize(card.selected) },
      ],
    });
  }
  for (const entry of unscreened) {
    const asset = text(entry.asset) ?? "?";
    const history = record(entry.priceHistory);
    const measured = history.status === "measured" || history.status === "partial";
    if (measured) windows.add(windowText(history));
    if (history.status === "partial") partial.push(asset);
    sections.push({
      kind: "metrics",
      title: `${text(entry.name) ?? asset} (${asset})`,
      items: [
        { label: "Overall score", value: "Not scored" },
        measured ? score("Price risk (higher is riskier)", entry.priceRiskScore) : { label: "Price risk", value: "Not measured" },
        { label: "Annualized volatility", value: pct(history.annualizedVolatilityPct) },
        { label: "Largest drawdown", value: pct(history.maxDrawdownPct) },
        { label: "Now below period high", value: pct(history.currentDrawdownPct) },
      ],
    });
  }
  if (unscreened.length > 0) {
    const names = unscreened.map((entry) => text(entry.asset) ?? "?").join(", ");
    sections.push({ kind: "note", text: `${names} ${unscreened.length === 1 ? "is" : "are"} outside the universe screened for the ${segment} segment, so there is no market standing or overall score. A different segment may include ${unscreened.length === 1 ? "it" : "them"}.` });
  }
  if (windows.size > 0) sections.push({ kind: "note", text: `Measured from ${[...windows].join("; ")}.` });
  for (const line of noOverall) sections.push({ kind: "note", text: line });
  if (partial.length > 0) {
    sections.push({ kind: "note", text: `${partial.join(", ")} ${partial.length === 1 ? "has" : "have"} under a year of price history, so the largest drawdown may be understated.` });
  }
  if (unscored.length > 0) {
    sections.push({
      kind: "note",
      text: `${unscored.join(", ")} could not be identified, so the service returned nothing for ${unscored.length === 1 ? "it" : "them"}.`,
    });
  }
  sections.push({ kind: "note", text: "The overall score is market standing reduced by measured price risk. Price risk uses fixed scales, so it doesn't depend on which other assets are listed. These figures describe the past and don't predict future prices." });
  sections.push({ kind: "note", text: "Market standing and liquidity come from trading volume, market size, and category, and they top out for the largest assets. Between those assets, overall scores differ only through price risk." });
  if (cards.some((card) => card.tokenCategory === "stablecoin")) {
    sections.push({ kind: "note", text: "A stablecoin scores high because its price barely moved during this period. That doesn't assess its reserves or issuer, and it isn't a comparison of potential returns." });
  }
  const qualified = number(result.qualifiedCandidatesCount);
  const screened = number(result.screenedCandidatesCount);
  if (qualified !== null && screened !== null) {
    sections.push({ kind: "note", text: `${qualified} of ${screened} screened assets qualified in this run.` });
  }
  sections.push({ kind: "note", text: `Scoring method: ${SCORECARD_METHODOLOGY_VERSION}.` });
  return sections;
}

/** Results paid for under the first method keep the labels and notes they were sold with. */
function presentScorecard(result: Json): ResultSection[] {
  if (isV2Scorecard(result)) return presentScorecardV2(result);
  const cards = Array.isArray(result.scorecards) ? result.scorecards.map(record) : [];
  const requested = Array.isArray(result.assetsRequested) ? result.assetsRequested.filter((entry): entry is string => typeof entry === "string") : [];
  const scored = new Set(cards.map((card) => String(card.asset ?? "").toUpperCase()));
  const unscored = requested.filter((asset) => !scored.has(asset.toUpperCase()));
  const sections: ResultSection[] = [];

  for (const card of cards) {
    sections.push({
      kind: "metrics",
      title: `${text(card.name) ?? text(card.asset) ?? "Asset"} (${text(card.asset) ?? "?"})`,
      items: [
        score("Overall score", card.compositeScore),
        score("Quality", card.qualityScore),
        score("Risk (higher is riskier)", card.riskScore),
        score("Liquidity", card.liquidityScore),
        score("Structural stability", card.structuralScore),
        { label: "Portfolio role", value: humanize(card.role) },
        { label: "Risk class", value: humanize(card.riskClass) },
        { label: "In Selun's model selection", value: humanize(card.selected) },
      ],
    });
    if (Array.isArray(card.selectionReasons) && card.selectionReasons.length > 0) {
      sections.push({ kind: "list", title: "Scoring notes", items: card.selectionReasons.filter((entry): entry is string => typeof entry === "string").slice(0, 5).map(humanize) });
    }
  }
  if (unscored.length > 0) {
    sections.push({
      kind: "note",
      text: `${unscored.join(", ")} did not appear in the universe screened for the ${text(result.portfolioSegment) ?? "selected"} segment, so the service returned no score for ${unscored.length === 1 ? "it" : "them"}.`,
    });
  }
  sections.push({ kind: "note", text: "These scores mainly reflect liquidity, market size, and structure rather than price history, so the largest assets can score the same." });
  const qualified = number(result.qualifiedCandidatesCount);
  const screened = number(result.screenedCandidatesCount);
  if (qualified !== null && screened !== null) {
    sections.push({ kind: "note", text: `${qualified} of ${screened} screened assets qualified in this run.` });
  }
  return sections;
}

function envelopeItems(envelope: Json) {
  const caps = record(envelope.exposure_caps);
  return [
    { label: "Risk budget", value: percent(envelope.risk_budget) },
    { label: "Minimum in stablecoins", value: percent(envelope.stablecoin_minimum) },
    { label: "Largest single asset", value: percent(caps.max_single_asset_exposure) },
    { label: "High-volatility asset cap", value: percent(caps.high_volatility_asset_cap) },
    { label: "Portfolio volatility target", value: percent(envelope.portfolio_volatility_target) },
    { label: "Liquidity requirement", value: humanize(envelope.liquidity_floor_requirement) },
  ];
}

const POLICY_REASON_COPY: Record<string, string> = {
  PHASE1_PROHIBITION_DOWNGRADED_TO_RESTRICTED: "The market reading prohibits new allocations. For this profile that's treated as restricted, so these tighter limits apply.",
};

/** Internal rule codes aren't shown to customers. Free text from the service still is. */
function policyReason(value: unknown): string | undefined {
  const reason = text(value);
  if (!reason) return undefined;
  return POLICY_REASON_COPY[reason] ?? (/^[A-Z0-9_]+$/.test(reason) ? undefined : reason);
}

function presentPolicyEnvelope(result: Json): ResultSection[] {
  const authorization = record(result.authorization);
  const sections: ResultSection[] = [
    { kind: "highlight", label: "Policy mode", value: humanize(result.policyMode), tone: toneFor(authorization.status), detail: policyReason(authorization.reason) },
    { kind: "metrics", title: "Limits for this profile", items: envelopeItems(record(result.policyEnvelope)) },
  ];
  return sections;
}

function presentRebalance(result: Json): ResultSection[] {
  const rows = Array.isArray(result.recommendations) ? result.recommendations.map(record) : [];
  const target = record(result.targetPortfolio);
  const portfolio = number(result.currentPortfolioUsd);
  return [
    {
      kind: "metrics",
      title: "Overview",
      items: [
        { label: "Portfolio value", value: portfolio === null ? "Not reported" : `$${portfolio.toLocaleString("en-US")}` },
        { label: "Policy mode", value: humanize(result.policyMode) },
        { label: "Target in stablecoins", value: percent(target.stablecoinAllocation) },
      ],
    },
    {
      kind: "table",
      title: "Suggested changes",
      columns: [
        { key: "asset", label: "Asset" },
        { key: "current", label: "Current", numeric: true },
        { key: "target", label: "Target", numeric: true },
        { key: "action", label: "Action" },
      ],
      rows: rows.map((row) => ({
        asset: text(row.asset) ?? "?",
        current: `${number(row.currentAllocationPct) ?? 0}%`,
        target: `${number(row.targetAllocationPct) ?? 0}%`,
        action: humanize(row.action),
      })),
    },
  ];
}

function domainItems(domains: Json) {
  return Object.entries(domains).map(([key, value]) => ({ label: humanize(key), value: humanize(value), tone: toneFor(value) }));
}

function confidenceItem(sce: Json) {
  const confidence = record(sce.confidence);
  return { label: "Confidence", value: percent(confidence.score), meter: number(confidence.score) ?? undefined };
}

function reasonList(sce: Json): ResultSection[] {
  const reasons = Array.isArray(sce.reason_codes) ? sce.reason_codes.filter((entry): entry is string => typeof entry === "string") : [];
  return reasons.length > 0 ? [{ kind: "list", title: "Reasons", items: reasons.slice(0, 8).map(humanize) }] : [];
}

function validityNote(sce: Json): ResultSection[] {
  const until = text(sce.valid_until);
  return until ? [{ kind: "note", text: `This result is time-limited. Check again after ${until}.` }] : [];
}

function presentContinuity(result: Json): ResultSection[] {
  const sce = record(result.sce);
  return [
    { kind: "highlight", label: "Continuity mode", value: humanize(sce.mode), tone: toneFor(sce.mode), detail: `Engine posture: ${humanize(sce.recommended_posture)}` },
    { kind: "metrics", title: "Risk domains", items: [...domainItems(record(sce.risk_domains)), confidenceItem(sce)] },
    ...reasonList(sce),
    ...validityNote(sce),
  ];
}

function presentRiskEvaluate(result: Json): ResultSection[] {
  const sce = record(result.sce);
  return [
    { kind: "highlight", label: "Decision", value: humanize(sce.doctrine_action), tone: toneFor(sce.doctrine_action), detail: `Engine posture: ${humanize(sce.recommended_posture)}` },
    {
      kind: "metrics",
      title: "Risk",
      items: [
        { label: "Risk level", value: humanize(sce.risk_level), tone: toneFor(sce.risk_level) },
        { label: "Risk score", value: number(sce.risk_score) === null ? "Not reported" : `${sce.risk_score} / 100` },
        confidenceItem(sce),
        ...domainItems(record(sce.risk_domains)),
      ],
    },
    ...reasonList(sce),
    ...validityNote(sce),
  ];
}

function presentCaseRelevance(result: Json): ResultSection[] {
  const sce = record(result.sce);
  const families = Array.isArray(sce.matched_threat_families) ? sce.matched_threat_families.filter((entry): entry is string => typeof entry === "string") : [];
  return [
    { kind: "highlight", label: "Relevance", value: humanize(sce.relevance_level), tone: toneFor(sce.relevance_level), detail: number(sce.relevance_score) === null ? undefined : `Score: ${sce.relevance_score} / 100` },
    ...(families.length > 0 ? [{ kind: "list" as const, title: "Matched threat families", items: families }] : []),
    ...reasonList(sce),
    ...validityNote(sce),
  ];
}

function presentComprehensive(result: Json): ResultSection[] {
  return [
    { kind: "heading", text: "Market conditions" },
    ...presentMarketRegime(record(result.marketRegime)),
    { kind: "heading", text: "Risk limits for this profile" },
    ...presentPolicyEnvelope(record(result.policyEnvelope)),
    { kind: "heading", text: "Asset scores" },
    ...presentScorecard(record(result.assetScorecard)),
  ];
}

const PRESENTERS: Record<string, (result: Json) => ResultSection[]> = {
  comprehensive_report: presentComprehensive,
  market_regime: presentMarketRegime,
  asset_scorecard: presentScorecard,
  policy_envelope: presentPolicyEnvelope,
  rebalance: presentRebalance,
  sce_continuity_mode: presentContinuity,
  sce_risk_evaluate: presentRiskEvaluate,
  sce_case_relevance: presentCaseRelevance,
};

function explorerUrl(network: string | null, transactionHash: string): string | null {
  if (!/^0x[0-9a-fA-F]{64}$/.test(transactionHash)) return null;
  if (network === "eip155:8453" || network === "base" || network === "base-mainnet") return `https://basescan.org/tx/${transactionHash}`;
  if (network === "eip155:84532" || network === "base-sepolia") return `https://sepolia.basescan.org/tx/${transactionHash}`;
  return null;
}

/** Requires PayPal's own capture or sale reference. An amount alone doesn't prove a payment. */
function payPalEvidence(payment: Json): PayPalPaymentEvidence | null {
  if (payment.provider !== "paypal") return null;
  const captureId = text(payment.captureId);
  const saleId = text(payment.saleId);
  const amount = text(payment.amount);
  if ((!captureId && !saleId) || !amount || payment.currency !== "USD") return null;
  return {
    provider: "paypal",
    amount,
    currency: "USD",
    orderId: text(payment.orderId),
    captureId,
    saleId,
    subscriptionId: text(payment.subscriptionId),
    payerId: text(payment.payerId),
    environment: payment.environment === "live" ? "live" : "sandbox",
  };
}

function coverageFor(capabilityId: string, result: Json): DataCoverageItem[] {
  const coverage = getToolSpec(capabilityId)?.dataCoverage ?? [];
  // A result from before the price-history method didn't use any.
  if (capabilityId === "asset_scorecard" && !isV2Scorecard(result)) return coverage.filter((item) => item.label !== "Price history");
  return coverage;
}

/**
 * Builds the customer document from a completed route response. Returns null
 * unless the response carries a verified payment receipt: that receipt, not any
 * statement from the model, is what makes a result "completed".
 */
export function presentResult(capabilityId: string, title: string, data: Json): ResultDocument | null {
  const payment = record(data.payment);
  const transactionHash = text(payment.transactionHash);
  const paypal = payPalEvidence(payment);
  const result = data.result;
  if (data.status !== "completed" || payment.verified !== true || (!transactionHash && !paypal) || !result || typeof result !== "object") {
    return null;
  }
  const presenter = PRESENTERS[capabilityId];
  if (!presenter) return null;
  const body = record(result);
  if (paypal) {
    return {
      schemaVersion: "selun.result.v1",
      capabilityId,
      title,
      snapshotAt: text(body.snapshotAt),
      sections: presenter(body),
      coverage: coverageFor(capabilityId, body),
      notice: ANALYSIS_NOTICE,
      provenance: {
        source: "Selun by Sagitta",
        endpoint: text(data.endpoint) ?? "",
        decisionId: text(data.decisionId) ?? "",
        amountUsdc: null,
        transactionHash: null,
        network: null,
        explorerUrl: null,
        payer: null,
        paypal,
      },
    };
  }
  const network = text(payment.network);
  return {
    schemaVersion: "selun.result.v1",
    capabilityId,
    title,
    snapshotAt: text(body.snapshotAt) ?? text(body.validUntil),
    sections: presenter(body),
    coverage: coverageFor(capabilityId, body),
    notice: ANALYSIS_NOTICE,
    provenance: {
      source: "Selun by Sagitta",
      endpoint: text(data.endpoint) ?? "",
      decisionId: text(data.decisionId) ?? "",
      amountUsdc: text(payment.chargedAmountUsdc) ?? "",
      transactionHash,
      network,
      explorerUrl: explorerUrl(network, transactionHash!),
      payer: text(payment.fromAddress),
    },
  };
}
