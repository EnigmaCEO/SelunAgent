import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function getBackendBaseUrl() {
  return process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";
}

function parseSignalValue(signals: unknown, prefix: string): string | null {
  if (!Array.isArray(signals)) return null;
  const entry = signals.find((item) => typeof item === "string" && item.startsWith(`${prefix}:`));
  if (typeof entry !== "string") return null;
  return entry.slice(prefix.length + 1) || null;
}

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function sanitizePhase1OutputShape(result: unknown): unknown {
  if (!result || typeof result !== "object") return result;
  const payload = result as Record<string, unknown>;
  const jobContext = payload.jobContext;
  if (!jobContext || typeof jobContext !== "object") return result;

  const context = jobContext as Record<string, unknown>;
  const phase1 = context.phase1;
  if (!phase1 || typeof phase1 !== "object") return result;

  const phase1Record = phase1 as Record<string, unknown>;
  const output = phase1Record.output;
  if (!output || typeof output !== "object") return result;

  const outputRecord = { ...(output as Record<string, unknown>) };
  delete outputRecord.assetReviews;

  if (!outputRecord.market_condition && outputRecord.marketRegime && typeof outputRecord.marketRegime === "object") {
    const marketRegime = outputRecord.marketRegime as Record<string, unknown>;
    const signals = Array.isArray(marketRegime.signals) ? marketRegime.signals : [];
    const riskAppetite = parseSignalValue(signals, "risk_appetite") || "neutral";
    const sentimentDirection = toNumber(parseSignalValue(signals, "sentiment_direction"), 0);

    outputRecord.market_condition = {
      volatility_state: parseSignalValue(signals, "volatility_state") || "moderate",
      liquidity_state: "stable",
      risk_appetite: riskAppetite,
      sentiment_direction: sentimentDirection,
      sentiment_alignment: toNumber(marketRegime.confidence, 0.4),
      public_echo_strength: 0,
      confidence: toNumber(marketRegime.confidence, 0.4),
      uncertainty: 0.6,
    };
  }

  if (!outputRecord.evidence || typeof outputRecord.evidence !== "object") {
    outputRecord.evidence = {
      volatility_metrics: {
        btc_volatility_24h: 0,
        eth_volatility_24h: 0,
        volatility_zscore: 0,
      },
      liquidity_metrics: {
        total_volume_24h: 0,
        volume_deviation_zscore: 0,
        avg_spread: 0,
        stablecoin_dominance: 0,
      },
      sentiment_metrics: {
        headline_count: 0,
        aggregate_sentiment_score: 0,
        engagement_deviation: 0,
        fear_greed_index: -1,
        fear_greed_available: false,
      },
    };
  } else {
    const evidence = outputRecord.evidence as Record<string, unknown>;
    const sentimentMetricsRaw = evidence.sentiment_metrics;
    const sentimentMetrics =
      sentimentMetricsRaw && typeof sentimentMetricsRaw === "object"
        ? (sentimentMetricsRaw as Record<string, unknown>)
        : {};

    evidence.sentiment_metrics = {
      headline_count: toNumber(sentimentMetrics.headline_count, 0),
      aggregate_sentiment_score: toNumber(sentimentMetrics.aggregate_sentiment_score, 0),
      engagement_deviation: toNumber(sentimentMetrics.engagement_deviation, 0),
      fear_greed_index:
        sentimentMetrics.fear_greed_index === undefined
          ? -1
          : toNumber(sentimentMetrics.fear_greed_index, -1),
      fear_greed_available: Boolean(sentimentMetrics.fear_greed_available),
    };
    outputRecord.evidence = evidence;
  }

  if (!outputRecord.allocation_authorization || typeof outputRecord.allocation_authorization !== "object") {
    const legacyAuth =
      outputRecord.allocationAuthorization && typeof outputRecord.allocationAuthorization === "object"
        ? (outputRecord.allocationAuthorization as Record<string, unknown>)
        : null;
    outputRecord.allocation_authorization = legacyAuth
      ? {
          status: String(legacyAuth.status || "DEFERRED"),
          confidence: toNumber(legacyAuth.confidence, 0.3),
          justification: Array.isArray(legacyAuth.justification)
            ? legacyAuth.justification.map((item) => String(item))
            : ["allocation_activity_deferred_pending_phase_3_asset_checks"],
        }
      : {
        status: "DEFERRED",
        confidence: 0.3,
        justification: ["allocation_activity_deferred_pending_phase_3_asset_checks"],
      };
  }

  outputRecord.phase_boundaries = {
    asset_evaluation: "PHASE_3",
    portfolio_construction: "PHASE_4",
  };

  if (!outputRecord.audit || typeof outputRecord.audit !== "object") {
    outputRecord.audit = {
      sources: [],
      data_freshness: String(outputRecord.timestamp || new Date().toISOString()),
      missing_domains: [],
      assumptions: [],
      source_credibility: [],
      source_selection: [],
    };
  } else {
    const audit = outputRecord.audit as Record<string, unknown>;
    const legacyToolCalls = Array.isArray(audit.toolCalls) ? audit.toolCalls : [];

    outputRecord.audit = {
      sources: Array.isArray(audit.sources)
        ? audit.sources
        : legacyToolCalls.map((toolCall) => {
            const id = String(toolCall);
            const separatorIndex = id.indexOf(":");
            const provider = separatorIndex > 0 ? id.slice(0, separatorIndex) : "unknown";
            const endpoint = separatorIndex > 0 ? id.slice(separatorIndex + 1) : id;
            return {
              id,
              provider,
              endpoint,
              url: "",
              fetched_at: String(audit.dataFreshness || outputRecord.timestamp || new Date().toISOString()),
            };
          }),
      data_freshness: String(audit.data_freshness || audit.dataFreshness || outputRecord.timestamp || new Date().toISOString()),
      missing_domains: Array.isArray(audit.missing_domains)
        ? audit.missing_domains.map((item) => String(item))
        : Array.isArray(audit.limitations)
          ? audit.limitations.map((item) => String(item))
          : [],
      assumptions: Array.isArray(audit.assumptions) ? audit.assumptions.map((item) => String(item)) : [],
      source_credibility: Array.isArray(audit.source_credibility)
        ? audit.source_credibility.map((entry) => {
            const record = entry as Record<string, unknown>;
            return {
              domain: String(record.domain || "sentiment"),
              provider: String(record.provider || "unknown"),
              score: toNumber(record.score, 0.5),
              successes: Math.max(0, Math.floor(toNumber(record.successes, 0))),
              failures: Math.max(0, Math.floor(toNumber(record.failures, 0))),
              last_success_at: record.last_success_at ? String(record.last_success_at) : null,
              last_failure_at: record.last_failure_at ? String(record.last_failure_at) : null,
              avg_latency_ms: Math.max(0, toNumber(record.avg_latency_ms, 0)),
            };
          })
        : [],
      source_selection: Array.isArray(audit.source_selection)
        ? audit.source_selection
        : [],
    };
  }

  if (!outputRecord.execution_model_version) {
    outputRecord.execution_model_version = "Selun-1.0.0";
  }
  if (!outputRecord.doctrine_version) {
    outputRecord.doctrine_version = "SELUN-SIGNAL-1.0";
  }
  if (!outputRecord.timestamp) {
    outputRecord.timestamp =
      (outputRecord.audit as Record<string, unknown> | undefined)?.data_freshness ||
      new Date().toISOString();
  }

  phase1Record.output = outputRecord;
  context.phase1 = phase1Record;

  const phase2 = context.phase2;
  if (phase2 && typeof phase2 === "object") {
    const phase2Record = phase2 as Record<string, unknown>;
    const phase2Output = phase2Record.output;
    if (phase2Output && typeof phase2Output === "object") {
      const phase2OutputRecord = { ...(phase2Output as Record<string, unknown>) };
      const policyEnvelopeRaw = phase2OutputRecord.policy_envelope;
      if (policyEnvelopeRaw && typeof policyEnvelopeRaw === "object") {
        const policyEnvelope = { ...(policyEnvelopeRaw as Record<string, unknown>) };
        const exposureCapsRaw = policyEnvelope.exposure_caps;
        const exposureCaps =
          exposureCapsRaw && typeof exposureCapsRaw === "object"
            ? { ...(exposureCapsRaw as Record<string, unknown>) }
            : {};

        if (exposureCaps.max_single_asset_exposure === undefined) {
          exposureCaps.max_single_asset_exposure = toNumber(policyEnvelope.max_single_asset_exposure, 0);
        }
        if (exposureCaps.high_volatility_asset_cap === undefined) {
          exposureCaps.high_volatility_asset_cap = toNumber(policyEnvelope.high_volatility_asset_cap, 0);
        }

        policyEnvelope.exposure_caps = {
          max_single_asset_exposure: toNumber(exposureCaps.max_single_asset_exposure, 0),
          high_volatility_asset_cap: toNumber(exposureCaps.high_volatility_asset_cap, 0),
        };

        delete policyEnvelope.max_single_asset_exposure;
        delete policyEnvelope.high_volatility_asset_cap;
        phase2OutputRecord.policy_envelope = policyEnvelope;
      }
      phase2Record.output = phase2OutputRecord;
    }
    context.phase2 = phase2Record;
  }

  payload.jobContext = context;
  return payload;
}

export async function GET(
  _req: Request,
  context: { params: Promise<{ jobId: string }> },
) {
  const { jobId } = await context.params;

  if (!jobId?.trim()) {
    return NextResponse.json(
      {
        success: false,
        error: "jobId is required.",
      },
      { status: 400 },
    );
  }

  try {
    const response = await fetch(`${getBackendBaseUrl()}/execution-status/${encodeURIComponent(jobId.trim())}`, {
      method: "GET",
      cache: "no-store",
    });

    const result = (await response.json()) as unknown;
    return NextResponse.json(sanitizePhase1OutputShape(result), { status: response.status });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Failed to fetch execution status.",
      },
      { status: 502 },
    );
  }
}
