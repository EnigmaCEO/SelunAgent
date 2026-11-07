import type { ScreenCard, ScreenProfile, ScreenResult } from "../assistant/scorecard-preflight";
import { getExecutionStatus, runPhase1, runPhase3, runPhase4, runPhase5 } from "./phase1-execution.service";

const POLL_MS = 500;
const TIMEOUT_MS = 3 * 60_000;
type Phase = "phase2" | "phase3" | "phase4" | "phase5";

async function waitFor(jobId: string, phase: Phase): Promise<void> {
  const deadline = Date.now() + TIMEOUT_MS;
  while (Date.now() < deadline) {
    const context = getExecutionStatus(jobId).jobContext;
    const stage = context?.[phase];
    if (stage?.status === "complete") return;
    if (stage?.status === "failed") throw new Error(`${phase} failed.`);
    // Screens run one at a time, so one that can't finish mustn't hold up the rest until the timeout.
    if (context?.phase1.status === "failed") throw new Error("phase1 failed.");
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
  throw new Error(`${phase} timed out.`);
}

/** One screened token as a scorecard card. Used for a locked snapshot and a live run alike. */
export function toScreenCard(token: {
  symbol: string;
  name: string;
  coingecko_id: string;
  market_cap_rank: number | null;
  token_category: string;
  role: string;
  risk_class: string;
  selection_bucket: string;
  selected: boolean;
  quality_score: number;
  risk_score: number;
  liquidity_score: number;
  structural_score: number;
  composite_score: number;
  selection_reasons: string[];
}): ScreenCard {
  return {
    asset: token.symbol,
    name: token.name,
    coingeckoId: token.coingecko_id,
    marketCapRank: token.market_cap_rank,
    tokenCategory: token.token_category,
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
  };
}

/**
 * Runs the paid analysis's own screen, with nothing paid for. It uses market data only, no model, so its
 * cost is provider rate limit, which is why ScorecardPreflight caches it and runs one at a time.
 */
export async function runScorecardScreen(profile: ScreenProfile): Promise<ScreenResult> {
  const jobId = `selun-preflight-${profile.portfolioSegment}-${profile.riskTolerance}-${Date.now()}`.replace(/[^a-zA-Z0-9-_]/g, "-");
  runPhase1({
    jobId,
    executionTimestamp: new Date().toISOString(),
    riskMode: profile.riskTolerance.toLowerCase() as never,
    riskTolerance: profile.riskTolerance as never,
    investmentTimeframe: profile.timeframe,
    portfolioSegment: profile.portfolioSegment,
  });
  await waitFor(jobId, "phase2");
  runPhase3(jobId);
  await waitFor(jobId, "phase3");
  runPhase4(jobId);
  await waitFor(jobId, "phase4");
  runPhase5(jobId);
  await waitFor(jobId, "phase5");
  const output = getExecutionStatus(jobId).jobContext?.phase5.output;
  if (!output) throw new Error("The screen returned nothing.");
  return {
    screenedAt: output.timestamp,
    counts: {
      screened: output.evaluation.screened_candidates_count,
      qualified: output.evaluation.qualified_candidates_count,
      selected: output.evaluation.selected_candidates_count,
    },
    cards: output.evaluation.tokens.map(toScreenCard),
  };
}
