import { scorecardPriceHistory } from "../services/asset-scorecard-v2";
import { PORTFOLIO_SEGMENTS } from "../services/portfolio-segments";
import { runScorecardScreen } from "../services/scorecard-screen";
import { ScorecardPreflight, type ScreenProfile } from "./scorecard-preflight";

/** Refreshed just before its two-hour cache runs out. */
export const SCREEN_WARM_MS = 110 * 60_000;
/** What a customer gets when they haven't named a risk, horizon, or segment. */
export const DEFAULT_SCREEN_PROFILE: ScreenProfile = { riskTolerance: "Balanced", timeframe: "1-3_years", portfolioSegment: "Bluechips" };

let preflight: ScorecardPreflight | null = null;

export function getScorecardPreflight(): ScorecardPreflight {
  return (preflight ??= new ScorecardPreflight({
    runScreen: runScorecardScreen,
    measure: (asset) => scorecardPriceHistory().measure(asset),
    segments: PORTFOLIO_SEGMENTS,
  }));
}
