import type { PriceTrend, ResultDocument } from "./types";

const TREND_POINTS = 60;

/** A thinned copy of an asset's charted closes for a grid row. Always keeps the first and last close. */
export function trendOf(document: ResultDocument | null | undefined, asset: string): PriceTrend | undefined {
  const closes = document?.charts?.find((chart) => chart.asset === asset)?.closes;
  if (!closes || closes.length < 2) return undefined;
  const step = Math.max(1, Math.ceil((closes.length - 1) / (TREND_POINTS - 1)));
  const picked = closes.filter((_, index) => index % step === 0);
  if (picked.at(-1) !== closes.at(-1)) picked.push(closes.at(-1)!);
  const first = closes[0][1];
  const last = closes[closes.length - 1][1];
  return { closes: picked.map(([, close]) => close), from: closes[0][0], to: closes[closes.length - 1][0], changePct: Math.round((last / first - 1) * 1000) / 10 };
}
