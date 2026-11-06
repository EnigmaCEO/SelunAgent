# Asset Scorecard methodology

Current version: `asset-scorecard-v2`, named in each result's `scoringMethodology.version`. The code is in `backend/src/services/asset-scorecard-v2.ts`.

## Why it changed

The first method scored assets with the Phase 5 portfolio model alone. That model's inputs top out for the largest assets, so BTC and ETH got identical scores. Its "volatility" and "drawdown" inputs were estimates from rank, liquidity, and structure, not price history.

## What v2 measures

Each asset is measured from up to 365 complete UTC daily closes.

| Field | Definition |
|---|---|
| `annualizedVolatilityPct` | Standard deviation of daily log returns x sqrt(365) |
| `maxDrawdownPct` | Largest peak-to-trough fall within the window |
| `currentDrawdownPct` | Latest close below the window's highest close |
| `priceRiskScore` | 0.6 x min(volatility / 150%, 1) + 0.4 x min(drawdown / 90%, 1) |
| `overallScore` | `qualityScore` x (1 - 0.72 x `priceRiskScore`) |

The scales are fixed, so a score doesn't depend on the other assets on the card. Assets with similar histories get similar scores. The 0.72 is the risk discount the portfolio model already uses. The 150% and 90% ceilings and the 60/40 weighting are judgment calls, not fitted values.

`qualityScore` is shown as "Market standing". It comes from volume, rank, and category, so it still tops out for the largest assets. Between those assets, the overall score differs only through price risk. Its liquidity input is measured against the risk profile's volume floor, so smaller assets score a little differently under different profiles.

A requested asset outside the segment's screened universe appears in `unscreenedAssets` with its measurements and price risk, but no overall score, because it has no market standing. The asset must be one Selun can identify by name.

## Data

CoinGecko is the primary source, by CoinGecko ID. Coinbase is the fallback, by symbol, for assets ranked 200 or better, because a symbol is only reliable for assets that size. Requests run one at a time and results are kept for the UTC day, so reruns that day agree. The cache is in memory and clears on restart.

| `priceHistory.status` | When | Scored? |
|---|---|---|
| `measured` | 360 or more closes | Yes |
| `partial` | 180 to 359 closes. The card warns that drawdown may be understated | Yes |
| `insufficient` | Under 180 closes, latest close over 2 days old, a gap over 5 days, or 5 or more extreme one-day moves (+50% or -33%) | No: `overallScore` and `priceRiskScore` are null |
| `unavailable` | No feed responded for this asset | No, same as above |

If no feed responds for any asset on the card, the run fails after settlement and the payer can retry without paying again. An estimate is never substituted for a missing measurement.

A close is dropped as a feed error only when it moves at least 50% from both neighboring days, in the same direction, and those neighbors agree within about 10%. The count is reported as `badPrintsExcluded`. Smaller errors stay in, and can move volatility by a few points.

The extreme-move check exists because CoinGecko's GALA series flips between two price levels for weeks in March 2026, which measured as 219% volatility. In a sample of 48 assets across the top 250, it fired on 4: GALA and three new, thinly traded tokens. It can't tell a faulty feed from a truly erratic token, so both go unscored.

## What didn't change

- **Portfolio plans.** Phase 5 scoring and everything built on it (Phase 6 allocation, Rebalance, the wizard) still use the model's estimates. The Phase 5 output isn't modified.
- **Existing API fields.** `compositeScore`, `riskScore`, `qualityScore`, `liquidityScore`, `structuralScore`, and `selectionReasons` keep their meaning and remain portfolio-model values. The new fields are additions.
- **Past results.** A stored result has no `scoringMethodology`, so it renders with its original labels, notes, and coverage. Frozen report records don't change.

## Limits

- It describes the past year, not the future, and one year may miss a full market cycle.
- A daily close misses moves within a day, so drawdown can be understated.
- Stablecoin scores show price stability only, not reserves or the issuer.
- The two sources were compared for BTC, ETH, SOL, DOGE, LINK, and AAVE over 365 days. Volatility differed by at most 2.6 points and largest drawdown by at most 0.1 points. Some thinner series on CoinGecko are noisier. WBTC measured 52.1% against BTC's 43.8%, though the two should track closely.
