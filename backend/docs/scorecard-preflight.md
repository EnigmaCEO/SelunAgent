# Scorecard eligibility and the purchase snapshot

Selun only sells an Asset Scorecard, a $4.99 Comprehensive Report, or a monthly subscription once it has confirmed every named asset can get its overall score. That decision is locked to the purchase, so a paid run can't reach a different answer from the one that was quoted.

## The transaction

1. **Free check.** Selun confirms each asset is recognized, is in the screen for the customer's risk, horizon, and segment, and has at least 180 days of reliable price history.
2. **Quote and snapshot.** A passing check produces a snapshot holding the screened scores for those assets, the eligible universe, the exact assets, the profile, horizon, segment, and scoring method version. Its ID is a hash of that content. It's valid for payment for 2 hours.
3. **Payment approval.** The snapshot must still match the purchase and be within its 2 hours. If it has expired, Selun checks again and binds a new one. If the assets no longer qualify, payment stops with nothing charged.
4. **Paid run.** The run scores from the snapshot and doesn't screen again. It measures price history when it runs, under that data's own rules, so the screening decision is frozen but the prices aren't old.
5. **Delivery.** The stored result records the snapshot ID and screen time, and the report states both.

## What is checked

| Check | How | If it fails |
| --- | --- | --- |
| Asset is recognized | The screen, then Selun's asset list | No purchase. Chat asks what they meant. The API returns 409 |
| In the segment's screen | The paid analysis's own screen | No purchase. Selun explains and names a segment that can score it, if there is one |
| At least 180 days of reliable price history | The paid analysis's own price feed and data-quality rules | No purchase, with the reason |
| Anything that can't be confirmed | Screen failed or is slow, or the feed is down | No purchase. Selun asks them to try again |

Every named asset must pass. A report naming several assets is refused as a whole, with the reason for each and an offer to adjust the request. Selun never swaps an asset.

The screen uses market data only and no AI model. It takes about 15 to 30 seconds. To stay inside provider rate limits, each profile's result is cached for 2 hours, only one screen runs at a time, and the default profile is refreshed in the background every 110 minutes. Price history shares the paid run's cache. `SELUN_SCREEN_WARM=0` turns off the background refresh.

## Where each purchase holds its snapshot

| Purchase | Bound | Checked last | Stored on |
| --- | --- | --- | --- |
| Wallet, from chat or the direct API | When the 402 payment request is issued, or at payment if there's no earlier request | Before settlement | The x402 tool record |
| $4.99 report through PayPal | When the customer approves on the card | Before PayPal capture | The PayPal purchase |
| Monthly subscription, first report | When the customer approves on the card | At that approval | The subscription, then the first month's purchase |

Once payment is confirmed, the snapshot stays good at any age for every retry and restart. It's only tested for integrity and its match to the purchase.

A direct API call that names assets gets its answer before paying: 402 if it qualifies, or 409 with `details.code: "scorecard_not_offered"`, `details.charged: false`, and a verdict per asset. A discovery probe naming no assets gets 402 without a screen. Paying with no assets named locks the screen's top assets that can score.

## After payment

| What happens | Wallet | PayPal |
| --- | --- | --- |
| A price feed fails, or an asset's price data no longer supports a score | The run fails and isn't presented as finished. The customer retries with the same request ID without paying again, on the same snapshot | Retried automatically, up to 3 runs, on the same snapshot |
| It still can't be delivered | The admin team is emailed about each failed run. Staff decide on a refund | Refunded in full automatically. If that refund fails, staff are alerted and the customer is told |

## Refund cases that remain

A locked snapshot removes eligibility drift. It can't remove a provider outage or a change in the provider's data, so these can still charge a customer without delivering:

1. **Long price-feed outage on a wallet purchase.** Retries are free, but there's no automatic refund and no customer-facing refund request.
2. **Price data no longer supports a score on a wallet purchase.** A provider can revise its data after the check. Retrying won't help, so this needs a refund.

Wallet refunds are manual today. `POST /agent/admin/refund` moves USDC from the seller wallet to the admin refund address and marks the purchase refunded, which also stops further retries. It doesn't send anything to the customer, so staff must then send the USDC to the payer's address. PayPal's fee on a refunded payment isn't returned to Selun.

## Not locked

**Later subscription months.** Each is a new analysis on that month's market, so it's screened when it runs. An asset that no longer qualifies then shows price measurements and no overall score, and the report says why. The purchase card discloses this.

**Other wallet services.** Only the Asset Scorecard is checked. Allocation and rebalancing screen when they run and don't promise a score for a named asset.

## Payment methods

Individual $0.50 services stay on x402. PayPal offers only the $4.99 comprehensive report and the $9.99 monthly subscription, because PayPal's fee would exceed the price of a $0.50 service.
