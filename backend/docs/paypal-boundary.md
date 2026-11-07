# PayPal integration

Status: validated in PayPal's sandbox on the `selunagent-staging` Fly app, with real webhooks. It isn't ready for live payments. The code is in `backend/src/assistant/paypal-commerce.service.ts` and `paypal-client.ts`. See "Before going live" for what's left.

## What's sold

| Product | x402 | PayPal |
|---|---|---|
| Individual intelligence services | Existing USDC prices | Not offered |
| Comprehensive Intelligence Report | Buy the services individually | $4.99 (`PAYPAL_REPORT_PRICE_USD`) |
| Monthly Intelligence Subscription | Not offered | $9.99/month (`PAYPAL_SUBSCRIPTION_PRICE_USD`), one report per paid cycle |
| Allocation, Allocation with Report | $19, $34 USDC | Deferred |

PayPal products are offered only when `PAYPAL_ENV`, `PAYPAL_CLIENT_ID`, and `PAYPAL_CLIENT_SECRET` are set.

## One execution and report path

- **Execution:** `runComprehensiveReport` runs the same Phase 1 to 5 pipeline and result builders as the x402 services, and produces no analysis of its own.
- **Reports:** `ReportService` freezes, renders, and emails PayPal results the same way as x402 results.
- **Records:** a PayPal purchase keeps its own payment evidence (`PayPalPaymentEvidence`) in `document.provenance.paypal`. Its `transactionHash`, `payer`, `network`, and `amountUsdc` stay null and are never filled with substitutes. x402 documents don't have that field, so their fingerprints don't change.

`runAndRecordPaidTool` remains x402-only because its record shape is on-chain. PayPal records live in `paypal-commerce.json` (`SELUN_PAYPAL_STATE_FILE`).

## One-time purchase

1. The card's button calls `POST /assistant/actions/:id/paypal-checkout`. The server creates an order at its own price, and returns PayPal's approval URL and a purchase token.
2. The customer approves on PayPal and returns to `/?paypal=return`.
3. The browser sends the token to `POST /assistant/paypal/capture`. The server reads the order from PayPal, checks its amount, currency, and purchase reference, then captures it.
4. Only a capture PayPal reports as `COMPLETED` starts the run. `PENDING` waits for a webhook or the next check.

If the buyer never returns, `CHECKOUT.ORDER.APPROVED` does step 3 instead.
5. The browser polls `POST /assistant/paypal/status`, which returns the result and report token once it's complete.

## Rules the service enforces

| Event | What happens |
|---|---|
| Repeat capture, double click, webhook repeat | One capture and one run. Order, capture, and refund requests use stable `PayPal-Request-Id` values, and webhook IDs are recorded |
| A provider fails during the run | Retried on the original payment for 24 hours, with the schedule saved so a restart keeps it. The customer is told it's delayed |
| A locked asset can't score | 3 tries over a few minutes on the original payment |
| Still failing at the end of that, or the result can't ever become a report | Full refund. If the refund fails, the admin is alerted and the customer is told |
| Restart mid-run | Paid purchases resume on startup without another capture |
| Refund or reversal | That purchase's access is revoked and pending work stops. Other purchases aren't affected |
| Partial refund | Access stays, since part of the payment stands |
| Dispute opened | Access is restricted |
| Dispute ends with the payment standing | Access returns |
| Dispute ends with the money returned to the buyer | Treated as a reversal, so access is revoked |
| Unknown dispute outcome | Access stays restricted, and the admin is alerted |

Revocation is checked on every report use, so a signed report token isn't enough on its own. The check re-reads the capture from PayPal if it hasn't been checked in 15 minutes, which catches a missed webhook. If PayPal can't be reached, the last known state applies.

Results, hashes, and the audit trail are never deleted. A PDF already downloaded can't be recalled.

## Subscription

Each `COMPLETED` sale becomes one purchase keyed by sale ID, so repeated syncs and webhooks can't create a second report for the same cycle. Each cycle runs the same fulfillment and refund rules, and refunds that month's payment only. The report is emailed to the address PayPal provides. Cancelling stops future billing, and paid reports stay available.

A sync runs on every status request and webhook, and a sweep runs every 30 minutes.

## Webhooks

`POST /assistant/paypal/webhook` is called by PayPal directly, not through the frontend.

1. PayPal's signature is verified over the raw bytes with its REST API. A failed check returns 401 and stores nothing. If PayPal can't be reached to verify, it returns 503 and PayPal retries.
2. The event is written to `webhookEvents` and flushed to disk, then acknowledged with 200. A repeated event ID is acknowledged and ignored.
3. Processing happens afterwards, in arrival order. Each event re-reads the capture, subscription, or dispute from PayPal, so a late or out-of-order event can only move a record to what PayPal says now.
4. A failed event retries with backoff, starting at 30 seconds and capping at an hour, up to 12 attempts. It then becomes `failed` and alerts the admin. Pending events resume on startup.

Events for a payment Selun doesn't know become `unmatched` and are kept. PayPal's payment list can lag its events, so a sale event for a payment not listed yet is retried rather than dropped.

Two behaviors depend on webhooks:

- **A buyer who never returns:** `CHECKOUT.ORDER.APPROVED` captures the order on the server, so closing the browser after approving doesn't lose the sale.
- **Only paid periods earn a report:** subscription activation and cancellation events never create one. Only a `COMPLETED` payment read from PayPal does.

## Staging

| Setting | Value |
|---|---|
| App | `selunagent-staging`, one Machine in `sin` |
| Config | `fly.staging.toml`, built with `.dockerignore.staging` so no local `backend/data` enters the image |
| Volume | `selun_staging_data`, 1 GB, daily snapshots kept 14 days |
| Webhook URL | `https://selunagent-staging.fly.dev/assistant/paypal/webhook` |
| Payments | PayPal sandbox and Base Sepolia only |
| Email | Mock provider, written to `/app/backend/data/outbox` |

Deploy with:

```bash
flyctl deploy --config fly.staging.toml --app selunagent-staging --ignorefile .dockerignore.staging --ha=false
```

**Fly secrets:** `PAYPAL_CLIENT_ID`, `PAYPAL_CLIENT_SECRET`, `PAYPAL_WEBHOOK_ID`, `PAYPAL_RETURN_BASE_URL`, `SELUN_REFERRAL_INTERNAL_TOKEN`, `SELUN_ADMIN_API_TOKEN`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `COINBASE_API_KEY`, `COINBASE_API_SECRET`, `COINBASE_WALLET_SECRET`, `COINMARKETCAP_API_KEY`, `BASE_SEPOLIA_RPC`, `USDC_CONTRACT_ADDRESS`.

The signing and admin tokens are generated for staging only. It has no `RESEND_API_KEY`, `DATABASE_URL`, or `SCE_ADMIN_KEY`.

**Frontend:** staging has no deployed frontend. Tests run a local production build on port 3001 with `SELUN_BACKEND_URL` set to the staging app and the staging `SELUN_REFERRAL_INTERNAL_TOKEN`. `PAYPAL_RETURN_BASE_URL` points at that build.

**Registering a webhook:** `POST /v1/notifications/webhooks` with the URL and these 23 event types, then store the returned ID as `PAYPAL_WEBHOOK_ID`:

- `CHECKOUT.ORDER.APPROVED`
- `PAYMENT.CAPTURE.COMPLETED`, `PENDING`, `DENIED`, `DECLINED`, `REFUNDED`, `REVERSED`
- `PAYMENT.SALE.COMPLETED`, `PENDING`, `DENIED`, `REFUNDED`, `REVERSED`
- `BILLING.SUBSCRIPTION.CREATED`, `ACTIVATED`, `UPDATED`, `RE-ACTIVATED`, `SUSPENDED`, `CANCELLED`, `EXPIRED`, `PAYMENT.FAILED`
- `CUSTOMER.DISPUTE.CREATED`, `UPDATED`, `RESOLVED`

**Reading the evidence:** `flyctl logs` shows `[paypal-webhook]` lines. `flyctl ssh console -C "cat /app/backend/data/paypal-commerce.json"` shows the stored events and audit trail, and includes subscriber emails.

## Verified event coverage

Tested on 2026-11-06.

| Event | How it was tested | Result |
|---|---|---|
| `CHECKOUT.ORDER.APPROVED` | Real, from PayPal | Captured the order, including with the buyer's return blocked |
| `PAYMENT.CAPTURE.COMPLETED` | Real, from PayPal | Processed. One capture, one report |
| `PAYMENT.CAPTURE.REFUNDED` | Real, from a sandbox refund | Report access revoked about 20 seconds after the refund |
| Redelivery of a real event | Real, using PayPal's resend API | Recognized as a duplicate |
| `BILLING.SUBSCRIPTION.CREATED`, `ACTIVATED` | Real, from PayPal | Processed. No report created |
| `PAYMENT.SALE.COMPLETED` | Real, from PayPal | One report for that payment |
| `BILLING.SUBSCRIPTION.CANCELLED` | Real, from PayPal | No report created. The paid report stayed available |
| `PAYMENT.SALE.REFUNDED` | Real, from a sandbox refund | That month's report revoked |
| `CUSTOMER.DISPUTE.CREATED`, `UPDATED` | Real, from a sandbox buyer's dispute | Report held, 403. The event arrived about 3 minutes after filing |
| `CUSTOMER.DISPUTE.RESOLVED`, buyer wins | Real, after the merchant accepted the claim | `RESOLVED_BUYER_FAVOUR` with `SELLER_ISSUED_REFUND`. Access stayed revoked and the result was kept |
| Purchase logged in as a PayPal buyer | Real | Captured and fulfilled |
| `CUSTOMER.DISPUTE.RESOLVED`, merchant wins | PayPal's simulator and automated tests | Not reached on a real purchase. See "Before going live" |
| `PAYMENT.CAPTURE.REVERSED`, `DENIED` | PayPal's simulator | Same |
| `BILLING.SUBSCRIPTION.PAYMENT.FAILED`, `PAYMENT.SALE.REVERSED` | PayPal's simulator | Same |
| Forged requests | Real requests to the endpoint | 401, nothing stored |
| Their effect on a real purchase: merchant wins, reversals, denied captures, failed subscription payments | Automated tests with a fake PayPal only | Passing |
| A second billing cycle | Automated tests with a fake PayPal only | Passing |

Simulator events pass signature verification, but they can't reference a real purchase. A signed simulator event can't change a record, because every event is matched to a local purchase or re-read from PayPal.

Restart recovery on staging:

- A Machine restart mid-run resumed on the same payment with no second capture.
- A redeploy kept all records, and the PDF stayed byte-identical.
- A refund made while the Machine was stopping woke it when PayPal delivered the event, and access was revoked.

A sandbox buyer's dispute moves through `INQUIRY` and `CHARGEBACK` as `CUSTOMER.DISPUTE.UPDATED` events. Access stays restricted throughout. When a merchant accepts a claim, `PAYMENT.CAPTURE.REFUNDED` arrives before `CUSTOMER.DISPUTE.RESOLVED`, so access is revoked first and the resolved event confirms it.

## Before going live

1. **Remaining real lifecycle events:** a merchant win hasn't happened on a real purchase. PayPal's sandbox `adjudicate` call returned `ACTION_NOT_ALLOWED_IN_CURRENT_DISPUTE_STATE` at `INQUIRY` and `CHARGEBACK`, and a 422 evidence-type error after test evidence was submitted. Sandbox dispute `PP-R-UDN-10196723` is still under review, and its report is correctly held. Also unobserved: `PAYMENT.CAPTURE.REVERSED`, denied captures, and failed subscription payments. Of the codes in `applyDisputeOutcome`, only `RESOLVED_BUYER_FAVOUR` is confirmed against PayPal.
2. **Account recovery:** purchases are found through a token in the browser that started them, so a customer on another device has no way back in. Planned: passwordless access through a verified email, linked to PayPal purchase records.
3. **Durable shared storage:** `paypal-commerce.json` only supports one Machine. Its purchases, subscriptions, plans, and events each map to one Postgres table, with unique keys on capture ID, sale ID, order ID, and event ID. Move it before scaling out.
4. **Production configuration:** as of 2026-11-06, production (v107) serves Selun AI with `PAYPAL_ENV=sandbox`, and has no `PAYPAL_RETURN_BASE_URL` or `PAYPAL_WEBHOOK_ID`. The return URL therefore falls back to `http://localhost:3000`, and its webhook endpoint returns 503. Before PayPal is offered there, either remove the PayPal secrets, which stops offering it, or set live credentials, the return URL, and a registered webhook.
5. **Admin visibility:** PayPal purchases aren't in the admin panel or usage emails. Failures do alert the admin.
6. **Event retention:** stored events are never pruned. Add a retention rule before volume grows, keeping at least PayPal's three-day retry window.
7. **Privacy and terms:** the subscriber's PayPal email is stored in plain text for delivery. Update the privacy policy and terms, including refund and subscription terms.
8. **Regulatory and PayPal review:** selling financial analysis for fiat, and PayPal's rules on crypto-related goods, need legal review.
9. **Fee review:** PayPal generally keeps its fee on refunds, so an undelivered report costs money. Track the refund rate.
