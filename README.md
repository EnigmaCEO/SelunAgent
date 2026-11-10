# Selun Agent

Selun Agent is the browser and API surface for Selun, a Sagitta AAA-powered crypto allocation product.

This repo combines:

- a Next.js frontend for the public site, allocation wizard, Stripe checkout, report generation, and admin tooling
- an Express backend for agent payment flows, x402 resources, treasury operations, and referral persistence
- retail and agent-facing payment paths, including card checkout, onchain USDC settlement, promo codes, and referrals

## Selun AI: PayPal, AI, and AG Grid

Selun AI is a crypto intelligence assistant for beginners. Ask in plain language ("Tell me more about Bitcoin", then "What are the risks?"). Selun explains, picks the right analysis, and shows its exact price. Pay with PayPal, and the analysis runs and comes back as a readable result with an AI-written summary and a PDF. Buy a second analysis, and an AG Grid compares them; ask "Compare Bitcoin and Ethereum", and Selun explains the difference from your own purchased results.

Live demo: https://selun.sagitta.systems/ (PayPal sandbox during judging).

### How PayPal is used

All PayPal calls are server-side REST calls, in `backend/src/assistant/paypal-client.ts` and `backend/src/assistant/paypal-commerce.service.ts`:

- **Orders API v2** (`/v2/checkout/orders`, create and capture): one-time purchase of an analysis or report. The customer approves at PayPal; Selun captures the payment, then runs the analysis.
- **Subscriptions API** with **Catalog Products** and **Billing Plans**: the monthly intelligence report. Selun creates the product and plan, starts the subscription, reads its status and renewal transactions, and supports cancellation.
- **Payments API**: reads captures and issues refunds (`/v2/payments/captures/{id}/refund`, `/v1/payments/sale/{id}/refund`).
- **Webhooks** with signature verification (`/v1/notifications/verify-webhook-signature`) at `POST /assistant/paypal/webhook`: captures completed, pending, declined, refunded, or reversed; orders approved; subscriptions activated, updated, or failed; subscription payments.
- **Disputes API**: reads a dispute so access to a disputed purchase can be revoked.
- **OAuth 2.0** client credentials.

Only a verified capture or a verified webhook event unlocks a paid result. Purchases are recorded durably, so a result survives a restart. See [backend/docs/paypal-boundary.md](backend/docs/paypal-boundary.md).

### How AI is used

- **OpenAI Chat Completions with tool calling** (`backend/src/assistant/openai-client.ts`, `assistant.service.ts`). The model understands the question in context, explains concepts, and calls tools to find a service, resolve an asset, show a quote card, read Sagitta Radar's live alerts, or compare purchased analyses.
- The model **cannot charge or run anything**. Its tools only show quotes; the customer pays in PayPal, and the server verifies the payment before the analysis runs.
- **Every figure comes from code.** Result cards are built by `result-presenter.ts`, not the model. The model's plain-language summary is checked against the result, and `claim-guard.ts` blocks prices with no source and investment advice.
- **Comparisons** (`compare_analyses`): the browser sends only signed report tokens for its purchases. The server reads each verified result, refusing refunded or forged ones, and works out which asset leads each measure before the model explains it.

### How AG Grid is used

`app/chat/AssetGrid.tsx` uses **AG Grid Community** (`ag-grid-community` and `ag-grid-react` 36.2.0, MIT) to compare every asset the customer has analyzed: overall score, price risk, volatility, largest drop, market standing, and the date of each analysis. Columns sort (unmeasured values always last), scores show as bars, header tooltips explain each measure, and **Download CSV** exports with units. A one-line finding above the grid ("Ethereum had the higher price risk (55 vs 38) and the deeper largest drop…") is computed from the grid's own rows. The grid appears under a result once there's something to compare, and in chat when Selun answers a comparison question. It loads on demand, so the first page stays light.

### Run it locally with the PayPal sandbox

1. Create a sandbox REST app at https://developer.paypal.com (Apps & Credentials, Sandbox), and a sandbox personal account under Testing Tools to pay with.
2. Install and copy the env templates (see Local Setup below).
3. In `backend/.env`, set at least:
   - `PAYPAL_ENV=sandbox`, `PAYPAL_CLIENT_ID`, `PAYPAL_CLIENT_SECRET` (from the sandbox app)
   - `PAYPAL_RETURN_BASE_URL=http://localhost:3000` (the default)
   - `OPENAI_API_KEY` and `OPENAI_MODEL`
   - `SELUN_REFERRAL_INTERNAL_TOKEN`: any long random string, which signs report tokens for PDFs and comparisons. Use the same value in the frontend `.env.local`.
   - `PAYPAL_WEBHOOK_ID` is optional locally: purchases are confirmed by capture on return. To receive webhooks, expose `/assistant/paypal/webhook` through a tunnel and register it in the sandbox app.
   - Running the analyses themselves uses the backend's x402 service settings, described in `backend/README.md`.
4. `npm run backend:dev` and `npm run dev`, then open http://localhost:3000 and pay with the sandbox personal account.

Tests: `npm run backend:test` (416 backend tests, including PayPal webhook, capture, refund, and dispute handling, and the comparison tool).

## What Lives Here

Selun currently supports four primary product surfaces:

0. Selun AI chat (the home page)
   - conversational gateway at `/`, backed by `/assistant/*` on the Express backend
   - see "Selun AI" below

1. Wizard checkout flow
   - human-facing allocation wizard at `/wizard`
   - card checkout through Stripe
   - onchain USDC checkout through backend payment verification
   - optional certified decision report

2. Agent and x402 APIs
   - x402-paid endpoints under `/agent/x402/*`
   - discovery routes at `/.well-known/x402`, `/openapi`, and `/openapi.json`
   - async execution status routes under `/execution-status/*`

3. Referral and promo infrastructure
   - wallet-based referral tracking and leaderboard
   - agent referral program with attribution, payouts, and withdrawal flows
   - promo code and free-code support for the wizard and agent pay routes

## Repo Layout

```text
SelunAgent/
  app/                 Next.js App Router frontend and server routes
  backend/             Express API, x402 seller logic, payment ops, referrals
  public/              Static assets
  scripts/             Local utility and smoke-test scripts
  next.config.ts       Frontend rewrites into backend routes
  .env.local.example   Frontend env template
```

Key areas:

- `app/page.tsx` and `app/chat/*`: Selun AI chat, the customer home page
- `app/plan/page.tsx`: the portfolio-plan landing page (formerly the home page)
- `backend/src/assistant/*`: assistant service, capability registry, asset resolver, OpenAI client
- `app/wizard/page.tsx`: allocation wizard and retail payment UX
- `app/api/stripe/*`: Stripe checkout, confirmation, and webhook handling
- `app/api/agent/*`: frontend-to-backend proxy routes for wallet payment and phase execution
- `app/earn/*`: referral dashboard UI
- `backend/src/server.ts`: backend entrypoint
- `backend/src/routes/agent.routes.ts`: wallet payment, pricing, admin, and phase routes
- `backend/src/routes/referral.routes.ts`: wallet referral endpoints
- `backend/src/routes/agent-program.routes.ts`: agent referral program endpoints

## Architecture

```text
Browser / Agent Client
        |
        v
Next.js app + server routes
        |
        +--> Stripe checkout + webhook handling
        |
        +--> Express backend
                |
                +--> Sagitta AAA execution phases
                +--> x402 payment-protected resources
                +--> referral persistence in Postgres
                +--> treasury / admin wallet operations
```

## Prerequisites

- Node.js 20+
- npm
- PostgreSQL if you want referral persistence enabled locally
- Stripe test credentials if you want the card checkout path locally
- WalletConnect project ID if you want QR / mobile wallet testing

## Local Setup

Install dependencies for both the frontend and backend:

```bash
cd SelunAgent
npm install
npm --prefix backend install
```

Create local env files:

```bash
Copy-Item .env.local.example .env.local
Copy-Item backend\env.example backend\.env
```

Minimum frontend env usually needed in `.env.local`:

- `SELUN_BACKEND_URL`
- `SELUN_PUBLIC_BACKEND_ORIGIN`
- `NEXT_PUBLIC_SITE_URL`
- `SELUN_REFERRAL_INTERNAL_TOKEN`
- `NEXT_PUBLIC_WALLETCONNECT_PROJECT_ID` when testing QR wallet connection

Additional frontend env required for card checkout:

- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- optionally `STRIPE_SELUN_ALLOCATION_PRICE_ID`
- optionally `STRIPE_REPORT_PRICE_ID`

Backend env is more extensive. Use `backend/env.example` and the detailed backend guide in `backend/README.md`.

## Running Locally

Start the backend:

```bash
npm run backend:dev
```

Start the frontend in a second terminal:

```bash
npm run dev
```

Default local split:

- frontend: `http://localhost:3000`
- backend: `http://localhost:8787`

## Main Workflows

### Wizard Flow

The wizard supports:

- risk / horizon / segment configuration
- card checkout through Stripe
- onchain USDC payment through `/api/agent/pay` and `/api/agent/verify-payment`
- report download and optional result email delivery
- referral-aware checkout metadata

Relevant files:

- `app/wizard/page.tsx`
- `app/api/stripe/wizard-checkout/route.ts`
- `app/api/stripe/wizard-confirm/route.ts`
- `app/api/stripe/webhook/route.ts`

### x402 API Surface

The branded frontend can expose backend x402 routes through rewrites, including:

- `/agent/x402/*`
- `/.well-known/x402`
- `/.well-known/x402.json`
- `/openapi`
- `/openapi.json`
- `/execution-status/*`

Smoke test helpers are available from the root package scripts:

```bash
npm run x402:bazaar:smoke
npm run x402:smoke:allocate
npm run x402:smoke:allocate-with-report
npm run x402:smoke:market-regime
npm run x402:smoke:policy-envelope
npm run x402:smoke:asset-scorecard
npm run x402:smoke:rebalance
```

### Referral System

Selun currently has two referral modes:

1. Wallet referral tracking
   - public `?ref=` capture on the landing page
   - payout accounting stored in Postgres
   - leaderboard and earnings views through `/referral/*`

2. Agent referral program
   - referrer registration
   - click attribution
   - allocation confirmation
   - fixed payout accounting and withdrawals

Frontend and Stripe routes pass referral metadata into backend tracking so conversions can be recorded after payment confirmation.

### Selun AI

Customers ask in plain language. The model chooses a service, the customer approves the exact price in their wallet, and the existing x402 route does the work.

```text
Browser  ->  /api/assistant/* (Next proxy, path allowlist)  ->  /assistant/* (Express)
                                                                    |-- OpenAI Chat Completions (function calling)
                                                                    |-- live x402 catalog (prices)
                                                                    '-- /agent/x402/* over loopback (payment + execution)
```

Rules the code enforces:

- The model can only list services, resolve assets, and show quotes. It has no tool that runs a paid service and never sees a payment signature.
- Prices come from the live x402 catalog. If the catalog is unavailable, no price is shown. A price change forces re-approval.
- A paid action needs `approved: true`, the current price, and a wallet signature for that amount.
- A result appears only when the route returns a verified payment receipt. Cards are built by `result-presenter.ts`, not the model.
- Limits: per purchase, per conversation, per wallet per day, per client request rate, and a daily model-turn budget. See `backend/env.example`.

Configuration, set as Fly secrets on the backend:

```bash
fly secrets set OPENAI_API_KEY=... OPENAI_MODEL=...
```

Without both values the chat shows an offline state, and every other route keeps working. No frontend environment changes are needed.

Adding a service: add a spec to `backend/src/assistant/capability-registry.ts` and a presenter to `result-presenter.ts`. Anything not in the registry stays invisible to the assistant. Then add its typical requests to `backend/scripts/assistant-routing-eval.ts`.

PayPal sells the Comprehensive Intelligence Report and the monthly subscription through the chat, using the same pipeline and report path as x402. See `backend/docs/paypal-boundary.md`.

Selun AI also answers questions about Sagitta, for free and with no commerce, from Sagitta's own websites. It re-reads them daily and starts from the snapshot written by `npx tsx backend/scripts/sync-sagitta-sites.ts`. System cards come from `Systems/src/content/systems.ts`, the source behind sagitta.systems: after that changes, run `npx tsx backend/scripts/sync-sagitta-directory.ts`.

Routing check: `npm run assistant:routing-eval -- --runs=3` sends those requests to the configured model and reports any that reach the wrong service. It uses model tokens, but nothing is paid for or run. If a customer names a token and the model quotes only Market Regime, the server adds that token's Asset Scorecard quote.

Hosted checkouts (PayPal, and Stripe for cards) implement `CheckoutProvider` in `backend/src/assistant/commerce.ts`. Once the payment is verified, the backend runs the same paid service a wallet purchase would, and records the result.

Paid chat results have a PDF download and an email option on their card, served by `backend/src/assistant/report.service.ts`. A report is built only from the paid route's stored result and frozen on first use in `backend/data/report-fulfillment.json`, so it needs that volume to persist. The browser sends only a signed report token. The next payment rail's records and entry point are described in [backend/docs/paypal-boundary.md](backend/docs/paypal-boundary.md).

Conversations and spend counters live in backend memory, so they reset when the backend restarts. Customers' visible history is stored only in their browser.

## Useful Scripts

Frontend and repo-level scripts:

- `npm run dev`
- `npm run build`
- `npm run start`
- `npm run lint`
- `npm run backend:dev`
- `npm run backend:build`
- `npm run backend:start`
- `npm run backend:test`

Wallet / treasury helpers:

- `npm run wallet:generate`
- `npm run wallet:derive`
- `npm run wallet:refund-usdc`
- `npm run wallet:fund-treasury-eth`

Backend-only referral helpers live under `backend/package.json`, including commission backfill, wallet seed data, and agent referral simulation.

## Deployment Notes

- Next.js rewrites public x402 and referral routes to the backend in `next.config.ts`.
- Stripe checkout and webhook logic runs inside Next.js server routes, so frontend deployment needs the Stripe secret env values.
- Referral persistence depends on backend Postgres access through `DATABASE_URL`.
- The backend README documents Fly.io volume setup, x402 behavior, admin routes, and required backend environment.

## Where To Read Next

- `backend/README.md`: detailed backend setup, environment variables, routes, x402 semantics, admin operations
- `.env.local.example`: frontend environment template
- `backend/env.example`: backend environment template

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
