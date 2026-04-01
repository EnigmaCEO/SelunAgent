# Selun Agent

Selun Agent is the browser and API surface for Selun, a Sagitta AAA-powered crypto allocation product.

This repo combines:

- a Next.js frontend for the public site, allocation wizard, Stripe checkout, report generation, and admin tooling
- an Express backend for agent payment flows, x402 resources, treasury operations, and referral persistence
- retail and agent-facing payment paths, including card checkout, onchain USDC settlement, promo codes, and referrals

## What Lives Here

Selun currently supports three primary product surfaces:

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

- `app/page.tsx`: public landing page and referral capture
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

MIT
