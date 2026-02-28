# Selun Express Backend

This backend adds Coinbase AgentKit operational identity and on-chain operations for Selun on Base.

Copy `backend/env.example` to `backend/.env` and set values there.
Frontend should only keep `SELUN_BACKEND_URL` plus UI-safe vars in `.env.local`.

## Required Environment Variables

- `COINBASE_API_KEY`
- `COINBASE_API_SECRET`
- `AGENT_WALLET_ID`
- `NETWORK_ID` (`base-mainnet` or `base-sepolia`)
- `BASE_MAINNET_RPC` (required when `NETWORK_ID=base-mainnet`)
- `BASE_SEPOLIA_RPC` (required when `NETWORK_ID=base-sepolia`)
- `USDC_CONTRACT_ADDRESS`
- `STRUCTURED_ALLOCATION_PRICE_USDC`
- `CERTIFIED_DECISION_RECORD_FEE_USDC`

Optional:

- `COINBASE_WALLET_SECRET` (falls back to `COINBASE_API_SECRET`)
- `PAYMENT_CONFIRMATIONS` (default `2`, capped to `2`)
- `PAYMENT_POLL_INTERVAL_MS` (default `4000`)
- `PAYMENT_TIMEOUT_MS` (default `120000`)
- `X402_FACILITATOR_URL` (optional override; defaults to `https://www.x402.org/facilitator` on `base-sepolia` and `https://api.cdp.coinbase.com/platform/v2/x402` on `base-mainnet`)
- `CDP_API_KEY_ID` (optional explicit CDP x402 facilitator credential for mainnet; falls back to `COINBASE_API_KEY`)
- `CDP_API_KEY_SECRET` (optional explicit CDP x402 facilitator secret for mainnet; falls back to `COINBASE_API_SECRET`)
- `X402_EIP712_DOMAIN_NAME` (optional override; defaults are derived from the configured payment asset)
- `X402_EIP712_DOMAIN_VERSION` (optional override; defaults are derived from the configured payment asset)
- `X402_QUOTE_TTL_MS` (default `600000`)
- `X402_IP_BURST_WINDOW_MS` (default `60000`)
- `X402_IP_BURST_LIMIT` (default `60`)
- `X402_FROM_ADDRESS_DAILY_CAP` (default `20`)
- `X402_GLOBAL_CONCURRENCY_CAP` (default `8`)
- `X402_MARKET_REGIME_PRICE_USDC` (default `0.25`)
- `X402_POLICY_ENVELOPE_PRICE_USDC` (default `0.25`)
- `X402_ASSET_SCORECARD_PRICE_USDC` (default `0.5`)
- `X402_REBALANCE_PRICE_USDC` (default `1`)
- `X402_STATE_FILE` (optional path override; default `backend/data/x402-state.json`)
- `X402_STATE_RETENTION_DAYS` (default `3`)
- `X402_DISCOVERY_OWNERSHIP_PROOFS` (optional CSV of absolute proof URLs for `/.well-known/x402`)
- `X402_DISCOVERY_INSTRUCTIONS` (optional human-readable instructions string for `/.well-known/x402`)
- `TRUST_PROXY` (default `false`; set for reverse proxies/load balancers)
- `RESEND_API_KEY` (required for email notifications)
- `SELUN_EMAIL_FROM` (sender email, e.g. `Selun <noreply@yourdomain.com>`)
- `SELUN_ADMIN_USAGE_EMAILS_ENABLED` (default `0`)
- `SELUN_ADMIN_USAGE_EMAILS` (CSV list of admin recipients)
- `SELUN_RESULT_EMAILS_ENABLED` (default `0`; enables user allocation summary emails after generation and report emails from the backend)
- `PORT` (default `8787`)
- `SELUN_FREE_CODES_JSON` (JSON array of promo code rules; supports `discountPercent` from `0 < x <= 100`)
- `SELUN_FREE_CODES` (CSV fallback of one-use free codes at 100% discount)

## Commands

- `npm run backend:dev`
- `npm run backend:build`
- `npm run backend:start`

## Fly.io Volume

Fly config mounts persistent storage at `/app/backend/data` using volume source `selun_data`.
Deployment strategy is set to `immediate` and the service keeps one machine warm by default so x402 state is not split across replacement machines.

Create the volume before first deploy (region must match app primary region):

```bash
fly volumes create selun_data --region sin --size 1 --app selunagent
```

Keep the app at a single machine unless you move x402 state into a shared datastore:

```bash
fly scale count 1 --app selunagent
```

## Endpoints

- `POST /agent/x402/allocate`
- `POST /agent/x402/allocate-with-report`
- `POST /agent/x402/market-regime`
- `POST /agent/x402/policy-envelope`
- `POST /agent/x402/asset-scorecard`
- `POST /agent/x402/rebalance`
- `GET /agent/x402/capabilities`
- `POST /agent/init`
- `GET /agent/wallet`
- `GET /agent/pricing`
- `POST /agent/pay-quote`
- `POST /agent/usdc-balance`
- `POST /agent/pay`
- `POST /agent/verify-payment`
- `POST /agent/store-hash`

Wizard UI can continue using `/agent/pay`, `/agent/verify-payment`, and `/agent/phase1..6` routes.

## x402 Resource Catalog

- `POST /agent/x402/allocate`
  - allocation only
  - asynchronous `202` response with `statusPath`
- `POST /agent/x402/allocate-with-report`
  - allocation plus certified decision record
  - asynchronous `202` response with `statusPath`
- `POST /agent/x402/market-regime`
  - synchronous Phase 1 market-condition summary
- `POST /agent/x402/policy-envelope`
  - synchronous Phase 2 policy-envelope output
- `POST /agent/x402/asset-scorecard`
  - synchronous Phase 5 shortlist/scorecard output
- `POST /agent/x402/rebalance`
  - synchronous rebalance recommendations from supplied holdings

All x402 resources:

- require `decisionId` in the body or `Idempotency-Key`
- return `402` with `PAYMENT-REQUIRED` when unpaid
- accept `PAYMENT-SIGNATURE` on paid retries
- return `PAYMENT-RESPONSE` after successful settlement
- enforce quote expiry, replay protection, and transaction single-use

## `/agent/x402/allocate`

Request body:

```json
{
  "decisionId": "required (or use Idempotency-Key header)",
  "riskTolerance": "Conservative | Balanced | Growth | Aggressive",
  "timeframe": "<1_year | 1-3_years | 3+_years"
}
```

Headers:

- `Idempotency-Key: <same value as decisionId>` (optional if `decisionId` is in body)
- `PAYMENT-SIGNATURE: <base64 x402 payment payload>` on the paid retry

Behavior:

- Returns `402` with a `PAYMENT-REQUIRED` header when payment is missing or invalid.
- Paid retries use `PAYMENT-SIGNATURE`, and successful accepts return `PAYMENT-RESPONSE`.
- Empty probe requests also return `402` so discovery crawlers can read canonical payment requirements.
- Exact-EVM requirements include the token EIP-712 domain metadata (`extra.name`, `extra.version`) required by official buyer SDKs.
- On `base-mainnet`, Selun uses Coinbase's authenticated CDP facilitator config for `verify`/`settle`.
- If no `decisionId` is supplied on a probe request, response includes a provisional quote `decisionId`; paid execution must reuse that `decisionId` in the body or `Idempotency-Key`.
- Quoted payment windows are enforced through request-scoped requirements; expired quotes return a fresh `402` challenge.
- Settled transaction hashes are single-use across decision IDs (replay-safe) and persisted across restarts.
- Returns `409` if the same `decisionId` is reused with different inputs.
- Returns `429` for burst, per-address daily cap, or global concurrency limits.
- Returns `202` after successful settlement and starts full allocation orchestration (Phase 1 -> 6) automatically.
- Poll `GET /execution-status/:jobId` from returned `statusPath`.

When available, `GET /execution-status/:jobId` includes a machine-first `agentContract` object:

- `allocation` (flat symbol -> weight map)
- `decisionHash` (deterministic `sha256:` hash of inputs/output metadata)
- `allocatorVersion`
- `executionModelVersion`
- `doctrineVersion`
- `timestamp`
- `confidenceScore`
- echoed `inputs` and `payment`

Pricing:

- `allocation only`: `STRUCTURED_ALLOCATION_PRICE_USDC`
- Caller-provided amount is ignored; server verifies payment meets/exceeds required amount.

## `/agent/x402/allocate-with-report`

Same request body as `/agent/x402/allocate`.

Pricing:

- `allocation + report`: `STRUCTURED_ALLOCATION_PRICE_USDC + CERTIFIED_DECISION_RECORD_FEE_USDC`

## `/agent/x402/rebalance`

Request body:

```json
{
  "decisionId": "required (or use Idempotency-Key header)",
  "riskTolerance": "Conservative | Balanced | Growth | Aggressive",
  "timeframe": "<1_year | 1-3_years | 3+_years",
  "holdings": [
    { "asset": "BTC", "usdValue": 4300 },
    { "asset": "ETH", "usdValue": 3700 },
    { "asset": "USDC", "usdValue": 2000 }
  ]
}
```

Pricing:

- `rebalance`: `X402_REBALANCE_PRICE_USDC` (default `1`)

## `/agent/x402/capabilities`

Free discovery endpoint for agent integrators. Returns:

- resource catalog across allocation, report, market-regime, policy-envelope, asset-scorecard, and rebalance
- computed prices for each resource
- normalized `paymentRequirementsV2` preview entries
- x402 v2 transport headers and facilitator URL
- discoverability metadata (method/category/tags/network)
- idempotency policy
- current rate limit configuration

## Bazaar Cataloging

Selun now registers the official Bazaar resource-server extension on the x402 seller server and declares Bazaar discovery metadata directly on each public x402 resource.

That means:

- the facilitator can extract Selun's discovery metadata from real x402 payment requests
- Selun can be indexed by Bazaar-compatible facilitator catalogs
- a live payment must still be processed before Selun appears in facilitator discovery results

The legacy `X-X402-Bazaar-Discovery` response header remains as a convenience hint for custom clients, but Bazaar listing depends on the official x402 extension path, not that custom header.

## Well-Known Discovery

Selun now exposes a root discovery document for crawler-style registration:

- `GET /.well-known/x402`
- `GET /.well-known/x402.json`

The document now follows x402scan's discovery schema:

- `version`
- `resources` (absolute x402 resource URLs)
- optional `ownershipProofs`
- optional `instructions`

If you are using the branded frontend domain, the Next.js rewrite forwards these well-known URLs to the backend automatically.

### Bazaar Smoke Test

Use the root script to trigger a real x402 payment flow that can seed facilitator discovery:

```bash
npm run x402:bazaar:smoke
```

Generate a throwaway buyer wallet first if needed:

```bash
npm run wallet:generate
```

Or generate one from a fresh mnemonic:

```bash
npm run wallet:generate -- --mnemonic
```

Required environment:

- `SELUN_X402_SMOKE_URL` or `SELUN_BACKEND_URL`
- `SELUN_X402_SMOKE_PRIVATE_KEY` or `EVM_PRIVATE_KEY`

Optional environment:

- `SELUN_X402_SMOKE_DECISION_ID`
- `SELUN_X402_SMOKE_RISK_TOLERANCE` (default `Balanced`)
- `SELUN_X402_SMOKE_TIMEFRAME` (default `1-3_years`)
- `SELUN_X402_SMOKE_WITH_REPORT` (default `false`)
- `SELUN_X402_SMOKE_RESULT_EMAIL`
- `SELUN_X402_SMOKE_PROMO_CODE`
- `SELUN_X402_SMOKE_NETWORK` (default `eip155:*`)
- `SELUN_X402_SMOKE_POLL` (default `true`)
- `SELUN_X402_SMOKE_POLL_INTERVAL_MS` (default `5000`)
- `SELUN_X402_SMOKE_POLL_TIMEOUT_MS` (default `600000`)

The script:

- probes the selected allocation endpoint for a real `402` + `PAYMENT-REQUIRED`
- retries with the official x402 buyer wrapper
- decodes `PAYMENT-RESPONSE`
- optionally polls `statusPath` until Phase 6 completes

When `SELUN_X402_SMOKE_WITH_REPORT=true`, the smoke script automatically targets `/agent/x402/allocate-with-report`.
