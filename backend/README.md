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
- `X402_QUOTE_TTL_MS` (default `600000`)
- `X402_IP_BURST_WINDOW_MS` (default `60000`)
- `X402_IP_BURST_LIMIT` (default `60`)
- `X402_FROM_ADDRESS_DAILY_CAP` (default `20`)
- `X402_GLOBAL_CONCURRENCY_CAP` (default `8`)
- `PORT` (default `8787`)
- `SELUN_FREE_CODES_JSON` (JSON array of promo code rules; supports `discountPercent` from `0 < x <= 100`)
- `SELUN_FREE_CODES` (CSV fallback of one-use free codes at 100% discount)

## Commands

- `npm run backend:dev`
- `npm run backend:build`
- `npm run backend:start`

## Endpoints

- `POST /agent/x402/allocate`
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

## `/agent/x402/allocate` (x402-style single entrypoint)

Request body:

```json
{
  "decisionId": "required (or use Idempotency-Key header)",
  "riskTolerance": "Conservative | Balanced | Growth | Aggressive",
  "timeframe": "<1_year | 1-3_years | 3+_years",
  "withReport": false
}
```

Headers:

- `Idempotency-Key: <same value as decisionId>` (optional if `decisionId` is in body)

Payment proof (retry request with headers):

- `x402-from-address: 0x...`
- `x402-transaction-hash: 0x...`

Deprecated payment proof fields (backward compatibility only):

- `payment.fromAddress`
- `payment.transactionHash`
- `fromAddress`
- `transactionHash`

Behavior:

- Returns `402` when payment proof is missing/invalid.
- Empty probe requests (no payment headers/body) also return `402` so discovery crawlers can read canonical payment requirements.
- If no `decisionId` is supplied on a probe request, response includes a provisional quote `decisionId`; paid execution still requires caller-provided `decisionId` (or `Idempotency-Key`).
- `402` payload includes `x402.accepts` with both exact price tiers (`allocation_only`, `allocation_with_report`) so agents can choose upfront.
- Returns `409` if the same `decisionId` is reused with different inputs.
- Returns `429` for burst, per-address daily cap, or global concurrency limits.
- Returns `202` after payment verification and starts full allocation orchestration (Phase 1 -> 6) automatically.
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

Deterministic pricing rule (server computed):

- `allocation only`: `STRUCTURED_ALLOCATION_PRICE_USDC`
- `allocation + report`: `STRUCTURED_ALLOCATION_PRICE_USDC + CERTIFIED_DECISION_RECORD_FEE_USDC`
- Caller-provided amount is ignored; server verifies payment meets/exceeds required amount.

## `/agent/x402/capabilities`

Free discovery endpoint for agent integrators. Returns:

- supported `riskTolerance` and `timeframe` enums
- pricing rule and computed prices
- exact `accepts` options for both tiers
- discoverability metadata (method/category/tags/network)
- canonical payment proof headers
- idempotency policy
- current rate limit configuration
