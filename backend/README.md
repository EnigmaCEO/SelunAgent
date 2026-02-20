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
- `PORT` (default `8787`)

## Commands

- `npm run backend:dev`
- `npm run backend:build`
- `npm run backend:start`

## Endpoints

- `POST /agent/init`
- `GET /agent/wallet`
- `GET /agent/pricing`
- `POST /agent/usdc-balance`
- `POST /agent/pay`
- `POST /agent/verify-payment`
- `POST /agent/store-hash`
