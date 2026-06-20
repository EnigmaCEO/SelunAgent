# Selun | Sagitta AAA — Agent Skill Guide

**Base URL:** `https://selun.sagitta.systems`  
**Payment:** x402 — pay per call in USDC on Base or Solana mainnet  
**Discovery:** `https://selun.sagitta.systems/agent/x402/capabilities`  
**OpenAPI:** `https://selun.sagitta.systems/openapi.json`  
**Docs:** `https://selun.sagitta.systems/for-developers`

---

## When to use each skill

### 1. Market Regime — `POST /agent/x402/market-regime`

Use this **before allocation or rebalancing** when you need to know the current market context.

Returns: volatility state, liquidity state, risk appetite, confidence score, and allocation authorization inputs shaped by the requested portfolio segment.

Call this first in any portfolio workflow. The regime output feeds directly into allocation and policy decisions.

```json
{
  "decisionId": "unique-decision-id",
  "riskTolerance": "Balanced",
  "timeframe": "1-3_years",
  "portfolioSegment": "Bluechips"
}
```

---

### 2. Policy Envelope — `POST /agent/x402/policy-envelope`

Use this **when you need constraints before allocation** — exposure caps, risk budget, stablecoin floor, and segment-aware universe guidance.

Returns: per-asset exposure caps, stablecoin minimum, max drawdown budget, authorization constraints, and eligible asset universe filters.

Call this after market regime and before asset scoring or allocation.

```json
{
  "decisionId": "unique-decision-id",
  "riskTolerance": "Growth",
  "timeframe": "3+_years",
  "portfolioSegment": "Yield Farm"
}
```

---

### 3. Asset Scorecard — `POST /agent/x402/asset-scorecard`

Use this **before asset selection or allocation** to score candidate assets.

Returns: liquidity score, structural stability score, role classification (core/satellite/stablecoin), and composite quality score for each candidate — shaped by the requested portfolio segment.

Call this after policy envelope and before constructing the allocation.

```json
{
  "decisionId": "unique-decision-id",
  "riskTolerance": "Balanced",
  "timeframe": "1-3_years",
  "portfolioSegment": "Bluechips"
}
```

---

### 4. Allocate — `POST /agent/x402/allocate`

Use this **when the agent needs a target crypto allocation** for a specified risk tolerance, timeframe, and portfolio segment.

Returns: an accepted allocation job with a `statusPath` for async polling. Poll the status path until `status: "completed"`, then read the allocation weights.

```json
{
  "decisionId": "unique-decision-id",
  "riskTolerance": "Balanced",
  "timeframe": "1-3_years",
  "portfolioSegment": "Bluechips"
}
```

For allocation + a certified decision record (on-chain hash + PDF report), use `POST /agent/x402/allocate-with-report` instead.

---

### 5. Rebalance — `POST /agent/x402/rebalance`

Use this **after allocation or on a monitoring schedule** when you have current holdings and need to know what to adjust.

Returns: target-vs-current drift per asset and the rebalance adjustments required to return within policy constraints for the requested segment.

```json
{
  "decisionId": "unique-decision-id",
  "riskTolerance": "Balanced",
  "timeframe": "1-3_years",
  "portfolioSegment": "Bluechips",
  "currentHoldings": { "ETH": 0.45, "BTC": 0.30, "USDC": 0.25 }
}
```

---

### 6. SCE Continuity Mode — `POST /agent/x402/sce/continuity-mode`

Use this **before any protocol interaction** to check whether prior case doctrine permits the proposed action or requires escalation.

Returns: a continuity verdict (`proceed` / `escalate` / `halt`) and the doctrine basis for the decision.

Call this before interacting with a DeFi protocol, bridge, oracle, or any on-chain action that has a case history.

---

### 7. SCE Case Relevance — `POST /agent/x402/sce/case-relevance`

Use this **when there is a specific threat, protocol, oracle, bridge, CVE, or incident** you need to reason about.

Returns: matching doctrine cases from the Sagitta AAA case library relevant to the proposed action, ranked by relevance.

Call this when the agent encounters a named protocol, token, bridge, or incident and needs case-grounded context before deciding.

---

### 8. SCE Risk Evaluate — `POST /agent/x402/sce/risk-evaluate`

Use this **before capital movement, routing, bridging, allocation, or execution**.

Returns: a structured risk verdict (`low` / `elevated` / `high` / `critical`), recommended escalation level, and the doctrine evidence behind the verdict.

This is the broadest SCE call — use it any time the agent is about to move capital or execute on-chain and wants a doctrine-grounded risk check.

---

## Recommended call order

For a full portfolio decision workflow:

```
market-regime → policy-envelope → asset-scorecard → allocate
```

For execution safety before any on-chain action:

```
sce/risk-evaluate → [optional] sce/case-relevance → sce/continuity-mode → execute
```

For portfolio maintenance:

```
market-regime → rebalance
```

---

## Payment

All endpoints require x402 payment in USDC. The server returns `402` with a `PAYMENT-REQUIRED` header on unauthenticated requests. Sign the payment header with your wallet and retry the same request with a `PAYMENT-SIGNATURE` header.

Supported networks: **Base mainnet**, **Solana mainnet**  
Facilitator: see `https://selun.sagitta.systems/agent/x402/capabilities` for current facilitator URL and per-endpoint pricing.

---

## Agent card

`https://selun.sagitta.systems/.well-known/agent-card.json`
