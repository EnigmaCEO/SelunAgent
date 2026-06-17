# SELUN SCE WRAPPER — STEP 0 REPO MAP

> Read-only orientation. No code changes made.

---

## 1. Repo Snapshot

| Field | Value |
|---|---|
| Repo name | `selun-agent` |
| Branch | `master` |
| Commit | `de04f9e` ("Copy changes") |
| Working tree | **clean** (`nothing to commit, working tree clean`) |
| Relevant apps | `backend/` — Express TypeScript API (primary x402 implementation); `app/` — Next.js frontend (separate, no x402 routes) |

The backend is a standalone Express server (`backend/src/server.ts`), started with `npm run backend:dev` (port 8787 by default). All x402 payment logic lives there. The Next.js app has its own API routes but none of them implement x402.

---

## 2. Existing x402 Endpoint Map

All routes are defined in `backend/src/routes/agent.routes.ts` and mounted at the `/agent` prefix in `backend/src/server.ts` line 594:
```
app.use("/agent", createAgentRouter());
```

| Endpoint | File | Method | productId | Price (USDC) | Sync/Async | Payment helper | Response shape | Tests | Notes |
|---|---|---|---|---|---|---|---|---|---|
| `/agent/x402/allocate` | `agent.routes.ts:3889` | POST | `allocate` | `19` (env `STRUCTURED_ALLOCATION_PRICE_USDC`) | **Async** → 202 | `handleX402AllocateRequest()` | `{ success, data: { status: "accepted", jobId, decisionId, statusPath } }` | None for route | Starts 6-phase job, polls via `/execution-status/:jobId` |
| `/agent/x402/allocate-with-report` | `agent.routes.ts:3898` | POST | `allocate_with_report` | `19 + 15 = 34` (env sums) | **Async** → 202 | `handleX402AllocateRequest()` | Same as allocate | None for route | Same flow, `withReport=true` |
| `/agent/x402/market-regime` | `agent.routes.ts:3907` | POST | `market_regime` | `0.25` (env `X402_MARKET_REGIME_PRICE_USDC`) | **Sync** → 200 | `handleX402ToolRequest()` | `{ success, data: { status, endpoint, decisionId, productId, payment, result } }` | None for route | Waits for phase2 inline |
| `/agent/x402/policy-envelope` | `agent.routes.ts:3930` | POST | `policy_envelope` | `0.25` (env `X402_POLICY_ENVELOPE_PRICE_USDC`) | **Sync** → 200 | `handleX402ToolRequest()` | Same as tool shape | None for route | Waits for phase2 inline |
| `/agent/x402/asset-scorecard` | `agent.routes.ts:3953` | POST | `asset_scorecard` | `0.50` (env `X402_ASSET_SCORECARD_PRICE_USDC`) | **Sync** → 200 | `handleX402ToolRequest()` | Same as tool shape | None for route | Waits for phases 2–5 inline |
| `/agent/x402/rebalance` | `agent.routes.ts:3981` | POST | `rebalance` | `1.00` (env `X402_REBALANCE_PRICE_USDC`) | **Sync** → 200 | `handleX402ToolRequest()` | Same as tool shape | None for route | Waits for phases 2–6 inline |

**Framework**: Express 5.2.1 (`express` package), TypeScript, Node 20+.

**POST body parsing**: `app.use(express.json({ limit: "1mb" }))` in `server.ts:234`. Bodies arrive as `req.body` typed as `unknown` until accessed.

**Error responses**: all routed through `failure(res, error, statusCode?)` which returns:
```json
{
  "success": false,
  "executionModelVersion": "Selun-1.0.0",
  "error": "<message>",
  "logs": [...]
}
```

**Sync vs async distinction**: allocate routes return 202 and fire off multi-phase background jobs. Tool routes (market-regime, policy-envelope, asset-scorecard, rebalance) await their result inline and return 200. SCE wrapper endpoints would follow the **sync tool pattern** (like `handleX402ToolRequest`).

---

## 3. Capabilities / Catalog Map

**File**: `backend/src/routes/agent.routes.ts`

**Function**: `buildX402CapabilitiesData()` (lines ~1817–1923), exported as `getX402CapabilitiesData()` (line 1926), served at `GET /agent/x402/capabilities` (line 3101) and `GET /agent/x402/discovery` (alias, line 3116).

**How resources are built**:

1. **Two hardcoded allocate resources** (lines ~1825–1851): inline objects for `/agent/x402/allocate` and `/agent/x402/allocate-with-report` with `productId: "allocate"` / `"allocate_with_report"`.

2. **Dynamic tool resources** (lines ~1852–1868): `getX402ToolDefinitions()` returns an array of `X402ToolDefinition[]`; the capabilities builder maps over it. Each entry in the array generates one resource entry automatically.

**`getX402ToolDefinitions()` return array** (lines 771–958):
- `market_regime` → `/agent/x402/market-regime`, `category: "finance:market-regime"`, tags: `["portfolio", "market-regime", "volatility", "sentiment", "x402"]`
- `policy_envelope` → `/agent/x402/policy-envelope`, `category: "finance:policy-envelope"`, tags: `["portfolio", "policy-engine", "risk-budget", "exposure-caps", "x402"]`
- `asset_scorecard` → `/agent/x402/asset-scorecard`, `category: "finance:asset-scorecard"`, tags: `["portfolio", "asset-scoring", "liquidity", "quality", "x402"]`
- `rebalance` → `/agent/x402/rebalance`, `category: "finance:rebalance"`, tags: `["portfolio", "rebalance", "drift-analysis", "allocation-target", "x402"]`

**Capabilities output schema**:
```jsonc
{
  "discoverable": true,
  "name": "Selun | Sagitta AAA Portfolio Infrastructure",
  "description": "...",
  "provider": { "name": "Sagitta AAA", "url": "https://selun.sagitta.systems" },
  "x402Version": 2,
  "versions": { "executionModelVersion": "Selun-1.0.0" },
  "pricing": { "allocationOnlyUsdc": "19", "allocationWithReportUsdc": "34", ..., "marketRegimeUsdc": "0.25", ... },
  "resources": [ { "endpoint", "method", "productId", "title", "description", "pricing", "paymentRequirementsV2", "inputSchema" } ],
  "paymentTransport": { "facilitatorUrl", "headers", "requestScopedRequirements" },
  "limits": { "ipBurstLimit", "fromAddressDailyCap", "globalConcurrencyCap" },
  "idempotency": { ... },
  "discovery": { "type": "http", "category": "finance:portfolio-agent", "tags": [...], ... }
}
```

**Where SCE wrapper resources should be added**:
- Add each SCE product as a new entry in the `X402ToolDefinition[]` returned by `getX402ToolDefinitions()` (after the `rebalance` entry, line ~957).
- The capabilities builder automatically includes them — no manual change to `buildX402CapabilitiesData()` needed.

**Recommended SCE discovery tags** (machine-native, distinct from existing tags):

| SCE product | category | tags |
|---|---|---|
| `sce_continuity_mode` | `"intelligence:continuity-mode"` | `["sce", "continuity-mode", "active-intelligence", "scenario", "x402"]` |
| `sce_case_relevance` | `"intelligence:case-relevance"` | `["sce", "case-relevance", "active-intelligence", "classification", "x402"]` |
| `sce_risk_evaluate` | `"intelligence:risk-evaluate"` | `["sce", "risk-evaluate", "active-intelligence", "risk-scoring", "x402"]` |

---

## 4. Payment Flow Map

All x402 payment logic flows through functions in `agent.routes.ts`. The tool flow via `handleX402ToolRequest()` is canonical for sync endpoints.

### 4a. Challenge Generation (402 response)

1. Client sends POST with no `PAYMENT-SIGNATURE` header.
2. Server resolves `decisionId` from body or `Idempotency-Key` header (`resolveDecisionId()`).
3. Server fetches agent wallet address (`getAgentAddress()`).
4. Server builds a `X402ToolPaymentRequirementV2` or `X402PaymentRequirementV2` scoped to: `decisionId`, `inputFingerprint`, `quoteIssuedAt`, `quoteExpiresAt` (`buildToolRequirement()` / `buildRequestScopedPaymentRequirement()`).
5. `x402HTTPResourceServer.processHTTPRequest()` returns `{ type: "payment-error", response: { status: 402, headers: { "PAYMENT-REQUIRED": ... }, body: challengeBody } }`.
6. Server calls `sendX402HttpResponse()` which sets `PAYMENT-REQUIRED` header and returns 402 with `challengeBody`.
7. Quote window (`quoteExpiresAt`) is written to state store (`stateStore.setToolRecord()` with `state: "quoted"`).

### 4b. Verification (payment-verified path)

1. Client re-sends POST with `PAYMENT-SIGNATURE` header.
2. `processHTTPRequest()` returns `{ type: "payment-verified", paymentPayload, paymentRequirements, declaredExtensions }`.
3. `httpServer.server.verifyPayment(paymentPayload, paymentRequirements)` → `verifyResult.isValid + verifyResult.payer`.
4. If not valid → 502 with `x402 verification mismatch: ...`.
5. `payer = verifyResult.payer` (fromAddress) — this is an EVM address.

### 4c. Settlement / Facilitator

1. `httpServer.processSettlement(paymentPayload, paymentRequirements, declaredExtensions, { request: httpContext })`.
2. Returns `settlement.success`, `settlement.transaction` (transactionHash), `settlement.network`, `settlement.payer`.
3. Failure → 502 with structured error including `facilitatorUrl`.
4. Success → `transactionHash = settlement.transaction`.

### 4d. Mismatch Handling

- If `verifyResult.isValid === false` → 502 "x402 verification mismatch".
- If `settlement.success === false` → 502 "x402 settlement failed".
- If `transactionHash` missing → 502 "x402 settlement completed without a transaction hash".

### 4e. Idempotency / Conflict

- `stateStore.reserveTransactionHash(transactionHash, stateKey)` — if the same transaction hash was already used for a different decisionId → 409.
- On repeated call with same `decisionId` + same inputs + already `"accepted"` → 200 idempotent replay via `idempotentToolResponse()`.
- On repeated call with same `decisionId` + **different** inputs → 409 conflict.

### 4f. decisionId Binding

- `decisionId` from body `req.body.decisionId` or `Idempotency-Key` header; must match if both present (`resolveDecisionId()`).
- `inputFingerprint = sha256(JSON.stringify(normalizedInput))` — bound into `paymentRequirementsV2.extra`.
- Quote window (issuedAt / expiresAt) also bound into `extra`.
- Mismatch on retry causes re-challenge (fresh 402).

### 4g. Shared Helpers SCE Wrappers Must Reuse

| Helper | Location | Purpose |
|---|---|---|
| `handleX402ToolRequest()` | `agent.routes.ts:2094` | Full payment lifecycle for sync tool endpoints; **SCE wrappers should use this or replicate its pattern exactly** |
| `resolveDecisionId()` | `agent.routes.ts:490` | decisionId extraction and conflict detection |
| `buildToolRequirement()` | `agent.routes.ts:1296` | Builds scoped `PaymentRequirements` for a tool |
| `buildToolPreviewRequirement()` | `agent.routes.ts:1317` | Builds non-scoped requirement for capabilities catalog |
| `buildToolChallengeBody()` | `agent.routes.ts:1334` | Builds 402 JSON body |
| `createToolX402HttpServer()` | `agent.routes.ts:1399` | Instantiates x402 HTTP server per-request |
| `enforceIpBurstLimit()` | `agent.routes.ts:528` | IP-level rate limiting |
| `getAddressUsageCount()` / `incrementAddressUsage()` | `agent.routes.ts:561` | Per-address daily cap |
| `getX402SellerServer()` | `agent.routes.ts:1261` | Singleton x402ResourceServer (lazy-init) |
| `stateStore.reserveTransactionHash()` | `x402-state.service.ts` | Anti-replay guard |
| `stateStore.setToolRecord()` | `x402-state.service.ts` | Persist accepted payment + response |
| `applySettlementResponseHeaders()` | `agent.routes.ts:1512` | Set `PAYMENT-RESPONSE` header |
| `attachBazaarDiscovery()` | `agent.routes.ts:1132` | Set discovery header on response |
| `sendAdminUsageEmail()` | `email.service.ts:267` | Fire-and-forget usage log email |

### 4h. `evaluationId` for SCE Wrappers

Use the existing `decisionId` parameter name unchanged — it already serves as an idempotency key and is what buyers generate. No need for a separate `evaluationId` field. The SCE upstream will receive whatever identifier the SCE API expects in the proxied body; Selun tracks it under `decisionId`.

---

## 5. Usage Logging Map

**File**: `backend/src/services/email.service.ts`

**Mechanism**: fire-and-forget email via Resend (`sendAdminUsageEmail()`), enabled via `SELUN_ADMIN_USAGE_EMAILS_ENABLED=1`. All logs go to `SELUN_ADMIN_USAGE_EMAILS` recipients.

**Current `AdminUsageChannel` type** (line 1):
```typescript
type AdminUsageChannel = "legacy_pay" | "x402_allocate";
```

**Fields currently logged** in `AdminUsageEmailInput`:

| Field | Type | Notes |
|---|---|---|
| `channel` | `"legacy_pay" \| "x402_allocate"` | Tool calls use `"x402_allocate"` |
| `decisionId` | `string` | Required |
| `endpoint` | `string \| null` | Route path |
| `productId` | `string \| null` | e.g. `"market_regime"` |
| `walletAddress` | `string \| null` | fromAddress/payer |
| `resultEmail` | `string \| null` | If provided in body |
| `promoCode` | `string \| null` | If provided in body |
| `chargedAmountUsdc` | `string \| number \| null` | USDC amount |
| `transactionHash` | `string \| null` | Settlement txHash |
| `paymentMethod` | `string \| null` | `"x402"` |
| `paymentNetwork` | `string \| null` | settlement.network |
| `includeCertifiedDecisionRecord` | `boolean \| null` | For allocate only |
| `riskTolerance` | `string \| null` | Input field |
| `timeframe` | `string \| null` | Input field |
| `jobId` | `string \| null` | Internal job ID |
| `requestInput` | `Record<string,unknown> \| null` | Full normalized input |
| `responseOutput` | `Record<string,unknown> \| null` | Full response (12,000 char cap via `serializeAdminPayload`) |

**State store persistence** (`backend/data/x402-state.json`): per-tool records (`X402ToolRecord`) store `payment.fromAddress`, `payment.transactionHash`, `payment.network`, `payment.verifiedAt`, `chargedAmountUsdc`, `requestBody`, `responseData`, `productId`, `decisionId`.

**Recommended SCE wrapper logging fields**:

| Field | Recommendation |
|---|---|
| `channel` | Add `"x402_sce"` to the `AdminUsageChannel` union — keeps SCE traffic separate from portfolio x402 |
| `endpoint` | `/agent/x402/sce/continuity-mode`, etc. |
| `productId` | `"sce_continuity_mode"`, `"sce_case_relevance"`, `"sce_risk_evaluate"` |
| `walletAddress` | payer address (fromAddress) |
| `chargedAmountUsdc` | SCE product price |
| `transactionHash` | settlement.transaction |
| `paymentMethod` | `"x402"` |
| `paymentNetwork` | settlement.network |
| `requestInput` | Selun-normalized input passed to SCE (redact any sensitive user fields if applicable) |
| `responseOutput` | Full SCE response wrapped in Selun envelope (12k char cap applies automatically) |

**SCE response logging strategy**: log the **full wrapped response** (same pattern as existing tool calls). The 12,000-char automatic truncation in `serializeAdminPayload()` provides a natural safety limit. Do not log raw SCE responses separately. If SCE responses contain PII or sensitive signals, apply field-level redaction before passing to `sendAdminUsageEmail()`.

---

## 6. SCE Client Placement Plan

**No outbound HTTP client library exists** in the backend today. The only outbound `fetch()` calls are to Resend (emails) and to the x402 facilitator (handled by `@x402/core` internals).

### Proposed file

```
backend/src/services/sce-client.ts
```

### Env vars

| Var | Description |
|---|---|
| `SCE_API_BASE_URL` | Base URL for SCE (e.g. `http://localhost:8000` or production URL) |
| `SCE_API_TIMEOUT_MS` | Optional, default `30000` (30 seconds) |
| `SCE_API_SECRET` | Optional HMAC or bearer token if SCE requires auth |

### Timeout handling

Use `AbortController` + `fetch(url, { signal: controller.signal })`:
```typescript
const controller = new AbortController();
const timer = setTimeout(() => controller.abort(), getSceTimeoutMs());
try {
  const res = await fetch(url, { ..., signal: controller.signal });
  ...
} finally {
  clearTimeout(timer);
}
```

### Failure behavior — **fail closed**

If SCE is unavailable or returns a non-2xx after payment was already settled:
- Return **502** to the caller with a structured error: `{ success: false, error: "sce_upstream_unavailable", ... }`.
- Include the payment receipt in the error body so the buyer can prove payment and request a refund.
- Do **not** silently return a partial result — that would mislead agents.
- Record the usage event with `responseOutput: null` / error message for audit.

### Tests needed

- Unit tests for `callSce*()` functions using mocked `fetch` — verify correct base URL, headers, body forwarding, timeout behavior, error mapping.
- Integration smoke test in scripts (extend existing `x402:smoke:*` pattern).

---

## 7. Planned SCE Wrapper Endpoint Placement

### 7a. `POST /agent/x402/sce/continuity-mode`

| Field | Value |
|---|---|
| Selun route | `router.post("/x402/sce/continuity-mode", ...)` mounted under `/agent` |
| SCE upstream | `POST /v1/sce/continuity-mode` at `SCE_API_BASE_URL` |
| productId | `"sce_continuity_mode"` |
| Proposed price | `0.005` USDC |
| Input shape | Pass-through of SCE input; `decisionId` added by Selun |
| Output wrapping | `{ success: true, executionModelVersion, data: { status: "completed", endpoint, decisionId, productId, payment: {...}, result: <SCE response body> } }` |
| capabilities entry | New entry in `getX402ToolDefinitions()` array |
| Tests needed | Route integration test with mocked SCE; x402 smoke script |

### 7b. `POST /agent/x402/sce/case-relevance`

| Field | Value |
|---|---|
| Selun route | `router.post("/x402/sce/case-relevance", ...)` |
| SCE upstream | `POST /v1/sce/case-relevance` at `SCE_API_BASE_URL` |
| productId | `"sce_case_relevance"` |
| Proposed price | `0.05` USDC |
| Input shape | Pass-through of SCE input; `decisionId` added by Selun |
| Output wrapping | Same tool envelope as above |
| capabilities entry | New entry in `getX402ToolDefinitions()` array |
| Tests needed | Same pattern |

### 7c. `POST /agent/x402/sce/risk-evaluate`

| Field | Value |
|---|---|
| Selun route | `router.post("/x402/sce/risk-evaluate", ...)` |
| SCE upstream | `POST /v1/sce/risk/evaluate` at `SCE_API_BASE_URL` |
| productId | `"sce_risk_evaluate"` |
| Proposed price | `0.25` USDC |
| Input shape | Pass-through of SCE input; `decisionId` added by Selun |
| Output wrapping | Same tool envelope as above |
| capabilities entry | New entry in `getX402ToolDefinitions()` array |
| Tests needed | Same pattern |

### 7d. Common code changes required (Step 1 scope)

| File | Change |
|---|---|
| `backend/src/services/x402-state.types.ts` | Extend `X402ToolProductId` union: `\| "sce_continuity_mode" \| "sce_case_relevance" \| "sce_risk_evaluate"` |
| `backend/src/config.ts` | Add `x402SceContinuityModePriceUsdc`, `x402SceCaseRelevancePriceUsdc`, `x402SceRiskEvaluatePriceUsdc` to config type and parser with env vars `X402_SCE_CONTINUITY_MODE_PRICE_USDC`, etc. |
| `backend/src/routes/agent.routes.ts` | Extend `getX402ToolPriceUsdc()` switch; add 3 entries to `getX402ToolDefinitions()`; add 3 route handlers calling `handleX402ToolRequest()` with a SCE-calling execute function |
| `backend/src/services/email.service.ts` | Add `"x402_sce"` to `AdminUsageChannel` |
| `backend/src/services/sce-client.ts` | New file — SCE API client |

### 7e. Capabilities entry changes

No changes to `buildX402CapabilitiesData()` itself — the dynamic tool resource builder auto-includes any entry added to `getX402ToolDefinitions()`. Only `getX402ToolDefinitions()` and the server-level discovery `X402_SERVER_METADATA.discoveryTags` need updating (add `"sce"`, `"active-intelligence"`).

---

## 8. Risk Register

| # | Risk | Impact | Mitigation |
|---|---|---|---|
| 1 | **Breaking existing x402 endpoints** | High — existing buyers stop working | All existing routes remain untouched; new routes use new paths `/x402/sce/*`. No changes to existing `handleX402AllocateRequest` or `handleX402ToolRequest` functions. |
| 2 | **Breaking capabilities JSON shape** | Medium — Bazaar crawlers may fail | The capabilities builder is additive; new resources append to the `resources` array. Shape does not change. Verify with `GET /agent/x402/capabilities` after deploy. |
| 3 | **Price precision for `0.005` USDC** | Low — technical risk only | `viem.parseUnits("0.005", 6) = 5000n` is exact. `Number("0.005").toFixed(6) = "0.005000"` → `Number("0.005000").toString() = "0.005"` — safe. No floating-point loss at this magnitude. |
| 4 | **Payment requirement mismatch** | High — buyer payment rejected | The `inputFingerprint` is bound to the SCE-bound payload. Any normalization of the SCE input before fingerprinting must be deterministic and consistent across challenge and re-submit. |
| 5 | **SCE upstream downtime** | High — paid buyer gets 502 | Fail-closed strategy: return 502 with payment receipt in error body. Include refund instructions in error message. Log for admin follow-up. |
| 6 | **Logging sensitive SCE output** | Medium — PII or alpha signals | Audit SCE response schemas before enabling full response logging. Apply field-level redaction or summary logging for sensitive fields if needed. |
| 7 | **SCE response shape instability** | Medium — Selun response shape breaks buyers | Wrap SCE response opaquely in `result` field. Buyers receive `data.result` as-is. Selun does not parse or re-shape SCE output. |
| 8 | **Bazaar discovery metadata** | Low — discovery quality degraded | Provide accurate `inputSchema` and `exampleOutput` in each `X402ToolDefinition`. Discovery tags must not overlap misleadingly with portfolio tags. |
| 9 | **`X402ToolProductId` type widening** | Low — TypeScript compile check | Extending the union is additive. Existing switch statements in `getX402ToolPriceUsdc()` and `executeToolProduct()` will get TS exhaustiveness errors for unmapped cases — which is the desired compile-time guard. |
| 10 | **Multi-network expansion (Base/Solana later)** | Low now — architectural debt | All payment logic is EVM/Base-specific. SCE wrappers should document this constraint explicitly. No Solana support is blocked by SCE integration. |
| 11 | **Referral attribution on SCE tools** | Low — revenue leak | Current referral code is only hooked into allocate routes. Tool routes (including future SCE tools) do not record referrals. This is consistent with existing tool behaviour — acceptable for Step 1. |
| 12 | **Global concurrency cap contention** | Low — SCE tools share cap with allocations | `runningAllocateOrchestration` size is checked against `getX402GlobalConcurrencyCap()` (default 8). If allocations are running, SCE requests may be rate-limited. Consider a separate concurrency counter for SCE if needed. |

---

## 9. Recommended Step 1 Prompt

> **Scope**: SCE upstream client + capabilities entries only, no live routes.

---

```
Step 1 — SCE Client + Capabilities Entries

Context: Selun is a Next.js + Express TypeScript backend. All x402 route logic
is in backend/src/routes/agent.routes.ts (Express router). Capabilities JSON is
built by buildX402CapabilitiesData() in the same file. The STEP0 repo map is
in SELUN_SCE_WRAPPER_STEP0_REPO_MAP.md.

Task: implement only these two things:

1. New file: backend/src/services/sce-client.ts
   - Export three async functions:
       callSceContinuityMode(body: Record<string, unknown>): Promise<Record<string, unknown>>
       callSceCaseRelevance(body: Record<string, unknown>): Promise<Record<string, unknown>>
       callSceRiskEvaluate(body: Record<string, unknown>): Promise<Record<string, unknown>>
   - Reads SCE_API_BASE_URL from process.env (required, throw if missing).
   - Reads SCE_API_TIMEOUT_MS from process.env (optional, default 30000).
   - Uses native fetch() with AbortController for timeout.
   - On non-2xx response, throws an Error with status code + body excerpt.
   - On network error, re-throws with a clear prefix.
   - Each function maps to:
       continuity-mode → POST /v1/sce/continuity-mode
       case-relevance  → POST /v1/sce/case-relevance
       risk-evaluate   → POST /v1/sce/risk/evaluate

2. Extend capabilities catalog (no live routes yet):
   a. In backend/src/services/x402-state.types.ts:
      Add to X402ToolProductId union:
        | "sce_continuity_mode" | "sce_case_relevance" | "sce_risk_evaluate"
   b. In backend/src/config.ts:
      Add three config fields to SelunBackendConfig:
        x402SceContinuityModePriceUsdc: number   // env X402_SCE_CONTINUITY_MODE_PRICE_USDC, default 0.005
        x402SceCaseRelevancePriceUsdc: number    // env X402_SCE_CASE_RELEVANCE_PRICE_USDC, default 0.05
        x402SceRiskEvaluatePriceUsdc: number     // env X402_SCE_RISK_EVALUATE_PRICE_USDC, default 0.25
      Parse them with parseNonNegativeFloat() alongside the existing tool prices.
   c. In backend/src/routes/agent.routes.ts:
      Extend getX402ToolPriceUsdc() switch to handle the three new productIds.
      Add three entries to the X402ToolDefinition[] returned by getX402ToolDefinitions():
        - sce_continuity_mode → routePath "/agent/x402/sce/continuity-mode"
          title "Selun SCE Continuity Mode"
          category "intelligence:continuity-mode"
          tags ["sce", "continuity-mode", "active-intelligence", "scenario", "x402"]
          inputSchema: { type: "object", properties: { decisionId: { type: "string" } }, additionalProperties: true, required: ["decisionId"] }
          exampleInput/exampleOutput: placeholder values
        - sce_case_relevance → routePath "/agent/x402/sce/case-relevance"
          title "Selun SCE Case Relevance"
          category "intelligence:case-relevance"
          tags ["sce", "case-relevance", "active-intelligence", "classification", "x402"]
          same inputSchema pattern
        - sce_risk_evaluate → routePath "/agent/x402/sce/risk-evaluate"
          title "Selun SCE Risk Evaluate"
          category "intelligence:risk-evaluate"
          tags ["sce", "risk-evaluate", "active-intelligence", "risk-scoring", "x402"]
          same inputSchema pattern
      Add "sce" and "active-intelligence" to X402_SERVER_METADATA.discoveryTags.
      Add "x402_sce" to AdminUsageChannel union in email.service.ts.

3. Write unit tests for the SCE client in backend/src/services/sce-client.test.ts:
   - Test: correct URL construction for each of the three endpoints
   - Test: timeout AbortController fires on slow response
   - Test: non-2xx response throws with status code
   - Test: 2xx response returns parsed JSON

Do NOT implement live route handlers in this step. Routes come in Step 2.
Do NOT change X402_MARKET_REGIME_PRICE_USDC or any existing prices.
Run npm run backend:test after to confirm all 13 existing tests still pass.
```

---

*End of Step 0 report. No code was modified during this orientation.*
