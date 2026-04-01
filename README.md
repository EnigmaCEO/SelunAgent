# Selun

**Selun** is an autonomous portfolio intelligence agent powered by **Sagitta AAA**, exposing decision signals through **x402 pay-per-call endpoints**.

Agents can query Selun to obtain **market regime intelligence** before making allocation, trading, or yield decisions.

Instead of subscriptions, API keys, or accounts, Selun uses **x402 payments** so agents can buy intelligence **on demand**.

---

# Why Selun Exists

AI agents are becoming autonomous economic actors.

They book services.  
They allocate capital.  
They interact with onchain protocols.

But most APIs are still built for humans:

- API keys  
- subscriptions  
- centralized access control  

Selun uses **x402** so agents can:

- discover endpoints
- pay per call
- receive structured intelligence

This turns decision signals into **agent-native microservices**.

---

# What Selun Provides

Selun exposes **market intelligence primitives** useful for autonomous portfolio agents.

## Market Regime Classification

Agents can query:

- volatility conditions
- liquidity environment
- sentiment alignment

The response can be used as a **pre-flight check** before portfolio allocations or rebalancing.

Example use cases:

- prevent risky allocations during high-chaos regimes
- authorize bluechip allocations during stable conditions
- deny speculative segments when volatility spikes

---

# Example Agent Workflow

Typical agent decision loop:

1. Query market regime  
2. Evaluate allocation strategy  
3. Execute portfolio action  

Selun sits at **Step 1**.


Agent
↓
Query Selun Regime Signal
↓
Receive Market Classification
↓
Authorize / Deny Allocation
↓
Execute Portfolio Action


---

# Endpoint


/agent/x402/market-regime


This endpoint returns structured market intelligence used by agents to evaluate risk conditions.

**Pricing**


0.25 USDC per query


Payment handled via **x402**.

No API keys required.

---

# Example Call

Example using AgentCash / x402:

```bash
npx agentcash try https://selun.sagitta.systems/agent/x402/market-regime

Agents pay automatically via USDC and receive the response payload.

Example Response
{
  "regime": "stable",
  "volatility": "low",
  "liquidity": "high",
  "sentiment": "positive",
  "allocation_authorization": {
    "bluechips": "authorized",
    "yield": "conditional",
    "gaming": "speculative",
    "memes": "denied"
  }
}
Architecture

Selun runs on top of Sagitta AAA, a quantitative allocation engine designed to produce structured decision outputs for autonomous agents.

Selun Agent
    ↓
Sagitta AAA Allocator
    ↓
Market Data Signals
    ↓
Structured Intelligence Output

Selun focuses on signal generation.

Execution remains agent-controlled.

Design Principles

Selun follows three principles:

Agent-Native Access

Endpoints are designed for agents, not humans.

Pay-Per-Intelligence

Signals are purchased when needed.

Structured Decisions

Outputs are deterministic JSON inputs agents can consume programmatically.

Roadmap

Planned endpoints:

asset scoring

allocation proposals

portfolio rebalance intelligence

strategy simulation

decision certification

Selun is intended to become a library of decision primitives for autonomous financial agents.

Relationship to Sagitta

Selun is part of the Sagitta ecosystem.

Sagitta provides the underlying Autonomous Allocation Agent (AAA).

Selun exposes selected intelligence outputs through agent-accessible endpoints.

License

MIT
