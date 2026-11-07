# Capital Flow Diagram: text version

Approved by the owner on 2026-11-06. Selun answers from this text, held in `backend/src/assistant/sagitta-capital-flow.ts`, so change the two together. Not yet published on GitBook.

Capital in the Sagitta Protocol moves through separate layers. Each layer has its own authority, and no layer controls another's. This page describes the design. It doesn't describe a live product or state any legal conclusion.

## Normal flow

**1. Depositors and the Vault.** A depositor contributes capital to a Vault instance. The Vault is the accounting authority and the source of truth for balances and ownership claims. It alone can change a balance. Its record of principal doesn't depend on what happens to allocation or execution. An ownership claim can be represented by an NFT deposit receipt, and the receipt is burned when an unlocked position is withdrawn.

**2. The Treasury forms a batch.** The Treasury observes the total value of eligible Vault deposits and allocates a matching amount of Stability Units into a closed allocation batch, weekly by default. Forming a batch doesn't move a depositor's deposit into an investment, and it doesn't change the depositor's claim. Batch size is limited by Reserve coverage.

**3. The Autonomous Allocation Agent informs.** The agent evaluates strategies and supplies simulations, risk analysis, and performance comparisons. It has no control over capital. The Treasury decides what capital is committed.

**4. The Escrow executes.** Under Treasury authorization, the Escrow deploys each batch in its own isolated context and temporarily holds the capital in motion. It doesn't set policy, evaluate strategy, or decide risk.

**5. External venues bear the risk.** Capital is deployed to approved on-chain and off-chain venues, which are the risk-bearing environment.

**6. The Escrow returns outcomes.** At the end of a batch, the Escrow reconciles positions, converts outcomes into Stability Units, prepares settlement reports, and returns assets to the Treasury.

**7. The Treasury settles.** The Treasury evaluates the outcome, buys back and burns Treasury Tokens to finalize the batch, and authorizes any yield. Yield may be positive or zero. The Vault records it against eligible deposits, and recorded principal is unchanged.

## The Reserve

The Reserve is the protocol-level protection layer. It sits outside allocation logic and outside execution paths, and its assets are never deployed for yield. It constrains the system rather than funding it:

- **It limits growth.** The Treasury targets 2:1 Reserve collateralization against net principal exposure, and deposits, batch sizes, and liquidity scale with Reserve coverage.
- **It's measured against each batch.** The Treasury records a snapshot of Reserve value when a batch forms and uses it for reserve-relative settlement.
- **It absorbs losses by rule.** When a batch underperforms, the Escrow reports the outcome, the Treasury evaluates settlement, and losses are absorbed through Treasury-retained capital and then controlled liquidation of Reserve assets.
- **It restores principal if the Vault fails.** Balances are rebuilt from the last valid snapshots and principal is restored through Reserve-backed settlement.

**The Escrow doesn't access Reserve assets.** Nothing passes directly between them.

"Insurance" here means this protocol-level reserve mechanism. It isn't an insurance contract and isn't guaranteed by any legal entity. See Scope & Non-Banking Clarification.

## The Continuity Engine

The Continuity Engine is an overlay authority across every subsystem. It takes no part in routine operations, and it governs how the protocol behaves when normal assumptions fail.

**Evacuation is its authority alone.** The Allocation Agent, Treasury, and Escrow don't decide it. When evacuation is triggered, the Continuity Engine coordinates:

1. halting new allocations
2. suspending execution
3. recalling deployed capital from the Escrow and external venues
4. consolidating assets into Stability Units or Reserve form
5. freezing non-essential state changes

Reconstitution follows, and normal operation resumes only after solvency and protection thresholds are met. Depositor balances remain invariant throughout.

Governance sets continuity thresholds, substitution candidates, and evacuation permissions in advance. Once those conditions are met, the Continuity Engine acts without discretionary delay.

## What this flow doesn't mean

- Vault deposits aren't automatically transferred into Treasury investments.
- The Treasury doesn't gain ownership of deposits or authority over Vault balances.
- The Escrow has no access to the Reserve, and the Reserve doesn't fund Escrow execution.
- The Allocation Agent doesn't move capital or trigger evacuation.
- This flow doesn't make the Sagitta Protocol a bank, trust company, or custodial service provider.
