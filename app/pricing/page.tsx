import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "Pricing | Selun AI Crypto Allocation Agent",
  description:
    "Selun endpoint pricing: Allocation ($19), Allocation with Report ($34), Market Regime ($0.25), Policy Envelope ($0.25), Asset Scorecard ($0.50), Rebalance ($1.00), SCE Continuity Mode ($0.01), SCE Case Relevance ($0.05), SCE Risk Evaluate ($0.25). Pay per call in USDC on Base.",
  alternates: { canonical: "/pricing" },
  openGraph: {
    title: "Pricing | Selun",
    description:
      "Pay per call in USDC on Base. No subscription. No API key. Nine endpoints priced by capability.",
    url: "/pricing",
  },
};

const PRICING = [
  {
    id: "allocation",
    title: "Allocation",
    price: "$19.00",
    amount: "19",
    kind: "Async",
    isAsync: true,
    endpoint: "/agent/x402/allocate",
    description:
      "Full allocation engine. Returns target portfolio weights for your risk profile and timeframe. Runs regime classification, policy scoping, and asset scoring internally.",
    whenToUse: "When you need a fresh target allocation without a documented record.",
  },
  {
    id: "allocation-with-report",
    title: "Allocation with Report",
    price: "$34.00",
    amount: "34",
    kind: "Async",
    isAsync: true,
    endpoint: "/agent/x402/allocate-with-report",
    description:
      "Same as Allocation, plus a certified PDF decision record capturing regime snapshot, policy constraints, asset scores, and rationale.",
    whenToUse: "When you need a documented, shareable record of the allocation decision — for governance, stakeholders, or audit.",
  },
  {
    id: "market-regime",
    title: "Market Regime",
    price: "$0.25",
    amount: "0.25",
    kind: "Sync",
    isAsync: false,
    endpoint: "/agent/x402/market-regime",
    description:
      "Classifies the current market environment: volatility, liquidity, and sentiment. Returns regime signals that inform allocation and rebalancing.",
    whenToUse: "Before an allocation, as a standalone market monitor, or as an input to your own decision logic.",
  },
  {
    id: "policy-envelope",
    title: "Policy Envelope",
    price: "$0.25",
    amount: "0.25",
    kind: "Sync",
    isAsync: false,
    endpoint: "/agent/x402/policy-envelope",
    description:
      "Computes exposure caps, stablecoin floor, and risk budget for a given risk profile and timeframe. These are the hard constraints any allocation must satisfy.",
    whenToUse: "When you want explicit policy constraints before running an allocation, or for governance workflows that validate policy parameters independently.",
  },
  {
    id: "asset-scorecard",
    title: "Asset Scorecard",
    price: "$0.50",
    amount: "0.50",
    kind: "Sync",
    isAsync: false,
    endpoint: "/agent/x402/asset-scorecard",
    description:
      "Scores candidate assets on liquidity, structural stability, role classification, and composite quality. Helps filter and rank assets before allocation.",
    whenToUse: "Before building a target allocation or when screening a set of assets for quality.",
  },
  {
    id: "rebalance",
    title: "Rebalance",
    price: "$1.00",
    amount: "1",
    kind: "Sync",
    isAsync: false,
    endpoint: "/agent/x402/rebalance",
    description:
      "Takes your current holdings and computes the drift vs optimal target allocation. Returns the trade instructions needed to rebalance within policy.",
    whenToUse: "After an initial allocation when holdings have drifted, or on a regular monitoring schedule.",
  },
  {
    id: "sce-continuity-mode",
    title: "SCE Continuity Mode",
    price: "$0.01",
    amount: "0.01",
    kind: "Sync",
    isAsync: false,
    endpoint: "/agent/x402/sce/continuity-mode",
    description:
      "Pre-execution safety check. Returns the current on-chain threat posture: mode (NORMAL / INCIDENT / DEGRADED), recommended_posture, reason_codes, and per-domain risk signals from the Sagitta Continuity Engine.",
    whenToUse: "Before any protocol interaction, capital movement, or rebalance. The default first call in the SCE preflight policy.",
  },
  {
    id: "sce-case-relevance",
    title: "SCE Case Relevance",
    price: "$0.05",
    amount: "0.05",
    kind: "Sync",
    isAsync: false,
    endpoint: "/agent/x402/sce/case-relevance",
    description:
      "Classifies whether a specific protocol, asset type, or threat family is relevant to active SCE intelligence. Returns relevance_score, relevance_level, matched threat families, and matched doctrine tags.",
    whenToUse: "When you have threat context to evaluate — specific threat families, doctrine tags, or a protocol name. Not a default preflight; use after continuity-mode when you have targeted context.",
  },
  {
    id: "sce-risk-evaluate",
    title: "SCE Risk Evaluate",
    price: "$0.25",
    amount: "0.25",
    kind: "Sync",
    isAsync: false,
    endpoint: "/agent/x402/sce/risk-evaluate",
    description:
      "Protocol risk gate. Returns doctrine_action (BLOCK / ALLOW / ESCALATE), risk_level, risk_score, and per-domain risk breakdown. If doctrine_action is BLOCK, halt execution and surface reason_codes to the user.",
    whenToUse: "When continuity-mode returns mode !== NORMAL or recommended_posture is require_review or pause. The action gate in the SCE preflight policy.",
  },
];

export default function PricingPage() {
  const currentYear = new Date().getFullYear();
  return (
    <main className={styles.page}>
      <article className={styles.shell}>
        <header className={styles.header}>
          <Link href="/" className={styles.brand}>
            <Image
              src="/selun-mark.svg"
              alt="Selun"
              width={32}
              height={32}
              className={styles.brandMark}
            />
            <span>Selun</span>
          </Link>
          <nav className={styles.headerNav} aria-label="Quick nav">
            <Link href="/capabilities" className={styles.navLink}>Capabilities</Link>
            <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.navLink}>Compare</Link>
            <Link href="/x402" className={styles.navLink}>x402 Catalog</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <span>Pricing</span>
          </nav>
          <p className={styles.eyebrow}>Endpoint Pricing</p>
          <h1 className={styles.pageTitle}>Pricing</h1>
          <p className={styles.pageSubhead}>
            Selun uses pay-per-call pricing. No subscription, no API key, no upfront commitment.
            Each call is a discrete micropayment in USDC on the Base network. Prices are set
            per capability.
          </p>
        </section>

        <div className={styles.content}>

          {/* Payment model */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>How payment works</h2>
            <p className={styles.sectionBody}>
              x402 endpoint calls are paid in <strong>USDC on Base</strong> (eip155:8453). When
              you call an endpoint without payment, you receive HTTP 402 with payment
              requirements. Your x402-compatible client sends the payment and retries. Selun
              verifies via the Coinbase CDP facilitator and returns the result. No account, no
              API key, no recurring charge.
            </p>
            <p className={styles.sectionBody}>
              The allocation wizard accepts <strong>card payment via Stripe</strong>. No wallet
              required for wizard users.
            </p>
          </section>

          {/* Async vs sync */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Async vs synchronous</h2>
            <p className={styles.sectionBody}>
              <strong>Allocation endpoints are asynchronous.</strong> You submit the request and
              receive a job ID and status path. Poll the status path until the result is ready.
              This allows the allocation engine to run a thorough multi-step analysis without
              blocking your HTTP request.
            </p>
            <p className={styles.sectionBody}>
              <strong>Tool endpoints are synchronous.</strong> Market Regime, Policy Envelope,
              Asset Scorecard, and Rebalance all return their result immediately in the same
              HTTP response.
            </p>
          </section>

          {/* Pricing cards */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Endpoint prices</h2>
            <div className={styles.pricingGrid}>
              {PRICING.map((item) => (
                <div key={item.id} className={styles.pricingCard}>
                  <div className={styles.pricingHead}>
                    <h3 className={styles.pricingTitle}>{item.title}</h3>
                    <div>
                      <div className={styles.pricingAmount}>{item.price}</div>
                      <div className={styles.pricingCurrency}>USDC</div>
                    </div>
                  </div>
                  <p className={styles.pricingBody}>{item.description}</p>
                  <div style={{ marginTop: "auto" }}>
                    <p style={{ fontSize: 12, color: "var(--ink-soft)", marginBottom: 6 }}>
                      <strong style={{ color: "var(--ink-main)" }}>When to use:</strong>{" "}
                      {item.whenToUse}
                    </p>
                    <span className={`${styles.kindBadge} ${item.isAsync ? styles.kindBadgeAsync : ""}`}>
                      {item.kind}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </section>

          {/* Why separate prices */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Why are capabilities priced separately?</h2>
            <p className={styles.sectionBody}>
              Each capability represents a discrete computation. Market Regime, Policy Envelope,
              and Asset Scorecard are inexpensive synchronous lookups — useful on their own or
              as components in a larger workflow. The full Allocation runs all of them together
              and adds an allocation engine on top. Separate pricing lets you pay only for what
              your workflow actually needs.
            </p>
            <p className={styles.sectionBody}>
              If you are building an agent pipeline, you might call Market Regime frequently
              (low cost), run a full Allocation less often, and only request a Report when
              a decision needs documentation.
            </p>
          </section>

          {/* What report adds */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What the $15 report premium adds</h2>
            <p className={styles.sectionBody}>
              The Allocation with Report ($34) costs $15 more than the standard Allocation ($19).
              That premium covers the generation of a certified PDF decision record — the regime
              snapshot, policy envelope, asset scores, decision rationale, timestamp, and
              decision ID — in a shareable, archivable format. If you do not need documentation,
              the standard allocation is the right call.
            </p>
            <p>
              <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.linkPill}>
                Full comparison: Allocation vs Report →
              </Link>
            </p>
          </section>

          {/* Referral program */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Earn back through the referral program</h2>
            <p className={styles.sectionBody}>
              Selun has an agent referral program. If you refer users who complete an allocation,
              you earn <strong>50% of the allocation revenue</strong> — $9.50 per $19 allocation,
              paid in USDC on Base. Referrals work for both human users (via referral link) and
              AI agents (via the agent spec). Payouts go to your wallet on the 1st and 15th of
              each month.
            </p>
            <p style={{ marginTop: 12 }}>
              <Link href="/earn" className={styles.linkPill}>
                Join the Referral Program →
              </Link>
            </p>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Compare &amp; explore</span>
            <Link href="/compare/allocation-vs-rebalance" className={styles.linkPill}>Allocation vs Rebalance</Link>
            <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.linkPill}>Allocation vs Report</Link>
            <Link href="/capabilities" className={styles.linkPill}>All Capabilities</Link>
            <Link href="/for-developers" className={styles.linkPill}>For Developers</Link>
            <Link href="/x402" className={styles.linkPill}>x402 Endpoint Catalog</Link>
          </div>

          <footer className={styles.footer}>
            <p>© {currentYear} Sagitta Labs</p>
            <nav className={styles.footerNav} aria-label="Site links">
              <Link href="/">Home</Link>
              <span aria-hidden>|</span>
              <Link href="/what-is-selun">What Is Selun</Link>
              <span aria-hidden>|</span>
              <Link href="/capabilities">Capabilities</Link>
              <span aria-hidden>|</span>
              <Link href="/faq">FAQ</Link>
              <span aria-hidden>|</span>
              <Link href="/security">Security</Link>
              <span aria-hidden>|</span>
              <Link href="/for-developers">For Developers</Link>
              <span aria-hidden>|</span>
              <Link href="/x402">x402 API</Link>
              <span aria-hidden>|</span>
              <Link href="/earn">Referral Program</Link>
              <span aria-hidden>|</span>
              <Link href="/terms">Terms</Link>
              <span aria-hidden>|</span>
              <Link href="/privacy">Privacy</Link>
              <span aria-hidden>|</span>
              <Link href="/support">Support</Link>
            </nav>
          </footer>
        </div>
      </article>
    </main>
  );
}
