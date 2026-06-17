import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "Capabilities | Selun AI Crypto Allocation Agent",
  description:
    "All nine Selun capabilities: Allocation, Allocation with Report, Market Regime, Policy Envelope, Asset Scorecard, Rebalance, SCE Continuity Mode, SCE Case Relevance, and SCE Risk Evaluate. What each does, when to use it, and what it returns.",
  alternates: { canonical: "/capabilities" },
  openGraph: {
    title: "Capabilities | Selun AI Crypto Allocation Agent",
    description:
      "All nine Selun capabilities: Allocation, Allocation with Report, Market Regime, Policy Envelope, Asset Scorecard, Rebalance, and three SCE preflight safety endpoints.",
    url: "/capabilities",
  },
};

const CAPABILITIES = [
  {
    id: "allocation",
    eyebrow: "$19.00 USDC · Async",
    title: "Allocation",
    endpoint: "/agent/x402/allocate",
    what: "Generates a target portfolio allocation based on your risk tolerance, investment timeframe, and current market conditions. The engine runs market regime classification, policy scoping, and asset scoring internally before producing the final weights.",
    when: "Use this when you need a fresh target allocation and do not need a documented record of the decision. It is the primary allocation endpoint for automated workflows.",
    output: "A structured allocation with target weights per asset, within policy constraints. Returned asynchronously — poll the status path for the result.",
    audience: "Crypto investors, treasury operators, agent-based workflows needing target weights.",
    isAsync: true,
  },
  {
    id: "allocation-with-report",
    eyebrow: "$34.00 USDC · Async",
    title: "Allocation with Report",
    endpoint: "/agent/x402/allocate-with-report",
    what: "Same as Allocation, but also produces a certified decision report — a structured PDF record capturing the regime snapshot, policy constraints, asset scores, and decision rationale at the time of the call.",
    when: "Use this when the allocation will be presented to stakeholders, used in a governance vote, stored for audit purposes, or when the decision-maker needs a verifiable, time-stamped record.",
    output: "Allocation result plus a signed, downloadable decision report. Returned asynchronously.",
    audience: "DAOs, treasury committees, institutional operators, any context requiring documented allocation decisions.",
    isAsync: true,
  },
  {
    id: "market-regime",
    eyebrow: "$0.25 USDC · Sync",
    title: "Market Regime",
    endpoint: "/agent/x402/market-regime",
    what: "Classifies the current crypto market environment across volatility, liquidity, and sentiment dimensions. Returns regime-level signals that inform downstream allocation and rebalancing decisions.",
    when: "Use this before an allocation or rebalancing call when you want explicit regime context, or as a standalone signal for your own decision logic. Also useful for monitoring: call it on a schedule to track how market conditions are shifting.",
    output: "Regime classification with volatility level, liquidity score, sentiment signal, and authorization inputs for allocation.",
    audience: "Quant workflows, monitoring systems, any agent that needs market-state awareness before making a portfolio decision.",
    isAsync: false,
  },
  {
    id: "policy-envelope",
    eyebrow: "$0.25 USDC · Sync",
    title: "Policy Envelope",
    endpoint: "/agent/x402/policy-envelope",
    what: "Computes the risk policy envelope for a given risk tolerance and timeframe. Returns the maximum exposure caps by asset class, the minimum stablecoin floor, and the risk budget — the hard constraints any compliant allocation must satisfy.",
    when: "Use this when you want to know what an allocation for a given risk profile is allowed to look like before running a full allocation. Also useful for governance workflows that need to confirm policy parameters independently.",
    output: "Exposure caps per asset class, stablecoin floor percentage, risk budget, and constraint summary.",
    audience: "Developers building allocation logic, DAOs that want to validate policy constraints, risk-aware treasury operators.",
    isAsync: false,
  },
  {
    id: "asset-scorecard",
    eyebrow: "$0.50 USDC · Sync",
    title: "Asset Scorecard",
    endpoint: "/agent/x402/asset-scorecard",
    what: "Evaluates a list of candidate assets across liquidity, structural stability, role classification (store of value, yield, speculative), and a composite quality score. Lower-quality assets receive lower scores and are less likely to receive meaningful allocation weights.",
    when: "Use this before an allocation to understand asset quality, or as a standalone screening tool when selecting which assets to include in a portfolio strategy.",
    output: "Per-asset scores for liquidity, structural stability, role classification, and a composite quality score.",
    audience: "Investors building a custom asset list, developers selecting inputs for allocation, anyone who needs objective scoring before committing to an asset set.",
    isAsync: false,
  },
  {
    id: "rebalance",
    eyebrow: "$1.00 USDC · Sync",
    title: "Rebalance",
    endpoint: "/agent/x402/rebalance",
    what: "Takes your current holdings and a risk profile, then computes the drift between your current portfolio and the optimal target allocation. Returns the adjustments — buy, sell, or hold — needed to bring the portfolio back within policy.",
    when: "Use this on a monitoring schedule (daily, weekly) after an initial allocation to detect drift and produce actionable rebalancing instructions. Also use it after market conditions shift significantly.",
    output: "Drift analysis per asset and a set of rebalancing trade instructions ordered by priority.",
    audience: "Investors with existing holdings who want to maintain policy alignment, automated rebalancing bots, treasury operators running regular portfolio reviews.",
    isAsync: false,
  },
  {
    id: "sce-continuity-mode",
    eyebrow: "$0.005 USDC · Sync",
    title: "SCE Continuity Mode",
    endpoint: "/agent/x402/sce/continuity-mode",
    what: "Queries the Sagitta Continuity Engine for the current on-chain threat posture. Returns mode (NORMAL / INCIDENT / DEGRADED), recommended_posture, reason_codes, and per-domain risk_domains across authority, governance, oracle, bridge, dependency, and more.",
    when: "Use this as the default pre-execution check before any protocol interaction, routing decision, rebalance, or capital movement. If mode is not NORMAL, escalate to SCE Risk Evaluate before proceeding.",
    output: "mode, recommended_posture, reason_codes, risk_domains, confidence, and recommended_next — including which Selun endpoint to call next and its estimated price.",
    audience: "Any autonomous agent that moves capital on-chain. Designed to be called on every execution cycle as a lightweight safety gate.",
    isAsync: false,
  },
  {
    id: "sce-risk-evaluate",
    eyebrow: "$0.25 USDC · Sync",
    title: "SCE Risk Evaluate",
    endpoint: "/agent/x402/sce/risk-evaluate",
    what: "Scores and classifies protocol risk for a specific protocol or address set. Returns doctrine_action (BLOCK / ALLOW / ESCALATE), risk_level, risk_score, risk_domains, and reason_codes from the Sagitta Continuity Engine's composite evaluation.",
    when: "Use this when SCE Continuity Mode returns mode !== NORMAL or recommended_posture is require_review or pause. If doctrine_action is BLOCK, halt the action and surface reason_codes and recommended_next to the user. Recheck after valid_until.",
    output: "doctrine_action, risk_level, risk_score, risk_domains, reason_codes, continuity_mode summary, and recommended_next.",
    audience: "Agents executing protocol interactions, swaps, deposits, or capital movements that require a doctrine-gated go/no-go decision.",
    isAsync: false,
  },
  {
    id: "sce-case-relevance",
    eyebrow: "$0.05 USDC · Sync",
    title: "SCE Case Relevance",
    endpoint: "/agent/x402/sce/case-relevance",
    what: "Classifies whether a specific protocol, asset type, or threat family is relevant to the active SCE intelligence context. Returns relevance_score, relevance_level, matched_threat_families, matched_doctrine_tags, and reason_codes.",
    when: "Use this only when you already have threat context to evaluate — specific threat families, doctrine tags, asset categories, or a protocol name. Not a default preflight check; best used when screening case-library candidates or filtering execution paths with known risk context.",
    output: "relevance_score, relevance_level (NONE / LOW / MEDIUM / HIGH), matched_threat_families, matched_doctrine_tags, and reason_codes.",
    audience: "Agents with prior threat context that need to determine whether a specific protocol or asset class is implicated in active SCE cases.",
    isAsync: false,
  },
];

export default function CapabilitiesPage() {
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
            <Link href="/how-it-works" className={styles.navLink}>How it Works</Link>
            <Link href="/pricing" className={styles.navLink}>Pricing</Link>
            <Link href="/x402" className={styles.navLink}>x402 Catalog</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <span>Capabilities</span>
          </nav>
          <p className={styles.eyebrow}>Endpoint Capabilities</p>
          <h1 className={styles.pageTitle}>Selun Capabilities</h1>
          <p className={styles.pageSubhead}>
            Selun exposes nine capabilities as payment-gated endpoints. Two are asynchronous
            allocation engines. Seven are synchronous tool endpoints that return immediately.
            The three SCE endpoints are preflight safety checks powered by the Sagitta Continuity Engine.
            Each can be called independently via x402 or through the guided allocation wizard.
          </p>
        </section>

        <div className={styles.content}>
          {CAPABILITIES.map((cap) => (
            <section key={cap.id} id={cap.id} className={styles.section}>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>{cap.eyebrow}</p>
                <h2 className={styles.cardTitle} style={{ fontSize: 20 }}>{cap.title}</h2>

                <div className={styles.dataRow}>
                  <span className={styles.dataRowLabel}>Endpoint</span>
                  <span className={styles.dataRowValue}>
                    <code style={{ fontFamily: "monospace", fontSize: 13 }}>{cap.endpoint}</code>
                  </span>
                </div>

                <div className={styles.dataRow}>
                  <span className={styles.dataRowLabel}>What it does</span>
                  <span className={styles.dataRowValue} style={{ maxWidth: "60%", textAlign: "left" }}>
                    {cap.what}
                  </span>
                </div>

                <div className={styles.dataRow}>
                  <span className={styles.dataRowLabel}>When to use it</span>
                  <span className={styles.dataRowValue} style={{ maxWidth: "60%", textAlign: "left" }}>
                    {cap.when}
                  </span>
                </div>

                <div className={styles.dataRow}>
                  <span className={styles.dataRowLabel}>Output</span>
                  <span className={styles.dataRowValue} style={{ maxWidth: "60%", textAlign: "left" }}>
                    {cap.output}
                  </span>
                </div>

                <div className={styles.dataRow}>
                  <span className={styles.dataRowLabel}>Best for</span>
                  <span className={styles.dataRowValue} style={{ maxWidth: "60%", textAlign: "left" }}>
                    {cap.audience}
                  </span>
                </div>
              </div>
            </section>
          ))}

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Machine-facing catalog</h2>
            <p className={styles.sectionBody}>
              This page explains each capability in human terms. If you are building an agent or
              need the full technical spec — input schemas, pricing, discovery root, and
              payment transport details — see the{" "}
              <Link href="/x402" style={{ color: "var(--accent)", fontWeight: 600 }}>
                x402 endpoint catalog
              </Link>
              .
            </p>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Related pages</span>
            <Link href="/how-it-works" className={styles.linkPill}>How it Works</Link>
            <Link href="/decision-report" className={styles.linkPill}>Decision Report</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
            <Link href="/compare/allocation-vs-rebalance" className={styles.linkPill}>Allocation vs Rebalance</Link>
            <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.linkPill}>Allocation vs Report</Link>
            <Link href="/x402" className={styles.linkPill}>x402 Endpoint Catalog</Link>
          </div>

          <footer className={styles.footer}>
            <p>© {currentYear} Sagitta Labs</p>
            <nav className={styles.footerNav} aria-label="Site links">
              <Link href="/">Home</Link>
              <span aria-hidden>|</span>
              <Link href="/what-is-selun">What Is Selun</Link>
              <span aria-hidden>|</span>
              <Link href="/how-it-works">How it Works</Link>
              <span aria-hidden>|</span>
              <Link href="/pricing">Pricing</Link>
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
