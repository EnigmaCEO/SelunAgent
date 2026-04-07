import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "How Selun Works | AI Crypto Allocation Workflow",
  description:
    "See how Selun generates a crypto portfolio allocation: market regime assessment, policy envelope, asset scoring, allocation generation, decision report, and rebalancing.",
  alternates: { canonical: "/how-it-works" },
  openGraph: {
    title: "How Selun Works | AI Crypto Allocation Workflow",
    description:
      "See how Selun generates a crypto portfolio allocation: market regime assessment, policy envelope, asset scoring, allocation generation, decision report, and rebalancing.",
    url: "/how-it-works",
  },
};

const STEPS = [
  {
    title: "Assess market regime",
    body: "Before any allocation is made, Selun classifies the current market environment. It evaluates volatility, liquidity conditions, and market sentiment to determine what kind of market you are operating in. This context shapes every subsequent step — a risk-on regime and a risk-off regime produce very different allocation constraints.",
    link: { href: "/capabilities#market-regime", label: "Market Regime endpoint" },
  },
  {
    title: "Define a policy envelope",
    body: "Given your risk tolerance and the market regime, Selun computes a risk policy envelope: the maximum exposure caps per asset class, the minimum stablecoin floor, and the overall risk budget. This envelope acts as a hard constraint. The final allocation will always stay within it.",
    link: { href: "/capabilities#policy-envelope", label: "Policy Envelope endpoint" },
  },
  {
    title: "Score candidate assets",
    body: "Selun evaluates each candidate asset on liquidity, structural stability, role classification (e.g. store of value, yield, speculative), and a composite quality score. Lower-quality assets are down-weighted or excluded entirely. This step ensures the allocation is built from defensible components.",
    link: { href: "/capabilities#asset-scorecard", label: "Asset Scorecard endpoint" },
  },
  {
    title: "Generate the allocation",
    body: "Using the regime context, the policy envelope, and the asset scores, Selun produces a target portfolio allocation — concrete weights for each asset. This is an asynchronous operation: you submit the request, receive a status path, and poll for the completed result. The output is a structured allocation you can act on.",
    link: { href: "/capabilities#allocation", label: "Allocation endpoint" },
  },
  {
    title: "Optionally produce a decision report",
    body: "If you need a documented record — for governance, for stakeholders, or for your own audit trail — you can request an allocation with report. This produces the same allocation result plus a certified PDF that captures the regime snapshot, policy constraints, asset scoring, and decision rationale at the time the call was made.",
    link: { href: "/decision-report", label: "About the Decision Report" },
  },
  {
    title: "Rebalance when needed",
    body: "If you already have holdings and the target allocation has drifted, Selun computes the adjustments needed to bring the portfolio back within policy. You submit your current holdings alongside the usual inputs, and Selun returns drift analysis and the trade instructions required to rebalance. This is a synchronous call and returns immediately.",
    link: { href: "/capabilities#rebalance", label: "Rebalance endpoint" },
  },
];

export default function HowItWorksPage() {
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
            <Link href="/what-is-selun" className={styles.navLink}>What Is Selun</Link>
            <Link href="/capabilities" className={styles.navLink}>Capabilities</Link>
            <Link href="/decision-report" className={styles.navLink}>Decision Report</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <span>How It Works</span>
          </nav>
          <p className={styles.eyebrow}>Workflow</p>
          <h1 className={styles.pageTitle}>How Selun Works</h1>
          <p className={styles.pageSubhead}>
            Selun applies a structured, repeatable process to every allocation request. Each step
            feeds the next — regime context shapes policy, policy shapes asset selection, and asset
            quality shapes the final weights.
          </p>
        </section>

        <div className={styles.content}>
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>The allocation flow</h2>
            <div className={styles.steps}>
              {STEPS.map((step, i) => (
                <div key={step.title} className={styles.step}>
                  <div className={styles.stepNumber}>
                    <div className={styles.stepNumCircle}>{i + 1}</div>
                    <div className={styles.stepLine} />
                  </div>
                  <div className={styles.stepContent}>
                    <h3 className={styles.stepTitle}>{step.title}</h3>
                    <p className={styles.stepBody}>{step.body}</p>
                    <p style={{ marginTop: 8, marginBottom: 0 }}>
                      <Link href={step.link.href} className={styles.linkPill} style={{ display: "inline-block", marginTop: 4 }}>
                        {step.link.label} →
                      </Link>
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Async vs synchronous endpoints</h2>
            <p className={styles.sectionBody}>
              Allocation endpoints (steps 4 and 5 above) are <strong>asynchronous</strong>. When
              you call them, Selun accepts the job, returns a status path, and processes the
              allocation in the background. You poll the status path until the result is ready.
              This allows the engine to run a thorough analysis without blocking your request.
            </p>
            <p className={styles.sectionBody}>
              The tool endpoints — Market Regime, Policy Envelope, Asset Scorecard, and Rebalance
              — are <strong>synchronous</strong>. They process your inputs immediately and return
              the result in the same response.
            </p>
          </section>

          {/* ── Real example run ── */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What a real run produces</h2>
            <p className={styles.sectionBody}>
              Below is a real allocation run produced by Selun (AAA v4, deterministic mode).
              Inputs: Aggressive risk tolerance, under 1 year timeframe, Bluechips segment.
            </p>

            <div className={styles.exampleRun}>
              <div className={styles.exampleRunHeader}>
                <span className={styles.exampleRunLabel}>Real Example Run</span>
                <span className={styles.exampleRunMeta}>Decision SELUN-DEC-1772241059584 · 2026-02-28 01:11 UTC · Engine v4</span>
              </div>
              <div className={styles.exampleRunBody}>

                {/* Step 1 output: regime */}
                <p style={{ fontSize: 12, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--ink-soft)", marginBottom: 8, marginTop: 0 }}>Step 1 output — Market Regime</p>
                <div className={styles.exampleGrid3}>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Market Condition</div>
                    <div className={styles.exampleTileValue}>Defensive</div>
                    <div className={styles.exampleTileSub}>Confidence 82%</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Fear &amp; Greed</div>
                    <div className={styles.exampleTileValue}>11</div>
                    <div className={styles.exampleTileSub}>Extreme Fear</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Volatility / Liquidity</div>
                    <div className={styles.exampleTileValue}>Low / Weak</div>
                    <div className={styles.exampleTileSub}>Sentiment: −0.412</div>
                  </div>
                </div>

                {/* Step 2 output: policy */}
                <p style={{ fontSize: 12, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--ink-soft)", marginBottom: 8, marginTop: 16 }}>Step 2 output — Policy Envelope</p>
                <div className={styles.exampleGrid3}>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Risk Budget</div>
                    <div className={styles.exampleTileValue}>0.2432</div>
                    <div className={styles.exampleTileSub}>Defensive adjustment active</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Max Single Asset</div>
                    <div className={styles.exampleTileValue}>23.66%</div>
                    <div className={styles.exampleTileSub}>Cap enforced</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>High-Vol Asset Cap</div>
                    <div className={styles.exampleTileValue}>5%</div>
                    <div className={styles.exampleTileSub}>Liquidity floor: Tier 1 only</div>
                  </div>
                </div>

                {/* Step 3 output: scores */}
                <p style={{ fontSize: 12, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--ink-soft)", marginBottom: 8, marginTop: 16 }}>Step 3 output — Asset Scores (top assets)</p>
                <table className={styles.scoreTable}>
                  <thead>
                    <tr>
                      <th>Asset</th>
                      <th>Quality</th>
                      <th>Liquidity</th>
                      <th>Structural</th>
                      <th>Volatility</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr><td>USDT</td><td>0.916</td><td>1.000</td><td>0.960</td><td>0.004</td></tr>
                    <tr><td>BTC</td><td>0.911</td><td>1.000</td><td>0.938</td><td>0.063</td></tr>
                    <tr><td>ETH</td><td>0.911</td><td>1.000</td><td>0.938</td><td>0.063</td></tr>
                    <tr><td>SOL</td><td>0.906</td><td>0.976</td><td>0.938</td><td>0.072</td></tr>
                    <tr><td>XRP</td><td>0.899</td><td>0.947</td><td>0.938</td><td>0.083</td></tr>
                    <tr><td>BNB</td><td>0.846</td><td>0.835</td><td>0.938</td><td>0.126</td></tr>
                  </tbody>
                </table>
                <p style={{ fontSize: 11, color: "var(--ink-soft)", marginTop: 6, marginBottom: 0 }}>
                  Candidate funnel: 300 universe → 64 eligible → 12 selected. 186 assets excluded for liquidity below threshold; meme tokens (DOGE, PEPE, SHIB, TRUMP) excluded by policy.
                </p>

                {/* Step 4 output: allocation */}
                <p style={{ fontSize: 12, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--ink-soft)", marginBottom: 8, marginTop: 16 }}>Step 4 output — Final Allocation (12 assets)</p>
                {[
                  { ticker: "USDT", role: "Stable Holdings", pct: 42.72 },
                  { ticker: "ETH",  role: "Core Holdings",   pct: 17.98 },
                  { ticker: "BTC",  role: "Core Holdings",   pct: 17.89 },
                  { ticker: "SOL",  role: "Income Position", pct: 4.58  },
                  { ticker: "XRP",  role: "Growth",          pct: 3.96  },
                  { ticker: "BNB",  role: "Growth",          pct: 2.61  },
                  { ticker: "TRX",  role: "Growth",          pct: 1.94  },
                  { ticker: "CC",   role: "Income Position", pct: 1.88  },
                  { ticker: "ADA",  role: "Growth",          pct: 1.81  },
                  { ticker: "SUI",  role: "Growth",          pct: 1.63  },
                  { ticker: "LINK", role: "Growth",          pct: 1.55  },
                  { ticker: "BCH",  role: "Growth",          pct: 1.46  },
                ].map((row) => (
                  <div key={row.ticker} className={styles.allocationBar}>
                    <span className={styles.allocationBarLabel}>{row.ticker}</span>
                    <span className={styles.allocationBarRole}>{row.role}</span>
                    <div className={styles.allocationBarTrack}>
                      <div className={styles.allocationBarFill} style={{ width: `${(row.pct / 43) * 100}%` }} />
                    </div>
                    <span className={styles.allocationBarPct}>{row.pct}%</span>
                  </div>
                ))}

                <p style={{ fontSize: 11, color: "var(--ink-soft)", marginTop: 10, marginBottom: 0 }}>
                  Data: 16 sources including Binance, Coinbase Exchange, CoinGecko, Alternative.me, CoinDesk RSS — all fetched at execution time.
                  Integrity hash (SHA-256) generated and embedded in report.
                </p>
              </div>
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Two ways to access Selun</h2>
            <div className={styles.grid2}>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Human interface</p>
                <h3 className={styles.cardTitle}>Allocation Wizard</h3>
                <p className={styles.cardBody}>
                  A guided step-by-step flow where you choose your risk tolerance, asset
                  preferences, and whether you want a certified report. Accepts card payment
                  via Stripe. No wallet required to start.
                </p>
                <Link href="/wizard" className={styles.cardMeta}>
                  Open Wizard →
                </Link>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Machine interface</p>
                <h3 className={styles.cardTitle}>x402 Endpoints</h3>
                <p className={styles.cardBody}>
                  Call any capability directly via HTTP. Each endpoint is payment-gated using
                  the x402 protocol — USDC on Base, pay per call. Suitable for agents,
                  scripts, and automated workflows.
                </p>
                <Link href="/x402" className={styles.cardMeta}>
                  View Endpoint Catalog →
                </Link>
              </div>
            </div>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Related pages</span>
            <Link href="/capabilities" className={styles.linkPill}>Capabilities</Link>
            <Link href="/decision-report" className={styles.linkPill}>Decision Report</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
            <Link href="/compare/allocation-vs-rebalance" className={styles.linkPill}>Allocation vs Rebalance</Link>
            <Link href="/for-developers" className={styles.linkPill}>For Developers</Link>
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
              <Link href="/pricing">Pricing</Link>
              <span aria-hidden>|</span>
              <Link href="/faq">FAQ</Link>
              <span aria-hidden>|</span>
              <Link href="/security">Security</Link>
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
