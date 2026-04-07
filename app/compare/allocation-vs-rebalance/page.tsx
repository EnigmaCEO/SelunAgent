import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../../info.module.css";

export const metadata: Metadata = {
  title: "Allocation vs Rebalance | Selun",
  description:
    "When to use Selun's Allocation endpoint vs the Rebalance endpoint. Allocation generates a target portfolio. Rebalance adjusts an existing one toward a target.",
  alternates: { canonical: "/compare/allocation-vs-rebalance" },
  openGraph: {
    title: "Allocation vs Rebalance | Selun",
    description:
      "Allocation generates a target portfolio. Rebalance adjusts an existing one. Learn when to use each.",
    url: "/compare/allocation-vs-rebalance",
  },
};

export default function AllocationVsRebalancePage() {
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
            <Link href="/pricing" className={styles.navLink}>Pricing</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <Link href="/capabilities">Capabilities</Link>
            <span aria-hidden>›</span>
            <span>Allocation vs Rebalance</span>
          </nav>
          <p className={styles.eyebrow}>Comparison</p>
          <h1 className={styles.pageTitle}>Allocation vs Rebalance</h1>
          <p className={styles.pageSubhead}>
            Both endpoints are about portfolio positioning, but they answer different questions.
            Allocation asks: <em>what should I hold?</em> Rebalance asks: <em>how do I get
            from what I hold now to where I should be?</em>
          </p>
        </section>

        <div className={styles.content}>

          <section className={styles.section}>
            <div className={styles.compareGrid}>
              <div className={styles.compareCol}>
                <h2 className={styles.compareHead}>Allocation</h2>
                <div className={styles.comparePrice}>$19 <span style={{ fontSize: 14, fontWeight: 600, color: "var(--ink-soft)" }}>USDC</span></div>
                <p className={styles.comparePriceSub}>Async · POST /agent/x402/allocate</p>
                <p className={styles.compareBody}>
                  Generates a target portfolio allocation from scratch. You do not need existing
                  holdings. You provide your risk tolerance and timeframe, and Selun returns
                  the weights you should target.
                </p>
                <ul className={styles.compareList}>
                  <li>Use when you are starting a new allocation</li>
                  <li>Use when you want a fresh target regardless of current holdings</li>
                  <li>Runs regime, policy, and scoring internally</li>
                  <li>Returns target weights per asset</li>
                  <li>Asynchronous — poll statusPath for result</li>
                </ul>
              </div>
              <div className={styles.compareCol}>
                <h2 className={styles.compareHead}>Rebalance</h2>
                <div className={styles.comparePrice}>$1 <span style={{ fontSize: 14, fontWeight: 600, color: "var(--ink-soft)" }}>USDC</span></div>
                <p className={styles.comparePriceSub}>Sync · POST /agent/x402/rebalance</p>
                <p className={styles.compareBody}>
                  Takes your current holdings and computes the gap between what you hold and
                  the optimal target for your profile under current conditions. Returns the
                  adjustments needed.
                </p>
                <ul className={styles.compareList}>
                  <li>Use when you already have holdings and want to maintain policy alignment</li>
                  <li>Use on a schedule (daily, weekly) for drift monitoring</li>
                  <li>Requires current holdings as input</li>
                  <li>Returns drift per asset + trade instructions</li>
                  <li>Synchronous — returns immediately</li>
                </ul>
              </div>
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>The typical workflow</h2>
            <p className={styles.sectionBody}>
              Most portfolio workflows use Allocation first, then Rebalance on an ongoing basis.
            </p>
            <div className={styles.steps}>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>1</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Initial allocation</h3>
                  <p className={styles.stepBody}>
                    Call the Allocation endpoint with your risk profile. Get back target weights.
                    Deploy the portfolio according to those weights.
                  </p>
                </div>
              </div>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>2</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Ongoing rebalancing</h3>
                  <p className={styles.stepBody}>
                    Over time, asset prices drift and the portfolio moves away from the target
                    allocation. Call Rebalance periodically with your current holdings to get
                    the adjustments needed to stay within policy.
                  </p>
                </div>
              </div>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>3</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Regime-triggered re-allocation</h3>
                  <p className={styles.stepBody}>
                    When market conditions shift significantly — a volatility spike, a sentiment
                    reversal — run a fresh Allocation to update the target. Then use Rebalance
                    to compute the path from current holdings to the new target.
                  </p>
                </div>
              </div>
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Key input difference</h2>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Allocation inputs</span>
              <span className={styles.dataRowValue}>
                decisionId, riskTolerance, timeframe, portfolioSegment (optional)
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Rebalance inputs</span>
              <span className={styles.dataRowValue}>
                decisionId, riskTolerance, timeframe, <strong>holdings</strong> (required — array of current positions)
              </span>
            </div>
            <p className={styles.sectionBody} style={{ marginTop: 14 }}>
              The Rebalance endpoint requires your current holdings. Without them, it cannot
              compute drift. If you do not have holdings to submit, use Allocation instead.
            </p>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Related pages</span>
            <Link href="/capabilities#allocation" className={styles.linkPill}>Allocation capability</Link>
            <Link href="/capabilities#rebalance" className={styles.linkPill}>Rebalance capability</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
            <Link href="/how-it-works" className={styles.linkPill}>How it Works</Link>
            <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.linkPill}>Allocation vs Report</Link>
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
