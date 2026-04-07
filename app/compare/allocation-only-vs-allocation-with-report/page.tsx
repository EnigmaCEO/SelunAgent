import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../../info.module.css";

export const metadata: Metadata = {
  title: "Allocation Only vs Allocation with Report | Selun",
  description:
    "Compare Selun's $19 Allocation and $34 Allocation with Report. The report adds a certified PDF decision record — regime snapshot, policy constraints, asset scores, and rationale.",
  alternates: { canonical: "/compare/allocation-only-vs-allocation-with-report" },
  openGraph: {
    title: "Allocation Only vs Allocation with Report | Selun",
    description:
      "The $15 premium buys a certified PDF decision record. Learn when the report is worth it and when it is not.",
    url: "/compare/allocation-only-vs-allocation-with-report",
  },
};

export default function AllocationVsReportPage() {
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
            <Link href="/decision-report" className={styles.navLink}>Decision Report</Link>
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
            <span>Allocation Only vs With Report</span>
          </nav>
          <p className={styles.eyebrow}>Comparison</p>
          <h1 className={styles.pageTitle}>Allocation Only vs Allocation with Report</h1>
          <p className={styles.pageSubhead}>
            Both endpoints run the same allocation engine. The difference is what comes with the
            result. Allocation only returns the target weights. Allocation with Report also
            produces a certified PDF record of the decision.
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
                  Runs the full allocation engine — regime, policy, asset scoring — and returns
                  a structured allocation result you can act on immediately.
                </p>
                <ul className={styles.compareList}>
                  <li>Target weights per asset</li>
                  <li>Decision ID for tracking</li>
                  <li>Policy constraints applied</li>
                  <li>Asynchronous — poll statusPath</li>
                </ul>
              </div>
              <div className={styles.compareCol}>
                <h2 className={styles.compareHead}>Allocation with Report</h2>
                <div className={styles.comparePrice}>$34 <span style={{ fontSize: 14, fontWeight: 600, color: "var(--ink-soft)" }}>USDC</span></div>
                <p className={styles.comparePriceSub}>Async · POST /agent/x402/allocate-with-report</p>
                <p className={styles.compareBody}>
                  Everything in the standard allocation, plus a certified PDF decision record
                  capturing the full analysis context at the moment of the decision.
                </p>
                <ul className={styles.compareList}>
                  <li>Target weights per asset</li>
                  <li>Decision ID for tracking</li>
                  <li>Policy constraints applied</li>
                  <li>Market regime snapshot at decision time</li>
                  <li>Asset quality scores with rationale</li>
                  <li>Decision narrative and timestamp</li>
                  <li>Downloadable certified PDF</li>
                </ul>
              </div>
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What the $15 difference buys</h2>
            <p className={styles.sectionBody}>
              The premium covers the generation and delivery of a certified PDF decision record.
              The document captures the <strong>state of the analysis at the moment the
              allocation was made</strong> — not just the output, but the context that produced it.
            </p>
            <p className={styles.sectionBody}>
              That means: the regime Selun observed, the policy envelope it computed, the asset
              scores it assigned, and the rationale for the final weights — all timestamped and
              tied to the decision ID. It is a record you can share, store, and reference later.
            </p>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>When to choose Allocation only</h2>
            <div className={styles.card}>
              <p className={styles.cardBody}>
                Use standard Allocation when you need a target portfolio and do not need to
                document or share the decision. This is appropriate for personal, one-time
                allocation decisions, automated agent workflows where the result feeds directly
                into the next step without a governance checkpoint, and test or development
                runs where you are exploring output behavior.
              </p>
              <p className={styles.cardMeta}>$19 USDC per call</p>
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>When to choose Allocation with Report</h2>
            <div className={styles.card}>
              <p className={styles.cardBody}>
                Choose the report when the allocation will be reviewed by stakeholders who were
                not present when it was made, used in a DAO governance vote or proposal, stored
                as part of an audit trail for treasury management, or presented to co-signers
                who need to understand and approve the decision before execution.
              </p>
              <p className={styles.cardMeta}>$34 USDC per call</p>
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>The same engine, twice</h2>
            <p className={styles.sectionBody}>
              It is worth being explicit: both endpoints call the same underlying allocation
              engine. The resulting weights will be identical for the same inputs at the same
              time. The report endpoint does not produce a different or better allocation — it
              produces the same allocation plus a documented record of it. The choice is about
              documentation, not about quality.
            </p>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Related pages</span>
            <Link href="/decision-report" className={styles.linkPill}>About the Decision Report</Link>
            <Link href="/capabilities#allocation" className={styles.linkPill}>Allocation capability</Link>
            <Link href="/capabilities#allocation-with-report" className={styles.linkPill}>Allocation with Report capability</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
            <Link href="/compare/allocation-vs-rebalance" className={styles.linkPill}>Allocation vs Rebalance</Link>
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
              <Link href="/decision-report">Decision Report</Link>
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
