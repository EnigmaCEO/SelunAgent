import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "Decision Report | Selun Certified Allocation Record",
  description:
    "The Selun Decision Report is a 9-section certified PDF record of your allocation — regime snapshot, policy envelope, 16-source data audit, asset scoring, candidate filtering rationale, governance signals, and payment integrity hash.",
  alternates: { canonical: "/decision-report" },
  openGraph: {
    title: "Decision Report | Selun Certified Allocation Record",
    description:
      "A 9-section certified PDF capturing the full execution trace of your allocation decision — from data sources to final weights.",
    url: "/decision-report",
  },
};

const REPORT_SECTIONS = [
  {
    title: "Summary dashboard",
    desc: "Market condition (with confidence %), strategy name, Fear & Greed index, concentration snapshot (largest position, top 3 combined). Plain-language explanation of why the allocation fits current market conditions. Key control decisions applied.",
    example: "Market Condition: Defensive · Confidence 82% · Fear & Greed: 11 (Extreme Fear)",
  },
  {
    title: "Portfolio Mix Overview + Recommended Asset Allocation",
    desc: "Allocation broken down by role category (Stable Holdings, Core Holdings, Income Position, Growth Positions, Liquidity Reserve, High-Risk Positions). Full asset table with ticker, role, risk class, and target weight percentage.",
    example: "Stable 42.72% · Core 35.87% · Growth 14.96% · Income 6.46%",
  },
  {
    title: "Role explanations + Action Plan",
    desc: "Plain-language explanation of what each role category means in the context of this allocation. Numbered action plan: how to use target weights, when to rebalance, when to re-run the allocation, and how to maintain risk discipline.",
    example: "Review weekly. Rebalance when allocations drift meaningfully from targets.",
  },
  {
    title: "Engine Trace & Audit Artifacts",
    desc: "The full policy snapshot used during candidate selection: Phase 2 policy rules (26), Phase 3 selection rules (22), Phase 4 screening rules (26). Policy envelope numeric values: capital preservation bias, defensive adjustment, exposure caps, liquidity floor, volatility target, risk budget, risk scaling factor, volatility ceiling. Phase 4 screening thresholds: min liquidity score, min screening score, min structural score, min volume 24h, rank sanity threshold, eligible count.",
    example: "Risk Budget: 0.2432 · Max Single Asset: 23.66% · High-Vol Cap: 5% · Volatility Ceiling: 0.4399",
  },
  {
    title: "Audit Trail: Data Sources & Coverage",
    desc: "Complete source registry with provider name, endpoint, and fetch timestamp for all data consumed during the run. Source selection rationale for each data dimension (volatility, liquidity, sentiment, market metrics) with credibility scores. Flags any missing domains or unresolved tokens encountered by the pipeline.",
    example: "16 sources · Binance, Coinbase Exchange, CoinGecko, Alternative.me, CoinDesk RSS — all timestamped",
  },
  {
    title: "Audit Trail: Candidate Filtering Rationale",
    desc: "Full candidate funnel from initial universe to final allocation, with explicit exclusion and selection reasons at each gate. Top exclusion reasons by count. Named examples of excluded assets (e.g. meme tokens, unverified stablecoins). Named examples of selected assets with their primary selection reasons.",
    example: "300 universe → 64 eligible → 12 selected → 12 allocated · 186 excluded: Liquidity Below Threshold · DOGE/PEPE/SHIB excluded: Meme Token Not Allowed",
  },
  {
    title: "Audit Trail: Asset Scoring Detail",
    desc: "Numeric score components for every selected asset: composite quality, risk component, composite score, liquidity, structural stability, and volatility. AAA v4 final score trace showing final score, weight percentage, sentiment input, quality, expected return, and volatility for each asset. All figures are retained for deterministic replay and traceability.",
    example: "BTC: Quality 0.911 · Liquidity 1.000 · Structural 0.938 · Volatility 0.063",
  },
  {
    title: "Governance Signals & Exception Register",
    desc: "Allocator execution status, primary reason code, and full reason code register. Effective constraints applied by the AAA engine: high-volatility asset cap, volatility threshold, max asset weight, max concentration, risk budget. Role counts (Carry, Core, Defensive, Satellite). Input sanitation summary: assets in, assets out, dropped, invalid roles.",
    example: "Status: OK · Reason: Role Targeting Applied · High-Volatility Cap Active",
  },
  {
    title: "Verification, Payment & Integrity",
    desc: "Decision ID, purchase status, payment transaction hash on Base, paid wallet address, report type, pipeline coverage summary. SHA-256 integrity hash of the document. On-chain hash registration status (can be enabled via SELUN_REPORT_AUTO_ATTEST). Disclaimer confirming certification scope: document integrity and provenance only.",
    example: "Decision ID: SELUN-DEC-* · Integrity Hash (SHA-256): 46da5d... · Payment tx: 0xc85b6ad...",
  },
];

export default function DecisionReportPage() {
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
            <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.navLink}>Compare</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <Link href="/capabilities">Capabilities</Link>
            <span aria-hidden>›</span>
            <span>Decision Report</span>
          </nav>
          <p className={styles.eyebrow}>Optional Add-On · $34 USDC</p>
          <h1 className={styles.pageTitle}>The Selun Decision Report</h1>
          <p className={styles.pageSubhead}>
            The decision report is a 9-section certified PDF produced alongside an allocation.
            It captures the full execution trace — regime, policy, data sources, candidate
            filtering, asset scores, governance signals, and payment integrity — in one
            structured, shareable document.
          </p>
        </section>

        <div className={styles.content}>

          {/* What it is */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What the report is</h2>
            <p className={styles.sectionBody}>
              When you request <em>Allocation with Report</em>, Selun runs the same allocation
              engine as the standard endpoint, then generates a PDF that records the entire
              execution state at the moment the decision was made. The report is tied to a
              unique <strong>decision ID</strong>, timestamped at generation, and sealed with a
              SHA-256 integrity hash.
            </p>
            <p className={styles.sectionBody}>
              It is not a prospectus, not a financial advisory document, and not an audit by a
              third party. It is a <strong>machine-generated execution record</strong> — every
              data source, every policy rule, every exclusion reason, and every score that
              produced your allocation, captured in a format you can share with stakeholders,
              reference in governance, or use as an audit artifact.
            </p>
            <div className={styles.infoBox}>
              <strong>Certification scope:</strong> The report confirms document integrity and
              provenance. It does not certify suitability, expected return, or future
              performance. This is stated explicitly on the report itself.
            </div>
          </section>

          {/* Real example highlight */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What a real report looks like</h2>
            <p className={styles.sectionBody}>
              A real Selun decision report runs to 9 pages. Here is the actual execution summary
              from a live run (Decision SELUN-DEC-1772241059584):
            </p>
            <div className={styles.exampleRun}>
              <div className={styles.exampleRunHeader}>
                <span className={styles.exampleRunLabel}>Real Report — Execution Summary</span>
                <span className={styles.exampleRunMeta}>2026-02-28 01:11:22 UTC · Engine v4 · Deterministic Mode</span>
              </div>
              <div className={styles.exampleRunBody}>
                <div className={styles.exampleGrid3} style={{ gridTemplateColumns: "repeat(2, 1fr)" }}>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Market Condition</div>
                    <div className={styles.exampleTileValue}>Defensive</div>
                    <div className={styles.exampleTileSub}>Confidence 82% · F&G: 11 Extreme Fear</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Strategy</div>
                    <div className={styles.exampleTileValue}>Balanced Defensive</div>
                    <div className={styles.exampleTileSub}>Aggressive risk · Under 1 year</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Candidate Funnel</div>
                    <div className={styles.exampleTileValue}>300 → 64 → 12</div>
                    <div className={styles.exampleTileSub}>Universe → Eligible → Allocated</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Data Sources</div>
                    <div className={styles.exampleTileValue}>16 sources</div>
                    <div className={styles.exampleTileSub}>Binance, Coinbase, CoinGecko, Alternative.me, CoinDesk…</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Policy Rules Applied</div>
                    <div className={styles.exampleTileValue}>74 rules</div>
                    <div className={styles.exampleTileSub}>Phase 2: 26 · Phase 3: 22 · Phase 4: 26</div>
                  </div>
                  <div className={styles.exampleTile}>
                    <div className={styles.exampleTileLabel}>Integrity</div>
                    <div className={styles.exampleTileValue}>SHA-256</div>
                    <div className={styles.exampleTileSub}>46da5d47a425a7f4… (hash embedded in PDF)</div>
                  </div>
                </div>
                <p style={{ fontSize: 11, color: "var(--ink-soft)", marginTop: 4, marginBottom: 0 }}>
                  Largest position: 42.72% (USDT) · Top 3 combined: 78.59% · Risk budget: 0.2432 · Meme tokens excluded by policy.
                </p>
              </div>
            </div>
          </section>

          {/* What it includes — actual 9 sections */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Report structure — all 9 sections</h2>
            <p className={styles.sectionBody}>
              Every report contains the same nine sections in this order, regardless of the
              allocation outcome. The examples below are drawn from real output.
            </p>
            <div className={styles.reportSections}>
              {REPORT_SECTIONS.map((s, i) => (
                <div key={s.title} className={styles.reportSection}>
                  <div className={styles.reportSectionNum}>{i + 1}</div>
                  <div>
                    <p className={styles.reportSectionTitle}>{s.title}</p>
                    <p className={styles.reportSectionDesc}>{s.desc}</p>
                    <p className={styles.reportSectionExample}>{s.example}</p>
                  </div>
                </div>
              ))}
            </div>
          </section>

          {/* Why it matters */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Why it matters</h2>
            <p className={styles.sectionBody}>
              Most allocation tools return a number. Selun can also return the reasoning — every
              data source consulted, every rule applied, every asset excluded and why. When you
              need to explain a portfolio decision to a DAO, a co-signer, a board, or your
              future self, the allocation weights are not enough. The report gives you the
              complete chain of reasoning in one document.
            </p>
            <p className={styles.sectionBody}>
              It also creates a durable, verifiable record. The SHA-256 hash in the final section
              allows anyone to verify the document has not been altered since generation. If you
              enable on-chain attestation (<code>SELUN_REPORT_AUTO_ATTEST=true</code>), the hash
              can also be registered on-chain for independent verification.
            </p>
          </section>

          {/* Who should use it */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Who should request the report</h2>
            <div className={styles.pillRow}>
              <span className={styles.pill}>DAOs with treasury governance</span>
              <span className={styles.pill}>Multi-sig treasury operators</span>
              <span className={styles.pill}>Fund administrators</span>
              <span className={styles.pill}>Investors who review decisions periodically</span>
              <span className={styles.pill}>Developers building auditable workflows</span>
            </div>
            <p className={styles.sectionBody} style={{ marginTop: 16 }}>
              If you are making a one-time personal allocation and do not need a shareable
              record, the standard allocation at $19 USDC is sufficient. The report is most
              valuable when the decision involves multiple stakeholders, requires documentation,
              or will be referenced in governance, compliance, or audit contexts.
            </p>
          </section>

          {/* Comparison */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Allocation only vs allocation with report</h2>
            <div className={styles.compareGrid}>
              <div className={styles.compareCol}>
                <h3 className={styles.compareHead}>Allocation</h3>
                <div className={styles.comparePrice}>$19 <span style={{ fontSize: 14, fontWeight: 600, color: "var(--ink-soft)" }}>USDC</span></div>
                <p className={styles.compareBody}>
                  Full allocation engine. Returns structured target weights.
                </p>
                <ul className={styles.compareList}>
                  <li>Target allocation weights</li>
                  <li>Decision ID</li>
                  <li>Policy constraints applied</li>
                  <li>Asynchronous — poll statusPath</li>
                </ul>
              </div>
              <div className={styles.compareCol}>
                <h3 className={styles.compareHead}>Allocation with Report</h3>
                <div className={styles.comparePrice}>$34 <span style={{ fontSize: 14, fontWeight: 600, color: "var(--ink-soft)" }}>USDC</span></div>
                <p className={styles.compareBody}>
                  Same allocation engine, plus 9-section certified PDF execution record.
                </p>
                <ul className={styles.compareList}>
                  <li>Target allocation weights</li>
                  <li>Decision ID</li>
                  <li>Policy constraints applied</li>
                  <li>Regime snapshot with confidence</li>
                  <li>16-source data audit with timestamps</li>
                  <li>Candidate funnel (300 → final)</li>
                  <li>Per-asset numeric scores</li>
                  <li>Governance signals &amp; reason codes</li>
                  <li>SHA-256 integrity hash</li>
                  <li>Optional on-chain hash registration</li>
                </ul>
              </div>
            </div>
            <p style={{ marginTop: 14 }}>
              <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.linkPill}>
                Full comparison →
              </Link>
            </p>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Related pages</span>
            <Link href="/capabilities" className={styles.linkPill}>All Capabilities</Link>
            <Link href="/how-it-works" className={styles.linkPill}>How it Works</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
            <Link href="/compare/allocation-only-vs-allocation-with-report" className={styles.linkPill}>Compare: Allocation vs Report</Link>
            <Link href="/security" className={styles.linkPill}>Security &amp; Trust</Link>
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
