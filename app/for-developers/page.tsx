import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "For Developers | Selun Machine-Payable Allocation Endpoints",
  description:
    "Selun is a machine-payable AI allocation service built on x402. Learn how to discover, call, and pay for allocation, scoring, and rebalancing endpoints from agent workflows.",
  alternates: { canonical: "/for-developers" },
  openGraph: {
    title: "For Developers | Selun Machine-Payable Allocation Endpoints",
    description:
      "Discover and call Selun allocation, scoring, and rebalancing endpoints via x402. Pay per call in USDC on Base. No API key required.",
    url: "/for-developers",
  },
};

export default function ForDevelopersPage() {
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
            <Link href="/x402" className={styles.navLink}>x402 Catalog</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <span>For Developers</span>
          </nav>
          <p className={styles.eyebrow}>Developer Guide</p>
          <h1 className={styles.pageTitle}>Selun for Developers</h1>
          <p className={styles.pageSubhead}>
            Selun is a machine-payable AI allocation service. Every capability is a payment-gated
            HTTP endpoint on the x402 protocol — callable from agents, scripts, and automated
            pipelines. No API key, no account, no subscription. Pay per call in USDC on Base.
          </p>
        </section>

        <div className={styles.content}>

          {/* x402 posture */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>x402 integration posture</h2>
            <p className={styles.sectionBody}>
              x402 is a machine-payment protocol built on HTTP 402. When your client calls a
              Selun endpoint without payment, the server responds <code>402 Payment Required</code>{" "}
              with a <code>PAYMENT-REQUIRED</code> header describing the amount, asset, and
              network. Your x402-compatible client sends a USDC payment on Base and retries.
              Selun verifies via the Coinbase CDP facilitator and returns the result.
            </p>
            <p className={styles.sectionBody}>
              There is no API key, no OAuth flow, no account setup. The payment IS the
              authentication. Any caller with USDC on Base can call any endpoint.
            </p>
          </section>

          {/* Discovery */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Endpoint discovery</h2>
            <p className={styles.sectionBody}>
              Selun publishes an x402 well-known discovery document and a capabilities endpoint.
              An x402-compatible agent can scan the origin and auto-discover all available
              resources, prices, and input schemas without manual configuration.
            </p>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>x402 discovery root</span>
              <span className={styles.dataRowValue}>
                <code style={{ fontFamily: "monospace", fontSize: 13 }}>
                  https://selun.sagitta.systems/.well-known/x402
                </code>
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Capabilities JSON</span>
              <span className={styles.dataRowValue}>
                <code style={{ fontFamily: "monospace", fontSize: 13 }}>
                  https://selun.sagitta.systems/agent/x402/capabilities
                </code>
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Discovery alias</span>
              <span className={styles.dataRowValue}>
                <code style={{ fontFamily: "monospace", fontSize: 13 }}>
                  https://selun.sagitta.systems/agent/x402/discovery
                </code>
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Network</span>
              <span className={styles.dataRowValue}>Base mainnet (eip155:8453)</span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Payment asset</span>
              <span className={styles.dataRowValue}>USDC</span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Facilitator</span>
              <span className={styles.dataRowValue}>Coinbase CDP</span>
            </div>
          </section>

          {/* Endpoints summary */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Capability summary</h2>
            <p className={styles.sectionBody}>
              Six endpoints. Two are async (allocation). Four are sync (tools).
            </p>
            <div className={styles.grid2}>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>$19.00 USDC · POST · Async</p>
                <h3 className={styles.cardTitle}>Allocation</h3>
                <p className={styles.cardBody}>/agent/x402/allocate</p>
                <p className={styles.cardMeta}>Returns target weights. Poll statusPath for result.</p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>$34.00 USDC · POST · Async</p>
                <h3 className={styles.cardTitle}>Allocation with Report</h3>
                <p className={styles.cardBody}>/agent/x402/allocate-with-report</p>
                <p className={styles.cardMeta}>Same as allocation + certified PDF. Poll statusPath.</p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>$0.25 USDC · POST · Sync</p>
                <h3 className={styles.cardTitle}>Market Regime</h3>
                <p className={styles.cardBody}>/agent/x402/market-regime</p>
                <p className={styles.cardMeta}>Volatility, liquidity, sentiment signals. Immediate.</p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>$0.25 USDC · POST · Sync</p>
                <h3 className={styles.cardTitle}>Policy Envelope</h3>
                <p className={styles.cardBody}>/agent/x402/policy-envelope</p>
                <p className={styles.cardMeta}>Exposure caps, stablecoin floor, risk budget. Immediate.</p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>$0.50 USDC · POST · Sync</p>
                <h3 className={styles.cardTitle}>Asset Scorecard</h3>
                <p className={styles.cardBody}>/agent/x402/asset-scorecard</p>
                <p className={styles.cardMeta}>Per-asset quality scores. Immediate.</p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>$1.00 USDC · POST · Sync</p>
                <h3 className={styles.cardTitle}>Rebalance</h3>
                <p className={styles.cardBody}>/agent/x402/rebalance</p>
                <p className={styles.cardMeta}>Drift analysis + trade instructions. Immediate.</p>
              </div>
            </div>
            <p style={{ marginTop: 14 }}>
              <Link href="/x402" className={styles.linkPill}>
                Full endpoint catalog with input schemas and pricing →
              </Link>
            </p>
          </section>

          {/* Integration pattern */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Typical agent integration pattern</h2>
            <div className={styles.steps}>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>1</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Discover capabilities</h3>
                  <p className={styles.stepBody}>
                    Fetch <code>/agent/x402/capabilities</code> to get the full resource list
                    with endpoints, prices, and input schemas. Cache the result — it changes
                    infrequently.
                  </p>
                </div>
              </div>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>2</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Call a tool endpoint (optional)</h3>
                  <p className={styles.stepBody}>
                    Call Market Regime or Policy Envelope with a USDC payment if your workflow
                    needs explicit regime or policy context before allocating. Both return
                    immediately.
                  </p>
                </div>
              </div>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>3</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Submit allocation request</h3>
                  <p className={styles.stepBody}>
                    POST to <code>/agent/x402/allocate</code> with your decisionId, riskTolerance,
                    and timeframe. Include the USDC payment ($19). Receive a jobId and statusPath.
                  </p>
                </div>
              </div>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>4</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Poll for completion</h3>
                  <p className={styles.stepBody}>
                    Poll the statusPath until the job completes. The response will include the
                    allocation result or an error state.
                  </p>
                </div>
              </div>
              <div className={styles.step}>
                <div className={styles.stepNumber}>
                  <div className={styles.stepNumCircle}>5</div>
                  <div className={styles.stepLine} />
                </div>
                <div className={styles.stepContent}>
                  <h3 className={styles.stepTitle}>Rebalance as needed</h3>
                  <p className={styles.stepBody}>
                    On a monitoring schedule or after significant market moves, POST to
                    <code>/agent/x402/rebalance</code> with current holdings to get drift
                    analysis and trade instructions. Costs $1.00 USDC, returns immediately.
                  </p>
                </div>
              </div>
            </div>
          </section>

          {/* Agent referral program */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Agent referral program</h2>
            <p className={styles.sectionBody}>
              AI agents and developers can participate in the Selun referral program. Include a
              referral code in allocation requests and earn <strong>50% of confirmed allocation
              revenue</strong> — paid in USDC on Base. Human users share a referral link;
              agents use the machine-readable agent spec available at <code>/earn</code>.
            </p>
            <div className={styles.grid2}>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Human referrals</p>
                <h3 className={styles.cardTitle}>Referral link</h3>
                <p className={styles.cardBody}>
                  Share <code>https://selun.sagitta.systems/?ref=YOUR_WALLET</code>. When the
                  referred user completes an allocation, you earn 50% of the $19 allocation fee
                  ($9.50) — or 50% of the $34 report fee when applicable.
                </p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Agent referrals</p>
                <h3 className={styles.cardTitle}>Agent program spec</h3>
                <p className={styles.cardBody}>
                  Agents integrate via the agent spec JSON (copy it from <Link href="/earn" style={{ color: "var(--accent)" }}>/earn</Link>).
                  Include your referral code in allocation API calls. Earnings accrue per
                  confirmed payment and are paid to your Base wallet on the 1st and 15th.
                </p>
              </div>
            </div>
            <p style={{ marginTop: 14 }}>
              <Link href="/earn" className={styles.linkPill}>View agent workspace and earn spec →</Link>
            </p>
          </section>

          {/* Which page to use */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>x402 catalog vs explainer pages</h2>
            <div className={styles.grid2}>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Machine-facing</p>
                <h3 className={styles.cardTitle}>x402 Endpoint Catalog</h3>
                <p className={styles.cardBody}>
                  Full technical detail: discovery root, payment transport headers, facilitator
                  URL, input schemas, required and optional fields, product IDs, async vs sync
                  classification. This is what an x402 scanner reads.
                </p>
                <Link href="/x402" className={styles.cardMeta}>View catalog →</Link>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Human-facing</p>
                <h3 className={styles.cardTitle}>Capabilities Page</h3>
                <p className={styles.cardBody}>
                  Explanation of what each capability does, when to use it,
                  and what it returns. Use this to understand the service before building
                  the integration.
                </p>
                <Link href="/capabilities" className={styles.cardMeta}>View capabilities →</Link>
              </div>
            </div>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Jump to</span>
            <Link href="/x402" className={styles.linkPill}>x402 Endpoint Catalog</Link>
            <Link href="/capabilities" className={styles.linkPill}>Capabilities</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
            <Link href="/faq#x402" className={styles.linkPill}>What is x402?</Link>
            <Link href="/security" className={styles.linkPill}>Security</Link>
            <a href="/SKILL.md" className={styles.linkPill}>Agent Skill File</a>
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
              <span aria-hidden>|</span>
              <a href="/SKILL.md">Agent Skill File</a>
            </nav>
          </footer>
        </div>
      </article>
    </main>
  );
}
