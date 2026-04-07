import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "Security & Trust | Selun Non-Custodial Allocation Agent",
  description:
    "Selun is a non-custodial crypto allocation agent. Learn what Selun controls, what it does not, what data it needs, and how access and payment boundaries work.",
  alternates: { canonical: "/security" },
  openGraph: {
    title: "Security & Trust | Selun",
    description:
      "Selun is non-custodial. It never holds, moves, or signs your assets. Learn the full access and payment boundaries.",
    url: "/security",
  },
};

export default function SecurityPage() {
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
            <Link href="/faq" className={styles.navLink}>FAQ</Link>
            <Link href="/privacy" className={styles.navLink}>Privacy Policy</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <span>Security &amp; Trust</span>
          </nav>
          <p className={styles.eyebrow}>Trust &amp; Safety</p>
          <h1 className={styles.pageTitle}>Security &amp; Trust</h1>
          <p className={styles.pageSubhead}>
            Selun is a non-custodial allocation agent. It computes recommendations — it does not
            hold assets, execute trades, or control wallets. This page describes exactly what
            Selun can and cannot do, what data it needs, and how to reach us.
          </p>
        </section>

        <div className={styles.content}>

          {/* Non-custodial */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Non-custodial posture</h2>
            <p className={styles.sectionBody}>
              Selun is designed to be non-custodial at every layer. This is not a feature — it
              is a structural constraint. The service has no mechanism to hold, transfer, or sign
              transactions on your behalf.
            </p>
            <div className={styles.infoBox}>
              <strong>Selun never holds your assets.</strong> No deposit addresses, no asset
              storage, no wallet management.
            </div>
            <div className={styles.infoBox}>
              <strong>Selun never executes trades.</strong> It returns allocation recommendations
              and rebalancing instructions. Execution is always your decision.
            </div>
            <div className={styles.infoBox}>
              <strong>Selun never requests private keys.</strong> No input field, API parameter,
              or integration step requires a private key or seed phrase. If something claims to
              be Selun and asks for one, it is not Selun.
            </div>
          </section>

          {/* What Selun controls */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What Selun controls</h2>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Computation</span>
              <span className={styles.dataRowValue}>
                Market regime classification, policy envelope, asset scoring, allocation weights, rebalancing instructions
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Report generation</span>
              <span className={styles.dataRowValue}>
                PDF decision records linked to a decision ID and timestamp
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Payment receipt</span>
              <span className={styles.dataRowValue}>
                USDC received via x402 per-call payment (Base network) or Stripe card payment for the allocation wizard
              </span>
            </div>
          </section>

          {/* What Selun does not control */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What Selun does not control</h2>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Your wallet</span>
              <span className={styles.dataRowValue}>
                Selun has no access to your wallet. It receives payment per call but cannot initiate transfers.
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Trade execution</span>
              <span className={styles.dataRowValue}>
                Selun returns instructions. It does not connect to any exchange or DEX.
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Asset custody</span>
              <span className={styles.dataRowValue}>
                No deposit mechanism exists. Selun cannot hold assets on your behalf.
              </span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Ongoing wallet access</span>
              <span className={styles.dataRowValue}>
                Each x402 payment is a discrete per-call transaction. No recurring access is established.
              </span>
            </div>
          </section>

          {/* Data requirements */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Data Selun needs</h2>
            <p className={styles.sectionBody}>
              Selun endpoints accept the minimum data needed to compute the requested output. No
              personally identifiable information is required to use x402 endpoints.
            </p>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Risk tolerance</span>
              <span className={styles.dataRowValue}>e.g. conservative, moderate, aggressive</span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Timeframe</span>
              <span className={styles.dataRowValue}>short, medium, or long term horizon</span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Portfolio segment</span>
              <span className={styles.dataRowValue}>optional — e.g. Bluechips, Memecoins, Gaming, Yield Farm</span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Asset list</span>
              <span className={styles.dataRowValue}>optional — for asset scoring; you provide the candidates</span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Current holdings</span>
              <span className={styles.dataRowValue}>required for rebalancing only — amounts by asset</span>
            </div>
            <div className={styles.dataRow}>
              <span className={styles.dataRowLabel}>Decision ID</span>
              <span className={styles.dataRowValue}>a unique identifier you generate to track the request</span>
            </div>
            <p className={styles.sectionBody} style={{ marginTop: 14 }}>
              For the allocation wizard, an email address is optionally collected for result
              delivery. Refer to the{" "}
              <Link href="/privacy" style={{ color: "var(--accent)", fontWeight: 600 }}>
                Privacy Policy
              </Link>{" "}
              for full data handling details.
            </p>
          </section>

          {/* Payment boundaries */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Payment boundaries</h2>
            <p className={styles.sectionBody}>
              x402 endpoint payments are per-call USDC micropayments on the Base network
              (eip155:8453). Each call authorizes and settles one discrete payment. There is no
              subscription, no stored payment method, and no recurring charge mechanism.
            </p>
            <p className={styles.sectionBody}>
              The allocation wizard accepts card payment via Stripe. Card data is handled
              entirely by Stripe and is never stored on Selun infrastructure.
            </p>
          </section>

          {/* Integration boundaries */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Integration boundaries</h2>
            <p className={styles.sectionBody}>
              Selun is a REST API service. Integrations call HTTP endpoints. The service returns
              JSON (or an async status path for allocation endpoints). There is no SDK that
              requires elevated permissions, no browser extension, and no mobile app requesting
              wallet connection.
            </p>
            <p className={styles.sectionBody}>
              The x402 facilitator used is Coinbase CDP. Payment routing and facilitator
              infrastructure is managed by Coinbase, not Sagitta Labs.
            </p>
          </section>

          {/* Responsible disclosure */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Responsible disclosure &amp; contact</h2>
            <p className={styles.sectionBody}>
              If you discover a security issue, please report it to the Selun team before
              disclosing publicly. We take security reports seriously and will respond promptly.
            </p>
            <p className={styles.sectionBody}>
              
              Contact:{" "}
              <a
                href="mailto:security@sagittalabs.com?subject=Security%20Report"
                style={{ color: "var(--accent)", fontWeight: 600 }}
              >
                security@sagittalabs.com
              </a>
              {" "}with subject line <em>Security Report</em>.
            </p>
            
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Related pages</span>
            <Link href="/what-is-selun" className={styles.linkPill}>What Is Selun</Link>
            <Link href="/faq" className={styles.linkPill}>FAQ</Link>
            <Link href="/privacy" className={styles.linkPill}>Privacy Policy</Link>
            <Link href="/terms" className={styles.linkPill}>Terms of Service</Link>
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
