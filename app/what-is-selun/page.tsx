import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "What Is Selun? | AI Crypto Allocation Agent — Sagitta AAA",
  description:
    "Selun is a non-custodial AI crypto allocation and rebalancing agent powered by Sagitta AAA. Learn what it does, who it is for, and how it works.",
  alternates: { canonical: "/what-is-selun" },
  openGraph: {
    title: "What Is Selun? | AI Crypto Allocation Agent",
    description:
      "Selun is a non-custodial AI crypto allocation and rebalancing agent powered by Sagitta AAA.",
    url: "/what-is-selun",
  },
};

const jsonLd = {
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  name: "Selun",
  applicationCategory: "FinanceApplication",
  description:
    "Non-custodial AI crypto allocation and rebalancing agent powered by Sagitta AAA. Provides machine-payable portfolio allocation, market regime classification, policy envelope computation, asset scoring, and rebalancing via x402 endpoints.",
  operatingSystem: "Web",
  url: "https://selun.sagitta.systems",
  offers: {
    "@type": "Offer",
    priceCurrency: "USD",
    price: "19.00",
    description: "Allocation endpoint — pay per call via x402 in USDC on Base",
  },
  provider: {
    "@type": "Organization",
    name: "Sagitta Labs",
    url: "https://sagitta.systems",
  },
};

export default function WhatIsSelunPage() {
  const currentYear = new Date().getFullYear();
  return (
    <main className={styles.page}>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
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
            <Link href="/security" className={styles.navLink}>Security</Link>
            <Link href="/faq" className={styles.navLink}>FAQ</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <span>What Is Selun?</span>
          </nav>
          <p className={styles.eyebrow}>Product Overview</p>
          <h1 className={styles.pageTitle}>What Is Selun?</h1>
          <p className={styles.pageSubhead}>
            Selun is a non-custodial AI crypto allocation and rebalancing agent powered by Sagitta
            AAA. It helps crypto investors, treasury operators, and DAOs make structured,
            data-informed allocation decisions — without handing over their assets.
          </p>
        </section>

        <div className={styles.content}>

          {/* Core identity */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>The short version</h2>
            <p className={styles.sectionBody}>
              Selun is an <strong>AI crypto allocation agent</strong>. You give it your risk
              tolerance and timeframe. It classifies the current market regime, computes a risk
              policy envelope, scores candidate assets, and returns a target portfolio allocation.
              If you need a documented record, it also produces a certified decision report.
            </p>
            <p className={styles.sectionBody}>
              Selun does not hold your money, execute trades, or manage a wallet on your behalf.
              It produces <strong>recommendations and structured outputs</strong> — what you do
              with them is your decision.
            </p>
          </section>

          {/* Who it is for */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Who it is for</h2>
            <div className={styles.grid2}>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Crypto Investors</p>
                <h3 className={styles.cardTitle}>Individual investors</h3>
                <p className={styles.cardBody}>
                  Get a structured target allocation based on your risk profile and the current
                  market environment — without needing to build or maintain a quant model yourself.
                </p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Institutional</p>
                <h3 className={styles.cardTitle}>Treasury operators &amp; DAOs</h3>
                <p className={styles.cardBody}>
                  Produce defensible, documented allocation decisions for on-chain treasuries.
                  The optional decision report gives governance participants a structured audit
                  record.
                </p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Builders</p>
                <h3 className={styles.cardTitle}>Developers &amp; agent builders</h3>
                <p className={styles.cardBody}>
                  Integrate Selun as a machine-payable service via the x402 protocol. Call
                  allocation, scoring, and rebalancing endpoints directly from automated
                  workflows — paying per call in USDC on Base.
                </p>
              </div>
              <div className={styles.card}>
                <p className={styles.cardEyebrow}>Explorers</p>
                <h3 className={styles.cardTitle}>x402 and agent-payment explorers</h3>
                <p className={styles.cardBody}>
                  Selun is one of the first production services built on the x402 payment
                  protocol. It is a concrete example of what machine-payable AI services look
                  like in practice.
                </p>
              </div>
            </div>
          </section>

          {/* What problems it solves */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What problems Selun solves</h2>
            <p className={styles.sectionBody}>
              Crypto allocation decisions are hard. Market conditions shift fast. Risk tolerance
              is personal. Policy constraints are easy to ignore. And once a decision is made,
              there is usually no documented record of why.
            </p>
            <p className={styles.sectionBody}>
              Selun applies a consistent, structured process — regime assessment, policy scoping,
              asset scoring, and allocation generation — every time. It replaces gut feeling with
              a repeatable engine and optionally produces a certified record you can share with
              stakeholders or reference later.
            </p>
          </section>

          {/* What it is not */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>What Selun is not</h2>
            <div className={styles.infoBox}>
              <strong>Selun is not a trading bot.</strong> It does not connect to an exchange,
              sign transactions, or execute trades on your behalf.
            </div>
            <div className={styles.infoBox}>
              <strong>Selun is not a custodian.</strong> It never holds, controls, or moves your
              assets. Your keys stay yours.
            </div>
            <div className={styles.infoBox}>
              <strong>Selun is not a fund.</strong> It does not pool capital, manage positions,
              or generate returns. It produces allocation recommendations.
            </div>
            <div className={styles.infoBox}>
              <strong>Selun is not financial advice.</strong> Outputs are data products for
              informed decision-making. You remain responsible for your investment decisions.
            </div>
          </section>

          {/* Non-custodial */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Non-custodial by design</h2>
            <p className={styles.sectionBody}>
              Selun is built to be non-custodial at every layer. You never share private keys.
              Selun never initiates transfers. Payment for endpoint access uses the x402 protocol
              — a per-call USDC micropayment on Base — so there is no subscription, no stored
              payment method, and no ongoing wallet access.
            </p>
          </section>

          {/* Powered by */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Powered by Sagitta AAA</h2>
            <p className={styles.sectionBody}>
              Selun is the product interface for <strong>Sagitta AAA</strong>, a quantitative
              allocation infrastructure layer. The Sagitta AAA engine provides the market regime
              classifier, policy envelope logic, asset scorecard model, and portfolio allocator
              that Selun exposes as callable endpoints.
            </p>
          </section>

          {/* x402 */}
          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Machine-payable via x402</h2>
            <p className={styles.sectionBody}>
              All Selun capabilities are available as payment-gated HTTP endpoints using the
              <strong> x402 protocol</strong> — a standard for machine-to-machine micropayments
              over USDC on Base. Agents, scripts, and automated workflows can discover, call,
              and pay for Selun endpoints without any human interaction.
            </p>
            <p className={styles.sectionBody}>
              Human users can access the same capabilities through the allocation wizard, which
              accepts card payment.
            </p>
          </section>

          {/* See also */}
          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Explore further</span>
            <Link href="/how-it-works" className={styles.linkPill}>How it Works</Link>
            <Link href="/capabilities" className={styles.linkPill}>Capabilities</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
            <Link href="/security" className={styles.linkPill}>Security</Link>
            <Link href="/faq" className={styles.linkPill}>FAQ</Link>
            <Link href="/x402" className={styles.linkPill}>x402 Endpoint Catalog</Link>
          </div>

          <footer className={styles.footer}>
            <p>© {currentYear} Sagitta Labs</p>
            <nav className={styles.footerNav} aria-label="Site links">
              <Link href="/">Home</Link>
              <span aria-hidden>|</span>
              <Link href="/capabilities">Capabilities</Link>
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
