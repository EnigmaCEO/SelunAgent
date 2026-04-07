import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../info.module.css";

export const metadata: Metadata = {
  title: "FAQ | Selun AI Crypto Allocation Agent",
  description:
    "Frequently asked questions about Selun: what it is, how it works, non-custodial posture, rebalancing, x402 payments, decision reports, and how it relates to Sagitta AAA.",
  alternates: { canonical: "/faq" },
  openGraph: {
    title: "FAQ | Selun AI Crypto Allocation Agent",
    description:
      "Frequently asked questions about Selun: what it is, non-custodial design, x402, rebalancing, decision reports, and more.",
    url: "/faq",
  },
};

const FAQS = [
  {
    q: "What is Selun?",
    a: (
      <>
        Selun is a non-custodial AI crypto allocation and rebalancing agent powered by{" "}
        <Link href="/what-is-selun">Sagitta AAA</Link>. It generates target portfolio
        allocations, classifies market regimes, computes risk policy envelopes, scores candidate
        assets, and produces rebalancing instructions. All capabilities are available via
        payment-gated x402 endpoints and through a guided allocation wizard.
      </>
    ),
  },
  {
    q: "Is Selun a trading bot?",
    a: "No. Selun does not connect to any exchange, sign transactions, or execute trades. It produces allocation recommendations and rebalancing instructions — what you do with them is entirely your decision. Execution happens outside Selun.",
  },
  {
    q: "Does Selun take custody of my assets?",
    a: "No. Selun is non-custodial by design. It never holds, moves, or controls your assets. Payments for endpoint access are discrete per-call micropayments via x402 (USDC on Base). There is no deposit mechanism, no ongoing wallet access, and no stored payment method.",
  },
  {
    q: "Who is Selun for?",
    a: (
      <>
        Selun is designed for crypto investors who want structured allocation decisions, treasury
        operators and DAOs who need documented decision records, and developers or agent builders
        integrating machine-payable allocation services. See{" "}
        <Link href="/what-is-selun">What Is Selun</Link> for the full breakdown by audience.
      </>
    ),
  },
  {
    q: "What is the difference between Allocation and Allocation with Report?",
    a: (
      <>
        Both endpoints run the same allocation engine and return target portfolio weights. The
        difference is the <em>Allocation with Report</em> also produces a certified PDF decision
        record — a structured document capturing the market regime at the time of the decision,
        the risk policy envelope, asset quality scores, and decision rationale. The standard
        Allocation is $19 USDC. With Report is $34 USDC. See the{" "}
        <Link href="/compare/allocation-only-vs-allocation-with-report">full comparison</Link>{" "}
        or learn more about the{" "}
        <Link href="/decision-report">Decision Report</Link>.
      </>
    ),
  },
  {
    q: "How does rebalancing work?",
    a: (
      <>
        Submit your current holdings alongside your risk tolerance and timeframe, and Selun
        computes the drift between what you hold now and the optimal target allocation under
        current market conditions. It returns the drift analysis per asset and the trade
        instructions needed to rebalance within policy. The Rebalance endpoint is synchronous
        and costs $1.00 USDC. See{" "}
        <Link href="/compare/allocation-vs-rebalance">Allocation vs Rebalance</Link>{" "}
        and the{" "}
        <Link href="/capabilities#rebalance">Rebalance capability</Link>.
      </>
    ),
  },
  {
    q: "What is x402?",
    a: (
      <>
        x402 is a protocol for machine-payable HTTP endpoints. When a client calls an x402
        endpoint without payment, the server responds with HTTP 402 Payment Required and
        describes what payment is needed. The client sends a USDC micropayment on Base and
        retries the call. The server verifies payment and returns the result. It enables
        programmatic pay-per-call access to services without accounts, subscriptions, or API
        keys. Selun uses x402 for all its AI allocation endpoints. See the{" "}
        <Link href="/x402">x402 endpoint catalog</Link>.
      </>
    ),
  },
  {
    q: "Do I need a wallet to use Selun?",
    a: "Not necessarily. The allocation wizard — the guided human-facing flow — accepts card payment via Stripe. You do not need a crypto wallet to use it. If you are calling x402 endpoints directly (e.g. from an agent or script), you need a wallet with USDC on Base (eip155:8453) to pay for each call.",
  },
  {
    q: "Is Selun for retail investors, DAOs, or both?",
    a: "Both. Individual investors can use the allocation wizard to get a portfolio recommendation in minutes. DAOs and institutional treasury operators can use the Allocation with Report endpoint to produce documented, governance-ready allocation decisions. Developers can integrate any endpoint into automated workflows.",
  },
  {
    q: "How is Selun related to Sagitta AAA?",
    a: "Selun is the product interface for Sagitta AAA — a quantitative allocation infrastructure layer. Sagitta AAA provides the underlying market regime classifier, policy envelope engine, asset scoring model, and portfolio allocator. Selun exposes these capabilities as callable, payment-gated endpoints and as a guided wizard for human users.",
  },
];

const jsonLd = {
  "@context": "https://schema.org",
  "@type": "FAQPage",
  mainEntity: FAQS.map((faq) => ({
    "@type": "Question",
    name: faq.q,
    acceptedAnswer: {
      "@type": "Answer",
      text: typeof faq.a === "string" ? faq.a : faq.q, // fallback for JSX answers
    },
  })),
};

export default function FAQPage() {
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
            <Link href="/what-is-selun" className={styles.navLink}>What Is Selun</Link>
            <Link href="/security" className={styles.navLink}>Security</Link>
            <Link href="/pricing" className={styles.navLink}>Pricing</Link>
            <Link href="/" className={styles.homeLink}>Home</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <nav className={styles.breadcrumb} aria-label="Breadcrumb">
            <Link href="/">Home</Link>
            <span aria-hidden>›</span>
            <span>FAQ</span>
          </nav>
          <p className={styles.eyebrow}>Frequently Asked Questions</p>
          <h1 className={styles.pageTitle}>FAQ</h1>
          <p className={styles.pageSubhead}>
            Common questions about what Selun is, how it works, and how to use it.
          </p>
        </section>

        <div className={styles.content}>
          <section className={styles.section}>
            <div className={styles.faqList}>
              {FAQS.map((faq) => (
                <div key={faq.q} className={styles.faqItem}>
                  <div className={styles.faqQ}>{faq.q}</div>
                  <p className={styles.faqA}>{faq.a}</p>
                </div>
              ))}
            </div>
          </section>

          <section className={styles.section}>
            <h2 className={styles.sectionTitle}>Still have questions?</h2>
            <p className={styles.sectionBody}>
              Reach the Selun team at{" "}
              <a
                href="mailto:selun@sagitta.systems?subject=Selun%20Question"
                style={{ color: "var(--accent)", fontWeight: 600 }}
              >
                selun@sagitta.systems
              </a>
              . We usually respond within one business day.
            </p>
          </section>

          <div className={styles.linkRow}>
            <span className={styles.linkRowLabel}>Explore further</span>
            <Link href="/what-is-selun" className={styles.linkPill}>What Is Selun</Link>
            <Link href="/how-it-works" className={styles.linkPill}>How it Works</Link>
            <Link href="/capabilities" className={styles.linkPill}>Capabilities</Link>
            <Link href="/security" className={styles.linkPill}>Security</Link>
            <Link href="/pricing" className={styles.linkPill}>Pricing</Link>
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
