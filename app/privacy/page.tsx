import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../legal.module.css";

export const metadata: Metadata = {
  title: "Privacy Policy | Selun",
  description:
    "Selun Privacy Policy. What we collect, how we use it, and how payment, wallet, and report data are handled for this non-custodial crypto allocation service.",
  alternates: { canonical: "/privacy" },
};

export default function PrivacyPage() {
  const currentYear = new Date().getFullYear();
  return (
    <main className={styles.page}>
      <article className={styles.shell}>
        <header className={styles.header}>
          <Link href="/" className={styles.brand}>
            <Image src="/selun-mark.svg" alt="Selun" width={32} height={32} className={styles.brandMark} />
            <span>Selun</span>
          </Link>
          <Link href="/" className={styles.homeLink}>Back to Home</Link>
        </header>

        <section className={styles.content}>
          <h1 className={styles.title}>Privacy Policy</h1>
          <p className={styles.subtitle}>Last updated: April 7, 2026</p>

          <section className={styles.section}>
            <h2>Overview</h2>
            <p>
              Selun is a non-custodial crypto allocation and rebalancing agent. We collect the
              minimum data needed to operate the service, deliver results, and maintain reliability.
              We do not collect private keys, seed phrases, or unnecessary personal information.
            </p>
          </section>

          <section className={styles.section}>
            <h2>What we collect</h2>
            <ul>
              <li>
                <strong>Allocation inputs.</strong> Risk tolerance, investment timeframe, portfolio
                segment, and any asset list you submit. These are used to generate your allocation
                output and are processed transiently. They may be retained in operational logs for
                reliability and debugging.
              </li>
              <li>
                <strong>Decision ID.</strong> A unique identifier you provide (or that is generated)
                for each allocation request. Used to track and retrieve results.
              </li>
              <li>
                <strong>Wallet address.</strong> Required for x402 payment attribution and, if used,
                for referral tracking. We do not use wallet addresses to initiate transactions or
                request signing.
              </li>
              <li>
                <strong>Payment metadata.</strong> For card payments via Stripe: a payment intent
                reference and confirmation status. For x402 payments: the on-chain transaction hash
                and the paying wallet address on Base. We do not store raw card numbers.
              </li>
              <li>
                <strong>Email address.</strong> Optionally collected when you request email delivery
                of your allocation result or report. Used only for that delivery. Not used for
                marketing unless you separately opt in.
              </li>
              <li>
                <strong>Referral code.</strong> If you arrive or call the service with a referral
                code, it is stored in your session to attribute the conversion. Referral codes may
                be wallet addresses or agent-generated identifiers.
              </li>
              <li>
                <strong>Operational logs.</strong> Request timestamps, job IDs, execution phase
                states, and error traces. Used for reliability, debugging, and fraud prevention.
                Not sold or shared beyond operational necessity.
              </li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>What we do not collect</h2>
            <ul>
              <li>Private keys or seed phrases — these are never requested, transmitted, or stored.</li>
              <li>Device fingerprints, browsing history, or cross-site tracking data.</li>
              <li>Sensitive personal information such as government ID, national identification numbers, or biometric data.</li>
              <li>Portfolio balances or custodial account data from exchanges or wallets.</li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>Wallet and address handling</h2>
            <p>
              Wallet addresses are used for two purposes: payment attribution (confirming that
              the correct address paid for a request) and referral tracking (associating a
              conversion with a referring address). We do not connect to your wallet, request
              transaction signing, or initiate any outbound transfer from our systems to your
              wallet other than referral payouts through the referral program you explicitly
              join.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Payment data</h2>
            <p>
              Card payments are processed by <strong>Stripe</strong>. Selun does not store raw card
              numbers or card data. Stripe handles PCI-compliant card processing and provides us
              with a payment reference and status.
            </p>
            <p>
              x402 payments are made in USDC on the Base network (eip155:8453) through the
              <strong> Coinbase CDP</strong> facilitator. Payment verification uses the on-chain
              transaction hash. We retain the transaction hash and paying wallet address as part
              of the purchase record for your allocation or report.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Report and request metadata</h2>
            <p>
              When you purchase an Allocation with Report, the report PDF is generated and
              associated with your decision ID. The report includes your allocation inputs,
              the market regime at time of execution, the policy constraints applied, asset
              scores, and a SHA-256 integrity hash. This information is part of the product
              output you paid for.
            </p>
            <p>
              Report data may be retained on our infrastructure for a period sufficient to
              allow report retrieval and support queries. We do not share report contents
              with third parties except as required to operate the service.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Analytics and logging</h2>
            <p>
              We use <strong>Vercel Analytics</strong> for aggregate site usage data (page views,
              performance metrics). This does not include personal identification beyond standard
              anonymous browser signals. Refer to Vercel&apos;s privacy documentation for details
              on their data handling.
            </p>
            <p>
              Operational server logs are retained for reliability, security, and abuse prevention.
              These are not used for advertising or sold to third parties.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Third-party services</h2>
            <p>The following third-party services are used in operating Selun:</p>
            <ul>
              <li><strong>Stripe</strong> — card payment processing</li>
              <li><strong>Coinbase CDP</strong> — x402 payment facilitation on Base</li>
              <li><strong>WalletConnect</strong> — optional wallet connection for in-browser wallet flows</li>
              <li><strong>Vercel</strong> — hosting, CDN, and analytics</li>
            </ul>
            <p>
              Each of these services operates under its own privacy policy. We share only the
              data each service needs to fulfill its function.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Data retention</h2>
            <p>
              Allocation inputs and request logs are retained for operational purposes. We do
              not commit to specific retention periods here; if you have a specific deletion
              request, contact us at the address below and we will respond within a reasonable
              timeframe.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Contact</h2>
            <p>
              For privacy questions or requests, email{" "}
              <a className={styles.emailLink} href="mailto:selun@sagitta.systems?subject=Selun%20Privacy">
                selun@sagitta.systems
              </a>{" "}
              with the subject <em>Selun Privacy</em>. We usually respond within 2 business days.
            </p>
          </section>

          <footer className={styles.footer}>
            <p>© {currentYear} Sagitta Labs</p>
            <nav className={styles.footerNav} aria-label="Site links">
              <Link href="/">Home</Link>
              <span aria-hidden>|</span>
              <Link href="/terms">Terms</Link>
              <span aria-hidden>|</span>
              <Link href="/privacy">Privacy</Link>
              <span aria-hidden>|</span>
              <Link href="/support">Support</Link>
              <span aria-hidden>|</span>
              <Link href="/security">Security</Link>
              <span aria-hidden>|</span>
              <Link href="/faq">FAQ</Link>
              <span aria-hidden>|</span>
              <Link href="/what-is-selun">What Is Selun</Link>
            </nav>
          </footer>
        </section>
      </article>
    </main>
  );
}
