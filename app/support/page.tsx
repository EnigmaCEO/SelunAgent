import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../legal.module.css";

export const metadata: Metadata = {
  title: "Support | Selun",
  description:
    "Get help with Selun. Contact the team for product questions, billing and payment issues, report questions, and technical integration support.",
  alternates: { canonical: "/support" },
};

export default function SupportPage() {
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
          <h1 className={styles.title}>Support</h1>
          <p className={styles.subtitle}>We usually respond within 1 business day.</p>

          <section className={styles.section}>
            <h2>Contact</h2>
            <p>
              Email the Selun team at{" "}
              <a className={styles.emailLink} href="mailto:selun@sagitta.systems?subject=Selun%20Support">
                selun@sagitta.systems
              </a>
              .
            </p>
          </section>

          <section className={styles.section}>
            <h2>What support covers</h2>
            <ul>
              <li>
                <strong>Product questions.</strong> How Selun works, what each capability does,
                which endpoint to use for your use case.
              </li>
              <li>
                <strong>Allocation and report questions.</strong> Questions about a specific
                allocation result, report content, or decision ID. Include your decision ID or
                job ID when contacting us.
              </li>
              <li>
                <strong>Payment and billing questions.</strong> Issues with a charge, failed
                payment, or request for review of a transaction. Include your decision ID and
                the wallet address or email used at checkout.
              </li>
              <li>
                <strong>Technical integration.</strong> Questions about x402 endpoint behavior,
                input schemas, async polling, or agent integration. For background context, see
                the{" "}
                <Link href="/for-developers">For Developers</Link> page and the{" "}
                <Link href="/x402">x402 endpoint catalog</Link>.
              </li>
              <li>
                <strong>Referral program.</strong> Questions about earnings, payout status, or
                agent spec integration. See the <Link href="/earn">Referral Program</Link> page.
              </li>
              <li>
                <strong>Security reports.</strong> Please email{" "}
                <a className={styles.emailLink} href="mailto:security@sagittalabs.com?subject=Security%20Report">
                  security@sagittalabs.com
                </a>{" "}
                with the subject <em>Security Report</em> before disclosing publicly.
              </li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>What to include in your message</h2>
            <ul>
              <li>Decision ID or job ID (if available)</li>
              <li>Wallet address or email used for the run</li>
              <li>A short description of the issue and expected outcome</li>
              <li>For integration questions: the endpoint and input you are calling</li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>Self-serve resources</h2>
            <ul>
              <li>
                <Link href="/faq">FAQ</Link> — common questions about Selun, x402, custody, rebalancing, and more
              </li>
              <li>
                <Link href="/how-it-works">How it Works</Link> — the full allocation workflow explained
              </li>
              <li>
                <Link href="/capabilities">Capabilities</Link> — what each endpoint does and when to use it
              </li>
              <li>
                <Link href="/pricing">Pricing</Link> — endpoint prices and payment model explained
              </li>
              <li>
                <Link href="/decision-report">Decision Report</Link> — what the certified report contains
              </li>
              <li>
                <Link href="/security">Security &amp; Trust</Link> — non-custodial posture, data boundaries, disclosure contact
              </li>
              <li>
                <Link href="/for-developers">For Developers</Link> — x402 integration guide, endpoint discovery
              </li>
              <li>
                <Link href="/x402">x402 Endpoint Catalog</Link> — machine-facing spec with input schemas and prices
              </li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>Start an allocation</h2>
            <ul>
              <li>
                <Link href="/wizard">Open Allocation Wizard</Link> — guided wizard with card or USDC payment
              </li>
            </ul>
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
              <span aria-hidden>|</span>
              <Link href="/x402">x402 API</Link>
            </nav>
          </footer>
        </section>
      </article>
    </main>
  );
}
