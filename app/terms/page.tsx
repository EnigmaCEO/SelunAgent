import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../legal.module.css";

export const metadata: Metadata = {
  title: "Terms of Service | Selun",
  description:
    "Selun Terms of Service. Non-custodial crypto allocation service. Outputs are informational only, not financial advice. Users are responsible for independent judgment and execution.",
  alternates: { canonical: "/terms" },
};

export default function TermsPage() {
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
          <h1 className={styles.title}>Terms of Service</h1>
          <p className={styles.subtitle}>Last updated: April 7, 2026</p>

          <section className={styles.section}>
            <h2>Who operates Selun</h2>
            <p>
              Selun is operated by Sagitta Labs. By using Selun — through the allocation
              wizard, the x402 endpoints, or any other interface — you agree to these terms.
              If you do not agree, do not use the service.
            </p>
          </section>

          <section className={styles.section}>
            <h2>What Selun is</h2>
            <p>
              Selun is a software tool that generates crypto portfolio allocation recommendations,
              market regime classifications, risk policy envelopes, asset quality scores, and
              portfolio rebalancing instructions. Selun is powered by the Sagitta AAA quantitative
              allocation engine.
            </p>
            <p>
              Selun is a <strong>non-custodial</strong> service. It does not hold, transfer, or
              sign transactions on your behalf. It does not connect to your exchange accounts,
              wallet signing keys, or custodial services. It produces recommendations; you
              decide whether and how to act on them.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Not financial, legal, or tax advice</h2>
            <p>
              Selun outputs — including allocation weights, market regime classifications, policy
              envelopes, asset scores, rebalancing instructions, and decision reports — are
              <strong> informational and operational only</strong>. They are not personalized
              investment advice, financial planning advice, tax advice, legal advice, or any
              other regulated advisory service.
            </p>
            <p>
              No output from Selun guarantees or predicts any level of portfolio performance,
              return, or protection from loss. Crypto markets are highly volatile. Past allocation
              outputs do not indicate future results.
            </p>
            <p>
              You remain solely responsible for your own investment decisions, execution, risk
              assessment, and compliance with applicable laws in your jurisdiction.
            </p>
          </section>

          <section className={styles.section}>
            <h2>User responsibilities</h2>
            <ul>
              <li>
                <strong>Independent judgment.</strong> Review all Selun outputs before taking any
                action. Do not execute trades, transfer assets, or make any financial decision
                based solely on Selun output without independent assessment.
              </li>
              <li>
                <strong>Accurate inputs.</strong> You are responsible for providing accurate risk
                tolerance, timeframe, holdings, and other inputs. Selun cannot validate your
                inputs against your actual financial situation.
              </li>
              <li>
                <strong>Wallet security.</strong> You are responsible for the security of your
                own wallet keys, signing devices, and custody arrangements. Selun never requests
                private keys or seed phrases.
              </li>
              <li>
                <strong>Transaction confirmation.</strong> You are responsible for reviewing and
                authorizing any on-chain transactions. Selun does not initiate or sign
                transactions on your behalf.
              </li>
              <li>
                <strong>Jurisdictional compliance.</strong> You are responsible for determining
                whether using this service is lawful in your jurisdiction and for any reporting
                or compliance obligations that apply to you.
              </li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>Payments and refunds</h2>
            <p>
              Access to certain Selun capabilities requires payment before execution. Payments
              are accepted via USDC on Base (through the x402 protocol) or by card via Stripe.
            </p>
            <p>
              <strong>Payments are non-refundable once the service has been consumed.</strong>{" "}
              This means: once an allocation has been executed, a report has been generated, or
              a synchronous endpoint has returned a result, the corresponding payment is final.
            </p>
            <p>
              If you believe you were charged incorrectly or a service failed to deliver a
              result, contact{" "}
              <a className={styles.emailLink} href="mailto:selun@sagitta.systems?subject=Selun%20Billing">
                selun@sagitta.systems
              </a>{" "}
              with your decision ID or job ID and we will investigate.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Acceptable use</h2>
            <p>You agree not to:</p>
            <ul>
              <li>Use Selun endpoints in a way that intentionally circumvents per-call payment (e.g. replaying paid responses to bypass the payment gate for new requests).</li>
              <li>Resell or redistribute Selun outputs as a commercial service without explicit written permission from Sagitta Labs.</li>
              <li>Attempt to reverse-engineer the underlying Sagitta AAA model or allocation engine beyond what is exposed through the public API.</li>
              <li>Use the service for any purpose that violates applicable law, including market manipulation or sanctions evasion.</li>
              <li>Submit false or misleading inputs designed to generate outputs for deceptive purposes.</li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>Service availability and modification</h2>
            <p>
              Sagitta Labs reserves the right to modify, suspend, or discontinue Selun or any
              of its capabilities at any time, with or without notice. Pricing, endpoint
              availability, and output formats may change. We will make reasonable efforts to
              communicate significant changes, but make no guarantees about service continuity
              or uptime.
            </p>
            <p>
              We reserve the right to update these terms at any time. Continued use of the
              service after a terms update constitutes acceptance of the revised terms. The
              effective date at the top of this page reflects the most recent update.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Limitation of liability</h2>
            <p>
              To the fullest extent permitted by law, Sagitta Labs and its affiliates are not
              liable for any direct, indirect, incidental, special, or consequential damages
              arising from your use of Selun or reliance on its outputs — including but not
              limited to investment losses, missed opportunities, or errors in allocation
              recommendations.
            </p>
            <p>
              Selun is provided &ldquo;as is&rdquo; without warranty of any kind. We do not
              warrant that outputs will be accurate, complete, timely, or suitable for any
              particular purpose.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Governing law</h2>
            <p>
              These terms are governed by the laws applicable to Sagitta Labs.
              Please contact us for jurisdiction-specific questions.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Contact</h2>
            <p>
              Questions about these terms:{" "}
              <a className={styles.emailLink} href="mailto:selun@sagitta.systems?subject=Selun%20Terms">
                selun@sagitta.systems
              </a>
              .
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
