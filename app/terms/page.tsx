import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../legal.module.css";

export const metadata: Metadata = {
  title: "Terms of Service | Selun",
  description: "Selun Terms of Service.",
};

export default function TermsPage() {
  const currentYear = new Date().getFullYear();
  return (
    <main className={styles.page}>
      <article className={styles.shell}>
        <header className={styles.header}>
          <Link href="/" className={styles.brand}>
            <Image src="/selun-mark.svg" alt="Selun" width={32} height={32} className={styles.brandMark} />
            <span>Selun Agent</span>
          </Link>
          <Link href="/" className={styles.homeLink}>
            Back to Home
          </Link>
        </header>

        <section className={styles.content}>
          <h1 className={styles.title}>Terms of Service</h1>
          <p className={styles.subtitle}>Last updated: February 20, 2026</p>

          <section className={styles.section}>
            <h2>Scope</h2>
            <p>
              Selun provides software tools for crypto allocation workflows. You are responsible for your own wallet,
              custody decisions, and transaction approvals.
            </p>
          </section>

          <section className={styles.section}>
            <h2>No Financial Advice</h2>
            <p>
              Selun outputs are informational and operational. They are not personalized investment, tax, accounting,
              or legal advice.
            </p>
          </section>

          <section className={styles.section}>
            <h2>User Responsibilities</h2>
            <ul>
              <li>Provide accurate inputs and review outputs before acting.</li>
              <li>Protect wallet credentials and signing devices.</li>
              <li>Confirm all on-chain transaction details before authorization.</li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>Payments and Access</h2>
            <p>
              Certain features may require payment before report generation or execution. Unless required by law, paid
              services are non-refundable once consumed.
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
            <nav className={styles.footerNav} aria-label="Legal links">
              <Link href="/terms">Terms</Link>
              <span aria-hidden>|</span>
              <Link href="/privacy">Privacy</Link>
              <span aria-hidden>|</span>
              <Link href="/support">Support</Link>
            </nav>
          </footer>
        </section>
      </article>
    </main>
  );
}
