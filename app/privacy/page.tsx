import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../legal.module.css";

export const metadata: Metadata = {
  title: "Privacy Policy | Selun",
  description: "Selun Privacy Policy.",
};

export default function PrivacyPage() {
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
          <h1 className={styles.title}>Privacy Policy</h1>
          <p className={styles.subtitle}>Last updated: February 20, 2026</p>

          <section className={styles.section}>
            <h2>What We Collect</h2>
            <ul>
              <li>Wallet addresses and transaction references needed for allocation execution.</li>
              <li>Configuration inputs used to generate allocation outputs and reports.</li>
              <li>Operational logs for reliability, debugging, and fraud prevention.</li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>How We Use Data</h2>
            <p>
              We use collected data to operate Selun, deliver allocation results, generate purchased reports, and
              improve system safety and performance.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Data Sharing</h2>
            <p>
              We do not sell personal data. Data may be shared with infrastructure and payment providers only to run
              the service.
            </p>
          </section>

          <section className={styles.section}>
            <h2>Contact</h2>
            <p>
              Privacy requests can be sent to{" "}
              <a className={styles.emailLink} href="mailto:selun@sagitta.systems?subject=Selun%20Privacy">
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
