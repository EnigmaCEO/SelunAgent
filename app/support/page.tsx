import type { Metadata } from "next";
import Image from "next/image";
import Link from "next/link";
import styles from "../legal.module.css";

export const metadata: Metadata = {
  title: "Support | Selun",
  description: "Support and contact details for Selun.",
};

export default function SupportPage() {
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
          <h1 className={styles.title}>Support</h1>
          <p className={styles.subtitle}>We usually respond within 1 business day.</p>

          <section className={styles.section}>
            <h2>Contact Team</h2>
            <p>
              Email the Selun team at{" "}
              <a className={styles.emailLink} href="mailto:selun@sagitta.systems?subject=Selun%20Support">
                selun@sagitta.systems
              </a>
              .
            </p>
          </section>

          <section className={styles.section}>
            <h2>Include in Your Message</h2>
            <ul>
              <li>Decision ID or job ID (if available)</li>
              <li>Wallet address used for the run</li>
              <li>A short description of the issue and expected outcome</li>
            </ul>
          </section>

          <section className={styles.section}>
            <h2>Quick Links</h2>
            <ul>
              <li>
                <Link href="/wizard">Open Allocation Wizard</Link>
              </li>
              <li>
                <Link href="/terms">Terms of Service</Link>
              </li>
              <li>
                <Link href="/privacy">Privacy Policy</Link>
              </li>
            </ul>
          </section>

          <footer className={styles.footer}>
            <p>© {currentYear} Sagitta Labs</p>
            <nav className={styles.footerNav} aria-label="Support links">
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
