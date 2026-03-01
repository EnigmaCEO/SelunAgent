"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useRef, useState } from "react";
import Image from "next/image";
import styles from "./page.module.css";

const DECORATIVE_STEPS = ["1. Risk Tolerance", "2. Crypto Assets", "3. Decision Report"] as const;
const TOKEN_CONTRACT = "0xc0ffee254729296a45a3885639AC7E10F9d54979";
const TOKEN_TICKER = "$SELUN";

type ActivationPhase = "idle" | "activating" | "redirecting";

export default function Home() {
  const router = useRouter();
  const [phase, setPhase] = useState<ActivationPhase>("idle");
  const activationTimerRef = useRef<number | null>(null);
  const redirectTimerRef = useRef<number | null>(null);

  const currentYear = new Date().getFullYear();
  const tokenLabel = `${TOKEN_CONTRACT.slice(0, 8)}...${TOKEN_CONTRACT.slice(-6)}`;
  const isBusy = phase !== "idle";

  const ctaLabel =
  phase === "idle"
    ? "Engage Selun"
    : phase === "activating"
    ? "Engaging Selun..."
    : "Opening Allocation Wizard...";

const coreEyebrow =
  phase === "idle"
    ? "SELUN AGENT"
    : phase === "activating"
    ? "ENGAGING SELUN"
    : "SELUN AGENT";

const coreTitle =
  phase === "idle"
    ? "LIVE"
    : phase === "activating"
    ? "ONLINE"
    : "READY";

  const handleEngage = useCallback(() => {
    if (isBusy) return;

    setPhase("activating");

    activationTimerRef.current = window.setTimeout(() => {
      setPhase("redirecting");

      redirectTimerRef.current = window.setTimeout(() => {
        router.push("/wizard");
      }, 700);
    }, 1400);
  }, [isBusy, router]);

  useEffect(() => {
    return () => {
      if (activationTimerRef.current) window.clearTimeout(activationTimerRef.current);
      if (redirectTimerRef.current) window.clearTimeout(redirectTimerRef.current);
    };
  }, []);

  return (
    <main className={styles.page}>
      <div className={styles.bgGlow} aria-hidden />
      <div className={styles.grid} aria-hidden />

      <div className={styles.shell}>
        <header className={`${styles.topbar} ${styles.reveal} ${styles.delay0}`}>
          <Link href="/" className={styles.brand}>
            <Image src="/selun-logo.svg" alt="Selun" width={154} height={48} className={styles.brandLogo} priority />
          </Link>

          <nav className={styles.nav} aria-label="Primary navigation">
            <Link href="/x402">x402 API</Link>
          </nav>

          {/* <div className={styles.contractPill} title={TOKEN_CONTRACT}>
            <span className={styles.tokenTicker}>{TOKEN_TICKER}</span>
            <span className={styles.contractDivider} aria-hidden />
            <span className={styles.contractLabel}>Contract: {tokenLabel}</span>
          </div> */}
        </header>

        <section className={`${styles.hero} ${styles.reveal} ${styles.delay1}`}>
          <div className={styles.heroCopy}>
            <p className={styles.eyebrow}>Sagitta AAA - Allocator v4</p>
            <h1>Simple Crypto Allocation</h1>
            <p className={styles.subhead}>Powered by Market Intelligence.</p>
          </div>

          <div className={styles.hudStage} aria-label="Selun decision status">
            <div
              className={`${styles.hud} ${
                phase === "activating" ? styles.hudActivating : phase === "redirecting" ? styles.hudRedirecting : ""
              }`}
            >
              <div className={styles.hudRingA} />
              <div className={styles.hudRingB} />
              <div className={styles.hudRingC} />
              <div className={styles.hudCore}>
                <span>{coreEyebrow}</span>
                <strong>{coreTitle}</strong>
              </div>
            </div>

            <button
              type="button"
              className={`${styles.primaryCta} ${styles.talkingCta} ${
                phase !== "idle" ? styles.talkingCtaActivating : ""
              }`}
              onClick={handleEngage}
              disabled={isBusy}
            >
              {ctaLabel}
            </button>
          </div>
        </section>

        <section id="loop" className={`${styles.loopSection} ${styles.reveal} ${styles.delay2}`}>
          <div className={styles.signalRow}>
            {DECORATIVE_STEPS.map((item) => (
              <span key={item} className={styles.signalChip}>
                {item}
              </span>
            ))}
          </div>
        </section>

        <section id="access" className={`${styles.ctaBand} ${styles.reveal} ${styles.delay3}`}>
          <p>In and out in seconds.</p>
          <a className={styles.secondaryCta} href="mailto:selun@sagitta.systems?subject=Selun%20Team%20Inquiry">
            Contact Team
          </a>
        </section>

        <footer className={styles.siteFooter}>
          <p className={styles.footerCopy}>(c) {currentYear} Sagitta Labs</p>
          <nav className={styles.footerLinks} aria-label="Footer links">
            <Link className={styles.footerLink} href="/terms">
              Terms of Service
            </Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/privacy">
              Privacy Policy
            </Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/support">
              Support
            </Link>
          </nav>
        </footer>
      </div>
    </main>
  );
}
