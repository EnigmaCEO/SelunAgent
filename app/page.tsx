"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useCallback, useEffect, useRef, useState } from "react";
import Image from "next/image";
import styles from "./page.module.css";
import {
  ACTIVE_REFERRAL_CODE_STORAGE_KEY,
  isAgentProgramReferrerId,
  appendReferralParam,
  normalizeReferralCode,
  readOrCreateAgentReferralVisitorId,
  stripReferralParam,
} from "@/app/lib/referral";

const DECORATIVE_STEPS = ["1. Risk Tolerance", "2. Crypto Assets", "3. Decision Report"] as const;
const TOKEN_CONTRACT = "0xc0ffee254729296a45a3885639AC7E10F9d54979";
const TOKEN_TICKER = "$SELUN";

type ActivationPhase = "idle" | "activating" | "redirecting";

function HomeContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [phase, setPhase] = useState<ActivationPhase>("idle");
  const activationTimerRef = useRef<number | null>(null);
  const redirectTimerRef = useRef<number | null>(null);

  const currentYear = new Date().getFullYear();
  const tokenLabel = `${TOKEN_CONTRACT.slice(0, 8)}...${TOKEN_CONTRACT.slice(-6)}`;
  const isBusy = phase !== "idle";
  const referralCode = normalizeReferralCode(searchParams.get("ref"));
  const trackedAgentReferralRef = useRef<string | null>(null);

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
        router.push(appendReferralParam("/wizard", referralCode));
      }, 700);
    }, 1400);
  }, [isBusy, referralCode, router]);

  useEffect(() => {
    if (typeof window === "undefined" || !referralCode) return;
    window.localStorage.setItem(ACTIVE_REFERRAL_CODE_STORAGE_KEY, referralCode);
  }, [referralCode]);

  useEffect(() => {
    if (typeof window === "undefined" || !referralCode || !isAgentProgramReferrerId(referralCode)) {
      return;
    }

    const visitorId = readOrCreateAgentReferralVisitorId(window.localStorage);
    const trackingKey = `${referralCode}:${visitorId}`;
    if (trackedAgentReferralRef.current === trackingKey) {
      return;
    }
    trackedAgentReferralRef.current = trackingKey;

    void fetch("/api/referral/agent-track", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        referrer_id: referralCode,
        user_id: visitorId,
        event: "click",
        source: "site_referral",
      }),
    }).catch(() => {
      trackedAgentReferralRef.current = null;
    });
  }, [referralCode]);

  useEffect(() => {
    if (typeof window === "undefined" || !referralCode) return;

    const currentPath = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    const cleanPath = stripReferralParam(currentPath);
    if (cleanPath !== currentPath) {
      router.replace(cleanPath, { scroll: false });
    }
  }, [referralCode, router]);

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
            <Link href="/what-is-selun">About</Link>
            <Link href="/capabilities">Capabilities</Link>
            <Link href="/pricing">Pricing</Link>
            <Link href="/faq">FAQ</Link>
            <Link href="/x402">x402 API</Link>
            <Link href="/earn">Referral</Link>
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
            <Link className={styles.footerLink} href="/what-is-selun">What Is Selun</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/how-it-works">How it Works</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/capabilities">Capabilities</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/pricing">Pricing</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/faq">FAQ</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/security">Security</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/for-developers">For Developers</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/x402">x402 API</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/terms">Terms</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/privacy">Privacy</Link>
            <span aria-hidden>|</span>
            <Link className={styles.footerLink} href="/support">Support</Link>
          </nav>
        </footer>
      </div>
    </main>
  );
}

export default function Home() {
  return (
    <Suspense fallback={null}>
      <HomeContent />
    </Suspense>
  );
}
