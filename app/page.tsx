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

const DECORATIVE_STEPS = ["1. Your Profile", "2. Market Check", "3. Your Portfolio Plan"] as const;

const ALLOCATION_RESEARCH_URL =
  "https://www.sagitta.systems/newsroom/what-aggressive-means-in-a-defensive-market";

const SAMPLE_CONDITION = {
  label: "BALANCED",
  strategy: "Balanced Growth",
  tags: ["Portfolio Strategy", "Market Analysis", "Crypto Education", "Investment Calculator"],
} as const;

const SAMPLE_GROUPS = [
  {
    heading: "STABLE HOLDINGS — 15%",
    assets: [
      { ticker: "USDT", name: "Tether", role: "Defensive", pct: 15 },
    ],
  },
  {
    heading: "CORE HOLDINGS — 45%",
    assets: [
      { ticker: "BTC", name: "Bitcoin", role: "Core", pct: 25 },
      { ticker: "ETH", name: "Ethereum", role: "Core", pct: 20 },
    ],
  },
  {
    heading: "GROWTH POSITIONS — 40%",
    assets: [
      { ticker: "SOL", name: "Solana", role: "Growth", pct: 15 },
      { ticker: "BNB", name: "BNB", role: "Growth", pct: 10 },
      { ticker: "XRP", name: "XRP", role: "Growth", pct: 8 },
      { ticker: "LINK", name: "Chainlink", role: "Growth", pct: 7 },
    ],
  },
] as const;

type ActivationPhase = "idle" | "activating" | "redirecting";

function HomeContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [phase, setPhase] = useState<ActivationPhase>("idle");
  const activationTimerRef = useRef<number | null>(null);
  const redirectTimerRef = useRef<number | null>(null);

  const currentYear = new Date().getFullYear();
  const isBusy = phase !== "idle";
  const referralCode = normalizeReferralCode(searchParams.get("ref"));
  const trackedAgentReferralRef = useRef<string | null>(null);


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
          <div className={styles.planGrid}>
            <div className={styles.planCopy}>
              <div className={styles.signalRow} style={{ marginBottom: 20 }}>
                {DECORATIVE_STEPS.map((item) => (
                  <span key={item} className={styles.signalChip}>{item}</span>
                ))}
              </div>
              <h1 className={styles.planHeadline}>Get Your Crypto Portfolio Plan</h1>
              <p className={styles.planDesc}>
                Answer 3 quick questions and see a recommended crypto plan for your risk level,
                time horizon, and current market conditions.
              </p>
              <button
                type="button"
                className={`${styles.primaryCta} ${styles.talkingCta} ${isBusy ? styles.talkingCtaActivating : ""}`}
                onClick={handleEngage}
                disabled={isBusy}
              >
                {phase === "idle" ? "Start My Plan" : phase === "activating" ? "Loading..." : "Opening..."}
              </button>

              <div>&nbsp;</div>
              <p className={styles.planPriceNote}>
              For $19, you get recommended assets, target percentages, and an explanation of why the plan fits you.
              </p>
              <p className={styles.planDisclaimer}>No wallet required.</p>
            </div>

            <div className={styles.planCard}>
              <p className={styles.planCardLabel}>Sample Plan</p>
              <div className={styles.sampleCondition}>
                <span className={styles.sampleConditionBadge}>{SAMPLE_CONDITION.label}</span>
                <span className={styles.sampleStrategy}>Strategy: {SAMPLE_CONDITION.strategy}</span>
              </div>
              <div className={styles.sampleTags}>
                {SAMPLE_CONDITION.tags.map((tag) => (
                  <span key={tag} className={styles.sampleTag}>{tag}</span>
                ))}
              </div>
              <div className={styles.sampleGroups}>
                {SAMPLE_GROUPS.map((group) => (
                  <div key={group.heading} className={styles.sampleGroup}>
                    <p className={styles.sampleGroupHeading}>{group.heading}</p>
                    {group.assets.map(({ ticker, name, role, pct }) => (
                      <div key={ticker} className={styles.sampleAssetRow}>
                        <div className={styles.sampleAssetMeta}>
                          <span className={styles.sampleTicker}>{ticker}</span>
                          <span className={styles.sampleName}>{name}</span>
                          <span className={styles.sampleRole}>{role}</span>
                        </div>
                        <div className={styles.sampleAssetRight}>
                          <span className={styles.samplePct}>{pct}%</span>
                          <div className={styles.planBar}>
                            <div className={styles.planBarFill} style={{ width: `${pct * 2.8}%` }} />
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
              <p className={styles.planCardCaption}>
                Sample only. Your plan is generated fresh from current market conditions.
              </p>
            </div>
          </div>
        </section>

        <aside
          className={`${styles.researchCard} ${styles.reveal} ${styles.delay2}`}
          aria-labelledby="allocation-research-title"
        >
          <a
            className={styles.researchCardLink}
            href={ALLOCATION_RESEARCH_URL}
            target="_blank"
            rel="noopener noreferrer"
          >
            <div className={styles.researchCardCopy}>
              <p className={styles.researchCardEyebrow}>Allocation Research · Sagitta Systems</p>
              <h2 id="allocation-research-title" className={styles.researchCardTitle}>
                What Aggressive Means in a Defensive Market
              </h2>
              <p className={styles.researchCardSummary}>
                See how risk tolerance changes Selun&apos;s allocation while the market state stays the same.
              </p>
            </div>
            <span className={styles.researchCardAction}>
              Read the analysis <span aria-hidden>↗</span>
            </span>
          </a>
        </aside>

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
