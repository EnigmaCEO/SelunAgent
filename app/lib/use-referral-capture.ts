"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useEffect, useRef } from "react";
import {
  ACTIVE_REFERRAL_CODE_STORAGE_KEY,
  isAgentProgramReferrerId,
  normalizeReferralCode,
  readOrCreateAgentReferralVisitorId,
  stripReferralParam,
} from "@/app/lib/referral";

/** Stores ?ref= locally, records agent-program clicks, and cleans the URL. */
export function useReferralCapture(): string | null {
  const router = useRouter();
  const searchParams = useSearchParams();
  const referralCode = normalizeReferralCode(searchParams.get("ref"));
  const trackedAgentReferralRef = useRef<string | null>(null);

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

  return referralCode;
}
