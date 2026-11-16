"use client";

import { useRouter } from "next/navigation";
import { useEffect, useRef, useSyncExternalStore } from "react";
import {
  ACTIVE_REFERRAL_CODE_STORAGE_KEY,
  isAgentProgramReferrerId,
  normalizeReferralCode,
  readOrCreateAgentReferralVisitorId,
  stripReferralParam,
} from "@/app/lib/referral";

function subscribeToHistory(onChange: () => void): () => void {
  window.addEventListener("popstate", onChange);
  return () => window.removeEventListener("popstate", onChange);
}

function readReferralFromLocation(): string | null {
  return normalizeReferralCode(new URLSearchParams(window.location.search).get("ref"));
}

function noReferralOnServer(): string | null {
  return null;
}

/**
 * Stores ?ref= locally, records agent-program clicks, and cleans the URL.
 *
 * Reads window.location rather than useSearchParams: on a statically rendered
 * page, useSearchParams makes Next skip server rendering for everything under
 * the nearest Suspense boundary, which left crawlers an empty page. The server
 * render and hydration see no code; the browser reads it right after.
 */
export function useReferralCapture(): string | null {
  const router = useRouter();
  const referralCode = useSyncExternalStore(subscribeToHistory, readReferralFromLocation, noReferralOnServer);
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
