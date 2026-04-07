import type { MetadataRoute } from "next";

const BASE = "https://selun.sagitta.systems";

// Use a fixed date for pages built as part of the current release.
// Update when content materially changes.
const RELEASE_DATE = new Date("2026-04-07");
const POLICY_DATE = new Date("2026-04-07");

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    // ── Core entry points ────────────────────────────────────────
    {
      url: `${BASE}/`,
      lastModified: RELEASE_DATE,
      changeFrequency: "weekly",
      priority: 1.0,
    },
    {
      url: `${BASE}/x402`,
      lastModified: RELEASE_DATE,
      changeFrequency: "weekly",
      priority: 0.9,
    },

    // ── Primary explainer pages ──────────────────────────────────
    {
      url: `${BASE}/what-is-selun`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.9,
    },
    {
      url: `${BASE}/how-it-works`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.85,
    },
    {
      url: `${BASE}/capabilities`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.9,
    },
    {
      url: `${BASE}/pricing`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.85,
    },
    {
      url: `${BASE}/faq`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.8,
    },
    {
      url: `${BASE}/decision-report`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.75,
    },

    // ── Trust and security ───────────────────────────────────────
    {
      url: `${BASE}/security`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.7,
    },

    // ── Developer-facing ─────────────────────────────────────────
    {
      url: `${BASE}/for-developers`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.8,
    },

    // ── Compare pages ────────────────────────────────────────────
    {
      url: `${BASE}/compare/allocation-vs-rebalance`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.6,
    },
    {
      url: `${BASE}/compare/allocation-only-vs-allocation-with-report`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.6,
    },

    // ── Referral program ─────────────────────────────────────────
    {
      url: `${BASE}/earn`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.7,
    },

    // ── Legal and support ────────────────────────────────────────
    {
      url: `${BASE}/privacy`,
      lastModified: POLICY_DATE,
      changeFrequency: "yearly",
      priority: 0.4,
    },
    {
      url: `${BASE}/terms`,
      lastModified: POLICY_DATE,
      changeFrequency: "yearly",
      priority: 0.4,
    },
    {
      url: `${BASE}/support`,
      lastModified: RELEASE_DATE,
      changeFrequency: "monthly",
      priority: 0.5,
    },
  ];
}
