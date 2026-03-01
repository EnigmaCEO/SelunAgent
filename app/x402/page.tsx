"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import styles from "./x402.module.css";

type ResourceCard = {
  endpoint: string;
  method: string;
  productId: string;
  title: string;
  description: string;
  pricing: {
    amountUsdc: string;
    price: string;
  };
  inputSchema?: {
    required?: string[];
    properties?: Record<string, unknown>;
  };
};

type CapabilitiesPayload = {
  pricing: Record<string, unknown>;
  resources: ResourceCard[];
  paymentTransport?: {
    facilitatorUrl?: string;
    headers?: Record<string, string>;
  };
  discovery?: {
    network?: string;
    caip2Network?: string;
  };
};

const PUBLIC_RESOURCE_DESCRIPTIONS: Record<string, string> = {
  "/agent/x402/allocate":
    "Portfolio allocation engine. Call this when an automated portfolio workflow needs a target crypto allocation for a specified risk tolerance and timeframe. Returns an accepted allocation job with a statusPath for asynchronous completion.",
  "/agent/x402/allocate-with-report":
    "Portfolio allocation and report engine. Call this when an automated portfolio workflow needs the target allocation plus a certified decision record. Returns an accepted allocation job with a statusPath for asynchronous completion.",
  "/agent/x402/market-regime":
    "Market regime classifier. Call this before allocation or rebalancing. Returns volatility, liquidity, sentiment, and allocation authorization inputs for automated portfolio workflows.",
  "/agent/x402/policy-envelope":
    "Risk policy engine. Call this when portfolio constraints must be computed for a supplied risk tolerance and timeframe. Returns exposure caps, stablecoin floor, risk budget, and authorization constraints.",
  "/agent/x402/asset-scorecard":
    "Asset scoring engine. Call this before asset selection or allocation. Returns liquidity, structural stability, role classification, and composite quality scores for candidate assets.",
  "/agent/x402/rebalance":
    "Portfolio rebalance engine. Call this after allocation or on a monitoring schedule with current holdings. Returns target-vs-current drift and the adjustments required to rebalance within policy constraints.",
};

const FALLBACK_CAPABILITIES: CapabilitiesPayload = {
  pricing: {
    allocationOnlyUsdc: "19",
    allocationWithReportUsdc: "34",
    marketRegimeUsdc: "0.25",
    policyEnvelopeUsdc: "0.25",
    assetScorecardUsdc: "0.5",
    rebalanceUsdc: "1",
  },
  paymentTransport: {
    facilitatorUrl: "https://api.cdp.coinbase.com/platform/v2/x402",
    headers: {
      paymentRequired: "PAYMENT-REQUIRED",
      paymentSignature: "PAYMENT-SIGNATURE",
      paymentResponse: "PAYMENT-RESPONSE",
    },
  },
  discovery: {
    network: "base-mainnet",
    caip2Network: "eip155:8453",
  },
  resources: [
    {
      endpoint: "/agent/x402/allocate",
      method: "POST",
      productId: "allocate",
      title: "Selun Allocation",
      description: PUBLIC_RESOURCE_DESCRIPTIONS["/agent/x402/allocate"],
      pricing: { amountUsdc: "19", price: "$19.00" },
      inputSchema: { required: ["decisionId", "riskTolerance", "timeframe"] },
    },
    {
      endpoint: "/agent/x402/allocate-with-report",
      method: "POST",
      productId: "allocate_with_report",
      title: "Selun Allocation With Report",
      description: PUBLIC_RESOURCE_DESCRIPTIONS["/agent/x402/allocate-with-report"],
      pricing: { amountUsdc: "34", price: "$34.00" },
      inputSchema: { required: ["decisionId", "riskTolerance", "timeframe"] },
    },
    {
      endpoint: "/agent/x402/market-regime",
      method: "POST",
      productId: "market_regime",
      title: "Selun Market Regime",
      description: PUBLIC_RESOURCE_DESCRIPTIONS["/agent/x402/market-regime"],
      pricing: { amountUsdc: "0.25", price: "$0.25" },
      inputSchema: { required: ["decisionId", "riskTolerance", "timeframe"] },
    },
    {
      endpoint: "/agent/x402/policy-envelope",
      method: "POST",
      productId: "policy_envelope",
      title: "Selun Policy Envelope",
      description: PUBLIC_RESOURCE_DESCRIPTIONS["/agent/x402/policy-envelope"],
      pricing: { amountUsdc: "0.25", price: "$0.25" },
      inputSchema: { required: ["decisionId", "riskTolerance", "timeframe"] },
    },
    {
      endpoint: "/agent/x402/asset-scorecard",
      method: "POST",
      productId: "asset_scorecard",
      title: "Selun Asset Scorecard",
      description: PUBLIC_RESOURCE_DESCRIPTIONS["/agent/x402/asset-scorecard"],
      pricing: { amountUsdc: "0.5", price: "$0.50" },
      inputSchema: { required: ["decisionId", "riskTolerance", "timeframe"] },
    },
    {
      endpoint: "/agent/x402/rebalance",
      method: "POST",
      productId: "rebalance",
      title: "Selun Rebalance",
      description: PUBLIC_RESOURCE_DESCRIPTIONS["/agent/x402/rebalance"],
      pricing: { amountUsdc: "1", price: "$1.00" },
      inputSchema: { required: ["decisionId", "riskTolerance", "timeframe", "holdings"] },
    },
  ],
};

type LoadState = "loading" | "live" | "fallback";

function endpointKind(resource: ResourceCard): string {
  if (resource.endpoint.includes("allocate")) return "Async";
  return "Sync";
}

function normalizePublicDescription(description: string): string {
  return description
    .replace(/\bSelun\S*\s+Phase\s+\d+\s+/gi, "Selun ")
    .replace(/\bPhase\s+\d+\s+/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function sentenceCaseFirst(value: string): string {
  if (!value) return value;
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function publicDescriptionFor(resource: ResourceCard): string {
  const canonical = PUBLIC_RESOURCE_DESCRIPTIONS[resource.endpoint];
  if (canonical) return canonical;
  return sentenceCaseFirst(normalizePublicDescription(resource.description));
}

function requiredFields(resource: ResourceCard): string[] {
  const required = resource.inputSchema?.required;
  return Array.isArray(required) ? required : [];
}

export default function X402Page() {
  const [capabilities, setCapabilities] = useState<CapabilitiesPayload>(FALLBACK_CAPABILITIES);
  const [state, setState] = useState<LoadState>("loading");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const response = await fetch("/agent/x402/capabilities", {
          method: "GET",
          headers: {
            Accept: "application/json",
          },
        });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = (await response.json()) as { data?: CapabilitiesPayload };
        if (!cancelled && payload?.data?.resources?.length) {
          setCapabilities(payload.data);
          setState("live");
        }
      } catch {
        if (!cancelled) {
          setCapabilities(FALLBACK_CAPABILITIES);
          setState("fallback");
        }
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  const discoveryRoot = useMemo(() => {
    if (typeof window !== "undefined") {
      return window.location.origin;
    }
    return "https://selun.sagitta.systems";
  }, []);

  const resources = (capabilities.resources ?? FALLBACK_CAPABILITIES.resources).map((resource) => ({
    ...resource,
    description: publicDescriptionFor(resource),
  }));

  return (
    <main className={styles.page}>
      <div className={styles.aura} aria-hidden />
      <div className={styles.mesh} aria-hidden />

      <div className={styles.shell}>
        <header className={styles.header}>
          <Link href="/" className={styles.brand}>
            <Image src="/selun-logo.svg" alt="Selun" width={154} height={48} className={styles.brandLogo} priority />
          </Link>

          
        </header>

        <section className={styles.hero}>
          <div className={styles.heroCopy}>
            <p className={styles.eyebrow}>x402 Machine Registry</p>
            <h1>Selun Public Endpoints</h1>
            <p className={styles.subhead}>
              Payment-gated x402 endpoints exposing Sagitta AAA&apos;s quant allocator, market regime engine, policy envelope logic, asset scorecard, and rebalancing engine.
            </p>
          </div>

          <div className={styles.heroMeta}>
            <div className={styles.metric}>
              <span>Resources</span>
              <strong>{resources.length}</strong>
            </div>
            <div className={styles.metric}>
              <span>Network</span>
              <strong>{capabilities.discovery?.network ?? "base-mainnet"}</strong>
            </div>
            <div className={styles.metric}>
              <span>Catalog</span>
              <strong>{state === "live" ? "Live" : state === "loading" ? "Loading" : "Fallback"}</strong>
            </div>
          </div>
        </section>

        <section className={styles.discoveryBand}>
          <div className={styles.discoveryPanel}>
            <p className={styles.discoveryLabel}>Discovery Root</p>
            <code>{discoveryRoot}/.well-known/x402</code>
          </div>
          <div className={styles.discoveryPanel}>
            <p className={styles.discoveryLabel}>Transport</p>
            <code>
              {(capabilities.paymentTransport?.headers?.paymentRequired ?? "PAYMENT-REQUIRED")} /{" "}
              {(capabilities.paymentTransport?.headers?.paymentSignature ?? "PAYMENT-SIGNATURE")}
            </code>
          </div>
          <div className={styles.discoveryPanel}>
            <p className={styles.discoveryLabel}>Facilitator</p>
            <code>{capabilities.paymentTransport?.facilitatorUrl ?? "https://api.cdp.coinbase.com/platform/v2/x402"}</code>
          </div>
        </section>

        <section className={styles.catalogSection}>
          <div className={styles.sectionHeader}>
            <div>
              <p className={styles.sectionEyebrow}>Catalog</p>
              <h2>Endpoint Pricing</h2>
            </div>
            <p className={styles.sectionNote}>
              Allocation endpoints are async and return a `statusPath`. Tool endpoints settle and return the result immediately.
            </p>
          </div>

          <div className={styles.cardGrid}>
            {resources.map((resource) => (
              <article key={resource.endpoint} className={styles.card}>
                <div className={styles.cardTop}>
                  <div>
                    <span className={styles.method}>{resource.method}</span>
                    <h3>{resource.title}</h3>
                  </div>
                  <div className={styles.priceBlock}>
                    <span>{resource.pricing.amountUsdc} USDC</span>
                    <strong>{resource.pricing.price}</strong>
                  </div>
                </div>

                <p className={styles.description}>{resource.description}</p>

                <div className={styles.pathRow}>
                  <code>{resource.endpoint}</code>
                  <span className={styles.kind}>{endpointKind(resource)}</span>
                </div>

                <div className={styles.metaRow}>
                  <div>
                    <span className={styles.metaLabel}>Inputs</span>
                    <p>{requiredFields(resource).join(", ") || "See capabilities"}</p>
                  </div>
                  <div>
                    <span className={styles.metaLabel}>Product ID</span>
                    <p>{resource.productId}</p>
                  </div>
                </div>
              </article>
            ))}
          </div>
        </section>

        <section className={styles.integrationSection}>
          <div className={styles.sectionHeader}>
            <div>
              <p className={styles.sectionEyebrow}>Integration</p>
              <h2>Scan and Consume</h2>
            </div>
          </div>

          <div className={styles.integrationGrid}>
            <div className={styles.integrationCard}>
              <span className={styles.integrationLabel}>x402scan origin</span>
              <code>{discoveryRoot}</code>
            </div>
            <div className={styles.integrationCard}>
              <span className={styles.integrationLabel}>Capabilities</span>
              <code>{discoveryRoot}/agent/x402/capabilities</code>
            </div>
            <div className={styles.integrationCard}>
              <span className={styles.integrationLabel}>Discovery alias</span>
              <code>{discoveryRoot}/agent/x402/discovery</code>
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
