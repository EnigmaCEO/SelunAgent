import { ImageResponse } from "next/og";

/**
 * The social preview card for selun.sagitta.systems, shared by
 * opengraph-image.tsx and twitter-image.tsx so the two cannot drift apart.
 *
 * It leads with Selun AI, the conversational front door for people, and keeps
 * the agent path (x402) as the second line: both reach the same services.
 */
export const SOCIAL_CARD_SIZE = { width: 1200, height: 630 };

export function renderSocialCard(): ImageResponse {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          padding: "56px 64px",
          background:
            "radial-gradient(circle at 15% 25%, #e9f8ff 0%, #d9efff 38%, #c6e2f8 62%, #b5d6ee 100%)",
          color: "#0f2742",
          fontFamily: "Arial, sans-serif",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
          <div
            style={{
              width: 72,
              height: 72,
              borderRadius: 18,
              border: "2px solid #7dc2ef",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              background: "rgba(255,255,255,0.66)",
              fontSize: 36,
              fontWeight: 700,
              color: "#1d6cb0",
            }}
          >
            S
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ fontSize: 34, fontWeight: 700, letterSpacing: 6, color: "#0d4f84" }}>SELUN AI</div>
            <div style={{ fontSize: 22, color: "#2e5270" }}>Onchain intelligence for people and agents</div>
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <div style={{ fontSize: 68, fontWeight: 700, lineHeight: 1.05 }}>Ask about crypto in your own words.</div>
          <div style={{ fontSize: 30, color: "#2b4f70", lineHeight: 1.3 }}>
            Risk, market regime, and portfolio analysis, priced upfront. Pay by PayPal, card, or crypto wallet.
          </div>
        </div>

        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            fontSize: 22,
            color: "#31597a",
          }}
        >
          <span>selun.sagitta.systems</span>
          <span>Agents: x402 pay-per-call in USDC on Base</span>
        </div>
      </div>
    ),
    SOCIAL_CARD_SIZE,
  );
}
