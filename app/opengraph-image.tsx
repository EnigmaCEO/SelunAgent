import { ImageResponse } from "next/og";

export const runtime = "edge";
export const size = {
  width: 1200,
  height: 630,
};
export const contentType = "image/png";

export default function OpenGraphImage() {
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
            <div style={{ fontSize: 34, fontWeight: 700, letterSpacing: 6, color: "#0d4f84" }}>SELUN</div>
            <div style={{ fontSize: 22, color: "#2e5270" }}>Autonomous Portfolio Agent</div>
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div style={{ fontSize: 76, fontWeight: 700, lineHeight: 1.05 }}>Simple Crypto Allocation</div>
          <div style={{ fontSize: 34, color: "#2b4f70" }}>Powered by market intelligence.</div>
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
          <span>Built on AAA v4</span>
        </div>
      </div>
    ),
    size,
  );
}
