export const PORTFOLIO_SEGMENTS = ["Bluechips", "Memecoins", "Gaming", "Yield Farm"] as const;

export type PortfolioSegment = (typeof PORTFOLIO_SEGMENTS)[number];

function normalizeSegmentString(value: string): string {
  return value.trim().toLowerCase().replace(/[_-]+/g, " ");
}

export function normalizePortfolioSegment(value: unknown): PortfolioSegment | null {
  if (typeof value !== "string") return null;

  const normalized = normalizeSegmentString(value);
  if (normalized === "bluechips" || normalized === "blue chips" || normalized === "bluechip") return "Bluechips";
  if (normalized === "memecoins" || normalized === "meme coins" || normalized === "meme coin" || normalized === "memecoin") {
    return "Memecoins";
  }
  if (normalized === "gaming" || normalized === "gaming tokens" || normalized === "gaming token") return "Gaming";
  if (
    normalized === "yield farm" ||
    normalized === "yield farms" ||
    normalized === "yield farming" ||
    normalized === "yield"
  ) {
    return "Yield Farm";
  }

  return null;
}

export function portfolioSegmentKey(segment: PortfolioSegment): string {
  switch (segment) {
    case "Bluechips":
      return "bluechips";
    case "Memecoins":
      return "memecoins";
    case "Gaming":
      return "gaming";
    case "Yield Farm":
      return "yield_farm";
  }
}
