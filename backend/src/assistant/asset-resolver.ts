/**
 * Asset-name and symbol resolution for the assistant.
 *
 * Selun's scorecard and rebalance endpoints take ticker symbols. This resolver
 * turns what customers actually type ("bitcoin", "ether", "$eth") into those
 * symbols, and refuses to guess when a name could mean more than one asset.
 */

export type KnownAsset = {
  symbol: string;
  name: string;
  coingeckoId: string;
  aliases?: string[];
};

export type AssetResolution =
  | { status: "resolved"; query: string; asset: KnownAsset }
  | { status: "ambiguous"; query: string; options: KnownAsset[]; reason: string }
  | { status: "unknown"; query: string };

const KNOWN_ASSETS: KnownAsset[] = [
  { symbol: "BTC", name: "Bitcoin", coingeckoId: "bitcoin", aliases: ["xbt", "btc", "bitcoin"] },
  { symbol: "ETH", name: "Ethereum", coingeckoId: "ethereum", aliases: ["ether", "eth"] },
  { symbol: "USDT", name: "Tether", coingeckoId: "tether", aliases: ["tether usd"] },
  { symbol: "USDC", name: "USD Coin", coingeckoId: "usd-coin", aliases: ["usdc coin"] },
  { symbol: "DAI", name: "Dai", coingeckoId: "dai" },
  { symbol: "BNB", name: "BNB", coingeckoId: "binancecoin", aliases: ["binance coin"] },
  { symbol: "SOL", name: "Solana", coingeckoId: "solana" },
  { symbol: "XRP", name: "XRP", coingeckoId: "ripple", aliases: ["ripple"] },
  { symbol: "ADA", name: "Cardano", coingeckoId: "cardano" },
  { symbol: "DOGE", name: "Dogecoin", coingeckoId: "dogecoin", aliases: ["doge coin"] },
  { symbol: "TRX", name: "TRON", coingeckoId: "tron" },
  { symbol: "TON", name: "Toncoin", coingeckoId: "the-open-network", aliases: ["the open network"] },
  { symbol: "AVAX", name: "Avalanche", coingeckoId: "avalanche-2" },
  { symbol: "DOT", name: "Polkadot", coingeckoId: "polkadot" },
  { symbol: "LINK", name: "Chainlink", coingeckoId: "chainlink" },
  { symbol: "LTC", name: "Litecoin", coingeckoId: "litecoin" },
  { symbol: "BCH", name: "Bitcoin Cash", coingeckoId: "bitcoin-cash" },
  { symbol: "ETC", name: "Ethereum Classic", coingeckoId: "ethereum-classic" },
  { symbol: "WBTC", name: "Wrapped Bitcoin", coingeckoId: "wrapped-bitcoin" },
  { symbol: "UNI", name: "Uniswap", coingeckoId: "uniswap" },
  { symbol: "AAVE", name: "Aave", coingeckoId: "aave" },
  { symbol: "MKR", name: "Maker", coingeckoId: "maker" },
  { symbol: "ATOM", name: "Cosmos", coingeckoId: "cosmos" },
  { symbol: "NEAR", name: "NEAR Protocol", coingeckoId: "near", aliases: ["near"] },
  { symbol: "ARB", name: "Arbitrum", coingeckoId: "arbitrum" },
  { symbol: "OP", name: "Optimism", coingeckoId: "optimism" },
  { symbol: "SUI", name: "Sui", coingeckoId: "sui" },
  { symbol: "APT", name: "Aptos", coingeckoId: "aptos" },
  { symbol: "XLM", name: "Stellar", coingeckoId: "stellar" },
  { symbol: "HBAR", name: "Hedera", coingeckoId: "hedera-hashgraph" },
  { symbol: "PEPE", name: "Pepe", coingeckoId: "pepe" },
  { symbol: "SHIB", name: "Shiba Inu", coingeckoId: "shiba-inu" },
  { symbol: "POL", name: "Polygon", coingeckoId: "polygon-ecosystem-token" },
  { symbol: "MATIC", name: "Polygon (legacy MATIC)", coingeckoId: "matic-network" },
  { symbol: "LUNA", name: "Terra", coingeckoId: "terra-luna-2" },
  { symbol: "LUNC", name: "Terra Classic", coingeckoId: "terra-luna" },
];

/** Names that map to several real assets. Asking beats silently picking one. */
const AMBIGUOUS_TERMS: Record<string, { symbols: string[]; reason: string }> = {
  polygon: { symbols: ["POL", "MATIC"], reason: "Polygon migrated from MATIC to POL." },
  luna: { symbols: ["LUNA", "LUNC"], reason: "Luna can mean the current Terra token or Terra Classic." },
  terra: { symbols: ["LUNA", "LUNC"], reason: "Terra can mean the current token or Terra Classic." },
  dollar: { symbols: ["USDC", "USDT", "DAI"], reason: "Several stablecoins track the US dollar." },
  usd: { symbols: ["USDC", "USDT", "DAI"], reason: "Several stablecoins track the US dollar." },
  stablecoin: { symbols: ["USDC", "USDT", "DAI"], reason: "Several stablecoins are available." },
  classic: { symbols: ["ETC", "LUNC"], reason: "More than one asset has a Classic version." },
  wrapped: { symbols: ["WBTC"], reason: "Please confirm which wrapped asset you mean." },
};

function normalize(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/^\$/, "")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const LOOKUP = new Map<string, KnownAsset>();
for (const asset of KNOWN_ASSETS) {
  for (const key of [asset.symbol, asset.name, asset.coingeckoId, ...(asset.aliases ?? [])]) {
    LOOKUP.set(normalize(key), asset);
  }
}

function bySymbol(symbol: string): KnownAsset {
  const asset = KNOWN_ASSETS.find((entry) => entry.symbol === symbol);
  if (!asset) throw new Error(`Unknown asset symbol in resolver table: ${symbol}`);
  return asset;
}

export function resolveAsset(query: string): AssetResolution {
  const key = normalize(query);
  if (!key || key.length > 64) return { status: "unknown", query };

  const ambiguous = AMBIGUOUS_TERMS[key];
  if (ambiguous) {
    return {
      status: "ambiguous",
      query,
      options: ambiguous.symbols.map(bySymbol),
      reason: ambiguous.reason,
    };
  }

  const exact = LOOKUP.get(key);
  if (exact) return { status: "resolved", query, asset: exact };

  // Drop filler ("bitcoin price", "the ethereum token"), but only accept the
  // result when one asset remains. A longer name is tried first so "bitcoin
  // cash" can never collapse to Bitcoin.
  const words = key
    .split(" ")
    .filter((word) => !["the", "coin", "token", "crypto", "price", "market", "network"].includes(word));
  const candidate = LOOKUP.get(words.join(" "));
  if (candidate) return { status: "resolved", query, asset: candidate };

  return { status: "unknown", query };
}

/** Also ordinary words, so they only count in capitals: "near the highs" names no asset. */
const WORDS_THAT_NEED_CAPITALS = new Set([
  "apt", "arb", "atom", "avalanche", "cosmos", "dai", "dot", "link", "maker", "near",
  "op", "optimism", "pepe", "pol", "sol", "stellar", "sui", "the open network", "ton", "uni",
]);

// Longest first, so "bitcoin cash" can never also count as Bitcoin. Ambiguous names are
// left out: the model resolves those and the customer chooses.
const MENTION_PATTERNS = [...LOOKUP.entries()]
  .filter(([key]) => !AMBIGUOUS_TERMS[key])
  .sort(([left], [right]) => right.length - left.length)
  .map(([key, asset]) => ({
    asset,
    pattern: WORDS_THAT_NEED_CAPITALS.has(key)
      ? new RegExp(`\\b${key.toUpperCase()}\\b`, "g")
      : new RegExp(`\\b${key}\\b`, "gi"),
  }));

/** Assets a customer's message names outright. Only adds a quote, so a false match costs nothing. */
export function findAssetMentions(text: string): KnownAsset[] {
  let remaining = text;
  const found = new Map<string, KnownAsset>();
  for (const { asset, pattern } of MENTION_PATTERNS) {
    const next = remaining.replace(pattern, " ");
    if (next !== remaining) found.set(asset.symbol, asset);
    remaining = next;
  }
  return [...found.values()];
}

export function assetLabel(asset: KnownAsset): string {
  return asset.name === asset.symbol ? asset.symbol : `${asset.name} (${asset.symbol})`;
}
