import { isAddress, type Address } from "viem";

export const EXECUTION_MODEL_VERSION = "Selun-1.0.0" as const;
export const SUPPORTED_BASE_NETWORKS = ["base-mainnet", "base-sepolia"] as const;
export type SupportedBaseNetwork = (typeof SUPPORTED_BASE_NETWORKS)[number];

export type SelunBackendConfig = {
  coinbaseApiKey: string;
  coinbaseApiSecret: string;
  coinbaseWalletSecret: string;
  agentWalletId: string;
  baseRpc: string;
  usdcContractAddress: Address;
  networkId: SupportedBaseNetwork;
  structuredAllocationPriceUsdc: number;
  certifiedDecisionRecordFeeUsdc: number;
  paymentConfirmations: number;
  paymentPollIntervalMs: number;
  paymentTimeoutMs: number;
};

let cachedConfig: SelunBackendConfig | null = null;

const requireEnv = (name: string): string => {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
};

const normalizeSecret = (value: string): string => {
  const withoutWrappingQuotes =
    (value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))
      ? value.slice(1, -1)
      : value;

  return withoutWrappingQuotes.replace(/\\n/g, "\n");
};

const parsePositiveInt = (value: string | undefined, fallback: number): number => {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const parseNonNegativeFloat = (value: string | undefined, fallback: number): number => {
  const parsed = Number.parseFloat(value ?? "");
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
};

const parseNetwork = (value: string | undefined): SupportedBaseNetwork => {
  const candidate = value?.trim() as SupportedBaseNetwork | undefined;
  return candidate && SUPPORTED_BASE_NETWORKS.includes(candidate) ? candidate : "base-mainnet";
};

export function getConfig(): SelunBackendConfig {
  if (cachedConfig) return cachedConfig;

  const coinbaseApiKey = requireEnv("COINBASE_API_KEY");
  const coinbaseApiSecret = normalizeSecret(requireEnv("COINBASE_API_SECRET"));
  const networkId = parseNetwork(process.env.NETWORK_ID);
  const baseRpc =
    networkId === "base-mainnet"
      ? requireEnv("BASE_MAINNET_RPC")
      : requireEnv("BASE_SEPOLIA_RPC");
  const usdcContractRaw = requireEnv("USDC_CONTRACT_ADDRESS");
  const agentWalletId = process.env.AGENT_WALLET_ID?.trim() || `selun-agent-${networkId}`;
  const coinbaseWalletSecretRaw = process.env.COINBASE_WALLET_SECRET?.trim();
  const coinbaseWalletSecret = coinbaseWalletSecretRaw
    ? normalizeSecret(coinbaseWalletSecretRaw)
    : coinbaseApiSecret;

  if (!isAddress(usdcContractRaw)) {
    throw new Error("USDC_CONTRACT_ADDRESS must be a valid EVM address.");
  }

  cachedConfig = {
    coinbaseApiKey,
    coinbaseApiSecret,
    coinbaseWalletSecret,
    agentWalletId,
    baseRpc,
    usdcContractAddress: usdcContractRaw,
    networkId,
    structuredAllocationPriceUsdc: parseNonNegativeFloat(process.env.STRUCTURED_ALLOCATION_PRICE_USDC, 19),
    certifiedDecisionRecordFeeUsdc: parseNonNegativeFloat(process.env.CERTIFIED_DECISION_RECORD_FEE_USDC, 15),
    paymentConfirmations: Math.min(parsePositiveInt(process.env.PAYMENT_CONFIRMATIONS, 2), 2),
    paymentPollIntervalMs: parsePositiveInt(process.env.PAYMENT_POLL_INTERVAL_MS, 4000),
    paymentTimeoutMs: parsePositiveInt(process.env.PAYMENT_TIMEOUT_MS, 120000),
  };

  return cachedConfig;
}
