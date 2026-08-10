import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { CdpClient, type EvmServerAccount } from "@coinbase/cdp-sdk";
import {
  createPublicClient,
  createWalletClient,
  encodeFunctionData,
  formatUnits,
  http,
  isAddress,
  parseAbiItem,
  parseUnits,
  stringToHex,
  type Address,
  type Hex,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { base, baseSepolia } from "viem/chains";
import { appendBuilderCode } from "../builder-code";
import { getConfig } from "../config";
import { emitExecutionLog } from "../logging/execution-logs";
import { resolveBackendDataFilePath } from "../runtime-paths";

const USDC_DECIMALS = 6;
const RPC_READ_TIMEOUT_MS = 12_000;
const BALANCE_CACHE_TTL_MS = 10_000;
const NATIVE_ETH_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE" as Address;
const IDENTITY_PATH = resolveBackendDataFilePath("agent-identity.json");
const PROMO_CODE_REDEMPTIONS_PATH = resolveBackendDataFilePath("free-code-redemptions.json");
const TRANSFER_EVENT = parseAbiItem("event Transfer(address indexed from, address indexed to, uint256 value)");
const ERC20_BALANCE_OF_ABI = [
  {
    type: "function",
    name: "balanceOf",
    stateMutability: "view",
    inputs: [{ name: "account", type: "address" }],
    outputs: [{ name: "balance", type: "uint256" }],
  },
] as const;
const ERC20_APPROVE_ABI = [
  {
    type: "function",
    name: "approve",
    stateMutability: "nonpayable",
    inputs: [
      { name: "spender", type: "address" },
      { name: "value", type: "uint256" },
    ],
    outputs: [{ name: "", type: "bool" }],
  },
] as const;
type PersistedIdentity = {
  agentId: string;
  walletAddress: Address;
  network: string;
};

type AgentIdentity = {
  agentId: string;
  walletAddress: Address;
  network: string;
};

type AgentRuntimeContext = {
  walletProvider: CdpWalletRuntime;
  identity: AgentIdentity;
};

type PromoCodeKind = "free" | "percent_discount";

type PromoCodeRule = {
  code: string;
  maxUses: number;
  includeCertifiedDecisionRecord: boolean;
  discountPercent: number;
  expiresAt?: string;
};

type PromoCodeRedemption = {
  code: string;
  promoKind: PromoCodeKind;
  discountPercent: number;
  walletAddress: Address;
  decisionId: string;
  transactionId: string;
  redeemedAt: string;
  includeCertifiedDecisionRecord: boolean;
  amountBeforeDiscountUsdc: string;
  chargedAmountUsdc: string;
};

type PromoCodeRedemptionStore = {
  redemptions: PromoCodeRedemption[];
};

export type PaymentVerificationInput = {
  fromAddress: string;
  expectedAmountUSDC: number | string;
  transactionHash?: string;
  decisionId?: string;
};

export type PaymentReceipt = {
  transactionHash: Hex;
  amount: string;
  confirmed: true;
  blockNumber: number;
};

export type StoreDecisionHashInput = {
  decisionId: string;
  pdfHash: string;
};

export type AgentWalletSnapshot = {
  agentId: string;
  walletAddress: Address;
  network: string;
  usdcContractAddress: Address;
  nativeBalance: string;
  nativeBalanceBaseUnits: string;
  usdcBalance: string;
  usdcBalanceBaseUnits: string;
};

export type ProjectSellerWalletSnapshot = {
  agentId: string | null;
  walletAddress: Address;
  network: string;
  usdcContractAddress: Address;
  nativeBalance: string;
  nativeBalanceBaseUnits: string;
  usdcBalance: string;
  usdcBalanceBaseUnits: string;
  policies: string[];
  active: boolean;
};

export type TreasuryOwnerWalletSnapshot = {
  name: string;
  walletAddress: Address;
  network: string;
  usdcContractAddress: Address;
  nativeBalance: string;
  nativeBalanceBaseUnits: string;
  usdcBalance: string;
  usdcBalanceBaseUnits: string;
  policies: string[];
};

export type TreasurySmartAccountSnapshot = {
  name: string;
  walletAddress: Address;
  ownerAddresses: Address[];
  network: string;
  usdcContractAddress: Address;
  nativeBalance: string;
  nativeBalanceBaseUnits: string;
  usdcBalance: string;
  usdcBalanceBaseUnits: string;
  policies: string[];
  paymasterUrl: string | null;
  sponsorshipMode: "configured_paymaster" | "manual";
};

export type TreasuryWalletSetupSnapshot = {
  ownerName: string;
  smartAccountName: string;
  paymasterUrl: string | null;
  solanaAddress: string | null;
  owner: TreasuryOwnerWalletSnapshot | null;
  smartAccount: TreasurySmartAccountSnapshot | null;
};

export type SellerUsdcTransferInput = {
  sellerAddress?: string;
  toAddress: string;
  amountUsdc: number | string;
  note?: string;
};

export type TreasuryUsdcTransferInput = {
  toAddress: string;
  amountUsdc: number | string;
  note?: string;
};

export type SellerUsdcToEthSwapInput = {
  sellerAddress?: string;
  amountUsdc: number | string;
  slippageBps?: number;
  note?: string;
};

export type SellerNativeTransferInput = {
  fromSellerAddress?: string;
  toAddress: string;
  amountEth: number | string;
  note?: string;
};

export type AdminNativeFundingWalletSnapshot = {
  walletAddress: Address;
  network: string;
  nativeBalance: string;
  nativeBalanceBaseUnits: string;
};

export type SellerUsdcTransferResult = {
  sellerWalletAddress: Address;
  network: string;
  usdcContractAddress: Address;
  toAddress: Address;
  amountUsdc: string;
  amountBaseUnits: string;
  note?: string;
  transactionHash: Hex;
  nativeBalanceBefore: string;
  nativeBalanceAfter: string;
  usdcBalanceBefore: string;
  usdcBalanceAfter: string;
};

export type TreasuryUsdcTransferResult = {
  treasuryWalletAddress: Address;
  network: string;
  usdcContractAddress: Address;
  toAddress: Address;
  amountUsdc: string;
  amountBaseUnits: string;
  note?: string;
  userOpHash: Hex;
  transactionHash: Hex;
  nativeBalanceBefore: string;
  nativeBalanceAfter: string;
  usdcBalanceBefore: string;
  usdcBalanceAfter: string;
};

export type SellerUsdcToEthSwapResult = {
  sellerWalletAddress: Address;
  network: string;
  fromTokenAddress: Address;
  toTokenAddress: Address;
  amountUsdc: string;
  amountBaseUnits: string;
  estimatedToAmountEth: string;
  minimumToAmountEth: string;
  slippageBps: number;
  note?: string;
  transactionHash: Hex;
  approvalTransactionHash?: Hex;
  nativeBalanceBefore: string;
  nativeBalanceAfter: string;
  usdcBalanceBefore: string;
  usdcBalanceAfter: string;
};

export type SellerNativeTransferResult = {
  fromWalletAddress: Address;
  network: string;
  toAddress: Address;
  amountEth: string;
  amountBaseUnits: string;
  note?: string;
  transactionHash: Hex;
  nativeBalanceBefore: string;
  nativeBalanceAfter: string;
};

export type AuthorizeWizardPaymentInput = {
  walletAddress: string;
  includeCertifiedDecisionRecord?: boolean;
  riskMode?: string;
  investmentHorizon?: string;
  promoCode?: string;
};

export type QuoteWizardPaymentInput = {
  walletAddress: string;
  includeCertifiedDecisionRecord?: boolean;
  promoCode?: string;
};

export type WizardPricing = {
  structuredAllocationPriceUsdc: number;
  certifiedDecisionRecordFeeUsdc: number;
};

export type AuthorizeWizardPaymentResult = {
  status: "paid";
  transactionId: Hex;
  decisionId: string;
  agentNote: string;
  chargedAmountUsdc: string;
  certifiedDecisionRecordPurchased: boolean;
  paymentMethod?: "onchain" | "free_code";
  freeCodeApplied?: boolean;
};

export type QuoteWizardPaymentResult = {
  totalBeforeDiscountUsdc: string;
  chargedAmountUsdc: string;
  discountAmountUsdc: string;
  discountPercent: number;
  promoCodeApplied: boolean;
  promoCode?: string;
  certifiedDecisionRecordPurchased: boolean;
  paymentMethod: "onchain" | "free_code";
  message: string;
};

let cachedRuntime: AgentRuntimeContext | null = null;
const balanceCache = new Map<
  string,
  {
    expiresAt: number;
    nativeBalance: bigint;
    usdcBalance: bigint;
  }
>();

function getBalanceCacheKey(address: Address, config = getConfig()): string {
  return `${config.networkId}:${config.baseRpc.toLowerCase()}:${config.usdcContractAddress.toLowerCase()}:${address.toLowerCase()}`;
}

function invalidateBalanceCache(address: Address, config = getConfig()) {
  balanceCache.delete(getBalanceCacheKey(address, config));
}

function ensureIdentityDir() {
  fs.mkdirSync(path.dirname(IDENTITY_PATH), { recursive: true });
}

function ensurePromoCodeRedemptionsDir() {
  fs.mkdirSync(path.dirname(PROMO_CODE_REDEMPTIONS_PATH), { recursive: true });
}

function isPromoCodeKind(value: unknown): value is PromoCodeKind {
  return value === "free" || value === "percent_discount";
}

function parseDiscountPercent(value: unknown, fallback = 100): number {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number.parseFloat(value.trim())
        : Number.NaN;

  if (!Number.isFinite(parsed)) return fallback;
  if (parsed <= 0 || parsed > 100) return fallback;
  return Number(parsed.toFixed(2));
}

function readPromoCodeRedemptions(): PromoCodeRedemption[] {
  if (!fs.existsSync(PROMO_CODE_REDEMPTIONS_PATH)) return [];
  try {
    const parsed = JSON.parse(fs.readFileSync(PROMO_CODE_REDEMPTIONS_PATH, "utf8")) as PromoCodeRedemptionStore;
    if (!Array.isArray(parsed?.redemptions)) return [];
    return parsed.redemptions.filter((entry) =>
      typeof entry?.code === "string" &&
      isAddress(entry?.walletAddress as string) &&
      typeof entry?.decisionId === "string" &&
      typeof entry?.transactionId === "string" &&
      typeof entry?.redeemedAt === "string"
    ).map((entry) => {
      const normalizedCode = normalizeCode(entry.code);
      const normalizedKind: PromoCodeKind = isPromoCodeKind(entry.promoKind)
        ? entry.promoKind
        : entry.transactionId.startsWith("FREE-")
          ? "free"
          : "percent_discount";
      const amountBeforeDiscountUsdc =
        typeof entry.amountBeforeDiscountUsdc === "string" && entry.amountBeforeDiscountUsdc.trim()
          ? entry.amountBeforeDiscountUsdc.trim()
          : "0";
      const chargedAmountUsdc =
        typeof entry.chargedAmountUsdc === "string" && entry.chargedAmountUsdc.trim()
          ? entry.chargedAmountUsdc.trim()
          : normalizedKind === "free"
            ? "0"
            : amountBeforeDiscountUsdc;
      let fallbackDiscountPercent = normalizedKind === "free" ? 100 : 0;
      if (normalizedKind === "percent_discount") {
        const before = Number.parseFloat(amountBeforeDiscountUsdc);
        const charged = Number.parseFloat(chargedAmountUsdc);
        if (Number.isFinite(before) && before > 0 && Number.isFinite(charged) && charged >= 0 && charged <= before) {
          fallbackDiscountPercent = Number((((before - charged) / before) * 100).toFixed(2));
        }
      }
      const discountPercent = parseDiscountPercent(entry.discountPercent, fallbackDiscountPercent);
      return {
        code: normalizedCode,
        promoKind: normalizedKind,
        discountPercent,
        walletAddress: entry.walletAddress,
        decisionId: entry.decisionId,
        transactionId: entry.transactionId,
        redeemedAt: entry.redeemedAt,
        includeCertifiedDecisionRecord: Boolean(entry.includeCertifiedDecisionRecord),
        amountBeforeDiscountUsdc,
        chargedAmountUsdc,
      };
    });
  } catch {
    return [];
  }
}

function writePromoCodeRedemptions(redemptions: PromoCodeRedemption[]) {
  ensurePromoCodeRedemptionsDir();
  const payload: PromoCodeRedemptionStore = { redemptions };
  fs.writeFileSync(PROMO_CODE_REDEMPTIONS_PATH, JSON.stringify(payload, null, 2), "utf8");
}

function normalizeCode(value: string): string {
  return value.trim().toUpperCase();
}

function parsePositiveIntOrDefault(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) return Math.floor(value);
  if (typeof value === "string") {
    const parsed = Number.parseInt(value.trim(), 10);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return fallback;
}

function parseBooleanOrDefault(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
    if (normalized === "false" || normalized === "0" || normalized === "no") return false;
  }
  return fallback;
}

function parsePromoCodeRules(): PromoCodeRule[] {
  const rules: PromoCodeRule[] = [];
  const rawJson = process.env.SELUN_FREE_CODES_JSON?.trim();

  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson) as unknown;
      if (Array.isArray(parsed)) {
        for (const entry of parsed) {
          if (typeof entry !== "object" || entry === null) continue;
          const record = entry as Record<string, unknown>;
          const codeValue = typeof record.code === "string" ? normalizeCode(record.code) : "";
          if (!codeValue) continue;
          const hasExplicitDiscount =
            record.discountPercent !== undefined &&
            record.discountPercent !== null &&
            String(record.discountPercent).trim() !== "";
          const discountPercent = hasExplicitDiscount
            ? parseDiscountPercent(record.discountPercent, Number.NaN)
            : 100;
          if (!Number.isFinite(discountPercent) || discountPercent <= 0 || discountPercent > 100) continue;
          rules.push({
            code: codeValue,
            maxUses: parsePositiveIntOrDefault(record.maxUses, 1),
            includeCertifiedDecisionRecord: parseBooleanOrDefault(record.includeCertifiedDecisionRecord, true),
            discountPercent,
            expiresAt: typeof record.expiresAt === "string" ? record.expiresAt : undefined,
          });
        }
      }
    } catch {
      // Ignore invalid JSON; fallback parsing still applies.
    }
  }

  if (rules.length > 0) return rules;

  const csv = process.env.SELUN_FREE_CODES?.trim();
  if (!csv) return [];

  return csv
    .split(",")
    .map((token) => normalizeCode(token))
    .filter(Boolean)
    .map((code) => ({
      code,
      maxUses: 1,
      includeCertifiedDecisionRecord: true,
      discountPercent: 100,
    }));
}

function createFreeCodeTransactionId(code: string): Hex {
  const suffix = randomUUID().replace(/-/g, "").slice(0, 20).toUpperCase();
  return `FREE-${normalizeCode(code)}-${suffix}` as Hex;
}

function validatePromoCodeRule(rule: PromoCodeRule, now = Date.now()) {
  if (!rule.expiresAt) return;
  const expiresAtMs = Date.parse(rule.expiresAt);
  if (!Number.isFinite(expiresAtMs)) {
    throw new Error(`Free code ${rule.code} has invalid expiresAt format.`);
  }
  if (expiresAtMs < now) {
    throw new Error("Promo code has expired.");
  }
}

function findPromoCodeRedemption(input: { walletAddress: Address; decisionId?: string; transactionId?: string }): PromoCodeRedemption | null {
  const wallet = input.walletAddress.toLowerCase();
  const decision = input.decisionId?.trim();
  const transaction = input.transactionId?.trim();
  const redemptions = readPromoCodeRedemptions();

  for (const entry of redemptions) {
    if (entry.walletAddress.toLowerCase() !== wallet) continue;
    if (decision) {
      if (entry.decisionId !== decision) continue;
      if (transaction && entry.promoKind === "free" && entry.transactionId !== transaction) continue;
      return entry;
    }
    if (transaction) {
      if (entry.transactionId !== transaction) continue;
      return entry;
    }
  }

  return null;
}

type ResolvedPromoCodeGrant = {
  normalizedCode: string;
  rule: PromoCodeRule;
  amountBeforeDiscountUsdc: string;
  chargedAmountUsdc: string;
  discountAmountUsdc: string;
  chargedAmountUsdcBaseUnits: bigint;
  promoKind: PromoCodeKind;
};

function applyDiscountPercent(amountBaseUnits: bigint, discountPercent: number): bigint {
  const discountBps = Math.max(0, Math.min(10000, Math.round(discountPercent * 100)));
  const chargeBps = 10000 - discountBps;
  if (chargeBps <= 0) return 0n;
  return (amountBaseUnits * BigInt(chargeBps)) / 10000n;
}

function resolvePromoCodeGrant(input: {
  walletAddress: Address;
  code: string;
  includeCertifiedDecisionRecord: boolean;
  totalPriceUsdcBaseUnits: bigint;
}): ResolvedPromoCodeGrant {
  const normalizedCode = normalizeCode(input.code);
  if (!normalizedCode) {
    throw new Error("Promo code is empty.");
  }

  const rules = parsePromoCodeRules();
  const rule = rules.find((candidate) => candidate.code === normalizedCode);
  if (!rule) {
    throw new Error("Invalid promo code.");
  }

  validatePromoCodeRule(rule);

  if (input.includeCertifiedDecisionRecord && !rule.includeCertifiedDecisionRecord) {
    throw new Error("This promo code does not include a certified decision report.");
  }

  const redemptions = readPromoCodeRedemptions();
  const redemptionsForCode = redemptions.filter((entry) => normalizeCode(entry.code) === normalizedCode);
  if (redemptionsForCode.length >= rule.maxUses) {
    throw new Error("Promo code usage limit reached.");
  }

  const alreadyRedeemedByWallet = redemptionsForCode.find(
    (entry) => entry.walletAddress.toLowerCase() === input.walletAddress.toLowerCase(),
  );
  if (alreadyRedeemedByWallet) {
    throw new Error("Promo code already redeemed by this wallet.");
  }

  const amountBeforeDiscountUsdc = formatUnits(input.totalPriceUsdcBaseUnits, USDC_DECIMALS);
  const chargedAmountUsdcBaseUnits = applyDiscountPercent(input.totalPriceUsdcBaseUnits, rule.discountPercent);
  const chargedAmountUsdc = formatUnits(chargedAmountUsdcBaseUnits, USDC_DECIMALS);
  const discountAmountUsdc = formatUnits(input.totalPriceUsdcBaseUnits - chargedAmountUsdcBaseUnits, USDC_DECIMALS);
  const promoKind: PromoCodeKind = chargedAmountUsdcBaseUnits === 0n ? "free" : "percent_discount";

  return {
    normalizedCode,
    rule,
    amountBeforeDiscountUsdc,
    chargedAmountUsdc,
    discountAmountUsdc,
    chargedAmountUsdcBaseUnits,
    promoKind,
  };
}

function redeemPromoCodeGrant(input: {
  walletAddress: Address;
  code: string;
  includeCertifiedDecisionRecord: boolean;
  totalPriceUsdcBaseUnits: bigint;
  riskMode?: string;
  investmentHorizon?: string;
}): AuthorizeWizardPaymentResult {
  const resolvedPromo = resolvePromoCodeGrant({
    walletAddress: input.walletAddress,
    code: input.code,
    includeCertifiedDecisionRecord: input.includeCertifiedDecisionRecord,
    totalPriceUsdcBaseUnits: input.totalPriceUsdcBaseUnits,
  });

  const decisionId = `SELUN-DEC-${Date.now()}`;
  const mode = input.riskMode?.trim() || "unspecified risk mode";
  const horizon = input.investmentHorizon?.trim() || "unspecified horizon";
  const includeCert = input.includeCertifiedDecisionRecord && resolvedPromo.rule.includeCertifiedDecisionRecord;
  const transactionId = resolvedPromo.promoKind === "free"
    ? createFreeCodeTransactionId(resolvedPromo.normalizedCode)
    : buildTransactionId();

  const redemptions = readPromoCodeRedemptions();
  redemptions.push({
    code: resolvedPromo.normalizedCode,
    promoKind: resolvedPromo.promoKind,
    discountPercent: resolvedPromo.rule.discountPercent,
    walletAddress: input.walletAddress,
    decisionId,
    transactionId,
    redeemedAt: new Date().toISOString(),
    includeCertifiedDecisionRecord: includeCert,
    amountBeforeDiscountUsdc: resolvedPromo.amountBeforeDiscountUsdc,
    chargedAmountUsdc: resolvedPromo.chargedAmountUsdc,
  });
  writePromoCodeRedemptions(redemptions);

  const promoSummary =
    resolvedPromo.promoKind === "free"
      ? "100% discount (free grant)"
      : `${resolvedPromo.rule.discountPercent}% discount`;

  emitExecutionLog({
    phase: "PAYMENT_AUTH",
    action: "authorize_wizard_payment_promo_code",
    status: "success",
    transactionHash: transactionId,
  });

  return {
    status: "paid",
    transactionId,
    decisionId,
    agentNote: `Selun agent applied promo code ${resolvedPromo.normalizedCode} (${promoSummary}) for ${mode} / ${horizon}.`,
    chargedAmountUsdc: resolvedPromo.chargedAmountUsdc,
    certifiedDecisionRecordPurchased: includeCert,
    paymentMethod: resolvedPromo.promoKind === "free" ? "free_code" : "onchain",
    freeCodeApplied: resolvedPromo.promoKind === "free",
  };
}

function readPersistedIdentity(): PersistedIdentity | null {
  if (!fs.existsSync(IDENTITY_PATH)) return null;

  try {
    return JSON.parse(fs.readFileSync(IDENTITY_PATH, "utf8")) as PersistedIdentity;
  } catch {
    return null;
  }
}

function writePersistedIdentity(identity: PersistedIdentity) {
  ensureIdentityDir();
  fs.writeFileSync(IDENTITY_PATH, JSON.stringify(identity, null, 2), "utf8");
}

function resolveConfiguredWalletAddress(
  agentWalletId: string,
  persisted: PersistedIdentity | null,
  expectedNetwork: string,
): Address | undefined {
  if (isAddress(agentWalletId)) return agentWalletId as Address;
  if (
    persisted?.walletAddress &&
    isAddress(persisted.walletAddress) &&
    persisted.network === expectedNetwork
  ) {
    return persisted.walletAddress;
  }
  return undefined;
}

function normalizeExpectedUsdc(value: number | string): bigint {
  const valueAsString = typeof value === "number" ? value.toString() : value.trim();
  const parsed = Number.parseFloat(valueAsString);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error("expectedAmountUSDC must be greater than zero.");
  }
  return parseUnits(valueAsString, USDC_DECIMALS);
}

function normalizeExpectedNativeAmount(value: number | string): bigint {
  const valueAsString = typeof value === "number" ? value.toString() : value.trim();
  const parsed = Number.parseFloat(valueAsString);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error("amountEth must be greater than zero.");
  }
  return parseUnits(valueAsString, 18);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildDecisionMemo(decisionId: string, pdfHash: string): Hex {
  const memo = `SELUN|${decisionId}|${pdfHash}`.slice(0, 220);
  return stringToHex(memo);
}

function buildTransactionId(): Hex {
  const body = randomUUID().replace(/-/g, "").padEnd(64, "0").slice(0, 64);
  return `0x${body}` as Hex;
}

function calculateTotalPriceUsdcBaseUnits(includeCertifiedDecisionRecord: boolean): bigint {
  const config = getConfig();
  const basePrice = parseUnits(config.structuredAllocationPriceUsdc.toString(), USDC_DECIMALS);
  const addOn = includeCertifiedDecisionRecord
    ? parseUnits(config.certifiedDecisionRecordFeeUsdc.toString(), USDC_DECIMALS)
    : 0n;
  return basePrice + addOn;
}

function createConfiguredCdpClient(config = getConfig()): CdpClient {
  return new CdpClient({
    apiKeyId: config.coinbaseApiKey,
    apiKeySecret: config.coinbaseApiSecret,
    walletSecret: config.coinbaseWalletSecret,
  });
}

function createConfiguredPublicClient(config = getConfig()) {
  return createPublicClient({
    transport: http(config.baseRpc, {
      timeout: RPC_READ_TIMEOUT_MS,
      retryCount: 1,
    }),
  });
}

type CdpWalletTransaction = {
  to?: Address;
  value?: bigint;
  data?: Hex;
};

class CdpWalletRuntime {
  readonly account: EvmServerAccount;
  private readonly network: "base" | "base-sepolia";
  private readonly publicClient: ReturnType<typeof createConfiguredPublicClient>;

  constructor(account: EvmServerAccount, config = getConfig()) {
    this.account = account;
    this.network = getCdpTransferNetwork(config);
    this.publicClient = createConfiguredPublicClient(config);
  }

  getPublicClient() {
    return this.publicClient;
  }

  getBalance() {
    return this.publicClient.getBalance({ address: this.account.address });
  }

  async sendTransaction(transaction: CdpWalletTransaction): Promise<Hex> {
    const result = await this.account.sendTransaction({
      network: this.network,
      transaction: {
        to: transaction.to,
        value: transaction.value ?? 0n,
        data: appendBuilderCode(transaction.data),
      },
    });
    return result.transactionHash;
  }

  waitForTransactionReceipt(transactionHash: Hex) {
    return this.publicClient.waitForTransactionReceipt({ hash: transactionHash });
  }
}

async function createCdpWalletRuntime(
  input: { address?: Address; idempotencyKey?: string },
  config = getConfig(),
): Promise<CdpWalletRuntime> {
  const cdp = createConfiguredCdpClient(config);
  const account = input.address
    ? await cdp.evm.getAccount({ address: input.address })
    : await cdp.evm.createAccount({
      ...(input.idempotencyKey ? { idempotencyKey: input.idempotencyKey } : {}),
    });

  return new CdpWalletRuntime(account, config);
}

function getConfiguredChain(config = getConfig()) {
  return config.networkId === "base-mainnet" ? base : baseSepolia;
}

function getCdpTransferNetwork(config = getConfig()): "base" | "base-sepolia" {
  return config.networkId === "base-mainnet" ? "base" : "base-sepolia";
}

async function readWalletBalances(address: Address, config = getConfig()) {
  const cacheKey = getBalanceCacheKey(address, config);
  const cached = balanceCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return {
      nativeBalance: cached.nativeBalance,
      usdcBalance: cached.usdcBalance,
    };
  }

  const publicClient = createConfiguredPublicClient(config);
  const [nativeBalance, usdcBalance] = await Promise.all([
    publicClient.getBalance({ address }),
    publicClient.readContract({
      address: config.usdcContractAddress,
      abi: ERC20_BALANCE_OF_ABI,
      functionName: "balanceOf",
      args: [address],
    }),
  ]);

  balanceCache.set(cacheKey, {
    expiresAt: Date.now() + BALANCE_CACHE_TTL_MS,
    nativeBalance,
    usdcBalance,
  });

  return {
    nativeBalance,
    usdcBalance,
  };
}

function isCdpNotFoundError(error: unknown): error is { statusCode: number } {
  return typeof error === "object" && error !== null && "statusCode" in error && (error as { statusCode?: number }).statusCode === 404;
}

async function resolveTreasuryOwnerAccount(
  cdp: CdpClient,
  config = getConfig(),
): Promise<EvmServerAccount | null> {
  if (config.treasuryOwnerAddress) {
    try {
      return await cdp.evm.getAccount({ address: config.treasuryOwnerAddress });
    } catch (error) {
      if (isCdpNotFoundError(error)) {
        throw new Error(
          `Configured SELUN_TREASURY_OWNER_ADDRESS ${config.treasuryOwnerAddress} was not found in the current CDP project.`,
        );
      }
      throw error;
    }
  }

  try {
    return await cdp.evm.getAccount({ name: config.treasuryOwnerName });
  } catch (error) {
    if (isCdpNotFoundError(error)) {
      return null;
    }
    throw error;
  }
}

async function resolveTreasurySmartAccount(
  cdp: CdpClient,
  ownerAccount: EvmServerAccount | null,
  config = getConfig(),
): Promise<{
  name?: string;
  address: Address;
  owners: Address[];
  policies?: string[];
} | null> {
  if (!ownerAccount) return null;

  try {
    const smartAccount = config.treasurySmartAccountAddress
      ? await cdp.evm.getSmartAccount({
        address: config.treasurySmartAccountAddress,
        owner: ownerAccount,
      })
      : await cdp.evm.getSmartAccount({
        name: config.treasurySmartAccountName,
        owner: ownerAccount,
      });

    return {
      name: smartAccount.name,
      address: smartAccount.address,
      owners: smartAccount.owners.map((owner) => owner.address),
      policies: smartAccount.policies,
    };
  } catch (error) {
    if (isCdpNotFoundError(error)) {
      if (config.treasurySmartAccountAddress) {
        throw new Error(
          `Configured SELUN_TREASURY_SMART_ACCOUNT_ADDRESS ${config.treasurySmartAccountAddress} was not found for treasury owner ${ownerAccount.address}.`,
        );
      }
      return null;
    }
    throw error;
  }
}

async function buildTreasuryOwnerWalletSnapshot(account: {
  name?: string;
  address: Address;
  policies?: string[];
}): Promise<TreasuryOwnerWalletSnapshot> {
  const config = getConfig();
  const balances = await readWalletBalances(account.address, config);

  return {
    name: account.name || config.treasuryOwnerName,
    walletAddress: account.address,
    network: config.networkId,
    usdcContractAddress: config.usdcContractAddress,
    nativeBalance: formatUnits(balances.nativeBalance, 18),
    nativeBalanceBaseUnits: balances.nativeBalance.toString(),
    usdcBalance: formatUnits(balances.usdcBalance, USDC_DECIMALS),
    usdcBalanceBaseUnits: balances.usdcBalance.toString(),
    policies: account.policies ?? [],
  };
}

async function buildTreasurySmartAccountSnapshot(account: {
  name?: string;
  address: Address;
  owners: Address[];
  policies?: string[];
}): Promise<TreasurySmartAccountSnapshot> {
  const config = getConfig();
  const balances = await readWalletBalances(account.address, config);

  return {
    name: account.name || config.treasurySmartAccountName,
    walletAddress: account.address,
    ownerAddresses: account.owners,
    network: config.networkId,
    usdcContractAddress: config.usdcContractAddress,
    nativeBalance: formatUnits(balances.nativeBalance, 18),
    nativeBalanceBaseUnits: balances.nativeBalance.toString(),
    usdcBalance: formatUnits(balances.usdcBalance, USDC_DECIMALS),
    usdcBalanceBaseUnits: balances.usdcBalance.toString(),
    policies: account.policies ?? [],
    paymasterUrl: config.treasuryPaymasterUrl,
    sponsorshipMode: config.treasuryPaymasterUrl ? "configured_paymaster" : "manual",
  };
}

function getAdminFundingPrivateKey(): Hex | null {
  const candidate = process.env.SELUN_ADMIN_FUNDING_PRIVATE_KEY?.trim();
  if (!candidate) return null;
  if (!/^0x[a-fA-F0-9]{64}$/.test(candidate)) {
    throw new Error("SELUN_ADMIN_FUNDING_PRIVATE_KEY must be a valid 0x-prefixed 32-byte private key.");
  }
  return candidate as Hex;
}

function normalizeSwapSlippageBps(value: unknown, fallback = 100): number {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number.parseInt(value.trim(), 10)
        : Number.NaN;

  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 10_000) {
    return fallback;
  }

  return parsed;
}

async function getSellerWalletContext(sellerAddress?: Address): Promise<{
  identity: AgentIdentity;
  walletProvider: CdpWalletRuntime;
}> {
  const runtime = await getRuntimeContext();
  if (!sellerAddress || runtime.identity.walletAddress.toLowerCase() === sellerAddress.toLowerCase()) {
    return {
      identity: runtime.identity,
      walletProvider: runtime.walletProvider,
    };
  }

  const config = getConfig();
  const walletProvider = await createCdpWalletRuntime({
    address: sellerAddress,
  }, config);

  return {
    identity: {
      agentId: walletProvider.account.name || sellerAddress!,
      walletAddress: walletProvider.account.address,
      network: config.networkId,
    },
    walletProvider,
  };
}

const UUID_V4_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function asUuidV4(seed: string): string {
  if (UUID_V4_REGEX.test(seed)) return seed;

  const hex = createHash("sha256").update(seed).digest("hex");
  const part1 = hex.slice(0, 8);
  const part2 = hex.slice(8, 12);
  const part3 = `4${hex.slice(13, 16)}`;
  const variantNibble = ((Number.parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
  const part4 = `${variantNibble}${hex.slice(17, 20)}`;
  const part5 = hex.slice(20, 32);
  return `${part1}-${part2}-${part3}-${part4}-${part5}`;
}

async function getRuntimeContext(): Promise<AgentRuntimeContext> {
  if (cachedRuntime) return cachedRuntime;
  await initializeAgent();
  if (!cachedRuntime) {
    throw new Error("Agent runtime failed to initialize.");
  }
  return cachedRuntime;
}

export async function initializeAgent(): Promise<AgentIdentity> {
  const config = getConfig();
  const persistedIdentity = readPersistedIdentity();
  const idempotencyKey = asUuidV4(config.agentWalletId);

  emitExecutionLog({
    phase: "AGENT_INIT",
    action: "initialize_wallet_provider",
    status: "started",
    transactionHash: null,
  });

  const walletAddressToLoad = resolveConfiguredWalletAddress(config.agentWalletId, persistedIdentity, config.networkId);
  const walletProvider = await createCdpWalletRuntime({
    address: walletAddressToLoad,
    idempotencyKey: walletAddressToLoad ? undefined : idempotencyKey,
  }, config);

  const identity: AgentIdentity = {
    agentId: walletProvider.account.name || config.agentWalletId,
    walletAddress: walletProvider.account.address,
    network: config.networkId,
  };

  writePersistedIdentity(identity);

  cachedRuntime = {
    walletProvider,
    identity,
  };

  emitExecutionLog({
    phase: "AGENT_INIT",
    action: "initialize_wallet_provider",
    status: "success",
    transactionHash: null,
  });

  return identity;
}

export async function getAgentAddress(): Promise<{ agentId: string; walletAddress: Address; network: string }> {
  try {
    const runtime = await getRuntimeContext();
    emitExecutionLog({
      phase: "WALLET",
      action: "get_address",
      status: "success",
      transactionHash: null,
    });
    return runtime.identity;
  } catch (error) {
    const config = getConfig();
    const persistedIdentity = readPersistedIdentity();
    const fallbackAddress = resolveConfiguredWalletAddress(
      config.agentWalletId,
      persistedIdentity,
      config.networkId,
    );

    if (!fallbackAddress) {
      throw error;
    }

    const fallbackIdentity: AgentIdentity = {
      agentId: persistedIdentity?.agentId || config.agentWalletId,
      walletAddress: fallbackAddress,
      network: config.networkId,
    };

    emitExecutionLog({
      phase: "WALLET",
      action: "get_address_fallback",
      status: "success",
      transactionHash: null,
    });

    return fallbackIdentity;
  }
}

export async function getAgentUSDCBalance(): Promise<{
  walletAddress: Address;
  network: string;
  usdcContractAddress: Address;
  usdcBalance: string;
  usdcBalanceBaseUnits: string;
}> {
  const runtime = await getRuntimeContext();
  const config = getConfig();

  emitExecutionLog({
    phase: "WALLET",
    action: "read_usdc_balance",
    status: "started",
    transactionHash: null,
  });

  const balance = await runtime.walletProvider.getPublicClient().readContract({
    address: config.usdcContractAddress,
    abi: ERC20_BALANCE_OF_ABI,
    functionName: "balanceOf",
    args: [runtime.identity.walletAddress],
  });

  emitExecutionLog({
    phase: "WALLET",
    action: "read_usdc_balance",
    status: "success",
    transactionHash: null,
  });

  return {
    walletAddress: runtime.identity.walletAddress,
    network: runtime.identity.network,
    usdcContractAddress: config.usdcContractAddress,
    usdcBalance: formatUnits(balance, USDC_DECIMALS),
    usdcBalanceBaseUnits: balance.toString(),
  };
}

export async function getAgentWalletSnapshot(): Promise<AgentWalletSnapshot> {
  const runtime = await getRuntimeContext();
  const config = getConfig();

  emitExecutionLog({
    phase: "WALLET",
    action: "read_wallet_snapshot",
    status: "started",
    transactionHash: null,
  });

  const { nativeBalance, usdcBalance } = await readWalletBalances(runtime.identity.walletAddress, config);

  emitExecutionLog({
    phase: "WALLET",
    action: "read_wallet_snapshot",
    status: "success",
    transactionHash: null,
  });

  return {
    agentId: runtime.identity.agentId,
    walletAddress: runtime.identity.walletAddress,
    network: runtime.identity.network,
    usdcContractAddress: config.usdcContractAddress,
    nativeBalance: formatUnits(nativeBalance, 18),
    nativeBalanceBaseUnits: nativeBalance.toString(),
    usdcBalance: formatUnits(usdcBalance, USDC_DECIMALS),
    usdcBalanceBaseUnits: usdcBalance.toString(),
  };
}

export async function listProjectSellerWalletSnapshots(): Promise<ProjectSellerWalletSnapshot[]> {
  const config = getConfig();
  const cdp = createConfiguredCdpClient(config);
  const activeIdentity = await getAgentAddress().catch(() => null);

  const accounts: Array<{ name?: string; address: Address; policies?: string[] }> = [];
  let nextPageToken: string | undefined;

  do {
    const page = await cdp.evm.listAccounts({
      pageSize: 100,
      ...(nextPageToken ? { pageToken: nextPageToken } : {}),
    });
    for (const account of page.accounts) {
      accounts.push({
        name: account.name,
        address: account.address,
        policies: Array.isArray(account.policies) ? account.policies : [],
      });
    }
    nextPageToken = page.nextPageToken;
  } while (nextPageToken);

  const snapshots: ProjectSellerWalletSnapshot[] = [];
  for (const account of accounts) {
    const { nativeBalance, usdcBalance } = await readWalletBalances(account.address, config);
    snapshots.push({
      agentId: account.name || null,
      walletAddress: account.address,
      network: config.networkId,
      usdcContractAddress: config.usdcContractAddress,
      nativeBalance: formatUnits(nativeBalance, 18),
      nativeBalanceBaseUnits: nativeBalance.toString(),
      usdcBalance: formatUnits(usdcBalance, USDC_DECIMALS),
      usdcBalanceBaseUnits: usdcBalance.toString(),
      policies: account.policies ?? [],
      active: activeIdentity?.walletAddress.toLowerCase() === account.address.toLowerCase(),
    });
  }

  return snapshots.sort((left, right) => {
    if (left.active && !right.active) return -1;
    if (!left.active && right.active) return 1;
    const leftUsdc = Number(left.usdcBalance);
    const rightUsdc = Number(right.usdcBalance);
    if (Number.isFinite(leftUsdc) && Number.isFinite(rightUsdc) && rightUsdc !== leftUsdc) {
      return rightUsdc - leftUsdc;
    }
    return left.walletAddress.localeCompare(right.walletAddress);
  });
}

export async function getTreasuryWalletSetupSnapshot(): Promise<TreasuryWalletSetupSnapshot> {
  const config = getConfig();
  const cdp = createConfiguredCdpClient(config);
  const resolvedOwnerAccount = await resolveTreasuryOwnerAccount(cdp, config);
  const smartAccount = await resolveTreasurySmartAccount(cdp, resolvedOwnerAccount, config);

  return {
    ownerName: config.treasuryOwnerName,
    smartAccountName: config.treasurySmartAccountName,
    paymasterUrl: config.treasuryPaymasterUrl,
    solanaAddress: config.treasurySolanaAddress,
    owner: resolvedOwnerAccount ? await buildTreasuryOwnerWalletSnapshot(resolvedOwnerAccount) : null,
    smartAccount: smartAccount
      ? await buildTreasurySmartAccountSnapshot({
        name: smartAccount.name,
        address: smartAccount.address,
        owners: smartAccount.owners,
        policies: smartAccount.policies,
      })
      : null,
  };
}

export async function ensureTreasurySmartAccount(): Promise<TreasuryWalletSetupSnapshot> {
  const config = getConfig();
  const cdp = createConfiguredCdpClient(config);
  const ownerAccount = config.treasuryOwnerAddress
    ? await cdp.evm.getAccount({ address: config.treasuryOwnerAddress })
    : await cdp.evm.getOrCreateAccount({
      name: config.treasuryOwnerName,
    });
  const smartAccount = config.treasurySmartAccountAddress
    ? await cdp.evm.getSmartAccount({
      address: config.treasurySmartAccountAddress,
      owner: ownerAccount,
    })
    : await cdp.evm.getOrCreateSmartAccount({
      name: config.treasurySmartAccountName,
      owner: ownerAccount,
    });

  return {
    ownerName: config.treasuryOwnerName,
    smartAccountName: config.treasurySmartAccountName,
    paymasterUrl: config.treasuryPaymasterUrl,
    solanaAddress: config.treasurySolanaAddress,
    owner: await buildTreasuryOwnerWalletSnapshot({
      name: ownerAccount.name,
      address: ownerAccount.address,
      policies: ownerAccount.policies,
    }),
    smartAccount: await buildTreasurySmartAccountSnapshot({
      name: smartAccount.name,
      address: smartAccount.address,
      owners: smartAccount.owners.map((owner) => owner.address),
      policies: smartAccount.policies,
    }),
  };
}

export async function getAdminFundingWalletSnapshot(): Promise<AdminNativeFundingWalletSnapshot | null> {
  const privateKey = getAdminFundingPrivateKey();
  if (!privateKey) return null;

  const config = getConfig();
  const publicClient = createConfiguredPublicClient(config);
  const account = privateKeyToAccount(privateKey);
  const nativeBalance = await publicClient.getBalance({ address: account.address });

  return {
    walletAddress: account.address,
    network: config.networkId,
    nativeBalance: formatUnits(nativeBalance, 18),
    nativeBalanceBaseUnits: nativeBalance.toString(),
  };
}

export async function authorizeWizardPayment(
  input: AuthorizeWizardPaymentInput,
): Promise<AuthorizeWizardPaymentResult> {
  if (!isAddress(input.walletAddress)) {
    throw new Error("walletAddress must be a valid EVM address.");
  }

  const walletAddress = input.walletAddress as Address;
  const includeDecisionRecord = Boolean(input.includeCertifiedDecisionRecord);
  const totalPriceUsdcBaseUnits = calculateTotalPriceUsdcBaseUnits(includeDecisionRecord);
  const totalPriceDisplay = formatUnits(totalPriceUsdcBaseUnits, USDC_DECIMALS);
  const promoCode = input.promoCode?.trim();
  if (promoCode) {
    return redeemPromoCodeGrant({
      walletAddress,
      code: promoCode,
      includeCertifiedDecisionRecord: includeDecisionRecord,
      totalPriceUsdcBaseUnits,
      riskMode: input.riskMode,
      investmentHorizon: input.investmentHorizon,
    });
  }

  emitExecutionLog({
    phase: "PAYMENT_AUTH",
    action: "authorize_wizard_payment",
    status: "started",
    transactionHash: null,
  });

  const mode = input.riskMode?.trim() || "unspecified risk mode";
  const horizon = input.investmentHorizon?.trim() || "unspecified horizon";
  const decisionId = `SELUN-DEC-${Date.now()}`;
  const transactionId = buildTransactionId();
  const agentNote = `Selun agent authorized ${totalPriceDisplay} USDC for ${mode} / ${horizon}${includeDecisionRecord ? " with certified record." : "."}`;

  emitExecutionLog({
    phase: "PAYMENT_AUTH",
    action: "authorize_wizard_payment",
    status: "success",
    transactionHash: transactionId,
  });

  return {
    status: "paid",
    transactionId,
    decisionId,
    agentNote,
    chargedAmountUsdc: totalPriceDisplay,
    certifiedDecisionRecordPurchased: includeDecisionRecord,
    paymentMethod: "onchain",
    freeCodeApplied: false,
  };
}

export function getWizardPricing(): WizardPricing {
  const config = getConfig();
  return {
    structuredAllocationPriceUsdc: config.structuredAllocationPriceUsdc,
    certifiedDecisionRecordFeeUsdc: config.certifiedDecisionRecordFeeUsdc,
  };
}

export async function quoteWizardPayment(
  input: QuoteWizardPaymentInput,
): Promise<QuoteWizardPaymentResult> {
  if (!isAddress(input.walletAddress)) {
    throw new Error("walletAddress must be a valid EVM address.");
  }

  const walletAddress = input.walletAddress as Address;
  const includeDecisionRecord = Boolean(input.includeCertifiedDecisionRecord);
  const totalPriceUsdcBaseUnits = calculateTotalPriceUsdcBaseUnits(includeDecisionRecord);
  const totalPriceDisplay = formatUnits(totalPriceUsdcBaseUnits, USDC_DECIMALS);
  const promoCode = input.promoCode?.trim();

  if (!promoCode) {
    return {
      totalBeforeDiscountUsdc: totalPriceDisplay,
      chargedAmountUsdc: totalPriceDisplay,
      discountAmountUsdc: "0",
      discountPercent: 0,
      promoCodeApplied: false,
      promoCode: undefined,
      certifiedDecisionRecordPurchased: includeDecisionRecord,
      paymentMethod: "onchain",
      message: "No promo code applied.",
    };
  }

  const resolvedPromo = resolvePromoCodeGrant({
    walletAddress,
    code: promoCode,
    includeCertifiedDecisionRecord: includeDecisionRecord,
    totalPriceUsdcBaseUnits,
  });

  const promoSummary =
    resolvedPromo.promoKind === "free"
      ? "100% discount (free grant)"
      : `${resolvedPromo.rule.discountPercent}% discount`;

  return {
    totalBeforeDiscountUsdc: resolvedPromo.amountBeforeDiscountUsdc,
    chargedAmountUsdc: resolvedPromo.chargedAmountUsdc,
    discountAmountUsdc: resolvedPromo.discountAmountUsdc,
    discountPercent: resolvedPromo.rule.discountPercent,
    promoCodeApplied: true,
    promoCode: resolvedPromo.normalizedCode,
    certifiedDecisionRecordPurchased: includeDecisionRecord,
    paymentMethod: resolvedPromo.promoKind === "free" ? "free_code" : "onchain",
    message: `Promo code ${resolvedPromo.normalizedCode} valid: ${promoSummary}.`,
  };
}

export async function getUSDCBalanceForAddress(walletAddress: string): Promise<{
  walletAddress: Address;
  network: string;
  usdcContractAddress: Address;
  usdcBalance: string;
  usdcBalanceBaseUnits: string;
}> {
  const config = getConfig();

  if (!isAddress(walletAddress)) {
    throw new Error("walletAddress must be a valid EVM address.");
  }

  const parsedWalletAddress = walletAddress as Address;

  emitExecutionLog({
    phase: "WALLET",
    action: "read_usdc_balance_for_address",
    status: "started",
    transactionHash: null,
  });

  const network = config.networkId;
  const networkFallbackRpc = network === "base-mainnet"
    ? "https://mainnet.base.org"
    : "https://sepolia.base.org";
  const rpcCandidates = Array.from(new Set([config.baseRpc, networkFallbackRpc]));

  let balance: bigint | null = null;
  let lastError: unknown = null;

  for (let index = 0; index < rpcCandidates.length; index += 1) {
    const rpcUrl = rpcCandidates[index];
    const publicClient = createPublicClient({
      transport: http(rpcUrl, {
        timeout: RPC_READ_TIMEOUT_MS,
        retryCount: 1,
      }),
    });

    try {
      balance = await publicClient.readContract({
        address: config.usdcContractAddress,
        abi: ERC20_BALANCE_OF_ABI,
        functionName: "balanceOf",
        args: [parsedWalletAddress],
      });
      break;
    } catch (error) {
      lastError = error;
      emitExecutionLog({
        phase: "WALLET",
        action: index === 0 ? "read_usdc_balance_primary_rpc_failed" : "read_usdc_balance_fallback_rpc_failed",
        status: "pending",
        transactionHash: null,
      });
    }
  }

  if (balance === null) {
    const message = lastError instanceof Error ? lastError.message : "Unknown RPC error.";
    throw new Error(`Unable to read USDC balance from configured Base RPC endpoints. ${message}`);
  }

  emitExecutionLog({
    phase: "WALLET",
    action: "read_usdc_balance_for_address",
    status: "success",
    transactionHash: null,
  });

  return {
    walletAddress: parsedWalletAddress,
    network,
    usdcContractAddress: config.usdcContractAddress,
    usdcBalance: formatUnits(balance, USDC_DECIMALS),
    usdcBalanceBaseUnits: balance.toString(),
  };
}

export async function verifyIncomingPayment(input: PaymentVerificationInput): Promise<PaymentReceipt> {
  const config = getConfig();

  if (!isAddress(input.fromAddress)) {
    throw new Error("fromAddress must be a valid EVM address.");
  }

  const fromAddress = input.fromAddress as Address;
  const decisionId = input.decisionId?.trim();
  const providedTransactionHash = input.transactionHash?.trim();
  const promoGrant = findPromoCodeRedemption({
    walletAddress: fromAddress,
    decisionId,
    transactionId: providedTransactionHash,
  });

  if (promoGrant?.promoKind === "free") {
    emitExecutionLog({
      phase: "PAYMENT_VERIFY",
      action: "verify_free_code_grant",
      status: "success",
      transactionHash: promoGrant.transactionId,
    });
    return {
      transactionHash: promoGrant.transactionId as Hex,
      amount: "0",
      confirmed: true,
      blockNumber: 0,
    };
  }

  const expectedAmount = promoGrant?.promoKind === "percent_discount"
    ? normalizeExpectedUsdc(promoGrant.chargedAmountUsdc)
    : normalizeExpectedUsdc(input.expectedAmountUSDC);
  const identity = await getAgentAddress();
  const toAddress = identity.walletAddress;

  emitExecutionLog({
    phase: "PAYMENT_VERIFY",
    action: "watch_usdc_transfer",
    status: "started",
    transactionHash: null,
  });

  let runtimeClient: ReturnType<CdpWalletRuntime["getPublicClient"]> | null = null;
  try {
    const runtime = await getRuntimeContext();
    runtimeClient = runtime.walletProvider.getPublicClient();
  } catch {
    emitExecutionLog({
      phase: "PAYMENT_VERIFY",
      action: "use_public_rpc_fallback",
      status: "pending",
      transactionHash: null,
    });
  }
  const client = runtimeClient ??
    (createPublicClient({
      transport: http(config.baseRpc),
    }) as ReturnType<CdpWalletRuntime["getPublicClient"]>);

  if (providedTransactionHash) {
    if (!/^0x[a-fA-F0-9]{64}$/.test(providedTransactionHash)) {
      throw new Error("transactionHash must be a valid transaction hash.");
    }

    const txHash = providedTransactionHash as Hex;
    emitExecutionLog({
      phase: "PAYMENT_VERIFY",
      action: "wait_for_confirmations",
      status: "pending",
      transactionHash: txHash,
    });

    try {
      const receipt = await client.waitForTransactionReceipt({
        hash: txHash,
        confirmations: config.paymentConfirmations,
        timeout: config.paymentTimeoutMs,
      });

      const logs = await client.getLogs({
        address: config.usdcContractAddress,
        event: TRANSFER_EVENT,
        args: {
          from: fromAddress,
          to: toAddress,
        },
        fromBlock: receipt.blockNumber,
        toBlock: receipt.blockNumber,
      });

      const matchedTransfer = logs.find((log) => {
        const value = log.args.value;
        return (
          log.transactionHash === txHash &&
          typeof value === "bigint" &&
          value >= expectedAmount
        );
      });

      if (!matchedTransfer || typeof matchedTransfer.args.value !== "bigint") {
        throw new Error("Provided transaction does not satisfy required USDC payment details.");
      }

      emitExecutionLog({
        phase: "PAYMENT_VERIFY",
        action: "confirm_usdc_transfer",
        status: "success",
        transactionHash: txHash,
      });

      return {
        transactionHash: txHash,
        amount: formatUnits(matchedTransfer.args.value, USDC_DECIMALS),
        confirmed: true,
        blockNumber: Number(receipt.blockNumber),
      };
    } catch {
      emitExecutionLog({
        phase: "PAYMENT_VERIFY",
        action: "wait_for_confirmations",
        status: "error",
        transactionHash: txHash,
      });
      emitExecutionLog({
        phase: "PAYMENT_VERIFY",
        action: "fallback_to_log_scan",
        status: "pending",
        transactionHash: txHash,
      });
    }
  }

  const latestBlock = await client.getBlockNumber();
  const fromBlock = latestBlock > 250n ? latestBlock - 250n : 0n;
  const deadline = Date.now() + config.paymentTimeoutMs;

  while (Date.now() < deadline) {
    const logs = await client.getLogs({
      address: config.usdcContractAddress,
      event: TRANSFER_EVENT,
      args: {
        from: fromAddress,
        to: toAddress,
      },
      fromBlock,
      toBlock: "latest",
    });

    const matchedTransfer = logs.find((log) => {
      const value = log.args.value;
      return typeof value === "bigint" && value >= expectedAmount && Boolean(log.transactionHash);
    });

    if (matchedTransfer && matchedTransfer.transactionHash) {
      const txHash = matchedTransfer.transactionHash;
      emitExecutionLog({
        phase: "PAYMENT_VERIFY",
        action: "wait_for_confirmations",
        status: "pending",
        transactionHash: txHash,
      });

      const receipt = await client.waitForTransactionReceipt({
        hash: txHash,
        confirmations: config.paymentConfirmations,
      });

      emitExecutionLog({
        phase: "PAYMENT_VERIFY",
        action: "confirm_usdc_transfer",
        status: "success",
        transactionHash: txHash,
      });

      return {
        transactionHash: txHash,
        amount: formatUnits(matchedTransfer.args.value as bigint, USDC_DECIMALS),
        confirmed: true,
        blockNumber: Number(receipt.blockNumber),
      };
    }

    await sleep(config.paymentPollIntervalMs);
  }

  emitExecutionLog({
    phase: "PAYMENT_VERIFY",
    action: "watch_usdc_transfer",
    status: "error",
    transactionHash: null,
  });
  throw new Error("USDC payment not confirmed within timeout window.");
}

export async function storeDecisionHashOnChain(input: StoreDecisionHashInput): Promise<{
  hashStored: true;
  transactionHash: Hex;
}> {
  const runtime = await getRuntimeContext();

  const decisionId = input.decisionId.trim();
  const pdfHash = input.pdfHash.trim();
  if (!decisionId) {
    throw new Error("decisionId is required.");
  }
  if (!pdfHash) {
    throw new Error("pdfHash is required.");
  }

  emitExecutionLog({
    phase: "STORE_HASH",
    action: "submit_decision_hash_tx",
    status: "started",
    transactionHash: null,
  });

  const txHash = await runtime.walletProvider.sendTransaction({
    to: runtime.identity.walletAddress,
    value: 0n,
    data: buildDecisionMemo(decisionId, pdfHash),
  });

  await runtime.walletProvider.getPublicClient().waitForTransactionReceipt({
    hash: txHash,
    confirmations: 1,
  });

  emitExecutionLog({
    phase: "STORE_HASH",
    action: "submit_decision_hash_tx",
    status: "success",
    transactionHash: txHash,
  });

  return {
    hashStored: true,
    transactionHash: txHash,
  };
}

export async function transferSellerUsdc(input: SellerUsdcTransferInput): Promise<SellerUsdcTransferResult> {
  if (!isAddress(input.toAddress)) {
    throw new Error("toAddress must be a valid EVM address.");
  }

  const sellerAddress =
    typeof input.sellerAddress === "string" && isAddress(input.sellerAddress)
      ? (input.sellerAddress as Address)
      : undefined;
  const runtime = await getSellerWalletContext(sellerAddress);
  const config = getConfig();
  const cdp = createConfiguredCdpClient(config);
  const sellerAccount = await cdp.evm.getAccount({ address: runtime.identity.walletAddress });
  const toAddress = input.toAddress as Address;
  const amountBaseUnits = normalizeExpectedUsdc(input.amountUsdc);

  emitExecutionLog({
    phase: "WALLET",
    action: "seller_usdc_transfer",
    status: "started",
    transactionHash: null,
  });

  const [nativeBalanceBefore, usdcBalanceBefore] = await Promise.all([
    runtime.walletProvider.getBalance(),
    runtime.walletProvider.getPublicClient().readContract({
      address: config.usdcContractAddress,
      abi: ERC20_BALANCE_OF_ABI,
      functionName: "balanceOf",
      args: [runtime.identity.walletAddress],
    }),
  ]);

  if (usdcBalanceBefore < amountBaseUnits) {
    throw new Error(
      `Seller wallet balance is too low. Balance: ${formatUnits(usdcBalanceBefore, USDC_DECIMALS)} USDC, requested transfer: ${formatUnits(amountBaseUnits, USDC_DECIMALS)} USDC.`,
    );
  }

  const transferResult = await sellerAccount.transfer({
    to: toAddress,
    amount: amountBaseUnits,
    token: "usdc",
    network: getCdpTransferNetwork(config),
  });
  const txHash = transferResult.transactionHash;

  await runtime.walletProvider.waitForTransactionReceipt(txHash);
  invalidateBalanceCache(runtime.identity.walletAddress, config);
  invalidateBalanceCache(toAddress, config);

  const [nativeBalanceAfter, usdcBalanceAfter] = await Promise.all([
    runtime.walletProvider.getBalance(),
    runtime.walletProvider.getPublicClient().readContract({
      address: config.usdcContractAddress,
      abi: ERC20_BALANCE_OF_ABI,
      functionName: "balanceOf",
      args: [runtime.identity.walletAddress],
    }),
  ]);

  emitExecutionLog({
    phase: "WALLET",
    action: "seller_usdc_transfer",
    status: "success",
    transactionHash: txHash,
  });

  return {
    sellerWalletAddress: runtime.identity.walletAddress,
    network: runtime.identity.network,
    usdcContractAddress: config.usdcContractAddress,
    toAddress,
    amountUsdc: formatUnits(amountBaseUnits, USDC_DECIMALS),
    amountBaseUnits: amountBaseUnits.toString(),
    ...(input.note?.trim() ? { note: input.note.trim() } : {}),
    transactionHash: txHash,
    nativeBalanceBefore: formatUnits(nativeBalanceBefore, 18),
    nativeBalanceAfter: formatUnits(nativeBalanceAfter, 18),
    usdcBalanceBefore: formatUnits(usdcBalanceBefore, USDC_DECIMALS),
    usdcBalanceAfter: formatUnits(usdcBalanceAfter, USDC_DECIMALS),
  };
}

export async function swapSellerUsdcToEth(input: SellerUsdcToEthSwapInput): Promise<SellerUsdcToEthSwapResult> {
  const config = getConfig();
  if (config.networkId !== "base-mainnet") {
    throw new Error("Seller wallet USDC-to-ETH swaps are only enabled on base-mainnet.");
  }

  const sellerAddress =
    typeof input.sellerAddress === "string" && isAddress(input.sellerAddress)
      ? (input.sellerAddress as Address)
      : undefined;
  const runtime = await getSellerWalletContext(sellerAddress);
  const cdp = createConfiguredCdpClient(config);
  const sellerAccount = await cdp.evm.getAccount({ address: runtime.identity.walletAddress });
  const amountBaseUnits = normalizeExpectedUsdc(input.amountUsdc);
  const slippageBps = normalizeSwapSlippageBps(input.slippageBps, 100);

  emitExecutionLog({
    phase: "WALLET",
    action: "seller_usdc_to_eth_swap",
    status: "started",
    transactionHash: null,
  });

  const [nativeBalanceBefore, usdcBalanceBefore] = await Promise.all([
    runtime.walletProvider.getBalance(),
    runtime.walletProvider.getPublicClient().readContract({
      address: config.usdcContractAddress,
      abi: ERC20_BALANCE_OF_ABI,
      functionName: "balanceOf",
      args: [runtime.identity.walletAddress],
    }),
  ]);

  if (usdcBalanceBefore < amountBaseUnits) {
    throw new Error(
      `Seller wallet balance is too low. Balance: ${formatUnits(usdcBalanceBefore, USDC_DECIMALS)} USDC, requested swap: ${formatUnits(amountBaseUnits, USDC_DECIMALS)} USDC.`,
    );
  }

  const buildSwapQuote = () =>
    sellerAccount.quoteSwap({
      network: "base",
      fromToken: config.usdcContractAddress,
      toToken: NATIVE_ETH_ADDRESS,
      fromAmount: amountBaseUnits,
      slippageBps,
    });

  let swapQuote = await buildSwapQuote();
  if (!swapQuote.liquidityAvailable) {
    throw new Error("Insufficient liquidity for seller wallet USDC-to-ETH swap.");
  }

  let approvalTransactionHash: Hex | undefined;
  if (swapQuote.issues.allowance) {
    approvalTransactionHash = await runtime.walletProvider.sendTransaction({
      to: config.usdcContractAddress,
      value: 0n,
      data: encodeFunctionData({
        abi: ERC20_APPROVE_ABI,
        functionName: "approve",
        args: [swapQuote.issues.allowance.spender, amountBaseUnits],
      }),
    });

    await runtime.walletProvider.waitForTransactionReceipt(approvalTransactionHash);
    invalidateBalanceCache(runtime.identity.walletAddress, config);
    swapQuote = await buildSwapQuote();
    if (!swapQuote.liquidityAvailable) {
      throw new Error("Liquidity became unavailable after approving Permit2 allowance for swap.");
    }
    if (swapQuote.issues.allowance) {
      throw new Error("Swap still requires additional token allowance after approval.");
    }
  }

  const { transactionHash } = await sellerAccount.swap({
    swapQuote,
  });
  await runtime.walletProvider.waitForTransactionReceipt(transactionHash);
  invalidateBalanceCache(runtime.identity.walletAddress, config);

  const [nativeBalanceAfter, usdcBalanceAfter] = await Promise.all([
    runtime.walletProvider.getBalance(),
    runtime.walletProvider.getPublicClient().readContract({
      address: config.usdcContractAddress,
      abi: ERC20_BALANCE_OF_ABI,
      functionName: "balanceOf",
      args: [runtime.identity.walletAddress],
    }),
  ]);

  emitExecutionLog({
    phase: "WALLET",
    action: "seller_usdc_to_eth_swap",
    status: "success",
    transactionHash,
  });

  return {
    sellerWalletAddress: runtime.identity.walletAddress,
    network: runtime.identity.network,
    fromTokenAddress: config.usdcContractAddress,
    toTokenAddress: NATIVE_ETH_ADDRESS,
    amountUsdc: formatUnits(amountBaseUnits, USDC_DECIMALS),
    amountBaseUnits: amountBaseUnits.toString(),
    estimatedToAmountEth: formatUnits(swapQuote.toAmount, 18),
    minimumToAmountEth: formatUnits(swapQuote.minToAmount, 18),
    slippageBps,
    ...(input.note?.trim() ? { note: input.note.trim() } : {}),
    transactionHash,
    ...(approvalTransactionHash ? { approvalTransactionHash } : {}),
    nativeBalanceBefore: formatUnits(nativeBalanceBefore, 18),
    nativeBalanceAfter: formatUnits(nativeBalanceAfter, 18),
    usdcBalanceBefore: formatUnits(usdcBalanceBefore, USDC_DECIMALS),
    usdcBalanceAfter: formatUnits(usdcBalanceAfter, USDC_DECIMALS),
  };
}

export async function transferTreasuryUsdc(input: TreasuryUsdcTransferInput): Promise<TreasuryUsdcTransferResult> {
  if (!isAddress(input.toAddress)) {
    throw new Error("toAddress must be a valid EVM address.");
  }

  const config = getConfig();
  const cdp = createConfiguredCdpClient(config);
  const ownerAccount = await resolveTreasuryOwnerAccount(cdp, config);
  if (!ownerAccount) {
    throw new Error("Treasury owner account is not available.");
  }
  const smartAccount = await resolveTreasurySmartAccount(cdp, ownerAccount, config);
  if (!smartAccount) {
    throw new Error("Treasury smart account is not available.");
  }
  const toAddress = input.toAddress as Address;
  const amountBaseUnits = normalizeExpectedUsdc(input.amountUsdc);

  emitExecutionLog({
    phase: "WALLET",
    action: "treasury_usdc_transfer",
    status: "started",
    transactionHash: null,
  });

  const balancesBefore = await readWalletBalances(smartAccount.address, config);
  if (balancesBefore.usdcBalance < amountBaseUnits) {
    throw new Error(
      `Treasury smart account balance is too low. Balance: ${formatUnits(balancesBefore.usdcBalance, USDC_DECIMALS)} USDC, requested transfer: ${formatUnits(amountBaseUnits, USDC_DECIMALS)} USDC.`,
    );
  }

  const liveSmartAccount = await cdp.evm.getSmartAccount({
    address: smartAccount.address,
    owner: ownerAccount,
  });
  const scopedSmartAccount = await liveSmartAccount.useNetwork(getCdpTransferNetwork(config));
  const transferResult = await scopedSmartAccount.transfer({
    to: toAddress,
    amount: amountBaseUnits,
    token: "usdc",
    ...(config.treasuryPaymasterUrl ? { paymasterUrl: config.treasuryPaymasterUrl } : {}),
  });
  const completed = await scopedSmartAccount.waitForUserOperation({
    userOpHash: transferResult.userOpHash,
    waitOptions: {
      timeoutSeconds: 90,
    },
  });

  if (completed.status !== "complete") {
    throw new Error(`Treasury transfer failed for user operation ${transferResult.userOpHash}.`);
  }

  invalidateBalanceCache(smartAccount.address, config);
  invalidateBalanceCache(toAddress, config);
  const balancesAfter = await readWalletBalances(smartAccount.address, config);

  emitExecutionLog({
    phase: "WALLET",
    action: "treasury_usdc_transfer",
    status: "success",
    transactionHash: completed.transactionHash as Hex,
  });

  return {
    treasuryWalletAddress: smartAccount.address,
    network: config.networkId,
    usdcContractAddress: config.usdcContractAddress,
    toAddress,
    amountUsdc: formatUnits(amountBaseUnits, USDC_DECIMALS),
    amountBaseUnits: amountBaseUnits.toString(),
    ...(input.note?.trim() ? { note: input.note.trim() } : {}),
    userOpHash: transferResult.userOpHash,
    transactionHash: completed.transactionHash as Hex,
    nativeBalanceBefore: formatUnits(balancesBefore.nativeBalance, 18),
    nativeBalanceAfter: formatUnits(balancesAfter.nativeBalance, 18),
    usdcBalanceBefore: formatUnits(balancesBefore.usdcBalance, USDC_DECIMALS),
    usdcBalanceAfter: formatUnits(balancesAfter.usdcBalance, USDC_DECIMALS),
  };
}

export async function transferSellerNative(input: SellerNativeTransferInput): Promise<SellerNativeTransferResult> {
  if (!isAddress(input.toAddress)) {
    throw new Error("toAddress must be a valid EVM address.");
  }

  const fromSellerAddress =
    typeof input.fromSellerAddress === "string" && isAddress(input.fromSellerAddress)
      ? (input.fromSellerAddress as Address)
      : undefined;
  const runtime = await getSellerWalletContext(fromSellerAddress);
  const toAddress = input.toAddress as Address;
  const amountBaseUnits = normalizeExpectedNativeAmount(input.amountEth);

  emitExecutionLog({
    phase: "WALLET",
    action: "seller_native_transfer",
    status: "started",
    transactionHash: null,
  });

  const nativeBalanceBefore = await runtime.walletProvider.getBalance();
  if (nativeBalanceBefore < amountBaseUnits) {
    throw new Error(
      `Seller wallet native balance is too low. Balance: ${formatUnits(nativeBalanceBefore, 18)} ETH, requested transfer: ${formatUnits(amountBaseUnits, 18)} ETH.`,
    );
  }

  const txHash = await runtime.walletProvider.sendTransaction({
    to: toAddress,
    value: amountBaseUnits,
  });

  await runtime.walletProvider.waitForTransactionReceipt(txHash);
  invalidateBalanceCache(runtime.identity.walletAddress, getConfig());
  invalidateBalanceCache(toAddress, getConfig());

  const nativeBalanceAfter = await runtime.walletProvider.getBalance();

  emitExecutionLog({
    phase: "WALLET",
    action: "seller_native_transfer",
    status: "success",
    transactionHash: txHash,
  });

  return {
    fromWalletAddress: runtime.identity.walletAddress,
    network: runtime.identity.network,
    toAddress,
    amountEth: formatUnits(amountBaseUnits, 18),
    amountBaseUnits: amountBaseUnits.toString(),
    ...(input.note?.trim() ? { note: input.note.trim() } : {}),
    transactionHash: txHash,
    nativeBalanceBefore: formatUnits(nativeBalanceBefore, 18),
    nativeBalanceAfter: formatUnits(nativeBalanceAfter, 18),
  };
}

export async function transferNativeFromAdminWallet(input: Omit<SellerNativeTransferInput, "fromSellerAddress">): Promise<SellerNativeTransferResult> {
  if (!isAddress(input.toAddress)) {
    throw new Error("toAddress must be a valid EVM address.");
  }

  const privateKey = getAdminFundingPrivateKey();
  if (!privateKey) {
    throw new Error("SELUN_ADMIN_FUNDING_PRIVATE_KEY is not configured.");
  }

  const config = getConfig();
  const chain = getConfiguredChain(config);
  const account = privateKeyToAccount(privateKey);
  const publicClient = createConfiguredPublicClient(config);
  const walletClient = createWalletClient({
    account,
    chain,
    transport: http(config.baseRpc, {
      timeout: RPC_READ_TIMEOUT_MS,
      retryCount: 1,
    }),
  });
  const toAddress = input.toAddress as Address;
  const amountBaseUnits = normalizeExpectedNativeAmount(input.amountEth);

  emitExecutionLog({
    phase: "WALLET",
    action: "admin_native_transfer",
    status: "started",
    transactionHash: null,
  });

  const nativeBalanceBefore = await publicClient.getBalance({ address: account.address });
  if (nativeBalanceBefore < amountBaseUnits) {
    throw new Error(
      `Admin funding wallet native balance is too low. Balance: ${formatUnits(nativeBalanceBefore, 18)} ETH, requested transfer: ${formatUnits(amountBaseUnits, 18)} ETH.`,
    );
  }

  const txHash = await walletClient.sendTransaction({
    account,
    chain,
    to: toAddress,
    value: amountBaseUnits,
  });

  await publicClient.waitForTransactionReceipt({ hash: txHash });
  invalidateBalanceCache(toAddress, config);

  const nativeBalanceAfter = await publicClient.getBalance({ address: account.address });

  emitExecutionLog({
    phase: "WALLET",
    action: "admin_native_transfer",
    status: "success",
    transactionHash: txHash,
  });

  return {
    fromWalletAddress: account.address,
    network: config.networkId,
    toAddress,
    amountEth: formatUnits(amountBaseUnits, 18),
    amountBaseUnits: amountBaseUnits.toString(),
    ...(input.note?.trim() ? { note: input.note.trim() } : {}),
    transactionHash: txHash,
    nativeBalanceBefore: formatUnits(nativeBalanceBefore, 18),
    nativeBalanceAfter: formatUnits(nativeBalanceAfter, 18),
  };
}
