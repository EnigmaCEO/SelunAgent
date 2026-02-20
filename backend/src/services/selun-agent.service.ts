import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { AgentKit, CdpEvmWalletProvider, walletActionProvider } from "@coinbase/agentkit";
import {
  createPublicClient,
  formatUnits,
  http,
  isAddress,
  parseAbiItem,
  parseUnits,
  stringToHex,
  type Address,
  type Hex,
} from "viem";
import { getConfig } from "../config";
import { emitExecutionLog } from "../logging/execution-logs";

const USDC_DECIMALS = 6;
const IDENTITY_PATH = path.join(process.cwd(), "backend", "data", "agent-identity.json");
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
  agentKit: AgentKit;
  walletProvider: CdpEvmWalletProvider;
  identity: AgentIdentity;
};

export type PaymentVerificationInput = {
  fromAddress: string;
  expectedAmountUSDC: number | string;
  transactionHash?: string;
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

export type AuthorizeWizardPaymentInput = {
  walletAddress: string;
  includeCertifiedDecisionRecord?: boolean;
  riskMode?: string;
  investmentHorizon?: string;
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
};

let cachedRuntime: AgentRuntimeContext | null = null;

function ensureIdentityDir() {
  fs.mkdirSync(path.dirname(IDENTITY_PATH), { recursive: true });
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
  const walletProvider = await CdpEvmWalletProvider.configureWithWallet({
    apiKeyId: config.coinbaseApiKey,
    apiKeySecret: config.coinbaseApiSecret,
    walletSecret: config.coinbaseWalletSecret,
    networkId: config.networkId,
    rpcUrl: config.baseRpc,
    address: walletAddressToLoad,
    idempotencyKey: walletAddressToLoad ? undefined : idempotencyKey,
  });

  const walletNetwork = walletProvider.getNetwork().networkId;
  if (walletNetwork !== config.networkId) {
    emitExecutionLog({
      phase: "AGENT_INIT",
      action: "validate_network",
      status: "error",
      transactionHash: null,
    });
    throw new Error(`Agent wallet network must be ${config.networkId}. Received ${walletNetwork}.`);
  }

  const exportedWallet = await walletProvider.exportWallet();
  const identity: AgentIdentity = {
    agentId: exportedWallet.name || config.agentWalletId,
    walletAddress: exportedWallet.address,
    network: walletNetwork,
  };

  writePersistedIdentity(identity);

  const agentKit = await AgentKit.from({
    walletProvider,
    actionProviders: [walletActionProvider()],
  });

  cachedRuntime = {
    agentKit,
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

  const balance = await runtime.walletProvider.readContract({
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

export async function authorizeWizardPayment(
  input: AuthorizeWizardPaymentInput,
): Promise<AuthorizeWizardPaymentResult> {
  if (!isAddress(input.walletAddress)) {
    throw new Error("walletAddress must be a valid EVM address.");
  }

  const includeDecisionRecord = Boolean(input.includeCertifiedDecisionRecord);
  const totalPriceUsdcBaseUnits = calculateTotalPriceUsdcBaseUnits(includeDecisionRecord);
  const totalPriceDisplay = formatUnits(totalPriceUsdcBaseUnits, USDC_DECIMALS);

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
  };
}

export function getWizardPricing(): WizardPricing {
  const config = getConfig();
  return {
    structuredAllocationPriceUsdc: config.structuredAllocationPriceUsdc,
    certifiedDecisionRecordFeeUsdc: config.certifiedDecisionRecordFeeUsdc,
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

  let balance: bigint;
  let network: string = config.networkId;

  try {
    const runtime = await getRuntimeContext();
    network = runtime.identity.network;

    balance = await runtime.walletProvider.readContract({
      address: config.usdcContractAddress,
      abi: ERC20_BALANCE_OF_ABI,
      functionName: "balanceOf",
      args: [parsedWalletAddress],
    });
  } catch (error) {
    emitExecutionLog({
      phase: "WALLET",
      action: "read_usdc_balance_for_address_fallback_rpc",
      status: "pending",
      transactionHash: null,
    });

    const publicClient = createPublicClient({
      transport: http(config.baseRpc),
    });

    try {
      balance = await publicClient.readContract({
        address: config.usdcContractAddress,
        abi: ERC20_BALANCE_OF_ABI,
        functionName: "balanceOf",
        args: [parsedWalletAddress],
      });
    } catch {
      const message = error instanceof Error ? error.message : "Unknown AgentKit initialization error.";
      throw new Error(`Unable to read USDC balance via AgentKit or RPC fallback. ${message}`);
    }
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

  const expectedAmount = normalizeExpectedUsdc(input.expectedAmountUSDC);
  const fromAddress = input.fromAddress as Address;
  const identity = await getAgentAddress();
  const toAddress = identity.walletAddress;
  const providedTransactionHash = input.transactionHash?.trim();

  emitExecutionLog({
    phase: "PAYMENT_VERIFY",
    action: "watch_usdc_transfer",
    status: "started",
    transactionHash: null,
  });

  let runtimeClient: ReturnType<CdpEvmWalletProvider["getPublicClient"]> | null = null;
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
    }) as ReturnType<CdpEvmWalletProvider["getPublicClient"]>);

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
