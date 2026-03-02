import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { CdpClient } from "@coinbase/cdp-sdk";
import { erc20Abi, formatUnits, http, isAddress, parseUnits, createPublicClient } from "viem";
import { base, baseSepolia } from "viem/chains";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const backendRoot = path.join(projectRoot, "backend");
const backendDataDir = path.join(backendRoot, "data");
const identityPath = path.join(backendDataDir, "agent-identity.json");

loadEnvFiles([
  path.join(projectRoot, ".env"),
  path.join(projectRoot, ".env.local"),
  path.join(backendRoot, ".env"),
  path.join(backendRoot, ".env.local"),
]);

const args = process.argv.slice(2);
const flags = new Set(args.filter((arg) => arg.startsWith("--")));

if (flags.has("--help") || flags.has("-h")) {
  printHelp();
  process.exit(0);
}

const options = parseOptions(args);

if (!options.to || !isAddress(options.to)) {
  console.error("--to must be a valid EVM address.");
  process.exit(1);
}

if (!options.amount) {
  console.error("--amount is required.");
  process.exit(1);
}

const amountBaseUnits = parseUsdcAmount(options.amount);
if (amountBaseUnits <= 0n) {
  console.error("--amount must be greater than zero.");
  process.exit(1);
}

const config = loadRefundConfig();
const sellerTarget = resolveSellerTarget(config);
if (!sellerTarget.address && !sellerTarget.name) {
  throw new Error(
    "Could not resolve Selun's seller wallet. Pass --seller 0x... for a direct wallet address or set AGENT_WALLET_ID / backend/data/agent-identity.json for the correct network.",
  );
}

const cdp = new CdpClient({
  apiKeyId: config.apiKeyId,
  apiKeySecret: config.apiKeySecret,
  walletSecret: config.walletSecret,
});

const sellerAccount = await loadSellerAccount(cdp, sellerTarget, config);
const scopedSellerAccount = await sellerAccount.useNetwork(config.cdpNetwork);

const publicClient = createPublicClient({
  chain: config.networkId === "base-mainnet" ? base : baseSepolia,
  transport: http(config.rpcUrl),
});

const actualSellerAddress = sellerAccount.address;
const nativeBalanceBefore = await publicClient.getBalance({ address: actualSellerAddress });
const usdcBalanceBefore = await readUsdcBalance(publicClient, config.usdcContractAddress, actualSellerAddress);

if (usdcBalanceBefore < amountBaseUnits) {
  console.error(
    `Seller wallet balance is too low. Balance: ${formatUnits(usdcBalanceBefore, 6)} USDC, requested refund: ${formatUnits(amountBaseUnits, 6)} USDC.`,
  );
  process.exit(1);
}

console.log(`Seller wallet: ${actualSellerAddress}`);
console.log(`Network: ${config.networkId}`);
console.log(`USDC contract: ${config.usdcContractAddress}`);
console.log(`Refunding: ${formatUnits(amountBaseUnits, 6)} USDC -> ${options.to}`);
if (options.note) {
  console.log(`Note: ${options.note}`);
}
console.log(`Seller native balance before: ${formatUnits(nativeBalanceBefore, 18)}`);
console.log(`Seller USDC balance before: ${formatUnits(usdcBalanceBefore, 6)}`);

if (nativeBalanceBefore === 0n) {
  throw new Error(
    `Seller wallet has 0 native ETH on ${config.networkId}. CDP server-account USDC transfers still need gas. Fund ${actualSellerAddress} with a small amount of ETH on ${config.networkId} and retry.`,
  );
}

const { transactionHash: txHash } = await scopedSellerAccount.transfer({
  to: options.to,
  amount: amountBaseUnits,
  token: "usdc",
});

console.log(`Refund tx submitted: ${txHash}`);

const receipt = await scopedSellerAccount.waitForTransactionReceipt({ transactionHash: txHash });
const nativeBalanceAfter = await publicClient.getBalance({ address: actualSellerAddress });
const usdcBalanceAfter = await readUsdcBalance(publicClient, config.usdcContractAddress, actualSellerAddress);

console.log(`Refund confirmed in block: ${receipt.blockNumber?.toString?.() ?? "unknown"}`);
console.log(`Seller native balance after: ${formatUnits(nativeBalanceAfter, 18)}`);
console.log(`Seller USDC balance after: ${formatUnits(usdcBalanceAfter, 6)}`);
console.log(`Refund complete: ${txHash}`);

function parseOptions(argv) {
  const optionValue = (name) => {
    const inline = argv.find((arg) => arg.startsWith(`${name}=`));
    if (inline) return inline.slice(name.length + 1);
    const index = argv.indexOf(name);
    if (index === -1) return undefined;
    return argv[index + 1];
  };

  return {
    to: optionValue("--to"),
    amount: optionValue("--amount"),
    note: optionValue("--note"),
    seller: optionValue("--seller"),
    network: optionValue("--network"),
    rpc: optionValue("--rpc"),
    usdcContractAddress: optionValue("--usdc"),
  };
}

function loadEnvFiles(paths) {
  for (const envPath of paths) {
    if (fs.existsSync(envPath)) {
      dotenv.config({ path: envPath, quiet: true });
    }
  }
}

function loadRefundConfig() {
  const networkId = normalizeNetwork(
    options.network?.trim() || process.env.SELUN_REFUND_NETWORK?.trim() || process.env.NETWORK_ID?.trim(),
  );
  const apiKeySecret = normalizeSecret(requiredAnyEnv("CDP_API_KEY_SECRET", "COINBASE_API_SECRET"));
  return {
    apiKeyId: requiredAnyEnv("CDP_API_KEY_ID", "COINBASE_API_KEY"),
    apiKeySecret,
    walletSecret: normalizeSecret(
      process.env.CDP_WALLET_SECRET?.trim() ||
        process.env.COINBASE_WALLET_SECRET?.trim() ||
        apiKeySecret,
    ),
    seller: options.seller?.trim() || process.env.SELUN_REFUND_SELLER_ADDRESS?.trim() || process.env.AGENT_WALLET_ID?.trim(),
    agentWalletId:
      process.env.AGENT_WALLET_ID?.trim() ||
      `selun-agent-${networkId}`,
    networkId,
    cdpNetwork: networkId === "base-mainnet" ? "base" : "base-sepolia",
    rpcUrl:
      options.rpc?.trim() ||
      process.env.SELUN_REFUND_RPC_URL?.trim() ||
      (networkId === "base-mainnet"
        ? process.env.BASE_MAINNET_RPC?.trim() || "https://mainnet.base.org"
        : process.env.BASE_SEPOLIA_RPC?.trim() || "https://sepolia.base.org"),
    usdcContractAddress: resolveUsdcContractAddress(networkId, options.usdcContractAddress),
  };
}

function resolveSellerTarget(config) {
  if (config.seller && isAddress(config.seller)) {
    return { address: config.seller };
  }

  if (config.seller) {
    return { name: config.seller };
  }

  if (fs.existsSync(identityPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(identityPath, "utf8"));
      if (parsed?.network === config.networkId && parsed?.walletAddress && isAddress(parsed.walletAddress)) {
        return { address: parsed.walletAddress };
      }
    } catch {
      // Fall through to provider-created lookup rules.
    }
  }

  if (config.agentWalletId && isAddress(config.agentWalletId)) {
    return { address: config.agentWalletId };
  }

  if (config.agentWalletId) {
    return { name: config.agentWalletId };
  }

  return {};
}

async function loadSellerAccount(cdp, sellerTarget, config) {
  try {
    if (sellerTarget.address) {
      return await cdp.evm.getAccount({ address: sellerTarget.address });
    }
    return await cdp.evm.getAccount({ name: sellerTarget.name });
  } catch (error) {
    const notFound =
      error &&
      typeof error === "object" &&
      "errorType" in error &&
      error.errorType === "not_found";

    if (!notFound) {
      throw error;
    }

    const sellerDescriptor = sellerTarget.address ?? sellerTarget.name ?? "unknown";
    const configuredAgentWallet = config.agentWalletId || "unset";
    throw new Error(
      `CDP could not find seller account '${sellerDescriptor}' with the currently loaded API credentials. ` +
        `Your local env is configured for '${configuredAgentWallet}' on ${config.networkId}. ` +
        `Use the production CDP credentials that own the mainnet seller wallet, or pass the correct CDP account name with --seller.`,
    );
  }
}

function normalizeNetwork(value) {
  if (value === "base-sepolia") return "base-sepolia";
  if (value === "base" || value === "base-mainnet") return "base-mainnet";
  return "base-mainnet";
}

function normalizeSecret(value) {
  const trimmed = value.trim();
  const unwrapped =
    (trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))
      ? trimmed.slice(1, -1)
      : trimmed;
  return unwrapped.replace(/\\n/g, "\n");
}

function requiredEnv(name) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function requiredAnyEnv(...names) {
  for (const name of names) {
    const value = process.env[name]?.trim();
    if (value) {
      return value;
    }
  }
  throw new Error(`Missing required environment variable. Expected one of: ${names.join(", ")}`);
}

function requiredAddressEnv(name) {
  const value = requiredEnv(name);
  if (!isAddress(value)) {
    throw new Error(`${name} must be a valid EVM address.`);
  }
  return value;
}

function resolveUsdcContractAddress(networkId, override) {
  if (override?.trim()) {
    if (!isAddress(override.trim())) {
      throw new Error("--usdc must be a valid EVM address.");
    }
    return override.trim();
  }

  const explicit = process.env.SELUN_REFUND_USDC_CONTRACT_ADDRESS?.trim();
  if (explicit) {
    if (!isAddress(explicit)) {
      throw new Error("SELUN_REFUND_USDC_CONTRACT_ADDRESS must be a valid EVM address.");
    }
    return explicit;
  }

  return networkId === "base-mainnet"
    ? "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
    : "0x036CbD53842c5426634e7929541eC2318f3dCF7e";
}

function parseUsdcAmount(amount) {
  try {
    return parseUnits(amount, 6);
  } catch {
    throw new Error("--amount must be a valid decimal USDC amount.");
  }
}

async function readUsdcBalance(publicClient, tokenAddress, walletAddress) {
  return publicClient.readContract({
    address: tokenAddress,
    abi: erc20Abi,
    functionName: "balanceOf",
    args: [walletAddress],
  });
}

function printHelp() {
  console.log(`Usage: node scripts/refund-usdc.mjs --to 0xBuyerAddress --amount 1 [--seller 0xSeller] [--network base-mainnet] [--note "optional"]

Sends a USDC refund from Selun's seller wallet using the same backend CDP wallet configuration.

Required:
  --to <address>       Buyer wallet address that should receive the refund
  --amount <usdc>      Decimal USDC amount to send, e.g. 1 or 19.25

Optional:
  --seller <address>   Explicit seller wallet address or CDP account name to refund from
  --network <id>       base-mainnet or base-sepolia (default resolves from env)
  --rpc <url>          Override the RPC URL used for balance checks and receipts
  --usdc <address>     Override the USDC contract address
  --note <text>        Local note printed to stdout for your records

Environment:
  Loads .env/.env.local and backend/.env/backend/.env.local
  Requires the same backend wallet env used by Selun:
    CDP_API_KEY_ID or COINBASE_API_KEY
    CDP_API_KEY_SECRET or COINBASE_API_SECRET
    CDP_WALLET_SECRET or COINBASE_WALLET_SECRET (optional; falls back to API secret)
    AGENT_WALLET_ID
    NETWORK_ID
    BASE_MAINNET_RPC or BASE_SEPOLIA_RPC

  Refund-specific overrides:
    SELUN_REFUND_SELLER_ADDRESS
    SELUN_REFUND_NETWORK
    SELUN_REFUND_RPC_URL
    SELUN_REFUND_USDC_CONTRACT_ADDRESS

Examples:
  node scripts/refund-usdc.mjs --to 0xBuyerAddress --amount 1 --seller 0xac6a...
  node scripts/refund-usdc.mjs --to 0xBuyerAddress --amount 19 --network base-mainnet --seller 0xac6a... --note "Bazaar listing refund"
`);
}
