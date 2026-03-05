import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { CdpClient } from "@coinbase/cdp-sdk";
import { createPublicClient, formatUnits, getAddress, http, parseEther } from "viem";
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
const config = loadConfig(options);
const sellerTarget = resolveSellerTarget(config, options);
const treasuryTarget = resolveTreasuryTarget(config, options);

if (!sellerTarget.address && !sellerTarget.name) {
  throw new Error(
    "Could not resolve the source seller wallet. Pass --from 0x... or set AGENT_WALLET_ID / backend/data/agent-identity.json.",
  );
}

if (!treasuryTarget.address && !treasuryTarget.name) {
  throw new Error(
    "Could not resolve the treasury smart account. Pass --to 0x... or set SELUN_TREASURY_SMART_ACCOUNT_ADDRESS / SELUN_TREASURY_SMART_ACCOUNT_NAME.",
  );
}

const amountBaseUnits = parseAmountEth(options.amount);
const cdp = new CdpClient({
  apiKeyId: config.apiKeyId,
  apiKeySecret: config.apiKeySecret,
  walletSecret: config.walletSecret,
});

const sellerAccount = await loadSellerAccount(cdp, sellerTarget);
const treasuryAddress = await resolveTreasuryAddress(cdp, treasuryTarget, config);
const scopedSellerAccount = await sellerAccount.useNetwork(config.cdpNetwork);
const publicClient = createPublicClient({
  chain: config.networkId === "base-mainnet" ? base : baseSepolia,
  transport: http(config.rpcUrl),
});

const [sellerNativeBefore, treasuryNativeBefore] = await Promise.all([
  publicClient.getBalance({ address: sellerAccount.address }),
  publicClient.getBalance({ address: treasuryAddress }),
]);

if (sellerNativeBefore < amountBaseUnits) {
  throw new Error(
    `Seller wallet native balance is too low. Balance: ${formatUnits(sellerNativeBefore, 18)} ETH, requested transfer: ${formatUnits(amountBaseUnits, 18)} ETH.`,
  );
}

console.log(`Network: ${config.networkId}`);
console.log(`From seller wallet: ${sellerAccount.address}`);
console.log(`To treasury wallet: ${treasuryAddress}`);
console.log(`Amount: ${formatUnits(amountBaseUnits, 18)} ETH`);
if (options.note) {
  console.log(`Note: ${options.note}`);
}
console.log(`Seller native balance before: ${formatUnits(sellerNativeBefore, 18)} ETH`);
console.log(`Treasury native balance before: ${formatUnits(treasuryNativeBefore, 18)} ETH`);

const { transactionHash } = await scopedSellerAccount.transfer({
  to: treasuryAddress,
  amount: amountBaseUnits,
  token: "eth",
});

console.log(`Transfer tx submitted: ${transactionHash}`);

const receipt = await scopedSellerAccount.waitForTransactionReceipt({ transactionHash });
const [sellerNativeAfter, treasuryNativeAfter] = await Promise.all([
  publicClient.getBalance({ address: sellerAccount.address }),
  publicClient.getBalance({ address: treasuryAddress }),
]);

console.log(`Transfer confirmed in block: ${receipt.blockNumber?.toString?.() ?? "unknown"}`);
console.log(`Seller native balance after: ${formatUnits(sellerNativeAfter, 18)} ETH`);
console.log(`Treasury native balance after: ${formatUnits(treasuryNativeAfter, 18)} ETH`);
console.log(`Transfer complete: ${transactionHash}`);

function parseOptions(argv) {
  const optionValue = (name) => {
    const inline = argv.find((arg) => arg.startsWith(`${name}=`));
    if (inline) return inline.slice(name.length + 1);
    const index = argv.indexOf(name);
    if (index === -1) return undefined;
    return argv[index + 1];
  };

  return {
    from: optionValue("--from"),
    to: optionValue("--to"),
    amount: optionValue("--amount"),
    note: optionValue("--note"),
    network: optionValue("--network"),
    rpc: optionValue("--rpc"),
  };
}

function loadEnvFiles(paths) {
  for (const envPath of paths) {
    if (fs.existsSync(envPath)) {
      dotenv.config({ path: envPath, quiet: true });
    }
  }
}

function loadConfig(options) {
  const networkId = normalizeNetwork(
    options.network?.trim() || process.env.SELUN_TREASURY_FUND_NETWORK?.trim() || process.env.NETWORK_ID?.trim(),
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
    networkId,
    cdpNetwork: networkId === "base-mainnet" ? "base" : "base-sepolia",
    rpcUrl:
      options.rpc?.trim() ||
      process.env.SELUN_TREASURY_FUND_RPC_URL?.trim() ||
      (networkId === "base-mainnet"
        ? process.env.BASE_MAINNET_RPC?.trim() || "https://mainnet.base.org"
        : process.env.BASE_SEPOLIA_RPC?.trim() || "https://sepolia.base.org"),
    agentWalletId: process.env.AGENT_WALLET_ID?.trim() || `selun-agent-${networkId}`,
    treasuryOwnerName: process.env.SELUN_TREASURY_OWNER_NAME?.trim() || null,
    treasuryOwnerAddress: normalizeOptionalAddress(process.env.SELUN_TREASURY_OWNER_ADDRESS?.trim()),
    treasurySmartAccountName: process.env.SELUN_TREASURY_SMART_ACCOUNT_NAME?.trim() || null,
    treasurySmartAccountAddress: normalizeOptionalAddress(process.env.SELUN_TREASURY_SMART_ACCOUNT_ADDRESS?.trim()),
  };
}

function resolveSellerTarget(config, options) {
  const explicit = options.from?.trim();
  const explicitAddress = normalizeAddress(explicit);
  if (explicitAddress) {
    return { address: explicitAddress };
  }
  if (explicit) {
    return { name: explicit };
  }

  if (fs.existsSync(identityPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(identityPath, "utf8"));
      const persistedAddress = normalizeAddress(parsed?.walletAddress);
      if (parsed?.network === config.networkId && persistedAddress) {
        return { address: persistedAddress };
      }
    } catch {
      // Ignore bad local state and continue with env resolution.
    }
  }

  const agentWalletAddress = normalizeAddress(config.agentWalletId);
  if (agentWalletAddress) {
    return { address: agentWalletAddress };
  }
  if (config.agentWalletId) {
    return { name: config.agentWalletId };
  }
  return {};
}

function resolveTreasuryTarget(config, options) {
  const explicit = options.to?.trim();
  const explicitAddress = normalizeAddress(explicit);
  if (explicitAddress) {
    return { address: explicitAddress };
  }
  if (explicit) {
    return { name: explicit, ownerName: config.treasuryOwnerName, ownerAddress: config.treasuryOwnerAddress };
  }

  if (config.treasurySmartAccountAddress) {
    return { address: config.treasurySmartAccountAddress };
  }
  if (config.treasurySmartAccountName) {
    return {
      name: config.treasurySmartAccountName,
      ownerName: config.treasuryOwnerName,
      ownerAddress: config.treasuryOwnerAddress,
    };
  }
  return {};
}

async function loadSellerAccount(cdp, sellerTarget) {
  if (sellerTarget.address) {
    return cdp.evm.getAccount({ address: sellerTarget.address });
  }
  return cdp.evm.getAccount({ name: sellerTarget.name });
}

async function resolveTreasuryAddress(cdp, treasuryTarget, config) {
  if (treasuryTarget.address) {
    return treasuryTarget.address;
  }

  const owner = await loadTreasuryOwner(cdp, treasuryTarget, config);
  const smartAccount = await cdp.evm.getSmartAccount({
    name: treasuryTarget.name,
    owner,
  });
  return smartAccount.address;
}

async function loadTreasuryOwner(cdp, treasuryTarget, config) {
  if (treasuryTarget.ownerAddress) {
    return cdp.evm.getAccount({ address: treasuryTarget.ownerAddress });
  }
  if (treasuryTarget.ownerName) {
    return cdp.evm.getAccount({ name: treasuryTarget.ownerName });
  }
  if (config.treasuryOwnerAddress) {
    return cdp.evm.getAccount({ address: config.treasuryOwnerAddress });
  }
  if (config.treasuryOwnerName) {
    return cdp.evm.getAccount({ name: config.treasuryOwnerName });
  }
  throw new Error(
    "Treasury smart account name is configured, but no treasury owner could be resolved. Set SELUN_TREASURY_OWNER_ADDRESS or SELUN_TREASURY_OWNER_NAME.",
  );
}

function parseAmountEth(amount) {
  if (!amount) {
    throw new Error("--amount is required.");
  }
  const parsed = Number.parseFloat(amount);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error("--amount must be greater than zero.");
  }
  return parseEther(amount);
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

function normalizeOptionalAddress(value) {
  return normalizeAddress(value);
}

function normalizeAddress(value) {
  if (!value) return null;
  const trimmed = value.trim();
  if (!/^0x[a-fA-F0-9]{40}$/.test(trimmed)) {
    return null;
  }

  try {
    return getAddress(trimmed);
  } catch {
    return null;
  }
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

function printHelp() {
  console.log(`Usage:
  node scripts/fund-treasury-eth.mjs --amount 0.0001 [options]

Options:
  --amount <eth>        Required ETH amount to transfer.
  --from <wallet>       Optional seller wallet address or CDP account name.
  --to <wallet>         Optional treasury smart account address or CDP smart account name.
  --network <network>   Optional network (base-mainnet | base-sepolia). Defaults from NETWORK_ID.
  --rpc <url>           Optional RPC URL override.
  --note <text>         Optional note printed to stdout.
  --help, -h            Show this help text.

Environment:
  CDP_API_KEY_ID / COINBASE_API_KEY
  CDP_API_KEY_SECRET / COINBASE_API_SECRET
  CDP_WALLET_SECRET or COINBASE_WALLET_SECRET (optional; falls back to API secret)
  NETWORK_ID
  AGENT_WALLET_ID
  BASE_MAINNET_RPC / BASE_SEPOLIA_RPC
  SELUN_TREASURY_OWNER_NAME / SELUN_TREASURY_OWNER_ADDRESS
  SELUN_TREASURY_SMART_ACCOUNT_NAME / SELUN_TREASURY_SMART_ACCOUNT_ADDRESS
`);
}
