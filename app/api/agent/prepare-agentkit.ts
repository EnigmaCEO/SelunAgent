import fs from "node:fs";
import path from "node:path";
import type { ActionProvider, AgentKit, WalletProvider } from "@coinbase/agentkit";
import type { Hex } from "viem";

const WALLET_DATA_FILE = path.join(process.cwd(), "wallet_data.txt");

type StoredWalletData = {
  privateKey?: string;
};

function normalizePrivateKey(value: string): Hex {
  const trimmed = value.trim();
  const hexWithoutPrefix = trimmed.startsWith("0x") ? trimmed.slice(2) : trimmed;

  if (!/^[0-9a-fA-F]{64}$/.test(hexWithoutPrefix)) {
    throw new Error("PRIVATE_KEY must be a 64-character hex string.");
  }

  return `0x${hexWithoutPrefix}` as Hex;
}

function getStoredPrivateKey(): Hex | null {
  if (!fs.existsSync(WALLET_DATA_FILE)) return null;

  try {
    const walletData = JSON.parse(fs.readFileSync(WALLET_DATA_FILE, "utf8")) as StoredWalletData;
    if (!walletData.privateKey) return null;
    return normalizePrivateKey(walletData.privateKey);
  } catch (error) {
    console.warn("Could not load wallet_data.txt, generating a fresh wallet.", error);
    return null;
  }
}

function storePrivateKey(privateKey: Hex) {
  const payload = JSON.stringify({ privateKey }, null, 2);
  fs.writeFileSync(WALLET_DATA_FILE, payload, "utf8");
}

export async function prepareAgentkitAndWalletProvider(): Promise<{
  agentkit: AgentKit;
  walletProvider: WalletProvider;
}> {
  const {
    AgentKit,
    NETWORK_ID_TO_VIEM_CHAIN,
    ViemWalletProvider,
    cdpApiActionProvider,
    erc20ActionProvider,
    pythActionProvider,
    walletActionProvider,
    wethActionProvider,
  } = await import("@coinbase/agentkit");
  const { createWalletClient, http } = await import("viem");
  const { generatePrivateKey, privateKeyToAccount } = await import("viem/accounts");

  let privateKey = process.env.PRIVATE_KEY ? normalizePrivateKey(process.env.PRIVATE_KEY) : getStoredPrivateKey();
  if (!privateKey) {
    privateKey = generatePrivateKey();
    storePrivateKey(privateKey);
  }

  const networkId = process.env.NETWORK_ID ?? "base-sepolia";
  const chain = NETWORK_ID_TO_VIEM_CHAIN[networkId];

  if (!chain) {
    throw new Error(
      `Unsupported NETWORK_ID "${networkId}". Use a network from AgentKit NETWORK_ID_TO_VIEM_CHAIN.`,
    );
  }

  const account = privateKeyToAccount(privateKey);
  const rpcUrl = process.env.RPC_URL?.trim() || chain.rpcUrls.default.http[0];
  const walletClient = createWalletClient({
    account,
    chain,
    transport: http(rpcUrl),
  });
  const walletProvider = new ViemWalletProvider(walletClient);

  const actionProviders: ActionProvider[] = [
    wethActionProvider(),
    pythActionProvider(),
    walletActionProvider(),
    erc20ActionProvider(),
  ];
  if (process.env.CDP_API_KEY_ID && process.env.CDP_API_KEY_SECRET) {
    actionProviders.push(cdpApiActionProvider());
  }

  const agentkit = await AgentKit.from({
    walletProvider,
    actionProviders,
  });

  return { agentkit, walletProvider };
}
