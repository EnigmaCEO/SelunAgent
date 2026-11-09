import type { PaymentChallenge } from "./types";

type Eip1193Provider = {
  request(args: { method: string; params?: unknown }): Promise<unknown>;
};

export class WalletError extends Error {
  constructor(
    readonly code: "no_wallet" | "rejected" | "wrong_network" | "failed",
    message: string,
  ) {
    super(message);
    this.name = "WalletError";
  }
}

const CHAINS: Record<string, { chainIdHex: string; chainName: string; rpcUrl: string; explorerUrl: string }> = {
  "eip155:8453": { chainIdHex: "0x2105", chainName: "Base", rpcUrl: "https://mainnet.base.org", explorerUrl: "https://basescan.org" },
  "eip155:84532": { chainIdHex: "0x14a34", chainName: "Base Sepolia", rpcUrl: "https://sepolia.base.org", explorerUrl: "https://sepolia.basescan.org" },
};

function getProvider(): Eip1193Provider {
  const provider = (window as unknown as { ethereum?: Eip1193Provider }).ethereum;
  if (!provider?.request) {
    throw new WalletError(
      "no_wallet",
      "No crypto wallet was found in this browser. Install a wallet such as MetaMask or Coinbase Wallet, or open this page in your wallet's browser.",
    );
  }
  return provider;
}

/** Lets the UI explain a missing wallet before doing any payment work. */
export function assertWalletAvailable() {
  getProvider();
}

function isUserRejection(error: unknown): boolean {
  const { code, message } = (error ?? {}) as { code?: unknown; message?: unknown };
  // 4001 from a browser wallet; 5000 or "User rejected" from a phone wallet over WalletConnect.
  return code === 4001 || code === 5000 || (typeof message === "string" && /user (rejected|denied|cancel)/i.test(message));
}

async function ensureChain(provider: Eip1193Provider, network: string) {
  const chain = CHAINS[network];
  if (!chain) throw new WalletError("wrong_network", "This payment uses a network your wallet can't be switched to automatically.");

  const current = String(await provider.request({ method: "eth_chainId" })).toLowerCase();
  if (current === chain.chainIdHex) return;

  try {
    await provider.request({ method: "wallet_switchEthereumChain", params: [{ chainId: chain.chainIdHex }] });
  } catch (error) {
    if (isUserRejection(error)) throw new WalletError("rejected", `Please switch your wallet to ${chain.chainName} to continue. You have not been charged.`);
    if ((error as { code?: unknown })?.code !== 4902) {
      throw new WalletError("wrong_network", `Please switch your wallet to ${chain.chainName} and try again.`);
    }
    try {
      await provider.request({
        method: "wallet_addEthereumChain",
        params: [{
          chainId: chain.chainIdHex,
          chainName: chain.chainName,
          nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
          rpcUrls: [chain.rpcUrl],
          blockExplorerUrls: [chain.explorerUrl],
        }],
      });
    } catch {
      throw new WalletError("wrong_network", `Please add ${chain.chainName} to your wallet and try again.`);
    }
  }
}

function stringifyTypedData(value: unknown): string {
  return JSON.stringify(value, (_key, entry) => (typeof entry === "bigint" ? entry.toString() : entry));
}

/**
 * Has the customer's own wallet sign the exact x402 challenge. The private key
 * never leaves the wallet. The result is a one-time authorization for this
 * amount, payable only to the challenge's address, and is sent only to Selun's
 * x402 route. The x402 packages load on demand to keep the first page light.
 */
/** Whether this browser has a wallet of its own, such as MetaMask or Coinbase Wallet. */
export function hasBrowserWallet(): boolean {
  return typeof window !== "undefined" && Boolean((window as unknown as { ethereum?: Eip1193Provider }).ethereum?.request);
}

export const PHONE_WALLET_PROJECT_ID = process.env.NEXT_PUBLIC_WALLETCONNECT_PROJECT_ID?.trim() ?? "";

/** A wallet on another device, connected by scanning a QR code (WalletConnect). Signing happens on that device. */
export type PhoneWallet = { provider: Eip1193Provider; address: `0x${string}`; network: string; disconnect: () => Promise<void> };

type UniversalProviderLike = {
  on(event: string, listener: (value: unknown) => void): void;
  connect(options: Record<string, unknown>): Promise<{ namespaces?: Record<string, { accounts?: string[] }> } | undefined>;
  request<T>(args: { method: string; params?: unknown }, chain?: string): Promise<T>;
  disconnect(): Promise<void>;
  session?: { namespaces?: Record<string, { accounts?: string[] }> };
};

/** The pairing link holds the session's key, so its QR code is drawn here and never sent to another service. */
export async function qrDataUrl(uri: string): Promise<string> {
  const QRCode = (await import("qrcode")).default;
  return QRCode.toDataURL(uri, { width: 260, margin: 1, errorCorrectionLevel: "M" });
}

/**
 * Connects a phone wallet for one network. `onPairingUri` receives the link to show as a QR code, or to open
 * directly on a phone. Resolves once the wallet approves the connection.
 */
export async function connectPhoneWallet(network: string, onPairingUri: (uri: string) => void): Promise<PhoneWallet> {
  if (!PHONE_WALLET_PROJECT_ID) throw new WalletError("no_wallet", "Paying from a phone wallet isn't set up here yet. Please use a browser wallet.");
  if (!CHAINS[network]) throw new WalletError("wrong_network", "This payment uses a network phone wallets can't be connected to here.");
  const { default: UniversalProvider } = await import("@walletconnect/universal-provider");
  const origin = window.location.origin;
  const provider = (await UniversalProvider.init({
    projectId: PHONE_WALLET_PROJECT_ID,
    metadata: { name: "Selun", description: "Approve a Selun purchase from your wallet.", url: origin, icons: [`${origin}/favicon.ico`] },
  })) as unknown as UniversalProviderLike;
  provider.on("display_uri", (uri) => {
    if (typeof uri === "string") onPairingUri(uri);
  });
  let accounts: string[] = [];
  try {
    const session = await provider.connect({
      namespaces: {
        eip155: {
          methods: ["eth_signTypedData_v4", "eth_accounts", "eth_chainId"],
          events: ["accountsChanged", "chainChanged"],
          chains: [network],
          rpcMap: { [Number.parseInt(CHAINS[network].chainIdHex, 16)]: CHAINS[network].rpcUrl },
        },
      },
    });
    accounts = (session?.namespaces ?? provider.session?.namespaces)?.eip155?.accounts ?? [];
  } catch (error) {
    await provider.disconnect().catch(() => undefined);
    const message = String((error as { message?: unknown })?.message ?? "");
    if (isUserRejection(error) || /reject/i.test(message)) {
      throw new WalletError("rejected", "The connection was declined in your wallet. You have not been charged.");
    }
    if (/expired/i.test(message)) throw new WalletError("failed", "The QR code expired before a wallet connected. You have not been charged. Please try again.");
    throw new WalletError("failed", "We couldn't connect to your phone wallet. Please try again.");
  }
  // "eip155:8453:0xabc…" for the network that was asked for.
  const address = accounts.map((account) => account.split(":")).find((parts) => `${parts[0]}:${parts[1]}` === network)?.[2];
  if (!address || !/^0x[0-9a-fA-F]{40}$/.test(address)) {
    await provider.disconnect().catch(() => undefined);
    throw new WalletError("wrong_network", `Your wallet didn't connect on ${CHAINS[network].chainName}. Please try again and allow ${CHAINS[network].chainName}.`);
  }
  return {
    provider: { request: (args) => provider.request(args, network) },
    address: address as `0x${string}`,
    network,
    disconnect: () => provider.disconnect().catch(() => undefined),
  };
}

export async function signX402Challenge(challenge: PaymentChallenge, expectedBaseUnits: string, phone?: PhoneWallet): Promise<{ paymentSignature: string; payer: string }> {
  const option = challenge.accepts.find((accept) => accept.network.startsWith("eip155:") && accept.amount === expectedBaseUnits);
  if (!option) throw new WalletError("failed", "The payment request didn't match the price you approved, so nothing was signed.");
  if (phone && phone.network !== option.network) throw new WalletError("wrong_network", "Your phone wallet is connected on a different network. Please try again.");

  const provider = phone?.provider ?? getProvider();
  let address: `0x${string}`;
  if (phone) {
    address = phone.address;
  } else {
    try {
      const accounts = (await provider.request({ method: "eth_requestAccounts" })) as string[];
      if (!accounts?.[0]) throw new Error("no account");
      address = accounts[0] as `0x${string}`;
    } catch (error) {
      if (isUserRejection(error)) throw new WalletError("rejected", "Wallet connection was cancelled. You have not been charged.");
      throw new WalletError("failed", "We couldn't connect to your wallet. Please try again.");
    }
    await ensureChain(provider, option.network);
  }

  const [{ x402Client, x402HTTPClient }, { ExactEvmScheme }] = await Promise.all([
    import("@x402/fetch"),
    import("@x402/evm"),
  ]);

  const scheme = new ExactEvmScheme({
    address,
    signTypedData: async (message) =>
      (await provider.request({
        method: "eth_signTypedData_v4",
        params: [address, stringifyTypedData({
          ...message,
          types: { EIP712Domain: eip712DomainType(message.domain), ...message.types },
        })],
      })) as `0x${string}`,
  });
  const http = new x402HTTPClient(new x402Client().register("eip155:*", scheme));

  try {
    const single = { ...challenge, accepts: [option] };
    const payload = await http.createPaymentPayload(single as unknown as Parameters<typeof http.createPaymentPayload>[0]);
    const headers = http.encodePaymentSignatureHeader(payload);
    const paymentSignature = headers["PAYMENT-SIGNATURE"];
    if (!paymentSignature) throw new WalletError("failed", "The wallet signature could not be prepared.");
    return { paymentSignature, payer: address };
  } catch (error) {
    if (error instanceof WalletError) throw error;
    if (isUserRejection(error) || isUserRejection((error as { cause?: unknown })?.cause)) {
      throw new WalletError("rejected", "You declined the request in your wallet. You have not been charged.");
    }
    console.error("x402 signing failed", error);
    throw new WalletError("failed", "Your wallet couldn't sign this request. You have not been charged.");
  }
}

function eip712DomainType(domain: Record<string, unknown>) {
  const fields: [string, string][] = [
    ["name", "string"],
    ["version", "string"],
    ["chainId", "uint256"],
    ["verifyingContract", "address"],
    ["salt", "bytes32"],
  ];
  return fields.filter(([name]) => domain[name] !== undefined).map(([name, type]) => ({ name, type }));
}

export function toBaseUnits(priceUsdc: string): string {
  const [whole, fraction = ""] = priceUsdc.split(".");
  return (BigInt(whole || "0") * 1_000_000n + BigInt(fraction.padEnd(6, "0").slice(0, 6) || "0")).toString();
}
