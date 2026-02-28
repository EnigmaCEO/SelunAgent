import { generateMnemonic, generatePrivateKey, mnemonicToAccount, privateKeyToAccount } from "viem/accounts";

const args = new Set(process.argv.slice(2));

if (args.has("--help") || args.has("-h")) {
  printHelp();
  process.exit(0);
}

const useMnemonic = args.has("--mnemonic");
const json = args.has("--json");

const wallet = useMnemonic ? createMnemonicWallet() : createPrivateKeyWallet();

if (json) {
  console.log(JSON.stringify(wallet, null, 2));
  process.exit(0);
}

console.log("Generated EVM wallet");
console.log("");
console.log(`Address: ${wallet.address}`);
console.log(`Private Key: ${wallet.privateKey}`);
if (wallet.mnemonic) {
  console.log(`Mnemonic: ${wallet.mnemonic}`);
  console.log(`Derivation Path: ${wallet.derivationPath}`);
}
console.log("");
console.log("Smoke test env:");
console.log(`SELUN_X402_SMOKE_PRIVATE_KEY=${wallet.privateKey}`);
console.log(`EVM_PRIVATE_KEY=${wallet.privateKey}`);
console.log("");
console.log("Use this only for a throwaway test wallet. Fund it with ETH + USDC on the target network before running the x402 smoke test.");

function createPrivateKeyWallet() {
  const privateKey = generatePrivateKey();
  const account = privateKeyToAccount(privateKey);

  return {
    address: account.address,
    privateKey,
    source: "private_key",
  };
}

function createMnemonicWallet() {
  const mnemonic = generateMnemonic();
  const derivationPath = "m/44'/60'/0'/0/0";
  const account = mnemonicToAccount(mnemonic, {
    path: derivationPath,
  });

  return {
    address: account.address,
    privateKey: account.getHdKey().privateKey ? `0x${Buffer.from(account.getHdKey().privateKey).toString("hex")}` : null,
    mnemonic,
    derivationPath,
    source: "mnemonic",
  };
}

function printHelp() {
  console.log(`Usage: node scripts/generate-evm-wallet.mjs [options]

Options:
  --mnemonic   Generate from a fresh BIP-39 mnemonic instead of a raw private key
  --json       Print JSON output
  --help       Show this help text

Examples:
  node scripts/generate-evm-wallet.mjs
  node scripts/generate-evm-wallet.mjs --mnemonic
  npm run wallet:generate
  npm run wallet:generate -- --mnemonic
`);
}
