import { mnemonicToAccount } from "viem/accounts";
import { isAddress } from "viem";

const args = process.argv.slice(2);
const flags = new Set(args.filter((arg) => arg.startsWith("--")));

if (flags.has("--help") || flags.has("-h")) {
  printHelp();
  process.exit(0);
}

const options = parseOptions(args);
const mnemonic = await readMnemonic(options);

if (!mnemonic) {
  console.error("Mnemonic is required. Provide --mnemonic, pipe it on stdin, or set MNEMONIC.");
  process.exit(1);
}

if (!isValidPositiveInt(options.count) || options.count < 1) {
  console.error("--count must be a positive integer.");
  process.exit(1);
}

if (!isValidNonNegativeInt(options.index)) {
  console.error("--index must be a non-negative integer.");
  process.exit(1);
}

if (options.match && !isAddress(options.match)) {
  console.error("--match must be a valid EVM address.");
  process.exit(1);
}

if (options.path && options.count > 1) {
  console.error("--count cannot be used with --path. Use --index with the standard path template instead.");
  process.exit(1);
}

const derived = deriveAccounts({
  mnemonic,
  path: options.path,
  startIndex: options.index,
  count: options.count,
  match: options.match,
});

if (options.match) {
  const matched = derived.find((entry) => entry.address.toLowerCase() === options.match.toLowerCase());
  if (!matched) {
    console.error(`No derived address matched ${options.match} in the scanned range.`);
    console.error(`Scanned ${options.count} path(s) starting at index ${options.index}.`);
    process.exit(1);
  }
}

if (options.json) {
  console.log(JSON.stringify(derived, null, 2));
  process.exit(0);
}

printDerived(derived, options.match);

function parseOptions(argv) {
  const optionValue = (name) => {
    const inline = argv.find((arg) => arg.startsWith(`${name}=`));
    if (inline) return inline.slice(name.length + 1);
    const index = argv.indexOf(name);
    if (index === -1) return undefined;
    return argv[index + 1];
  };

  return {
    mnemonic: optionValue("--mnemonic"),
    path: optionValue("--path"),
    index: Number.parseInt(optionValue("--index") ?? "0", 10),
    count: Number.parseInt(optionValue("--count") ?? "1", 10),
    match: optionValue("--match"),
    json: flags.has("--json"),
  };
}

async function readMnemonic(options) {
  const direct = options.mnemonic?.trim();
  if (direct) return normalizeMnemonic(direct);

  const envMnemonic = process.env.MNEMONIC?.trim();
  if (envMnemonic) return normalizeMnemonic(envMnemonic);

  if (process.stdin.isTTY) return "";

  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }

  return normalizeMnemonic(Buffer.concat(chunks).toString("utf8"));
}

function normalizeMnemonic(value) {
  return value
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .join(" ");
}

function deriveAccounts(input) {
  const results = [];

  if (input.path) {
    results.push(deriveAccount(input.mnemonic, input.path));
    return results;
  }

  for (let offset = 0; offset < input.count; offset += 1) {
    const index = input.startIndex + offset;
    const path = standardEvmPath(index);
    results.push(deriveAccount(input.mnemonic, path));
  }

  return results;
}

function deriveAccount(mnemonic, path) {
  const account = mnemonicToAccount(mnemonic, { path });
  const hdKey = account.getHdKey();
  const privateKeyBytes = hdKey.privateKey;

  if (!privateKeyBytes) {
    throw new Error(`Unable to derive private key for path ${path}.`);
  }

  return {
    address: account.address,
    privateKey: `0x${Buffer.from(privateKeyBytes).toString("hex")}`,
    derivationPath: path,
  };
}

function standardEvmPath(index) {
  return `m/44'/60'/0'/0/${index}`;
}

function printDerived(derived, matchAddress) {
  console.log("Derived EVM account(s)");
  console.log("");

  for (const entry of derived) {
    const matched = matchAddress && entry.address.toLowerCase() === matchAddress.toLowerCase();
    if (matched) {
      console.log("[match]");
    }
    console.log(`Address: ${entry.address}`);
    console.log(`Private Key: ${entry.privateKey}`);
    console.log(`Derivation Path: ${entry.derivationPath}`);
    console.log("");
  }

  console.log("Use this locally only. Do not paste mnemonic phrases or private keys into chat, browsers, or third-party tools.");
}

function isValidPositiveInt(value) {
  return Number.isInteger(value) && value > 0;
}

function isValidNonNegativeInt(value) {
  return Number.isInteger(value) && value >= 0;
}

function printHelp() {
  console.log(`Usage: node scripts/derive-evm-key-from-mnemonic.mjs [options]

Derive one or more EVM private keys from a BIP-39 mnemonic.

Mnemonic input sources, in order:
  1. --mnemonic "word1 word2 ..."
  2. MNEMONIC environment variable
  3. stdin

Options:
  --mnemonic <words>   Mnemonic phrase. Avoid this if shell history is a concern.
  --path <path>        Exact derivation path, e.g. m/44'/60'/0'/0/0
  --index <n>          Start index for standard EVM path m/44'/60'/0'/0/{n}. Default: 0
  --count <n>          Number of sequential standard-path accounts to derive. Default: 1
  --match <address>    Expected address to match against derived accounts
  --json               Print JSON output
  --help               Show this help text

Examples:
  $env:MNEMONIC=\"word1 word2 ...\"; npm run wallet:derive
  $env:MNEMONIC=\"word1 word2 ...\"; npm run wallet:derive -- --count=5
  $env:MNEMONIC=\"word1 word2 ...\"; npm run wallet:derive -- --match=0xYourAddress --count=10
  Get-Content .\\mnemonic.txt | node scripts/derive-evm-key-from-mnemonic.mjs --index 0 --count 5
`);
}
