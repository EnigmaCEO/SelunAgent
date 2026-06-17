import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { CdpClient } from "@coinbase/cdp-sdk";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const backendRoot = path.join(projectRoot, "backend");

loadEnvFiles([
  path.join(projectRoot, ".env"),
  path.join(projectRoot, ".env.local"),
  path.join(backendRoot, ".env"),
  path.join(backendRoot, ".env.local"),
]);

const apiKeyId = (process.env.CDP_API_KEY_ID || process.env.COINBASE_API_KEY)?.trim();
const apiKeySecret = (process.env.CDP_API_KEY_SECRET || process.env.COINBASE_API_SECRET)?.trim();
const walletSecret = (process.env.CDP_WALLET_SECRET || process.env.COINBASE_WALLET_SECRET || apiKeySecret)?.trim();

if (!apiKeyId || !apiKeySecret) {
  throw new Error("Missing CDP credentials. Set COINBASE_API_KEY + COINBASE_API_SECRET (or CDP_API_KEY_ID + CDP_API_KEY_SECRET) in backend/.env.");
}

const cdp = new CdpClient({ apiKeyId, apiKeySecret, walletSecret });

console.log("Creating Solana treasury account via CDP...");

const account = await cdp.solana.createAccount();

console.log("");
console.log(`Solana treasury address: ${account.address}`);
console.log("");
console.log("Add to your Fly secrets:");
console.log(`  fly secrets set SELUN_TREASURY_SOLANA_ADDRESS=${account.address}`);
console.log("");
console.log("Add to backend/.env for local dev:");
console.log(`  SELUN_TREASURY_SOLANA_ADDRESS=${account.address}`);

function loadEnvFiles(paths) {
  for (const envPath of paths) {
    if (fs.existsSync(envPath)) {
      dotenv.config({ path: envPath, quiet: true });
    }
  }
}
