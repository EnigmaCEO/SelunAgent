/**
 * Solana x402 smoke test.
 *
 * Hits /agent/x402/sce/continuity-mode ($0.01 USDC) with a Solana payment.
 * Requires a Solana keypair whose associated token account holds Solana USDC.
 *
 * Usage (mnemonic — recommended if from Phantom/multi-chain wallet):
 *   $env:SOLANA_SMOKE_MNEMONIC="word1 word2 ... word12"
 *   node scripts/x402-smoke-solana.mjs
 *
 * Usage (raw private key — from Phantom → Settings → Security → Export Private Key):
 *   $env:SOLANA_SMOKE_PRIVATE_KEY="<base58 64-byte secret key>"
 *   node scripts/x402-smoke-solana.mjs
 *
 * Optional:
 *   $env:SELUN_BACKEND_URL="https://selun.sagitta.systems"  (default)
 *   $env:SOLANA_DERIVATION_PATH="44'/501'/0'/0'"             (Phantom default)
 */

import { createHmac, pbkdf2Sync } from "crypto";
import { createKeyPairSignerFromBytes, getBase58Decoder } from "@solana/kit";
import { ed25519 } from "@noble/curves/ed25519";
import { ExactSvmScheme } from "@x402/svm/exact/client";

const BACKEND_URL = process.env.SELUN_BACKEND_URL?.trim() || "https://selun.sagitta.systems";
const SOLANA_MAINNET = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp";

// --- Build signer from mnemonic or raw key ---
async function buildSigner() {
  const mnemonic = process.env.SOLANA_SMOKE_MNEMONIC?.trim();
  const privateKeyB58 = process.env.SOLANA_SMOKE_PRIVATE_KEY?.trim();

  if (!mnemonic && !privateKeyB58) {
    console.error("Set SOLANA_SMOKE_MNEMONIC (seed phrase) or SOLANA_SMOKE_PRIVATE_KEY (base58).");
    process.exit(1);
  }

  if (mnemonic) {
    const pathStr = process.env.SOLANA_DERIVATION_PATH?.trim() || "44'/501'/0'/0'";
    console.log("Deriving Solana key from mnemonic at m/" + pathStr);
    const seed = pbkdf2Sync(
      Buffer.from(mnemonic.normalize("NFKD"), "utf8"),
      Buffer.from("mnemonic", "utf8"),
      2048,
      64,
      "sha512",
    );
    const indices = pathStr.split("/").map((s) => {
      const hardened = s.endsWith("'");
      const n = parseInt(s.replace("'", ""), 10);
      return hardened ? n | 0x80000000 : n;
    });
    // SLIP-0010 Ed25519 derivation
    let I = createHmac("sha512", "ed25519 seed").update(seed).digest();
    let IL = I.subarray(0, 32);
    let IR = I.subarray(32);
    for (const index of indices) {
      const data = Buffer.concat([
        Buffer.from([0x00]),
        IL,
        Buffer.from([(index >>> 24) & 0xff, (index >>> 16) & 0xff, (index >>> 8) & 0xff, index & 0xff]),
      ]);
      I = createHmac("sha512", IR).update(data).digest();
      IL = I.subarray(0, 32);
      IR = I.subarray(32);
    }
    // IL is 32-byte private seed; derive public key and build 64-byte keypair
    const publicKey = ed25519.getPublicKey(IL);
    const keypair64 = new Uint8Array(64);
    keypair64.set(IL, 0);
    keypair64.set(publicKey, 32);
    return createKeyPairSignerFromBytes(keypair64);
  }

  // Raw base58 private key (64 bytes: 32 seed + 32 pubkey, standard Solana format)
  const decoder = getBase58Decoder();
  const keyBytes = decoder.decode(privateKeyB58);
  return createKeyPairSignerFromBytes(keyBytes);
}

const signer = await buildSigner();
console.log("Buyer address:", signer.address);

const TARGET = "Cc8prGdx5kbEKuGvSRb2wg8DN9tiXMjAKmXeqeFigu5L";
if (signer.address !== TARGET) {
  console.warn(`\nWARNING: derived address does not match expected buyer ${TARGET}`);
  console.warn("Trying other common derivation paths to find the right account...");
  const mnemonic = process.env.SOLANA_SMOKE_MNEMONIC?.trim();
  if (mnemonic) {
    const { pbkdf2Sync: _pbk, createHmac: _hmac } = await import("crypto");
    const paths = [
      "44'/501'/0'/0'",
      "44'/501'/1'/0'",
      "44'/501'/2'/0'",
      "44'/501'/0'",
      "44'/501'/1'",
    ];
    const seed = pbkdf2Sync(
      Buffer.from(mnemonic.normalize("NFKD"), "utf8"),
      Buffer.from("mnemonic", "utf8"),
      2048, 64, "sha512",
    );
    for (const pathStr of paths) {
      const indices = pathStr.split("/").map((s) => {
        const h = s.endsWith("'");
        return h ? (parseInt(s, 10) | 0x80000000) : parseInt(s, 10);
      });
      let I = createHmac("sha512", "ed25519 seed").update(seed).digest();
      let IL = I.subarray(0, 32), IR = I.subarray(32);
      for (const index of indices) {
        const data = Buffer.concat([Buffer.from([0x00]), IL,
          Buffer.from([(index>>>24)&0xff,(index>>>16)&0xff,(index>>>8)&0xff,index&0xff])]);
        I = createHmac("sha512", IR).update(data).digest();
        IL = I.subarray(0, 32); IR = I.subarray(32);
      }
      const pub = ed25519.getPublicKey(IL);
      const kp = new Uint8Array(64); kp.set(IL, 0); kp.set(pub, 32);
      const s = await createKeyPairSignerFromBytes(kp);
      const match = s.address === TARGET ? " ✓ MATCH" : "";
      console.warn(`  m/${pathStr} → ${s.address}${match}`);
    }
  }
  console.error("\nSet SOLANA_DERIVATION_PATH to the matching path and re-run.");
  process.exit(1);
}

// --- Step 1: fetch 402 to get payment requirements ---
const decisionId = `solana-smoke-${Date.now()}`;
const requestBody = JSON.stringify({ decisionId, scope: "global" });

console.log("\nRequesting payment requirements...");
const res1 = await fetch(`${BACKEND_URL}/agent/x402/sce/continuity-mode`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: requestBody,
});

if (res1.status !== 402) {
  const text = await res1.text();
  console.error(`Expected 402, got ${res1.status}:`, text);
  process.exit(1);
}

const headerValue = res1.headers.get("payment-required");
if (!headerValue) {
  console.error("No PAYMENT-REQUIRED header in 402 response.");
  process.exit(1);
}

const paymentRequired = JSON.parse(
  Buffer.from(headerValue.replace(/\.$/, ""), "base64").toString("utf8"),
);

console.log(
  "Available networks:",
  paymentRequired.accepts.map((a) => `${a.network} (${Number(a.amount) / 1e6} USDC)`).join(", "),
);

// --- Find Solana requirement ---
const solanaReq = paymentRequired.accepts.find((a) => a.network === SOLANA_MAINNET);
if (!solanaReq) {
  console.error("No Solana option in PAYMENT-REQUIRED. Check SELUN_TREASURY_SOLANA_ADDRESS is set on the server.");
  process.exit(1);
}

const PAYAI_FEE_PAYER = "2wKupLR9q6wXYppw8Gr2NvWxKBUqm4PPJKkQfoxHDBg4";

if (!solanaReq.extra?.feePayer) {
  console.error("Solana requirement missing extra.feePayer — payAI facilitator did not enhance requirements.");
  console.error("Requirement:", JSON.stringify(solanaReq, null, 2));
  process.exit(1);
}

if (solanaReq.extra.feePayer !== PAYAI_FEE_PAYER) {
  console.warn(`\nWARNING: feePayer is ${solanaReq.extra.feePayer}, not payAI's (${PAYAI_FEE_PAYER}).`);
  console.warn("This means a different facilitator (likely CDP) claimed Solana support first.");
  console.warn("Overriding to payAI fee payer for smoke test — deploy the PayAIFacilitatorClient fix to resolve permanently.\n");
  solanaReq.extra.feePayer = PAYAI_FEE_PAYER;
}

console.log(`\nSolana: ${Number(solanaReq.amount) / 1e6} USDC → ${solanaReq.payTo}`);
console.log("USDC mint:", solanaReq.asset);
console.log("Fee payer:", solanaReq.extra.feePayer);

// --- Step 2: sign the Solana payment ---
console.log("\nSigning Solana transaction...");
const scheme = new ExactSvmScheme(signer);
const paymentPayload = await scheme.createPaymentPayload(2, solanaReq);
console.log("Transaction signed.");

const signaturePayload = {
  x402Version: 2,
  accepted: solanaReq,
  payload: paymentPayload.payload,
};
const paymentSignature = Buffer.from(JSON.stringify(signaturePayload)).toString("base64");

// --- Step 2b: probe payAI /verify directly (payAI uses non-standard format) ---
console.log("\nProbing payAI /verify directly...");
const payAiUrl = "https://facilitator.payai.network";
// payAI expects `paymentPayload` (decoded object) not `paymentHeader` (base64),
// and `paymentRequirements` as a single object with only feePayer in extra.
const strippedSolanaReq = {
  scheme: solanaReq.scheme,
  network: solanaReq.network,
  amount: solanaReq.amount,
  asset: solanaReq.asset,
  payTo: solanaReq.payTo,
  maxTimeoutSeconds: solanaReq.maxTimeoutSeconds,
  extra: { feePayer: solanaReq.extra.feePayer },
};
const verifyBody = {
  paymentPayload: {
    x402Version: signaturePayload.x402Version,
    scheme: solanaReq.scheme,
    network: solanaReq.network,
    accepted: strippedSolanaReq,
    payload: signaturePayload.payload,
    extensions: {},
  },
  paymentRequirements: strippedSolanaReq,
};
const verifyRes = await fetch(`${payAiUrl}/verify`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(verifyBody),
});
const verifyResult = await verifyRes.json();
console.log("payAI /verify status:", verifyRes.status);
console.log("payAI /verify response:", JSON.stringify(verifyResult, null, 2));
if (!verifyRes.ok || verifyResult.isValid === false) {
  console.error("\npayAI rejected the transaction. Not submitting to Selun.");
  process.exit(1);
}

// --- Step 3: submit payment ---
console.log("\nSubmitting payment...");
const res2 = await fetch(`${BACKEND_URL}/agent/x402/sce/continuity-mode`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "PAYMENT-SIGNATURE": paymentSignature,
  },
  body: requestBody,
});

const result = await res2.json();

if (res2.ok) {
  const sce = result.data?.result?.sce ?? result.data?.result ?? result;
  console.log("\n✓ Solana x402 payment succeeded! Status:", res2.status);
  console.log("SCE mode:", sce.mode ?? "(see full result)");
  console.log("Decision ID:", result.data?.decisionId ?? decisionId);
  const paymentResponse = res2.headers.get("payment-response");
  if (paymentResponse) {
    const pr = JSON.parse(Buffer.from(paymentResponse.replace(/\.$/, ""), "base64").toString("utf8"));
    console.log("Transaction:", pr.transaction ?? JSON.stringify(pr));
  }
} else {
  console.error("\n✗ Payment failed. Status:", res2.status);
  const reissuedHeader = res2.headers.get("payment-required");
  if (reissuedHeader) {
    const reissued = JSON.parse(Buffer.from(reissuedHeader.replace(/\.$/, ""), "base64").toString("utf8"));
    console.error("Re-issued networks:", reissued.accepts?.map((a) => a.network).join(", ") ?? "(none)");
  }
  const { logs: _logs, ...trimmed } = result?.x402 ?? {};
  console.error("x402 error:", JSON.stringify({ ...result, x402: trimmed }, null, 2));
}
