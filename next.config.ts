import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: [
    "@coinbase/agentkit",
    "@coinbase/cdp-sdk",
    "@coinbase/coinbase-sdk",
    "@solana/web3.js",
    "@solana/spl-token",
    "opensea-js",
    "@zerodev/intent",
    "@zerodev/sdk",
    "@zerodev/ecdsa-validator",
    "clanker-sdk",
  ],
};

export default nextConfig;
