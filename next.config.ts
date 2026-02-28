import type { NextConfig } from "next";

function trimTrailingSlash(value: string) {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function getPublicBackendOrigin() {
  return trimTrailingSlash(
    process.env.SELUN_PUBLIC_BACKEND_ORIGIN?.trim()
      || process.env.SELUN_BACKEND_URL?.trim()
      || "https://selunagent.fly.dev",
  );
}

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
  async rewrites() {
    const backendOrigin = getPublicBackendOrigin();
    return [
      {
        source: "/agent/:path*",
        destination: `${backendOrigin}/agent/:path*`,
      },
      {
        source: "/execution-status/:path*",
        destination: `${backendOrigin}/execution-status/:path*`,
      },
    ];
  },
};

export default nextConfig;
