delete process.env.SELUN_X402_SMOKE_URL;
process.env.SELUN_X402_SMOKE_ENDPOINT = "asset-scorecard";
await import("./x402-bazaar-smoke.mjs");
