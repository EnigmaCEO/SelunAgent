delete process.env.SELUN_X402_SMOKE_URL;
process.env.SELUN_X402_SMOKE_ENDPOINT = "sce/risk-evaluate";
await import("./x402-bazaar-smoke.mjs");
