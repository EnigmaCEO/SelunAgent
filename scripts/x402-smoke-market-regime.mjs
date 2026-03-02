delete process.env.SELUN_X402_SMOKE_URL;
process.env.SELUN_X402_SMOKE_ENDPOINT = "market-regime";
await import("./x402-bazaar-smoke.mjs");
