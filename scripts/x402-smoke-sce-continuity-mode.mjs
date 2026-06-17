delete process.env.SELUN_X402_SMOKE_URL;
process.env.SELUN_X402_SMOKE_ENDPOINT = "sce/continuity-mode";
await import("./x402-bazaar-smoke.mjs");
