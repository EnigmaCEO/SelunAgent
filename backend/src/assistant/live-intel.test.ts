import assert from "node:assert/strict";
import test from "node:test";
import { AssistantService } from "./assistant.service";
import {
  RADAR_ALERTS_URL,
  SceLiveIntel,
  advisoriesText,
  alertsText,
  scopeOf,
  selectAdvisories,
  selectAlerts,
  type AdvisoryRead,
  type AlertRead,
  type LiveIntel,
} from "./live-intel";

const NOW = new Date("2026-11-09T15:10:00.000Z");

function alert(overrides: Record<string, unknown> = {}) {
  return {
    id: "RADAR-1",
    severity: "warning",
    status: "resolved",
    monitorType: "oracle",
    summary: "Chainlink ETH/USD on Base differs from CoinMarketCap ETH/USD beyond configured threshold.",
    whatHappened: "Chainlink ETH/USD on Base diverged.",
    asset: "ETH",
    chain: "Base",
    oracle: "ETH/USD",
    visibility: "public",
    createdAt: "2026-11-07T15:46:46Z",
    resolvedAt: "2026-11-07T16:01:49Z",
    ...overrides,
  };
}

function advisory(overrides: Record<string, unknown> = {}) {
  return {
    caseId: "CASE-1",
    title: "Electrum: Malicious server can replace a just-broadcast tx",
    summary: "The main Electrum server the client connects to is trusted.",
    source: "GitHub Advisories",
    severity: "critical",
    cveRefs: ["CVE-2026-117601"],
    updatedAt: "2026-10-29T00:00:00Z",
    affectedPackages: [{ package_name: "electrum" }],
    sourceRefs: [{ source: "GitHub Advisories", externalId: "GHSA-6jp4", referenceUrls: ["https://github.com/advisories/GHSA-6jp4-8v48-cc5v", "javascript:alert(1)"] }],
    ...overrides,
  };
}

function fakeFetch(routes: (url: string) => unknown) {
  const calls: string[] = [];
  const fetch = async (url: string) => {
    calls.push(url);
    const body = routes(url);
    if (body instanceof Error) throw body;
    if (body === 500) return { ok: false, json: async () => ({}) };
    return { ok: true, json: async () => body };
  };
  return { fetch, calls };
}

test("a question's scope comes from the tokens, networks, and protocols it names", () => {
  assert.deepEqual(scopeOf("latest alerts for Ethereum").label, "Ethereum");
  assert.ok(scopeOf("latest alerts for Ethereum").terms.includes("eth"));
  assert.equal(scopeOf("any active alerts on Aave?").label, "Aave");
  assert.equal(scopeOf("show me the latest Radar alerts").label, null);
  assert.deepEqual(scopeOf("show me the latest Radar alerts").terms, []);
  // "base" and "across" are ordinary words unless written as names.
  assert.equal(scopeOf("any alerts across the board right now?").label, null);
  assert.equal(scopeOf("anything wrong with Chainlink on Base?").label, "Chainlink");
  assert.ok(scopeOf("anything wrong with Chainlink on Base?").terms.includes("base"));
  assert.ok(scopeOf("security advisories for Bitcoin").advisoryTerms.includes("electrum"));
});

test("alerts in scope: active ones first by severity, the latest either way, and nothing that isn't public", () => {
  const raw = [
    alert({ id: "A", status: "resolved", createdAt: "2026-11-07T00:00:00Z" }),
    alert({ id: "B", status: "active", severity: "warning", createdAt: "2026-11-08T00:00:00Z" }),
    alert({ id: "C", status: "active", severity: "critical", createdAt: "2026-11-06T00:00:00Z" }),
    alert({ id: "D", status: "active", asset: "BTC", chain: "Ethereum", oracle: "BTC/USD" }),
    alert({ id: "E", status: "active", asset: "GHO", chain: "Arbitrum", oracle: "GHO/USD" }),
    alert({ id: "F", status: "active", severity: "critical", visibility: "private" }),
  ];
  const read = selectAlerts(raw, scopeOf("alerts for Ethereum"), NOW.toISOString());
  assert.equal(read.state, "ok");
  if (read.state !== "ok") return;
  // ETH alerts match by asset, and the BTC oracle on Ethereum by chain; GHO and the private one don't.
  assert.deepEqual(read.active.map((a) => a.id), ["C", "B", "D"]);
  assert.equal(read.latest?.id, "B");
  assert.equal(read.active[0].url, `${RADAR_ALERTS_URL}/C`);
  const everything = selectAlerts(raw, scopeOf("latest radar alerts"), NOW.toISOString());
  assert.equal(everything.state === "ok" && everything.active.length, 4);
});

test("advisories name a term as a word, skip exploit history, and keep only safe links", () => {
  const items = selectAdvisories([
    advisory(),
    advisory({ caseId: "CASE-2", title: "CVE-1999-0101: Buffer overflow in gethostbyname", severity: "high", cveRefs: ["CVE-1999-0101"], affectedPackages: [], summary: "" }),
    advisory({ caseId: "CASE-3", title: "UniCat.farm — Rugpull ($713K lost)", source: "De.Fi REKT", summary: "ethereum" }),
    advisory({ caseId: "CASE-4", title: "MAL-2026-1: Malicious code in eth-base (npm)", severity: "medium", cveRefs: [], summary: "Steals Ethereum wallet keys.", affectedPackages: [{ package_name: "eth-base" }], sourceRefs: [{ source: "OSV", externalId: "MAL-2026-1", referenceUrls: [] }] }),
  ], ["geth", "electrum", "ethereum"]);
  assert.deepEqual(items.map((item) => item.id), ["CASE-1", "CASE-4"]);
  assert.equal(items[0].url, "https://github.com/advisories/GHSA-6jp4-8v48-cc5v");
  assert.equal(items[0].cve, "CVE-2026-117601");
  assert.equal(items[1].url, "https://osv.dev/vulnerability/MAL-2026-1");
});

test("a failed read is unavailable, never an empty list, and a good one is cached briefly", async () => {
  for (const failure of [new Error("network down"), 500, { error: "not a list" }]) {
    const { fetch } = fakeFetch(() => failure);
    const read = await new SceLiveIntel({ fetch, now: () => NOW }).alerts(scopeOf("alerts for Ethereum"));
    assert.equal(read.state, "unavailable", String(failure));
    assert.match(alertsText(read), /couldn't reach Radar's alert feed/);
    assert.doesNotMatch(alertsText(read), /no active alerts/i);
  }
  let now = NOW;
  const { fetch, calls } = fakeFetch(() => [alert()]);
  const live = new SceLiveIntel({ fetch, now: () => now });
  await live.alerts(scopeOf("alerts for Ethereum"));
  await live.alerts(scopeOf("alerts for Bitcoin"));
  assert.equal(calls.length, 1);
  now = new Date(NOW.getTime() + 61_000);
  await live.alerts(scopeOf("alerts for Ethereum"));
  assert.equal(calls.length, 2);
});

test("advisories are unavailable if any search failed, so 'none found' is only said when every search answered", async () => {
  const { fetch } = fakeFetch((url) => (url.includes("search=electrum") ? 500 : { items: [] }));
  const read = await new SceLiveIntel({ fetch, now: () => NOW }).advisories(scopeOf("security advisories for Bitcoin"));
  assert.equal(read.state, "unavailable");
  assert.match(advisoriesText(read), /couldn't reach SCE's case library/);
  const ok = fakeFetch(() => ({ items: [advisory()] }));
  const found = await new SceLiveIntel({ fetch: ok.fetch, now: () => NOW }).advisories(scopeOf("security advisories for Bitcoin"));
  assert.equal(found.state === "ok" && found.total, 1);
  assert.ok(ok.calls.every((url) => url.includes("updatedFrom=2026-08-11")), ok.calls[0]);
});

test("the wording states only what was read, with its time", () => {
  const base = { state: "ok" as const, fetchedAt: NOW.toISOString(), scope: "Ethereum" };
  const latest = selectAlerts([alert()], scopeOf("Ethereum"), NOW.toISOString());
  const none: AlertRead = { ...base, active: [], latest: latest.state === "ok" ? latest.latest : null };
  assert.equal(alertsText(none), "Radar shows no active alerts involving Ethereum as of 3:10 PM UTC on Nov 9. The most recent, a warning oracle alert from Nov 7, was resolved on Nov 7. Radar watches specific oracles, bridges, and liquidity pools, so no alert doesn't mean no risk.");
  const active = selectAlerts([alert({ status: "active", severity: "critical" })], scopeOf("Ethereum"), NOW.toISOString());
  assert.match(alertsText(active), /^Radar shows 1 active alert involving Ethereum as of 3:10 PM UTC on Nov 9\. The most severe is a critical oracle alert: Chainlink/);
  const empty: AdvisoryRead = { state: "ok", fetchedAt: NOW.toISOString(), scope: "Aave", windowDays: 90, total: 0, items: [] };
  assert.equal(advisoriesText(empty), "SCE's case library has no crypto-related security advisories relevant to Aave added or updated in the past 90 days, as of 3:10 PM UTC on Nov 9.");
});

// ---- In the assistant

const gateway = { requestChallenge: async () => { throw new Error("no"); }, submitPaid: async () => { throw new Error("no"); } };

function fakeLive(alerts: Record<string, unknown>, advisories?: Record<string, unknown>): LiveIntel & { asked: string[] } {
  const asked: string[] = [];
  return {
    asked,
    alerts: async (scope) => { asked.push(`alerts:${scope.label}`); return { ...alerts, scope: scope.label } as AlertRead; },
    advisories: async (scope) => { asked.push(`advisories:${scope.label}`); return { ...(advisories ?? { state: "ok", fetchedAt: NOW.toISOString(), windowDays: 90, total: 0, items: [] }), scope: scope.label } as AdvisoryRead; },
  };
}

test("an alerts question is answered from the live read, free and without the model", async () => {
  let calls = 0;
  const live = fakeLive({ state: "ok", fetchedAt: NOW.toISOString(), active: [], latest: null });
  const service = new AssistantService({ llm: { complete: async () => { calls += 1; return { content: "x", toolCalls: [] }; } }, gateway, loadCatalog: async () => ({ resources: [] }), live });
  const reply = await service.chat({ message: "Give me the latest alerts for Ethereum" });
  assert.equal(calls, 0);
  assert.match(reply.text, /^Radar shows no active alerts involving Ethereum as of 3:10 PM UTC on Nov 9\./);
  assert.deepEqual(reply.blocks.map((block) => block.type), ["live_status"]);
  const card = reply.blocks[0] as { alerts: { state: string; scope: string }; advisories: unknown; href: string };
  assert.deepEqual([card.alerts.state, card.alerts.scope, card.advisories, card.href], ["ok", "Ethereum", null, RADAR_ALERTS_URL]);
});

test("when the feed can't be read, the reply says so, and never that all is clear", async () => {
  const live = fakeLive({ state: "unavailable", fetchedAt: NOW.toISOString() });
  const service = new AssistantService({ llm: { complete: async () => ({ content: "x", toolCalls: [] }) }, gateway, loadCatalog: async () => ({ resources: [] }), live });
  const reply = await service.chat({ message: "Are there active alerts on Aave?" });
  assert.match(reply.text, /couldn't reach Radar's alert feed/);
  assert.equal((reply.blocks[0] as { alerts: { state: string } }).alerts.state, "unavailable");
});

test("a vulnerabilities question is answered from SCE's case library, free and without the model", async () => {
  let calls = 0;
  const items = selectAdvisories([advisory()], ["electrum"]);
  const live = fakeLive({ state: "ok", fetchedAt: NOW.toISOString(), active: [], latest: null }, { state: "ok", fetchedAt: NOW.toISOString(), windowDays: 90, total: 1, items });
  const service = new AssistantService({ llm: { complete: async () => { calls += 1; return { content: "x", toolCalls: [] }; } }, gateway, loadCatalog: async () => ({ resources: [] }), live });
  const reply = await service.chat({ message: "Any new security vulnerabilities affecting Bitcoin?" });
  assert.equal(calls, 0);
  assert.match(reply.text, /1 crypto-related security advisory relevant to Bitcoin[^.]*\. The most severe is rated critical \(CVE-2026-117601\)/);
  const card = reply.blocks[0] as { type: string; advisories: { items: { url: string }[] } };
  assert.equal(card.type, "live_status");
  assert.equal(card.advisories.items[0].url, "https://github.com/advisories/GHSA-6jp4-8v48-cc5v");
  assert.deepEqual(live.asked.sort(), ["advisories:Bitcoin", "alerts:Bitcoin"]);
});

test("a warning-signs question gives the model the live read first, and its card leads", async () => {
  const seen: { role: string; content: string | null }[][] = [];
  const toolNames: string[][] = [];
  const live = fakeLive({ state: "ok", fetchedAt: NOW.toISOString(), active: [], latest: null });
  const service = new AssistantService({
    llm: {
      complete: async (request: { messages: { role: string; content: string | null }[]; tools?: { function: { name: string } }[] }) => {
        seen.push(request.messages);
        toolNames.push((request.tools ?? []).map((tool) => tool.function.name));
        return { content: "As of 3:10 PM UTC on Nov 9, Radar shows no active alerts involving Ethereum.", toolCalls: [] };
      },
    },
    gateway,
    loadCatalog: async () => ({ resources: [] }),
    live,
  });
  const reply = await service.chat({ message: "Are there any warning signs on Ethereum right now?" });
  const tool = seen[0].find((message) => message.role === "tool");
  assert.ok(tool, "the live read was given to the model");
  const facts = JSON.parse(String(tool.content));
  assert.equal(facts.radar_alerts.active_count, 0);
  assert.equal(facts.radar_alerts.as_of, "3:10 PM UTC on Nov 9");
  assert.equal(facts.security_advisories.count, 0);
  assert.equal(reply.blocks[0].type, "live_status");
  assert.equal(reply.blocks.filter((block) => block.type === "live_status" || block.type === "live_feed").length, 1);
  assert.ok(toolNames[0].includes("check_live_status"));
});

test("without a live source, Selun keeps linking to Radar and offers no live tool", async () => {
  const toolNames: string[] = [];
  const service = new AssistantService({
    llm: { complete: async (request: { tools?: { function: { name: string } }[] }) => { toolNames.push(...(request.tools ?? []).map((tool) => tool.function.name)); return { content: "Hello.", toolCalls: [] }; } },
    gateway,
    loadCatalog: async () => ({ resources: [] }),
  });
  await service.chat({ message: "hello there" });
  assert.ok(!toolNames.includes("check_live_status"));
  const reply = await service.chat({ message: "Any new crypto vulnerabilities?" });
  assert.ok(reply.blocks.every((block) => block.type !== "live_status"));
});
