import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AssistantService, LIVE_ALERTS_TEXT, RADAR_ALERTS_URL, asksAboutCurrentRisk, asksAboutNow, buildSystemPrompt } from "./assistant.service";
import { CAPITAL_FLOW_PAGE, CAPITAL_FLOW_PASSAGES } from "./sagitta-capital-flow";
import { KNOWLEDGE_CASES, WHITEPAPER_CASES } from "./sagitta-knowledge-cases";
import { buildFactSheet, citedSections } from "./sagitta-facts";
import { SAGITTA_SITES_SNAPSHOT } from "./sagitta-sites.data";
import { SAGITTA_CAPABILITIES, SAGITTA_SYSTEMS } from "./sagitta-directory.data";
import { EXCLUDED_NAMES, SAGITTA_HOSTS, SiteIndex, SiteKnowledge, crawlSagittaSites, pageBlocks, sourceType, whitepaperTopics, type Fetcher, type SiteCorpus } from "./sagitta-sites";

const page = (title: string, body: string) => `<html><head><title>${title}</title><script>var secret = "ignore me";</script></head><body><nav><a href="/x">Menu</a></nav><main>${body}</main><footer><p>Footer text here</p></footer></body></html>`;

function site(pages: Record<string, string>, extra: Record<string, { status?: number; url?: string; text?: string; contentType?: string }> = {}) {
  const asked: string[] = [];
  const fetcher: Fetcher = async (url) => {
    asked.push(url);
    if (extra[url]) return { status: extra[url].status ?? 200, url: extra[url].url ?? url, contentType: extra[url].contentType ?? "text/html", text: extra[url].text ?? "" };
    const key = new URL(url).pathname;
    if (pages[key] === undefined) return { status: 404, url, contentType: "text/html", text: "" };
    return { status: 200, url, contentType: "text/html", text: pages[key] };
  };
  return { fetcher, asked };
}

test("page text comes through, without scripts or page chrome", () => {
  const { title, blocks } = pageBlocks(page("About &amp; more", "<h1>Who builds this</h1><p>Xavier D. Moore is the founder.</p><ul><li>Item one here</li></ul>"));
  assert.equal(title, "About & more");
  assert.deepEqual(blocks.map((block) => block.text), ["Who builds this", "Xavier D. Moore is the founder.", "Item one here"]);
  const serialized = JSON.stringify(blocks);
  for (const absent of ["ignore me", "Menu", "Footer text"]) assert.ok(!serialized.includes(absent), absent);
});

test("text in plain divs and spans isn't lost, which is where prices often are", () => {
  const { blocks } = pageBlocks(page("Pricing", '<h2>Plans</h2><div class="card"><div>Watch</div><div><span>$7</span><span>/month</span></div></div>'));
  assert.deepEqual(blocks.map((block) => block.text), ["Plans", "Watch", "$7 /month"]);
  assert.equal(blocks[0].heading, true);
  assert.equal(blocks[1].heading, false);
});

test("the crawl stays on Sagitta's hosts and skips private paths", async () => {
  const { fetcher, asked } = site({
    "/": page("Home", '<p>Welcome to the home page.</p><a href="/about">About</a><a href="https://evil.example.com/fake">Fake</a><a href="/login">Login</a><a href="/account/x">Account</a><a href="/hidden">Hidden</a>'),
    "/about": page("About", "<p>About this site and what it does.</p>"),
    "/login": page("Login", "<p>Should never be read.</p>"),
    "/hidden": page("Hidden", "<p>Disallowed by robots.</p>"),
  }, { "https://www.sagitta.systems/robots.txt": { text: "User-agent: *\nDisallow: /hidden\n", contentType: "text/plain" } });
  const corpus = await crawlSagittaSites({ fetcher, hosts: ["www.sagitta.systems"] });
  assert.deepEqual([...new Set(corpus.chunks.map((chunk) => chunk.url))].sort(), ["https://www.sagitta.systems/", "https://www.sagitta.systems/about"]);
  assert.ok(!asked.some((url) => /evil|login|account|hidden$/.test(url)), asked.join(" "));
});

test("a page that redirects off Sagitta's host is dropped", async () => {
  const { fetcher } = site({ "/": page("Home", '<p>Home page text is here.</p><a href="/moved">Moved</a>') }, {
    "https://www.sagitta.systems/moved": { url: "https://evil.example.com/moved", text: page("Forged", "<p>Forged claim that must not be stored.</p>") },
  });
  const corpus = await crawlSagittaSites({ fetcher, hosts: ["www.sagitta.systems"] });
  assert.ok(!JSON.stringify(corpus).includes("Forged"));
});

test("a host outside the allowlist isn't on it", () => {
  for (const host of SAGITTA_HOSTS) assert.ok(/(\.sagitta\.systems|^sagitta-protocol\.gitbook\.io)$/.test(host), host);
});

test("nothing shipped mentions an excluded name", () => {
  assert.ok(!EXCLUDED_NAMES.test(JSON.stringify(SAGITTA_SITES_SNAPSHOT)));
  assert.ok(!EXCLUDED_NAMES.test(JSON.stringify({ SAGITTA_SYSTEMS, SAGITTA_CAPABILITIES })));
});

test("a page's mention of an excluded name is dropped, and the rest of the page stays", async () => {
  const { fetcher } = site({ "/": page("Home", "<p>Sagitta Labs built this part.</p><p>Written by Orion Gray here.</p><p>The Reserve holds tokenized gold.</p>") });
  const corpus = await crawlSagittaSites({ fetcher, hosts: ["www.sagitta.systems"] });
  const text = JSON.stringify(corpus);
  assert.ok(!EXCLUDED_NAMES.test(text));
  assert.ok(text.includes("tokenized gold"));
});

test("a copy from before the exclusion can't bring it back", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "selun-sites-"));
  try {
    const file = path.join(dir, "sites.json");
    const old: SiteCorpus = {
      fetchedAt: "2026-02-01T00:00:00.000Z",
      pages: 2,
      hosts: { "www.sagittalabs.com": 1, "www.sagitta.systems": 1 },
      chunks: [
        { url: "https://www.sagittalabs.com/a", site: "www.sagittalabs.com", title: "T", heading: "", text: "excluded host text" },
        { url: "https://www.sagitta.systems/b", site: "www.sagitta.systems", title: "T", heading: "", text: "Sagitta Labs mention" },
      ],
    };
    fs.writeFileSync(file, JSON.stringify(old));
    const knowledge = new SiteKnowledge({ bundled: corpus("2026-01-01T00:00:00.000Z", { "www.sagitta.systems": 1 }), file });
    assert.equal(knowledge.fetchedAt, old.fetchedAt);
    assert.deepEqual(knowledge.search("excluded mention text"), []);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("Selun can't say an excluded name, even if the model writes it", async () => {
  const service = assistant([
    { content: "Sagitta Labs is the umbrella company.", toolCalls: [] },
    { content: "Sagitta Labs is the umbrella company.", toolCalls: [] },
  ]);
  const reply = await service.chat({ message: "What is Sagitta Labs?" });
  assert.ok(!EXCLUDED_NAMES.test(reply.text), reply.text);

  const corrected = assistant([
    { content: "Orion Gray wrote it.", toolCalls: [] },
    { content: "I can only help with Sagitta Systems.", toolCalls: [] },
  ]);
  assert.equal((await corrected.chat({ message: "Who wrote the whitepaper?" })).text, "I can only help with Sagitta Systems.");
});

test("accounts that appear only in footers, icons, or metadata are still found", async () => {
  const home = `<html><head><title>Home</title><script type="application/ld+json">{"sameAs":["https://x.com/SagittaSystems"]}</script></head><body><main><p>Welcome to the site.</p></main><footer><a href="https://discord.gg/abc123"><svg></svg></a><a href="https://x.com/SagittaSystems/status/123">post</a><a href="mailto:contact@sagitta.systems">Email</a><a href="mailto:security@sagittalabs.com">Security</a><a href="https://x.com/SomeoneElse">Them</a></footer></body></html>`;
  const { fetcher } = site({ "/": home });
  const corpus = await crawlSagittaSites({ fetcher, hosts: ["www.sagitta.systems"] });
  const chunk = corpus.chunks.find((item) => item.text.includes("x.com/SagittaSystems"));
  assert.ok(chunk);
  assert.equal(chunk.text, "YouTube: https://www.youtube.com/@sagittasystems. X (Twitter): https://x.com/SagittaSystems. Discord: https://discord.gg/abc123. Email: contact@sagitta.systems.");
  assert.ok(new SiteIndex(corpus).search("what is the twitter page").some((hit) => hit.text.includes("x.com/SagittaSystems")));
});

test("every expected fact in the knowledge cases is really on the Sagitta page it cites", () => {
  const snapshot = SAGITTA_SITES_SNAPSHOT as SiteCorpus;
  const wrong: string[] = [];
  for (const testCase of [...KNOWLEDGE_CASES, ...WHITEPAPER_CASES]) {
    if (!testCase.facts?.length) continue;
    const text = snapshot.chunks.filter((chunk) => chunk.url === testCase.source).map((chunk) => `${chunk.heading} ${chunk.text}`).join(" ");
    for (const fact of testCase.facts) {
      if (!fact.test(text)) wrong.push(`"${testCase.ask}": ${fact.source} isn't on ${testCase.source}`);
    }
  }
  assert.deepEqual(wrong, []);
});

test("the fact sheet holds the basics, read from the sites", () => {
  const sheet = buildFactSheet(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  assert.ok(sheet);
  for (const fact of ["Xavier D. Moore", "development network", "Watch $29/month", "From $2,500/month", "Sandbox Authority: $79 per month", "Doctrine Authority: Custom pricing", "Starter Defense Review $3,000", "Allocation: $19.00 USDC", "Comprehensive Intelligence Report: $4.99 USDC", "Research Horizon", "x.com/SagittaSystems", "youtube.com/@sagittasystems", "selun@sagitta.systems"]) {
    assert.ok(sheet.text.includes(fact), fact);
  }
  assert.ok(!EXCLUDED_NAMES.test(sheet.text));
  assert.deepEqual(citedSections(sheet, "It costs $29 a month.").map((section) => section.url), ["https://radar.sagitta.systems/pricing"]);
  assert.deepEqual(citedSections(sheet, "It costs $299 a month."), []);
});

test("a sheet with a missing price is refused, and the last good one stays through a refresh", async () => {
  const good = SAGITTA_SITES_SNAPSHOT as SiteCorpus;
  const broken: SiteCorpus = { ...good, fetchedAt: "2099-01-01T00:00:00.000Z", chunks: good.chunks.map((chunk) => (chunk.heading === "Watch" && chunk.url.endsWith("radar.sagitta.systems/pricing") ? { ...chunk, text: "Contact us" } : chunk)) };
  assert.equal(buildFactSheet(broken), null);
  const knowledge = new SiteKnowledge({ bundled: good, crawl: async () => broken });
  const before = knowledge.facts?.text;
  assert.ok(before);
  await knowledge.refresh();
  assert.equal(knowledge.facts?.text, before);
});

test("an answer from the sheet needs no search, passes the price check, and cites its page", async () => {
  const sheet = buildFactSheet(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const asked: number[] = [];
  const responses = [{ content: "Radar Watch is $29 a month.", toolCalls: [] }];
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  const service = new AssistantService({
    llm: { complete: async () => { asked.push(1); return responses.shift() ?? { content: "Done.", toolCalls: [] }; } },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: () => [], fetchedAt: "2026-11-05T00:00:00.000Z", facts: sheet },
  });
  const reply = await service.chat({ message: "How much is Radar Watch?" });
  assert.equal(asked.length, 1);
  assert.equal(reply.text, "Radar Watch is $29 a month.");
  const sources = reply.blocks.find((block) => block.type === "sagitta_sources");
  assert.equal(sources?.type === "sagitta_sources" && sources.sources[0].url, "https://radar.sagitta.systems/pricing");
});

test("a passage already in the conversation isn't sent to the model again", async () => {
  const sent: string[] = [];
  const responses = [
    { content: null, toolCalls: [call("search_sagitta_sites", { query: "Radar pricing plans" })] },
    { content: "First answer.", toolCalls: [] },
    { content: null, toolCalls: [call("search_sagitta_sites", { query: "Radar pricing plans" }, "2")] },
    { content: "Second answer.", toolCalls: [] },
  ];
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  const service = new AssistantService({
    llm: {
      complete: async (request) => {
        const last = request.messages.at(-1);
        if (last?.role === "tool") sent.push(last.content);
        return responses.shift() ?? { content: "Done.", toolCalls: [] };
      },
    },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: (query) => realIndex.search(query), fetchedAt: "2026-11-05T00:00:00.000Z" },
  });
  const first = await service.chat({ message: "What are the Radar plans?" });
  await service.chat({ conversationId: first.conversationId, message: "What are the Radar plans again?" });
  const passages = sent.map((content) => (JSON.parse(content) as { passages: unknown[] }).passages.length);
  assert.ok(passages[0] > 0);
  assert.ok(passages[1] < passages[0], `${passages}`);
});

test("repeated navigation text doesn't crowd out real answers", async () => {
  const pages: Record<string, string> = { "/": page("Home", `<p>Repeated banner across all the pages.</p><p>Unique home text.</p>${[1, 2, 3, 4, 5].map((n) => `<a href="/p${n}">p</a>`).join("")}`) };
  for (const n of [1, 2, 3, 4, 5]) pages[`/p${n}`] = page(`P${n}`, `<p>Repeated banner across all the pages.</p><p>Unique text number ${n}.</p>`);
  const corpus = await crawlSagittaSites({ fetcher: site(pages).fetcher, hosts: ["www.sagitta.systems"] });
  assert.ok(!JSON.stringify(corpus.chunks).includes("Repeated banner"));
  assert.ok(JSON.stringify(corpus.chunks).includes("Unique text number 3"));
});

const realIndex = new SiteIndex(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
const topPages = (query: string) => realIndex.search(query).map((match) => match.url);

test("real questions find the right pages in the shipped snapshot", () => {
  const cases: [string, RegExp][] = [
    ["who is the developer", /sagitta\.systems\/(about|press)/],
    ["who founded Sagitta", /sagitta\.systems\/(about|press)/],
    ["who builds Sagitta", /sagitta\.systems\/(about|press)/],
    ["AAA methodology", /aaa\.sagitta\.systems\/methodology/],
    ["Radar pricing plans", /radar\.sagitta\.systems\/pricing|sagitta-radar/],
    ["Defense Review flat fee", /defense\.sagitta\.systems/],
    ["careers open roles", /sagitta\.systems\/careers/],
    ["whitepaper Reserve composition", /gitbook\.io\/sagitta-whitepaper\/reserve/],
    ["roadmap", /sagitta\.systems\/roadmap/],
    ["Sagitta Radar pricing Watch Intel Signal Desk plan price", /radar\.sagitta\.systems\/pricing/],
  ];
  for (const [query, expected] of cases) assert.ok(topPages(query).some((url) => expected.test(url)), `${query}: ${topPages(query).join(" ")}`);
});

test("the founder's public profile is in the snapshot", () => {
  assert.ok(realIndex.search("who is the founder of Sagitta Systems").some((match) => match.text.includes("Xavier D. Moore")));
});

test("Radar's published prices are in the snapshot", () => {
  const text = realIndex.search("Sagitta Radar pricing Watch Intel Signal Desk plan price", 8).map((match) => match.text).join(" ");
  for (const price of ["$29", "$149"]) assert.ok(text.includes(price), `${price} in: ${text.slice(0, 300)}`);
});

test("the same question always gets the same passages", () => {
  assert.deepEqual(realIndex.search("Radar plans"), realIndex.search("Radar plans"));
  assert.deepEqual(realIndex.search("the of and"), [], "nothing searchable");
});

const corpus = (fetchedAt: string, hosts: Record<string, number>): SiteCorpus => ({
  fetchedAt,
  pages: Object.values(hosts).reduce((sum, count) => sum + count, 0),
  hosts,
  chunks: Object.entries(hosts).flatMap(([host, count]) => Array.from({ length: count }, (_, index) => ({ url: `https://${host}/p${index}`, site: host, title: `${host} ${fetchedAt}`, heading: "", text: `text ${fetchedAt}` }))),
});

test("a site that's down keeps its last good pages, and the others update", async () => {
  const before = corpus("2026-01-01T00:00:00.000Z", { "a.sagitta.systems": 10, "b.sagitta.systems": 10 });
  const after = corpus("2026-02-01T00:00:00.000Z", { "a.sagitta.systems": 10, "b.sagitta.systems": 0 });
  const knowledge = new SiteKnowledge({ bundled: before, crawl: async () => after });
  await knowledge.refresh();
  assert.equal(knowledge.fetchedAt, after.fetchedAt);
  const text = JSON.stringify(knowledge.search("text"));
  assert.ok(text.includes("2026-02-01"));
  const merged = knowledge.merge(after)!;
  assert.equal(merged.hosts["b.sagitta.systems"], 10, "not wiped");
});

test("a read that loses most of a site's pages isn't served", () => {
  const before = corpus("2026-01-01T00:00:00.000Z", { "a.sagitta.systems": 20 });
  const knowledge = new SiteKnowledge({ bundled: before });
  const merged = knowledge.merge(corpus("2026-02-01T00:00:00.000Z", { "a.sagitta.systems": 3 }))!;
  assert.equal(merged.hosts["a.sagitta.systems"], 20);
  assert.ok(merged.chunks.every((chunk) => chunk.text.includes("2026-01-01")));
});

test("a failed refresh leaves the current read in place", async () => {
  const before = corpus("2026-01-01T00:00:00.000Z", { "a.sagitta.systems": 2 });
  const knowledge = new SiteKnowledge({ bundled: before, crawl: async () => { throw new Error("network down"); } });
  await knowledge.refresh();
  assert.equal(knowledge.fetchedAt, before.fetchedAt);
});

test("a newer read survives a restart, and an older one doesn't replace a newer build", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "selun-sites-"));
  try {
    const file = path.join(dir, "sites.json");
    const bundled = corpus("2026-01-01T00:00:00.000Z", { "a.sagitta.systems": 2 });
    const first = new SiteKnowledge({ bundled, file, crawl: async () => corpus("2026-02-01T00:00:00.000Z", { "a.sagitta.systems": 2 }) });
    await first.refresh();
    assert.equal(new SiteKnowledge({ bundled, file }).fetchedAt, "2026-02-01T00:00:00.000Z");
    assert.equal(new SiteKnowledge({ bundled: corpus("2026-03-01T00:00:00.000Z", { "a.sagitta.systems": 2 }), file }).fetchedAt, "2026-03-01T00:00:00.000Z");
    fs.writeFileSync(file, "{ damaged");
    assert.equal(new SiteKnowledge({ bundled, file }).fetchedAt, bundled.fetchedAt, "a damaged file falls back");
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("it's due for a refresh after a day", () => {
  const bundled = corpus("2026-01-01T00:00:00.000Z", { "a.sagitta.systems": 1 });
  assert.equal(new SiteKnowledge({ bundled, now: () => new Date("2026-01-01T12:00:00Z") }).isStale, false);
  assert.equal(new SiteKnowledge({ bundled, now: () => new Date("2026-01-02T00:00:00Z") }).isStale, true);
});

function assistant(responses: { content: string | null; toolCalls: { id: string; type: "function"; function: { name: string; arguments: string } }[] }[]) {
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  return new AssistantService({
    llm: { complete: async () => responses.shift() ?? { content: "Done.", toolCalls: [] } },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: (query) => realIndex.search(query), fetchedAt: "2026-11-05T00:00:00.000Z" },
  });
}

const call = (name: string, args: unknown, id = "1") => ({ id, type: "function" as const, function: { name, arguments: JSON.stringify(args) } });

test("an answer from the sites lists its real sources, and sells nothing", async () => {
  const service = assistant([{ content: null, toolCalls: [call("search_sagitta_sites", { query: "who builds Sagitta founder" })] }, { content: "Sagitta Systems' founder is Xavier D. Moore.", toolCalls: [] }]);
  const reply = await service.chat({ message: "Who is the developer?" });
  assert.deepEqual(reply.blocks.map((block) => block.type), ["sagitta_sources"]);
  const block = reply.blocks[0];
  assert.ok(block.type === "sagitta_sources");
  assert.ok(block.sources.length >= 1 && block.sources.length <= 5);
  assert.ok(block.sources.every((source) => SAGITTA_HOSTS.includes(new URL(source.url).hostname as (typeof SAGITTA_HOSTS)[number])));
  assert.equal(new Set(block.sources.map((source) => source.url)).size, block.sources.length, "no repeats");
});

test("searching twice in a turn shows one list of sources", async () => {
  const service = assistant([{ content: null, toolCalls: [call("search_sagitta_sites", { query: "Radar plans" }, "1"), call("search_sagitta_sites", { query: "Radar pricing" }, "2")] }, { content: "See the plans.", toolCalls: [] }]);
  assert.equal((await service.chat({ message: "Radar plans?" })).blocks.filter((block) => block.type === "sagitta_sources").length, 1);
});

test("an answer that the sites don't cover something shows no sources", async () => {
  const service = assistant([{ content: null, toolCalls: [call("search_sagitta_sites", { query: "founder favorite color" })] }, { content: "Sagitta's websites don't say that.", toolCalls: [] }]);
  assert.deepEqual((await service.chat({ message: "What's the founder's favorite color?" })).blocks, []);
});

test("only the strongest pages are listed as sources", async () => {
  const service = assistant([{ content: null, toolCalls: [call("search_sagitta_sites", { query: "Sagitta Radar pricing plans" })] }, { content: "Here are the plans.", toolCalls: [] }]);
  const block = (await service.chat({ message: "Radar plans?" })).blocks[0];
  assert.ok(block.type === "sagitta_sources");
  assert.ok(block.sources.length <= 4);
  assert.ok(block.sources.some((source) => source.url.includes("radar.sagitta.systems/pricing")), JSON.stringify(block.sources));
});

test("a dollar figure the sites didn't give is still held back", async () => {
  const service = assistant([
    { content: null, toolCalls: [call("search_sagitta_sites", { query: "Radar pricing" })] },
    { content: "Radar costs exactly $123456 a month.", toolCalls: [] },
    { content: "Radar costs exactly $123456 a month.", toolCalls: [] },
  ]);
  assert.ok(!(await service.chat({ message: "How much is Radar?" })).text.includes("$123456"));
});

test("a published price can be repeated, because it came from the sites", async () => {
  const service = assistant([
    { content: null, toolCalls: [call("search_sagitta_sites", { query: "Sagitta Defense Review flat fee 3,000" })] },
    { content: "The Starter Defense Review is a flat $3,000 according to Sagitta's site.", toolCalls: [] },
  ]);
  assert.ok((await service.chat({ message: "What does a Defense Review cost?" })).text.includes("$3,000"));
});

test("with nothing found, no sources appear and the model is told not to guess", async () => {
  const seen: string[] = [];
  const refuse = async () => {
    throw new Error("no");
  };
  const service = new AssistantService({
    llm: {
      complete: async (request) => {
        const last = request.messages.at(-1);
        if (last?.role === "tool") {
          seen.push(last.content);
          return { content: "Sagitta's websites don't cover that.", toolCalls: [] };
        }
        return { content: null, toolCalls: [call("search_sagitta_sites", { query: "zzzznothing" })] };
      },
    },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: () => [], fetchedAt: "2026-11-05T00:00:00.000Z" },
  });
  const reply = await service.chat({ message: "What is the CEO's favorite color?" });
  assert.equal(reply.blocks.length, 0);
  assert.ok(seen[0].includes("no_results"));
});

const WHITEPAPER_PAGE = "https://sagitta-protocol.gitbook.io/sagitta-whitepaper";

test("questions the whitepaper governs read the page that answers them", () => {
  const index = new SiteIndex(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const expected: [string, string][] = [
    ["Is Sagitta a bank?", "scope-and-non-banking-clarification"],
    ["Who regulates the Sagitta Protocol?", "scope-and-non-banking-clarification"],
    ["Are deposits in the Sagitta Protocol FDIC insured?", "scope-and-non-banking-clarification"],
    ["How does the Treasury work?", "treasury"],
    ["What is the Vault?", "vault"],
    ["What backs the Reserve?", "reserve"],
    ["What does the Escrow do?", "escrow"],
    ["What are the system invariants?", "system-invariants"],
    ["What happens during an evacuation?", "continuity-engine"],
    ["What happens if an oracle fails in the protocol?", "failure-threat-matrix"],
    ["How does capital flow through the protocol?", "capital-flow-diagram"],
  ];
  for (const [question, page] of expected) {
    // The model's own words don't decide this, even when they're someone else's.
    const matches = index.search("vault custody treasury liquidity gold-backed reserve", 8, 5, question);
    assert.ok(matches.some((match) => match.url === `${WHITEPAPER_PAGE}/${page}`), `${question} should read ${page}`);
    assert.equal(sourceType(matches[0].url), "whitepaper", `${question} should lead with the whitepaper`);
  }
});

test("the Capital Flow Diagram can be answered from its approved text, which a refresh can't lose", () => {
  const index = new SiteIndex(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const matches = index.search("capital flow", 8, 5, "How does capital flow through the Sagitta Protocol?");
  assert.ok(matches.some((match) => match.heading === "Capital Flow: the Reserve"));
  assert.ok(matches.some((match) => match.text.includes("Vault deposits aren't automatically transferred")));
  assert.ok(!CAPITAL_FLOW_PASSAGES.some((passage) => EXCLUDED_NAMES.test(passage.text)));
  const elsewhere = new SiteIndex({ fetchedAt: "2026-11-05T00:00:00.000Z", pages: 1, hosts: {}, chunks: [{ url: "https://www.sagitta.systems/", site: "www.sagitta.systems", title: "Home", heading: "", text: "Reserve" }] });
  assert.ok(elsewhere.search("capital flow Reserve").every((match) => match.url !== CAPITAL_FLOW_PAGE), "not added where its page is gone");
});

test("the same words in another sense don't pull in the whitepaper", () => {
  for (const question of [
    "What does AAA do for treasury operators?",
    "How should my DAO treasury be allocated?",
    "What is Sagitta Banking?",
    "How much is Radar Watch?",
    "Is USDC FDIC insured?",
    "What is the Federal Reserve?",
    "What happens if a stablecoin depegs?",
    "Is Radar a bank?",
  ]) {
    assert.deepEqual(whitepaperTopics(question), [], question);
  }
});

test("a governing page is searched for what was asked, not just its opening", () => {
  const index = new SiteIndex(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  assert.ok(index.search("escrow", 8, 5, "Can the Escrow access Reserve assets directly?").some((match) => match.text.includes("does not access Reserve assets directly")));
  assert.ok(index.search("vault", 8, 5, "Can I withdraw from the Sagitta Vault at any time?").some((match) => match.text.includes("12-month commitment period")), "withdraw finds withdrawal");
});

test("a whitepaper question reads the page before the model answers, and shows it", async () => {
  const index = new SiteIndex(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const sheet = buildFactSheet(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const requests: { tools: unknown; passages: string }[] = [];
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  const service = new AssistantService({
    llm: {
      complete: async (request) => {
        requests.push({ tools: request.tools, passages: request.messages.filter((message) => message.role === "tool").map((message) => message.content).join("\n") });
        return { content: "The whitepaper doesn't name a regulator.", toolCalls: [] };
      },
    },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: (query: string, question?: string) => index.search(query, 8, 5, question ?? ""), fetchedAt: "2026-11-05T00:00:00.000Z", facts: sheet },
  });
  const reply = await service.chat({ message: "Who regulates the Sagitta Protocol?" });
  assert.equal(requests.length, 1, "no extra request");
  assert.ok(requests[0].tools, "the model can still search further");
  assert.match(requests[0].passages, /"source_type":"whitepaper"/);
  assert.match(requests[0].passages, /does not engage in banking|not a banking institution/);
  const sources = reply.blocks.find((block) => block.type === "sagitta_sources");
  assert.ok(sources?.type === "sagitta_sources" && sources.sources[0].url.endsWith("/scope-and-non-banking-clarification"), "an answer from the page still shows the page");
  assert.ok(sources?.type === "sagitta_sources" && sources.sources.every((source) => sourceType(source.url) === "whitepaper"), "marketing pages aren't listed as authorities on this");
});

test("a whitepaper answer shows what's actually deployed, even if the reply doesn't say", async () => {
  const index = new SiteIndex(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  const service = new AssistantService({
    llm: { complete: async () => ({ content: "The whitepaper describes a Reserve mechanism.", toolCalls: [] }) },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: (query: string, question?: string) => index.search(query, 8, 5, question ?? ""), fetchedAt: "2026-11-05T00:00:00.000Z", facts: buildFactSheet(SAGITTA_SITES_SNAPSHOT as SiteCorpus) },
  });
  const reply = await service.chat({ message: "Is my principal guaranteed by Sagitta?" });
  assert.ok(reply.blocks.some((block) => block.type === "sagitta_system" && block.system.slug.includes("protocol") && block.system.status === "Public Test"));
});

test("a whitepaper reply that can't be fixed points to the page instead of offering to sell something", async () => {
  const index = new SiteIndex(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  let requests = 0;
  const service = new AssistantService({
    llm: { complete: async () => { requests += 1; return { content: "The Treasury converts Vault deposits into batches.", toolCalls: [] }; } },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: (query: string, question?: string) => index.search(query, 8, 5, question ?? ""), fetchedAt: "2026-11-05T00:00:00.000Z", facts: buildFactSheet(SAGITTA_SITES_SNAPSHOT as SiteCorpus) },
  });
  const reply = await service.chat({ message: "How does the Treasury work?" });
  assert.equal(requests, 2, "one rewrite, then stop");
  assert.match(reply.text, /read the whitepaper page/);
  assert.doesNotMatch(reply.text, /converts/);
  const sources = reply.blocks.find((block) => block.type === "sagitta_sources");
  assert.ok(sources?.type === "sagitta_sources" && sources.sources[0].url.endsWith("/treasury"));
});

test("offering to search instead of searching is sent to search", async () => {
  const sheet = buildFactSheet(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  const responses = [
    { content: "Sagitta Protocol is Public Test. If you want, I can look up more about it.", toolCalls: [] },
    { content: null, toolCalls: [call("search_sagitta_sites", { query: "Sagitta Protocol founders" })] },
    { content: "Here's what it says.", toolCalls: [] },
  ];
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  const searched: string[] = [];
  const service = new AssistantService({
    llm: { complete: async () => responses.shift() ?? { content: "Done.", toolCalls: [] } },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
    sites: { search: (query: string) => { searched.push(query); return []; }, fetchedAt: "2026-11-05T00:00:00.000Z", facts: sheet },
  });
  await service.chat({ message: "Tell me about Sagitta Protocol's history." });
  assert.deepEqual(searched, ["Sagitta Protocol founders"]);
});

test("the fact sheet links the whitepaper and the instructions rank the sources", () => {
  const sheet = buildFactSheet(SAGITTA_SITES_SNAPSHOT as SiteCorpus);
  assert.ok(sheet);
  assert.ok(sheet.text.includes("Whitepaper: https://sagitta-protocol.gitbook.io."));
  assert.deepEqual(citedSections(sheet, "Read it at https://sagitta-protocol.gitbook.io.").map((section) => section.title), ["Sagitta Protocol Whitepaper"]);
  const prompt = buildSystemPrompt("2026-11-06", sheet, "2026-11-06");
  assert.match(prompt, /the whitepaper governs/);
  assert.match(prompt, /Never infer legal status or regulatory authority/);
});

test("Radar's live alert pages are never kept, so old alerts can't be read back as current", () => {
  const alertsChunks = SAGITTA_SITES_SNAPSHOT.chunks.filter((chunk) => chunk.url.startsWith("https://radar.sagitta.systems/alerts"));
  assert.ok(alertsChunks.length > 0, "the bundled copy still holds alert pages for this test to mean anything");
  const knowledge = new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT });
  const found = knowledge.search("Radar alerts Ethereum critical", "Give me the latest alerts for Ethereum");
  assert.ok(found.length > 0);
  assert.ok(found.every((match) => !match.url.startsWith("https://radar.sagitta.systems/alerts")));
});

test("a refresh without the live alert pages doesn't look like Radar's site shrinking", async () => {
  const chunk = (url: string) => ({ url, site: "radar.sagitta.systems", title: "Radar", heading: "", text: `Text for ${url}` });
  const before: SiteCorpus = { fetchedAt: "2026-01-01T00:00:00.000Z", pages: 4, hosts: { "radar.sagitta.systems": 4 }, chunks: [chunk("https://radar.sagitta.systems/"), chunk("https://radar.sagitta.systems/alerts"), chunk("https://radar.sagitta.systems/alerts/RADAR-1"), chunk("https://radar.sagitta.systems/alerts/RADAR-2")] };
  const after: SiteCorpus = { fetchedAt: "2026-02-01T00:00:00.000Z", pages: 1, hosts: { "radar.sagitta.systems": 1 }, chunks: [{ ...chunk("https://radar.sagitta.systems/"), text: "Fresh Radar homepage" }] };
  const knowledge = new SiteKnowledge({ bundled: before, crawl: async () => after });
  await knowledge.refresh();
  assert.equal(knowledge.fetchedAt, after.fetchedAt);
  assert.ok(knowledge.search("Fresh Radar homepage", "").some((match) => match.text === "Fresh Radar homepage"));
});

test("the crawl skips Radar's live alert pages", async () => {
  const { fetcher, asked } = site({ "/": page("Radar", '<p>Radar watches DeFi infrastructure.</p><a href="/alerts">Alerts</a><a href="/alerts/RADAR-1">One</a><a href="/pricing">Pricing</a>'), "/pricing": page("Pricing", "<p>Plans and prices for Radar.</p>"), "/alerts": page("Alerts", "<p>Critical alert now.</p>") });
  const corpus = await crawlSagittaSites({ fetcher, hosts: ["radar.sagitta.systems"] });
  assert.ok(!asked.some((url) => url.includes("/alerts")));
  assert.ok(corpus.chunks.every((c) => !c.url.includes("/alerts")));
});

/** Words a customer might use for what's happening now. */
const CURRENT_EVENT_QUERIES = ["Radar alerts Ethereum critical", "latest incidents Ethereum", "Radar incidents today", "active alerts Aave", "crvUSD USDC balance concentration observed", "stale beyond expected heartbeat Chainlink"];
const isLivePage = (url: string) => url.startsWith("https://radar.sagitta.systems/alerts");

test("old alerts can't come back after a restart from a saved copy that still holds them", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "selun-sites-"));
  try {
    const file = path.join(dir, "sites.json");
    // A copy saved before live pages were excluded, newer than the bundled one, so it's the one loaded.
    fs.writeFileSync(file, JSON.stringify({ ...SAGITTA_SITES_SNAPSHOT, fetchedAt: "2099-01-01T00:00:00.000Z" }));
    const knowledge = new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT, file });
    assert.equal(knowledge.fetchedAt, "2099-01-01T00:00:00.000Z");
    for (const query of CURRENT_EVENT_QUERIES) {
      const found = knowledge.search(query, query);
      assert.ok(found.every((match) => !isLivePage(match.url)), `${query}: ${found.map((m) => m.url).join(", ")}`);
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("old alerts can't come back through an index refresh, or the copy it saves", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "selun-sites-"));
  try {
    const file = path.join(dir, "sites.json");
    // Even a crawl that somehow returns alert pages doesn't put them back.
    const crawled: SiteCorpus = { ...SAGITTA_SITES_SNAPSHOT, fetchedAt: "2099-02-01T00:00:00.000Z" };
    const knowledge = new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT, file, crawl: async () => crawled });
    await knowledge.refresh();
    assert.equal(knowledge.fetchedAt, crawled.fetchedAt);
    for (const query of CURRENT_EVENT_QUERIES) assert.ok(knowledge.search(query, query).every((match) => !isLivePage(match.url)), query);
    const saved = JSON.parse(fs.readFileSync(file, "utf8")) as SiteCorpus;
    assert.ok(saved.chunks.every((chunk) => !isLivePage(chunk.url)));
    // And the next restart, from what the refresh saved, stays clean.
    const restarted = new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT, file });
    for (const query of CURRENT_EVENT_QUERIES) assert.ok(restarted.search(query, query).every((match) => !isLivePage(match.url)), query);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("the prompt sends current alerts and latest incidents to Radar, and past incidents to Threat Case Relevance", () => {
  const prompt = buildSystemPrompt("November 8, 2026");
  assert.match(prompt, /latest alerts for Ethereum[^\n]*https:\/\/radar\.sagitta\.systems\/alerts/);
  assert.match(prompt, /any recent incidents involving Aave\?[^\n]*propose_sce_case_relevance/);
  assert.match(prompt, /status with the date it was reported/);
});

test("questions about what's happening now are told apart from past cases and from Radar itself", () => {
  for (const q of ["Show me the latest Radar alerts", "Give me the latest alerts for Ethereum", "Are there active alerts on Aave?", "Any current warnings for Curve?", "Open Radar's live feed"]) assert.ok(asksAboutNow(q), q);
  // Broader than alerts: answered with the services that fit, plus the Radar card.
  for (const q of ["Are there any warning signs on Ethereum right now?", "What are the risks of Ethereum right now?", "Any red flags in crypto today?"]) {
    assert.ok(!asksAboutNow(q), q);
    assert.ok(asksAboutCurrentRisk(q), q);
  }
  for (const q of ["Analyze the risks of Ethereum", "Show me the latest Radar alerts", "Any recent incidents involving Aave?", "Is it safe to use DeFi right now?"]) assert.ok(!asksAboutCurrentRisk(q), q);
  // Incident history, even recent, is Threat Case Relevance; what Radar is, is product information.
  for (const q of ["Any recent incidents involving Aave?", "What incidents affected Aave last month?", "Have there been any known incidents affecting Aave?", "What does Radar monitor?", "How do Radar alerts work?", "What is the latest Radar pricing?", "Is it safe to use DeFi right now?", "Analyze the risks of Bitcoin"]) assert.ok(!asksAboutNow(q), q);
});

test("a question about what's happening now gets Radar's live page on its own, without the model or the website cards", async () => {
  let calls = 0;
  const service = new AssistantService({
    llm: { complete: async () => { calls += 1; return { content: "Radar is operating.", toolCalls: [] }; } },
    gateway: { requestChallenge: async () => { throw new Error("no"); }, submitPaid: async () => { throw new Error("no"); } },
    loadCatalog: async () => ({ resources: [] }),
    sites: new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT }),
  });
  for (const message of ["Show me the latest Radar alerts", "Give me the latest alerts for Ethereum", "Are there active alerts on Aave?"]) {
    const reply = await service.chat({ message });
    assert.equal(reply.text, LIVE_ALERTS_TEXT, message);
    assert.deepEqual(reply.blocks.map((block) => block.type), ["live_feed"], message);
    const feed = reply.blocks[0] as { href: string; linkLabel: string };
    assert.equal(feed.href, RADAR_ALERTS_URL);
    assert.equal(feed.linkLabel, "View latest Radar alerts");
  }
  assert.equal(calls, 0);
});

test("a live ticker inside an ordinary page, such as Radar's homepage feed, is dropped too", () => {
  const chunk = (text: string) => ({ url: "https://radar.sagitta.systems/", site: "radar.sagitta.systems", title: "Radar", heading: "", text });
  const knowledge = new SiteKnowledge({ bundled: { fetchedAt: "2026-01-01T00:00:00.000Z", pages: 1, hosts: { "radar.sagitta.systems": 1 }, chunks: [chunk("Chainlink ETH/USD on Arbitrum is stale beyond heartbeat · oracle · 4m ago"), chunk("Radar tracks oracle price freshness and deviation.")] } });
  const found = knowledge.search("Chainlink oracle stale heartbeat freshness", "");
  assert.ok(found.length > 0);
  assert.ok(found.every((match) => !/ago/.test(match.text)));
  assert.ok(SAGITTA_SITES_SNAPSHOT.chunks.some((c) => c.url === "https://radar.sagitta.systems/" && /\b\d+m ago\b/.test(c.text)), "the bundled homepage still has its ticker, so this test means something");
  assert.ok(new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT }).search("Chainlink ETH/USD Arbitrum differs CoinMarketCap", "").every((match) => !/\b\d+m ago\b/.test(match.text)));
});

test("a question about current risk in general gets the model's answer, with Radar's live alerts as one compact card and no website cards", async () => {
  let calls = 0;
  const service = new AssistantService({
    llm: {
      complete: async () => {
        calls += 1;
        return calls === 1
          ? { content: null, toolCalls: [{ id: "l1", type: "function", function: { name: "lookup_sagitta", arguments: JSON.stringify({ query: "Radar" }) } }] }
          : { content: "I can help you look into warning signs for Ethereum.", toolCalls: [] };
      },
    },
    gateway: { requestChallenge: async () => { throw new Error("no"); }, submitPaid: async () => { throw new Error("no"); } },
    loadCatalog: async () => ({ resources: [] }),
    sites: new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT }),
  });
  const reply = await service.chat({ message: "Are there any warning signs on Ethereum right now?" });
  assert.equal(reply.text, "I can help you look into warning signs for Ethereum.");
  assert.ok(calls >= 2);
  assert.equal(reply.blocks[0].type, "live_feed");
  const feed = reply.blocks[0] as { title: string; href: string; linkLabel: string };
  assert.deepEqual([feed.title, feed.href, feed.linkLabel], ["Live infrastructure alerts", RADAR_ALERTS_URL, "View Radar alerts"]);
  assert.ok(reply.blocks.every((block) => block.type !== "sagitta_system" && block.type !== "sagitta_sources"), reply.blocks.map((b) => b.type).join(", "));
  assert.equal(reply.blocks.filter((block) => block.type === "live_feed").length, 1);
});

test("a question about current risk can't pick up services it didn't ask for", async () => {
  const seen: string[] = [];
  let calls = 0;
  const service = new AssistantService({
    llm: {
      complete: async (request: { messages: { role: string; content: string | null }[] }) => {
        calls += 1;
        if (calls === 1) return { content: null, toolCalls: [{ id: "p1", type: "function", function: { name: "propose_sce_continuity_mode", arguments: "{}" } }] };
        seen.push(String(request.messages.at(-1)?.content ?? ""));
        return { content: "I can help you look into warning signs for Ethereum.", toolCalls: [] };
      },
    },
    gateway: { requestChallenge: async () => { throw new Error("no"); }, submitPaid: async () => { throw new Error("no"); } },
    loadCatalog: async () => ({ resources: [] }),
  });
  const reply = await service.chat({ message: "Are there any warning signs on Ethereum right now?" });
  assert.ok(reply.blocks.every((block) => block.type !== "action"), reply.blocks.map((b) => b.type).join(", "));
  assert.match(seen[0], /not_proposed/);
});
