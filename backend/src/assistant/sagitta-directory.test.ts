import assert from "node:assert/strict";
import test from "node:test";
import { AssistantService } from "./assistant.service";
import { SAGITTA_CAPABILITIES, SAGITTA_SYSTEMS } from "./sagitta-directory.data";
import { lookupSagitta } from "./sagitta-directory";

test("each system is found by the names people use", () => {
  const cases: [string, string][] = [
    ["Radar", "sagitta-radar"],
    ["sagitta radar", "sagitta-radar"],
    ["SCE", "sagitta-continuity-engine"],
    ["the continuity engine?", "sagitta-continuity-engine"],
    ["AAA", "aaa"],
    ["Autonomous Allocation Agent", "aaa"],
    ["Selun", "selun"],
    ["defence", "sagitta-defense"],
    ["Banking", "sagitta-banking"],
    ["protocol", "sagitta-protocol"],
    ["wallet", "sagitta-wallet"],
    ["x402", "selun-x402"],
    ["Rebalancing", "rebalancing"],
  ];
  for (const [query, slug] of cases) {
    const lookup = lookupSagitta(query);
    assert.ok(lookup.kind === "system" || lookup.kind === "capability", query);
    assert.equal(lookup.system.slug, slug, query);
  }
});

test("a broad or empty question returns every system", () => {
  for (const query of ["", "Sagitta", "sagitta systems", undefined, null]) {
    const lookup = lookupSagitta(query);
    assert.equal(lookup.kind, "directory");
    assert.equal(lookup.systems.length, SAGITTA_SYSTEMS.length);
  }
});

test("nothing is guessed for an unknown name", () => {
  for (const query of ["Sagitta Mining", "Treasury Coin", "radar2", "investors", "revenue"]) {
    assert.equal(lookupSagitta(query).kind, "not_found", query);
  }
});

test("a system's real status comes through, including ones that aren't live", () => {
  const status = (query: string) => {
    const lookup = lookupSagitta(query);
    return lookup.kind === "system" ? lookup.system.status : null;
  };
  assert.equal(status("Radar"), "Operating");
  assert.equal(status("Banking"), "In Development");
  assert.equal(status("Protocol"), "Public Test");
  assert.equal(status("Wallet"), "Research Horizon");
});

test("the snapshot carries only public content", () => {
  const serialized = JSON.stringify({ SAGITTA_SYSTEMS, SAGITTA_CAPABILITIES });
  for (const forbidden of ["owner decision", "owner confirmation", "Projects/", "statusEvidence", "publicationState", "verification", "founder"]) {
    assert.ok(!serialized.includes(forbidden), forbidden);
  }
  for (const record of [...SAGITTA_SYSTEMS, ...SAGITTA_CAPABILITIES]) {
    assert.ok(record.url === null || record.url.startsWith("https://"), record.slug);
  }
  assert.ok(!serialized.includes("sagitta-grants"), "archived capabilities stay out");
});

function serviceScripted(responses: { content: string | null; toolCalls: { id: string; type: "function"; function: { name: string; arguments: string } }[] }[]) {
  const refuse = async () => {
    throw new Error("Nothing paid may run here.");
  };
  return new AssistantService({
    llm: { complete: async () => responses.shift() ?? { content: "Done.", toolCalls: [] } },
    gateway: { requestChallenge: refuse, submitPaid: refuse },
    loadCatalog: async () => ({ resources: [] }),
  });
}

const call = (name: string, args: unknown) => ({ id: "1", type: "function" as const, function: { name, arguments: JSON.stringify(args) } });

test("looking up a system shows a card, costs nothing, and quotes nothing", async () => {
  const service = serviceScripted([{ content: null, toolCalls: [call("lookup_sagitta", { query: "Radar" })] }, { content: "Radar is operating. The card has the details.", toolCalls: [] }]);
  const reply = await service.chat({ message: "What is Sagitta Radar?" });
  assert.deepEqual(reply.blocks.map((block) => block.type), ["sagitta_system"]);
  const block = reply.blocks[0];
  assert.ok(block.type === "sagitta_system");
  assert.equal(block.system.url, "https://radar.sagitta.systems");
  assert.equal(block.system.status, "Operating");
  assert.ok(!reply.blocks.some((entry) => entry.type === "action"));
});

test("a repeated lookup shows one card, and an unknown one shows none", async () => {
  const repeat = serviceScripted([{ content: null, toolCalls: [call("lookup_sagitta", { query: "Radar" }), call("lookup_sagitta", { query: "radar" })] }, { content: "Here it is.", toolCalls: [] }]);
  assert.equal((await repeat.chat({ message: "Radar?" })).blocks.length, 1);

  const unknown = serviceScripted([{ content: null, toolCalls: [call("lookup_sagitta", { query: "Sagitta Mining" })] }, { content: "I don't have public information on that.", toolCalls: [] }]);
  assert.equal((await unknown.chat({ message: "What is Sagitta Mining?" })).blocks.length, 0);
});

test("a dollar figure not in the record is held back", async () => {
  const service = serviceScripted([
    { content: null, toolCalls: [call("lookup_sagitta", { query: "Radar" })] },
    { content: "Radar costs $49 a month.", toolCalls: [] },
    { content: "Radar costs $49 a month.", toolCalls: [] },
  ]);
  const reply = await service.chat({ message: "How much is Radar?" });
  assert.ok(!reply.text.includes("$49"), reply.text);
});
