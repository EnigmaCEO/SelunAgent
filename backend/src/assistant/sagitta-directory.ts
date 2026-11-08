import { SAGITTA_CAPABILITIES, SAGITTA_SYSTEMS } from "./sagitta-directory.data";

/**
 * What Selun can say about Sagitta's systems. Information only: nothing here runs a service
 * or takes payment. The data is a snapshot of the owner-verified public record behind
 * sagitta.systems, so product claims and statuses aren't written by Selun or by the model.
 */

export type SystemSummary = {
  slug: string;
  name: string;
  status: string;
  summary: string;
  url: string | null;
  /** When Sagitta's public record reported the status. It is not a live health check. */
  statusAsOf: string | null;
};

export type SystemDetail = SystemSummary & {
  parent: string | null;
  availableToday: string[];
  lastVerifiedAt: string | null;
};

export type DirectoryLookup =
  | { kind: "directory"; systems: SystemSummary[] }
  | { kind: "system"; system: SystemDetail; record: Record<string, unknown> }
  | { kind: "capability"; system: SystemDetail; record: Record<string, unknown> }
  | { kind: "not_found"; known: string[] };

/** Other ways people name these systems. Exact phrases only, so nothing is guessed. */
const ALIASES: Record<string, string[]> = {
  "sagitta-continuity-engine": ["sce", "continuity engine", "the continuity engine", "continuity"],
  "sagitta-defense": ["defense", "defence", "sagitta defence", "defense review", "defense reviews"],
  "sagitta-radar": ["radar", "radar by sagitta"],
  aaa: ["sagitta aaa", "allocation agent", "autonomous allocation agent", "the allocation agent"],
  selun: ["selun ai", "this assistant", "you"],
  "sagitta-banking": ["banking", "sagitta bank"],
  "sagitta-protocol": ["protocol", "the protocol", "sag protocol", "sagprotocol"],
  "sagitta-wallet": ["wallet", "the wallet"],
  "selun-x402": ["x402", "x402 api", "the x402 api"],
  rebalancing: ["rebalance service", "sagitta rebalancing"],
};

const DIRECTORY_QUERIES = new Set(["", "sagitta", "sagitta systems", "all", "everything", "overview", "products", "systems", "the company"]);

function normalize(value: string): string {
  return value.trim().toLowerCase().replace(/[?.!,]+$/g, "").replace(/\s+/g, " ");
}

function matches(query: string, slug: string, names: string[]): boolean {
  return [slug, ...names, ...(ALIASES[slug] ?? [])].some((name) => normalize(name) === query);
}

function summaryOf(record: (typeof SAGITTA_SYSTEMS)[number]): SystemSummary {
  return { slug: record.slug, name: record.name, status: record.status, summary: record.summary, url: record.url, statusAsOf: record.lastVerifiedAt ?? null };
}

export function lookupSagitta(rawQuery: unknown): DirectoryLookup {
  const query = normalize(typeof rawQuery === "string" ? rawQuery : "");
  if (DIRECTORY_QUERIES.has(query)) return { kind: "directory", systems: SAGITTA_SYSTEMS.map(summaryOf) };

  const system = SAGITTA_SYSTEMS.find((record) => matches(query, record.slug, [record.name, record.shortName]));
  if (system) {
    return {
      kind: "system",
      system: { ...summaryOf(system), parent: system.parent, availableToday: [...system.availableToday], lastVerifiedAt: system.lastVerifiedAt },
      record: system as unknown as Record<string, unknown>,
    };
  }

  const capability = SAGITTA_CAPABILITIES.find((record) => matches(query, record.slug, [record.name]));
  if (capability) {
    return {
      kind: "capability",
      system: {
        slug: capability.slug,
        name: capability.name,
        status: `Delivered through ${capability.deliveredBy.join(", ")}`,
        summary: capability.summary,
        url: capability.url,
        statusAsOf: capability.lastVerifiedAt ?? null,
        parent: null,
        availableToday: capability.accessPath ? [capability.accessPath] : [],
        lastVerifiedAt: capability.lastVerifiedAt,
      },
      record: capability as unknown as Record<string, unknown>,
    };
  }

  return { kind: "not_found", known: [...SAGITTA_SYSTEMS.map((record) => record.name), ...SAGITTA_CAPABILITIES.map((record) => record.name)] };
}
