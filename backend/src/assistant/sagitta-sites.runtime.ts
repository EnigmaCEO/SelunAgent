import path from "node:path";
import { SAGITTA_SITES_SNAPSHOT } from "./sagitta-sites.data";
import { SiteKnowledge, type SiteCorpus } from "./sagitta-sites";

export const SITES_REFRESH_CHECK_MS = 60 * 60_000;

let knowledge: SiteKnowledge | null = null;

function storedFile(): string {
  const configured = process.env.SELUN_SITES_FILE?.trim();
  if (configured) return path.resolve(configured);
  return path.resolve(process.cwd(), process.cwd().endsWith("backend") ? "data" : "backend/data", "sagitta-sites.json");
}

/** Starts from the snapshot shipped with the build, and from a newer read if one is on disk. */
export function getSiteKnowledge(): SiteKnowledge {
  return (knowledge ??= new SiteKnowledge({ bundled: SAGITTA_SITES_SNAPSHOT as SiteCorpus, file: storedFile() }));
}
