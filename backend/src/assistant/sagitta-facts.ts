import { SAGITTA_SYSTEMS } from "./sagitta-directory.data";
import { EXCLUDED_NAMES, OFFICIAL_HEADING, WHITEPAPER_URL, type SiteCorpus } from "./sagitta-sites";

/**
 * A short sheet of the basics, read from fixed pages on Sagitta's sites and kept in Selun's
 * instructions, so common questions need no search. Each section knows its page, so an answer
 * can still cite it. If a fact can't be read, the whole sheet is refused rather than shipped
 * with a gap, and the last good one stays.
 */
export type FactSection = { title: string; url: string; site: string; evidence: string[] };
export type FactSheet = { text: string; sections: FactSection[] };

const WWW = "https://www.sagitta.systems";
const STATUS_PAGE = `${WWW}/status`;
const ABOUT_PAGE = `${WWW}/about`;
const ROADMAP_PAGE = `${WWW}/roadmap`;
const RADAR_PRICING = "https://radar.sagitta.systems/pricing";
const AAA_PRICING = "https://aaa.sagitta.systems/pricing";
const SELUN_PRICING = "https://selun.sagitta.systems/pricing";
const DEFENSE_PAGE = "https://defense.sagitta.systems/";

const STATUSES = ["Operating", "Public Test", "In Development", "Research Horizon"];
const DEFENSE_AREAS = ["Authority Paths", "Treasury Controls", "Oracle & Price Risk", "Governance / Timelocks", "Keeper & Liveness", "Emergency Procedures"];
const RADAR_PLANS = ["Watch", "Intel", "Signal", "Desk"];
const AAA_TIERS = ["Observer Access", "Sandbox Authority", "Production Authority", "Doctrine Authority"];
/** A price that can't be read for one of these means the page changed shape, so the sheet is unsafe. */
const SELUN_REQUIRED = ["Allocation", "Allocation with Report", "Comprehensive Intelligence Report"];

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const pricesIn = (text: string) => [...text.matchAll(/\$[\d,]+(?:\.\d+)?/g)].map((match) => match[0].replace(/\.00$/, ""));

/** Whether a reply repeats something from this section, such as a price, address, or status. */
export function citedSections(sheet: FactSheet, reply: string): FactSection[] {
  return sheet.sections.filter((section) => section.evidence.some((item) => new RegExp(`${escapeRegExp(item)}(?![\\d,])`, "i").test(reply)));
}

export function buildFactSheet(corpus: SiteCorpus): FactSheet | null {
  const chunks = (url: string) => corpus.chunks.filter((chunk) => chunk.url === url);
  const pageText = (url: string) => chunks(url).map((chunk) => chunk.text).join(" ");
  const section = (title: string, url: string, evidence: string[]): FactSection => ({ title, url, site: new URL(url).hostname, evidence });

  // The company.
  const about = pageText(ABOUT_PAGE);
  const identity = /Sagitta Systems is the development network[^.]*\./.exec(about)?.[0];
  const families = /Continuity and Defense keeps[^.]*\.\s*Allocation and Agent Intelligence[^.]*\.\s*Capital Infrastructure[^.]*\./.exec(about)?.[0];
  const thesis = /authority precedes automation[^.]*\./.exec(about)?.[0];
  const foundations = /Three core foundations[^.]*\./.exec(about)?.[0];
  if (!identity || !families || !thesis || !foundations || !about.includes("Xavier D. Moore")) return null;

  // Each system's status and the evidence for it, from the status page.
  const statusText = pageText(STATUS_PAGE);
  const systems: string[] = [];
  for (const record of SAGITTA_SYSTEMS) {
    const shown = new RegExp(`${escapeRegExp(record.name)}\\s+\\S*sagitta\\.systems\\s+(.*?)\\s*Last verified \\d+ \\w+ \\d{4}\\s+(${STATUSES.join("|")})`).exec(statusText);
    if (!shown) return null;
    systems.push(`- ${record.name} (${shown[2]}, ${record.url}${record.parent ? `, part of ${record.parent}` : ""}): ${record.summary} ${shown[1]}`);
  }

  // Published prices.
  const radarCards = chunks(RADAR_PRICING);
  const radar: string[] = [];
  for (const plan of RADAR_PLANS) {
    const card = radarCards.find((chunk) => chunk.heading === plan);
    const price = card && /^((?:From )?\$[\d,]+) ?\/mo\s+([^.]*\.)/.exec(card.text);
    if (!price) return null;
    radar.push(`${plan} ${price[1]}/month (${price[2]})`);
  }
  const aaaText = pageText(AAA_PRICING);
  const aaa: string[] = [];
  for (const tier of AAA_TIERS) {
    const price = new RegExp(`${tier}\\s+(?:Enterprise\\s+)?(Free|\\$[\\d,]+ per month|Custom pricing)`).exec(aaaText);
    if (!price) return null;
    aaa.push(`${tier}: ${price[1]}`);
  }
  const defensePrice = /Starter Defense Review \$[\d,]+/.exec(pageText(DEFENSE_PAGE))?.[0];
  const defenseTerms = /A Starter Defense Review is purchasable[^.]*\./.exec(pageText(ROADMAP_PAGE))?.[0];
  if (!defensePrice || !defenseTerms) return null;
  const defenseAreas = DEFENSE_AREAS.filter((area) => chunks(DEFENSE_PAGE).some((chunk) => chunk.heading === area));

  const selunCards = chunks(SELUN_PRICING).flatMap((chunk) => {
    const price = /^\$[\d.,]+ USDC?(?: ?\/ ?\w+)?/.exec(chunk.text);
    return price && chunk.heading ? [`${chunk.heading}: ${price[0]}`] : [];
  });
  if (SELUN_REQUIRED.some((name) => !selunCards.some((line) => line.startsWith(`${name}:`)))) return null;
  const selunPayment = /x402 endpoint calls are paid in USDC on Base[^.]*\./.exec(pageText(SELUN_PRICING))?.[0];
  const selunCard = /The allocation wizard accepts card payment[^.]*\./.exec(pageText(SELUN_PRICING))?.[0];
  if (!selunPayment || !selunCard) return null;

  // Roles being filled now, if the careers page lists them this way. Their absence isn't a reason to refuse.
  const careers = chunks(`${WWW}/careers`);
  const openFrom = careers.findIndex((chunk) => chunk.heading === "Open Now");
  const openRoles: string[] = [];
  for (const chunk of openFrom >= 0 ? careers.slice(openFrom + 1) : []) {
    if (!/Compensation/.test(chunk.text) || /contributor|future|closed/i.test(chunk.heading)) break;
    if (!openRoles.includes(chunk.heading)) openRoles.push(chunk.heading);
  }

  // Official accounts and addresses, one list per site.
  const linkChunks = corpus.chunks.filter((chunk) => chunk.heading === OFFICIAL_HEADING);
  const main = linkChunks.find((chunk) => chunk.site === "www.sagitta.systems");
  if (!main || !/x\.com\//i.test(main.text)) return null;
  const addresses = linkChunks.flatMap((chunk) => [...chunk.text.matchAll(/https:\/\/\S+?(?=\.(?:\s|$))|[\w.+-]+@sagitta\.systems/g)].map((match) => match[0]));

  const text = [
    `Company (${ABOUT_PAGE}): ${identity} The founder is Xavier D. Moore. Its thesis is that ${thesis} ${foundations} Its three system families: ${families}`,
    `Systems (${STATUS_PAGE}), with status and evidence:\n${systems.join("\n")}`,
    `Radar plans (${RADAR_PRICING}): ${radar.join("; ")}`,
    `AAA tiers (${AAA_PRICING}): ${aaa.join("; ")}`,
    `Defense (${DEFENSE_PAGE}): ${defensePrice}, flat fee. ${defenseTerms}${defenseAreas.length ? ` It reviews: ${defenseAreas.join("; ")}.` : ""}`,
    `Selun prices (${SELUN_PRICING}): ${selunCards.join("; ")}. ${selunPayment} ${selunCard}`,
    ...(openRoles.length ? [`Careers (${WWW}/careers): open now: ${openRoles.join("; ")}.`] : []),
    `Whitepaper: ${WHITEPAPER_URL}. It specifies how the Sagitta Protocol is designed and what it legally is. These facts don't cover that, so search it.`,
    `Official links and contact, by site:\n${linkChunks.map((chunk) => `- ${chunk.site}: ${chunk.text}`).join("\n")}`,
  ].join("\n\n");
  if (EXCLUDED_NAMES.test(text)) return null;

  return {
    text,
    sections: [
      section("About — Sagitta Systems", ABOUT_PAGE, ["Xavier D. Moore", "development network", "authority precedes automation", "Capital Infrastructure"]),
      section("Status — Sagitta Systems", STATUS_PAGE, ["Operating", "Public Test", "In Development", "Research Horizon"]),
      section("Pricing | Radar", RADAR_PRICING, pricesIn(radar.join(" "))),
      section("Pricing & Access Tiers — Sagitta AAA", AAA_PRICING, pricesIn(aaa.join(" "))),
      section("Sagitta Defense Review", DEFENSE_PAGE, pricesIn(defensePrice)),
      section("Pricing | Selun", SELUN_PRICING, pricesIn(selunCards.join(" "))),
      ...(openRoles.length ? [section("Careers — Sagitta Systems", `${WWW}/careers`, openRoles.map((role) => role.split(" — ")[0]))] : []),
      section("Sagitta Protocol Whitepaper", WHITEPAPER_URL, [WHITEPAPER_URL.replace("https://", "")]),
      section(OFFICIAL_HEADING, main.url, addresses),
    ],
  };
}
