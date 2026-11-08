import { findAssetMentions } from "./asset-resolver";
import { namesService, requestedServices, type CapabilityId } from "./capability-registry";

/**
 * What a customer's message is for, decided before any question limit applies.
 *
 * - service_discovery: what Selun offers, what it costs, or a request one of its services answers.
 * - transaction: paying, a quote, a purchase, a refund, a receipt, a subscription.
 * - result_followup: a question about an analysis this conversation already holds.
 * - general: anything else, such as "what is staking?".
 *
 * Discovery and transactions are always served, with or without questions left, so nobody is ever
 * kept from finding or buying a service. Whether a turn uses a question is settled by what the reply
 * did: one that offered a service is always free.
 */
export type Intent = "service_discovery" | "transaction" | "result_followup" | "general";

export type IntentContext = {
  /** A quote in this conversation is waiting for approval. */
  hasQuote: boolean;
  /** This conversation holds a completed analysis. */
  hasResult: boolean;
};

export type Classified = {
  intent: Intent;
  /** Services the message asks for, from the capability registry. */
  services: CapabilityId[];
  /** Asking about the catalog or a price, or naming a service, which is free however it's answered. */
  catalog: boolean;
  /** Asking what Selun offers or what it costs, which the service cards answer on their own. */
  listing: boolean;
};

/** About Selun's offering itself, rather than the market. */
const CATALOG = /\b(what (can|do|does) (you|selun) (do|offer|sell|provide|analy[sz]e)|what (services|analyses|reports|products)|your (services|analyses|reports|products|prices|pricing)|selun'?s (services|analyses|reports|prices|pricing)|(list|show)( me)? (the |your )?(services|options|analyses)|services? (do|does|can) you|price list|pricing|how much (is|are|does|do|would|will)|(price|cost) of (an?|the|your|each)\b|(what|which) (does|do) (it|they|each|the \w+) (cost|include)|what('s| is) included|how does selun work|what is selun|who is selun)/i;

/** Paying, quotes, purchases, and support, in this conversation or any other. */
const TRANSACTION = /\b(pay|paying|paid|payment|payments|checkout|check out|paypal|stripe|credit card|debit card|by card|with (a |my )?card|usdc|(my|connect|connected|connect a) wallet|with (a |my )?wallet|buy (it|this|that|one|the (report|analysis|scorecard|check|plan|subscription))|purchase (it|this|that|one)|refund\w*|receipt|invoice|(was|been|got|get) charged|charge me|cancel\w*|unsubscribe|renew\w*|my (purchase|order|payment|subscription|report|analysis|receipt)|download|pdf|(email|send|resend) (me|it|the)|the quote|this quote|new quote|quote (expired|changed)|payment method)\b/i;

/** Agreeing to, or adjusting, a quote that's waiting. */
const QUOTE_REPLY = /^(yes|yeah|yep|ok|okay|sure|go ahead|do it|proceed|confirm|approve|sounds good|let'?s do it|please do)\b|\b(instead|change|switch|swap|add|remove|make it|use)\b[^.?!]*\b(assets?|risk|profile|conservative|balanced|growth|aggressive|horizon|timeframe|segment|bluechips|memecoins|gaming|yield)\b|\b(conservative|balanced|growth|aggressive)\b/i;

/** About a result: its figures, or what they mean. */
const FOLLOWUP = /\b(my|this|that|the|these) (report|result|results|analysis|score|scores|scorecard|check|figures|numbers|metrics)\b|\bwhat does (this|that|it) mean\b|\bwhy (is|was|did|does) (it|my|the|this)\b|\b(volatility|drawdown|price risk|market standing|overall score|regime)\b/i;

/** Ongoing reports: "monthly report", "subscription", "sign me up". Not managing one that exists. */
const SUBSCRIBE = /\b(monthly|every month|recurring|ongoing)\b[^.?!]*\breports?\b|\bsubscri(be|ption)\b|\bsign me up\b/i;
const MANAGING = /\b(cancel\w*|unsubscribe|refund\w*|my subscription|renew\w*|stop)\b/i;
/** The full report, however it's asked for: "comprehensive report", "a report on Bitcoin", "full analysis". */
const FULL_REPORT = /\b(full|complete|comprehensive|detailed|intelligence) (report|analysis)\b|\breports? (on|for|about|covering)\b/i;
/** Asked for a different product by name, so "report" isn't the full report. */
const OTHER_PRODUCT = /\b(scorecard|risk analysis|safety check|regime|rebalanc\w*|portfolio plan|policy envelope)\b/i;

export type PurchaseRoute = { capabilityId: "comprehensive_report" | "monthly_intelligence"; needsAssets: boolean };

/**
 * A request that can only mean one product, decided from the registry's own products rather than left to the model.
 * The report and the subscription are the ones a model most often swaps for a cheaper tool or a catalog card.
 * `needsAssets` when nothing in the conversation names an asset yet: both products need one.
 */
export function explicitPurchase(message: string, earlierCustomerText: string): PurchaseRoute | null {
  // "How much is the report?" wants a price, not a quote yet.
  if (MANAGING.test(message) || CATALOG.test(message)) return null;
  const capabilityId = SUBSCRIBE.test(message) ? "monthly_intelligence" : FULL_REPORT.test(message) && !OTHER_PRODUCT.test(message) ? "comprehensive_report" : null;
  if (!capabilityId) return null;
  const needsAssets = findAssetMentions(message).length === 0 && findAssetMentions(earlierCustomerText).length === 0;
  return { capabilityId, needsAssets };
}

export function classifyIntent(message: string, context: IntentContext): Classified {
  const services = requestedServices(message);
  const listing = CATALOG.test(message);
  const catalog = listing || namesService(message);
  const of = (intent: Intent): Classified => ({ intent, services, catalog, listing: listing && intent === "service_discovery" });
  if (TRANSACTION.test(message) || (context.hasQuote && QUOTE_REPLY.test(message))) return of("transaction");
  if (catalog) return of("service_discovery");
  // A result's own figures come before the services that produce them: "why is the drawdown so large?".
  if (context.hasResult && FOLLOWUP.test(message)) return of("result_followup");
  // A named asset is what the Asset Scorecard analyzes.
  if (services.length > 0 || findAssetMentions(message).length > 0) return of("service_discovery");
  return of("general");
}
