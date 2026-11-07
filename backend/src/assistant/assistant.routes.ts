import { AiCredits, readCreditConfig } from "./ai-credits";
import type { PaymentRow } from "./paypal-commerce.service";
import path from "node:path";
import { resolveAsset } from "./asset-resolver";
import { sandboxFaultFile, withSandboxFaults } from "./sandbox-faults";
import { sendCoverageNoticeEmail } from "../services/email.service";
import { createHash } from "node:crypto";
import { Router, type NextFunction, type Request, type RequestHandler, type Response } from "express";
import { getChatCapabilitiesData, getPaidToolResponse, getX402CapabilitiesData, listWalletPaymentRows, requireAdminRequest, runPaidCapability } from "../routes/agent.routes";
import { issuePurchaseToken, issueReportToken, readPurchaseToken } from "../services/job-access.service";
import { legacyClientIp } from "../services/legacy-cooldown";
import { AssistantError, AssistantService, DEFAULT_LIMITS, STRIPE_META, hostedProvider, type AssistantDeps, type AssistantLimits } from "./assistant.service";
import { COMPREHENSIVE_REPORT_ID, getPaymentRails } from "./commerce";
import { OpenAiClient, readOpenAiConfig } from "./openai-client";
import { PayPalClient, readPayPalConfig } from "./paypal-client";
import { readStripeConfig, StripeClient } from "./stripe-client";
import { PayPalCommerceService, type CoverageNotice, type Purchase, type PurchaseView, type SubscriptionView } from "./paypal-commerce.service";
import type { ScreenProfile } from "./scorecard-preflight";
import { ReportService, type ReportDeps } from "./report.service";
import { SITES_REFRESH_CHECK_MS, getSiteKnowledge } from "./sagitta-sites.runtime";
import { DEFAULT_SCREEN_PROFILE, SCREEN_WARM_MS, getScorecardPreflight } from "./scorecard-preflight.runtime";
import { HttpX402Gateway } from "./x402-gateway";

const CONVERSATION_HEADER = "x-selun-conversation";

/** Sliding-window limiter, per client. Protects model spend on a public endpoint. */
export class WindowRateLimiter {
  private readonly hits = new Map<string, number[]>();
  private nextSweep = 0;

  constructor(
    private readonly limit: number,
    private readonly windowMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  /** Returns 0 when allowed, otherwise the seconds to wait. */
  take(key: string): number {
    const now = this.now();
    if (now >= this.nextSweep) {
      for (const [entry, times] of this.hits) {
        if (!times.some((time) => time > now - this.windowMs)) this.hits.delete(entry);
      }
      this.nextSweep = now + this.windowMs;
    }
    const recent = (this.hits.get(key) ?? []).filter((time) => time > now - this.windowMs);
    if (recent.length >= this.limit) {
      this.hits.set(key, recent);
      return Math.max(1, Math.ceil((recent[0] + this.windowMs - now) / 1_000));
    }
    recent.push(now);
    this.hits.set(key, recent);
    return 0;
  }
}

function readPositiveInt(name: string, fallback: number): number {
  const parsed = Number.parseInt(process.env[name] ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function readPositiveAmount(name: string, fallback: string): string {
  const raw = process.env[name]?.trim();
  return raw && /^\d+(\.\d{1,6})?$/.test(raw) && Number(raw) > 0 ? raw : fallback;
}

export function readAssistantLimits(): Partial<AssistantLimits> {
  return {
    maxActionUsdc: readPositiveAmount("ASSISTANT_MAX_ACTION_USDC", DEFAULT_LIMITS.maxActionUsdc),
    sessionLimitUsdc: readPositiveAmount("ASSISTANT_SESSION_SPEND_LIMIT_USDC", DEFAULT_LIMITS.sessionLimitUsdc),
    walletDailyLimitUsdc: readPositiveAmount("ASSISTANT_WALLET_DAILY_LIMIT_USDC", DEFAULT_LIMITS.walletDailyLimitUsdc),
    dailyTurnLimit: readPositiveInt("ASSISTANT_DAILY_TURN_LIMIT", DEFAULT_LIMITS.dailyTurnLimit),
    maxTurnsPerSession: readPositiveInt("ASSISTANT_MAX_TURNS_PER_CONVERSATION", DEFAULT_LIMITS.maxTurnsPerSession),
  };
}

const PAYPAL_SWEEP_MS = 30 * 60_000;

/** Production wiring: read at first use because server.ts loads dotenv after imports. */
function positiveMs(value: string | undefined): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function createDefaultDeps(paypal: PayPalCommerceService | null, stripe: PayPalCommerceService | null, prices: { reportUsd: string; subscriptionUsd: string } | null): AssistantDeps {
  const openAi = readOpenAiConfig();
  const port = process.env.PORT ?? "8787";
  const internalBaseUrl = (process.env.ASSISTANT_INTERNAL_BASE_URL?.trim() || `http://127.0.0.1:${port}`).replace(/\/+$/, "");
  return {
    llm: openAi ? new OpenAiClient(openAi) : null,
    gateway: new HttpX402Gateway(internalBaseUrl),
    loadCatalog: async () => {
      // The chat's prices, which include its markup on per-use services.
      const data = await getChatCapabilitiesData();
      return { resources: data.resources };
    },
    limits: readAssistantLimits(),
    sites: getSiteKnowledge(),
    preflight: getScorecardPreflight(),
    hostedPrices: prices,
    credits: process.env.SELUN_AI_CREDITS?.trim() === "off" ? null : new AiCredits(readCreditConfig(), process.env.SELUN_AI_CREDITS_FILE?.trim() || path.resolve(process.cwd(), process.cwd().endsWith("backend") ? "data" : "backend/data", "ai-credits.json")),
    checkoutProviders: stripe ? [hostedProvider(STRIPE_META, stripe)] : [],
    paypal: paypal && prices
      ? {
        prices,
        createCheckout: (input) => paypal.createCheckout(input),
        createSubscription: (input) => paypal.createSubscription(input),
      }
      : null,
  };
}

function sendError(res: Response, error: unknown) {
  res.set("Cache-Control", "no-store");
  if (error instanceof AssistantError) {
    const retryAfter = error.extra.retryAfterSeconds;
    if (typeof retryAfter === "number") res.set("Retry-After", String(retryAfter));
    return res.status(error.status).json({ success: false, code: error.code, error: error.message, ...error.extra });
  }
  console.error("Assistant route error:", error instanceof Error ? error.message : error);
  return res.status(500).json({ success: false, code: "internal_error", error: "Something went wrong. Please try again." });
}

const GUEST_HEADER = "x-selun-guest";

function conversationId(req: Request): string | undefined {
  return req.get(CONVERSATION_HEADER)?.trim() || undefined;
}

function clientKey(req: Request): string {
  return createHash("sha256").update(legacyClientIp(req)).digest("hex").slice(0, 24);
}

function limited(limiter: WindowRateLimiter): RequestHandler {
  return (req: Request, res: Response, next: NextFunction) => {
    const retryAfterSeconds = limiter.take(clientKey(req));
    if (!retryAfterSeconds) return next();
    res.set("Retry-After", String(retryAfterSeconds));
    res.set("Cache-Control", "no-store");
    res.status(429).json({
      success: false,
      code: "rate_limited",
      error: "You're sending requests quickly. Please wait a moment and try again.",
      retryAfterSeconds,
    });
  };
}

/** Totals by provider and currency. Payments with no reported fee are counted, not guessed. */
export function summarizePayments(rows: PaymentRow[]) {
  const totals = new Map<string, { provider: string; currency: string; payments: number; gross: number; fees: number; netKept: number; missingFee: number }>();
  for (const row of rows) {
    const key = `${row.provider}:${row.currency}`;
    const total = totals.get(key) ?? { provider: row.provider, currency: row.currency, payments: 0, gross: 0, fees: 0, netKept: 0, missingFee: 0 };
    total.payments += 1;
    total.gross += Number(row.amount);
    if (row.fee === null || row.netKept === null) total.missingFee += 1;
    else {
      total.fees += Number(row.fee);
      total.netKept += Number(row.netKept);
    }
    totals.set(key, total);
  }
  return [...totals.values()].map((total) => ({ ...total, gross: total.gross.toFixed(2), fees: total.fees.toFixed(2), netKept: total.netKept.toFixed(2) }));
}

export function createAssistantRouter(injected?: AssistantDeps, injectedReports?: ReportDeps, injectedPayPal?: PayPalCommerceService) {
  const router = Router();
  let reports: ReportService | null = null;
  let paypal: PayPalCommerceService | null | undefined = injectedPayPal;
  let stripe: PayPalCommerceService | null | undefined;
  let service: AssistantService | null = null;

  const getReports = () => (reports ??= new ReportService(injectedReports ?? {
    loadPaidResponse: (capabilityId, decisionId) => ownerOf(decisionId)?.loadPaidResponse(capabilityId, decisionId) ?? getPaidToolResponse(capabilityId, decisionId),
    assertAccess: async (capabilityId, decisionId) => {
      await ownerOf(decisionId)?.assertReportAccess(capabilityId, decisionId);
    },
  }));

  function hostedServices(): PayPalCommerceService[] {
    return [getPayPal(), getStripe()].filter((entry): entry is PayPalCommerceService => entry !== null);
  }

  function ownerOf(decisionId: string): PayPalCommerceService | undefined {
    return hostedServices().find((entry) => entry.ownsDecision(decisionId));
  }

  function hostedDeps(env: "sandbox" | "live") {
    return {
      runReport: withSandboxFaults(env, sandboxFaultFile(), runPaidCapability),
      retryWindowMs: positiveMs(process.env.PAYPAL_RETRY_WINDOW_MS),
      retryDelayMs: positiveMs(process.env.PAYPAL_RETRY_DELAY_MS),
      notifyCoverage: async (email: string, notice: CoverageNotice) => {
        const sent = await sendCoverageNoticeEmail({ toEmail: email, ...notice });
        if (sent.status === "failed") throw new Error(sent.error ?? "The notice couldn't be sent.");
      },
      checkAssets: (assets: string[], profile: ScreenProfile) => getScorecardPreflight().check(assets, profile),
      deliverReport: async (reportToken: string, email: string) => {
        await getReports().email(reportToken, { email, approved: true });
      },
    };
  }

  function getStripe(): PayPalCommerceService | null {
    if (stripe !== undefined) return stripe;
    if (injected || injectedPayPal) return (stripe = null);
    const config = readStripeConfig();
    if (!config) return (stripe = null);
    const service: PayPalCommerceService = new PayPalCommerceService({
      ...hostedDeps(config.env),
      api: new StripeClient(config),
      config,
      provider: { id: STRIPE_META.id, label: STRIPE_META.label },
      checkUnpaidOrders: true,
      stateFile: process.env.SELUN_STRIPE_STATE_FILE?.trim() || path.resolve(process.cwd(), process.cwd().endsWith("backend") ? "data" : "backend/data", "stripe-commerce.json"),
      onPurchaseUpdate: (purchase) => onPurchaseUpdate(service, purchase),
    });
    return (stripe = service);
  }

  function requireProvider(id: unknown): PayPalCommerceService {
    const current = id === "paypal" ? getPayPal() : id === "stripe" ? getStripe() : null;
    if (!current) throw new AssistantError("service_unavailable", 503, "That payment method isn't available right now.");
    return current;
  }

  function onPurchaseUpdate(current: PayPalCommerceService, purchase: Purchase) {
    if (purchase.fulfillment.status !== "complete" || purchase.access !== "active") return;
    // Once per purchase, kept with the purchase so a restart or an expired conversation doesn't lose it.
    getService().grantQuestions(purchase.creditId ?? null, purchase.purchaseId, purchase.kind === "subscription_cycle");
    const document = current.documentOf(purchase);
    if (document) getService().recordPayPalResult(purchase.conversationId, purchase.actionId, document, issueReportToken(purchase.capabilityId, purchase.decisionId));
  }

  function getPayPal(): PayPalCommerceService | null {
    if (paypal !== undefined) return paypal;
    if (injected) return (paypal = null);
    const config = readPayPalConfig();
    if (!config) return (paypal = null);
    const service: PayPalCommerceService = new PayPalCommerceService({
      ...hostedDeps(config.env),
      api: new PayPalClient(config),
      config,
      onPurchaseUpdate: (purchase) => onPurchaseUpdate(service, purchase),
    });
    return (paypal = service);
  }

  const getService = () => {
    if (service) return service;
    const current = getPayPal();
    const config = injected ? null : readPayPalConfig() ?? readStripeConfig();
    return (service = new AssistantService(injected ?? createDefaultDeps(current, injected ? null : getStripe(), config ? { reportUsd: config.reportPriceUsd, subscriptionUsd: config.subscriptionPriceUsd } : null)));
  };

  if (!injected && !injectedPayPal && process.env.SELUN_SITES_REFRESH !== "0") {
    // Keeps what Selun knows about Sagitta in step with the live websites.
    const refreshSites = () => {
      const knowledge = getSiteKnowledge();
      if (knowledge.isStale) void knowledge.refresh();
    };
    setTimeout(refreshSites, 60_000).unref();
    setInterval(refreshSites, SITES_REFRESH_CHECK_MS).unref();
  }

  if (!injected && !injectedPayPal && process.env.SELUN_SCREEN_WARM !== "0") {
    // Most customers take the default profile, so they shouldn't wait for its screen.
    const warm = () => void getScorecardPreflight().warm(DEFAULT_SCREEN_PROFILE).catch((error) => console.warn("Market screen warm-up failed:", error instanceof Error ? error.message : error));
    setTimeout(warm, 30_000).unref();
    setInterval(warm, SCREEN_WARM_MS).unref();
  }

  if (!injected && !injectedPayPal) {
    // Picks up paid purchases interrupted by a restart, then catches anything webhooks missed.
    setImmediate(() => {
      try {
        for (const current of hostedServices()) {
          current.resume();
          setInterval(() => void current.sweep().catch((error) => console.warn("Purchase sweep failed:", error instanceof Error ? error.message : error)), PAYPAL_SWEEP_MS).unref();
        }
      } catch (error) {
        console.error("PayPal purchases could not be resumed:", error instanceof Error ? error.message : error);
      }
    });
  }

  function requirePayPal(): PayPalCommerceService {
    const current = getPayPal();
    if (!current) throw new AssistantError("service_unavailable", 503, "PayPal isn't available right now.");
    return current;
  }

  /** The purchase token is the only input, so a browser can only act on the purchase it started. */
  function purchaseClaim(token: unknown) {
    const claim = readPurchaseToken(token);
    if (!claim) throw new AssistantError("report_not_authorized", 403, "This purchase link isn't valid. Open it from your conversation.");
    return claim;
  }

  function withSummary(view: PurchaseView | SubscriptionView, purchase: { conversationId: string | null; actionId: string | null } | null) {
    const summary = purchase ? getService().payPalExplanation(purchase.conversationId, purchase.actionId) : null;
    return { ...view, explanation: summary?.explanation ?? null, explanationStatus: summary?.explanationStatus ?? "none" };
  }

  const chatLimiter = new WindowRateLimiter(readPositiveInt("ASSISTANT_CHAT_RATE_LIMIT_PER_15M", 40), 15 * 60_000);
  const actionLimiter = new WindowRateLimiter(readPositiveInt("ASSISTANT_ACTION_RATE_LIMIT_PER_15M", 60), 15 * 60_000);

  // Internal: what each payment cost to process and what Selun kept. Staff token only, and it holds no customer details.
  router.get("/admin/payments", (req, res) => {
    if (!requireAdminRequest(req, res)) return;
    res.set("Cache-Control", "no-store");
    const rows = [...hostedServices().flatMap((entry) => entry.paymentRows()), ...listWalletPaymentRows()]
      .sort((left, right) => right.paidAt.localeCompare(left.paidAt));
    res.json({ success: true, summary: summarizePayments(rows), payments: rows });
  });

  // Internal: what Selun AI answers cost, for setting allowances and credit packs. Staff token only.
  router.get("/admin/ai-usage", (req, res) => {
    if (!requireAdminRequest(req, res)) return;
    res.set("Cache-Control", "no-store");
    res.json({ success: true, usage: getService().creditSummary() });
  });

  // The questions a guest has left. Reading it never opens an allowance.
  router.get("/credits", (req, res) => {
    res.set("Cache-Control", "no-store");
    res.json({ success: true, credits: getService().creditView(req.get(GUEST_HEADER)?.trim() || null) });
  });

  router.get("/status", (_req, res) => {
    const current = getService();
    res.set("Cache-Control", "no-store");
    res.json({
      success: true,
      available: current.isConfigured,
      limits: current.publicLimits(),
      rails: getPaymentRails(Boolean(getPayPal())),
    });
  });

  router.get("/capabilities", async (_req, res) => {
    try {
      res.set("Cache-Control", "no-store");
      res.json({ success: true, capabilities: await getService().getPublicCapabilities(), rails: getPaymentRails(Boolean(getPayPal())) });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/chat", limited(chatLimiter), async (req, res) => {
    try {
      const reply = await getService().chat({ conversationId: conversationId(req), message: req.body?.message, guestToken: req.get(GUEST_HEADER)?.trim() || null, clientKey: clientKey(req) });
      res.set("Cache-Control", "no-store");
      res.json({ success: true, ...reply });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.get("/actions/:actionId", (req, res) => {
    try {
      res.set("Cache-Control", "no-store");
      res.json({ success: true, action: getService().getAction(conversationId(req), String(req.params.actionId)) });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/actions/:actionId/challenge", limited(actionLimiter), async (req, res) => {
    try {
      const prepared = await getService().prepareApproval(conversationId(req), String(req.params.actionId));
      res.set("Cache-Control", "no-store");
      res.json({ success: true, ...prepared });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/actions/:actionId/execute", limited(actionLimiter), async (req, res) => {
    try {
      const action = await getService().approveAndExecute(conversationId(req), String(req.params.actionId), {
        approved: req.body?.approved,
        approvedPriceUsdc: req.body?.approvedPriceUsdc,
        paymentSignature: req.body?.paymentSignature,
        rail: req.body?.rail,
      });
      res.set("Cache-Control", "no-store");
      res.status(202).json({ success: true, action });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/actions/:actionId/decline", limited(actionLimiter), (req, res) => {
    try {
      res.set("Cache-Control", "no-store");
      res.json({ success: true, action: getService().decline(conversationId(req), String(req.params.actionId)) });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/actions/:actionId/checkout", limited(actionLimiter), async (req, res) => {
    try {
      const started = await getService().startCheckout(conversationId(req), String(req.params.actionId), {
        method: req.body?.method,
        approved: req.body?.approved,
        approvedPrice: req.body?.approvedPrice,
      });
      res.set("Cache-Control", "no-store");
      res.json({ success: true, approveUrl: started.approveUrl, kind: started.kind, method: started.method, purchaseToken: issuePurchaseToken(started.id, started.kind), action: started.action });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/:provider/capture", limited(actionLimiter), async (req, res) => {
    try {
      const claim = purchaseClaim(req.body?.purchaseToken);
      const current = requireProvider(req.params.provider);
      const view = claim.kind === "subscription" ? await current.syncSubscription(claim.id) : await current.capture(claim.id);
      res.set("Cache-Control", "no-store");
      res.json({ success: true, ...withSummary(view, current.conversationOf(claim)) });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/:provider/status", limited(actionLimiter), async (req, res) => {
    try {
      const claim = purchaseClaim(req.body?.purchaseToken);
      const current = requireProvider(req.params.provider);
      const view = claim.kind === "subscription" ? await current.syncSubscription(claim.id) : await current.status(claim.id);
      res.set("Cache-Control", "no-store");
      res.json({ success: true, ...withSummary(view, current.conversationOf(claim)) });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/:provider/cancel", limited(actionLimiter), async (req, res) => {
    try {
      const claim = purchaseClaim(req.body?.purchaseToken);
      if (claim.kind !== "subscription") throw new AssistantError("invalid_request", 400, "Only a subscription can be cancelled.");
      const current = requireProvider(req.params.provider);
      res.set("Cache-Control", "no-store");
      res.json({ success: true, ...withSummary(await current.cancelSubscription(claim.id), current.conversationOf(claim)) });
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/:provider/subscription/assets", limited(actionLimiter), async (req, res) => {
    try {
      const claim = purchaseClaim(req.body?.purchaseToken);
      if (claim.kind !== "subscription") throw new AssistantError("invalid_request", 400, "Only a subscription can change its assets.");
      const queries: unknown[] = Array.isArray(req.body?.assets) ? req.body.assets : [];
      const resolved = queries.flatMap((query) => {
        const found = typeof query === "string" ? resolveAsset(query) : null;
        return found?.status === "resolved" ? [found.asset.symbol] : [];
      });
      const assets = [...new Set(resolved)];
      if (assets.length === 0 || assets.length > 12 || resolved.length !== queries.length) {
        throw new AssistantError("invalid_request", 400, "Name between 1 and 12 assets Selun recognizes, by name or symbol.");
      }
      const current = requireProvider(req.params.provider);
      const view = await current.updateSubscriptionAssets(claim.id, assets);
      res.set("Cache-Control", "no-store");
      res.json({ success: true, ...withSummary(view, current.conversationOf(claim)) });
    } catch (error) {
      sendError(res, error);
    }
  });

  // Called by PayPal directly, not through the frontend. PayPal's signature is checked before anything is read.
  router.post("/paypal/webhook", async (req, res) => {
    try {
      const rawBody = (req as Request & { rawBody?: string }).rawBody;
      if (!rawBody) throw new AssistantError("invalid_request", 400, "Empty webhook.");
      const result = await requirePayPal().handleWebhook({
        authAlgo: req.get("paypal-auth-algo") ?? "",
        certUrl: req.get("paypal-cert-url") ?? "",
        transmissionId: req.get("paypal-transmission-id") ?? "",
        transmissionSig: req.get("paypal-transmission-sig") ?? "",
        transmissionTime: req.get("paypal-transmission-time") ?? "",
      }, rawBody);
      res.json({ success: true, ...result });
    } catch (error) {
      sendError(res, error);
    }
  });

  // Called by Stripe directly. Its signature is checked over the exact bytes before anything is read.
  router.post("/stripe/webhook", async (req, res) => {
    try {
      const rawBody = (req as Request & { rawBody?: string }).rawBody;
      if (!rawBody) throw new AssistantError("invalid_request", 400, "Empty webhook.");
      const result = await requireProvider("stripe").handleWebhook(
        { authAlgo: "", certUrl: "", transmissionId: "", transmissionSig: "", transmissionTime: "", stripeSignature: req.get("stripe-signature") ?? "" },
        rawBody,
      );
      res.json({ success: true, ...result });
    } catch (error) {
      sendError(res, error);
    }
  });

  // The token in the body is the only input: no report content is accepted from the browser.
  router.post("/report/download", limited(actionLimiter), async (req, res) => {
    try {
      const report = await getReports().download(req.body?.reportToken);
      res.set({
        "Cache-Control": "no-store",
        "Content-Type": "application/pdf",
        "Content-Disposition": `attachment; filename="${report.filename}"`,
        "X-Selun-Record-Hash": report.recordHash,
      });
      res.send(Buffer.from(report.bytes));
    } catch (error) {
      sendError(res, error);
    }
  });

  router.post("/report/email", limited(actionLimiter), async (req, res) => {
    try {
      const result = await getReports().email(req.body?.reportToken, { email: req.body?.email, approved: req.body?.approved });
      res.set("Cache-Control", "no-store");
      res.json({ success: true, ...result });
    } catch (error) {
      sendError(res, error);
    }
  });

  return router;
}
