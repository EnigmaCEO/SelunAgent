import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { sendAdminErrorEmail } from "../services/email.service";
import { ScoreUnavailableError, UndeliverableError } from "../services/fulfillment-errors";
import { issueReportToken } from "../services/job-access.service";
import { AssistantError } from "./assistant-error";
import { getToolSpec } from "./capability-registry";
import { COMPREHENSIVE_REPORT_ID, type PayPalPaymentEvidence } from "./commerce";
import { PayPalApiError, type PayPalApi, type PayPalConfig, type PayPalSaleTransaction, type WebhookHeaders, type ProcessorFee } from "./paypal-client";
import { presentResult, type ResultDocument } from "./result-presenter";
import { purchaseOf, snapshotProblem, type PreflightResult, type ScreenProfile, type ScreeningSnapshot } from "./scorecard-preflight";

/**
 * PayPal purchases and subscriptions.
 *
 * The rules this exists to keep:
 * - The service runs only after PayPal itself reports a completed capture or sale.
 * - One capture or sale produces at most one fulfillment, however often it's retried.
 * - A failed run retries on the original payment, and that schedule survives a restart.
 *   A provider outage gets a long window, because a brief one shouldn't cost a sale and a fee.
 *   If the report still can't be delivered, the payment is refunded.
 * - A refund or reversal revokes that purchase only. Records and hashes are never deleted.
 */

export type PaymentStatus = "created" | "capture_pending" | "captured" | "declined" | "refunded" | "reversed" | "refund_failed";
export type FulfillmentStatus = "waiting" | "running" | "complete" | "failed" | "stopped";
/** Temporary provider trouble, an asset that can't score, or a renewal with nothing left to score. */
export type FailureKind = "provider" | "score_unavailable" | "nothing_scored";
export type AccessStatus = "active" | "restricted" | "revoked";

export type Purchase = {
  purchaseId: string;
  decisionId: string;
  kind: "one_time" | "subscription_cycle";
  capabilityId: string;
  title: string;
  input: Record<string, unknown>;
  amount: string;
  currency: "USD";
  conversationId: string | null;
  actionId: string | null;
  subscriptionLocalId: string | null;
  approveUrl: string | null;
  payment: {
    status: PaymentStatus;
    orderId: string | null;
    captureId: string | null;
    saleId: string | null;
    payerId: string | null;
    /** Never part of a result, a report, or anything the model reads. */
    payerEmail?: string | null;
    /** Internal reporting only: never in a result, a view, or anything the model reads. */
    processorFee?: ProcessorFee | null;
    capturedAt: string | null;
    refundId: string | null;
    checkedAt: string | null;
  };
  fulfillment: {
    status: FulfillmentStatus;
    attempts: number;
    lastError: string | null;
    completedAt: string | null;
    /** When the next try is due, saved so a restart keeps the schedule. */
    nextAttemptAt?: string | null;
    failureKind?: FailureKind | null;
  };
  access: AccessStatus;
  dispute: { id: string; status: string; outcome: string | null } | null;
  /** The stored result the report is built from. Never overwritten once complete. */
  responseData: Record<string, unknown> | null;
  /**
   * The screening decision this payment bought, used for every run and retry. Null for later
   * subscription months, which are new analyses screened when they run.
   */
  screening?: ScreeningSnapshot | null;
  audit: { at: string; event: string; detail?: string }[];
  createdAt: string;
  updatedAt: string;
};

export type SubscriptionStatus = "created" | "active" | "suspended" | "cancelled" | "expired";

export type LocalSubscription = {
  localId: string;
  paypalSubscriptionId: string | null;
  planId: string;
  capabilityId: string;
  title: string;
  input: Record<string, unknown>;
  amount: string;
  status: SubscriptionStatus;
  approveUrl: string | null;
  /** Taken from PayPal, needed to deliver each cycle's report. Never from a chat message. */
  subscriberEmail: string | null;
  payerId: string | null;
  nextBillingTime: string | null;
  conversationId: string | null;
  actionId: string | null;
  /** Applies to the first paid month only. */
  screening?: ScreeningSnapshot | null;
  /** PayPal sale ID to purchase ID. One entry per paid billing cycle. */
  cycles: Record<string, string>;
  audit: { at: string; event: string; detail?: string }[];
  createdAt: string;
  updatedAt: string;
};

/** A verified PayPal event, saved before it's acknowledged. Kept after processing as evidence. */
export type WebhookEventRecord = {
  id: string;
  type: string;
  resource: Record<string, unknown>;
  receivedAt: string;
  status: "pending" | "processed" | "unmatched" | "failed";
  attempts: number;
  nextAttemptAt: string;
  lastError: string | null;
  processedAt: string | null;
};

type StoreShape = {
  purchases: Record<string, Purchase>;
  subscriptions: Record<string, LocalSubscription>;
  plans: Record<string, string>;
  webhookEvents: Record<string, WebhookEventRecord>;
};

export type PurchaseView = {
  kind: "one_time";
  purchaseId: string;
  phase: "awaiting_payment" | "payment_pending" | "running" | "delayed" | "needs_selection" | "completed" | "refunded" | "restricted" | "revoked" | "failed";
  message: string | null;
  decisionId: string;
  document: ResultDocument | null;
  reportToken: string | null;
  /** Fills in the report's email field, for the customer to confirm or change. */
  payerEmail: string | null;
};

/** One payment in the internal fee report. A null fee means the provider hasn't reported one yet. */
export type PaymentRow = {
  provider: string;
  productId: string;
  kind: string;
  paidAt: string;
  reference: string;
  amount: string;
  currency: string;
  status: string;
  fee: string | null;
  net: string | null;
  feeCurrency: string | null;
  netKept: string | null;
};

export type SubscriptionView = {
  kind: "subscription";
  subscriptionLocalId: string;
  status: SubscriptionStatus;
  phase: PurchaseView["phase"];
  message: string | null;
  nextBillingTime: string | null;
  decisionId: string | null;
  document: ResultDocument | null;
  reportToken: string | null;
  reports: number;
};

export type CoverageNotice = {
  title: string;
  /** Each asset without an overall score, and why. */
  missing: { asset: string; reason: string }[];
  /** No report exists yet, and this month's payment depends on new assets by this date. */
  chooseBy: string | null;
};

export type PayPalCommerceDeps = {
  api: PayPalApi;
  config: Pick<PayPalConfig, "env" | "returnBaseUrl" | "reportPriceUsd" | "subscriptionPriceUsd" | "webhookId">;
  /** The same pipeline as the x402 services. Takes a decision and an input, returns the result. */
  runReport: (decisionId: string, input: Record<string, unknown>, screening?: ScreeningSnapshot | null, capabilityId?: string) => Promise<Record<string, unknown>>;
  /** Which provider this instance runs. Defaults to PayPal. */
  provider?: { id: string; label: string };
  /** For a provider that charges at checkout: a customer who paid and closed the page is found by checking. */
  checkUnpaidOrders?: boolean;
  /** Tells a subscriber a report lost an asset's score, or that this month's report needs different assets. */
  notifyCoverage?: (email: string, notice: CoverageNotice) => Promise<void>;
  /** Confirms new assets for a subscription before they replace the old ones. */
  checkAssets?: (assets: string[], profile: ScreenProfile) => Promise<PreflightResult>;
  /** Delivers a subscription cycle's report to the subscriber, through the report service. */
  deliverReport?: (reportToken: string, email: string) => Promise<void>;
  /** Told about any purchase that has reached an end state, so a live conversation can update. */
  onPurchaseUpdate?: (purchase: Purchase) => void;
  stateFile?: string;
  now?: () => Date;
  alert?: (message: string, purchase: Purchase | null) => void;
  /** Tries for a locked asset that can't score, since a feed coming back won't fix that. */
  maxAttempts?: number;
  /** The first wait. It doubles per try, up to an hour. */
  retryDelayMs?: number;
  /** How long a provider outage is retried, counted from payment. */
  retryWindowMs?: number;
  /** How long a renewal with nothing to score waits for different assets, counted from payment. */
  selectionWindowMs?: number;
  /** A market move can bring an asset back, so a month waiting on new assets is also tried again. */
  nothingScoredRetryMs?: number;
  maxConcurrentRuns?: number;
  reconcileAfterMs?: number;
  eventRetryMs?: number;
  maxEventAttempts?: number;
};

function defaultStateFile(): string {
  const configured = process.env.SELUN_PAYPAL_STATE_FILE?.trim();
  if (configured) return path.resolve(configured);
  return path.resolve(process.cwd(), process.cwd().endsWith("backend") ? "data" : "backend/data", "paypal-commerce.json");
}

const QUOTE_EXPIRED_MESSAGE = "This quote expired before payment was confirmed, so nothing was charged. Please ask Selun for a new quote.";
const RETRY_WINDOW_MS = 24 * 60 * 60_000;
const SELECTION_WINDOW_MS = 3 * 24 * 60 * 60_000;
const MAX_RETRY_DELAY_MS = 60 * 60_000;
const NOTHING_SCORED_RETRY_MS = 6 * 60 * 60_000;
const DELAYED_MESSAGE = "A market data provider is temporarily unavailable, so your report is taking longer. Your payment is safe and you won't be charged again. Selun keeps trying for up to 24 hours, and refunds you in full if it can't finish.";
const NEEDS_SELECTION_MESSAGE = "None of your assets can get an overall score this month. Choose different assets below to get this month's report. If you don't within 3 days, this month's payment is refunded in full.";
const REFUND_NOTE = "Selun could not deliver your report, so this payment has been refunded in full.";
const REFUNDED_MESSAGE = "We couldn't complete your report, so your payment has been refunded in full. It can take a few days to appear.";
const REFUND_FAILED_MESSAGE = "We couldn't complete your report, and the automatic refund didn't go through. Our team has been alerted and will refund you. Please contact support with the request ID below.";
const REVOKED_MESSAGE = "This payment was refunded or reversed, so this report is no longer available.";
const RESTRICTED_MESSAGE = "This purchase is under an open payment dispute, so this report is on hold until it's resolved.";
const SLOW_MESSAGE = "This payment is still being confirmed. Your report will start once it clears.";

/** Which assets a delivered report couldn't give an overall score, and why, from the result itself. */
export function missingScores(response: Record<string, unknown> | null): CoverageNotice["missing"] {
  const scorecard = (response?.result as { assetScorecard?: { scorecards?: unknown; unscreenedAssets?: unknown } } | undefined)?.assetScorecard;
  const list = (value: unknown) => (Array.isArray(value) ? (value as { asset?: string; overallScore?: number | null; priceHistory?: { reason?: string } }[]) : []);
  return [
    ...list(scorecard?.scorecards)
      .filter((card) => card.overallScore === null)
      .map((card) => ({ asset: card.asset ?? "An asset", reason: card.priceHistory?.reason ?? "Its price history wasn't enough to score." })),
    ...list(scorecard?.unscreenedAssets).map((entry) => ({ asset: entry.asset ?? "An asset", reason: "It wasn't in the group of assets screened for this segment this month." })),
  ];
}

export function purchaseIdFor(decisionId: string): string {
  return `pp-${createHash("sha256").update(decisionId).digest("hex").slice(0, 24)}`;
}

export class PayPalCommerceService {
  private readonly api: PayPalApi;
  private readonly stateFile: string;
  private readonly now: () => Date;
  private readonly maxAttempts: number;
  private readonly retryDelayMs: number;
  private readonly retryWindowMs: number;
  private readonly selectionWindowMs: number;
  private readonly nothingScoredRetryMs: number;
  private readonly retryTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly maxConcurrentRuns: number;
  private readonly reconcileAfterMs: number;
  private readonly eventRetryMs: number;
  private readonly maxEventAttempts: number;
  private drainTimer: ReturnType<typeof setTimeout> | null = null;
  private draining: Promise<void> | null = null;
  private store: StoreShape | null = null;
  private readonly locks = new Map<string, Promise<unknown>>();
  private readonly active = new Set<string>();
  private runsInFlight = 0;
  private readonly runWaiters: (() => void)[] = [];

  private readonly providerId: string;
  private readonly label: string;

  constructor(private readonly deps: PayPalCommerceDeps) {
    this.api = deps.api;
    this.providerId = deps.provider?.id ?? "paypal";
    this.label = deps.provider?.label ?? "PayPal";
    this.stateFile = deps.stateFile ?? defaultStateFile();
    this.now = deps.now ?? (() => new Date());
    this.maxAttempts = deps.maxAttempts ?? 3;
    this.retryDelayMs = deps.retryDelayMs ?? 60_000;
    this.retryWindowMs = deps.retryWindowMs ?? RETRY_WINDOW_MS;
    this.selectionWindowMs = deps.selectionWindowMs ?? SELECTION_WINDOW_MS;
    this.nothingScoredRetryMs = deps.nothingScoredRetryMs ?? NOTHING_SCORED_RETRY_MS;
    this.maxConcurrentRuns = deps.maxConcurrentRuns ?? 3;
    this.reconcileAfterMs = deps.reconcileAfterMs ?? 15 * 60_000;
    this.eventRetryMs = deps.eventRetryMs ?? 30_000;
    this.maxEventAttempts = deps.maxEventAttempts ?? 12;
  }

  // ---- Persistence ----

  private all(): StoreShape {
    if (this.store) return this.store;
    try {
      const parsed = JSON.parse(fs.readFileSync(this.stateFile, "utf8")) as Partial<StoreShape>;
      this.store = { purchases: parsed.purchases ?? {}, subscriptions: parsed.subscriptions ?? {}, plans: parsed.plans ?? {}, webhookEvents: parsed.webhookEvents ?? {} };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        // Starting empty could repeat a charge's fulfillment or lose a refund record.
        console.error("PayPal state could not be read:", error instanceof Error ? error.message : error);
        throw new AssistantError("service_unavailable", 503, `${this.label} checkout is temporarily unavailable. Please try again shortly.`);
      }
      this.store = { purchases: {}, subscriptions: {}, plans: {}, webhookEvents: {} };
    }
    return this.store;
  }

  private save() {
    fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
    const temp = `${this.stateFile}.tmp`;
    // Flushed to disk before the rename, because an acknowledged event must survive a power loss.
    const handle = fs.openSync(temp, "w");
    try {
      fs.writeFileSync(handle, JSON.stringify(this.store, null, 2), "utf8");
      fs.fsyncSync(handle);
    } finally {
      fs.closeSync(handle);
    }
    fs.renameSync(temp, this.stateFile);
  }

  private stamp(target: Purchase | LocalSubscription, event: string, detail?: string) {
    const at = this.now().toISOString();
    target.audit.push({ at, event, ...(detail ? { detail } : {}) });
    target.updatedAt = at;
    this.save();
  }

  /** One operation at a time per purchase, so a double click and a webhook can't race. */
  private withLock<T>(key: string, work: () => Promise<T>): Promise<T> {
    const run = (this.locks.get(key) ?? Promise.resolve()).then(work, work);
    this.locks.set(key, run.catch(() => undefined));
    return run;
  }

  private alert(message: string, purchase: Purchase | null) {
    console.error(`[paypal] ${message}`, purchase?.purchaseId ?? "");
    if (this.deps.alert) return this.deps.alert(message, purchase);
    void sendAdminErrorEmail({
      decisionId: purchase?.decisionId ?? "unknown",
      endpoint: "paypal",
      productId: purchase?.capabilityId ?? "paypal",
      errorReason: "paypal_fulfillment",
      errorMessage: message,
      walletAddress: null,
      transactionHash: purchase?.payment.captureId ?? purchase?.payment.saleId ?? null,
      network: "paypal",
      chargedAmountUsdc: purchase?.amount ?? "0",
      requestInput: purchase?.input ?? {},
    }).catch(() => undefined);
  }

  private findPurchase(match: (purchase: Purchase) => boolean): Purchase | undefined {
    return Object.values(this.all().purchases).find(match);
  }

  private requirePurchase(purchaseId: string): Purchase {
    const purchase = this.all().purchases[purchaseId];
    if (!purchase) throw new AssistantError("purchase_not_found", 404, "We couldn't find that purchase.");
    return purchase;
  }

  private requireSubscription(localId: string): LocalSubscription {
    const subscription = this.all().subscriptions[localId];
    if (!subscription) throw new AssistantError("purchase_not_found", 404, "We couldn't find that subscription.");
    return subscription;
  }

  // ---- One-time checkout ----

  async createCheckout(input: {
    /** The server's price. Defaults to the report's. */
    amount?: string;
    decisionId: string;
    capabilityId: string;
    title: string;
    body: Record<string, unknown>;
    conversationId: string | null;
    actionId: string | null;
    screening?: ScreeningSnapshot | null;
  }): Promise<{ purchaseId: string; approveUrl: string }> {
    const purchaseId = purchaseIdFor(input.decisionId);
    return this.withLock(purchaseId, async () => {
      const existing = this.all().purchases[purchaseId];
      if (existing) {
        if (existing.payment.status !== "created" || !existing.approveUrl) {
          throw new AssistantError("action_not_payable", 409, "This purchase has already been paid for.");
        }
        // Nothing is paid yet, so a re-checked quote replaces the one the order started with.
        if (input.screening && input.screening.id !== existing.screening?.id) {
          existing.screening = input.screening;
          this.stamp(existing, "screening_rebound", input.screening.id);
        }
        return { purchaseId, approveUrl: existing.approveUrl };
      }
      const amount = input.amount ?? this.deps.config.reportPriceUsd;
      const { orderId, approveUrl } = await this.api.createOrder({
        purchaseId,
        amount,
        description: `Selun ${input.title}`,
        returnUrl: `${this.deps.config.returnBaseUrl}/?checkout=return&purchase=${purchaseId}`,
        cancelUrl: `${this.deps.config.returnBaseUrl}/?checkout=cancel&purchase=${purchaseId}`,
      });
      const at = this.now().toISOString();
      const purchase: Purchase = {
        purchaseId,
        decisionId: input.decisionId,
        kind: "one_time",
        capabilityId: input.capabilityId,
        title: input.title,
        input: input.body,
        amount,
        currency: "USD",
        conversationId: input.conversationId,
        actionId: input.actionId,
        subscriptionLocalId: null,
        approveUrl,
        payment: { status: "created", orderId, captureId: null, saleId: null, payerId: null, capturedAt: null, refundId: null, checkedAt: null },
        fulfillment: { status: "waiting", attempts: 0, lastError: null, completedAt: null },
        access: "active",
        dispute: null,
        responseData: null,
        screening: input.screening ?? null,
        audit: [{ at, event: "order_created", detail: orderId }],
        createdAt: at,
        updatedAt: at,
      };
      this.all().purchases[purchaseId] = purchase;
      this.save();
      return { purchaseId, approveUrl };
    });
  }

  /**
   * Called when the customer returns. The browser's claim that it paid isn't used: only
   * PayPal's own order and capture responses move this purchase forward. Safe to repeat.
   */
  capture(purchaseId: string): Promise<PurchaseView> {
    return this.withLock(purchaseId, async () => {
      const purchase = this.requirePurchase(purchaseId);
      if (purchase.kind !== "one_time" || !purchase.payment.orderId) {
        throw new AssistantError("purchase_not_found", 404, "We couldn't find that purchase.");
      }
      if (purchase.payment.status === "created") await this.captureOrder(purchase);
      this.startFulfillment(purchase);
      return this.viewOf(purchase);
    });
  }

  private matchesOrder(purchase: Purchase, amount: string | null, currency: string | null, customId: string | null): string | null {
    if (customId !== purchase.purchaseId) return `purchase reference ${customId ?? "missing"}`;
    if (Number(amount) !== Number(purchase.amount) || currency !== purchase.currency) return `amount ${amount} ${currency}`;
    return null;
  }

  private async captureOrder(purchase: Purchase) {
    const orderId = purchase.payment.orderId!;
    let order = await this.api.getOrder(orderId);
    const mismatch = this.matchesOrder(purchase, order.amount, order.currency, order.customId);
    if (mismatch) {
      this.stamp(purchase, "order_mismatch", mismatch);
      this.alert(`${this.label} order ${orderId} doesn't match its purchase: ${mismatch}`, purchase);
      throw new AssistantError("payment_not_verified", 409, "We couldn't verify this payment. You have not been charged by Selun for it.");
    }
    if (order.status !== "COMPLETED") {
      if (order.status !== "APPROVED") {
        throw new AssistantError("payment_not_approved", 409, `${this.label} hasn't confirmed your approval yet, so nothing has been charged. Please try checkout again.`);
      }
      // The last moment nothing has been charged. Once captured, the purchase keeps this decision.
      if (purchase.screening && snapshotProblem(purchase.screening, purchaseOf(purchase.input), this.now().getTime())) {
        this.stamp(purchase, "capture_refused_quote_expired", purchase.screening.id);
        throw new AssistantError("quote_expired", 409, QUOTE_EXPIRED_MESSAGE);
      }
      order = await this.api.captureOrder(orderId, `selun-capture-${purchase.purchaseId}`);
    }
    if (!order.captureId) throw new AssistantError("payment_not_verified", 502, `${this.label} didn't confirm the payment. Please try again shortly.`);

    const captureMismatch = this.matchesOrder(purchase, order.captureAmount ?? order.amount, order.captureCurrency ?? order.currency, order.customId);
    if (captureMismatch) {
      this.stamp(purchase, "capture_mismatch", captureMismatch);
      this.alert(`${this.label} capture ${order.captureId} doesn't match its purchase: ${captureMismatch}`, purchase);
      throw new AssistantError("payment_not_verified", 409, "We couldn't verify this payment. Our team has been alerted.");
    }

    const owner = this.findPurchase((entry) => entry.payment.captureId === order.captureId);
    if (owner && owner.purchaseId !== purchase.purchaseId) {
      this.alert(`${this.label} capture ${order.captureId} is already tied to ${owner.purchaseId}`, purchase);
      throw new AssistantError("payment_not_verified", 409, "We couldn't verify this payment. Our team has been alerted.");
    }

    purchase.payment.captureId = order.captureId;
    purchase.payment.payerId = order.payerId;
    purchase.payment.payerEmail = order.payerEmail ?? null;
    purchase.payment.processorFee = order.processorFee ?? null;
    purchase.payment.checkedAt = this.now().toISOString();
    this.applyCaptureStatus(purchase, order.captureStatus ?? "UNKNOWN");
  }

  /** Only COMPLETED starts work. PENDING waits, whichever route learns about it. */
  private applyCaptureStatus(purchase: Purchase, status: string) {
    if (status === "COMPLETED" && (purchase.payment.status === "created" || purchase.payment.status === "capture_pending")) {
      purchase.payment.status = "captured";
      purchase.payment.capturedAt = this.now().toISOString();
      this.stamp(purchase, "capture_completed", purchase.payment.captureId ?? undefined);
    } else if (status === "PENDING" && purchase.payment.status === "created") {
      purchase.payment.status = "capture_pending";
      this.stamp(purchase, "capture_pending", purchase.payment.captureId ?? undefined);
    } else if (status === "DECLINED" || status === "FAILED") {
      if (purchase.payment.status === "created" || purchase.payment.status === "capture_pending") {
        purchase.payment.status = "declined";
        this.stamp(purchase, "capture_declined", status);
      }
    } else if (status === "REFUNDED") {
      this.revoke(purchase, "refunded", "capture_refunded");
    } else if (status === "REVERSED") {
      this.revoke(purchase, "reversed", "capture_reversed");
    }
    // PARTIALLY_REFUNDED leaves access alone: part of the payment still stands.
  }

  private revoke(purchase: Purchase, payment: "refunded" | "reversed", event: string) {
    if (purchase.access === "revoked" && purchase.payment.status === payment) return;
    purchase.payment.status = payment;
    purchase.access = "revoked";
    if (purchase.fulfillment.status !== "complete") purchase.fulfillment.status = "stopped";
    this.stamp(purchase, event);
    this.notify(purchase);
  }

  private notify(purchase: Purchase) {
    try {
      this.deps.onPurchaseUpdate?.(purchase);
    } catch (error) {
      console.error("PayPal update listener failed:", error instanceof Error ? error.message : error);
    }
  }

  // ---- Fulfillment ----

  private mayFulfill(purchase: Purchase): boolean {
    return purchase.payment.status === "captured" && purchase.access === "active" && purchase.fulfillment.status !== "complete" && purchase.fulfillment.status !== "stopped";
  }

  /** Starts the run in the background. Does nothing unless a verified payment is waiting and a try is due. */
  private startFulfillment(purchase: Purchase) {
    if (!this.mayFulfill(purchase) || this.active.has(purchase.purchaseId)) return;
    const due = purchase.fulfillment.nextAttemptAt ? Date.parse(purchase.fulfillment.nextAttemptAt) : 0;
    if (due > this.now().getTime()) {
      this.armRetry(purchase);
      return;
    }
    this.active.add(purchase.purchaseId);
    void this.fulfill(purchase)
      .catch((error) => this.alert(`Fulfillment crashed: ${error instanceof Error ? error.message : String(error)}`, purchase))
      .finally(() => this.active.delete(purchase.purchaseId));
  }

  /** The saved schedule is the record. This timer only acts on it, and resume() sets it again after a restart. */
  private armRetry(purchase: Purchase) {
    const next = purchase.fulfillment.nextAttemptAt;
    if (!next) return;
    clearTimeout(this.retryTimers.get(purchase.purchaseId));
    const timer = setTimeout(() => {
      this.retryTimers.delete(purchase.purchaseId);
      this.startFulfillment(purchase);
    }, Math.max(0, Date.parse(next) - this.now().getTime()));
    timer.unref?.();
    this.retryTimers.set(purchase.purchaseId, timer);
  }

  private async takeRunSlot() {
    if (this.runsInFlight < this.maxConcurrentRuns) {
      this.runsInFlight += 1;
      return;
    }
    await new Promise<void>((resolve) => this.runWaiters.push(resolve));
    this.runsInFlight += 1;
  }

  private releaseRunSlot() {
    this.runsInFlight -= 1;
    this.runWaiters.shift()?.();
  }

  private buildResponse(purchase: Purchase, result: Record<string, unknown>): Record<string, unknown> {
    const evidence: PayPalPaymentEvidence = {
      provider: this.providerId,
      ...(this.providerId === "paypal" ? {} : { label: this.label }),
      amount: purchase.amount,
      currency: purchase.currency,
      orderId: purchase.payment.orderId,
      captureId: purchase.payment.captureId,
      saleId: purchase.payment.saleId,
      subscriptionId: purchase.subscriptionLocalId ? this.all().subscriptions[purchase.subscriptionLocalId]?.paypalSubscriptionId ?? null : null,
      payerId: purchase.payment.payerId,
      environment: this.deps.config.env,
    };
    return {
      status: "completed",
      endpoint: `${this.providerId}:${purchase.capabilityId}`,
      decisionId: purchase.decisionId,
      productId: purchase.capabilityId,
      payment: { required: true, verified: true, ...evidence },
      result,
    };
  }

  /** One try. A failure decides the next step, which is saved rather than held in this call. */
  private async fulfill(purchase: Purchase) {
    purchase.fulfillment.status = "running";
    purchase.fulfillment.attempts += 1;
    purchase.fulfillment.nextAttemptAt = null;
    this.stamp(purchase, "fulfillment_started", `attempt ${purchase.fulfillment.attempts}`);

    let failure: unknown;
    await this.takeRunSlot();
    try {
      const result = await this.deps.runReport(purchase.decisionId, purchase.input, purchase.screening ?? null, purchase.capabilityId);
      const response = this.buildResponse(purchase, result);
      const spec = getToolSpec(purchase.capabilityId);
      // A result that can't become a report isn't a delivered one.
      if (!spec || !presentResult(purchase.capabilityId, spec.title, response)) throw new UndeliverableError("The result couldn't be turned into a report.");

      // The payment may have been reversed while this ran. Evidence is kept, access isn't granted.
      purchase.responseData = response;
      if (purchase.access === "revoked" || (purchase.fulfillment.status as FulfillmentStatus) === "stopped") {
        this.stamp(purchase, "fulfillment_finished_after_revocation");
        return;
      }
      purchase.fulfillment.status = "complete";
      purchase.fulfillment.completedAt = this.now().toISOString();
      purchase.fulfillment.lastError = null;
      purchase.fulfillment.failureKind = null;
      this.stamp(purchase, "fulfillment_completed");
      this.notify(purchase);
      await this.deliverCycle(purchase);
      return;
    } catch (error) {
      failure = error;
    } finally {
      this.releaseRunSlot();
    }
    await this.afterFailure(purchase, failure);
  }

  private async afterFailure(purchase: Purchase, error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    const renewal = purchase.kind === "subscription_cycle" && !purchase.screening;
    const kind: FailureKind = error instanceof ScoreUnavailableError ? (renewal ? "nothing_scored" : "score_unavailable") : "provider";
    purchase.fulfillment.status = "failed";
    purchase.fulfillment.lastError = message;
    purchase.fulfillment.failureKind = kind;
    this.stamp(purchase, "fulfillment_failed", message);
    if (!this.mayFulfill(purchase)) return;

    const now = this.now().getTime();
    const paidAt = Date.parse(purchase.payment.capturedAt ?? purchase.createdAt);
    const backoff = Math.min(this.retryDelayMs * 2 ** (purchase.fulfillment.attempts - 1), MAX_RETRY_DELAY_MS);
    let deadline: number;
    let delay = backoff;
    if (error instanceof UndeliverableError) deadline = now;
    else if (kind === "provider") deadline = paidAt + this.retryWindowMs;
    else if (kind === "nothing_scored") {
      deadline = paidAt + this.selectionWindowMs;
      delay = this.nothingScoredRetryMs;
      await this.sendCoverageNotice(purchase, [{ asset: purchaseOf(purchase.input).assets.join(", "), reason: message }], new Date(deadline).toISOString());
    } else {
      deadline = purchase.fulfillment.attempts >= this.maxAttempts ? now : Number.POSITIVE_INFINITY;
    }
    if (now >= deadline) {
      await this.refundUndelivered(purchase);
      return;
    }
    purchase.fulfillment.nextAttemptAt = new Date(Math.min(now + delay, deadline)).toISOString();
    this.stamp(purchase, "fulfillment_retry_scheduled", `${kind} at ${purchase.fulfillment.nextAttemptAt}`);
    this.armRetry(purchase);
  }

  /** Full refund for a payment that couldn't be delivered. PayPal keeps its fee. */
  private async refundUndelivered(purchase: Purchase) {
    await this.withLock(`refund-${purchase.purchaseId}`, async () => {
      if (purchase.payment.refundId) return;
      try {
        const refund = purchase.payment.captureId
          ? await this.api.refundCapture(purchase.payment.captureId, `selun-refund-${purchase.purchaseId}`, REFUND_NOTE)
          : await this.api.refundSale(purchase.payment.saleId ?? "", `selun-refund-${purchase.purchaseId}`);
        purchase.payment.refundId = refund.refundId;
        purchase.payment.status = "refunded";
        purchase.access = "revoked";
        purchase.fulfillment.status = "stopped";
        this.stamp(purchase, "refunded_undelivered", refund.refundId);
      } catch (error) {
        purchase.payment.status = "refund_failed";
        purchase.fulfillment.status = "stopped";
        this.stamp(purchase, "refund_failed", error instanceof Error ? error.message : String(error));
        this.alert(`Automatic refund failed for an undelivered report. Refund it by hand. Reference: ${purchase.payment.captureId ?? purchase.payment.saleId}`, purchase);
      }
      this.notify(purchase);
    });
  }

  private async deliverCycle(purchase: Purchase) {
    if (purchase.kind !== "subscription_cycle" || !purchase.subscriptionLocalId || !this.deps.deliverReport) return;
    const email = this.all().subscriptions[purchase.subscriptionLocalId]?.subscriberEmail;
    if (!email) return;
    try {
      await this.deps.deliverReport(issueReportToken(purchase.capabilityId, purchase.decisionId), email);
      this.stamp(purchase, "cycle_report_emailed");
    } catch (error) {
      // The report still exists and can be downloaded, so this isn't a failed delivery of the report.
      this.stamp(purchase, "cycle_report_email_failed", error instanceof Error ? error.message : String(error));
      this.alert("A subscription report was generated, but emailing it failed.", purchase);
    }
    const missing = missingScores(purchase.responseData);
    if (missing.length > 0) await this.sendCoverageNotice(purchase, missing, null);
  }

  /** Sent at most once per billing cycle. A notice that fails doesn't affect the report or the payment. */
  private async sendCoverageNotice(purchase: Purchase, missing: CoverageNotice["missing"], chooseBy: string | null) {
    if (purchase.audit.some((entry) => entry.event === "coverage_notice_sent")) return;
    const email = purchase.subscriptionLocalId ? this.all().subscriptions[purchase.subscriptionLocalId]?.subscriberEmail : null;
    if (!email || !this.deps.notifyCoverage) return;
    try {
      await this.deps.notifyCoverage(email, { title: purchase.title, missing, chooseBy });
      this.stamp(purchase, "coverage_notice_sent", missing.map((entry) => entry.asset).join(", "));
    } catch (error) {
      this.stamp(purchase, "coverage_notice_failed", error instanceof Error ? error.message : String(error));
    }
  }

  /** Runs after a restart. A pipeline run is in memory, so a paid purchase mid-run starts again. */
  resume() {
    this.scheduleDrain(0);
    for (const purchase of Object.values(this.all().purchases)) {
      if (purchase.payment.status === "captured" && purchase.fulfillment.status === "running") {
        purchase.fulfillment.status = "waiting";
        this.stamp(purchase, "fulfillment_resumed_after_restart");
      }
      // A try that's due now runs. One that isn't due is scheduled again from its saved time.
      this.startFulfillment(purchase);
    }
  }

  // ---- Reading state ----

  /** Which live conversation, if any, this purchase came from. */
  conversationOf(claim: { id: string; kind: "one_time" | "subscription" }): { conversationId: string | null; actionId: string | null } | null {
    return claim.kind === "subscription" ? this.all().subscriptions[claim.id] ?? null : this.all().purchases[claim.id] ?? null;
  }

  private phaseOf(purchase: Purchase): PurchaseView["phase"] {
    if (purchase.access === "revoked") return purchase.payment.status === "refunded" && purchase.fulfillment.completedAt === null ? "refunded" : "revoked";
    if (purchase.access === "restricted") return "restricted";
    if (purchase.payment.status === "created") return "awaiting_payment";
    if (purchase.payment.status === "capture_pending") return "payment_pending";
    if (purchase.payment.status === "declined" || purchase.payment.status === "refund_failed") return "failed";
    if (purchase.fulfillment.status === "complete") return "completed";
    if (purchase.fulfillment.status === "failed" && purchase.fulfillment.nextAttemptAt) {
      return purchase.fulfillment.failureKind === "nothing_scored" ? "needs_selection" : "delayed";
    }
    return "running";
  }

  private messageOf(purchase: Purchase, phase: PurchaseView["phase"]): string | null {
    if (phase === "refunded") return REFUNDED_MESSAGE;
    if (phase === "revoked") return REVOKED_MESSAGE;
    if (phase === "restricted") return RESTRICTED_MESSAGE;
    if (phase === "payment_pending") return SLOW_MESSAGE;
    if (phase === "delayed") return DELAYED_MESSAGE;
    if (phase === "needs_selection") return NEEDS_SELECTION_MESSAGE;
    if (phase === "failed") return purchase.payment.status === "declined" ? `${this.label} declined this payment, so you have not been charged.` : REFUND_FAILED_MESSAGE;
    return null;
  }

  documentOf(purchase: Purchase): ResultDocument | null {
    const spec = getToolSpec(purchase.capabilityId);
    return spec && purchase.responseData ? presentResult(purchase.capabilityId, spec.title, purchase.responseData) : null;
  }

  private viewOf(purchase: Purchase): PurchaseView {
    const phase = this.phaseOf(purchase);
    const completed = phase === "completed";
    return {
      kind: "one_time",
      purchaseId: purchase.purchaseId,
      phase,
      message: this.messageOf(purchase, phase),
      decisionId: purchase.decisionId,
      document: completed ? this.documentOf(purchase) : null,
      reportToken: completed ? issueReportToken(purchase.capabilityId, purchase.decisionId) : null,
      payerEmail: completed ? purchase.payment.payerEmail ?? null : null,
    };
  }

  /** Checks PayPal for anything a missed webhook didn't tell us. A PayPal outage keeps the last known state. */
  private async reconcile(purchase: Purchase, force = false) {
    if (!purchase.payment.captureId) return;
    const checkedAt = purchase.payment.checkedAt ? Date.parse(purchase.payment.checkedAt) : 0;
    if (!force && this.now().getTime() - checkedAt < this.reconcileAfterMs) return;
    try {
      const capture = await this.api.getCapture(purchase.payment.captureId);
      purchase.payment.checkedAt = this.now().toISOString();
      purchase.payment.processorFee = capture.processorFee ?? purchase.payment.processorFee ?? null;
      this.applyCaptureStatus(purchase, capture.status);
      this.save();
    } catch (error) {
      console.warn("PayPal capture check failed:", error instanceof Error ? error.message : error);
    }
  }

  status(purchaseId: string): Promise<PurchaseView> {
    return this.withLock(purchaseId, async () => {
      const purchase = this.requirePurchase(purchaseId);
      await this.reconcile(purchase, purchase.payment.status === "capture_pending");
      this.startFulfillment(purchase);
      return this.viewOf(purchase);
    });
  }

  /** Every payment taken, for internal reporting. Holds no customer details. */
  paymentRows(): PaymentRow[] {
    return Object.values(this.all().purchases).flatMap((purchase): PaymentRow[] => {
      const { payment } = purchase;
      const reference = payment.captureId ?? payment.saleId;
      if (!reference || !payment.capturedAt) return [];
      const refunded = payment.status === "refunded" || payment.status === "reversed";
      const fee = payment.processorFee ?? null;
      return [{
        provider: this.providerId,
        productId: purchase.capabilityId,
        kind: purchase.kind,
        paidAt: payment.capturedAt,
        reference,
        amount: purchase.amount,
        currency: purchase.currency,
        status: payment.status,
        fee: fee?.fee ?? null,
        net: fee?.net ?? null,
        feeCurrency: fee?.currency ?? null,
        // A refund returns the sale, and the provider keeps its fee.
        netKept: fee ? (refunded ? (-Number(fee.fee)).toFixed(2) : fee.net) : null,
      }];
    });
  }

  ownsDecision(decisionId: string): boolean {
    return Boolean(this.findPurchase((entry) => entry.decisionId === decisionId));
  }

  /** For the report service. Results this service didn't sell aren't handled here and pass. */
  async assertReportAccess(capabilityId: string, decisionId: string): Promise<void> {
    const purchase = this.findPurchase((entry) => entry.decisionId === decisionId);
    if (!purchase && capabilityId !== COMPREHENSIVE_REPORT_ID) return;
    if (!purchase) throw new AssistantError("report_not_found", 404, "We couldn't find a paid result for this report.");
    await this.withLock(purchase.purchaseId, () => this.reconcile(purchase));
    if (purchase.access === "revoked") throw new AssistantError("report_revoked", 403, REVOKED_MESSAGE);
    if (purchase.access === "restricted") throw new AssistantError("report_restricted", 403, RESTRICTED_MESSAGE);
  }

  loadPaidResponse(capabilityId: string, decisionId: string): Record<string, unknown> | null {
    const purchase = this.findPurchase((entry) => entry.decisionId === decisionId && entry.capabilityId === capabilityId);
    return purchase?.fulfillment.status === "complete" ? purchase.responseData : null;
  }

  // ---- Subscriptions ----

  private async ensurePlan(priceUsd: string): Promise<string> {
    return this.withLock(`plan-${priceUsd}`, async () => {
      const store = this.all();
      if (!store.plans[priceUsd]) {
        store.plans[priceUsd] = await this.api.createPlan(priceUsd);
        this.save();
      }
      return store.plans[priceUsd];
    });
  }

  async createSubscription(input: {
    actionKey: string;
    capabilityId: string;
    title: string;
    body: Record<string, unknown>;
    conversationId: string | null;
    actionId: string | null;
    screening?: ScreeningSnapshot | null;
  }): Promise<{ subscriptionLocalId: string; approveUrl: string }> {
    const localId = `ps-${createHash("sha256").update(input.actionKey).digest("hex").slice(0, 24)}`;
    return this.withLock(localId, async () => {
      const existing = this.all().subscriptions[localId];
      if (existing) {
        if (existing.status !== "created" || !existing.approveUrl) {
          throw new AssistantError("action_not_payable", 409, "This subscription has already been started.");
        }
        if (input.screening && input.screening.id !== existing.screening?.id) {
          existing.screening = input.screening;
          this.stamp(existing, "screening_rebound", input.screening.id);
        }
        return { subscriptionLocalId: localId, approveUrl: existing.approveUrl };
      }
      const amount = this.deps.config.subscriptionPriceUsd;
      const planId = await this.ensurePlan(amount);
      const { subscriptionId, approveUrl } = await this.api.createSubscription({
        planId,
        customId: localId,
        returnUrl: `${this.deps.config.returnBaseUrl}/?checkout=return&subscription=${localId}`,
        cancelUrl: `${this.deps.config.returnBaseUrl}/?checkout=cancel&subscription=${localId}`,
      });
      const at = this.now().toISOString();
      this.all().subscriptions[localId] = {
        localId,
        paypalSubscriptionId: subscriptionId,
        planId,
        capabilityId: input.capabilityId,
        title: input.title,
        input: input.body,
        amount,
        status: "created",
        approveUrl,
        subscriberEmail: null,
        payerId: null,
        nextBillingTime: null,
        conversationId: input.conversationId,
        actionId: input.actionId,
        screening: input.screening ?? null,
        cycles: {},
        audit: [{ at, event: "subscription_created", detail: subscriptionId }],
        createdAt: at,
        updatedAt: at,
      };
      this.save();
      return { subscriptionLocalId: localId, approveUrl };
    });
  }

  /**
   * Reads the subscription and its payments from PayPal. Each completed sale becomes one
   * report, keyed by sale ID, so a repeated sync or webhook can't produce a second one.
   */
  syncSubscription(localId: string): Promise<SubscriptionView> {
    return this.withLock(localId, async () => {
      const subscription = this.requireSubscription(localId);
      if (subscription.paypalSubscriptionId) await this.syncFromPayPal(subscription);
      return this.subscriptionView(subscription);
    });
  }

  private async syncFromPayPal(subscription: LocalSubscription): Promise<PayPalSaleTransaction[] | null> {
    const remote = await this.api.getSubscription(subscription.paypalSubscriptionId!);
    if (remote.customId !== subscription.localId || remote.planId !== subscription.planId) {
      this.alert(`${this.label} subscription ${remote.subscriptionId} doesn't match its record.`, null);
      throw new AssistantError("payment_not_verified", 409, "We couldn't verify this subscription. Our team has been alerted.");
    }
    subscription.subscriberEmail = remote.subscriberEmail ?? subscription.subscriberEmail;
    subscription.payerId = remote.payerId ?? subscription.payerId;
    subscription.nextBillingTime = remote.nextBillingTime;
    const status: SubscriptionStatus | null = remote.status === "ACTIVE" ? "active"
      : remote.status === "SUSPENDED" ? "suspended"
        : remote.status === "CANCELLED" ? "cancelled"
          : remote.status === "EXPIRED" ? "expired" : null;
    if (status && status !== subscription.status) {
      subscription.status = status;
      this.stamp(subscription, `subscription_${status}`);
    }
    if (subscription.status === "created") return null;

    const end = this.now();
    const start = new Date(Date.parse(subscription.createdAt) - 86_400_000);
    const sales = await this.api.listSubscriptionTransactions(subscription.paypalSubscriptionId!, start.toISOString(), end.toISOString());
    for (const sale of sales) this.applySale(subscription, sale);
    this.save();
    return sales;
  }

  private applySale(subscription: LocalSubscription, sale: PayPalSaleTransaction) {
    const known = subscription.cycles[sale.id];
    if (known) {
      const purchase = this.all().purchases[known];
      if (purchase && sale.processorFee) purchase.payment.processorFee = sale.processorFee;
      if (purchase && sale.status === "REFUNDED") this.revoke(purchase, "refunded", "sale_refunded");
      if (purchase && sale.status === "REVERSED") this.revoke(purchase, "reversed", "sale_reversed");
      if (purchase) this.startFulfillment(purchase);
      return;
    }
    if (sale.status !== "COMPLETED") return;
    if (Number(sale.amount) !== Number(subscription.amount) || sale.currency !== "USD") {
      this.alert(`Subscription payment ${sale.id} was ${sale.amount} ${sale.currency}, not ${subscription.amount} USD.`, null);
      return;
    }
    const owner = this.findPurchase((entry) => entry.payment.saleId === sale.id);
    if (owner) return;

    const purchaseId = `pc-${createHash("sha256").update(sale.id).digest("hex").slice(0, 24)}`;
    const at = this.now().toISOString();
    const purchase: Purchase = {
      purchaseId,
      decisionId: `selun-sub-${randomUUID()}`,
      kind: "subscription_cycle",
      capabilityId: subscription.capabilityId,
      title: subscription.title,
      input: subscription.input,
      amount: subscription.amount,
      currency: "USD",
      conversationId: subscription.conversationId,
      actionId: subscription.actionId,
      subscriptionLocalId: subscription.localId,
      approveUrl: null,
      payment: { status: "captured", orderId: null, captureId: null, saleId: sale.id, payerId: subscription.payerId, processorFee: sale.processorFee ?? null, capturedAt: at, refundId: null, checkedAt: at },
      fulfillment: { status: "waiting", attempts: 0, lastError: null, completedAt: null },
      access: "active",
      dispute: null,
      responseData: null,
      screening: Object.keys(subscription.cycles).length === 0 ? subscription.screening ?? null : null,
      audit: [{ at, event: "cycle_paid", detail: sale.id }],
      createdAt: at,
      updatedAt: at,
    };
    this.all().purchases[purchaseId] = purchase;
    subscription.cycles[sale.id] = purchaseId;
    this.stamp(subscription, "cycle_entitled", sale.id);
    this.startFulfillment(purchase);
  }

  private subscriptionView(subscription: LocalSubscription): SubscriptionView {
    const purchases = Object.values(subscription.cycles)
      .map((id) => this.all().purchases[id])
      .filter(Boolean)
      .sort((left, right) => left.createdAt.localeCompare(right.createdAt));
    const latest = purchases.at(-1);
    const phase: PurchaseView["phase"] = latest ? this.phaseOf(latest) : subscription.status === "created" ? "awaiting_payment" : "payment_pending";
    const completed = latest && phase === "completed";
    return {
      kind: "subscription",
      subscriptionLocalId: subscription.localId,
      status: subscription.status,
      phase,
      message: latest
        ? this.messageOf(latest, phase)
        : subscription.status === "active"
          ? "Your subscription is active. The first payment is being confirmed, and your report will start once it clears."
          : subscription.status === "created" ? `${this.label} hasn't confirmed your subscription, so nothing has been charged.` : null,
      nextBillingTime: subscription.nextBillingTime,
      decisionId: latest?.decisionId ?? null,
      document: completed ? this.documentOf(latest) : null,
      reportToken: completed ? issueReportToken(latest.capabilityId, latest.decisionId) : null,
      reports: purchases.filter((entry) => entry.fulfillment.status === "complete").length,
    };
  }

  /**
   * Replaces the assets for future months, once they're confirmed scoreable. A month waiting on new
   * assets runs straight away on them, under its original payment, and keeps that decision.
   */
  updateSubscriptionAssets(localId: string, assets: string[]): Promise<SubscriptionView> {
    return this.withLock(localId, async () => {
      const subscription = this.requireSubscription(localId);
      if (subscription.status !== "active") throw new AssistantError("action_not_payable", 409, "Only an active subscription can change its assets.");
      if (!this.deps.checkAssets) throw new AssistantError("service_unavailable", 503, "Selun can't check those assets right now. Please try again shortly.");
      const checked = await this.deps.checkAssets(assets, purchaseOf(subscription.input).profile);
      if (!checked.deliverable) {
        throw new AssistantError("scorecard_unavailable", 409, "Selun can't confirm an overall score for every one of those assets right now, so your subscription wasn't changed.", { assets: checked.assets });
      }
      subscription.input = { ...subscription.input, assets };
      this.stamp(subscription, "subscription_assets_updated", assets.join(", "));
      for (const id of Object.values(subscription.cycles)) {
        const purchase = this.all().purchases[id];
        if (!purchase || !this.mayFulfill(purchase) || purchase.fulfillment.failureKind !== "nothing_scored") continue;
        purchase.input = subscription.input;
        purchase.screening = checked.snapshot ?? null;
        purchase.fulfillment.nextAttemptAt = null;
        this.stamp(purchase, "cycle_assets_updated", assets.join(", "));
        this.startFulfillment(purchase);
      }
      return this.subscriptionView(subscription);
    });
  }

  /** Stops future billing. Reports already paid for stay available. */
  cancelSubscription(localId: string): Promise<SubscriptionView> {
    return this.withLock(localId, async () => {
      const subscription = this.requireSubscription(localId);
      if (!subscription.paypalSubscriptionId) throw new AssistantError("purchase_not_found", 404, "We couldn't find that subscription.");
      if (subscription.status === "active" || subscription.status === "suspended") {
        await this.api.cancelSubscription(subscription.paypalSubscriptionId, "Cancelled by the customer in Selun.");
        subscription.status = "cancelled";
        this.stamp(subscription, "subscription_cancelled_by_customer");
      }
      return this.subscriptionView(subscription);
    });
  }

  /** Catches anything webhooks missed. Failures are logged and retried on the next pass. */
  async sweep() {
    const abandoned = this.now().getTime() - 3 * 86_400_000;
    for (const subscription of Object.values(this.all().subscriptions)) {
      // A checkout never approved after a few days won't be, so it stops being checked.
      if (subscription.status === "created" && Date.parse(subscription.createdAt) < abandoned) continue;
      if (subscription.status === "created" || subscription.status === "active" || subscription.status === "suspended") {
        await this.withLock(subscription.localId, async () => void (await this.syncFromPayPal(subscription))).catch((error) => console.warn("PayPal subscription sync failed:", error instanceof Error ? error.message : error));
      }
    }
    const unpaidCutoff = this.now().getTime() - 2 * 60 * 60_000;
    for (const purchase of Object.values(this.all().purchases)) {
      if (this.deps.checkUnpaidOrders && purchase.kind === "one_time" && purchase.payment.status === "created" && Date.parse(purchase.createdAt) > unpaidCutoff) {
        // Refused until the customer has paid, so most of these simply wait for the next sweep.
        await this.capture(purchase.purchaseId).catch(() => undefined);
      } else if (purchase.payment.captureId && purchase.access !== "revoked") {
        await this.withLock(purchase.purchaseId, async () => {
          await this.reconcile(purchase);
          this.startFulfillment(purchase);
        }).catch((error) => console.warn("PayPal purchase sync failed:", error instanceof Error ? error.message : error));
      } else if (purchase.payment.saleId && purchase.access !== "revoked") {
        this.startFulfillment(purchase);
      }
    }
  }

  // ---- Webhooks ----

  /**
   * Verifies PayPal's signature, then saves the event before acknowledging it. Processing happens
   * afterwards, so nothing PayPal has been told we received can be lost to a crash or a slow
   * pipeline. A repeat of an event already saved is acknowledged and ignored.
   */
  async handleWebhook(headers: WebhookHeaders, rawBody: string): Promise<{ accepted: boolean; duplicate?: boolean }> {
    const webhookId = this.deps.config.webhookId;
    if (!webhookId) throw new AssistantError("service_unavailable", 503, "Webhooks aren't configured.");
    let verified = false;
    try {
      verified = await this.api.verifyWebhook(headers, rawBody, webhookId);
    } catch {
      // PayPal retries a failed delivery, so this isn't lost.
      throw new AssistantError("service_unavailable", 503, "Couldn't verify the webhook. Please retry.");
    }
    if (!verified) {
      console.warn(`[${this.providerId}-webhook] rejected: signature failed`);
      throw new AssistantError("report_not_authorized", 401, "Webhook signature rejected.");
    }

    let raw: Record<string, unknown>;
    try {
      raw = JSON.parse(rawBody) as typeof raw;
    } catch {
      throw new AssistantError("invalid_request", 400, "The webhook body wasn't valid JSON.");
    }
    const event = this.api.translateEvent?.(raw) ?? { id: raw.id, type: raw.event_type, resource: raw.resource };
    const eventId = typeof event.id === "string" ? event.id : "";
    if (!eventId) throw new AssistantError("invalid_request", 400, "The webhook had no event ID.");

    const store = this.all();
    if (store.webhookEvents[eventId]) {
      console.log(`[${this.providerId}-webhook] duplicate ${eventId} ${store.webhookEvents[eventId].type}`);
      return { accepted: true, duplicate: true };
    }
    const at = this.now().toISOString();
    store.webhookEvents[eventId] = {
      id: eventId,
      type: typeof event.type === "string" ? event.type : "UNKNOWN",
      resource: event.resource && typeof event.resource === "object" ? (event.resource as Record<string, unknown>) : {},
      receivedAt: at,
      status: "pending",
      attempts: 0,
      nextAttemptAt: at,
      lastError: null,
      processedAt: null,
    };
    this.save();
    console.log(`[${this.providerId}-webhook] accepted ${eventId} ${store.webhookEvents[eventId].type}`);
    this.scheduleDrain(0);
    return { accepted: true };
  }

  private scheduleDrain(delayMs: number) {
    if (this.drainTimer) return;
    this.drainTimer = setTimeout(() => {
      this.drainTimer = null;
      void this.drainEvents().catch((error) => console.error("PayPal event processing failed:", error instanceof Error ? error.message : error));
    }, delayMs);
    this.drainTimer.unref?.();
  }

  /**
   * Works through saved events in the order they arrived. Each one re-reads PayPal, so a late or
   * out-of-order event can only move a record to what PayPal says now. Safe to run at any time,
   * and it runs again on startup, which is what recovers events saved just before a restart.
   */
  async drainEvents(): Promise<void> {
    if (this.draining) return this.draining;
    this.draining = this.processDueEvents().finally(() => {
      this.draining = null;
    });
    return this.draining;
  }

  private async processDueEvents() {
    const events = Object.values(this.all().webhookEvents)
      .filter((event) => event.status === "pending" && Date.parse(event.nextAttemptAt) <= this.now().getTime())
      .sort((left, right) => left.receivedAt.localeCompare(right.receivedAt));
    for (const event of events) {
      event.attempts += 1;
      try {
        const handled = await this.applyEvent(event.type, event.resource);
        event.status = handled ? "processed" : "unmatched";
        event.processedAt = this.now().toISOString();
        event.lastError = null;
        console.log(`[${this.providerId}-webhook] ${event.status} ${event.id} ${event.type} attempt ${event.attempts}`);
      } catch (error) {
        event.lastError = error instanceof Error ? error.message : String(error);
        if (event.attempts >= this.maxEventAttempts) {
          event.status = "failed";
          this.alert(`A PayPal event could not be processed after ${event.attempts} attempts: ${event.type} ${event.id}. ${event.lastError}`, null);
        } else {
          const delay = Math.min(this.eventRetryMs * 2 ** (event.attempts - 1), 60 * 60_000);
          event.nextAttemptAt = new Date(this.now().getTime() + delay).toISOString();
        }
        console.warn(`[${this.providerId}-webhook] retry ${event.id} ${event.type} attempt ${event.attempts}: ${event.lastError}`);
      }
      this.save();
    }
    const next = Object.values(this.all().webhookEvents)
      .filter((event) => event.status === "pending")
      .map((event) => Date.parse(event.nextAttemptAt))
      .sort((left, right) => left - right)[0];
    if (next !== undefined) this.scheduleDrain(Math.max(next - this.now().getTime(), 1_000));
  }

  private async applyEvent(type: string, resource: Record<string, unknown>): Promise<boolean> {
    if (type === "CHECKOUT.ORDER.APPROVED") return this.applyOrderApproved(resource);
    if (type === "PAYMENT.CAPTURE.COMPLETED" || type === "PAYMENT.CAPTURE.PENDING" || type === "PAYMENT.CAPTURE.DENIED" || type === "PAYMENT.CAPTURE.DECLINED") {
      return this.applyCaptureEvent(String(resource.id ?? ""), resource, false);
    }
    if (type === "PAYMENT.CAPTURE.REFUNDED" || type === "PAYMENT.CAPTURE.REVERSED") {
      // These carry the refund. It may be partial, so the capture it points to decides.
      const links = Array.isArray(resource.links) ? (resource.links as { rel?: string; href?: string }[]) : [];
      const up = links.find((link) => link.rel === "up")?.href?.split("/").pop() ?? "";
      return this.applyCaptureEvent(up || String(resource.id ?? ""), resource, type === "PAYMENT.CAPTURE.REVERSED");
    }
    if (type.startsWith("CUSTOMER.DISPUTE.")) return this.applyDispute(type, resource);
    if (type.startsWith("BILLING.SUBSCRIPTION.")) return this.applySubscriptionEvent(type, resource);
    if (type.startsWith("PAYMENT.SALE.")) return this.applySaleEvent(type, resource);
    return false;
  }

  private purchaseForEvent(captureId: string, resource: Record<string, unknown>): Purchase | undefined {
    const related = ((resource.supplementary_data as { related_ids?: { order_id?: string } } | undefined)?.related_ids?.order_id) ?? null;
    const customId = typeof resource.custom_id === "string" ? resource.custom_id : null;
    return (captureId ? this.findPurchase((entry) => entry.payment.captureId === captureId) : undefined)
      ?? (customId ? this.all().purchases[customId] : undefined)
      ?? (related ? this.findPurchase((entry) => entry.payment.orderId === related) : undefined);
  }

  /** The buyer approved at PayPal. They may never come back to the page, so the capture can't depend on it. */
  private async applyOrderApproved(resource: Record<string, unknown>): Promise<boolean> {
    const orderId = String(resource.id ?? "");
    const purchase = orderId ? this.findPurchase((entry) => entry.payment.orderId === orderId) : undefined;
    if (!purchase) return false;
    await this.withLock(purchase.purchaseId, async () => {
      if (purchase.payment.status === "created") {
        try {
          await this.captureOrder(purchase);
        } catch (error) {
          // Retrying can't change an expired quote. The customer is told when they return, and isn't charged.
          if (error instanceof AssistantError && error.code === "quote_expired") return;
          throw error;
        }
        this.stamp(purchase, "captured_from_webhook");
      }
      this.startFulfillment(purchase);
    });
    return true;
  }

  private async applyCaptureEvent(captureId: string, resource: Record<string, unknown>, reversed: boolean): Promise<boolean> {
    const purchase = this.purchaseForEvent(captureId, resource);
    if (!purchase?.payment.orderId) return false;
    await this.withLock(purchase.purchaseId, async () => {
      // The event can arrive before this server has recorded the capture, so learn it from the order.
      if (purchase.payment.status === "created") await this.captureOrder(purchase);
      if (!purchase.payment.captureId) throw new Error("The order has no capture yet.");
      if (captureId && purchase.payment.captureId !== captureId) {
        this.stamp(purchase, "event_for_other_capture", captureId);
        return;
      }
      const capture = await this.api.getCapture(purchase.payment.captureId);
      purchase.payment.checkedAt = this.now().toISOString();
      purchase.payment.processorFee = capture.processorFee ?? purchase.payment.processorFee ?? null;
      this.applyCaptureStatus(purchase, capture.status);
      // PayPal signed this reversal. If the capture still reads as complete, the signed event wins.
      if (reversed && purchase.access !== "revoked") this.revoke(purchase, "reversed", "capture_reversed");
      this.save();
      this.startFulfillment(purchase);
    });
    return true;
  }

  private subscriptionFor(paypalSubscriptionId: string): LocalSubscription | undefined {
    return paypalSubscriptionId ? Object.values(this.all().subscriptions).find((entry) => entry.paypalSubscriptionId === paypalSubscriptionId) : undefined;
  }

  private async applySubscriptionEvent(type: string, resource: Record<string, unknown>): Promise<boolean> {
    const subscription = this.subscriptionFor(String(resource.id ?? "")) ?? this.all().subscriptions[String(resource.custom_id ?? "")];
    if (!subscription) return false;
    await this.withLock(subscription.localId, async () => {
      if (type === "BILLING.SUBSCRIPTION.PAYMENT.FAILED") this.stamp(subscription, "subscription_payment_failed");
      await this.syncFromPayPal(subscription);
    });
    return true;
  }

  /**
   * A paid month, a refund, or a reversal. PayPal's payment list can lag its events, so an event
   * for a payment not listed yet is retried rather than dropped. Only a listed, completed payment
   * ever earns a report, so activation and cancellation events can't create one.
   */
  private async applySaleEvent(type: string, resource: Record<string, unknown>): Promise<boolean> {
    const saleId = String((type === "PAYMENT.SALE.REFUNDED" ? resource.sale_id : resource.id) ?? "");
    const knownPurchase = saleId ? this.findPurchase((entry) => entry.payment.saleId === saleId) : undefined;
    const subscription = this.subscriptionFor(String(resource.billing_agreement_id ?? ""))
      ?? this.all().subscriptions[String(resource.custom_id ?? "")]
      ?? (knownPurchase?.subscriptionLocalId ? this.all().subscriptions[knownPurchase.subscriptionLocalId] : undefined);
    if (!subscription) return false;
    await this.withLock(subscription.localId, async () => {
      const sales = await this.syncFromPayPal(subscription);
      const listed = sales?.find((entry) => entry.id === saleId);
      if (!listed) throw new Error(`Payment ${saleId} isn't in PayPal's list yet.`);
      const purchase = this.findPurchase((entry) => entry.payment.saleId === saleId);
      if (type === "PAYMENT.SALE.REVERSED" && purchase) this.revoke(purchase, "reversed", "sale_reversed");
      if (type === "PAYMENT.SALE.REFUNDED" && purchase && listed.status === "COMPLETED") {
        throw new Error(`Payment ${saleId} isn't showing as refunded yet.`);
      }
    });
    return true;
  }

  /** Reads the dispute from PayPal where it can. The event is already signed, so it's the fallback. */
  private async disputeState(resource: Record<string, unknown>) {
    const disputeId = String(resource.dispute_id ?? "");
    try {
      if (disputeId) return await this.api.getDispute(disputeId);
    } catch (error) {
      if (!(error instanceof PayPalApiError) || ![401, 403, 404].includes(error.status)) throw error;
      console.warn(`[${this.providerId}-webhook] dispute ${disputeId} unreadable (${error.status}), using the signed event`);
    }
    const transactions = Array.isArray(resource.disputed_transactions) ? (resource.disputed_transactions as { seller_transaction_id?: string }[]) : [];
    return {
      disputeId,
      status: String(resource.status ?? ""),
      outcome: (resource.dispute_outcome as { outcome_code?: string } | undefined)?.outcome_code ?? null,
      transactionIds: transactions.map((entry) => entry.seller_transaction_id ?? "").filter(Boolean),
    };
  }

  private async applyDispute(type: string, resource: Record<string, unknown>): Promise<boolean> {
    const dispute = await this.disputeState(resource);
    const ids = new Set(dispute.transactionIds);
    const purchases = Object.values(this.all().purchases).filter((entry) => ids.has(entry.payment.captureId ?? "") || ids.has(entry.payment.saleId ?? ""));
    const resolved = type === "CUSTOMER.DISPUTE.RESOLVED" || dispute.status === "RESOLVED";
    for (const purchase of purchases) {
      purchase.dispute = { id: dispute.disputeId, status: dispute.status, outcome: dispute.outcome };
      if (resolved) {
        this.applyDisputeOutcome(purchase, dispute.outcome);
      } else if (purchase.access === "active") {
        purchase.access = "restricted";
        this.stamp(purchase, "dispute_opened", dispute.disputeId);
        this.notify(purchase);
      } else {
        this.stamp(purchase, "dispute_updated", dispute.disputeId);
      }
    }
    return purchases.length > 0;
  }

  /**
   * Access returns only when the payment stands. If the buyer won, the money went back to
   * them, which is a reversal: access stays off and the evidence stays on record.
   */
  private applyDisputeOutcome(purchase: Purchase, outcome: string | null) {
    const paymentStands = outcome === "RESOLVED_SELLER_FAVOUR" || outcome === "CANCELED_BY_BUYER" || outcome === "DENIED";
    const paymentReturned = outcome === "RESOLVED_BUYER_FAVOUR" || outcome === "ACCEPTED" || outcome === "RESOLVED_WITH_PAYOUT";
    if (paymentStands && purchase.access === "restricted") {
      purchase.access = "active";
      this.stamp(purchase, "dispute_resolved_access_restored", outcome ?? undefined);
      this.notify(purchase);
      // Fulfillment was held while the dispute was open.
      this.startFulfillment(purchase);
    } else if (paymentReturned) {
      this.revoke(purchase, "reversed", "dispute_resolved_payment_returned");
    } else if (!paymentStands) {
      // An outcome we don't recognize stays on hold for a person to look at.
      this.stamp(purchase, "dispute_resolved_unknown_outcome", outcome ?? undefined);
      this.alert(`Dispute ended with an unrecognized outcome: ${outcome}`, purchase);
    }
  }
}
