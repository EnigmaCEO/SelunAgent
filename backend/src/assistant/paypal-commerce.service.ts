import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { sendAdminErrorEmail } from "../services/email.service";
import { issueReportToken } from "../services/job-access.service";
import { AssistantError } from "./assistant-error";
import { getToolSpec } from "./capability-registry";
import { COMPREHENSIVE_REPORT_ID, type PayPalPaymentEvidence } from "./commerce";
import { PayPalApiError, type PayPalApi, type PayPalConfig, type PayPalSaleTransaction, type WebhookHeaders } from "./paypal-client";
import { presentResult, type ResultDocument } from "./result-presenter";

/**
 * PayPal purchases and subscriptions.
 *
 * The rules this exists to keep:
 * - The service runs only after PayPal itself reports a completed capture or sale.
 * - One capture or sale produces at most one fulfillment, however often it's retried.
 * - A failed run retries on the original payment. If it can't be delivered, it's refunded.
 * - A refund or reversal revokes that purchase only. Records and hashes are never deleted.
 */

export type PaymentStatus = "created" | "capture_pending" | "captured" | "declined" | "refunded" | "reversed" | "refund_failed";
export type FulfillmentStatus = "waiting" | "running" | "complete" | "failed" | "stopped";
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
    capturedAt: string | null;
    refundId: string | null;
    checkedAt: string | null;
  };
  fulfillment: { status: FulfillmentStatus; attempts: number; lastError: string | null; completedAt: string | null };
  access: AccessStatus;
  dispute: { id: string; status: string; outcome: string | null } | null;
  /** The stored result the report is built from. Never overwritten once complete. */
  responseData: Record<string, unknown> | null;
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
  phase: "awaiting_payment" | "payment_pending" | "running" | "completed" | "refunded" | "restricted" | "revoked" | "failed";
  message: string | null;
  decisionId: string;
  document: ResultDocument | null;
  reportToken: string | null;
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

export type PayPalCommerceDeps = {
  api: PayPalApi;
  config: Pick<PayPalConfig, "env" | "returnBaseUrl" | "reportPriceUsd" | "subscriptionPriceUsd" | "webhookId">;
  /** The same pipeline as the x402 services. Takes a decision and an input, returns the result. */
  runReport: (decisionId: string, input: Record<string, unknown>) => Promise<Record<string, unknown>>;
  /** Delivers a subscription cycle's report to the subscriber, through the report service. */
  deliverReport?: (reportToken: string, email: string) => Promise<void>;
  /** Told about any purchase that has reached an end state, so a live conversation can update. */
  onPurchaseUpdate?: (purchase: Purchase) => void;
  stateFile?: string;
  now?: () => Date;
  sleep?: (ms: number) => Promise<void>;
  alert?: (message: string, purchase: Purchase | null) => void;
  maxAttempts?: number;
  retryDelayMs?: number;
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

const REFUND_NOTE = "Selun could not deliver your report, so this payment has been refunded in full.";
const REFUNDED_MESSAGE = "We couldn't complete your report, so your payment has been refunded in full. It can take a few days to appear.";
const REFUND_FAILED_MESSAGE = "We couldn't complete your report, and the automatic refund didn't go through. Our team has been alerted and will refund you. Please contact support with the request ID below.";
const REVOKED_MESSAGE = "This payment was refunded or reversed, so this report is no longer available.";
const RESTRICTED_MESSAGE = "This purchase is under an open payment dispute, so this report is on hold until it's resolved.";
const SLOW_MESSAGE = "PayPal is still confirming this payment. Your report will start once it clears.";

export function purchaseIdFor(decisionId: string): string {
  return `pp-${createHash("sha256").update(decisionId).digest("hex").slice(0, 24)}`;
}

export class PayPalCommerceService {
  private readonly api: PayPalApi;
  private readonly stateFile: string;
  private readonly now: () => Date;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly maxAttempts: number;
  private readonly retryDelayMs: number;
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

  constructor(private readonly deps: PayPalCommerceDeps) {
    this.api = deps.api;
    this.stateFile = deps.stateFile ?? defaultStateFile();
    this.now = deps.now ?? (() => new Date());
    this.sleep = deps.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    this.maxAttempts = deps.maxAttempts ?? 3;
    this.retryDelayMs = deps.retryDelayMs ?? 30_000;
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
        throw new AssistantError("service_unavailable", 503, "PayPal checkout is temporarily unavailable. Please try again shortly.");
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
    decisionId: string;
    capabilityId: string;
    title: string;
    body: Record<string, unknown>;
    conversationId: string | null;
    actionId: string | null;
  }): Promise<{ purchaseId: string; approveUrl: string }> {
    const purchaseId = purchaseIdFor(input.decisionId);
    return this.withLock(purchaseId, async () => {
      const existing = this.all().purchases[purchaseId];
      if (existing) {
        if (existing.payment.status !== "created" || !existing.approveUrl) {
          throw new AssistantError("action_not_payable", 409, "This purchase has already been paid for.");
        }
        return { purchaseId, approveUrl: existing.approveUrl };
      }
      const amount = this.deps.config.reportPriceUsd;
      const { orderId, approveUrl } = await this.api.createOrder({
        purchaseId,
        amount,
        description: `Selun ${input.title}`,
        returnUrl: `${this.deps.config.returnBaseUrl}/?paypal=return&purchase=${purchaseId}`,
        cancelUrl: `${this.deps.config.returnBaseUrl}/?paypal=cancel&purchase=${purchaseId}`,
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
      this.alert(`PayPal order ${orderId} doesn't match its purchase: ${mismatch}`, purchase);
      throw new AssistantError("payment_not_verified", 409, "We couldn't verify this payment. You have not been charged by Selun for it.");
    }
    if (order.status !== "COMPLETED") {
      if (order.status !== "APPROVED") {
        throw new AssistantError("payment_not_approved", 409, "PayPal hasn't confirmed your approval yet, so nothing has been charged. Please try checkout again.");
      }
      order = await this.api.captureOrder(orderId, `selun-capture-${purchase.purchaseId}`);
    }
    if (!order.captureId) throw new AssistantError("payment_not_verified", 502, "PayPal didn't confirm the payment. Please try again shortly.");

    const captureMismatch = this.matchesOrder(purchase, order.captureAmount ?? order.amount, order.captureCurrency ?? order.currency, order.customId);
    if (captureMismatch) {
      this.stamp(purchase, "capture_mismatch", captureMismatch);
      this.alert(`PayPal capture ${order.captureId} doesn't match its purchase: ${captureMismatch}`, purchase);
      throw new AssistantError("payment_not_verified", 409, "We couldn't verify this payment. Our team has been alerted.");
    }

    const owner = this.findPurchase((entry) => entry.payment.captureId === order.captureId);
    if (owner && owner.purchaseId !== purchase.purchaseId) {
      this.alert(`PayPal capture ${order.captureId} is already tied to ${owner.purchaseId}`, purchase);
      throw new AssistantError("payment_not_verified", 409, "We couldn't verify this payment. Our team has been alerted.");
    }

    purchase.payment.captureId = order.captureId;
    purchase.payment.payerId = order.payerId;
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

  /** Starts the run in the background. Does nothing unless a verified payment is waiting. */
  private startFulfillment(purchase: Purchase) {
    if (!this.mayFulfill(purchase) || this.active.has(purchase.purchaseId)) return;
    this.active.add(purchase.purchaseId);
    void this.fulfill(purchase)
      .catch((error) => this.alert(`Fulfillment crashed: ${error instanceof Error ? error.message : String(error)}`, purchase))
      .finally(() => this.active.delete(purchase.purchaseId));
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
      provider: "paypal",
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
      endpoint: `paypal:${purchase.capabilityId}`,
      decisionId: purchase.decisionId,
      productId: purchase.capabilityId,
      payment: { required: true, verified: true, ...evidence },
      result,
    };
  }

  private async fulfill(purchase: Purchase) {
    while (this.mayFulfill(purchase) && purchase.fulfillment.attempts < this.maxAttempts) {
      purchase.fulfillment.status = "running";
      purchase.fulfillment.attempts += 1;
      this.stamp(purchase, "fulfillment_started", `attempt ${purchase.fulfillment.attempts}`);

      let failure: string | null = null;
      await this.takeRunSlot();
      try {
        const result = await this.deps.runReport(purchase.decisionId, purchase.input);
        const response = this.buildResponse(purchase, result);
        const spec = getToolSpec(purchase.capabilityId);
        // A result that can't become a report isn't a delivered one.
        if (!spec || !presentResult(purchase.capabilityId, spec.title, response)) throw new Error("The result couldn't be turned into a report.");

        // The payment may have been reversed while this ran. Evidence is kept, access isn't granted.
        purchase.responseData = response;
        if (purchase.access === "revoked" || (purchase.fulfillment.status as FulfillmentStatus) === "stopped") {
          this.stamp(purchase, "fulfillment_finished_after_revocation");
          return;
        }
        purchase.fulfillment.status = "complete";
        purchase.fulfillment.completedAt = this.now().toISOString();
        purchase.fulfillment.lastError = null;
        this.stamp(purchase, "fulfillment_completed");
        this.notify(purchase);
        await this.deliverCycle(purchase);
        return;
      } catch (error) {
        failure = error instanceof Error ? error.message : String(error);
      } finally {
        this.releaseRunSlot();
      }

      purchase.fulfillment.status = "failed";
      purchase.fulfillment.lastError = failure;
      this.stamp(purchase, "fulfillment_failed", failure);
      if (purchase.fulfillment.attempts < this.maxAttempts) await this.sleep(this.retryDelayMs);
    }
    if (this.mayFulfill(purchase) && purchase.fulfillment.attempts >= this.maxAttempts) await this.refundUndelivered(purchase);
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
  }

  /** Runs after a restart. A pipeline run is in memory, so a paid purchase mid-run starts again. */
  resume() {
    this.scheduleDrain(0);
    for (const purchase of Object.values(this.all().purchases)) {
      if (purchase.payment.status === "captured" && purchase.fulfillment.status === "running") {
        purchase.fulfillment.status = "waiting";
        this.stamp(purchase, "fulfillment_resumed_after_restart");
      }
      if (this.mayFulfill(purchase) && purchase.fulfillment.attempts >= this.maxAttempts) {
        void this.refundUndelivered(purchase);
      } else {
        this.startFulfillment(purchase);
      }
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
    return purchase.fulfillment.status === "complete" ? "completed" : "running";
  }

  private messageOf(purchase: Purchase, phase: PurchaseView["phase"]): string | null {
    if (phase === "refunded") return REFUNDED_MESSAGE;
    if (phase === "revoked") return REVOKED_MESSAGE;
    if (phase === "restricted") return RESTRICTED_MESSAGE;
    if (phase === "payment_pending") return SLOW_MESSAGE;
    if (phase === "failed") return purchase.payment.status === "declined" ? "PayPal declined this payment, so you have not been charged." : REFUND_FAILED_MESSAGE;
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

  /** For the report service. x402 results aren't handled here and always pass. */
  async assertReportAccess(capabilityId: string, decisionId: string): Promise<void> {
    if (capabilityId !== COMPREHENSIVE_REPORT_ID) return;
    const purchase = this.findPurchase((entry) => entry.decisionId === decisionId);
    if (!purchase) throw new AssistantError("report_not_found", 404, "We couldn't find a paid result for this report.");
    await this.withLock(purchase.purchaseId, () => this.reconcile(purchase));
    if (purchase.access === "revoked") throw new AssistantError("report_revoked", 403, REVOKED_MESSAGE);
    if (purchase.access === "restricted") throw new AssistantError("report_restricted", 403, RESTRICTED_MESSAGE);
  }

  loadPaidResponse(capabilityId: string, decisionId: string): Record<string, unknown> | null {
    if (capabilityId !== COMPREHENSIVE_REPORT_ID) return null;
    const purchase = this.findPurchase((entry) => entry.decisionId === decisionId);
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
  }): Promise<{ subscriptionLocalId: string; approveUrl: string }> {
    const localId = `ps-${createHash("sha256").update(input.actionKey).digest("hex").slice(0, 24)}`;
    return this.withLock(localId, async () => {
      const existing = this.all().subscriptions[localId];
      if (existing) {
        if (existing.status !== "created" || !existing.approveUrl) {
          throw new AssistantError("action_not_payable", 409, "This subscription has already been started.");
        }
        return { subscriptionLocalId: localId, approveUrl: existing.approveUrl };
      }
      const amount = this.deps.config.subscriptionPriceUsd;
      const planId = await this.ensurePlan(amount);
      const { subscriptionId, approveUrl } = await this.api.createSubscription({
        planId,
        customId: localId,
        returnUrl: `${this.deps.config.returnBaseUrl}/?paypal=return&subscription=${localId}`,
        cancelUrl: `${this.deps.config.returnBaseUrl}/?paypal=cancel&subscription=${localId}`,
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
      this.alert(`PayPal subscription ${remote.subscriptionId} doesn't match its record.`, null);
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

  private applySale(subscription: LocalSubscription, sale: { id: string; status: string; amount: string | null; currency: string | null }) {
    const known = subscription.cycles[sale.id];
    if (known) {
      const purchase = this.all().purchases[known];
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
      payment: { status: "captured", orderId: null, captureId: null, saleId: sale.id, payerId: subscription.payerId, capturedAt: at, refundId: null, checkedAt: at },
      fulfillment: { status: "waiting", attempts: 0, lastError: null, completedAt: null },
      access: "active",
      dispute: null,
      responseData: null,
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
          ? "Your subscription is active. PayPal is confirming the first payment, and your report will start once it clears."
          : subscription.status === "created" ? "PayPal hasn't confirmed your subscription, so nothing has been charged." : null,
      nextBillingTime: subscription.nextBillingTime,
      decisionId: latest?.decisionId ?? null,
      document: completed ? this.documentOf(latest) : null,
      reportToken: completed ? issueReportToken(latest.capabilityId, latest.decisionId) : null,
      reports: purchases.filter((entry) => entry.fulfillment.status === "complete").length,
    };
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
    for (const purchase of Object.values(this.all().purchases)) {
      if (purchase.payment.captureId && purchase.access !== "revoked") {
        await this.withLock(purchase.purchaseId, async () => {
          await this.reconcile(purchase);
          this.startFulfillment(purchase);
        }).catch((error) => console.warn("PayPal purchase sync failed:", error instanceof Error ? error.message : error));
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
      console.warn("[paypal-webhook] rejected: signature failed");
      throw new AssistantError("report_not_authorized", 401, "Webhook signature rejected.");
    }

    let event: { id?: unknown; event_type?: unknown; resource?: unknown };
    try {
      event = JSON.parse(rawBody) as typeof event;
    } catch {
      throw new AssistantError("invalid_request", 400, "The webhook body wasn't valid JSON.");
    }
    const eventId = typeof event.id === "string" ? event.id : "";
    if (!eventId) throw new AssistantError("invalid_request", 400, "The webhook had no event ID.");

    const store = this.all();
    if (store.webhookEvents[eventId]) {
      console.log(`[paypal-webhook] duplicate ${eventId} ${store.webhookEvents[eventId].type}`);
      return { accepted: true, duplicate: true };
    }
    const at = this.now().toISOString();
    store.webhookEvents[eventId] = {
      id: eventId,
      type: typeof event.event_type === "string" ? event.event_type : "UNKNOWN",
      resource: event.resource && typeof event.resource === "object" ? (event.resource as Record<string, unknown>) : {},
      receivedAt: at,
      status: "pending",
      attempts: 0,
      nextAttemptAt: at,
      lastError: null,
      processedAt: null,
    };
    this.save();
    console.log(`[paypal-webhook] accepted ${eventId} ${store.webhookEvents[eventId].type}`);
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
        console.log(`[paypal-webhook] ${event.status} ${event.id} ${event.type} attempt ${event.attempts}`);
      } catch (error) {
        event.lastError = error instanceof Error ? error.message : String(error);
        if (event.attempts >= this.maxEventAttempts) {
          event.status = "failed";
          this.alert(`A PayPal event could not be processed after ${event.attempts} attempts: ${event.type} ${event.id}. ${event.lastError}`, null);
        } else {
          const delay = Math.min(this.eventRetryMs * 2 ** (event.attempts - 1), 60 * 60_000);
          event.nextAttemptAt = new Date(this.now().getTime() + delay).toISOString();
        }
        console.warn(`[paypal-webhook] retry ${event.id} ${event.type} attempt ${event.attempts}: ${event.lastError}`);
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
        await this.captureOrder(purchase);
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
    const subscription = this.subscriptionFor(String(resource.id ?? ""));
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
      console.warn(`[paypal-webhook] dispute ${disputeId} unreadable (${error.status}), using the signed event`);
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
