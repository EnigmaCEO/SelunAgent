import { createHmac, timingSafeEqual } from "node:crypto";
import { PayPalApiError, type PayPalApi, type PayPalCapture, type PayPalConfig, type PayPalDispute, type PayPalOrder, type PayPalSaleTransaction, type PayPalSubscription, type ProcessorFee, type WebhookHeaders } from "./paypal-client";

/**
 * Stripe behind the same interface as PayPal, so the durable purchase service works unchanged. Nothing from the
 * browser is trusted: amounts and statuses are read back from Stripe. A Checkout Session stands for PayPal's order
 * and subscription, and the API version is pinned because later ones move invoice fields this reads.
 */
const API_VERSION = "2024-06-20";
/** Stripe's shortest allowed. Stripe charges at checkout, so this keeps payment inside the quote's 2-hour window. */
const SESSION_MINUTES = 30;

export type StripeConfig = Pick<PayPalConfig, "env" | "returnBaseUrl" | "reportPriceUsd" | "subscriptionPriceUsd" | "webhookId"> & { secretKey: string };

function readPrice(raw: string | undefined, fallback: string): string {
  const value = raw?.trim();
  return value && /^\d+\.\d{2}$/.test(value) && Number(value) > 0 ? value : fallback;
}

/** A live key is refused unless live payments were explicitly turned on. */
export function readStripeConfig(env: NodeJS.ProcessEnv = process.env): StripeConfig | null {
  const secretKey = env.STRIPE_SECRET_KEY?.trim();
  if (!secretKey) return null;
  const test = secretKey.startsWith("sk_test_") || secretKey.startsWith("rk_test_");
  if (!test && env.STRIPE_ENV?.trim() !== "live") {
    console.warn("Stripe stays off for Selun AI: this isn't a test key, and STRIPE_ENV isn't live.");
    return null;
  }
  return {
    env: test ? "sandbox" : "live",
    secretKey,
    // Stripe's signing secret. Each webhook endpoint has its own.
    webhookId: env.STRIPE_WEBHOOK_SECRET?.trim() || null,
    returnBaseUrl: (env.STRIPE_RETURN_BASE_URL?.trim() || env.PAYPAL_RETURN_BASE_URL?.trim() || "http://localhost:3000").replace(/\/+$/, ""),
    reportPriceUsd: readPrice(env.PAYPAL_REPORT_PRICE_USD, "4.99"),
    subscriptionPriceUsd: readPrice(env.PAYPAL_SUBSCRIPTION_PRICE_USD, "9.99"),
  };
}

type Json = Record<string, unknown>;
const record = (value: unknown): Json => (value && typeof value === "object" && !Array.isArray(value) ? (value as Json) : {});
const text = (value: unknown): string | null => (typeof value === "string" && value ? value : null);
const idOf = (value: unknown): string | null => text(value) ?? text(record(value).id);
const dollars = (cents: unknown): string | null => (typeof cents === "number" ? (cents / 100).toFixed(2) : null);
const cents = (amount: string): number => Math.round(Number(amount) * 100);
const upper = (value: unknown): string | null => text(value)?.toUpperCase() ?? null;

function encode(body: Record<string, string | number>): string {
  return new URLSearchParams(Object.entries(body).map(([key, value]): [string, string] => [key, String(value)])).toString();
}

function paymentIntentStatus(status: string | null): string {
  if (status === "succeeded") return "COMPLETED";
  if (status === "processing") return "PENDING";
  if (status === "canceled") return "FAILED";
  return "UNKNOWN";
}

/** Stripe reports its fee on the charge's balance transaction, in the account's own currency. */
function chargeFee(charge: Json): ProcessorFee | null {
  const balance = record(charge.balance_transaction);
  const fee = dollars(balance.fee);
  const net = dollars(balance.net);
  return fee && net ? { fee, net, currency: upper(balance.currency) ?? "USD" } : null;
}

function chargeStatus(charge: Json, fallback: string): string {
  // `disputed` stays true after a dispute is won, so only a lost one means the money went back.
  // An open one is restricted by its webhook, which can restore access if it's won.
  if (record(charge.dispute).status === "lost") return "REVERSED";
  if (charge.refunded === true) return "REFUNDED";
  if (typeof charge.amount_refunded === "number" && charge.amount_refunded > 0) return "PARTIALLY_REFUNDED";
  return fallback;
}

const SIGNATURE_TOLERANCE_SECONDS = 300;

/** Stripe's closing status, in the outcome terms the purchase service acts on. */
const DISPUTE_OUTCOME: Record<string, string> = {
  won: "RESOLVED_SELLER_FAVOUR",
  warning_closed: "RESOLVED_SELLER_FAVOUR",
  lost: "RESOLVED_BUYER_FAVOUR",
  charge_refunded: "RESOLVED_BUYER_FAVOUR",
};

export class StripeClient implements PayPalApi {
  constructor(
    private readonly config: Pick<StripeConfig, "secretKey">,
    private readonly fetchImpl: typeof fetch = fetch,
    private readonly now: () => number = Date.now,
  ) {}

  private async call(method: "GET" | "POST" | "DELETE", path: string, body?: Record<string, string | number>, idempotencyKey?: string): Promise<Json> {
    const response = await this.fetchImpl(`https://api.stripe.com/v1/${path}`, {
      method,
      headers: {
        authorization: `Bearer ${this.config.secretKey}`,
        "stripe-version": API_VERSION,
        ...(body ? { "content-type": "application/x-www-form-urlencoded" } : {}),
        ...(idempotencyKey ? { "idempotency-key": idempotencyKey } : {}),
      },
      ...(body ? { body: encode(body) } : {}),
    });
    const data = record(await response.json().catch(() => ({})));
    if (!response.ok) {
      const error = record(data.error);
      // Stripe's message can quote request details, so only its codes leave this module.
      throw new PayPalApiError(response.status, text(error.code) ?? text(error.type) ?? "stripe_error", `Stripe request failed with status ${response.status}.`);
    }
    return data;
  }

  private session(id: string): Promise<Json> {
    return this.call("GET", `checkout/sessions/${encodeURIComponent(id)}?expand[]=payment_intent.latest_charge.balance_transaction&expand[]=payment_intent.latest_charge.dispute&expand[]=subscription.latest_invoice`);
  }

  private expiry(): number {
    return Math.floor(Date.now() / 1000) + SESSION_MINUTES * 60 + 60;
  }

  async createOrder(input: { purchaseId: string; amount: string; description: string; returnUrl: string; cancelUrl: string }): Promise<{ orderId: string; approveUrl: string }> {
    const session = await this.call("POST", "checkout/sessions", {
      mode: "payment",
      client_reference_id: input.purchaseId,
      success_url: input.returnUrl,
      cancel_url: input.cancelUrl,
      expires_at: this.expiry(),
      // Only cards settle at checkout. Slower methods would need events this doesn't handle yet.
      "payment_method_types[0]": "card",
      "line_items[0][quantity]": 1,
      "line_items[0][price_data][currency]": "usd",
      "line_items[0][price_data][unit_amount]": cents(input.amount),
      "line_items[0][price_data][product_data][name]": input.description,
    });
    const orderId = text(session.id);
    const approveUrl = text(session.url);
    if (!orderId || !approveUrl) throw new PayPalApiError(502, "invalid_response", "Stripe didn't return a checkout page.");
    return { orderId, approveUrl };
  }

  async getOrder(orderId: string): Promise<PayPalOrder> {
    const session = await this.session(orderId);
    const intent = record(session.payment_intent);
    const paid = session.payment_status === "paid" && text(intent.id) !== null;
    const status = paymentIntentStatus(text(intent.status));
    const charge = record(intent.latest_charge);
    return {
      orderId,
      // Stripe has already charged by the time a session completes, so there's no separate capture step.
      status: paid ? "COMPLETED" : "CREATED",
      customId: text(session.client_reference_id),
      amount: dollars(session.amount_total),
      currency: upper(session.currency),
      payerId: idOf(session.customer),
      payerEmail: text(record(session.customer_details).email),
      processorFee: paid ? chargeFee(charge) : null,
      captureId: paid ? text(intent.id) : null,
      captureStatus: paid ? chargeStatus(charge, status) : null,
      captureAmount: paid ? dollars(intent.amount_received) : null,
      captureCurrency: paid ? upper(intent.currency) : null,
    };
  }

  captureOrder(orderId: string): Promise<PayPalOrder> {
    return this.getOrder(orderId);
  }

  async getCapture(captureId: string): Promise<PayPalCapture> {
    const intent = await this.call("GET", `payment_intents/${encodeURIComponent(captureId)}?expand[]=latest_charge.balance_transaction&expand[]=latest_charge.dispute`);
    return {
      captureId,
      status: chargeStatus(record(intent.latest_charge), paymentIntentStatus(text(intent.status))),
      amount: dollars(intent.amount_received),
      currency: upper(intent.currency),
    };
  }

  async refundCapture(captureId: string, requestId: string): Promise<{ refundId: string; status: string }> {
    const refund = await this.call("POST", "refunds", { payment_intent: captureId }, requestId);
    return this.refundResult(refund);
  }

  private refundResult(refund: Json): { refundId: string; status: string } {
    const refundId = text(refund.id);
    if (!refundId) throw new PayPalApiError(502, "invalid_response", "Stripe didn't confirm the refund.");
    const status = text(refund.status);
    return { refundId, status: status === "succeeded" ? "COMPLETED" : status === "failed" || status === "canceled" ? "FAILED" : "PENDING" };
  }

  async createPlan(priceUsd: string): Promise<string> {
    const price = await this.call("POST", "prices", {
      currency: "usd",
      unit_amount: cents(priceUsd),
      "recurring[interval]": "month",
      "product_data[name]": "Selun monthly Comprehensive Intelligence Report",
    });
    const id = text(price.id);
    if (!id) throw new PayPalApiError(502, "invalid_response", "Stripe didn't create the monthly price.");
    return id;
  }

  async createSubscription(input: { planId: string; customId: string; returnUrl: string; cancelUrl: string }): Promise<{ subscriptionId: string; approveUrl: string }> {
    const session = await this.call("POST", "checkout/sessions", {
      mode: "subscription",
      client_reference_id: input.customId,
      success_url: input.returnUrl,
      cancel_url: input.cancelUrl,
      expires_at: this.expiry(),
      // Only cards settle at checkout. Slower methods would need events this doesn't handle yet.
      "payment_method_types[0]": "card",
      "line_items[0][quantity]": 1,
      "line_items[0][price]": input.planId,
      // Renewal events name the subscription, not this session, so this ties them back.
      "subscription_data[metadata][selun_subscription]": input.customId,
    });
    const subscriptionId = text(session.id);
    const approveUrl = text(session.url);
    if (!subscriptionId || !approveUrl) throw new PayPalApiError(502, "invalid_response", "Stripe didn't return a checkout page.");
    // Stripe only creates the subscription once the customer pays, so the session is the stable reference.
    return { subscriptionId, approveUrl };
  }

  async getSubscription(subscriptionId: string): Promise<PayPalSubscription> {
    const session = await this.session(subscriptionId);
    const subscription = record(session.subscription);
    const remote = text(subscription.status);
    const items = record(subscription.items);
    const firstItem = record((Array.isArray(items.data) ? items.data : [])[0]);
    const periodEnd = subscription.current_period_end;
    return {
      subscriptionId,
      status: remote === "active" || remote === "trialing" ? "ACTIVE"
        : remote === "past_due" || remote === "unpaid" || remote === "paused" ? "SUSPENDED"
          : remote === "canceled" ? "CANCELLED"
            : remote === "incomplete_expired" || session.status === "expired" ? "EXPIRED" : "APPROVAL_PENDING",
      customId: text(session.client_reference_id),
      planId: idOf(record(firstItem.price)),
      payerId: idOf(session.customer),
      subscriberEmail: text(record(session.customer_details).email),
      nextBillingTime: typeof periodEnd === "number" ? new Date(periodEnd * 1000).toISOString() : null,
    };
  }

  async listSubscriptionTransactions(subscriptionId: string, start: string, end: string): Promise<PayPalSaleTransaction[]> {
    const session = await this.session(subscriptionId);
    const remote = idOf(session.subscription);
    if (!remote) return [];
    const query = `invoices?subscription=${encodeURIComponent(remote)}&status=paid&limit=100&expand[]=data.charge.balance_transaction&expand[]=data.charge.dispute&created[gte]=${Math.floor(Date.parse(start) / 1000)}&created[lte]=${Math.ceil(Date.parse(end) / 1000)}`;
    const invoices = await this.call("GET", query);
    const list = Array.isArray(invoices.data) ? invoices.data.map(record) : [];
    return list.flatMap((invoice): PayPalSaleTransaction[] => {
      const charge = record(invoice.charge);
      const id = text(charge.id);
      if (!id) return [];
      const paidAt = record(invoice.status_transitions).paid_at;
      return [{
        id,
        status: chargeStatus(charge, charge.status === "succeeded" ? "COMPLETED" : "PENDING"),
        amount: dollars(invoice.amount_paid),
        currency: upper(invoice.currency),
        time: new Date((typeof paidAt === "number" ? paidAt : Number(invoice.created)) * 1000).toISOString(),
        processorFee: chargeFee(charge),
      }];
    });
  }

  async cancelSubscription(subscriptionId: string): Promise<void> {
    const session = await this.session(subscriptionId);
    const remote = idOf(session.subscription);
    // Stripe refuses to cancel twice, and a cancelled subscription is already what was asked for.
    if (remote && record(session.subscription).status !== "canceled") await this.call("DELETE", `subscriptions/${encodeURIComponent(remote)}`);
    else if (session.status === "open") await this.call("POST", `checkout/sessions/${encodeURIComponent(subscriptionId)}/expire`, {});
  }

  async refundSale(saleId: string, requestId: string): Promise<{ refundId: string; status: string }> {
    const refund = await this.call("POST", "refunds", { charge: saleId }, requestId);
    return this.refundResult(refund);
  }

  /** The signed dispute event already says everything needed, so there's nothing further to read. */
  async getDispute(disputeId: string): Promise<PayPalDispute> {
    throw new PayPalApiError(404, "not_supported", `Stripe dispute ${disputeId} isn't read here.`);
  }

  /** Checks Stripe's signature over the exact bytes it sent, and refuses an old one so a captured event can't be replayed. */
  async verifyWebhook(headers: WebhookHeaders, rawBody: string, secret: string): Promise<boolean> {
    const parts = (headers.stripeSignature ?? "").split(",").map((part) => part.trim().split("="));
    const timestamp = Number(parts.find(([key]) => key === "t")?.[1]);
    const signatures = parts.filter(([key]) => key === "v1").map(([, value]) => value ?? "");
    if (!Number.isFinite(timestamp) || signatures.length === 0) return false;
    if (Math.abs(this.now() / 1000 - timestamp) > SIGNATURE_TOLERANCE_SECONDS) return false;
    const expected = Buffer.from(createHmac("sha256", secret).update(`${timestamp}.${rawBody}`).digest("hex"));
    return signatures.some((signature) => {
      const given = Buffer.from(signature);
      return given.length === expected.length && timingSafeEqual(given, expected);
    });
  }

  /** Each result is only a prompt: the purchase service re-reads Stripe before changing anything. */
  translateEvent(event: Json): { id: string; type: string; resource: Json } {
    const id = text(event.id) ?? "";
    const object = record(record(event.data).object);
    const stripeType = text(event.type) ?? "UNKNOWN";
    const to = (type: string, resource: Json) => ({ id, type, resource });
    const subscriptionKey = text(record(record(object.subscription_details).metadata).selun_subscription) ?? text(record(object.metadata).selun_subscription);
    switch (stripeType) {
      case "checkout.session.completed":
        return to(object.mode === "subscription" ? "BILLING.SUBSCRIPTION.ACTIVATED" : "CHECKOUT.ORDER.APPROVED", { id: text(object.id) });
      case "charge.refunded":
        return object.invoice
          ? to("PAYMENT.SALE.REFUNDED", { sale_id: text(object.id) })
          : to("PAYMENT.CAPTURE.REFUNDED", { id: idOf(object.payment_intent) });
      case "charge.dispute.created":
      case "charge.dispute.updated":
      case "charge.dispute.funds_withdrawn":
      case "charge.dispute.funds_reinstated":
      case "charge.dispute.closed": {
        const closed = stripeType === "charge.dispute.closed";
        return to(closed ? "CUSTOMER.DISPUTE.RESOLVED" : "CUSTOMER.DISPUTE.UPDATED", {
          dispute_id: text(object.id),
          status: closed ? "RESOLVED" : "OPEN",
          dispute_outcome: { outcome_code: closed ? DISPUTE_OUTCOME[String(object.status)] ?? String(object.status) : null },
          // A one-time purchase is held by its payment intent, and a monthly one by its charge.
          disputed_transactions: [{ seller_transaction_id: idOf(object.payment_intent) }, { seller_transaction_id: idOf(object.charge) }],
        });
      }
      case "invoice.paid":
        return to("PAYMENT.SALE.COMPLETED", { id: idOf(object.charge), custom_id: subscriptionKey });
      case "invoice.payment_failed":
        return to("BILLING.SUBSCRIPTION.PAYMENT.FAILED", { custom_id: subscriptionKey });
      case "customer.subscription.updated":
      case "customer.subscription.deleted":
        return to("BILLING.SUBSCRIPTION.UPDATED", { custom_id: subscriptionKey });
      default:
        return to(stripeType, {});
    }
  }
}
