/**
 * Server-side PayPal REST client (native fetch, no SDK). Credentials and access
 * tokens stay in this module and never appear in errors. Nothing from the browser
 * is trusted: amounts, statuses, and payer details are read back from PayPal.
 */

export type PayPalConfig = {
  env: "sandbox" | "live";
  baseUrl: string;
  clientId: string;
  clientSecret: string;
  webhookId: string | null;
  returnBaseUrl: string;
  reportPriceUsd: string;
  subscriptionPriceUsd: string;
};

function readPrice(raw: string | undefined, fallback: string): string {
  const value = raw?.trim();
  return value && /^\d+\.\d{2}$/.test(value) && Number(value) > 0 ? value : fallback;
}

export function readPayPalConfig(env: NodeJS.ProcessEnv = process.env): PayPalConfig | null {
  const mode = env.PAYPAL_ENV?.trim();
  const clientId = env.PAYPAL_CLIENT_ID?.trim();
  const clientSecret = env.PAYPAL_CLIENT_SECRET?.trim();
  if ((mode !== "sandbox" && mode !== "live") || !clientId || !clientSecret) return null;
  return {
    env: mode,
    baseUrl: mode === "live" ? "https://api-m.paypal.com" : "https://api-m.sandbox.paypal.com",
    clientId,
    clientSecret,
    webhookId: env.PAYPAL_WEBHOOK_ID?.trim() || null,
    returnBaseUrl: (env.PAYPAL_RETURN_BASE_URL?.trim() || "http://localhost:3000").replace(/\/+$/, ""),
    reportPriceUsd: readPrice(env.PAYPAL_REPORT_PRICE_USD, "4.99"),
    subscriptionPriceUsd: readPrice(env.PAYPAL_SUBSCRIPTION_PRICE_USD, "9.99"),
  };
}

export class PayPalApiError extends Error {
  constructor(
    readonly status: number,
    readonly issue: string,
    message: string,
  ) {
    super(message);
    this.name = "PayPalApiError";
  }
}

/** What the provider says it kept and paid out. For internal reporting only: customers never see it. */
export type ProcessorFee = { fee: string; net: string; currency: string };

export type PayPalOrder = {
  orderId: string;
  status: string;
  customId: string | null;
  amount: string | null;
  currency: string | null;
  payerId: string | null;
  /** Only ever offered back to whoever holds this purchase, to confirm where a report goes. */
  payerEmail?: string | null;
  processorFee?: ProcessorFee | null;
  captureId: string | null;
  captureStatus: string | null;
  captureAmount: string | null;
  captureCurrency: string | null;
};

export type PayPalCapture = { captureId: string; status: string; amount: string | null; currency: string | null; processorFee?: ProcessorFee | null };

export type PayPalSubscription = {
  subscriptionId: string;
  status: string;
  customId: string | null;
  planId: string | null;
  payerId: string | null;
  subscriberEmail: string | null;
  nextBillingTime: string | null;
};

export type PayPalSaleTransaction = { id: string; status: string; amount: string | null; currency: string | null; time: string; processorFee?: ProcessorFee | null };

export type PayPalDispute = { disputeId: string; status: string; outcome: string | null; transactionIds: string[] };

export type WebhookHeaders = {
  authAlgo: string;
  certUrl: string;
  transmissionId: string;
  transmissionSig: string;
  transmissionTime: string;
};

/** Everything the commerce service needs from PayPal. Tests supply a fake. */
export interface PayPalApi {
  createOrder(input: { purchaseId: string; amount: string; description: string; returnUrl: string; cancelUrl: string }): Promise<{ orderId: string; approveUrl: string }>;
  getOrder(orderId: string): Promise<PayPalOrder>;
  captureOrder(orderId: string, requestId: string): Promise<PayPalOrder>;
  getCapture(captureId: string): Promise<PayPalCapture>;
  refundCapture(captureId: string, requestId: string, note: string): Promise<{ refundId: string; status: string }>;
  createPlan(priceUsd: string): Promise<string>;
  createSubscription(input: { planId: string; customId: string; returnUrl: string; cancelUrl: string }): Promise<{ subscriptionId: string; approveUrl: string }>;
  getSubscription(subscriptionId: string): Promise<PayPalSubscription>;
  listSubscriptionTransactions(subscriptionId: string, start: string, end: string): Promise<PayPalSaleTransaction[]>;
  cancelSubscription(subscriptionId: string, reason: string): Promise<void>;
  refundSale(saleId: string, requestId: string): Promise<{ refundId: string; status: string }>;
  getDispute(disputeId: string): Promise<PayPalDispute>;
  verifyWebhook(headers: WebhookHeaders, rawBody: string, webhookId: string): Promise<boolean>;
}

type Json = Record<string, unknown>;

function record(value: unknown): Json {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Json) : {};
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function approveLink(payload: Json): string {
  const links = Array.isArray(payload.links) ? payload.links.map(record) : [];
  const link = links.find((entry) => entry.rel === "approve" || entry.rel === "payer-action");
  const href = text(link?.href);
  if (!href) throw new PayPalApiError(502, "NO_APPROVE_LINK", "PayPal did not return an approval link.");
  return href;
}

function processorFeeOf(fee: Json, net: Json): ProcessorFee | null {
  const feeValue = text(fee.value);
  const netValue = text(net.value);
  return feeValue && netValue ? { fee: feeValue, net: netValue, currency: text(fee.currency_code) ?? "USD" } : null;
}

function captureFee(capture: Json): ProcessorFee | null {
  const breakdown = record(capture.seller_receivable_breakdown);
  return processorFeeOf(record(breakdown.paypal_fee), record(breakdown.net_amount));
}

export function parseOrder(payload: Json): PayPalOrder {
  const unit = record(Array.isArray(payload.purchase_units) ? payload.purchase_units[0] : null);
  const amount = record(unit.amount);
  const captures = record(unit.payments).captures;
  const capture = record(Array.isArray(captures) ? captures[0] : null);
  const captureAmount = record(capture.amount);
  const payer = record(payload.payer);
  const paypalSource = record(record(payload.payment_source).paypal);
  return {
    orderId: text(payload.id) ?? "",
    status: text(payload.status) ?? "UNKNOWN",
    customId: text(unit.custom_id) ?? text(capture.custom_id),
    amount: text(amount.value),
    currency: text(amount.currency_code),
    payerId: text(payer.payer_id) ?? text(paypalSource.account_id),
    payerEmail: text(payer.email_address) ?? text(paypalSource.email_address),
    processorFee: captureFee(capture),
    captureId: text(capture.id),
    captureStatus: text(capture.status),
    captureAmount: text(captureAmount.value),
    captureCurrency: text(captureAmount.currency_code),
  };
}

const PLAN_PRODUCT = { name: "Selun Monthly Intelligence", description: "One comprehensive Selun intelligence report per paid billing cycle.", type: "SERVICE", category: "SOFTWARE" };

export class PayPalClient implements PayPalApi {
  private token: { value: string; expiresAt: number } | null = null;

  constructor(private readonly config: PayPalConfig) {}

  private async accessToken(): Promise<string> {
    if (this.token && this.token.expiresAt > Date.now() + 60_000) return this.token.value;
    const response = await fetch(`${this.config.baseUrl}/v1/oauth2/token`, {
      method: "POST",
      headers: {
        Authorization: `Basic ${Buffer.from(`${this.config.clientId}:${this.config.clientSecret}`).toString("base64")}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: "grant_type=client_credentials",
      signal: AbortSignal.timeout(20_000),
    });
    const body = record(await response.json().catch(() => null));
    const value = text(body.access_token);
    if (!response.ok || !value) throw new PayPalApiError(response.status, "AUTH_FAILED", "PayPal rejected Selun's credentials.");
    this.token = { value, expiresAt: Date.now() + Number(body.expires_in ?? 0) * 1000 };
    return value;
  }

  private async call(method: "GET" | "POST", path: string, body?: unknown, requestId?: string): Promise<Json> {
    const response = await fetch(`${this.config.baseUrl}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${await this.accessToken()}`,
        Accept: "application/json",
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
        ...(requestId ? { "PayPal-Request-Id": requestId } : {}),
        Prefer: "return=representation",
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(30_000),
    });
    const payload = record(await response.json().catch(() => null));
    if (!response.ok) {
      const details = Array.isArray(payload.details) ? payload.details.map(record) : [];
      const issue = text(details[0]?.issue) ?? text(payload.name) ?? `HTTP_${response.status}`;
      throw new PayPalApiError(response.status, issue, `PayPal ${method} ${path.split("?")[0]} failed: ${issue}`);
    }
    return payload;
  }

  async createOrder(input: { purchaseId: string; amount: string; description: string; returnUrl: string; cancelUrl: string }) {
    const payload = await this.call(
      "POST",
      "/v2/checkout/orders",
      {
        intent: "CAPTURE",
        purchase_units: [{
          custom_id: input.purchaseId,
          description: input.description,
          amount: { currency_code: "USD", value: input.amount },
        }],
        payment_source: {
          paypal: {
            experience_context: {
              brand_name: "Selun",
              user_action: "PAY_NOW",
              shipping_preference: "NO_SHIPPING",
              return_url: input.returnUrl,
              cancel_url: input.cancelUrl,
            },
          },
        },
      },
      `selun-order-${input.purchaseId}`,
    );
    return { orderId: text(payload.id) ?? "", approveUrl: approveLink(payload) };
  }

  async getOrder(orderId: string) {
    return parseOrder(await this.call("GET", `/v2/checkout/orders/${encodeURIComponent(orderId)}`));
  }

  async captureOrder(orderId: string, requestId: string) {
    return parseOrder(await this.call("POST", `/v2/checkout/orders/${encodeURIComponent(orderId)}/capture`, {}, requestId));
  }

  async getCapture(captureId: string) {
    const payload = await this.call("GET", `/v2/payments/captures/${encodeURIComponent(captureId)}`);
    const amount = record(payload.amount);
    return { captureId, status: text(payload.status) ?? "UNKNOWN", amount: text(amount.value), currency: text(amount.currency_code), processorFee: captureFee(payload) };
  }

  async refundCapture(captureId: string, requestId: string, note: string) {
    const payload = await this.call("POST", `/v2/payments/captures/${encodeURIComponent(captureId)}/refund`, { note_to_payer: note.slice(0, 255) }, requestId);
    return { refundId: text(payload.id) ?? "", status: text(payload.status) ?? "UNKNOWN" };
  }

  async createPlan(priceUsd: string) {
    const product = await this.call("POST", "/v1/catalogs/products", PLAN_PRODUCT, `selun-product-${priceUsd}`);
    const plan = await this.call(
      "POST",
      "/v1/billing/plans",
      {
        product_id: product.id,
        name: `Selun Monthly Intelligence $${priceUsd}`,
        description: PLAN_PRODUCT.description,
        status: "ACTIVE",
        billing_cycles: [{
          frequency: { interval_unit: "MONTH", interval_count: 1 },
          tenure_type: "REGULAR",
          sequence: 1,
          total_cycles: 0,
          pricing_scheme: { fixed_price: { value: priceUsd, currency_code: "USD" } },
        }],
        payment_preferences: { auto_bill_outstanding: true, payment_failure_threshold: 1 },
      },
      `selun-plan-${priceUsd}`,
    );
    const planId = text(plan.id);
    if (!planId) throw new PayPalApiError(502, "NO_PLAN_ID", "PayPal did not return a plan.");
    return planId;
  }

  async createSubscription(input: { planId: string; customId: string; returnUrl: string; cancelUrl: string }) {
    const payload = await this.call(
      "POST",
      "/v1/billing/subscriptions",
      {
        plan_id: input.planId,
        custom_id: input.customId,
        application_context: {
          brand_name: "Selun",
          user_action: "SUBSCRIBE_NOW",
          shipping_preference: "NO_SHIPPING",
          return_url: input.returnUrl,
          cancel_url: input.cancelUrl,
        },
      },
      `selun-subscription-${input.customId}`,
    );
    return { subscriptionId: text(payload.id) ?? "", approveUrl: approveLink(payload) };
  }

  async getSubscription(subscriptionId: string) {
    const payload = await this.call("GET", `/v1/billing/subscriptions/${encodeURIComponent(subscriptionId)}`);
    const subscriber = record(payload.subscriber);
    return {
      subscriptionId,
      status: text(payload.status) ?? "UNKNOWN",
      customId: text(payload.custom_id),
      planId: text(payload.plan_id),
      payerId: text(subscriber.payer_id),
      subscriberEmail: text(subscriber.email_address),
      nextBillingTime: text(record(payload.billing_info).next_billing_time),
    };
  }

  async listSubscriptionTransactions(subscriptionId: string, start: string, end: string) {
    const query = new URLSearchParams({ start_time: start, end_time: end });
    // PayPal answers 404 for a subscription with no payments yet, which isn't a failure.
    const payload = await this.call("GET", `/v1/billing/subscriptions/${encodeURIComponent(subscriptionId)}/transactions?${query}`).catch((error: unknown) => {
      if (error instanceof PayPalApiError && error.status === 404) return {} as Json;
      throw error;
    });
    const transactions = Array.isArray(payload.transactions) ? payload.transactions.map(record) : [];
    return transactions.map((entry) => {
      const breakdown = record(entry.amount_with_breakdown);
      const gross = record(breakdown.gross_amount);
      return {
        id: text(entry.id) ?? "",
        status: text(entry.status) ?? "UNKNOWN",
        amount: text(gross.value),
        currency: text(gross.currency_code),
        time: text(entry.time) ?? "",
        processorFee: processorFeeOf(record(breakdown.fee_amount), record(breakdown.net_amount)),
      };
    });
  }

  async cancelSubscription(subscriptionId: string, reason: string) {
    await this.call("POST", `/v1/billing/subscriptions/${encodeURIComponent(subscriptionId)}/cancel`, { reason: reason.slice(0, 128) });
  }

  async refundSale(saleId: string, requestId: string) {
    const payload = await this.call("POST", `/v1/payments/sale/${encodeURIComponent(saleId)}/refund`, {}, requestId);
    return { refundId: text(payload.id) ?? "", status: (text(payload.state) ?? text(payload.status) ?? "UNKNOWN").toUpperCase() };
  }

  async getDispute(disputeId: string) {
    const payload = await this.call("GET", `/v1/customer/disputes/${encodeURIComponent(disputeId)}`);
    const transactions = Array.isArray(payload.disputed_transactions) ? payload.disputed_transactions.map(record) : [];
    return {
      disputeId,
      status: text(payload.status) ?? "UNKNOWN",
      outcome: text(record(payload.dispute_outcome).outcome_code),
      transactionIds: transactions.map((entry) => text(entry.seller_transaction_id)).filter((id): id is string => Boolean(id)),
    };
  }

  /** The raw body goes in unparsed: PayPal checks a signature over the exact bytes it sent. */
  async verifyWebhook(headers: WebhookHeaders, rawBody: string, webhookId: string) {
    const envelope = JSON.stringify({
      auth_algo: headers.authAlgo,
      cert_url: headers.certUrl,
      transmission_id: headers.transmissionId,
      transmission_sig: headers.transmissionSig,
      transmission_time: headers.transmissionTime,
      webhook_id: webhookId,
      webhook_event: "__EVENT__",
    }).replace('"__EVENT__"', () => rawBody);
    const response = await fetch(`${this.config.baseUrl}/v1/notifications/verify-webhook-signature`, {
      method: "POST",
      headers: { Authorization: `Bearer ${await this.accessToken()}`, "Content-Type": "application/json" },
      body: envelope,
      signal: AbortSignal.timeout(20_000),
    });
    const payload = record(await response.json().catch(() => null));
    return response.ok && payload.verification_status === "SUCCESS";
  }
}
