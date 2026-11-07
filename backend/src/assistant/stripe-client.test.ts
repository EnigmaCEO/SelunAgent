import assert from "node:assert/strict";
import test from "node:test";
import { PayPalApiError } from "./paypal-client";
import { readStripeConfig, StripeClient } from "./stripe-client";

type Call = { url: string; init: RequestInit };

function client(replies: unknown[], status = 200) {
  const calls: Call[] = [];
  const fetchImpl = (async (url: string, init: RequestInit) => {
    calls.push({ url, init });
    return new Response(JSON.stringify(replies.shift() ?? {}), { status });
  }) as unknown as typeof fetch;
  return { api: new StripeClient({ secretKey: "sk_test_x" }, fetchImpl), calls };
}

const paidSession = {
  id: "cs_1",
  status: "complete",
  payment_status: "paid",
  client_reference_id: "pc-1",
  amount_total: 50,
  currency: "usd",
  customer: "cus_1",
  payment_intent: { id: "pi_1", status: "succeeded", amount_received: 50, currency: "usd", latest_charge: { id: "ch_1", refunded: false, amount_refunded: 0 } },
};

test("a live Stripe key stays off unless live payments are turned on", () => {
  assert.equal(readStripeConfig({ STRIPE_SECRET_KEY: "sk_live_x" }), null);
  assert.equal(readStripeConfig({})?.env, undefined);
  assert.equal(readStripeConfig({ STRIPE_SECRET_KEY: "sk_test_x" })?.env, "sandbox");
  assert.equal(readStripeConfig({ STRIPE_SECRET_KEY: "sk_live_x", STRIPE_ENV: "live" })?.env, "live");
});

test("an order is a checkout session at the server's price, tied to the purchase", async () => {
  const { api, calls } = client([{ id: "cs_1", url: "https://checkout.stripe.test/cs_1" }]);
  const order = await api.createOrder({ purchaseId: "pc-1", amount: "0.50", description: "Selun Scorecard", returnUrl: "http://x/?checkout=return", cancelUrl: "http://x/?checkout=cancel" });
  assert.deepEqual(order, { orderId: "cs_1", approveUrl: "https://checkout.stripe.test/cs_1" });
  const body = new URLSearchParams(String(calls[0].init.body));
  assert.equal(body.get("mode"), "payment");
  assert.equal(body.get("client_reference_id"), "pc-1");
  assert.equal(body.get("line_items[0][price_data][unit_amount]"), "50");
  assert.equal(body.get("line_items[0][price_data][currency]"), "usd");
});

test("an unpaid session has no capture, and a paid one reports Stripe's own amounts", async () => {
  const unpaid = client([{ id: "cs_1", status: "open", payment_status: "unpaid", client_reference_id: "pc-1", amount_total: 50, currency: "usd" }]);
  const open = await unpaid.api.getOrder("cs_1");
  assert.equal(open.status, "CREATED");
  assert.equal(open.captureId, null);

  const paid = await client([paidSession]).api.getOrder("cs_1");
  assert.equal(paid.status, "COMPLETED");
  assert.deepEqual([paid.captureId, paid.captureStatus, paid.captureAmount, paid.captureCurrency, paid.customId], ["pi_1", "COMPLETED", "0.50", "USD", "pc-1"]);
});

test("refunds and disputes read back as a lost payment", async () => {
  const refunded = await client([{ status: "succeeded", amount_received: 50, currency: "usd", latest_charge: { refunded: true } }]).api.getCapture("pi_1");
  assert.equal(refunded.status, "REFUNDED");
  const disputed = await client([{ status: "succeeded", amount_received: 50, currency: "usd", latest_charge: { disputed: true } }]).api.getCapture("pi_1");
  assert.equal(disputed.status, "REVERSED");
  const partial = await client([{ status: "succeeded", amount_received: 50, currency: "usd", latest_charge: { amount_refunded: 10 } }]).api.getCapture("pi_1");
  assert.equal(partial.status, "PARTIALLY_REFUNDED");
});

test("a refund is idempotent on the request ID", async () => {
  const { api, calls } = client([{ id: "re_1", status: "succeeded" }]);
  assert.deepEqual(await api.refundCapture("pi_1", "refund-pc-1"), { refundId: "re_1", status: "COMPLETED" });
  assert.equal((calls[0].init.headers as Record<string, string>)["idempotency-key"], "refund-pc-1");
  assert.equal(new URLSearchParams(String(calls[0].init.body)).get("payment_intent"), "pi_1");
});

test("a subscription's status and monthly payments come from Stripe", async () => {
  const session = {
    id: "cs_2",
    status: "complete",
    client_reference_id: "ps-1",
    customer: "cus_1",
    customer_details: { email: "buyer@example.com" },
    subscription: { id: "sub_1", status: "active", current_period_end: 1_800_000_000, items: { data: [{ price: { id: "price_1" } }] } },
  };
  const remote = await client([session]).api.getSubscription("cs_2");
  assert.deepEqual([remote.status, remote.planId, remote.customId, remote.subscriberEmail], ["ACTIVE", "price_1", "ps-1", "buyer@example.com"]);

  const invoice = { amount_paid: 999, currency: "usd", created: 1_700_000_000, status_transitions: { paid_at: 1_700_000_000 }, charge: { id: "ch_9", status: "succeeded" } };
  const sales = await client([session, { data: [invoice] }]).api.listSubscriptionTransactions("cs_2", "2023-01-01T00:00:00.000Z", "2030-01-01T00:00:00.000Z");
  assert.deepEqual(sales.map((sale) => [sale.id, sale.status, sale.amount, sale.currency]), [["ch_9", "COMPLETED", "9.99", "USD"]]);
});

test("cancelling a cancelled subscription changes nothing", async () => {
  const { api, calls } = client([{ id: "cs_2", subscription: { id: "sub_1", status: "canceled" } }]);
  await api.cancelSubscription("cs_2");
  assert.equal(calls.length, 1);
});

test("Stripe's error text never leaves the client", async () => {
  const { api } = client([{ error: { code: "amount_too_small", message: "secret detail" } }], 400);
  await assert.rejects(() => api.getCapture("pi_1"), (error: unknown) => {
    assert.ok(error instanceof PayPalApiError);
    assert.equal(error.issue, "amount_too_small");
    assert.doesNotMatch(error.message, /secret detail/);
    return true;
  });
});
