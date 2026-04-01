import { NextResponse } from "next/server";
import Stripe from "stripe";
import { ensureStripeWizardPhase1Started } from "@/app/api/stripe/_lib/fulfillment";
import { trackReferralConversionOnBackend } from "@/app/lib/referral-backend";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function getStripeClient() {
  const secretKey = process.env.STRIPE_SECRET_KEY?.trim();
  if (!secretKey) {
    throw new Error("STRIPE_SECRET_KEY is not configured.");
  }

  return new Stripe(secretKey);
}

function getWebhookSecret() {
  const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET?.trim();
  if (!webhookSecret) {
    throw new Error("STRIPE_WEBHOOK_SECRET is not configured.");
  }

  return webhookSecret;
}

function resolvePaymentIntentUserId(paymentIntent: Stripe.PaymentIntent): string {
  return (
    paymentIntent.metadata?.referralUserId?.trim() ||
    paymentIntent.metadata?.resultEmail?.trim()?.toLowerCase() ||
    paymentIntent.receipt_email?.trim()?.toLowerCase() ||
    paymentIntent.customer?.toString() ||
    paymentIntent.id
  );
}

function resolveSessionUserId(session: Stripe.Checkout.Session, paymentIntent?: Stripe.PaymentIntent): string {
  return (
    session.metadata?.referralUserId?.trim() ||
    paymentIntent?.metadata?.referralUserId?.trim() ||
    session.metadata?.resultEmail?.trim()?.toLowerCase() ||
    session.customer_details?.email?.trim()?.toLowerCase() ||
    paymentIntent?.metadata?.resultEmail?.trim()?.toLowerCase() ||
    paymentIntent?.receipt_email?.trim()?.toLowerCase() ||
    session.customer?.toString() ||
    paymentIntent?.id ||
    session.id
  );
}

async function trackWizardReferralConversion(params: {
  referralCode: string | null | undefined;
  transactionId: string | null | undefined;
  amountCents: number | null | undefined;
  allocationAmountCents?: number | null | undefined;
  reportAmountCents?: number | null | undefined;
  paymentReference?: string | null | undefined;
  userId?: string | null | undefined;
  source?: string | null | undefined;
  status?: "pending" | "confirmed" | "paid" | "rejected";
}) {
  const amountCents = typeof params.amountCents === "number" && Number.isFinite(params.amountCents)
    ? params.amountCents
    : null;
  if (amountCents === null) {
    return;
  }

  await trackReferralConversionOnBackend({
    referralCode: params.referralCode,
    transactionId: params.transactionId,
    amountUsd: amountCents / 100,
    allocationAmountUsd:
      typeof params.allocationAmountCents === "number" && Number.isFinite(params.allocationAmountCents)
        ? params.allocationAmountCents / 100
        : undefined,
    reportAmountUsd:
      typeof params.reportAmountCents === "number" && Number.isFinite(params.reportAmountCents)
        ? params.reportAmountCents / 100
        : undefined,
    paymentReference: params.paymentReference,
    userId: params.userId,
    source: params.source,
    status: params.status,
  });
}

async function markPaymentIntentFromWebhook(params: {
  stripe: Stripe;
  paymentIntentId: string;
  eventId: string;
  eventType: string;
  chargeId?: string;
}) {
  const metadata: Record<string, string> = {
    selun_webhook_payment_succeeded_at: new Date().toISOString(),
    selun_webhook_event_id: params.eventId,
    selun_webhook_event_type: params.eventType,
  };

  if (params.chargeId) {
    metadata.selun_webhook_charge_id = params.chargeId;
  }

  await params.stripe.paymentIntents.update(params.paymentIntentId, {
    metadata,
  });
}

async function handlePaymentIntentSucceededEvent(stripe: Stripe, event: Stripe.Event) {
  const paymentIntent = event.data.object as Stripe.PaymentIntent;
  if (!paymentIntent.id || paymentIntent.metadata?.source !== "selun_wizard") return;

  await markPaymentIntentFromWebhook({
    stripe,
    paymentIntentId: paymentIntent.id,
    eventId: event.id,
    eventType: event.type,
  });
  await trackWizardReferralConversion({
    referralCode: paymentIntent.metadata?.referralCode,
    transactionId: paymentIntent.id,
    amountCents: paymentIntent.amount_received || paymentIntent.amount,
    allocationAmountCents: 1900,
    reportAmountCents: Math.max(0, (paymentIntent.amount_received || paymentIntent.amount) - 1900),
    paymentReference: paymentIntent.id,
    userId: resolvePaymentIntentUserId(paymentIntent),
    source: "stripe_webhook",
    status: "confirmed",
  });

  await ensureStripeWizardPhase1Started({
    decisionId: paymentIntent.metadata?.decisionId || paymentIntent.id,
    riskMode: paymentIntent.metadata?.riskMode,
    investmentHorizon: paymentIntent.metadata?.investmentHorizon,
    portfolioSegment: paymentIntent.metadata?.portfolioSegment,
  });
}

async function handleChargeSucceededEvent(stripe: Stripe, event: Stripe.Event) {
  const charge = event.data.object as Stripe.Charge;
  const rawPaymentIntent = charge.payment_intent;
  const paymentIntentId =
    typeof rawPaymentIntent === "string" ? rawPaymentIntent : rawPaymentIntent?.id;
  if (!paymentIntentId) return;

  const paymentIntent = await stripe.paymentIntents.retrieve(paymentIntentId);
  if (paymentIntent.metadata?.source !== "selun_wizard") return;

  await markPaymentIntentFromWebhook({
    stripe,
    paymentIntentId,
    eventId: event.id,
    eventType: event.type,
    chargeId: charge.id,
  });
  await trackWizardReferralConversion({
    referralCode: paymentIntent.metadata?.referralCode,
    transactionId: paymentIntent.id || paymentIntentId,
    amountCents: charge.amount || paymentIntent.amount_received || paymentIntent.amount,
    allocationAmountCents: 1900,
    reportAmountCents: Math.max(0, (charge.amount || paymentIntent.amount_received || paymentIntent.amount) - 1900),
    paymentReference: paymentIntent.id || paymentIntentId,
    userId: resolvePaymentIntentUserId(paymentIntent),
    source: "stripe_webhook",
    status: "confirmed",
  });

  await ensureStripeWizardPhase1Started({
    decisionId: paymentIntent.metadata?.decisionId || paymentIntentId,
    riskMode: paymentIntent.metadata?.riskMode,
    investmentHorizon: paymentIntent.metadata?.investmentHorizon,
    portfolioSegment: paymentIntent.metadata?.portfolioSegment,
  });
}

async function syncReferralFromRefundedCharge(
  stripe: Stripe,
  charge: Stripe.Charge,
  source: string,
) {
  const rawPaymentIntent = charge.payment_intent;
  const paymentIntentId =
    typeof rawPaymentIntent === "string" ? rawPaymentIntent : rawPaymentIntent?.id;
  if (!paymentIntentId) return;

  const paymentIntent = await stripe.paymentIntents.retrieve(paymentIntentId);
  if (paymentIntent.metadata?.source !== "selun_wizard") return;

  const grossAmountCents = charge.amount || paymentIntent.amount_received || paymentIntent.amount;
  const refundedAmountCents =
    typeof charge.amount_refunded === "number" && Number.isFinite(charge.amount_refunded)
      ? charge.amount_refunded
      : 0;
  const retainedAmountCents = Math.max(0, grossAmountCents - refundedAmountCents);
  const isFullyRefunded = Boolean(charge.refunded) || retainedAmountCents <= 0;

  await trackWizardReferralConversion({
    referralCode: paymentIntent.metadata?.referralCode,
    transactionId: paymentIntent.id || paymentIntentId,
    amountCents: isFullyRefunded ? grossAmountCents : retainedAmountCents,
    paymentReference: paymentIntent.id || paymentIntentId,
    userId: resolvePaymentIntentUserId(paymentIntent),
    source,
    status: isFullyRefunded ? "rejected" : "confirmed",
  });
}

async function handleChargeRefundedEvent(stripe: Stripe, event: Stripe.Event) {
  const charge = event.data.object as Stripe.Charge;
  await syncReferralFromRefundedCharge(stripe, charge, "stripe_webhook_refund");
}

async function handleRefundEvent(stripe: Stripe, event: Stripe.Event) {
  const refund = event.data.object as Stripe.Refund;
  if (refund.status !== "succeeded") return;

  const rawChargeId = refund.charge;
  const chargeId = typeof rawChargeId === "string" ? rawChargeId : rawChargeId?.id;
  if (!chargeId) return;

  const charge = await stripe.charges.retrieve(chargeId);
  await syncReferralFromRefundedCharge(stripe, charge, "stripe_webhook_refund");
}

async function handleCheckoutSessionEvent(stripe: Stripe, event: Stripe.Event) {
  const session = event.data.object as Stripe.Checkout.Session;
  if (session.metadata?.source !== "selun_wizard") return;

  if (event.type === "checkout.session.completed" && session.payment_status !== "paid") {
    return;
  }

  const rawPaymentIntent = session.payment_intent;
  const paymentIntentId =
    typeof rawPaymentIntent === "string" ? rawPaymentIntent : rawPaymentIntent?.id;
  if (!paymentIntentId) return;

  await markPaymentIntentFromWebhook({
    stripe,
    paymentIntentId,
    eventId: event.id,
    eventType: event.type,
  });
  await trackWizardReferralConversion({
    referralCode: session.metadata?.referralCode,
    transactionId: paymentIntentId,
    amountCents: session.amount_total,
    allocationAmountCents: 1900,
    reportAmountCents: Math.max(0, (session.amount_total ?? 0) - 1900),
    paymentReference: paymentIntentId,
    userId: resolveSessionUserId(session),
    source: "stripe_webhook",
    status: "confirmed",
  });

  await ensureStripeWizardPhase1Started({
    decisionId: session.metadata?.decisionId || paymentIntentId,
    riskMode: session.metadata?.riskMode,
    investmentHorizon: session.metadata?.investmentHorizon,
    portfolioSegment: session.metadata?.portfolioSegment,
  });
}

async function handlePaymentIntentFailedEvent(_stripe: Stripe, event: Stripe.Event) {
  const paymentIntent = event.data.object as Stripe.PaymentIntent;
  if (!paymentIntent.id || paymentIntent.metadata?.source !== "selun_wizard") return;

  await trackWizardReferralConversion({
    referralCode: paymentIntent.metadata?.referralCode,
    transactionId: paymentIntent.id,
    amountCents: paymentIntent.amount || paymentIntent.amount_received,
    userId: resolvePaymentIntentUserId(paymentIntent),
    source: "stripe_webhook",
    status: "rejected",
  });
}

async function handleCheckoutSessionFailedEvent(stripe: Stripe, event: Stripe.Event) {
  const session = event.data.object as Stripe.Checkout.Session;
  if (session.metadata?.source !== "selun_wizard") return;

  const rawPaymentIntent = session.payment_intent;
  const paymentIntentId =
    typeof rawPaymentIntent === "string" ? rawPaymentIntent : rawPaymentIntent?.id;
  if (!paymentIntentId) return;

  const paymentIntent = await stripe.paymentIntents.retrieve(paymentIntentId);
  await trackWizardReferralConversion({
    referralCode: session.metadata?.referralCode || paymentIntent.metadata?.referralCode,
    transactionId: paymentIntentId,
    amountCents: session.amount_total ?? paymentIntent.amount ?? paymentIntent.amount_received,
    userId: resolveSessionUserId(session, paymentIntent),
    source: "stripe_webhook",
    status: "rejected",
  });
}

export async function POST(req: Request): Promise<NextResponse> {
  const signature = req.headers.get("stripe-signature");
  if (!signature) {
    return NextResponse.json({ success: false, error: "Missing Stripe signature." }, { status: 400 });
  }

  try {
    const stripe = getStripeClient();
    const webhookSecret = getWebhookSecret();
    const body = await req.text();

    let event: Stripe.Event;
    try {
      event = stripe.webhooks.constructEvent(body, signature, webhookSecret);
    } catch {
      return NextResponse.json({ success: false, error: "Invalid Stripe webhook signature." }, { status: 400 });
    }

    switch (event.type) {
      case "payment_intent.succeeded":
        await handlePaymentIntentSucceededEvent(stripe, event);
        break;
      case "payment_intent.payment_failed":
        await handlePaymentIntentFailedEvent(stripe, event);
        break;
      case "charge.succeeded":
        await handleChargeSucceededEvent(stripe, event);
        break;
      case "charge.refunded":
        await handleChargeRefundedEvent(stripe, event);
        break;
      case "charge.refund.updated":
      case "refund.created":
      case "refund.updated":
        await handleRefundEvent(stripe, event);
        break;
      case "checkout.session.completed":
      case "checkout.session.async_payment_succeeded":
        await handleCheckoutSessionEvent(stripe, event);
        break;
      case "checkout.session.async_payment_failed":
      case "checkout.session.expired":
        await handleCheckoutSessionFailedEvent(stripe, event);
        break;
      default:
        break;
    }

    return NextResponse.json({ success: true, received: true });
  } catch (error) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : "Stripe webhook handling failed.",
      },
      { status: 500 },
    );
  }
}
