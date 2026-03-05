import { NextResponse } from "next/server";
import Stripe from "stripe";

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
      case "charge.succeeded":
        await handleChargeSucceededEvent(stripe, event);
        break;
      case "checkout.session.completed":
      case "checkout.session.async_payment_succeeded":
        await handleCheckoutSessionEvent(stripe, event);
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
