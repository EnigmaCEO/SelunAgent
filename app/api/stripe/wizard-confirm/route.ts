import { NextResponse } from "next/server";
import Stripe from "stripe";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type WizardConfirmRequest = {
  sessionId?: string;
};

type PaymentIntentSnapshot = {
  id: string | null;
  status: Stripe.PaymentIntent.Status | null;
  metadata: Record<string, string>;
};

function getStripeClient() {
  const secretKey = process.env.STRIPE_SECRET_KEY?.trim();
  if (!secretKey) {
    throw new Error("STRIPE_SECRET_KEY is not configured.");
  }

  return new Stripe(secretKey);
}

async function resolvePaymentIntentSnapshot(
  stripe: Stripe,
  session: Stripe.Checkout.Session,
): Promise<PaymentIntentSnapshot> {
  const rawPaymentIntent = session.payment_intent;
  if (!rawPaymentIntent) {
    return {
      id: null,
      status: null,
      metadata: {},
    };
  }

  if (typeof rawPaymentIntent !== "string") {
    return {
      id: rawPaymentIntent.id,
      status: rawPaymentIntent.status ?? null,
      metadata: rawPaymentIntent.metadata ?? {},
    };
  }

  const paymentIntent = await stripe.paymentIntents.retrieve(rawPaymentIntent);
  return {
    id: paymentIntent.id,
    status: paymentIntent.status,
    metadata: paymentIntent.metadata ?? {},
  };
}

export async function POST(req: Request): Promise<NextResponse> {
  let payload: WizardConfirmRequest;

  try {
    payload = (await req.json()) as WizardConfirmRequest;
  } catch {
    return NextResponse.json({ success: false, error: "Invalid card confirmation payload." }, { status: 400 });
  }

  const sessionId = payload.sessionId?.trim();
  if (!sessionId) {
    return NextResponse.json({ success: false, error: "Stripe sessionId is required." }, { status: 400 });
  }

  try {
    const stripe = getStripeClient();
    const session = await stripe.checkout.sessions.retrieve(sessionId, {
      expand: ["payment_intent"],
    });

    if (session.metadata?.source !== "selun_wizard") {
      return NextResponse.json({ success: false, error: "Stripe session is not a Selun wizard checkout." }, { status: 400 });
    }

    if (session.status !== "complete") {
      return NextResponse.json(
        { success: false, error: "Stripe checkout is not complete yet." },
        { status: 409 },
      );
    }

    const paymentIntent = await resolvePaymentIntentSnapshot(stripe, session);
    const webhookPaidAt = paymentIntent.metadata.selun_webhook_payment_succeeded_at?.trim() ?? "";
    const isPaid =
      session.payment_status === "paid" ||
      paymentIntent.status === "succeeded" ||
      Boolean(webhookPaidAt);

    if (!isPaid) {
      const pendingReason =
        paymentIntent.status === "processing"
          ? "Payment is processing with Stripe. We will continue automatically once it succeeds."
          : "Waiting for Stripe to confirm payment.";

      return NextResponse.json({
        success: true,
        status: "pending",
        pendingReason,
        transactionId: paymentIntent.id || session.id,
        decisionId: session.metadata?.decisionId || session.id,
      });
    }

    const includeReport = session.metadata?.includeCertifiedDecisionRecord === "true";
    const chargedAmountUsd = Number(session.amount_total ?? 0) / 100;

    return NextResponse.json({
      success: true,
      status: "paid",
      transactionId: paymentIntent.id || session.id,
      decisionId: session.metadata?.decisionId || session.id,
      agentNote: includeReport
        ? "Stripe card payment confirmed for Selun allocation with certified report."
        : "Stripe card payment confirmed for Selun allocation.",
      chargedAmountUsdc: chargedAmountUsd.toFixed(2),
      certifiedDecisionRecordPurchased: includeReport,
      paymentMethod: "card",
      freeCodeApplied: false,
      riskMode: session.metadata?.riskMode,
      investmentHorizon: session.metadata?.investmentHorizon,
      portfolioSegment: session.metadata?.portfolioSegment,
      resultEmail: session.metadata?.resultEmail || "",
    });
  } catch (error) {
    return NextResponse.json(
      { success: false, error: error instanceof Error ? error.message : "Failed to confirm card payment." },
      { status: 500 },
    );
  }
}
