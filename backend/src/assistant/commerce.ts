import type { ScreeningSnapshot } from "./scorecard-preflight";
import { formatUnits, parseUnits } from "viem";
import type { ResultDocument } from "./result-presenter";

/**
 * Commerce boundaries for the assistant.
 *
 * x402 pays for individual services with a wallet. PayPal pays for the comprehensive
 * report and the monthly subscription (paypal-commerce.service.ts). Each keeps its own
 * payment evidence: a PayPal purchase has no wallet address or transaction hash.
 */

export const COMPREHENSIVE_REPORT_ID = "comprehensive_report";
export const MONTHLY_INTELLIGENCE_ID = "monthly_intelligence";

export type PayPalPaymentEvidence = {
  provider: string;
  /** Set for providers other than PayPal. PayPal's is left out so existing records hash as they always did. */
  label?: string;
  amount: string;
  currency: "USD";
  orderId: string | null;
  captureId: string | null;
  saleId: string | null;
  subscriptionId: string | null;
  payerId: string | null;
  environment: "sandbox" | "live";
};

export type PaymentRailId = "x402_wallet" | "paypal_once" | "paypal_subscription";

export type PaymentRail = {
  id: PaymentRailId;
  label: string;
  description: string;
  status: "available" | "coming_soon";
};

export function getPaymentRails(paypalAvailable = false): PaymentRail[] {
  const paypalStatus = paypalAvailable ? "available" : "coming_soon";
  return [
    {
      id: "x402_wallet",
      label: "Crypto wallet",
      description: "Pay in USDC with your own wallet. You approve the exact amount, and network fees are covered.",
      status: "available",
    },
    { id: "paypal_once", label: "PayPal", description: "One-time purchase of the comprehensive report.", status: paypalStatus },
    { id: "paypal_subscription", label: "Subscription", description: "One comprehensive report each month through PayPal.", status: paypalStatus },
  ];
}

/**
 * How a customer pays. The card and the chat client work from `flow`, never from a provider's name, so a new
 * hosted checkout is one `CheckoutProvider` passed to the assistant. It then appears on every hosted product.
 */
export type Billing = "once" | "monthly";
export type PaymentFlow = "wallet_signature" | "hosted_checkout";
/** `label` and `detail` fill the method's row, `shortLabel` its button ("Continue with PayPal"), and `note` shows once it's selected. */
export type PaymentOption = { id: string; label: string; shortLabel: string; detail: string; flow: PaymentFlow; currency: "USDC" | "USD"; note: string };

export const WALLET_METHOD = "x402_wallet";

export const WALLET_OPTION: PaymentOption = {
  id: WALLET_METHOD,
  label: "Crypto wallet",
  shortLabel: "wallet",
  detail: "USDC on Base",
  flow: "wallet_signature",
  currency: "USDC",
  note: "Nothing is charged until you approve this exact amount in your wallet. Pays in USDC on Base.",
};

export type CheckoutInput = {
  capabilityId: string;
  title: string;
  body: Record<string, unknown>;
  conversationId: string;
  actionId: string;
  /** Stable for this quote, so a repeated request reuses the same purchase. */
  purchaseKey: string;
  /** The server's price for a one-time purchase. A subscription uses its plan's. */
  amount: string;
  billing: Billing;
  screening?: ScreeningSnapshot | null;
  /** The Selun AI account a completed purchase adds questions to. */
  creditId?: string | null;
};

/**
 * A provider also serves `POST /assistant/<id>/capture` and `/status`, which the client calls on return from
 * checkout, and it must only start a purchase's fulfillment once it has verified the payment itself.
 */
export type CheckoutProvider = {
  id: string;
  /** The name on receipts and in messages. */
  label: string;
  /** What the payment choice shows, when it differs from `label`. */
  choice?: { label: string; shortLabel: string; detail: string };
  /** Left off the card below this, for a provider that refuses smaller charges. */
  minimumUsd?: number;
  note: (billing: Billing) => string;
  start: (input: CheckoutInput) => Promise<{ approveUrl: string; kind: "one_time" | "subscription"; id: string }>;
};

export type SpendLimits = {
  maxActionUsdc: string;
  sessionLimitUsdc: string;
  walletDailyLimitUsdc: string;
};

export type SpendDenial = "action_limit" | "session_limit" | "wallet_daily_limit";

const USDC_DECIMALS = 6;

export function toMicroUsdc(amount: string): bigint {
  return parseUnits(amount as `${number}`, USDC_DECIMALS);
}

export function fromMicroUsdc(amount: bigint): string {
  return formatUnits(amount, USDC_DECIMALS);
}

/**
 * Assistant-side spending guardrail, on top of the x402 routes' own per-wallet
 * request cap. Amounts are integer micro-USDC, so no float rounding. State is
 * in memory and resets on restart. The wallet signature still binds each charge.
 */
export class SpendLedger {
  private readonly session = new Map<string, Map<string, bigint>>();
  private readonly wallet = new Map<string, Map<string, bigint>>();

  constructor(
    private readonly limits: SpendLimits,
    private readonly now: () => Date = () => new Date(),
  ) {}

  private dayKey(payer: string): string {
    return `${this.now().toISOString().slice(0, 10)}:${payer.toLowerCase()}`;
  }

  private static sum(entries: Map<string, bigint> | undefined, exceptActionId: string): bigint {
    let total = 0n;
    for (const [actionId, amount] of entries ?? []) {
      if (actionId !== exceptActionId) total += amount;
    }
    return total;
  }

  /** Checks against limits without reserving anything. */
  check(sessionId: string, actionId: string, priceUsdc: string, payer?: string): SpendDenial | null {
    const price = toMicroUsdc(priceUsdc);
    if (price > toMicroUsdc(this.limits.maxActionUsdc)) return "action_limit";
    if (SpendLedger.sum(this.session.get(sessionId), actionId) + price > toMicroUsdc(this.limits.sessionLimitUsdc)) {
      return "session_limit";
    }
    if (payer && SpendLedger.sum(this.wallet.get(this.dayKey(payer)), actionId) + price > toMicroUsdc(this.limits.walletDailyLimitUsdc)) {
      return "wallet_daily_limit";
    }
    return null;
  }

  reserve(sessionId: string, actionId: string, priceUsdc: string, payer: string): SpendDenial | null {
    const denial = this.check(sessionId, actionId, priceUsdc, payer);
    if (denial) return denial;
    const price = toMicroUsdc(priceUsdc);
    const sessionEntries = this.session.get(sessionId) ?? new Map<string, bigint>();
    sessionEntries.set(actionId, price);
    this.session.set(sessionId, sessionEntries);
    const key = this.dayKey(payer);
    const walletEntries = this.wallet.get(key) ?? new Map<string, bigint>();
    walletEntries.set(actionId, price);
    this.wallet.set(key, walletEntries);
    return null;
  }

  /** Call only when the x402 route confirmed that no funds moved. */
  release(sessionId: string, actionId: string, payer: string): void {
    this.session.get(sessionId)?.delete(actionId);
    this.wallet.get(this.dayKey(payer))?.delete(actionId);
  }

  forgetSession(sessionId: string): void {
    this.session.delete(sessionId);
  }

  sessionSpent(sessionId: string): string {
    return fromMicroUsdc(SpendLedger.sum(this.session.get(sessionId), ""));
  }
}

/**
 * Delivery of one paid result. Live today and stored by report.service.ts. Whichever rail
 * paid, PDF and email come from this frozen document, so PayPal needs no report code of its own.
 */
export type FulfillmentRecord = {
  decisionId: string;
  capabilityId: string;
  frozenAt: string;
  /** The PDF layout it was issued with. Absent on the earliest reports, which use layout 1. */
  reportLayout?: number;
  recordHash: string;
  document: ResultDocument;
  downloads: number;
  emails: { recipientHash: string; status: "sending" | "sent" | "test_recorded"; updatedAt: string }[];
};
