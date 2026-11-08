export type Tone = "positive" | "neutral" | "caution" | "negative";

export type ResultSection = SectionBody & { more?: boolean };

type SectionBody =
  | { kind: "highlight"; label: string; value: string; tone: Tone; detail?: string }
  | { kind: "metrics"; title?: string; items: { label: string; value: string; hint?: string; tone?: Tone; meter?: number }[] }
  | { kind: "table"; title?: string; columns: { key: string; label: string; numeric?: boolean }[]; rows: Record<string, string>[] }
  | { kind: "list"; title?: string; items: string[] }
  | { kind: "note"; text: string }
  | { kind: "heading"; text: string };

export type CoverageItem = { label: string; included: boolean; note: string };

export type ResultDocument = {
  schemaVersion: "selun.result.v1";
  capabilityId: string;
  title: string;
  snapshotAt: string | null;
  sections: ResultSection[];
  coverage?: CoverageItem[];
  notice?: string;
  provenance: {
    source: string;
    endpoint: string;
    decisionId: string;
    amountUsdc: string | null;
    transactionHash: string | null;
    network: string | null;
    explorerUrl: string | null;
    paypal?: { label?: string; amount: string; currency: string; captureId: string | null; saleId: string | null; environment: "sandbox" | "live" };
  };
};

export type PayPalPhase = "awaiting_payment" | "payment_pending" | "running" | "delayed" | "needs_selection" | "completed" | "refunded" | "restricted" | "revoked" | "failed";

/** Kept with the action in this browser. The token is the only way to read the purchase. */
export type PayPalState = {
  /** From the payment provider, to confirm where a one-time report goes. */
  payerEmail?: string | null;
  /** The hosted checkout holding this purchase. */
  method?: string;
  token: string;
  kind: "one_time" | "subscription";
  phase: PayPalPhase;
  message: string | null;
  subscriptionStatus?: "created" | "active" | "suspended" | "cancelled" | "expired";
  nextBillingTime?: string | null;
  reports?: number;
};

export type PayPalStatusView = {
  payerEmail?: string | null;
  kind: "one_time" | "subscription";
  phase: PayPalPhase;
  message: string | null;
  decisionId: string | null;
  document: ResultDocument | null;
  reportToken: string | null;
  explanation: string | null;
  explanationStatus: "none" | "pending" | "ready" | "unavailable";
  status?: PayPalState["subscriptionStatus"];
  nextBillingTime?: string | null;
  reports?: number;
};

export type PaymentRail = {
  id: "x402_wallet" | "paypal_once" | "paypal_subscription";
  label: string;
  description: string;
  status: "available" | "coming_soon";
};

/** Selun AI questions left. Kept on the server: this is only what it reported. */
export type CreditView = { remaining: number; free: number; earned: number; guestQuestions: number; purchaseGrant: number };

export type PaymentOption = { id: string; label: string; shortLabel: string; detail: string; flow: "wallet_signature" | "hosted_checkout"; currency: "USDC" | "USD"; note: string };
export type Billing = "once" | "monthly";

export type ActionState = "awaiting_approval" | "executing" | "completed" | "failed" | "declined";

export type ActionView = {
  id: string;
  capabilityId: string;
  title: string;
  summary: string;
  priceUsdc: string;
  currency: "USDC" | "USD";
  execution?: PublicCapability["execution"];
  paypal?: PayPalState;
  duration: string;
  limitations: string | null;
  coverage?: CoverageItem[];
  details: { label: string; value: string }[];
  assumptions: string[];
  state: ActionState;
  decisionId: string;
  paymentOptions: PaymentOption[];
  /** Set in the browser when the server no longer holds this quote, so it can't be paid. */
  expired?: boolean;
  billing: Billing;
  document: ResultDocument | null;
  reportToken?: string | null;
  /** Whether a paid result can offer a PDF here. */
  reportAvailable?: boolean;
  /** Selun confirmed before quoting that this analysis can deliver its overall score. */
  eligibilityChecked?: boolean;
  explanation: string | null;
  explanationStatus: "none" | "pending" | "ready" | "unavailable";
  failure: { message: string; retryable: boolean; mayHaveCharged: boolean } | null;
};

export type PublicCapability = {
  id: string;
  title: string;
  summary: string;
  outputType: string;
  duration: string;
  limitations: string | null;
  execution: "x402_tool" | "wizard_handoff" | "hosted_checkout";
  priceUsdc: string;
  currency?: "USDC" | "USD";
  billing?: Billing;
};

export type SystemSummary = { slug: string; name: string; status: string; summary: string; url: string | null };
export type SystemDetail = SystemSummary & { parent: string | null; availableToday: string[]; lastVerifiedAt: string | null };

export type SiteSource = { title: string; url: string; site: string };

export type Block =
  | { type: "sagitta_sources"; sources: SiteSource[]; readAt: string }
  | { type: "sagitta_directory"; systems: SystemSummary[] }
  | { type: "sagitta_system"; system: SystemDetail }
  | { type: "action"; action: ActionView }
  | { type: "capabilities"; items: PublicCapability[] }
  | { type: "asset_choice"; query: string; reason: string; options: { symbol: string; label: string }[] }
  | { type: "handoff"; title: string; summary: string; href: string; prices: { label: string; priceUsdc: string }[]; note: string }
  | { type: "notice"; tone: "info" | "warning"; text: string };

export type ChatItem =
  | { id: string; role: "user"; text: string }
  | { id: string; role: "assistant"; text: string; blocks: Block[] }
  | { id: string; role: "error"; text: string; retryText: string | null };

export type Conversation = {
  id: string;
  serverId: string | null;
  title: string;
  items: ChatItem[];
  updatedAt: number;
};

export type AssistantStatus = {
  available: boolean;
  limits: { maxMessageChars: number; sessionLimitUsdc: string };
  rails: PaymentRail[];
  /** Payment methods this deployment accepts. Missing from an older backend. */
  methods?: PaymentMethodId[];
};

export type PaymentMethodId = "paypal" | "card" | "wallet";

/** The decoded 402 challenge, passed unchanged to the x402 client for signing. */
export type PaymentChallenge = {
  x402Version: number;
  accepts: { network: string; amount: string; scheme: string; payTo: string; asset: string }[];
  [key: string]: unknown;
};
