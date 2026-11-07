export type { PortfolioSegment } from "./portfolio-segments";
import type { PortfolioSegment } from "./portfolio-segments";
import type { ScreeningSnapshot } from "../assistant/scorecard-preflight";

export type AllocateRiskTolerance = "Conservative" | "Balanced" | "Growth" | "Aggressive";

export type AllocateTimeframe = "<1_year" | "1-3_years" | "3+_years";

export type X402ToolProductId =
  | "market_regime"
  | "policy_envelope"
  | "asset_scorecard"
  | "rebalance"
  | "sce_continuity_mode"
  | "sce_case_relevance"
  | "sce_risk_evaluate";

/**
 * A wallet refund has two steps. `transactionHash` and `toAddress` record USDC recovered to the admin
 * wallet, which is `refund_pending`: the customer has nothing yet. It's `refunded` only once
 * `customerTransactionHash` records the payment to them. A record with no status predates this, and is pending.
 */
export type X402RefundRecord = {
  refundedAt: string;
  transactionHash: string;
  toAddress: string;
  amountUsdc: string;
  note?: string;
  status?: "refund_pending" | "refunded";
  customerTransactionHash?: string;
  completedAt?: string;
};

export function isRefundCompleted(refund: X402RefundRecord): boolean {
  return refund.status === "refunded" && Boolean(refund.customerTransactionHash);
}

export function completeRefund(refund: X402RefundRecord, customerTransactionHash: string, completedAt: string): X402RefundRecord {
  return { ...refund, status: "refunded", customerTransactionHash, completedAt };
}

export type X402PostSettlementFailureRecord = {
  failedAt: string;
  reason: string;
  message?: string;
};

export type AllocateInputShape = {
  riskTolerance: AllocateRiskTolerance;
  timeframe: AllocateTimeframe;
  portfolioSegment: PortfolioSegment;
  withReport: boolean;
};

export type X402AllocateRecordState = "quoted" | "accepted";

export type X402AllocateRecord = {
  decisionId: string;
  inputFingerprint: string;
  inputs: AllocateInputShape;
  chargedAmountUsdc: string;
  quoteIssuedAt: string;
  quoteExpiresAt: string;
  state: X402AllocateRecordState;
  createdAt: string;
  updatedAt: string;
  jobId?: string;
  payment?: {
    fromAddress: string;
    transactionHash: string;
    network?: string;
    verifiedAt: string;
  };
  refund?: X402RefundRecord;
};

export type X402ToolRecordState = "quoted" | "accepted" | "failed_post_settlement";

export type X402ToolRecord = {
  /** The screening decision this purchase is locked to. Kept for every run and retry of it. */
  screeningSnapshot?: ScreeningSnapshot;
  decisionId: string;
  productId: X402ToolProductId;
  inputFingerprint: string;
  requestBody: Record<string, unknown>;
  chargedAmountUsdc: string;
  quoteIssuedAt: string;
  quoteExpiresAt: string;
  state: X402ToolRecordState;
  createdAt: string;
  updatedAt: string;
  responseData?: Record<string, unknown>;
  payment?: {
    fromAddress: string;
    transactionHash: string;
    network?: string;
    verifiedAt: string;
  };
  failure?: X402PostSettlementFailureRecord;
  refund?: X402RefundRecord;
};
