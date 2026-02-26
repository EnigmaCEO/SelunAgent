export type AllocateRiskTolerance = "Conservative" | "Balanced" | "Growth" | "Aggressive";

export type AllocateTimeframe = "<1_year" | "1-3_years" | "3+_years";

export type AllocateInputShape = {
  riskTolerance: AllocateRiskTolerance;
  timeframe: AllocateTimeframe;
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
    verifiedAt: string;
  };
};
