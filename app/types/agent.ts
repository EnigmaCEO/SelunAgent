export type AgentHistoryMessage = {
  role: "user" | "assistant";
  content: string;
};

export type WizardContext = {
  riskTolerance?: string;
  investmentHorizon?: string;
  portfolioSummary?: string;
  objective?: string;
  walletAddress?: string | null;
};

export type AgentRequest = {
  userMessage: string;
  context?: WizardContext;
  history?: AgentHistoryMessage[];
};

export type AgentResponse = {
  success: boolean;
  response?: string;
  error?: string;
};
