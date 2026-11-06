export type AssistantErrorCode =
  | "assistant_not_configured"
  | "assistant_busy"
  | "assistant_unavailable"
  | "conversation_not_found"
  | "conversation_limit"
  | "daily_limit"
  | "invalid_request"
  | "action_not_found"
  | "action_not_payable"
  | "approval_required"
  | "price_changed"
  | "spending_limit"
  | "service_unavailable"
  | "rate_limited"
  | "invalid_payment"
  | "report_not_authorized"
  | "report_revoked"
  | "report_restricted"
  | "purchase_not_found"
  | "payment_not_approved"
  | "payment_not_verified"
  | "report_not_found"
  | "invalid_email"
  | "report_email_limit"
  | "report_email_failed";

export class AssistantError extends Error {
  constructor(
    readonly code: AssistantErrorCode,
    readonly status: number,
    message: string,
    readonly extra: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = "AssistantError";
  }
}
