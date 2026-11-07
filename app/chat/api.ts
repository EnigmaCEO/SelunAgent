import type { ActionView, AssistantStatus, Block, PaymentChallenge, PayPalStatusView } from "./types";

export class ApiError extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
    message: string,
    readonly action?: ActionView,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

const OFFLINE_MESSAGE = "We can't reach Selun right now. Check your connection and try again.";

async function request<T>(path: string, init: { method?: "GET" | "POST"; conversationId?: string | null; body?: unknown } = {}): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api/assistant/${path}`, {
      method: init.method ?? "GET",
      headers: {
        ...(init.body !== undefined ? { "Content-Type": "application/json" } : {}),
        ...(init.conversationId ? { "x-selun-conversation": init.conversationId } : {}),
      },
      body: init.body !== undefined ? JSON.stringify(init.body) : undefined,
    });
  } catch {
    throw new ApiError("network_error", 0, OFFLINE_MESSAGE);
  }
  const payload = (await response.json().catch(() => null)) as (Record<string, unknown> & { action?: ActionView }) | null;
  if (!response.ok || !payload || payload.success === false) {
    throw new ApiError(
      typeof payload?.code === "string" ? payload.code : "unknown_error",
      response.status,
      typeof payload?.error === "string" ? payload.error : "Something went wrong. Please try again.",
      payload?.action,
    );
  }
  return payload as T;
}

export function fetchStatus() {
  return request<AssistantStatus>("status");
}

export function sendChat(conversationId: string | null, message: string) {
  return request<{ conversationId: string; text: string; blocks: Block[] }>("chat", {
    method: "POST",
    conversationId,
    body: { message },
  });
}

export function fetchAction(conversationId: string, actionId: string) {
  return request<{ action: ActionView }>(`actions/${actionId}`, { conversationId });
}

export function requestChallenge(conversationId: string, actionId: string) {
  return request<{ paymentRequired: PaymentChallenge; action: ActionView }>(`actions/${actionId}/challenge`, {
    method: "POST",
    conversationId,
    body: {},
  });
}

export function executeAction(conversationId: string, actionId: string, approvedPriceUsdc: string, paymentSignature: string) {
  return request<{ action: ActionView }>(`actions/${actionId}/execute`, {
    method: "POST",
    conversationId,
    body: { approved: true, approvedPriceUsdc, paymentSignature, rail: "x402_wallet" },
  });
}

export function startCheckout(conversationId: string, actionId: string, method: string, approvedPrice: string) {
  return request<{ approveUrl: string; kind: "one_time" | "subscription"; method: string; purchaseToken: string; action: ActionView }>(`actions/${actionId}/checkout`, {
    method: "POST",
    conversationId,
    body: { method, approved: true, approvedPrice },
  });
}

export function capturePurchase(provider: string, purchaseToken: string) {
  return request<PayPalStatusView>(`${provider}/capture`, { method: "POST", body: { purchaseToken } });
}

export function fetchPurchaseStatus(provider: string, purchaseToken: string) {
  return request<PayPalStatusView>(`${provider}/status`, { method: "POST", body: { purchaseToken } });
}

export function cancelSubscription(provider: string, purchaseToken: string) {
  return request<PayPalStatusView>(`${provider}/cancel`, { method: "POST", body: { purchaseToken } });
}

export function updateSubscriptionAssets(provider: string, purchaseToken: string, assets: string[]) {
  return request<PayPalStatusView>(`${provider}/subscription/assets`, { method: "POST", body: { purchaseToken, assets } });
}

export function declineAction(conversationId: string, actionId: string) {
  return request<{ action: ActionView }>(`actions/${actionId}/decline`, {
    method: "POST",
    conversationId,
    body: {},
  });
}

export type ReportEmailResult = { status: "sent" | "already_sent" | "test_recorded"; recipientsRemaining: number };

export function emailReport(reportToken: string, email: string) {
  return request<ReportEmailResult>("report/email", { method: "POST", body: { reportToken, email, approved: true } });
}

/** Asks the server for the report and saves it. The browser only sends the signed token. */
export async function downloadReport(reportToken: string): Promise<void> {
  let response: Response;
  try {
    response = await fetch("/api/assistant/report/download", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reportToken }),
    });
  } catch {
    throw new ApiError("network_error", 0, OFFLINE_MESSAGE);
  }
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as { code?: string; error?: string } | null;
    throw new ApiError(payload?.code ?? "unknown_error", response.status, payload?.error ?? "We couldn't get that report. Please try again.");
  }
  const filename = /filename="([^"]+)"/.exec(response.headers.get("content-disposition") ?? "")?.[1] ?? "selun-report.pdf";
  const url = URL.createObjectURL(await response.blob());
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}
