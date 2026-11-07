/**
 * Minimal server-side OpenAI Chat Completions client (native fetch, no SDK).
 * The API key never leaves this module and errors never include it.
 */

export type ChatToolCall = {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
};

export type ChatMessage =
  | { role: "system" | "user"; content: string }
  | { role: "assistant"; content: string | null; tool_calls?: ChatToolCall[] }
  | { role: "tool"; tool_call_id: string; content: string };

export type ChatTool = {
  type: "function";
  function: { name: string; description: string; parameters: Record<string, unknown> };
};

export type LlmRequest = {
  messages: ChatMessage[];
  tools?: ChatTool[];
  deadlineMs?: number;
  /** Lower than the configured limit, for answers that should stay short. */
  maxOutputTokens?: number;
};

export type LlmUsage = { inputTokens: number; cachedTokens: number; outputTokens: number };

export type LlmResponse = {
  content: string | null;
  toolCalls: ChatToolCall[];
  usage?: LlmUsage;
};

export interface LlmClient {
  complete(request: LlmRequest): Promise<LlmResponse>;
}

export type LlmFailureKind = "auth" | "rate_limited" | "timeout" | "upstream" | "empty";

export class LlmError extends Error {
  constructor(readonly kind: LlmFailureKind, message: string) {
    super(message);
    this.name = "LlmError";
  }
}

export type OpenAiConfig = {
  apiKey: string;
  model: string;
  baseUrl: string;
  timeoutMs: number;
  maxOutputTokens: number;
};

export function readOpenAiConfig(env: NodeJS.ProcessEnv = process.env): OpenAiConfig | null {
  const apiKey = env.OPENAI_API_KEY?.trim();
  const model = env.OPENAI_MODEL?.trim();
  if (!apiKey || !model) return null;
  const parsedTimeout = Number.parseInt(env.OPENAI_TIMEOUT_MS ?? "", 10);
  const parsedTokens = Number.parseInt(env.OPENAI_MAX_OUTPUT_TOKENS ?? "", 10);
  return {
    apiKey,
    model,
    baseUrl: (env.OPENAI_BASE_URL?.trim() || "https://api.openai.com/v1").replace(/\/+$/, ""),
    timeoutMs: Number.isFinite(parsedTimeout) && parsedTimeout > 0 ? parsedTimeout : 30_000,
    // Reasoning models count reasoning toward this limit, so keep room for it.
    maxOutputTokens: Number.isFinite(parsedTokens) && parsedTokens > 0 ? parsedTokens : 2_000,
  };
}

export class OpenAiClient implements LlmClient {
  constructor(
    private readonly config: OpenAiConfig,
    private readonly fetchImpl: typeof fetch = fetch,
  ) {}

  async complete(request: LlmRequest): Promise<LlmResponse> {
    const timeoutMs = Math.max(1_000, Math.min(this.config.timeoutMs, request.deadlineMs ?? this.config.timeoutMs));
    let response: Response;
    try {
      response = await this.fetchImpl(`${this.config.baseUrl}/chat/completions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.config.apiKey}`,
        },
        body: JSON.stringify({
          model: this.config.model,
          messages: request.messages,
          max_completion_tokens: Math.min(request.maxOutputTokens ?? this.config.maxOutputTokens, this.config.maxOutputTokens),
          store: false,
          ...(request.tools?.length ? { tools: request.tools, tool_choice: "auto" } : {}),
        }),
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      if (error instanceof Error && (error.name === "TimeoutError" || error.name === "AbortError")) {
        throw new LlmError("timeout", "The language model timed out.");
      }
      throw new LlmError("upstream", "The language model could not be reached.");
    }

    if (!response.ok) {
      const detail = await readUpstreamError(response);
      console.error(`OpenAI request failed: HTTP ${response.status} ${detail}`);
      if (response.status === 401 || response.status === 403) {
        throw new LlmError("auth", "The language model rejected Selun's credentials.");
      }
      if (response.status === 429) throw new LlmError("rate_limited", "The language model is rate limited.");
      throw new LlmError("upstream", `The language model returned HTTP ${response.status}.`);
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      throw new LlmError("upstream", "The language model returned an unreadable response.");
    }

    const message = (payload as { choices?: { message?: { content?: unknown; refusal?: unknown; tool_calls?: unknown } }[] })
      ?.choices?.[0]?.message;
    if (!message) throw new LlmError("empty", "The language model returned no message.");

    const toolCalls = Array.isArray(message.tool_calls) ? message.tool_calls.filter(isToolCall) : [];
    const content = typeof message.content === "string" && message.content.trim()
      ? message.content
      : typeof message.refusal === "string" && message.refusal.trim()
        ? message.refusal
        : null;
    if (!content && toolCalls.length === 0) throw new LlmError("empty", "The language model returned an empty message.");
    return { content, toolCalls, usage: readUsage(payload) };
  }
}

function readUsage(payload: unknown): LlmUsage | undefined {
  const usage = (payload as { usage?: { prompt_tokens?: unknown; completion_tokens?: unknown; prompt_tokens_details?: { cached_tokens?: unknown } } })?.usage;
  if (typeof usage?.prompt_tokens !== "number") return undefined;
  return {
    inputTokens: usage.prompt_tokens,
    cachedTokens: typeof usage.prompt_tokens_details?.cached_tokens === "number" ? usage.prompt_tokens_details.cached_tokens : 0,
    outputTokens: typeof usage.completion_tokens === "number" ? usage.completion_tokens : 0,
  };
}

function isToolCall(value: unknown): value is ChatToolCall {
  const call = value as ChatToolCall;
  return typeof call?.id === "string" && typeof call?.function?.name === "string" && typeof call?.function?.arguments === "string";
}

async function readUpstreamError(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { error?: { code?: unknown; type?: unknown } };
    return String(body?.error?.code ?? body?.error?.type ?? "");
  } catch {
    return "";
  }
}
