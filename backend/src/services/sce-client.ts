const SCE_DEFAULT_BASE_URL = "https://continuityengineserver.fly.dev";
const SCE_DEFAULT_TIMEOUT_MS = 8000;

function getSceBaseUrl(): string {
  const raw = process.env.SCE_API_BASE_URL?.trim() || SCE_DEFAULT_BASE_URL;
  return raw.endsWith("/") ? raw.slice(0, -1) : raw;
}

function getSceTimeoutMs(): number {
  const raw = process.env.SCE_API_TIMEOUT_MS?.trim();
  if (!raw) return SCE_DEFAULT_TIMEOUT_MS;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : SCE_DEFAULT_TIMEOUT_MS;
}

function getSceAdminKey(): string | null {
  return process.env.SCE_ADMIN_KEY?.trim() || null;
}

async function callSce(path: string, body: Record<string, unknown>): Promise<Record<string, unknown>> {
  const url = `${getSceBaseUrl()}${path}`;
  const timeoutMs = getSceTimeoutMs();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const adminKey = getSceAdminKey();
  if (adminKey) headers["X-SCE-Admin-Key"] = adminKey;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    const text = await response.text();

    if (!response.ok) {
      const excerpt = text.slice(0, 300).trim();
      throw new Error(`SCE upstream returned HTTP ${response.status}${excerpt ? `: ${excerpt}` : ""}`);
    }

    try {
      const parsed = JSON.parse(text) as unknown;
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error(`SCE upstream returned unexpected JSON type (HTTP ${response.status})`);
      }
      return parsed as Record<string, unknown>;
    } catch (parseError) {
      if (parseError instanceof SyntaxError) {
        throw new Error(`SCE upstream returned non-JSON response (HTTP ${response.status})`);
      }
      throw parseError;
    }
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`SCE upstream timed out after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export function callSceContinuityMode(body: Record<string, unknown>): Promise<Record<string, unknown>> {
  return callSce("/v1/sce/continuity-mode", body);
}

export function callSceCaseRelevance(body: Record<string, unknown>): Promise<Record<string, unknown>> {
  return callSce("/v1/sce/case-relevance", body);
}

export function callSceRiskEvaluate(body: Record<string, unknown>): Promise<Record<string, unknown>> {
  return callSce("/v1/sce/risk/evaluate", body);
}
