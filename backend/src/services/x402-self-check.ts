// End-to-end self-check of the x402 surface, for the network status page.
//
// Reads this server's own discovery document and calls every resource it lists
// without payment, over loopback. A healthy paid route answers an unpaid call
// with 402 and an x402 payment challenge; anything else (5xx, a 400, a timeout,
// a non-x402 body) marks that route as failing.
//
// The check is cached and single-flight: the public /health/x402 route can be
// hit by anyone, so it must never fan out into ten route calls per request.
// Unpaid calls carry no decisionId, so they write no quote records.

export interface X402RouteCheck {
  path: string;
  status: number | null;
  ok: boolean;
  latencyMs: number;
  error?: string;
}

export interface X402SelfCheckResult {
  status: "ok" | "degraded";
  checkedAt: string;
  /** e.g. "10/10 routes answer unpaid calls with 402" */
  detail: string;
  routes: X402RouteCheck[];
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export interface X402SelfCheckOptions {
  baseUrl: string;
  fetchImpl?: FetchLike;
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 15_000;

/** An unpaid call is healthy when it returns the x402 payment challenge. */
export function isPaymentChallenge(status: number, body: unknown): boolean {
  if (status !== 402) return false;
  return typeof body === "object" && body !== null && (body as { error?: unknown }).error === "x402_payment_required";
}

function resourcePaths(document: unknown): string[] {
  if (typeof document !== "object" || document === null) return [];
  const resources = (document as { resources?: unknown }).resources;
  if (!Array.isArray(resources)) return [];
  return resources.flatMap((resource) => {
    if (typeof resource !== "string") return [];
    try {
      return [new URL(resource).pathname];
    } catch {
      return [];
    }
  });
}

async function checkRoute(baseUrl: string, path: string, fetchImpl: FetchLike, timeoutMs: number): Promise<X402RouteCheck> {
  const startedAt = Date.now();
  try {
    const response = await fetchImpl(`${baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
      signal: AbortSignal.timeout(timeoutMs),
    });
    const body: unknown = await response.json().catch(() => null);
    const ok = isPaymentChallenge(response.status, body);
    const error = ok
      ? undefined
      : typeof (body as { error?: unknown } | null)?.error === "string"
        ? String((body as { error: string }).error)
        : `unexpected HTTP ${response.status}`;
    return { path, status: response.status, ok, latencyMs: Date.now() - startedAt, ...(error ? { error } : {}) };
  } catch (error) {
    return {
      path,
      status: null,
      ok: false,
      latencyMs: Date.now() - startedAt,
      error: error instanceof Error && error.name === "TimeoutError" ? "timeout" : "request failed",
    };
  }
}

export async function runX402SelfCheck(options: X402SelfCheckOptions): Promise<X402SelfCheckResult> {
  const fetchImpl = options.fetchImpl ?? ((input, init) => fetch(input, init));
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const checkedAt = new Date().toISOString();

  let paths: string[] = [];
  try {
    const response = await fetchImpl(`${options.baseUrl}/.well-known/x402`, { signal: AbortSignal.timeout(timeoutMs) });
    if (response.ok) paths = resourcePaths(await response.json());
  } catch {
    paths = [];
  }
  if (paths.length === 0) {
    return { status: "degraded", checkedAt, detail: "Discovery document unreadable or lists no routes", routes: [] };
  }

  // Sequential on purpose: ten light calls, and no burst against the IP limiter.
  const routes: X402RouteCheck[] = [];
  for (const path of paths) routes.push(await checkRoute(options.baseUrl, path, fetchImpl, timeoutMs));

  const passing = routes.filter((route) => route.ok).length;
  return {
    status: passing === routes.length ? "ok" : "degraded",
    checkedAt,
    detail: `${passing}/${routes.length} routes answer unpaid calls with 402`,
    routes,
  };
}

/** Cache a check for `ttlMs` and share one in-flight run between callers. */
export function createCachedCheck<T>(run: () => Promise<T>, ttlMs: number, now: () => number = Date.now) {
  let cached: { value: T; at: number } | null = null;
  let inflight: Promise<T> | null = null;

  return async function cachedCheck(): Promise<T> {
    if (cached && now() - cached.at < ttlMs) return cached.value;
    if (!inflight) {
      inflight = run()
        .then((value) => {
          cached = { value, at: now() };
          return value;
        })
        .finally(() => {
          inflight = null;
        });
    }
    return inflight;
  };
}
