import assert from "node:assert/strict";
import test from "node:test";
import { callSceContinuityMode, callSceCaseRelevance, callSceRiskEvaluate } from "./sce-client";

type FetchArgs = [string | URL | Request, RequestInit | undefined];

function makeFetchMock(ok: boolean, status: number, body: string): { fn: typeof fetch; calls: FetchArgs[] } {
  const calls: FetchArgs[] = [];
  const fn = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    calls.push([input, init]);
    return {
      ok,
      status,
      text: async () => body,
    } as Response;
  };
  return { fn: fn as typeof fetch, calls };
}

function withFetch<T>(mockFn: typeof fetch, run: () => Promise<T>): Promise<T> {
  const original = globalThis.fetch;
  globalThis.fetch = mockFn;
  return run().finally(() => {
    globalThis.fetch = original;
  });
}

function withEnv<T>(vars: Record<string, string | undefined>, run: () => Promise<T>): Promise<T> {
  const saved: Record<string, string | undefined> = {};
  for (const key of Object.keys(vars)) {
    saved[key] = process.env[key];
    if (vars[key] === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = vars[key];
    }
  }
  return run().finally(() => {
    for (const key of Object.keys(saved)) {
      if (saved[key] === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = saved[key];
      }
    }
  });
}

test("callSceContinuityMode builds correct URL from configured base", async () => {
  const mock = makeFetchMock(true, 200, JSON.stringify({ status: "ok" }));
  await withEnv({ SCE_API_BASE_URL: "https://test.example.com", SCE_API_TIMEOUT_MS: undefined }, () =>
    withFetch(mock.fn, () => callSceContinuityMode({ decisionId: "d-001" })),
  );
  assert.equal(mock.calls.length, 1);
  assert.equal(mock.calls[0][0], "https://test.example.com/v1/sce/continuity-mode");
});

test("callSceContinuityMode normalizes trailing slash in base URL", async () => {
  const mock = makeFetchMock(true, 200, JSON.stringify({ status: "ok" }));
  await withEnv({ SCE_API_BASE_URL: "https://test.example.com/", SCE_API_TIMEOUT_MS: undefined }, () =>
    withFetch(mock.fn, () => callSceContinuityMode({ decisionId: "d-002" })),
  );
  assert.equal(mock.calls[0][0], "https://test.example.com/v1/sce/continuity-mode");
});

test("callSceCaseRelevance builds correct URL", async () => {
  const mock = makeFetchMock(true, 200, JSON.stringify({ relevant: true }));
  await withEnv({ SCE_API_BASE_URL: "https://test.example.com", SCE_API_TIMEOUT_MS: undefined }, () =>
    withFetch(mock.fn, () => callSceCaseRelevance({ decisionId: "d-003" })),
  );
  assert.equal(mock.calls[0][0], "https://test.example.com/v1/sce/case-relevance");
});

test("callSceRiskEvaluate builds correct URL", async () => {
  const mock = makeFetchMock(true, 200, JSON.stringify({ risk: "low" }));
  await withEnv({ SCE_API_BASE_URL: "https://test.example.com", SCE_API_TIMEOUT_MS: undefined }, () =>
    withFetch(mock.fn, () => callSceRiskEvaluate({ decisionId: "d-004" })),
  );
  assert.equal(mock.calls[0][0], "https://test.example.com/v1/sce/risk/evaluate");
});

test("callSceContinuityMode uses Fly production URL when env var is absent", async () => {
  const mock = makeFetchMock(true, 200, JSON.stringify({ status: "ok" }));
  await withEnv({ SCE_API_BASE_URL: undefined, SCE_API_TIMEOUT_MS: undefined }, () =>
    withFetch(mock.fn, () => callSceContinuityMode({ decisionId: "d-005" })),
  );
  assert.equal(mock.calls[0][0], "https://continuityengineserver.fly.dev/v1/sce/continuity-mode");
});

test("callSceContinuityMode forwards request body as JSON", async () => {
  const mock = makeFetchMock(true, 200, JSON.stringify({ status: "ok" }));
  const body = { decisionId: "d-006", payload: "test" };
  await withEnv({ SCE_API_BASE_URL: "https://test.example.com", SCE_API_TIMEOUT_MS: undefined }, () =>
    withFetch(mock.fn, () => callSceContinuityMode(body)),
  );
  const init = mock.calls[0][1];
  assert.equal(init?.method, "POST");
  assert.equal(JSON.parse(init?.body as string).decisionId, "d-006");
});

test("callSceContinuityMode throws on non-2xx response with status code", async () => {
  const mock = makeFetchMock(false, 503, "service unavailable");
  await assert.rejects(
    () =>
      withEnv({ SCE_API_BASE_URL: "https://test.example.com", SCE_API_TIMEOUT_MS: undefined }, () =>
        withFetch(mock.fn, () => callSceContinuityMode({ decisionId: "d-007" })),
      ),
    (error: Error) => {
      assert.match(error.message, /SCE upstream returned HTTP 503/);
      return true;
    },
  );
});

test("callSceContinuityMode throws on non-JSON 2xx response", async () => {
  const mock = makeFetchMock(true, 200, "not json at all");
  await assert.rejects(
    () =>
      withEnv({ SCE_API_BASE_URL: "https://test.example.com", SCE_API_TIMEOUT_MS: undefined }, () =>
        withFetch(mock.fn, () => callSceContinuityMode({ decisionId: "d-008" })),
      ),
    (error: Error) => {
      assert.match(error.message, /non-JSON response/);
      return true;
    },
  );
});

test("callSceContinuityMode throws timeout error on AbortError", async () => {
  const calls: FetchArgs[] = [];
  const abortingFetch = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    calls.push([input, init]);
    await new Promise<void>((_, reject) => {
      if (init?.signal) {
        init.signal.addEventListener("abort", () => {
          const err = new Error("The operation was aborted.");
          err.name = "AbortError";
          reject(err);
        });
      }
    });
    throw new Error("unreachable");
  };

  await assert.rejects(
    () =>
      withEnv({ SCE_API_BASE_URL: "https://test.example.com", SCE_API_TIMEOUT_MS: "50" }, () =>
        withFetch(abortingFetch as typeof fetch, () => callSceContinuityMode({ decisionId: "d-009" })),
      ),
    (error: Error) => {
      assert.match(error.message, /timed out after 50ms/);
      return true;
    },
  );
});
