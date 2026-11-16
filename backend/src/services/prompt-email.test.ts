import assert from "node:assert/strict";
import test from "node:test";
import { buildPromptEmail, promptEmailRecipients, sendAdminPromptEmail } from "./email.service";

const input = {
  conversationId: "3f2a9c1e-5b7d-4e2a-9c1e-5b7d4e2a9c1e",
  message: "What are the risks of <Bitcoin>?",
  replyText: "Here is a short overview.",
  cards: ["Quote: Bitcoin Risk Analysis ($1.50)"],
  environment: "staging",
  at: new Date("2026-11-16T22:00:00.000Z"),
};

function withEnv(vars: Record<string, string | undefined>, run: () => Promise<void> | void) {
  const saved = Object.fromEntries(Object.keys(vars).map((key) => [key, process.env[key]]));
  for (const [key, value] of Object.entries(vars)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  return Promise.resolve(run()).finally(() => {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });
}

test("one subject per conversation, so a mail client threads it", () => {
  const first = buildPromptEmail(input);
  const second = buildPromptEmail({ ...input, message: "And Ethereum?", at: new Date("2026-11-16T22:01:00.000Z") });
  assert.equal(first.subject, "Selun AI staging · conversation 3f2a9c1e");
  assert.equal(second.subject, first.subject);
});

test("carries the prompt, the reply, and the cards, and escapes HTML", () => {
  const email = buildPromptEmail(input);
  assert.match(email.text, /What are the risks of <Bitcoin>\?/);
  assert.match(email.text, /Here is a short overview\./);
  assert.match(email.text, /Quote: Bitcoin Risk Analysis \(\$1\.50\)/);
  assert.match(email.html, /&lt;Bitcoin&gt;/);
  assert.doesNotMatch(email.html, /<Bitcoin>/);
});

test("says so when a reply had no text or cards", () => {
  const email = buildPromptEmail({ ...input, replyText: null, cards: [] });
  assert.match(email.text, /\(no text reply\)/);
  assert.match(email.text, /Cards shown: none/);
});

test("is off unless SELUN_PROMPT_EMAILS names a valid recipient", () =>
  withEnv({ SELUN_PROMPT_EMAILS: undefined }, () => {
    assert.deepEqual(promptEmailRecipients(), []);
    process.env.SELUN_PROMPT_EMAILS = "not-an-email, ops@example.com";
    assert.deepEqual(promptEmailRecipients(), ["ops@example.com"]);
  }));

test("sends through Resend even when customer email is mocked", () =>
  withEnv(
    {
      SELUN_PROMPT_EMAILS: "ops@example.com",
      SELUN_EMAIL_PROVIDER: "mock",
      RESEND_API_KEY: "re_test",
      SELUN_EMAIL_FROM: "Selun <noreply@example.com>",
    },
    async () => {
      const calls: { url: string; body: { to: string[]; subject: string } }[] = [];
      const realFetch = globalThis.fetch;
      globalThis.fetch = (async (url: string, init: RequestInit) => {
        calls.push({ url, body: JSON.parse(String(init.body)) });
        return new Response(JSON.stringify({ id: "email_1" }), { status: 200 });
      }) as typeof fetch;
      try {
        await sendAdminPromptEmail(input);
      } finally {
        globalThis.fetch = realFetch;
      }
      assert.equal(calls.length, 1);
      assert.equal(calls[0].url, "https://api.resend.com/emails");
      assert.deepEqual(calls[0].body.to, ["ops@example.com"]);
      assert.equal(calls[0].body.subject, "Selun AI staging · conversation 3f2a9c1e");
    },
  ));

test("sends nothing without a recipient", () =>
  withEnv({ SELUN_PROMPT_EMAILS: undefined }, async () => {
    let called = false;
    const realFetch = globalThis.fetch;
    globalThis.fetch = (async () => {
      called = true;
      return new Response("{}");
    }) as typeof fetch;
    try {
      await sendAdminPromptEmail(input);
    } finally {
      globalThis.fetch = realFetch;
    }
    assert.equal(called, false);
  }));
