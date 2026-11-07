"use client";

import Image from "next/image";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { appendReferralParam } from "@/app/lib/referral";
import { useReferralCapture } from "@/app/lib/use-referral-capture";
import { ApiError, cancelSubscription, capturePurchase, declineAction, executeAction, fetchAction, fetchPurchaseStatus, fetchStatus, requestChallenge, sendChat, startCheckout, updateSubscriptionAssets } from "./api";
import { BlockView, RichText, type ApprovalPhase } from "./blocks";
import type { ActionView, AssistantStatus, Block, ChatItem, Conversation, PaymentOption, PayPalStatusView } from "./types";
import { assertWalletAvailable, signX402Challenge, toBaseUnits, WalletError } from "./wallet";
import styles from "./chat.module.css";

const STORAGE_KEY = "selun.chat.v1";
const MAX_STORED_CONVERSATIONS = 20;
const POLL_INTERVAL_MS = 2_500;
const PAYPAL_POLL_INTERVAL_MS = 5_000;
const PAYPAL_PENDING_KEY = "selun.paypal.pending";
const MAX_POLL_FAILURES = 6;

const SUGGESTIONS = [
  "Give me a Bitcoin market update",
  "Analyze the risks of Ethereum",
  "Is it safe to use DeFi right now?",
  "Help me build a crypto portfolio",
  "What does Sagitta Systems do?",
];

function newId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `id-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function newConversation(): Conversation {
  return { id: newId(), serverId: null, title: "New conversation", items: [], updatedAt: Date.now() };
}

function loadConversations(): Conversation[] {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    const parsed = raw ? (JSON.parse(raw) as Conversation[]) : [];
    return Array.isArray(parsed) ? parsed.filter((entry) => entry?.id && Array.isArray(entry.items)) : [];
  } catch {
    return [];
  }
}

function saveConversations(conversations: Conversation[]) {
  try {
    const kept = conversations
      .filter((entry) => entry.items.length > 0)
      .sort((left, right) => right.updatedAt - left.updatedAt)
      .slice(0, MAX_STORED_CONVERSATIONS);
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(kept));
  } catch {
    // Storage may be full or blocked. The chat still works for this visit.
  }
}

function mapBlockActions(items: ChatItem[], update: (action: ActionView) => ActionView): ChatItem[] {
  return items.map((item) => {
    if (item.role !== "assistant") return item;
    const blocks = item.blocks.map((block): Block => (block.type === "action" ? { ...block, action: update(block.action) } : block));
    return { ...item, blocks };
  });
}

function hasOpenWork(action: ActionView): boolean {
  // A PayPal purchase is read by its own token, so it survives the conversation expiring.
  if (action.paypal) return false;
  return action.state === "executing" || (action.state === "completed" && action.explanationStatus === "pending");
}

function hasOpenPayPalWork(action: ActionView): boolean {
  const paypal = action.paypal;
  if (!paypal) return false;
  if (paypal.phase === "running" || paypal.phase === "payment_pending" || paypal.phase === "delayed" || paypal.phase === "needs_selection") return true;
  return action.state === "completed" && action.explanationStatus === "pending";
}

type PendingPayPal = { method?: string; label?: string; token: string; kind: "one_time" | "subscription"; conversationId: string; actionId: string };

function readPendingPayPal(): PendingPayPal | null {
  try {
    const raw = window.localStorage.getItem(PAYPAL_PENDING_KEY);
    return raw ? (JSON.parse(raw) as PendingPayPal) : null;
  } catch {
    return null;
  }
}

/** What the card should show. The server's document replaces whatever was on screen, so a revoked report disappears. */
function applyPayPalView(action: ActionView, token: string, view: PayPalStatusView, method = action.paypal?.method ?? "paypal"): ActionView {
  const running = view.phase === "running" || view.phase === "payment_pending" || view.phase === "delayed" || view.phase === "needs_selection";
  return {
    ...action,
    state: view.phase === "completed" && view.document
      ? "completed"
      : running
        ? "executing"
        : view.phase === "awaiting_payment" ? "awaiting_approval" : "failed",
    document: view.document,
    reportToken: view.reportToken,
    explanation: view.explanation ?? action.explanation,
    explanationStatus: view.document ? (view.explanationStatus === "none" ? "none" : view.explanationStatus) : "none",
    decisionId: view.decisionId ?? action.decisionId,
    failure: null,
    paypal: {
      method,
      token,
      kind: view.kind,
      phase: view.phase,
      message: view.message,
      subscriptionStatus: view.status,
      nextBillingTime: view.nextBillingTime,
      reports: view.reports,
    },
  };
}

export default function ChatApp() {
  const referralCode = useReferralCapture();
  const [hydrated, setHydrated] = useState(false);
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeId, setActiveId] = useState<string>("");
  const [draft, setDraft] = useState("");
  const [thinking, setThinking] = useState(false);
  const [status, setStatus] = useState<AssistantStatus | null>(null);
  const [statusFailed, setStatusFailed] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [phases, setPhases] = useState<Record<string, ApprovalPhase>>({});
  const [actionErrors, setActionErrors] = useState<Record<string, string | null>>({});
  const [payPalNotice, setPayPalNotice] = useState<string | null>(null);

  const composerRef = useRef<HTMLTextAreaElement>(null);
  const logEndRef = useRef<HTMLDivElement>(null);
  const pollFailures = useRef<Record<string, number>>({});
  const conversationsRef = useRef<Conversation[]>([]);
  conversationsRef.current = conversations;

  useEffect(() => {
    // localStorage is only readable after mount.
    const stored = loadConversations();
    const fresh = newConversation();
    setConversations([fresh, ...stored]);
    setHydrated(true);
    fetchStatus().then(setStatus).catch(() => setStatusFailed(true));

    const params = new URLSearchParams(window.location.search);
    const outcome = params.get("checkout") ?? params.get("paypal");
    if (outcome !== "return" && outcome !== "cancel") {
      setActiveId(fresh.id);
      return;
    }
    window.history.replaceState(null, "", window.location.pathname);
    const pending = readPendingPayPal();
    const home = pending && stored.some((entry) => entry.id === pending.conversationId) ? pending.conversationId : fresh.id;
    setActiveId(home);
    if (!pending || home !== pending.conversationId) {
      if (outcome === "return") setPayPalNotice("We can't find this checkout in this browser. If you paid, open Selun in the browser you started from, or contact support.");
      return;
    }
    if (outcome === "cancel") {
      setActionErrors((previous) => ({ ...previous, [pending.actionId]: `You cancelled at ${pending.label ?? "PayPal"}, so nothing was charged.` }));
      return;
    }
    setPhases((previous) => ({ ...previous, [pending.actionId]: "submitting" }));
    capturePurchase(pending.method ?? "paypal", pending.token)
      .then((view) => {
        setConversations((previous) => previous.map((conversation) => (
          conversation.id === pending.conversationId
            ? { ...conversation, updatedAt: Date.now(), items: mapBlockActions(conversation.items, (action) => (action.id === pending.actionId ? applyPayPalView(action, pending.token, view, pending.method) : action)) }
            : conversation
        )));
      })
      .catch((error: unknown) => {
        const message = error instanceof ApiError ? error.message : "We couldn't confirm your payment yet. Please reload this page to check again.";
        setActionErrors((previous) => ({ ...previous, [pending.actionId]: message }));
      })
      .finally(() => setPhases((previous) => ({ ...previous, [pending.actionId]: "idle" })));
  }, []);

  useEffect(() => {
    if (hydrated) saveConversations(conversations);
  }, [conversations, hydrated]);

  const active = useMemo(() => conversations.find((entry) => entry.id === activeId) ?? null, [conversations, activeId]);
  const history = useMemo(
    () => conversations.filter((entry) => entry.items.length > 0).sort((left, right) => right.updatedAt - left.updatedAt),
    [conversations],
  );
  const isEmpty = !active || active.items.length === 0;
  const offline = status?.available === false || statusFailed;
  const itemCount = active?.items.length ?? 0;

  useEffect(() => {
    if (itemCount > 0 || thinking) logEndRef.current?.scrollIntoView({ block: "end", behavior: "smooth" });
  }, [itemCount, thinking]);

  const updateConversation = useCallback((id: string, update: (conversation: Conversation) => Conversation) => {
    setConversations((previous) => previous.map((entry) => (entry.id === id ? update(entry) : entry)));
  }, []);

  const applyActionView = useCallback((view: ActionView) => {
    setConversations((previous) => previous.map((conversation) => {
      const holds = conversation.items.some((item) => item.role === "assistant" && item.blocks.some((block) => block.type === "action" && block.action.id === view.id));
      return holds
        ? { ...conversation, updatedAt: Date.now(), items: mapBlockActions(conversation.items, (action) => (action.id === view.id ? view : action)) }
        : conversation;
    }));
  }, []);

  // Poll every action still running, including ones restored after a reload.
  const openWork = useMemo(() => {
    const open: { conversationId: string; action: ActionView }[] = [];
    for (const conversation of conversations) {
      if (!conversation.serverId) continue;
      for (const item of conversation.items) {
        if (item.role !== "assistant") continue;
        for (const block of item.blocks) {
          if (block.type === "action" && hasOpenWork(block.action)) open.push({ conversationId: conversation.serverId, action: block.action });
        }
      }
    }
    return open;
  }, [conversations]);
  const openKey = openWork.map((entry) => entry.action.id).join(",");

  useEffect(() => {
    if (!openKey) return;
    const timer = window.setInterval(() => {
      for (const { conversationId, action } of openWork) {
        fetchAction(conversationId, action.id)
          .then(({ action: view }) => {
            pollFailures.current[action.id] = 0;
            applyActionView(view);
          })
          .catch((error: unknown) => {
            const lost = error instanceof ApiError && (error.status === 404);
            const failures = (pollFailures.current[action.id] ?? 0) + 1;
            pollFailures.current[action.id] = failures;
            if (!lost && failures < MAX_POLL_FAILURES) return;
            // The server no longer knows this job. Never present that as success
            // or as "not charged", because a charge may already have happened.
            applyActionView(action.state === "completed"
              ? { ...action, explanationStatus: "unavailable" }
              : {
                ...action,
                state: "failed",
                failure: {
                  message: "We lost track of this request, possibly because Selun restarted. If your wallet shows a charge, please contact support with the request ID below.",
                  retryable: false,
                  mayHaveCharged: true,
                },
              });
          });
      }
    }, POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
    // openKey captures which actions are open.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [openKey, applyActionView]);

  const refreshPayPal = useCallback((action: ActionView) => {
    const token = action.paypal?.token;
    if (!token) return;
    fetchPurchaseStatus(action.paypal?.method ?? "paypal", token)
      .then((view) => applyActionView(applyPayPalView(action, token, view)))
      .catch((error: unknown) => {
        // The purchase stays recorded on the server. A failed check changes nothing on screen.
        console.warn("Purchase status check failed:", error instanceof ApiError ? error.code : error);
      });
  }, [applyActionView]);

  const openPayPal = useMemo(() => {
    const open: ActionView[] = [];
    for (const conversation of conversations) {
      for (const item of conversation.items) {
        if (item.role !== "assistant") continue;
        for (const block of item.blocks) {
          if (block.type === "action" && hasOpenPayPalWork(block.action)) open.push(block.action);
        }
      }
    }
    return open;
  }, [conversations]);
  const openPayPalKey = openPayPal.map((action) => action.id).join(",");
  const openPayPalRef = useRef(openPayPal);
  openPayPalRef.current = openPayPal;

  useEffect(() => {
    if (!openPayPalKey) return;
    const timer = window.setInterval(() => {
      for (const action of openPayPalRef.current) refreshPayPal(action);
    }, PAYPAL_POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [openPayPalKey, refreshPayPal]);

  // On each visit, recheck saved purchases once: one may have been refunded, disputed, or renewed since.
  useEffect(() => {
    if (!hydrated) return;
    for (const conversation of conversationsRef.current) {
      for (const item of conversation.items) {
        if (item.role !== "assistant") continue;
        for (const block of item.blocks) {
          if (block.type === "action" && block.action.paypal && !hasOpenPayPalWork(block.action)) refreshPayPal(block.action);
        }
      }
    }
  }, [hydrated, refreshPayPal]);

  const send = useCallback(async (rawText: string) => {
    const text = rawText.trim();
    if (!text || thinking || !active) return;
    const conversationId = active.id;
    const serverId = active.serverId;

    setDraft("");
    setThinking(true);
    updateConversation(conversationId, (conversation) => ({
      ...conversation,
      title: conversation.items.length === 0 ? text.slice(0, 60) : conversation.title,
      updatedAt: Date.now(),
      items: [...conversation.items.filter((item) => item.role !== "error"), { id: newId(), role: "user", text }],
    }));

    const attempt = async (id: string | null) => {
      const reply = await sendChat(id, text);
      updateConversation(conversationId, (conversation) => ({
        ...conversation,
        serverId: reply.conversationId,
        updatedAt: Date.now(),
        items: [...conversation.items, { id: newId(), role: "assistant", text: reply.text, blocks: reply.blocks }],
      }));
    };

    try {
      try {
        await attempt(serverId);
      } catch (error) {
        if (!(error instanceof ApiError) || error.code !== "conversation_not_found") throw error;
        // The server session expired. Continue here; earlier results stay on screen.
        updateConversation(conversationId, (conversation) => ({
          ...conversation,
          items: [
            ...conversation.items,
            { id: newId(), role: "assistant", text: "", blocks: [{ type: "notice", tone: "info", text: "This conversation was idle for a while, so I no longer remember what we discussed. Your earlier results are still shown above." }] },
          ],
        }));
        await attempt(null);
      }
    } catch (error) {
      const message = error instanceof ApiError ? error.message : "Something went wrong. Please try again.";
      const retryable = !(error instanceof ApiError) || !["conversation_limit", "invalid_request", "assistant_not_configured"].includes(error.code);
      updateConversation(conversationId, (conversation) => ({
        ...conversation,
        items: [...conversation.items, { id: newId(), role: "error", text: message, retryText: retryable ? text : null }],
      }));
    } finally {
      setThinking(false);
      composerRef.current?.focus();
    }
  }, [active, thinking, updateConversation]);

  const handleApprove = useCallback(async (action: ActionView, option: PaymentOption) => {
    const serverId = active?.serverId;
    if (!serverId) return;
    const setPhase = (phase: ApprovalPhase) => setPhases((previous) => ({ ...previous, [action.id]: phase }));
    setActionErrors((previous) => ({ ...previous, [action.id]: null }));

    if (option.flow === "hosted_checkout") {
      setPhase("redirecting");
      try {
        const started = await startCheckout(serverId, action.id, option.id, action.priceUsdc);
        const conversationId = conversationsRef.current.find((entry) => entry.serverId === serverId)?.id ?? activeId;
        // Written before leaving the page, so the return can find this purchase.
        window.localStorage.setItem(PAYPAL_PENDING_KEY, JSON.stringify({ method: started.method, label: option.label, token: started.purchaseToken, kind: started.kind, conversationId, actionId: action.id }));
        window.location.assign(started.approveUrl);
      } catch (error) {
        if (error instanceof ApiError && error.action) applyActionView(error.action);
        const message = error instanceof ApiError ? error.message : `${option.label} checkout couldn't start. You have not been charged. Please try again.`;
        setActionErrors((previous) => ({ ...previous, [action.id]: message }));
        setPhase("idle");
      }
      return;
    }

    try {
      assertWalletAvailable();
      setPhase("preparing");
      const { paymentRequired, action: fresh } = await requestChallenge(serverId, action.id);
      applyActionView(fresh);

      setPhase("signing");
      const { paymentSignature } = await signX402Challenge(paymentRequired, toBaseUnits(fresh.priceUsdc));

      setPhase("submitting");
      const executed = await executeAction(serverId, action.id, fresh.priceUsdc, paymentSignature);
      applyActionView(executed.action);
    } catch (error) {
      if (error instanceof ApiError && error.action) applyActionView(error.action);
      // A repeated request lost the race to the first one, which is running fine.
      if (error instanceof ApiError && error.code === "action_not_payable" && (error.action?.state === "executing" || error.action?.state === "completed")) return;
      const message = error instanceof WalletError || error instanceof ApiError
        ? error.message
        : "Something went wrong. You have not been charged. Please try again.";
      setActionErrors((previous) => ({ ...previous, [action.id]: message }));
    } finally {
      setPhase("idle");
    }
  }, [active?.serverId, activeId, applyActionView]);

  const handleCancelSubscription = useCallback(async (action: ActionView) => {
    const token = action.paypal?.token;
    if (!token || !window.confirm("Cancel your monthly subscription? You won't be charged again, and reports you've paid for stay available.")) return;
    try {
      applyActionView(applyPayPalView(action, token, await cancelSubscription(action.paypal?.method ?? "paypal", token)));
    } catch (error) {
      setActionErrors((previous) => ({ ...previous, [action.id]: error instanceof ApiError ? error.message : "We couldn't cancel that. Please try again, or cancel in your PayPal account." }));
    }
  }, [applyActionView]);

  const handleUpdateAssets = useCallback(async (action: ActionView, assets: string[]) => {
    const token = action.paypal?.token;
    if (!token) return;
    setActionErrors((previous) => ({ ...previous, [action.id]: null }));
    try {
      applyActionView(applyPayPalView(action, token, await updateSubscriptionAssets(action.paypal?.method ?? "paypal", token, assets)));
    } catch (error) {
      setActionErrors((previous) => ({ ...previous, [action.id]: error instanceof ApiError ? error.message : "We couldn't change your assets. Please try again." }));
    }
  }, [applyActionView]);

  const handleDecline = useCallback(async (action: ActionView) => {
    if (!active?.serverId) return;
    try {
      const { action: view } = await declineAction(active.serverId, action.id);
      applyActionView(view);
    } catch {
      applyActionView({ ...action, state: "declined" });
    }
  }, [active?.serverId, applyActionView]);

  const startNew = useCallback(() => {
    const fresh = newConversation();
    setConversations((previous) => [fresh, ...previous.filter((entry) => entry.items.length > 0)]);
    setActiveId(fresh.id);
    setDraft("");
    setSidebarOpen(false);
    window.setTimeout(() => composerRef.current?.focus(), 0);
  }, []);

  const removeConversation = useCallback((id: string) => {
    const remaining = conversations.filter((entry) => entry.id !== id);
    if (id !== activeId) {
      setConversations(remaining);
      return;
    }
    const fresh = newConversation();
    setConversations([fresh, ...remaining.filter((entry) => entry.items.length > 0)]);
    setActiveId(fresh.id);
  }, [activeId, conversations]);

  const onComposerKey = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      void send(draft);
    }
  };

  useEffect(() => {
    const element = composerRef.current;
    if (!element) return;
    element.style.height = "auto";
    element.style.height = `${Math.min(element.scrollHeight, 180)}px`;
  }, [draft]);

  const maxChars = status?.limits.maxMessageChars ?? 2_000;
  const referralHref = useCallback((href: string) => appendReferralParam(href, referralCode), [referralCode]);

  const composer = (
    <form
      className={styles.composer}
      onSubmit={(event) => {
        event.preventDefault();
        void send(draft);
      }}
    >
      <label htmlFor="selun-message" className={styles.srOnly}>Message Selun</label>
      <textarea
        id="selun-message"
        ref={composerRef}
        rows={1}
        value={draft}
        maxLength={maxChars}
        onChange={(event) => setDraft(event.target.value)}
        onKeyDown={onComposerKey}
        placeholder={offline ? "Selun AI is offline right now" : "Ask about crypto markets or Sagitta"}
        disabled={offline}
      />
      <button type="submit" className={styles.sendButton} disabled={!draft.trim() || thinking || offline} aria-label="Send message">
        <svg viewBox="0 0 20 20" width="18" height="18" aria-hidden><path fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" d="M10 16V4m0 0-5 5m5-5 5 5" /></svg>
      </button>
    </form>
  );

  const offlineNotice = offline && (
    <p className={styles.offlineNotice} role="status">
      Selun AI is offline right now. You can still <Link href={referralHref("/plan")}>build a portfolio plan</Link> or <Link href="/capabilities">browse our services</Link>.
    </p>
  );
  const payPalBanner = payPalNotice && <p className={styles.offlineNotice} role="alert">{payPalNotice}</p>;

  return (
    <div className={styles.app}>
      {sidebarOpen && <button type="button" className={styles.scrim} aria-label="Close menu" onClick={() => setSidebarOpen(false)} />}

      <aside className={`${styles.sidebar} ${sidebarOpen ? styles.sidebarOpen : ""}`} aria-label="Conversations and site navigation">
        <Link href="/" className={styles.logo} aria-label="Selun home">
          <Image src="/selun-logo.svg" alt="Selun" width={122} height={38} priority />
        </Link>

        <button type="button" className={styles.newChat} onClick={startNew}>
          <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden><path fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" d="M8 3v10M3 8h10" /></svg>
          New conversation
        </button>

        <nav className={styles.history} aria-label="Conversation history">
          {history.length > 0 && <h2 className={styles.historyTitle}>Recent</h2>}
          {history.length === 0 && hydrated && <p className={styles.historyEmpty}>Your conversations will appear here. They are saved only in this browser.</p>}
          <ul>
            {history.map((entry) => (
              <li key={entry.id} className={entry.id === activeId ? styles.historyActive : undefined}>
                <button
                  type="button"
                  className={styles.historyItem}
                  aria-current={entry.id === activeId ? "true" : undefined}
                  onClick={() => {
                    setActiveId(entry.id);
                    setSidebarOpen(false);
                  }}
                >
                  {entry.title}
                </button>
                <button type="button" className={styles.historyDelete} aria-label={`Delete conversation: ${entry.title}`} onClick={() => removeConversation(entry.id)}>
                  <svg viewBox="0 0 16 16" width="12" height="12" aria-hidden><path fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" d="m4 4 8 8m0-8-8 8" /></svg>
                </button>
              </li>
            ))}
          </ul>
        </nav>

        <nav className={styles.sideLinks} aria-label="Site">
          <Link href={referralHref("/plan")}>Portfolio plan</Link>
          <Link href="/capabilities">Capabilities</Link>
          <Link href="/pricing">Pricing</Link>
          <Link href="/what-is-selun">About</Link>
          <Link href="/faq">FAQ</Link>
          <Link href="/x402">x402 API</Link>
          <Link href="/earn">Referral</Link>
          <Link href="/terms">Terms</Link>
          <Link href="/privacy">Privacy</Link>
          <Link href="/support">Support</Link>
        </nav>
      </aside>

      <main className={styles.main}>
        <header className={styles.mobileBar}>
          <button type="button" className={styles.iconButton} aria-label="Open menu" aria-expanded={sidebarOpen} onClick={() => setSidebarOpen(true)}>
            <svg viewBox="0 0 20 20" width="20" height="20" aria-hidden><path fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" d="M3 6h14M3 10h14M3 14h14" /></svg>
          </button>
          <Image src="/selun-logo.svg" alt="Selun" width={96} height={30} />
          <button type="button" className={styles.iconButton} aria-label="New conversation" onClick={startNew}>
            <svg viewBox="0 0 20 20" width="20" height="20" aria-hidden><path fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" d="M10 4v12M4 10h12" /></svg>
          </button>
        </header>

        {isEmpty ? (
          <section className={styles.welcome}>
            <Image src="/selun-mark.svg" alt="" width={68} height={68} className={styles.welcomeMark} priority />
            <h1>What can Selun help you do?</h1>
            <p className={styles.welcomeLede}>Ask in your own words. Selun finds the right analysis, tells you what it costs first, and explains the result.</p>
            {composer}
            {offlineNotice}
            {payPalBanner}
            <div className={styles.suggestions} aria-label="Suggested requests">
              {SUGGESTIONS.map((suggestion) => (
                <button key={suggestion} type="button" className={styles.chip} disabled={thinking || offline} onClick={() => void send(suggestion)}>{suggestion}</button>
              ))}
            </div>
          </section>
        ) : (
          <>
            <div className={styles.log} role="log" aria-live="polite" aria-label="Conversation with Selun">
              <div className={styles.logInner}>
                {active?.items.map((item) => {
                  if (item.role === "user") {
                    return <div key={item.id} className={styles.userRow}><p className={styles.userBubble}>{item.text}</p></div>;
                  }
                  if (item.role === "error") {
                    return (
                      <div key={item.id} className={styles.errorRow} role="alert">
                        <p>{item.text}</p>
                        {item.retryText && <button type="button" className={styles.textButton} disabled={thinking} onClick={() => void send(item.retryText!)}>Try again</button>}
                      </div>
                    );
                  }
                  return (
                    <div key={item.id} className={styles.assistantRow}>
                      <Image src="/selun-mark.svg" alt="" width={28} height={28} className={styles.avatar} />
                      <div className={styles.assistantBody}>
                        {item.text && <RichText text={item.text} />}
                        {item.blocks.map((block, index) => (
                          <BlockView
                            key={block.type === "action" ? block.action.id : index}
                            block={block}
                            phase={block.type === "action" ? phases[block.action.id] ?? "idle" : "idle"}
                            actionError={block.type === "action" ? actionErrors[block.action.id] ?? null : null}
                            referralHref={referralHref}
                            onApprove={handleApprove}
                            onDecline={handleDecline}
                            onCancelSubscription={handleCancelSubscription}
                            onUpdateAssets={handleUpdateAssets}
                            onAsk={(text) => void send(text)}
                            disabled={thinking}
                          />
                        ))}
                      </div>
                    </div>
                  );
                })}
                {thinking && (
                  <div className={styles.assistantRow} role="status">
                    <Image src="/selun-mark.svg" alt="" width={28} height={28} className={styles.avatar} />
                    <div className={styles.typing}><span /><span /><span /><span className={styles.srOnly}>Selun is thinking</span></div>
                  </div>
                )}
                <div ref={logEndRef} />
              </div>
            </div>
            <div className={styles.dock}>
              {offlineNotice}
              {payPalBanner}
              {composer}
              <p className={styles.disclaimer}>Selun provides analysis, not financial advice. AI summaries can make mistakes, so check results cards for the source figures.</p>
            </div>
          </>
        )}
      </main>
    </div>
  );
}
