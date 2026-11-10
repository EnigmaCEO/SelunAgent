"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { Fragment, useState, useSyncExternalStore, type FormEvent, type ReactNode } from "react";
import { ApiError, downloadReport, emailReport } from "./api";
import type { ActionView, Block, ComparisonRow, CoverageItem, PaymentOption, PublicCapability, ResultDocument, ResultSection, SiteSource, SystemDetail, SystemSummary, Tone } from "./types";
import { hasBrowserWallet } from "./wallet";
import styles from "./chat.module.css";

/** A browser wallet is injected before the page runs and isn't watched for changes. */
const noSubscription = () => () => undefined;

/** AG Grid loads only when a result has something to compare. */
const AssetGrid = dynamic(() => import("./AssetGrid"), { ssr: false, loading: () => <p className={styles.quietStatus}>Loading the comparison…</p> });
const PriceChart = dynamic(() => import("./PriceChart"), { ssr: false, loading: () => <div className={styles.priceChartLoading} aria-hidden /> });

export function formatUsdc(amount: string): string {
  const value = Number(amount);
  if (!Number.isFinite(value)) return `${amount} USDC`;
  return `$${value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 6 })}`;
}

const TONE_CLASS: Record<Tone, string> = {
  positive: styles.tonePositive,
  neutral: styles.toneNeutral,
  caution: styles.toneCaution,
  negative: styles.toneNegative,
};

/** Bold and "- " lists only. Model text is rendered as text nodes, never as HTML or links. */
export function RichText({ text }: { text: string }) {
  const blocks = text.split(/\n{2,}/).map((block) => block.trim()).filter(Boolean);
  const inline = (line: string): ReactNode[] =>
    line.split(/(\*\*[^*]+\*\*)/g).map((part, index) =>
      part.startsWith("**") && part.endsWith("**") && part.length > 4
        ? <strong key={index}>{part.slice(2, -2)}</strong>
        : <Fragment key={index}>{part}</Fragment>,
    );
  return (
    <div className={styles.richText}>
      {blocks.map((block, blockIndex) => {
        const lines = block.split("\n");
        if (lines.every((line) => /^\s*[-*]\s+/.test(line))) {
          return <ul key={blockIndex}>{lines.map((line, index) => <li key={index}>{inline(line.replace(/^\s*[-*]\s+/, ""))}</li>)}</ul>;
        }
        return <p key={blockIndex}>{lines.map((line, index) => <Fragment key={index}>{index > 0 && <br />}{inline(line)}</Fragment>)}</p>;
      })}
    </div>
  );
}

function SectionView({ section }: { section: ResultSection }) {
  switch (section.kind) {
    case "highlight":
      return (
        <div className={`${styles.highlight} ${TONE_CLASS[section.tone]}`}>
          <span className={styles.highlightLabel}>{section.label}</span>
          <span className={styles.highlightValue}>{section.value}</span>
          {section.detail && <span className={styles.highlightDetail}>{section.detail}</span>}
        </div>
      );
    case "metrics": {
      // Labels wrap to different lengths, so each value sits at the foot of its tile, and tiles without a meter keep
      // its space whenever another tile in the row has one: the figures line up however the labels fall.
      const anyMeter = section.items.some((item) => item.meter !== undefined);
      return (
        <div className={styles.section}>
          {section.title && <h4 className={styles.sectionTitle}>{section.title}</h4>}
          <dl className={styles.metrics}>
            {section.items.map((item) => (
              <div key={item.label} className={styles.metric}>
                <dt>
                  {item.label}
                  {item.hint && <span className={styles.metricHint}>{item.hint}</span>}
                </dt>
                <dd className={item.tone ? TONE_CLASS[item.tone] : undefined}>{item.value}</dd>
                {item.meter !== undefined ? (
                  <span className={styles.meter} aria-hidden>
                    <span style={{ width: `${Math.round(item.meter * 100)}%` }} />
                  </span>
                ) : anyMeter ? (
                  <span className={`${styles.meter} ${styles.meterSpace}`} aria-hidden />
                ) : null}
              </div>
            ))}
          </dl>
        </div>
      );
    }
    case "table":
      return (
        <div className={styles.section}>
          {section.title && <h4 className={styles.sectionTitle}>{section.title}</h4>}
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>{section.columns.map((column) => <th key={column.key} scope="col" className={column.numeric ? styles.numeric : undefined}>{column.label}</th>)}</tr>
              </thead>
              <tbody>
                {section.rows.map((row, index) => (
                  <tr key={index}>{section.columns.map((column) => <td key={column.key} className={column.numeric ? styles.numeric : undefined}>{row[column.key]}</td>)}</tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      );
    case "list":
      return (
        <div className={styles.section}>
          {section.title && <h4 className={styles.sectionTitle}>{section.title}</h4>}
          <ul className={styles.resultList}>{section.items.map((item) => <li key={item}>{item}</li>)}</ul>
        </div>
      );
    case "note":
      return <p className={styles.resultNote}>{section.text}</p>;
    case "heading":
      return <h4 className={styles.sectionHeading}>{section.text}</h4>;
  }
}

function formatDate(value: string | null): string | null {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

function CoverageList({ items }: { items?: CoverageItem[] }) {
  if (!items?.length) return null;
  return (
    <dl className={styles.coverage} aria-label="What this includes">
      {items.map((item) => (
        <div key={item.label}>
          <dt>{item.label}: <strong>{item.included ? "Included" : "Not included"}</strong></dt>
          <dd>{item.note}</dd>
        </div>
      ))}
    </dl>
  );
}

function ReportActions({ reportToken, title, monthly, suggestedEmail }: { reportToken: string; title: string; monthly: boolean; suggestedEmail: string | null }) {
  const [downloading, setDownloading] = useState(false);
  const [sending, setSending] = useState(false);
  const [email, setEmail] = useState(suggestedEmail ?? "");
  const [message, setMessage] = useState<{ text: string; error: boolean } | null>(null);
  const failure = (error: unknown, fallback: string) => setMessage({ text: error instanceof ApiError ? error.message : fallback, error: true });

  const onDownload = async () => {
    setDownloading(true);
    setMessage(null);
    try {
      await downloadReport(reportToken);
    } catch (error) {
      failure(error, "We couldn't get that report. Please try again.");
    } finally {
      setDownloading(false);
    }
  };

  const onEmail = async (event: FormEvent) => {
    event.preventDefault();
    setSending(true);
    setMessage(null);
    try {
      const { status } = await emailReport(reportToken, email);
      setMessage({
        error: false,
        text: status === "test_recorded"
          ? `Test mode: the report for ${email} was recorded locally. No email was sent.`
          : status === "already_sent"
            ? `This report has already gone to ${email}, so we didn't send it again.`
            : `Sent to ${email}.`,
      });
      setEmail("");
    } catch (error) {
      failure(error, "We couldn't send that email. Please try again, or download the PDF instead.");
    } finally {
      setSending(false);
    }
  };

  return (
    <div className={styles.reportActions}>
      <button type="button" className={styles.ghostButton} disabled={downloading} onClick={() => void onDownload()}>
        {downloading ? "Preparing PDF…" : "Download PDF report"}
      </button>
      <form className={styles.reportEmail} onSubmit={(event) => void onEmail(event)}>
        <label htmlFor={`report-email-${reportToken.slice(0, 12)}`}>Email this report to</label>
        <div>
          <input
            id={`report-email-${reportToken.slice(0, 12)}`}
            type="email"
            required
            autoComplete="email"
            placeholder="you@example.com"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
          />
          <button type="submit" className={styles.ghostButton} disabled={sending || !email}>{sending ? "Sending…" : "Send"}</button>
        </div>
      </form>
      <p className={styles.reportNote}>The report contains the {title} result exactly as the service returned it, not the AI summary. {monthly ? "Each paid month's report goes to your PayPal address, and we only send copies to addresses you enter here." : suggestedEmail ? "We filled in the address from your payment. Change it if you'd like, and nothing is sent until you press Send." : "We only send it to the address you enter here."}</p>
      {message && <p className={message.error ? styles.inlineError : styles.reportStatus} role={message.error ? "alert" : "status"}>{message.text}</p>}
    </div>
  );
}

type ReportGroup = { title: string; headline: string | null; tone?: Tone; sections: ResultSection[] };

/**
 * A report made of several analyses, split at its headings, so the summary comes first and each analysis opens on
 * request. Each row shows its headline reading: the first highlight, or else the first figure. Null for a
 * single analysis, which stays as it is. The PDF always has everything.
 */
function reportGroups(sections: ResultSection[]): ReportGroup[] | null {
  if (!sections.some((section) => section.kind === "heading")) return null;
  const groups: ReportGroup[] = [];
  for (const section of sections) {
    if (section.kind === "heading") groups.push({ title: section.text, headline: null, sections: [] });
    else if (groups.length === 0) groups.push({ title: "Overview", headline: null, sections: [section] });
    else groups[groups.length - 1].sections.push(section);
  }
  for (const group of groups) {
    const highlight = group.sections.find((section) => section.kind === "highlight");
    const metrics = group.sections.find((section) => section.kind === "metrics");
    // Several assets have no single figure to lead with.
    const single = group.sections.filter((section) => section.kind === "metrics").length === 1;
    if (highlight?.kind === "highlight") Object.assign(group, { headline: highlight.value, tone: highlight.tone });
    else if (metrics?.kind === "metrics" && metrics.items[0] && single) group.headline = `${metrics.items[0].label} ${metrics.items[0].value}`;
  }
  return groups;
}

function ResultCard({ document, reportToken, monthly = false, suggestedEmail = null, comparison = null, children }: { document: ResultDocument; reportToken?: string | null; monthly?: boolean; suggestedEmail?: string | null; comparison?: ComparisonRow[] | null; children?: ReactNode }) {
  const { provenance } = document;
  const more = document.sections.filter((section) => section.more);
  const groups = reportGroups(document.sections.filter((section) => !section.more));
  const snapshot = formatDate(document.snapshotAt);
  const tx = provenance.transactionHash ?? "";
  const shortTx = `${tx.slice(0, 8)}…${tx.slice(-6)}`;
  const paypalReference = provenance.paypal?.saleId ?? provenance.paypal?.captureId ?? null;
  const charts = document.charts?.length
    ? <div className={styles.priceCharts}>{document.charts.map((chart) => <PriceChart key={chart.asset} chart={chart} />)}</div>
    : null;
  return (
    <article className={styles.resultCard} aria-label={`${document.title} result`}>
      <header className={styles.resultHeader}>
        <h3>{document.title}</h3>
        <span className={styles.verifiedBadge}>
          <svg viewBox="0 0 16 16" width="13" height="13" aria-hidden><path fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" d="m3.5 8.5 3 3 6-7" /></svg>
          From Selun&apos;s service
        </span>
      </header>
      {snapshot && <p className={styles.resultMeta}>Data as of {snapshot}</p>}
      {groups ? (
        <>
          {children}
          {charts}
          <div className={styles.reportSections}>
            {groups.map((group, index) => (
              <details key={index} className={styles.reportSection}>
                <summary>
                  <span>{group.title}</span>
                  {group.headline && <span className={`${styles.reportHeadline} ${group.tone ? TONE_CLASS[group.tone] : ""}`}>{group.headline}</span>}
                </summary>
                <div className={styles.resultBody}>
                  {group.sections.map((section, inner) => <SectionView key={inner} section={section} />)}
                </div>
              </details>
            ))}
          </div>
        </>
      ) : (
        <>
          <div className={styles.resultBody}>
            {document.sections.filter((section) => !section.more).map((section, index) => <SectionView key={index} section={section} />)}
          </div>
          {charts}
          {children}
        </>
      )}
      {comparison && <AssetGrid rows={comparison} title={comparison.some((row) => !row.current) ? "Compare with your other analyses" : "Compare these assets"} />}
      {more.length > 0 ? (
        <details className={styles.moreInfo}>
          <summary>Full analysis and methodology</summary>
          <div className={styles.resultBody}>
            {more.map((section, index) => <SectionView key={index} section={section} />)}
          </div>
          <CoverageList items={document.coverage} />
        </details>
      ) : <CoverageList items={document.coverage} />}
      {document.notice && <p className={styles.analysisNotice}>{document.notice}</p>}
      <footer className={styles.receipt}>
        {provenance.paypal
          ? <span>Paid {formatUsdc(provenance.paypal.amount)} USD with {provenance.paypal.label ?? "PayPal"}{provenance.paypal.environment === "sandbox" ? " (test)" : ""}</span>
          : <span>Paid {formatUsdc(provenance.amountUsdc ?? "")} USDC</span>}
        {provenance.paypal
          ? <span>{provenance.paypal.label ?? "PayPal"} reference {paypalReference}</span>
          : provenance.explorerUrl
          ? <a href={provenance.explorerUrl} target="_blank" rel="noopener noreferrer">Receipt {shortTx}<span className={styles.srOnly}> (opens in a new tab)</span></a>
          : <span>Receipt {shortTx}</span>}
      </footer>
      {reportToken && <ReportActions reportToken={reportToken} title={document.title} monthly={monthly} suggestedEmail={monthly ? null : suggestedEmail} />}
    </article>
  );
}

export type ApprovalPhase = "idle" | "preparing" | "connecting" | "signing" | "submitting" | "redirecting";
/** Which wallet signs a crypto payment: one in this browser, or one on a phone, connected by QR code. */
export type WalletMethod = "browser" | "phone";
/** A QR code waiting for a phone wallet to scan it. */
export type WalletPairing = { dataUrl: string; uri: string };

const TERMS_UP_FRONT = new Set(["comprehensive_report", "monthly_intelligence"]);

const PHASE_LABEL: Record<ApprovalPhase, string> = {
  idle: "",
  preparing: "Confirming price…",
  connecting: "Waiting for your wallet…",
  redirecting: "Opening checkout…",
  signing: "Check your wallet…",
  submitting: "Sending approval…",
};

type SimpleCard = { title: string; eyebrow: string; lead: string; includes: string[]; note: string; assurance?: string; moreLabel: string };

/** "Bitcoin (BTC), Ethereum (ETH)" as the card's own details state it. */
function namedAssets(action: ActionView): { name: string; symbol: string }[] {
  const value = action.details.find((detail) => detail.label === "Assets")?.value ?? "";
  return [...value.matchAll(/([^,(]+?)\s*\(([^)]+)\)/g)].map((match) => ({ name: match[1].trim(), symbol: match[2].trim() }));
}

/** What a customer buying a one-off analysis needs in order to decide. The full terms stay one click away. */
function simpleCard(action: ActionView): SimpleCard | null {
  const pdf = action.reportAvailable === false ? [] : ["PDF report"];
  if (action.capabilityId === "asset_scorecard") {
    const assets = namedAssets(action);
    if (assets.length === 0) return null;
    const one = assets.length === 1 ? assets[0] : null;
    const symbols = assets.map((asset) => asset.symbol).join(", ");
    return {
      title: one ? `${one.name} Risk Analysis` : `Risk Analysis: ${symbols}`,
      eyebrow: `Asset Scorecard · ${symbols}`,
      lead: "Historical volatility, major price declines, and market-standing analysis.",
      includes: ["Risk assessment", "Historical data, up to 1 year", ...pdf],
      note: "A historical risk analysis, not a live market update. Data runs through the last completed UTC day, without live prices or current news.",
      assurance: action.eligibilityChecked
        ? `${one ? `${one.name} is` : "These assets are"} eligible for a complete analysis. If the data feed fails, you can retry at no additional cost.`
        : undefined,
      moreLabel: "What's included and how scores work",
    };
  }
  if (action.capabilityId === "market_regime") {
    return {
      title: "Crypto Market Conditions",
      eyebrow: "Market Regime · Whole market",
      lead: "Get a snapshot of overall crypto market conditions across volatility, liquidity, and sentiment.",
      includes: ["Overall market classification", "Volatility, liquidity, and sentiment readings", ...pdf],
      note: "A snapshot from when the analysis runs. Live prices and current news are not included.",
      moreLabel: "What's included and how it works",
    };
  }
  return null;
}

/** Which detail each default stands for, so only settings the customer chose are shown up front. */
const DEFAULTED_DETAIL: Record<string, string> = {
  "Balanced risk profile": "Risk profile",
  "1 to 3 year horizon": "Horizon",
  "Bluechips segment": "Segment",
};

/** A result for the same service and assets that this browser already holds. */
export type PreviousPurchase = { at: string | null; onReview: () => void };

function ActionCard(props: {
  action: ActionView;
  phase: ApprovalPhase;
  error: string | null;
  onCheckQuote: (action: ActionView) => Promise<ActionView | null>;
  onNewQuote: (action: ActionView) => void;
  onApprove: (action: ActionView, option: PaymentOption, wallet?: WalletMethod) => void;
  onDecline: (action: ActionView) => void;
  onCancelSubscription: (action: ActionView) => void;
  onUpdateAssets: (action: ActionView, assets: string[]) => void;
  onAsk: (text: string) => void;
  previous?: PreviousPurchase | null;
  comparison?: ComparisonRow[] | null;
  walletPairing?: WalletPairing | null;
  phoneWalletAvailable?: boolean;
  onCancelPairing?: (action: ActionView) => void;
}) {
  const { previous, comparison, action, phase, error, onCheckQuote, onNewQuote, onApprove, onDecline, onCancelSubscription, onUpdateAssets, onAsk, walletPairing, phoneWalletAvailable, onCancelPairing } = props;
  // Read after the first render, since the server can't see the browser's wallet.
  const browserWallet = useSyncExternalStore(noSubscription, hasBrowserWallet, () => null);
  const [walletChoice, setWalletChoice] = useState<WalletMethod | null>(null);
  const walletMethod: WalletMethod = walletChoice ?? (browserWallet === false && phoneWalletAvailable ? "phone" : "browser");
  const busy = phase !== "idle";
  // PayPal leads, and starts selected: the customer still approves the exact amount at PayPal before anything is charged.
  const options = [...(action.paymentOptions ?? [])].sort((left, right) => Number(right.id === "paypal") - Number(left.id === "paypal"));
  const retryOption = options.find((option) => option.flow === "wallet_signature");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [chosen, setChosen] = useState<PaymentOption | null>(null);
  const [checking, setChecking] = useState(false);
  const preferred = options.find((option) => option.id === "paypal") ?? null;
  const selected = options.length === 1 ? options[0] : options.find((option) => option.id === selectedId) ?? preferred;
  const continueToPay = async () => {
    if (!selected) return;
    setChecking(true);
    const fresh = await onCheckQuote(action);
    setChecking(false);
    // If the method or the price changed, the card now shows the new terms to approve.
    const current = fresh?.paymentOptions.find((option) => option.id === selected.id);
    if (fresh && current && fresh.priceUsdc === action.priceUsdc) pay(current);
  };
  const pay = (option: PaymentOption) => {
    setChosen(option);
    onApprove(action, option, option.flow === "wallet_signature" ? walletMethod : undefined);
  };
  const subscription = action.paypal?.kind === "subscription" ? action.paypal : null;
  const subscriptionRow = subscription && subscription.subscriptionStatus && subscription.subscriptionStatus !== "created" && (
    <div className={styles.subscriptionRow}>
      <p role="status">
        {subscription.subscriptionStatus === "active"
          ? `Your monthly subscription is active.${subscription.nextBillingTime ? ` Next payment: ${formatDate(subscription.nextBillingTime)}.` : ""} Each paid month's report is also emailed to you.`
          : subscription.subscriptionStatus === "cancelled"
            ? "Your subscription is cancelled, so you won't be charged again. Reports you've paid for stay available."
            : `Your subscription is ${subscription.subscriptionStatus}.`}
      </p>
      {subscription.subscriptionStatus === "active" && (
        <>
          <form
            className={styles.assetForm}
            onSubmit={(event) => {
              event.preventDefault();
              const field = event.currentTarget.elements.namedItem("assets") as HTMLInputElement;
              const assets = field.value.split(",").map((entry) => entry.trim()).filter(Boolean);
              if (assets.length > 0) onUpdateAssets(action, assets);
            }}
          >
            <label htmlFor={`assets-${action.id}`}>Change the assets in your reports</label>
            <div className={styles.assetFormRow}>
              <input id={`assets-${action.id}`} name="assets" className={styles.assetInput} placeholder="Bitcoin, Ethereum" autoComplete="off" />
              <button type="submit" className={styles.ghostButton}>Update</button>
            </div>
          </form>
          <button type="button" className={styles.ghostButton} onClick={() => onCancelSubscription(action)}>Cancel subscription</button>
        </>
      )}
      {error && <p className={styles.inlineError} role="alert">{error}</p>}
    </div>
  );

  // Refunded, reversed, on hold, or declined: the purchase has ended or paused, so the quote no longer applies.
  if (action.paypal && action.state === "failed") {
    return (
      <div className={styles.stack}>
        <div className={styles.actionCard} role="status">
          <p className={styles.actionRunningTitle}>{action.title}</p>
          <p>{action.paypal.message}</p>
          <p className={styles.requestId}>Request ID: {action.decisionId}</p>
        </div>
        {subscriptionRow}
      </div>
    );
  }

  if (action.state === "completed" && action.document) {
    return (
      <div className={styles.stack} id={`result-${action.id}`}>
        {subscriptionRow}
        <ResultCard document={action.document} reportToken={action.reportToken} monthly={Boolean(subscription)} suggestedEmail={action.paypal?.payerEmail ?? null} comparison={comparison}>
        {action.explanationStatus === "pending" && <p className={styles.quietStatus} role="status"><span className={styles.spinner} aria-hidden />Writing a plain-language summary…</p>}
        {action.explanation && (
          <div className={styles.explanation}>
            <span className={styles.aiLabel}>Selun AI summary</span>
            <RichText text={action.explanation} />
          </div>
        )}
        {action.explanationStatus === "unavailable" && <p className={styles.quietStatus}>A written summary isn&apos;t available right now, but the result is complete.</p>}
        </ResultCard>
      </div>
    );
  }

  if (action.expired) {
    return (
      <div className={styles.expiredLine} role="status">
        <span><strong>{simpleCard(action)?.title ?? action.title}</strong> · Previous quote expired. Nothing was charged.</span>
        <button type="button" className={styles.textButton} onClick={() => onNewQuote(action)}>Refresh</button>
      </div>
    );
  }

  if (action.state === "declined") {
    return <p className={styles.quietStatus}>You skipped {action.title}. Nothing was charged.</p>;
  }

  if (action.state === "executing") {
    return (
      <div className={styles.actionCard} role="status">
        <div className={styles.runningRow}>
          <span className={styles.spinner} aria-hidden />
          <div>
            <p className={styles.actionRunningTitle}>
              {action.paypal?.phase === "payment_pending"
                ? `Waiting for payment: ${action.title}`
                : action.paypal?.phase === "delayed"
                  ? `Delayed: ${action.title}`
                  : action.paypal?.phase === "needs_selection"
                    ? `Choose assets: ${action.title}`
                    : `Running ${action.title}`}
            </p>
            <p className={styles.actionRunningHint}>{action.paypal?.message ?? `${action.duration}. You can keep chatting, and this will update here when it's done.`}</p>
          </div>
        </div>
        {subscriptionRow}
      </div>
    );
  }

  const failed = action.state === "failed";
  const simple = simpleCard(action);
  const defaulted = new Set(action.assumptions.map((assumption) => DEFAULTED_DETAIL[assumption]));
  const defaults = simple ? action.details.filter((detail) => defaulted.has(detail.label)) : [];
  // The simple card's heading already names the assets, and defaults wait in the section below.
  const shownDetails = simple ? action.details.filter((detail) => detail.label !== "Assets" && !defaulted.has(detail.label)) : action.details;
  // A subscription's or report's terms are part of the decision, so they stay in view.
  const limitationUpFront = TERMS_UP_FRONT.has(action.capabilityId) && action.limitations;
  const hasMore = Boolean(action.limitations && !limitationUpFront) || (action.coverage?.length ?? 0) > 0 || defaults.length > 0;
  return (
    <article className={styles.actionCard} aria-label={`${simple?.title ?? action.title} quote`}>
      <header className={styles.actionHeader}>
        <div>
          {simple && <p className={styles.actionEyebrow}>{simple.eyebrow}</p>}
          <h3>{simple?.title ?? action.title}</h3>
          <p>{simple?.lead ?? action.summary}</p>
        </div>
        <div className={styles.price}>
          <strong>{formatUsdc(action.priceUsdc)}</strong>
          {action.billing === "monthly" && <span>/ month</span>}
        </div>
      </header>

      {simple && (
        <ul className={styles.includes} aria-label="What you get">
          {simple.includes.map((item) => <li key={item}>{item}</li>)}
        </ul>
      )}
      {shownDetails.length > 0 && (
        <dl className={styles.details}>
          {shownDetails.map((detail) => (
            <div key={detail.label}><dt>{detail.label}</dt><dd>{detail.value}</dd></div>
          ))}
        </dl>
      )}
      {!simple && action.assumptions.length > 0 && <p className={styles.assumptions}>Selun chose these defaults for you. Just say so if you&apos;d like something different.</p>}
      {simple?.assurance && <p className={styles.cardNote}>{simple.assurance}</p>}
      {simple && <p className={styles.cardNote}>{simple.note}</p>}
      {limitationUpFront && <p className={styles.limitation}>{action.limitations}</p>}
      {hasMore && (
        <details className={styles.moreInfo}>
          <summary>{simple?.moreLabel ?? "What's included"}</summary>
          <p className={styles.assumptions}>{action.duration}.</p>
          {action.limitations && !limitationUpFront && <p className={styles.limitation}>{action.limitations}</p>}
          <CoverageList items={action.coverage} />
          {defaults.length > 0 && (
            <div className={styles.defaultSettings}>
              <p>Settings Selun chose for you: {defaults.map((detail) => `${detail.label}: ${detail.value}`).join(" · ")}.</p>
              <button type="button" className={styles.textButton} disabled={busy} onClick={() => onAsk(`I'd like to change the settings for the ${simple?.title ?? action.title}`)}>Customize</button>
            </div>
          )}
        </details>
      )}

      {failed && action.failure && (
        <div className={styles.inlineError} role="alert">
          <p>{action.failure.message}</p>
          {action.failure.mayHaveCharged && <p className={styles.requestId}>Request ID: {action.decisionId}</p>}
        </div>
      )}
      {(error ?? (action.paypal?.phase === "awaiting_payment" ? action.paypal.message : null)) && (
        <p className={styles.inlineError} role="alert">{error ?? action.paypal?.message}</p>
      )}

      {busy && phase === "connecting" && walletPairing && (
        <div className={styles.walletQr} role="group" aria-label="Connect a phone wallet">
          {/* eslint-disable-next-line @next/next/no-img-element -- a data URL drawn in this browser */}
          <img src={walletPairing.dataUrl} alt="QR code to connect your wallet" width={220} height={220} />
          <p>Scan with your wallet app, such as Coinbase Wallet, MetaMask, or Rainbow. You&apos;ll approve the exact amount there.</p>
          <div className={styles.walletQrActions}>
            <a href={walletPairing.uri} className={styles.textButton}>Open wallet app on this phone</a>
            <button type="button" className={styles.textButton} onClick={() => onCancelPairing?.(action)}>Cancel</button>
          </div>
        </div>
      )}
      {busy ? (
        <div className={styles.actionButtons}>
          <button type="button" className={styles.approveButton} disabled>{PHASE_LABEL[phase]}</button>
        </div>
      ) : failed ? (
        // Only a wallet run can fail here, and retrying on it reuses the same request without paying again.
        action.failure?.retryable && retryOption && (
          <div className={styles.actionButtons}>
            <button type="button" className={styles.approveButton} onClick={() => pay(retryOption)}>Try again</button>
          </div>
        )
      ) : options.length === 0 ? null : (
        <>
          {previous && (
            <div className={styles.previousPurchase}>
              <p>
                You already have a {simple?.title ?? action.title}{formatDate(previous.at) ? ` from ${formatDate(previous.at)}` : ""}.
                Would you like to review it, or buy an updated analysis below?
              </p>
              <button type="button" className={styles.textButton} onClick={previous.onReview}>Review your previous analysis</button>
            </div>
          )}
          <fieldset className={styles.paymentChoice} disabled={checking}>
            <legend>Payment method</legend>
            {options.map((option) => (
              <label key={option.id} className={styles.paymentRow}>
                <input type="radio" name={`payment-${action.id}`} checked={selected?.id === option.id} onChange={() => setSelectedId(option.id)} />
                <strong>{option.label}</strong>
                {option.detail && <span>{option.detail}</span>}
              </label>
            ))}
          </fieldset>
          {selected?.flow === "wallet_signature" && phoneWalletAvailable && (
            <fieldset className={styles.walletChoice} disabled={checking}>
              <legend>Sign with</legend>
              <label>
                <input type="radio" name={`wallet-${action.id}`} checked={walletMethod === "browser"} onChange={() => setWalletChoice("browser")} />
                {browserWallet === false ? "A wallet in this browser (none found)" : "A wallet in this browser"}
              </label>
              <label>
                <input type="radio" name={`wallet-${action.id}`} checked={walletMethod === "phone"} onChange={() => setWalletChoice("phone")} />
                A phone wallet (scan a QR code)
              </label>
            </fieldset>
          )}
          <div className={styles.actionButtons}>
            <button type="button" className={styles.approveButton} disabled={!selected || checking} onClick={() => void continueToPay()}>
              {checking ? "Checking…" : selected ? `Continue with ${selected.shortLabel}` : "Choose a payment method"}
            </button>
            <button type="button" className={styles.ghostButton} onClick={() => onDecline(action)}>Not now</button>
          </div>
        </>
      )}

      {busy && chosen && <p className={styles.approvalNote}>{chosen.note}</p>}
      {!busy && !failed && options.length > 0 && <p className={styles.approvalNote}>{selected?.note ?? "Nothing is charged until you approve."}</p>}
      {options.length === 0 && !failed && <p className={styles.approvalNote}>Payment isn&apos;t available for this right now, and nothing has been charged.</p>}
    </article>
  );
}

/** Only Sagitta's own HTTPS sites become links. The data is a server snapshot, so this is a backstop. */
function sagittaLink(url: string | null): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    const own = parsed.hostname === "sagitta.systems" || parsed.hostname.endsWith(".sagitta.systems") || parsed.hostname === "sagitta-protocol.gitbook.io";
    return parsed.protocol === "https:" && own ? url : null;
  } catch {
    return null;
  }
}

/** A day from the public record, such as 2026-07-29, in the reader's own format and without a timezone shift. */
function formatDay(value: string | null | undefined): string | null {
  if (!value) return null;
  const date = new Date(/^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T12:00:00` : value);
  return Number.isNaN(date.getTime()) ? null : date.toLocaleDateString(undefined, { dateStyle: "medium" });
}

/** A status from Sagitta's public record, always with the day it was reported, so it can't pass for a live health check. */
function StatusBadge({ status, asOf }: { status: string; asOf?: string | null }) {
  const day = formatDay(asOf);
  return (
    <span className={styles.statusGroup}>
      <span className={status === "Operating" ? styles.systemStatusLive : styles.systemStatus}>{status}</span>
      {day && <span className={styles.statusAsOf}>status reported {day}</span>}
    </span>
  );
}

function SystemCard({ system }: { system: SystemDetail }) {
  const href = sagittaLink(system.url);
  return (
    <article className={styles.systemCard} aria-label={`${system.name} information`}>
      <header className={styles.systemHeader}>
        <h3>{system.name}</h3>
        <StatusBadge status={system.status} asOf={system.statusAsOf ?? system.lastVerifiedAt} />
      </header>
      <p>{system.summary}</p>
      {system.parent && <p className={styles.systemMeta}>Part of {system.parent}</p>}
      {system.availableToday.length > 0 && (
        <>
          <h4 className={styles.sectionTitle}>Available today</h4>
          <ul className={styles.resultList}>{system.availableToday.map((item) => <li key={item}>{item}</li>)}</ul>
        </>
      )}
      <footer className={styles.systemFooter}>
        {href && <a className={styles.systemLink} href={href} target="_blank" rel="noopener noreferrer">Visit {system.name}<span className={styles.srOnly}> (opens in a new tab)</span></a>}
      </footer>
    </article>
  );
}

function SourcesCard({ sources, readAt }: { sources: SiteSource[]; readAt: string }) {
  const read = formatDate(readAt);
  return (
    <div className={styles.sources}>
      <span className={styles.sourcesLabel}>From Sagitta&apos;s websites</span>
      <ul>
        {sources.map((source) => {
          const href = sagittaLink(source.url);
          return <li key={source.url}>{href ? <a href={href} target="_blank" rel="noopener noreferrer">{source.title}<span className={styles.srOnly}> (opens in a new tab)</span></a> : source.title}</li>;
        })}
      </ul>
      {read && <span className={styles.sourcesRead}>Read {read}</span>}
    </div>
  );
}

function DirectoryCard({ systems }: { systems: SystemSummary[] }) {
  return (
    <ul className={styles.capabilityGrid} aria-label="Sagitta systems">
      {systems.map((system) => {
        const href = sagittaLink(system.url);
        return (
          <li key={system.slug} className={styles.capability}>
            <div className={styles.capabilityTop}>
              <h3>{system.name}</h3>
              <StatusBadge status={system.status} asOf={system.statusAsOf} />
            </div>
            <p>{system.summary}</p>
            {href && <a className={styles.systemLink} href={href} target="_blank" rel="noopener noreferrer">Visit site<span className={styles.srOnly}>: {system.name} (opens in a new tab)</span></a>}
          </li>
        );
      })}
    </ul>
  );
}

function CapabilitiesCard({ items, onAsk }: { items: PublicCapability[]; onAsk: (text: string) => void }) {
  return (
    <ul className={styles.capabilityGrid}>
      {items.map((item) => (
        <li key={item.id} className={styles.capability}>
          <div className={styles.capabilityTop}>
            <h3>{item.title}</h3>
            <span className={styles.capabilityPrice}>{formatUsdc(item.priceUsdc)}{item.billing === "monthly" ? "/month" : ""}</span>
          </div>
          <p>{item.summary}</p>
          <button type="button" className={styles.textButton} onClick={() => onAsk(`Tell me more about ${item.title}`)}>Tell me more</button>
        </li>
      ))}
    </ul>
  );
}

export function BlockView(props: {
  block: Block;
  phase: ApprovalPhase;
  actionError: string | null;
  referralHref: (href: string) => string;
  onCheckQuote: (action: ActionView) => Promise<ActionView | null>;
  onNewQuote: (action: ActionView) => void;
  onApprove: (action: ActionView, option: PaymentOption, wallet?: WalletMethod) => void;
  onDecline: (action: ActionView) => void;
  onCancelSubscription: (action: ActionView) => void;
  onUpdateAssets: (action: ActionView, assets: string[]) => void;
  onAsk: (text: string) => void;
  previousFor?: (action: ActionView) => PreviousPurchase | null;
  comparisonFor?: (action: ActionView) => ComparisonRow[] | null;
  disabled: boolean;
  walletPairing?: WalletPairing | null;
  phoneWalletAvailable?: boolean;
  onCancelPairing?: (action: ActionView) => void;
}) {
  const { block } = props;
  switch (block.type) {
    case "action":
      return <ActionCard action={block.action} phase={props.phase} error={props.actionError} onUpdateAssets={props.onUpdateAssets} onCheckQuote={props.onCheckQuote} onNewQuote={props.onNewQuote} previous={props.previousFor?.(block.action) ?? null} comparison={props.comparisonFor?.(block.action) ?? null} onApprove={props.onApprove} onDecline={props.onDecline} onCancelSubscription={props.onCancelSubscription} onAsk={props.onAsk} walletPairing={props.walletPairing} phoneWalletAvailable={props.phoneWalletAvailable} onCancelPairing={props.onCancelPairing} />;
    case "sagitta_sources":
      return <SourcesCard sources={block.sources} readAt={block.readAt} />;
    case "sagitta_system":
      return <SystemCard system={block.system} />;
    case "sagitta_directory":
      return <DirectoryCard systems={block.systems} />;
    case "capabilities":
      return <CapabilitiesCard items={block.items} onAsk={props.onAsk} />;
    case "asset_choice":
      return (
        <div className={styles.choice}>
          <p>{block.reason}</p>
          <div className={styles.chips}>
            {block.options.map((option) => (
              <button key={option.symbol} type="button" className={styles.chip} disabled={props.disabled} onClick={() => props.onAsk(`I mean ${option.label}`)}>{option.label}</button>
            ))}
          </div>
        </div>
      );
    case "handoff":
      return (
        <article className={styles.handoff}>
          <h3>{block.title}</h3>
          <p>{block.summary}</p>
          <ul className={styles.handoffPrices}>
            {block.prices.map((price) => <li key={price.label}><span>{price.label}</span><strong>{formatUsdc(price.priceUsdc)}</strong></li>)}
          </ul>
          <Link href={props.referralHref(block.href)} className={styles.approveButton}>Start my plan</Link>
          <p className={styles.approvalNote}>{block.note}</p>
        </article>
      );
    case "live_feed":
      return (
        <article className={styles.handoff} aria-label={block.title}>
          <h3>{block.title}</h3>
          <p>{block.summary}</p>
          <a href={block.href} target="_blank" rel="noopener noreferrer" className={styles.approveButton}>{block.linkLabel}</a>
          <p className={styles.approvalNote}>{block.note}</p>
        </article>
      );
    case "live_status":
      return <LiveStatusCard block={block} />;
    case "comparison":
      return (
        <AssetGrid
          title={block.title}
          rows={block.rows.map((row) => ({ ...row, key: `${row.decisionId}:${row.asset}`, source: row.analyzedAt ? `Analysis · ${utcDay(row.analyzedAt) ?? "date not reported"}` : "Your analysis", current: false }))}
        />
      );
    case "notice":
      return <p className={block.tone === "warning" ? styles.noticeWarning : styles.noticeInfo} role={block.tone === "warning" ? "alert" : "status"}>{block.text}</p>;
  }
}

function SeverityTag({ severity }: { severity: string }) {
  const level = severity.toLowerCase();
  const tone = level === "critical" || level === "high" ? styles.severityHigh : level === "warning" || level === "medium" ? styles.severityMedium : styles.severityLow;
  return <span className={`${styles.severityTag} ${tone}`}>{level}</span>;
}

/** The card's times are UTC, so its dates are too. */
function utcDay(value: string | null | undefined): string | null {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toLocaleDateString(undefined, { dateStyle: "medium", timeZone: "UTC" });
}

/** What SCE showed when the customer asked: Radar's alerts and recent advisories, each with the time it was read. */
function LiveStatusCard({ block }: { block: Extract<Block, { type: "live_status" }> }) {
  const { alerts, advisories } = block;
  const scope = alerts.scope ? ` involving ${alerts.scope}` : "";
  return (
    <article className={styles.handoff} aria-label={block.title}>
      <h3>{block.title}</h3>
      <section className={styles.liveSection} aria-label="Radar alerts">
        <h4>Radar alerts <span className={styles.statusAsOf}>as of {alerts.asOf}</span></h4>
        {alerts.state === "unavailable" ? (
          <p className={styles.liveUnavailable}>Radar&apos;s alert feed couldn&apos;t be reached, so this couldn&apos;t be checked.</p>
        ) : alerts.active.length ? (
          <ul className={styles.liveList}>
            {alerts.active.map((alert) => (
              <li key={alert.id}>
                <SeverityTag severity={alert.severity} />
                <a href={alert.url} target="_blank" rel="noopener noreferrer">{alert.summary}</a>
                <span className={styles.liveMeta}>{alert.monitorType} · since {utcDay(alert.createdAt)}</span>
              </li>
            ))}
          </ul>
        ) : (
          <>
            <p>No active alerts{scope}.</p>
            {alerts.latest && (
              <p className={styles.liveMeta}>
                Most recent: <a href={alerts.latest.url} target="_blank" rel="noopener noreferrer">{alerts.latest.summary}</a>{" "}
                ({alerts.latest.severity}, {alerts.latest.resolvedAt ? `resolved ${utcDay(alerts.latest.resolvedAt)}` : utcDay(alerts.latest.createdAt)})
              </p>
            )}
          </>
        )}
      </section>
      {advisories && (
        <section className={styles.liveSection} aria-label="Security advisories">
          <h4>Security advisories <span className={styles.statusAsOf}>past {advisories.windowDays} days</span></h4>
          {advisories.state === "unavailable" ? (
            <p className={styles.liveUnavailable}>SCE&apos;s case library couldn&apos;t be reached, so this couldn&apos;t be checked.</p>
          ) : advisories.items.length ? (
            <ul className={styles.liveList}>
              {advisories.items.map((item) => (
                <li key={item.id}>
                  <SeverityTag severity={item.severity} />
                  {item.url ? <a href={item.url} target="_blank" rel="noopener noreferrer">{item.title}</a> : <span>{item.title}</span>}
                  <span className={styles.liveMeta}>{[item.cve, item.source, `updated ${utcDay(item.updatedAt)}`].filter(Boolean).join(" · ")}</span>
                </li>
              ))}
              {advisories.total > advisories.items.length && <li className={styles.liveMeta}>{advisories.total - advisories.items.length} more in SCE&apos;s case library.</li>}
            </ul>
          ) : (
            <p>No crypto-related advisories found.</p>
          )}
        </section>
      )}
      <a href={block.href} target="_blank" rel="noopener noreferrer" className={styles.approveButton}>{block.linkLabel}</a>
      <p className={styles.approvalNote}>{block.note}</p>
    </article>
  );
}
