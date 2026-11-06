"use client";

import Link from "next/link";
import { Fragment, useState, type FormEvent, type ReactNode } from "react";
import { ApiError, downloadReport, emailReport } from "./api";
import type { ActionView, Block, CoverageItem, PublicCapability, ResultDocument, ResultSection, SystemDetail, SystemSummary, Tone } from "./types";
import styles from "./chat.module.css";

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
    case "metrics":
      return (
        <div className={styles.section}>
          {section.title && <h4 className={styles.sectionTitle}>{section.title}</h4>}
          <dl className={styles.metrics}>
            {section.items.map((item) => (
              <div key={item.label} className={styles.metric}>
                <dt>{item.label}</dt>
                <dd className={item.tone ? TONE_CLASS[item.tone] : undefined}>{item.value}</dd>
                {item.meter !== undefined && (
                  <span className={styles.meter} aria-hidden>
                    <span style={{ width: `${Math.round(item.meter * 100)}%` }} />
                  </span>
                )}
              </div>
            ))}
          </dl>
        </div>
      );
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

function ReportActions({ reportToken, title, monthly }: { reportToken: string; title: string; monthly: boolean }) {
  const [downloading, setDownloading] = useState(false);
  const [sending, setSending] = useState(false);
  const [email, setEmail] = useState("");
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
      <p className={styles.reportNote}>The report contains the {title} result exactly as the service returned it, not the AI summary. {monthly ? "Each paid month's report goes to your PayPal address, and we only send copies to addresses you enter here." : "We only send it to the address you enter here."}</p>
      {message && <p className={message.error ? styles.inlineError : styles.reportStatus} role={message.error ? "alert" : "status"}>{message.text}</p>}
    </div>
  );
}

function ResultCard({ document, reportToken, monthly = false }: { document: ResultDocument; reportToken?: string | null; monthly?: boolean }) {
  const { provenance } = document;
  const snapshot = formatDate(document.snapshotAt);
  const tx = provenance.transactionHash ?? "";
  const shortTx = `${tx.slice(0, 8)}…${tx.slice(-6)}`;
  const paypalReference = provenance.paypal?.saleId ?? provenance.paypal?.captureId ?? null;
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
      <div className={styles.resultBody}>
        {document.sections.map((section, index) => <SectionView key={index} section={section} />)}
      </div>
      <CoverageList items={document.coverage} />
      {document.notice && <p className={styles.analysisNotice}>{document.notice}</p>}
      <footer className={styles.receipt}>
        {provenance.paypal
          ? <span>Paid {formatUsdc(provenance.paypal.amount)} USD with PayPal{provenance.paypal.environment === "sandbox" ? " (test)" : ""}</span>
          : <span>Paid {formatUsdc(provenance.amountUsdc ?? "")} USDC</span>}
        {provenance.paypal
          ? <span>PayPal reference {paypalReference}</span>
          : provenance.explorerUrl
          ? <a href={provenance.explorerUrl} target="_blank" rel="noopener noreferrer">Receipt {shortTx}<span className={styles.srOnly}> (opens in a new tab)</span></a>
          : <span>Receipt {shortTx}</span>}
      </footer>
      {reportToken && <ReportActions reportToken={reportToken} title={document.title} monthly={monthly} />}
    </article>
  );
}

export type ApprovalPhase = "idle" | "preparing" | "signing" | "submitting" | "redirecting";

const PHASE_LABEL: Record<ApprovalPhase, string> = {
  idle: "",
  preparing: "Confirming price…",
  redirecting: "Opening PayPal…",
  signing: "Check your wallet…",
  submitting: "Sending approval…",
};

function ActionCard(props: {
  action: ActionView;
  phase: ApprovalPhase;
  error: string | null;
  onApprove: (action: ActionView) => void;
  onDecline: (action: ActionView) => void;
  onCancelSubscription: (action: ActionView) => void;
}) {
  const { action, phase, error, onApprove, onDecline, onCancelSubscription } = props;
  const busy = phase !== "idle";
  const comingSoon = action.rails.filter((rail) => rail.status === "coming_soon");
  const usesPayPal = action.execution === "paypal_checkout" || action.execution === "paypal_subscription";
  const monthly = action.execution === "paypal_subscription";
  const subscription = action.paypal?.kind === "subscription" ? action.paypal : null;
  const subscriptionRow = subscription && subscription.subscriptionStatus && subscription.subscriptionStatus !== "created" && (
    <div className={styles.subscriptionRow}>
      <p role="status">
        {subscription.subscriptionStatus === "active"
          ? `Your monthly subscription is active.${subscription.nextBillingTime ? ` Next payment: ${formatDate(subscription.nextBillingTime)}.` : ""} Each paid month's report is also emailed to your PayPal address.`
          : subscription.subscriptionStatus === "cancelled"
            ? "Your subscription is cancelled, so you won't be charged again. Reports you've paid for stay available."
            : `Your subscription is ${subscription.subscriptionStatus}.`}
      </p>
      {subscription.subscriptionStatus === "active" && (
        <button type="button" className={styles.ghostButton} onClick={() => onCancelSubscription(action)}>Cancel subscription</button>
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
      <div className={styles.stack}>
        {subscriptionRow}
        <ResultCard document={action.document} reportToken={action.reportToken} monthly={Boolean(subscription)} />
        {action.explanationStatus === "pending" && <p className={styles.quietStatus} role="status"><span className={styles.spinner} aria-hidden />Writing a plain-language summary…</p>}
        {action.explanation && (
          <div className={styles.explanation}>
            <span className={styles.aiLabel}>Selun AI summary</span>
            <RichText text={action.explanation} />
          </div>
        )}
        {action.explanationStatus === "unavailable" && <p className={styles.quietStatus}>A written summary isn&apos;t available right now, but the result above is complete.</p>}
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
            <p className={styles.actionRunningTitle}>{action.paypal?.phase === "payment_pending" ? `Waiting for PayPal: ${action.title}` : `Running ${action.title}`}</p>
            <p className={styles.actionRunningHint}>{action.paypal?.message ?? `${action.duration}. You can keep chatting, and this will update here when it's done.`}</p>
          </div>
        </div>
        {subscriptionRow}
      </div>
    );
  }

  const failed = action.state === "failed";
  return (
    <article className={styles.actionCard} aria-label={`${action.title} quote`}>
      <header className={styles.actionHeader}>
        <div>
          <h3>{action.title}</h3>
          <p>{action.summary}</p>
        </div>
        <div className={styles.price}>
          <strong>{formatUsdc(action.priceUsdc)}</strong>
          <span>{usesPayPal ? (monthly ? "USD / month" : "USD") : "USDC"}</span>
        </div>
      </header>

      {action.details.length > 0 && (
        <dl className={styles.details}>
          {action.details.map((detail) => (
            <div key={detail.label}><dt>{detail.label}</dt><dd>{detail.value}</dd></div>
          ))}
        </dl>
      )}
      {action.assumptions.length > 0 && <p className={styles.assumptions}>Selun chose these defaults for you. Just say so if you&apos;d like something different.</p>}
      {action.limitations && <p className={styles.limitation}>{action.limitations}</p>}
      <CoverageList items={action.coverage} />

      {failed && action.failure && (
        <div className={styles.inlineError} role="alert">
          <p>{action.failure.message}</p>
          {action.failure.mayHaveCharged && <p className={styles.requestId}>Request ID: {action.decisionId}</p>}
        </div>
      )}
      {(error ?? (action.paypal?.phase === "awaiting_payment" ? action.paypal.message : null)) && (
        <p className={styles.inlineError} role="alert">{error ?? action.paypal?.message}</p>
      )}

      {(!failed || action.failure?.retryable) && (
        <div className={styles.actionButtons}>
          <button type="button" className={styles.approveButton} disabled={busy} onClick={() => onApprove(action)}>
            {busy
              ? PHASE_LABEL[phase]
              : usesPayPal
                ? monthly ? `Subscribe for ${formatUsdc(action.priceUsdc)}/month with PayPal` : `Pay ${formatUsdc(action.priceUsdc)} with PayPal`
                : failed ? "Try again with wallet" : `Approve ${formatUsdc(action.priceUsdc)} with wallet`}
          </button>
          {!failed && <button type="button" className={styles.ghostButton} disabled={busy} onClick={() => onDecline(action)}>Not now</button>}
        </div>
      )}

      <p className={styles.approvalNote}>
        {usesPayPal
          ? monthly
            ? "You'll go to PayPal to approve. PayPal charges this amount each month until you cancel, and you can cancel here or in PayPal at any time."
            : "You'll go to PayPal to approve this exact amount. Nothing is charged until you do."
          : "Nothing is charged until you approve this exact amount in your wallet. Pays in USDC on Base."}
        {!usesPayPal && comingSoon.length > 0 && <> <span className={styles.soon}>PayPal and subscriptions coming soon.</span></>}
      </p>
    </article>
  );
}

/** Only Sagitta's own HTTPS sites become links. The data is a server snapshot, so this is a backstop. */
function sagittaLink(url: string | null): string | null {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" && (parsed.hostname === "sagitta.systems" || parsed.hostname.endsWith(".sagitta.systems")) ? url : null;
  } catch {
    return null;
  }
}

function StatusBadge({ status }: { status: string }) {
  return <span className={status === "Operating" ? styles.systemStatusLive : styles.systemStatus}>{status}</span>;
}

function SystemCard({ system }: { system: SystemDetail }) {
  const href = sagittaLink(system.url);
  return (
    <article className={styles.systemCard} aria-label={`${system.name} information`}>
      <header className={styles.systemHeader}>
        <h3>{system.name}</h3>
        <StatusBadge status={system.status} />
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
        {href && <a href={href} target="_blank" rel="noopener noreferrer">Visit {system.name}<span className={styles.srOnly}> (opens in a new tab)</span></a>}
        {system.lastVerifiedAt && <span>From Sagitta&apos;s public record, checked {system.lastVerifiedAt}</span>}
      </footer>
    </article>
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
              <StatusBadge status={system.status} />
            </div>
            <p>{system.summary}</p>
            {href && <a href={href} target="_blank" rel="noopener noreferrer">Visit<span className={styles.srOnly}> {system.name} (opens in a new tab)</span></a>}
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
            <span className={styles.capabilityPrice}>{formatUsdc(item.priceUsdc)}{item.execution === "paypal_subscription" ? "/month" : ""}</span>
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
  onApprove: (action: ActionView) => void;
  onDecline: (action: ActionView) => void;
  onCancelSubscription: (action: ActionView) => void;
  onAsk: (text: string) => void;
  disabled: boolean;
}) {
  const { block } = props;
  switch (block.type) {
    case "action":
      return <ActionCard action={block.action} phase={props.phase} error={props.actionError} onApprove={props.onApprove} onDecline={props.onDecline} onCancelSubscription={props.onCancelSubscription} />;
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
    case "notice":
      return <p className={block.tone === "warning" ? styles.noticeWarning : styles.noticeInfo} role={block.tone === "warning" ? "alert" : "status"}>{block.text}</p>;
  }
}
