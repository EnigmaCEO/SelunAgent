import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { isValidEmail, sendResultDocumentEmail, type DocumentEmailOutcome } from "../services/email.service";
import { readReportToken } from "../services/job-access.service";
import { AssistantError } from "./assistant-error";
import { getToolSpec } from "./capability-registry";
import type { FulfillmentRecord } from "./commerce";
import { presentResult, type ResultDocument } from "./result-presenter";
import { CURRENT_REPORT_LAYOUT, renderResultPdf, resultRecordHash } from "./result-report";

/**
 * Delivers the report for a paid request. This is the one path for PDF and email, whichever
 * rail paid for the request. A report token can only read a result that already exists, so
 * repeating a download or an email never runs the service or creates a new report.
 */

const MAX_REPORT_RECIPIENTS = 3;
const STALE_SEND_MS = 2 * 60_000;

export type ReportDeps = {
  /** The paid route's stored response, or null when no paid result exists. */
  loadPaidResponse: (capabilityId: string, decisionId: string) => Record<string, unknown> | null;
  /** Throws if this purchase's access has been revoked or put on hold. Checked on every use, so a token alone isn't enough. */
  assertAccess?: (capabilityId: string, decisionId: string) => Promise<void>;
  sendEmail?: typeof sendResultDocumentEmail;
  stateFile?: string;
  now?: () => Date;
};

function defaultStateFile(): string {
  const configured = process.env.SELUN_FULFILLMENT_STATE_FILE?.trim();
  if (configured) return path.resolve(configured);
  return path.resolve(process.cwd(), process.cwd().endsWith("backend") ? "data" : "backend/data", "report-fulfillment.json");
}

function recipientKey(email: string): string {
  return createHash("sha256").update(email.trim().toLowerCase()).digest("hex");
}

function paymentFacts(document: ResultDocument): { amountPaid: string; paymentReference: { label: string; value: string } } {
  const { paypal, amountUsdc, transactionHash } = document.provenance;
  if (paypal) {
    return {
      amountPaid: `${paypal.amount} ${paypal.currency}`,
      paymentReference: { label: `${paypal.label ?? "PayPal"} ${paypal.saleId ? "payment" : "capture"}`, value: paypal.saleId ?? paypal.captureId ?? "" },
    };
  }
  return { amountPaid: `${amountUsdc} USDC`, paymentReference: { label: "Payment transaction", value: transactionHash ?? "" } };
}

export type ReportEmailResult = {
  status: "sent" | "already_sent" | "test_recorded";
  recipientsRemaining: number;
};

export class ReportService {
  private readonly stateFile: string;
  private readonly now: () => Date;
  private records: Record<string, FulfillmentRecord> | null = null;

  constructor(private readonly deps: ReportDeps) {
    this.stateFile = deps.stateFile ?? defaultStateFile();
    this.now = deps.now ?? (() => new Date());
  }

  private all(): Record<string, FulfillmentRecord> {
    if (this.records) return this.records;
    try {
      this.records = JSON.parse(fs.readFileSync(this.stateFile, "utf8")) as Record<string, FulfillmentRecord>;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
        // Starting empty here could lose the frozen reports and repeat customers' emails.
        console.error("Report fulfillment state could not be read:", error instanceof Error ? error.message : error);
        throw new AssistantError("service_unavailable", 503, "Reports are temporarily unavailable. Please try again shortly.");
      }
      this.records = {};
    }
    return this.records;
  }

  private save() {
    fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
    const temp = `${this.stateFile}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(this.records, null, 2), "utf8");
    fs.renameSync(temp, this.stateFile);
  }

  /** The record is frozen on first use, so later code changes can't alter a report already issued. */
  private recordFor(token: unknown): FulfillmentRecord {
    const claim = readReportToken(token);
    if (!claim) throw new AssistantError("report_not_authorized", 403, "This report link isn't valid. Open the report from your conversation.");

    const records = this.all();
    const existing = records[claim.decisionId];
    if (existing) {
      if (existing.capabilityId !== claim.capabilityId) {
        throw new AssistantError("report_not_authorized", 403, "This report link isn't valid. Open the report from your conversation.");
      }
      return existing;
    }

    const spec = getToolSpec(claim.capabilityId);
    const response = spec ? this.deps.loadPaidResponse(claim.capabilityId, claim.decisionId) : null;
    const document: ResultDocument | null = spec && response ? presentResult(claim.capabilityId, spec.title, response) : null;
    if (!document || document.provenance.decisionId !== claim.decisionId) {
      throw new AssistantError("report_not_found", 404, "We couldn't find a paid result for this report.");
    }
    const record: FulfillmentRecord = {
      decisionId: claim.decisionId,
      capabilityId: claim.capabilityId,
      frozenAt: this.now().toISOString(),
      reportLayout: CURRENT_REPORT_LAYOUT,
      recordHash: resultRecordHash(document),
      document,
      downloads: 0,
      emails: [],
    };
    records[claim.decisionId] = record;
    this.save();
    return record;
  }

  private filename(record: FulfillmentRecord): string {
    const safe = record.decisionId.replace(/[^a-zA-Z0-9-]/g, "-").slice(0, 80);
    return `selun-${record.capabilityId.replaceAll("_", "-")}-${safe}.pdf`;
  }

  private async authorized(token: unknown): Promise<FulfillmentRecord> {
    const claim = readReportToken(token);
    if (claim && this.deps.assertAccess) await this.deps.assertAccess(claim.capabilityId, claim.decisionId);
    return this.recordFor(token);
  }

  /** A purchased result, for comparing. The same checks as a download: a valid token, and a purchase that still stands. */
  async documentFor(token: unknown): Promise<ResultDocument> {
    return (await this.authorized(token)).document;
  }

  async download(token: unknown): Promise<{ filename: string; bytes: Uint8Array; recordHash: string }> {
    const record = await this.authorized(token);
    const bytes = await renderResultPdf(record.document, record.frozenAt, record.reportLayout);
    record.downloads += 1;
    this.save();
    return { filename: this.filename(record), bytes, recordHash: record.recordHash };
  }

  /**
   * Sends the report to an address the customer typed and confirmed. The model has no tool for
   * this. Only a hash of the address is stored. Repeats to the same address send nothing new.
   */
  async email(token: unknown, input: { email?: unknown; approved?: unknown }): Promise<ReportEmailResult> {
    const record = await this.authorized(token);
    if (input.approved !== true) throw new AssistantError("approval_required", 400, "Please confirm where to send this report.");
    const email = typeof input.email === "string" ? input.email.trim() : "";
    if (!email || email.length > 254 || !isValidEmail(email)) {
      throw new AssistantError("invalid_email", 400, "Please enter a valid email address.");
    }

    const now = this.now();
    const key = recipientKey(email);
    const delivered = (entry: FulfillmentRecord["emails"][number]) => entry.status === "sent" || entry.status === "test_recorded";
    const remaining = () => MAX_REPORT_RECIPIENTS - record.emails.filter((entry) => delivered(entry) || entry.status === "sending").length;

    const previous = record.emails.find((entry) => entry.recipientHash === key);
    if (previous && delivered(previous)) return { status: "already_sent", recipientsRemaining: remaining() };
    if (previous?.status === "sending" && now.getTime() - Date.parse(previous.updatedAt) < STALE_SEND_MS) {
      return { status: "already_sent", recipientsRemaining: remaining() };
    }
    if (!previous && remaining() <= 0) {
      throw new AssistantError("report_email_limit", 429, "This report has reached its limit of email recipients.");
    }

    // Recorded before any await, so a double click can't send twice.
    const entry = previous ?? { recipientHash: key, status: "sending" as const, updatedAt: now.toISOString() };
    entry.status = "sending";
    entry.updatedAt = now.toISOString();
    if (!previous) record.emails.push(entry);
    this.save();

    let outcome: DocumentEmailOutcome;
    try {
      const bytes = await renderResultPdf(record.document, record.frozenAt, record.reportLayout);
      outcome = await (this.deps.sendEmail ?? sendResultDocumentEmail)({
        toEmail: email,
        decisionId: record.decisionId,
        title: record.document.title,
        filename: this.filename(record),
        pdfBase64: Buffer.from(bytes).toString("base64"),
        ...paymentFacts(record.document),
        recordHash: record.recordHash,
      });
    } catch (error) {
      outcome = { status: "failed", error: error instanceof Error ? error.message : "The report email failed." };
    }

    if (outcome.status === "skipped" || outcome.status === "failed") {
      record.emails.splice(record.emails.indexOf(entry), 1);
      this.save();
      console.error(`Report email for ${record.decisionId} ${outcome.status}:`, outcome.error);
      throw new AssistantError(
        "report_email_failed",
        503,
        outcome.status === "skipped" ? "Email delivery isn't available right now. You can still download the PDF." : "We couldn't send that email. Please try again, or download the PDF instead.",
      );
    }

    entry.status = outcome.status;
    entry.updatedAt = this.now().toISOString();
    this.save();
    return { status: outcome.status, recipientsRemaining: remaining() };
  }
}
