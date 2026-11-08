import { createHash } from "node:crypto";
import { PDFDocument, StandardFonts, rgb, type PDFFont, type PDFPage } from "pdf-lib";
import type { ResultDocument, ResultSection, Tone } from "./result-presenter";

/**
 * Renders a ResultDocument as a PDF. It only reads the server's frozen document, so nothing
 * from a browser or the model can reach it. Rendering is deterministic: the same document
 * always produces the same bytes.
 */

const PAGE_WIDTH = 595;
const PAGE_HEIGHT = 842;
const MARGIN = 48;
const CONTENT_WIDTH = PAGE_WIDTH - MARGIN * 2;

const INK = rgb(0.06, 0.13, 0.22);
const MUTED = rgb(0.36, 0.43, 0.52);
const RULE = rgb(0.84, 0.88, 0.93);
const BRAND = rgb(0.05, 0.42, 0.68);
const TONE_COLOR: Record<Tone, ReturnType<typeof rgb>> = {
  positive: rgb(0.05, 0.45, 0.28),
  neutral: INK,
  caution: rgb(0.6, 0.36, 0.02),
  negative: rgb(0.7, 0.13, 0.13),
};

function stable(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value as object)
        .sort()
        .filter((key) => (value as Record<string, unknown>)[key] !== undefined)
        .map((key) => [key, stable((value as Record<string, unknown>)[key])]),
    );
  }
  return value;
}

/** Fingerprint of the certified content, printed in the PDF and kept with the record. */
export function resultRecordHash(document: ResultDocument): string {
  return createHash("sha256").update(JSON.stringify(stable(document))).digest("hex");
}

/** The standard PDF fonts only cover WinAnsi, and an unsupported character would throw. */
function printable(value: string): string {
  return value
    .replace(/[‘’]/g, "'")
    .replace(/[“”]/g, '"')
    .replace(/[–—]/g, "-")
    .replace(/…/g, "...")
    .replace(/[^\x20-\x7E\xA0-\xFF]/g, "?");
}

/** A fixed format and time zone, so the output doesn't depend on the server's locale. */
function readableDate(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : `${date.toISOString().slice(0, 16).replace("T", " ")} UTC`;
}

function wrap(text: string, font: PDFFont, size: number, width: number): string[] {
  const lines: string[] = [];
  let line = "";
  for (const raw of printable(text).split(/\s+/).filter(Boolean)) {
    // Hashes and ids have no spaces, so long words are split to stay on the page.
    let word = raw;
    while (font.widthOfTextAtSize(word, size) > width) {
      let cut = word.length - 1;
      while (cut > 1 && font.widthOfTextAtSize(word.slice(0, cut), size) > width) cut -= 1;
      if (line) lines.push(line);
      lines.push(word.slice(0, cut));
      line = "";
      word = word.slice(cut);
    }
    const candidate = line ? `${line} ${word}` : word;
    if (font.widthOfTextAtSize(candidate, size) > width) {
      lines.push(line);
      line = word;
    } else {
      line = candidate;
    }
  }
  if (line) lines.push(line);
  return lines;
}

class Layout {
  private page!: PDFPage;
  private y = 0;

  constructor(
    private readonly pdf: PDFDocument,
    readonly regular: PDFFont,
    readonly bold: PDFFont,
  ) {
    this.addPage();
  }

  private addPage() {
    this.page = this.pdf.addPage([PAGE_WIDTH, PAGE_HEIGHT]);
    this.y = PAGE_HEIGHT - MARGIN;
  }

  ensure(height: number) {
    if (this.y - height < MARGIN + 24) this.addPage();
  }

  gap(height: number) {
    this.y -= height;
  }

  text(value: string, options: { size?: number; font?: PDFFont; color?: ReturnType<typeof rgb>; x?: number; width?: number } = {}) {
    const size = options.size ?? 10;
    const font = options.font ?? this.regular;
    const x = options.x ?? MARGIN;
    const lineHeight = size * 1.4;
    for (const line of wrap(value, font, size, options.width ?? CONTENT_WIDTH - (x - MARGIN))) {
      this.ensure(lineHeight);
      this.y -= lineHeight;
      this.page.drawText(line, { x, y: this.y, size, font, color: options.color ?? INK });
    }
  }

  /** A label and value on one row. */
  row(label: string, value: string, options: { color?: ReturnType<typeof rgb>; font?: PDFFont; size?: number } = {}) {
    const labelWidth = 150;
    const font = options.font ?? this.bold;
    const size = options.size ?? 10;
    const lines = wrap(value, font, size, CONTENT_WIDTH - labelWidth);
    const height = lines.length * 14 + 4;
    this.ensure(height);
    this.y -= 14;
    this.page.drawText(printable(label), { x: MARGIN, y: this.y, size: 10, font: this.regular, color: MUTED });
    lines.forEach((line, index) => {
      if (index > 0) this.y -= 14;
      this.page.drawText(line, { x: MARGIN + labelWidth, y: this.y, size, font, color: options.color ?? INK });
    });
    this.y -= 4;
  }

  rule() {
    this.ensure(10);
    this.y -= 6;
    this.page.drawLine({ start: { x: MARGIN, y: this.y }, end: { x: PAGE_WIDTH - MARGIN, y: this.y }, thickness: 0.75, color: RULE });
    this.y -= 4;
  }

  heading(value: string) {
    this.ensure(44);
    this.gap(12);
    this.text(value, { size: 12, font: this.bold });
    this.gap(2);
  }
}

const NETWORK_NAME: Record<string, string> = {
  "eip155:8453": "Base",
  "base": "Base",
  "base-mainnet": "Base",
  "eip155:84532": "Base Sepolia (test network)",
  "base-sepolia": "Base Sepolia (test network)",
};

/**
 * Layout 1 is what early reports were issued with, and a frozen report keeps it so its
 * bytes never change. Layout 2 keeps a card on one page and names the payment network.
 */
export const CURRENT_REPORT_LAYOUT = 2;
const ROW_HEIGHT = 18;

function drawSection(layout: Layout, section: ResultSection, keepTogether: boolean) {
  if (keepTogether && section.kind === "metrics") layout.ensure(44 + section.items.length * ROW_HEIGHT);
  switch (section.kind) {
    case "highlight":
      layout.heading(section.label);
      layout.text(section.value, { size: 20, font: layout.bold, color: TONE_COLOR[section.tone] });
      if (section.detail) layout.text(section.detail, { color: MUTED });
      return;
    case "metrics":
      if (section.title) layout.heading(section.title);
      for (const item of section.items) layout.row(item.hint ? `${item.label} (${item.hint})` : item.label, item.value, item.tone ? { color: TONE_COLOR[item.tone] } : {});
      return;
    case "table":
      if (section.title) layout.heading(section.title);
      for (const row of section.rows) {
        const [first, ...rest] = section.columns;
        layout.row(row[first.key] ?? "", rest.map((column) => `${column.label}: ${row[column.key] ?? ""}`).join("   "));
      }
      return;
    case "list":
      if (section.title) layout.heading(section.title);
      for (const item of section.items) layout.text(`- ${item}`);
      return;
    case "note":
      layout.gap(6);
      layout.text(section.text, { color: MUTED });
      return;
    case "heading":
      layout.ensure(60);
      layout.gap(16);
      layout.text(section.text, { size: 15, font: layout.bold, color: BRAND });
      layout.rule();
      return;
  }
}

export async function renderResultPdf(document: ResultDocument, frozenAt: string, reportLayout = 1): Promise<Uint8Array> {
  const modern = reportLayout >= 2;
  const pdf = await PDFDocument.create();
  const fixedDate = new Date(frozenAt);
  pdf.setTitle(`Selun ${document.title} report`);
  pdf.setAuthor("Selun by Sagitta");
  pdf.setSubject(document.provenance.decisionId);
  pdf.setProducer("Selun");
  pdf.setCreator("Selun");
  pdf.setCreationDate(fixedDate);
  pdf.setModificationDate(fixedDate);

  const layout = new Layout(pdf, await pdf.embedFont(StandardFonts.Helvetica), await pdf.embedFont(StandardFonts.HelveticaBold));
  const { provenance } = document;

  layout.text("SELUN BY SAGITTA", { size: 9, font: layout.bold, color: BRAND });
  layout.text(document.title, { size: 22, font: layout.bold });
  layout.text(
    document.snapshotAt ? `Service data as of ${readableDate(document.snapshotAt)}` : "The service did not report a data timestamp.",
    { color: MUTED },
  );
  layout.rule();

  for (const section of document.sections) drawSection(layout, section, modern);

  if (document.coverage.length > 0) {
    layout.heading("What this report includes");
    for (const item of document.coverage) layout.row(item.label, `${item.included ? "Included" : "Not included"}. ${item.note}`, { font: layout.regular });
  }

  layout.gap(10);
  layout.text(document.notice, { font: layout.bold });

  if (modern) layout.ensure(30 + (provenance.paypal ? 8 : 6) * ROW_HEIGHT);
  layout.heading("Purchase and execution record");
  const record = { font: layout.regular, size: 8.5 };
  layout.row("Decision ID", provenance.decisionId, record);
  layout.row("Service endpoint", provenance.endpoint, record);
  if (provenance.paypal) {
    const { paypal } = provenance;
    const provider = paypal.label ?? "PayPal";
    layout.row("Amount paid", `${paypal.amount} ${paypal.currency}`, record);
    layout.row("Payment provider", paypal.environment === "live" ? provider : `${provider} (sandbox test payment)`, record);
    layout.row(`${provider} ${paypal.saleId ? "payment" : "capture"}`, paypal.saleId ?? paypal.captureId ?? "Not reported", record);
    if (paypal.orderId) layout.row(`${provider} order`, paypal.orderId, record);
    if (paypal.subscriptionId) layout.row(`${provider} subscription`, paypal.subscriptionId, record);
    layout.row("Paid by", paypal.payerId ? `${provider} payer ${paypal.payerId}` : "Not reported", record);
  } else {
    layout.row("Amount paid", `${provenance.amountUsdc} USDC`, record);
    layout.row("Payment transaction", provenance.transactionHash ?? "Not reported", record);
    const network = provenance.network ?? "Not reported";
    layout.row("Payment network", modern ? NETWORK_NAME[network] ?? network : network, record);
    layout.row("Paid by", provenance.payer ?? "Not reported", record);
  }
  layout.row("Record fingerprint", `SHA-256 ${resultRecordHash(document)}`, { ...record, size: 8 });

  const pages = pdf.getPages();
  pages.forEach((page, index) => {
    page.drawText("Generated by Selun's server from the stored result of this paid request. Any AI summary in the conversation is not part of this report.", { x: MARGIN, y: 38, size: 7.5, font: layout.regular, color: MUTED });
    page.drawText(`Page ${index + 1} of ${pages.length}`, { x: MARGIN, y: 26, size: 7.5, font: layout.regular, color: MUTED });
  });
  // Object streams off keeps the output byte-stable and simple for any reader.
  return pdf.save({ useObjectStreams: false });
}
