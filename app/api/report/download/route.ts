import { createHash } from "node:crypto";
import { PDFDocument, StandardFonts, degrees, rgb } from "pdf-lib";
import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type AllocationRow = {
  asset?: string;
  name?: string;
  category?: string;
  riskClass?: string;
  allocationPct?: number;
};

type DownloadRequestPayload = {
  riskMode?: string;
  investmentHorizon?: string;
  includeCertifiedDecisionRecord?: boolean;
  totalPriceUsdc?: number;
  walletAddress?: string | null;
  regimeDetected?: string;
  payment?: {
    status?: string;
    transactionId?: string;
    decisionId?: string;
    amountUsdc?: number | string;
    chargedAmountUsdc?: number | string;
    agentNote?: string;
    certifiedDecisionRecordPurchased?: boolean;
    paymentMethod?: "onchain" | "free_code";
    freeCodeApplied?: boolean;
  } | null;
  allocations?: AllocationRow[];
  phase1Artifact?: unknown;
  phase2Artifact?: unknown;
  phase3Artifact?: unknown;
  phase4Artifact?: unknown;
  phase5Artifact?: unknown;
  phase6Artifact?: unknown;
  aaaAllocateDispatch?: unknown;
  decisionRecord?: unknown;
};

type StoreHashResponse = {
  success?: boolean;
  error?: string;
  data?: {
    hashStored?: boolean;
    transactionHash?: string;
  };
};

type VerifyPaymentResponse = {
  success?: boolean;
  error?: string;
  data?: {
    transactionHash?: string;
    amount?: string;
    confirmed?: boolean;
    blockNumber?: number;
  };
};

type PricingResponse = {
  success?: boolean;
  error?: string;
  data?: {
    structuredAllocationPriceUsdc?: number;
    certifiedDecisionRecordFeeUsdc?: number;
  };
};

type ChainAttestationResult = {
  stored: boolean;
  transactionHash?: string;
  error?: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toText(value: unknown, fallback = "n/a"): string {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : fallback;
  }
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  return fallback;
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function readPath(root: unknown, path: string[]): unknown {
  let cursor: unknown = root;
  for (const part of path) {
    if (!isRecord(cursor)) return undefined;
    cursor = cursor[part];
  }
  return cursor;
}

function stableClone(value: unknown, seen?: WeakSet<object>): unknown {
  const visited = seen ?? new WeakSet<object>();

  if (value === null || value === undefined) return value;
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "number" || typeof value === "string" || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.map((entry) => stableClone(entry, visited));
  if (!isRecord(value)) return String(value);

  if (visited.has(value)) return "[Circular]";
  visited.add(value);

  const out: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort((a, b) => a.localeCompare(b))) {
    out[key] = stableClone(value[key], visited);
  }
  return out;
}

function stableStringify(value: unknown, space = 0): string {
  return JSON.stringify(stableClone(value), null, space);
}

function sanitizeFilenamePart(value: string): string {
  const safe = value.replace(/[^a-zA-Z0-9_-]/g, "-").replace(/-+/g, "-").replace(/^-+|-+$/g, "");
  return safe || "report";
}

function wrapTextByChars(text: string, maxChars: number): string[] {
  const source = text.replace(/\r/g, "").split("\n");
  const lines: string[] = [];

  for (const paragraphRaw of source) {
    const paragraph = paragraphRaw.trim();
    if (!paragraph) {
      lines.push("");
      continue;
    }

    const words = paragraph.split(/\s+/);
    let current = "";
    for (const word of words) {
      if (!current) {
        if (word.length <= maxChars) {
          current = word;
          continue;
        }

        let cursor = 0;
        while (cursor < word.length) {
          lines.push(word.slice(cursor, cursor + maxChars));
          cursor += maxChars;
        }
        continue;
      }

      const candidate = `${current} ${word}`;
      if (candidate.length <= maxChars) {
        current = candidate;
        continue;
      }

      lines.push(current);
      if (word.length <= maxChars) {
        current = word;
        continue;
      }

      let cursor = 0;
      while (cursor < word.length) {
        const chunk = word.slice(cursor, cursor + maxChars);
        if (chunk.length < maxChars) {
          current = chunk;
        } else {
          lines.push(chunk);
          current = "";
        }
        cursor += maxChars;
      }
    }

    if (current) lines.push(current);
  }

  return lines;
}

function formatUsdc(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "n/a";
  return `${value.toFixed(2)} USDC`;
}

function formatPct(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "n/a";
  return `${value.toFixed(2).replace(/\.?0+$/, "")}%`;
}

function formatTimestampForReport(value: unknown): string {
  const raw = toText(value, "");
  if (!raw) return "n/a";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toISOString().replace("T", " ").replace("Z", " UTC");
}

function toTitleCase(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
    .join(" ");
}

function mapRoleLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (!normalized) return "Other Holdings";
  if (normalized === "core") return "Core Holdings";
  if (normalized === "defensive" || normalized === "stability") return "Stable Holdings";
  if (normalized === "carry") return "Income Position";
  if (normalized === "satellite") return "Growth Positions";
  if (normalized === "liquidity") return "Liquidity Reserve";
  if (normalized === "speculative" || normalized === "high volatility" || normalized === "high_volatility") {
    return "High-Risk Positions";
  }
  return toTitleCase(value);
}

function mapRoleRationale(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (normalized === "core" || normalized === "core holdings") {
    return "Anchor allocation intended for portfolio stability over time.";
  }
  if (normalized === "defensive" || normalized === "stability" || normalized === "stable holdings") {
    return "Defensive capital buffer to reduce drawdown pressure.";
  }
  if (normalized === "carry" || normalized === "income position") {
    return "Income-focused position with moderate growth support.";
  }
  if (normalized === "satellite" || normalized === "growth positions") {
    return "Growth tilt with tighter sizing controls.";
  }
  if (normalized === "liquidity" || normalized === "liquidity reserve") {
    return "Liquidity reserve used for flexibility and rebalance execution.";
  }
  if (normalized === "speculative" || normalized === "high volatility" || normalized === "high_volatility") {
    return "High-volatility sleeve capped to control downside risk.";
  }
  if (normalized === "high-risk positions") return "High-volatility sleeve capped to control downside risk.";
  return "Supporting position under portfolio diversification controls.";
}

function mapRiskClassLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (!normalized || normalized === "unknown") return "Unspecified";
  if (normalized === "large_cap_crypto") return "Large Cap Crypto";
  if (normalized === "stablecoin") return "Stablecoin";
  return toTitleCase(normalized);
}

function mapPolicyModeLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (normalized === "capital_preservation") return "Capital Preservation";
  if (normalized === "balanced_defensive") return "Balanced Defensive";
  if (normalized === "balanced_growth") return "Balanced Growth";
  if (normalized === "offensive_growth") return "Growth-Focused";
  return toTitleCase(value || "Unspecified");
}

function mapReasonCode(reasonCode: string): string {
  const normalized = reasonCode.trim().toUpperCase();
  if (!normalized) return "Portfolio controls were applied.";
  if (normalized === "ROLE_SLEEVE_TARGETING_APPLIED") return "Role-level targets were applied to keep the portfolio balanced.";
  if (normalized === "ASSET_CAP_ENFORCED") return "Position caps were enforced to reduce concentration risk.";
  if (normalized === "HIGH_VOL_CAP_ACTIVE") return "High-volatility assets were capped to protect downside.";
  if (normalized === "HIGH_VOL_CAP_RELAXED_FOR_FEASIBILITY") return "High-volatility cap was relaxed only to preserve feasibility.";
  if (normalized === "MAX_ASSET_WEIGHT_RELAXED_FOR_FEASIBILITY") return "Single-asset cap was relaxed only to avoid infeasible allocations.";
  return toTitleCase(reasonCode);
}

function mapReasonCodeLabel(reasonCode: string): string {
  const normalized = reasonCode.trim().toUpperCase();
  if (!normalized) return "Portfolio Controls Applied";
  if (normalized === "ROLE_SLEEVE_TARGETING_APPLIED") return "Role Targeting Applied";
  if (normalized === "ASSET_CAP_ENFORCED") return "Asset Cap Enforced";
  if (normalized === "HIGH_VOL_CAP_ACTIVE") return "High-Volatility Cap Active";
  if (normalized === "HIGH_VOL_CAP_RELAXED_FOR_FEASIBILITY") return "High-Volatility Cap Relaxed For Feasibility";
  if (normalized === "MAX_ASSET_WEIGHT_RELAXED_FOR_FEASIBILITY") return "Max Asset Weight Relaxed For Feasibility";
  return toTitleCase(reasonCode.replace(/_/g, " "));
}

function mapMarketConditionLabel(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (normalized === "defensive" || normalized === "risk_off") return "Defensive";
  if (normalized === "expansionary" || normalized === "risk_on" || normalized === "aggressive") return "Growth";
  if (normalized === "neutral") return "Neutral";
  return toTitleCase(value || "Unknown");
}

function mapFearGreedLabel(score: number | null): string {
  if (score === null) return "Unavailable";
  if (score <= 24) return "Extreme Fear";
  if (score <= 44) return "Fear";
  if (score <= 54) return "Neutral";
  if (score <= 74) return "Greed";
  return "Extreme Greed";
}

function mapFearGreedInterpretation(score: number | null): string {
  if (score === null) return "Fear & Greed data was unavailable for this run.";
  if (score <= 44) return "Sentiment is cautious, which supports a more defensive portfolio mix.";
  if (score <= 54) return "Sentiment is balanced, supporting a neutral allocation stance.";
  return "Sentiment is optimistic; upside is possible but overheating risk can rise.";
}

function collectAllocationRows(value: unknown): Array<{ asset: string; name: string; category: string; riskClass: string; allocationPct: number }> {
  if (!Array.isArray(value)) return [];
  const rows: Array<{ asset: string; name: string; category: string; riskClass: string; allocationPct: number }> = [];
  for (const row of value) {
    if (!isRecord(row)) continue;
    const asset = toText(row.asset, "");
    const name = toText(row.name, asset);
    const category = toText(row.category, "unknown");
    const riskClass = toText(row.riskClass, "unknown");
    const allocationPct = toFiniteNumber(row.allocationPct);
    if (!asset || allocationPct === null) continue;
    rows.push({
      asset,
      name,
      category,
      riskClass,
      allocationPct: Math.max(0, Math.min(100, allocationPct)),
    });
  }
  return rows.sort((left, right) => right.allocationPct - left.allocationPct || left.asset.localeCompare(right.asset));
}

function toRecordArray(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return [];
  return value.filter((entry): entry is Record<string, unknown> => isRecord(entry));
}

function toTextArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => toText(entry, "")).filter(Boolean);
}

function humanizeKey(key: string): string {
  const phaseSpaced = key.replace(/phase(\d+)/gi, "phase $1");
  return toTitleCase(phaseSpaced.replace(/_/g, " "));
}

function truncateText(value: string, maxChars: number): string {
  if (value.length <= maxChars) return value;
  return `${value.slice(0, Math.max(0, maxChars - 3))}...`;
}

async function storeHashOnBackend(decisionId: string, pdfHash: string): Promise<ChainAttestationResult> {
  const backendBase = process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";

  try {
    const response = await fetch(`${backendBase}/agent/store-hash`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ decisionId, pdfHash }),
      cache: "no-store",
    });

    const body = (await response.json().catch(() => ({}))) as StoreHashResponse;
    if (!response.ok || !body.success || !body.data?.hashStored || !body.data.transactionHash) {
      return {
        stored: false,
        error: body.error || `store-hash returned HTTP ${response.status}`,
      };
    }

    return {
      stored: true,
      transactionHash: body.data.transactionHash,
    };
  } catch (error) {
    return {
      stored: false,
      error: error instanceof Error ? error.message : "store-hash request failed",
    };
  }
}

async function fetchBackendPricing(): Promise<{ ok: true; requiredCertifiedPriceUsdc: number } | { ok: false; error: string }> {
  const backendBase = process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";

  try {
    const response = await fetch(`${backendBase}/agent/pricing`, {
      method: "GET",
      cache: "no-store",
    });
    const body = (await response.json().catch(() => ({}))) as PricingResponse;
    const base = toFiniteNumber(body?.data?.structuredAllocationPriceUsdc);
    const addOn = toFiniteNumber(body?.data?.certifiedDecisionRecordFeeUsdc);

    if (!response.ok || !body.success || base === null || addOn === null || base < 0 || addOn < 0) {
      return {
        ok: false,
        error: body.error || `pricing request failed (HTTP ${response.status})`,
      };
    }

    return {
      ok: true,
      requiredCertifiedPriceUsdc: base + addOn,
    };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : "pricing request failed",
    };
  }
}

async function verifyPurchaseOnBackend(
  walletAddress: string,
  expectedAmountUsdc: number,
  transactionHash: string,
  decisionId: string,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const backendBase = process.env.SELUN_BACKEND_URL?.trim() || "http://localhost:8787";

  try {
    const response = await fetch(`${backendBase}/agent/verify-payment`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        fromAddress: walletAddress,
        expectedAmountUSDC: expectedAmountUsdc,
        transactionHash,
        decisionId,
      }),
      cache: "no-store",
    });

    const body = (await response.json().catch(() => ({}))) as VerifyPaymentResponse;
    if (!response.ok || !body.success || !body.data?.confirmed) {
      return {
        ok: false,
        error: body.error || `payment verification failed (HTTP ${response.status})`,
      };
    }

    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : "payment verification request failed",
    };
  }
}

async function buildPdf(payload: DownloadRequestPayload, integrityHash: string, chainAttestation: ChainAttestationResult): Promise<Uint8Array> {
  const pdf = await PDFDocument.create();
  const fontRegular = await pdf.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const fontMono = await pdf.embedFont(StandardFonts.Courier);

  const pageWidth = 612;
  const pageHeight = 792;
  const headerHeight = 72;
  const headerBottomY = pageHeight - headerHeight;
  const headerToContentGap = 32;
  const margin = 52;
  const topY = headerBottomY - headerToContentGap;
  const bottomY = 52;
  const pageBodyHeight = topY - bottomY;
  const contentWidth = pageWidth - margin * 2;
  const colors = {
    ink: rgb(0.11, 0.16, 0.24),
    muted: rgb(0.35, 0.42, 0.52),
    brand: rgb(0.05, 0.3, 0.52),
    border: rgb(0.79, 0.84, 0.9),
    cardBg: rgb(0.96, 0.98, 1),
    headerBg: rgb(0.91, 0.96, 1),
  } as const;

  let page = pdf.addPage([pageWidth, pageHeight]);
  let y = topY;

  const drawSelunMark = (x: number, yTop: number, size: number) => {
    const boxY = yTop - size;
    const cx = x + size / 2;
    const cy = boxY + size / 2;

    page.drawRectangle({
      x,
      y: boxY,
      width: size,
      height: size,
      color: rgb(1, 1, 1),
      borderColor: rgb(0.66, 0.83, 0.94),
      borderWidth: 1.1,
    });

    page.drawEllipse({
      x: cx,
      y: cy,
      xScale: size * 0.38,
      yScale: size * 0.2,
      rotate: degrees(-27),
      borderColor: rgb(0.45, 0.8, 0.97),
      borderWidth: 1.7,
    });
    page.drawEllipse({
      x: cx,
      y: cy,
      xScale: size * 0.38,
      yScale: size * 0.2,
      rotate: degrees(27),
      borderColor: rgb(0.23, 0.63, 0.91),
      borderWidth: 1.7,
    });

    page.drawCircle({
      x: cx,
      y: cy,
      size: size * 0.19,
      color: rgb(0.32, 0.73, 0.96),
    });
    page.drawCircle({
      x: cx,
      y: cy,
      size: size * 0.205,
      borderColor: rgb(0.39, 0.78, 0.96),
      borderWidth: 0.8,
      opacity: 0.55,
    });

    page.drawCircle({ x: cx + size * 0.29, y: cy + size * 0.25, size: size * 0.03, color: rgb(0.67, 0.91, 1) });
    page.drawCircle({ x: cx + size * 0.4, y: cy + size * 0.05, size: size * 0.026, color: rgb(0.67, 0.91, 1) });
    page.drawCircle({ x: cx - size * 0.34, y: cy + size * 0.09, size: size * 0.022, color: rgb(0.67, 0.91, 1) });
    page.drawCircle({ x: cx - size * 0.33, y: cy - size * 0.13, size: size * 0.022, color: rgb(0.67, 0.91, 1) });
  };

  const drawPageChrome = () => {
    page.drawRectangle({
      x: 0,
      y: headerBottomY,
      width: pageWidth,
      height: headerHeight,
      color: colors.headerBg,
    });
    page.drawLine({
      start: { x: 0, y: headerBottomY },
      end: { x: pageWidth, y: headerBottomY },
      thickness: 1,
      color: colors.border,
    });
    const logoSize = 28;
    const logoTop = headerBottomY + (headerHeight + logoSize) / 2;
    const leftTextX = margin + logoSize + 8;
    const rightTextX = pageWidth - margin - 170;
    drawSelunMark(margin, logoTop, logoSize);
    page.drawText("SELUN", {
      x: leftTextX,
      y: headerBottomY + 36,
      size: 13,
      font: fontBold,
      color: colors.brand,
    });
    page.drawText("Official Allocation Record", {
      x: leftTextX,
      y: headerBottomY + 21,
      size: 9,
      font: fontRegular,
      color: colors.muted,
    });
    page.drawText("Certified Allocation Record", {
      x: rightTextX,
      y: headerBottomY + 36,
      size: 8.5,
      font: fontBold,
      color: colors.brand,
    });
    page.drawText("Generated by Selun Allocation Agent", {
      x: rightTextX,
      y: headerBottomY + 21,
      size: 8,
      font: fontRegular,
      color: colors.muted,
    });
    y = topY;
  };

  const addPage = () => {
    page = pdf.addPage([pageWidth, pageHeight]);
    drawPageChrome();
  };

  const ensureSpace = (heightNeeded: number) => {
    if (y - heightNeeded < bottomY) addPage();
  };

  const startSectionOnFreshPageIfTight = (minimumRoom: number) => {
    const remaining = y - bottomY;
    if (remaining < minimumRoom && minimumRoom <= pageBodyHeight) {
      addPage();
    }
  };

  const drawSpacer = (height = 6) => {
    ensureSpace(height);
    y -= height;
  };

  const drawWrapped = (
    text: string,
    opts?: {
      font?: typeof fontRegular;
      size?: number;
      color?: ReturnType<typeof rgb>;
      maxChars?: number;
      indent?: number;
      lineGap?: number;
      paragraphGap?: number;
    },
  ) => {
    const size = opts?.size ?? 10;
    const lineGap = opts?.lineGap ?? 5.4;
    const paragraphGap = opts?.paragraphGap ?? 2.8;
    const maxChars = opts?.maxChars ?? 95;
    const indent = opts?.indent ?? 0;
    const lines = wrapTextByChars(text, maxChars);
    for (const line of lines) {
      ensureSpace(size + lineGap);
      page.drawText(line, {
        x: margin + indent,
        y,
        size,
        font: opts?.font ?? fontRegular,
        color: opts?.color ?? colors.ink,
      });
      y -= size + lineGap;
    }
    if (paragraphGap > 0) {
      ensureSpace(paragraphGap);
      y -= paragraphGap;
    }
  };

  const drawSectionTitle = (title: string, subtitle?: string) => {
    ensureSpace(subtitle ? 52 : 36);
    page.drawLine({
      start: { x: margin, y: y + 7 },
      end: { x: margin + contentWidth, y: y + 7 },
      thickness: 1,
      color: colors.border,
    });
    y -= 14;
    drawWrapped(title, {
      font: fontBold,
      size: 12,
      color: colors.brand,
      maxChars: 80,
      lineGap: 3,
      paragraphGap: 0,
    });
    if (subtitle) {
      drawWrapped(subtitle, {
        size: 9.5,
        color: colors.muted,
        maxChars: 92,
        lineGap: 3,
        paragraphGap: 0,
      });
    }
    y -= 8;
  };

  const drawCard = (
    x: number,
    top: number,
    width: number,
    height: number,
    label: string,
    value: string,
    note?: string,
  ) => {
    page.drawRectangle({
      x,
      y: top - height,
      width,
      height,
      color: colors.cardBg,
      borderColor: colors.border,
      borderWidth: 1,
    });
    page.drawText(label, {
      x: x + 10,
      y: top - 16,
      size: 8,
      font: fontBold,
      color: colors.muted,
    });
    page.drawText(value, {
      x: x + 10,
      y: top - 34,
      size: 12,
      font: fontBold,
      color: colors.ink,
    });
    if (note) {
      page.drawText(note, {
        x: x + 10,
        y: top - 49,
        size: 8,
        font: fontRegular,
        color: colors.muted,
      });
    }
  };

  const payment = isRecord(payload.payment) ? payload.payment : null;
  const decisionId = toText(payment?.decisionId, "n/a");
  const txHash = toText(payment?.transactionId, "n/a");
  const purchased = toText(payment?.status, "unknown");
  const totalPaid = toFiniteNumber(payment?.chargedAmountUsdc ?? payment?.amountUsdc);
  const includeCertified = Boolean(payload.includeCertifiedDecisionRecord);
  const allocationRows = collectAllocationRows(payload.allocations);

  const aaaDispatch = isRecord(payload.aaaAllocateDispatch) ? payload.aaaAllocateDispatch : null;
  const aaaResponse = isRecord(aaaDispatch?.response) ? aaaDispatch.response : null;
  const aaaResult = isRecord(readPath(aaaResponse, ["allocation_result"])) ? readPath(aaaResponse, ["allocation_result"]) : null;
  const aaaMeta = isRecord(readPath(aaaResult, ["meta"])) ? readPath(aaaResult, ["meta"]) : null;
  const aaaReasonCodes = Array.isArray(readPath(aaaMeta, ["reason_codes"]))
    ? (readPath(aaaMeta, ["reason_codes"]) as unknown[]).map((entry) => toText(entry, "")).filter(Boolean)
    : [];
  const aaaStatus = toText(readPath(aaaMeta, ["status"]), toText(aaaDispatch?.status, "n/a"));
  const aaaReasonCode = toText(readPath(aaaMeta, ["reason_code"]), "n/a");
  const aaaConstraintsEffective = isRecord(readPath(aaaMeta, ["constraints_effective"]))
    ? (readPath(aaaMeta, ["constraints_effective"]) as Record<string, unknown>)
    : null;
  const aaaInputSanitized = isRecord(readPath(aaaMeta, ["input_assets_sanitized"]))
    ? (readPath(aaaMeta, ["input_assets_sanitized"]) as Record<string, unknown>)
    : null;
  const aaaRoleCounts = isRecord(readPath(aaaMeta, ["role_counts"]))
    ? (readPath(aaaMeta, ["role_counts"]) as Record<string, unknown>)
    : null;

  const phase3Eligible = toText(readPath(payload.phase3Artifact, ["universe", "total_candidates_count"]), "n/a");
  const phase4Eligible = toText(readPath(payload.phase4Artifact, ["screening", "eligible_candidates_count"]), "n/a");
  const phase5Selected = toText(readPath(payload.phase5Artifact, ["evaluation", "selected_candidates_count"]), "n/a");
  const phase6Selected = toText(readPath(payload.phase6Artifact, ["allocation", "selected_candidates_count"]), "n/a");
  const riskProfile = toText(payload.riskMode, "n/a");
  const horizon = toText(payload.investmentHorizon, "n/a");
  const strategyMode = mapPolicyModeLabel(toText(readPath(payload.phase2Artifact, ["allocation_policy", "mode"]), "unspecified"));
  const marketCondition = mapMarketConditionLabel(
    toText(readPath(payload.phase1Artifact, ["market_condition", "risk_appetite"]), toText(readPath(aaaMeta, ["market_regime"]), "")),
  );
  const volatilityLabel = toTitleCase(toText(readPath(payload.phase1Artifact, ["market_condition", "volatility_state"]), "unknown"));
  const liquidityLabel = toTitleCase(toText(readPath(payload.phase1Artifact, ["market_condition", "liquidity_state"]), "unknown"));
  const confidenceRaw =
    toFiniteNumber(readPath(payload.phase1Artifact, ["market_condition", "confidence"])) ??
    toFiniteNumber(readPath(aaaMeta, ["market_regime_confidence"]));
  const confidencePct = confidenceRaw !== null ? Math.max(0, Math.min(100, confidenceRaw * 100)) : null;
  const fearGreedAvailable = readPath(payload.phase1Artifact, ["evidence", "sentiment_metrics", "fear_greed_available"]) === true;
  const fearGreedRaw = fearGreedAvailable
    ? toFiniteNumber(readPath(payload.phase1Artifact, ["evidence", "sentiment_metrics", "fear_greed_index"]))
    : null;
  const fearGreedScore = fearGreedRaw !== null ? Math.max(0, Math.min(100, fearGreedRaw)) : null;
  const marketSentiment = toFiniteNumber(readPath(aaaMeta, ["market_sentiment"]));
  const executionTimestamp = formatTimestampForReport(
    readPath(payload.phase6Artifact, ["timestamp"]) ??
      readPath(payload.decisionRecord, ["generatedAt"]) ??
      readPath(payload.decisionRecord, ["timestamp"]),
  );
  const engineVersion = toText(
    readPath(aaaMeta, ["allocator_version_effective"]),
    toText(readPath(aaaMeta, ["allocator"]), toText(readPath(aaaDispatch, ["allocator_version_requested"]), "n/a")),
  );
  const deterministicSignal =
    readPath(payload.phase1Artifact, ["audit", "deterministic"]) ??
    readPath(payload.phase6Artifact, ["audit", "deterministic"]) ??
    readPath(payload.decisionRecord, ["audit", "deterministic"]);
  const deterministicMode = deterministicSignal === false ? "Disabled" : "Enabled";
  const attestationStatus = includeCertified
    ? chainAttestation.stored
      ? "Stored on-chain"
      : "Hash generated (not stored on-chain)"
    : "Not requested";
  const assetCount = allocationRows.length;
  const topAllocation = allocationRows.length > 0 ? allocationRows[0].allocationPct : null;
  const top3Allocation =
    allocationRows.slice(0, 3).reduce((sum, row) => sum + row.allocationPct, 0);

  const roleTotals = new Map<string, number>();
  for (const row of allocationRows) {
    const label = mapRoleLabel(row.category);
    roleTotals.set(label, (roleTotals.get(label) ?? 0) + row.allocationPct);
  }
  const roleOrder = [
    "Stable Holdings",
    "Core Holdings",
    "Income Position",
    "Growth Positions",
    "Liquidity Reserve",
    "High-Risk Positions",
  ];
  const roleBreakdown = roleOrder.map((label) => ({ label, pct: roleTotals.get(label) ?? 0 }));
  const nonZeroRoleBreakdown = roleBreakdown.filter((entry) => entry.pct > 0);
  const formatMetric = (value: number | null, digits = 3): string => (value === null ? "n/a" : value.toFixed(digits));
  const toAuditValueText = (value: unknown): string => {
    if (isRecord(value) || Array.isArray(value)) {
      const text = stableStringify(value, 0);
      return text.length > 110 ? `${text.slice(0, 107)}...` : text;
    }
    return toText(value, "n/a");
  };

  const phase2PolicyRules = toTextArray(readPath(payload.phase2Artifact, ["audit", "policy_rules_applied"]));
  const phase3SelectionRules = toTextArray(readPath(payload.phase3Artifact, ["audit", "selection_rules"]));
  const phase4SelectionRules = toTextArray(readPath(payload.phase4Artifact, ["audit", "selection_rules"]));
  const phase2PolicyEnvelope = isRecord(readPath(payload.phase2Artifact, ["policy_envelope"]))
    ? (readPath(payload.phase2Artifact, ["policy_envelope"]) as Record<string, unknown>)
    : null;
  const phase4Thresholds = isRecord(readPath(payload.phase4Artifact, ["inputs", "screening_thresholds"]))
    ? (readPath(payload.phase4Artifact, ["inputs", "screening_thresholds"]) as Record<string, unknown>)
    : null;
  const phase5Constraints = isRecord(readPath(payload.phase5Artifact, ["inputs", "portfolio_constraints"]))
    ? (readPath(payload.phase5Artifact, ["inputs", "portfolio_constraints"]) as Record<string, unknown>)
    : null;

  const phase1Sources = toRecordArray(readPath(payload.phase1Artifact, ["audit", "sources"]));
  const phase3Sources = toRecordArray(readPath(payload.phase3Artifact, ["audit", "sources"]));
  const sourceRegistry = new Map<string, { provider: string; endpoint: string; url: string; fetchedAt: string }>();
  for (const source of [...phase1Sources, ...phase3Sources]) {
    const provider = toText(source.provider, "unknown");
    const endpoint = toText(source.endpoint, "n/a");
    const url = toText(source.url, "n/a");
    const fetchedAt = toText(source.fetched_at, "n/a");
    const key = `${provider}|${endpoint}|${url}`;
    if (!sourceRegistry.has(key)) {
      sourceRegistry.set(key, { provider, endpoint, url, fetchedAt });
    }
  }
  const sourceRows = [...sourceRegistry.values()].sort((left, right) => left.provider.localeCompare(right.provider));
  const phase1MissingDomains = toTextArray(readPath(payload.phase1Artifact, ["audit", "missing_domains"]));
  const phase3MissingDomains = toTextArray(readPath(payload.phase3Artifact, ["audit", "missing_domains"]));
  const phase4MissingDomains = toTextArray(readPath(payload.phase4Artifact, ["audit", "missing_domains"]));
  const missingDomains = [...new Set([...phase1MissingDomains, ...phase3MissingDomains, ...phase4MissingDomains])];
  const sourceSelectionRows = toRecordArray(readPath(payload.phase1Artifact, ["audit", "source_selection"])).map((row) => ({
    domain: toText(row.domain, "unknown"),
    selected: toTextArray(row.selected),
    rationale: toTextArray(row.rationale),
  }));

  const phase4Tokens = toRecordArray(readPath(payload.phase4Artifact, ["screening", "tokens"]));
  const excludedTokens = phase4Tokens.filter((token) => token.eligible !== true);
  const exclusionReasonCounts = new Map<string, number>();
  const exclusionExamples: string[] = [];
  for (const token of excludedTokens) {
    const symbol = toText(token.symbol, "unknown");
    const reasons = toTextArray(token.exclusion_reasons);
    const effectiveReasons = reasons.length > 0 ? reasons : ["unspecified_filter"];
    for (const reason of effectiveReasons) {
      exclusionReasonCounts.set(reason, (exclusionReasonCounts.get(reason) ?? 0) + 1);
    }
    if (exclusionExamples.length < 10) {
      exclusionExamples.push(`${symbol}: ${effectiveReasons.slice(0, 2).map((reason) => humanizeKey(reason)).join("; ")}`);
    }
  }
  const topExclusionReasons = [...exclusionReasonCounts.entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, 8);

  const phase5Tokens = toRecordArray(readPath(payload.phase5Artifact, ["evaluation", "tokens"]));
  const selectedPhase5Tokens = phase5Tokens.filter((token) => token.selected === true);
  const selectionReasonCounts = new Map<string, number>();
  const selectionExamples: string[] = [];
  for (const token of selectedPhase5Tokens) {
    const symbol = toText(token.symbol, "unknown");
    const reasons = toTextArray(token.selection_reasons);
    const effectiveReasons = reasons.length > 0 ? reasons : ["selected_by_composite_score"];
    for (const reason of effectiveReasons) {
      selectionReasonCounts.set(reason, (selectionReasonCounts.get(reason) ?? 0) + 1);
    }
    if (selectionExamples.length < 10) {
      selectionExamples.push(`${symbol}: ${effectiveReasons.slice(0, 2).map((reason) => humanizeKey(reason)).join("; ")}`);
    }
  }
  const topSelectionReasons = [...selectionReasonCounts.entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, 8);

  const phase5ScoreRows = selectedPhase5Tokens
    .map((token) => ({
      symbol: toText(token.symbol, "unknown"),
      quality: toFiniteNumber(token.quality_score),
      risk: toFiniteNumber(token.risk_score),
      composite: toFiniteNumber(token.composite_score),
      liquidity: toFiniteNumber(token.liquidity_score),
      structural: toFiniteNumber(token.structural_score),
      volatility: toFiniteNumber(token.volatility),
    }))
    .sort((left, right) => (right.composite ?? -1) - (left.composite ?? -1) || left.symbol.localeCompare(right.symbol));

  const aaaScoreTrace = isRecord(readPath(aaaResult, ["score_trace_by_asset"]))
    ? (readPath(aaaResult, ["score_trace_by_asset"]) as Record<string, unknown>)
    : null;
  const aaaScoreRows = aaaScoreTrace
    ? Object.entries(aaaScoreTrace)
        .filter(([, value]) => isRecord(value))
        .map(([asset, value]) => {
          const record = value as Record<string, unknown>;
          return {
            asset,
            scoreFinal: toFiniteNumber(record.score_final),
            weightFinal: toFiniteNumber(record.weight_final),
            sentiment: toFiniteNumber(record.sentiment_effective),
            quality: toFiniteNumber(record.quality),
            expectedReturn: toFiniteNumber(record.expected_return_used),
            volatility: toFiniteNumber(record.volatility_used),
          };
        })
        .sort((left, right) => (right.weightFinal ?? -1) - (left.weightFinal ?? -1) || left.asset.localeCompare(right.asset))
    : [];

  drawPageChrome();
  drawWrapped("Your Personalized Allocation Plan", {
    font: fontBold,
    size: 22,
    color: colors.ink,
    maxChars: 42,
    lineGap: 4,
    paragraphGap: 0,
  });
  drawWrapped("Model-generated allocation output with plain-language reasoning for this market environment.", {
    size: 10.5,
    color: colors.muted,
    maxChars: 92,
    lineGap: 4,
    paragraphGap: 0,
  });
  drawWrapped(
    "Informational use only. This report is not investment, legal, or tax advice and does not guarantee performance or outcomes.",
    {
      size: 9.2,
      color: colors.muted,
      maxChars: 92,
      lineGap: 3.6,
      paragraphGap: 0,
    },
  );
  drawSpacer(10);

  ensureSpace(132);
  const cardGap = 10;
  const cardWidth = (contentWidth - cardGap) / 2;
  const cardHeight = 58;
  const cardTop = y;
  drawCard(margin, cardTop, cardWidth, cardHeight, "Market Condition", marketCondition, `Confidence ${formatPct(confidencePct)}`);
  drawCard(margin + cardWidth + cardGap, cardTop, cardWidth, cardHeight, "Strategy", strategyMode, `${riskProfile} | ${horizon}`);
  const secondRowTop = cardTop - cardHeight - 10;
  drawCard(
    margin,
    secondRowTop,
    cardWidth,
    cardHeight,
    "Fear & Greed",
    fearGreedScore !== null ? `${Math.round(fearGreedScore)} (${mapFearGreedLabel(fearGreedScore)})` : "Unavailable",
    fearGreedScore !== null ? "Sentiment signal for risk posture." : "Sentiment data unavailable.",
  );
  drawCard(
    margin + cardWidth + cardGap,
    secondRowTop,
    cardWidth,
    cardHeight,
    "Concentration Snapshot",
    `Largest position: ${formatPct(topAllocation)}`,
    `Top 3 combined: ${formatPct(top3Allocation)}`,
  );
  y = secondRowTop - cardHeight - 18;

  drawSectionTitle("ALLOCATOR EXECUTION SUMMARY");
  drawWrapped(`Execution Timestamp: ${executionTimestamp}`, { maxChars: 92 });
  drawWrapped(`Engine Version: ${engineVersion}`, { maxChars: 92 });
  drawWrapped(`Deterministic Mode: ${deterministicMode}`, { maxChars: 92 });
  drawWrapped(`Agent Attestation Status: ${attestationStatus}`, { maxChars: 92 });
  drawWrapped(`Assets In Allocation Plan: ${assetCount}`, { maxChars: 92 });
  drawSpacer(2);

  drawSectionTitle("Why This Allocation Fits Current Conditions");
  drawWrapped(
    `Market condition is ${marketCondition.toLowerCase()} with ${volatilityLabel.toLowerCase()} volatility and ${liquidityLabel.toLowerCase()} liquidity.`,
    { maxChars: 92 },
  );
  drawWrapped(
    `Fear & Greed reads ${fearGreedScore !== null ? `${Math.round(fearGreedScore)} (${mapFearGreedLabel(fearGreedScore)})` : "unavailable"}. ${mapFearGreedInterpretation(fearGreedScore)}`,
    { maxChars: 92 },
  );
  drawWrapped(
    "This allocation limits downside risk while keeping exposure to long-term growth assets.",
    { maxChars: 92 },
  );
  if (marketSentiment !== null) {
    drawWrapped(
      `Allocator market sentiment input: ${marketSentiment.toFixed(3)} (negative values indicate caution).`,
      { maxChars: 92 },
    );
  }
  if (aaaReasonCodes.length > 0) {
    drawWrapped("Key control decisions applied:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
    });
    for (const code of aaaReasonCodes.slice(0, 5)) {
      drawWrapped(`- ${mapReasonCode(code)}`, { maxChars: 92, indent: 6 });
    }
  }

  const mixCols = 3;
  const mixGap = 8;
  const mixCardHeight = 40;
  const mixRows = Math.max(1, Math.ceil(roleBreakdown.length / mixCols));
  const mixBlockHeight = 46 + mixRows * (mixCardHeight + 8) + 8;
  startSectionOnFreshPageIfTight(mixBlockHeight);
  drawSectionTitle("Portfolio Mix Overview");
  ensureSpace(mixRows * (mixCardHeight + 8) + 16);
  const mixCardWidth = (contentWidth - mixGap * (mixCols - 1)) / mixCols;
  const mixTop = y;
  roleBreakdown.forEach((entry, index) => {
    const row = Math.floor(index / mixCols);
    const col = index % mixCols;
    const x = margin + col * (mixCardWidth + mixGap);
    const top = mixTop - row * (mixCardHeight + 8);
    drawCard(x, top, mixCardWidth, mixCardHeight, entry.label, formatPct(entry.pct));
  });
  y = mixTop - (Math.ceil(roleBreakdown.length / mixCols) * (mixCardHeight + 8)) - 4;

  const tableRowHeight = 32;
  const tableHeaderHeight = 25;
  const tableSectionHeaderHeight = 64;
  const fullTableHeight = tableSectionHeaderHeight + tableHeaderHeight + allocationRows.length * tableRowHeight + 10;
  const minimumRowsBeforeSplit = Math.min(allocationRows.length, 8);
  const minimumTableStartHeight = tableSectionHeaderHeight + tableHeaderHeight + minimumRowsBeforeSplit * tableRowHeight;
  startSectionOnFreshPageIfTight(fullTableHeight <= pageBodyHeight ? fullTableHeight : minimumTableStartHeight);
  drawSectionTitle("Recommended Asset Allocation", "Percentages are target weights for this allocation cycle.");
  if (allocationRows.length === 0) {
    drawWrapped("No allocation rows were available for this report.", { maxChars: 90 });
  } else {
    const drawTableHeader = () => {
      ensureSpace(26);
      page.drawRectangle({
        x: margin,
        y: y - 20,
        width: contentWidth,
        height: 20,
        color: rgb(0.94, 0.97, 1),
        borderColor: colors.border,
        borderWidth: 1,
      });
      page.drawText("Asset", { x: margin + 8, y: y - 14, size: 8.5, font: fontBold, color: colors.muted });
      page.drawText("Role", { x: margin + 180, y: y - 14, size: 8.5, font: fontBold, color: colors.muted });
      page.drawText("Risk Class", { x: margin + 300, y: y - 14, size: 8.5, font: fontBold, color: colors.muted });
      page.drawText("Allocation", { x: margin + 438, y: y - 14, size: 8.5, font: fontBold, color: colors.muted });
      y -= 25;
    };

    drawTableHeader();
    for (const row of allocationRows) {
      const rowHeight = tableRowHeight;
      if (y - rowHeight < bottomY + 10) {
        addPage();
        drawSectionTitle("Recommended Asset Allocation (continued)");
        drawTableHeader();
      }

      page.drawLine({
        start: { x: margin, y: y - rowHeight },
        end: { x: margin + contentWidth, y: y - rowHeight },
        thickness: 0.8,
        color: colors.border,
      });
      page.drawText(row.asset, {
        x: margin + 8,
        y: y - 13,
        size: 9.5,
        font: fontBold,
        color: colors.ink,
      });
      page.drawText(toText(row.name, row.asset), {
        x: margin + 8,
        y: y - 26,
        size: 7.8,
        font: fontRegular,
        color: colors.muted,
      });
      page.drawText(mapRoleLabel(row.category), {
        x: margin + 180,
        y: y - 17,
        size: 8.6,
        font: fontRegular,
        color: colors.ink,
      });
      page.drawText(mapRiskClassLabel(row.riskClass), {
        x: margin + 300,
        y: y - 17,
        size: 8.4,
        font: fontRegular,
        color: colors.muted,
      });
      page.drawText(formatPct(row.allocationPct), {
        x: margin + 486 - fontBold.widthOfTextAtSize(formatPct(row.allocationPct), 9.8),
        y: y - 17,
        size: 9.8,
        font: fontBold,
        color: colors.ink,
      });
      y -= rowHeight;
    }
    drawSpacer(8);
  }

  drawSectionTitle("What Each Role Means");
  if (nonZeroRoleBreakdown.length === 0) {
    drawWrapped("Role-level allocation details were unavailable.", { maxChars: 90 });
  } else {
    nonZeroRoleBreakdown.forEach((role) => {
      drawWrapped(`${role.label} (${formatPct(role.pct)}): ${mapRoleRationale(role.label)}`, {
        maxChars: 92,
      });
    });
  }

  startSectionOnFreshPageIfTight(170);
  drawSectionTitle("Action Plan For Investors");
  drawWrapped("1. Position sizing: Use these percentages as target weights when placing or adjusting positions.", {
    maxChars: 92,
  });
  drawWrapped("2. Rebalancing: Review weekly. Rebalance when allocations drift meaningfully from targets.", {
    maxChars: 92,
  });
  drawWrapped("3. Regime checks: Re-run allocation after significant market shifts (volatility, liquidity, or sentiment changes).", {
    maxChars: 92,
  });
  drawWrapped("4. Risk discipline: Avoid adding exposure outside policy caps unless your risk profile changes.", {
    maxChars: 92,
  });
  drawSpacer(4);
  addPage();
  drawSectionTitle(
    "ENGINE TRACE & AUDIT ARTIFACTS",
    "For advanced users and verification.",
  );
  drawWrapped(
    "The sections below contain the technical execution trace used for audit, reproducibility, and compliance checks.",
    { maxChars: 92 },
  );
  drawWrapped(
    "Below is the technical policy snapshot for audit and replay.",
    { maxChars: 92 },
  );
  drawSpacer(2);

  drawSectionTitle(
    "Audit Trail: Selection Criteria and Controls",
    "Policy thresholds, constraints, and deterministic rule sets used during candidate selection.",
  );
  drawWrapped(
    `Rule counts -> Phase 2 policy rules: ${phase2PolicyRules.length}, Phase 3 selection rules: ${phase3SelectionRules.length}, Phase 4 screening rules: ${phase4SelectionRules.length}.`,
    { maxChars: 92 },
  );
  drawWrapped(
    "Technical context: these controls are the machine-readable rule envelope that constrained this run.",
    { maxChars: 92, color: colors.muted },
  );
  if (phase2PolicyRules.length > 0) {
    drawWrapped("Phase 2 policy rules applied:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    for (const rule of phase2PolicyRules.slice(0, 8)) {
      drawWrapped(`- ${humanizeKey(rule)}`, { maxChars: 92, indent: 6, paragraphGap: 1.6 });
    }
    if (phase2PolicyRules.length > 8) {
      drawWrapped(`... ${phase2PolicyRules.length - 8} additional policy rules captured in artifact logs.`, {
        maxChars: 92,
        indent: 6,
        color: colors.muted,
      });
    }
  }
  if (phase2PolicyEnvelope) {
    drawWrapped("Phase 2 policy envelope:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    const envelopeKeys = Object.keys(phase2PolicyEnvelope).sort((a, b) => a.localeCompare(b));
    for (const key of envelopeKeys) {
      drawWrapped(`- ${humanizeKey(key)}: ${toAuditValueText(phase2PolicyEnvelope[key])}`, {
        maxChars: 92,
        indent: 6,
        paragraphGap: 1.6,
      });
    }
  }
  if (phase4Thresholds) {
    drawWrapped("Phase 4 screening thresholds:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    const thresholdKeys = Object.keys(phase4Thresholds).sort((a, b) => a.localeCompare(b));
    for (const key of thresholdKeys) {
      drawWrapped(`- ${humanizeKey(key)}: ${toAuditValueText(phase4Thresholds[key])}`, {
        maxChars: 92,
        indent: 6,
        paragraphGap: 1.6,
      });
    }
  }
  if (phase5Constraints) {
    drawWrapped("Phase 5 portfolio constraints:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    const constraintKeys = Object.keys(phase5Constraints).sort((a, b) => a.localeCompare(b));
    for (const key of constraintKeys) {
      drawWrapped(`- ${humanizeKey(key)}: ${toAuditValueText(phase5Constraints[key])}`, {
        maxChars: 92,
        indent: 6,
        paragraphGap: 1.6,
      });
    }
  }
  drawSpacer(4);

  drawSectionTitle(
    "Audit Trail: Data Sources and Coverage",
    "Primary data providers and freshness context used to build this recommendation.",
  );
  drawWrapped(
    "Technical context: provider-level source provenance is listed below for verification and replay.",
    { maxChars: 92, color: colors.muted },
  );
  drawWrapped(`Unique sources referenced across Phase 1 and Phase 3: ${sourceRows.length}.`, { maxChars: 92 });
  if (sourceRows.length > 0) {
    drawWrapped("Source registry (provider | endpoint | fetched_at):", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    for (const row of sourceRows.slice(0, 14)) {
      drawWrapped(`- ${row.provider} | ${row.endpoint} | ${row.fetchedAt}`, {
        font: fontMono,
        size: 8.6,
        maxChars: 96,
        lineGap: 2.8,
        indent: 6,
        paragraphGap: 1.2,
      });
    }
    if (sourceRows.length > 14) {
      drawWrapped(`... ${sourceRows.length - 14} additional sources are captured in execution artifacts.`, {
        maxChars: 92,
        indent: 6,
        color: colors.muted,
      });
    }
  }
  if (missingDomains.length > 0) {
    drawWrapped(`Missing domains flagged by pipeline audit: ${missingDomains.map((domain) => humanizeKey(domain)).join(", ")}.`, {
      maxChars: 92,
    });
  }
  if (sourceSelectionRows.length > 0) {
    const selectionBlockHeight = 36 + Math.min(sourceSelectionRows.length, 6) * 22;
    if (y - selectionBlockHeight < bottomY + 12) {
      addPage();
      drawSectionTitle("Audit Trail: Data Sources and Coverage (continued)");
    }
    drawWrapped("Source selection rationale snapshot:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 0.8,
    });
    for (const row of sourceSelectionRows.slice(0, 6)) {
      const selectedPreview =
        row.selected.length > 4
          ? `${row.selected.slice(0, 4).join(", ")} (+${row.selected.length - 4} more)`
          : row.selected.join(", ");
      const rationalePreview = truncateText(row.rationale[0] ?? "n/a", 120);
      drawWrapped(
        `- ${humanizeKey(row.domain)} | selected: ${selectedPreview || "n/a"} | rationale: ${rationalePreview}`,
        { maxChars: 92, indent: 6, paragraphGap: 1.6 },
      );
    }
  }
  drawSpacer(4);

  startSectionOnFreshPageIfTight(500);
  drawSectionTitle(
    "Audit Trail: Candidate Filtering Rationale",
    "From broad universe to final allocation, with explicit exclusion and selection reasons.",
  );
  drawWrapped(
    "Technical context: this section records why assets were excluded or selected at each gate.",
    { maxChars: 92, color: colors.muted },
  );
  drawWrapped(
    `Candidate funnel: Phase 3 universe ${phase3Eligible} -> Phase 4 eligible ${phase4Eligible} -> Phase 5 selected ${phase5Selected} -> Phase 6 allocated ${phase6Selected}.`,
    { maxChars: 92 },
  );
  drawWrapped(`Phase 4 excluded candidates: ${excludedTokens.length}.`, { maxChars: 92 });
  if (topExclusionReasons.length > 0) {
    drawWrapped("Top exclusion reasons:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    for (const [reason, count] of topExclusionReasons) {
      drawWrapped(`- ${humanizeKey(reason)}: ${count} assets`, { maxChars: 92, indent: 6, paragraphGap: 1.6 });
    }
  }
  if (exclusionExamples.length > 0) {
    drawWrapped("Excluded candidate examples:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    for (const example of exclusionExamples.slice(0, 8)) {
      drawWrapped(`- ${example}`, { maxChars: 92, indent: 6, paragraphGap: 1.6 });
    }
  }
  drawWrapped(`Phase 5 selected candidates: ${selectedPhase5Tokens.length}.`, { maxChars: 92 });
  if (topSelectionReasons.length > 0) {
    const topSelectionHeight = 30 + Math.min(topSelectionReasons.length, 8) * 20;
    if (y - topSelectionHeight < bottomY + 12) {
      addPage();
      drawSectionTitle("Audit Trail: Candidate Filtering Rationale (continued)");
    }
    drawWrapped("Top selection reasons:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    for (const [reason, count] of topSelectionReasons) {
      drawWrapped(`- ${humanizeKey(reason)}: ${count} assets`, { maxChars: 92, indent: 6, paragraphGap: 1.6 });
    }
  }
  if (selectionExamples.length > 0) {
    const selectionExamplesHeight = 30 + Math.min(selectionExamples.length, 8) * 20;
    if (y - selectionExamplesHeight < bottomY + 12) {
      addPage();
      drawSectionTitle("Audit Trail: Candidate Filtering Rationale (continued)");
    }
    drawWrapped("Selected candidate examples:", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
      paragraphGap: 1.2,
    });
    for (const example of selectionExamples.slice(0, 8)) {
      drawWrapped(`- ${example}`, { maxChars: 92, indent: 6, paragraphGap: 1.6 });
    }
  }
  drawSpacer(8);

  startSectionOnFreshPageIfTight(420);
  drawSectionTitle(
    "Audit Trail: Asset Scoring Detail",
    "Score components used in Phase 5 and AAA v4 final weighting.",
  );
  drawWrapped(
    "Technical context: numeric score components below are retained to support deterministic replay and traceability.",
    { maxChars: 92, color: colors.muted },
  );
  if (phase5ScoreRows.length > 0) {
    drawWrapped("Phase 5 scoring snapshot (selected assets):", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
    });
    drawWrapped("Asset  Quality  Risk    Comp    Liq     Struct  Vol", {
      font: fontMono,
      size: 8.8,
      maxChars: 92,
      lineGap: 2.8,
      indent: 6,
      color: colors.muted,
    });
    for (const row of phase5ScoreRows.slice(0, 16)) {
      const line =
        `${row.symbol.padEnd(6).slice(0, 6)} ` +
        `${formatMetric(row.quality).padStart(7)} ` +
        `${formatMetric(row.risk).padStart(7)} ` +
        `${formatMetric(row.composite).padStart(7)} ` +
        `${formatMetric(row.liquidity).padStart(7)} ` +
        `${formatMetric(row.structural).padStart(7)} ` +
        `${formatMetric(row.volatility).padStart(7)}`;
      drawWrapped(line, {
        font: fontMono,
        size: 8.6,
        maxChars: 92,
        lineGap: 2.8,
        indent: 6,
      });
    }
    if (phase5ScoreRows.length > 16) {
      drawWrapped(`... ${phase5ScoreRows.length - 16} additional scored assets are captured in the Phase 5 artifact.`, {
        maxChars: 92,
        indent: 6,
        color: colors.muted,
      });
    }
  } else {
    drawWrapped("Phase 5 scoring rows were not available in payload.", { maxChars: 92 });
  }
  if (aaaScoreRows.length > 0) {
    drawWrapped("AAA v4 score trace (final portfolio stage):", {
      font: fontBold,
      size: 10,
      maxChars: 90,
      lineGap: 3,
      color: colors.ink,
    });
    drawWrapped("Asset  FinalScore  Weight%  Sentiment  Quality  ER       Vol", {
      font: fontMono,
      size: 8.8,
      maxChars: 96,
      lineGap: 2.8,
      indent: 6,
      color: colors.muted,
    });
    for (const row of aaaScoreRows.slice(0, 16)) {
      const weightPct = row.weightFinal === null ? "n/a" : (row.weightFinal * 100).toFixed(2);
      const line =
        `${row.asset.padEnd(6).slice(0, 6)} ` +
        `${formatMetric(row.scoreFinal).padStart(10)} ` +
        `${weightPct.padStart(7)} ` +
        `${formatMetric(row.sentiment).padStart(10)} ` +
        `${formatMetric(row.quality).padStart(8)} ` +
        `${formatMetric(row.expectedReturn).padStart(8)} ` +
        `${formatMetric(row.volatility).padStart(8)}`;
      drawWrapped(line, {
        font: fontMono,
        size: 8.6,
        maxChars: 96,
        lineGap: 2.8,
        indent: 6,
      });
    }
    if (aaaScoreRows.length > 16) {
      drawWrapped(`... ${aaaScoreRows.length - 16} additional AAA score rows are available in allocation trace data.`, {
        maxChars: 92,
        indent: 6,
        color: colors.muted,
      });
    }
  } else {
    drawWrapped("AAA score trace rows were not available in payload.", { maxChars: 92 });
  }
  drawSpacer(4);

  drawSectionTitle(
    "Governance Signals and Exception Register",
    "Allocator execution state, reason codes, and constraint outcomes captured for audit replay.",
  );
  drawWrapped(
    "Technical context: governance reason codes and effective constraints document control-path decisions for this run.",
    { maxChars: 92, color: colors.muted },
  );
  drawWrapped(`Allocator status: ${aaaStatus} | Primary reason code: ${mapReasonCodeLabel(aaaReasonCode)}`, {
    maxChars: 92,
  });
  if (aaaReasonCodes.length > 0) {
    drawWrapped(`Reason code register: ${aaaReasonCodes.map((code) => mapReasonCodeLabel(code)).join(", ")}.`, {
      maxChars: 92,
    });
  }
  if (aaaConstraintsEffective) {
    drawWrapped("Effective constraints (AAA):", { font: fontBold, size: 10, maxChars: 90, lineGap: 3, color: colors.ink });
    const keys = Object.keys(aaaConstraintsEffective).sort((a, b) => a.localeCompare(b));
    for (const key of keys) {
      drawWrapped(`- ${humanizeKey(key)}: ${toAuditValueText(aaaConstraintsEffective[key])}`, {
        maxChars: 92,
        indent: 6,
      });
    }
  }
  if (aaaRoleCounts) {
    drawWrapped(`Role counts (AAA): ${Object.entries(aaaRoleCounts).map(([role, count]) => `${humanizeKey(role)}=${toText(count, "0")}`).join(", ")}.`, {
      maxChars: 92,
    });
  }
  if (aaaInputSanitized) {
    drawWrapped(
      `Input sanitation summary: assets_in=${toText(aaaInputSanitized.assets_in, "n/a")}, assets_out=${toText(aaaInputSanitized.assets_out, "n/a")}, dropped=${toText(aaaInputSanitized.dropped_count, "0")}, invalid_roles=${toText(aaaInputSanitized.invalid_role_count, "0")}.`,
      { maxChars: 92 },
    );
  }

  drawSectionTitle("Verification, Payment, and Integrity");
  drawWrapped(
    "Technical context: payment verification, integrity hashing, and attestation fields are included for authenticity checks.",
    { maxChars: 92, color: colors.muted },
  );
  drawWrapped(`Decision ID: ${decisionId}`, { maxChars: 92 });
  drawWrapped(`Purchase Status: ${purchased} | Charged Amount: ${formatUsdc(totalPaid)}`, { maxChars: 92 });
  drawWrapped(`Payment Transaction: ${txHash}`, { maxChars: 92 });
  drawWrapped(`Paid Wallet: ${toText(payload.walletAddress, "n/a")}`, { maxChars: 92 });
  drawWrapped(`Report Type: ${includeCertified ? "Certified Decision Record" : "Structured Allocation Report"}`, { maxChars: 92 });
  drawWrapped(`Pipeline Coverage: Phase3 ${phase3Eligible} -> Phase4 ${phase4Eligible} -> Phase5 ${phase5Selected} -> Phase6 ${phase6Selected}`, {
    maxChars: 92,
  });
  drawWrapped(`Integrity Hash (SHA-256): ${integrityHash}`, {
    font: fontMono,
    size: 8.8,
    maxChars: 96,
    lineGap: 2.8,
    color: colors.ink,
  });
  if (includeCertified) {
    drawWrapped(
      chainAttestation.stored
        ? `On-chain hash registration: stored (${toText(chainAttestation.transactionHash, "n/a")}).`
        : `On-chain hash registration: not stored (${toText(chainAttestation.error, "unknown error")}).`,
      { maxChars: 92 },
    );
    drawWrapped(
      "Certification confirms document integrity and provenance only; it does not certify suitability, expected return, or future performance.",
      { maxChars: 92, color: colors.muted },
    );
  } else {
    drawWrapped("On-chain hash registration: skipped (certified record not purchased).", { maxChars: 92 });
  }
  ensureSpace(84);
  page.drawRectangle({
    x: margin,
    y: y - 56,
    width: contentWidth,
    height: 56,
    color: colors.cardBg,
    borderColor: colors.border,
    borderWidth: 1,
  });
  page.drawText("SELUN AUTHENTICATION", {
    x: margin + 10,
    y: y - 18,
    size: 9,
    font: fontBold,
    color: colors.brand,
  });
  page.drawText(`Decision: ${decisionId}`, {
    x: margin + 10,
    y: y - 32,
    size: 8.5,
    font: fontRegular,
    color: colors.ink,
  });
  page.drawText(includeCertified ? "Record Type: Certified" : "Record Type: Structured", {
    x: margin + 180,
    y: y - 32,
    size: 8.5,
    font: fontRegular,
    color: colors.ink,
  });
  page.drawText(chainAttestation.stored ? "Status: On-chain hash stored" : "Status: Hash generated", {
    x: margin + 340,
    y: y - 32,
    size: 8.5,
    font: fontRegular,
    color: colors.ink,
  });
  y -= 66;
  drawSpacer(4);
  ensureSpace(40);
  page.drawRectangle({
    x: margin,
    y: y - 36,
    width: contentWidth,
    height: 36,
    color: rgb(0.985, 0.992, 1),
    borderColor: colors.border,
    borderWidth: 1,
  });
  page.drawText(
    "Important: Informational report only. Not investment, legal, or tax advice. No guarantee of performance or outcomes.",
    {
      x: margin + 8,
      y: y - 16,
      size: 8.6,
      font: fontRegular,
      color: colors.muted,
    },
  );
  page.drawText(
    "Users remain responsible for independent due diligence and execution decisions.",
    {
      x: margin + 8,
      y: y - 28,
      size: 8.8,
      font: fontRegular,
      color: colors.muted,
    },
  );
  y -= 42;

  const pages = pdf.getPages();
  const totalPages = pages.length;
  const footerDecisionId = decisionId.length > 20 ? `${decisionId.slice(0, 20)}...` : decisionId;
  for (let i = 0; i < totalPages; i += 1) {
    const current = pages[i];
    if (includeCertified) {
      const stickerWidth = 148;
      const stickerHeight = 24;
      const stickerX = pageWidth - margin - stickerWidth;
      const stickerY = 14;
      current.drawRectangle({
        x: stickerX,
        y: stickerY,
        width: stickerWidth,
        height: stickerHeight,
        color: rgb(0.94, 0.97, 1),
        borderColor: colors.brand,
        borderWidth: 1,
      });
      current.drawText("CERTIFIED RECORD", {
        x: stickerX + 10,
        y: stickerY + 8,
        size: 8.8,
        font: fontBold,
        color: colors.brand,
      });
    }
    current.drawText(`Selun Allocation Record | Decision ${footerDecisionId} | Page ${i + 1} of ${totalPages}`, {
      x: margin,
      y: 24,
      size: 9,
      font: fontRegular,
      color: colors.muted,
    });
  }

  return pdf.save();
}

export async function POST(req: Request): Promise<Response> {
  let raw: unknown;

  try {
    raw = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid request payload." }, { status: 400 });
  }

  if (!isRecord(raw)) {
    return NextResponse.json({ error: "Request body must be a JSON object." }, { status: 400 });
  }

  const payload = raw as DownloadRequestPayload;
  const payment = isRecord(payload.payment) ? payload.payment : null;

  const paymentStatus = toText(payment?.status, "").toLowerCase();
  const decisionId = toText(payment?.decisionId, "");
  const transactionId = toText(payment?.transactionId, "");
  const paymentMethod = toText(payment?.paymentMethod, "onchain").toLowerCase();
  const chargedAmountUsdc = toFiniteNumber(payment?.chargedAmountUsdc ?? payment?.amountUsdc);

  if (paymentStatus !== "paid" || !decisionId || !transactionId) {
    return NextResponse.json(
      { error: "Paid purchase record is required before report download." },
      { status: 402 },
    );
  }

  const includeCertified = Boolean(payload.includeCertifiedDecisionRecord);
  const certifiedPurchased = Boolean(payment?.certifiedDecisionRecordPurchased);
  if (!includeCertified || !certifiedPurchased) {
    return NextResponse.json(
      { error: "Phase 7 is disabled because certified report was not purchased." },
      { status: 402 },
    );
  }

  const walletAddress = toText(payload.walletAddress, "");
  if (!walletAddress || !/^0x[a-fA-F0-9]{40}$/.test(walletAddress)) {
    return NextResponse.json({ error: "Valid paid wallet address is required before report download." }, { status: 400 });
  }

  const isFreeCodePayment =
    paymentMethod === "free_code" ||
    transactionId.startsWith("FREE-") ||
    (chargedAmountUsdc !== null && chargedAmountUsdc <= 0);

  let expectedVerificationAmount = 0;
  if (!isFreeCodePayment) {
    const pricing = await fetchBackendPricing();
    if (!pricing.ok) {
      return NextResponse.json(
        { error: `Pricing lookup required for certified report: ${pricing.error}` },
        { status: 502 },
      );
    }
    expectedVerificationAmount = pricing.requiredCertifiedPriceUsdc;
  }

  const verifiedPayment = await verifyPurchaseOnBackend(
    walletAddress,
    expectedVerificationAmount,
    transactionId,
    decisionId,
  );
  if (!verifiedPayment.ok) {
    return NextResponse.json(
      { error: `Payment verification required: ${verifiedPayment.error}` },
      { status: 402 },
    );
  }

  const integrityHash = createHash("sha256")
    .update(stableStringify(payload))
    .digest("hex");

  let chainAttestation: ChainAttestationResult = { stored: false };
  const autoAttestEnabled = String(process.env.SELUN_REPORT_AUTO_ATTEST || "").trim().toLowerCase() === "true";
  if (includeCertified && autoAttestEnabled) {
    chainAttestation = await storeHashOnBackend(decisionId, integrityHash);
  } else if (includeCertified && !autoAttestEnabled) {
    chainAttestation = {
      stored: false,
      error: "auto attestation disabled (set SELUN_REPORT_AUTO_ATTEST=true to enable)",
    };
  }

  const pdfBytes = await buildPdf(payload, integrityHash, chainAttestation);
  const pdfBuffer = Buffer.from(pdfBytes);
  const fileStem = includeCertified ? "selun-certified-decision-record" : "selun-structured-allocation-report";
  const safeDecision = sanitizeFilenamePart(decisionId);
  const filename = `${fileStem}-${safeDecision}.pdf`;

  return new Response(pdfBuffer, {
    status: 200,
    headers: {
      "Content-Type": "application/pdf",
      "Content-Disposition": `attachment; filename=\"${filename}\"`,
      "Cache-Control": "no-store",
    },
  });
}
