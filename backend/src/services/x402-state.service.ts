import fs from "node:fs";
import path from "node:path";
import { resolveBackendDataFilePath } from "../runtime-paths";
import type { AllocateInputShape, X402AllocateRecord } from "./x402-state.types";

type PersistedX402State = {
  version: 1;
  updatedAt: string;
  allocateByDecisionId: Record<string, X402AllocateRecord>;
  decisionIdByJobId: Record<string, string>;
  addressDailyUsage: Record<string, number>;
  consumedTransactionByHash: Record<string, string>;
};

type ReserveTransactionResult =
  | { accepted: true; reused: boolean }
  | { accepted: false; existingDecisionId: string };

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function toIsoOrNow(value: unknown): string {
  if (typeof value !== "string") return new Date().toISOString();
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : new Date().toISOString();
}

function normalizeTransactionHash(value: string): string {
  return value.trim().toLowerCase();
}

function toSortedObject<V>(map: Map<string, V>): Record<string, V> {
  const sortedEntries = Array.from(map.entries()).sort(([a], [b]) => a.localeCompare(b));
  return Object.fromEntries(sortedEntries);
}

function normalizeAllocateInputs(value: unknown): AllocateInputShape | null {
  if (!isObject(value)) return null;

  const riskTolerance = typeof value.riskTolerance === "string" ? value.riskTolerance : null;
  const timeframe = typeof value.timeframe === "string" ? value.timeframe : null;
  const withReport = typeof value.withReport === "boolean" ? value.withReport : null;

  if (
    (riskTolerance !== "Conservative" && riskTolerance !== "Balanced" && riskTolerance !== "Growth" && riskTolerance !== "Aggressive") ||
    (timeframe !== "<1_year" && timeframe !== "1-3_years" && timeframe !== "3+_years") ||
    withReport === null
  ) {
    return null;
  }

  return {
    riskTolerance,
    timeframe,
    withReport,
  };
}

function normalizeAllocateRecord(decisionId: string, value: unknown): X402AllocateRecord | null {
  if (!isObject(value)) return null;

  const normalizedDecisionId = typeof value.decisionId === "string" ? value.decisionId.trim() : decisionId;
  const inputFingerprint = typeof value.inputFingerprint === "string" ? value.inputFingerprint.trim() : "";
  const inputs = normalizeAllocateInputs(value.inputs);
  const chargedAmountUsdc = typeof value.chargedAmountUsdc === "string" ? value.chargedAmountUsdc.trim() : "";
  const quoteIssuedAt = toIsoOrNow(value.quoteIssuedAt);
  const quoteExpiresAt = toIsoOrNow(value.quoteExpiresAt);
  const state = value.state === "accepted" ? "accepted" : value.state === "quoted" ? "quoted" : null;
  const createdAt = toIsoOrNow(value.createdAt);
  const updatedAt = toIsoOrNow(value.updatedAt);
  const jobId = typeof value.jobId === "string" && value.jobId.trim() ? value.jobId.trim() : undefined;

  if (!normalizedDecisionId || !inputFingerprint || !inputs || !chargedAmountUsdc || !state) {
    return null;
  }

  let payment: X402AllocateRecord["payment"];
  if (isObject(value.payment)) {
    const fromAddress = typeof value.payment.fromAddress === "string" ? value.payment.fromAddress.trim() : "";
    const transactionHash =
      typeof value.payment.transactionHash === "string" ? value.payment.transactionHash.trim() : "";
    const verifiedAt = toIsoOrNow(value.payment.verifiedAt);
    if (fromAddress && transactionHash) {
      payment = {
        fromAddress,
        transactionHash,
        verifiedAt,
      };
    }
  }

  return {
    decisionId: normalizedDecisionId,
    inputFingerprint,
    inputs,
    chargedAmountUsdc,
    quoteIssuedAt,
    quoteExpiresAt,
    state,
    createdAt,
    updatedAt,
    ...(jobId ? { jobId } : {}),
    ...(payment ? { payment } : {}),
  };
}

function normalizeDailyUsageCount(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  const normalized = Math.floor(value);
  return normalized >= 0 ? normalized : null;
}

function resolveStateFilePath(): string {
  const configured = process.env.X402_STATE_FILE?.trim();
  if (!configured) {
    return resolveBackendDataFilePath("x402-state.json");
  }
  return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

export class X402StateStore {
  private readonly filePath: string;
  private readonly dailyUsageRetentionDays: number;
  private readonly allocateByDecisionId = new Map<string, X402AllocateRecord>();
  private readonly decisionIdByJobId = new Map<string, string>();
  private readonly addressDailyUsage = new Map<string, number>();
  private readonly consumedTransactionByHash = new Map<string, string>();

  constructor(filePath = resolveStateFilePath(), dailyUsageRetentionDays = readPositiveIntEnv("X402_STATE_RETENTION_DAYS", 3)) {
    this.filePath = filePath;
    this.dailyUsageRetentionDays = Math.max(2, dailyUsageRetentionDays);
    this.load();
  }

  getAllocateRecord(decisionId: string): X402AllocateRecord | undefined {
    return this.allocateByDecisionId.get(decisionId);
  }

  setAllocateRecord(decisionId: string, record: X402AllocateRecord) {
    this.allocateByDecisionId.set(decisionId, record);
    if (record.jobId) {
      this.decisionIdByJobId.set(record.jobId, decisionId);
    }
    if (record.payment?.transactionHash) {
      this.consumedTransactionByHash.set(normalizeTransactionHash(record.payment.transactionHash), decisionId);
    }
    this.persist();
  }

  getDecisionIdForJob(jobId: string): string | undefined {
    return this.decisionIdByJobId.get(jobId);
  }

  setDecisionIdForJob(jobId: string, decisionId: string) {
    this.decisionIdByJobId.set(jobId, decisionId);
    this.persist();
  }

  getAddressDailyUsage(key: string): number {
    return this.addressDailyUsage.get(key) ?? 0;
  }

  incrementAddressDailyUsage(key: string): number {
    const next = this.getAddressDailyUsage(key) + 1;
    this.addressDailyUsage.set(key, next);
    this.pruneAddressDailyUsage();
    this.persist();
    return next;
  }

  reserveTransactionHash(transactionHash: string, decisionId: string): ReserveTransactionResult {
    const normalizedHash = normalizeTransactionHash(transactionHash);
    const existingDecisionId = this.consumedTransactionByHash.get(normalizedHash);

    if (existingDecisionId && existingDecisionId !== decisionId) {
      return { accepted: false, existingDecisionId };
    }

    const reused = existingDecisionId === decisionId;
    this.consumedTransactionByHash.set(normalizedHash, decisionId);
    if (!reused) {
      this.persist();
    }
    return { accepted: true, reused };
  }

  getTransactionOwner(transactionHash: string): string | undefined {
    return this.consumedTransactionByHash.get(normalizeTransactionHash(transactionHash));
  }

  private load() {
    if (!fs.existsSync(this.filePath)) return;

    try {
      const raw = fs.readFileSync(this.filePath, "utf8");
      const parsed = JSON.parse(raw) as unknown;
      if (!isObject(parsed)) return;

      const allocateByDecisionId = isObject(parsed.allocateByDecisionId) ? parsed.allocateByDecisionId : {};
      for (const [decisionId, value] of Object.entries(allocateByDecisionId)) {
        const record = normalizeAllocateRecord(decisionId, value);
        if (!record) continue;
        this.allocateByDecisionId.set(record.decisionId, record);
      }

      const decisionIdByJobId = isObject(parsed.decisionIdByJobId) ? parsed.decisionIdByJobId : {};
      for (const [jobId, decisionId] of Object.entries(decisionIdByJobId)) {
        if (!jobId.trim() || typeof decisionId !== "string" || !decisionId.trim()) continue;
        this.decisionIdByJobId.set(jobId, decisionId.trim());
      }

      const addressDailyUsage = isObject(parsed.addressDailyUsage) ? parsed.addressDailyUsage : {};
      for (const [key, value] of Object.entries(addressDailyUsage)) {
        const normalizedUsage = normalizeDailyUsageCount(value);
        if (!key.trim() || normalizedUsage === null) continue;
        this.addressDailyUsage.set(key, normalizedUsage);
      }

      const consumedByHash = isObject(parsed.consumedTransactionByHash) ? parsed.consumedTransactionByHash : {};
      for (const [hash, decisionId] of Object.entries(consumedByHash)) {
        if (typeof decisionId !== "string" || !decisionId.trim()) continue;
        this.consumedTransactionByHash.set(normalizeTransactionHash(hash), decisionId.trim());
      }

      // Backfill consumed hashes from accepted records when older state files do not have the map.
      const recordsByCreatedAt = Array.from(this.allocateByDecisionId.values()).sort((a, b) => a.createdAt.localeCompare(b.createdAt));
      for (const record of recordsByCreatedAt) {
        if (!record.payment?.transactionHash) continue;
        const hash = normalizeTransactionHash(record.payment.transactionHash);
        if (!this.consumedTransactionByHash.has(hash)) {
          this.consumedTransactionByHash.set(hash, record.decisionId);
        }
      }

      this.pruneAddressDailyUsage();
    } catch {
      // Ignore corrupt state files and continue with clean in-memory state.
    }
  }

  private pruneAddressDailyUsage() {
    const today = new Date();
    const minimumDay = new Date(today);
    minimumDay.setUTCDate(today.getUTCDate() - (this.dailyUsageRetentionDays - 1));
    const minimumDayKey = minimumDay.toISOString().slice(0, 10);

    for (const key of this.addressDailyUsage.keys()) {
      const dayKey = key.split(":", 1)[0];
      if (!dayKey || dayKey < minimumDayKey) {
        this.addressDailyUsage.delete(key);
      }
    }
  }

  private persist() {
    const payload: PersistedX402State = {
      version: 1,
      updatedAt: new Date().toISOString(),
      allocateByDecisionId: toSortedObject(this.allocateByDecisionId),
      decisionIdByJobId: toSortedObject(this.decisionIdByJobId),
      addressDailyUsage: toSortedObject(this.addressDailyUsage),
      consumedTransactionByHash: toSortedObject(this.consumedTransactionByHash),
    };

    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    const tempPath = `${this.filePath}.tmp`;
    fs.writeFileSync(tempPath, JSON.stringify(payload, null, 2), "utf8");
    fs.renameSync(tempPath, this.filePath);
  }
}

let cachedStore: X402StateStore | null = null;

export function getX402StateStore(): X402StateStore {
  if (cachedStore) return cachedStore;
  cachedStore = new X402StateStore();
  return cachedStore;
}

