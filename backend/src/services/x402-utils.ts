export function isExpiredIsoTimestamp(value: string, nowMs = Date.now()): boolean {
  const expiresAtMs = Date.parse(value);
  if (!Number.isFinite(expiresAtMs)) return true;
  return expiresAtMs <= nowMs;
}

export function normalizeOptionalBoolean(value: unknown): { value: boolean; valid: boolean } {
  if (value === undefined || value === null) {
    return { value: false, valid: true };
  }

  if (typeof value === "boolean") {
    return { value, valid: true };
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return { value: true, valid: true };
    if (normalized === "false") return { value: false, valid: true };
  }

  return { value: false, valid: false };
}
