export const ACTIVE_REFERRAL_CODE_STORAGE_KEY = "selun.activeReferralCode";
export const GENERATED_REFERRAL_CODE_STORAGE_KEY = "selun.generatedReferralCode";
export const AGENT_REFERRAL_VISITOR_ID_STORAGE_KEY = "selun.agentReferralVisitorId";
export const SELUN_PUBLIC_SITE_URL = process.env.NEXT_PUBLIC_SITE_URL?.trim().replace(/\/+$/, "") || "https://selun.sagitta.systems";

const REFERRAL_CODE_REGEX = /^[A-Z0-9]{6,8}$/;
const EVM_WALLET_ADDRESS_REGEX = /^0x[a-fA-F0-9]{40}$/;
const AGENT_PROGRAM_REFERRER_ID_REGEX = /^(human|agent)_[a-z0-9][a-z0-9_-]{2,63}$/;

export function normalizeReferralCode(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  if (!trimmed) return null;

  if (EVM_WALLET_ADDRESS_REGEX.test(trimmed)) {
    return trimmed.toLowerCase();
  }

  const normalizedAgentProgramId = trimmed.toLowerCase();
  if (AGENT_PROGRAM_REFERRER_ID_REGEX.test(normalizedAgentProgramId)) {
    return normalizedAgentProgramId;
  }

  const normalized = trimmed.toUpperCase();
  return REFERRAL_CODE_REGEX.test(normalized) ? normalized : null;
}

export function isWalletReferralInput(value: string | null | undefined): boolean {
  const trimmed = value?.trim() ?? "";
  return EVM_WALLET_ADDRESS_REGEX.test(trimmed);
}

export function isAgentProgramReferrerId(value: string | null | undefined): boolean {
  const trimmed = value?.trim()?.toLowerCase() ?? "";
  return AGENT_PROGRAM_REFERRER_ID_REGEX.test(trimmed);
}

export function readOrCreateAgentReferralVisitorId(storage: Pick<Storage, "getItem" | "setItem">): string {
  const existing = storage.getItem(AGENT_REFERRAL_VISITOR_ID_STORAGE_KEY)?.trim() ?? "";
  if (existing) {
    return existing;
  }

  const randomPart =
    typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const visitorId = `visitor_${randomPart.replace(/[^a-zA-Z0-9_-]/g, "").toLowerCase()}`;
  storage.setItem(AGENT_REFERRAL_VISITOR_ID_STORAGE_KEY, visitorId);
  return visitorId;
}

export function appendReferralParam(path: string, referralCode: string | null): string {
  if (!referralCode) {
    return path;
  }

  const url = new URL(path, "https://selun.local");
  url.searchParams.set("ref", referralCode);
  return `${url.pathname}${url.search}${url.hash}`;
}

export function stripReferralParam(pathOrUrl: string): string {
  const url = new URL(pathOrUrl, "https://selun.local");
  url.searchParams.delete("ref");
  return `${url.pathname}${url.search}${url.hash}`;
}

export function buildReferralLink(referralCode: string, origin: string = SELUN_PUBLIC_SITE_URL): string {
  const url = new URL("/", origin);
  url.searchParams.set("ref", referralCode);
  return url.toString();
}
