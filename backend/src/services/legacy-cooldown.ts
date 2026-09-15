import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { isIP } from "node:net";
import fs from "node:fs";
import path from "node:path";
import type { Request, RequestHandler } from "express";
import { resolveBackendDataFilePath } from "../runtime-paths";

type Bucket = { attempts: Record<string, number[]>; blockedUntil: number; strikes: number; lastAbuseAt: number };
type Policy = { limit: number; windowMs: number; cooldownMs: number };
const HISTORY_MS = 7 * 24 * 60 * 60_000;

export class CooldownStore {
  private readonly buckets = new Map<string, Bucket>();
  private nextSweep = 0;
  constructor(private readonly capacity = 10_000, private readonly stateFile?: string) {
    if (!stateFile || !fs.existsSync(stateFile)) return;
    if (fs.statSync(stateFile).size > 8 * 1024 * 1024) throw new Error("Cooldown state is too large.");
    const state = JSON.parse(fs.readFileSync(stateFile, "utf8"));
    if (state.version !== 1 || !Array.isArray(state.buckets) || state.buckets.length > capacity) throw new Error("Invalid cooldown state.");
    for (const entry of state.buckets) {
      if (!Array.isArray(entry) || entry.length !== 2) throw new Error("Invalid cooldown bucket.");
      const [key, bucket] = entry;
      if (typeof key !== "string" || key.length > 100 || !bucket ||
        !Number.isSafeInteger(bucket.strikes) || bucket.strikes < 0 || bucket.strikes > 3 ||
        !Number.isFinite(bucket.blockedUntil) || bucket.blockedUntil < 0 ||
        !Number.isFinite(bucket.lastAbuseAt) || bucket.lastAbuseAt < 0 ||
        !bucket.attempts || typeof bucket.attempts !== "object" || Array.isArray(bucket.attempts) ||
        Object.entries(bucket.attempts).some(([group, attempts]) => !["default", "checkout", "email", "execution"].includes(group) ||
          !Array.isArray(attempts) || attempts.length > 100 || attempts.some(at => !Number.isFinite(at) || at < 0))) {
        throw new Error("Invalid cooldown bucket.");
      }
      this.buckets.set(key, bucket);
    }
  }

  private persist(): void {
    if (!this.stateFile) return;
    fs.mkdirSync(path.dirname(this.stateFile), { recursive: true });
    const temporary = `${this.stateFile}.tmp`;
    // One process owns the Fly volume; rename makes each snapshot atomic.
    fs.writeFileSync(temporary, JSON.stringify({ version: 1, buckets: Array.from(this.buckets) }), { mode: 0o600 });
    fs.renameSync(temporary, this.stateFile);
  }

  private prune(bucket: Bucket, policy: Policy, now: number): void {
    for (const group of Object.keys(bucket.attempts)) {
      bucket.attempts[group] = bucket.attempts[group].filter(at => at > now - policy.windowMs);
      if (!bucket.attempts[group].length) delete bucket.attempts[group];
    }
    if (bucket.strikes && bucket.blockedUntil <= now && now - bucket.lastAbuseAt >= HISTORY_MS) {
      bucket.strikes = 0;
      bucket.lastAbuseAt = 0;
    }
  }

  strikeLevel(keys: string[]): number {
    return Math.max(0, ...keys.map(key => this.buckets.get(key)?.strikes ?? 0));
  }

  check(keys: string[], policy: Policy, now = Date.now(), group = "default"): number {
    // Bound memory without evicting active locks (which would allow bypass).
    if (now >= this.nextSweep) {
      for (const [key, bucket] of this.buckets) {
        this.prune(bucket, policy, now);
        if (!Object.keys(bucket.attempts).length && !bucket.strikes && bucket.blockedUntil <= now) this.buckets.delete(key);
      }
      this.nextSweep = now + 60_000;
    }
    for (const key of keys) {
      const bucket = this.buckets.get(key);
      if (bucket) this.prune(bucket, policy, now);
    }
    const retryAt = Math.max(0, ...keys.map(key => this.buckets.get(key)?.blockedUntil ?? 0));
    if (retryAt > now) {
      // Attempts during a lock do not extend it or increase strikes. Preserve
      // recent abuse time at most once per minute for the quiet-period reset.
      let changed = false;
      for (const key of keys) {
        const bucket = this.buckets.get(key);
        if (bucket && bucket.blockedUntil > now && now - bucket.lastAbuseAt >= 60_000) {
          bucket.lastAbuseAt = now;
          changed = true;
        }
      }
      if (changed) this.persist();
      return Math.ceil((retryAt - now) / 1000);
    }
    if (this.buckets.size + keys.filter(key => !this.buckets.has(key)).length > this.capacity) return 60;
    if (keys.some(key => (this.buckets.get(key)?.attempts[group]?.length ?? 0) >= policy.limit)) {
      const strikes = Math.min(3, this.strikeLevel(keys) + 1);
      const duration = strikes === 1 ? policy.cooldownMs : strikes === 2 ? 2 * 60 * 60_000 : 24 * 60 * 60_000;
      for (const key of keys) {
        const bucket = this.buckets.get(key) ?? { attempts: {}, blockedUntil: 0, strikes: 0, lastAbuseAt: 0 };
        bucket.blockedUntil = now + duration;
        bucket.strikes = strikes;
        bucket.lastAbuseAt = now;
        this.buckets.set(key, bucket);
      }
      this.persist();
      return Math.ceil(duration / 1000);
    }
    for (const key of keys) {
      const bucket = this.buckets.get(key) ?? { attempts: {}, blockedUntil: 0, strikes: 0, lastAbuseAt: 0 };
      (bucket.attempts[group] ??= []).push(now);
      this.buckets.set(key, bucket);
    }
    this.persist();
    return 0;
  }
}

function fingerprint(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 24);
}

export function legacyClientIp(req: Request): string {
  // Only the trusted frontend may assert the original browser IP.
  const secret = process.env.SELUN_REFERRAL_INTERNAL_TOKEN?.trim() || process.env.SELUN_ADMIN_API_TOKEN?.trim();
  const ip = req.get("x-selun-client-ip") ?? "";
  const timestamp = req.get("x-selun-client-timestamp") ?? "";
  const signature = req.get("x-selun-client-signature") ?? "";
  if (secret && isIP(ip) && /^\d+$/.test(timestamp) && Math.abs(Date.now() - Number(timestamp)) <= 60_000 && /^[a-f0-9]{64}$/.test(signature)) {
    const expected = createHmac("sha256", secret).update(`${timestamp}\n${req.method}\n${req.originalUrl.split("?")[0]}\n${ip}`).digest();
    if (timingSafeEqual(expected, Buffer.from(signature, "hex"))) return ip;
  }
  // Fly Proxy supplies this header; never trust arbitrary X-Forwarded-For.
  const flyIp = req.get("fly-client-ip") ?? "";
  if (process.env.FLY_APP_NAME && isIP(flyIp)) return flyIp;
  return req.socket.remoteAddress || "unknown";
}

const groups: Record<string, string> = {
  "/pay": "checkout", "/result-email": "email", "/report-email": "email",
  "/x402/sce/escalation-brief": "email",
  "/phase1/run": "execution",
};

export function createLegacyCooldown(options: { stateFile?: string | false } = {}): RequestHandler {
  // Resolve lazily, after server.ts loads dotenv. Counters remain per group;
  // a lock and strike history apply across all protected legacy routes.
  let store: CooldownStore | undefined;
  return (req, res, next) => {
    // Express accepts case variants and trailing slashes by default.
    const route = req.path.toLowerCase().replace(/\/+$/, "");
    const group = groups[route];
    if (req.method !== "POST" || !group) return next();
    const limit = group === "checkout" ? 3 : group === "email" ? 6 : 10;
    const policy = { limit, windowMs: 15 * 60_000, cooldownMs: 30 * 60_000 };
    const ip = fingerprint(legacyClientIp(req));
    const keys = [`ip:${ip}`];
    const wallet = req.body?.walletAddress;
    const email = req.body?.resultEmail;
    if (typeof wallet === "string" && /^0x[0-9a-fA-F]{40}$/.test(wallet)) keys.push(`wallet:${fingerprint(wallet.toLowerCase())}`);
    if (typeof email === "string" && email.length <= 254 && email.includes("@")) keys.push(`email:${fingerprint(email.trim().toLowerCase())}`);
    let retryAfterSeconds: number;
    try {
      store ??= new CooldownStore(10_000, options.stateFile === false ? undefined :
        options.stateFile || process.env.SELUN_LEGACY_COOLDOWN_STATE_FILE?.trim() || resolveBackendDataFilePath("legacy-cooldown-state.json"));
      retryAfterSeconds = store.check(keys, policy, Date.now(), group);
    } catch (error) {
      // Never perform protected work if durable security state cannot be saved.
      console.error("Legacy cooldown state unavailable:", error instanceof Error ? error.message : "unknown error");
      res.set("Retry-After", "60");
      res.set("Cache-Control", "no-store");
      res.status(503).json({ success: false, error: "Checkout temporarily unavailable. Please try again later.", reason: "security_state_unavailable" });
      return;
    }
    if (!retryAfterSeconds) return next();
    console.warn(JSON.stringify({ event: "legacy_security_cooldown", group, route: req.path, client: ip, strikeLevel: store.strikeLevel(keys), retryAfterSeconds }));
    res.set("Retry-After", String(retryAfterSeconds));
    res.set("Cache-Control", "no-store");
    res.status(429).json({ success: false, error: "Too many attempts. Please wait before trying again.", reason: "security_cooldown", retryAfterSeconds });
  };
}
