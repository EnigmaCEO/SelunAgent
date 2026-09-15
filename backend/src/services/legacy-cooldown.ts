import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { isIP } from "node:net";
import type { Request, RequestHandler } from "express";

type Bucket = { attempts: number[]; blockedUntil: number };
type Policy = { limit: number; windowMs: number; cooldownMs: number };

export class CooldownStore {
  private readonly buckets = new Map<string, Bucket>();
  private nextSweep = 0;
  constructor(private readonly capacity = 10_000) {}

  check(keys: string[], policy: Policy, now = Date.now()): number {
    // Bound memory without evicting active locks (which would allow bypass).
    if (now >= this.nextSweep) {
      for (const [key, bucket] of this.buckets) {
        bucket.attempts = bucket.attempts.filter(at => at > now - policy.windowMs);
        if (!bucket.attempts.length && bucket.blockedUntil <= now) this.buckets.delete(key);
      }
      this.nextSweep = now + 60_000;
    }
    for (const key of keys) {
      const bucket = this.buckets.get(key);
      if (bucket) bucket.attempts = bucket.attempts.filter(at => at > now - policy.windowMs);
    }
    const retryAt = Math.max(0, ...keys.map(key => this.buckets.get(key)?.blockedUntil ?? 0));
    if (retryAt > now) return Math.ceil((retryAt - now) / 1000);
    if (this.buckets.size + keys.filter(key => !this.buckets.has(key)).length > this.capacity) return 60;
    if (keys.some(key => (this.buckets.get(key)?.attempts.length ?? 0) >= policy.limit)) {
      for (const key of keys) {
        const bucket = this.buckets.get(key) ?? { attempts: [], blockedUntil: 0 };
        bucket.blockedUntil = now + policy.cooldownMs;
        this.buckets.set(key, bucket);
      }
      return Math.ceil(policy.cooldownMs / 1000);
    }
    for (const key of keys) {
      const bucket = this.buckets.get(key) ?? { attempts: [], blockedUntil: 0 };
      bucket.attempts.push(now);
      this.buckets.set(key, bucket);
    }
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
  "/phase1/run": "execution",
};

export function createLegacyCooldown(): RequestHandler {
  // Separate stores ensure one route's window cannot prune another's locks.
  const stores = { checkout: new CooldownStore(), email: new CooldownStore(), execution: new CooldownStore() };
  return (req, res, next) => {
    // Express accepts case variants and trailing slashes by default.
    const route = req.path.toLowerCase().replace(/\/+$/, "");
    const group = groups[route] as keyof typeof stores | undefined;
    if (req.method !== "POST" || !group) return next();
    const limit = group === "checkout" ? 3 : group === "email" ? 6 : 10;
    const policy = { limit, windowMs: 15 * 60_000, cooldownMs: 30 * 60_000 };
    const ip = fingerprint(legacyClientIp(req));
    const keys = [`ip:${ip}`];
    const wallet = req.body?.walletAddress;
    const email = req.body?.resultEmail;
    if (typeof wallet === "string" && /^0x[0-9a-fA-F]{40}$/.test(wallet)) keys.push(`wallet:${fingerprint(wallet.toLowerCase())}`);
    if (typeof email === "string" && email.length <= 254 && email.includes("@")) keys.push(`email:${fingerprint(email.trim().toLowerCase())}`);
    const retryAfterSeconds = stores[group].check(keys, policy);
    if (!retryAfterSeconds) return next();
    console.warn(JSON.stringify({ event: "legacy_security_cooldown", group, route: req.path, client: ip, retryAfterSeconds }));
    res.set("Retry-After", String(retryAfterSeconds));
    res.set("Cache-Control", "no-store");
    res.status(429).json({ success: false, error: "Too many attempts. Please wait before trying again.", reason: "security_cooldown", retryAfterSeconds });
  };
}
