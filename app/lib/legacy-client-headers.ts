import { createHmac } from "node:crypto";
import { isIP } from "node:net";

export function legacyClientHeaders(req: Request, backendPath: string): Record<string, string> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const secret = process.env.SELUN_REFERRAL_INTERNAL_TOKEN?.trim() || process.env.SELUN_ADMIN_API_TOKEN?.trim();
  // Vercel overwrites this header with the connecting client's IP.
  const ip = process.env.VERCEL ? (req.headers.get("x-vercel-forwarded-for") || req.headers.get("x-forwarded-for"))?.split(",")[0]?.trim() : undefined;
  if (!secret || !ip || !isIP(ip)) return headers;
  const timestamp = String(Date.now());
  headers["x-selun-client-ip"] = ip;
  headers["x-selun-client-timestamp"] = timestamp;
  headers["x-selun-client-signature"] = createHmac("sha256", secret).update(`${timestamp}\nPOST\n${backendPath}\n${ip}`).digest("hex");
  return headers;
}
