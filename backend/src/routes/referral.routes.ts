import { timingSafeEqual } from "node:crypto";
import { Router, type Request, type Response } from "express";
import {
  createReferralCode,
  getReferralEventsByCode,
  getReferralLeaderboard,
  getReferralStatsByCode,
  isReferralDatabaseConfigured,
  normalizeReferralCode,
  normalizeReferralTransactionStatus,
  recordReferralConversion,
} from "../services/referral.service";

const router = Router();

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message.trim() ? error.message : fallback;
}

function isDatabaseConfigurationError(error: unknown): boolean {
  return error instanceof Error && /DATABASE_URL/i.test(error.message);
}

function sendReferralError(res: Response, error: unknown, fallback: string) {
  const status = isDatabaseConfigurationError(error) ? 503 : 500;
  return res.status(status).json({
    error: errorMessage(error, fallback),
  });
}

function getInternalTrackToken(): string {
  return process.env.SELUN_REFERRAL_INTERNAL_TOKEN?.trim() || process.env.SELUN_ADMIN_API_TOKEN?.trim() || "";
}

function isInternalTrackAuthorized(req: Request): boolean {
  const configured = getInternalTrackToken();
  if (!configured) {
    return false;
  }

  const headerValue =
    req.header("x-selun-referral-token")?.trim() ||
    req.header("authorization")?.trim().replace(/^Bearer\s+/i, "") ||
    "";

  if (!headerValue) {
    return false;
  }

  const provided = Buffer.from(headerValue);
  const expected = Buffer.from(configured);
  return provided.length === expected.length && timingSafeEqual(provided, expected);
}

function readAmountUsd(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

router.post("/create", async (_req: Request, res: Response) => {
  if (!isReferralDatabaseConfigured()) {
    return res.status(503).json({
      error: "Referral database is not configured.",
    });
  }

  try {
    const code = await createReferralCode();
    return res.status(201).json({ code });
  } catch (error) {
    return sendReferralError(res, error, "Unable to create referral code.");
  }
});

router.post("/track", async (req: Request, res: Response) => {
  const configuredToken = getInternalTrackToken();
  if (!configuredToken) {
    return res.status(503).json({
      error: "Referral tracking token is not configured.",
    });
  }

  if (!isInternalTrackAuthorized(req)) {
    return res.status(401).json({
      error: "Unauthorized referral tracking request.",
    });
  }

  const referralCode = typeof req.body?.referralCode === "string" ? req.body.referralCode : "";
  const transactionId = typeof req.body?.transactionId === "string" ? req.body.transactionId.trim() : "";
  const amountUsd = readAmountUsd(req.body?.amountUsd);
  const status = normalizeReferralTransactionStatus(req.body?.status) ?? undefined;

  if (!referralCode.trim()) {
    return res.status(400).json({
      error: "referralCode is required.",
    });
  }
  if (!transactionId) {
    return res.status(400).json({
      error: "transactionId is required.",
    });
  }
  if (amountUsd === null || amountUsd < 0) {
    return res.status(400).json({
      error: "amountUsd must be a non-negative number.",
    });
  }

  try {
    const result = await recordReferralConversion({
      referralCode,
      transactionId,
      amountUsd,
      status,
    });
    return res.status(200).json({
      recorded: result.recorded,
    });
  } catch (error) {
    return sendReferralError(res, error, "Unable to record referral conversion.");
  }
});

router.get("/leaderboard", async (_req: Request, res: Response) => {
  if (!isReferralDatabaseConfigured()) {
    return res.status(503).json({
      error: "Referral database is not configured.",
    });
  }

  try {
    const leaderboard = await getReferralLeaderboard(10);
    return res.status(200).json(
      leaderboard.map((entry) => ({
        referral_code: entry.referralCode,
        total_earnings: entry.totalEarnings,
        total_conversions: entry.totalConversions,
      })),
    );
  } catch (error) {
    return sendReferralError(res, error, "Unable to load referral leaderboard.");
  }
});

router.get("/:code/events", async (req: Request, res: Response) => {
  if (!isReferralDatabaseConfigured()) {
    return res.status(503).json({
      error: "Referral database is not configured.",
    });
  }

  const code = normalizeReferralCode(typeof req.params.code === "string" ? req.params.code : undefined);
  if (!code) {
    return res.status(404).json({
      error: "Referral identifier not found.",
    });
  }

  try {
    const events = await getReferralEventsByCode(code, 100);
    if (!events) {
      return res.status(404).json({
        error: "Referral identifier not found.",
      });
    }

    return res.status(200).json(
      events.map((event) => ({
        id: event.id,
        referral_code: event.referralCode,
        transaction_id: event.transactionId,
        amount_usd: event.amountUsd,
        commission_usd: event.commissionUsd,
        status: event.status,
        created_at: event.createdAt,
        status_updated_at: event.statusUpdatedAt,
      })),
    );
  } catch (error) {
    return sendReferralError(res, error, "Unable to load referral events.");
  }
});

router.get("/:code", async (req: Request, res: Response) => {
  if (!isReferralDatabaseConfigured()) {
    return res.status(503).json({
      error: "Referral database is not configured.",
    });
  }

  const code = normalizeReferralCode(typeof req.params.code === "string" ? req.params.code : undefined);
  if (!code) {
    return res.status(404).json({
      error: "Referral identifier not found.",
    });
  }

  try {
    const stats = await getReferralStatsByCode(code);
    if (!stats) {
      return res.status(404).json({
        error: "Referral identifier not found.",
      });
    }

    return res.status(200).json({
      code: stats.code,
      earnings: stats.earnings,
      conversions: stats.conversions,
    });
  } catch (error) {
    return sendReferralError(res, error, "Unable to load referral stats.");
  }
});

export function createReferralRouter(): Router {
  return router;
}
