import { Router, type Request, type Response } from "express";
import {
  confirmAgentAllocation,
  getAgentReferrerActivity,
  getAgentProgramConfig,
  getAgentReferrerStats,
  isAgentProgramDatabaseConfigured,
  registerAgentReferrer,
  requestAgentWithdrawal,
  sendAgentConversionWebhook,
  signAgentProgramPayload,
  trackAgentReferral,
} from "../services/agent-program.service";

const router = Router();

type JsonRecord = Record<string, unknown>;

function getTrimmedString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function getOptionalString(value: unknown): string | null {
  const trimmed = getTrimmedString(value);
  return trimmed || null;
}

function getOptionalNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function readReferrerId(req: Request): string {
  return (
    getTrimmedString(req.body?.referrer_id) ||
    getTrimmedString(req.body?.referrerId) ||
    getTrimmedString(req.query?.referrer_id) ||
    getTrimmedString(req.query?.referrerId) ||
    getTrimmedString(req.query?.ref) ||
    getTrimmedString(req.body?.ref)
  );
}

function isDatabaseConfigurationError(error: unknown): boolean {
  return error instanceof Error && /DATABASE_URL/i.test(error.message);
}

function classifyStatusCode(error: unknown): number {
  if (!(error instanceof Error)) {
    return 500;
  }
  if (isDatabaseConfigurationError(error)) return 503;
  if (/cooldown/i.test(error.message)) return 429;
  if (/signature/i.test(error.message)) return 401;
  if (/not found/i.test(error.message)) return 404;
  if (/locked/i.test(error.message)) return 409;
  if (/required|invalid|valid|non-negative/i.test(error.message)) return 400;
  return 500;
}

function sendSignedJson(res: Response, status: number, payload: JsonRecord) {
  const signature = signAgentProgramPayload(payload);
  return res.status(status).json(signature ? { ...payload, signature } : payload);
}

function sendError(res: Response, error: unknown, fallback: string) {
  const status = classifyStatusCode(error);
  const message = error instanceof Error && error.message.trim() ? error.message : fallback;
  return sendSignedJson(res, status, { error: message });
}

router.post("/referrer/register", async (req: Request, res: Response) => {
  if (!isAgentProgramDatabaseConfigured()) {
    return sendSignedJson(res, 503, { error: "Agent referral database is not configured." });
  }

  try {
    const type = getOptionalString(req.body?.type);
    const registration = await registerAgentReferrer({
      type: type === "human" || type === "agent" ? type : undefined,
      walletAddress: getOptionalString(req.body?.wallet_address) ?? getOptionalString(req.body?.walletAddress),
      webhookUrl: getOptionalString(req.body?.webhook_url) ?? getOptionalString(req.body?.webhookUrl),
    });
    const config = getAgentProgramConfig();
    return sendSignedJson(res, 201, {
      referrer_id: registration.referrerId,
      type: registration.type,
      wallet_address: registration.walletAddress,
      webhook_url: registration.webhookUrl,
      referral_url: registration.referralUrl,
      payout_amount: config.fixedPayoutUsdc,
      currency: config.payoutAsset,
      network: config.payoutNetwork,
      minimum_withdrawal: config.minimumWithdrawalUsdc,
      created_at: registration.createdAt,
    });
  } catch (error) {
    return sendError(res, error, "Unable to register referrer.");
  }
});

router.post("/referral/track", async (req: Request, res: Response) => {
  if (!isAgentProgramDatabaseConfigured()) {
    return sendSignedJson(res, 503, { error: "Agent referral database is not configured." });
  }

  const referrerId = readReferrerId(req);
  const userId = getTrimmedString(req.body?.user_id) || getTrimmedString(req.body?.userId);
  const event = getTrimmedString(req.body?.event) || "click";

  if (!referrerId) {
    return sendSignedJson(res, 400, { error: "referrer_id is required." });
  }
  if (!userId) {
    return sendSignedJson(res, 400, { error: "user_id is required." });
  }

  try {
    const result = await trackAgentReferral({
      referrerId,
      userId,
      event,
      source: getOptionalString(req.body?.source),
    });
    return sendSignedJson(res, 200, {
      status: result.status,
      accepted: result.status === "tracked",
      recorded: result.recorded,
      referrer_id: result.referrerId,
      attributed_referrer_id: result.attributedReferrerId,
      user_id: result.userId,
      event: result.event,
      source: result.source,
      created_attribution: result.createdAttribution,
      timestamp: result.timestamp,
    });
  } catch (error) {
    return sendError(res, error, "Unable to track referral event.");
  }
});

router.post("/allocation/confirm", async (req: Request, res: Response) => {
  if (!isAgentProgramDatabaseConfigured()) {
    return sendSignedJson(res, 503, { error: "Agent referral database is not configured." });
  }

  const referrerId = readReferrerId(req);
  const userId = getTrimmedString(req.body?.user_id) || getTrimmedString(req.body?.userId);
  const allocationId = getTrimmedString(req.body?.allocation_id) || getTrimmedString(req.body?.allocationId);

  if (!referrerId) {
    return sendSignedJson(res, 400, { error: "referrer_id is required." });
  }
  if (!userId) {
    return sendSignedJson(res, 400, { error: "user_id is required." });
  }
  if (!allocationId) {
    return sendSignedJson(res, 400, { error: "allocation_id is required." });
  }

  try {
    const confirmation = await confirmAgentAllocation({
      referrerId,
      userId,
      allocationId,
      amountUsd: getOptionalNumber(req.body?.amount) ?? getOptionalNumber(req.body?.amount_usd) ?? getOptionalNumber(req.body?.amountUsd),
      allocationAmountUsd:
        getOptionalNumber(req.body?.allocation_amount) ??
        getOptionalNumber(req.body?.allocation_amount_usd) ??
        getOptionalNumber(req.body?.allocationAmountUsd),
      reportAmountUsd:
        getOptionalNumber(req.body?.report_amount) ??
        getOptionalNumber(req.body?.report_amount_usd) ??
        getOptionalNumber(req.body?.reportAmountUsd),
      totalPaidUsd:
        getOptionalNumber(req.body?.total_paid) ??
        getOptionalNumber(req.body?.total_paid_usd) ??
        getOptionalNumber(req.body?.totalPaidUsd) ??
        getOptionalNumber(req.body?.amount) ??
        getOptionalNumber(req.body?.amount_usd) ??
        getOptionalNumber(req.body?.amountUsd),
      paymentReference:
        getOptionalString(req.body?.payment_reference) ??
        getOptionalString(req.body?.paymentReference) ??
        allocationId,
      source: getOptionalString(req.body?.source),
    });

    if (confirmation.webhookUrl && !confirmation.result.duplicate) {
      void sendAgentConversionWebhook(confirmation.webhookUrl, {
        event: "allocation_confirmed",
        referrer_id: confirmation.result.referrerId,
        user_id: confirmation.result.userId,
        allocation_id: confirmation.result.allocationId,
        payout: confirmation.result.payout,
        currency: confirmation.result.currency,
        network: confirmation.result.network,
        amount_usd: confirmation.result.amountUsd,
        confirmed_at: confirmation.result.confirmedAt,
      });
    }

    return sendSignedJson(res, 200, {
      status: confirmation.result.status,
      payout: confirmation.result.payout,
      currency: confirmation.result.currency,
      network: confirmation.result.network,
      referrer_id: confirmation.result.referrerId,
      user_id: confirmation.result.userId,
      allocation_id: confirmation.result.allocationId,
      payment_reference: confirmation.result.paymentReference,
      payout_status: confirmation.result.payoutStatus,
      duplicate: confirmation.result.duplicate,
      confirmed_at: confirmation.result.confirmedAt,
    });
  } catch (error) {
    return sendError(res, error, "Unable to confirm allocation.");
  }
});

router.get("/referrer/stats", async (req: Request, res: Response) => {
  if (!isAgentProgramDatabaseConfigured()) {
    return sendSignedJson(res, 503, { error: "Agent referral database is not configured." });
  }

  const referrerId = readReferrerId(req);
  if (!referrerId) {
    return sendSignedJson(res, 400, { error: "referrer_id is required." });
  }

  try {
    const stats = await getAgentReferrerStats(referrerId);
    if (!stats) {
      return sendSignedJson(res, 404, { error: "Referrer not found." });
    }

    return sendSignedJson(res, 200, {
      referrer_id: stats.referrerId,
      clicks: stats.clicks,
      allocations: stats.allocations,
      conversion_rate: stats.conversionRate,
      total_payout: stats.totalPayout,
      pending_payout: stats.pendingPayout,
      paid_payout: stats.paidPayout,
      minimum_withdrawal: stats.minimumWithdrawal,
      currency: stats.currency,
      network: stats.network,
      wallet_address: stats.walletAddress,
    });
  } catch (error) {
    return sendError(res, error, "Unable to load referrer stats.");
  }
});

router.post("/payouts/withdraw", async (req: Request, res: Response) => {
  if (!isAgentProgramDatabaseConfigured()) {
    return sendSignedJson(res, 503, { error: "Agent referral database is not configured." });
  }

  const referrerId = readReferrerId(req);
  if (!referrerId) {
    return sendSignedJson(res, 400, { error: "referrer_id is required." });
  }

  try {
    const result = await requestAgentWithdrawal({
      referrerId,
      walletAddress: getOptionalString(req.body?.wallet_address) ?? getOptionalString(req.body?.walletAddress),
      signature: getOptionalString(req.body?.signature),
      signedAt: getOptionalString(req.body?.signed_at) ?? getOptionalString(req.body?.signedAt),
    });

    return sendSignedJson(res, 200, {
      status: result.status,
      referrer_id: result.referrerId,
      amount: result.amount,
      currency: result.currency,
      network: result.network,
      wallet_address: result.walletAddress,
      payout_count: result.payoutCount,
      minimum_withdrawal: result.minimumWithdrawal,
      withdrawal_id: result.withdrawalId,
      requested_at: result.requestedAt,
    });
  } catch (error) {
    return sendError(res, error, "Unable to request withdrawal.");
  }
});

router.get("/referrer/activity", async (req: Request, res: Response) => {
  if (!isAgentProgramDatabaseConfigured()) {
    return sendSignedJson(res, 503, { error: "Agent referral database is not configured." });
  }

  const referrerId = readReferrerId(req);
  if (!referrerId) {
    return sendSignedJson(res, 400, { error: "referrer_id is required." });
  }

  try {
    const limit = getOptionalNumber(req.query?.limit) ?? 100;
    const activity = await getAgentReferrerActivity(referrerId, limit);
    if (!activity) {
      return sendSignedJson(res, 404, { error: "Referrer not found." });
    }

    return sendSignedJson(res, 200, {
      referrer_id: referrerId,
      activity: activity.map((entry) => ({
        referrer_id: entry.referrerId,
        user_id: entry.userId,
        allocation_id: entry.allocationId,
        payment_reference: entry.paymentReference,
        allocation_amount_usd: entry.allocationAmountUsd,
        report_amount_usd: entry.reportAmountUsd,
        total_paid_usd: entry.totalPaidUsd,
        payout_amount_usd: entry.payoutAmountUsd,
        payout_status: entry.payoutStatus,
        source: entry.source,
        created_at: entry.createdAt,
        confirmed_at: entry.confirmedAt,
        paid_at: entry.paidAt,
      })),
    });
  } catch (error) {
    return sendError(res, error, "Unable to load referrer activity.");
  }
});

export function createAgentProgramRouter(): Router {
  return router;
}
