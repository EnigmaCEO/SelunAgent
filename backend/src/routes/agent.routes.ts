import { Router, type Request, type Response } from "express";
import { isAddress } from "viem";
import { EXECUTION_MODEL_VERSION } from "../config";
import { getExecutionLogs } from "../logging/execution-logs";
import {
  authorizeWizardPayment,
  getAgentAddress,
  getUSDCBalanceForAddress,
  getWizardPricing,
  initializeAgent,
  quoteWizardPayment,
  storeDecisionHashOnChain,
  verifyIncomingPayment,
} from "../services/selun-agent.service";
import {
  CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
  EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
  EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
  REVIEW_MARKET_CONDITIONS_PHASE,
  SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
  runPhase1,
  runPhase3,
  runPhase4,
  runPhase5,
  runPhase6,
} from "../services/phase1-execution.service";

const router = Router();

function success<T>(res: Response, data: T) {
  return res.status(200).json({
    success: true,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    data,
    logs: getExecutionLogs(120),
  });
}

function failure(res: Response, error: unknown, status = 500) {
  const message = error instanceof Error ? error.message : "Unexpected error.";
  return res.status(status).json({
    success: false,
    executionModelVersion: EXECUTION_MODEL_VERSION,
    error: message,
    logs: getExecutionLogs(120),
  });
}

router.post("/init", async (_req: Request, res: Response) => {
  try {
    const identity = await initializeAgent();
    return success(res, identity);
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.get("/wallet", async (_req: Request, res: Response) => {
  try {
    const identity = await getAgentAddress();
    const balance = await getUSDCBalanceForAddress(identity.walletAddress);
    return success(res, {
      ...identity,
      usdc: {
        contractAddress: balance.usdcContractAddress,
        balance: balance.usdcBalance,
        balanceBaseUnits: balance.usdcBalanceBaseUnits,
      },
    });
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.get("/pricing", (_req: Request, res: Response) => {
  try {
    return success(res, getWizardPricing());
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.post("/usdc-balance", async (req: Request, res: Response) => {
  const walletAddress = req.body?.walletAddress;

  if (typeof walletAddress !== "string") {
    return failure(res, new Error("walletAddress is required."), 400);
  }
  if (!isAddress(walletAddress)) {
    return failure(res, new Error("walletAddress must be a valid EVM address."), 400);
  }

  try {
    const balance = await getUSDCBalanceForAddress(walletAddress);
    return success(res, balance);
  } catch (error) {
    return failure(res, error, 500);
  }
});

router.post("/verify-payment", async (req: Request, res: Response) => {
  const fromAddress = req.body?.fromAddress;
  const expectedAmountUSDC = req.body?.expectedAmountUSDC;
  const transactionHash = req.body?.transactionHash;
  const decisionId = req.body?.decisionId;

  if (typeof fromAddress !== "string") {
    return failure(res, new Error("fromAddress is required."), 400);
  }
  if (!(typeof expectedAmountUSDC === "string" || typeof expectedAmountUSDC === "number")) {
    return failure(res, new Error("expectedAmountUSDC is required."), 400);
  }
  if (transactionHash !== undefined && typeof transactionHash !== "string") {
    return failure(res, new Error("transactionHash must be a string when provided."), 400);
  }
  if (decisionId !== undefined && typeof decisionId !== "string") {
    return failure(res, new Error("decisionId must be a string when provided."), 400);
  }

  try {
    const receipt = await verifyIncomingPayment({
      fromAddress,
      expectedAmountUSDC,
      transactionHash,
      decisionId,
    });
    return success(res, receipt);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/pay", async (req: Request, res: Response) => {
  const walletAddress = req.body?.walletAddress;

  if (typeof walletAddress !== "string") {
    return failure(res, new Error("walletAddress is required."), 400);
  }

  try {
    const receipt = await authorizeWizardPayment({
      walletAddress,
      includeCertifiedDecisionRecord: req.body?.includeCertifiedDecisionRecord,
      riskMode: req.body?.riskMode,
      investmentHorizon: req.body?.investmentHorizon,
      promoCode: req.body?.promoCode,
    });
    return success(res, receipt);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/pay-quote", async (req: Request, res: Response) => {
  const walletAddress = req.body?.walletAddress;

  if (typeof walletAddress !== "string") {
    return failure(res, new Error("walletAddress is required."), 400);
  }

  try {
    const quote = await quoteWizardPayment({
      walletAddress,
      includeCertifiedDecisionRecord: req.body?.includeCertifiedDecisionRecord,
      promoCode: req.body?.promoCode,
    });
    return success(res, quote);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/store-hash", async (req: Request, res: Response) => {
  const decisionId = req.body?.decisionId;
  const pdfHash = req.body?.pdfHash;

  if (typeof decisionId !== "string" || !decisionId.trim()) {
    return failure(res, new Error("decisionId is required."), 400);
  }
  if (typeof pdfHash !== "string" || !pdfHash.trim()) {
    return failure(res, new Error("pdfHash is required."), 400);
  }

  try {
    const receipt = await storeDecisionHashOnChain({ decisionId, pdfHash });
    return success(res, receipt);
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase1/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  const riskMode = typeof req.body?.riskMode === "string" ? req.body.riskMode : undefined;
  const timeWindow = typeof req.body?.timeWindow === "string" ? req.body.timeWindow : undefined;
  const executionTimestamp = typeof req.body?.executionTimestamp === "string"
    ? req.body.executionTimestamp
    : undefined;
  const riskTolerance = typeof req.body?.riskTolerance === "string" ? req.body.riskTolerance : undefined;
  const investmentTimeframe = typeof req.body?.investmentTimeframe === "string"
    ? req.body.investmentTimeframe
    : undefined;
  const walletAddress = typeof req.body?.walletAddress === "string" ? req.body.walletAddress : undefined;

  try {
    runPhase1({
      jobId,
      executionTimestamp,
      riskMode,
      riskTolerance,
      investmentTimeframe,
      timeWindow,
      walletAddress,
    });
    return res.status(202).json({
      status: "started",
      phase: REVIEW_MARKET_CONDITIONS_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase3/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase3(jobId);
    return res.status(202).json({
      status: "started",
      phase: EXPAND_ELIGIBLE_ASSET_UNIVERSE_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase4/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase4(jobId);
    return res.status(202).json({
      status: "started",
      phase: SCREEN_LIQUIDITY_AND_STRUCTURAL_STABILITY_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase5/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase5(jobId);
    return res.status(202).json({
      status: "started",
      phase: EVALUATE_ASSET_RISK_AND_QUALITY_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

router.post("/phase6/run", (req: Request, res: Response) => {
  const jobId = req.body?.jobId;

  if (typeof jobId !== "string" || !jobId.trim()) {
    return failure(res, new Error("jobId is required."), 400);
  }

  try {
    runPhase6(jobId);
    return res.status(202).json({
      status: "started",
      phase: CONSTRUCT_PORTFOLIO_ALLOCATION_PHASE,
    });
  } catch (error) {
    return failure(res, error, 400);
  }
});

export function createAgentRouter() {
  return router;
}
