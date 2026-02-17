"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useState } from "react";

type WizardState = "CONFIGURE" | "REVIEW" | "PROCESSING" | "COMPLETE";

type ProcessingStep =
  | "SIGNAL_PULL"
  | "REGIME_CLASSIFICATION"
  | "ASSET_SELECTION"
  | "ALLOCATION_CONSTRUCTION"
  | "REPORT_GENERATION";

type RiskMode = "Conservative" | "Balanced" | "Growth" | "Aggressive";

type InvestmentHorizon = "< 1 Year" | "1-3 Years" | "3+ Years";

type AllocationRow = {
  asset: string;
  category: string;
  allocationPct: number;
};

type ProcessingStepMeta = {
  key: ProcessingStep;
  label: string;
};

type AgentPaymentReceipt = {
  transactionId: string;
  decisionId: string;
  agentNote: string;
};

type AgentPaymentResponse = {
  success: boolean;
  status?: "paid";
  transactionId?: string;
  decisionId?: string;
  agentNote?: string;
  error?: string;
};

type EthereumProvider = {
  request: (args: { method: string; params?: unknown[] | object }) => Promise<unknown>;
};

declare global {
  interface Window {
    ethereum?: EthereumProvider;
  }
}

const RISK_MODES: RiskMode[] = ["Conservative", "Balanced", "Growth", "Aggressive"];
const HORIZONS: InvestmentHorizon[] = ["< 1 Year", "1-3 Years", "3+ Years"];
const DEFAULT_RISK_MODE: RiskMode = "Balanced";
const DEFAULT_HORIZON: InvestmentHorizon = "1-3 Years";
const BASE_PRICE_USDC = 29;
const CERTIFIED_DECISION_RECORD_FEE_USDC = 25;
const PROCESSING_STEP_DURATION_MS = 800;
const WIZARD_FLOW: WizardState[] = ["CONFIGURE", "REVIEW", "PROCESSING", "COMPLETE"];

const PROCESSING_STEPS: ProcessingStepMeta[] = [
  { key: "SIGNAL_PULL", label: "Pulling market signals..." },
  { key: "REGIME_CLASSIFICATION", label: "Classifying regime..." },
  { key: "ASSET_SELECTION", label: "Selecting asset universe..." },
  { key: "ALLOCATION_CONSTRUCTION", label: "Constructing allocation..." },
  { key: "REPORT_GENERATION", label: "Generating structured report..." },
];

const ALLOCATION_ROWS: AllocationRow[] = [
  { asset: "BTC", category: "Core", allocationPct: 28 },
  { asset: "ETH", category: "Core", allocationPct: 22 },
  { asset: "SOL", category: "Growth", allocationPct: 12 },
  { asset: "LINK", category: "Infrastructure", allocationPct: 9 },
  { asset: "MKR", category: "DeFi", allocationPct: 8 },
  { asset: "ONDO", category: "RWA", allocationPct: 7 },
  { asset: "ARB", category: "L2", allocationPct: 6 },
  { asset: "USDC", category: "Stability", allocationPct: 8 },
];

const REGIME_DETECTED = "Late-cycle risk-on with selective defensive ballast";

const shortenAddress = (address: string) => `${address.slice(0, 6)}...${address.slice(-4)}`;

type ConfigureStepProps = {
  riskMode: RiskMode | null;
  investmentHorizon: InvestmentHorizon | null;
  onRiskModeSelect: (mode: RiskMode) => void;
  onInvestmentHorizonSelect: (horizon: InvestmentHorizon) => void;
  onContinue: () => void;
};

type SliderSelectorProps<T extends string> = {
  title: string;
  options: readonly T[];
  value: T | null;
  onSelect: (value: T) => void;
};

function SliderSelector<T extends string>({ title, options, value, onSelect }: SliderSelectorProps<T>) {
  const activeIndex = value ? options.indexOf(value) : -1;
  const maxIndex = Math.max(options.length - 1, 1);
  const progressWidth = activeIndex < 0 ? "0%" : `${(activeIndex / maxIndex) * 100}%`;

  return (
    <div className="rounded-xl border border-slate-300/70 bg-slate-50/80 p-4">
      <div className="flex items-center justify-between gap-2">
        <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">{title}</p>
        <span className="text-sm font-semibold text-cyan-800">{value ?? "Select"}</span>
      </div>

      <div className="mt-3 grid gap-2" style={{ gridTemplateColumns: `repeat(${options.length}, minmax(0,1fr))` }}>
        {options.map((option) => {
          const selected = value === option;
          return (
            <button
              key={option}
              type="button"
              onClick={() => onSelect(option)}
              className={`rounded-full border px-3 py-1.5 text-xs font-semibold transition ${
                selected
                  ? "border-cyan-700 bg-cyan-900 text-cyan-50"
                  : "border-slate-300 bg-white text-slate-600 hover:border-cyan-400"
              }`}
            >
              {option}
            </button>
          );
        })}
      </div>

      <div className="relative mt-4 h-8">
        <div className="absolute left-0 right-0 top-1/2 h-1.5 -translate-y-1/2 rounded-full border border-slate-300 bg-slate-200" />
        <div
          className="absolute left-0 top-1/2 h-1.5 -translate-y-1/2 rounded-full bg-cyan-400 transition-all duration-300"
          style={{ width: progressWidth }}
        />

        {options.map((option, index) => {
          const selected = value === option;
          const left = `${(index / maxIndex) * 100}%`;

          return (
            <button
              key={`${option}-stop`}
              type="button"
              onClick={() => onSelect(option)}
              className="absolute top-1/2 h-6 w-6 -translate-y-1/2 -translate-x-1/2"
              style={{ left }}
              aria-label={`Set ${title} to ${option}`}
            >
              <span
                className={`mx-auto block transition-all ${
                  selected
                    ? "h-6 w-6 rounded-full border-2 border-cyan-700 bg-cyan-300 shadow-[0_0_0_4px_rgba(6,182,212,0.16)]"
                    : "h-3 w-3 rotate-45 rounded-sm border border-slate-500 bg-slate-400"
                }`}
              />
            </button>
          );
        })}
      </div>
    </div>
  );
}

function ConfigureStep({
  riskMode,
  investmentHorizon,
  onRiskModeSelect,
  onInvestmentHorizonSelect,
  onContinue,
}: ConfigureStepProps) {
  const canContinue = Boolean(riskMode && investmentHorizon);

  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 1. Configure Allocation</h2>
      <p className="mt-2 text-slate-600">Align risk profile and investment horizon.</p>

      <div className="mt-6 grid gap-6 md:grid-cols-2">
        <SliderSelector title="Risk Mode" options={RISK_MODES} value={riskMode} onSelect={onRiskModeSelect} />
        <SliderSelector
          title="Investment Horizon"
          options={HORIZONS}
          value={investmentHorizon}
          onSelect={onInvestmentHorizonSelect}
        />
      </div>

      <div className="mt-6 flex justify-end">
        <button
          type="button"
          onClick={onContinue}
          disabled={!canContinue}
          className="rounded-full bg-cyan-500 px-5 py-2.5 text-sm font-semibold text-slate-950 shadow-sm transition hover:bg-cyan-400 disabled:cursor-not-allowed disabled:bg-slate-300 disabled:text-slate-500"
        >
          Continue
        </button>
      </div>
    </section>
  );
}

type ReviewStepProps = {
  riskMode: RiskMode;
  investmentHorizon: InvestmentHorizon;
  includeCertifiedDecisionRecord: boolean;
  totalPriceUsdc: number;
  walletAddress: string | null;
  isConnectingWallet: boolean;
  paymentError: string | null;
  isPaying: boolean;
  onToggleCertifiedDecisionRecord: (nextValue: boolean) => void;
  onConnectWallet: () => Promise<void>;
  onBack: () => void;
  onGenerate: () => void;
};

function ReviewStep({
  riskMode,
  investmentHorizon,
  includeCertifiedDecisionRecord,
  totalPriceUsdc,
  walletAddress,
  isConnectingWallet,
  paymentError,
  isPaying,
  onToggleCertifiedDecisionRecord,
  onConnectWallet,
  onBack,
  onGenerate,
}: ReviewStepProps) {
  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 2. Review & Confirm</h2>
      <p className="mt-2 text-slate-600">Confirm configuration and proceed to generation.</p>

      <div className="mt-6 grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Risk Mode</p>
          <p className="mt-2 text-lg font-semibold text-slate-800">{riskMode}</p>
        </div>
        <div className="rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Investment Horizon</p>
          <p className="mt-2 text-lg font-semibold text-slate-800">{investmentHorizon}</p>
        </div>
        <div className="rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
          <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Base Price</p>
          <p className="mt-2 text-lg font-semibold text-slate-800">${BASE_PRICE_USDC} USDC</p>
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
        <label className="flex cursor-pointer items-start gap-3">
          <input
            type="checkbox"
            checked={includeCertifiedDecisionRecord}
            onChange={(event) => onToggleCertifiedDecisionRecord(event.target.checked)}
            disabled={isPaying}
            className="mt-1 h-4 w-4 accent-cyan-700"
          />
          <div className="space-y-1">
            <p className="text-sm font-semibold text-slate-800">Add Certified Decision Record</p>
            <p className="text-sm font-semibold text-slate-700">${CERTIFIED_DECISION_RECORD_FEE_USDC} USDC</p>
            <p className="text-xs text-slate-600">
              Includes structured rationale, timestamped Decision ID, and formal PDF export.
            </p>
          </div>
        </label>
      </div>

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-white/80 p-4">
        <div className="flex items-center justify-between text-sm text-slate-700">
          <span>Structured Allocation Engine</span>
          <span>${BASE_PRICE_USDC} USDC</span>
        </div>
        <div className="mt-1 flex items-center justify-between text-sm text-slate-700">
          <span>Certified Decision Record</span>
          <span>{includeCertifiedDecisionRecord ? `$${CERTIFIED_DECISION_RECORD_FEE_USDC} USDC` : "$0 USDC"}</span>
        </div>
        <div className="mt-3 flex items-center justify-between border-t border-slate-200 pt-3 text-sm font-semibold text-slate-900">
          <span>Total</span>
          <span>${totalPriceUsdc} USDC</span>
        </div>
      </div>

      <div className="mt-4 rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Wallet</p>
            <p className="mt-1 text-sm font-semibold text-slate-800">
              {walletAddress ? `Connected: ${shortenAddress(walletAddress)}` : "Not connected"}
            </p>
            <p className="mt-1 text-xs text-slate-600">
              Payment is executed by Selun agent after wallet verification.
            </p>
          </div>

          <button
            type="button"
            onClick={onConnectWallet}
            disabled={isConnectingWallet || isPaying}
            className="rounded-full border border-slate-400 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-cyan-400 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {isConnectingWallet ? "Connecting..." : walletAddress ? "Reconnect Wallet" : "Connect Wallet"}
          </button>
        </div>
      </div>

      {paymentError && (
        <p className="mt-3 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm font-medium text-rose-700">
          {paymentError}
        </p>
      )}

      <div className="mt-6 flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={onBack}
          disabled={isPaying}
          className="rounded-full border border-slate-400 bg-white px-5 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-slate-500 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Back
        </button>
        <button
          type="button"
          onClick={onGenerate}
          disabled={isPaying || !walletAddress}
          className="rounded-full bg-cyan-500 px-5 py-2.5 text-sm font-semibold text-slate-950 shadow-sm transition hover:bg-cyan-400 disabled:cursor-wait disabled:bg-cyan-300"
        >
          {isPaying ? "Agent processing payment..." : `Agent Pay ${totalPriceUsdc} USDC & Continue`}
        </button>
      </div>
    </section>
  );
}

type ProcessingStepViewProps = {
  steps: ProcessingStepMeta[];
  currentStepIndex: number;
};

function ProcessingStepView({ steps, currentStepIndex }: ProcessingStepViewProps) {
  const progressPercent = ((currentStepIndex + 1) / steps.length) * 100;

  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 3. Processing</h2>
      <p className="mt-2 text-slate-600">Generating your structured allocation...</p>

      <div className="mt-5 h-2 w-full overflow-hidden rounded-full bg-slate-200">
        <div
          className="h-full rounded-full bg-cyan-500 transition-all duration-500"
          style={{ width: `${progressPercent}%` }}
        />
      </div>

      <div className="mt-6 space-y-3">
        {steps.map((step, index) => {
          const isDone = index < currentStepIndex;
          const isCurrent = index === currentStepIndex;
          const statusText = isDone ? "Done" : isCurrent ? "In Progress" : "Pending";

          return (
            <div
              key={step.key}
              className={`flex items-center justify-between rounded-xl border px-4 py-3 transition ${
                isCurrent
                  ? "border-cyan-300 bg-cyan-50"
                  : isDone
                    ? "border-emerald-200 bg-emerald-50"
                    : "border-slate-300 bg-white"
              }`}
            >
              <p className="text-sm font-medium text-slate-700">{step.label}</p>
              <span
                className={`rounded-full px-2.5 py-1 text-xs font-semibold uppercase tracking-[0.12em] ${
                  isCurrent
                    ? "bg-cyan-600 text-cyan-50"
                    : isDone
                      ? "bg-emerald-600 text-emerald-50"
                      : "bg-slate-400 text-slate-50"
                }`}
              >
                {statusText}
              </span>
            </div>
          );
        })}
      </div>
    </section>
  );
}

type CompleteStepProps = {
  regimeDetected: string;
  allocations: AllocationRow[];
  includeCertifiedDecisionRecord: boolean;
  walletAddress: string | null;
  agentPaymentReceipt: AgentPaymentReceipt | null;
  isDownloading: boolean;
  onDownloadReport: () => void;
  onStartOver: () => void;
};

function CompleteStep({
  regimeDetected,
  allocations,
  includeCertifiedDecisionRecord,
  walletAddress,
  agentPaymentReceipt,
  isDownloading,
  onDownloadReport,
  onStartOver,
}: CompleteStepProps) {
  return (
    <section className="rounded-2xl border border-slate-300/70 bg-white/70 p-6 backdrop-blur">
      <h2 className="text-2xl font-semibold text-slate-900">Step 4. Allocation Complete</h2>
      <p className="mt-2 text-slate-600">Your structured allocation report is ready.</p>

      <div className="mt-6 rounded-xl border border-slate-300/60 bg-slate-50/70 p-4">
        <p className="text-xs font-bold uppercase tracking-[0.16em] text-slate-500">Regime Detected</p>
        <p className="mt-2 text-lg font-semibold text-slate-800">{regimeDetected}</p>
        {includeCertifiedDecisionRecord && (
          <p className="mt-2 text-sm font-medium text-cyan-800">Certified Decision Record enabled</p>
        )}
        {walletAddress && <p className="mt-2 text-sm text-slate-700">Paid wallet: {shortenAddress(walletAddress)}</p>}
        {agentPaymentReceipt && (
          <p className="mt-1 text-xs text-slate-600">
            Agent payment confirmed: {agentPaymentReceipt.transactionId.slice(0, 14)}... | Decision ID{" "}
            {agentPaymentReceipt.decisionId}
          </p>
        )}
      </div>

      <div className="mt-6 overflow-hidden rounded-xl border border-slate-300/70 bg-white">
        <table className="min-w-full border-collapse">
          <thead>
            <tr className="bg-slate-100">
              <th className="px-4 py-3 text-left text-xs font-bold uppercase tracking-[0.14em] text-slate-500">Asset</th>
              <th className="px-4 py-3 text-left text-xs font-bold uppercase tracking-[0.14em] text-slate-500">Category</th>
              <th className="px-4 py-3 text-right text-xs font-bold uppercase tracking-[0.14em] text-slate-500">Allocation %</th>
            </tr>
          </thead>
          <tbody>
            {allocations.map((row) => (
              <tr key={row.asset} className="border-t border-slate-200">
                <td className="px-4 py-3 text-sm font-semibold text-slate-800">{row.asset}</td>
                <td className="px-4 py-3 text-sm text-slate-700">{row.category}</td>
                <td className="px-4 py-3 text-right text-sm font-semibold text-slate-800">{row.allocationPct}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-6 flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={onStartOver}
          disabled={isDownloading}
          className="rounded-full border border-slate-400 bg-white px-5 py-2.5 text-sm font-semibold text-slate-700 transition hover:border-slate-500 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Start Over
        </button>
        <button
          type="button"
          onClick={onDownloadReport}
          disabled={isDownloading}
          className="rounded-full bg-cyan-500 px-5 py-2.5 text-sm font-semibold text-slate-950 shadow-sm transition hover:bg-cyan-400 disabled:cursor-wait disabled:bg-cyan-300"
        >
          {isDownloading
            ? "Preparing Download..."
            : includeCertifiedDecisionRecord
              ? "Download Certified Decision Record"
              : "Download Report"}
        </button>
      </div>
    </section>
  );
}

function SelunAllocationWizard() {
  const [wizardState, setWizardState] = useState<WizardState>("CONFIGURE");
  const [riskMode, setRiskMode] = useState<RiskMode | null>(DEFAULT_RISK_MODE);
  const [investmentHorizon, setInvestmentHorizon] = useState<InvestmentHorizon | null>(DEFAULT_HORIZON);
  const [includeCertifiedDecisionRecord, setIncludeCertifiedDecisionRecord] = useState(false);
  const [walletAddress, setWalletAddress] = useState<string | null>(null);
  const [isConnectingWallet, setIsConnectingWallet] = useState(false);
  const [paymentError, setPaymentError] = useState<string | null>(null);
  const [agentPaymentReceipt, setAgentPaymentReceipt] = useState<AgentPaymentReceipt | null>(null);
  const [isPaying, setIsPaying] = useState(false);
  const [currentProcessingIndex, setCurrentProcessingIndex] = useState(0);
  const [isDownloading, setIsDownloading] = useState(false);
  const totalPriceUsdc =
    BASE_PRICE_USDC + (includeCertifiedDecisionRecord ? CERTIFIED_DECISION_RECORD_FEE_USDC : 0);

  useEffect(() => {
    if (wizardState !== "PROCESSING") return;

    let cancelled = false;
    let timeoutId: number | undefined;

    const runStep = (index: number) => {
      if (cancelled) return;
      setCurrentProcessingIndex(index);

      timeoutId = window.setTimeout(() => {
        if (cancelled) return;

        if (index >= PROCESSING_STEPS.length - 1) {
          setWizardState("COMPLETE");
          return;
        }

        runStep(index + 1);
      }, PROCESSING_STEP_DURATION_MS);
    };

    runStep(0);

    return () => {
      cancelled = true;
      if (timeoutId) window.clearTimeout(timeoutId);
    };
  }, [wizardState]);

  const canContinueFromConfigure = Boolean(riskMode && investmentHorizon);

  const handleContinueFromConfigure = () => {
    if (!canContinueFromConfigure) return;
    setWizardState("REVIEW");
  };

  const handleBackToConfigure = () => {
    if (isPaying) return;
    setPaymentError(null);
    setWizardState("CONFIGURE");
  };

  const handleConnectWallet = async () => {
    if (!window.ethereum?.request) {
      setPaymentError("No Ethereum wallet detected. Install MetaMask or another EVM wallet.");
      return;
    }

    setIsConnectingWallet(true);
    setPaymentError(null);

    try {
      const accountsResult = await window.ethereum.request({ method: "eth_requestAccounts" });

      if (!Array.isArray(accountsResult) || accountsResult.length === 0 || typeof accountsResult[0] !== "string") {
        throw new Error("Wallet connection did not return an account.");
      }

      setWalletAddress(accountsResult[0]);
    } catch (error) {
      setPaymentError(error instanceof Error ? error.message : "Wallet connection failed.");
    } finally {
      setIsConnectingWallet(false);
    }
  };

  const handleGenerateAllocation = async () => {
    if (!riskMode || !investmentHorizon || isPaying) return;
    if (!walletAddress) {
      setPaymentError("Connect wallet to authorize Selun agent payment.");
      return;
    }

    setIsPaying(true);
    setPaymentError(null);

    try {
      const response = await fetch("/api/agent/pay", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          walletAddress,
          totalPriceUsdc,
          includeCertifiedDecisionRecord,
          riskMode,
          investmentHorizon,
        }),
      });

      const paymentResult = (await response.json()) as AgentPaymentResponse;
      if (!response.ok || !paymentResult.success) {
        throw new Error(paymentResult.error || "Agent payment failed.");
      }

      if (!paymentResult.transactionId || !paymentResult.decisionId || !paymentResult.agentNote) {
        throw new Error("Incomplete agent payment response.");
      }

      setAgentPaymentReceipt({
        transactionId: paymentResult.transactionId,
        decisionId: paymentResult.decisionId,
        agentNote: paymentResult.agentNote,
      });
      setCurrentProcessingIndex(0);
      setWizardState("PROCESSING");
    } catch (error) {
      setAgentPaymentReceipt(null);
      setPaymentError(error instanceof Error ? error.message : "Agent payment failed.");
    } finally {
      setIsPaying(false);
    }
  };

  const handleDownloadReport = async () => {
    if (!riskMode || !investmentHorizon || isDownloading) return;

    try {
      setIsDownloading(true);

      const response = await fetch("/api/report/download", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          riskMode,
          investmentHorizon,
          includeCertifiedDecisionRecord,
          totalPriceUsdc,
          walletAddress,
          payment: agentPaymentReceipt
            ? {
                status: "paid",
                transactionId: agentPaymentReceipt.transactionId,
                decisionId: agentPaymentReceipt.decisionId,
                amountUsdc: totalPriceUsdc,
                agentNote: agentPaymentReceipt.agentNote,
              }
            : null,
          regimeDetected: REGIME_DETECTED,
          decisionRecord: includeCertifiedDecisionRecord
            ? {
                decisionId: agentPaymentReceipt?.decisionId ?? `SELUN-DEC-${Date.now()}`,
                generatedAt: new Date().toISOString(),
                rationaleSummary: "Structured rationale generated from deterministic pipeline outputs.",
                format: "formal-pdf-export-mock",
              }
            : null,
          allocations: ALLOCATION_ROWS,
        }),
      });

      const blob = await response.blob();
      const objectUrl = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = objectUrl;
      anchor.download = "selun-structured-allocation-report.json";
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      URL.revokeObjectURL(objectUrl);
    } finally {
      setIsDownloading(false);
    }
  };

  const handleStartOver = () => {
    if (isDownloading) return;
    setWizardState("CONFIGURE");
    setRiskMode(DEFAULT_RISK_MODE);
    setInvestmentHorizon(DEFAULT_HORIZON);
    setIncludeCertifiedDecisionRecord(false);
    setWalletAddress(null);
    setIsConnectingWallet(false);
    setPaymentError(null);
    setAgentPaymentReceipt(null);
    setIsPaying(false);
    setCurrentProcessingIndex(0);
    setIsDownloading(false);
  };

  return (
    <div className="mx-auto w-full max-w-5xl p-5 md:p-8">
      <header className="mb-6 rounded-2xl border border-slate-300/70 bg-white/65 p-5 backdrop-blur">
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div className="flex items-start gap-4">
            <Link
              href="/"
              className="rounded-xl border border-cyan-200/70 bg-white/80 p-2 shadow-sm transition hover:border-cyan-300 hover:shadow"
              aria-label="Go to Selun home page"
            >
              <Image
                src="/selun-mark.svg"
                alt="Selun mark"
                width={80}
                height={80}
                className="h-[58px] w-[80px] md:h-[80px] md:w-[80px]"
                priority
              />
            </Link>

            <div>
              <p className="text-xs font-bold uppercase tracking-[0.24em] text-cyan-700">SELUN AGENT</p>
              <h1 className="mt-2 text-3xl font-semibold tracking-tight text-slate-900 md:text-5xl">
                Crypto Allocation Agent
              </h1>
              <p className="mt-2 text-slate-600">
                Deterministic pipeline: configure, review, process, complete.
              </p>
            </div>
          </div>

          <Link
            href="/"
            className="inline-flex h-fit items-center justify-center rounded-full border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-cyan-400 hover:text-cyan-700"
          >
            Back to Home
          </Link>
        </div>
      </header>

      <div className="mb-6 grid grid-cols-2 gap-2 md:grid-cols-4">
        {WIZARD_FLOW.map((state, index) => {
          const isActive = wizardState === state;
          const isComplete = WIZARD_FLOW.indexOf(wizardState) > index;
          const stateLabel = state.charAt(0) + state.slice(1).toLowerCase();

          return (
            <div
              key={state}
              className={`flex items-center gap-2 rounded-lg border px-3 py-2 text-xs font-bold uppercase tracking-[0.12em] ${
                isActive
                  ? "border-cyan-700 bg-gradient-to-r from-cyan-900 to-cyan-700 text-cyan-50 shadow-sm"
                  : isComplete
                    ? "border-emerald-300 bg-emerald-50 text-emerald-700"
                    : "border-dashed border-slate-300 bg-slate-100/80 text-slate-500"
              }`}
            >
              <span
                className={`inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-md text-[10px] font-extrabold ${
                  isActive
                    ? "bg-cyan-200/20 text-cyan-50"
                    : isComplete
                      ? "bg-emerald-100 text-emerald-700"
                      : "bg-slate-200 text-slate-500"
                }`}
              >
                {index + 1}
              </span>
              <span className="truncate">{stateLabel}</span>
            </div>
          );
        })}
      </div>

      {wizardState === "CONFIGURE" && (
        <ConfigureStep
          riskMode={riskMode}
          investmentHorizon={investmentHorizon}
          onRiskModeSelect={setRiskMode}
          onInvestmentHorizonSelect={setInvestmentHorizon}
          onContinue={handleContinueFromConfigure}
        />
      )}

      {wizardState === "REVIEW" && riskMode && investmentHorizon && (
        <ReviewStep
          riskMode={riskMode}
          investmentHorizon={investmentHorizon}
          includeCertifiedDecisionRecord={includeCertifiedDecisionRecord}
          totalPriceUsdc={totalPriceUsdc}
          walletAddress={walletAddress}
          isConnectingWallet={isConnectingWallet}
          paymentError={paymentError}
          isPaying={isPaying}
          onToggleCertifiedDecisionRecord={setIncludeCertifiedDecisionRecord}
          onConnectWallet={handleConnectWallet}
          onBack={handleBackToConfigure}
          onGenerate={handleGenerateAllocation}
        />
      )}

      {wizardState === "PROCESSING" && (
        <ProcessingStepView steps={PROCESSING_STEPS} currentStepIndex={currentProcessingIndex} />
      )}

      {wizardState === "COMPLETE" && (
        <CompleteStep
          regimeDetected={REGIME_DETECTED}
          allocations={ALLOCATION_ROWS}
          includeCertifiedDecisionRecord={includeCertifiedDecisionRecord}
          walletAddress={walletAddress}
          agentPaymentReceipt={agentPaymentReceipt}
          isDownloading={isDownloading}
          onDownloadReport={handleDownloadReport}
          onStartOver={handleStartOver}
        />
      )}
    </div>
  );
}

export default function WizardPage() {
  return (
    <main className="min-h-screen bg-[radial-gradient(circle_at_12%_10%,rgba(125,211,252,0.22),transparent_38%),radial-gradient(circle_at_85%_8%,rgba(96,165,250,0.2),transparent_32%),linear-gradient(165deg,#eff6ff,#dbeafe,#e0e7ff)]">
      <SelunAllocationWizard />
    </main>
  );
}
