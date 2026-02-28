type AdminUsageChannel = "legacy_pay" | "x402_allocate";

type AdminUsageEmailInput = {
  channel: AdminUsageChannel;
  decisionId: string;
  walletAddress?: string | null;
  resultEmail?: string | null;
  promoCode?: string | null;
  chargedAmountUsdc?: string | number | null;
  transactionHash?: string | null;
  paymentMethod?: string | null;
  includeCertifiedDecisionRecord?: boolean | null;
  riskTolerance?: string | null;
  timeframe?: string | null;
  jobId?: string | null;
};

export type UserResultEmailStatus = "sent" | "skipped" | "failed";

type UserSummaryEmailInput = {
  toEmail: string;
  decisionId: string;
  riskMode: string;
  investmentHorizon: string;
  regimeDetected: string;
  strategyLabel: string;
  walletAddress: string;
  chargedAmountUsdc: number | null;
  transactionHash: string;
  certifiedDecisionRecordPurchased: boolean;
  allocations: Array<{
    asset: string;
    name: string;
    category: string;
    riskClass: string;
    allocationPct: number;
  }>;
};

type UserReportEmailInput = {
  toEmail: string;
  decisionId: string;
  filename: string;
  pdfBase64: string;
  riskMode: string;
  investmentHorizon: string;
  includeCertified: boolean;
  walletAddress: string;
  amountUsdc: number | null;
  transactionHash: string;
};

type ResendResponse = {
  id?: string;
  error?: {
    message?: string;
  };
};

type ResendAttachment = {
  filename: string;
  content: string;
};

function readBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function parseCsv(value: string | undefined): string[] {
  if (!value?.trim()) return [];
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export function isValidEmail(value: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function escapeHtml(value: string | number | boolean | null | undefined): string {
  return String(value ?? "n/a")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function isAdminEmailEnabled() {
  return readBooleanEnv("SELUN_ADMIN_USAGE_EMAILS_ENABLED", false);
}

function isUserResultEmailEnabled() {
  return readBooleanEnv("SELUN_RESULT_EMAILS_ENABLED", false);
}

function formatUsdc(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "n/a";
  return `${value.toFixed(2)} USDC`;
}

function formatPct(value: number): string {
  return `${value.toFixed(2).replace(/\.?0+$/, "")}%`;
}

function toTitleCase(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
    .join(" ");
}

async function sendViaResend(params: {
  to: string[];
  subject: string;
  text: string;
  html: string;
  idempotencyKey: string;
  attachments?: ResendAttachment[];
}): Promise<{ ok: true } | { ok: false; error: string }> {
  const apiKey = process.env.RESEND_API_KEY?.trim();
  const from = process.env.SELUN_EMAIL_FROM?.trim();

  if (!apiKey || !from) {
    return {
      ok: false,
      error: "Result email delivery is not configured.",
    };
  }

  try {
    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        "Idempotency-Key": params.idempotencyKey,
      },
      body: JSON.stringify({
        from,
        to: params.to,
        subject: params.subject,
        text: params.text,
        html: params.html,
        attachments: params.attachments,
      }),
    });

    if (!response.ok) {
      let errorMessage = `Resend email failed with HTTP ${response.status}.`;
      try {
        const body = (await response.json()) as ResendResponse;
        if (body?.error?.message) {
          errorMessage = body.error.message;
        }
      } catch {
        // best effort
      }
      return { ok: false, error: errorMessage };
    }

    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : "Resend email request failed.",
    };
  }
}

function buildAdminUsageTextPayload(input: AdminUsageEmailInput): string {
  const lines = [
    "Selun usage event received.",
    "",
    `Channel: ${input.channel}`,
    `Decision ID: ${input.decisionId}`,
    `Wallet: ${input.walletAddress ?? "n/a"}`,
    `Result Email: ${input.resultEmail ?? "n/a"}`,
    `Promo Code: ${input.promoCode ?? "n/a"}`,
    `Charged Amount (USDC): ${input.chargedAmountUsdc ?? "n/a"}`,
    `Payment Method: ${input.paymentMethod ?? "n/a"}`,
    `Transaction Hash: ${input.transactionHash ?? "n/a"}`,
    `Certified Report: ${input.includeCertifiedDecisionRecord === null || input.includeCertifiedDecisionRecord === undefined ? "n/a" : String(input.includeCertifiedDecisionRecord)}`,
    `Risk Tolerance: ${input.riskTolerance ?? "n/a"}`,
    `Timeframe: ${input.timeframe ?? "n/a"}`,
    `Job ID: ${input.jobId ?? "n/a"}`,
    `Timestamp: ${new Date().toISOString()}`,
  ];
  return lines.join("\n");
}

function buildAdminUsageHtmlPayload(input: AdminUsageEmailInput): string {
  return `
    <h2>Selun Usage Event</h2>
    <p><strong>Channel:</strong> ${escapeHtml(input.channel)}</p>
    <p><strong>Decision ID:</strong> ${escapeHtml(input.decisionId)}</p>
    <p><strong>Wallet:</strong> ${escapeHtml(input.walletAddress)}</p>
    <p><strong>Result Email:</strong> ${escapeHtml(input.resultEmail)}</p>
    <p><strong>Promo Code:</strong> ${escapeHtml(input.promoCode)}</p>
    <p><strong>Charged Amount (USDC):</strong> ${escapeHtml(input.chargedAmountUsdc)}</p>
    <p><strong>Payment Method:</strong> ${escapeHtml(input.paymentMethod)}</p>
    <p><strong>Transaction Hash:</strong> ${escapeHtml(input.transactionHash)}</p>
    <p><strong>Certified Report:</strong> ${escapeHtml(input.includeCertifiedDecisionRecord)}</p>
    <p><strong>Risk Tolerance:</strong> ${escapeHtml(input.riskTolerance)}</p>
    <p><strong>Timeframe:</strong> ${escapeHtml(input.timeframe)}</p>
    <p><strong>Job ID:</strong> ${escapeHtml(input.jobId)}</p>
    <p><strong>Timestamp:</strong> ${escapeHtml(new Date().toISOString())}</p>
  `.trim();
}

export async function sendAdminUsageEmail(input: AdminUsageEmailInput): Promise<void> {
  if (!isAdminEmailEnabled()) return;

  const recipients = parseCsv(process.env.SELUN_ADMIN_USAGE_EMAILS).filter((email) => isValidEmail(email));
  if (recipients.length === 0) {
    return;
  }

  const result = await sendViaResend({
    to: recipients,
    subject: `Selun Usage: ${input.channel} (${input.decisionId})`,
    text: buildAdminUsageTextPayload(input),
    html: buildAdminUsageHtmlPayload(input),
    idempotencyKey: `selun-usage-${input.channel}-${input.decisionId}`,
  });

  if (!result.ok) {
    throw new Error(result.error);
  }
}

export async function sendUserSummaryEmail(input: UserSummaryEmailInput): Promise<{ status: UserResultEmailStatus; error?: string }> {
  if (!isUserResultEmailEnabled()) {
    return {
      status: "skipped",
      error: "Result email delivery is disabled.",
    };
  }
  if (!isValidEmail(input.toEmail)) {
    return {
      status: "failed",
      error: "Recipient email address is invalid.",
    };
  }

  const allocationTextRows = input.allocations.length > 0
    ? input.allocations.map(
      (row, index) =>
        `${index + 1}. ${row.asset} (${row.name}) - ${formatPct(row.allocationPct)} | ${toTitleCase(row.category)} | ${toTitleCase(row.riskClass)}`,
    )
    : ["Allocation rows were unavailable in this email snapshot."];

  const allocationHtmlRows = input.allocations.length > 0
    ? input.allocations
      .map(
        (row) => `
          <tr>
            <td style="padding:8px;border:1px solid #dbe2ea;font-weight:600;">${escapeHtml(row.asset)}</td>
            <td style="padding:8px;border:1px solid #dbe2ea;">${escapeHtml(row.name)}</td>
            <td style="padding:8px;border:1px solid #dbe2ea;">${escapeHtml(toTitleCase(row.category))}</td>
            <td style="padding:8px;border:1px solid #dbe2ea;">${escapeHtml(toTitleCase(row.riskClass))}</td>
            <td style="padding:8px;border:1px solid #dbe2ea;text-align:right;">${escapeHtml(formatPct(row.allocationPct))}</td>
          </tr>
        `.trim(),
      )
      .join("")
    : `
      <tr>
        <td colspan="5" style="padding:8px;border:1px solid #dbe2ea;">Allocation rows were unavailable in this email snapshot.</td>
      </tr>
    `.trim();

  const textBody = [
    "Your Selun allocation is ready.",
    "",
    `Decision ID: ${input.decisionId}`,
    `Risk Mode: ${input.riskMode}`,
    `Investment Horizon: ${input.investmentHorizon}`,
    `Market Condition: ${input.regimeDetected}`,
    `Strategy: ${input.strategyLabel}`,
    `Certified Decision Report Purchased: ${input.certifiedDecisionRecordPurchased ? "Yes" : "No"}`,
    `Wallet: ${input.walletAddress}`,
    `Charged Amount: ${formatUsdc(input.chargedAmountUsdc)}`,
    `Payment Transaction: ${input.transactionHash}`,
    "",
    "Allocation Summary:",
    ...allocationTextRows,
    "",
    input.certifiedDecisionRecordPurchased
      ? "You can still download the certified decision record from Selun to receive the PDF copy."
      : "This email preserves your allocation summary even without a certified report purchase.",
  ].join("\n");

  const htmlBody = `
    <div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.55;">
      <h2 style="margin-bottom:8px;">Your Selun Allocation Is Ready</h2>
      <p><strong>Decision ID:</strong> ${escapeHtml(input.decisionId)}</p>
      <p><strong>Risk Mode:</strong> ${escapeHtml(input.riskMode)}</p>
      <p><strong>Investment Horizon:</strong> ${escapeHtml(input.investmentHorizon)}</p>
      <p><strong>Market Condition:</strong> ${escapeHtml(input.regimeDetected)}</p>
      <p><strong>Strategy:</strong> ${escapeHtml(input.strategyLabel)}</p>
      <p><strong>Certified Decision Report Purchased:</strong> ${input.certifiedDecisionRecordPurchased ? "Yes" : "No"}</p>
      <p><strong>Wallet:</strong> ${escapeHtml(input.walletAddress)}</p>
      <p><strong>Charged Amount:</strong> ${escapeHtml(formatUsdc(input.chargedAmountUsdc))}</p>
      <p><strong>Payment Transaction:</strong> ${escapeHtml(input.transactionHash)}</p>
      <h3 style="margin-top:24px;margin-bottom:8px;">Allocation Summary</h3>
      <table style="border-collapse:collapse;width:100%;font-size:14px;">
        <thead>
          <tr style="background:#eff6ff;">
            <th style="padding:8px;border:1px solid #dbe2ea;text-align:left;">Asset</th>
            <th style="padding:8px;border:1px solid #dbe2ea;text-align:left;">Name</th>
            <th style="padding:8px;border:1px solid #dbe2ea;text-align:left;">Role</th>
            <th style="padding:8px;border:1px solid #dbe2ea;text-align:left;">Risk Class</th>
            <th style="padding:8px;border:1px solid #dbe2ea;text-align:right;">Allocation</th>
          </tr>
        </thead>
        <tbody>${allocationHtmlRows}</tbody>
      </table>
      <p style="margin-top:16px;">
        ${input.certifiedDecisionRecordPurchased
          ? "You can still download the certified decision record from Selun to receive the PDF copy."
          : "This email preserves your allocation summary even without a certified report purchase."}
      </p>
    </div>
  `.trim();

  const result = await sendViaResend({
    to: [input.toEmail],
    subject: `Selun Allocation Summary - ${input.decisionId}`,
    text: textBody,
    html: htmlBody,
    idempotencyKey: `selun-summary-${input.decisionId}-${input.toEmail.toLowerCase()}`,
  });

  return result.ok ? { status: "sent" } : { status: "failed", error: result.error };
}

export async function sendUserReportEmail(input: UserReportEmailInput): Promise<{ status: UserResultEmailStatus; error?: string }> {
  if (!isUserResultEmailEnabled()) {
    return {
      status: "skipped",
      error: "Result email delivery is disabled.",
    };
  }
  if (!isValidEmail(input.toEmail)) {
    return {
      status: "failed",
      error: "Recipient email address is invalid.",
    };
  }

  const reportType = input.includeCertified ? "Certified Decision Record" : "Structured Allocation Report";
  const amountText = formatUsdc(input.amountUsdc);
  const textBody = [
    "Your Selun report is ready.",
    "",
    `Decision ID: ${input.decisionId}`,
    `Report Type: ${reportType}`,
    `Risk Mode: ${input.riskMode}`,
    `Investment Horizon: ${input.investmentHorizon}`,
    `Wallet: ${input.walletAddress}`,
    `Charged Amount: ${amountText}`,
    `Payment Transaction: ${input.transactionHash}`,
  ].join("\n");

  const htmlBody = `
    <h2>Your Selun Report Is Ready</h2>
    <p><strong>Decision ID:</strong> ${escapeHtml(input.decisionId)}</p>
    <p><strong>Report Type:</strong> ${escapeHtml(reportType)}</p>
    <p><strong>Risk Mode:</strong> ${escapeHtml(input.riskMode)}</p>
    <p><strong>Investment Horizon:</strong> ${escapeHtml(input.investmentHorizon)}</p>
    <p><strong>Wallet:</strong> ${escapeHtml(input.walletAddress)}</p>
    <p><strong>Charged Amount:</strong> ${escapeHtml(amountText)}</p>
    <p><strong>Payment Transaction:</strong> ${escapeHtml(input.transactionHash)}</p>
    <p>The report PDF is attached.</p>
  `.trim();

  const result = await sendViaResend({
    to: [input.toEmail],
    subject: `Selun ${reportType} - ${input.decisionId}`,
    text: textBody,
    html: htmlBody,
    idempotencyKey: `selun-result-${input.decisionId}-${input.toEmail.toLowerCase()}`,
    attachments: [
      {
        filename: input.filename,
        content: input.pdfBase64,
      },
    ],
  });

  return result.ok ? { status: "sent" } : { status: "failed", error: result.error };
}
