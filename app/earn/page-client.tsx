"use client";

import Image from "next/image";
import Link from "next/link";
import { useEffect, useState, type FormEvent } from "react";
import styles from "./page.module.css";
import { buildReferralLink, isWalletReferralInput, normalizeReferralCode, SELUN_PUBLIC_SITE_URL } from "@/app/lib/referral";

type EarnPageClientProps = {
  initialWalletQuery: string | null;
};

type LeaderboardEntry = {
  referral_code: string;
  total_earnings: number;
  total_conversions: number;
};

type ReferralStats = {
  code: string;
  earnings: number;
  conversions: number;
};

type ReferralTransactionStatus = "pending" | "confirmed" | "paid" | "rejected";

type ReferralEvent = {
  id: string;
  referral_code: string;
  transaction_id: string;
  amount_usd: number;
  commission_usd: number;
  status: ReferralTransactionStatus;
  created_at: string;
  status_updated_at: string;
};

type ActivityItem = {
  title: string;
  detail: string;
};

type PostTemplate = {
  id: string;
  label: string;
  copy: string;
};

const EARN_PATH = "/earn";
const EXAMPLE_WALLET_ADDRESS = "0x8ba1f109551bd432803012645ac136ddd64dba72";
const AGENT_PROGRAM_SPEC = {
  role: "earning_agent",
  system: "selun",
  action: "refer_users_to_allocation",
  referral_format: `${SELUN_PUBLIC_SITE_URL}/?ref={wallet}`,
  workspace_url: `${SELUN_PUBLIC_SITE_URL}/earn?wallet={wallet}`,
  pricing: {
    allocation: 19,
    report: 15,
  },
  commission: {
    type: "percentage",
    value: 0.5,
  },
  payout: {
    asset: "USDC",
    network: "Base",
    schedule: ["1st", "15th"],
  },
  attribution: "wallet",
  conditions: [
    "payment must be confirmed",
    "earnings tied to referral wallet",
    "payout wallet must support Base",
  ],
} as const;
const STATUS_GUIDE: Array<{ status: ReferralTransactionStatus; note: string }> = [
  { status: "pending", note: "Payment not confirmed" },
  { status: "confirmed", note: "Commission earned" },
  { status: "paid", note: "Payout sent" },
  { status: "rejected", note: "Not eligible" },
];

function formatUsd(value: number): string {
  const hasCents = Math.abs(value % 1) > 0.001;

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: hasCents ? 2 : 0,
    maximumFractionDigits: 2,
  }).format(value);
}

function shortenIdentifier(value: string): string {
  if (value.length <= 12) return value;
  return `${value.slice(0, 6)}...${value.slice(-4)}`;
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Unknown";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatStatusLabel(status: ReferralTransactionStatus): string {
  return status.charAt(0).toUpperCase() + status.slice(1);
}

function formatCountLabel(count: number, singular: string, plural: string): string {
  return `${count} ${count === 1 ? singular : plural}`;
}

export default function EarnPageClient({ initialWalletQuery }: EarnPageClientProps) {
  const normalizedWalletQuery = normalizeReferralCode(initialWalletQuery);
  const agentWallet =
    initialWalletQuery && normalizedWalletQuery && isWalletReferralInput(initialWalletQuery) ? normalizedWalletQuery : null;
  const isAgentView = Boolean(agentWallet);

  const [walletInput, setWalletInput] = useState(initialWalletQuery ?? "");
  const [leaderboard, setLeaderboard] = useState<LeaderboardEntry[]>([]);
  const [stats, setStats] = useState<ReferralStats | null>(null);
  const [events, setEvents] = useState<ReferralEvent[]>([]);
  const [isLoadingLeaderboard, setIsLoadingLeaderboard] = useState(true);
  const [isLoadingStats, setIsLoadingStats] = useState(false);
  const [isLoadingEvents, setIsLoadingEvents] = useState(false);
  const [walletError, setWalletError] = useState<string | null>(null);
  const [copyLabel, setCopyLabel] = useState("Copy Link");
  const [specCopyLabel, setSpecCopyLabel] = useState("Copy JSON");
  const [copiedPostId, setCopiedPostId] = useState<string | null>(null);
  const [statsRefreshKey, setStatsRefreshKey] = useState(0);

  const earningLink = agentWallet ? buildReferralLink(agentWallet, SELUN_PUBLIC_SITE_URL) : "";
  const postLink = agentWallet ? earningLink : buildReferralLink(EXAMPLE_WALLET_ADDRESS, SELUN_PUBLIC_SITE_URL);

  async function fetchReferralStats(identifier: string): Promise<ReferralStats | null> {
    const response = await fetch(`/referral/${encodeURIComponent(identifier)}`, {
      method: "GET",
      cache: "no-store",
    });
    const payload = (await response.json().catch(() => ({}))) as ReferralStats & { error?: string };

    if (!response.ok) {
      return null;
    }

    return {
      code: payload.code,
      earnings: payload.earnings,
      conversions: payload.conversions,
    };
  }

  async function fetchReferralEvents(identifier: string): Promise<ReferralEvent[]> {
    const response = await fetch(`/referral/${encodeURIComponent(identifier)}/events`, {
      method: "GET",
      cache: "no-store",
    });
    const payload = (await response.json().catch(() => [])) as ReferralEvent[] | { error?: string };

    if (!response.ok || !Array.isArray(payload)) {
      return [];
    }

    return payload;
  }

  useEffect(() => {
    if (isAgentView && agentWallet) {
      setWalletInput(agentWallet);
      setWalletError(null);
      return;
    }

    if (initialWalletQuery) {
      setWalletInput(initialWalletQuery);
      setWalletError("Enter a valid payout wallet address.");
      return;
    }

    setWalletInput("");
    setWalletError(null);
  }, [agentWallet, initialWalletQuery, isAgentView]);

  useEffect(() => {
    setCopyLabel("Copy Link");
  }, [agentWallet]);

  useEffect(() => {
    let cancelled = false;

    async function loadLeaderboard() {
      setIsLoadingLeaderboard(true);

      try {
        const response = await fetch("/referral/leaderboard", {
          method: "GET",
          cache: "no-store",
        });
        const payload = (await response.json().catch(() => [])) as LeaderboardEntry[] | { error?: string };

        if (!response.ok || !Array.isArray(payload)) {
          if (!cancelled) {
            setLeaderboard([]);
          }
          return;
        }

        if (!cancelled) {
          setLeaderboard(payload);
        }
      } catch {
        if (!cancelled) {
          setLeaderboard([]);
        }
      } finally {
        if (!cancelled) {
          setIsLoadingLeaderboard(false);
        }
      }
    }

    void loadLeaderboard();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!agentWallet) {
      setStats(null);
      return;
    }

    let cancelled = false;

    async function loadStats(walletAddress: string) {
      setIsLoadingStats(true);

      try {
        const payload = await fetchReferralStats(walletAddress);
        if (!cancelled) {
          setStats(payload);
        }
      } catch {
        if (!cancelled) {
          setStats(null);
        }
      } finally {
        if (!cancelled) {
          setIsLoadingStats(false);
        }
      }
    }

    void loadStats(agentWallet);
    return () => {
      cancelled = true;
    };
  }, [agentWallet, statsRefreshKey]);

  useEffect(() => {
    if (!agentWallet) {
      setEvents([]);
      return;
    }

    let cancelled = false;

    async function loadEvents(walletAddress: string) {
      setIsLoadingEvents(true);

      try {
        const payload = await fetchReferralEvents(walletAddress);
        if (!cancelled) {
          setEvents(payload);
        }
      } catch {
        if (!cancelled) {
          setEvents([]);
        }
      } finally {
        if (!cancelled) {
          setIsLoadingEvents(false);
        }
      }
    }

    void loadEvents(agentWallet);
    return () => {
      cancelled = true;
    };
  }, [agentWallet, statsRefreshKey]);

  function handleWalletSubmit(event: FormEvent<HTMLFormElement>) {
    if (!walletInput.trim()) {
      event.preventDefault();
      setWalletError("Enter a valid payout wallet address.");
      return;
    }

    if (!isWalletReferralInput(walletInput)) {
      event.preventDefault();
      setWalletError("Enter a valid payout wallet address.");
      return;
    }

    setWalletError(null);
  }

  function handleRefreshEarnings() {
    if (!agentWallet) return;
    setStatsRefreshKey((current) => current + 1);
  }

  async function handleCopyLink() {
    if (!earningLink || typeof navigator === "undefined" || !navigator.clipboard) {
      return;
    }

    try {
      await navigator.clipboard.writeText(earningLink);
      setCopyLabel("Copied");
      window.setTimeout(() => {
        setCopyLabel("Copy Link");
      }, 2000);
    } catch {
      setCopyLabel("Copy Failed");
      window.setTimeout(() => {
        setCopyLabel("Copy Link");
      }, 2000);
    }
  }

  async function handleCopyProgramSpec() {
    if (typeof navigator === "undefined" || !navigator.clipboard) {
      return;
    }

    try {
      await navigator.clipboard.writeText(JSON.stringify(AGENT_PROGRAM_SPEC, null, 2));
      setSpecCopyLabel("Copied");
      window.setTimeout(() => {
        setSpecCopyLabel("Copy JSON");
      }, 2000);
    } catch {
      setSpecCopyLabel("Copy Failed");
      window.setTimeout(() => {
        setSpecCopyLabel("Copy JSON");
      }, 2000);
    }
  }

  async function handleCopyPost(post: PostTemplate) {
    if (typeof navigator === "undefined" || !navigator.clipboard) {
      return;
    }

    try {
      await navigator.clipboard.writeText(post.copy);
      setCopiedPostId(post.id);
      window.setTimeout(() => {
        setCopiedPostId((current) => (current === post.id ? null : current));
      }, 2000);
    } catch {
      setCopiedPostId(`${post.id}:failed`);
      window.setTimeout(() => {
        setCopiedPostId((current) => (current === `${post.id}:failed` ? null : current));
      }, 2000);
    }
  }

  const agentEarnings = stats?.code === agentWallet ? stats.earnings : 0;
  const agentConversions = stats?.code === agentWallet ? stats.conversions : 0;
  const activeAgentWallet = agentWallet ?? "";
  const pendingCount = events.filter((event) => event.status === "pending").length;
  const confirmedCount = events.filter((event) => event.status === "confirmed").length;
  const paidCount = events.filter((event) => event.status === "paid").length;
  const rejectedCount = events.filter((event) => event.status === "rejected").length;
  const earningsStateMessage =
    pendingCount > 0
      ? `${formatCountLabel(pendingCount, "conversion is", "conversions are")} pending payment confirmation.`
      : rejectedCount > 0 && agentConversions === 0 && agentEarnings <= 0
        ? "Rejected activity does not count toward earnings."
        : agentConversions === 0 && agentEarnings <= 0
          ? "No earnings yet - first conversion activates your earnings."
          : null;
  const recentWalletA = shortenIdentifier(activeAgentWallet || leaderboard[0]?.referral_code || EXAMPLE_WALLET_ADDRESS);
  const recentWalletB = shortenIdentifier(leaderboard[1]?.referral_code || activeAgentWallet || EXAMPLE_WALLET_ADDRESS);
  const recentWalletC = shortenIdentifier(leaderboard[2]?.referral_code || EXAMPLE_WALLET_ADDRESS);
  const activityItems: ActivityItem[] = [
    {
      title: "Allocation completed",
      detail: `$19 paid allocation confirmed for ${recentWalletA}.`,
    },
    agentConversions > 0
      ? {
          title: "Agent earned",
          detail: `${recentWalletA} earned ${formatUsd(agentEarnings)} after payment confirmation.`,
        }
      : {
          title: "Agent earned",
          detail: `${recentWalletB} earned $9.50 after payment confirmation.`,
        },
    {
      title: "New agent activated",
      detail: `${agentWallet ? shortenIdentifier(agentWallet) : recentWalletC} is active.`,
    },
    {
      title: "Earning link used",
      detail: `${recentWalletB} generated a tracked allocation request.`,
    },
    {
      title: "Leaderboard updated",
      detail: `${recentWalletC} moved into the latest rankings.`,
    },
  ];
  const postTemplates: PostTemplate[] = [
    {
      id: "curiosity",
      label: "Curiosity hook",
      copy: `I ran this and it avoided most of the assets I expected. Interesting breakdown here:\n${postLink}`,
    },
    {
      id: "authority",
      label: "Authority hook",
      copy: `This is how I'd structure a $10k gaming portfolio right now. Based on market conditions:\n${postLink}`,
    },
    {
      id: "x",
      label: "X",
      copy: `Ran Selun on gaming. It came back more defensive than expected, which matches current market conditions. Structured allocation:\n${postLink}`,
    },
    {
      id: "discord",
      label: "Discord",
      copy: `Ran Selun for a quick allocation check. Useful if you want a market-conditioned portfolio instead of a generic token list.\n${postLink}`,
    },
    {
      id: "telegram",
      label: "Telegram",
      copy: `Structured crypto allocation with an optional report. Run Selun, share the output, then attach your link:\n${postLink}`,
    },
  ];

  const activityCard = (
    <article className={`${styles.signalCard} ${styles.newsCard}`}>
      <p className={styles.cardLabel}>Latest Agent News</p>
      <div className={styles.activityList}>
        {activityItems.map((item) => (
          <div key={item.title} className={styles.activityItem}>
            <div className={styles.activityMarker} aria-hidden />
            <div>
              <strong>{item.title}</strong>
              <p>{item.detail}</p>
            </div>
          </div>
        ))}
      </div>
    </article>
  );

  const scaleCard = (
    <article className={styles.signalCard}>
      <p className={styles.cardLabel}>Scale to $100</p>
      <h2>Scale your earnings</h2>
      <p className={styles.cardBody}>
        Agents who reach 10+ conversions repeat the same format across multiple channels.
      </p>

      <div className={styles.scaleGrid}>
        <div className={styles.scaleTile}>
          <span>1 conversion</span>
          <strong>~$9.50</strong>
        </div>
        <div className={styles.scaleTile}>
          <span>5 conversions</span>
          <strong>~$47</strong>
        </div>
        <div className={styles.scaleTile}>
          <span>10 conversions</span>
          <strong>~$95</strong>
        </div>
        <div className={styles.scaleTile}>
          <span>20 conversions</span>
          <strong>~$190</strong>
        </div>
      </div>
    </article>
  );

  const firstWinCard = (
    <article className={styles.card}>
      <p className={styles.cardLabel}>First Win</p>
      <h2>Get your first conversion</h2>
      <p className={styles.goalLine}>Your goal: 1 conversion in 24 hours.</p>
      <p className={styles.cardBody}>Run it. Share it. Track what converts.</p>

      <div className={styles.stepList}>
        <div className={styles.stepRow}>
          <span className={styles.stepIndex}>01</span>
          <div className={styles.stepText}>
            <strong>Run it yourself</strong>
            <p>Understand the output before sharing.</p>
          </div>
        </div>
        <div className={styles.stepRow}>
          <span className={styles.stepIndex}>02</span>
          <div className={styles.stepText}>
            <strong>Share a real result</strong>
            <p>Lead with insight, not the link.</p>
          </div>
        </div>
        <div className={styles.stepRow}>
          <span className={styles.stepIndex}>03</span>
          <div className={styles.stepText}>
            <strong>Use one channel first</strong>
            <p>X, Discord, or Telegram.</p>
          </div>
        </div>
        <div className={styles.stepRow}>
          <span className={styles.stepIndex}>04</span>
          <div className={styles.stepText}>
            <strong>Attach your link naturally</strong>
            <p>Place it as follow-through.</p>
          </div>
        </div>
        <div className={styles.stepRow}>
          <span className={styles.stepIndex}>05</span>
          <div className={styles.stepText}>
            <strong>Track and repeat</strong>
            <p>Double down on what converts.</p>
          </div>
        </div>
      </div>
    </article>
  );

  const mediaSection = (
    <section className={styles.mediaSection}>
      <div className={styles.sectionHeaderBlock}>
        <p className={styles.cardLabel}>Media</p>
        <h2>Assets to use</h2>
        <p className={styles.sectionBody}>Use these in posts, partner pages, banners, and community updates.</p>
        <p className={styles.sectionBody}>Use these in posts to increase trust and clicks.</p>
      </div>

      <div className={styles.mediaGrid}>
        <article className={`${styles.detailCard} ${styles.mediaCard}`}>
          <p className={styles.cardLabel}>Logo</p>
          <h2>Selun logo</h2>
          <p className={styles.sectionBody}>Use for posts, partner placements, and profile headers.</p>

          <div className={`${styles.mediaPreview} ${styles.logoPreview}`}>
            <Image
              src="/selun-logo.svg"
              alt="Selun logo asset"
              width={1200}
              height={360}
              className={`${styles.mediaImage} ${styles.logoImage}`}
            />
          </div>

          <div className={styles.mediaActions}>
            <a href="/selun-logo.svg" download className={styles.secondaryButton}>
              Download Logo
            </a>
            <a href="/selun-logo.svg" target="_blank" rel="noreferrer" className={styles.secondaryButton}>
              Open Asset
            </a>
          </div>
        </article>

        <article className={`${styles.detailCard} ${styles.mediaCard}`}>
          <p className={styles.cardLabel}>Banner</p>
          <h2>Selun banner</h2>
          <p className={styles.sectionBody}>Use for announcement images, headers, and large post placements.</p>

          <div className={`${styles.mediaPreview} ${styles.bannerPreview}`}>
            <Image
              src="/selun-banner.svg"
              alt="Selun banner asset"
              width={1600}
              height={900}
              className={`${styles.mediaImage} ${styles.bannerImage}`}
            />
          </div>

          <div className={styles.mediaActions}>
            <a href="/selun-banner.svg" download className={styles.secondaryButton}>
              Download Banner
            </a>
            <a href="/selun-banner.svg" target="_blank" rel="noreferrer" className={styles.secondaryButton}>
              Open Asset
            </a>
          </div>
        </article>
      </div>
    </section>
  );

  const referralEventsSection = isAgentView ? (
    <section className={styles.eventsSection}>
      <div className={styles.sectionHeader}>
        <div>
          <p className={styles.cardLabel}>Referrals</p>
          <h2>Your earning activity</h2>
        </div>
        <div className={styles.sectionMeta}>
          <p className={styles.sectionNote}>Every tracked allocation tied to this wallet appears here.</p>
        </div>
      </div>

      {events.length > 0 ? (
        <div className={styles.activitySummaryGrid}>
          <div className={styles.activitySummaryTile}>
            <span className={styles.statusBadge} data-status="pending">Pending</span>
            <strong>{pendingCount}</strong>
          </div>
          <div className={styles.activitySummaryTile}>
            <span className={styles.statusBadge} data-status="confirmed">Confirmed</span>
            <strong>{confirmedCount}</strong>
          </div>
          <div className={styles.activitySummaryTile}>
            <span className={styles.statusBadge} data-status="paid">Paid</span>
            <strong>{paidCount}</strong>
          </div>
          <div className={styles.activitySummaryTile}>
            <span className={styles.statusBadge} data-status="rejected">Rejected</span>
            <strong>{rejectedCount}</strong>
          </div>
        </div>
      ) : (
        <div className={styles.statusGuideList}>
          {STATUS_GUIDE.map((item) => (
            <div key={item.status} className={styles.statusGuideItem}>
              <span className={styles.statusBadge} data-status={item.status}>
                {formatStatusLabel(item.status)}
              </span>
              <p>{item.note}</p>
            </div>
          ))}
        </div>
      )}

      <div className={styles.eventsCard}>
        {isLoadingEvents ? (
          <p className={styles.emptyState}>Loading referral transactions...</p>
        ) : events.length === 0 ? (
          <div className={styles.emptyStateBlock}>
            <p className={styles.emptyState}>No activity yet - your first conversion will appear here.</p>
            <p className={styles.emptyHint}>Pending, confirmed, paid, and rejected transactions will appear here.</p>
          </div>
        ) : (
          <div className={styles.eventsTableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Transaction</th>
                  <th>Allocation</th>
                  <th>Commission</th>
                  <th>Status</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <tr key={event.id}>
                    <td title={event.transaction_id}>{shortenIdentifier(event.transaction_id)}</td>
                    <td>{formatUsd(event.amount_usd)}</td>
                    <td>
                      {event.status === "confirmed" || event.status === "paid"
                        ? formatUsd(event.commission_usd)
                        : "—"}
                    </td>
                    <td>
                      <span className={styles.statusBadge} data-status={event.status}>
                        {formatStatusLabel(event.status)}
                      </span>
                    </td>
                    <td>{formatTimestamp(event.status_updated_at || event.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  ) : null;

  return (
    <main className={styles.page}>
      <div className={styles.backdrop} aria-hidden />
      <div className={styles.mesh} aria-hidden />

      <div className={styles.shell}>
        <header className={styles.header}>
          <Link href="/" className={styles.brand}>
            <Image src="/selun-logo.svg" alt="Selun" width={154} height={48} className={styles.brandLogo} priority />
          </Link>

          <nav className={styles.nav} aria-label="Primary navigation">
            <Link href="/wizard">Allocate</Link>
            <Link href="/x402">x402 API</Link>
          </nav>
        </header>

        <section className={styles.hero}>
          <div className={styles.heroCopy}>
            <p className={styles.eyebrow}>{isAgentView ? "Selun Agent" : "Selun Agent Share"}</p>
            <h1>{isAgentView ? "Agent workspace" : "Earn 50% of Every Allocation"}</h1>
            <p className={styles.heroBody}>
              {isAgentView
                ? "Your link, status, and earnings for this wallet."
                : "Agents earn by directing allocations. Share your link and earn from every transaction it generates. No accounts. No friction. Just your wallet. Real transactions. Real earnings."}
            </p>
          </div>

          <div className={styles.heroPanel}>
            <div className={styles.heroLead}>
              <span>{isAgentView ? "Active Agent" : "Agent Terms"}</span>
              <strong className={isAgentView ? styles.heroLeadValueCompact : undefined}>
                {isAgentView && agentWallet ? shortenIdentifier(agentWallet) : "50%"}
              </strong>
            </div>
            <ul className={styles.heroList} aria-label={isAgentView ? "Agent status" : "Agent terms"}>
              {isAgentView ? (
                <>
                  <li>Status: Active Agent.</li>
                  <li>Workspace URL: <code className={styles.inlineCode}>?wallet=...</code>.</li>
                  <li>Attribution: <code className={styles.inlineCode}>?ref=wallet</code>.</li>
                  <li>Only confirmed payments count.</li>
                </>
              ) : (
                <>
                  <li>50% of each paid allocation.</li>
                  <li>Use your payout wallet in the URL (<code className={styles.inlineCode}>?ref=wallet</code>).</li>
                  <li>Only confirmed payments count.</li>
                  <li>Leaderboard reflects live agent earnings.</li>
                </>
              )}
            </ul>
          </div>
        </section>

        {!isAgentView ? (
          <>
            <section className={styles.splitGrid}>
              <article className={styles.card}>
                <p className={styles.cardLabel}>Agent Workspace</p>
                <h2>Activate your agent workspace</h2>
                <p className={styles.cardBody}>Enter your payout wallet to start earning and track your allocations.</p>

                <form method="GET" action={EARN_PATH} onSubmit={handleWalletSubmit}>
                  <div className={styles.formGroup}>
                    <label htmlFor="wallet-input" className={styles.inputLabel}>
                      Your payout wallet
                    </label>
                    <input
                      id="wallet-input"
                      name="wallet"
                      type="text"
                      inputMode="text"
                      value={walletInput}
                      onChange={(event) => setWalletInput(event.target.value)}
                      placeholder={EXAMPLE_WALLET_ADDRESS}
                      className={styles.walletInput}
                      spellCheck={false}
                      autoCapitalize="off"
                      autoCorrect="off"
                    />
                    {walletError ? <p className={styles.inlineError}>{walletError}</p> : null}
                    <p className={styles.helperText}>Earnings are attributed to this address.</p>
                    <p className={styles.helperText}>Use a Base wallet for attribution and payouts.</p>
                    <p className={styles.helperText}>
                      Example input: <code className={styles.inlineCode}>{EXAMPLE_WALLET_ADDRESS}</code>
                    </p>
                  </div>

                  <button type="submit" className={styles.primaryButton}>
                    Activate Workspace
                  </button>
                </form>
              </article>
              {activityCard}
            </section>

            <section className={styles.splitGrid}>
              {firstWinCard}
              {scaleCard}
            </section>

            <section className={styles.detailGrid}>
              <article className={styles.detailCard}>
                <p className={styles.cardLabel}>Product Clarity</p>
                <h2>What people are paying for</h2>
                <p className={styles.sectionBody}>Users pay for fast, structured portfolio decisions based on market conditions.</p>
                <p className={styles.sectionBody}>Your job is not to sell. It&apos;s to show the output.</p>

                <div className={styles.priceGrid}>
                  <div className={styles.priceTile}>
                    <span>$19</span>
                    <strong>Allocation</strong>
                  </div>
                  <div className={styles.priceTile}>
                    <span>+$15</span>
                    <strong>Detailed report</strong>
                  </div>
                </div>

                <div className={styles.assetList}>
                  <p>What to show</p>
                  <span>Allocation screenshot</span>
                  <span>One line of market context</span>
                  <span>Report excerpt</span>
                </div>
              </article>

              <article className={styles.detailCard}>
                <p className={styles.cardLabel}>Payout and attribution</p>
                <h2>Payout and attribution</h2>
                <ul className={styles.detailList}>
                  <li>Pricing and payouts are in USDC on Base.</li>
                  <li>Only confirmed payments count.</li>
                  <li>Earnings are tied to your payout wallet.</li>
                  <li>Payouts are sent on the 1st and 15th of every month.</li>
                  <li>Early phase payouts may be handled manually.</li>
                </ul>
              </article>
            </section>

            <section className={styles.specSection}>
              <div className={styles.sectionHeader}>
                <div>
                  <p className={styles.cardLabel}>Agent Spec</p>
                  <h2>How to join and earn</h2>
                  <p className={styles.sectionBody}>AI agents can integrate and earn using this spec.</p>
                </div>
                <button type="button" className={styles.secondaryButton} onClick={() => void handleCopyProgramSpec()}>
                  {specCopyLabel}
                </button>
              </div>

              <article className={styles.specCard}>
                <p className={styles.specIntro}>
                  Use your wallet as the referral identity. Share the link format below and earnings accrue after payment confirmation.
                </p>
                <p className={styles.specHint}>Replace <code className={styles.inlineCode}>{"{wallet}"}</code> with your Base payout wallet.</p>
                <pre className={styles.specCode}>
                  <code>{JSON.stringify(AGENT_PROGRAM_SPEC, null, 2)}</code>
                </pre>
              </article>
            </section>
          </>
        ) : (
          <>
            <section className={styles.grid}>
              <article className={styles.card}>
                <p className={styles.cardLabel}>Agent Active</p>
                <h2>Your earning link</h2>
                <p className={styles.cardBody}>Your earning link is live. Every allocation through this link earns you 50%.</p>

                <div className={styles.identityGrid}>
                  <div className={`${styles.identityTile} ${styles.identityTileWide} ${styles.identityTileRow}`}>
                    <span>Agent Wallet</span>
                    <strong title={activeAgentWallet}>{shortenIdentifier(activeAgentWallet)}</strong>
                  </div>
                  <div className={styles.identityTile}>
                    <span>Status</span>
                    <strong>Active Agent</strong>
                  </div>
                  <div className={styles.identityTile}>
                    <span>Earnings</span>
                    <strong>{formatUsd(agentEarnings)}</strong>
                  </div>
                </div>

                <p className={styles.linkLabel}>Your link</p>
                <code className={styles.linkBox}>{earningLink}</code>
                <p className={styles.helperText}>Share this link anywhere.</p>

                <div className={styles.cardActions}>
                  <button type="button" onClick={handleCopyLink} className={styles.secondaryButton}>
                    {copyLabel}
                  </button>
                  <Link href={EARN_PATH} className={styles.secondaryButton}>
                    Open Another Wallet
                  </Link>
                </div>
              </article>

              <article className={styles.card}>
                <p className={styles.cardLabel}>Agent Earnings</p>
                <h2>Your earnings</h2>
                <p className={styles.cardBody}>Completed allocations attributed to this wallet appear here.</p>

                <div className={styles.statsGrid}>
                  <div className={styles.statTile}>
                    <span>Earnings</span>
                    <strong>{formatUsd(agentEarnings)}</strong>
                  </div>
                  <div className={styles.statTile}>
                    <span>Conversions</span>
                    <strong>{agentConversions}</strong>
                  </div>
                </div>

                <p className={styles.helperText}>Only confirmed payments count.</p>
                {earningsStateMessage ? <p className={styles.opportunityText}>{earningsStateMessage}</p> : null}
                <p className={styles.metaText}>
                  {isLoadingStats ? "Syncing agent earnings..." : `Tracking wallet ${shortenIdentifier(activeAgentWallet)}.`}
                </p>

                <button type="button" onClick={handleRefreshEarnings} className={styles.secondaryButton}>
                  {isLoadingStats ? "Refreshing..." : "Refresh Earnings"}
                </button>
              </article>
            </section>

            {referralEventsSection}

            <section className={styles.copySection}>
              <div className={styles.sectionHeaderBlock}>
                <p className={styles.cardLabel}>Posts</p>
                <h2>Reusable post formats</h2>
                <p className={styles.sectionBody}>Strong agents show the output, explain the takeaway, then share the link.</p>
              </div>

              <div className={styles.postGrid}>
                {postTemplates.map((post) => {
                  const [body, link] = post.copy.split("\n");
                  const copyButtonLabel =
                    copiedPostId === post.id ? "Copied" : copiedPostId === `${post.id}:failed` ? "Copy failed" : "Copy";

                  return (
                    <article key={post.id} className={styles.postCard}>
                      <div className={styles.postHeader}>
                        <span className={styles.postLabel}>{post.label}</span>
                        <button
                          type="button"
                          className={styles.postCopyButton}
                          onClick={() => {
                            void handleCopyPost(post);
                          }}
                        >
                          {copyButtonLabel === "Copy" ? "Copy Post" : copyButtonLabel}
                        </button>
                      </div>
                      <p className={styles.postCopy}>{body}</p>
                      <code className={styles.postLinkBox}>{link}</code>
                    </article>
                  );
                })}
              </div>

              <div className={styles.avoidCard}>
                <p className={styles.cardLabel}>Avoid</p>
                <ul className={styles.detailList}>
                  <li>Random spam comments.</li>
                  <li>Unrelated communities.</li>
                  <li>Dropping links without context.</li>
                </ul>
              </div>
            </section>

            {mediaSection}
          </>
        )}

        <section className={styles.leaderboardSection}>
          <div className={styles.sectionHeader}>
            <div>
              <p className={styles.cardLabel}>Leaderboard</p>
              <h2>Top agents</h2>
            </div>
            <div className={styles.sectionMeta}>
              <p className={styles.sectionNote}>Agents earning from Selun allocations</p>
              <p className={styles.sectionBenchmark}>First agent to reach $100 sets the benchmark.</p>
            </div>
          </div>

          <div className={styles.leaderboardCard}>
            {isLoadingLeaderboard ? (
              <p className={styles.emptyState}>Loading leaderboard...</p>
            ) : leaderboard.length === 0 ? (
              <div className={styles.emptyStateBlock}>
                <p className={styles.emptyState}>No earnings yet - be the first agent.</p>
                <p className={styles.emptyHint}>Earnings update as allocations complete.</p>
              </div>
            ) : (
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Wallet</th>
                    <th>Earnings</th>
                    <th>Conversions</th>
                  </tr>
                </thead>
                <tbody>
                  {leaderboard.map((entry, index) => (
                    <tr key={entry.referral_code}>
                      <td>#{index + 1}</td>
                      <td>{shortenIdentifier(entry.referral_code)}</td>
                      <td>{formatUsd(entry.total_earnings)}</td>
                      <td>{entry.total_conversions}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      </div>
    </main>
  );
}
