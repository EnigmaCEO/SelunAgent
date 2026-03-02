"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import styles from "./page.module.css";

type WalletSnapshot = {
  agentId: string;
  walletAddress: string;
  network: string;
  usdcContractAddress: string;
  nativeBalance: string;
  nativeBalanceBaseUnits?: string;
  usdcBalance: string;
};

type ProjectWalletSnapshot = {
  agentId: string | null;
  walletAddress: string;
  network: string;
  usdcContractAddress: string;
  nativeBalance: string;
  nativeBalanceBaseUnits?: string;
  usdcBalance: string;
  policies: string[];
  active: boolean;
};

type TreasuryOwnerWalletSnapshot = {
  name: string;
  walletAddress: string;
  network: string;
  usdcContractAddress: string;
  nativeBalance: string;
  nativeBalanceBaseUnits?: string;
  usdcBalance: string;
  usdcBalanceBaseUnits?: string;
  policies: string[];
};

type TreasurySmartAccountSnapshot = {
  name: string;
  walletAddress: string;
  ownerAddresses: string[];
  network: string;
  usdcContractAddress: string;
  nativeBalance: string;
  nativeBalanceBaseUnits?: string;
  usdcBalance: string;
  usdcBalanceBaseUnits?: string;
  policies: string[];
  paymasterUrl: string | null;
  sponsorshipMode: "configured_paymaster" | "manual";
};

type TreasuryWalletSetupSnapshot = {
  ownerName: string;
  smartAccountName: string;
  paymasterUrl: string | null;
  owner: TreasuryOwnerWalletSnapshot | null;
  smartAccount: TreasurySmartAccountSnapshot | null;
};

type AdminPurchase = {
  purchaseKey: string;
  kind: "allocate" | "tool";
  endpoint: string;
  title: string;
  decisionId: string;
  productId: string;
  chargedAmountUsdc: string;
  fromAddress: string;
  paymentTransactionHash: string;
  paymentNetwork: string | null;
  purchasedAt: string;
  refunded: boolean;
  refund: {
    refundedAt: string;
    transactionHash: string;
    toAddress: string;
    amountUsdc: string;
    note: string | null;
  } | null;
};

type OverviewResponse = {
  success: boolean;
  error?: string;
  data?: {
    wallet: WalletSnapshot;
    wallets: ProjectWalletSnapshot[];
    adminRefundAddress: string;
    defaultGasTopUpAmountEth: string;
    adminFundingWallet?: {
      walletAddress: string;
      network: string;
      nativeBalance: string;
      nativeBalanceBaseUnits: string;
    } | null;
    transferMode?: {
      kind: string;
      gaslessUsdc: boolean;
      note: string;
    };
    treasury: TreasuryWalletSetupSnapshot;
    purchases: AdminPurchase[];
  };
};

type RefundResponse = {
  success: boolean;
  error?: string;
  data?: {
    transfer: {
      transactionHash: string;
      userOpHash?: string;
      amountUsdc: string;
      toAddress: string;
      sellerWalletAddress: string;
      usdcBalanceBefore: string;
      usdcBalanceAfter: string;
    };
    sourceWallet?: {
      walletAddress: string;
      usdcBalanceBefore: string;
      nativeBalanceBefore: string;
    };
    destinationWallet?: {
      walletAddress: string;
      usdcBalanceBefore: string;
      nativeBalanceBefore: string;
    };
    nativeTopUp?: {
      transactionHash: string;
      amountEth: string;
      toAddress: string;
      fromWalletAddress: string;
      nativeBalanceBefore: string;
      nativeBalanceAfter: string;
    };
  };
};

type TreasurySetupResponse = {
  success: boolean;
  error?: string;
  data?: {
    treasury: TreasuryWalletSetupSnapshot;
    message?: string;
  };
};

function formatDate(value: string): string {
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return value;
  return new Date(parsed).toLocaleString();
}

function shortHash(value: string): string {
  if (value.length < 14) return value;
  return `${value.slice(0, 8)}...${value.slice(-6)}`;
}

export default function AdminPage() {
  const [adminToken, setAdminToken] = useState("");
  const [overview, setOverview] = useState<OverviewResponse["data"] | null>(null);
  const [loadError, setLoadError] = useState("");
  const [loading, setLoading] = useState(false);
  const [note, setNote] = useState("");
  const [withdrawError, setWithdrawError] = useState("");
  const [withdrawResult, setWithdrawResult] = useState<RefundResponse["data"] | null>(null);
  const [withdrawing, setWithdrawing] = useState(false);
  const [treasuryRollupError, setTreasuryRollupError] = useState("");
  const [treasuryRollupResult, setTreasuryRollupResult] = useState<RefundResponse["data"] | null>(null);
  const [treasuryError, setTreasuryError] = useState("");
  const [treasuryMessage, setTreasuryMessage] = useState("");
  const [initializingTreasury, setInitializingTreasury] = useState(false);
  const [rollingUpTreasuryWalletAddress, setRollingUpTreasuryWalletAddress] = useState("");
  const [nativeTopUpAmountEth, setNativeTopUpAmountEth] = useState("0.00005");
  const [nativeTopUpFromSellerAddress, setNativeTopUpFromSellerAddress] = useState("");
  const [withdrawAll, setWithdrawAll] = useState(true);
  const [withdrawAmountUsdc, setWithdrawAmountUsdc] = useState("");

  useEffect(() => {
    const stored = window.sessionStorage.getItem("selun-admin-token");
    if (stored) {
      setAdminToken(stored);
    }
  }, []);

  useEffect(() => {
    if (!adminToken) return;
    window.sessionStorage.setItem("selun-admin-token", adminToken);
  }, [adminToken]);

  async function loadOverview() {
    setLoading(true);
    setLoadError("");
    setTreasuryError("");

    try {
      const response = await fetch("/api/admin/overview", {
        method: "GET",
        headers: {
          "X-Selun-Admin-Token": adminToken,
        },
        cache: "no-store",
      });

      const payload = (await response.json()) as OverviewResponse;
      if (!response.ok || !payload.success || !payload.data) {
        throw new Error(payload.error || "Failed to load admin overview.");
      }

      setOverview(payload.data);
      setNativeTopUpAmountEth(payload.data.defaultGasTopUpAmountEth || "0.00005");
      const defaultTopUpSource =
        payload.data.wallets.find((wallet) => Number(wallet.nativeBalance || 0) > 0)?.walletAddress || "";
      setNativeTopUpFromSellerAddress(payload.data.adminFundingWallet ? "" : defaultTopUpSource);
      setWithdrawAmountUsdc(payload.data.treasury.smartAccount?.usdcBalance ?? "");
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : "Failed to load admin overview.");
      setOverview(null);
    } finally {
      setLoading(false);
    }
  }

  const treasurySmartAccount = useMemo(() => overview?.treasury.smartAccount ?? null, [overview]);

  useEffect(() => {
    if (!withdrawAll) return;
    setWithdrawAmountUsdc(treasurySmartAccount?.usdcBalance ?? "");
  }, [treasurySmartAccount, withdrawAll]);

  async function withdrawTreasury() {
    if (!treasurySmartAccount) {
      setWithdrawError("Treasury smart account is not available.");
      return;
    }

    if (!withdrawAll && (!withdrawAmountUsdc || Number(withdrawAmountUsdc) <= 0)) {
      setWithdrawError("Provide a USDC amount greater than zero or enable full balance withdrawal.");
      return;
    }

    setWithdrawing(true);
    setWithdrawError("");
    setWithdrawResult(null);

    try {
      const response = await fetch("/api/admin/withdraw-treasury", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Selun-Admin-Token": adminToken,
        },
        body: JSON.stringify({
          withdrawAll,
          ...(!withdrawAll ? { amountUsdc: withdrawAmountUsdc } : {}),
          note,
        }),
      });

      const payload = (await response.json()) as RefundResponse;
      if (!response.ok || !payload.success || !payload.data) {
        throw new Error(payload.error || "Withdrawal failed.");
      }

      setWithdrawResult(payload.data);
      await loadOverview();
    } catch (error) {
      setWithdrawError(error instanceof Error ? error.message : "Treasury withdrawal failed.");
    } finally {
      setWithdrawing(false);
    }
  }

  async function initializeTreasury() {
    setInitializingTreasury(true);
    setTreasuryError("");
    setTreasuryMessage("");

    try {
      const response = await fetch("/api/admin/treasury-smart-account", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "X-Selun-Admin-Token": adminToken,
        },
      });

      const payload = (await response.json()) as TreasurySetupResponse;
      if (!response.ok || !payload.success || !payload.data?.treasury) {
        throw new Error(payload.error || "Failed to initialize treasury smart account.");
      }

      setTreasuryMessage(payload.data.message || "Treasury smart account is ready.");
      await loadOverview();
    } catch (error) {
      setTreasuryError(error instanceof Error ? error.message : "Failed to initialize treasury smart account.");
    } finally {
      setInitializingTreasury(false);
    }
  }

  async function rollupWalletToTreasury(sellerAddress: string) {
    setRollingUpTreasuryWalletAddress(sellerAddress);
    setTreasuryRollupError("");
    setTreasuryRollupResult(null);

    try {
      const response = await fetch("/api/admin/rollup-usdc-to-treasury", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Selun-Admin-Token": adminToken,
        },
        body: JSON.stringify({
          sellerAddress,
          rollupAll: true,
          ensureNativeTopUp: true,
          ...(nativeTopUpFromSellerAddress ? { nativeTopUpFromSellerAddress } : {}),
          ...(nativeTopUpAmountEth ? { nativeTopUpAmountEth } : {}),
        }),
      });

      const payload = (await response.json()) as RefundResponse;
      if (!response.ok || !payload.success || !payload.data) {
        throw new Error(payload.error || "Treasury USDC rollup failed.");
      }

      setTreasuryRollupResult(payload.data);
      await loadOverview();
    } catch (error) {
      setTreasuryRollupError(error instanceof Error ? error.message : "Treasury USDC rollup failed.");
    } finally {
      setRollingUpTreasuryWalletAddress("");
    }
  }

  return (
    <main className={styles.page}>
      <div className={styles.shell}>
        <header className={styles.header}>
          <div>
            <p className={styles.eyebrow}>Selun Admin</p>
            <h1>Wallet Withdrawals</h1>
            <p className={styles.subhead}>
              Sweep USDC balances from Selun&apos;s CDP seller wallets to the configured admin address. Purchase history remains visible below for audit context.
            </p>
          </div>
          <div className={styles.actions}>
            <Link href="/x402" className={styles.link}>
              View x402 Catalog
            </Link>
          </div>
        </header>

        <section className={styles.authPanel}>
          <label className={styles.label} htmlFor="adminToken">
            Admin token
          </label>
          <div className={styles.authRow}>
            <input
              id="adminToken"
              type="password"
              value={adminToken}
              onChange={(event) => setAdminToken(event.target.value)}
              placeholder="SELUN_ADMIN_API_TOKEN"
              className={styles.input}
            />
            <button type="button" onClick={loadOverview} disabled={!adminToken || loading} className={styles.primaryButton}>
              {loading ? "Loading..." : "Load"}
            </button>
          </div>
          {loadError ? <p className={styles.error}>{loadError}</p> : null}
        </section>

        {overview ? (
          <>
            <section className={styles.walletGrid}>
              <article className={styles.metricCard}>
                <span>Seller wallet</span>
                <strong>{overview.wallet.walletAddress}</strong>
                <small>{overview.wallet.agentId}</small>
              </article>
              <article className={styles.metricCard}>
                <span>Network</span>
                <strong>{overview.wallet.network}</strong>
                <small>USDC: {overview.wallet.usdcContractAddress}</small>
              </article>
              <article className={styles.metricCard}>
                <span>USDC balance</span>
                <strong>{overview.wallet.usdcBalance}</strong>
                <small>Native: {overview.wallet.nativeBalance}</small>
              </article>
              <article className={styles.metricCard}>
                <span>Admin address</span>
                <strong>{overview.adminRefundAddress}</strong>
                <small>Admin-only withdrawal target</small>
              </article>
            </section>

            <section className={styles.tableSection}>
              <div className={styles.tableHeader}>
                <div>
                  <p className={styles.eyebrow}>Treasury</p>
                  <h2>Smart Treasury Wallet</h2>
                </div>
                <button
                  type="button"
                  onClick={initializeTreasury}
                  disabled={initializingTreasury}
                  className={styles.secondaryButton}
                >
                  {initializingTreasury
                    ? "Initializing..."
                    : overview.treasury.smartAccount
                      ? "Recheck Treasury"
                      : "Create Treasury Smart Account"}
                </button>
              </div>
              <p className={styles.selectionText}>
                Dedicated CDP smart account for Selun treasury operations. This does not change the current seller-wallet flow by itself.
              </p>
              <p className={styles.selectionText}>
                Sponsorship mode:{" "}
                {overview.treasury.smartAccount?.sponsorshipMode === "configured_paymaster"
                  ? `custom paymaster (${overview.treasury.smartAccount.paymasterUrl})`
                  : "manual or managed-default not assumed"}
              </p>
              {overview.transferMode?.note ? <p className={styles.selectionText}>{overview.transferMode.note}</p> : null}
              {treasuryError ? <p className={styles.error}>{treasuryError}</p> : null}
              {treasuryMessage ? <p className={styles.success}>{treasuryMessage}</p> : null}
              <div className={styles.walletGrid}>
                <article className={styles.metricCard}>
                  <span>Treasury owner</span>
                  <strong>{overview.treasury.owner?.walletAddress ?? "Not created"}</strong>
                  <small>{overview.treasury.owner?.name ?? overview.treasury.ownerName}</small>
                </article>
                <article className={styles.metricCard}>
                  <span>Smart account</span>
                  <strong>{overview.treasury.smartAccount?.walletAddress ?? "Not created"}</strong>
                  <small>{overview.treasury.smartAccount?.name ?? overview.treasury.smartAccountName}</small>
                </article>
                <article className={styles.metricCard}>
                  <span>Treasury USDC</span>
                  <strong>{overview.treasury.smartAccount?.usdcBalance ?? "0"}</strong>
                  <small>Native: {overview.treasury.smartAccount?.nativeBalance ?? "0"}</small>
                </article>
                <article className={styles.metricCard}>
                  <span>Owner / policies</span>
                  <strong>{overview.treasury.smartAccount?.ownerAddresses.length ?? 0} owner(s)</strong>
                  <small>Policies: {overview.treasury.smartAccount?.policies.length ?? 0}</small>
                </article>
              </div>
            </section>

            <section className={styles.refundPanel}>
              <div>
                <p className={styles.label}>Treasury Withdrawal</p>
                <p className={styles.selectionText}>
                  Sweep the treasury smart account to the configured admin address.
                </p>
                <p className={styles.selectionText}>
                  Treasury native balance: {treasurySmartAccount?.nativeBalance ?? "0"} ETH
                </p>
                <p className={styles.selectionText}>
                  Treasury USDC balance: {treasurySmartAccount?.usdcBalance ?? "0"} USDC
                </p>
              </div>
              <div className={styles.refundControls}>
                <input
                  type="text"
                  value={
                    treasurySmartAccount
                      ? `${shortHash(treasurySmartAccount.walletAddress)} | ${treasurySmartAccount.usdcBalance} USDC`
                      : "Treasury smart account not initialized"
                  }
                  className={styles.input}
                  readOnly
                />
                <input
                  type="text"
                  value={note}
                  onChange={(event) => setNote(event.target.value)}
                  className={styles.input}
                  placeholder="Optional withdrawal note"
                />
                <button
                  type="button"
                  onClick={withdrawTreasury}
                  disabled={withdrawing || !treasurySmartAccount || Number(treasurySmartAccount.usdcBalance) <= 0}
                  className={styles.primaryButton}
                >
                  {withdrawing ? "Withdrawing..." : "Withdraw Treasury to Admin"}
                </button>
              </div>
              <label className={styles.toggleRow}>
                <input
                  type="checkbox"
                  checked={withdrawAll}
                  onChange={(event) => setWithdrawAll(event.target.checked)}
                />
                <span>Withdraw the full USDC balance from the treasury smart account</span>
              </label>
              {!withdrawAll ? (
                <div className={styles.refundControls}>
                  <input
                    type="text"
                    value={withdrawAmountUsdc}
                    onChange={(event) => setWithdrawAmountUsdc(event.target.value)}
                    className={styles.input}
                    placeholder="USDC withdrawal amount"
                  />
                </div>
              ) : null}
              {withdrawError ? <p className={styles.error}>{withdrawError}</p> : null}
              {withdrawResult ? (
                <div className={styles.success}>
                  <div>
                    Treasury withdrawal submitted: {withdrawResult.transfer.amountUsdc} USDC to {withdrawResult.transfer.toAddress}. Tx{" "}
                    {withdrawResult.transfer.transactionHash}
                  </div>
                  {withdrawResult.transfer.userOpHash ? (
                    <div>
                      User operation: {withdrawResult.transfer.userOpHash}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </section>

            <section className={styles.tableSection}>
              <div className={styles.tableHeader}>
                <div>
                  <p className={styles.eyebrow}>Project Wallets</p>
                  <h2>Seller Wallets</h2>
                </div>
              </div>

              {treasuryRollupError ? <p className={styles.error}>{treasuryRollupError}</p> : null}
              {treasuryRollupResult ? (
                <p className={styles.success}>
                  Rolled up {treasuryRollupResult.transfer.amountUsdc} USDC from {treasuryRollupResult.sourceWallet?.walletAddress} to treasury{" "}
                  {treasuryRollupResult.destinationWallet?.walletAddress}. Tx {treasuryRollupResult.transfer.transactionHash}
                  {treasuryRollupResult.nativeTopUp
                    ? ` Top-up: ${treasuryRollupResult.nativeTopUp.amountEth} ETH from ${treasuryRollupResult.nativeTopUp.fromWalletAddress}.`
                    : ""}
                </p>
              ) : null}

              <div className={styles.tableWrapper}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>Wallet</th>
                      <th>USDC</th>
                      <th>Native</th>
                      <th>Policies</th>
                      <th>Status</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview.wallets.map((wallet) => (
                      <tr key={wallet.walletAddress}>
                        <td>
                          <strong>{wallet.agentId ?? "Unnamed"}</strong>
                          <div className={styles.muted}>{wallet.walletAddress}</div>
                        </td>
                        <td>{wallet.usdcBalance}</td>
                        <td>{wallet.nativeBalance}</td>
                        <td>{wallet.policies.length}</td>
                        <td>{wallet.active ? <span className={styles.pending}>Active</span> : <span className={styles.muted}>Project wallet</span>}</td>
                        <td>
                          {Number(wallet.usdcBalance) > 0 ? (
                            <div className={styles.actions}>
                              <button
                                type="button"
                                onClick={() => rollupWalletToTreasury(wallet.walletAddress)}
                                disabled={
                                  !overview.treasury.smartAccount ||
                                  Boolean(rollingUpTreasuryWalletAddress)
                                }
                                className={styles.secondaryButton}
                              >
                                {rollingUpTreasuryWalletAddress === wallet.walletAddress ? "Rolling Up..." : "Roll Up to Treasury"}
                              </button>
                            </div>
                          ) : (
                            <span className={styles.muted}>-</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className={styles.tableSection}>
              <div className={styles.tableHeader}>
                <div>
                  <p className={styles.eyebrow}>x402 Purchases</p>
                  <h2>Accepted payments</h2>
                </div>
                <button type="button" onClick={loadOverview} disabled={loading} className={styles.secondaryButton}>
                  Refresh
                </button>
              </div>

              <div className={styles.tableWrapper}>
                <table className={styles.table}>
                  <thead>
                    <tr>
                      <th>Endpoint</th>
                      <th>Amount</th>
                      <th>Payer</th>
                      <th>Payment Tx</th>
                      <th>Purchased</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview.purchases.map((purchase) => {
                      return (
                        <tr key={purchase.purchaseKey}>
                          <td>
                            <strong>{purchase.title}</strong>
                            <div className={styles.muted}>{purchase.endpoint}</div>
                            <div className={styles.muted}>decisionId: {purchase.decisionId}</div>
                          </td>
                          <td>{purchase.chargedAmountUsdc} USDC</td>
                          <td title={purchase.fromAddress}>{shortHash(purchase.fromAddress)}</td>
                          <td title={purchase.paymentTransactionHash}>{shortHash(purchase.paymentTransactionHash)}</td>
                          <td>{formatDate(purchase.purchasedAt)}</td>
                          <td>
                            {purchase.refunded ? (
                              <div className={styles.refunded}>
                                Refunded
                                <span>{purchase.refund ? shortHash(purchase.refund.transactionHash) : ""}</span>
                              </div>
                            ) : (
                              <span className={styles.pending}>Available</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        ) : null}
      </div>
    </main>
  );
}
