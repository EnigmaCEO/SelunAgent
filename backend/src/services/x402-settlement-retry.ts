/**
 * The facilitator signs every settlement from one wallet, so simultaneous settlements
 * (from any buyer on that facilitator) can collide on that wallet's transaction nonce.
 * These errors mean the transaction was refused at submission, so it never reached the chain.
 *
 * Retrying reuses the buyer's signed authorization. USDC refuses an authorization nonce
 * that has been used, so a retry cannot charge twice.
 */
const FACILITATOR_NONCE_COLLISION = /nonce provided for the transaction is lower|nonce too low|missing or invalid parameters/i;
const RETRY_DELAYS_MS = [400, 800, 1500, 2500, 4000];

type SettlementLike = { success: boolean; transaction?: string; errorMessage?: string };

export function isFacilitatorNonceCollision(result: SettlementLike): boolean {
  return !result.success && !result.transaction?.trim() && FACILITATOR_NONCE_COLLISION.test(result.errorMessage ?? "");
}

export async function settleWithRetry<T extends SettlementLike>(
  settle: () => Promise<T>,
  sleep: (ms: number) => Promise<void> = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  random: () => number = Math.random,
): Promise<T> {
  let result = await settle();
  for (const delay of RETRY_DELAYS_MS) {
    if (!isFacilitatorNonceCollision(result)) break;
    // Jitter so that colliding settlements don't retry in lockstep.
    await sleep(delay * (0.5 + random()));
    result = await settle();
  }
  return result;
}
