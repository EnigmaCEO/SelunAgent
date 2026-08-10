import type { Hex } from "viem";
import { Attribution } from "ox/erc8021";

/**
 * Base Builder Code for Selun, registered at base.dev against selun.sagitta.systems.
 * The matching domain-verification tag lives in `app/layout.tsx` (`base:app_id`).
 */
export const BASE_BUILDER_CODE = "bc_s5qg51do";

/**
 * ERC-8021 attribution suffix. Appended to transaction calldata so offchain
 * indexers can attribute the transaction to Selun. Contracts ignore the
 * trailing bytes, so execution is unaffected.
 */
export const BUILDER_CODE_DATA_SUFFIX = Attribution.toDataSuffix({
  codes: [BASE_BUILDER_CODE],
});

/**
 * Appends the attribution suffix to existing calldata.
 *
 * Only applied when `data` is already a contract call. A bare value transfer
 * carries no calldata, and attaching some would turn it into a contract call —
 * harmless against an EOA, but it would invoke the fallback of a smart account
 * (e.g. the CDP treasury) and can revert. Those transfers stay unattributed.
 */
export function appendBuilderCode(data: Hex | undefined): Hex {
  if (!data || data === "0x") return data ?? "0x";
  return `${data}${BUILDER_CODE_DATA_SUFFIX.slice(2)}` as Hex;
}
