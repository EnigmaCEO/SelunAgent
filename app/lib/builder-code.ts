/**
 * Base Builder Code for Selun, registered at base.dev against selun.sagitta.systems.
 * The matching domain-verification tag lives in `app/layout.tsx` (`base:app_id`).
 */
export const BASE_BUILDER_CODE = "bc_s5qg51do";

/**
 * ERC-8021 attribution suffix for BASE_BUILDER_CODE.
 *
 * Hardcoded rather than derived, to keep `ox` out of the client bundle for what
 * is a compile-time constant. The backend derives the same value from the same
 * code in `backend/src/builder-code.ts`. To regenerate after a code change:
 *
 *   node -e "console.log(require('ox/erc8021').Attribution.toDataSuffix({codes:['bc_s5qg51do']}))"
 */
export const BUILDER_CODE_DATA_SUFFIX =
  "0x62635f733571673531646f0b0080218021802180218021802180218021";

/**
 * Appends the attribution suffix to existing calldata.
 *
 * Only applied when `data` is already a contract call — see the note in
 * `backend/src/builder-code.ts` for why bare value transfers are left alone.
 */
export function appendBuilderCode(data: string): string {
  if (!data || data === "0x") return data || "0x";
  return `${data}${BUILDER_CODE_DATA_SUFFIX.slice(2)}`;
}
