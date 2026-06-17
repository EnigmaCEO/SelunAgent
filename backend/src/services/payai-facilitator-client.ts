import type { FacilitatorClient } from "@x402/core/server";
import type { PaymentPayload, PaymentRequirements, SettleResponse, SupportedResponse, VerifyResponse } from "@x402/core/types";

function stripExtra(req: PaymentRequirements): PaymentRequirements {
  return {
    scheme: req.scheme,
    network: req.network,
    amount: req.amount,
    asset: req.asset,
    payTo: req.payTo,
    maxTimeoutSeconds: req.maxTimeoutSeconds,
    extra: req.extra?.feePayer ? { feePayer: req.extra.feePayer } : {},
  };
}

function buildPayAIPayload(paymentPayload: PaymentPayload, req: PaymentRequirements, stripped: PaymentRequirements) {
  return {
    paymentPayload: {
      x402Version: paymentPayload.x402Version,
      scheme: req.scheme,
      network: req.network,
      accepted: stripped,
      payload: paymentPayload.payload,
      extensions: {},
    },
    paymentRequirements: stripped,
  };
}

/**
 * Facilitator client for payAI (https://facilitator.payai.network).
 *
 * payAI uses a different wire format than the standard HTTPFacilitatorClient:
 * - /verify and /settle take `paymentPayload` (object) not `paymentHeader` (base64)
 * - `paymentPayload` requires top-level `scheme`, `network`, and `accepted`
 * - `paymentRequirements` must be a single stripped object (only `feePayer` in `extra`)
 *
 * getSupported() filters out eip155 networks so that the primary CDP facilitator
 * continues to handle EVM/Base payments; payAI handles Solana and other non-EVM networks.
 */
export class PayAIFacilitatorClient implements FacilitatorClient {
  private readonly url: string;

  constructor(url: string) {
    this.url = url.replace(/\/$/, "");
  }

  async getSupported() {
    const res = await fetch(`${this.url}/supported`);
    if (!res.ok) throw new Error(`payAI /supported failed: ${res.status}`);
    const raw = await res.json() as SupportedResponse;
    // Exclude EVM networks — CDP handles those. PayAI handles Solana, Polygon, etc.
    const filtered: SupportedResponse = {
      ...raw,
      extensions: raw.extensions ?? [],
      signers: raw.signers ?? {},
      kinds: raw.kinds.filter((k) => !k.network.startsWith("eip155:")),
    };
    return filtered;
  }

  async verify(paymentPayload: PaymentPayload, paymentRequirements: PaymentRequirements): Promise<VerifyResponse> {
    const stripped = stripExtra(paymentRequirements);
    const body = buildPayAIPayload(paymentPayload, paymentRequirements, stripped);

    const res = await fetch(`${this.url}/verify`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const data = await res.json() as Record<string, unknown>;
    if (!data["isValid"]) {
      const err = Object.assign(
        new Error(`payAI verify: ${String(data["invalidReason"] ?? "unknown")} — ${String(data["invalidMessage"] ?? "")}`),
        { isValid: false, invalidReason: data["invalidReason"], invalidMessage: data["invalidMessage"] },
      );
      throw err;
    }
    return { isValid: true, payer: data["payer"] as string | undefined };
  }

  async settle(paymentPayload: PaymentPayload, paymentRequirements: PaymentRequirements): Promise<SettleResponse> {
    const stripped = stripExtra(paymentRequirements);
    const body = buildPayAIPayload(paymentPayload, paymentRequirements, stripped);

    const res = await fetch(`${this.url}/settle`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const data = await res.json() as Record<string, unknown>;
    if (!data["success"]) {
      throw new Error(`payAI settle failed: ${String(data["errorReason"] ?? "unknown")}`);
    }
    return {
      success: true,
      transaction: String(data["transaction"] ?? ""),
      network: data["network"] as `${string}:${string}`,
      payer: data["payer"] as string | undefined,
    };
  }
}
