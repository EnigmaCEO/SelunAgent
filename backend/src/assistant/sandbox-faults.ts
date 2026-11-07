import fs from "node:fs";
import path from "node:path";
import { ScoreUnavailableError, UndeliverableError } from "../services/fulfillment-errors";

/**
 * Lets a staging test make a real PayPal sandbox purchase fail, to exercise retries and refunds. A paid run
 * reuses the day's cached prices, so blocking a provider can't reliably fail one. It's a no-op unless PayPal
 * is in sandbox, so a live purchase can never reach it. With no file present, runs behave normally.
 */
type RunReport<Args extends unknown[]> = (...args: Args) => Promise<Record<string, unknown>>;

const FAULTS: Record<string, () => Error> = {
  provider: () => new Error("Sandbox test: a market data provider is unavailable."),
  score_unavailable: () => new ScoreUnavailableError("Sandbox test: an asset can't get an overall score."),
  undeliverable: () => new UndeliverableError("Sandbox test: this purchase can't be delivered."),
};

export function withSandboxFaults<Args extends unknown[]>(env: string, faultFile: string, run: RunReport<Args>): RunReport<Args> {
  if (env !== "sandbox") return run;
  return async (...args) => {
    let fault: string | null = null;
    try {
      fault = (JSON.parse(fs.readFileSync(faultFile, "utf8")) as { fault?: string }).fault ?? null;
    } catch {
      // No file means no fault.
    }
    const make = fault ? FAULTS[fault] : undefined;
    if (make) throw make();
    return run(...args);
  };
}

export function sandboxFaultFile(): string {
  return path.resolve(process.cwd(), process.cwd().endsWith("backend") ? "data" : "backend/data", "sandbox-fault.json");
}
