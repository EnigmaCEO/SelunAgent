/**
 * Why a paid run failed decides what happens to the payment. Anything not named here counts as a
 * temporary provider failure, which is the safe default: the customer keeps their entitlement and the
 * run is tried again.
 */

/** An asset the purchase needs can't get an overall score. Waiting for a feed to return won't change that. */
export class ScoreUnavailableError extends Error {
  constructor(message = "Price history no longer supports an overall score for every purchased asset.") {
    super(message);
    this.name = "ScoreUnavailableError";
  }
}

/** This purchase can never be delivered as sold, so it shouldn't wait to be refunded. */
export class UndeliverableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UndeliverableError";
  }
}
