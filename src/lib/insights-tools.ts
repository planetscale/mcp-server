import { PlanetScaleAPIError } from "./planetscale-api.ts";

/**
 * The `period` vocabulary every Insights endpoint shares.
 *
 * The API does not reject a value outside this list -- it ignores it and falls
 * back to the default 24-hour window, so an unvalidated typo would return
 * plausible data for the wrong range. That is why tools validate against this
 * list before sending, and why it lives in one place rather than being
 * restated per tool.
 */
export const INSIGHTS_PERIODS = [
  "15m",
  "1h",
  "3h",
  "6h",
  "12h",
  "1d",
  "2d",
  "7d",
  "8d",
] as const;

export type InsightsPeriod = (typeof INSIGHTS_PERIODS)[number];

/**
 * The Insights list endpoints cap results at `per_page` but do not paginate:
 * the API returns next_page/prev_page as null unconditionally and ignores the
 * `page` param, so a full page is the only available signal that results were
 * cut off. Report that rather than a page cursor the caller cannot act on.
 */
export function resultFields(
  returned: number,
  limit: number
): Record<string, unknown> {
  return {
    returned,
    truncated: returned >= limit,
  };
}

/** Prefer ctx.env, falling back to process.env for local development. */
export function resolveEnv(ctxEnv: object): Record<string, string | undefined> {
  return Object.keys(ctxEnv).length > 0
    ? (ctxEnv as Record<string, string | undefined>)
    : process.env;
}

export function errorMessage(error: unknown): string {
  if (error instanceof PlanetScaleAPIError) {
    return `Error: ${error.message} (status: ${error.statusCode})`;
  }

  if (error instanceof Error) {
    return `Error: ${error.message}`;
  }

  return "Error: An unexpected error occurred";
}
