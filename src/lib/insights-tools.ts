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
 * The widest `from`/`to` window the Insights statistics endpoints serve without
 * further conditions.
 *
 * Up to this many hours a range is served as asked. Past it the API requires the
 * range to cover whole hours and rejects a misaligned one outright, which is why
 * the tools describe the rule rather than quietly repairing the input.
 *
 * The tools returning individual query executions -- the query error tools, and
 * the executions half of `get_insights` fingerprint mode -- never go past this
 * window at all: a wider range falls back to the last 24 hours.
 */
export const LEGACY_MAX_RANGE_HOURS = 25;

/** Widest from/to range the statistics endpoints will serve. */
export const MAX_RANGE_DAYS = 365;

/** Closing sentence for the tool descriptions of the endpoints that serve the wide range. */
export const EXTENDED_RANGE_NOTE = `Default time window is the last 24 hours; from/to reaches back up to ${MAX_RANGE_DAYS} days, but a range wider than ${LEGACY_MAX_RANGE_HOURS} hours must be hour-aligned -- see the from/to descriptions.`;

/** Closing sentence for the tools still capped at the legacy window. */
export const LEGACY_RANGE_NOTE = `Default time window is the last 24 hours. These results describe individual query executions, so unlike \`get_insights\` and the query tag tools the window is capped at ${LEGACY_MAX_RANGE_HOURS} hours: a wider from/to falls back to the default window server-side.`;

export const EXTENDED_FROM_DESCRIPTION = `Start of time range (ISO 8601 format). Defaults to 24 hours ago. A range wider than ${LEGACY_MAX_RANGE_HOURS} hours must start on the hour (e.g. '2026-08-19T00:00:00Z') and span at most ${MAX_RANGE_DAYS} days; a wide range that is not hour-aligned is rejected, not narrowed.`;

export const EXTENDED_TO_DESCRIPTION = `End of time range (ISO 8601 format). Defaults to now. For a range wider than ${LEGACY_MAX_RANGE_HOURS} hours it must land on an hour boundary -- either the top of an hour or its last second ('T13:00:00Z' or 'T12:59:59Z') -- so the simplest way to ask for a wide window is to omit it, which covers everything through the current hour.`;

/**
 * True when an explicit from/to range is wider than the legacy cap, and so is
 * only servable by the endpoints that offer the extended window.
 *
 * Unparseable input is reported as not wide: the API owns validating a
 * timestamp, and guessing here would attach a wide-range caveat to a range that
 * was never understood in the first place.
 */
export function isWideRange(from?: string, to?: string): boolean {
  if (!from) return false;

  const start = Date.parse(from);
  if (Number.isNaN(start)) return false;

  const end = to === undefined ? Date.now() : Date.parse(to);
  if (Number.isNaN(end)) return false;

  return end - start > LEGACY_MAX_RANGE_HOURS * 60 * 60 * 1000;
}

/**
 * The `message` an API error body carries, if it has one.
 *
 * The status-specific hints the tools raise read better than a bare "Bad
 * Request", but they cannot anticipate every validation the API applies -- the
 * time-range rules among them -- so prefer the server's own wording whenever it
 * sent any.
 */
export function apiErrorMessage(details: unknown): string | null {
  if (details === null || typeof details !== "object") return null;

  const message = (details as { message?: unknown }).message;
  return typeof message === "string" && message.trim() !== "" ? message : null;
}

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
