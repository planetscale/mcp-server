import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError, USER_AGENT } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";
import {
  errorMessage,
  INSIGHTS_PERIODS,
  LEGACY_MAX_RANGE_HOURS,
  LEGACY_RANGE_NOTE,
  resolveEnv,
  resultFields,
  type InsightsPeriod,
} from "../lib/insights-tools.ts";

const API_BASE = "https://api.planetscale.com/v1";

const SORT_FIELDS = ["error", "lastRun", "count", "totalTime", "timePerQuery"] as const;

const TABLET_TYPES = ["primary", "replica", "rdonly"] as const;

const DEFAULT_LIMIT = 25;
const MAX_LIMIT = 100;

export interface ErrorPatternEntry {
  id: string;
  error_fingerprint: string;
  started_at: string;
  total_duration_millis: number;
  time_per_query: number;
  error_count: number;
  error_message: string;
}

export interface ErrorExecutionEntry {
  id: string;
  fingerprint?: string;
  started_at?: string;
  statement_type?: string;
  keyspace?: string;
  tables?: string[];
  username?: string;
  rows_read?: number;
  rows_affected?: number;
  rows_returned?: number;
  total_duration_millis?: number;
  error_message?: string;
  normalized_sql?: string;
  tags?: Array<{ name: string; value: string }>;
}

interface ListResponse<T> {
  data: T[];
}

// Fields to include in error execution results for token efficiency (the API
// also returns syntax-highlighted HTML variants, timestamps, etc.)
const ERROR_EXECUTION_FIELDS = [
  "id",
  "fingerprint",
  "started_at",
  "statement_type",
  "keyspace",
  "tables",
  "username",
  "rows_read",
  "rows_affected",
  "rows_returned",
  "total_duration_millis",
  "error_message",
  "normalized_sql",
  "tags",
] as const;

function filterErrorExecution(entry: ErrorExecutionEntry): Partial<ErrorExecutionEntry> {
  const filtered: Partial<ErrorExecutionEntry> = {};
  for (const field of ERROR_EXECUTION_FIELDS) {
    const value = entry[field as keyof ErrorExecutionEntry];
    if (value === undefined || value === null) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    (filtered as Record<string, unknown>)[field] = value;
  }
  return filtered;
}

// Input fields shared by both error tools.
const commonInputSchema = {
  organization: z.string().describe("PlanetScale organization name"),
  database: z.string().describe("Database name"),
  branch: z.string().describe("Branch name (e.g., 'main')"),
  period: z
    .enum(INSIGHTS_PERIODS)
    .optional()
    .describe(
      "Shorthand for a recent time window ending at now. Cannot be combined with from/to."
    ),
  from: z
    .string()
    .optional()
    .describe(
      `Start of time range (ISO 8601 format). Defaults to 24 hours ago. These endpoints report individual query executions and do not serve the wide windows \`get_insights\` and the query tag tools do: a range longer than ${LEGACY_MAX_RANGE_HOURS} hours falls back to the default window instead of being rejected, which makes recent data look like a wide-window answer.`
    ),
  to: z
    .string()
    .optional()
    .describe("End of time range (ISO 8601 format). Defaults to now."),
  limit: z
    .number()
    .optional()
    .describe(
      `Maximum number of results to return (default: ${DEFAULT_LIMIT}, max: ${MAX_LIMIT}). These endpoints do not paginate, so when a response reports truncated: true, raise this limit or narrow the time window to see the rest.`
    ),
};

interface CommonInput {
  organization: string;
  database: string;
  branch: string;
  period?: InsightsPeriod | undefined;
  from?: string | undefined;
  to?: string | undefined;
  limit?: number | undefined;
}

interface RequestContext {
  authHeader: string;
  basePath: string;
  timeParams: Record<string, string | undefined>;
  limit: number;
}

/**
 * Resolve auth, the branch errors endpoint path, and the shared time/paging
 * query params. Returns an error string instead of throwing when the request
 * can't be built.
 */
function buildRequestContext(
  env: Record<string, string | undefined>,
  input: CommonInput
): RequestContext | string {
  if (!getAuthToken(env)) {
    return "Error: No PlanetScale authentication configured.";
  }

  const { organization, database, branch } = input;
  if (!organization || !database || !branch) {
    return "Error: organization, database, and branch are required";
  }

  if (input.period && (input.from || input.to)) {
    return "Error: 'period' cannot be combined with 'from'/'to'. Use either period (e.g. '1h', '6h') for a recent window, or from/to for a specific time range.";
  }

  const limit = Math.min(Math.max(Math.trunc(input.limit ?? DEFAULT_LIMIT), 1), MAX_LIMIT);

  return {
    authHeader: getAuthHeader(env),
    basePath: `/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/insights/errors`,
    timeParams: {
      period: input.period,
      from: input.from,
      to: input.to,
      per_page: limit.toString(),
    },
    limit,
  };
}

async function fetchErrorsAPI<T>(
  endpoint: string,
  params: Record<string, string | undefined>,
  authHeader: string,
  signal: AbortSignal
): Promise<T[]> {
  const url = new URL(`${API_BASE}${endpoint}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) url.searchParams.set(key, value);
  }

  const response = await fetch(url, {
    method: "GET",
    signal,
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
      "User-Agent": USER_AGENT,
    },
  });

  if (!response.ok) {
    // Read the body once -- response.json() consumes it even when parsing
    // fails, so a text() fallback on the same response would throw and mask
    // the status-specific messages below.
    const raw = await response.text();
    let details: unknown;
    try {
      details = JSON.parse(raw);
    } catch {
      details = raw;
    }

    if (response.status === 404) {
      throw new PlanetScaleAPIError(
        "Query errors not found. Check your organization, database, and branch names, and that Insights is enabled for this database.",
        response.status,
        details
      );
    }

    if (response.status === 401 || response.status === 403) {
      throw new PlanetScaleAPIError(
        "Permission denied. Please check your API token has the required permissions.",
        response.status,
        details
      );
    }

    throw new PlanetScaleAPIError(
      `Failed to fetch query errors: ${response.statusText}`,
      response.status,
      details
    );
  }

  const body = (await response.json()) as ListResponse<T>;
  return body.data || [];
}

export const queryErrorsGram = new Gram()
  .tool({
    name: "list_query_error_patterns",
    description:
      "List failing queries for a PlanetScale database branch, aggregated by error fingerprint. Each pattern reports the error message, how many times it occurred, when it was last seen, and total/average duration. Use `list_query_error_executions` with an `error_fingerprint` from these results to see the individual failed executions behind a pattern. Data comes from PlanetScale Insights. " +
      LEGACY_RANGE_NOTE,
    annotations: {
      readOnlyHint: true,
      destructiveHint: false,
      openWorldHint: false,
    },
    inputSchema: {
      ...commonInputSchema,
      query: z
        .string()
        .optional()
        .describe("Search error patterns by error message text."),
      sort_by: z
        .enum(SORT_FIELDS)
        .optional()
        .describe(
          "Sort error patterns by: 'count' (default, most frequent), 'lastRun' (most recent), 'error' (message), 'totalTime', or 'timePerQuery'."
        ),
      tablet_type: z
        .enum(TABLET_TYPES)
        .optional()
        .describe("Filter by tablet type: 'primary', 'replica', or 'rdonly'."),
    },
    async execute(ctx, input) {
      try {
        const request = buildRequestContext(resolveEnv(ctx.env), input);
        if (typeof request === "string") {
          return ctx.text(request);
        }

        const sortBy = input["sort_by"] ?? "count";

        const entries = await fetchErrorsAPI<ErrorPatternEntry>(
          request.basePath,
          {
            ...request.timeParams,
            q: input["query"],
            sort: sortBy,
            dir: "desc",
            tablet_type: input["tablet_type"],
          },
          request.authHeader,
          ctx.signal
        );

        return ctx.json({
          sort_by: sortBy,
          ...resultFields(entries.length, request.limit),
          patterns: entries.map((e) => ({
            error_fingerprint: e.error_fingerprint,
            error_message: e.error_message,
            error_count: e.error_count,
            started_at: e.started_at,
            total_duration_millis: e.total_duration_millis,
            time_per_query: e.time_per_query,
          })),
        });
      } catch (error) {
        return ctx.text(errorMessage(error));
      }
    },
  })
  .tool({
    name: "list_query_error_executions",
    description:
      "List the individual captured query executions that failed with a given error fingerprint on a PlanetScale database branch. Each execution includes the normalized SQL, tables, keyspace, user, row counts, duration, error message, and query tags. Get an `error_fingerprint` from `list_query_error_patterns` first. This endpoint cannot filter by tablet type, so executions from all tablet types are returned even when the fingerprint came from a tablet-filtered pattern list. Data comes from PlanetScale Insights. " +
      LEGACY_RANGE_NOTE,
    annotations: {
      readOnlyHint: true,
      destructiveHint: false,
      openWorldHint: false,
    },
    inputSchema: {
      ...commonInputSchema,
      error_fingerprint: z
        .string()
        .describe(
          "Error fingerprint identifying the error to list executions for. Use the `error_fingerprint` value from `list_query_error_patterns`."
        ),
    },
    async execute(ctx, input) {
      try {
        const request = buildRequestContext(resolveEnv(ctx.env), input);
        if (typeof request === "string") {
          return ctx.text(request);
        }

        const errorFingerprint = input["error_fingerprint"];
        if (!errorFingerprint) {
          return ctx.text("Error: error_fingerprint is required");
        }

        const entries = await fetchErrorsAPI<ErrorExecutionEntry>(
          `${request.basePath}/${encodeURIComponent(errorFingerprint)}`,
          request.timeParams,
          request.authHeader,
          ctx.signal
        );

        return ctx.json({
          error_fingerprint: errorFingerprint,
          ...resultFields(entries.length, request.limit),
          executions: entries.map(filterErrorExecution),
        });
      } catch (error) {
        return ctx.text(errorMessage(error));
      }
    },
  });
