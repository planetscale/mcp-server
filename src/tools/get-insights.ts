import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

// Available sort metrics for insights
const SORT_METRICS = [
  "totalTime",
  "rowsReadPerReturned",
  "rowsRead",
  "p99Latency",
  "rowsAffected",
] as const;

type SortMetric = (typeof SORT_METRICS)[number];

// Fields to include in the result for token efficiency
const RESULT_FIELDS = [
  "id",
  "fingerprint",
  "normalized_sql",
  "query_count",
  "sum_total_duration_millis",
  "sum_total_duration_percent",
  "rows_read_per_returned",
  "sum_rows_read",
  "sum_rows_returned",
  "sum_rows_affected",
  "p50_latency",
  "p99_latency",
  "max_latency",
  "egress_bytes",
  "egress_bytes_per_query",
  "max_egress_bytes",
  "max_shard_queries",
  "tables",
  "index_usages",
  "keyspace",
  "last_run_at",
] as const;

export interface InsightsEntry {
  id: string;
  fingerprint?: string;
  normalized_sql?: string;
  query_count?: number;
  sum_total_duration_millis?: number;
  sum_total_duration_percent?: number;
  rows_read_per_returned?: number;
  sum_rows_read?: number;
  sum_rows_returned?: number;
  sum_rows_affected?: number;
  p50_latency?: number;
  p99_latency?: number;
  max_latency?: number;
  egress_bytes?: number;
  egress_bytes_per_query?: number;
  max_egress_bytes?: number;
  max_shard_queries?: number;
  tables?: string[];
  index_usages?: unknown[];
  keyspace?: string;
  last_run_at?: string;
}

export interface InsightsResponse {
  data: InsightsEntry[];
}

export interface SelectedQueryEntry {
  id: string;
  fingerprint: string;
  normalized_sql: string;
  started_at: string;
  statement_type: string;
  keyspace: string;
  tables: string[];
  rows_read: number;
  rows_affected: number;
  rows_returned: number;
  total_duration_millis: number;
  error_message: string | null;
  shard_queries: number;
  tags: Array<{ name: string; value: string }>;
}

export interface SelectedQueryResponse {
  data: SelectedQueryEntry[];
}

// Fields to include in selected query results for token efficiency
const SELECTED_QUERY_FIELDS = [
  "id",
  "fingerprint",
  "normalized_sql",
  "started_at",
  "statement_type",
  "keyspace",
  "tables",
  "rows_read",
  "rows_affected",
  "rows_returned",
  "total_duration_millis",
  "error_message",
  "shard_queries",
  "tags",
] as const;

/**
 * Fetch insights from the PlanetScale API with a specific sort order
 */
async function fetchInsights(
  organization: string,
  database: string,
  branch: string,
  sortBy: SortMetric,
  limit: number,
  authHeader: string,
  tabletType?: string,
  fields?: string[]
): Promise<InsightsEntry[]> {
  let url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/insights?per_page=${limit}&sort=${sortBy}&dir=desc`;
  if (tabletType) {
    url += `&tablet_type=${encodeURIComponent(tabletType)}`;
  }
  if (fields && fields.length > 0) {
    url += `&${fields.map((f) => `fields[]=${encodeURIComponent(f)}`).join("&")}`;
  }

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let details: unknown;
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }

    if (response.status === 404) {
      throw new PlanetScaleAPIError(
        "Insights not found. Please check your organization, database, and branch names.",
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
      `Failed to fetch insights: ${response.statusText}`,
      response.status,
      details
    );
  }

  const data = (await response.json()) as InsightsResponse;
  return data.data || [];
}

/**
 * Filter an insights entry to only include the fields we want
 */
function filterEntry(entry: InsightsEntry): Partial<InsightsEntry> {
  const filtered: Partial<InsightsEntry> = {};
  for (const field of RESULT_FIELDS) {
    const value = entry[field as keyof InsightsEntry];
    if (value === undefined || value === 0) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    (filtered as Record<string, unknown>)[field] = value;
  }
  return filtered;
}

/**
 * Filter a selected query entry to only include the fields we want
 */
function filterSelectedEntry(
  entry: SelectedQueryEntry
): Partial<SelectedQueryEntry> {
  const filtered: Partial<SelectedQueryEntry> = {};
  for (const field of SELECTED_QUERY_FIELDS) {
    if (
      field in entry &&
      entry[field as keyof SelectedQueryEntry] !== undefined
    ) {
      (filtered as Record<string, unknown>)[field] =
        entry[field as keyof SelectedQueryEntry];
    }
  }
  return filtered;
}

/**
 * Fetch individual query executions for a specific fingerprint (drill-down view)
 */
async function fetchSelectedQueries(
  organization: string,
  database: string,
  branch: string,
  fingerprint: string,
  options: {
    keyspace?: string;
    from?: string;
    to?: string;
    perPage: number;
    tabletType?: string;
  },
  authHeader: string
): Promise<SelectedQueryEntry[]> {
  let url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/insights/${encodeURIComponent(fingerprint)}?per_page=${options.perPage}`;
  if (options.keyspace) {
    url += `&keyspace=${encodeURIComponent(options.keyspace)}`;
  }
  if (options.from) {
    url += `&from=${encodeURIComponent(options.from)}`;
  }
  if (options.to) {
    url += `&to=${encodeURIComponent(options.to)}`;
  }
  if (options.tabletType) {
    url += `&tablet_type=${encodeURIComponent(options.tabletType)}`;
  }

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    let details: unknown;
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }

    if (response.status === 404) {
      throw new PlanetScaleAPIError(
        "Insights not found. Please check your organization, database, branch, and fingerprint.",
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
      `Failed to fetch selected queries: ${response.statusText}`,
      response.status,
      details
    );
  }

  const data = (await response.json()) as SelectedQueryResponse;
  return data.data || [];
}

export const getInsightsGram = new Gram().tool({
  name: "get_insights",
  description:
    "Get query performance insights for a PlanetScale database branch. By default, aggregates the top queries across 5 different metrics (slowest, most time-consuming, most rows read, most inefficient, most rows affected) for a comprehensive view. Can also fetch queries sorted by a single metric. Supports filtering by tablet type (primary/replica) and drilling down into individual executions of a specific query pattern via fingerprint.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    sort_by: z
      .enum(["all", ...SORT_METRICS])
      .optional()
      .describe(
        "Sort order: 'all' (default) aggregates 5 API calls for comprehensive view, or specify a single metric: 'totalTime', 'rowsRead', 'p99Latency', 'rowsReadPerReturned', 'rowsAffected'. Ignored when fingerprint is provided."
      ),
    limit: z
      .number()
      .optional()
      .describe("Number of results per metric (default: 5, max: 20)"),
    tablet_type: z
      .enum(["primary", "replica"])
      .optional()
      .describe("Filter by tablet type: 'primary' or 'replica'"),
    fields: z
      .array(z.string())
      .optional()
      .describe(
        "Request specific metric fields from the API (e.g. ['query', 'count', 'rowsRead', 'rowsAffected', 'rowsReadPerReturned', 'egressBytes', 'indexes', 'maxShardQueries'])"
      ),
    fingerprint: z
      .string()
      .optional()
      .describe(
        "Query fingerprint hash to fetch individual executions (selected queries / drill-down view)"
      ),
    keyspace: z
      .string()
      .optional()
      .describe("Filter by keyspace name (used with fingerprint drill-down)"),
    from: z
      .string()
      .optional()
      .describe(
        "Start of time range (ISO 8601 format, e.g. '2026-03-09T00:00:00.000Z'). Defaults to 24 hours ago. Used with fingerprint drill-down."
      ),
    to: z
      .string()
      .optional()
      .describe(
        "End of time range (ISO 8601 format). Defaults to now. Used with fingerprint drill-down."
      ),
  },
  async execute(ctx, input) {
    try {
      // Try ctx.env first, fall back to process.env for local development
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

      // Check authentication
      const auth = getAuthToken(env);
      if (!auth) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const organization = input["organization"];
      const database = input["database"];
      const branch = input["branch"];

      if (!organization || !database || !branch) {
        return ctx.text(
          "Error: organization, database, and branch are required"
        );
      }

      const sortBy = input["sort_by"] ?? "all";
      const limit = Math.min(input["limit"] ?? 5, 20); // Cap at 20
      const tabletType = input["tablet_type"];
      const fields = input["fields"];
      const fingerprint = input["fingerprint"];

      const authHeader = getAuthHeader(env);

      // Fingerprint drill-down mode: fetch individual executions of a query pattern
      if (fingerprint) {
        const now = new Date();
        const twentyFourHoursAgo = new Date(
          now.getTime() - 24 * 60 * 60 * 1000
        );
        const from = input["from"] ?? twentyFourHoursAgo.toISOString();
        const to = input["to"] ?? now.toISOString();

        const entries = await fetchSelectedQueries(
          organization,
          database,
          branch,
          fingerprint,
          {
            keyspace: input["keyspace"],
            from,
            to,
            perPage: limit,
            tabletType,
          },
          authHeader
        );

        const results = entries.map(filterSelectedEntry);
        return ctx.json({
          mode: "selected_queries",
          fingerprint,
          keyspace: input["keyspace"],
          from,
          to,
          total_queries: results.length,
          queries: results,
        });
      }

      if (sortBy === "all") {
        // Aggregate mode: fetch from all 5 metrics and deduplicate
        const uniqueEntries = new Map<string, Partial<InsightsEntry>>();

        for (const metric of SORT_METRICS) {
          const entries = await fetchInsights(
            organization,
            database,
            branch,
            metric,
            limit,
            authHeader,
            tabletType,
            fields
          );

          for (const entry of entries) {
            if (entry.id && !uniqueEntries.has(entry.id)) {
              uniqueEntries.set(entry.id, filterEntry(entry));
            }
          }
        }

        const results = Array.from(uniqueEntries.values());
        return ctx.json({
          mode: "aggregated",
          metrics_queried: SORT_METRICS,
          limit_per_metric: limit,
          total_unique_queries: results.length,
          queries: results,
        });
      } else {
        // Single metric mode
        const entries = await fetchInsights(
          organization,
          database,
          branch,
          sortBy as SortMetric,
          limit,
          authHeader,
          tabletType,
          fields
        );

        const results = entries.map(filterEntry);
        return ctx.json({
          mode: "single_metric",
          sort_by: sortBy,
          limit,
          total_queries: results.length,
          queries: results,
        });
      }
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }

      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }

      return ctx.text(`Error: An unexpected error occurred`);
    }
  },
});
