import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError, USER_AGENT } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";
import {
  apiErrorMessage,
  errorMessage,
  EXTENDED_FROM_DESCRIPTION,
  EXTENDED_RANGE_NOTE,
  EXTENDED_TO_DESCRIPTION,
  INSIGHTS_PERIODS,
  isWideRange,
  LEGACY_MAX_RANGE_HOURS,
} from "../lib/insights-tools.ts";

const API_BASE = "https://api.planetscale.com/v1";

// Supported sort metrics for single-sort insights requests.
const SORT_METRICS = [
  "count",
  "percentTime",
  "totalTime",
  "cpuTime",
  "p50Latency",
  "p99Latency",
  "rowsRead",
  "rowsReadPerQuery",
  "rowsReadPerReturned",
  "rowsAffected",
  "egressBytes",
  "egressBytesPerQuery",
  "maxEgressBytes",
  "ingressBytes",
  "ingressBytesPerQuery",
  "maxIngressBytes",
] as const;

// Curated metrics used by aggregate mode to keep the default request lightweight.
const AGGREGATE_SORT_METRICS = [
  "totalTime",
  "rowsReadPerReturned",
  "rowsRead",
  "p99Latency",
  "rowsAffected",
  "egressBytes",
] as const;

type SortMetric = (typeof SORT_METRICS)[number];

const CAPABILITY_GATED_SORT_METRICS = [
  "cpuTime",
  "maxEgressBytes",
  "ingressBytes",
  "ingressBytesPerQuery",
  "maxIngressBytes",
] as const;

type CapabilityGatedSortMetric = (typeof CAPABILITY_GATED_SORT_METRICS)[number];

// Fields to include in the result for token efficiency
const RESULT_FIELDS = [
  "id",
  "fingerprint",
  "normalized_sql",
  "query_count",
  "sum_total_duration_millis",
  "sum_total_duration_percent",
  "sum_cpu_duration_millis",
  "rows_read_per_returned",
  "rows_read_per_query",
  "sum_rows_read",
  "sum_rows_returned",
  "sum_rows_affected",
  "p50_latency",
  "p99_latency",
  "max_latency",
  "egress_bytes",
  "egress_bytes_per_query",
  "max_egress_bytes",
  "ingress_bytes",
  "ingress_bytes_per_query",
  "max_ingress_bytes",
  "max_shard_queries",
  "tables",
  "qualified_tables",
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
  sum_cpu_duration_millis?: number;
  rows_read_per_returned?: number;
  rows_read_per_query?: number;
  sum_rows_read?: number;
  sum_rows_returned?: number;
  sum_rows_affected?: number;
  p50_latency?: number;
  p99_latency?: number;
  max_latency?: number;
  egress_bytes?: number;
  egress_bytes_per_query?: number;
  max_egress_bytes?: number;
  ingress_bytes?: number;
  ingress_bytes_per_query?: number;
  max_ingress_bytes?: number;
  max_shard_queries?: number;
  tables?: string[];
  qualified_tables?: string[];
  index_usages?: unknown[];
  keyspace?: string;
  last_run_at?: string;
}

export interface InsightsResponse {
  data: InsightsEntry[];
}

interface BranchMetadata {
  kind?: string;
  parameters?: {
    pgconf?: Record<string, string | undefined>;
  };
  insights_cpu_timing?: boolean;
  insights_io_timing?: boolean;
  insights_egress_bytes?: boolean;
  insights_max_egress_bytes?: boolean;
  insights_ingress_bytes?: boolean;
}

export interface BranchCapabilities {
  kind: string | null;
  cpu_timing: boolean;
  io_timing: boolean;
  egress_bytes: boolean;
  max_egress_bytes: boolean;
  ingress_bytes: boolean;
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

export interface FingerprintSummary {
  id: string;
  fingerprint: string;
  normalized_sql: string;
  statement_type: string;
  keyspace: string;
  tables: string[];
  qualified_tables: string[];
  table_keyspaces: string[];
  index_usages: unknown[];
  query_count: number;
  error_count: number;
  sum_rows_read: number;
  sum_rows_returned: number;
  sum_rows_affected: number;
  rows_read_per_returned: number;
  rows_read_per_query: number;
  sum_total_duration_millis: number;
  sum_total_duration_percent: number;
  sum_cpu_duration_millis: number;
  time_per_query: number;
  p50_latency: number;
  p99_latency: number;
  max_latency: number;
  egress_bytes: number;
  egress_bytes_per_query: number;
  max_egress_bytes: number;
  ingress_bytes: number;
  ingress_bytes_per_query: number;
  max_ingress_bytes: number;
  max_shard_queries: number;
  last_run_at: string | null;
  slugs: Array<{
    style: string;
    name: string;
    alias: string;
    type: string;
    required: boolean;
  }>;
  multishard: boolean;
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
 * The last instant of `date`'s hour, matching how the API rounds an omitted
 * `to` up, so the reported window is the one the request actually covered.
 */
function endOfHour(date: Date): string {
  const end = new Date(date);
  end.setUTCMinutes(59, 59, 999);
  return end.toISOString();
}

function isCapabilityGatedSortMetric(
  sortBy: SortMetric
): sortBy is CapabilityGatedSortMetric {
  return (CAPABILITY_GATED_SORT_METRICS as readonly string[]).includes(sortBy);
}

/**
 * Fetch branch metadata needed to decide which insights metrics are meaningful.
 */
async function fetchBranchMetadata(
  organization: string,
  database: string,
  branch: string,
  authHeader: string
): Promise<BranchMetadata> {
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
      "User-Agent": USER_AGENT,
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
        "Branch not found. Please check your organization, database, and branch names.",
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
      `Failed to fetch branch metadata: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as BranchMetadata;
}

function getBranchCapabilities(branch: BranchMetadata): BranchCapabilities {
  const kind = branch.kind ?? null;
  const trackIoTiming = branch.parameters?.pgconf?.["track_io_timing"] === "on";

  return {
    kind,
    cpu_timing: branch.insights_cpu_timing ?? kind === "postgresql",
    io_timing:
      branch.insights_io_timing ?? (kind === "postgresql" && trackIoTiming),
    egress_bytes: branch.insights_egress_bytes ?? true,
    max_egress_bytes: branch.insights_max_egress_bytes ?? kind === "mysql",
    ingress_bytes: branch.insights_ingress_bytes ?? kind === "mysql",
  };
}

function formatBranchKind(kind: string | null): string {
  return kind ?? "unknown";
}

function unsupportedSortMessage(
  sortBy: CapabilityGatedSortMetric,
  capabilities: BranchCapabilities
): string | null {
  switch (sortBy) {
    case "cpuTime":
      if (capabilities.cpu_timing) return null;
      return `cpuTime is only available for Postgres branches. This branch is ${formatBranchKind(capabilities.kind)}; use totalTime, count, rowsRead, rowsReadPerReturned, or egressBytes instead.`;
    case "maxEgressBytes":
      if (capabilities.max_egress_bytes) return null;
      return `maxEgressBytes is only available for MySQL branches. This branch is ${formatBranchKind(capabilities.kind)}; use egressBytes or egressBytesPerQuery instead.`;
    case "ingressBytes":
    case "ingressBytesPerQuery":
    case "maxIngressBytes":
      if (capabilities.ingress_bytes) return null;
      return `${sortBy} is only available for Vitess/MySQL branches with insights ingress bytes enabled. This branch is ${formatBranchKind(capabilities.kind)}; use egressBytes, egressBytesPerQuery, or maxEgressBytes instead.`;
  }
}

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
  fields?: string[],
  q?: string,
  from?: string,
  to?: string,
  period?: string
): Promise<InsightsEntry[]> {
  let url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/insights?per_page=${limit}&sort=${sortBy}&dir=desc`;
  if (tabletType) {
    url += `&tablet_type=${encodeURIComponent(tabletType)}`;
  }
  if (fields && fields.length > 0) {
    url += `&${fields.map((f) => `fields[]=${encodeURIComponent(f)}`).join("&")}`;
  }
  if (q) {
    url += `&q=${encodeURIComponent(q)}`;
  }
  if (from) {
    url += `&from=${encodeURIComponent(from)}`;
  }
  if (to) {
    url += `&to=${encodeURIComponent(to)}`;
  }
  if (period) {
    url += `&period=${encodeURIComponent(period)}`;
  }

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
      "User-Agent": USER_AGENT,
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

    // A 400 here is almost always an unservable time range, and the API says
    // exactly which rule was broken. Its wording beats anything guessable from
    // the status alone.
    if (response.status === 400) {
      throw new PlanetScaleAPIError(
        apiErrorMessage(details) ?? `Invalid insights request: ${response.statusText}`,
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

// Fields to include in fingerprint summary results (strip HTML fields)
const SUMMARY_FIELDS = [
  "id",
  "fingerprint",
  "normalized_sql",
  "statement_type",
  "keyspace",
  "tables",
  "qualified_tables",
  "index_usages",
  "query_count",
  "error_count",
  "sum_rows_read",
  "sum_rows_returned",
  "sum_rows_affected",
  "rows_read_per_returned",
  "rows_read_per_query",
  "sum_total_duration_millis",
  "sum_total_duration_percent",
  "sum_cpu_duration_millis",
  "time_per_query",
  "p50_latency",
  "p99_latency",
  "max_latency",
  "egress_bytes",
  "egress_bytes_per_query",
  "max_egress_bytes",
  "ingress_bytes",
  "ingress_bytes_per_query",
  "max_ingress_bytes",
  "max_shard_queries",
  "last_run_at",
  "slugs",
  "multishard",
] as const;

/**
 * Filter a fingerprint summary to only include useful fields (strip HTML)
 */
function filterSummary(
  entry: FingerprintSummary
): Partial<FingerprintSummary> {
  const filtered: Partial<FingerprintSummary> = {};
  for (const field of SUMMARY_FIELDS) {
    const value = entry[field as keyof FingerprintSummary];
    if (value === undefined) continue;
    if (value === 0 || value === 0.0) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (value === null) continue;
    (filtered as Record<string, unknown>)[field] = value;
  }
  return filtered;
}

/**
 * Fetch aggregated summary stats for a specific fingerprint
 */
async function fetchFingerprintSummary(
  organization: string,
  database: string,
  branch: string,
  fingerprint: string,
  options: {
    keyspace?: string;
    from?: string;
    to?: string;
    tabletType?: string;
  },
  authHeader: string
): Promise<FingerprintSummary | null> {
  let url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/insights/${encodeURIComponent(fingerprint)}/summary?`;
  const params: string[] = [];
  if (options.keyspace) {
    params.push(`keyspace=${encodeURIComponent(options.keyspace)}`);
  }
  if (options.from) {
    params.push(`from=${encodeURIComponent(options.from)}`);
  }
  if (options.to) {
    params.push(`to=${encodeURIComponent(options.to)}`);
  }
  if (options.tabletType) {
    params.push(`tablet_type=${encodeURIComponent(options.tabletType)}`);
  }
  url += params.join("&");

  const response = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
      "User-Agent": USER_AGENT,
    },
  });

  if (!response.ok) {
    if (response.status === 404) {
      return null;
    }

    let details: unknown;
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }

    if (response.status === 401 || response.status === 403) {
      throw new PlanetScaleAPIError(
        "Permission denied. Please check your API token has the required permissions.",
        response.status,
        details
      );
    }

    if (response.status === 400) {
      throw new PlanetScaleAPIError(
        apiErrorMessage(details) ?? `Invalid summary request: ${response.statusText}`,
        response.status,
        details
      );
    }

    throw new PlanetScaleAPIError(
      `Failed to fetch fingerprint summary: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as FingerprintSummary;
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
    period?: string;
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
      "User-Agent": USER_AGENT,
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
    "Get query performance insights for a PlanetScale database branch. By default, aggregates the top queries across curated metrics (slowest, most time-consuming, most rows read, most inefficient, most rows affected, and highest egress) for a comprehensive view. Can also fetch queries sorted by a single metric. Supports filtering by tablet type (primary/replica). To drill down into a specific query pattern, first call without fingerprint to discover queries (each result includes a `fingerprint` and `keyspace`), then call again with both `fingerprint` and `keyspace` from that result to get the aggregated summary stats and individual executions. Note: egress_bytes and ingress_bytes values are raw bytes; the PlanetScale UI displays these as binary megabytes (1 MB = 2^20 bytes). Durations (sum_total_duration_millis) are in milliseconds. cpuTime is available for Postgres branches only; maxEgressBytes and ingressBytes/ingressBytesPerQuery/maxIngressBytes are available for MySQL/Vitess branches only. " +
    `${EXTENDED_RANGE_NOTE} The individual executions in fingerprint mode are the exception: they are always limited to the last ${LEGACY_MAX_RANGE_HOURS} hours, so a wider fingerprint call returns a full-range \`summary\` next to executions from the last 24 hours, and the response says so.`,
  annotations: {
    title: "Get query performance insights",
    readOnlyHint: true,
    destructiveHint: false,
    openWorldHint: false,
  },
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    sort_by: z
      .enum(["all", ...SORT_METRICS])
      .optional()
      .describe(
        "Sort order: 'all' (default) aggregates curated API calls for a comprehensive view, or specify a single metric: 'count', 'percentTime', 'totalTime', 'cpuTime', 'p50Latency', 'p99Latency', 'rowsRead', 'rowsReadPerQuery', 'rowsReadPerReturned', 'rowsAffected', 'egressBytes', 'egressBytesPerQuery', 'maxEgressBytes', 'ingressBytes', 'ingressBytesPerQuery', 'maxIngressBytes'. 'cpuTime' is Postgres-only; 'maxEgressBytes' and ingress* are MySQL/Vitess-only. Ignored when fingerprint is provided."
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
        "Request specific metric fields from the API (e.g. ['query', 'count', 'percentTime', 'totalTime', 'cpuTime', 'p50Latency', 'rowsRead', 'rowsReadPerQuery', 'rowsAffected', 'egressBytes', 'egressBytesPerQuery', 'maxEgressBytes', 'ingressBytes', 'ingressBytesPerQuery', 'maxIngressBytes', 'indexes', 'maxShardQueries']). 'cpuTime' is Postgres-only; 'maxEgressBytes' and ingress* are MySQL/Vitess-only. Ignored when fingerprint is provided."
      ),
    query: z
      .string()
      .optional()
      .describe(
        'Filter insights by search query. Supports plain text matching and structured filters: ' +
        'exact match with quotes ("select count"), ' +
        'statement_type:select|delete|update|insert, ' +
        'table:table_name, ' +
        'keyspace:keyspace_name, ' +
        'table_keyspace:keyspace_name, ' +
        'tag:tag_key:tag_value for queries carrying that tag with that value, ' +
        'tag:tag_key on its own for queries carrying the tag whatever its value (including ones whose value went unrecorded), ' +
        'where tag_key is the bare tag name without the \'S\'/\'B\' prefix `list_query_tags` reports and a value containing spaces or colons needs quoting (tag:route:"GET /horses"), ' +
        'index:index_name or index:table.index_name, ' +
        'indexed:true|false, ' +
        'multishard:true|false, ' +
        'query_count:>N or query_count:<N, ' +
        'p99:>N or p50:<N (ms), ' +
        'max_latency:>N (ms). ' +
        'Any term can be negated by an immediately preceding \'!\' (!tag:app:web). ' +
        'Ignored when fingerprint is provided.'
      ),
    fingerprint: z
      .string()
      .optional()
      .describe(
        "Query fingerprint hash to drill down into a specific query pattern. Use the `fingerprint` value from an initial insights call. Always include `keyspace` (also from the initial results) to get summary data."
      ),
    keyspace: z
      .string()
      .optional()
      .describe(
        "Keyspace for fingerprint drill-down. Required to get summary data. Use the `keyspace` value returned in insights results (e.g. 'my_keyspace' for MySQL/Vitess or 'postgres.public' for Postgres databases)."
      ),
    period: z
      .enum(INSIGHTS_PERIODS)
      .optional()
      .describe(
        "Shorthand for a recent time window ending at now. Cannot be combined with from/to — use one or the other. Only supported in discovery mode (ignored in fingerprint mode — use from/to instead)."
      ),
    from: z
      .string()
      .optional()
      .describe(
        `${EXTENDED_FROM_DESCRIPTION} Supported in both discovery and fingerprint modes, though in fingerprint mode only the summary honours a wide range.`
      ),
    to: z
      .string()
      .optional()
      .describe(
        `${EXTENDED_TO_DESCRIPTION} Supported in both discovery and fingerprint modes.`
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

      if (input["period"] && (input["from"] || input["to"])) {
        return ctx.text(
          "Error: 'period' cannot be combined with 'from'/'to'. Use either period (e.g. '1h', '6h') for a recent window, or from/to for a specific time range."
        );
      }

      const sortBy = input["sort_by"] ?? "all";
      const limit = Math.min(input["limit"] ?? 5, 20); // Cap at 20
      const tabletType = input["tablet_type"];
      const fields = input["fields"];
      const q = input["query"];
      const fingerprint = input["fingerprint"];

      const authHeader = getAuthHeader(env);
      let branchCapabilities: BranchCapabilities | undefined;

      // Fingerprint mode: fetch summary stats + individual executions
      if (fingerprint) {
        const now = new Date();
        const wide = isWideRange(input["from"], input["to"]);
        // Sending a `to` of "now" is fine inside the legacy window but breaks a
        // wide one: the summary endpoint requires a range over
        // LEGACY_MAX_RANGE_HOURS to cover whole hours, and rejects it outright
        // otherwise. Left off, the API rounds the window up to the end of the
        // current hour itself -- so drop it and report that same end below.
        const openEnded = wide && !input["to"];
        const from =
          input["from"] ??
          new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();
        const to = input["to"] ?? (openEnded ? endOfHour(now) : now.toISOString());

        const sharedOptions = {
          keyspace: input["keyspace"],
          from,
          ...(openEnded ? {} : { to }),
          tabletType,
        };

        // Fetch summary and individual executions in parallel, tolerating partial failures
        const [summaryResult, entriesResult] = await Promise.allSettled([
          fetchFingerprintSummary(
            organization,
            database,
            branch,
            fingerprint,
            sharedOptions,
            authHeader
          ),
          fetchSelectedQueries(
            organization,
            database,
            branch,
            fingerprint,
            { ...sharedOptions, perPage: limit },
            authHeader
          ),
        ]);

        const summary =
          summaryResult.status === "fulfilled" ? summaryResult.value : null;
        const entries =
          entriesResult.status === "fulfilled" ? entriesResult.value : [];
        const executions = entries.map(filterSelectedEntry);
        return ctx.json({
          mode: "fingerprint",
          fingerprint,
          keyspace: input["keyspace"],
          from,
          to,
          // Half the call failing used to be indistinguishable from a
          // fingerprint with no data. It matters more now that a range the
          // summary refuses is a thing a caller can ask for: without the
          // reason, a rejected wide range reads as "no such query".
          ...(summaryResult.status === "rejected"
            ? { summary_error: errorMessage(summaryResult.reason) }
            : {}),
          summary: summary ? filterSummary(summary) : null,
          executions: {
            total: executions.length,
            ...(wide
              ? {
                  window_note: `Individual executions are capped at ${LEGACY_MAX_RANGE_HOURS} hours: these are the last 24 hours only, not the requested range. Only \`summary\` covers the full range.`,
                }
              : {}),
            ...(entriesResult.status === "rejected"
              ? { error: errorMessage(entriesResult.reason) }
              : {}),
            queries: executions,
          },
        });
      }

      const from = input["from"];
      const to = input["to"];
      const period = input["period"];

      if (sortBy !== "all" && isCapabilityGatedSortMetric(sortBy)) {
        const branchMetadata = await fetchBranchMetadata(
          organization,
          database,
          branch,
          authHeader
        );
        branchCapabilities = getBranchCapabilities(branchMetadata);

        const message = unsupportedSortMessage(sortBy, branchCapabilities);
        if (message) {
          return ctx.text(`Error: ${message}`);
        }
      }

      if (sortBy === "all") {
        // Aggregate mode: fetch from curated metrics and deduplicate
        const uniqueEntries = new Map<string, Partial<InsightsEntry>>();

        for (const metric of AGGREGATE_SORT_METRICS) {
          const entries = await fetchInsights(
            organization,
            database,
            branch,
            metric,
            limit,
            authHeader,
            tabletType,
            fields,
            q,
            from,
            to,
            period
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
          metrics_queried: AGGREGATE_SORT_METRICS,
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
          fields,
          q,
          from,
          to,
          period
        );

        const results = entries.map(filterEntry);
        return ctx.json({
          mode: "single_metric",
          sort_by: sortBy,
          limit,
          ...(branchCapabilities
            ? { branch_capabilities: branchCapabilities }
            : {}),
          total_queries: results.length,
          queries: results,
        });
      }
    } catch (error) {
      return ctx.text(errorMessage(error));
    }
  },
});
