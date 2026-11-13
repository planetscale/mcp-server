import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { getAuthHeader, getAuthToken } from "../lib/auth.ts";
import { apiErrorMessage } from "../lib/insights-tools.ts";
import { PlanetScaleAPIError, USER_AGENT } from "../lib/planetscale-api.ts";

const API_BASE = "https://api.planetscale.com/v1";

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;
const QUERY_TEXT_LIMIT = 500;

const COMMON_FIELDS = [
  "pid",
  "instance",
  "connection_id",
  "query_id",
  "datname",
  "state",
  "usename",
  "client_addr",
  "duration_ms",
] as const;

const POSTGRES_FIELDS = [
  "transaction_id",
  "leader_pid",
  "wait_event",
  "wait_event_type",
  "application_name",
  "backend_type",
  "query_start",
  "xact_start",
  "backend_start",
  "state_change",
  "blocked_by",
] as const;

const NEKI_FIELDS = [
  "transaction_id",
  "sidecar_backends",
  "router_cell",
  "router_uid",
  "router_name",
  "availability_zone",
  "application_name",
  "backend_type",
  "query_start",
  "xact_start",
  "backend_start",
  "state_change",
] as const;

export const LIST_LOCK_RETRY =
  "The connections list is cached for 1 second and returns 503 while that lock is held. Retry once after about 1 second. Do not poll faster than every few seconds.";

type Row = Record<string, unknown>;

export interface ConnectionSummary {
  counts_by_state: Record<string, number>;
  counts_by_user: Record<string, number>;
  counts_by_app: Record<string, number>;
  longest_query: Row | null;
  oldest_transaction: Row | null;
  blocked_count: number;
}

export interface LiveConnectionFilters {
  state?: string | undefined;
  minDurationMs?: number | undefined;
}

export function emptySummary(): ConnectionSummary {
  return {
    counts_by_state: {},
    counts_by_user: {},
    counts_by_app: {},
    longest_query: null,
    oldest_transaction: null,
    blocked_count: 0,
  };
}

/** Bound a caller limit to 1..max. Out of range returns rows instead of an error. */
export function clampLimit(value: number, max: number): number {
  return Math.min(Math.max(Math.trunc(value), 1), max);
}

export function truncateQuery(text: string): { text: string; truncated: boolean } {
  if (text.length <= QUERY_TEXT_LIMIT) return { text, truncated: false };
  return { text: text.slice(0, QUERY_TEXT_LIMIT), truncated: true };
}

function numberField(row: Row, key: string): number | undefined {
  const value = row[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function stringField(row: Row, key: string): string | undefined {
  const value = row[key];
  return typeof value === "string" && value !== "" ? value : undefined;
}

function bump(counts: Record<string, number>, key: string | undefined): void {
  if (!key) return;
  counts[key] = (counts[key] ?? 0) + 1;
}

function extraFields(kind: string | undefined): readonly string[] {
  if (kind === "postgresql") return POSTGRES_FIELDS;
  if (kind === "neki") return NEKI_FIELDS;
  if (kind === "mysql") return [];
  return [...POSTGRES_FIELDS, ...NEKI_FIELDS];
}

function copyFields(row: Row, fields: readonly string[]): Row {
  const out: Row = {};
  for (const field of fields) {
    if (!Object.hasOwn(row, field)) continue;
    const value = row[field];
    if (value === undefined || value === null) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    out[field] = value;
  }
  return out;
}

function applyQueryText(target: Row, includeQueryText: boolean): void {
  const queryText = target["query_text"];
  if (!includeQueryText || typeof queryText !== "string" || queryText === "") {
    delete target["query_text"];
    return;
  }
  const query = truncateQuery(queryText);
  target["query_text"] = query.text;
  if (query.truncated) target["query_text_truncated"] = true;
}

/**
 * A Vitess list with no single tablet returns 400 plus the keyspaces or shards
 * the caller can pick. Anything else (including keyspace on Postgres) stays an error.
 */
export function targetRequiredBody(
  details: unknown,
): { message: string; available: Record<string, string[]> } | undefined {
  if (details === null || typeof details !== "object") return undefined;
  const available = (details as Row)["available"];
  if (available === null || typeof available !== "object" || Array.isArray(available)) {
    return undefined;
  }
  const picked: Record<string, string[]> = {};
  for (const key of ["keyspaces", "shards"]) {
    const value = (available as Row)[key];
    if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) continue;
    picked[key] = value;
  }
  if (Object.keys(picked).length === 0) return undefined;
  const message = (details as Row)["message"];
  return {
    message:
      typeof message === "string" && message !== ""
        ? message
        : "Specify a Vitess keyspace and shard.",
    available: picked,
  };
}

export function selectRows(rows: Row[], filters: LiveConnectionFilters): Row[] {
  const state = filters.state?.trim().toLowerCase();
  return rows.filter((row) => {
    if (state) {
      const current = stringField(row, "state");
      if (!current || current.toLowerCase() !== state) return false;
    }
    if (filters.minDurationMs !== undefined) {
      const duration = numberField(row, "duration_ms");
      if (duration === undefined || duration < filters.minDurationMs) return false;
    }
    return true;
  });
}

function identity(row: Row): Row {
  const out: Row = {};
  for (const key of ["pid", "instance", "state", "usename"]) {
    const value = row[key];
    if (value !== undefined && value !== null) out[key] = value;
  }
  return out;
}

/** Counts and extremes over the filtered rows, before the limit drops any. */
export function summarize(rows: Row[]): ConnectionSummary {
  const summary = emptySummary();
  for (const row of rows) {
    bump(summary.counts_by_state, stringField(row, "state"));
    bump(summary.counts_by_user, stringField(row, "usename"));
    bump(summary.counts_by_app, stringField(row, "application_name"));

    const duration = numberField(row, "duration_ms");
    const longest = numberField(summary.longest_query ?? {}, "duration_ms");
    if (duration !== undefined && (summary.longest_query === null || duration > (longest ?? -Infinity))) {
      summary.longest_query = { ...identity(row), duration_ms: duration };
    }

    const xactStart = stringField(row, "xact_start");
    const oldest = stringField(summary.oldest_transaction ?? {}, "xact_start");
    if (xactStart && (summary.oldest_transaction === null || xactStart < oldest!)) {
      summary.oldest_transaction = { ...identity(row), xact_start: xactStart };
    }

    const blockedBy = row["blocked_by"];
    if (Array.isArray(blockedBy) && blockedBy.length > 0) summary.blocked_count++;
  }
  return summary;
}

export function shapeRow(row: Row, kind: string | undefined, includeQueryText: boolean): Row {
  const shaped: Row = {
    ...copyFields(row, COMMON_FIELDS),
    ...copyFields(row, extraFields(kind)),
  };
  if (typeof row["query_text"] === "string") shaped["query_text"] = row["query_text"];
  applyQueryText(shaped, includeQueryText);

  const sidecars = shaped["sidecar_backends"];
  if (Array.isArray(sidecars)) {
    shaped["sidecar_backends"] = sidecars.map((backend) => {
      if (backend === null || typeof backend !== "object" || Array.isArray(backend)) return backend;
      const copy: Row = { ...(backend as Row) };
      applyQueryText(copy, includeQueryText);
      return copy;
    });
  }
  return shaped;
}

function byDurationDesc(left: Row, right: Row): number {
  const leftDuration = numberField(left, "duration_ms");
  const rightDuration = numberField(right, "duration_ms");
  if (leftDuration === undefined && rightDuration === undefined) return 0;
  if (leftDuration === undefined) return 1;
  if (rightDuration === undefined) return -1;
  return rightDuration - leftDuration;
}

function instanceWarnings(instances: unknown): string[] {
  if (!Array.isArray(instances)) return [];
  const warnings: string[] = [];
  for (const instance of instances) {
    if (instance === null || typeof instance !== "object") continue;
    const error = (instance as Row)["error"];
    if (typeof error !== "string" || error === "") continue;
    const id = (instance as Row)["id"];
    warnings.push(`${typeof id === "string" && id !== "" ? id : "instance"}: ${error}`);
  }
  return warnings;
}

function warningsFrom(payload: Row): string[] {
  const warnings = Array.isArray(payload["warnings"])
    ? payload["warnings"].filter((warning): warning is string => typeof warning === "string" && warning !== "")
    : [];
  return [...warnings, ...instanceWarnings(payload["instances"])];
}

function connectionsUrl(
  organization: string,
  database: string,
  branch: string,
  keyspace: string | undefined,
  shard: string | undefined,
): string {
  const url = new URL(
    `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/connections`,
  );
  if (keyspace) url.searchParams.set("keyspace", keyspace);
  if (shard) url.searchParams.set("shard", shard);
  return url.toString();
}

async function listConnections(
  url: string,
  authHeader: string,
  signal?: AbortSignal,
): Promise<Row> {
  const response = await fetch(url, {
    method: "GET",
    signal,
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
      "User-Agent": USER_AGENT,
    },
  });

  let details: unknown;
  if (!response.ok) {
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }
    if (response.status === 404) {
      throw new PlanetScaleAPIError(
        "Resource not found. Please check your organization, database, and branch names.",
        response.status,
        details,
      );
    }
    if (response.status === 401 || response.status === 403) {
      throw new PlanetScaleAPIError(
        "Permission denied. Live connections require the read_live_connections OAuth scope, and read_production_live_connections on a production branch. Service tokens cannot list connections.",
        response.status,
        details,
      );
    }
    const message = apiErrorMessage(details) ?? `API request failed: ${response.statusText}`;
    throw new PlanetScaleAPIError(message, response.status, details);
  }

  return (await response.json()) as Row;
}

function rowsOf(payload: Row): Row[] {
  const data = payload["data"];
  if (!Array.isArray(data)) return [];
  return data.filter((row): row is Row => row !== null && typeof row === "object" && !Array.isArray(row));
}

export const liveConnectionsGram = new Gram().tool({
  name: "list_live_connections",
  description:
    "List live connections on a PlanetScale branch for Vitess/MySQL, Postgres, and Neki. Read-only: it does not cancel queries or terminate connections. Do not poll faster than every few seconds. The API caches each list for 1 second and returns 503 while that lock is held; retry once after about 1 second. Vitess reads one primary tablet's processlist, not replicas or vtgate sessions, and those rows have no transaction start. A Vitess branch with more than one keyspace, or a sharded keyspace, returns needs_target and the available keyspaces or shards; call again with keyspace and shard. This tool does not fan out across shards. Postgres and Neki reject keyspace and shard. include_query_text defaults to false so raw SQL stays out of the response; when true, query text is truncated to 500 characters. The summary counts the filtered rows by state, user, and application, and reports the longest query, the oldest transaction, and how many sessions are blocked. rows keeps the longest matches, at most limit (default 50, max 200).",
  annotations: {
    title: "List live connections",
    readOnlyHint: true,
    destructiveHint: false,
    openWorldHint: false,
  },
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    keyspace: z
      .string()
      .optional()
      .describe(
        "Vitess keyspace. Required when the branch has more than one keyspace. Ignored by Postgres and Neki, which reject it.",
      ),
    shard: z
      .string()
      .optional()
      .describe(
        "Vitess shard, such as '-' or '-80'. Required when the keyspace is sharded. Ignored by Postgres and Neki.",
      ),
    state: z
      .string()
      .optional()
      .describe(
        "Case-insensitive exact state match, such as 'active', 'idle', 'idle in transaction', 'Sleep', or 'Query/executing'.",
      ),
    min_duration_ms: z
      .number()
      .optional()
      .describe(
        "Only include rows whose duration_ms is at least this many milliseconds. Rows with no duration are left out.",
      ),
    limit: z
      .number()
      .optional()
      .describe(
        `Maximum rows to return (default: ${DEFAULT_LIMIT}, max: ${MAX_LIMIT}). The longest queries are kept. The summary still covers every row that matched the filters.`,
      ),
    include_query_text: z
      .boolean()
      .optional()
      .describe(
        `Include query text. Defaults to false. Text longer than ${QUERY_TEXT_LIMIT} characters is truncated and the row sets query_text_truncated.`,
      ),
  },
  async execute(ctx, input) {
    try {
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

      if (!getAuthToken(env)) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const organization = input.organization;
      const database = input.database;
      const branch = input.branch;
      if (!organization || !database || !branch) {
        return ctx.text("Error: organization, database, and branch are required.");
      }

      if (
        input.min_duration_ms !== undefined &&
        (!Number.isFinite(input.min_duration_ms) || input.min_duration_ms < 0)
      ) {
        return ctx.text("Error: min_duration_ms must be zero or greater.");
      }

      const authHeader = getAuthHeader(env);
      const payload = await listConnections(
        connectionsUrl(
          organization,
          database,
          branch,
          input.keyspace?.trim() || undefined,
          input.shard?.trim() || undefined,
        ),
        authHeader,
        ctx.signal,
      );

      const kind = typeof payload["database_kind"] === "string" ? payload["database_kind"] : undefined;
      const limit = clampLimit(input.limit ?? DEFAULT_LIMIT, MAX_LIMIT);
      const includeQueryText = input.include_query_text === true;
      const matched = selectRows(rowsOf(payload), {
        state: input.state,
        minDurationMs: input.min_duration_ms,
      });
      const summary = summarize(matched);
      const rows = [...matched]
        .sort(byDurationDesc)
        .slice(0, limit)
        .map((row) => shapeRow(row, kind, includeQueryText));

      return ctx.json({
        ...(kind ? { database_kind: kind } : {}),
        ...(typeof payload["captured_at"] === "string" ? { captured_at: payload["captured_at"] } : {}),
        ...(Array.isArray(payload["instances"]) ? { instances: payload["instances"] } : {}),
        ...(payload["topology"] !== null &&
        typeof payload["topology"] === "object" &&
        !Array.isArray(payload["topology"])
          ? { target: payload["topology"] }
          : {}),
        summary,
        rows,
        matched: matched.length,
        truncated: matched.length > limit,
        warnings: warningsFrom(payload),
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError && error.statusCode === 400) {
        const target = targetRequiredBody(error.details);
        if (target) {
          // shortcut: one tablet per call, fan-out trips the 1s list lock. Upgrade if agents need a cross-shard snapshot.
          return ctx.json({
            database_kind: "mysql",
            needs_target: true,
            message: target.message,
            available: target.available,
            summary: emptySummary(),
            rows: [],
            matched: 0,
            truncated: false,
            warnings: [
              "Vitess lists one primary tablet. Pass keyspace, and shard when the keyspace is sharded.",
            ],
          });
        }
      }

      if (error instanceof PlanetScaleAPIError && error.statusCode === 503) {
        const detail = apiErrorMessage(error.details) ?? error.message;
        return ctx.text(`Error: ${detail} ${LIST_LOCK_RETRY} (status: 503)`);
      }

      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
