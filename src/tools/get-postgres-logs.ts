import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import {
  PlanetScaleAPIError,
  createLogSignature,
} from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const LOG_LEVELS = ["INFO", "DEBUG", "WARNING", "ERROR"] as const;

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 1000;
const MAX_ERROR_BODY_CHARS = 4096;

// A LogsQL duration: one or more number+unit pairs, e.g. '5m', '24h', '1h30m'.
// Anything else (e.g. '1 hour') would be interpolated into the query and read
// as a duration plus a stray word filter, silently changing the results.
const PERIOD_PATTERN = /^(\d+(?:ms|[smhdwy]))+$/;

const SERVER_ROLES = ["primary", "replica"] as const;

/**
 * Quote a value for interpolation into a LogsQL filter
 */
function quoteLogsQLValue(value: string): string {
  return `"${value.replace(/["\\]/g, "\\$&")}"`;
}

interface RawLogEntry {
  _time?: string;
  _msg?: string | null;
  "planetscale.level"?: string;
  "planetscale.pod"?: string;
  "planetscale.role"?: string;
  "planetscale.container"?: string;
  "planetscale.availability_zone"?: string;
}

export interface LogEntry {
  time?: string;
  level?: string;
  message: string;
  pod?: string;
  role?: string;
  container?: string;
  availability_zone?: string;
}

/**
 * Split a LogsQL query on its top-level `|` separators, ignoring any that
 * appear inside a quoted string (LogsQL quotes with `"`, `'` or a backtick,
 * and the first two honour backslash escapes) or inside parentheses. A regex
 * filter such as `_msg:~"error|warning"` must not be treated as a pipe stage,
 * and neither must the `| fields` pipe every `in(...)` subquery ends with.
 */
function splitPipeStages(query: string): string[] {
  const stages: string[] = [];
  let current = "";
  let quote: string | undefined;
  let depth = 0;

  for (let i = 0; i < query.length; i++) {
    const char = query[i]!;

    if (quote) {
      current += char;
      if (char === "\\" && quote !== "`" && i + 1 < query.length) {
        current += query[++i]!;
      } else if (char === quote) {
        quote = undefined;
      }
      continue;
    }

    if (char === '"' || char === "'" || char === "`") {
      quote = char;
      current += char;
    } else if (char === "(") {
      depth++;
      current += char;
    } else if (char === ")") {
      // A stray closer must not disable splitting for the rest of the query.
      if (depth > 0) depth--;
      current += char;
    } else if (char === "|" && depth === 0) {
      stages.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  stages.push(current);
  return stages;
}

/**
 * Build a LogsQL query string for the logs service. The user-supplied query
 * may contain a filter expression plus optional pipe stages after `|`;
 * structured filters (time, levels, role, pods) are ANDed onto the filter,
 * and sorting/pagination stages are appended last.
 */
function buildLogsQuery(options: {
  query?: string;
  time: string;
  levels?: string[];
  role?: string;
  pods?: string[];
  offset: number;
}): string {
  const [filter, ...pipes] = splitPipeStages((options.query ?? "").trim());

  // Wrap the caller's filter so a top-level OR in it doesn't bind looser than
  // the AND-joined time/level/server filters appended below.
  const trimmedFilter = filter?.trim();
  const parts = [trimmedFilter ? `(${trimmedFilter})` : "*"];
  parts.push(`_time:${options.time}`);

  if (options.levels && options.levels.length > 0) {
    parts.push(
      `(${options.levels.map((l) => `planetscale.level:${l}`).join(" OR ")})`
    );
  }

  if (options.role) {
    parts.push(`planetscale.role:${options.role}`);
  }

  if (options.pods && options.pods.length > 0) {
    const podQueries = options.pods.map(
      (pod) => `planetscale.pod:${quoteLogsQLValue(pod)}`
    );
    parts.push(`(${podQueries.join(" OR ")})`);
  }

  const stages = pipes.map((p) => p.trim()).filter(Boolean);
  if (stages.length > 0) {
    parts.push(`| ${stages.join(" | ")}`);
  }

  parts.push(`| sort by (_time desc) | offset ${options.offset}`);

  return parts.join(" ");
}

/**
 * Parse one NDJSON line from the logs service. The `_msg` field usually
 * contains JSON with a `message` key, but can also be plain text.
 */
function parseLogLine(line: string): LogEntry | undefined {
  if (!line.trim()) return undefined;

  let parsed: unknown;
  try {
    parsed = JSON.parse(line) as unknown;
  } catch {
    return undefined;
  }

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return undefined;
  }
  const raw = parsed as RawLogEntry;

  // Rows produced by aggregating pipe stages (e.g. `| stats count()`) have
  // no _msg field; surface them raw instead of dropping them.
  if (!Object.hasOwn(raw, "_msg")) return { message: line };

  let message = typeof raw._msg === "string" ? raw._msg : line;
  if (raw._msg) {
    try {
      const inner = JSON.parse(raw._msg) as { message?: string };
      if (inner && typeof inner.message === "string") {
        message = inner.message;
      }
    } catch {
      // _msg is plain text; use it as-is
    }
  }

  const entry: LogEntry = { message };
  if (raw._time) entry.time = raw._time;
  if (raw["planetscale.level"]) entry.level = raw["planetscale.level"];
  if (raw["planetscale.pod"]) entry.pod = raw["planetscale.pod"];
  if (raw["planetscale.role"]) entry.role = raw["planetscale.role"];
  if (raw["planetscale.container"]) {
    entry.container = raw["planetscale.container"];
  }
  if (raw["planetscale.availability_zone"]) {
    entry.availability_zone = raw["planetscale.availability_zone"];
  }
  return entry;
}

export const getPostgresLogsGram = new Gram().tool({
  name: "get_postgres_logs",
  description:
    "Fetch server logs for a PlanetScale Postgres database branch (not available for Vitess/MySQL databases). Returns recent log entries sorted newest first. Supports filtering by log level, time range, server role, and pod name, plus an optional raw LogsQL query for advanced filtering (e.g. word matching, field filters, and pipe stages like 'error | stats count()'). Default time window is the last hour.",
  annotations: {
    title: "Get Postgres server logs",
    readOnlyHint: true,
    destructiveHint: false,
    openWorldHint: false,
  },
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    query: z
      .string()
      .optional()
      .describe(
        "Optional LogsQL filter expression matched against log content (e.g. 'checkpoint', 'error AND connection'). May include pipe stages after '|'. The tool appends its own 'sort by (_time desc)' and pagination stages after any supplied here, so a custom 'sort by' stage is overridden. Defaults to '*' (all logs)."
      ),
    levels: z
      .array(z.enum(LOG_LEVELS))
      .optional()
      .describe(
        "Filter to specific log levels: 'INFO', 'DEBUG', 'WARNING', 'ERROR'. Omit for all levels."
      ),
    period: z
      .string()
      .optional()
      .describe(
        "Recent time window ending at now, written as number+unit with no spaces (units: ms, s, m, h, d, w, y), e.g. '5m', '30m', '1h', '6h', '24h', '7d'. Cannot be combined with from/to. Defaults to '1h' when no time range is given."
      ),
    from: z
      .string()
      .optional()
      .describe(
        "Start of time range (ISO 8601, e.g. '2026-08-17T00:00:00Z'). Must be used together with 'to'."
      ),
    to: z
      .string()
      .optional()
      .describe(
        "End of time range (ISO 8601). Must be used together with 'from'."
      ),
    role: z
      .enum(SERVER_ROLES)
      .optional()
      .describe(
        "Filter to servers in this role: 'primary' or 'replica'. Omit for all roles."
      ),
    pods: z
      .array(z.string())
      .optional()
      .describe("Filter to specific pod names."),
    limit: z
      .number()
      .int()
      .optional()
      .describe(
        `Number of log entries to return (default: ${DEFAULT_LIMIT}, max: ${MAX_LIMIT})`
      ),
    page: z
      .number()
      .int()
      .optional()
      .describe("Page number for pagination (default: 1)"),
  },
  async execute(ctx, input) {
    try {
      // Try ctx.env first, fall back to process.env for local development
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

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

      const period = input["period"];
      const from = input["from"];
      const to = input["to"];

      if (period && (from || to)) {
        return ctx.text(
          "Error: 'period' cannot be combined with 'from'/'to'. Use either period (e.g. '1h', '6h') for a recent window, or from/to for a specific time range."
        );
      }

      if ((from && !to) || (to && !from)) {
        return ctx.text(
          "Error: 'from' and 'to' must be provided together."
        );
      }

      if (period !== undefined && !PERIOD_PATTERN.test(period)) {
        return ctx.text(
          `Error: invalid 'period' value ${JSON.stringify(period)}. Use a duration like '5m', '1h', '24h', '7d' — no spaces.`
        );
      }

      let time: string;
      if (from && to) {
        // Normalized to ISO 8601: LogsQL cannot parse a timestamp containing a
        // space, and an unparseable one would otherwise reach the service raw.
        const start = new Date(from);
        const end = new Date(to);
        for (const [name, value, parsed] of [
          ["from", from, start],
          ["to", to, end],
        ] as const) {
          if (Number.isNaN(parsed.getTime())) {
            return ctx.text(
              `Error: invalid '${name}' value ${JSON.stringify(value)}. Use an ISO 8601 timestamp, e.g. '2026-08-17T00:00:00Z'.`
            );
          }
        }
        // A reversed range would silently match nothing rather than error.
        if (start.getTime() > end.getTime()) {
          return ctx.text(
            `Error: 'from' (${start.toISOString()}) must not be later than 'to' (${end.toISOString()}).`
          );
        }
        time = `[${start.toISOString()}, ${end.toISOString()}]`;
      } else {
        time = period ?? "1h";
      }
      // A fractional `offset` is a LogsQL parse error, so these stay whole;
      // the clamp bounds out-of-range values rather than refusing them.
      const limit = Math.trunc(
        Math.min(Math.max(input["limit"] ?? DEFAULT_LIMIT, 1), MAX_LIMIT)
      );
      const page = Math.trunc(Math.max(input["page"] ?? 1, 1));

      const authHeader = getAuthHeader(env);

      // Signatures are only issued for Postgres/Neki branches; Vitess/MySQL
      // branches 404 here.
      const signature = await createLogSignature(
        organization,
        database,
        branch,
        authHeader,
        ctx.signal
      );

      const logsQuery = buildLogsQuery({
        query: input["query"],
        time,
        levels: input["levels"],
        role: input["role"],
        pods: input["pods"],
        offset: (page - 1) * limit,
      });

      // The signed URL already points at the branch's logs endpoint and
      // carries the sig/exp credential pair.
      // Fetch one row beyond the page so has_next reflects the service
      // actually holding more rows, rather than guessing from a full page.
      const url = new URL(signature.url);
      url.searchParams.set("limit", (limit + 1).toString());
      url.searchParams.set("query", logsQuery);

      // The signature is the credential; no auth header is sent here.
      const response = await fetch(url, { signal: ctx.signal });

      if (!response.ok) {
        const body = await response.text();
        const reported =
          body.length > MAX_ERROR_BODY_CHARS
            ? `${body.slice(0, MAX_ERROR_BODY_CHARS)} … (truncated)`
            : body;
        return ctx.text(
          `Error: Failed to fetch logs (status: ${response.status}): ${reported}`
        );
      }

      const text = await response.text();
      // has_next counts raw lines, not parsed entries, so an unparseable
      // line cannot end pagination early; offsets stay raw-row-based too.
      const lines = text.split("\n").filter((line) => line.trim());
      const logs = lines
        .slice(0, limit)
        .map(parseLogLine)
        .filter((entry): entry is LogEntry => entry !== undefined);

      return ctx.json({
        branch,
        query: logsQuery,
        page,
        total: logs.length,
        has_next: lines.length > limit,
        logs,
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        if (error.statusCode === 404) {
          return ctx.text(
            "Error: Logs not available. Check your organization, database, and branch names, and note that logs are only available for Postgres databases. (status: 404)"
          );
        }
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }

      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }

      return ctx.text(`Error: An unexpected error occurred`);
    }
  },
});
