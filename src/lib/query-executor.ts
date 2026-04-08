import { connect } from "@planetscale/database";
import { neon, neonConfig } from "@neondatabase/serverless";
import type { VitessCredentials, PostgresCredentials } from "./planetscale-api.ts";

const QUERY_TIMEOUT_MS = 50_000;

export class QueryTimeoutError extends Error {
  constructor(executionTimeMs: number) {
    super(
      `Query exceeded the maximum allowed execution time of ${QUERY_TIMEOUT_MS / 1000} seconds (ran for ~${Math.round(executionTimeMs / 1000)}s). ` +
      `Please optimize your query — consider adding indexes, reducing the result set, or breaking it into smaller queries.`
    );
    this.name = "QueryTimeoutError";
  }
}

function isTimeoutError(error: unknown): boolean {
  if (error instanceof DOMException && error.name === "TimeoutError") return true;
  if (error instanceof DOMException && error.name === "AbortError") return true;
  if (error instanceof Error && error.name === "AbortError") return true;
  return false;
}

/**
 * Add sqlcommenter tag to identify queries from this MCP server.
 * Format follows the sqlcommenter spec: https://google.github.io/sqlcommenter/spec/
 */
function addSqlCommenterTag(query: string): string {
  // Don't modify queries that already have comments
  if (query.includes("/*") || query.includes("--")) {
    return query;
  }

  // Format: source='planetscale-mcp'
  const tag = `source='planetscale-mcp'`;

  // Trim trailing whitespace/semicolon, append comment, restore semicolon if needed
  const trimmed = query.trimEnd();
  const hasSemicolon = trimmed.endsWith(";");
  const base = hasSemicolon ? trimmed.slice(0, -1) : trimmed;

  return `${base} /*${tag}*/${hasSemicolon ? ";" : ""}`;
}

export interface QueryResult {
  success: boolean;
  database_type: "vitess" | "postgres";
  rows: Record<string, unknown>[];
  row_count: number;
  columns: string[];
  execution_time_ms: number;
  rows_affected?: number;
}

/**
 * Execute a query against a Vitess (MySQL) database using @planetscale/database
 */
export async function executeVitessQuery(
  credentials: VitessCredentials,
  query: string
): Promise<QueryResult> {
  const startTime = performance.now();

  const conn = connect({
    host: credentials.host,
    username: credentials.username,
    password: credentials.password,
    fetch: (input, init) =>
      fetch(input, { ...init, signal: AbortSignal.timeout(QUERY_TIMEOUT_MS) }),
  });

  try {
    const taggedQuery = addSqlCommenterTag(query);
    const result = await conn.execute(taggedQuery);
    const executionTime = performance.now() - startTime;

    const columns = result.fields?.map((f) => f.name) ?? [];
    const rows = (result.rows as Record<string, unknown>[]) ?? [];

    return {
      success: true,
      database_type: "vitess",
      rows,
      row_count: rows.length,
      columns,
      execution_time_ms: Math.round(executionTime),
      rows_affected: result.rowsAffected ?? undefined,
    };
  } catch (error) {
    if (isTimeoutError(error)) {
      throw new QueryTimeoutError(performance.now() - startTime);
    }
    throw error;
  }
}

/**
 * Execute a query against a Postgres database using @neondatabase/serverless.
 * @param credentials - Short-lived Postgres credentials from the PlanetScale API.
 * @param query - SQL query to execute.
 * @param databaseNameOverride - Optional. When set, connect to this database instead of credentials.database_name. Use when the user has created additional databases in the same cluster.
 */
export async function executePostgresQuery(
  credentials: PostgresCredentials,
  query: string,
  databaseNameOverride?: string
): Promise<QueryResult> {
  const startTime = performance.now();

  // Configure Neon for PlanetScale Postgres connections
  neonConfig.fetchEndpoint = (host) => `https://${host}/sql`;

  // Append |replica to username for replica routing if enabled
  const username = credentials.replica
    ? `${credentials.username}|replica`
    : credentials.username;

  const databaseName =
    databaseNameOverride !== undefined && databaseNameOverride !== ""
      ? databaseNameOverride
      : credentials.database_name;

  const connectionUrl = `postgresql://${encodeURIComponent(username)}:${encodeURIComponent(credentials.password)}@${credentials.host}:5432/${encodeURIComponent(databaseName)}`;

  const sql = neon(connectionUrl, {
    fetchOptions: { signal: AbortSignal.timeout(QUERY_TIMEOUT_MS) },
  });

  try {
    const taggedQuery = addSqlCommenterTag(query);
    const result = await sql.query(taggedQuery);
    const executionTime = performance.now() - startTime;

    const rows = Array.isArray(result) ? result : [];
    const firstRow = rows[0];
    const columns = firstRow !== undefined ? Object.keys(firstRow) : [];

    return {
      success: true,
      database_type: "postgres",
      rows: rows as Record<string, unknown>[],
      row_count: rows.length,
      columns,
      execution_time_ms: Math.round(executionTime),
    };
  } catch (error) {
    if (isTimeoutError(error)) {
      throw new QueryTimeoutError(performance.now() - startTime);
    }
    throw error;
  }
}
