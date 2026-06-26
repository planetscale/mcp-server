import { connect } from "@planetscale/database";
import { neon, neonConfig, type NeonQueryFunction } from "@neondatabase/serverless";
import type { VitessCredentials, PostgresCredentials } from "./planetscale-api.ts";

const QUERY_TIMEOUT_MS = 50_000;
const MAX_RLS_WARNING_RELATIONS = 10;

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
  warnings?: QueryWarning[];
}

export interface QueryWarning {
  code: "postgres_rls_active";
  message: string;
  relations?: string[];
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

type PostgresSql = NeonQueryFunction<false, false>;

interface PostgresRlsRelation {
  schema_name: string;
  relation_name: string;
  row_security_enabled: boolean;
  force_row_security: boolean;
}

function isZeroLikeValue(value: unknown): boolean {
  if (typeof value === "number") return value === 0;
  if (typeof value === "bigint") return value === 0n;
  if (typeof value === "string") return value === "0";
  return false;
}

function isZeroCountResult(
  query: string,
  rows: Record<string, unknown>[],
  columns: string[]
): boolean {
  if (!/\bcount\s*\(/i.test(query)) return false;
  if (rows.length !== 1 || columns.length === 0) return false;

  const row = rows[0];
  if (row === undefined) return false;

  return columns.every((column) => isZeroLikeValue(row[column]));
}

async function getActivePostgresRlsRelations(
  sql: PostgresSql
): Promise<PostgresRlsRelation[]> {
  const result = await sql.query(`
    SELECT
      n.nspname AS schema_name,
      c.relname AS relation_name,
      c.relrowsecurity AS row_security_enabled,
      c.relforcerowsecurity AS force_row_security
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
      AND pg_catalog.row_security_active(c.oid)
    ORDER BY n.nspname, c.relname
    LIMIT ${MAX_RLS_WARNING_RELATIONS + 1};
  `);

  return (Array.isArray(result) ? result : []) as PostgresRlsRelation[];
}

async function buildPostgresRlsWarnings(
  sql: PostgresSql,
  query: string,
  rows: Record<string, unknown>[],
  columns: string[]
): Promise<QueryWarning[] | undefined> {
  if (rows.length > 0 && !isZeroCountResult(query, rows, columns)) {
    return undefined;
  }

  let relations: PostgresRlsRelation[];
  try {
    relations = await getActivePostgresRlsRelations(sql);
  } catch {
    return undefined;
  }

  if (relations.length === 0) {
    return undefined;
  }

  const visibleRelations = relations.slice(0, MAX_RLS_WARNING_RELATIONS);
  const relationNames = visibleRelations.map(
    (relation) => `${relation.schema_name}.${relation.relation_name}`
  );
  const moreRelationsCount = relations.length - visibleRelations.length;
  const listedRelations = relationNames.join(", ");
  const relationSummary =
    moreRelationsCount > 0
      ? `${listedRelations}, and ${moreRelationsCount} more relation${moreRelationsCount === 1 ? "" : "s"}`
      : listedRelations;

  return [
    {
      code: "postgres_rls_active",
      message:
        `Postgres row-level security is active for ${relationSummary} under this MCP role. ` +
        "The MCP role has pg_read_all_data but does not bypass RLS, so returned rows may be filtered; a zero-row or zero-count result does not necessarily mean the underlying tables are empty.",
      relations: relationNames,
    },
  ];
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
    const warnings = await buildPostgresRlsWarnings(
      sql,
      query,
      rows as Record<string, unknown>[],
      columns
    );

    return {
      success: true,
      database_type: "postgres",
      rows: rows as Record<string, unknown>[],
      row_count: rows.length,
      columns,
      execution_time_ms: Math.round(executionTime),
      ...(warnings !== undefined ? { warnings } : {}),
    };
  } catch (error) {
    if (isTimeoutError(error)) {
      throw new QueryTimeoutError(performance.now() - startTime);
    }
    throw error;
  }
}
