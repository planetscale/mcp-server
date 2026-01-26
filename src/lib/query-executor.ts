import { connect } from "@planetscale/database";
import { neon, neonConfig } from "@neondatabase/serverless";
import type { VitessCredentials, PostgresCredentials } from "./planetscale-api.ts";

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
  });

  const taggedQuery = addSqlCommenterTag(query);
  const result = await conn.execute(taggedQuery);
  const executionTime = performance.now() - startTime;

  // Extract column names from the result
  const columns = result.fields?.map((f) => f.name) ?? [];

  // Handle both read and write queries
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
}

/**
 * Check if a query is a DDL statement that creates objects
 */
function isCreateStatement(query: string): boolean {
  const normalized = query.trim().toUpperCase();
  return normalized.startsWith("CREATE ");
}

/**
 * Execute a query against a Postgres database using @neondatabase/serverless
 */
export async function executePostgresQuery(
  credentials: PostgresCredentials,
  query: string,
  options?: { ownerRole?: string }
): Promise<QueryResult> {
  const startTime = performance.now();

  // Configure Neon for PlanetScale Postgres connections
  neonConfig.fetchEndpoint = (host) => `https://${host}/sql`;

  // Append |replica to username for replica routing if enabled
  const username = credentials.replica
    ? `${credentials.username}|replica`
    : credentials.username;

  const connectionUrl = `postgresql://${encodeURIComponent(username)}:${encodeURIComponent(credentials.password)}@${credentials.host}:5432/${encodeURIComponent(credentials.database_name)}`;

  const sql = neon(connectionUrl);

  const taggedQuery = addSqlCommenterTag(query);
  let result: unknown[];

  // For CREATE statements, use a transaction to SET ROLE first so the created
  // object is owned by a shared role (e.g., 'postgres') instead of the ephemeral user.
  // This allows future ephemeral users to manage these objects.
  if (options?.ownerRole && isCreateStatement(query)) {
    result = await sql.transaction([
      sql`SELECT set_config('role', ${options.ownerRole}, true)`,
      sql.query(taggedQuery),
    ]);
    // Transaction returns array of results, we want the last one (the actual query)
    result = (result as unknown[][])[1] ?? [];
  } else {
    result = await sql.query(taggedQuery);
  }

  const executionTime = performance.now() - startTime;

  // Result is an array of row objects
  const rows = Array.isArray(result) ? result : [];

  // Extract column names from the first row
  const firstRow = rows[0];
  const columns = firstRow !== undefined ? Object.keys(firstRow as Record<string, unknown>) : [];

  return {
    success: true,
    database_type: "postgres",
    rows: rows as Record<string, unknown>[],
    row_count: rows.length,
    columns,
    execution_time_ms: Math.round(executionTime),
  };
}
