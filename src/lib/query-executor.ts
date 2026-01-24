import { connect } from "@planetscale/database";
import { neon, neonConfig } from "@neondatabase/serverless";
import type { VitessCredentials, PostgresCredentials } from "./planetscale-api.ts";

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

  const result = await conn.execute(query);
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
 * Execute a query against a Postgres database using @neondatabase/serverless
 */
export async function executePostgresQuery(
  credentials: PostgresCredentials,
  query: string
): Promise<QueryResult> {
  const startTime = performance.now();

  // Configure Neon for PlanetScale Postgres connections
  neonConfig.fetchEndpoint = (host) => `https://${host}/sql`;

  // Append |replica to username for replica routing if enabled
  const username = credentials.replica
    ? `${credentials.username}|replica`
    : credentials.username;

  // Build the connection URL (port 5432 for direct connections, required for replicas)
  const connectionUrl = `postgresql://${encodeURIComponent(username)}:${encodeURIComponent(credentials.password)}@${credentials.host}:5432/${encodeURIComponent(credentials.database_name)}`;

  const sql = neon(connectionUrl);

  // Use sql.query() for raw string queries (not parameterized)
  // Note: This is safe because we're executing user-provided SQL directly
  // The user is responsible for the query content
  const result = await sql.query(query);
  const executionTime = performance.now() - startTime;

  // Result is an array of row objects
  const rows = Array.isArray(result) ? result : [];

  // Extract column names from the first row
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
}
