import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import {
  getDatabase,
  createVitessCredentials,
  createPostgresCredentials,
  deletePostgresRole,
  PlanetScaleAPIError,
} from "../lib/planetscale-api.ts";
import {
  executeVitessQuery,
  executePostgresQuery,
  QueryTimeoutError,
} from "../lib/query-executor.ts";
import { validateWriteQuery } from "../lib/query-validator.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

export const executeWriteQueryGram = new Gram().tool({
  name: "execute_write_query",
  description:
    "Execute a write SQL query (INSERT, UPDATE, DELETE, or DDL) against a PlanetScale database. This tool creates short-lived credentials and executes the query securely. Queries have a maximum execution time of 50 seconds — if a query exceeds this limit it will be cancelled, so ensure queries are optimized. TRUNCATE is blocked. DELETE and UPDATE without WHERE clause are blocked. For Postgres only: use postgres_database_name when the user has created additional databases in the same cluster and wants to run the query against a non-default database. IMPORTANT: DELETE queries and DDL statements (CREATE, DROP, ALTER, RENAME) require human confirmation - you MUST ask the user for explicit approval before setting confirm_destructive: true. Never set confirm_destructive without first showing the user the exact query and getting their explicit 'yes' or approval.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    query: z.string().describe("SQL INSERT/UPDATE/DELETE/DDL query to execute"),
    postgres_database_name: z
      .string()
      .optional()
      .describe(
        "Postgres only: target database name to connect to. Use when the user has created additional databases in the same PlanetScale Postgres cluster (e.g. via CREATE DATABASE). Omit to use the default database for the branch."
      ),
    confirm_destructive: z
      .boolean()
      .optional()
      .describe(
        "HUMAN CONFIRMATION REQUIRED: Only set to true after explicitly asking the user and receiving their approval. Show them the exact DELETE or DDL query first."
      ),
  },
  async execute(ctx, input) {
    try {
      // Try ctx.env first, fall back to process.env for local development
      const env = Object.keys(ctx.env).length > 0
        ? (ctx.env as Record<string, string | undefined>)
        : process.env;

      // Check authentication
      const auth = getAuthToken(env);
      if (!auth) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const query = input["query"];
      if (!query) {
        return ctx.text("Error: query is required");
      }

      const organization = input["organization"];
      const database = input["database"];
      const branch = input["branch"];
      const confirmed = input["confirm_destructive"] ?? false;

      if (!organization || !database || !branch) {
        return ctx.text("Error: organization, database, and branch are required");
      }

      // Validate the write query for safety
      const validation = validateWriteQuery(query, confirmed);
      if (!validation.allowed) {
        if (validation.requiresConfirmation) {
          return ctx.text(`HUMAN CONFIRMATION REQUIRED\n\nThis query needs explicit user approval before execution.\n\nQuery: ${query}\n\nReason: ${validation.reason}\n\nINSTRUCTIONS FOR AI: You must ASK the user if they want to proceed with this query. Do NOT set confirm_destructive: true until the user explicitly says "yes" or "approved" or similar confirmation.`);
        }
        return ctx.text(`Error: ${validation.reason ?? "Query validation failed"}`);
      }

      // Get auth header for API calls
      const authHeader = getAuthHeader(env);

      // Get database info to determine type
      const db = await getDatabase(organization, database, authHeader);

      if (db.kind === "mysql") {
        // Vitess database - create password with admin role for DDL support
        const credentials = await createVitessCredentials(
          organization,
          database,
          branch,
          "admin",
          authHeader
        );

        const result = await executeVitessQuery(credentials, query);
        return ctx.json(result);
      } else {
        // Postgres database - create role with full permissions including DDL
        // - 'postgres' provides full access to the database
        // - 'pg_write_all_data' provides write access to all tables
        // - 'pg_maintain' (Postgres 17+) allows maintenance operations (CREATE INDEX,
        //   VACUUM, etc.) on any table regardless of ownership
        const credentials = await createPostgresCredentials(
          organization,
          database,
          branch,
          ["postgres", "pg_write_all_data", "pg_maintain"],
          authHeader
        );

        const postgresDatabaseName = input["postgres_database_name"];
        const result = await executePostgresQuery(
          credentials,
          query,
          postgresDatabaseName
        );

        // Delete the role and transfer ownership of any objects created by this
        // role to the 'postgres' role. This ensures future ephemeral users can
        // manage (alter, drop, add indexes to) objects created in this session.
        // See: https://planetscale.com/docs/api/reference/delete_role
        await deletePostgresRole(
          organization,
          database,
          branch,
          credentials.id,
          authHeader,
          { successor: "postgres" }
        );

        return ctx.json(result);
      }
    } catch (error) {
      if (error instanceof QueryTimeoutError) {
        return ctx.text(`Error: ${error.message}`);
      }

      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }

      if (error instanceof Error) {
        const postgresDbOverride = input["postgres_database_name"];
        const isLikelyPostgresDbError =
          postgresDbOverride &&
          (error.message.includes("does not exist") ||
            error.message.includes("database") ||
            error.message.includes("connection") ||
            error.message.includes("ECONNREFUSED"));

        // Check for "direct DDL is disabled" error (Vitess with safe migrations enabled)
        if (error.message.includes("direct DDL is disabled")) {
          const branchUrl = `https://app.planetscale.com/${input["organization"]}/${input["database"]}/${input["branch"]}`;
          return ctx.text(
            `Error: Direct DDL is disabled on this branch.\n\n` +
            `This branch has safe migrations enabled, which means schema changes (CREATE, ALTER, DROP) ` +
            `cannot be executed directly.\n\n` +
            `To make schema changes, you can either:\n` +
            `1. Disable safe migrations on this branch: ${branchUrl}\n` +
            `2. Create a development branch, make your changes there, and deploy via a deploy request\n\n` +
            `Learn more: https://planetscale.com/docs/concepts/safe-migrations`
          );
        }

        let message = error.message;
        if (isLikelyPostgresDbError) {
          message += ` If you set postgres_database_name, ensure "${postgresDbOverride}" exists in this branch (e.g. created via CREATE DATABASE).`;
        }
        return ctx.text(`Error: ${message}`);
      }

      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
