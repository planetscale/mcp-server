import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import {
  getBranch,
  createVitessCredentials,
  createPostgresCredentials,
  deleteVitessPassword,
  deletePostgresRole,
  PlanetScaleAPIError,
} from "../lib/planetscale-api.ts";
import {
  executeVitessQuery,
  executePostgresQuery,
  QueryTimeoutError,
} from "../lib/query-executor.ts";
import { validateReadQuery } from "../lib/query-validator.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

export const executeReadQueryGram = new Gram().tool({
  name: "execute_read_query",
  description:
    "Execute a read-only SQL query (SELECT, SHOW, DESCRIBE, EXPLAIN) against a PlanetScale database. This tool creates short-lived credentials and executes the query securely. Queries have a maximum execution time of 50 seconds — if a query exceeds this limit it will be cancelled, so ensure queries are optimized. For Postgres only: optionally specify postgres_database_name when the user wants to query a non-default database.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    query: z.string().describe("SQL SELECT query to execute"),
    postgres_database_name: z
      .string()
      .optional()
      .describe(
        "Postgres only: target database name to connect to. Use when the user has created additional databases in the same PlanetScale Postgres cluster (e.g. via CREATE DATABASE). Omit to use the default database for the branch."
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

      if (!organization || !database || !branch) {
        return ctx.text("Error: organization, database, and branch are required");
      }

      // Validate the query is read-only
      const validation = validateReadQuery(query);
      if (!validation.allowed) {
        return ctx.text(`Error: ${validation.reason ?? "Query validation failed"}`);
      }

      // Get auth header for API calls
      const authHeader = getAuthHeader(env);

      // Get branch info to determine database type and replica availability
      const branchInfo = await getBranch(organization, database, branch, authHeader);
      const useReplica = branchInfo.has_replicas;

      if (branchInfo.kind === "mysql") {
        // Vitess database - create password with reader role
        // Use replica if available for safer read performance
        const credentials = await createVitessCredentials(
          organization,
          database,
          branch,
          "reader",
          authHeader,
          useReplica
        );

        const result = await executeVitessQuery(credentials, query);

        // Delete the password credentials after query execution
        await deleteVitessPassword(
          organization,
          database,
          branch,
          credentials.id,
          authHeader
        );

        return ctx.json(result);
      } else {
        // Postgres database - create role with read permissions
        const credentials = await createPostgresCredentials(
          organization,
          database,
          branch,
          ["pg_read_all_data"],
          authHeader
        );
        // Set replica flag for query execution
        credentials.replica = useReplica;

        const postgresDatabaseName = input["postgres_database_name"];
        const result = await executePostgresQuery(
          credentials,
          query,
          postgresDatabaseName
        );

        // Delete the role credentials after query execution
        await deletePostgresRole(
          organization,
          database,
          branch,
          credentials.id,
          authHeader
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
        let message = error.message;
        const postgresDbOverride = input["postgres_database_name"];
        if (
          postgresDbOverride &&
          (message.includes("does not exist") ||
            message.includes("database") ||
            message.includes("connection") ||
            message.includes("ECONNREFUSED"))
        ) {
          message += ` If you set postgres_database_name, ensure "${postgresDbOverride}" exists in this branch (e.g. created via CREATE DATABASE).`;
        }
        return ctx.text(`Error: ${message}`);
      }

      return ctx.text(`Error: An unexpected error occurred`);
    }
  },
});
