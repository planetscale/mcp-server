import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import {
  getBranch,
  createVitessCredentials,
  createPostgresCredentials,
  PlanetScaleAPIError,
} from "../lib/planetscale-api.ts";
import {
  executeVitessQuery,
  executePostgresQuery,
} from "../lib/query-executor.ts";
import { validateReadQuery } from "../lib/query-validator.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

export const executeReadQueryGram = new Gram().tool({
  name: "execute_read_query",
  description:
    "Execute a read-only SQL query (SELECT, SHOW, DESCRIBE, EXPLAIN) against a PlanetScale database. This tool creates short-lived credentials and executes the query securely.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    query: z.string().describe("SQL SELECT query to execute"),
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

        const result = await executePostgresQuery(credentials, query);
        return ctx.json(result);
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
