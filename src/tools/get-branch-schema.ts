import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import {
  PlanetScaleAPIError,
  getBranchSchema,
} from "../lib/planetscale-api.ts";
import type { SchemaTable } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

function filterTable(table: SchemaTable): { name: string; raw: string; annotated: boolean } {
  return { name: table.name, raw: table.raw, annotated: table.annotated };
}

function fetchSchema(ctx: { env: Record<string, unknown> }, input: { organization: string; database: string; branch: string; keyspace?: string }) {
  const env =
    Object.keys(ctx.env).length > 0
      ? (ctx.env as Record<string, string | undefined>)
      : process.env;

  const auth = getAuthToken(env);
  if (!auth) {
    throw new Error("No PlanetScale authentication configured.");
  }

  const authHeader = getAuthHeader(env);
  return getBranchSchema(
    input.organization,
    input.database,
    input.branch,
    authHeader,
    { keyspace: input.keyspace }
  );
}

const branchInputSchema = {
  organization: z.string().describe("PlanetScale organization name"),
  database: z.string().describe("Database name"),
  branch: z.string().describe("Branch name (e.g., 'main')"),
  keyspace: z
    .string()
    .optional()
    .describe(
      "Vitess keyspace to filter by. When omitted, only tables in the default keyspace are returned — tables in other keyspaces will not be visible. Use get_branch_keyspaces to discover available keyspaces."
    ),
};

export const getBranchTablesGram = new Gram().tool({
  name: "get_branch_tables",
  description:
    "List table names for a PlanetScale database branch. Returns only the table names without DDL — use get_table_schema to fetch the full CREATE TABLE statement for a specific table. Use get_branch_keyspaces first to discover available keyspaces.",
  inputSchema: branchInputSchema,
  async execute(ctx, input) {
    try {
      const response = await fetchSchema(ctx, input);

      return ctx.json({
        organization: input.organization,
        database: input.database,
        branch: input.branch,
        keyspace: input.keyspace ?? null,
        total_tables: response.data.length,
        tables: response.data.map((t) => t.name),
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(
          `Error: ${error.message} (status: ${error.statusCode})`
        );
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});

export const getTableSchemaGram = new Gram().tool({
  name: "get_table_schema",
  description:
    "Get the CREATE TABLE DDL for a specific table on a PlanetScale database branch. Use get_branch_tables first to discover available table names.",
  inputSchema: {
    ...branchInputSchema,
    table: z.string().describe("Table name to get the schema for"),
  },
  async execute(ctx, input) {
    try {
      const response = await fetchSchema(ctx, input);

      const match = response.data.find((t) => t.name === input.table);
      if (!match) {
        const keyspaceLabel = input.keyspace ? `keyspace '${input.keyspace}'` : "this branch";
        return ctx.text(
          `Error: Table '${input.table}' not found in ${keyspaceLabel}. Use get_branch_tables to list available tables.`
        );
      }

      return ctx.json({
        organization: input.organization,
        database: input.database,
        branch: input.branch,
        keyspace: input.keyspace ?? null,
        table: filterTable(match),
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(
          `Error: ${error.message} (status: ${error.statusCode})`
        );
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
