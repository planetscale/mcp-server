import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError, listKeyspaces } from "../lib/planetscale-api.ts";
import type { Keyspace } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

// Fields to strip from keyspace responses for token efficiency
const STRIP_FIELDS = new Set([
  "type",
  "cluster_rate_name",
  "cluster_rate_display_name",
]);

/**
 * Filter a keyspace entry to remove redundant fields and null values
 */
function filterKeyspace(keyspace: Keyspace): Partial<Keyspace> {
  const filtered: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(keyspace)) {
    if (STRIP_FIELDS.has(key)) continue;
    if (value === null) continue;
    filtered[key] = value;
  }
  return filtered as Partial<Keyspace>;
}

export const getBranchKeyspacesGram = new Gram().tool({
  name: "get_branch_keyspaces",
  description:
    "List keyspaces for a PlanetScale database branch. Returns keyspace configuration including shard count, cluster size, replica count, replication durability settings, and MySQL/VTTablet options. Useful for understanding the topology and sizing of a database.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    page: z
      .number()
      .optional()
      .describe("Page number for pagination (default: 1)"),
    per_page: z
      .number()
      .optional()
      .describe("Results per page (default: 25)"),
  },
  async execute(ctx, input) {
    try {
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

      const auth = getAuthToken(env);
      if (!auth) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const { organization, database, branch } = input;
      if (!organization || !database || !branch) {
        return ctx.text(
          "Error: organization, database, and branch are required"
        );
      }

      const authHeader = getAuthHeader(env);
      const response = await listKeyspaces(
        organization,
        database,
        branch,
        authHeader,
        { page: input.page, perPage: input.per_page }
      );

      const keyspaces = response.data.map(filterKeyspace);

      return ctx.json({
        organization,
        database,
        branch,
        total_keyspaces: keyspaces.length,
        current_page: response.current_page,
        next_page: response.next_page,
        keyspaces,
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
