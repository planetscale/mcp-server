import "dotenv/config";
import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { executeReadQueryGram } from "./tools/execute-read-query.ts";
import { executeWriteQueryGram } from "./tools/execute-write-query.ts";
import { getInsightsGram } from "./tools/get-insights.ts";
import { listClusterSizesGram } from "./tools/list-cluster-sizes.ts";
import { searchDocumentationGram } from "./tools/search-documentation.ts";
import { getBranchKeyspacesGram } from "./tools/get-branch-keyspaces.ts";
import { getBranchTablesGram, getTableSchemaGram } from "./tools/get-branch-schema.ts";

const gram = new Gram({
  envSchema: {
    PLANETSCALE_OAUTH2_ACCESS_TOKEN: z.string().describe(
      "OAuth2 access token for PlanetScale API"
    ),
    PLANETSCALE_DOCS_MCP_URL: z
      .string()
      .optional()
      .describe("Override URL for the PlanetScale docs MCP server"),
  },
  authInput: {
    oauthVariable: "PLANETSCALE_OAUTH2_ACCESS_TOKEN",
  },
})
  .extend(executeReadQueryGram)
  .extend(executeWriteQueryGram)
  .extend(getInsightsGram)
  .extend(listClusterSizesGram)
  .extend(searchDocumentationGram)
  .extend(getBranchKeyspacesGram)
  .extend(getBranchTablesGram)
  .extend(getTableSchemaGram);

export default gram;
