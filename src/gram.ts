import "dotenv/config";
import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { executeReadQueryGram } from "./tools/execute-read-query.ts";
import { executeWriteQueryGram } from "./tools/execute-write-query.ts";
import { getInsightsGram } from "./tools/get-insights.ts";
import { listClusterSizesGram } from "./tools/list-cluster-sizes.ts";
import { searchDocumentationGram } from "./tools/search-documentation.ts";
import { listDeployRequestsGram } from "./tools/list-deploy-requests.ts";
import { getDeployRequestGram } from "./tools/get-deploy-request.ts";
import { getDeployQueueGram } from "./tools/get-deploy-queue.ts";
import { listDeployOperationsGram } from "./tools/list-deploy-operations.ts";
import { createDeployRequestGram } from "./tools/create-deploy-request.ts";
import { deployDeployRequestGram } from "./tools/deploy-deploy-request.ts";
import { closeDeployRequestGram } from "./tools/close-deploy-request.ts";

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
  .extend(listDeployRequestsGram)
  .extend(getDeployRequestGram)
  .extend(getDeployQueueGram)
  .extend(listDeployOperationsGram)
  .extend(createDeployRequestGram)
  .extend(deployDeployRequestGram)
  .extend(closeDeployRequestGram);

export default gram;
