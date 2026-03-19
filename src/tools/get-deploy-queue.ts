import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface DeployOperationRaw {
  state?: string;
  table_name?: string;
  operation_name?: string;
  progress_percentage?: number;
  eta_seconds?: number;
}

interface DeploymentActorRaw {
  display_name?: string;
}

interface DeploymentRaw {
  id?: string;
  state?: string;
  deploy_request_number?: number;
  into_branch?: string;
  deployable?: boolean;
  auto_cutover?: boolean;
  auto_delete_branch?: boolean;
  actor?: DeploymentActorRaw;
  deploy_operations?: DeployOperationRaw[];
  created_at?: string;
  queued_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
}

interface PaginatedResponse {
  data: DeploymentRaw[];
  current_page: number;
  next_page: number | null;
  prev_page: number | null;
}

function filterDeployment(d: DeploymentRaw) {
  return {
    id: d.id,
    state: d.state,
    deploy_request_number: d.deploy_request_number,
    into_branch: d.into_branch,
    deployable: d.deployable,
    auto_cutover: d.auto_cutover,
    auto_delete_branch: d.auto_delete_branch,
    actor_name: d.actor?.display_name ?? null,
    created_at: d.created_at,
    queued_at: d.queued_at,
    started_at: d.started_at,
    finished_at: d.finished_at,
    deploy_operations: (d.deploy_operations || []).map((op) => ({
      state: op.state,
      table_name: op.table_name,
      operation_name: op.operation_name,
      progress_percentage: op.progress_percentage,
      eta_seconds: op.eta_seconds,
    })),
  };
}

export const getDeployQueueGram = new Gram().tool({
  name: "get_deploy_queue",
  description:
    "Get the deploy queue for a PlanetScale database, showing deployments that are currently queued or in progress. Useful for checking if there are pending deployments before creating or deploying a new deploy request.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    page: z.number().optional().describe("Page number (default: 1)"),
    per_page: z
      .number()
      .optional()
      .describe("Results per page (default: 25, max: 50)"),
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

      const organization = input["organization"];
      const database = input["database"];

      if (!organization || !database) {
        return ctx.text("Error: organization and database are required");
      }

      const authHeader = getAuthHeader(env);
      const page = input["page"] ?? 1;
      const perPage = Math.min(input["per_page"] ?? 25, 50);

      const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/deploy-queue?page=${page}&per_page=${perPage}`;

      const response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: authHeader,
          Accept: "application/json",
        },
      });

      if (!response.ok) {
        let details: unknown;
        try {
          details = await response.json();
        } catch {
          details = await response.text();
        }

        if (response.status === 404) {
          throw new PlanetScaleAPIError(
            "Resource not found. Please check your organization and database names.",
            response.status,
            details
          );
        }

        if (response.status === 401 || response.status === 403) {
          throw new PlanetScaleAPIError(
            "Permission denied. Please check your API token has the required permissions.",
            response.status,
            details
          );
        }

        throw new PlanetScaleAPIError(
          `Failed to fetch deploy queue: ${response.statusText}`,
          response.status,
          details
        );
      }

      const result = (await response.json()) as PaginatedResponse;
      const filtered = (result.data || []).map(filterDeployment);

      return ctx.json({
        organization,
        database,
        page: result.current_page,
        next_page: result.next_page,
        total: filtered.length,
        deployments: filtered,
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
