import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface DeployRequestActor {
  display_name?: string;
}

interface DeployOperationRaw {
  id?: string;
  state?: string;
  keyspace_name?: string;
  table_name?: string;
  operation_name?: string;
  ddl_statement?: string;
  eta_seconds?: number;
  progress_percentage?: number;
  can_drop_data?: boolean;
  deploy_errors?: string | null;
  created_at?: string;
  updated_at?: string;
}

interface DeploymentRaw {
  state?: string;
  auto_cutover?: boolean;
  auto_delete_branch?: boolean;
  deployable?: boolean;
  queued_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  ready_to_cutover_at?: string | null;
  cutover_at?: string | null;
  deploy_operations?: DeployOperationRaw[];
}

interface DeployRequestRaw {
  number: number;
  state: string;
  deployment_state: string;
  branch: string;
  into_branch: string;
  approved: boolean;
  actor?: DeployRequestActor;
  closed_by?: DeployRequestActor;
  notes: string | null;
  html_url: string;
  num_comments: number;
  created_at: string;
  updated_at: string;
  closed_at: string | null;
  deployed_at: string | null;
  deployment?: DeploymentRaw;
}

function filterOperation(op: DeployOperationRaw) {
  return {
    id: op.id,
    state: op.state,
    keyspace_name: op.keyspace_name,
    table_name: op.table_name,
    operation_name: op.operation_name,
    ddl_statement: op.ddl_statement,
    eta_seconds: op.eta_seconds,
    progress_percentage: op.progress_percentage,
    can_drop_data: op.can_drop_data,
    deploy_errors: op.deploy_errors,
  };
}

function filterDeployRequest(dr: DeployRequestRaw) {
  return {
    number: dr.number,
    state: dr.state,
    deployment_state: dr.deployment_state,
    branch: dr.branch,
    into_branch: dr.into_branch,
    approved: dr.approved,
    actor_name: dr.actor?.display_name ?? null,
    closed_by_name: dr.closed_by?.display_name ?? null,
    notes: dr.notes,
    html_url: dr.html_url,
    num_comments: dr.num_comments,
    created_at: dr.created_at,
    updated_at: dr.updated_at,
    closed_at: dr.closed_at,
    deployed_at: dr.deployed_at,
    ...(dr.deployment
      ? {
          deployment: {
            state: dr.deployment.state,
            auto_cutover: dr.deployment.auto_cutover,
            auto_delete_branch: dr.deployment.auto_delete_branch,
            deployable: dr.deployment.deployable,
            queued_at: dr.deployment.queued_at,
            started_at: dr.deployment.started_at,
            finished_at: dr.deployment.finished_at,
            ready_to_cutover_at: dr.deployment.ready_to_cutover_at,
            cutover_at: dr.deployment.cutover_at,
            deploy_operations: (dr.deployment.deploy_operations || []).map(
              filterOperation
            ),
          },
        }
      : {}),
  };
}

export const getDeployRequestGram = new Gram().tool({
  name: "get_deploy_request",
  description:
    "Get details of a specific deploy request by its number, including deployment status, schema change operations, and approval state. Use list_deploy_requests to find deploy request numbers.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    number: z.number().describe("Deploy request number"),
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
      const number = input["number"];

      if (!organization || !database || number == null) {
        return ctx.text(
          "Error: organization, database, and number are required"
        );
      }

      const authHeader = getAuthHeader(env);
      const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/deploy-requests/${encodeURIComponent(String(number))}`;

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
            "Deploy request not found. Please check your organization, database, and deploy request number.",
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
          `Failed to fetch deploy request: ${response.statusText}`,
          response.status,
          details
        );
      }

      const dr = (await response.json()) as DeployRequestRaw;
      return ctx.json(filterDeployRequest(dr));
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
