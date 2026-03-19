import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface DeployRequestActor {
  display_name?: string;
}

interface DeployRequestResponse {
  number: number;
  state: string;
  deployment_state: string;
  branch: string;
  into_branch: string;
  approved: boolean;
  actor?: DeployRequestActor;
  closed_by?: DeployRequestActor;
  html_url: string;
  created_at: string;
  updated_at: string;
  closed_at: string | null;
}

function filterDeployRequest(dr: DeployRequestResponse) {
  return {
    number: dr.number,
    state: dr.state,
    deployment_state: dr.deployment_state,
    branch: dr.branch,
    into_branch: dr.into_branch,
    html_url: dr.html_url,
    closed_by_name: dr.closed_by?.display_name ?? null,
    created_at: dr.created_at,
    closed_at: dr.closed_at,
  };
}

export const closeDeployRequestGram = new Gram().tool({
  name: "close_deploy_request",
  description:
    "Close a deploy request without deploying it. Only open deploy requests can be closed. Use get_deploy_request to check the current state first.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    number: z.number().describe("Deploy request number to close"),
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
        method: "PATCH",
        headers: {
          Authorization: authHeader,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ state: "closed" }),
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
          `Failed to close deploy request: ${response.statusText}`,
          response.status,
          details
        );
      }

      const dr = (await response.json()) as DeployRequestResponse;
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
