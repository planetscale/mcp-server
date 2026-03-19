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
  notes: string | null;
  html_url: string;
  created_at: string;
  updated_at: string;
  deployed_at: string | null;
  deployment?: {
    state?: string;
    auto_cutover?: boolean;
    auto_delete_branch?: boolean;
    deployable?: boolean;
    queued_at?: string | null;
    started_at?: string | null;
  };
}

function filterDeployRequest(dr: DeployRequestResponse) {
  return {
    number: dr.number,
    state: dr.state,
    deployment_state: dr.deployment_state,
    branch: dr.branch,
    into_branch: dr.into_branch,
    approved: dr.approved,
    actor_name: dr.actor?.display_name ?? null,
    html_url: dr.html_url,
    created_at: dr.created_at,
    updated_at: dr.updated_at,
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
          },
        }
      : {}),
  };
}

async function fetchDeployRequest(
  organization: string,
  database: string,
  number: number,
  authHeader: string
): Promise<DeployRequestResponse> {
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

    throw new PlanetScaleAPIError(
      `Failed to fetch deploy request: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as DeployRequestResponse;
}

async function queueDeployRequest(
  organization: string,
  database: string,
  number: number,
  instantDdl: boolean | undefined,
  authHeader: string
): Promise<DeployRequestResponse> {
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/deploy-requests/${encodeURIComponent(String(number))}/deploy`;

  const body: Record<string, boolean> = {};
  if (instantDdl != null) body["instant_ddl"] = instantDdl;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: authHeader,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(body),
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
      `Failed to deploy deploy request: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as DeployRequestResponse;
}

export const deployDeployRequestGram = new Gram().tool({
  name: "deploy_deploy_request",
  description:
    "Queue a deploy request for deployment, starting the process of applying schema changes to the target branch. WARNING: This will begin applying schema changes to the target branch (often production). You MUST ask the user for explicit confirmation before calling this tool. The deploy request must be in a deployable state (check with get_deploy_request first).",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    number: z.number().describe("Deploy request number to deploy"),
    confirm_deploy: z
      .boolean()
      .optional()
      .describe(
        "HUMAN CONFIRMATION REQUIRED: Only set to true after explicitly asking the user and receiving their approval. Show them the deploy request number and target branch first."
      ),
    instant_ddl: z
      .boolean()
      .optional()
      .describe(
        "Enable instant DDL for compatible operations (default: false)"
      ),
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

      const confirmed = input["confirm_deploy"] ?? false;
      const authHeader = getAuthHeader(env);

      if (!confirmed) {
        // Fetch the deploy request to show details in the confirmation message
        const dr = await fetchDeployRequest(
          organization,
          database,
          number,
          authHeader
        );

        return ctx.text(
          `HUMAN CONFIRMATION REQUIRED\n\n` +
            `You are about to deploy schema changes to the target branch.\n\n` +
            `Deploy Request #${dr.number}\n` +
            `  Source branch: ${dr.branch}\n` +
            `  Target branch: ${dr.into_branch}\n` +
            `  State: ${dr.deployment_state}\n` +
            `  URL: ${dr.html_url}\n\n` +
            `INSTRUCTIONS FOR AI: You must ASK the user if they want to proceed with this deployment. ` +
            `Do NOT set confirm_deploy: true until the user explicitly says "yes" or gives approval.`
        );
      }

      const dr = await queueDeployRequest(
        organization,
        database,
        number,
        input["instant_ddl"],
        authHeader
      );

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
