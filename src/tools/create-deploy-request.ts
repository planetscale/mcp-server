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
  deployment?: {
    state?: string;
    auto_cutover?: boolean;
    auto_delete_branch?: boolean;
    deployable?: boolean;
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
    notes: dr.notes,
    html_url: dr.html_url,
    created_at: dr.created_at,
    updated_at: dr.updated_at,
    ...(dr.deployment
      ? {
          deployment: {
            state: dr.deployment.state,
            auto_cutover: dr.deployment.auto_cutover,
            auto_delete_branch: dr.deployment.auto_delete_branch,
            deployable: dr.deployment.deployable,
          },
        }
      : {}),
  };
}

async function createDeployRequest(
  organization: string,
  database: string,
  body: {
    branch: string;
    into_branch?: string;
    notes?: string;
    auto_cutover?: boolean;
    auto_delete_branch?: boolean;
  },
  authHeader: string
): Promise<DeployRequestResponse> {
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/deploy-requests`;

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
        "Resource not found. Please check your organization, database, and branch names.",
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
      `Failed to create deploy request: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as DeployRequestResponse;
}

export const createDeployRequestGram = new Gram().tool({
  name: "create_deploy_request",
  description:
    "Create a new deploy request to deploy schema changes from a development branch into a target branch. The branch parameter is the source branch containing schema changes, and into_branch is the target branch to deploy into (defaults to the database's default branch). Creating a deploy request does not deploy the changes — use deploy_deploy_request to start the deployment.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z
      .string()
      .describe("Source branch containing schema changes to deploy"),
    into_branch: z
      .string()
      .optional()
      .describe(
        "Target branch to deploy into (defaults to the database's default branch, usually 'main')"
      ),
    notes: z
      .string()
      .optional()
      .describe("Description of the schema changes being deployed"),
    auto_cutover: z
      .boolean()
      .optional()
      .describe(
        "Automatically cut over to the new schema as soon as it is ready (default: false)"
      ),
    auto_delete_branch: z
      .boolean()
      .optional()
      .describe(
        "Automatically delete the source branch after the deploy request completes (default: false)"
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
      const branch = input["branch"];

      if (!organization || !database || !branch) {
        return ctx.text(
          "Error: organization, database, and branch are required"
        );
      }

      const authHeader = getAuthHeader(env);

      const body: {
        branch: string;
        into_branch?: string;
        notes?: string;
        auto_cutover?: boolean;
        auto_delete_branch?: boolean;
      } = { branch };

      if (input["into_branch"]) body.into_branch = input["into_branch"];
      if (input["notes"]) body.notes = input["notes"];
      if (input["auto_cutover"] != null)
        body.auto_cutover = input["auto_cutover"];
      if (input["auto_delete_branch"] != null)
        body.auto_delete_branch = input["auto_delete_branch"];

      const dr = await createDeployRequest(
        organization,
        database,
        body,
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
