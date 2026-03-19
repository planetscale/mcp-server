import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface DeployRequestActor {
  display_name?: string;
}

interface DeployRequestRaw {
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
}

interface PaginatedResponse {
  data: DeployRequestRaw[];
  current_page: number;
  next_page: number | null;
  prev_page: number | null;
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
    notes: dr.notes,
    html_url: dr.html_url,
    created_at: dr.created_at,
    updated_at: dr.updated_at,
    deployed_at: dr.deployed_at,
  };
}

async function fetchDeployRequests(
  organization: string,
  database: string,
  params: {
    state?: string;
    branch?: string;
    into_branch?: string;
    page: number;
    per_page: number;
  },
  authHeader: string
): Promise<PaginatedResponse> {
  const queryParts: string[] = [
    `page=${params.page}`,
    `per_page=${params.per_page}`,
  ];
  if (params.state) queryParts.push(`state=${encodeURIComponent(params.state)}`);
  if (params.branch) queryParts.push(`branch=${encodeURIComponent(params.branch)}`);
  if (params.into_branch) queryParts.push(`into_branch=${encodeURIComponent(params.into_branch)}`);

  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/deploy-requests?${queryParts.join("&")}`;

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
      `Failed to list deploy requests: ${response.statusText}`,
      response.status,
      details
    );
  }

  return (await response.json()) as PaginatedResponse;
}

export const listDeployRequestsGram = new Gram().tool({
  name: "list_deploy_requests",
  description:
    "List deploy requests for a PlanetScale database. Deploy requests are the mechanism for deploying schema changes from a development branch to a target branch. Supports filtering by state (open/closed) and by source or target branch.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    state: z
      .enum(["open", "closed"])
      .optional()
      .describe("Filter by deploy request state"),
    branch: z
      .string()
      .optional()
      .describe("Filter by source branch name"),
    into_branch: z
      .string()
      .optional()
      .describe("Filter by target branch name"),
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

      const result = await fetchDeployRequests(
        organization,
        database,
        {
          state: input["state"],
          branch: input["branch"],
          into_branch: input["into_branch"],
          page,
          per_page: perPage,
        },
        authHeader
      );

      const filtered = (result.data || []).map(filterDeployRequest);

      return ctx.json({
        organization,
        database,
        filters: {
          state: input["state"] ?? null,
          branch: input["branch"] ?? null,
          into_branch: input["into_branch"] ?? null,
        },
        page: result.current_page,
        next_page: result.next_page,
        total: filtered.length,
        deploy_requests: filtered,
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
