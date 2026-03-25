import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

interface Actor {
  id: string;
  type: string;
  display_name: string;
  avatar_url: string;
}

interface DeployOperation {
  id: string;
  type: string;
  state: string;
  keyspace_name: string;
  table_name: string;
  operation_name: string;
  ddl_statement: string;
  eta_seconds: number | null;
  progress_percentage: number | null;
  can_drop_data: boolean;
  created_at: string;
  updated_at: string;
}

interface Deployment {
  id: string;
  type: string;
  state: string;
  into_branch: string;
  deploy_request_number: number;
  auto_cutover: boolean;
  deploy_operations: DeployOperation[];
  created_at: string;
  finished_at: string | null;
  cutover_at: string | null;
}

interface DeployRequest {
  id: string;
  type: string;
  number: number;
  state: string;
  deployment_state: string;
  branch: string;
  branch_id: string;
  into_branch: string;
  approved: boolean;
  actor: Actor;
  closed_by: Actor | null;
  deployment: Deployment;
  created_at: string;
  updated_at: string;
  closed_at: string | null;
  deployed_at: string | null;
  html_url: string;
}

interface PaginatedList<T> {
  type: "list";
  current_page: number;
  next_page: number | null;
  data: T[];
}

function summarizeDeployRequest(entry: DeployRequest) {
  const ops = entry.deployment.deploy_operations.map((op) => {
    const summary: Record<string, unknown> = {
      keyspace: op.keyspace_name,
      table: op.table_name,
      operation: op.operation_name,
      ddl: op.ddl_statement,
      state: op.state,
    };
    if (op.progress_percentage != null) {
      summary["progress_pct"] = op.progress_percentage;
    }
    if (op.eta_seconds != null && op.eta_seconds > 0) {
      summary["eta_seconds"] = op.eta_seconds;
    }
    if (op.can_drop_data) {
      summary["can_drop_data"] = true;
    }
    return summary;
  });

  return {
    number: entry.number,
    state: entry.state,
    deployment_state: entry.deployment_state,
    branch: entry.branch,
    into_branch: entry.into_branch,
    actor: entry.actor.display_name,
    auto_cutover: entry.deployment.auto_cutover,
    created_at: entry.created_at,
    deployed_at: entry.deployed_at,
    closed_at: entry.closed_at,
    operations: ops,
  };
}

/**
 * Build a PlanetScale range filter string: "start..end"
 */
function buildRangeFilter(from: string, to: string): string {
  return `${from}..${to}`;
}

async function fetchDeployRequests(
  organization: string,
  database: string,
  authHeader: string,
  options: {
    intoBranch?: string;
    state?: string;
    deployedAtFrom?: string;
    deployedAtTo?: string;
    page: number;
    perPage: number;
  },
): Promise<PaginatedList<DeployRequest>> {
  const params = new URLSearchParams();
  params.set("page", String(options.page));
  params.set("per_page", String(options.perPage));
  if (options.intoBranch) {
    params.set("into_branch", options.intoBranch);
  }
  if (options.state) {
    params.set("state", options.state);
  }
  if (options.deployedAtFrom && options.deployedAtTo) {
    params.set("deployed_at", buildRangeFilter(options.deployedAtFrom, options.deployedAtTo));
  }

  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/deploy-requests?${params}`;

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
    throw new PlanetScaleAPIError(
      `Failed to fetch deploy requests: ${response.statusText}`,
      response.status,
      details,
    );
  }

  return (await response.json()) as PaginatedList<DeployRequest>;
}

export const listDeployRequestsGram = new Gram().tool({
  name: "list_deploy_requests",
  description:
    "List deploy requests (schema migrations) for a PlanetScale database. Deploy requests show DDL operations (ALTER TABLE, CREATE INDEX, etc.) and their progress.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    into_branch: z
      .string()
      .optional()
      .describe("Filter by target branch (e.g., 'main')"),
    state: z
      .enum(["open", "closed"])
      .optional()
      .describe("Filter by deploy request state: 'open' or 'closed'"),
    from: z
      .string()
      .optional()
      .describe(
        "Start of time range (ISO 8601, e.g., '2026-03-25T00:00:00.000Z'). Filters deploy requests by deployed_at. Must be paired with 'to'.",
      ),
    to: z
      .string()
      .optional()
      .describe(
        "End of time range (ISO 8601, e.g., '2026-03-25T23:59:00.000Z'). Must be paired with 'from'.",
      ),
    page: z.number().optional().describe("Page number (default: 1)"),
    per_page: z
      .number()
      .optional()
      .describe("Results per page (default: 10, max: 25)"),
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

      const { organization, database } = input;
      if (!organization || !database) {
        return ctx.text("Error: organization and database are required.");
      }

      const page = input.page ?? 1;
      const perPage = Math.min(input.per_page ?? 10, 25);
      const authHeader = getAuthHeader(env);

      const list = await fetchDeployRequests(organization, database, authHeader, {
        intoBranch: input.into_branch,
        state: input.state,
        deployedAtFrom: input.from,
        deployedAtTo: input.to,
        page,
        perPage,
      });

      return ctx.json({
        organization,
        database,
        total: list.data.length,
        page: list.current_page,
        next_page: list.next_page,
        requests: list.data.map(summarizeDeployRequest),
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
