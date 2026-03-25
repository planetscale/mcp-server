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

interface BranchResizeEntry {
  id: string;
  type: "BranchResizeRequest";
  state: string;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
  vtgate_size: string;
  previous_vtgate_size: string;
  vtgate_count: number;
  previous_vtgate_count: number;
  vtgate_max_count: number;
  previous_vtgate_max_count: number;
  vtgate_autoscaling: boolean;
  previous_vtgate_autoscaling: boolean;
  vtgate_target_cpu_utilization: number;
  previous_vtgate_target_cpu_utilization: number;
  vtgate_name: string;
  vtgate_display_name: string;
  previous_vtgate_name: string;
  previous_vtgate_display_name: string;
  actor: Actor;
}

interface PaginatedList<T> {
  type: "list";
  current_page: number;
  next_page: number | null;
  data: T[];
}

/**
 * Summarize a branch (VTGate) resize into the fields that actually changed.
 */
function summarizeBranchResize(entry: BranchResizeEntry) {
  const changes: Record<string, { from: unknown; to: unknown }> = {};

  if (entry.vtgate_display_name !== entry.previous_vtgate_display_name) {
    changes["vtgate_size"] = { from: entry.previous_vtgate_display_name, to: entry.vtgate_display_name };
  }
  if (entry.vtgate_count !== entry.previous_vtgate_count) {
    changes["vtgate_count"] = { from: entry.previous_vtgate_count, to: entry.vtgate_count };
  }
  if (entry.vtgate_max_count !== entry.previous_vtgate_max_count) {
    changes["vtgate_max_count"] = { from: entry.previous_vtgate_max_count, to: entry.vtgate_max_count };
  }
  if (entry.vtgate_autoscaling !== entry.previous_vtgate_autoscaling) {
    changes["vtgate_autoscaling"] = { from: entry.previous_vtgate_autoscaling, to: entry.vtgate_autoscaling };
  }
  if (entry.vtgate_target_cpu_utilization !== entry.previous_vtgate_target_cpu_utilization) {
    changes["vtgate_target_cpu_utilization"] = {
      from: entry.previous_vtgate_target_cpu_utilization,
      to: entry.vtgate_target_cpu_utilization,
    };
  }

  return {
    id: entry.id,
    state: entry.state,
    vtgate: entry.vtgate_display_name,
    created_at: entry.created_at,
    started_at: entry.started_at,
    completed_at: entry.completed_at,
    changes,
    actor: entry.actor.display_name,
  };
}

async function fetchBranchResizes(
  organization: string,
  database: string,
  branch: string,
  authHeader: string,
  page: number,
  perPage: number,
  completedAtRange?: string,
): Promise<PaginatedList<BranchResizeEntry>> {
  const params = new URLSearchParams();
  params.set("page", String(page));
  params.set("per_page", String(perPage));
  if (completedAtRange) {
    params.set("completed_at", completedAtRange);
  }
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/resizes?${params}`;

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
      `Failed to fetch branch resizes: ${response.statusText}`,
      response.status,
      details,
    );
  }

  return (await response.json()) as PaginatedList<BranchResizeEntry>;
}

export const listResizesGram = new Gram().tool({
  name: "list_resizes",
  description:
    "List recent VTGate (connection proxy) resize operations for a PlanetScale database branch. Each resize entry shows what changed (size, count, autoscaling settings) with before/after values, the current state, and who initiated it.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    from: z
      .string()
      .optional()
      .describe(
        "Start of time range to filter by completed_at (ISO 8601, e.g., '2026-03-25T00:00:00.000Z'). Must be paired with 'to'.",
      ),
    to: z
      .string()
      .optional()
      .describe(
        "End of time range to filter by completed_at (ISO 8601). Must be paired with 'from'.",
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

      const { organization, database, branch } = input;
      if (!organization || !database || !branch) {
        return ctx.text("Error: organization, database, and branch are required.");
      }

      const page = input.page ?? 1;
      const perPage = Math.min(input.per_page ?? 10, 25);
      const authHeader = getAuthHeader(env);
      const completedAtRange =
        input.from && input.to ? `${input.from}..${input.to}` : undefined;

      const list = await fetchBranchResizes(
        organization, database, branch, authHeader, page, perPage, completedAtRange,
      );

      return ctx.json({
        organization,
        database,
        branch,
        total: list.data.length,
        page: list.current_page,
        next_page: list.next_page,
        resizes: list.data.map(summarizeBranchResize),
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
