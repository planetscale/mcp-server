import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

const API_BASE = "https://api.planetscale.com/v1";

/**
 * Build a PlanetScale range filter string: "start..end"
 */
function buildRange(from: string, to: string): string {
  return `${from}..${to}`;
}

// ── Shared types ──────────────────────────────────────────────────────

interface Actor {
  id: string;
  type: string;
  display_name: string;
  avatar_url: string;
}

interface TimelineEvent {
  type: string;
  at: string;
  summary: string;
}

// ── Branch resizes ────────────────────────────────────────────────────

interface BranchResizeEntry {
  id: string;
  state: string;
  completed_at: string | null;
  created_at: string;
  vtgate_display_name: string;
  actor: Actor;
}

function branchResizeToEvent(entry: BranchResizeEntry): TimelineEvent {
  return {
    type: "vtgate_resize",
    at: entry.completed_at ?? entry.created_at,
    summary: `VTGate resize to ${entry.vtgate_display_name} by ${entry.actor.display_name} (${entry.state})`,
  };
}

// ── Deploy requests ───────────────────────────────────────────────────

interface DeployRequest {
  number: number;
  deployment_state: string;
  deployed_at: string | null;
  created_at: string;
  actor: Actor;
}

function deployRequestToEvent(entry: DeployRequest): TimelineEvent {
  return {
    type: "deploy_request",
    at: entry.deployed_at ?? entry.created_at,
    summary: `Deploy #${entry.number} (${entry.deployment_state}) by ${entry.actor.display_name}`,
  };
}

// ── Fetch helpers ─────────────────────────────────────────────────────

interface PaginatedList<T> {
  data: T[];
  next_page: number | null;
}

async function apiFetch<T>(url: string, authHeader: string, label: string): Promise<T> {
  const response = await fetch(url, {
    method: "GET",
    headers: { Authorization: authHeader, Accept: "application/json" },
  });

  if (!response.ok) {
    let details: unknown;
    try {
      details = await response.json();
    } catch {
      details = await response.text();
    }
    throw new PlanetScaleAPIError(
      `Failed to fetch ${label}: ${response.statusText}`,
      response.status,
      details,
    );
  }

  return (await response.json()) as T;
}

function e(s: string): string {
  return encodeURIComponent(s);
}

function buildBranchResizesUrl(
  org: string, db: string, branch: string, range: string,
): string {
  const params = new URLSearchParams();
  params.set("completed_at", range);
  params.set("per_page", "25");
  return `${API_BASE}/organizations/${e(org)}/databases/${e(db)}/branches/${e(branch)}/resizes?${params}`;
}

function buildDeployRequestsUrl(
  org: string, db: string, branch: string, range: string,
): string {
  const params = new URLSearchParams();
  params.set("deployed_at", range);
  params.set("into_branch", branch);
  params.set("per_page", "25");
  return `${API_BASE}/organizations/${e(org)}/databases/${e(db)}/deploy-requests?${params}`;
}

// ── Tool definition ───────────────────────────────────────────────────

export const getEventsGram = new Gram().tool({
  name: "get_events",
  description:
    "Get a unified chronological timeline of PlanetScale events for a database branch within a time range. Combines VTGate resizes and deploy requests (schema migrations) into a single sorted event stream. Useful for incident investigation — call with the incident time window to see everything that changed.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    from: z
      .string()
      .describe(
        "Start of time range (ISO 8601, e.g., '2026-03-25T00:00:00.000Z')",
      ),
    to: z
      .string()
      .describe(
        "End of time range (ISO 8601, e.g., '2026-03-25T23:59:00.000Z')",
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

      const { organization, database, branch, from, to } = input;
      if (!organization || !database || !branch || !from || !to) {
        return ctx.text(
          "Error: organization, database, branch, from, and to are all required.",
        );
      }

      const authHeader = getAuthHeader(env);
      const range = buildRange(from, to);

      // Fetch all event sources in parallel
      const [branchResizes, deployRequests] =
        await Promise.allSettled([
          apiFetch<PaginatedList<BranchResizeEntry>>(
            buildBranchResizesUrl(organization, database, branch, range),
            authHeader,
            "branch resizes",
          ),
          apiFetch<PaginatedList<DeployRequest>>(
            buildDeployRequestsUrl(organization, database, branch, range),
            authHeader,
            "deploy requests",
          ),
        ]);

      const events: TimelineEvent[] = [];
      const errors: string[] = [];
      const truncated: string[] = [];

      if (branchResizes.status === "fulfilled") {
        const list = branchResizes.value;
        for (const entry of list.data) {
          events.push(branchResizeToEvent(entry));
        }
        if (list.next_page != null) {
          truncated.push("vtgate_resizes");
        }
      } else {
        errors.push(`branch resizes: ${formatError(branchResizes.reason)}`);
      }

      if (deployRequests.status === "fulfilled") {
        const list = deployRequests.value;
        for (const entry of list.data) {
          events.push(deployRequestToEvent(entry));
        }
        if (list.next_page != null) {
          truncated.push("deploy_requests");
        }
      } else {
        errors.push(`deploy requests: ${formatError(deployRequests.reason)}`);
      }

      // Sort chronologically
      events.sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime());

      const result: Record<string, unknown> = {
        organization,
        database,
        branch,
        from,
        to,
        total_events: events.length,
        events,
      };

      if (truncated.length > 0) {
        result["truncated_sources"] = truncated;
        result["warning"] = `Some sources had more than 25 results and were truncated: ${truncated.join(", ")}. Narrow the time range for complete results.`;
      }

      if (errors.length > 0) {
        result["errors"] = errors;
      }

      return ctx.json(result);
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

function formatError(reason: unknown): string {
  if (reason instanceof PlanetScaleAPIError) {
    return `${reason.message} (status: ${reason.statusCode})`;
  }
  if (reason instanceof Error) {
    return reason.message;
  }
  return "unknown error";
}
