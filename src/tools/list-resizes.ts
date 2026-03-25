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

interface KeyspaceResizeEntry {
  id: string;
  type: "KeyspaceResizeRequest";
  state: string;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
  cluster_name: string;
  previous_cluster_name: string;
  cluster_rate_display_name: string;
  previous_cluster_rate_display_name: string;
  replicas: number;
  previous_replicas: number;
  cluster_rank: number;
  previous_cluster_rank: number;
  name?: string;
  actor: Actor;
}

function summarizeKeyspaceResize(keyspace: string, entry: KeyspaceResizeEntry) {
  const changes: Record<string, { from: unknown; to: unknown }> = {};

  if (entry.cluster_rate_display_name !== entry.previous_cluster_rate_display_name) {
    changes["cluster_size"] = {
      from: entry.previous_cluster_rate_display_name,
      to: entry.cluster_rate_display_name,
    };
  }
  if (entry.replicas !== entry.previous_replicas) {
    changes["replicas"] = { from: entry.previous_replicas, to: entry.replicas };
  }
  if (entry.cluster_rank !== entry.previous_cluster_rank) {
    changes["cluster_rank"] = { from: entry.previous_cluster_rank, to: entry.cluster_rank };
  }
  if (entry.cluster_name !== entry.previous_cluster_name) {
    changes["cluster_sku"] = {
      from: entry.previous_cluster_name,
      to: entry.cluster_name,
    };
  }

  return {
    id: entry.id,
    state: entry.state,
    keyspace,
    cluster_size: entry.cluster_rate_display_name,
    created_at: entry.created_at,
    started_at: entry.started_at,
    completed_at: entry.completed_at,
    changes,
    actor: entry.actor.display_name,
  };
}

interface ShardResizeEntry {
  id: string;
  type: "VitessShardResizeRequest";
  state: string;
  key_range: string;
  cluster_name: string;
  cluster_display_name: string;
  previous_cluster_name: string;
  previous_cluster_display_name: string;
  reset: boolean;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
  updated_at: string;
  actor: Actor;
}

function summarizeShardResize(keyspace: string, entry: ShardResizeEntry) {
  const changes: Record<string, { from: unknown; to: unknown }> = {};

  if (entry.cluster_display_name !== entry.previous_cluster_display_name) {
    changes["cluster_size"] = {
      from: entry.previous_cluster_display_name,
      to: entry.cluster_display_name,
    };
  }
  if (entry.cluster_name !== entry.previous_cluster_name) {
    changes["cluster_sku"] = {
      from: entry.previous_cluster_name,
      to: entry.cluster_name,
    };
  }

  return {
    id: entry.id,
    state: entry.state,
    keyspace,
    key_range: entry.key_range,
    cluster_size: entry.cluster_display_name,
    created_at: entry.created_at,
    started_at: entry.started_at,
    completed_at: entry.completed_at,
    changes,
    actor: entry.actor.display_name,
  };
}

interface BranchKeyspace {
  name: string;
  sharded: boolean;
}

async function fetchKeyspaces(
  organization: string,
  database: string,
  branch: string,
  authHeader: string,
): Promise<PaginatedList<BranchKeyspace>> {
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/keyspaces`;

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
      `Failed to fetch keyspaces: ${response.statusText}`,
      response.status,
      details,
    );
  }

  return (await response.json()) as PaginatedList<BranchKeyspace>;
}

async function fetchShardResizes(
  organization: string,
  database: string,
  branch: string,
  keyspace: string,
  authHeader: string,
  perPage: number,
): Promise<PaginatedList<ShardResizeEntry>> {
  const params = new URLSearchParams();
  params.set("per_page", String(perPage));
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/keyspaces/${encodeURIComponent(keyspace)}/shard-resizes?${params}`;

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
      `Failed to fetch shard resizes: ${response.statusText}`,
      response.status,
      details,
    );
  }

  return (await response.json()) as PaginatedList<ShardResizeEntry>;
}

async function fetchKeyspaceResizes(
  organization: string,
  database: string,
  branch: string,
  keyspace: string,
  authHeader: string,
  perPage: number,
): Promise<PaginatedList<KeyspaceResizeEntry>> {
  const params = new URLSearchParams();
  params.set("per_page", String(perPage));
  const url = `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}/keyspaces/${encodeURIComponent(keyspace)}/resizes?${params}`;

  const response = await fetch(url, {
    method: "GET",
    headers: { Authorization: authHeader, Accept: "application/json" },
  });

  if (!response.ok) {
    let details: unknown;
    try { details = await response.json(); } catch { details = await response.text(); }
    throw new PlanetScaleAPIError(`Failed to fetch keyspace resizes: ${response.statusText}`, response.status, details);
  }

  return (await response.json()) as PaginatedList<KeyspaceResizeEntry>;
}

export const listResizesGram = new Gram().tool({
  name: "list_resizes",
  description:
    "List recent resize operations for a PlanetScale database branch. Returns VTGate (connection proxy) resizes, keyspace/VTTablet (storage compute) resizes, and individual shard resizes. Automatically discovers keyspaces and fetches per-keyspace resize history. Each resize entry shows what changed with before/after values, the current state, and who initiated it.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    type: z
      .enum(["all", "vtgate", "keyspace", "shard"])
      .optional()
      .describe(
        "Type of resizes to list: 'all' (default) returns all resize types, 'vtgate' returns only VTGate/connection proxy resizes, 'keyspace' returns only keyspace/VTTablet resizes, 'shard' returns only individual shard resizes",
      ),
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

      const resizeType = input.type ?? "all";
      const page = input.page ?? 1;
      const perPage = Math.min(input.per_page ?? 10, 25);
      const authHeader = getAuthHeader(env);
      const completedAtRange =
        input.from && input.to ? `${input.from}..${input.to}` : undefined;
      const fromTime = input.from ? new Date(input.from).getTime() : null;
      const toTime = input.to ? new Date(input.to).getTime() : null;

      const wantVtgate = resizeType === "all" || resizeType === "vtgate";
      const wantKeyspace = resizeType === "all" || resizeType === "keyspace";
      const wantShard = resizeType === "all" || resizeType === "shard";
      const needKeyspaceDiscovery = wantKeyspace || wantShard;

      // Phase 1: Fetch VTGate resizes + keyspace discovery in parallel
      const [vtgateResult, keyspaceListResult] = await Promise.allSettled([
        wantVtgate
          ? fetchBranchResizes(organization, database, branch, authHeader, page, perPage, completedAtRange)
          : Promise.resolve(null),
        needKeyspaceDiscovery
          ? fetchKeyspaces(organization, database, branch, authHeader)
          : Promise.resolve(null),
      ]);

      // Phase 2: Fan out per-keyspace resize calls
      const allKeyspaces: string[] = [];
      const shardedKeyspaces: string[] = [];
      if (needKeyspaceDiscovery && keyspaceListResult.status === "fulfilled" && keyspaceListResult.value) {
        for (const ks of keyspaceListResult.value.data) {
          allKeyspaces.push(ks.name);
          if (ks.sharded) {
            shardedKeyspaces.push(ks.name);
          }
        }
      }

      const [keyspaceResizeResults, shardResizeResults] = await Promise.all([
        Promise.allSettled(
          wantKeyspace
            ? allKeyspaces.map((ks) =>
                fetchKeyspaceResizes(organization, database, branch, ks, authHeader, perPage)
                  .then((list) => ({ keyspace: ks, list }))
              )
            : [],
        ),
        Promise.allSettled(
          wantShard
            ? shardedKeyspaces.map((ks) =>
                fetchShardResizes(organization, database, branch, ks, authHeader, perPage)
                  .then((list) => ({ keyspace: ks, list }))
              )
            : [],
        ),
      ]);

      const result: Record<string, unknown> = {
        organization,
        database,
        branch,
      };

      if (wantVtgate) {
        if (vtgateResult.status === "fulfilled" && vtgateResult.value) {
          const list = vtgateResult.value;
          result["vtgate_resizes"] = {
            total: list.data.length,
            page: list.current_page,
            next_page: list.next_page,
            resizes: list.data.map(summarizeBranchResize),
          };
        } else {
          const reason =
            vtgateResult.status === "rejected" ? vtgateResult.reason : null;
          result["vtgate_resizes"] = {
            error:
              reason instanceof PlanetScaleAPIError
                ? `${reason.message} (status: ${reason.statusCode})`
                : "Failed to fetch VTGate resizes",
          };
        }
      }

      if (wantKeyspace) {
        if (keyspaceListResult.status === "rejected") {
          result["keyspace_resizes"] = {
            error: `keyspace discovery: ${
              keyspaceListResult.reason instanceof PlanetScaleAPIError
                ? `${keyspaceListResult.reason.message} (status: ${keyspaceListResult.reason.statusCode})`
                : "Failed to fetch keyspaces"
            }`,
          };
        } else {
          const allKsResizes: ReturnType<typeof summarizeKeyspaceResize>[] = [];
          const ksErrors: string[] = [];
          for (const r of keyspaceResizeResults) {
            if (r.status === "fulfilled") {
              const { keyspace, list } = r.value;
              for (const entry of list.data) {
                if (fromTime != null && toTime != null) {
                  const at = new Date(entry.completed_at ?? entry.created_at).getTime();
                  if (at < fromTime || at > toTime) continue;
                }
                allKsResizes.push(summarizeKeyspaceResize(keyspace, entry));
              }
            } else {
              ksErrors.push(
                r.reason instanceof PlanetScaleAPIError
                  ? `${r.reason.message} (status: ${r.reason.statusCode})`
                  : "Failed to fetch keyspace resizes",
              );
            }
          }
          const ksResult: Record<string, unknown> = {
            total: allKsResizes.length,
            keyspaces_checked: allKeyspaces,
            resizes: allKsResizes,
          };
          if (ksErrors.length > 0) {
            ksResult["errors"] = ksErrors;
          }
          result["keyspace_resizes"] = ksResult;
        }
      }

      if (wantShard) {
        if (keyspaceListResult.status === "rejected") {
          result["shard_resizes"] = {
            error: `keyspace discovery: ${
              keyspaceListResult.reason instanceof PlanetScaleAPIError
                ? `${keyspaceListResult.reason.message} (status: ${keyspaceListResult.reason.statusCode})`
                : "Failed to fetch keyspaces"
            }`,
          };
        } else {
          const allShardResizes: ReturnType<typeof summarizeShardResize>[] = [];
          const shardErrors: string[] = [];
          for (const r of shardResizeResults) {
            if (r.status === "fulfilled") {
              const { keyspace, list } = r.value;
              // Client-side time filtering since the API ignores completed_at
              for (const entry of list.data) {
                if (fromTime != null && toTime != null) {
                  const at = new Date(entry.completed_at ?? entry.created_at).getTime();
                  if (at < fromTime || at > toTime) continue;
                }
                allShardResizes.push(summarizeShardResize(keyspace, entry));
              }
            } else {
              shardErrors.push(
                r.reason instanceof PlanetScaleAPIError
                  ? `${r.reason.message} (status: ${r.reason.statusCode})`
                  : "Failed to fetch shard resizes",
              );
            }
          }
          const shardResult: Record<string, unknown> = {
            total: allShardResizes.length,
            keyspaces_checked: shardedKeyspaces,
            resizes: allShardResizes,
          };
          if (shardErrors.length > 0) {
            shardResult["errors"] = shardErrors;
          }
          result["shard_resizes"] = shardResult;
        }
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
