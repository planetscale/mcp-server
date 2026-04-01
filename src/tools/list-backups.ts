import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError, apiRequest } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";
import { formatBytes } from "./list-cluster-sizes.ts";

interface BackupPolicy {
  id: string;
  type: string;
  display_name: string;
  name: string;
  target: "production" | "development";
  retention_value: number;
  retention_unit: string;
  frequency_value: number;
  frequency_unit: string;
  schedule_time: string;
  schedule_day: number | null;
  schedule_week: number | null;
  created_at: string;
  updated_at: string;
  last_ran_at: string | null;
  next_run_at: string | null;
  required: boolean;
}

interface Backup {
  id: string;
  type: string;
  name: string;
  state: "pending" | "running" | "success" | "failed" | "canceled" | "ignored";
  size: number;
  estimated_storage_cost: number;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  expires_at: string | null;
  completed_at: string | null;
  deleted_at: string | null;
  pvc_size: number;
  protected: boolean;
  required: boolean;
  backup_policy: BackupPolicy | null;
  database_branch: { id: string; name: string } | null;
  restored_branches: { id: string; name: string }[];
}

interface PaginatedList<T> {
  type: string;
  current_page: number;
  next_page: number | null;
  next_page_url: string | null;
  prev_page: number | null;
  prev_page_url: string | null;
  data: T[];
}

function formatPolicy(p: BackupPolicy) {
  const freq =
    p.frequency_unit === "hour" && p.frequency_value === 24
      ? "daily"
      : `every ${p.frequency_value > 1 ? `${p.frequency_value} ${p.frequency_unit}s` : p.frequency_unit}`;
  const retention = `${p.retention_value} ${p.retention_unit}${p.retention_value > 1 ? "s" : ""}`;

  return {
    id: p.id,
    name: p.display_name,
    target: p.target,
    schedule: `${freq} at ${p.schedule_time} UTC`,
    retention,
    required: p.required,
    last_ran_at: p.last_ran_at,
    next_run_at: p.next_run_at,
  };
}

function formatBackup(b: Backup) {
  const duration =
    b.started_at && b.completed_at
      ? `${((new Date(b.completed_at).getTime() - new Date(b.started_at).getTime()) / 60000).toFixed(0)} min`
      : null;

  return {
    id: b.id,
    name: b.name,
    state: b.state,
    size: formatBytes(b.size),
    started_at: b.started_at,
    completed_at: b.completed_at,
    duration,
    expires_at: b.expires_at,
    policy: b.backup_policy?.display_name ?? null,
    protected: b.protected,
  };
}

export const listBackupsGram = new Gram().tool({
  name: "list_backups",
  description:
    "List backup policies and recent backups for a PlanetScale database. Shows configured backup schedules (frequency, retention, next run) and recent backup history with size, duration, and state. Useful for verifying backup health, checking when the last successful backup ran, or understanding retention policies.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z
      .string()
      .optional()
      .describe(
        "Branch name to list backups for (e.g., 'main'). Required when include_backups is true.",
      ),
    include_backups: z
      .boolean()
      .optional()
      .describe(
        "Fetch recent backups for the specified branch (default: false). Requires branch to be set.",
      ),
    backup_state: z
      .enum(["pending", "running", "success", "failed", "canceled", "ignored"])
      .optional()
      .describe("Filter backups by state (default: all states)."),
    per_page: z
      .number()
      .optional()
      .describe(
        "Number of recent backups to return when include_backups is true (default: 5, max: 25).",
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

      const { organization, database } = input;
      if (!organization || !database) {
        return ctx.text("Error: organization and database are required.");
      }

      const authHeader = getAuthHeader(env);
      const e = encodeURIComponent;

      const policies = await apiRequest<PaginatedList<BackupPolicy>>(
        `/organizations/${e(organization)}/databases/${e(database)}/backup-policies`,
        authHeader,
      );

      const formattedPolicies = policies.data.map(formatPolicy);

      if (!input.include_backups) {
        return ctx.json({
          organization,
          database,
          policies: formattedPolicies,
        });
      }

      if (!input.branch) {
        return ctx.text(
          "Error: branch is required when include_backups is true.",
        );
      }

      const perPage = Math.min(input.per_page ?? 5, 25);
      const params = new URLSearchParams({ per_page: String(perPage) });
      if (input.backup_state) params.set("state", input.backup_state);

      const backups = await apiRequest<PaginatedList<Backup>>(
        `/organizations/${e(organization)}/databases/${e(database)}/branches/${e(input.branch)}/backups?${params}`,
        authHeader,
      );

      return ctx.json({
        organization,
        database,
        branch: input.branch,
        policies: formattedPolicies,
        recent_backups: backups.data.map(formatBackup),
      });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        if (error.statusCode === 404) {
          return ctx.text(
            "Error: Not found. Check that the organization, database, and branch names are correct. (status: 404)",
          );
        }
        return ctx.text(
          `Error: ${error.message} (status: ${error.statusCode})`,
        );
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
