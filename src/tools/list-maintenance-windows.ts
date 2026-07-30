import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { PlanetScaleAPIError, apiRequest } from "../lib/planetscale-api.ts";
import { getAuthToken, getAuthHeader } from "../lib/auth.ts";

interface Actor {
  id: string;
  type: string;
  display_name: string;
  avatar_url: string;
}

interface DatabaseBranch {
  id: string;
  type: string;
  name: string;
  created_at: string;
  updated_at: string;
}

interface MaintenanceSchedule {
  id: string;
  type: string;
  name: string;
  created_at: string;
  updated_at: string;
  last_window_datetime: string | null;
  next_window_datetime: string | null;
  duration: number;
  day: number;
  hour: number;
  week: number | null;
  frequency_value: number;
  frequency_unit: "day" | "week" | "month" | "once";
  enabled: boolean;
  expires_at: string | null;
  deadline_at: string | null;
  required: boolean;
  pending_vitess_version_update: boolean;
  pending_vitess_version: string | null;
  actor: Actor;
  database_branch: DatabaseBranch;
}

interface MaintenanceWindow {
  id: string;
  type: string;
  created_at: string;
  updated_at: string;
  started_at: string;
  finished_at: string | null;
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

const DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Every day"];

function formatSchedule(s: MaintenanceSchedule) {
  const day = DAY_NAMES[s.day] ?? `day ${s.day}`;
  const time = `${String(s.hour).padStart(2, "0")}:00 UTC`;
  const freq =
    s.frequency_unit === "once"
      ? "one-time"
      : `every ${s.frequency_value > 1 ? `${s.frequency_value} ${s.frequency_unit}s` : s.frequency_unit}`;

  return {
    id: s.id,
    name: s.name,
    branch: s.database_branch.name,
    schedule: `${freq}, ${day} at ${time}, ${s.duration}h window`,
    enabled: s.enabled,
    required: s.required,
    next_window: s.next_window_datetime,
    last_window: s.last_window_datetime,
    pending_vitess_update: s.pending_vitess_version_update
      ? s.pending_vitess_version
      : null,
  };
}

export const listMaintenanceWindowsGram = new Gram().tool({
  name: "list_maintenance_windows",
  description:
    "Vitess/MySQL databases only. List maintenance schedules and their recent windows for a PlanetScale database. Shows when maintenance is scheduled (day, time, frequency, duration), whether a Vitess version update is pending, and the history of recent maintenance windows with start/finish times. Useful for understanding when a database was or will be under maintenance.",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    include_windows: z
      .boolean()
      .optional()
      .describe(
        "Fetch recent maintenance windows for each schedule (default: false). Shows the start/finish times of past maintenance runs.",
      ),
    windows_per_schedule: z
      .number()
      .optional()
      .describe(
        "Number of recent windows to fetch per schedule when include_windows is true (default: 5, max: 25).",
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

      const schedules = await apiRequest<PaginatedList<MaintenanceSchedule>>(
        `/organizations/${e(organization)}/databases/${e(database)}/maintenance-schedules`,
        authHeader,
      );

      if (schedules.data.length === 0) {
        return ctx.json({
          organization,
          database,
          schedules: [],
          message: "No maintenance schedules found for this database.",
        });
      }

      const formatted = schedules.data.map(formatSchedule);

      if (!input.include_windows) {
        return ctx.json({ organization, database, schedules: formatted });
      }

      const perPage = Math.min(input.windows_per_schedule ?? 5, 25);

      const windowResults = await Promise.allSettled(
        schedules.data.map((s) =>
          apiRequest<PaginatedList<MaintenanceWindow>>(
            `/organizations/${e(organization)}/databases/${e(database)}/maintenance-schedules/${e(s.id)}/windows?per_page=${perPage}`,
            authHeader,
          ).then((res) => ({
            schedule_id: s.id,
            windows: res.data.map((w) => ({
              started_at: w.started_at,
              finished_at: w.finished_at,
            })),
          })),
        ),
      );

      const windowsBySchedule = new Map<string, { started_at: string; finished_at: string | null }[]>();
      for (const r of windowResults) {
        if (r.status === "fulfilled") {
          windowsBySchedule.set(r.value.schedule_id, r.value.windows);
        }
      }

      const schedulesWithWindows = formatted.map((s) => ({
        ...s,
        recent_windows: windowsBySchedule.get(s.id) ?? [],
      }));

      return ctx.json({ organization, database, schedules: schedulesWithWindows });
    } catch (error) {
      if (error instanceof PlanetScaleAPIError) {
        if (error.statusCode === 404) {
          return ctx.text(
            "Error: Not found. Check that the organization and database names are correct, and that the database is a Vitess/MySQL database (maintenance schedules are not available for Postgres). (status: 404)",
          );
        }
        return ctx.text(`Error: ${error.message} (status: ${error.statusCode})`);
      }
      if (error instanceof Error) {
        return ctx.text(`Error: ${error.message}`);
      }
      return ctx.text("Error: An unexpected error occurred");
    }
  },
});
