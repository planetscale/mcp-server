import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import {
  PlanetScaleAPIError,
  getDatabase,
  getInstantMetrics,
  getMetricSeries,
} from "../lib/planetscale-api.ts";
import { getAuthHeader, getAuthToken } from "../lib/auth.ts";
import { reportSectionsForEngine } from "../lib/metrics-report-catalog.ts";
import {
  METRIC_PERIODS,
  summarizeMetricSeries,
  validateMetricRange,
} from "../lib/metrics-summary.ts";

const METRIC_MODES = ["report", "series", "instant"] as const;

type MetricMode = (typeof METRIC_MODES)[number];

export const getMetricsGram = new Gram().tool({
  name: "get_metrics",
  description:
    "Get performance metrics for a PlanetScale database branch. By default (mode 'report'), detects MySQL vs PostgreSQL and returns a curated overview matching the PlanetScale CLI metrics report: workload, latency, efficiency, storage, and (for Postgres) current connection/storage capacity and backup activity. Set mode to 'series' to query named historical metrics (e.g. queries, latency_p99, connections, planetscale_pods_cpu_util_percentages, planetscale_volume_usage_percentages). Set mode to 'instant' for current gauges (e.g. planetscale_volume_usage_percentage, PgBouncer connection metrics). Series values are summarized (latest, min, avg, max) unless include_points is true. Note: series storage uses planetscale_volume_usage_percentages (plural); instant storage uses planetscale_volume_usage_percentage (singular).",
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    mode: z
      .enum(METRIC_MODES)
      .optional()
      .describe(
        "What to fetch: 'report' (default) returns a curated engine-aware overview; 'series' fetches historical time series for the given metric names; 'instant' fetches current gauge values for the given metric names. metric is required for series and instant, ignored for report."
      ),
    metric: z
      .array(z.string())
      .optional()
      .describe(
        "Metric names to query. Required for mode 'series' and 'instant' (e.g. ['queries', 'latency_p99'] or ['planetscale_volume_usage_percentage']). Ignored in report mode."
      ),
    period: z
      .enum(METRIC_PERIODS)
      .optional()
      .describe(
        "Named time period. Valid values: '15m', '1h', '3h', '6h', '12h', '1d', '2d', '7d', '8d'. Defaults to '1d' in report mode and to 12h on the API in series mode when omitted. Cannot be combined with from/to. Ignored in instant mode."
      ),
    from: z
      .string()
      .optional()
      .describe(
        "Start of a custom time range as an ISO 8601 timestamp. Must be used with to. Cannot be combined with period. Ignored in instant mode."
      ),
    to: z
      .string()
      .optional()
      .describe(
        "End of a custom time range as an ISO 8601 timestamp. Must be used with from. Cannot be combined with period. Ignored in instant mode."
      ),
    steps: z
      .number()
      .optional()
      .describe(
        "Requested number of historical data points. Ignored in instant mode."
      ),
    tablet_type: z
      .string()
      .optional()
      .describe("Filter by tablet type. Series mode only."),
    keyspace: z.string().optional().describe("Filter by keyspace. Series mode only."),
    shard: z
      .string()
      .optional()
      .describe("Filter by shard. Series and instant modes only."),
    role: z
      .string()
      .optional()
      .describe("Filter by Postgres role. Series and instant modes only."),
    container: z
      .string()
      .optional()
      .describe("Filter by container. Series and instant modes only."),
    pod: z
      .string()
      .optional()
      .describe("Filter by one pod. Series and instant modes only."),
    pods: z.array(z.string()).optional().describe("Filter by pods. Series mode only."),
    query_id: z
      .array(z.string())
      .optional()
      .describe("Filter by query pattern ID. Series mode only."),
    fingerprint: z
      .string()
      .optional()
      .describe("Filter by query fingerprint. Series mode only."),
    budget_id: z
      .string()
      .optional()
      .describe("Filter by traffic budget ID. Series mode only."),
    rule_id: z
      .string()
      .optional()
      .describe("Filter by traffic rule ID. Series mode only."),
    query: z.string().optional().describe("Filter by search terms. Series mode only."),
    include_points: z
      .boolean()
      .optional()
      .describe(
        "Include raw [timestamp, value] samples for each series. Defaults to false. Series mode only."
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

      const mode: MetricMode = input["mode"] ?? "report";
      const authHeader = getAuthHeader(env);

      if (mode === "report") {
        const rangeError = validateMetricRange({
          period: input["period"],
          from: input["from"],
          to: input["to"],
          steps: input["steps"],
        });
        if (rangeError) {
          return ctx.text(`Error: ${rangeError}`);
        }
        return ctx.json(
          await fetchReport(organization, database, branch, input, authHeader)
        );
      }

      const metrics = input["metric"];
      if (!metrics || metrics.length === 0) {
        return ctx.text(
          `Error: metric is required when mode is '${mode}'`
        );
      }

      if (mode === "instant") {
        const result = await getInstantMetrics(
          {
            organization,
            database,
            branch,
            metrics,
            role: input["role"],
            shard: input["shard"],
            container: input["container"],
            pod: input["pod"],
          },
          authHeader
        );
        return ctx.json({
          mode,
          organization,
          database,
          branch,
          branch_info: result.branch ?? {},
          metrics: result.metrics ?? [],
        });
      }

      const rangeError = validateMetricRange({
        period: input["period"],
        from: input["from"],
        to: input["to"],
        steps: input["steps"],
      });
      if (rangeError) {
        return ctx.text(`Error: ${rangeError}`);
      }

      const result = await getMetricSeries(
        {
          organization,
          database,
          branch,
          metrics,
          period: input["period"],
          from: input["from"],
          to: input["to"],
          steps: input["steps"],
          tablet_type: input["tablet_type"],
          keyspace: input["keyspace"],
          shard: input["shard"],
          role: input["role"],
          container: input["container"],
          pod: input["pod"],
          pods: input["pods"],
          query_ids: input["query_id"],
          fingerprint: input["fingerprint"],
          budget_id: input["budget_id"],
          rule_id: input["rule_id"],
          q: input["query"],
        },
        authHeader
      );

      const summary = summarizeMetricSeries(
        result,
        input["include_points"] ?? false
      );

      return ctx.json({
        mode,
        organization,
        database,
        branch,
        ...summary,
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

async function fetchReport(
  organization: string,
  database: string,
  branch: string,
  input: {
    period?: string;
    from?: string;
    to?: string;
    steps?: number;
  },
  authHeader: string
) {
  const db = await getDatabase(organization, database, authHeader);
  const definitions = reportSectionsForEngine(db.kind);
  const period = input.from ? undefined : (input.period ?? "1d");
  const from = input.from;
  const to = input.to;
  const steps = input.steps;

  const sections = await Promise.all(
    definitions.map(async (definition) => {
      if (definition.kind === "series") {
        const result = await getMetricSeries(
          {
            organization,
            database,
            branch,
            metrics: definition.metrics,
            period,
            from,
            to,
            steps,
          },
          authHeader
        );
        const summary = summarizeMetricSeries(result);
        return {
          name: definition.name,
          kind: definition.kind,
          start_date: summary.start_date,
          end_date: summary.end_date,
          interval: summary.interval,
          series: summary.series,
        };
      }

      const result = await getInstantMetrics(
        {
          organization,
          database,
          branch,
          metrics: definition.metrics,
        },
        authHeader
      );
      return {
        name: definition.name,
        kind: definition.kind,
        metrics: result.metrics ?? [],
      };
    })
  );

  return {
    mode: "report" as const,
    type: "MetricsReport",
    organization,
    database,
    branch,
    engine: db.kind,
    period: period ?? "",
    from: from ?? "",
    to: to ?? "",
    steps: steps ?? 0,
    sections,
  };
}
