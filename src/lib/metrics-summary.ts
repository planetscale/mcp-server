import type { MetricSeries, TimeSeries } from "./planetscale-api.ts";

export type MetricUnit =
  | "number"
  | "bytes"
  | "bytes_per_second"
  | "percent"
  | "milliseconds"
  | "seconds";

export const METRIC_PERIODS = [
  "15m",
  "1h",
  "3h",
  "6h",
  "12h",
  "1d",
  "2d",
  "7d",
  "8d",
] as const;

export type MetricPeriod = (typeof METRIC_PERIODS)[number];

export interface SeriesSummary {
  metric: string;
  label: string;
  labels: Record<string, string>;
  latest: number | null;
  min: number | null;
  avg: number | null;
  max: number | null;
  point_count: number;
  unit: MetricUnit;
  points?: number[][];
}

export interface MetricSeriesSummary {
  start_date: string;
  end_date: string;
  interval: number;
  series: SeriesSummary[];
}

export function metricUnit(metric: string): MetricUnit {
  const lower = metric.toLowerCase();
  if (lower.includes("bytes") || lower.includes("storage")) {
    return lower.endsWith("_rate") ? "bytes_per_second" : "bytes";
  }
  if (
    lower.includes("percent") ||
    lower.endsWith("ratio") ||
    ((lower.includes("cpu") || lower.includes("memory")) &&
      (lower.includes("usage") || lower.includes("by_az")))
  ) {
    return "percent";
  }
  if (lower.includes("latency") || lower.includes("duration_millis")) {
    return "milliseconds";
  }
  if (
    lower.includes("lag") ||
    lower.endsWith("_seconds") ||
    lower.endsWith("_age_succeeded")
  ) {
    return "seconds";
  }
  return "number";
}

export function pointValues(points: number[][] | undefined): number[] {
  const values: number[] = [];
  for (const point of points ?? []) {
    const value = point[1];
    if (point.length < 2 || value === undefined || !Number.isFinite(value)) {
      continue;
    }
    values.push(value);
  }
  return values;
}

export function valueStats(values: number[]): {
  min: number;
  avg: number;
  max: number;
} {
  let min = values[0] ?? 0;
  let max = values[0] ?? 0;
  let sum = 0;
  for (const value of values) {
    min = Math.min(min, value);
    max = Math.max(max, value);
    sum += value;
  }
  return { min, avg: sum / values.length, max };
}

export function summarizeTimeSeries(
  series: TimeSeries,
  includePoints = false
): SeriesSummary {
  const values = pointValues(series.points);
  const summary: SeriesSummary = {
    metric: series.metric,
    label: series.label ?? "",
    labels: series.labels ?? {},
    latest: null,
    min: null,
    avg: null,
    max: null,
    point_count: values.length,
    unit: metricUnit(series.metric),
  };

  if (values.length > 0) {
    const stats = valueStats(values);
    summary.latest = values[values.length - 1] ?? null;
    summary.min = stats.min;
    summary.avg = stats.avg;
    summary.max = stats.max;
  }

  if (includePoints) {
    summary.points = series.points ?? [];
  }

  return summary;
}

export function summarizeMetricSeries(
  response: MetricSeries,
  includePoints = false
): MetricSeriesSummary {
  return {
    start_date: response.start_date,
    end_date: response.end_date,
    interval: response.interval,
    series: (response.series ?? []).map((series) =>
      summarizeTimeSeries(series, includePoints)
    ),
  };
}

export function validateMetricRange(input: {
  period?: string;
  from?: string;
  to?: string;
  steps?: number;
}): string | undefined {
  const hasFrom = Boolean(input.from);
  const hasTo = Boolean(input.to);
  if (hasFrom !== hasTo) {
    return "from and to must be used together";
  }
  if (input.period && (hasFrom || hasTo)) {
    return "period cannot be combined with from/to. Use either period or from/to.";
  }
  if (input.steps !== undefined && input.steps <= 0) {
    return "steps must be greater than zero";
  }
  return undefined;
}
