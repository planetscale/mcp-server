import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { getAuthHeader, getAuthToken } from "../lib/auth.ts";
import {
  getBranch,
  PlanetScaleAPIError,
  USER_AGENT,
  type DatabaseKind,
} from "../lib/planetscale-api.ts";

const API_BASE = "https://api.planetscale.com/v1";
const SERIES_CAP = 40;
const POINTS_STEPS_DEFAULT = 24;
const POINTS_STEPS_MAX = 60;

const PERIODS = ["15m", "1h", "3h", "6h", "12h", "1d", "2d", "7d", "8d"] as const;

const TABLET_SERIES_METRICS = [
  "replication_lag",
  "vreplication_lag",
  "pod_cpu_usage",
  "pod_memory_usage",
  "pod_queries",
  "pod_rows_read",
  "pod_iops",
  "pod_ooms",
  "primary_cpu_usage",
  "primary_memory_usage",
  "primary_iops",
  "primary_queries",
  "primary_rows_read",
  "primary_rows_written",
  "shard_storage_usage",
  "shard_storage_available",
] as const;

const TABLET_INSTANT_METRICS = [
  "replication_lag",
  "primary_cpu_usage",
  "primary_memory_usage",
  "vtgate_cpu_usage",
  "vtgate_memory_usage",
] as const;

const TABLET_SERIES = new Set<string>(TABLET_SERIES_METRICS);
const TABLET_INSTANT = new Set<string>(TABLET_INSTANT_METRICS);

const POSTGRES_SUMMARY_SERIES = [
  "planetscale_primary_pods_cpu_util_percentages",
  "planetscale_primary_pods_mem_util_percentages",
  "planetscale_primary_memory_rss_bytes",
  "planetscale_primary_memory_active_cache_bytes",
  "planetscale_volume_usage_percentages",
  "planetscale_replica_lag_seconds",
  "planetscale_pods_container_ooms",
];

const POSTGRES_SUMMARY_INSTANT = [
  "planetscale_volume_usage_percentage",
  "planetscale_volume_disk_usage_bytes",
  "planetscale_volume_capacity_bytes",
  "planetscale_postgres_connection_state",
  "planetscale_postgres_settings_max_connections",
];

const NEKI_SUMMARY_SERIES = [
  "planetscale_primary_pods_cpu_util_percentages",
  "planetscale_primary_pods_mem_util_percentages",
  "planetscale_primary_memory_rss_bytes",
  "planetscale_primary_memory_active_cache_bytes",
  "planetscale_primary_volume_usage_percentages",
  "planetscale_replica_lag_seconds",
  "planetscale_neki_router_queries",
  "planetscale_neki_router_query_errors",
  "planetscale_neki_router_query_latency_p99",
  "planetscale_neki_router_pods_cpu_util_percentages",
];

const NEKI_SUMMARY_INSTANT = [
  "planetscale_volume_usage_percentage",
  "planetscale_volume_disk_usage_bytes",
  "planetscale_volume_capacity_bytes",
  "planetscale_postgres_connection_state",
  "planetscale_postgres_settings_max_connections",
];

// Current-value names. They are not in /metrics/catalog, which only lists series.
const POSTGRES_INSTANT_METRICS = [
  "planetscale_backup_fetch_percent",
  "planetscale_backup_restore_active",
  "planetscale_dedicated_pgbouncer_current_connections",
  "planetscale_dedicated_pgbouncer_current_client_connections",
  "planetscale_dedicated_pgbouncer_current_server_connections",
  "planetscale_dedicated_pgbouncer_max_connections",
  "planetscale_dedicated_pgbouncer_cpu_usage",
  "planetscale_dedicated_pgbouncer_memory_usage",
  "planetscale_pgbouncer_current_client_connections",
  "planetscale_pgbouncer_current_server_connections",
  "planetscale_pgbouncer_settings_max_client_conn",
  "planetscale_postgres_connection_state",
  "planetscale_postgres_settings_max_connections",
  "planetscale_volume_disk_usage_bytes",
  "planetscale_volume_usage_percentage",
  "planetscale_volume_capacity_bytes",
] as const;

const NEKI_INSTANT_METRICS = [
  "planetscale_backup_fetch_percent",
  "planetscale_backup_restore_active",
  "planetscale_postgres_connection_state",
  "planetscale_postgres_settings_max_connections",
  "planetscale_postgres_settings_max_wal_size_bytes",
  "planetscale_volume_disk_usage_bytes",
  "planetscale_volume_usage_percentage",
  "planetscale_volume_capacity_bytes",
] as const;

const POSTGRES_INSTANT = new Set<string>(POSTGRES_INSTANT_METRICS);
const NEKI_INSTANT = new Set<string>(NEKI_INSTANT_METRICS);

const VITESS_SUMMARY_SERIES = [
  "primary_cpu_usage",
  "primary_memory_usage",
  "replication_lag",
  "primary_iops",
  "shard_storage_usage",
];

const VITESS_SUMMARY_INSTANT = [
  "primary_cpu_usage",
  "primary_memory_usage",
  "replication_lag",
  "vtgate_cpu_usage",
  "vtgate_memory_usage",
];

export type MetricsView = "summary" | "series" | "instant";

export type MetricEndpoint =
  | "metrics"
  | "metrics/instant"
  | "metrics/tablets"
  | "metrics/tablets-instant";

export interface PlannedRequest {
  endpoint: MetricEndpoint;
  metrics: string[];
}

export interface SeriesStats {
  latest: number;
  min: number;
  avg: number;
  max: number;
}

interface CatalogEntry {
  name: string;
  label: string;
  unit: string;
  available: boolean;
  source?: string;
  dimensions?: string[];
}

export interface SeriesRow {
  metric: string;
  label: string;
  unit?: string;
  labels: Record<string, string>;
  latest: number | null;
  min: number | null;
  avg: number | null;
  max: number | null;
  series_count?: number;
  points?: number[][];
}

interface MetricSeriesResponse {
  start_date?: string;
  end_date?: string;
  interval?: number;
  series?: {
    metric?: string;
    label?: string;
    labels?: Record<string, string>;
    points?: number[][];
  }[];
}

interface InstantMetricsResponse {
  metrics?: {
    metric?: string;
    label?: string;
    values?: Record<string, unknown>[];
  }[];
}

interface MetricFilters {
  period?: string;
  from?: string;
  to?: string;
  steps?: string;
  keyspace?: string;
  shard?: string;
  pod?: string;
  role?: string;
  tablet_type?: string;
  container?: string;
  workflow?: string;
}

const ENDPOINT_FILTERS: Record<MetricEndpoint, (keyof MetricFilters)[]> = {
  metrics: [
    "period",
    "from",
    "to",
    "steps",
    "keyspace",
    "shard",
    "pod",
    "role",
    "tablet_type",
    "container",
  ],
  "metrics/instant": ["role", "shard", "container", "pod"],
  "metrics/tablets": [
    "period",
    "from",
    "to",
    "steps",
    "keyspace",
    "shard",
    "pod",
    "workflow",
  ],
  "metrics/tablets-instant": ["keyspace", "shard"],
};

function uniqueMetrics(metrics: string[] | undefined): string[] {
  if (!metrics) return [];
  return [...new Set(metrics.map((name) => name.trim()).filter(Boolean))];
}

function isTabletMetric(name: string): boolean {
  return TABLET_SERIES.has(name) || TABLET_INSTANT.has(name);
}

/**
 * Decide which metrics endpoints a request needs.
 * Tablet names are Vitess-only; planetscale_* names are Postgres and Neki.
 */
export function planMetricRequests(
  kind: DatabaseKind,
  view: MetricsView,
  metrics?: string[],
  workflow?: string,
): { requests: PlannedRequest[] } | { error: string } {
  const names = uniqueMetrics(metrics);

  if (view === "summary" && names.length > 0) {
    return {
      error:
        "summary does not take metrics. Use view series or instant to choose metrics.",
    };
  }
  if (view !== "summary" && names.length === 0) {
    return { error: "metrics is required when view is series or instant." };
  }
  if (workflow && view === "summary") {
    return {
      error: "workflow only applies to the vreplication_lag tablet metric on Vitess.",
    };
  }
  if (workflow && (kind !== "mysql" || names.some((name) => name !== "vreplication_lag"))) {
    return {
      error: "workflow only applies to the vreplication_lag tablet metric on Vitess.",
    };
  }

  if (view === "summary") {
    if (kind === "mysql") {
      return {
        requests: [
          { endpoint: "metrics/tablets", metrics: VITESS_SUMMARY_SERIES },
          { endpoint: "metrics/tablets-instant", metrics: VITESS_SUMMARY_INSTANT },
        ],
      };
    }
    if (kind === "neki") {
      return {
        requests: [
          { endpoint: "metrics", metrics: NEKI_SUMMARY_SERIES },
          { endpoint: "metrics/instant", metrics: NEKI_SUMMARY_INSTANT },
        ],
      };
    }
    return {
      requests: [
        { endpoint: "metrics", metrics: POSTGRES_SUMMARY_SERIES },
        { endpoint: "metrics/instant", metrics: POSTGRES_SUMMARY_INSTANT },
      ],
    };
  }

  if (kind === "mysql") {
    const planetscale = names.filter((name) => name.startsWith("planetscale_"));
    if (planetscale.length > 0) {
      const verb = planetscale.length === 1 ? "is a Postgres or Neki metric" : "are Postgres and Neki metrics";
      return {
        error: `${planetscale.join(", ")} ${verb}. Vitess tablet metrics include primary_cpu_usage, replication_lag, pod_cpu_usage, and shard_storage_usage. shard_storage_usage is percent full, not bytes.`,
      };
    }

    if (view === "instant") {
      const unsupported = names.filter((name) => !TABLET_INSTANT.has(name));
      if (unsupported.length > 0) {
        return {
          error: `Vitess has no branch instant metrics. Instant tablet metrics are ${TABLET_INSTANT_METRICS.join(", ")}. Use view series for ${unsupported.join(", ")}.`,
        };
      }
      return { requests: [{ endpoint: "metrics/tablets-instant", metrics: names }] };
    }

    const instantOnly = names.filter(
      (name) => TABLET_INSTANT.has(name) && !TABLET_SERIES.has(name),
    );
    if (instantOnly.length > 0) {
      const verb = instantOnly.length === 1 ? "is a current tablet metric" : "are current tablet metrics";
      return {
        error: `${instantOnly.join(", ")} ${verb}. Use view instant.`,
      };
    }

    const tablet = names.filter((name) => TABLET_SERIES.has(name));
    const branch = names.filter((name) => !TABLET_SERIES.has(name));
    const requests: PlannedRequest[] = [];
    if (tablet.length > 0) requests.push({ endpoint: "metrics/tablets", metrics: tablet });
    if (branch.length > 0) requests.push({ endpoint: "metrics", metrics: branch });
    return { requests };
  }

  const tablet = names.filter(isTabletMetric);
  if (tablet.length > 0) {
    const verb = tablet.length === 1 ? "is a Vitess tablet metric" : "are Vitess tablet metrics";
    return {
      error: `${tablet.join(", ")} ${verb}. Postgres and Neki use planetscale_* metrics such as planetscale_primary_pods_cpu_util_percentages, planetscale_volume_usage_percentage, and planetscale_replica_lag_seconds.`,
    };
  }

  return {
    requests: [
      {
        endpoint: view === "instant" ? "metrics/instant" : "metrics",
        metrics: names,
      },
    ],
  };
}

/** Latest, min, average, and max. Skips missing, NaN, and infinite samples. */
export function summarizePoints(points: number[][]): SeriesStats | null {
  const values: number[] = [];
  for (const point of points) {
    if (!Array.isArray(point) || point.length < 2) continue;
    const value = point[1];
    if (typeof value !== "number" || !Number.isFinite(value)) continue;
    values.push(value);
  }
  if (values.length === 0) return null;

  let min = values[0]!;
  let max = values[0]!;
  let sum = 0;
  for (const value of values) {
    if (value < min) min = value;
    if (value > max) max = value;
    sum += value;
  }
  return {
    latest: values[values.length - 1]!,
    min,
    avg: sum / values.length,
    max,
  };
}

/**
 * Keep at most SERIES_CAP rows. Extra rows are shared across metrics so one
 * wide metric (a shard per row) cannot drop later summary metrics.
 */
export function capSeries<T extends { metric: string }>(
  series: T[],
): { series: T[]; truncated: boolean } {
  if (series.length <= SERIES_CAP) return { series, truncated: false };
  const groups = new Map<string, T[]>();
  for (const row of series) {
    const list = groups.get(row.metric) ?? [];
    list.push(row);
    groups.set(row.metric, list);
  }
  const names = [...groups.keys()];
  const kept = new Map(names.map((name) => [name, 0]));
  let remaining = SERIES_CAP;
  let progressed = true;
  while (remaining > 0 && progressed) {
    progressed = false;
    for (const name of names) {
      if (remaining === 0) break;
      const count = kept.get(name) ?? 0;
      if (count >= (groups.get(name)?.length ?? 0)) continue;
      kept.set(name, count + 1);
      remaining--;
      progressed = true;
    }
  }
  const capped: T[] = [];
  for (const name of names) {
    capped.push(...(groups.get(name) ?? []).slice(0, kept.get(name) ?? 0));
  }
  return { series: capped, truncated: true };
}

/** Unit string for tablet and other names that are not in the branch catalog. */
export function metricUnit(metric: string): string | undefined {
  const lower = metric.toLowerCase();
  // Usage is (used / capacity) * 100. Available bytes is the other storage series.
  if (lower === "shard_storage_usage") return "%";
  if (lower.includes("iops")) return "operations/second";
  if (lower.includes("connection")) return "connections";
  if (lower.includes("bytes") && lower.endsWith("_rate")) return "bytes/second";
  if (
    lower.includes("bytes") ||
    lower === "storage_per_table" ||
    lower === "shard_storage_available" ||
    lower === "planetscale_primary_storage_usage"
  ) {
    return "bytes";
  }
  if (
    lower.includes("percent") ||
    lower.includes("cpu_by_az") ||
    lower.includes("memory_by_az") ||
    lower.endsWith("cpu_usage") ||
    lower.endsWith("memory_usage") ||
    lower === "block_cache_hit_ratio"
  ) {
    return "%";
  }
  if (lower.includes("latency") || lower.includes("duration_millis")) return "ms";
  if (
    lower.includes("lag") ||
    lower.endsWith("_seconds") ||
    lower.endsWith("_age_succeeded")
  ) {
    return "seconds";
  }
  return undefined;
}

/** Four significant figures, matching the CLI's human summary. */
export function roundMetricValue(value: number): number {
  if (!Number.isFinite(value) || value === 0) return value;
  const magnitude = Math.floor(Math.log10(Math.abs(value)));
  let decimals = 4 - magnitude - 1;
  if (decimals < 0) decimals = 0;
  if (decimals > 12) decimals = 12;
  const scale = 10 ** decimals;
  return Math.round(value * scale) / scale;
}

function instantNames(kind: DatabaseKind): Set<string> {
  if (kind === "neki") return NEKI_INSTANT;
  if (kind === "postgresql") return POSTGRES_INSTANT;
  return TABLET_INSTANT;
}

function editDistance(left: string, right: string): number {
  if (Math.abs(left.length - right.length) > 3) return 4;
  const prev = Array.from({ length: right.length + 1 }, (_, index) => index);
  for (let i = 1; i <= left.length; i++) {
    let diagonal = prev[0]!;
    prev[0] = i;
    for (let j = 1; j <= right.length; j++) {
      const next = prev[j]!;
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      prev[j] = Math.min(prev[j - 1]! + 1, next + 1, diagonal + cost);
      diagonal = next;
    }
  }
  return prev[right.length]!;
}

export function suggestMetricName(name: string, candidates: string[]): string | undefined {
  const swap = name.endsWith("percentage")
    ? `${name.slice(0, -"percentage".length)}percentages`
    : name.endsWith("percentages")
      ? `${name.slice(0, -"percentages".length)}percentage`
      : undefined;
  if (swap && candidates.includes(swap)) return swap;

  let best: string | undefined;
  let bestScore = 4;
  const lower = name.toLowerCase();
  for (const candidate of candidates) {
    if (candidate === name) continue;
    const score = editDistance(lower, candidate.toLowerCase());
    if (score < bestScore) {
      best = candidate;
      bestScore = score;
    }
  }
  return best;
}

function branchBase(
  organization: string,
  database: string,
  branch: string,
): string {
  return `${API_BASE}/organizations/${encodeURIComponent(organization)}/databases/${encodeURIComponent(database)}/branches/${encodeURIComponent(branch)}`;
}

function requestUrl(
  organization: string,
  database: string,
  branch: string,
  request: PlannedRequest,
  filters: MetricFilters,
): string {
  const params = new URLSearchParams();
  params.set("metrics", request.metrics.join(","));
  for (const key of ENDPOINT_FILTERS[request.endpoint]) {
    const value = filters[key];
    if (value) params.set(key, value);
  }
  return `${branchBase(organization, database, branch)}/${request.endpoint}?${params.toString()}`;
}

async function fetchJson<T>(url: string, authHeader: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, {
    method: "GET",
    signal,
    headers: {
      Authorization: authHeader,
      Accept: "application/json",
      "User-Agent": USER_AGENT,
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
        "Resource not found. Please check your organization, database, and branch names.",
        response.status,
        details,
      );
    }
    if (response.status === 401 || response.status === 403) {
      throw new PlanetScaleAPIError(
        "Permission denied. Please check your API token has the required permissions.",
        response.status,
        details,
      );
    }
    throw new PlanetScaleAPIError(
      `API request failed: ${response.statusText}`,
      response.status,
      details,
    );
  }

  return response.json() as Promise<T>;
}

function numberOrNull(value: number | null): number | null {
  return value === null ? null : roundMetricValue(value);
}

/**
 * AZ and shard identity sometimes arrive in `label` instead of `labels`.
 * Move that identity into labels and keep the human title as `label`.
 */
export function seriesIdentity(
  metric: string,
  apiLabel: string | undefined,
  apiLabels: Record<string, string> | undefined,
  catalogEntry?: { label?: string; dimensions?: string[] },
): { label: string; labels: Record<string, string> } {
  const labels = { ...(apiLabels ?? {}) };
  const human = catalogEntry?.label;
  const identityKey = catalogEntry?.dimensions?.[0]
    ?? (metric.includes("storage") ? "shard" : undefined)
    ?? (metric.includes("by_az") ? "availability_zone" : undefined);
  if (apiLabel && identityKey && Object.keys(labels).length === 0 && apiLabel !== human) {
    labels[identityKey] = apiLabel;
    return { label: titled(metric, human || metric), labels };
  }
  return { label: titled(metric, apiLabel || human || metric), labels };
}

const FALLBACK_TITLES: Record<string, string> = {
  shard_storage_usage: "Shard storage used",
  shard_storage_available: "Shard storage available",
};

function titled(metric: string, label: string): string {
  if (label !== metric) return label;
  return FALLBACK_TITLES[metric] ?? label;
}

const SUMMARY_NOISE_LABELS = new Set(["pod", "cell"]);

/** Collapse per-pod summary rows that share a metric and the labels that matter. */
export function rollupSummarySeries(rows: SeriesRow[], preserve: string[] = []): SeriesRow[] {
  const drop = new Set([...SUMMARY_NOISE_LABELS].filter((key) => !preserve.includes(key)));
  const groups = new Map<string, SeriesRow[]>();
  for (const row of rows) {
    const labels = Object.fromEntries(
      Object.entries(row.labels)
        .filter(([key]) => !drop.has(key))
        .sort(([left], [right]) => left.localeCompare(right)),
    );
    const key = JSON.stringify([row.metric, labels]);
    const group = groups.get(key) ?? [];
    group.push({ ...row, labels });
    groups.set(key, group);
  }

  return [...groups.values()].map((group) => {
    const first = group[0]!;
    if (group.length === 1) return first;
    const numbers = (pick: (row: SeriesRow) => number | null) =>
      group.map(pick).filter((value): value is number => value !== null);
    const latest = numbers((row) => row.latest);
    const mins = numbers((row) => row.min);
    const maxes = numbers((row) => row.max);
    const avgs = numbers((row) => row.avg);
    const worst = group.reduce((best, row) =>
      (row.max ?? Number.NEGATIVE_INFINITY) > (best.max ?? Number.NEGATIVE_INFINITY) ? row : best,
    );
    return {
      ...first,
      latest: latest.length ? roundMetricValue(Math.max(...latest)) : null,
      min: mins.length ? roundMetricValue(Math.min(...mins)) : null,
      max: maxes.length ? roundMetricValue(Math.max(...maxes)) : null,
      avg: avgs.length
        ? roundMetricValue(avgs.reduce((sum, value) => sum + value, 0) / avgs.length)
        : null,
      series_count: group.length,
      ...(worst.points ? { points: worst.points } : {}),
    };
  });
}

function emptySeriesRow(metric: string, catalog: Map<string, CatalogEntry>): SeriesRow {
  const unit = unitFor(metric, catalog);
  return {
    metric,
    label: labelFor(metric, undefined, catalog),
    ...(unit ? { unit } : {}),
    labels: {},
    latest: null,
    min: null,
    avg: null,
    max: null,
  };
}

/** Keep requested order and emit a null row when the API returns no series. */
export function alignSeries(
  requested: string[],
  rows: SeriesRow[],
  catalog: Map<string, CatalogEntry>,
): SeriesRow[] {
  const byMetric = new Map<string, SeriesRow[]>();
  for (const row of rows) {
    const list = byMetric.get(row.metric) ?? [];
    list.push(row);
    byMetric.set(row.metric, list);
  }
  const aligned: SeriesRow[] = [];
  for (const metric of requested) {
    aligned.push(...(byMetric.get(metric) ?? [emptySeriesRow(metric, catalog)]));
  }
  for (const [metric, list] of byMetric) {
    if (!requested.includes(metric)) aligned.push(...list);
  }
  return aligned;
}

export function explainUnknownMetrics(
  missing: string[],
  catalog: CatalogEntry[],
  instant: readonly string[],
): string {
  const word = missing.length === 1 ? "metric" : "metrics";
  const lines = [`Unknown or unavailable ${word}: ${missing.join(", ")}.`];
  const instantSet = new Set(instant);
  const current = missing.filter((name) => instantSet.has(name));
  if (current.length > 0) {
    const verb = current.length === 1 ? "is a current value" : "are current values";
    lines.push(`${current.join(", ")} ${verb}. Use view instant.`);
  }

  const candidates = [
    ...catalog.filter((entry) => entry.available).map((entry) => entry.name),
    ...instant,
  ];
  for (const name of missing) {
    const suggestion = suggestMetricName(name, candidates);
    if (suggestion) lines.push(`Did you mean ${suggestion}?`);
  }

  const unresolved = missing.filter((name) => !instantSet.has(name));
  if (unresolved.length > 0) {
    const infra = catalog
      .filter(
        (entry) =>
          entry.available &&
          (entry.source === "metrics" ||
            entry.name.startsWith("planetscale_") ||
            entry.name.startsWith("vtgate_")),
      )
      .map((entry) => entry.name);
    if (infra.length > 0) lines.push(`Infrastructure metrics: ${infra.join(", ")}.`);
    lines.push("Query stats such as queries, latency, and rows belong on get_insights.");
  }
  return lines.join("\n");
}

function seriesNameError(
  requested: string[],
  catalog: CatalogEntry[],
  instant: readonly string[],
): string | undefined {
  const available = new Set(
    catalog.filter((entry) => entry.available).map((entry) => entry.name),
  );
  const missing = requested.filter((name) => !available.has(name));
  if (missing.length === 0) return undefined;
  return explainUnknownMetrics(missing, catalog, instant);
}

function instantNameError(
  requested: string[],
  allowed: Set<string>,
  catalog: CatalogEntry[],
): string | undefined {
  const missing = requested.filter((name) => !allowed.has(name));
  if (missing.length === 0) return undefined;
  const available = new Set(
    catalog.filter((entry) => entry.available).map((entry) => entry.name),
  );
  const series = missing.filter((name) => available.has(name));
  const unknown = missing.filter((name) => !available.has(name));
  const postgresOnly = unknown.filter((name) => POSTGRES_INSTANT.has(name) && !allowed.has(name));
  const lines: string[] = [];
  if (postgresOnly.length > 0) {
    const verb = postgresOnly.length === 1 ? "is" : "are";
    lines.push(`${postgresOnly.join(", ")} ${verb} only available on Postgres.`);
  }
  if (series.length > 0) {
    const verb = series.length === 1 ? "is a time series" : "are time series";
    lines.push(`${series.join(", ")} ${verb}. Use view series.`);
    for (const name of series) {
      const suggestion = suggestMetricName(name, [...allowed]);
      if (suggestion) lines.push(`Did you mean ${suggestion}?`);
    }
  }
  const unrecognized = unknown.filter((name) => !postgresOnly.includes(name));
  if (unrecognized.length > 0) {
    lines.push(explainUnknownMetrics(unrecognized, catalog, [...allowed]));
  }
  return lines.join("\n");
}

function unitFor(metric: string, catalog: Map<string, CatalogEntry>): string | undefined {
  return catalog.get(metric)?.unit || metricUnit(metric);
}

function labelFor(
  metric: string,
  apiLabel: string | undefined,
  catalog: Map<string, CatalogEntry>,
): string {
  if (apiLabel) return apiLabel;
  return catalog.get(metric)?.label || metric;
}

function cleanDimensions(raw: Record<string, unknown>): Record<string, unknown> {
  const dimensions: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (value !== null && value !== undefined) dimensions[key] = value;
  }
  return dimensions;
}

function roundDimensions(dimensions: Record<string, unknown>): Record<string, unknown> {
  if (typeof dimensions["value"] !== "number") return dimensions;
  return { ...dimensions, value: roundMetricValue(dimensions["value"]) };
}

function resolveSteps(
  includePoints: boolean,
  steps: number | undefined,
): number | undefined | { error: string } {
  if (!includePoints) return undefined;
  if (steps === undefined) return POINTS_STEPS_DEFAULT;
  if (!Number.isInteger(steps) || steps < 1) {
    return { error: "steps must be a positive integer." };
  }
  return Math.min(steps, POINTS_STEPS_MAX);
}

export const getBranchMetricsGram = new Gram().tool({
  name: "get_branch_metrics",
  description:
    "Get infrastructure metrics for a PlanetScale branch: CPU, memory, disk, replication lag, connections, PgBouncer, WAL, VTGate, and tablet health. The database kind selects the endpoint. Vitess uses tablet metrics; Postgres and Neki use branch metrics. view summary (the default) returns latest, min, avg, and max for a small health set plus current disk and connection values, without raw samples. Pod memory utilization includes page cache, so the summary also returns RSS and cache bytes. Vitess shard_storage_usage is percent full, not bytes. Use view series or instant and pass metrics to drill in. Series disk is planetscale_volume_usage_percentages; the current value is planetscale_volume_usage_percentage. Postgres and Neki names look like planetscale_primary_pods_cpu_util_percentages. Vitess tablet names are short, like primary_cpu_usage and replication_lag. Use get_insights for which queries are slow. Set include_points to return samples, at most 60 per series. Summary rolls per-pod series up to the busiest shard or role.",
  annotations: {
    title: "Get branch metrics",
    readOnlyHint: true,
    destructiveHint: false,
    openWorldHint: false,
  },
  inputSchema: {
    organization: z.string().describe("PlanetScale organization name"),
    database: z.string().describe("Database name"),
    branch: z.string().describe("Branch name (e.g., 'main')"),
    view: z
      .enum(["summary", "series", "instant"])
      .optional()
      .describe(
        "summary (default) is a curated health snapshot. series is historical metrics. instant is current values.",
      ),
    metrics: z
      .array(z.string())
      .optional()
      .describe(
        "Metric names. Omit for summary. Required for series and instant. Unknown names are rejected using the branch catalog instead of being dropped.",
      ),
    period: z
      .enum(PERIODS)
      .optional()
      .describe("Named time period. Defaults to 1h. Ignored when from or to is set."),
    from: z.string().optional().describe("Start of a custom range as an ISO 8601 timestamp"),
    to: z.string().optional().describe("End of a custom range as an ISO 8601 timestamp"),
    keyspace: z.string().optional().describe("Filter by keyspace"),
    shard: z.string().optional().describe("Filter by shard"),
    pod: z.string().optional().describe("Filter by pod"),
    role: z.string().optional().describe("Filter by Postgres role, such as primary or replica"),
    tablet_type: z.string().optional().describe("Filter Vitess branch series by tablet type"),
    container: z.string().optional().describe("Filter by container"),
    workflow: z
      .string()
      .optional()
      .describe("Vitess VReplication workflow id. Only valid with metric vreplication_lag."),
    include_points: z
      .boolean()
      .optional()
      .describe("Include sampled points. Defaults to false. At most 60 points per series."),
    steps: z
      .number()
      .optional()
      .describe(
        "Point count when include_points is true. Defaults to 24 and is capped at 60.",
      ),
  },
  async execute(ctx, input) {
    try {
      const env =
        Object.keys(ctx.env).length > 0
          ? (ctx.env as Record<string, string | undefined>)
          : process.env;

      if (!getAuthToken(env)) {
        return ctx.text("Error: No PlanetScale authentication configured.");
      }

      const organization = input.organization;
      const database = input.database;
      const branch = input.branch;
      if (!organization || !database || !branch) {
        return ctx.text("Error: organization, database, and branch are required.");
      }

      const view = (input.view ?? "summary") as MetricsView;
      const includePoints = input.include_points === true;
      const steps = resolveSteps(includePoints, input.steps);
      if (typeof steps === "object") return ctx.text(`Error: ${steps.error}`);

      const authHeader = getAuthHeader(env);
      // Branch kind uses read_branch, the same scope as the metrics endpoints.
      const branchInfo = await getBranch(organization, database, branch, authHeader);
      const kind = branchInfo.kind;

      const plan = planMetricRequests(kind, view, input.metrics, input.workflow);
      if ("error" in plan) return ctx.text(`Error: ${plan.error}`);

      const needsCatalog = plan.requests.some(
        (request) => request.endpoint === "metrics" || request.endpoint === "metrics/instant",
      );
      const catalogEntries = needsCatalog
        ? (
            await fetchJson<{ data?: CatalogEntry[] }>(
              `${branchBase(organization, database, branch)}/metrics/catalog`,
              authHeader,
              ctx.signal,
            )
          ).data ?? []
        : [];
      const catalog = new Map(catalogEntries.map((entry) => [entry.name, entry]));

      if (view !== "summary") {
        const allowedInstant = instantNames(kind);
        for (const request of plan.requests) {
          const message = request.endpoint === "metrics/instant"
            ? instantNameError(request.metrics, allowedInstant, catalogEntries)
            : request.endpoint === "metrics"
              ? seriesNameError(request.metrics, catalogEntries, [...allowedInstant])
              : undefined;
          if (message) return ctx.text(`Error: ${message}`);
        }
      }

      const filters: MetricFilters = {
        ...(input.from || input.to
          ? { from: input.from, to: input.to }
          : { period: input.period ?? "1h" }),
        ...(steps !== undefined ? { steps: String(steps) } : {}),
        keyspace: input.keyspace,
        shard: input.shard,
        pod: input.pod,
        role: input.role,
        tablet_type: input.tablet_type,
        container: input.container,
        workflow: input.workflow,
      };

      const payloads = await Promise.all(
        plan.requests.map((request) =>
          fetchJson<MetricSeriesResponse & InstantMetricsResponse>(
            requestUrl(organization, database, branch, request, filters),
            authHeader,
            ctx.signal,
          ),
        ),
      );

      const series: SeriesRow[] = [];
      const instant = [];
      let range: { start: string; end: string; interval_seconds: number } | undefined;
      const preserveLabels = input.pod ? ["pod"] : [];

      for (let i = 0; i < plan.requests.length; i++) {
        const request = plan.requests[i]!;
        const payload = payloads[i]!;
        if (request.endpoint === "metrics" || request.endpoint === "metrics/tablets") {
          if (!range && payload.start_date && payload.end_date && payload.interval != null) {
            range = {
              start: payload.start_date,
              end: payload.end_date,
              interval_seconds: payload.interval,
            };
          }
          let rows: SeriesRow[] = [];
          for (const item of payload.series ?? []) {
            const metric = item.metric ?? "";
            const stats = summarizePoints(item.points ?? []);
            const identity = seriesIdentity(metric, item.label, item.labels, catalog.get(metric));
            const unit = unitFor(metric, catalog);
            rows.push({
              metric,
              label: identity.label,
              ...(unit ? { unit } : {}),
              labels: identity.labels,
              latest: numberOrNull(stats?.latest ?? null),
              min: numberOrNull(stats?.min ?? null),
              avg: numberOrNull(stats?.avg ?? null),
              max: numberOrNull(stats?.max ?? null),
              ...(includePoints ? { points: item.points ?? [] } : {}),
            });
          }
          if (view === "summary") rows = rollupSummarySeries(rows, preserveLabels);
          series.push(...alignSeries(request.metrics, rows, catalog));
          continue;
        }

        const returned = new Set<string>();
        for (const item of payload.metrics ?? []) {
          const metric = item.metric ?? "";
          returned.add(metric);
          const unit = unitFor(metric, catalog);
          instant.push({
            metric,
            label: labelFor(metric, item.label, catalog),
            ...(unit ? { unit } : {}),
            values: (item.values ?? []).map((value) => roundDimensions(cleanDimensions(value))),
          });
        }
        for (const metric of request.metrics) {
          if (returned.has(metric)) continue;
          const unit = unitFor(metric, catalog);
          instant.push({
            metric,
            label: labelFor(metric, undefined, catalog),
            ...(unit ? { unit } : {}),
            values: [],
          });
        }
      }

      const capped = capSeries(series);
      return ctx.json({
        organization,
        database,
        branch,
        kind,
        view,
        ...(range ? { range } : {}),
        ...(view !== "instant" ? { series: capped.series } : {}),
        ...(view !== "series" ? { instant } : {}),
        ...(capped.truncated
          ? { truncated: true, narrow_with: ["shard", "pod", "keyspace"] }
          : {}),
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
