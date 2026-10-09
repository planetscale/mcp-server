import assert from "node:assert/strict";
import test from "node:test";
import {
  alignSeries,
  capSeries,
  explainUnknownMetrics,
  metricUnit,
  planMetricRequests,
  rollupSummarySeries,
  roundMetricValue,
  seriesIdentity,
  suggestMetricName,
  summarizePoints,
} from "./get-branch-metrics.ts";

test("summarizePoints reports latest, min, avg, and max and skips non-finite samples", () => {
  assert.deepEqual(
    summarizePoints([
      [1, 1],
      [2, Number.NaN],
      [3, Number.POSITIVE_INFINITY],
      [4],
      [5, 3],
      [6, 5],
    ]),
    { latest: 5, min: 1, avg: 3, max: 5 },
  );
});

test("summarizePoints returns null when every sample is unusable", () => {
  assert.equal(summarizePoints([[1], [2, Number.NaN]]), null);
  assert.equal(summarizePoints([]), null);
});

test("capSeries keeps forty series and flags the rest", () => {
  const rows = Array.from({ length: 41 }, (_, index) => index);
  const capped = capSeries(rows);
  assert.equal(capped.truncated, true);
  assert.equal(capped.series.length, 40);
  assert.equal(capped.series[0], 0);
  assert.equal(capped.series[39], 39);

  const exact = capSeries(rows.slice(0, 40));
  assert.equal(exact.truncated, false);
  assert.equal(exact.series.length, 40);
});

test("metricUnit matches tablet and byte metrics", () => {
  assert.equal(metricUnit("primary_cpu_usage"), "%");
  assert.equal(metricUnit("replication_lag"), "seconds");
  assert.equal(metricUnit("shard_storage_usage"), "%");
  assert.equal(metricUnit("shard_storage_available"), "bytes");
  assert.equal(metricUnit("planetscale_edge_bytes_sent_rate"), "bytes/second");
  assert.equal(metricUnit("planetscale_neki_router_query_latency_p99"), "ms");
  assert.equal(metricUnit("primary_iops"), "operations/second");
  assert.equal(metricUnit("planetscale_postgres_connection_state"), "connections");
});

test("postgres summary uses branch series and instant metrics", () => {
  const plan = planMetricRequests("postgresql", "summary");
  assert.ok(!("error" in plan));
  assert.deepEqual(
    plan.requests.map((request) => request.endpoint),
    ["metrics", "metrics/instant"],
  );
  assert.deepEqual(plan.requests[0]?.metrics, [
    "planetscale_primary_pods_cpu_util_percentages",
    "planetscale_primary_pods_mem_util_percentages",
    "planetscale_primary_memory_rss_bytes",
    "planetscale_primary_memory_active_cache_bytes",
    "planetscale_volume_usage_percentages",
    "planetscale_replica_lag_seconds",
    "planetscale_pods_container_ooms",
  ]);
  assert.ok(plan.requests[1]?.metrics.includes("planetscale_volume_capacity_bytes"));
  assert.ok(
    plan.requests[1]?.metrics.includes("planetscale_postgres_settings_max_connections"),
  );
  assert.ok(!plan.requests[0]?.metrics.includes("queries"));
});

test("neki summary includes router metrics and shard volume", () => {
  const plan = planMetricRequests("neki", "summary");
  assert.ok(!("error" in plan));
  assert.deepEqual(
    plan.requests.map((request) => request.endpoint),
    ["metrics", "metrics/instant"],
  );
  assert.ok(
    plan.requests[0]?.metrics.includes("planetscale_neki_router_query_latency_p99"),
  );
  assert.ok(
    plan.requests[0]?.metrics.includes("planetscale_primary_volume_usage_percentages"),
  );
  assert.ok(plan.requests[0]?.metrics.includes("planetscale_primary_memory_rss_bytes"));
  assert.deepEqual(plan.requests[1]?.metrics, [
    "planetscale_volume_usage_percentage",
    "planetscale_volume_disk_usage_bytes",
    "planetscale_volume_capacity_bytes",
    "planetscale_postgres_connection_state",
    "planetscale_postgres_settings_max_connections",
  ]);
});

test("vitess summary uses tablet series and tablet instant metrics", () => {
  const plan = planMetricRequests("mysql", "summary");
  assert.ok(!("error" in plan));
  assert.deepEqual(
    plan.requests.map((request) => request.endpoint),
    ["metrics/tablets", "metrics/tablets-instant"],
  );
  assert.deepEqual(plan.requests[0]?.metrics, [
    "primary_cpu_usage",
    "primary_memory_usage",
    "replication_lag",
    "primary_iops",
    "shard_storage_usage",
  ]);
  assert.ok(plan.requests[1]?.metrics.includes("vtgate_cpu_usage"));
  assert.ok(plan.requests[1]?.metrics.includes("vtgate_memory_usage"));
  assert.ok(!plan.requests[1]?.metrics.includes("primary_iops"));
});

test("vitess series splits tablet metrics from branch metrics", () => {
  const plan = planMetricRequests("mysql", "series", [
    "primary_cpu_usage",
    "vtgate_cpu_by_az",
  ]);
  assert.ok(!("error" in plan));
  assert.deepEqual(plan.requests, [
    { endpoint: "metrics/tablets", metrics: ["primary_cpu_usage"] },
    { endpoint: "metrics", metrics: ["vtgate_cpu_by_az"] },
  ]);
});

test("vitess instant rejects branch metrics and planetscale names", () => {
  const branch = planMetricRequests("mysql", "instant", ["connections"]);
  assert.ok("error" in branch);
  assert.match(branch.error, /no branch instant metrics/);

  const planetscale = planMetricRequests("mysql", "series", [
    "planetscale_volume_usage_percentage",
  ]);
  assert.ok("error" in planetscale);
  assert.match(planetscale.error, /Postgres or Neki/);
});

test("postgres and neki reject tablet metric names", () => {
  for (const kind of ["postgresql", "neki"] as const) {
    const plan = planMetricRequests(kind, "series", ["primary_cpu_usage"]);
    assert.ok("error" in plan);
    assert.match(plan.error, /planetscale_primary_pods_cpu_util_percentages/);
  }
});

test("series and instant require metrics, and summary rejects them", () => {
  const series = planMetricRequests("postgresql", "series");
  assert.ok("error" in series);
  assert.match(series.error, /metrics is required/);

  const summary = planMetricRequests("postgresql", "summary", ["queries"]);
  assert.ok("error" in summary);
  assert.match(summary.error, /does not take metrics/);
});

test("workflow is only valid for vreplication_lag on Vitess", () => {
  const ok = planMetricRequests("mysql", "series", ["vreplication_lag"], "wf_123");
  assert.ok(!("error" in ok));
  assert.deepEqual(ok.requests, [
    { endpoint: "metrics/tablets", metrics: ["vreplication_lag"] },
  ]);

  const mixed = planMetricRequests(
    "mysql",
    "series",
    ["vreplication_lag", "primary_cpu_usage"],
    "wf_123",
  );
  assert.ok("error" in mixed);
  assert.match(mixed.error, /vreplication_lag/);

  const postgres = planMetricRequests("postgresql", "series", ["vreplication_lag"], "wf_123");
  assert.ok("error" in postgres);
});

test("roundMetricValue keeps four significant figures", () => {
  assert.equal(roundMetricValue(16.8542285714), 16.85);
  assert.equal(roundMetricValue(5.70648193359375), 5.706);
  assert.equal(roundMetricValue(2343.983333), 2344);
  assert.equal(roundMetricValue(0), 0);
  assert.equal(roundMetricValue(100), 100);
});

test("seriesIdentity moves an availability zone out of the title", () => {
  assert.deepEqual(
    seriesIdentity("vtgate_cpu_by_az", "us-east-1a", {}, {
      label: "VTGate maximum CPU utilization",
      dimensions: ["availability_zone"],
    }),
    {
      label: "VTGate maximum CPU utilization",
      labels: { availability_zone: "us-east-1a" },
    },
  );
});

test("seriesIdentity keeps shard storage identity and treats it as a percent elsewhere", () => {
  assert.deepEqual(seriesIdentity("shard_storage_usage", "-", {}), {
    label: "Shard storage used",
    labels: { shard: "-" },
  });
});

test("rollupSummarySeries drops pod names and keeps the busiest row", () => {
  const rolled = rollupSummarySeries([
    {
      metric: "planetscale_replica_lag_seconds",
      label: "Replica lag",
      unit: "seconds",
      labels: { shard: "sh1", pod: "nkt-a", cell: "a" },
      latest: 0,
      min: 0,
      avg: 0,
      max: 0,
    },
    {
      metric: "planetscale_replica_lag_seconds",
      label: "Replica lag",
      unit: "seconds",
      labels: { shard: "sh1", pod: "nkt-b", cell: "b" },
      latest: 4,
      min: 1,
      avg: 2,
      max: 9,
    },
  ]);
  assert.equal(rolled.length, 1);
  assert.deepEqual(rolled[0]?.labels, { shard: "sh1" });
  assert.equal(rolled[0]?.latest, 4);
  assert.equal(rolled[0]?.max, 9);
  assert.equal(rolled[0]?.series_count, 2);
  assert.equal("pod" in (rolled[0]?.labels ?? {}), false);
});

test("alignSeries emits a null row when a requested metric has no series", () => {
  const rows = alignSeries(
    ["planetscale_pods_container_ooms"],
    [],
    new Map([
      ["planetscale_pods_container_ooms", {
        name: "planetscale_pods_container_ooms",
        label: "Pod container OOMs",
        unit: "count",
        available: true,
      }],
    ]),
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0]?.metric, "planetscale_pods_container_ooms");
  assert.equal(rows[0]?.latest, null);
  assert.equal(rows[0]?.unit, "count");
});

test("unknown instant names suggest the series twin and skip the query catalog", () => {
  const message = explainUnknownMetrics(
    ["planetscale_volume_usage_percentage"],
    [
      {
        name: "planetscale_volume_usage_percentages",
        label: "Volume utilization",
        unit: "%",
        available: true,
        source: "metrics",
      },
      {
        name: "queries",
        label: "Queries",
        unit: "queries",
        available: true,
        source: "insights",
      },
    ],
    ["planetscale_volume_usage_percentage"],
  );
  assert.match(message, /current value/);
  assert.match(message, /Did you mean planetscale_volume_usage_percentages/);
  assert.equal(message.includes("queries"), false);
});

test("suggestMetricName swaps percentage and percentages", () => {
  assert.equal(
    suggestMetricName("planetscale_volume_usage_percentage", [
      "planetscale_volume_usage_percentages",
    ]),
    "planetscale_volume_usage_percentages",
  );
});
