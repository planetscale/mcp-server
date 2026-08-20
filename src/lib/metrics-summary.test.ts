import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  metricUnit,
  pointValues,
  summarizeMetricSeries,
  validateMetricRange,
  valueStats,
} from "./metrics-summary.ts";

describe("metricUnit", () => {
  it("classifies CLI metric names", () => {
    assert.equal(metricUnit("planetscale_primary_storage_usage"), "bytes");
    assert.equal(metricUnit("storage_per_table"), "bytes");
    assert.equal(
      metricUnit("planetscale_edge_bytes_received_rate"),
      "bytes_per_second"
    );
    assert.equal(metricUnit("planetscale_edge_bytes_received"), "bytes");
    assert.equal(metricUnit("block_cache_hit_ratio"), "percent");
    assert.equal(metricUnit("vtgate_cpu_by_az"), "percent");
    assert.equal(metricUnit("vtgate_cpu_avg_by_az"), "percent");
    assert.equal(metricUnit("vtgate_memory_by_az"), "percent");
    assert.equal(metricUnit("vtgate_memory_avg_by_az"), "percent");
    assert.equal(metricUnit("planetscale_volume_usage_percentage"), "percent");
    assert.equal(metricUnit("latency_p99"), "milliseconds");
    assert.equal(metricUnit("cpu_duration_millis"), "milliseconds");
    assert.equal(metricUnit("planetscale_replica_lag_seconds"), "seconds");
    assert.equal(
      metricUnit("planetscale_wal_archiver_last_age_succeeded"),
      "seconds"
    );
    assert.equal(metricUnit("queries"), "number");
  });
});

describe("pointValues and valueStats", () => {
  it("skips incomplete, NaN, and Inf samples", () => {
    assert.deepEqual(
      pointValues([[1], [2, 10], [3, Number.NaN], [4, Number.POSITIVE_INFINITY], [5, 20]]),
      [10, 20]
    );
  });

  it("computes min, avg, and max", () => {
    assert.deepEqual(valueStats([10, 20, 30]), { min: 10, avg: 20, max: 30 });
  });
});

describe("summarizeMetricSeries", () => {
  const response = {
    type: "MetricSeries",
    start_date: "2026-08-18T16:00:00Z",
    end_date: "2026-08-18T17:00:00Z",
    interval: 60,
    series: [
      {
        type: "TimeSeries",
        metric: "queries",
        label: "Queries",
        labels: { tablet_type: "primary" },
        points: [
          [1787068800, 912],
          [1787068860, 1048],
          [1787068920, 1000],
        ],
      },
      {
        metric: "latency_p99",
        points: [],
      },
    ],
  };

  it("returns latest/min/avg/max without raw points by default", () => {
    const summary = summarizeMetricSeries(response);
    assert.equal(summary.series.length, 2);

    const queries = summary.series[0];
    assert.equal(queries?.metric, "queries");
    assert.equal(queries?.label, "Queries");
    assert.deepEqual(queries?.labels, { tablet_type: "primary" });
    assert.equal(queries?.latest, 1000);
    assert.equal(queries?.min, 912);
    assert.equal(queries?.avg, (912 + 1048 + 1000) / 3);
    assert.equal(queries?.max, 1048);
    assert.equal(queries?.point_count, 3);
    assert.equal(queries?.unit, "number");
    assert.equal(queries?.points, undefined);

    const latency = summary.series[1];
    assert.equal(latency?.latest, null);
    assert.equal(latency?.point_count, 0);
    assert.equal(latency?.unit, "milliseconds");
  });

  it("includes points when requested", () => {
    const summary = summarizeMetricSeries(response, true);
    assert.deepEqual(summary.series[0]?.points, response.series[0]?.points);
  });
});

describe("validateMetricRange", () => {
  it("requires from and to together", () => {
    assert.equal(
      validateMetricRange({ from: "2026-01-01T00:00:00Z" }),
      "from and to must be used together"
    );
  });

  it("rejects combining period with a custom range", () => {
    assert.equal(
      validateMetricRange({
        period: "1h",
        from: "2026-01-01T00:00:00Z",
        to: "2026-01-01T01:00:00Z",
      }),
      "period cannot be combined with from/to. Use either period or from/to."
    );
  });

  it("rejects non-positive steps", () => {
    assert.equal(
      validateMetricRange({ steps: 0 }),
      "steps must be greater than zero"
    );
  });

  it("accepts a valid period or custom range", () => {
    assert.equal(validateMetricRange({ period: "1d" }), undefined);
    assert.equal(
      validateMetricRange({
        from: "2026-01-01T00:00:00Z",
        to: "2026-01-01T01:00:00Z",
        steps: 12,
      }),
      undefined
    );
  });
});
