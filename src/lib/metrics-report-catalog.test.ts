import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { reportSectionsForEngine } from "./metrics-report-catalog.ts";

describe("reportSectionsForEngine", () => {
  it("returns seven MySQL series sections and no instant sections", () => {
    const sections = reportSectionsForEngine("mysql");
    assert.equal(sections.length, 7);
    assert.ok(sections.every((section) => section.kind === "series"));
    assert.ok(
      sections.some((section) => section.metrics.includes("vtgate_latency_p50"))
    );
    assert.ok(
      sections.some((section) => section.metrics.includes("storage_per_table"))
    );
    assert.ok(
      !sections.some((section) =>
        section.metrics.includes("planetscale_volume_usage_percentage")
      )
    );
  });

  it("returns Postgres series plus instant capacity sections", () => {
    const sections = reportSectionsForEngine("postgresql");
    const series = sections.filter((section) => section.kind === "series");
    const instant = sections.filter((section) => section.kind === "instant");

    assert.equal(series.length, 12);
    assert.equal(instant.length, 3);
    assert.ok(
      !sections.some((section) => section.metrics.includes("vtgate_latency_p50"))
    );
    assert.ok(
      series.some((section) =>
        section.metrics.includes("planetscale_volume_usage_percentages")
      )
    );
    assert.ok(
      instant.some((section) =>
        section.metrics.includes("planetscale_volume_usage_percentage")
      )
    );
    assert.ok(
      instant.some((section) =>
        section.metrics.includes("planetscale_backup_restore_active")
      )
    );
  });

  it("rejects unsupported engines", () => {
    assert.throws(
      () => reportSectionsForEngine("horizon"),
      /database engine "horizon" is not supported by metrics report/
    );
  });
});
