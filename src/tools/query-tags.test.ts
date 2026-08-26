import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import {
  SUMMARY_FIELD_KEYS,
  SUMMARY_FIELD_NAMES,
  summaryResponseKeys,
} from "../lib/query-summary-fields.ts";
import { clampLimit, compactEntry, queryTagsGram } from "./query-tags.ts";

const originalFetch = globalThis.fetch;
const originalTokens = {
  PLANETSCALE_OAUTH2_ACCESS_TOKEN: process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"],
  PLANETSCALE_API_TOKEN: process.env["PLANETSCALE_API_TOKEN"],
};

afterEach(() => {
  globalThis.fetch = originalFetch;
  for (const [name, value] of Object.entries(originalTokens)) {
    if (value === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = value;
    }
  }
});

/** Capture the URL of a single request and answer it with `body`. */
function stubFetch(body: unknown): { url: () => URL } {
  let captured: URL | undefined;
  globalThis.fetch = async (input) => {
    captured = new URL(input.toString());
    return Response.json(body);
  };
  return {
    url: () => {
      assert.ok(captured, "expected a request to have been made");
      return captured;
    },
  };
}

function refuseFetch(): void {
  globalThis.fetch = async () => {
    assert.fail("expected no API call");
  };
}

const branch = {
  organization: "stable",
  database: "paddock",
  branch: "main",
};

// The summaries endpoint serializes every metric it knows about on every row
// and zero-fills the ones we didn't request, so the requested keys are the only
// thing separating a measured 0 from padding. Note the request names and the
// response keys differ: `count` arrives as `query_count`.
const requested = summaryResponseKeys([
  "dimensions",
  "count",
  "errorCount",
  "totalTime",
]);

test("clamps a count into the documented range", () => {
  assert.equal(clampLimit(500, 100), 100);
  assert.equal(clampLimit(0, 100), 1);
  assert.equal(clampLimit(-7, 100), 1);
  assert.equal(clampLimit(25, 100), 25);
});

test("truncates fractional counts the way the API's to_i does", () => {
  assert.equal(clampLimit(10.9, 100), 10);
  assert.equal(clampLimit(0.5, 100), 1);
});

test("maps request field names to the keys the API serializes them under", () => {
  assert.deepEqual(
    summaryResponseKeys(["count", "totalTime", "p99Latency", "lastRun"]),
    new Set([
      "query_count",
      "sum_total_duration_millis",
      "p99_latency",
      "last_run_at",
    ])
  );
});

test("pairs every api name with a distinct response key", () => {
  // The table is maintained by hand against the API, so a copy/paste slip could
  // point two fields at one key and silently keep the wrong metric's zeros.
  const keys = Object.values(SUMMARY_FIELD_KEYS);

  assert.equal(new Set(keys).size, keys.length);
  assert.equal(SUMMARY_FIELD_NAMES.length, keys.length);
});

test("keeps zeros for fields that were requested", () => {
  const compacted = compactEntry(
    { query_count: 0, error_count: 0, sum_total_duration_millis: 0 },
    requested
  );

  assert.deepEqual(compacted, {
    query_count: 0,
    error_count: 0,
    sum_total_duration_millis: 0,
  });
});

test("strips zero-filled metrics that were not requested", () => {
  const compacted = compactEntry(
    {
      query_count: 12,
      error_count: 0,
      p99_latency: 0,
      sum_rows_read: 0,
      sum_cpu_duration_millis: 0,
      blocks_hit: 0,
    },
    requested
  );

  assert.deepEqual(compacted, { query_count: 12, error_count: 0 });
});

test("keeps nonzero values for unrequested fields, since padding is never nonzero", () => {
  const compacted = compactEntry({ query_count: 3, p99_latency: 42 }, requested);

  assert.deepEqual(compacted, { query_count: 3, p99_latency: 42 });
});

test("keeps every zero when no field set is supplied", () => {
  // The tag listing and single-tag endpoints take no `fields` param, so nothing
  // in their responses is padding.
  const compacted = compactEntry({ name: "sidekiq_job", query_count: 0 });

  assert.deepEqual(compacted, { name: "sidekiq_job", query_count: 0 });
});

test("drops absent values and the API's type annotations", () => {
  const compacted = compactEntry(
    { query_count: 0, error_count: null, values: [], type: "TagSummary" },
    requested
  );

  assert.deepEqual(compacted, { query_count: 0 });
});

test("compacts objects nested in arrays without applying the zero rule to them", () => {
  // `requestedKeys` holds row-level metric keys, which a nested object's own
  // key namespace can never intersect -- so applying the rule here would strip
  // every nested zero. Nothing inside these objects is zero-fill anyway.
  const compacted = compactEntry(
    {
      values: [
        { type: "QueryTagValue", name: "gallops#show", query_count: 0 },
        { type: "QueryTagValue", name: "gallops#index", query_count: 4 },
      ],
    },
    requested
  );

  assert.deepEqual(compacted, {
    values: [
      { name: "gallops#show", query_count: 0 },
      { name: "gallops#index", query_count: 4 },
    ],
  });
});

test("leaves primitive array members untouched", () => {
  const compacted = compactEntry({ tags: ["Sapplication", "Broute"] }, requested);

  assert.deepEqual(compacted, { tags: ["Sapplication", "Broute"] });
});

test("list_query_tags sends the value filters and no per_page", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch({
    data: [
      {
        type: "QueryTag",
        id: "Sapp",
        name: "app",
        source: "sql",
        query_count: 9,
        values: [
          { type: "QueryTagValue", name: "web", query_count: 9, kind: "literal" },
        ],
      },
    ],
  });

  const response = await queryTagsGram.handleToolCall({
    name: "list_query_tags",
    input: {
      ...branch,
      period: "1d",
      name_pattern: "%app%",
      // Above the API's own ceiling, so it should arrive clamped.
      values_limit: 500,
      literal_values_only: true,
    },
  });

  const url = request.url();
  assert.equal(
    url.pathname,
    "/v1/organizations/stable/databases/paddock/branches/main/insights/tags"
  );
  assert.equal(url.searchParams.get("q"), "%app%");
  assert.equal(url.searchParams.get("period"), "1d");
  assert.equal(url.searchParams.get("values_limit"), "100");
  assert.equal(url.searchParams.get("literal_values_only"), "true");
  // The endpoint declares per_page but ignores it; sending one would imply a
  // limit the caller could raise.
  assert.equal(url.searchParams.has("per_page"), false);

  const result = (await response.json()) as {
    returned: number;
    truncated: boolean;
    tags: Array<Record<string, unknown>>;
  };
  assert.equal(result.returned, 1);
  assert.equal(result.truncated, false);
  assert.deepEqual(result.tags[0], {
    id: "Sapp",
    name: "app",
    source: "sql",
    query_count: 9,
    values: [{ name: "web", query_count: 9, kind: "literal" }],
  });
});

test("get_query_tag puts the prefixed id in the path and unwraps nothing", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch({
    type: "QueryTag",
    id: "Busername",
    name: "username",
    source: "system",
    query_count: 4,
    values: [],
  });

  const response = await queryTagsGram.handleToolCall({
    name: "get_query_tag",
    input: { ...branch, tag: "Busername" },
  });

  assert.equal(
    request.url().pathname,
    "/v1/organizations/stable/databases/paddock/branches/main/insights/tags/Busername"
  );

  const result = (await response.json()) as { tag: Record<string, unknown> };
  // `values` survives as an empty array rather than being compacted away, so
  // "no values in this window" stays distinguishable from "no values key".
  assert.deepEqual(result.tag, {
    id: "Busername",
    name: "username",
    source: "system",
    query_count: 4,
    values: [],
  });
});

test("list_query_tag_summaries repeats array params and always sorts descending", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch({
    data: [
      {
        type: "DimensionsQuerySummary",
        dimensions: { Sapp: "web", Broute: "gallops#index" },
        query_count: 12,
        error_count: 0,
        sum_total_duration_millis: 340,
        // Zero-fill for metrics we did not request.
        blocks_hit: 0,
        max_latency: 0,
      },
    ],
    dimension_counts: { collapsed_count: 2, total_count: 14 },
  });

  const response = await queryTagsGram.handleToolCall({
    name: "list_query_tag_summaries",
    input: {
      ...branch,
      tags: ["Sapp", "Broute"],
      fields: ["count", "errorCount", "totalTime"],
      statement_type: "SELECT",
      limit: 5,
    },
  });

  const url = request.url();
  assert.equal(
    url.pathname,
    "/v1/organizations/stable/databases/paddock/branches/main/insights/tags/summaries"
  );
  assert.deepEqual(url.searchParams.getAll("tags[]"), ["Sapp", "Broute"]);
  // 'dimensions' is forced in even though the caller left it out, and the API
  // requires these as repeated key[] params rather than one scalar.
  assert.deepEqual(url.searchParams.getAll("fields[]"), [
    "dimensions",
    "count",
    "errorCount",
    "totalTime",
  ]);
  // totalTime is the default sort and was already requested here, so it must
  // not be appended a second time.
  assert.equal(url.searchParams.getAll("fields[]").length, 4);
  assert.equal(url.searchParams.get("sort"), "totalTime");
  assert.equal(url.searchParams.get("dir"), "desc");
  assert.equal(url.searchParams.get("type"), "SELECT");
  assert.equal(url.searchParams.get("per_page"), "5");

  const result = (await response.json()) as {
    grouped_by: string[];
    sort_by: string;
    fields: Record<string, string>;
    truncated: boolean;
    dimension_counts: { collapsed_count: number; total_count: number };
    summaries: Array<Record<string, unknown>>;
  };
  assert.deepEqual(result.grouped_by, ["Sapp", "Broute"]);
  assert.equal(result.sort_by, "totalTime");
  // Each requested name is paired with the key it actually arrives under.
  assert.deepEqual(result.fields, {
    dimensions: "dimensions",
    count: "query_count",
    errorCount: "error_count",
    totalTime: "sum_total_duration_millis",
  });
  assert.equal(result.truncated, false);
  assert.deepEqual(result.dimension_counts, {
    collapsed_count: 2,
    total_count: 14,
  });
  // error_count keeps its 0 because it was requested; blocks_hit and
  // max_latency are dropped because they were not.
  assert.deepEqual(result.summaries[0], {
    dimensions: { Sapp: "web", Broute: "gallops#index" },
    query_count: 12,
    error_count: 0,
    sum_total_duration_millis: 340,
  });
});

test("requests the sort metric even when fields leaves it out", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch({ data: [] });

  // The API computes the sort column regardless, so omitting it does not remove
  // it from the rows -- it removes it only where it measured zero, leaving an
  // ordering the caller cannot check against rows that half-carry the metric.
  const response = await queryTagsGram.handleToolCall({
    name: "list_query_tag_summaries",
    input: { ...branch, tags: ["Sapp"], fields: ["count"] },
  });

  assert.deepEqual(request.url().searchParams.getAll("fields[]"), [
    "dimensions",
    "count",
    "totalTime",
  ]);

  const result = (await response.json()) as { fields: Record<string, string> };
  assert.equal(result.fields["totalTime"], "sum_total_duration_millis");
});

test("refuses period combined with from before calling the API", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  refuseFetch();

  const response = await queryTagsGram.handleToolCall({
    name: "list_query_tags",
    input: { ...branch, period: "1d", from: "2026-08-24T00:00:00Z" },
  });

  assert.match(
    await response.text(),
    /'period' cannot be combined with 'from'\/'to'/
  );
});

test("reports a missing token without calling the API", async () => {
  delete process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"];
  delete process.env["PLANETSCALE_API_TOKEN"];
  refuseFetch();

  const response = await queryTagsGram.handleToolCall({
    name: "list_query_tag_summaries",
    input: { ...branch, tags: ["Sapp"] },
  });

  assert.match(await response.text(), /No PlanetScale authentication/);
});
