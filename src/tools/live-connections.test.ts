import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import {
  LIST_LOCK_RETRY,
  liveConnectionsGram,
  selectRows,
  shapeRow,
  summarize,
  targetRequiredBody,
  truncateQuery,
} from "./live-connections.ts";

const originalFetch = globalThis.fetch;
const originalToken = process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"];

afterEach(() => {
  globalThis.fetch = originalFetch;
  if (originalToken === undefined) {
    delete process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"];
  } else {
    process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = originalToken;
  }
});

const branch = {
  organization: "stable",
  database: "paddock",
  branch: "main",
};

test("summary covers filtered rows the limit has not dropped yet", () => {
  const rows = [
    {
      pid: 1,
      state: "active",
      usename: "alice",
      application_name: "checkout",
      duration_ms: 10,
      xact_start: "2026-06-01T00:00:02Z",
      blocked_by: [9],
    },
    {
      pid: 2,
      state: "idle",
      usename: "alice",
      application_name: "checkout",
      duration_ms: 500,
      xact_start: "2026-06-01T00:00:01Z",
      blocked_by: [],
    },
    {
      pid: 3,
      state: "active",
      usename: "bob",
      duration_ms: 40,
    },
  ];

  const summary = summarize(selectRows(rows, { state: "active" }));

  assert.deepEqual(summary.counts_by_state, { active: 2 });
  assert.deepEqual(summary.counts_by_user, { alice: 1, bob: 1 });
  assert.deepEqual(summary.counts_by_app, { checkout: 1 });
  assert.equal(summary.blocked_count, 1);
  assert.equal(summary.longest_query?.["pid"], 3);
  assert.equal(summary.oldest_transaction?.["pid"], 1);
  assert.equal(summary.oldest_transaction?.["xact_start"], "2026-06-01T00:00:02Z");
});

test("drops rows below the duration floor and rows with no duration", () => {
  const kept = selectRows(
    [
      { pid: 1, duration_ms: 1000 },
      { pid: 2, duration_ms: 999 },
      { pid: 3, state: "active" },
    ],
    { minDurationMs: 1000 },
  );

  assert.deepEqual(kept.map((row) => row["pid"]), [1]);
});

test("mysql rows stay on the common fields, postgres keeps lock detail", () => {
  const row = {
    pid: 101,
    instance: "zone1-2001",
    state: "Query/executing",
    usename: "vt_app",
    duration_ms: 42_000,
    query_text: "SELECT 1",
    wait_event: "transactionid",
    blocked_by: [7],
    application_name: "checkout",
  };

  assert.deepEqual(shapeRow(row, "mysql", false), {
    pid: 101,
    instance: "zone1-2001",
    state: "Query/executing",
    usename: "vt_app",
    duration_ms: 42_000,
  });
  assert.equal(shapeRow(row, "postgresql", false)["wait_event"], "transactionid");
  assert.deepEqual(shapeRow(row, "postgresql", false)["blocked_by"], [7]);
});

test("query text stays out unless asked, and a long query is cut", () => {
  const query = "select secret from accounts where id = 1";
  const hidden = shapeRow({ pid: 1, query_text: query, sidecar_backends: [{ query_text: query }] }, "neki", false);
  assert.equal("query_text" in hidden, false);
  assert.deepEqual(hidden["sidecar_backends"], [{}]);

  const padded = `${"a".repeat(500)}TAIL`;
  assert.equal(truncateQuery(padded).truncated, true);
  const shown = shapeRow({ pid: 1, query_text: padded }, "postgresql", true);
  assert.equal(shown["query_text"], "a".repeat(500));
  assert.equal(shown["query_text_truncated"], true);
});

test("a Vitess 400 that names targets becomes a retry payload", () => {
  const target = targetRequiredBody({
    code: "bad_request",
    message: "Keyspace 'commerce' is sharded. Specify which shard to target.",
    available: { shards: ["-80", "80-"] },
  });

  assert.deepEqual(target, {
    message: "Keyspace 'commerce' is sharded. Specify which shard to target.",
    available: { shards: ["-80", "80-"] },
  });
  assert.equal(
    targetRequiredBody({ code: "bad_request", message: "keyspace and shard are only supported for Vitess databases." }),
    undefined,
  );
});

function stubFetch(status: number, body: unknown): { calls: () => Array<{ url: URL; method: string }> } {
  const calls: Array<{ url: URL; method: string }> = [];
  globalThis.fetch = async (input, init) => {
    calls.push({ url: new URL(input.toString()), method: init?.method ?? "GET" });
    return Response.json(body, { status });
  };
  return { calls: () => calls };
}

test("lists one tablet and keeps the longest rows under the limit", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch(200, {
    database_kind: "mysql",
    captured_at: "2026-06-04T12:30:05.000Z",
    instances: [],
    topology: { keyspace: "main", shard: "-", tablet: "zone1-2001" },
    data: [
      { pid: 1, state: "Sleep", usename: "vt_app", duration_ms: 10, query_text: "select short" },
      { pid: 2, state: "Query/executing", usename: "vt_app", duration_ms: 9000, query_text: "select long" },
      { pid: 3, state: "Query/executing", usename: "vt_app", duration_ms: 50, query_text: "select mid" },
    ],
  });

  const response = await liveConnectionsGram.handleToolCall({
    name: "list_live_connections",
    input: { ...branch, keyspace: "main", shard: "-", state: "Query/executing", limit: 1, include_query_text: true },
  });
  const body = (await response.json()) as {
    target: { tablet: string };
    summary: { counts_by_state: Record<string, number>; longest_query: { pid: number } };
    rows: Array<{ pid: number; query_text: string }>;
    matched: number;
    truncated: boolean;
  };

  const call = request.calls()[0];
  assert.equal(call?.method, "GET");
  assert.equal(
    call?.url.pathname,
    "/v1/organizations/stable/databases/paddock/branches/main/connections",
  );
  assert.equal(call?.url.searchParams.get("keyspace"), "main");
  assert.equal(call?.url.searchParams.get("shard"), "-");
  assert.equal(body.target.tablet, "zone1-2001");
  assert.deepEqual(body.summary.counts_by_state, { "Query/executing": 2 });
  assert.equal(body.summary.longest_query.pid, 2);
  assert.equal(body.matched, 2);
  assert.equal(body.truncated, true);
  assert.deepEqual(
    body.rows.map((row) => row.pid),
    [2],
  );
  assert.equal(body.rows[0]?.query_text, "select long");
});

test("a multi-keyspace 400 is a target list, not a failure", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  stubFetch(400, {
    code: "bad_request",
    message: "This database has multiple keyspaces. Specify which keyspace to target.",
    available: { keyspaces: ["lookup", "main"] },
  });

  const response = await liveConnectionsGram.handleToolCall({
    name: "list_live_connections",
    input: branch,
  });
  const body = (await response.json()) as {
    needs_target: boolean;
    available: { keyspaces: string[] };
    rows: unknown[];
  };

  assert.equal(body.needs_target, true);
  assert.deepEqual(body.available.keyspaces, ["lookup", "main"]);
  assert.deepEqual(body.rows, []);
});

test("a 503 tells the agent to wait out the one second lock", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  stubFetch(503, {
    code: "service_unavailable",
    message: "This endpoint is currently unavailable. Please try again later.",
  });

  const response = await liveConnectionsGram.handleToolCall({
    name: "list_live_connections",
    input: branch,
  });

  const text = await response.text();
  assert.match(text, /Please try again later/);
  assert.match(text, new RegExp(LIST_LOCK_RETRY.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(text, /status: 503/);
});
