import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { isWideRange, LEGACY_MAX_RANGE_HOURS } from "../lib/insights-tools.ts";
import { getInsightsGram } from "./get-insights.ts";

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

/**
 * Answer every request with `body`, keyed by the last path segment so a
 * fingerprint call -- which fans out to the summary and the executions at once
 * -- can serve each half differently.
 */
function stubFetch(
  responder: (url: URL) => Response
): { urls: () => URL[]; url: (suffix: string) => URL } {
  const captured: URL[] = [];
  globalThis.fetch = async (input) => {
    const url = new URL(input.toString());
    captured.push(url);
    return responder(url);
  };
  return {
    urls: () => captured,
    url: (suffix) => {
      const match = captured.find((u) => u.pathname.endsWith(suffix));
      assert.ok(match, `expected a request to a path ending in ${suffix}`);
      return match;
    },
  };
}

const branch = {
  organization: "stable",
  database: "paddock",
  branch: "main",
};

const fingerprintCall = {
  ...branch,
  fingerprint: "abc123",
  keyspace: "horses",
};

const summaryBody = {
  id: "qs1",
  fingerprint: "abc123",
  normalized_sql: "select * from horses where barn_id = ?",
  query_count: 4200,
};

const executionsBody = {
  data: [
    {
      id: "q1",
      fingerprint: "abc123",
      normalized_sql: "select * from horses where barn_id = ?",
      started_at: "2026-08-26T12:00:00Z",
      total_duration_millis: 12,
    },
  ],
};

function respondBoth(url: URL): Response {
  return Response.json(url.pathname.endsWith("/summary") ? summaryBody : executionsBody);
}

interface FingerprintResult {
  mode: string;
  from: string;
  to: string;
  summary: Record<string, unknown> | null;
  summary_error?: string;
  executions: {
    total: number;
    window_note?: string;
    error?: string;
    queries: Array<Record<string, unknown>>;
  };
}

test("a range is wide only once it passes the legacy cap", () => {
  const hoursAgo = (h: number) =>
    new Date(Date.now() - h * 60 * 60 * 1000).toISOString();

  assert.equal(isWideRange(hoursAgo(LEGACY_MAX_RANGE_HOURS - 1), undefined), false);
  assert.equal(isWideRange(hoursAgo(LEGACY_MAX_RANGE_HOURS + 1), undefined), true);
  assert.equal(isWideRange(hoursAgo(48), hoursAgo(47)), false);
  // Nothing to measure, or nothing parseable: the API owns the verdict.
  assert.equal(isWideRange(undefined, hoursAgo(0)), false);
  assert.equal(isWideRange("last tuesday", undefined), false);
});

test("fingerprint mode keeps sending an explicit to inside the legacy window", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch(respondBoth);

  const response = await getInsightsGram.handleToolCall({
    name: "get_insights",
    input: fingerprintCall,
  });

  for (const url of request.urls()) {
    assert.ok(url.searchParams.get("from"), "expected a from on every request");
    assert.ok(url.searchParams.get("to"), "expected a to on every request");
  }

  const result = (await response.json()) as FingerprintResult;
  assert.equal(result.mode, "fingerprint");
  assert.equal(result.executions.window_note, undefined);
  assert.equal(result.executions.total, 1);
  assert.deepEqual(result.summary, {
    id: "qs1",
    fingerprint: "abc123",
    normalized_sql: "select * from horses where barn_id = ?",
    query_count: 4200,
  });
});

test("fingerprint mode drops an open-ended to on a wide range", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch(respondBoth);

  const response = await getInsightsGram.handleToolCall({
    name: "get_insights",
    input: { ...fingerprintCall, from: "2026-06-01T00:00:00Z" },
  });

  // "now" is not an hour boundary, so sending it would have the summary
  // endpoint reject the whole range. Omitted, the API rounds up for us.
  for (const url of request.urls()) {
    assert.equal(url.searchParams.get("from"), "2026-06-01T00:00:00Z");
    assert.equal(url.searchParams.has("to"), false);
  }

  const result = (await response.json()) as FingerprintResult;
  assert.equal(result.from, "2026-06-01T00:00:00Z");
  // Reported as the window actually covered: through the end of this hour.
  assert.match(result.to, /:59:59\.\d+Z$/);
  assert.match(result.executions.window_note ?? "", /last 24 hours only/);
});

test("fingerprint mode passes an explicit wide to through untouched", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  const request = stubFetch(respondBoth);

  await getInsightsGram.handleToolCall({
    name: "get_insights",
    input: {
      ...fingerprintCall,
      from: "2026-06-01T00:00:00Z",
      to: "2026-07-01T00:00:00Z",
    },
  });

  // Aligning a to the caller chose is the API's call, not ours -- it has the
  // rules and reports which one was broken.
  for (const url of request.urls()) {
    assert.equal(url.searchParams.get("to"), "2026-07-01T00:00:00Z");
  }
});

test("fingerprint mode reports why the summary was refused", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  stubFetch((url) =>
    url.pathname.endsWith("/summary")
      ? Response.json(
          {
            code: "bad_request",
            message:
              "from and to must fall on hour boundaries for time ranges longer than 25 hours",
          },
          { status: 400 }
        )
      : Response.json(executionsBody)
  );

  const response = await getInsightsGram.handleToolCall({
    name: "get_insights",
    input: {
      ...fingerprintCall,
      from: "2026-06-01T00:30:00Z",
      to: "2026-07-01T00:30:00Z",
    },
  });

  const result = (await response.json()) as FingerprintResult;
  // Without the reason, a refused range is indistinguishable from a
  // fingerprint that has no data.
  assert.equal(result.summary, null);
  assert.match(result.summary_error ?? "", /hour boundaries/);
  assert.match(result.summary_error ?? "", /status: 400/);
  assert.equal(result.executions.total, 1);
});

test("discovery mode surfaces the API's own message on a rejected range", async () => {
  process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"] = "oauth-token";
  stubFetch(() =>
    Response.json(
      { code: "bad_request", message: "time range must not be wider than 365 days" },
      { status: 400 }
    )
  );

  const response = await getInsightsGram.handleToolCall({
    name: "get_insights",
    input: { ...branch, from: "2020-01-01T00:00:00Z", to: "2026-01-01T00:00:00Z" },
  });

  assert.match(await response.text(), /time range must not be wider than 365 days/);
});
