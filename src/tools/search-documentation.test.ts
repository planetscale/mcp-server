import assert from "node:assert/strict";
import { test } from "node:test";
import {
  extractResultsFromContent,
  parseDocsTextBlock,
} from "./search-documentation.ts";

/** One upstream hit, shaped the way the Mintlify docs server returns it. */
const HIT = [
  "Title: Available query statistics",
  "Link: https://planetscale.com/docs/vitess/monitoring/query-insights#available-query-statistics",
  "Page: vitess/monitoring/query-insights",
  "Content: ### Available query statistics",
  "",
  "You can customize the metrics that show up on the Queries list.",
].join("\n");

test("parses title, link and page out of a docs hit", () => {
  const result = parseDocsTextBlock(HIT);

  assert.ok(result);
  assert.equal(result.title, "Available query statistics");
  assert.equal(
    result.url,
    "https://planetscale.com/docs/vitess/monitoring/query-insights#available-query-statistics"
  );
  assert.equal(result.page, "vitess/monitoring/query-insights");
});

test("keeps the whole content body in the snippet, headers excluded", () => {
  const result = parseDocsTextBlock(HIT);

  assert.ok(result?.snippet);
  assert.match(result.snippet, /^### Available query statistics/);
  assert.match(result.snippet, /Queries list\.$/);
  assert.doesNotMatch(result.snippet, /^Title:/m);
  assert.equal(result.truncated, undefined);
});

test("a Summary: line inside a body stays content, not a header", () => {
  const block = [
    "Title: List branch queries",
    "Link: https://planetscale.com/docs/api/reference/list_branch_queries",
    "Page: api/reference/list_branch_queries",
    "Content: REST Endpoint GET /organizations/{organization}/databases",
    "Summary: List branch queries",
  ].join("\n");

  const result = parseDocsTextBlock(block);

  assert.ok(result?.snippet);
  assert.match(result.snippet, /Summary: List branch queries$/);
});

test("an over-long body is truncated and flagged", () => {
  const block = `Title: Long page\nLink: https://example.com\nContent: ${"horse ".repeat(500)}`;

  const result = parseDocsTextBlock(block);

  assert.equal(result?.truncated, true);
  assert.ok(result.snippet);
  assert.equal(result.snippet.length, 1501, "1500 chars plus the ellipsis");
  assert.match(result.snippet, /…$/);
});

test("text carrying none of the headers is returned unparsed", () => {
  assert.equal(parseDocsTextBlock("no headers at all"), undefined);

  const results = extractResultsFromContent([
    { type: "text", text: "no headers at all" },
  ]);
  assert.deepEqual(results, [{ text: "no headers at all" }]);
});

test("each upstream text block becomes one result with a usable url", () => {
  const results = extractResultsFromContent([
    { type: "text", text: HIT },
    { type: "text", text: HIT.replaceAll("vitess", "postgres") },
  ]);

  assert.equal(results.length, 2);
  for (const result of results) {
    assert.ok(result.title, "every result needs a title");
    assert.ok(result.url?.startsWith("https://"), "every result needs a url");
  }
  assert.equal(results[1]?.page, "postgres/monitoring/query-insights");
});

test("resource_link blocks are still understood", () => {
  const results = extractResultsFromContent([
    {
      type: "resource_link",
      name: "Query insights",
      uri: "https://planetscale.com/docs/vitess/monitoring/query-insights",
      description: "How insights works",
    },
  ]);

  assert.deepEqual(results, [
    {
      title: "Query insights",
      url: "https://planetscale.com/docs/vitess/monitoring/query-insights",
      snippet: "How insights works",
    },
  ]);
});
