import assert from "node:assert/strict";
import { test } from "node:test";
import {
  engineOfResult,
  extractResultsFromContent,
  inferEngine,
  isTitleMatch,
  normalizeEngine,
  orderResults,
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

const doc = (page: string) => ({
  title: page,
  url: `https://planetscale.com/docs/${page}`,
  page,
});
/** What the tool does: infer the engine from the query, then order. */
const order = (query: string, docs: ReturnType<typeof doc>[], blogs: ReturnType<typeof doc>[]) =>
  orderResults(query, docs, blogs, inferEngine(query));
const blog = (slug: string, title: string) => ({
  title,
  url: `https://planetscale.com/blog/${slug}`,
  page: `blog/${slug}`,
});

test("infers the engine only when the query names exactly one", () => {
  assert.equal(inferEngine("restore my Postgres database"), "postgres");
  assert.equal(inferEngine("live connections on my MySQL database"), "vitess");
  assert.equal(inferEngine("Vitess read replicas"), "vitess");
  assert.equal(inferEngine("connect Prisma to Neki"), "neki");
  assert.equal(inferEngine("import a Postgres database into Neki"), undefined);
  assert.equal(inferEngine("create a service token"), undefined);
  assert.equal(inferEngine("postgresql extensions"), "postgres");
});

test("reads the engine from a docs page or url", () => {
  assert.equal(engineOfResult(doc("neki/backups")), "neki");
  assert.equal(
    engineOfResult({ url: "https://planetscale.com/docs/vitess/backups#restore" }),
    "vitess"
  );
  assert.equal(engineOfResult(doc("cli/backup")), undefined);
  assert.equal(engineOfResult(doc("postgres-vs-vitess")), undefined);
  assert.equal(engineOfResult(blog("postgres-17", "Postgres 17")), undefined);
});

test("asked engine first, then neutral pages, then other engines; upstream order kept within groups", () => {
  const docs = [
    doc("cli/backup"),
    doc("neki/coming-from-postgres"),
    doc("postgres/pricing"),
    doc("neki/backups"),
    doc("postgres/backups"),
  ];

  const pages = order("how do backups work on PlanetScale Postgres", docs, []).map(
    (r) => r.page
  );

  assert.deepEqual(pages, [
    "postgres/pricing",
    "postgres/backups",
    "cli/backup",
    "neki/coming-from-postgres",
    "neki/backups",
  ]);
});

test("an explicit engine overrides the query", () => {
  const docs = [doc("postgres/backups"), doc("vitess/backups")];
  const pages = orderResults("how do backups work", docs, [], "vitess").map((r) => r.page);
  assert.deepEqual(pages, ["vitess/backups", "postgres/backups"]);
});

test("no engine named keeps the upstream docs order", () => {
  const docs = [doc("vitess/backups"), doc("postgres/backups")];
  const pages = order("import Postgres into Neki", docs, []).map((r) => r.page);
  assert.deepEqual(pages, ["vitess/backups", "postgres/backups"]);
});

test("a blog post matching on title stays above docs", () => {
  const strong = blog("what-is-a-neki-router", "What is a Neki router?");
  assert.ok(isTitleMatch(strong, "what is a neki router"));

  const results = order("what is a neki router", [doc("neki/overview")], [strong]);
  assert.equal(results[0]?.page, "blog/what-is-a-neki-router");
});

test("a blog post matching only on body text goes below docs", () => {
  const weak = blog("introducing-tin", "Introducing Tin");
  assert.equal(isTitleMatch(weak, "how do backups work on PlanetScale Postgres"), false);

  const pages = order(
    "how do backups work on PlanetScale Postgres",
    [doc("cli/backup"), doc("postgres/backups")],
    [weak]
  ).map((r) => r.page);

  assert.deepEqual(pages, ["postgres/backups", "cli/backup", "blog/introducing-tin"]);
});

test("a slug can carry the title match", () => {
  assert.ok(isTitleMatch(blog("postgres-backups-explained", "Explained"), "postgres backups"));
});

test("branch kinds from other tools map to docs engines", () => {
  assert.equal(normalizeEngine("mysql"), "vitess");
  assert.equal(normalizeEngine("postgresql"), "postgres");
  assert.equal(normalizeEngine("neki"), "neki");
  assert.equal(normalizeEngine("vitess"), "vitess");
  assert.equal(normalizeEngine("postgres"), "postgres");
});

test("the engine input schema accepts branch kind values", async () => {
  const { searchDocumentationGram } = await import("./search-documentation.ts");
  const tool = searchDocumentationGram
    .manifest()
    .tools?.find((t) => t.name === "search_documentation");
  assert.ok(tool, "search_documentation is in the manifest");
  const schema = JSON.stringify(tool.inputSchema);
  for (const kind of ["mysql", "postgresql", "vitess", "postgres", "neki"]) {
    assert.ok(schema.includes(`"${kind}"`), `engine schema should accept ${kind}`);
  }
});

test("a neutral page whose path matches the query ranks with the engine's pages", () => {
  const docs = [doc("postgres/pricing"), doc("neki/backups"), doc("cli/backup"), doc("security/sso")];
  const pages = order("pscale backup create on my Postgres branch", docs, []).map((r) => r.page);
  assert.deepEqual(pages, ["postgres/pricing", "cli/backup", "security/sso", "neki/backups"]);
});

test("a neutral security page asked for by name ranks with the engine's pages", () => {
  const docs = [doc("postgres/connecting"), doc("vitess/sso"), doc("security/sso")];
  const pages = order("set up SSO for my Postgres organization", docs, []).map((r) => r.page);
  assert.deepEqual(pages, ["postgres/connecting", "security/sso", "vitess/sso"]);
});

test("Postgres tools and extensions name the Postgres engine", () => {
  for (const query of ["psql cannot connect", "pg_dump my database", "postgres_fdw setup", "PgBouncer pool size"]) {
    assert.equal(inferEngine(query), "postgres", query);
  }
});

test("the url fallback only reads planetscale.com docs urls", () => {
  assert.equal(engineOfResult({ url: "https://planetscale.com/docs/postgres/backups" }), "postgres");
  assert.equal(engineOfResult({ url: "https://evilplanetscale.com/docs/postgres/backups" }), undefined);
});

test("a one-word query is a title match only when the title is that word", () => {
  assert.equal(isTitleMatch(blog("postgres-17", "Postgres 17 is here"), "postgres"), false);
  assert.equal(isTitleMatch(blog("branching", "Branching"), "branching"), true);
});

test("another engine's import page for the asked engine stays above other engines' pages", () => {
  const docs = [doc("postgres/backups"), doc("postgres/imports/mysql"), doc("vitess/imports")];
  const pages = order("import a MySQL database", docs, []).map((r) => r.page);
  assert.deepEqual(pages, ["vitess/imports", "postgres/imports/mysql", "postgres/backups"]);
});

test("generic path words do not promote a neutral page", () => {
  const docs = [doc("api-reference/create-a-database"), doc("postgres/getting-started")];
  const pages = order("create a database on postgres", docs, []).map((r) => r.page);
  assert.deepEqual(pages, ["postgres/getting-started", "api-reference/create-a-database"]);
});

test("docs.planetscale.com urls are read too", () => {
  assert.equal(engineOfResult({ url: "https://docs.planetscale.com/vitess/backups" }), "vitess");
});
