# Query tag summaries (`list_query_tag_summaries`)

Source: `src/tools/query-tags.ts`

Answers "which part of my app is causing this load". Returns query statistics —
total time, latency, rows read, errors — grouped by tag value, so load is
attributed to an application, route, background job, or user rather than to a
SQL pattern. This is the payoff of the tag trio; `get_insights` answers *which
query*, this answers *whose*.

Needs a `TAG_ID` from [list-query-tags.md](./list-query-tags.md), and shares
`buildRequestContext`, `fetchTagsAPI`, and `compactEntry` with the other two tag
tools in the same source file.

## Reach it

A user asks their agent "which route is burning the most database time" or
"which background job is reading the most rows", and the client calls this with
the tag id that names routes or jobs.

## Drive it

Group by one tag, the default path:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tag_summaries \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tags":["TAG_ID"]}' \
  --expect '"sort_by":"totalTime"' --label tag-summaries-default
```

Group by two tags, which returns a row per combination of their values:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tag_summaries \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tags":["TAG_ID","OTHER_TAG_ID"]}' \
  --expect '"grouped_by"' --label tag-summaries-two-tags
```

A deliberately narrow field set. This is the check that unrequested metrics are
stripped rather than returned as zeros:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tag_summaries \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tags":["TAG_ID"],"fields":["count","totalTime"],"limit":5}' \
  --expect '"count":"query_count"' --label tag-summaries-fields
```

One row only, which is the cheap way to see `truncated: true`:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tag_summaries \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tags":["TAG_ID"],"limit":1}' \
  --expect '"truncated":true' --label tag-summaries-truncated
```

Sorted by a different metric, filtered to one statement type:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tag_summaries \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tags":["TAG_ID"],"sort_by":"rowsRead","statement_type":"SELECT"}' \
  --expect '"sort_by":"rowsRead"' --label tag-summaries-sort
```

## Proves it works

- The response carries `grouped_by`, `sort_by`, `fields`, `returned`,
  `truncated`, and a `summaries` array; `dimension_counts` appears whenever the
  API reports it.
- `grouped_by` echoes the tag ids as sent, and each row's `dimensions` is keyed
  by those same prefixed ids mapped to that row's values.
- `fields` echoes the **effective** set as a map from each requested name to the
  key it is serialized under (`count` -> `query_count`), which is what makes the
  stripping rule below checkable at all. Two names appear in it that the caller
  did not send: `dimensions`, without which rows cannot be told apart, and the
  `sort_by` metric, without which the ordering cannot be verified. Driving
  `fields: ["count"]` should return a three-entry map including
  `"totalTime": "sum_total_duration_millis"`.
- Metrics outside `fields` are **absent** from each row rather than `0`. Drive
  the narrow field set and confirm keys like `blocks_hit` and `max_latency` do
  not appear at all. The sharpest evidence is a row where both happen at once:
  with `fields: ["count","totalTime"]` a row whose duration really was zero
  keeps `sum_total_duration_millis: 0` (requested) while losing
  `time_per_query` (also zero, but never requestable). That distinction is the
  whole point of the compaction and cannot be observed any other way.
- Rows come back in descending order of `sort_by`; the first row should carry
  the largest value of that metric.
- `truncated` is `true` exactly when `returned` equals the effective limit.
  Drive with `limit: 1` against a tag with several values to see the true case.

## Notes

- The `sort_by` metric is unioned into `fields[]` on purpose. The API computes
  the sort column whether or not it was requested, so leaving it out does not
  drop it from the rows — it drops it only from the rows where it measured
  zero, which are indistinguishable from zero-fill once they arrive. The result
  is an ordering the caller cannot check, against rows that carry the metric
  only some of the time. If a change removes that union, `fields: ["count"]`
  sorted by `totalTime` is the drive that catches it.
- The tool always sends `dir=desc`. The API's own default is **ascending**, so a
  hand-rolled `curl` against this endpoint returns the *cheapest* queries first
  and looks inexplicably wrong. If a change ever stops sending `dir`, the
  ordering silently inverts and nothing errors — that is what the first-row
  check above is for.
- This endpoint ignores `fingerprint`, `keyspace`, `values_limit`, and
  `literal_values_only`, which is why the tool does not offer them. `query` is a
  structured search DSL and the only way to narrow these summaries that way:
  `fingerprint:<hash>`, `keyspace:<name>`, `tag:<key>:<value>` (or `tag:<key>`
  alone for any value), `user:<name>`, `statement_type:<type>`, `table:<name>`,
  `index:<name>`, comparisons like `p99:>100`, a bare word matching normalized
  SQL, and `!` to negate a term. The tag key here is the **bare** name, unlike
  the `tags` param on this same tool, which needs the `S`/`B` prefix.
  Note this is a different meaning of the same underlying `q` param than
  `list_query_tags` uses, where it is a raw `LIKE` pattern.
- An out-of-vocabulary `sort_by` or `fields` entry is refused by the input
  schema before any API call, which is the point of the enum: the API itself
  would silently fall back to `totalTime` for a bad `sort` and silently drop a
  bad `fields` entry, returning plausible wrong data. This is **not** a
  `drive.mjs` check, though. A schema violation comes back as a bare
  `MCP error -32603: Internal error` with no field name, and `drive.mjs` scores
  any rejection as FAIL, so there is nothing for `--expect` to match. That is
  pre-existing behavior for every tool in this server — `list_query_error_patterns`
  with a bad `sort_by` does the same. The enum's contents are covered by
  `src/tools/query-tags.test.ts` instead.
- Omitting `fields` server-side returns every metric the API knows about, which
  is why the tool sends a curated default instead of passing the omission
  through. The accepted vocabulary lives in
  `src/lib/query-summary-fields.ts`; because the tool validates against that
  table with an enum, a metric added upstream is rejected until the table is
  updated. That is the maintenance obligation the enum buys — add the field to
  `SUMMARY_FIELD_KEYS` with the key it is serialized under.
- `trafficControlBudgetsUsed` is accepted by the API but never serialized, so it
  is deliberately absent from the tool's vocabulary. `time_per_query` is the
  reverse: always serialized, never requestable, and derived from `count` and
  `totalTime` — which is one reason both stay in the default field set.
- `dimension_counts` reports `collapsed_count` out of `total_count`: queries
  that carry the grouping tag but whose value Insights had stopped recording for
  cardinality reasons. Those queries are counted and their value is
  unrecoverable, so a `collapsed_count` near `total_count` means the breakdown
  is not trustworthy even though every row looks fine.
- `limit` maps to `per_page` and is clamped to 1–100 (default 25 here; the API's
  own fallback is 100). This endpoint does not paginate — it returns
  `next_page`/`prev_page` as null unconditionally and ignores `page` — which is
  why the tool reports `truncated` instead of a cursor.
- Responses are cached server-side for roughly half a minute. An immediate
  re-drive with different tags or fields comes back fresh, but re-driving the
  *same* call can return the previous answer.
- `from`/`to` wider than 25 hours must be hour-aligned, and a 400 from a
  misaligned range arrives through the same handler as a bad `tags[]` prefix —
  the tool prefers the API's message, so read it rather than assuming which one
  fired. The whole trio shares the range rules; see
  [list-query-tags.md](./list-query-tags.md).
- A tag with a single value returns one row, which proves the plumbing but shows
  nothing about attribution. Pick a tag with several values.
