# Query tags (`list_query_tags`)

Source: `src/tools/query-tags.ts`

Answers "what is annotating the queries on this branch". Returns the query tags
Insights saw — SQLCommenter annotations the application put in SQL comments
(`app`, `controller`, `route`) plus dimensions Insights derived from the
connection (`username`, `application_name`) — each with its values and query
counts.

It is the entry point for the tag trio: the `id` values it returns are the only
way to reach [get-query-tag.md](./get-query-tag.md) and
[list-query-tag-summaries.md](./list-query-tag-summaries.md).

All three tag tools share `buildRequestContext`, `fetchTagsAPI`, and
`compactEntry` in the same source file, and `resultFields`/`resolveEnv`/
`errorMessage` from `src/lib/insights-tools.ts` with the error tools, so a
change to any of those needs every one of those files driven, not just this one.

## Reach it

A user asks their agent "what query tags is this database sending" or "is my app
tagging its queries at all", and the client calls the tool with organization,
database, and branch.

## Drive it

Default path, every tag seen in the last day:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tags \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d"}' \
  --expect '"tags"' --label tags-list-default
```

A pattern search:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tags \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","name_pattern":"%app%"}' \
  --expect '"tags"' --label tags-list-pattern
```

The same search without wildcards, which must come back empty. Run it right
after the one above: the pair is what proves the tool passes the pattern through
rather than helpfully wrapping it, and neither run proves that alone:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tags \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","name_pattern":"app"}' \
  --expect '"returned":0' --label tags-list-pattern-bare
```

One value per tag, which forces the `Other` overflow bucket to appear for any
tag with more than one value:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tags \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","values_limit":1}' \
  --expect '"kind":"overflow"' --label tags-list-overflow
```

The `period`/`from`/`to` conflict, which costs nothing to prove because it is
rejected before any API call:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_tags \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","from":"2026-08-24T00:00:00Z"}' \
  --expect "'period' cannot be combined with 'from'/'to'" --label tags-list-period-conflict
```

## Proves it works

- The response carries `returned`, `truncated`, and a `tags` array; each tag has
  `id`, `name`, `source`, `query_count`, and `values` with `name`,
  `query_count`, and `kind`.
- `id` keeps its one-character origin prefix and `name` is the same string with
  the prefix stripped: `id: "Sapp"` pairs with `name: "app"`. `source` is `sql`
  for an `S` id and `system` for a `B` id.
- `values_limit: 1` produces a `kind: "overflow"` entry named `Other` on any tag
  with more than one recorded value, and `literal_values_only: true` on the same
  call makes it disappear. The tag's own `query_count` still counts the whole
  population either way, so it should not move between those two runs.
- `%app%` returns the tags whose prefixed id contains "app" while a bare `app`
  returns `returned: 0`.
- An `id` from this response feeds both `get_query_tag` and
  `list_query_tag_summaries` and comes back describing the same tag. That round
  trip is the real proof the trio works.

## Notes

- `name_pattern` is a raw SQL `LIKE` pattern matched against the **prefixed**
  id, and the tool deliberately does not add wildcards. `app` matches nothing;
  `%app%` matches `Sapp`; `_app` matches exactly `Sapp` and `Bapp`. An empty
  result from a bare word is the API behaving as documented, not a bug.
- There is no `limit` here, on purpose. The endpoint declares `per_page` and
  then ignores it, returning at most 100 tags always. `truncated: true` means
  narrow with `name_pattern`, `fingerprint`, `keyspace`, or a shorter window —
  there is no limit to raise. Asserting on its absence cannot distinguish "the
  tool omits per_page" from "the tool never called the API", so pair it with a
  successful listing.
- `literal_values_only: true` drops the `Other` bucket along with `Collapsed`,
  and that bucket is the only marker that values past `values_limit` were cut.
  The response's `truncated` counts tags, not values, so a filtered list can be
  partial with nothing saying so.
- `values_limit` defaults to 25 and is clamped to 1–100 by both the tool and the
  API, so an out-of-range value returns results rather than an error.
- `kind` separates three different things: `literal` is a recorded value;
  `overflow` is the synthetic `Other` bucket for values ranked past
  `values_limit`; `collapsed` is the synthetic `Collapsed` bucket counting
  queries where Insights had already stopped recording this tag's values because
  the tag had too many distinct ones. A collapsed value is unrecoverable, not
  zero and not `Other`.
- `from`/`to` ranges longer than 25 hours silently fall back to the default
  24-hour window server-side. Use `period` (`2d`, `7d`, `8d`) for a wider
  window; a wide `from`/`to` that returns recent-looking data is expected.
- A 404 covers two cases: a wrong org/database/branch name, or Insights
  disabled for the database. Check the fixture before filing it as a tool bug.
- A branch whose traffic carries no tags returns `tags: []` with a 200. That is
  a valid response and an unusable fixture — pick a branch with a tagged
  application behind it.
