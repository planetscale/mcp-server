# Single query tag (`get_query_tag`)

Source: `src/tools/query-tags.ts`

Answers "what values does this one tag take". Returns a single tag with its
values and their query counts, so a tag found in
[list-query-tags.md](./list-query-tags.md) can be re-read under different
filters — a narrower window, one fingerprint, one keyspace, literals only —
without listing every tag on the branch again.

It cannot be reached without a `TAG_ID` from the listing, and it shares
`buildRequestContext`, `fetchTagsAPI`, and `compactEntry` with the other two tag
tools in the same source file.

## Reach it

An agent has already listed the branch's tags, the user asks "which routes are
in that `route` tag", and the client calls this with the tag's `id`.

## Drive it

Fetch one tag, using a `TAG_ID` copied from a `list_query_tags` run:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_query_tag \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tag":"TAG_ID"}' \
  --expect '"tag"' --label tag-get-default
```

The same tag with the synthetic buckets dropped. `values_limit: 1` is what
creates an `Other` bucket to drop in the first place — without it a tag with few
values has none, and the check passes while proving nothing:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_query_tag \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tag":"TAG_ID","values_limit":1,"literal_values_only":true}' \
  --expect '"kind":"literal"' --label tag-get-literals-only
```

An id stripped of its origin prefix, which is the failure mode a model falls
into after reading `name` instead of `id`:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_query_tag \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","tag":"app"}' \
  --expect 'Query tags not found' --label tag-get-unprefixed
```

## Proves it works

- `values` is always present, as `[]` when the tag has none in the window. That
  is deliberate: every other empty key is compacted away, but here absence would
  be indistinguishable from the tool simply not returning values, which is most
  misleading for a tag whose values were all collapsed and then filtered out by
  `literal_values_only`.
- The response is `{ "tag": { … } }` — a single object, not a list. The API
  returns this tag without the `data` envelope the two list endpoints use, so
  this is the check that the tool handles both response shapes.
- The tag's `id`, `name`, `source`, `query_count`, and `values` match what
  `list_query_tags` reported for the same id over the same window. Comparing the
  two responses is the proof, since neither number means much alone.
- With `values_limit: 1`, `literal_values_only: true` leaves exactly one
  `literal` value and no `Other` entry, while `query_count` on the tag itself
  still counts the whole population — dropping the bucket does not drop its
  queries from the total.
- An unprefixed or unknown id returns the not-found error text rather than an
  empty tag.

## Notes

- The id must keep its `S`/`B` prefix. An unprefixed or unknown id is a **404**,
  not a 400 — only `tags[]` on the summaries endpoint validates the prefix and
  rejects it as a bad request. Both surface as tool error text, so read the
  status in the message to tell them apart.
- A tag whose name contains a `.` cannot be fetched at all: the API's route
  splits the path on it, so the request never reaches the right handler. That is
  an API limit, not a tool bug — reach such a tag through
  `list_query_tags` and `list_query_tag_summaries` instead.
- `literal_values_only: true` also removes the `Other` bucket, which is the only
  signal that values past `values_limit` exist — so its output can be a partial
  value list with nothing marking it as partial. Prefer reading the unfiltered
  response first when the question is "what are all the values".
- There is no `limit` here either; `values_limit` (default 25, clamped 1–100) is
  the only cap, and it bounds literal values only — the `Other` and `Collapsed`
  buckets are returned on top of it.
- `from`/`to` wider than 25 hours falls back to the last 24 hours
  server-side. See the notes in
  [list-query-tags.md](./list-query-tags.md); the whole trio shares that
  behavior.
- A 404 also covers Insights being disabled for the database, so confirm the
  fixture works with `list_query_tags` first — otherwise this tool's 404 looks
  like a bad tag id when it is really a fixture problem.
