# Live connections (`list_live_connections`)

Source: `src/tools/live-connections.ts`

Lists live sessions on a branch. Postgres and Neki return every instance the API
reached. Vitess returns one primary tablet: pass `keyspace` and, when that
keyspace is sharded, `shard`. A missing target comes back as `needs_target`
with `available.keyspaces` or `available.shards`, not as a hard error.

The tool never calls the DELETE kill or cancel routes. Those stay off the
public OpenAPI spec, so Gram does not generate tools for them.

Query text is omitted unless `include_query_text` is true, and then cut at 500
characters. Do not poll faster than every few seconds; a 503 means the 1 second
list lock is held.

A service token is rejected. The call needs the `read_live_connections` OAuth
scope, plus `read_production_live_connections` on a production branch. That
scope ships in the api-bb change this tool depends on, so a drive before that
lands returns 403.

## Reach it

A user asks what is connected, which session is oldest, or which query has been
running the longest.

## Drive it

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_live_connections \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","limit":5}' \
  --expect '"database_kind":"postgresql"' --label connections-postgres
```

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_live_connections \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main"}' \
  --expect '"needs_target":true' --label connections-vitess-target
```

Use the keyspace and shard from that response:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_live_connections \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","keyspace":"KEYSPACE","shard":"SHARD","include_query_text":true,"limit":5}' \
  --expect '"database_kind":"mysql"' --label connections-vitess
```

## Proves it works

- Postgres returns `database_kind`, `captured_at`, `instances`, `summary`, and
  `rows`. `summary.blocked_count` counts rows whose `blocked_by` is non-empty.
- Vitess without a target returns `needs_target` and `available`, with empty
  `rows`. The same call with `keyspace` and `shard` returns `target.tablet`.
- `rows` omit `query_text` until `include_query_text` is true.
- The tool list has `list_live_connections` and no kill, cancel, or terminate
  tool.
