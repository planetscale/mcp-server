# Query error patterns (`list_query_error_patterns`)

Source: `src/tools/query-errors.ts`

Answers "what is failing on this branch". Returns failing queries aggregated by
error fingerprint, each with the error message, how many times it occurred, when
it was last seen, and total/average duration. It is the entry point for the
error pair: the `error_fingerprint` values it returns are the only way to reach
[list-query-error-executions.md](./list-query-error-executions.md).

Both error tools share `buildRequestContext`, `fetchErrorsAPI`, and
`resultFields` in the same source file, so a change to any of those needs both
files driven, not just this one.

## Reach it

A user asks their agent "what queries are erroring on main" or "why is this
branch throwing", and the client calls the tool with organization, database,
and branch.

## Drive it

Default path, most frequent errors over the last day:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_error_patterns \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d"}' \
  --expect '"sort_by":"count"' --label errors-patterns-default
```

Sorted by most recent, with an explicit limit:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_error_patterns \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","sort_by":"lastRun","limit":5}' \
  --expect '"sort_by":"lastRun"' --label errors-patterns-lastrun
```

The `period`/`from`/`to` conflict, which costs nothing to prove because it is
rejected before any API call:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_error_patterns \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","from":"2026-08-24T00:00:00Z"}' \
  --expect "'period' cannot be combined with 'from'/'to'" --label errors-patterns-period-conflict
```

## Proves it works

- The response carries `sort_by`, `returned`, `truncated`, and a `patterns`
  array; each pattern has `error_fingerprint`, `error_message`, `error_count`,
  `started_at`, `total_duration_millis`, and `time_per_query`.
- `sort_by` echoes back what was asked, defaulting to `"count"` when omitted.
- `truncated` is `true` exactly when `returned` equals the effective limit.
  Drive it with `limit: 1` against a branch with several error patterns to see
  the true case, and with a high limit to see the false case.
- The fingerprint from this response feeds
  `list_query_error_executions` and comes back with matching `error_message`
  values. That round trip is the real proof the pair works.

## Notes

- `limit` maps to `per_page` and is clamped to 1–100 (default 25). These
  endpoints do not paginate: the API returns `next_page`/`prev_page` as null
  unconditionally and ignores `page`, which is why the tool reports `truncated`
  instead of a cursor. If a change adds pagination, this is the assumption that
  breaks.
- `from`/`to` ranges longer than 25 hours silently fall back to the default
  24-hour window server-side, so a wide range that returns recent-looking data
  is expected behavior, not a bug.
- `tablet_type` filters here but has no counterpart on the executions endpoint.
- A branch with no errors in the window returns `patterns: []` with a 200. That
  is not a failing check; it is an unusable fixture. Pick a branch with errors.
