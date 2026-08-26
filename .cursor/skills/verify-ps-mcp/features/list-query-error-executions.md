# Query error executions (`list_query_error_executions`)

Source: `src/tools/query-errors.ts`

Answers "show me the actual queries behind this error". Given an
`error_fingerprint`, returns the individual captured executions that failed with
it: normalized SQL, tables, keyspace, user, row counts, duration, error message,
and query tags.

This tool is the second half of a pair. The fingerprint it requires only comes
from [list-query-error-patterns.md](./list-query-error-patterns.md), and both
tools share `buildRequestContext`, `fetchErrorsAPI`, and `resultFields` in the
same source file — a change to any of those needs both files driven.

## Reach it

A user has an error pattern in hand and asks "which queries are causing that"
or "what tables does that error touch". The agent has usually just called
`list_query_error_patterns` in the same turn.

## Drive it

Get a fingerprint first — this is the whole round trip, and running it as two
steps is the point:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_error_patterns \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","limit":3}' \
  --label errors-patterns-discovery
```

Copy an `error_fingerprint` out of that run's `result.txt`, then:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_error_executions \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","error_fingerprint":"ERROR_FINGERPRINT"}' \
  --expect '"executions"' --label errors-executions
```

The unknown-fingerprint path. The API answers 200 with nothing in it rather
than 404, so this is an empty-list check, not an error check:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call list_query_error_executions \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d","error_fingerprint":"not-a-real-fingerprint"}' \
  --expect '"executions":[]' --label errors-executions-unknown
```

## Proves it works

- The response echoes the `error_fingerprint` that was asked for and carries
  `returned`, `truncated`, and an `executions` array.
- Executions match the pattern they came from: their `error_message` values are
  the same message `list_query_error_patterns` reported for that fingerprint,
  and `returned` matches the pattern's `error_count` when the window and limit
  allow all of them through. Quote both in the proof — an executions list that
  does not correspond to its pattern is the failure this check exists to catch.
- Each execution carries the fields a user needs to act: `normalized_sql`,
  `keyspace`, `username`, `statement_type`, the row counts, and
  `total_duration_millis`. `tables` and `tags` are only present when the API
  captured them — a failed statement that never resolved a table, and an
  application that sends no query tags, both drop out here.
- Nothing else leaks through. `filterErrorExecution` allowlists fields, so the
  API's syntax-highlighted HTML variants and extra timestamps must be absent
  from `result.txt`. Grep the evidence for `<span` to confirm.

## Notes

- Empty and null-valued fields are dropped, and so are empty arrays. An
  execution with no query tags has no `tags` key rather than `tags: []`, so
  asserting on absence cannot distinguish "not captured" from "empty".
- There is no `tablet_type` filter here even though the patterns tool has one.
  Executions from every tablet type come back, including when the fingerprint
  was discovered through a tablet-filtered pattern list. The tool description
  says so; if that ever changes, both files need updating.
- `limit`, `period`, `from`/`to`, and the no-pagination `truncated` behavior are
  identical to the patterns tool — see the notes in
  [list-query-error-patterns.md](./list-query-error-patterns.md) rather than
  duplicating them here.
- An unknown fingerprint is not an error path: the API returns 200 with an
  empty list, so `executions: []` means either "no such fingerprint" or "no
  executions in this window" and the two cannot be told apart. The 404 handler
  in `fetchErrorsAPI` fires for a bad organization, database, or branch, or for
  Insights being disabled — not for a bad fingerprint.
- Each execution's `fingerprint` is the *query* fingerprint, which is a
  different value from the `error_fingerprint` that was requested. Do not
  expect them to match; the response echoes `error_fingerprint` at the top
  level for that correlation.
