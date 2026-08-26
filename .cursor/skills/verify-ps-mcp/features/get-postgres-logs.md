# Postgres logs (`get_postgres_logs`)

Source: `src/tools/get-postgres-logs.ts`, signature call in
`src/lib/planetscale-api.ts` (`createLogSignature`)

Answers "what is this Postgres branch's server actually doing". Returns recent
log entries newest-first, filterable by log level, time window, server role,
and pod name, plus an optional raw LogsQL filter that may carry pipe stages. Postgres/Neki only: the tool takes two hops, first POSTing to
`/logs/signatures` for a signed URL, then fetching NDJSON from that URL, and a
Vitess/MySQL branch has no signature to issue.

## Reach it

A user asks their agent something like "any errors on this branch in the last
hour" and the client calls the tool with organization, database, and branch.

## Drive it

The default path — last hour, all levels:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","limit":5}' \
  --expect '"query":"* _time:1h | sort by (_time desc) | offset 0"' --label logs-default
```

Structured filters, level and role:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","levels":["ERROR","WARNING"],"role":"primary","period":"24h","limit":5}' \
  --expect '(planetscale.level:ERROR OR planetscale.level:WARNING) planetscale.role:primary' --label logs-level-role
```

A pod name is quoted, so a value with a space cannot split into two terms:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","role":"replica","pods":["pod name"],"period":"24h","limit":5}' \
  --expect 'planetscale.role:replica (planetscale.pod:\"pod name\")' --label logs-role-pod
```

An explicit time range, which must normalize to ISO 8601:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","from":"2026-08-17T00:00:00Z","to":"2026-08-17T01:00:00Z","limit":5}' \
  --expect '_time:[2026-08-17T00:00:00.000Z, 2026-08-17T01:00:00.000Z]' --label logs-time-range
```

Pagination, where page 2 of 3 becomes `offset 3`:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","period":"24h","limit":3,"page":2}' \
  --expect 'offset 3' --label logs-page-2
```

An over-max `limit`, which is clamped to 1000 rather than refused:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","limit":5000,"page":2}' \
  --expect 'offset 1000' --label logs-clamp-max
```

## Proves it works

- The response echoes the assembled LogsQL as `query`. That echo is the cheapest
  observable for every query-building change — assert on it rather than on which
  log lines came back, which nobody controls.
- `logs` entries carry the mapped field names, not the wire ones: `time`,
  `level`, `message`, `pod`, `role`, `container`, `availability_zone`. A row
  still holding a `planetscale.` prefix means `parseLogLine` stopped mapping.
- `message` is the inner `message` value when `_msg` holds JSON, and the raw
  `_msg` text when it does not. Both shapes occur in one branch's logs; scan
  `result.txt` for a line that is bare prose rather than a JSON blob.
- `total` counts returned entries. `has_next` comes from fetching one raw line
  beyond `limit`, so a `limit:3` call on a busy branch returns
  `"total":3,"has_next":true`, while the page holding the final rows reports
  `has_next:false` even when it is exactly full.
- A quiet branch legitimately returns `"logs":[]`. That is not a failure, and it
  is also not a proof — use `period:"24h"` or a busier branch to get rows.

## Query building

`buildLogsQuery` wraps the caller's filter in parentheses before ANDing the
structured filters on, and `splitPipeStages` only splits `|` outside quotes
and parentheses. Both exist because getting them wrong changes results
silently rather than erroring, so both need a check when that code moves.

A caller filter containing a top-level `OR` must come back parenthesized —
unwrapped, the `OR` would bind looser than the appended `_time` filter and match
outside the window:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","query":"checkpoint OR connection","period":"24h","limit":5}' \
  --expect '"query":"(checkpoint OR connection) _time:24h' --label logs-or-precedence
```

A `|` inside a quoted regex is part of the filter, not a pipe stage. It must
stay inside the wrapped filter, ahead of `_time`:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","query":"_msg:~\"error|warning\"","period":"24h","limit":5}' \
  --expect 'error|warning\") _time:24h' --label logs-regex-pipe
```

A `|` inside parentheses is also part of the filter — every `in(...)` subquery
ends with a mandatory `| fields` pipe, which must not be split into a stage:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","query":"level:in(error | fields level)","period":"24h","limit":5}' \
  --expect '(level:in(error | fields level)) _time:24h' --label logs-subquery-pipe
```

A real pipe stage is kept, in order, and the tool's own
`sort by (_time desc) | offset` stages are appended after it:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","query":"error | stats count()","period":"24h"}' \
  --expect '| stats count() | sort by (_time desc)' --label logs-pipe-stage
```

Aggregating stages produce rows with no `_msg`. Those are surfaced as a single
`message` holding the raw JSON line rather than dropped, so the `stats` drive
above returns a `logs` array with one entry containing a count.

## Refusals

Every one of these returns before the logs fetch, so they cost nothing to drive.

| Input | Expect |
| --- | --- |
| Vitess/MySQL branch | `logs are only available for Postgres databases` |
| `period` plus `from`/`to` | `'period' cannot be combined with 'from'/'to'` |
| `from` without `to` | `'from' and 'to' must be provided together` |
| `period:"1 hour"` | `invalid 'period' value` |
| `from:"yesterday"` | `invalid 'from' value` |
| `from` later than `to` | `'from' (…) must not be later than 'to'` |

The engine gate is the half most likely to rot, since it depends on the API
404ing rather than on local logic:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main"}' \
  --expect 'logs are only available for Postgres databases' --label logs-vitess-refusal
```

A spaced duration is rejected locally rather than interpolated, where LogsQL
would have read `1 hour` as a duration plus a stray word filter:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_postgres_logs \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","period":"1 hour"}' \
  --expect "invalid 'period' value" --label logs-bad-period
```

## Notes

- Errors come back as `ctx.text` strings with an `Error:` prefix and a normal
  `isError: false` result, so `meta.json` shows `ok` purely from `--expect`.
  Always pass `--expect` here; a refused call otherwise looks like a pass.
- The signed URL from `/logs/signatures` is the credential and is short-lived;
  no auth header goes to the logs endpoint. Nothing in `result.json` echoes that
  URL today, so evidence files are safe to read — but if a change starts
  surfacing it, treat its `sig`/`exp` pair as a secret and keep it out of a
  proof.
- `limit` is clamped to 1..1000 and `page` to a minimum of 1, so an
  out-of-range value is corrected rather than refused: `{"limit":5000,"page":2}`
  echoes `offset 1000` and `{"limit":0,"page":3}` echoes `offset 2`.
- A fractional `limit` or `page` never reaches that clamp. The schema marks both
  `.int()`, and this repo leaves Gram's `lax` mode off (a tool bakes in the
  setting of the `new Gram()` it was defined on, so enabling it in `src/gram.ts`
  would not loosen this tool), so validation refuses the call. That refusal is a
  transport-level `MCP error -32603`, not an `Error:` string in the response, so
  `--expect` cannot match it — the drive exits 1 and records the error in
  `meta.json` with no `result.json`.
- Upstream failures from the logs endpoint are surfaced as
  `Failed to fetch logs (status: N)` carrying the service's own message, so a
  broken LogsQL query shows its parse error — useful when a query-building
  change goes wrong. A `query` of `error | notastage` provokes one. Only the
  first 4096 characters of that body are reported, with ` … (truncated)`
  appended, because the service decides its length and the whole of it would
  otherwise reach the model.
