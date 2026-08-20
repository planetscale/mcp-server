# Branch metrics (`get_metrics`)

Source: `src/tools/get-metrics.ts`

Answers "how is this branch performing" and "what is this metric doing". One
tool with three modes, the same shape as `get_insights`:

- `mode: "report"` (default) — curated MySQL/Postgres overview
- `mode: "series"` — historical time series for named metrics
- `mode: "instant"` — current gauge values for named metrics

Series values are summarized (`latest`, `min`, `avg`, `max`) unless
`include_points` is true.

## Reach it

A user asks how a branch is looking, or for query volume / disk usage, and the
client calls this tool with organization, database, and branch.

## Drive it

Default report, MySQL/Vitess:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_metrics \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1d"}' \
  --expect '"mode":"report"' --label metrics-report-mysql
```

Postgres report, which includes instant capacity sections:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_metrics \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","period":"1h"}' \
  --expect '"kind":"instant"' --label metrics-report-postgres
```

Named historical series:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_metrics \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","mode":"series","metric":["queries","latency_p99"],"period":"1h"}' \
  --expect '"mode":"series"' --label metrics-series-summary
```

Raw samples when explicitly requested:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_metrics \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","mode":"series","metric":["queries"],"period":"15m","include_points":true}' \
  --expect '"points"' --label metrics-series-points
```

Current volume usage:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_metrics \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","mode":"instant","metric":["planetscale_volume_usage_percentage"]}' \
  --expect '"mode":"instant"' --label metrics-instant-volume
```

Range validation:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_metrics \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"1h","from":"2026-01-01T00:00:00Z","to":"2026-01-01T01:00:00Z"}' \
  --expect 'period cannot be combined with from/to' --label metrics-range-guard
```

## Proves it works

- Default calls return `"mode":"report"` and `"type":"MetricsReport"`, with
  `engine` matching the database (`mysql` or `postgresql`).
- MySQL reports have seven `kind: "series"` sections and no instant sections.
- Postgres reports include 12 series sections and 3 `kind: "instant"` sections.
  Series storage uses `planetscale_volume_usage_percentages` (plural); instant
  storage uses `planetscale_volume_usage_percentage` (singular).
- Series mode returns `"mode":"series"` with `latest` / `min` / `avg` / `max` /
  `unit` and no `points` unless `include_points` is true.
- Instant mode returns `"mode":"instant"` with a `metrics` array of dimension
  maps plus `value`.
- Combining `period` with `from`/`to` returns an error string. Omitting
  `metric` in series or instant mode returns `metric is required`.

## Notes

- Report default `period` is `1d`. Series mode omits period and the API defaults
  to 12h. Valid values: `15m`, `1h`, `3h`, `6h`, `12h`, `1d`, `2d`, `7d`, `8d`.
- `from` and `to` must be used together and cannot be combined with `period`.
- Report section fetches run in parallel. A failure in any section fails the
  whole report.
- Series filters (`tablet_type`, `keyspace`, `query_id`, `fingerprint`, and so
  on) are ignored in report and instant modes.
