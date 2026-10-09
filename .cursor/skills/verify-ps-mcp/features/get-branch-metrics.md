# Branch metrics (`get_branch_metrics`)

Source: `src/tools/get-branch-metrics.ts`

Infrastructure health for a branch: CPU, memory, disk, replication lag,
connections, and, on Neki, router latency. The database kind picks the
endpoint. Vitess reads tablet metrics. Postgres and Neki read branch metrics.
The default `summary` view returns latest, min, avg, and max plus current
values, without raw samples. Query-level stats stay on `get_insights`.

## Reach it

A user asks whether a branch is hot, full, or lagging, or wants the current
disk and connection numbers.

## Drive it

Postgres:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_branch_metrics \
  '{"organization":"mike","database":"macros","branch":"main"}' \
  --expect '"kind":"postgresql"' --label metrics-postgres
```

Vitess:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_branch_metrics \
  '{"organization":"mike","database":"partial-cutover-repro","branch":"main"}' \
  --expect '"kind":"mysql"' --label metrics-vitess
```

Neki:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_branch_metrics \
  '{"organization":"mike","database":"macros-neki","branch":"main"}' \
  --expect '"kind":"neki"' --label metrics-neki
```

## Proves it works

- Each call returns `view: "summary"`, a `series` array, and an `instant` array.
- Postgres series include `planetscale_primary_pods_cpu_util_percentages` and
  instant values include `planetscale_volume_usage_percentage`.
- Vitess series include `primary_cpu_usage` and `replication_lag`.
  `shard_storage_usage` is percent full (`unit: "%"`), not bytes. Instant
  values include `vtgate_cpu_usage`. There is no `planetscale_*` metric.
- Postgres instant disk is `planetscale_volume_usage_percentage`. The series
  name is `planetscale_volume_usage_percentages`. Memory utilization can read
  100% from page cache; the summary also includes RSS and cache bytes.
- Neki series include `planetscale_neki_router_query_latency_p99`.
- Each series row has `latest`, `min`, `avg`, and `max`. Points are absent
  unless `include_points` is true.
- A Vitess call with `primary_cpu_usage` omitted and
  `planetscale_volume_usage_percentage` requested returns an error naming
  Postgres and Neki metrics. A Postgres call with `primary_cpu_usage` returns
  an error naming `planetscale_*` metrics.
