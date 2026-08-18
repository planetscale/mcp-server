# Query Insights (`get_insights`)

Source: `src/tools/get-insights.ts`

Answers "which queries are hurting this branch". Returns the top query patterns
for a branch, either aggregated across curated metrics (`sort_by: "all"`, the
default) or sorted by one metric. Passing a `fingerprint` plus `keyspace`
switches to drill-down mode and returns summary stats with individual
executions.

## Reach it

A user asks their agent something like "what are the slowest queries on this
branch" and the client calls the tool with organization, database, and branch.

## Drive it

Aggregate mode, the default path most calls take:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_insights \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","period":"24h"}' \
  --expect '"mode":"aggregated"' --label insights-aggregate
```

Single metric, Vitess bytes received:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_insights \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","sort_by":"ingressBytes","limit":3,"period":"24h"}' \
  --expect '"ingress_bytes"' --label insights-ingress-vitess
```

Drill-down, using a `fingerprint` and `keyspace` copied from a discovery run:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_insights \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","fingerprint":"<hash>","keyspace":"KEYSPACE"}' \
  --expect '"mode":"fingerprint"' --label insights-fingerprint
```

## Proves it works

- Aggregate mode returns `"mode":"aggregated"` with a non-empty `queries` array.
- `sort_by: "ingressBytes"` on a Vitess branch returns all three bytes-received
  fields with non-zero values: `ingress_bytes`, `ingress_bytes_per_query`,
  `max_ingress_bytes`. It also echoes `branch_capabilities` with
  `"kind":"mysql"` and `"ingress_bytes":true`.
- Drill-down returns `"mode":"fingerprint"` with a `summary` object and an
  `executions` list.

## Engine-gated metrics

`getBranchCapabilities` reads the branch first and refuses sorts the branch
cannot serve, so both halves need checking when this logic changes:

| Sort | Available on | Refusal to expect elsewhere |
| --- | --- | --- |
| `cpuTime` | Postgres | "only available for Postgres branches" |
| `maxEgressBytes` | MySQL | "only available for MySQL branches" |
| `ingressBytes`, `ingressBytesPerQuery`, `maxIngressBytes` | MySQL/Vitess | "only available for Vitess/MySQL branches" |

The Postgres half of the ingress gate:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_insights \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","sort_by":"ingressBytes","period":"24h"}' \
  --expect 'only available for Vitess/MySQL branches' --label insights-ingress-postgres
```

## Notes

- `filterEntry` drops zero-valued and empty fields, so a metric that is present
  but zero (ingress on Postgres) is absent from results rather than shown as 0.
  Asserting on the absence of a field cannot distinguish "not collected" from
  "collected and zero"; assert on the refusal message instead.
- `period` cannot be combined with `from`/`to`; the tool returns an error string
  saying so.
- `limit` is capped at 20 regardless of what the caller asks for.
- Field names match the PlanetScale OpenAPI spec (`ingress_bytes` in responses,
  `ingressBytes` as the sort key). When adding a metric, confirm both spellings
  against <https://planetscale.com/docs/openapi.yaml>.
