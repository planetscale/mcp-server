# Feature map

One file per user-facing MCP tool. Each answers: what a user gets from it, how
to reach it, how to drive it, and what observable result proves it works.

A proof that exercises only the convenient tool is incomplete when the change
touched others. Check this list before claiming a change is verified.

| Feature | File | Needs auth | Safe to drive |
| --- | --- | --- | --- |
| Query Insights | [get-insights.md](./get-insights.md) | yes | yes, read-only |
| Postgres logs | [get-postgres-logs.md](./get-postgres-logs.md) | yes | yes, read-only |
| Query error patterns | [list-query-error-patterns.md](./list-query-error-patterns.md) | yes | yes, read-only |
| Query error executions | [list-query-error-executions.md](./list-query-error-executions.md) | yes | yes, read-only |
| Read queries | [execute-read-query.md](./execute-read-query.md) | yes | yes, read-only |
| Write query guards | [execute-write-query.md](./execute-write-query.md) | yes | rejection paths only |
| Cluster sizes | [list-cluster-sizes.md](./list-cluster-sizes.md) | yes | yes, read-only |
| Docs search | [search-documentation.md](./search-documentation.md) | no | yes, read-only |

`get_payment_method_setup` and `update_payment_method`
(`src/tools/payment-methods.ts`) are registered but have no file here yet. The
doctor checks they exist; nothing here describes how to drive them.

## Fixtures

Ask the user which organization to use, plus one Vitess (MySQL) database and
one Postgres database that their token can read. Do not invent names or reuse
someone else's org. Several checks below depend on the engine of the branch.

In the drive commands, substitute:

- `YOUR_ORG` — organization slug
- `VITESS_DATABASE` — a MySQL/Vitess database (branch usually `main`)
- `POSTGRES_DATABASE` — a Postgres database (branch usually `main`)
- `KEYSPACE` — a keyspace on the Vitess database, copied from a discovery run
- `ERROR_FINGERPRINT` — an `error_fingerprint` copied from a
  `list_query_error_patterns` run; there is no way to invent one

A branch with no failing queries in the window returns an empty list, which is
a valid response but proves nothing. Ask the user for a branch that actually
has errors before verifying the error tools.

## Keeping this honest

When a tool gains an argument, a response field, or a refusal path, update its
file in the same change. A feature map that describes last quarter's tool sends
the next agent to verify the wrong thing.
