# Feature map

One file per user-facing MCP tool. Each answers: what a user gets from it, how
to reach it, how to drive it, and what observable result proves it works.

A proof that exercises only the convenient tool is incomplete when the change
touched others. Check this list before claiming a change is verified.

| Feature | File | Needs auth | Safe to drive |
| --- | --- | --- | --- |
| Query Insights | [get-insights.md](./get-insights.md) | yes | yes, read-only |
| Read queries | [execute-read-query.md](./execute-read-query.md) | yes | yes, read-only |
| Write query guards | [execute-write-query.md](./execute-write-query.md) | yes | rejection paths only |
| Cluster sizes | [list-cluster-sizes.md](./list-cluster-sizes.md) | yes | yes, read-only |
| Docs search | [search-documentation.md](./search-documentation.md) | no | yes, read-only |

## Fixtures

Ask the user which organization to use, plus one Vitess (MySQL) database and
one Postgres database that their token can read. Do not invent names or reuse
someone else's org. Several checks below depend on the engine of the branch.

In the drive commands, substitute:

- `YOUR_ORG` — organization slug
- `VITESS_DATABASE` — a MySQL/Vitess database (branch usually `main`)
- `POSTGRES_DATABASE` — a Postgres database (branch usually `main`)
- `KEYSPACE` — a keyspace on the Vitess database, copied from a discovery run

## Keeping this honest

When a tool gains an argument, a response field, or a refusal path, update its
file in the same change. A feature map that describes last quarter's tool sends
the next agent to verify the wrong thing.
