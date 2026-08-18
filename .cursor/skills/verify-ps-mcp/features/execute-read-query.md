# Read queries (`execute_read_query`)

Source: `src/tools/execute-read-query.ts`, helpers in `src/lib/query-executor.ts`
and `src/lib/planetscale-api.ts`

Runs a read-only SQL query against a branch. The tool mints short-lived
credentials (a Vitess `reader` password, or a Postgres role with
`pg_read_all_data`), runs the query, then deletes the credential. Reads prefer a
replica when the branch has one, unless `use_replica: false`.

## Reach it

A user asks their agent a question about their data and the agent runs SQL for
them.

## Drive it

Vitess:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call execute_read_query \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","query":"SELECT 1 AS ok"}' \
  --expect 'ok' --label read-vitess
```

Postgres:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call execute_read_query \
  '{"organization":"YOUR_ORG","database":"POSTGRES_DATABASE","branch":"main","query":"SELECT 1 AS ok"}' \
  --expect 'ok' --label read-postgres
```

Forcing the primary:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call execute_read_query \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","query":"SELECT 1 AS ok","use_replica":false}' \
  --expect 'ok' --label read-primary
```

## Proves it works

- The result carries the queried rows, not just a success flag.
- Both engines work; they take different code paths from the same tool.
- The credential lifecycle completed. A leaked password or role is the real
  failure mode here, and it is invisible in the tool response — check the
  branch's passwords/roles in the PlanetScale UI or API after a run that
  changed `src/lib/planetscale-api.ts`.

## Notes

- Queries are cancelled at 50 seconds (`QueryTimeoutError`); the tool returns
  that as an error string rather than throwing.
- The Postgres role obeys row-level security, so zero rows can mean "hidden by
  policy". The tool adds warnings when it detects that risk; a change to
  `warnOnRls` needs an RLS-protected table to verify against.
- `postgres_database_name` targets a non-default database in the same cluster.
  Errors mentioning a missing database get an extra hint appended.
