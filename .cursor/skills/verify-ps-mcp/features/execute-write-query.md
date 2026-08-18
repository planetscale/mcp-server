# Write query guards (`execute_write_query`)

Source: `src/tools/execute-write-query.ts`, rules in `src/lib/query-validator.ts`

Runs INSERT/UPDATE/DELETE/DDL against a branch using short-lived credentials.
Before any of that, `validateWriteQuery` decides whether the statement is
allowed, needs human confirmation, or is refused outright.

## Reach it

A user asks their agent to change data. The agent calls the tool; the guard
either runs the query, or hands back a refusal the agent is supposed to relay.

## Drive it

The guard runs before `getDatabase`, before credentials are created, and before
any connection opens, so the refusal paths are drivable without touching data.
This is the part to verify.

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call execute_write_query \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","query":"TRUNCATE TABLE users"}' \
  --expect 'TRUNCATE is not allowed' --label write-guard-truncate
```

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call execute_write_query \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","query":"DELETE FROM users"}' \
  --expect 'DELETE without a WHERE clause is not allowed' --label write-guard-delete-no-where
```

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call execute_write_query \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","query":"UPDATE users SET name = 1 WHERE 1=1"}' \
  --expect 'always-true WHERE clause' --label write-guard-tautology
```

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call execute_write_query \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","query":"DROP TABLE users"}' \
  --expect 'HUMAN CONFIRMATION REQUIRED' --label write-guard-ddl-confirm
```

## Proves it works

- `TRUNCATE` is refused outright, with no confirmation escape hatch.
- `DELETE`/`UPDATE` with no `WHERE`, or with a tautological one, are refused
  outright — `confirm_destructive: true` must not unlock them.
- DDL and `DELETE ... WHERE` return the confirmation prompt when
  `confirm_destructive` is absent.
- No credential is created for any refused query. The tool logs nothing about
  API calls, so confirm this by reading `execute()` (validation returns before
  `getDatabase`) and, after a change to that ordering, by checking the branch's
  passwords and roles in the PlanetScale UI after a refused run.

## Successful writes

Not part of routine verification. Never set `confirm_destructive: true` to make
a check pass. If a change genuinely needs a successful write proven, ask the
user which throwaway database and table to use, then verify the side effect with
a follow-up `execute_read_query` rather than trusting the write's own response.
