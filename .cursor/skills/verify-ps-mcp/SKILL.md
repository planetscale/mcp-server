---
name: verify-ps-mcp
description: Drive the local PlanetScale MCP server (stdio) end to end and capture proof that a tool behaves correctly. Use after changing anything in src/tools/ or src/lib/, before pushing to Gram, or when asked to verify/prove MCP tool behavior.
---

# Verify the PlanetScale MCP server

The user-facing surface of this repo is an MCP server spoken over stdio: a client
(Cursor, MCP Inspector, the hosted Gram deployment) starts `src/server.ts`, lists
tools, and calls them. Verification means being that client — start the real
server, call a real tool the way a real client would, and keep the response.

`npm run lint` only proves the code typechecks. It does not prove a tool returns
the right fields, so it is never sufficient as a proof on its own.

## Launch

There is no long-lived server to babysit. Each drive spawns its own
`node_modules/.bin/tsx src/server.ts` child over stdio and kills it when the run
ends, so runs are isolated from each other and can overlap safely.

One-time setup in a fresh checkout:

```bash
npm install
cp .env.example .env   # then fill in PLANETSCALE_API_TOKEN
```

The server loads `.env` itself through `dotenv/config`. Auth resolves in
`src/lib/auth.ts`: `PLANETSCALE_OAUTH2_ACCESS_TOKEN` wins if set, otherwise
`PLANETSCALE_API_TOKEN` (a `<token-id>:pscale_tkn_...` service token) is sent
as-is. Never echo either value into a terminal, a log, or an evidence file.

## Doctor

Run this first whenever anything looks wrong. It is read-only: it starts a
server, initializes the MCP session, lists tools, and checks the six expected
tools are registered.

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs doctor
```

Healthy output names all six tools and exits 0:

```
OK 6 tools registered: execute_read_query, execute_write_query, get_insights, get_postgres_logs, list_cluster_sizes, search_documentation
```

A crash here means the server itself is broken (a bad import, a Zod schema that
throws at construction), not the tool you were investigating. Fix that before
driving a feature.

## Drive

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call <tool> '<json args>' \
  [--expect <substring>] [--label <name>]
```

`--expect` is what turns a drive into a check: the run exits 1 when the
substring is absent from the tool's text response, so a regression fails loudly
instead of printing a wrong answer that looks fine. Choose a substring that only
appears when the behavior actually works — a field name the feature added, or
the exact guard message a rejection should produce.

Example, sorting Insights by the Vitess bytes-received metric. Ask the user
which org and database to use before filling these in:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call get_insights \
  '{"organization":"YOUR_ORG","database":"VITESS_DATABASE","branch":"main","sort_by":"ingressBytes","limit":3,"period":"24h"}' \
  --expect '"ingress_bytes"' --label insights-ingress-vitess
```

Pick the command shape from `features/` rather than inventing it. Those files
do not name a real org or database; ask the user which ones their token can
read.

### Safety while driving

These tools talk to the real PlanetScale API with real credentials. Reads
(`get_insights`, `get_postgres_logs`, `list_cluster_sizes`,
`search_documentation`, `execute_read_query`) are safe against any database you
own.

`execute_write_query` mutates real data and is not a normal verification target.
Its guard rules in `src/lib/query-validator.ts` run before any credential is
created or any connection is opened, so the rejection paths are the part you can
prove for free — driving a blocked `TRUNCATE` touches nothing. If a change
genuinely requires proving a successful write, ask the user which throwaway
database to use and say so in the proof.

## Evidence

Every run writes a timestamped directory under `.verify/` at the repo root
(gitignored) containing:

- `meta.json` — tool, arguments, server pid, duration, whether `--expect` matched
- `result.json` — the full MCP tool result
- `result.txt` — just the text content, which is what a client shows the model
- `tools.json` — the tool manifest (doctor runs only)
- `server.stderr.log` — the server's stderr for the run

A call the server rejects outright — schema validation, a transport failure, a
timeout — produces no tool result, so `result.json` and `result.txt` are absent
and `meta.json` carries an `error` object instead. The run still exits 1, so a
rejection is a loud failure rather than a crash with no record.

A proof cites the evidence directory and quotes the fields that matter. Standards
for what counts:

- Drive the tool through the MCP session, the way a client does. Importing the
  Gram instance and calling `handleToolCall` in a scratch script skips
  registration, schema validation, and serialization, so it proves less.
- Show the state that resulted, not only that a call returned 200. For a new
  response field, quote the field and its value from `result.txt`.
- Verify a claim on the branch it applies to. A capability that is MySQL-only
  needs both halves: the Vitess branch returning data, and the Postgres branch
  returning the refusal.
- Don't mock the PlanetScale API. It is the thing under test here.

## Cleanup

Each run kills the child it spawned, so normally there is nothing to clean.
Confirm and, if something was interrupted, kill the pid recorded in `meta.json`
rather than matching on a process name (a name match would also kill the user's
own `npm run dev` inspector session):

```bash
pgrep -fl 'src/server.ts' || echo 'no server processes left'
```

Evidence outlives cleanup. Prune `.verify/` only when you deliberately want the
old runs gone, never as part of finishing a verification.

## Helpers

`drive.mjs` is executable and is the only helper this skill ships. It resolves
the repo root from its own path, so it works from any working directory.
