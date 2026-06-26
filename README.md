# PlanetScale MCP Server Tools

This repository contains tool implementations that are part of the [PlanetScale MCP server][ps-mcp].

The full hosted MCP server includes additional tools that are generated from
the PlanetScale API OpenAPI spec, not every production tool lives in this
repository.

## Related docs

- Hosted server overview: [PlanetScale MCP docs][ps-mcp]
- PlanetScale API OpenAPI spec: [OpenAPI spec docs][ps-openapi]

## Project scope

This repo is focused on:

- MCP tools we maintain directly in TypeScript
- shared helper code used by those tools
- local development and validation of the open-source portions

Some MCP functionality is intentionally not duplicated here because it is
generated from the API spec and maintained in that generation pipeline.

## Postgres row-level security

The `execute_read_query` tool creates short-lived credentials for each query.
For Postgres branches, those credentials inherit `pg_read_all_data`. That role
can read tables broadly, but it does not bypass Postgres row-level security
(RLS), including tables with `FORCE ROW LEVEL SECURITY`.

This means a valid read query can return zero rows, or a `COUNT(*)` query can
return `0`, because policies filtered rows for the MCP role. Postgres treats
that as normal query behavior rather than an error. When the server detects a
zero-row or zero-count Postgres read while selectable tables have RLS active for
the current MCP role, the JSON response includes a warning like:

```json
{
  "warnings": [
    {
      "code": "postgres_rls_active",
      "message": "Postgres row-level security is active for public.users under this MCP role. The MCP role has pg_read_all_data but does not bypass RLS, so returned rows may be filtered; a zero-row or zero-count result does not necessarily mean the underlying tables are empty.",
      "relations": ["public.users"]
    }
  ]
}
```

The warning is advisory. The MCP role can detect that RLS is active for a table,
but it cannot prove how many rows were hidden without a separate owner,
superuser, or `BYPASSRLS` inspection context.

## Quick start

Install dependencies:

```bash
pnpm install
```

Build a deployment zip:

```bash
pnpm build
```

Push to Gram:

```bash
pnpm push
```

## Testing locally

Run a local MCP server over stdio with inspector support:

```bash
pnpm dev
```

This launches [MCP Inspector][mcp-inspector] so you can interactively test tool
behavior during development.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for contribution workflow and project
conventions.

[mcp-inspector]: https://github.com/modelcontextprotocol/inspector
[ps-mcp]: https://planetscale.com/docs/connect/mcp
[ps-openapi]: https://planetscale.com/docs/api/openapi-spec
