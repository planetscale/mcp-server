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
