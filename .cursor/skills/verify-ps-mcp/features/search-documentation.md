# Docs search (`search_documentation`)

Source: `src/tools/search-documentation.ts`

Searches the PlanetScale knowledge base. This tool is itself an MCP client: it
connects over Streamable HTTP to the docs MCP server (default
`https://planetscale.com/docs/mcp`, overridable with `PLANETSCALE_DOCS_MCP_URL`),
calls `SearchPlanetScale`, and normalizes whatever shape comes back into
`{ title, url, snippet }` entries.

## Reach it

A user asks a "how does PlanetScale do X" question and the agent looks it up
instead of guessing.

## Drive it

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call search_documentation \
  '{"query":"query insights bytes received"}' \
  --expect '"source":"planetscale-docs-mcp"' --label docs-search
```

Narrowed to API reference pages:

```bash
node .cursor/skills/verify-ps-mcp/drive.mjs call search_documentation \
  '{"query":"list branch queries","api_reference_only":true}' \
  --expect '"total"' --label docs-search-api-only
```

## Proves it works

- `total` is greater than zero and `results` entries carry usable `title` and
  `url` values, not empty objects. An empty result set with no `error` means
  normalization dropped everything — that is a failure even though the call
  succeeded.
- No `error` key in the response.

## Notes

- Needs no PlanetScale credentials, which makes it the cheapest end-to-end check
  that the MCP plumbing works in a checkout with no `.env`.
- Failures are returned as `{ results: [], total: 0, error: { message } }`
  rather than thrown, so `--expect '"source"'` alone would pass on a total
  outage. Assert on result content when that distinction matters.
- The upstream server owns the response shape and can change it. The
  normalizers try many key spellings (`results`/`data`/`items`/`documents`/
  `hits`/`entries`, `resource_link` and `text` content blocks); when one stops
  matching, this tool silently returns zero results.
