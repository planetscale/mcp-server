import { Gram } from "@gram-ai/functions";
import { z } from "zod";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const DEFAULT_MCP_URL = "https://planetscale.com/mcp";

type NormalizedResult = {
  title?: string;
  url?: string;
  snippet?: string;
  text?: string;
  metadata?: Record<string, unknown>;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function pickFirstString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value === "string" && value.trim().length > 0) {
      return value;
    }
  }
  return undefined;
}

function normalizeEntry(entry: unknown): NormalizedResult {
  if (typeof entry === "string") {
    return { text: entry };
  }

  if (!isRecord(entry)) {
    return {};
  }

  const title = pickFirstString(
    entry["title"],
    entry["name"],
    entry["heading"],
    entry["page_title"]
  );
  const url = pickFirstString(
    entry["url"],
    entry["href"],
    entry["link"],
    entry["uri"]
  );
  const snippet = pickFirstString(
    entry["snippet"],
    entry["summary"],
    entry["excerpt"],
    entry["content"],
    entry["text"]
  );

  const metadata: Record<string, unknown> = { ...entry };
  for (const key of ["title", "name", "heading", "page_title", "url", "href", "link", "uri", "snippet", "summary", "excerpt", "content", "text"]) {
    if (key in metadata) {
      delete metadata[key];
    }
  }

  return {
    title,
    url,
    snippet,
    metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
  };
}

function extractResultsFromStructuredContent(
  structuredContent: unknown
): NormalizedResult[] {
  if (Array.isArray(structuredContent)) {
    return structuredContent.map(normalizeEntry);
  }

  if (isRecord(structuredContent)) {
    const candidates = [
      structuredContent["results"],
      structuredContent["data"],
      structuredContent["items"],
      structuredContent["documents"],
      structuredContent["hits"],
      structuredContent["entries"],
    ];

    for (const candidate of candidates) {
      if (Array.isArray(candidate)) {
        return candidate.map(normalizeEntry);
      }
    }

    return [normalizeEntry(structuredContent)];
  }

  if (typeof structuredContent === "string") {
    return [{ text: structuredContent }];
  }

  return [];
}

function extractResultsFromContent(content: unknown): NormalizedResult[] {
  if (!Array.isArray(content)) {
    return [];
  }

  const results: NormalizedResult[] = [];
  for (const item of content) {
    if (!isRecord(item) || typeof item["type"] !== "string") {
      continue;
    }

    if (item["type"] === "resource_link") {
      results.push({
        title: pickFirstString(item["name"]),
        url: pickFirstString(item["uri"]),
        snippet: pickFirstString(item["description"]),
      });
      continue;
    }

    if (item["type"] === "text") {
      results.push({ text: pickFirstString(item["text"]) });
    }
  }

  return results.filter((result) =>
    result.title || result.url || result.snippet || result.text
  );
}

function getDocsMcpUrl(env: Record<string, string | undefined>): string {
  const url = env["PLANETSCALE_DOCS_MCP_URL"]?.trim();
  if (!url) {
    return DEFAULT_MCP_URL;
  }

  try {
    return new URL(url).toString();
  } catch {
    throw new Error("PLANETSCALE_DOCS_MCP_URL must be a valid URL");
  }
}

export const searchDocumentationGram = new Gram().tool({
  name: "search_documentation",
  description:
    "Search across the PlanetScale knowledge base to find relevant information, code examples, API references, and guides. Use this tool when you need to answer questions about PlanetScale, find specific documentation, understand how features work, or locate implementation details. The search returns contextual content with titles and direct links to the documentation pages.",
  inputSchema: {
    query: z.string().describe("Search query for PlanetScale docs"),
    version: z
      .string()
      .optional()
      .describe("Optional version filter (e.g., 'v0.7')"),
    language: z
      .string()
      .optional()
      .describe("Optional language filter (e.g., 'en', 'es')"),
    api_reference_only: z
      .boolean()
      .optional()
      .describe("Only return API reference docs"),
    code_only: z
      .boolean()
      .optional()
      .describe("Only return code snippets"),
  },
  async execute(ctx, input) {
    const env =
      Object.keys(ctx.env).length > 0
        ? (ctx.env as Record<string, string | undefined>)
        : process.env;

    let transport: StreamableHTTPClientTransport | undefined;

    try {
      const mcpUrl = getDocsMcpUrl(env);
      const client = new Client({ name: "planetscale-docs-wrapper", version: "1.0.0" });
      transport = new StreamableHTTPClientTransport(new URL(mcpUrl));

      await client.connect(transport);

      const result = await client.callTool({
        name: "SearchPlanetScale",
        arguments: {
          query: input.query,
          version: input.version,
          language: input.language,
          apiReferenceOnly: input.api_reference_only,
          codeOnly: input.code_only,
        },
      });

      const normalizedResults = result.structuredContent
        ? extractResultsFromStructuredContent(result.structuredContent)
        : extractResultsFromContent(result.content);

      return ctx.json({
        results: normalizedResults,
        total: normalizedResults.length,
        source: "planetscale-docs-mcp",
      });
    } catch (error) {
      if (error instanceof Error) {
        return ctx.json({
          results: [],
          total: 0,
          error: { message: error.message },
          source: "planetscale-docs-mcp",
        });
      }
      return ctx.json({
        results: [],
        total: 0,
        error: { message: "An unexpected error occurred" },
        source: "planetscale-docs-mcp",
      });
    } finally {
      if (transport) {
        await transport.close();
      }
    }
  },
});
