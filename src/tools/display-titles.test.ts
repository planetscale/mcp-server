import assert from "node:assert/strict";
import test from "node:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { fromGram } from "@gram-ai/functions/mcp";
import gram from "../gram.ts";

test("MCP tools/list preserves every custom tool's display title", async () => {
  const server = fromGram(gram, { name: "title-test", version: "1.0.0" });
  const client = new Client({ name: "title-test-client", version: "1.0.0" });
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();

  try {
    await server.connect(serverTransport);
    await client.connect(clientTransport);
    const { tools } = await client.listTools();
    const expected = gram.manifest().tools ?? [];
    assert.equal(tools.length, expected.length);

    for (const definition of expected) {
      const tool = tools.find((candidate) => candidate.name === definition.name);
      assert.ok(tool, `missing MCP tool: ${definition.name}`);
      const title = tool.annotations?.title;
      assert.ok(typeof title === "string" && title.trim().length > 0,
        `missing MCP display title: ${definition.name}`);
      assert.equal(title, definition.annotations?.title);
    }
  } finally {
    await client.close();
    await server.close();
  }
});
