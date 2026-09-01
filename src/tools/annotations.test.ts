import assert from "node:assert/strict";
import test from "node:test";
import gram from "../gram.ts";

const expectedAnnotations = {
  execute_read_query: [false, false, false],
  execute_write_query: [false, true, false],
  get_insights: [true, false, false],
  get_postgres_logs: [true, false, false],
  list_cluster_sizes: [true, false, false],
  update_payment_method: [false, false, true],
  get_payment_method_setup: [true, false, false],
  list_query_error_patterns: [true, false, false],
  list_query_error_executions: [true, false, false],
  list_query_tags: [true, false, false],
  get_query_tag: [true, false, false],
  list_query_tag_summaries: [true, false, false],
  search_documentation: [true, false, false],
} as const;

test("every custom tool declares its MCP safety annotations", () => {
  const tools = gram.manifest().tools ?? [];

  assert.deepEqual(
    tools.map((tool) => tool.name).sort(),
    Object.keys(expectedAnnotations).sort()
  );

  for (const tool of tools) {
    const expected = expectedAnnotations[
      tool.name as keyof typeof expectedAnnotations
    ];
    assert.ok(expected, `unexpected tool: ${tool.name}`);
    assert.deepEqual(tool.annotations, {
      readOnlyHint: expected[0],
      destructiveHint: expected[1],
      openWorldHint: expected[2],
    });
  }
});
