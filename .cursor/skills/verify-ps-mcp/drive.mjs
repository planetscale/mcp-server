#!/usr/bin/env node
// Drives the local PlanetScale MCP server over stdio and records evidence.
//
//   node .cursor/skills/verify-ps-mcp/drive.mjs doctor
//   node .cursor/skills/verify-ps-mcp/drive.mjs call <tool> '<json args>' [--expect <substring>] [--label <name>]
//
// Every run spawns its own server child process and kills it on exit.

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const EVIDENCE_ROOT = join(REPO_ROOT, ".verify");
const CALL_TIMEOUT_MS = 90_000;

const EXPECTED_TOOLS = [
  "execute_read_query",
  "execute_write_query",
  "get_insights",
  "get_metrics",
  "list_cluster_sizes",
  "search_documentation",
];

function parseArgs(argv) {
  const positional = [];
  const flags = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith("--")) {
      flags[arg.slice(2)] = argv[++i];
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

function evidenceDir(label) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const dir = join(EVIDENCE_ROOT, `${stamp}-${label}`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

function write(dir, name, data) {
  const body = typeof data === "string" ? data : JSON.stringify(data, null, 2);
  writeFileSync(join(dir, name), body.endsWith("\n") ? body : `${body}\n`);
}

function textOf(result) {
  return (result?.content ?? [])
    .filter((part) => part?.type === "text")
    .map((part) => part.text)
    .join("\n");
}

async function connect(dir) {
  const transport = new StdioClientTransport({
    command: join(REPO_ROOT, "node_modules/.bin/tsx"),
    args: [join(REPO_ROOT, "src/server.ts")],
    cwd: REPO_ROOT,
    stderr: "pipe",
  });

  const stderrChunks = [];
  const client = new Client({ name: "verify-ps-mcp", version: "1.0.0" });
  await client.connect(transport);
  transport.stderr?.on("data", (chunk) => stderrChunks.push(chunk.toString()));

  const flushStderr = () => write(dir, "server.stderr.log", stderrChunks.join(""));
  return { client, transport, flushStderr };
}

async function withTimeout(promise, ms, what) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

async function doctor() {
  const dir = evidenceDir("doctor");
  const started = Date.now();
  let session;
  try {
    session = await withTimeout(connect(dir), CALL_TIMEOUT_MS, "connect");
    const { tools } = await withTimeout(
      session.client.listTools(),
      CALL_TIMEOUT_MS,
      "tools/list"
    );
    const names = tools.map((tool) => tool.name).sort();
    const missing = EXPECTED_TOOLS.filter((name) => !names.includes(name));

    write(dir, "tools.json", tools);
    write(dir, "meta.json", {
      command: "doctor",
      server_pid: session.transport.pid ?? null,
      tools: names,
      missing_tools: missing,
      auth_env: {
        PLANETSCALE_OAUTH2_ACCESS_TOKEN: process.env["PLANETSCALE_OAUTH2_ACCESS_TOKEN"]
          ? "set"
          : "missing",
        PLANETSCALE_API_TOKEN_dotenv: "loaded by the server via dotenv; not read here",
      },
      duration_ms: Date.now() - started,
      ok: missing.length === 0,
    });

    if (missing.length > 0) {
      console.error(`FAIL missing tools: ${missing.join(", ")}`);
      console.error(`evidence: ${dir}`);
      return 1;
    }
    console.log(`OK ${names.length} tools registered: ${names.join(", ")}`);
    console.log(`evidence: ${dir}`);
    return 0;
  } finally {
    session?.flushStderr();
    await session?.client.close().catch(() => {});
  }
}

async function call(toolName, rawArgs, flags) {
  const dir = evidenceDir(flags.label ?? toolName);
  const args = rawArgs ? JSON.parse(rawArgs) : {};
  const started = Date.now();
  let session;
  try {
    session = await withTimeout(connect(dir), CALL_TIMEOUT_MS, "connect");
    const result = await withTimeout(
      session.client.callTool({ name: toolName, arguments: args }),
      CALL_TIMEOUT_MS,
      `tools/call ${toolName}`
    );

    const text = textOf(result);
    const expected = flags.expect;
    const matched = expected ? text.includes(expected) : true;

    write(dir, "result.json", result);
    write(dir, "result.txt", text);
    write(dir, "meta.json", {
      command: "call",
      tool: toolName,
      arguments: args,
      server_pid: session.transport.pid ?? null,
      is_error: result.isError ?? false,
      expected_substring: expected ?? null,
      expectation_met: matched,
      duration_ms: Date.now() - started,
      ok: matched,
    });

    console.log(text.slice(0, 2000));
    console.log(`evidence: ${dir}`);
    if (!matched) {
      console.error(`FAIL expected substring not found: ${expected}`);
      return 1;
    }
    console.log(`OK ${toolName}`);
    return 0;
  } finally {
    session?.flushStderr();
    await session?.client.close().catch(() => {});
  }
}

const { positional, flags } = parseArgs(process.argv.slice(2));
const [command, ...rest] = positional;

let code = 1;
if (command === "doctor") {
  code = await doctor();
} else if (command === "call" && rest[0]) {
  code = await call(rest[0], rest[1], flags);
} else {
  console.error("usage: drive.mjs doctor");
  console.error("       drive.mjs call <tool> '<json>' [--expect <substring>] [--label <name>]");
}
process.exit(code);
