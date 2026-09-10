import assert from "node:assert/strict";
import test from "node:test";
import {
  type PostgresCredentials,
  waitForPostgresRoleReady,
} from "./planetscale-api.ts";

function credentials(ready: boolean): PostgresCredentials {
  return {
    id: "role-1",
    username: "user",
    password: "create-only-password",
    host: "example.test",
    database_name: "postgres",
    ready,
    branch: { id: "branch-1", name: "main" },
  };
}

test("waitForPostgresRoleReady keeps the create response password", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return Response.json({ ready: calls === 2 });
  };

  try {
    const created = credentials(false);
    const result = await waitForPostgresRoleReady(
      "acme",
      "database",
      "main",
      created,
      "Bearer token",
      undefined,
      { pollIntervalMs: 0, timeoutMs: 1_000 }
    );

    assert.equal(result, created);
    assert.equal(result.ready, true);
    assert.equal(result.password, "create-only-password");
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("waitForPostgresRoleReady skips polling when already ready", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new Error("fetch should not be called");
  };

  try {
    const created = credentials(true);
    assert.equal(
      await waitForPostgresRoleReady(
        "acme",
        "database",
        "main",
        created,
        "Bearer token"
      ),
      created
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});
