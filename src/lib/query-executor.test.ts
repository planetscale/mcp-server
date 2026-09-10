import assert from "node:assert/strict";
import test from "node:test";
import type { PostgresCredentials } from "./planetscale-api.ts";
import { postgresConnectionUrl } from "./query-executor.ts";

function credentials(
  databaseKind: "postgresql" | "neki"
): PostgresCredentials {
  return {
    id: "role-1",
    username: "user.branch",
    password: "password",
    host: "example.test",
    database_name: "postgres",
    ready: true,
    replica: true,
    database_kind: databaseKind,
    branch: { id: "branch-1", name: "main" },
  };
}

test("Postgres and Neki use their engine-specific replica routing", () => {
  const postgres = new URL(postgresConnectionUrl(credentials("postgresql")));
  assert.equal(decodeURIComponent(postgres.username), "user.branch|replica");
  assert.equal(postgres.searchParams.has("options"), false);

  const neki = new URL(postgresConnectionUrl(credentials("neki")));
  assert.equal(decodeURIComponent(neki.username), "user.branch");
  assert.equal(neki.searchParams.get("options"), "-c __neki.target=REPLICA");
});
