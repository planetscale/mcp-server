import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { maskSql, validateWriteQuery } from "./query-validator.ts";

/** Runs with no confirmation and no prompt. */
function runsUnconfirmed(query: string): boolean {
  return validateWriteQuery(query, false).allowed;
}

/** Refused even with confirm_destructive: true. */
function blocked(query: string): boolean {
  const result = validateWriteQuery(query, true);
  return !result.allowed && !result.requiresConfirmation;
}

/** Needs confirm_destructive: true, then runs. */
function asksThenRuns(query: string): boolean {
  const first = validateWriteQuery(query, false);
  return !first.allowed && first.requiresConfirmation && validateWriteQuery(query, true).allowed;
}

describe("existing behavior is unchanged", () => {
  it("blocks TRUNCATE", () => assert.ok(blocked("TRUNCATE users")));
  it("asks before DROP TABLE", () => assert.ok(asksThenRuns("DROP TABLE users")));
  it("blocks UPDATE without WHERE", () => assert.ok(blocked("UPDATE users SET plan = 'free'")));
  it("blocks DELETE WHERE 1=1", () => assert.ok(blocked("DELETE FROM users WHERE 1=1")));
  it("blocks DELETE WHERE 'a'='a'", () => assert.ok(blocked("DELETE FROM users WHERE 'a'='a'")));
  it("asks before a scoped DELETE", () => assert.ok(asksThenRuns("DELETE FROM users WHERE id = 5")));
  it("runs a normal INSERT", () => assert.ok(runsUnconfirmed("INSERT INTO users (id) VALUES (1)")));
  it("runs a scoped UPDATE", () => assert.ok(runsUnconfirmed("UPDATE users SET plan = 'free' WHERE id = 5")));
  it("runs a SELECT", () => assert.ok(runsUnconfirmed("SELECT * FROM users")));
});

describe("a WHERE that does not execute does not count", () => {
  it("blocks UPDATE whose only WHERE is in a trailing comment", () =>
    assert.ok(blocked("UPDATE users SET plan = 'free' -- WHERE id = 5")));
  it("blocks UPDATE whose only WHERE is in a block comment", () =>
    assert.ok(blocked("UPDATE users SET plan = 'free' /* WHERE id = 5 */")));
  it("blocks UPDATE whose only WHERE is inside a string literal", () =>
    assert.ok(blocked("UPDATE users SET note = 'see WHERE clause'")));
  it("blocks DELETE whose only WHERE is a quoted identifier", () =>
    assert.ok(blocked('DELETE FROM "where"')));
  it("blocks UPDATE whose only WHERE is in a dollar-quoted body", () =>
    assert.ok(blocked("UPDATE users SET bio = $$ WHERE $$")));
  it("still runs a scoped UPDATE that also has a comment", () =>
    assert.ok(runsUnconfirmed("UPDATE users SET plan = 'free' WHERE id = 5 -- downgrade")));
});

describe("always-true WHERE with trailing text", () => {
  it("blocks DELETE WHERE 1=1 with a trailing semicolon", () =>
    assert.ok(blocked("DELETE FROM users WHERE 1=1;")));
  it("blocks UPDATE WHERE 1 = 1 followed by a comment", () =>
    assert.ok(blocked("UPDATE users SET plan = 'free' WHERE 1 = 1 -- all of them")));
  it("blocks DELETE WHERE TRUE across lines", () =>
    assert.ok(blocked("DELETE FROM users\nWHERE TRUE\n;")));
});

describe("statements behind a WITH clause", () => {
  it("blocks a CTE DELETE with no WHERE", () =>
    assert.ok(blocked("WITH x AS (SELECT 1) DELETE FROM users")));
  it("blocks a CTE UPDATE with no WHERE", () =>
    assert.ok(blocked("WITH RECURSIVE x AS (SELECT 1) UPDATE users SET plan = 'free'")));
  it("asks before a CTE DELETE with a WHERE", () =>
    assert.ok(asksThenRuns("WITH stale AS (SELECT id FROM sessions WHERE expired) DELETE FROM sessions WHERE id IN (SELECT id FROM stale)")));
  it("a WHERE inside the CTE body does not cover the DELETE", () =>
    assert.ok(blocked("WITH s AS (SELECT id FROM users WHERE id = 1) DELETE FROM users")));
  it("asks when the main statement cannot be found", () =>
    assert.ok(asksThenRuns("WITH x AS (SELECT 1)")));
  it("runs a CTE SELECT", () =>
    assert.ok(runsUnconfirmed("WITH x AS (SELECT 1) SELECT * FROM x")));
});

describe("every CREATE, DROP, ALTER and RENAME asks", () => {
  for (const query of [
    "CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
    "CREATE UNIQUE INDEX users_email ON users (email)",
    "CREATE MATERIALIZED VIEW mv AS SELECT 1",
    "DROP SEQUENCE users_id_seq",
    "DROP MATERIALIZED VIEW mv",
    "DROP EXTENSION pgcrypto CASCADE",
    "DROP TYPE status CASCADE",
    "DROP OWNED BY app_user",
    "ALTER SEQUENCE users_id_seq RESTART",
    "RENAME TABLE a TO b",
  ]) {
    it(query.slice(0, 48), () => assert.ok(asksThenRuns(query)));
  }
});

describe("MySQL executable comments", () => {
  it("are refused", () => assert.ok(blocked("UPDATE users SET plan = 'free' /*! WHERE id = 5 */")));
});

describe("maskSql", () => {
  it("keeps offsets and newlines", () => {
    const query = "UPDATE t -- c\nSET a = 'x' /* y */ WHERE id = 1";
    const masked = maskSql(query, { strings: true });
    assert.equal(masked.length, query.length);
    assert.equal(masked.indexOf("WHERE"), query.indexOf("WHERE"));
    assert.equal(masked.split("\n").length, 2);
    assert.doesNotMatch(masked, /'x'|\/\*|--/);
  });

  it("leaves strings alone when asked to", () => {
    assert.equal(maskSql("SELECT 'a' -- b", { strings: false }), "SELECT 'a'     ");
  });

  it("handles doubled quotes and nested block comments", () => {
    assert.equal(maskSql("'it''s' x", { strings: true }), "        x");
    assert.equal(maskSql("/* a /* b */ c */x", { strings: true }), " ".repeat(17) + "x");
  });

  it("masks an unterminated comment to the end", () => {
    assert.equal(maskSql("DELETE FROM t /* WHERE id = 1", { strings: true }).trim(), "DELETE FROM t");
  });
});
