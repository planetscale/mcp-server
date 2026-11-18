import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { dialectForKind, lexSql, precheckWriteQuery, validateWriteQuery, type SqlDialect } from "./query-validator.ts";

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
  it("a WITH with no statement has nothing to check (the database rejects it)", () =>
    assert.ok(runsUnconfirmed("WITH x AS (SELECT 1)")));
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

/** Same helpers, lexed with one engine's rules. */
function blockedOn(dialect: SqlDialect, query: string): boolean {
  const result = validateWriteQuery(query, true, dialect);
  return !result.allowed && !result.requiresConfirmation;
}
function runsOn(dialect: SqlDialect, query: string): boolean {
  return validateWriteQuery(query, false, dialect).allowed;
}
function asksOn(dialect: SqlDialect, query: string): boolean {
  const first = validateWriteQuery(query, false, dialect);
  return !first.allowed && first.requiresConfirmation && validateWriteQuery(query, true, dialect).allowed;
}

describe("comment markers inside strings (Bugbot on #79)", () => {
  it("blocks WHERE 1=1 after a string containing --", () =>
    assert.ok(blocked("UPDATE users SET note = '--' WHERE 1=1")));
  it("blocks WHERE 1=1 after a string containing #", () =>
    assert.ok(blockedOn("mysql", "UPDATE users SET note = '#' WHERE 1=1")));
  it("blocks WHERE TRUE after a string containing /*", () =>
    assert.ok(blocked("UPDATE users SET note = '/*' WHERE TRUE")));
  it("still runs a scoped UPDATE whose string contains --", () =>
    assert.ok(runsUnconfirmed("UPDATE users SET note = 'a -- b' WHERE id = 5")));
});

describe("DML anywhere in the query is checked (Bugbot on #79)", () => {
  it("blocks a DELETE inside a CTE under a SELECT", () =>
    assert.ok(blocked("WITH d AS (DELETE FROM users RETURNING *) SELECT count(*) FROM d")));
  it("blocks an UPDATE inside a CTE under a SELECT", () =>
    assert.ok(blocked("WITH u AS (UPDATE users SET plan = 'free' RETURNING id) SELECT * FROM u")));
  it("asks before a scoped DELETE inside a CTE", () =>
    assert.ok(asksThenRuns("WITH d AS (DELETE FROM sessions WHERE expired RETURNING id) SELECT count(*) FROM d")));
  it("blocks EXPLAIN ANALYZE DELETE with no WHERE (Postgres runs it)", () =>
    assert.ok(blocked("EXPLAIN ANALYZE DELETE FROM users")));
  it("blocks EXPLAIN (ANALYZE) UPDATE with no WHERE", () =>
    assert.ok(blocked("EXPLAIN (ANALYZE, BUFFERS) UPDATE users SET plan = 'free'")));
  it("blocks PREPARE ... AS DELETE with no WHERE", () =>
    assert.ok(blocked("PREPARE p AS DELETE FROM users")));
  it("checks every statement, not just the first", () =>
    assert.ok(blocked("DELETE FROM users WHERE id = 1; DELETE FROM users")));
  it("blocks TRUNCATE in a later statement", () =>
    assert.ok(blocked("SELECT 1; TRUNCATE users")));
  it("asks for DDL in a later statement", () =>
    assert.ok(asksThenRuns("INSERT INTO t (a) VALUES (1); DROP TABLE t")));
});

describe("a WHERE only counts at the statement's own level", () => {
  it("blocks UPDATE whose only WHERE is in a SET subquery", () =>
    assert.ok(blocked("UPDATE users SET plan = (SELECT plan FROM plans WHERE id = 1)")));
  it("blocks DELETE whose only WHERE is in a RETURNING subquery", () =>
    assert.ok(blocked("DELETE FROM users RETURNING (SELECT 1 WHERE true)")));
  it("runs UPDATE with a subquery and its own WHERE", () =>
    assert.ok(runsUnconfirmed("UPDATE users SET plan = (SELECT plan FROM plans WHERE id = 1) WHERE id = 5")));
  it("blocks WHERE (1=1)", () => assert.ok(blocked("DELETE FROM users WHERE (1=1)")));
  it("blocks WHERE 1=1 RETURNING *", () => assert.ok(blocked("UPDATE users SET a = 1 WHERE 1=1 RETURNING *")));
  it("blocks WHERE 1=1 LIMIT 10", () => assert.ok(blocked("DELETE FROM users WHERE 1=1 LIMIT 10")));
});

describe("DELETE/UPDATE words that are not statements", () => {
  for (const query of [
    "SELECT * FROM users WHERE id = 1 FOR UPDATE",
    "SELECT * FROM users WHERE id = 1 FOR NO KEY UPDATE",
    "INSERT INTO users (id, plan) VALUES (1, 'pro') ON DUPLICATE KEY UPDATE plan = 'pro'",
    "INSERT INTO users (id, plan) VALUES (1, 'pro') ON CONFLICT (id) DO UPDATE SET plan = 'pro'",
    "GRANT SELECT, UPDATE, DELETE ON users TO app",
    "REVOKE DELETE ON users FROM app",
    "SELECT t.update FROM t",
  ]) {
    it(query.slice(0, 60), () => assert.ok(runsUnconfirmed(query)));
  }
  it("CREATE TABLE with ON DELETE CASCADE asks (DDL), is not blocked", () =>
    assert.ok(asksThenRuns("CREATE TABLE o (id int, uid int REFERENCES users ON DELETE CASCADE ON UPDATE CASCADE)")));
  it("a trigger whose body deletes asks (DDL), is not blocked", () =>
    assert.ok(asksThenRuns("CREATE TRIGGER t AFTER INSERT ON a FOR EACH ROW DELETE FROM log")));
});

describe("each engine's own lexical rules", () => {
  // Postgres: backslash is literal in '...', so '\' is a whole string
  it("postgres: blocks a WHERE that backslash escaping would see but Postgres reads as a string", () =>
    assert.ok(blockedOn("postgres", String.raw`UPDATE users SET a = '\', b = ' WHERE id = 1 '`)));
  it("postgres: E'...' strings use backslash escapes", () =>
    assert.ok(runsOn("postgres", String.raw`UPDATE users SET a = E'it\'s' WHERE id = 1`)));
  it("mysql: backslash-escaped quotes stay inside the string", () =>
    assert.ok(runsOn("mysql", String.raw`UPDATE users SET name = 'O\'Brien' WHERE id = 1`)));
  // Postgres: $ inside an identifier does not open a dollar quote
  it("postgres: blocks a WHERE hidden by col$$ being read as a dollar quote", () =>
    assert.ok(blockedOn("postgres", "UPDATE t SET x$$ = 1, y = ' $$ WHERE id = 1 '")));
  // MySQL: block comments do not nest
  it("mysql: blocks a WHERE that nested-comment lexing would see", () =>
    assert.ok(blockedOn("mysql", "UPDATE t SET a = 1 /* /* */, b = ' */ WHERE id = 1 '")));
  // MySQL: -- needs whitespace after it, so 1--' is arithmetic
  it("mysql: blocks a WHERE hidden after 1--' (not a comment in MySQL)", () =>
    assert.ok(blockedOn("mysql", "UPDATE t SET a = 1--'\n WHERE id = 1 '")));
  it("mysql: -- followed by a space is a comment", () =>
    assert.ok(blockedOn("mysql", "UPDATE t SET a = 1 -- WHERE id = 1")));
  it("postgres: # is an operator, not a comment", () =>
    assert.ok(runsOn("postgres", "UPDATE t SET a = a # 1 WHERE id = 1")));
  it("postgres: asks before a DO block", () =>
    assert.ok(asksOn("postgres", "DO $$ BEGIN DELETE FROM users; END $$")));
  it("without a dialect the stricter engine wins", () =>
    assert.ok(blocked(String.raw`UPDATE users SET a = '\', b = ' WHERE id = 1 '`)));
  it("maps database kinds to dialects", () => {
    assert.equal(dialectForKind("mysql"), "mysql");
    assert.equal(dialectForKind("postgresql"), "postgres");
    assert.equal(dialectForKind("neki"), "postgres");
    assert.equal(dialectForKind("other"), undefined);
  });
});

describe("executable comments and unterminated input", () => {
  it("mysql: refuses a /*! comment", () =>
    assert.ok(blockedOn("mysql", "UPDATE t SET a = 1 /*! WHERE id = 1 */")));
  it("mysql: allows /*! inside a string", () =>
    assert.ok(runsOn("mysql", "UPDATE docs SET body = '/*! note */' WHERE id = 1")));
  it("postgres: /*! is an ordinary comment", () =>
    assert.ok(runsOn("postgres", "UPDATE t SET a = 1 /*! hi */ WHERE id = 1")));
  it("refuses an unterminated block comment that could hide a statement", () =>
    assert.ok(blocked("SELECT 1 /* ; DELETE FROM users")));
  it("refuses an unterminated string", () =>
    assert.ok(blocked("INSERT INTO t (a) VALUES ('x); DELETE FROM users")));
  it("postgres: refuses an unterminated dollar quote", () =>
    assert.ok(blockedOn("postgres", "SELECT $$ ; DELETE FROM users")));
});

describe("always-true conditions beyond 1=1", () => {
  for (const where of [
    "1=1 OR id = 5",
    "id = 5 OR 1=1",
    "id = id",
    "2 > 1",
    "1 <> 2",
    "NOT FALSE",
    "1=1 AND 1=1",
    "'a' LIKE 'a'",
    "name LIKE '%'",
    "((1=1))",
    "5",
  ]) {
    it(`blocks UPDATE ... WHERE ${where}`, () =>
      assert.ok(blocked(`UPDATE users SET plan = 'free' WHERE ${where}`)));
  }
  for (const where of [
    "id = 5 AND 1=1",
    "id BETWEEN 1 AND 10",
    "note = 'x OR 1=1'",
    "(id = 1) OR (id = 2)",
    "a = b",
    "0",
  ]) {
    it(`runs UPDATE ... WHERE ${where}`, () =>
      assert.ok(runsUnconfirmed(`UPDATE users SET plan = 'free' WHERE ${where}`)));
  }
  it("postgres: 'abc' = 'ABC' is false, so a DELETE only asks", () =>
    assert.ok(asksOn("postgres", "DELETE FROM sessions WHERE 'abc' = 'ABC'")));
  it("mysql: 'abc' = 'ABC' is true under default collations, so it is blocked", () =>
    assert.ok(blockedOn("mysql", "DELETE FROM sessions WHERE 'abc' = 'ABC'")));
});

describe("precheckWriteQuery (before the database kind is known)", () => {
  it("refuses TRUNCATE", () => assert.match(precheckWriteQuery("TRUNCATE users", true)?.reason ?? "", /TRUNCATE/));
  it("refuses DELETE without WHERE", () =>
    assert.equal(precheckWriteQuery("DELETE FROM users", true)?.allowed, false));
  it("asks before DROP TABLE", () =>
    assert.equal(precheckWriteQuery("DROP TABLE users", false)?.requiresConfirmation, true));
  it("defers when only one engine refuses", () =>
    assert.equal(precheckWriteQuery(String.raw`UPDATE users SET a = 'x\' WHERE id = 1`, false), undefined));
  it("defers an allowed query", () =>
    assert.equal(precheckWriteQuery("UPDATE users SET a = 1 WHERE id = 1", false), undefined));
});

describe("review round 2", () => {
  it("postgres: a bare \\r ends a -- comment, so the DELETE after it is checked", () =>
    assert.ok(blockedOn("postgres", "SELECT 1 --x\rDELETE FROM users")));
  it("mysql: a \\n still ends a # comment", () =>
    assert.ok(blockedOn("mysql", "SELECT 1 # x\nDELETE FROM users")));
  it("asks before MERGE ... THEN DELETE", () =>
    assert.ok(asksOn("postgres", "MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE")));
  it("asks before MERGE ... THEN UPDATE", () =>
    assert.ok(asksOn("postgres", "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = s.a")));
  it("asks before EXPLAIN ANALYZE CREATE TABLE AS (Postgres runs it)", () =>
    assert.ok(asksOn("postgres", "EXPLAIN ANALYZE CREATE TABLE x AS SELECT 1")));
  it("asks before EXPLAIN (ANALYZE) DROP-style DDL", () =>
    assert.ok(asksOn("postgres", "EXPLAIN (ANALYZE, VERBOSE) CREATE TABLE x AS SELECT 1")));
  it("mysql: blocks WHERE id = 1 || 1", () =>
    assert.ok(blockedOn("mysql", "UPDATE t SET a = 1 WHERE id = 1 || 1")));
  it("mysql: blocks WHERE 1 && 1", () =>
    assert.ok(blockedOn("mysql", "UPDATE t SET a = 1 WHERE 1 && 1")));
  it("postgres: || is concatenation, so a = b || c is a real filter", () =>
    assert.ok(runsOn("postgres", "UPDATE t SET x = 1 WHERE a = b || 'c'")));
  it("blocks WHERE NOT 1=2", () => assert.ok(blocked("UPDATE t SET a = 1 WHERE NOT 1=2")));
  it("blocks WHERE 1 <=> 1", () => assert.ok(blockedOn("mysql", "UPDATE t SET a = 1 WHERE 1 <=> 1")));
  it("blocks WHERE id <=> id", () => assert.ok(blockedOn("mysql", "UPDATE t SET a = 1 WHERE id <=> id")));
  it("does not call NOT LIKE '%' always true", () =>
    assert.ok(runsUnconfirmed("UPDATE t SET a = 1 WHERE name NOT LIKE '%'")));
  it("does not call NULL = NULL always true", () =>
    assert.ok(runsUnconfirmed("UPDATE t SET a = 1 WHERE NULL = NULL")));
  it("does not call NOT (id BETWEEN 0 AND 0) always true", () =>
    assert.ok(runsUnconfirmed("UPDATE t SET a = 1 WHERE NOT (id BETWEEN 0 AND 0)")));
  it("allows update as a column alias", () =>
    assert.ok(runsUnconfirmed("SELECT 1 AS update FROM t")));
  it("PREPARE ... AS DELETE is still checked", () =>
    assert.ok(blocked("PREPARE p AS DELETE FROM users; EXECUTE p")));
});

describe("review round 3", () => {
  it("postgres: typed PREPARE p (int) AS DELETE is still checked", () =>
    assert.ok(blockedOn("postgres", "PREPARE p (int) AS DELETE FROM users; EXECUTE p(1)")));
  it("mysql: asks before PREPARE ... FROM a string", () =>
    assert.ok(asksOn("mysql", "PREPARE s FROM 'DELETE FROM users'")));
  it("mysql: asks before PREPARE ... FROM a variable and EXECUTE", () =>
    assert.ok(asksOn("mysql", "SET @q = 'DELETE FROM users'; PREPARE s FROM @q; EXECUTE s")));
  it("asks before a bare EXECUTE", () => assert.ok(asksThenRuns("EXECUTE s")));
  for (const where of ["TRUE = TRUE", "true=true", "NULL IS NULL", "1 IN (1, 2)", "'a' IN ('a')", "1 IS NOT NULL", "FALSE = FALSE"]) {
    it(`blocks UPDATE ... WHERE ${where}`, () =>
      assert.ok(blocked(`UPDATE users SET plan = 'free' WHERE ${where}`)));
  }
  for (const where of ["id IS NULL", "id IN (1, 2)", "3 IN (1, 2)", "NULL IS NOT NULL", "1 NOT IN (1)"]) {
    it(`runs UPDATE ... WHERE ${where}`, () =>
      assert.ok(runsUnconfirmed(`UPDATE users SET plan = 'free' WHERE ${where}`)));
  }
  it("postgres: blocks WHERE 1 = 1 FOR UPDATE", () =>
    assert.ok(blockedOn("postgres", "UPDATE t SET a = 1 WHERE 1 = 1 FOR UPDATE")));
  it("blocks WHERE 1=1 OFFSET 5", () => assert.ok(blocked("DELETE FROM t WHERE 1=1 OFFSET 5")));
  it("refuses a query nested too deep to check instead of throwing", () => {
    const deep = "UPDATE t SET a = 1 WHERE " + "NOT ".repeat(20000) + "id = 1";
    const result = validateWriteQuery(deep, true, "mysql");
    assert.equal(typeof result.allowed, "boolean");
  });
  it("mysql: a trigger with a two-statement BEGIN...END body asks (DDL), is not blocked", () =>
    assert.ok(asksOn("mysql", "CREATE TRIGGER x AFTER INSERT ON t FOR EACH ROW BEGIN UPDATE a SET x = 1; UPDATE b SET y = 2; END")));
  it("mysql: IF ... END IF inside a body stays one statement", () =>
    assert.ok(asksOn("mysql", "CREATE PROCEDURE p() BEGIN IF 1 THEN DELETE FROM a; END IF; DELETE FROM b; END")));
  it("a statement after the body is still checked", () =>
    assert.ok(blockedOn("mysql", "CREATE TRIGGER x AFTER INSERT ON t FOR EACH ROW BEGIN UPDATE a SET x = 1; END; DELETE FROM users")));
  it("BEGIN; starts a transaction: the DELETE after it is checked", () =>
    assert.ok(blocked("BEGIN; DELETE FROM users; COMMIT")));
});

describe("Bugbot on a5c7b58", () => {
  for (const ch of [" ", "", "", ""]) {
    it(`mysql: -- followed by control char ${JSON.stringify(ch)} is a comment`, () =>
      assert.ok(blockedOn("mysql", `UPDATE t SET a = 1 --${ch}WHERE id = 1`)));
  }
  it("mysql: --x is still arithmetic, not a comment", () =>
    assert.ok(runsOn("mysql", "UPDATE t SET a = 1--1 WHERE id = 1")));
});

describe("lexSql", () => {
  it("keeps offsets and newlines", () => {
    const query = "UPDATE t -- c\nSET a = 'x' /* y */ WHERE id = 1";
    const { code, masked } = lexSql(query, "postgres");
    assert.equal(masked.length, query.length);
    assert.equal(code.length, query.length);
    assert.equal(masked.indexOf("WHERE"), query.indexOf("WHERE"));
    assert.equal(masked.split("\n").length, 2);
    assert.doesNotMatch(masked, /'x'|\/\*|--/);
  });

  it("keeps strings in the code view, including comment markers inside them", () => {
    assert.equal(lexSql("SELECT '--' -- b", "postgres").code, "SELECT '--'     ");
    assert.equal(lexSql("SELECT '#' # b", "mysql").code, "SELECT '#'    ");
  });

  it("handles doubled quotes and nested block comments per engine", () => {
    assert.equal(lexSql("'it''s' x", "postgres").masked, "        x");
    assert.equal(lexSql("/* a /* b */ c */x", "postgres").masked, " ".repeat(17) + "x");
    assert.equal(lexSql("/* a /* b */x", "mysql").masked, " ".repeat(12) + "x");
  });

  it("masks an unterminated comment to the end", () => {
    assert.equal(lexSql("DELETE FROM t /* WHERE id = 1", "postgres").masked.trim(), "DELETE FROM t");
  });
});
