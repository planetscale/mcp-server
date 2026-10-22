import assert from "node:assert/strict";
import test from "node:test";
import { validateWriteQuery } from "./query-validator.ts";

test("a leading comment does not hide DELETE or UPDATE without WHERE", () => {
  for (const query of [
    "-- clear old sessions\nDELETE FROM sessions",
    "-- WHERE id = 1\nDELETE FROM sessions",
    "/* downgrade */ UPDATE users SET plan = 'free'",
  ]) {
    const result = validateWriteQuery(query, true);
    assert.equal(result.allowed, false, query);
    assert.equal(result.requiresConfirmation, false, query);
  }
});

test("a leading comment does not let TRUNCATE through", () => {
  const result = validateWriteQuery("/* reset */\nTRUNCATE TABLE users", true);
  assert.equal(result.allowed, false);
});

test("a nested or /*! block comment at the start is rejected", () => {
  for (const query of [
    "/* a /* b */ c */ DELETE FROM sessions",
    "/*!DELETE FROM sessions*/",
  ]) {
    const result = validateWriteQuery(query, true);
    assert.equal(result.allowed, false, query);
    assert.equal(result.requiresConfirmation, false, query);
  }
});

test("DDL behind a leading comment still needs confirmation", () => {
  const result = validateWriteQuery("# old table\n-- not used since v2\nDROP TABLE users", false);
  assert.equal(result.allowed, false);
  assert.equal(result.requiresConfirmation, true);
});
