export interface ValidationResult {
  allowed: boolean;
  requiresConfirmation: boolean;
  reason?: string;
}

/**
 * Which lexical rules to read the query with. MySQL and Postgres disagree on
 * comments and string escapes, and each disagreement can make one engine see a
 * WHERE that the other treats as text, so the checks must lex the query the
 * way the engine that runs it will.
 *
 * - mysql: `#` comments; `--` starts a comment only before whitespace; block
 *   comments do not nest; backslash escapes in '...' and "..." strings;
 *   backtick identifiers.
 * - postgres: `--` always starts a comment; `#` is an operator; block comments
 *   nest; backslash escapes only in E'...' strings; "..." identifiers;
 *   $tag$ ... $tag$ dollar quoting.
 */
export type SqlDialect = "mysql" | "postgres";

export interface LexedSql {
  /** Comments replaced with spaces; strings kept. */
  code: string;
  /** Comments, strings, quoted identifiers and dollar-quoted bodies replaced with spaces. */
  masked: string;
  /** A quote, block comment or dollar quote was still open at the end. */
  unterminated: boolean;
  /** MySQL only: a /*! ... *\/ comment, whose body MySQL runs as SQL. */
  executableComment: boolean;
}

const IDENT_CHAR = /[A-Za-z0-9_$]/;
const DOLLAR_TAG = /\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/y;

/**
 * Lexes a query once and returns two views of it with offsets preserved.
 * Strings and comments are found in the same pass, so a comment marker inside
 * a string (or a quote inside a comment) is never misread.
 *
 * An unterminated quote or block comment is reported rather than trusted:
 * the database would reject such a query, so it usually means the engine reads
 * the text differently than this lexer (for example under a non-default
 * sql_mode), and whatever it hid could be a DELETE or UPDATE.
 */
export function lexSql(query: string, dialect: SqlDialect): LexedSql {
  const code: string[] = [];
  const masked: string[] = [];
  const n = query.length;
  let i = 0;
  let unterminated = false;
  let executableComment = false;

  const space = (k: number) => (query[k] === "\n" ? "\n" : " ");
  const comment = (from: number, to: number) => {
    for (let k = from; k < to; k++) {
      code.push(space(k));
      masked.push(space(k));
    }
  };
  const quoted = (from: number, to: number) => {
    for (let k = from; k < to; k++) {
      code.push(query[k]!);
      masked.push(space(k));
    }
  };

  while (i < n) {
    const c = query[i]!;
    const next = query[i + 1];
    const prev = i > 0 ? query[i - 1]! : "";

    // Line comments
    const mysqlDash = c === "-" && next === "-" && (i + 2 >= n || /\s/.test(query[i + 2]!));
    const lineComment =
      dialect === "mysql" ? mysqlDash || c === "#" : c === "-" && next === "-";
    if (lineComment) {
      // Postgres ends a -- comment at \n or \r; MySQL at \n
      const eol = dialect === "postgres" ? /[\n\r]/g : /\n/g;
      eol.lastIndex = i;
      const end = eol.exec(query)?.index ?? -1;
      const stop = end === -1 ? n : end;
      comment(i, stop);
      i = stop;
      continue;
    }

    // Block comments: Postgres nests them, MySQL ends at the first */
    if (c === "/" && next === "*") {
      if (dialect === "mysql" && query[i + 2] === "!") executableComment = true;
      let depth = 1;
      let j = i + 2;
      while (j < n && depth > 0) {
        if (dialect === "postgres" && query[j] === "/" && query[j + 1] === "*") {
          depth++;
          j += 2;
        } else if (query[j] === "*" && query[j + 1] === "/") {
          depth--;
          j += 2;
        } else {
          j++;
        }
      }
      if (depth > 0) unterminated = true;
      comment(i, j);
      i = j;
      continue;
    }

    // Quoted strings and identifiers
    const isQuote = c === "'" || c === '"' || (dialect === "mysql" && c === "`");
    if (isQuote) {
      const eString =
        dialect === "postgres" &&
        c === "'" &&
        (prev === "E" || prev === "e") &&
        (i < 2 || !IDENT_CHAR.test(query[i - 2]!));
      const backslashEscapes = dialect === "mysql" ? c !== "`" : eString;
      let j = i + 1;
      let closed = false;
      while (j < n) {
        if (backslashEscapes && query[j] === "\\") {
          j += 2;
          continue;
        }
        if (query[j] === c) {
          if (query[j + 1] === c) {
            j += 2;
            continue;
          }
          j++;
          closed = true;
          break;
        }
        j++;
      }
      if (!closed) unterminated = true;
      const stop = Math.min(j, n);
      quoted(i, stop);
      i = stop;
      continue;
    }

    // Postgres dollar quoting; `$` inside an identifier (col$$) does not open one
    if (dialect === "postgres" && c === "$" && !IDENT_CHAR.test(prev)) {
      DOLLAR_TAG.lastIndex = i;
      const tag = DOLLAR_TAG.exec(query);
      if (tag) {
        const close = query.indexOf(tag[0], i + tag[0].length);
        if (close === -1) unterminated = true;
        const stop = close === -1 ? n : close + tag[0].length;
        quoted(i, stop);
        i = stop;
        continue;
      }
    }

    code.push(c);
    masked.push(c);
    i++;
  }

  return { code: code.join(""), masked: masked.join(""), unterminated, executableComment };
}

interface Token {
  text: string; // upper-cased for words
  start: number;
  end: number;
  depth: number; // parenthesis depth at the token
}

/** Words, parentheses, `;`, `,` and `.` from masked SQL, with paren depth. */
function tokenize(masked: string): Token[] {
  const tokens: Token[] = [];
  const re = /[A-Za-z_][A-Za-z0-9_$]*|[(),;.]/g;
  let depth = 0;
  for (let m = re.exec(masked); m !== null; m = re.exec(masked)) {
    const text = m[0];
    if (text === ")") depth = Math.max(0, depth - 1);
    tokens.push({ text: text.toUpperCase(), start: m.index, end: m.index + text.length, depth });
    if (text === "(") depth++;
  }
  return tokens;
}

interface Statement {
  tokens: Token[];
  end: number; // offset of the closing `;`, or the query length
}

/** BEGIN followed by one of these starts a transaction, not a compound body. */
const TRANSACTION_BEGIN = new Set([";", "WORK", "TRANSACTION", "ISOLATION", "READ", "DEFERRABLE", "NOT"]);
/** Inside a compound body these open a block that a later END closes (END IF, END LOOP, ...). */
const BLOCK_OPENERS = new Set(["BEGIN", "CASE", "IF", "LOOP", "WHILE", "REPEAT"]);

/**
 * Splits tokens into statements at top-level semicolons. In a CREATE statement
 * a BEGIN ... END body (MySQL triggers/procedures, Postgres BEGIN ATOMIC) is
 * part of the CREATE, so semicolons inside it do not split.
 */
function splitStatements(tokens: Token[], length: number): Statement[] {
  const statements: Statement[] = [];
  let current: Token[] = [];
  let block = 0;
  let afterEnd = false;
  for (let k = 0; k < tokens.length; k++) {
    const t = tokens[k]!;
    const next = tokens[k + 1];
    if (current[0]?.text === "CREATE") {
      if (block === 0 && t.text === "BEGIN" && (!next || !TRANSACTION_BEGIN.has(next.text))) {
        block = 1;
      } else if (block > 0 && t.text === "END") {
        block--;
      } else if (
        block > 0 &&
        !afterEnd && // END IF / END LOOP / END CASE name the block they close
        BLOCK_OPENERS.has(t.text) &&
        !(t.text === "IF" && next?.text === "(") // IF(...) is a function
      ) {
        block++;
      }
      afterEnd = t.text === "END";
    }
    if (t.text === ";" && t.depth === 0 && block === 0) {
      statements.push({ tokens: current, end: t.start });
      current = [];
    } else {
      current.push(t);
    }
  }
  statements.push({ tokens: current, end: length });
  return statements.filter((s) => s.tokens.length > 0);
}

/**
 * A DELETE or UPDATE keyword after one of these is part of another clause, not
 * a statement: ON DELETE CASCADE, FOR UPDATE, ON DUPLICATE KEY UPDATE,
 * ON CONFLICT DO UPDATE, MERGE ... THEN DELETE (MERGE asks for confirmation
 * as a whole), trigger events (BEFORE/AFTER UPDATE, INSTEAD OF DELETE, INSERT
 * OR UPDATE), GRANT DELETE, UPDATE ON ..., and qualified names (t.update).
 * After AS it is an alias (SELECT 1 AS update), except in PREPARE name AS.
 */
const NOT_A_STATEMENT_AFTER = new Set([
  "ON", "FOR", "KEY", "DO", "THEN", "BEFORE", "AFTER", "OF", "OR",
  "GRANT", "REVOKE", ",", ".",
]);

function isClauseWord(statement: Token[], idx: number): boolean {
  const before = statement[idx - 1];
  if (!before) return false;
  if (NOT_A_STATEMENT_AFTER.has(before.text)) return true;
  return before.text === "AS" && statement[0]?.text !== "PREPARE";
}

/** EXPLAIN and its options run the statement after them (with ANALYZE). */
const EXPLAIN_PREFIX = new Set(["EXPLAIN", "ANALYZE", "ANALYSE", "VERBOSE", "FORMAT", "TREE", "JSON", "TRADITIONAL"]);

/** The statement's own first keyword, past EXPLAIN [ANALYZE] [(options)]. */
function statementHead(statement: Token[]): string {
  let k = 0;
  while (k < statement.length) {
    const t = statement[k]!;
    if (EXPLAIN_PREFIX.has(t.text)) {
      k++;
    } else if (t.text === "(" && k > 0 && statement[k - 1]!.text === "EXPLAIN") {
      while (k < statement.length && !(statement[k]!.text === ")" && statement[k]!.depth === t.depth)) k++;
      k++;
    } else {
      break;
    }
  }
  return statement[k]?.text ?? statement[0]!.text;
}

const DDL_KEYWORDS = new Set(["CREATE", "DROP", "ALTER", "RENAME"]);

/** Clause keywords that end a WHERE condition at the same depth. */
const WHERE_END = new Set(["RETURNING", "ORDER", "LIMIT", "GROUP", "FOR", "OFFSET", "FETCH"]);

interface DmlCheck {
  kind: "DELETE" | "UPDATE";
  hasWhere: boolean;
  alwaysTrue: boolean;
}

/**
 * Finds every DELETE and UPDATE that runs in a statement, wherever it sits:
 * behind WITH, inside a CTE (Postgres runs data-modifying CTEs to completion
 * even under a SELECT), after EXPLAIN ANALYZE or PREPARE ... AS. Each one only
 * counts a WHERE at its own parenthesis depth, so a WHERE inside a subquery
 * (UPDATE t SET a = (SELECT ... WHERE ...)) does not count.
 */
function findDml(
  { tokens: statement, end }: Statement,
  code: string,
  masked: string,
  dialect: SqlDialect
): DmlCheck[] {
  const found: DmlCheck[] = [];
  for (let idx = 0; idx < statement.length; idx++) {
    const t = statement[idx]!;
    if (t.text !== "DELETE" && t.text !== "UPDATE") continue;
    if (isClauseWord(statement, idx)) continue;

    let whereTok: Token | undefined;
    let clauseEnd = -1;
    let k = idx + 1;
    for (; k < statement.length; k++) {
      const u = statement[k]!;
      if (u.depth < t.depth) break; // the enclosing parenthesis closed
      if (u.depth !== t.depth) continue;
      if (!whereTok && u.text === "WHERE") {
        whereTok = u;
      } else if (whereTok && WHERE_END.has(u.text)) {
        clauseEnd = u.start;
        break;
      }
    }
    if (whereTok && clauseEnd === -1) {
      const stop = statement[k];
      clauseEnd = stop ? stop.start : end;
    }

    found.push({
      kind: t.text,
      hasWhere: whereTok !== undefined,
      alwaysTrue:
        whereTok !== undefined &&
        isAlwaysTrue(code.slice(whereTok.end, clauseEnd), masked.slice(whereTok.end, clauseEnd), dialect),
    });
  }
  return found;
}

/** Strips parentheses that wrap the whole span (checked on the masked view). */
function unwrap(code: string, masked: string): [string, string] {
  for (;;) {
    // Trim on the code view: masked strings are blank and would be trimmed away
    const lead = code.length - code.trimStart().length;
    const trail = code.trimEnd().length;
    code = code.slice(lead, trail);
    masked = masked.slice(lead, trail);
    if (!masked.startsWith("(") || !masked.endsWith(")")) return [code, masked];
    let depth = 0;
    for (let k = 0; k < masked.length; k++) {
      if (masked[k] === "(") depth++;
      else if (masked[k] === ")") depth--;
      if (depth === 0 && k < masked.length - 1) return [code, masked];
    }
    code = code.slice(1, -1);
    masked = masked.slice(1, -1);
  }
}

/** Splits a span at a top-level operator, outside strings and parentheses. */
function splitTop(code: string, masked: string, operator: RegExp): [string, string][] {
  const parts: [string, string][] = [];
  const re = new RegExp(`${operator.source}|[()]`, "gi");
  let depth = 0;
  let from = 0;
  for (let m = re.exec(masked); m !== null; m = re.exec(masked)) {
    if (m[0] === "(") depth++;
    else if (m[0] === ")") depth--;
    else if (depth === 0) {
      parts.push([code.slice(from, m.index), masked.slice(from, m.index)]);
      from = m.index + m[0].length;
    }
  }
  parts.push([code.slice(from), masked.slice(from)]);
  return parts;
}

const NUMBER = /^[+-]?\d+(?:\.\d+)?$/;
const STRING = /^'((?:[^']|'')*)'$|^"((?:[^"]|"")*)"$/s;
const IDENT = /^[A-Za-z_][A-Za-z0-9_$.]*$|^`[^`]+`$/;

/**
 * Evaluates a WHERE condition when it does not depend on any row: true or
 * false, or undefined when it might (a real filter). A condition that is
 * always true touches every row: TRUE, 1, 1=1, 2>1, 'a'='a', id=id, x<=>x,
 * col LIKE '%', NOT 1=2, any of those in parentheses, OR'd with anything, or
 * AND'd only with each other (MySQL's || and && too).
 * Best effort: it cannot decide every expression.
 */
function evaluate(rawCode: string, rawMasked: string, dialect: SqlDialect): boolean | undefined {
  const trimmedLength = rawCode.replace(/[;\s]+$/, "").length;
  const [code, masked] = unwrap(rawCode.slice(0, trimmedLength), rawMasked.slice(0, trimmedLength));
  if (code.trim() === "") return undefined;

  // In MySQL || and && are logical operators; in Postgres || concatenates
  const orOp = dialect === "mysql" ? /\bOR\b|\|\|/ : /\bOR\b/;
  const andOp = dialect === "mysql" ? /\bAND\b|&&/ : /\bAND\b/;

  const ors = splitTop(code, masked, orOp);
  if (ors.length > 1) {
    const values = ors.map(([c, m]) => evaluate(c, m, dialect));
    if (values.includes(true)) return true;
    return values.every((v) => v === false) ? false : undefined;
  }
  // x BETWEEN a AND b is one predicate, not a conjunction
  if (/\bBETWEEN\b/i.test(masked)) return undefined;
  const ands = splitTop(code, masked, andOp);
  if (ands.length > 1) {
    const values = ands.map(([c, m]) => evaluate(c, m, dialect));
    if (values.includes(false)) return false;
    return values.every((v) => v === true) ? true : undefined;
  }

  const not = /^NOT\b/i.exec(masked);
  if (not) {
    const inner = evaluate(code.slice(not[0].length), masked.slice(not[0].length), dialect);
    return inner === undefined ? undefined : !inner;
  }

  const atom = code.trim();
  const upper = atom.toUpperCase();
  if (upper === "TRUE") return true;
  if (upper === "FALSE") return false;
  if (NUMBER.test(atom)) return Number(atom) !== 0;

  // <literal> IS [NOT] NULL
  const isNull = /^(.*?)\s+IS\s+(NOT\s+)?NULL$/is.exec(atom);
  if (isNull) {
    const subject = isNull[1]!.trim();
    const literalNull = /^NULL$/i.test(subject);
    const literalValue = NUMBER.test(subject) || STRING.test(subject) || /^(TRUE|FALSE)$/i.test(subject);
    if (!literalNull && !literalValue) return undefined;
    return literalNull !== Boolean(isNull[2]);
  }

  // <literal> [NOT] IN (<literals>)
  const inList = /^(.+?)\s+(NOT\s+)?IN\s*\((.*)\)$/is.exec(atom);
  if (inList) {
    const subject = literalValue(inList[1]!.trim(), dialect);
    const items = inList[3]!.split(",").map((x) => literalValue(x.trim(), dialect));
    if (subject === undefined || items.some((x) => x === undefined)) return undefined;
    return items.includes(subject) !== Boolean(inList[2]);
  }

  // lhs <op> rhs, with the operator found outside strings
  const op = /<=>|<=|>=|<>|!=|=|<|>|\bNOT\s+LIKE\b|\bLIKE\b/i.exec(masked);
  if (!op) return undefined;
  const lhs = code.slice(0, op.index).trim();
  const rhs = code.slice(op.index + op[0].length).trim();
  const operator = op[0].toUpperCase().replace(/\s+/g, " ");
  if (operator === "NOT LIKE") return undefined;

  if (operator === "LIKE") {
    if (rhs === "'%'") return true;
    const l = STRING.exec(lhs);
    const r = STRING.exec(rhs);
    return l && r && !/[%_]/.test(rhs) ? sameString(l, r, dialect) : undefined;
  }

  const lv = literalNumber(lhs);
  const rv = literalNumber(rhs);
  if (lv !== undefined && rv !== undefined) {
    const a = lv;
    const b = rv;
    switch (operator) {
      case "=":
      case "<=>":
        return a === b;
      case "<>":
      case "!=":
        return a !== b;
      case "<":
        return a < b;
      case ">":
        return a > b;
      case "<=":
        return a <= b;
      case ">=":
        return a >= b;
    }
  }

  const l = STRING.exec(lhs);
  const r = STRING.exec(rhs);
  if (l && r) {
    const same = sameString(l, r, dialect);
    if (operator === "=" || operator === "<=>") return same;
    if (operator === "<>" || operator === "!=") return !same;
    return undefined;
  }

  // id = id: true for every row where the column is not NULL; x <=> x always
  const isColumn = IDENT.test(lhs) && !/^(NULL|TRUE|FALSE)$/i.test(lhs);
  if (isColumn && lhs.toUpperCase() === rhs.toUpperCase()) {
    if (["=", "<=", ">=", "<=>"].includes(operator)) return true;
  }
  return undefined;
}

/** Numbers, and TRUE/FALSE as 1/0 (true in both engines for comparisons). */
function literalNumber(text: string): number | undefined {
  if (NUMBER.test(text)) return Number(text);
  if (/^TRUE$/i.test(text)) return 1;
  if (/^FALSE$/i.test(text)) return 0;
  return undefined;
}

/** A literal as a comparable key (MySQL strings compare case-insensitively). */
function literalValue(text: string, dialect: SqlDialect): string | undefined {
  const n = literalNumber(text);
  if (n !== undefined) return `n:${n}`;
  const m = STRING.exec(text);
  if (!m) return undefined;
  const value = m[1] ?? m[2] ?? "";
  return `s:${dialect === "mysql" ? value.toLowerCase() : value}`;
}

function isAlwaysTrue(code: string, masked: string, dialect: SqlDialect): boolean {
  return evaluate(code, masked, dialect) === true;
}

/** MySQL's default collations compare case-insensitively; Postgres compares exactly. */
function sameString(l: RegExpExecArray, r: RegExpExecArray, dialect: SqlDialect): boolean {
  const a = l[1] ?? l[2] ?? "";
  const b = r[1] ?? r[2] ?? "";
  return dialect === "mysql" ? a.toLowerCase() === b.toLowerCase() : a === b;
}

function confirmReason(what: string, question: string): string {
  return `${what} STOP and ask the user: '${question} Do you want me to proceed with: [show the query]?' Only proceed with confirm_destructive: true after the user explicitly approves.`;
}

/** Any failure to check (for example a stack overflow on absurd nesting) refuses. */
function validateWithDialect(
  query: string,
  confirmed: boolean,
  dialect: SqlDialect
): ValidationResult {
  try {
    return checkWithDialect(query, confirmed, dialect);
  } catch {
    return {
      allowed: false,
      requiresConfirmation: false,
      reason: "This query could not be checked safely, so it was not run. Simplify it and try again.",
    };
  }
}

function checkWithDialect(
  query: string,
  confirmed: boolean,
  dialect: SqlDialect
): ValidationResult {
  const { code, masked, unterminated, executableComment } = lexSql(query, dialect);

  // MySQL runs the body of /*! ... */ comments, so masking them would hide SQL
  if (executableComment) {
    return {
      allowed: false,
      requiresConfirmation: false,
      reason: "Queries containing MySQL executable comments (/*! ... */) are not allowed.",
    };
  }

  // The database would reject this as written, so this lexer and the engine
  // disagree about where a string or comment ends; what it hides is unknown.
  if (unterminated) {
    return {
      allowed: false,
      requiresConfirmation: false,
      reason: "This query has an unterminated quote, block comment or dollar quote, so it cannot be checked safely. Fix the quoting and try again.",
    };
  }

  const statements = splitStatements(tokenize(masked), masked.length);
  let confirmation: string | undefined;

  for (const statement of statements) {
    const head = statementHead(statement.tokens);

    if (head === "TRUNCATE") {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "TRUNCATE is not allowed. This operation cannot be undone.",
      };
    }

    // DDL needs confirmation. Its body (trigger actions, rules, FK clauses)
    // defines behavior instead of running it, so DML inside is not checked.
    if (DDL_KEYWORDS.has(head)) {
      confirmation ??= confirmReason(
        `${head} is a DDL (schema-modifying) statement that requires human confirmation.`,
        `This ${head} query will modify the database schema.`
      );
      continue;
    }

    // MERGE updates or deletes every row its ON condition matches
    const mergeAt = statement.tokens.findIndex(
      (t, k) => t.text === "MERGE" && !isClauseWord(statement.tokens, k)
    );
    if (mergeAt !== -1) {
      confirmation ??= confirmReason(
        "MERGE can update or delete every row its ON condition matches, which cannot be checked automatically.",
        "This MERGE query may update or delete rows."
      );
    }

    // MySQL PREPARE ... FROM takes its SQL as a string or variable, and
    // EXECUTE runs a prepared statement: neither text can be checked here
    if (head === "EXECUTE" || (head === "PREPARE" && statement.tokens[2]?.text === "FROM")) {
      confirmation ??= confirmReason(
        `${head} runs SQL from a prepared statement that cannot be checked automatically.`,
        `This ${head} runs a prepared statement.`
      );
      continue;
    }

    // DO runs a procedural block whose body is a string we cannot check
    if (dialect === "postgres" && head === "DO") {
      confirmation ??= confirmReason(
        "DO runs a procedural code block that cannot be checked automatically.",
        "This DO block runs procedural code."
      );
      continue;
    }

    for (const dml of findDml(statement, code, masked, dialect)) {
      if (!dml.hasWhere) {
        return {
          allowed: false,
          requiresConfirmation: false,
          reason: `${dml.kind} without a WHERE clause is not allowed. This would affect the entire table.`,
        };
      }
      if (dml.alwaysTrue) {
        return {
          allowed: false,
          requiresConfirmation: false,
          reason: `${dml.kind} with an always-true WHERE clause (e.g., WHERE 1=1) is not allowed. This would affect the entire table.`,
        };
      }
      if (dml.kind === "DELETE") {
        confirmation ??= confirmReason(
          "DELETE queries require human confirmation.",
          "This DELETE query will modify data."
        );
      }
    }
  }

  if (confirmation && !confirmed) {
    return { allowed: false, requiresConfirmation: true, reason: confirmation };
  }
  return { allowed: true, requiresConfirmation: false };
}

/**
 * Validates a write query for safety.
 * - Blocks TRUNCATE entirely
 * - Requires confirmation for DDL (CREATE, DROP, ALTER, RENAME) statements
 * - Blocks DELETE/UPDATE without WHERE clause entirely (even with confirmation)
 * - Blocks DELETE/UPDATE with always-true WHERE clauses (e.g., WHERE 1=1)
 * - Requires confirmation for DELETE queries with WHERE clause
 * - Requires confirmation for Postgres DO blocks
 *
 * Every statement in the query is checked, and every DELETE/UPDATE inside it
 * (behind WITH, inside a CTE, after EXPLAIN ANALYZE). A WHERE only counts at
 * the statement's own level, outside comments and strings, lexed with the
 * rules of `dialect`. Without a dialect the query is checked under both and
 * the stricter result wins. The query sent to the database is unchanged.
 * Queries with an unterminated quote or comment, or (MySQL) an executable
 * comment, are refused.
 *
 * Limits: stored procedures, functions and triggers called by the query run
 * code this check cannot see (EXECUTE and MySQL PREPARE ... FROM ask for
 * confirmation), tautology detection is best effort, and lexing assumes the
 * engine's default modes (no NO_BACKSLASH_ESCAPES or ANSI_QUOTES in MySQL,
 * standard_conforming_strings on in Postgres).
 */
export function validateWriteQuery(
  query: string,
  confirmed: boolean,
  dialect?: SqlDialect
): ValidationResult {
  if (dialect) {
    return validateWithDialect(query, confirmed, dialect);
  }

  const results = (["mysql", "postgres"] as const).map((d) =>
    validateWithDialect(query, confirmed, d)
  );
  return (
    results.find((r) => !r.allowed && !r.requiresConfirmation) ??
    results.find((r) => r.requiresConfirmation) ??
    results[0]!
  );
}

/**
 * The verdict both engines agree on, available before the database kind is
 * known: a refusal, or a confirmation request, that holds under MySQL and
 * Postgres rules alike. Returns undefined when the engine matters (or the
 * query is allowed), so the caller looks up the kind and validates again.
 */
export function precheckWriteQuery(query: string, confirmed: boolean): ValidationResult | undefined {
  const mysql = validateWithDialect(query, confirmed, "mysql");
  const postgres = validateWithDialect(query, confirmed, "postgres");
  if (mysql.allowed || postgres.allowed) return undefined;
  if (mysql.requiresConfirmation === postgres.requiresConfirmation) return mysql;
  return undefined;
}

/** Maps a PlanetScale database kind to the dialect its queries are lexed with. */
export function dialectForKind(kind: string): SqlDialect | undefined {
  if (kind === "mysql") return "mysql";
  if (kind === "postgresql" || kind === "neki") return "postgres";
  return undefined;
}
