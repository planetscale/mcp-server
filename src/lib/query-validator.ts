export interface ValidationResult {
  allowed: boolean;
  requiresConfirmation: boolean;
  reason?: string;
}

/**
 * Replaces comments (and, when `strings` is set, quoted strings, quoted
 * identifiers and dollar-quoted bodies) with spaces of the same length, so
 * keyword checks only see SQL that executes. Offsets are preserved.
 *
 * Unterminated comments or quotes are masked to the end of the query, which
 * can only hide keywords and so makes the checks below stricter.
 */
export function maskSql(query: string, options: { strings: boolean }): string {
  const out: string[] = [];
  const n = query.length;
  let i = 0;

  const blank = (from: number, to: number) => {
    for (let k = from; k < to; k++) out.push(query[k] === "\n" ? "\n" : " ");
  };

  while (i < n) {
    const c = query[i]!;
    const next = query[i + 1];

    // -- and # line comments
    if ((c === "-" && next === "-") || c === "#") {
      const end = query.indexOf("\n", i);
      const stop = end === -1 ? n : end;
      blank(i, stop);
      i = stop;
      continue;
    }

    // /* */ block comments, nested the way Postgres nests them
    if (c === "/" && next === "*") {
      let depth = 1;
      let j = i + 2;
      while (j < n && depth > 0) {
        if (query[j] === "/" && query[j + 1] === "*") {
          depth++;
          j += 2;
        } else if (query[j] === "*" && query[j + 1] === "/") {
          depth--;
          j += 2;
        } else {
          j++;
        }
      }
      blank(i, j);
      i = j;
      continue;
    }

    if (options.strings) {
      // 'strings', "identifiers", `identifiers`; doubled quote or backslash escapes
      if (c === "'" || c === '"' || c === "`") {
        let j = i + 1;
        while (j < n) {
          if (query[j] === "\\" && c !== "`") {
            j += 2;
            continue;
          }
          if (query[j] === c) {
            if (query[j + 1] === c) {
              j += 2;
              continue;
            }
            j++;
            break;
          }
          j++;
        }
        blank(i, Math.min(j, n));
        i = Math.min(j, n);
        continue;
      }

      // $$ ... $$ and $tag$ ... $tag$ (Postgres)
      if (c === "$") {
        const tag = /^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/.exec(query.slice(i));
        if (tag) {
          const close = query.indexOf(tag[0], i + tag[0].length);
          const stop = close === -1 ? n : close + tag[0].length;
          blank(i, stop);
          i = stop;
          continue;
        }
      }
    }

    out.push(c);
    i++;
  }

  return out.join("");
}

const MAIN_STATEMENT_KEYWORDS = new Set(["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE"]);

/**
 * Finds where the statement that runs starts. For `WITH ... AS (...) DELETE ...`
 * that is the first top-level SELECT/INSERT/UPDATE/DELETE/MERGE/REPLACE after a
 * CTE body closes. Returns -1 when a WITH query has no recognizable main
 * statement.
 */
function mainStatementStart(masked: string): number {
  const leading = masked.length - masked.trimStart().length;
  if (!/^WITH\b/i.test(masked.slice(leading))) {
    return leading;
  }

  let depth = 0;
  let closedCte = false;
  const wordRe = /[A-Za-z_]+|[()]/g;
  for (let m = wordRe.exec(masked); m !== null; m = wordRe.exec(masked)) {
    const token = m[0];
    if (token === "(") {
      depth++;
    } else if (token === ")") {
      depth = Math.max(0, depth - 1);
      if (depth === 0) closedCte = true;
    } else if (depth === 0 && closedCte && MAIN_STATEMENT_KEYWORDS.has(token.toUpperCase())) {
      return m.index;
    }
  }
  return -1;
}

/**
 * Detects if a WHERE clause is a tautology (always true).
 * Common patterns: WHERE 1=1, WHERE 1, WHERE TRUE, WHERE 'a'='a', etc.
 * Expects comments already masked; a trailing semicolon is ignored.
 */
function hasAlwaysTrueWhereClause(query: string): boolean {
  const normalized = query.toUpperCase();

  // Extract the WHERE clause
  const whereMatch = normalized.match(/\bWHERE\s+(.+?)(?:ORDER\s+BY|GROUP\s+BY|LIMIT|$)/is);
  if (!whereMatch || !whereMatch[1]) return false;

  const whereClause = whereMatch[1].trim().replace(/[;\s]+$/, "");

  // Patterns that are always true
  const alwaysTruePatterns = [
    /^1\s*=\s*1$/,           // WHERE 1=1
    /^1$/,                    // WHERE 1
    /^TRUE$/,                 // WHERE TRUE
    /^'[^']*'\s*=\s*'[^']*'$/, // WHERE 'a'='a' (same string comparison)
    /^"[^"]*"\s*=\s*"[^"]*"$/, // WHERE "a"="a"
    /^\d+\s*=\s*\d+$/,        // WHERE 2=2, etc. (will check if equal below)
  ];

  for (const pattern of alwaysTruePatterns) {
    if (pattern.test(whereClause)) {
      // For numeric comparisons, verify both sides are equal
      if (whereClause.includes('=')) {
        const parts = whereClause.split('=').map(p => p.trim());
        if (parts.length === 2 && parts[0] === parts[1]) {
          return true;
        }
        // Check for 1=1 style
        const left = parts[0];
        const right = parts[1];
        if (left && right && /^\d+$/.test(left) && /^\d+$/.test(right) && left === right) {
          return true;
        }
      } else {
        return true;
      }
    }
  }

  // Check for string literal equality like 'a'='a'
  const stringEqualityMatch = whereClause.match(/^(['"])(.+?)\1\s*=\s*(['"])(.+?)\3$/);
  if (stringEqualityMatch) {
    const leftStr = stringEqualityMatch[2];
    const rightStr = stringEqualityMatch[4];
    if (leftStr && rightStr && leftStr === rightStr) {
      return true;
    }
  }

  return false;
}

/**
 * Detects if a query is a DDL (Data Definition Language) statement.
 * DDL statements modify database schema: CREATE, DROP, ALTER, TRUNCATE, RENAME.
 * Any object type counts (sequences, extensions, types, materialized views,
 * OR REPLACE functions, UNIQUE indexes, DROP OWNED, ...).
 */
function isDDLQuery(statement: string): { isDDL: boolean; statement: string | null } {
  const match = /^(CREATE|DROP|ALTER|TRUNCATE|RENAME)\b/i.exec(statement.trimStart());
  if (!match || !match[1]) {
    return { isDDL: false, statement: null };
  }
  return { isDDL: true, statement: match[1].toUpperCase() };
}

/**
 * Validates a write query for safety.
 * - Blocks TRUNCATE entirely
 * - Requires confirmation for DDL (CREATE, DROP, ALTER, RENAME) statements
 * - Blocks DELETE/UPDATE without WHERE clause entirely (even with confirmation)
 * - Blocks DELETE/UPDATE with always-true WHERE clauses (e.g., WHERE 1=1)
 * - Requires confirmation for DELETE queries with WHERE clause
 *
 * Checks run on the query with comments masked, and WHERE detection also
 * ignores string literals, so a WHERE that only appears in a comment or a
 * string does not count. A DELETE or UPDATE behind a WITH clause is checked
 * like the bare statement. The query sent to the database is unchanged.
 */
export function validateWriteQuery(
  query: string,
  confirmed: boolean
): ValidationResult {
  // MySQL runs the body of /*! ... */ comments, so masking them would hide SQL
  if (query.includes("/*!")) {
    return {
      allowed: false,
      requiresConfirmation: false,
      reason: "Queries containing MySQL executable comments (/*! ... */) are not allowed.",
    };
  }

  const withoutComments = maskSql(query, { strings: false });
  const masked = maskSql(query, { strings: true });

  const start = mainStatementStart(masked);
  if (start === -1) {
    if (!confirmed) {
      return {
        allowed: false,
        requiresConfirmation: true,
        reason: "This WITH query has no recognizable main statement, so it cannot be checked automatically. STOP and ask the user: 'Do you want me to proceed with: [show the query]?' Only proceed with confirm_destructive: true after the user explicitly approves.",
      };
    }
    return { allowed: true, requiresConfirmation: false };
  }

  const statement = withoutComments.slice(start);
  const maskedStatement = masked.slice(start);
  const normalized = maskedStatement.trimStart().toUpperCase();

  // Check for DDL statements
  const ddlCheck = isDDLQuery(maskedStatement);

  // Block TRUNCATE entirely
  if (normalized.startsWith("TRUNCATE")) {
    return {
      allowed: false,
      requiresConfirmation: false,
      reason: "TRUNCATE is not allowed. This operation cannot be undone.",
    };
  }

  // DDL statements (DROP, ALTER, CREATE, RENAME) require confirmation
  if (ddlCheck.isDDL && ddlCheck.statement !== "TRUNCATE") {
    if (!confirmed) {
      return {
        allowed: false,
        requiresConfirmation: true,
        reason: `${ddlCheck.statement} is a DDL (schema-modifying) statement that requires human confirmation. STOP and ask the user: 'This ${ddlCheck.statement} query will modify the database schema. Do you want me to proceed with: [show the query]?' Only proceed with confirm_destructive: true after the user explicitly approves.`,
      };
    }
  }

  // DELETE validation
  if (normalized.startsWith("DELETE")) {
    const hasWhere = /\bWHERE\b/i.test(maskedStatement);

    // Block DELETE without WHERE entirely
    if (!hasWhere) {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "DELETE without a WHERE clause is not allowed. This would affect the entire table.",
      };
    }

    // Block DELETE with always-true WHERE clause (e.g., WHERE 1=1)
    if (hasAlwaysTrueWhereClause(statement)) {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "DELETE with an always-true WHERE clause (e.g., WHERE 1=1) is not allowed. This would affect the entire table.",
      };
    }

    // DELETE with WHERE requires confirmation
    if (!confirmed) {
      return {
        allowed: false,
        requiresConfirmation: true,
        reason: "DELETE queries require human confirmation. STOP and ask the user: 'This DELETE query will modify data. Do you want me to proceed with: [show the query]?' Only proceed with confirm_destructive: true after the user explicitly approves.",
      };
    }
  }

  // UPDATE validation
  if (normalized.startsWith("UPDATE")) {
    const hasWhere = /\bWHERE\b/i.test(maskedStatement);

    // Block UPDATE without WHERE entirely
    if (!hasWhere) {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "UPDATE without a WHERE clause is not allowed. This would affect the entire table.",
      };
    }

    // Block UPDATE with always-true WHERE clause
    if (hasAlwaysTrueWhereClause(statement)) {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "UPDATE with an always-true WHERE clause (e.g., WHERE 1=1) is not allowed. This would affect the entire table.",
      };
    }
  }

  return { allowed: true, requiresConfirmation: false };
}
