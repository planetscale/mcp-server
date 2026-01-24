export interface ValidationResult {
  allowed: boolean;
  requiresConfirmation: boolean;
  reason?: string;
}

/**
 * Detects if a WHERE clause is a tautology (always true).
 * Common patterns: WHERE 1=1, WHERE 1, WHERE TRUE, WHERE 'a'='a', etc.
 */
function hasAlwaysTrueWhereClause(query: string): boolean {
  const normalized = query.toUpperCase();

  // Extract the WHERE clause
  const whereMatch = normalized.match(/\bWHERE\s+(.+?)(?:ORDER\s+BY|GROUP\s+BY|LIMIT|$)/i);
  if (!whereMatch || !whereMatch[1]) return false;

  const whereClause = whereMatch[1].trim();

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
 * Validates a write query for safety.
 * - Blocks TRUNCATE entirely
 * - Blocks DELETE/UPDATE without WHERE clause entirely (even with confirmation)
 * - Blocks DELETE/UPDATE with always-true WHERE clauses (e.g., WHERE 1=1)
 * - Requires confirmation for DELETE queries with WHERE clause
 */
export function validateWriteQuery(
  query: string,
  confirmed: boolean
): ValidationResult {
  const normalized = query.trim().toUpperCase();

  // Block TRUNCATE entirely
  if (normalized.startsWith("TRUNCATE")) {
    return {
      allowed: false,
      requiresConfirmation: false,
      reason: "TRUNCATE is not allowed. This operation cannot be undone.",
    };
  }

  // DELETE validation
  if (normalized.startsWith("DELETE")) {
    const hasWhere = /\bWHERE\b/i.test(query);

    // Block DELETE without WHERE entirely
    if (!hasWhere) {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "DELETE without a WHERE clause is not allowed. This would affect the entire table.",
      };
    }

    // Block DELETE with always-true WHERE clause (e.g., WHERE 1=1)
    if (hasAlwaysTrueWhereClause(query)) {
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
    const hasWhere = /\bWHERE\b/i.test(query);

    // Block UPDATE without WHERE entirely
    if (!hasWhere) {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "UPDATE without a WHERE clause is not allowed. This would affect the entire table.",
      };
    }

    // Block UPDATE with always-true WHERE clause
    if (hasAlwaysTrueWhereClause(query)) {
      return {
        allowed: false,
        requiresConfirmation: false,
        reason: "UPDATE with an always-true WHERE clause (e.g., WHERE 1=1) is not allowed. This would affect the entire table.",
      };
    }
  }

  return { allowed: true, requiresConfirmation: false };
}

/**
 * Validates that a query is read-only (SELECT, SHOW, DESCRIBE, EXPLAIN)
 */
export function validateReadQuery(query: string): ValidationResult {
  const normalized = query.trim().toUpperCase();

  // Allow read-only operations
  const readOnlyPrefixes = ["SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN"];
  const isReadOnly = readOnlyPrefixes.some((prefix) =>
    normalized.startsWith(prefix)
  );

  if (!isReadOnly) {
    return {
      allowed: false,
      requiresConfirmation: false,
      reason:
        "Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed with execute_read_query. Use execute_write_query for INSERT, UPDATE, or DELETE operations.",
    };
  }

  return { allowed: true, requiresConfirmation: false };
}
