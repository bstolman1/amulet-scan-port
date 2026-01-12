/**
 * SQL Sanitization Utilities
 * 
 * Provides input validation and sanitization for DuckDB queries.
 * CRITICAL: Use these functions to prevent SQL injection attacks.
 */

// Dangerous SQL patterns that should be rejected entirely
const DANGEROUS_PATTERNS = [
  /;\s*--/i,           // Comment after statement terminator
  /;\s*\/\*/i,         // Block comment after statement terminator
  /'\s*;\s*DROP/i,     // DROP statement injection
  /'\s*;\s*DELETE/i,   // DELETE statement injection
  /'\s*;\s*UPDATE/i,   // UPDATE statement injection
  /'\s*;\s*INSERT/i,   // INSERT statement injection
  /'\s*;\s*ALTER/i,    // ALTER statement injection
  /'\s*;\s*CREATE/i,   // CREATE statement injection
  /'\s*;\s*TRUNCATE/i, // TRUNCATE statement injection
  /'\s*;\s*EXEC/i,     // EXEC statement injection
  /UNION\s+SELECT/i,   // UNION injection
  /UNION\s+ALL/i,      // UNION ALL injection
  /INTO\s+OUTFILE/i,   // File write injection
  /LOAD_FILE/i,        // File read injection
  /xp_cmdshell/i,      // SQL Server command execution
  /sp_executesql/i,    // SQL Server dynamic execution
];

/**
 * Check if a string contains dangerous SQL patterns
 * @returns {boolean} true if dangerous patterns found
 */
export function containsDangerousPatterns(str) {
  if (typeof str !== 'string') return false;
  return DANGEROUS_PATTERNS.some(pattern => pattern.test(str));
}

/**
 * Escape a string for safe use in SQL LIKE patterns
 * Escapes: single quotes, backslashes, and LIKE wildcards (%, _)
 */
export function escapeLikePattern(str) {
  if (typeof str !== 'string') return '';
  if (containsDangerousPatterns(str)) return '';
  return str
    .replace(/\\/g, '\\\\')   // Escape backslashes first
    .replace(/'/g, "''")       // Escape single quotes
    .replace(/%/g, '\\%')      // Escape LIKE wildcard %
    .replace(/_/g, '\\_');     // Escape LIKE wildcard _
}

/**
 * Escape a string for safe use in SQL string literals
 * Rejects dangerous patterns entirely, then escapes single quotes
 */
export function escapeString(str) {
  if (typeof str !== 'string') return '';
  if (containsDangerousPatterns(str)) return '';
  return str.replace(/'/g, "''");
}

/**
 * Validate and sanitize a numeric parameter
 * Returns the number if valid, otherwise returns the default value
 * Enforces reasonable bounds to prevent DoS attacks
 */
export function sanitizeNumber(value, { min = 0, max = 10000, defaultValue = 0 } = {}) {
  const num = parseInt(value, 10);
  if (isNaN(num)) return defaultValue;
  return Math.min(max, Math.max(min, num));
}

/**
 * Validate a string parameter against an allowed pattern
 * Rejects strings that don't match the pattern or contain dangerous SQL
 */
export function validatePattern(str, pattern, maxLength = 1000) {
  if (typeof str !== 'string') return null;
  if (str.length > maxLength) return null;
  if (containsDangerousPatterns(str)) return null;
  if (!pattern.test(str)) return null;
  return str;
}

/**
 * Validate and sanitize an identifier (table name, column name, etc.)
 * Only allows alphanumeric, underscores, dots, and colons (for Daml template IDs)
 * Rejects any dangerous SQL patterns
 */
export function sanitizeIdentifier(str, maxLength = 500) {
  if (containsDangerousPatterns(str)) return null;
  return validatePattern(str, /^[\w.:@-]+$/i, maxLength);
}

/**
 * Validate a contract/event ID (hex string with optional dashes)
 */
export function sanitizeContractId(str) {
  if (containsDangerousPatterns(str)) return null;
  return validatePattern(str, /^[a-fA-F0-9:-]+$/, 200);
}

/**
 * Validate an event type (created, archived, etc.)
 * Uses whitelist approach - only allows known values
 */
export function sanitizeEventType(str) {
  const allowed = ['created', 'archived', 'exercised', 'CreatedEvent', 'ArchivedEvent', 'ExercisedEvent'];
  if (allowed.includes(str)) return str;
  return null;
}

/**
 * Validate an ISO date/timestamp string
 * Rejects any non-date patterns to prevent injection
 */
export function sanitizeTimestamp(str) {
  if (typeof str !== 'string') return null;
  if (str.length > 50) return null; // Reasonable max length for timestamps
  if (containsDangerousPatterns(str)) return null;
  
  // ISO 8601 format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD
  const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/;
  if (!isoPattern.test(str)) return null;
  
  // Verify it's a valid date using Date.parse
  const parsed = Date.parse(str);
  if (isNaN(parsed)) return null;
  
  return str;
}

/**
 * Validate a search query string
 * Allows alphanumeric, spaces, and common punctuation, but rejects SQL patterns
 */
export function sanitizeSearchQuery(str, maxLength = 200) {
  if (typeof str !== 'string') return null;
  if (str.length > maxLength) return null;
  if (containsDangerousPatterns(str)) return null;
  
  // Allow letters, numbers, spaces, and limited punctuation
  // Reject anything that looks like SQL control characters
  if (/[;'"\\`]/.test(str)) return null;
  
  return str.trim();
}

/**
 * Build a safe WHERE condition for LIKE queries
 * Returns null if the value contains dangerous patterns
 */
export function buildLikeCondition(column, value, position = 'contains') {
  if (containsDangerousPatterns(value)) return null;
  const escaped = escapeLikePattern(value);
  if (!escaped) return null;
  
  switch (position) {
    case 'starts':
      return `${column} LIKE '${escaped}%' ESCAPE '\\'`;
    case 'ends':
      return `${column} LIKE '%${escaped}' ESCAPE '\\'`;
    case 'contains':
    default:
      return `${column} LIKE '%${escaped}%' ESCAPE '\\'`;
  }
}

/**
 * Build a safe equality condition
 * Returns null if the value contains dangerous patterns
 */
export function buildEqualCondition(column, value) {
  if (containsDangerousPatterns(value)) return null;
  const escaped = escapeString(value);
  if (!escaped && value) return null; // Value was rejected
  return `${column} = '${escaped}'`;
}

/**
 * Validate and build a list condition (for IN clauses)
 * Rejects any values containing dangerous patterns
 */
export function buildInCondition(column, values, validator = escapeString) {
  if (!Array.isArray(values) || values.length === 0) return null;
  const sanitized = values
    .filter(v => !containsDangerousPatterns(v))
    .map(v => validator(v))
    .filter(v => v !== null && v !== '');
  if (sanitized.length === 0) return null;
  return `${column} IN (${sanitized.map(v => `'${v}'`).join(', ')})`;
}

export default {
  containsDangerousPatterns,
  escapeLikePattern,
  escapeString,
  sanitizeNumber,
  validatePattern,
  sanitizeIdentifier,
  sanitizeContractId,
  sanitizeEventType,
  sanitizeTimestamp,
  sanitizeSearchQuery,
  buildLikeCondition,
  buildEqualCondition,
  buildInCondition,
};
