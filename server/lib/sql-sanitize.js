/**
 * SQL Sanitization Utilities
 * 
 * Provides input validation and sanitization for DuckDB queries.
 * CRITICAL: Use these functions to prevent SQL injection attacks.
 */

/**
 * Escape a string for safe use in SQL LIKE patterns
 * Escapes: single quotes, backslashes, and LIKE wildcards (%, _)
 */
export function escapeLikePattern(str) {
  if (typeof str !== 'string') return '';
  return str
    .replace(/\\/g, '\\\\')   // Escape backslashes first
    .replace(/'/g, "''")       // Escape single quotes
    .replace(/%/g, '\\%')      // Escape LIKE wildcard %
    .replace(/_/g, '\\_');     // Escape LIKE wildcard _
}

/**
 * Escape a string for safe use in SQL string literals
 * Only escapes single quotes (standard SQL escaping)
 */
export function escapeString(str) {
  if (typeof str !== 'string') return '';
  return str.replace(/'/g, "''");
}

/**
 * Validate and sanitize a numeric parameter
 * Returns the number if valid, otherwise returns the default value
 */
export function sanitizeNumber(value, { min = 0, max = Infinity, defaultValue = 0 } = {}) {
  const num = parseInt(value, 10);
  if (isNaN(num)) return defaultValue;
  return Math.min(max, Math.max(min, num));
}

/**
 * Validate a string parameter against an allowed pattern
 * Rejects strings that don't match the pattern
 */
export function validatePattern(str, pattern, maxLength = 1000) {
  if (typeof str !== 'string') return null;
  if (str.length > maxLength) return null;
  if (!pattern.test(str)) return null;
  return str;
}

/**
 * Validate and sanitize an identifier (table name, column name, etc.)
 * Only allows alphanumeric, underscores, dots, and colons (for Daml template IDs)
 */
export function sanitizeIdentifier(str, maxLength = 500) {
  return validatePattern(str, /^[\w.:@-]+$/i, maxLength);
}

/**
 * Validate a contract/event ID (hex string with optional dashes)
 */
export function sanitizeContractId(str) {
  return validatePattern(str, /^[a-fA-F0-9:-]+$/, 200);
}

/**
 * Validate an event type (created, archived, etc.)
 */
export function sanitizeEventType(str) {
  const allowed = ['created', 'archived', 'exercised', 'CreatedEvent', 'ArchivedEvent', 'ExercisedEvent'];
  if (allowed.includes(str)) return str;
  return null;
}

/**
 * Validate an ISO date/timestamp string
 */
export function sanitizeTimestamp(str) {
  if (typeof str !== 'string') return null;
  // ISO 8601 format: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD
  const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/;
  if (!isoPattern.test(str)) return null;
  // Verify it's a valid date
  const date = new Date(str);
  if (isNaN(date.getTime())) return null;
  return str;
}

/**
 * Build a safe WHERE condition for LIKE queries
 */
export function buildLikeCondition(column, value, position = 'contains') {
  const escaped = escapeLikePattern(value);
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
 */
export function buildEqualCondition(column, value) {
  const escaped = escapeString(value);
  return `${column} = '${escaped}'`;
}

/**
 * Validate and build a list condition (for IN clauses)
 */
export function buildInCondition(column, values, validator = escapeString) {
  if (!Array.isArray(values) || values.length === 0) return null;
  const sanitized = values
    .map(v => validator(v))
    .filter(v => v !== null && v !== '');
  if (sanitized.length === 0) return null;
  return `${column} IN (${sanitized.map(v => `'${v}'`).join(', ')})`;
}

export default {
  escapeLikePattern,
  escapeString,
  sanitizeNumber,
  validatePattern,
  sanitizeIdentifier,
  sanitizeContractId,
  sanitizeEventType,
  sanitizeTimestamp,
  buildLikeCondition,
  buildEqualCondition,
  buildInCondition,
};
