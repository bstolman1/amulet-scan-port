/**
 * SQL Sanitization Tests
 * 
 * Critical security tests for SQL injection prevention
 */

import { describe, it, expect } from 'vitest';
import {
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
} from './sql-sanitize.js';

describe('containsDangerousPatterns', () => {
  it('returns false for safe strings', () => {
    expect(containsDangerousPatterns('hello world')).toBe(false);
    expect(containsDangerousPatterns('VoteRequest')).toBe(false);
    expect(containsDangerousPatterns('Splice:Amulet:Amulet')).toBe(false);
    expect(containsDangerousPatterns('party::alice123')).toBe(false);
  });

  it('detects SQL comment injection', () => {
    expect(containsDangerousPatterns("; --")).toBe(true);
    expect(containsDangerousPatterns("; /*")).toBe(true);
  });

  it('detects DROP injection', () => {
    expect(containsDangerousPatterns("'; DROP TABLE users")).toBe(true);
    expect(containsDangerousPatterns("'; DROP DATABASE")).toBe(true);
  });

  it('detects DELETE/UPDATE/INSERT injection', () => {
    expect(containsDangerousPatterns("'; DELETE FROM users")).toBe(true);
    expect(containsDangerousPatterns("'; UPDATE users SET")).toBe(true);
    expect(containsDangerousPatterns("'; INSERT INTO users")).toBe(true);
  });

  it('detects UNION injection', () => {
    expect(containsDangerousPatterns("' UNION SELECT * FROM")).toBe(true);
    expect(containsDangerousPatterns("UNION ALL SELECT")).toBe(true);
  });

  it('detects tautology-based SQL injection', () => {
    // Classic always-true conditions used to bypass authentication
    expect(containsDangerousPatterns("admin' OR 1=1--")).toBe(true);
    expect(containsDangerousPatterns("' OR 1=1")).toBe(true);
    expect(containsDangerousPatterns("x' OR 'a'='a")).toBe(true);
    expect(containsDangerousPatterns("1=1 OR something")).toBe(true);
    expect(containsDangerousPatterns("' OR true")).toBe(true);
  });

  it('detects comment-based bypass attempts', () => {
    expect(containsDangerousPatterns("admin/*bypass*/password")).toBe(true);
    expect(containsDangerousPatterns("query--")).toBe(true);
  });

  it('detects file operations', () => {
    expect(containsDangerousPatterns("INTO OUTFILE '/tmp/data'")).toBe(true);
    expect(containsDangerousPatterns("LOAD_FILE('/etc/passwd')")).toBe(true);
  });

  it('detects SQL Server specific attacks', () => {
    expect(containsDangerousPatterns("xp_cmdshell('dir')")).toBe(true);
    expect(containsDangerousPatterns("sp_executesql")).toBe(true);
  });

  // Mutation-killing: verify non-string guard is required (enforces input trust boundary)
  it('returns false for non-string inputs', () => {
    expect(containsDangerousPatterns(null)).toBe(false);
    expect(containsDangerousPatterns(undefined)).toBe(false);
    expect(containsDangerousPatterns(123)).toBe(false);
    expect(containsDangerousPatterns({})).toBe(false);
    expect(containsDangerousPatterns([])).toBe(false);
  });
});

describe('escapeLikePattern', () => {
  it('escapes single quotes', () => {
    expect(escapeLikePattern("O'Brien")).toBe("O''Brien");
  });

  it('escapes backslashes', () => {
    expect(escapeLikePattern("path\\to\\file")).toBe("path\\\\to\\\\file");
  });

  it('escapes LIKE wildcards', () => {
    expect(escapeLikePattern("100%")).toBe("100\\%");
    expect(escapeLikePattern("file_name")).toBe("file\\_name");
  });

  it('handles combined escaping', () => {
    expect(escapeLikePattern("100% O'Brien")).toBe("100\\% O''Brien");
  });

  it('returns empty string for dangerous patterns', () => {
    expect(escapeLikePattern("'; DROP TABLE")).toBe('');
    expect(escapeLikePattern("UNION SELECT")).toBe('');
  });

  it('handles non-string inputs', () => {
    expect(escapeLikePattern(null)).toBe('');
    expect(escapeLikePattern(undefined)).toBe('');
    expect(escapeLikePattern(123)).toBe('');
  });
});

describe('escapeString', () => {
  it('escapes single quotes', () => {
    expect(escapeString("It's a test")).toBe("It''s a test");
    expect(escapeString("''")).toBe("''''");
  });

  it('leaves safe strings unchanged', () => {
    expect(escapeString("Hello World")).toBe("Hello World");
    expect(escapeString("template_id")).toBe("template_id");
  });

  it('returns empty string for dangerous patterns', () => {
    expect(escapeString("'; DROP TABLE users")).toBe('');
  });

  it('handles non-string inputs', () => {
    expect(escapeString(null)).toBe('');
    expect(escapeString(undefined)).toBe('');
  });
});

describe('sanitizeNumber', () => {
  it('parses valid integers', () => {
    expect(sanitizeNumber('100')).toBe(100);
    expect(sanitizeNumber(50)).toBe(50);
    expect(sanitizeNumber('0')).toBe(0);
  });

  it('returns default for invalid inputs', () => {
    expect(sanitizeNumber('abc')).toBe(0);
    expect(sanitizeNumber('')).toBe(0);
    expect(sanitizeNumber(null)).toBe(0);
    expect(sanitizeNumber(undefined)).toBe(0);
    expect(sanitizeNumber(NaN)).toBe(0);
  });

  it('respects custom default value', () => {
    expect(sanitizeNumber('abc', { defaultValue: 10 })).toBe(10);
  });

  it('enforces minimum bound', () => {
    expect(sanitizeNumber(-5, { min: 0 })).toBe(0);
    expect(sanitizeNumber(5, { min: 10 })).toBe(10);
  });

  it('enforces maximum bound', () => {
    expect(sanitizeNumber(1000, { max: 100 })).toBe(100);
    expect(sanitizeNumber(50, { max: 100 })).toBe(50);
  });

  it('enforces both bounds', () => {
    expect(sanitizeNumber(500, { min: 1, max: 100 })).toBe(100);
    expect(sanitizeNumber(-10, { min: 1, max: 100 })).toBe(1);
    expect(sanitizeNumber(50, { min: 1, max: 100 })).toBe(50);
  });
});

describe('validatePattern', () => {
  it('validates against regex pattern', () => {
    expect(validatePattern('abc123', /^[a-z0-9]+$/)).toBe('abc123');
    expect(validatePattern('ABC', /^[a-z]+$/)).toBe(null);
  });

  it('rejects strings exceeding max length', () => {
    expect(validatePattern('abc', /^.*$/, 2)).toBe(null);
    expect(validatePattern('ab', /^.*$/, 2)).toBe('ab');
  });

  it('rejects dangerous patterns', () => {
    expect(validatePattern("'; DROP TABLE", /^.*$/)).toBe(null);
  });

  it('handles non-string inputs', () => {
    expect(validatePattern(null, /^.*$/)).toBe(null);
    expect(validatePattern(123, /^.*$/)).toBe(null);
  });
});

describe('sanitizeIdentifier', () => {
  it('accepts valid identifiers', () => {
    expect(sanitizeIdentifier('template_id')).toBe('template_id');
    expect(sanitizeIdentifier('Splice:Amulet:Amulet')).toBe('Splice:Amulet:Amulet');
    expect(sanitizeIdentifier('party::alice')).toBe('party::alice');
    expect(sanitizeIdentifier('user-123')).toBe('user-123');
    expect(sanitizeIdentifier('user@domain')).toBe('user@domain');
  });

  it('rejects identifiers with special SQL characters', () => {
    expect(sanitizeIdentifier("table'; DROP")).toBe(null);
    expect(sanitizeIdentifier('table/*comment*/')).toBe(null);
  });

  it('rejects identifiers exceeding max length', () => {
    const longId = 'a'.repeat(501);
    expect(sanitizeIdentifier(longId)).toBe(null);
  });

  // Mutation-killing tests: explicitly verify dangerous pattern gate
  it('rejects semicolon-based injection', () => {
    expect(sanitizeIdentifier("abc; DROP TABLE users")).toBeNull();
    expect(sanitizeIdentifier("id; DELETE FROM")).toBeNull();
  });

  it('rejects OR 1=1 tautology injection', () => {
    expect(sanitizeIdentifier("' OR 1=1 --")).toBeNull();
    expect(sanitizeIdentifier("admin' OR 1=1")).toBeNull();
  });

  it('rejects UNION injection', () => {
    expect(sanitizeIdentifier("id UNION SELECT *")).toBeNull();
  });
});

describe('sanitizeContractId', () => {
  it('accepts valid Daml contract IDs', () => {
    // Real Daml contract ID formats: hex::Package.Module:Template
    expect(sanitizeContractId('00abc123def')).toBe('00abc123def');
    expect(sanitizeContractId('AABBCC')).toBe('AABBCC');
    expect(sanitizeContractId('00abc123::Splice.Amulet:Amulet')).toBe('00abc123::Splice.Amulet:Amulet');
    expect(sanitizeContractId('00def456::Splice.ValidatorLicense:ValidatorLicense')).toBe('00def456::Splice.ValidatorLicense:ValidatorLicense');
    expect(sanitizeContractId('00abc123::Module:Template#suffix')).toBe('00abc123::Module:Template#suffix');
  });

  it('rejects SQL injection attempts in contract IDs', () => {
    expect(sanitizeContractId("00abc'; DROP TABLE--")).toBe(null);
    expect(sanitizeContractId('00abc UNION SELECT * FROM')).toBe(null);
    expect(sanitizeContractId("00abc' OR 1=1")).toBe(null);
  });

  // Mutation-killing: verify dangerous-pattern guard is required
  it('rejects contract IDs containing SQL injection patterns', () => {
    expect(sanitizeContractId("deadbeef::Tpl@abc'; DROP")).toBeNull();
    expect(sanitizeContractId("00abc123' OR 1=1 --")).toBeNull();
  });

  it('rejects malformed contract IDs', () => {
    expect(sanitizeContractId('')).toBe(null);
    expect(sanitizeContractId('not-a-valid-id!')).toBe(null);
    expect(sanitizeContractId('abc xyz')).toBe(null);
  });
});

describe('sanitizeEventType', () => {
  it('accepts valid event types', () => {
    expect(sanitizeEventType('created')).toBe('created');
    expect(sanitizeEventType('archived')).toBe('archived');
    expect(sanitizeEventType('exercised')).toBe('exercised');
    expect(sanitizeEventType('CreatedEvent')).toBe('CreatedEvent');
    expect(sanitizeEventType('ArchivedEvent')).toBe('ArchivedEvent');
  });

  it('rejects invalid event types', () => {
    expect(sanitizeEventType('deleted')).toBe(null);
    expect(sanitizeEventType('unknown')).toBe(null);
    expect(sanitizeEventType("created'; DROP")).toBe(null);
  });
});

describe('sanitizeTimestamp', () => {
  it('accepts valid ISO timestamps', () => {
    expect(sanitizeTimestamp('2024-01-15')).toBe('2024-01-15');
    expect(sanitizeTimestamp('2024-01-15T10:30:00')).toBe('2024-01-15T10:30:00');
    expect(sanitizeTimestamp('2024-01-15T10:30:00Z')).toBe('2024-01-15T10:30:00Z');
    expect(sanitizeTimestamp('2024-01-15T10:30:00.123Z')).toBe('2024-01-15T10:30:00.123Z');
    expect(sanitizeTimestamp('2024-01-15T10:30:00+05:30')).toBe('2024-01-15T10:30:00+05:30');
  });

  it('rejects invalid timestamps', () => {
    expect(sanitizeTimestamp('not-a-date')).toBe(null);
    expect(sanitizeTimestamp('2024-13-01')).toBe(null); // Invalid month
    expect(sanitizeTimestamp('2024-01-32')).toBe(null); // Invalid day
    expect(sanitizeTimestamp("2024-01-15'; DROP")).toBe(null);
  });

  it('rejects excessively long strings', () => {
    const longTimestamp = '2024-01-15T10:30:00' + 'Z'.repeat(100);
    expect(sanitizeTimestamp(longTimestamp)).toBe(null);
  });

  it('handles non-string inputs', () => {
    expect(sanitizeTimestamp(null)).toBe(null);
    expect(sanitizeTimestamp(123)).toBe(null);
  });

  // Mutation-killing tests: strict ISO format validation
  it('rejects malformed time components', () => {
    expect(sanitizeTimestamp('2025-01-01T99:99:99Z')).toBeNull();
    expect(sanitizeTimestamp('2025-01-01T25:00:00Z')).toBeNull();
    expect(sanitizeTimestamp('2025-01-01T12:60:00Z')).toBeNull();
  });

  it('rejects trailing garbage after valid timestamp', () => {
    expect(sanitizeTimestamp('2025-01-01T00:00:00ZZ')).toBeNull();
    expect(sanitizeTimestamp('2025-01-01T00:00:00Z extra')).toBeNull();
  });

  // Mutation-killing: verify length + dangerous-pattern guards
  it('rejects timestamps longer than max length', () => {
    expect(sanitizeTimestamp('2025-01-01T00:00:00Z'.repeat(10))).toBeNull();
  });

  it('rejects timestamps with SQL injection', () => {
    expect(sanitizeTimestamp("2025-01-01T00:00:00Z'; DROP")).toBeNull();
  });
});

describe('sanitizeSearchQuery', () => {
  it('accepts safe search queries', () => {
    expect(sanitizeSearchQuery('hello world')).toBe('hello world');
    expect(sanitizeSearchQuery('VoteRequest')).toBe('VoteRequest');
    expect(sanitizeSearchQuery('alice123')).toBe('alice123');
  });

  it('trims whitespace', () => {
    expect(sanitizeSearchQuery('  hello  ')).toBe('hello');
  });

  it('rejects SQL control characters', () => {
    expect(sanitizeSearchQuery("hello' world")).toBe(null);
    expect(sanitizeSearchQuery('hello; world')).toBe(null);
    expect(sanitizeSearchQuery('hello"world')).toBe(null);
    expect(sanitizeSearchQuery('hello\\world')).toBe(null);
    expect(sanitizeSearchQuery('hello`world')).toBe(null);
  });

  it('rejects excessively long queries', () => {
    const longQuery = 'a'.repeat(201);
    expect(sanitizeSearchQuery(longQuery)).toBe(null);
  });
});

describe('buildLikeCondition', () => {
  it('builds contains condition', () => {
    expect(buildLikeCondition('name', 'test')).toBe("name LIKE '%test%' ESCAPE '\\'");
  });

  it('builds starts-with condition', () => {
    expect(buildLikeCondition('name', 'test', 'starts')).toBe("name LIKE 'test%' ESCAPE '\\'");
  });

  it('builds ends-with condition', () => {
    expect(buildLikeCondition('name', 'test', 'ends')).toBe("name LIKE '%test' ESCAPE '\\'");
  });

  it('escapes special characters', () => {
    expect(buildLikeCondition('name', "O'Brien")).toBe("name LIKE '%O''Brien%' ESCAPE '\\'");
    expect(buildLikeCondition('name', '100%')).toBe("name LIKE '%100\\%%' ESCAPE '\\'");
  });

  it('returns null for dangerous patterns', () => {
    expect(buildLikeCondition('name', "'; DROP TABLE")).toBe(null);
  });

  // Mutation-killing tests: explicit LIKE injection bypass attempts
  it('returns null for LIKE injection attempts', () => {
    expect(buildLikeCondition('name', "' OR 1=1 --")).toBeNull();
    expect(buildLikeCondition('name', "'; DROP TABLE users")).toBeNull();
    expect(buildLikeCondition('name', "' UNION SELECT *")).toBeNull();
  });

  it('returns null for null/undefined values', () => {
    expect(buildLikeCondition('name', null)).toBeNull();
    expect(buildLikeCondition('name', undefined)).toBeNull();
  });
});

describe('buildEqualCondition', () => {
  it('builds equality condition', () => {
    expect(buildEqualCondition('name', 'test')).toBe("name = 'test'");
  });

  it('escapes single quotes', () => {
    expect(buildEqualCondition('name', "O'Brien")).toBe("name = 'O''Brien'");
  });

  it('returns null for dangerous patterns', () => {
    expect(buildEqualCondition('name', "'; DROP TABLE")).toBe(null);
  });
});

describe('buildInCondition', () => {
  it('builds IN condition', () => {
    expect(buildInCondition('type', ['a', 'b', 'c'])).toBe("type IN ('a', 'b', 'c')");
  });

  it('escapes values', () => {
    expect(buildInCondition('name', ["O'Brien", 'Smith'])).toBe("name IN ('O''Brien', 'Smith')");
  });

  it('filters out dangerous values', () => {
    expect(buildInCondition('type', ['safe', "'; DROP TABLE"])).toBe("type IN ('safe')");
  });

  it('returns null for empty arrays', () => {
    expect(buildInCondition('type', [])).toBe(null);
    expect(buildInCondition('type', ["'; DROP TABLE"])).toBe(null); // All values filtered
  });

  it('handles non-array inputs', () => {
    expect(buildInCondition('type', null)).toBe(null);
    expect(buildInCondition('type', 'string')).toBe(null);
  });

  // Mutation-killing tests: verify exact filtering logic
  it('filters out null and empty values in IN condition', () => {
    const clause = buildInCondition('party', ['Alice', '', 'Bob']);
    expect(clause).toContain("'Alice'");
    expect(clause).toContain("'Bob'");
    expect(clause).not.toContain("''");
  });

  it('returns null when all values are filtered out', () => {
    expect(buildInCondition('party', ['', '', ''])).toBeNull();
  });

  // Mutation-killing: verify dangerous values are filtered before building
  it('filters out dangerous and empty values before building IN clause', () => {
    const clause = buildInCondition('party', ['Alice', '', null, "' OR 1=1 --"]);
    expect(clause).toContain("'Alice'");
    expect(clause).not.toContain("1=1");
  });
});
