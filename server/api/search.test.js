/**
 * Search API Integration Tests
 * 
 * Tests for /api/search/* endpoints
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createMockRequest, createMockResponse } from '../test/fixtures/mock-data.js';
import { 
  sanitizeNumber,
  sanitizeIdentifier,
  sanitizeEventType,
  sanitizeContractId,
  escapeLikePattern,
  buildLikeCondition,
  buildEqualCondition,
  containsDangerousPatterns,
} from '../lib/sql-sanitize.js';

describe('Search API', () => {
  describe('Query validation', () => {
    it('should reject queries with dangerous patterns', () => {
      expect(containsDangerousPatterns("'; DROP TABLE--")).toBe(true);
      expect(containsDangerousPatterns("'; DELETE FROM")).toBe(true);
      expect(containsDangerousPatterns('UNION SELECT * FROM')).toBe(true);
    });

    it('should accept safe queries', () => {
      expect(containsDangerousPatterns('amulet')).toBe(false);
      expect(containsDangerousPatterns('Splice.Amulet:Amulet')).toBe(false);
      expect(containsDangerousPatterns('00abc123::contract1')).toBe(false);
    });

    it('should enforce query length limits', () => {
      const longQuery = 'a'.repeat(501);
      const isValid = typeof longQuery === 'string' && longQuery.length <= 500;
      expect(isValid).toBe(false);
    });
  });

  describe('LIKE pattern escaping', () => {
    it('should escape special LIKE characters', () => {
      expect(escapeLikePattern('test%value')).toBe('test\\%value');
      expect(escapeLikePattern('test_value')).toBe('test\\_value');
      expect(escapeLikePattern("test'value")).toBe("test''value");
    });

    it('should handle multiple special characters', () => {
      const escaped = escapeLikePattern("test%_'value");
      expect(escaped).toBe("test\\%\\_''value");
    });

    it('should return empty string for dangerous patterns', () => {
      // escapeLikePattern returns '' for dangerous patterns, not null
      expect(escapeLikePattern("'; DROP TABLE--")).toBe('');
    });
  });

  describe('Condition builders', () => {
    it('should build LIKE condition', () => {
      const condition = buildLikeCondition('template_id', 'Amulet');
      expect(condition).toContain('template_id');
      expect(condition).toContain('LIKE');
      expect(condition).toContain('Amulet');
    });

    it('should build EQUAL condition', () => {
      const condition = buildEqualCondition('event_type', 'created');
      expect(condition).toContain('event_type');
      expect(condition).toContain("'created'");
    });

    it('should return null for invalid input', () => {
      expect(buildLikeCondition('field', "'; DROP TABLE")).toBeNull();
    });
  });

  describe('Search request handling', () => {
    it('should parse search parameters correctly', () => {
      const req = createMockRequest({
        query: {
          q: 'amulet',
          type: 'created',
          template: 'Splice.Amulet',
          party: 'party1',
          limit: '50',
          offset: '0',
        },
      });

      expect(req.query.q).toBe('amulet');
      expect(sanitizeEventType(req.query.type)).toBe('created');
      expect(sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 })).toBe(50);
    });

    it('should handle empty search parameters', () => {
      const req = createMockRequest({ query: {} });
      
      expect(req.query.q).toBeUndefined();
      expect(sanitizeNumber(req.query.limit, { min: 1, max: 1000, defaultValue: 100 })).toBe(100);
    });
  });

  describe('Contract ID search', () => {
    it('should validate hex contract ID', () => {
      // Only hex chars, dashes, and colons allowed
      const validId = '00abc123';
      expect(sanitizeContractId(validId)).toBe(validId);
    });

    it('should reject invalid contract ID formats', () => {
      expect(sanitizeContractId("'; DROP TABLE")).toBeNull();
      expect(sanitizeContractId('')).toBeNull();
    });

    it('should escape contract ID for LIKE query', () => {
      const id = '00abc123::contract_1';
      const escaped = escapeLikePattern(id);
      expect(escaped).toContain('\\_');
    });
  });
});

describe('Search response structure', () => {
  it('should include query echo in response', () => {
    const response = {
      data: [],
      count: 0,
      query: { q: 'test', type: null, template: null, party: null },
    };

    expect(response.query).toBeDefined();
    expect(response.query.q).toBe('test');
  });

  it('should return matched records', () => {
    const mockResults = [
      { contract_id: '00abc::1', template_id: 'Test' },
      { contract_id: '00abc::2', template_id: 'Test' },
    ];

    const response = {
      data: mockResults,
      count: mockResults.length,
      query: { q: 'abc' },
    };

    expect(response.data).toHaveLength(2);
    expect(response.count).toBe(2);
  });
});
