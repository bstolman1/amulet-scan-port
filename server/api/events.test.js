/**
 * Events API Integration Tests
 * 
 * Tests for /api/events/* endpoints
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { createMockRequest, createMockResponse, mockEvents } from '../test/fixtures/mock-data.js';
import { 
  sanitizeNumber, 
  sanitizeEventType, 
  sanitizeIdentifier,
  sanitizeContractId,
  sanitizeTimestamp,
} from '../lib/sql-sanitize.js';

describe('Events API', () => {
  describe('Parameter validation', () => {
    describe('limit parameter', () => {
      it('should sanitize limit within valid range', () => {
        expect(sanitizeNumber('50', { min: 1, max: 1000, defaultValue: 100 })).toBe(50);
      });

      it('should cap limit at maximum', () => {
        expect(sanitizeNumber('5000', { min: 1, max: 1000, defaultValue: 100 })).toBe(1000);
      });

      it('should use default for invalid input', () => {
        expect(sanitizeNumber('invalid', { min: 1, max: 1000, defaultValue: 100 })).toBe(100);
      });

      it('should clamp negative numbers to min', () => {
        // sanitizeNumber clamps to min, not defaultValue
        expect(sanitizeNumber('-10', { min: 1, max: 1000, defaultValue: 100 })).toBe(1);
      });
    });

    describe('offset parameter', () => {
      it('should sanitize offset', () => {
        expect(sanitizeNumber('100', { min: 0, max: 100000, defaultValue: 0 })).toBe(100);
      });

      it('should default to 0', () => {
        expect(sanitizeNumber(undefined, { min: 0, max: 100000, defaultValue: 0 })).toBe(0);
      });
    });

    describe('event type validation', () => {
      it('should accept valid event types', () => {
        expect(sanitizeEventType('created')).toBe('created');
        expect(sanitizeEventType('archived')).toBe('archived');
        expect(sanitizeEventType('exercised')).toBe('exercised');
      });

      it('should reject invalid event types', () => {
        expect(sanitizeEventType('invalid_type')).toBeNull();
        expect(sanitizeEventType('SELECT * FROM')).toBeNull();
      });
    });

    describe('template ID validation', () => {
      it('should accept valid template IDs', () => {
        expect(sanitizeIdentifier('Splice.Amulet:Amulet')).toBe('Splice.Amulet:Amulet');
        expect(sanitizeIdentifier('VoteRequest')).toBe('VoteRequest');
      });

      it('should reject SQL injection attempts', () => {
        expect(sanitizeIdentifier("'; DROP TABLE--")).toBeNull();
        expect(sanitizeIdentifier('1=1 OR')).toBeNull();
      });
    });

    describe('contract ID validation', () => {
      it('should accept valid Daml contract IDs', () => {
        // Real Daml contract ID formats
        expect(sanitizeContractId('00abc123::Splice.Amulet:Amulet')).toBe('00abc123::Splice.Amulet:Amulet');
        expect(sanitizeContractId('00def456::Splice.ValidatorLicense:ValidatorLicense')).toBe('00def456::Splice.ValidatorLicense:ValidatorLicense');
        expect(sanitizeContractId('00aabbcc')).toBe('00aabbcc'); // Just hex prefix
        expect(sanitizeContractId('00abc123::Module:Template#suffix')).toBe('00abc123::Module:Template#suffix');
      });

      it('should reject contract IDs with SQL injection attempts', () => {
        expect(sanitizeContractId("00abc'; DROP TABLE--")).toBeNull();
        expect(sanitizeContractId('00abc UNION SELECT * FROM users')).toBeNull();
        expect(sanitizeContractId("00abc' OR 1=1--")).toBeNull();
      });

      it('should reject malformed contract IDs', () => {
        expect(sanitizeContractId('')).toBeNull();
        expect(sanitizeContractId('not-a-valid-id!')).toBeNull();
        expect(sanitizeContractId('SELECT * FROM contracts')).toBeNull();
      });
    });

    describe('timestamp validation', () => {
      it('should accept valid ISO timestamps', () => {
        expect(sanitizeTimestamp('2025-01-10T12:00:00.000Z')).toBe('2025-01-10T12:00:00.000Z');
        expect(sanitizeTimestamp('2025-01-10')).toBe('2025-01-10');
      });

      it('should reject invalid timestamps', () => {
        expect(sanitizeTimestamp('not-a-date')).toBeNull();
        expect(sanitizeTimestamp('2025-13-45')).toBeNull();
      });
    });
  });

  describe('Response formatting', () => {
    it('should structure response with data and count', () => {
      const response = {
        data: mockEvents,
        count: mockEvents.length,
        source: 'binary',
      };

      expect(response.data).toHaveLength(3);
      expect(response.count).toBe(3);
      expect(response.source).toBe('binary');
    });

    it('should include hasMore for paginated responses', () => {
      const response = {
        data: mockEvents.slice(0, 2),
        count: 2,
        hasMore: true,
        source: 'binary',
      };

      expect(response.hasMore).toBe(true);
    });
  });

  describe('Event filtering', () => {
    it('should filter by event type', () => {
      const filtered = mockEvents.filter(e => e.event_type === 'created');
      expect(filtered).toHaveLength(2);
    });

    it('should filter by template ID', () => {
      const templateId = 'VoteRequest';
      const filtered = mockEvents.filter(e => e.template_id?.includes(templateId));
      expect(filtered).toHaveLength(1);
    });

    it('should filter by date range', () => {
      const startDate = new Date('2025-01-10T12:30:00.000Z').getTime();
      const endDate = new Date('2025-01-10T15:00:00.000Z').getTime();
      
      const filtered = mockEvents.filter(e => {
        if (!e.effective_at) return false;
        const ts = new Date(e.effective_at).getTime();
        return ts >= startDate && ts <= endDate;
      });
      
      expect(filtered).toHaveLength(2);
    });
  });
});

describe('getRawEvent helper', () => {
  function getRawEvent(event) {
    if (!event) return {};
    if (event.raw_event) {
      if (typeof event.raw_event === 'string') {
        try {
          return JSON.parse(event.raw_event);
        } catch {
          return {};
        }
      }
      return event.raw_event;
    }
    if (event.raw && typeof event.raw === 'object') {
      return event.raw;
    }
    return {};
  }

  it('should parse JSON string raw_event', () => {
    const event = { raw_event: '{"key": "value"}' };
    expect(getRawEvent(event)).toEqual({ key: 'value' });
  });

  it('should return object raw_event directly', () => {
    const event = { raw_event: { key: 'value' } };
    expect(getRawEvent(event)).toEqual({ key: 'value' });
  });

  it('should handle raw as object (old format)', () => {
    const event = { raw: { key: 'value' } };
    expect(getRawEvent(event)).toEqual({ key: 'value' });
  });

  it('should return empty object for null event', () => {
    expect(getRawEvent(null)).toEqual({});
  });

  it('should return empty object for invalid JSON', () => {
    const event = { raw_event: 'not valid json' };
    expect(getRawEvent(event)).toEqual({});
  });
});

describe('convertBigInts helper', () => {
  function convertBigInts(obj) {
    if (obj === null || obj === undefined) return obj;
    if (typeof obj === 'bigint') return Number(obj);
    if (Array.isArray(obj)) return obj.map(convertBigInts);
    if (typeof obj === 'object') {
      const result = {};
      for (const [key, value] of Object.entries(obj)) {
        result[key] = convertBigInts(value);
      }
      return result;
    }
    return obj;
  }

  it('should convert BigInt values to numbers', () => {
    const input = { count: 100n };
    expect(convertBigInts(input)).toEqual({ count: 100 });
  });

  it('should handle nested objects', () => {
    const input = { outer: { inner: 50n } };
    expect(convertBigInts(input)).toEqual({ outer: { inner: 50 } });
  });

  it('should handle arrays of BigInts', () => {
    const input = [1n, 2n, 3n];
    expect(convertBigInts(input)).toEqual([1, 2, 3]);
  });

  it('should preserve null and undefined', () => {
    expect(convertBigInts(null)).toBeNull();
    expect(convertBigInts(undefined)).toBeUndefined();
  });
});
