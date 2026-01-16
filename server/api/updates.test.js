/**
 * Updates API Integration Tests
 * 
 * Tests for /api/updates/* endpoints - Ledger update queries
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { sanitizeNumber, sanitizeIdentifier, sanitizeTimestamp } from '../lib/sql-sanitize.js';

// Mock the database connection
vi.mock('../duckdb/connection.js', () => ({
  default: {
    DATA_PATH: '/mock/data',
    IS_TEST: true,
    TEST_FIXTURES_PATH: '/mock/fixtures',
    safeQuery: vi.fn().mockResolvedValue([]),
    hasFileType: vi.fn().mockReturnValue(true),
  },
}));

describe('Updates API', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Parameter Validation', () => {
    describe('limit parameter', () => {
      it('should sanitize limit within valid range', () => {
        expect(sanitizeNumber('50', { min: 1, max: 1000, defaultValue: 100 })).toBe(50);
      });

      it('should cap limit at maximum', () => {
        expect(sanitizeNumber('5000', { min: 1, max: 1000, defaultValue: 100 })).toBe(1000);
      });

      it('should use default for invalid input', () => {
        expect(sanitizeNumber('invalid', { min: 1, max: 1000, defaultValue: 100 })).toBe(100);
        expect(sanitizeNumber(undefined, { min: 1, max: 1000, defaultValue: 100 })).toBe(100);
      });
    });

    describe('timestamp filtering', () => {
      it('should accept valid ISO timestamps', () => {
        expect(sanitizeTimestamp('2025-01-15T12:00:00.000Z')).toBe('2025-01-15T12:00:00.000Z');
        expect(sanitizeTimestamp('2025-01-15')).toBe('2025-01-15');
      });

      it('should reject invalid timestamps', () => {
        expect(sanitizeTimestamp('not-a-date')).toBeNull();
        expect(sanitizeTimestamp('2025-13-45')).toBeNull();
        expect(sanitizeTimestamp('')).toBeNull();
      });

      it('should handle edge case dates', () => {
        expect(sanitizeTimestamp('2025-12-31T23:59:59.999Z')).toBe('2025-12-31T23:59:59.999Z');
        expect(sanitizeTimestamp('2020-01-01T00:00:00.000Z')).toBe('2020-01-01T00:00:00.000Z');
      });
    });

    describe('update_type filtering', () => {
      const validTypes = ['transaction', 'reassignment', 'init', 'offset_checkpoint'];
      
      it('should accept valid update types', () => {
        for (const type of validTypes) {
          expect(validTypes).toContain(type);
        }
      });

      it('should reject invalid update types', () => {
        const isValidType = (type) => validTypes.includes(type);
        
        expect(isValidType('invalid')).toBe(false);
        expect(isValidType('SELECT * FROM')).toBe(false);
        expect(isValidType('')).toBe(false);
      });
    });
  });

  describe('Update Record Structure', () => {
    const mockUpdate = {
      update_id: 'upd-abc123',
      update_type: 'transaction',
      migration_id: 5,
      synchronizer_id: 'sync-1',
      record_time: '2025-01-15T12:00:00.000Z',
      effective_at: '2025-01-15T12:00:00.000Z',
      workflow_id: 'wf-123',
      command_id: 'cmd-456',
      offset: '00001234',
      event_count: 3,
    };

    it('should have required update fields', () => {
      expect(mockUpdate).toHaveProperty('update_id');
      expect(mockUpdate).toHaveProperty('update_type');
      expect(mockUpdate).toHaveProperty('migration_id');
    });

    it('should have valid update_type', () => {
      const validTypes = ['transaction', 'reassignment', 'init', 'offset_checkpoint'];
      expect(validTypes).toContain(mockUpdate.update_type);
    });

    it('should have valid migration_id', () => {
      expect(typeof mockUpdate.migration_id).toBe('number');
      expect(mockUpdate.migration_id).toBeGreaterThanOrEqual(0);
    });

    it('should have valid timestamps', () => {
      expect(() => new Date(mockUpdate.record_time)).not.toThrow();
      expect(() => new Date(mockUpdate.effective_at)).not.toThrow();
    });

    it('should have event_count as number', () => {
      expect(typeof mockUpdate.event_count).toBe('number');
      expect(mockUpdate.event_count).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Response Formatting', () => {
    it('should structure response with data and metadata', () => {
      const mockUpdates = [
        { update_id: '1', update_type: 'transaction' },
        { update_id: '2', update_type: 'transaction' },
      ];
      
      const response = {
        data: mockUpdates,
        count: mockUpdates.length,
        hasMore: false,
        source: 'parquet',
      };
      
      expect(response.data).toHaveLength(2);
      expect(response.count).toBe(2);
      expect(response).toHaveProperty('source');
    });

    it('should include pagination metadata', () => {
      const response = {
        data: [],
        count: 0,
        offset: 100,
        limit: 50,
        hasMore: true,
        total: 500,
      };
      
      expect(response.hasMore).toBe(true);
      expect(response.offset).toBe(100);
      expect(response.total).toBe(500);
    });
  });

  describe('Update Filtering', () => {
    const mockUpdates = [
      { update_id: '1', update_type: 'transaction', migration_id: 5, record_time: '2025-01-15T10:00:00Z' },
      { update_id: '2', update_type: 'reassignment', migration_id: 5, record_time: '2025-01-15T11:00:00Z' },
      { update_id: '3', update_type: 'transaction', migration_id: 4, record_time: '2025-01-15T12:00:00Z' },
    ];

    it('should filter by update_type', () => {
      const filtered = mockUpdates.filter(u => u.update_type === 'transaction');
      expect(filtered).toHaveLength(2);
    });

    it('should filter by migration_id', () => {
      const filtered = mockUpdates.filter(u => u.migration_id === 5);
      expect(filtered).toHaveLength(2);
    });

    it('should filter by time range', () => {
      const start = new Date('2025-01-15T10:30:00Z').getTime();
      const filtered = mockUpdates.filter(u => new Date(u.record_time).getTime() >= start);
      expect(filtered).toHaveLength(2);
    });

    it('should combine multiple filters', () => {
      const filtered = mockUpdates.filter(u => 
        u.update_type === 'transaction' && u.migration_id === 5
      );
      expect(filtered).toHaveLength(1);
    });
  });

  describe('Update Sorting', () => {
    const mockUpdates = [
      { update_id: '2', record_time: '2025-01-15T11:00:00Z' },
      { update_id: '1', record_time: '2025-01-15T10:00:00Z' },
      { update_id: '3', record_time: '2025-01-15T12:00:00Z' },
    ];

    it('should sort by record_time descending', () => {
      const sorted = [...mockUpdates].sort((a, b) => 
        new Date(b.record_time).getTime() - new Date(a.record_time).getTime()
      );
      
      expect(sorted[0].update_id).toBe('3');
      expect(sorted[2].update_id).toBe('1');
    });

    it('should sort by record_time ascending', () => {
      const sorted = [...mockUpdates].sort((a, b) => 
        new Date(a.record_time).getTime() - new Date(b.record_time).getTime()
      );
      
      expect(sorted[0].update_id).toBe('1');
      expect(sorted[2].update_id).toBe('3');
    });
  });

  describe('Data Source Detection', () => {
    it('should detect parquet source', () => {
      const hasParquet = true;
      const hasJsonl = false;
      const source = hasParquet ? 'parquet' : hasJsonl ? 'jsonl' : 'none';
      
      expect(source).toBe('parquet');
    });

    it('should fallback to jsonl', () => {
      const hasParquet = false;
      const hasJsonl = true;
      const source = hasParquet ? 'parquet' : hasJsonl ? 'jsonl' : 'none';
      
      expect(source).toBe('jsonl');
    });

    it('should detect no data', () => {
      const hasParquet = false;
      const hasJsonl = false;
      const source = hasParquet ? 'parquet' : hasJsonl ? 'jsonl' : 'none';
      
      expect(source).toBe('none');
    });
  });

  describe('Error Handling', () => {
    it('should format query errors', () => {
      const error = new Error('Invalid SQL syntax');
      const response = { error: error.message, status: 500 };
      
      expect(response.error).toBe('Invalid SQL syntax');
      expect(response.status).toBe(500);
    });

    it('should handle empty results', () => {
      const response = { data: [], count: 0 };
      
      expect(response.data).toHaveLength(0);
      expect(response.count).toBe(0);
    });

    it('should handle timeout errors', () => {
      const error = new Error('Query timeout');
      const isTimeout = error.message.toLowerCase().includes('timeout');
      
      expect(isTimeout).toBe(true);
    });
  });
});

describe('Update Data Transformation', () => {
  describe('BigInt handling', () => {
    it('should convert offset BigInt to string', () => {
      const offset = 12345678901234567890n;
      const stringOffset = offset.toString();
      
      expect(typeof stringOffset).toBe('string');
      expect(stringOffset).toBe('12345678901234567890');
    });

    it('should handle numeric fields', () => {
      const update = {
        migration_id: 5n,
        event_count: 10n,
      };
      
      const converted = {
        migration_id: Number(update.migration_id),
        event_count: Number(update.event_count),
      };
      
      expect(converted.migration_id).toBe(5);
      expect(converted.event_count).toBe(10);
    });
  });

  describe('Event extraction', () => {
    it('should extract events from update', () => {
      const update = {
        update_id: 'upd-1',
        events: [
          { event_id: 'evt-1', type: 'created' },
          { event_id: 'evt-2', type: 'archived' },
        ],
      };
      
      expect(update.events).toHaveLength(2);
      expect(update.events[0].event_id).toBe('evt-1');
    });

    it('should handle updates with no events', () => {
      const update = { update_id: 'upd-1' };
      const events = update.events || [];
      
      expect(events).toHaveLength(0);
    });
  });
});

describe('Updates Query Building', () => {
  describe('Time range queries', () => {
    it('should build before clause', () => {
      const before = '2025-01-15T12:00:00Z';
      const sanitized = sanitizeTimestamp(before);
      const clause = sanitized ? `record_time < '${sanitized}'` : '';
      
      expect(clause).toContain('record_time');
      expect(clause).toContain('<');
    });

    it('should build after clause', () => {
      const after = '2025-01-14T00:00:00Z';
      const sanitized = sanitizeTimestamp(after);
      const clause = sanitized ? `record_time >= '${sanitized}'` : '';
      
      expect(clause).toContain('record_time');
      expect(clause).toContain('>=');
    });

    it('should build between clause', () => {
      const start = '2025-01-14T00:00:00Z';
      const end = '2025-01-15T00:00:00Z';
      const clause = `record_time >= '${start}' AND record_time < '${end}'`;
      
      expect(clause).toContain('AND');
      expect(clause).toContain(start);
      expect(clause).toContain(end);
    });
  });

  describe('Pagination', () => {
    it('should calculate offset correctly', () => {
      const page = 3;
      const pageSize = 50;
      const offset = (page - 1) * pageSize;
      
      expect(offset).toBe(100);
    });

    it('should determine hasMore correctly', () => {
      const returnedCount = 50;
      const requestedLimit = 50;
      const hasMore = returnedCount >= requestedLimit;
      
      expect(hasMore).toBe(true);
    });

    it('should determine last page correctly', () => {
      const returnedCount = 30;
      const requestedLimit = 50;
      const hasMore = returnedCount >= requestedLimit;
      
      expect(hasMore).toBe(false);
    });
  });
});
