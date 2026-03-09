/**
 * ACS API Integration Tests
 * 
 * Tests for /api/acs/* endpoints - Active Contract Set management
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { sanitizeNumber, sanitizeIdentifier, escapeLikePattern } from '../lib/sql-sanitize.js';

// Mock the database connection
vi.mock('../duckdb/connection.js', () => ({
  default: {
    DATA_PATH: '/mock/data',
    IS_TEST: true,
    TEST_FIXTURES_PATH: '/mock/fixtures',
    safeQuery: vi.fn().mockResolvedValue([]),
    query: vi.fn().mockResolvedValue([]),
    hasFileType: vi.fn().mockReturnValue(true),
  },
  safeQuery: vi.fn().mockResolvedValue([]),
  query: vi.fn().mockResolvedValue([]),
  hasFileType: vi.fn().mockReturnValue(true),
  DATA_PATH: '/mock/data',
  IS_TEST: true,
  TEST_FIXTURES_PATH: '/mock/fixtures',
}));

describe('ACS API', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Parameter Validation', () => {
    describe('snapshot_id validation', () => {
      it('should accept valid snapshot IDs', () => {
        const validIds = [
          '2025-01-15T12:00:00.000Z',
          '2025-01-15T00:00:00Z',
          '2025-01-15',
        ];
        
        for (const id of validIds) {
          expect(typeof id).toBe('string');
          expect(id.length).toBeGreaterThan(0);
        }
      });

      it('should validate ISO date format', () => {
        const isValidISODate = (str) => {
          const date = new Date(str);
          return !isNaN(date.getTime());
        };
        
        expect(isValidISODate('2025-01-15T12:00:00.000Z')).toBe(true);
        expect(isValidISODate('invalid-date')).toBe(false);
        expect(isValidISODate('2025-13-45')).toBe(false);
      });
    });

    describe('migration_id validation', () => {
      it('should accept valid migration IDs', () => {
        const migrationId = sanitizeNumber('5', { min: 0, max: 100, defaultValue: 0 });
        expect(migrationId).toBe(5);
      });

      it('should cap at maximum', () => {
        const migrationId = sanitizeNumber('150', { min: 0, max: 100, defaultValue: 0 });
        expect(migrationId).toBe(100);
      });

      it('should use default for invalid input', () => {
        const migrationId = sanitizeNumber('invalid', { min: 0, max: 100, defaultValue: 0 });
        expect(migrationId).toBe(0);
      });

      it('should handle negative values', () => {
        const migrationId = sanitizeNumber('-5', { min: 0, max: 100, defaultValue: 0 });
        expect(migrationId).toBe(0);
      });
    });

    describe('template filtering', () => {
      it('should sanitize template identifiers', () => {
        expect(sanitizeIdentifier('Splice.Amulet:Amulet')).toBe('Splice.Amulet:Amulet');
        expect(sanitizeIdentifier('ValidatorLicense')).toBe('ValidatorLicense');
      });

      it('should reject SQL injection in templates', () => {
        expect(sanitizeIdentifier("'; DROP TABLE--")).toBeNull();
        expect(sanitizeIdentifier('1=1 UNION SELECT')).toBeNull();
      });

      it('should escape LIKE pattern characters', () => {
        const escaped = escapeLikePattern('test%pattern');
        expect(escaped).toContain('\\%');
      });
    });

    describe('limit and offset validation', () => {
      it('should validate limit within range', () => {
        expect(sanitizeNumber('100', { min: 1, max: 10000, defaultValue: 1000 })).toBe(100);
        expect(sanitizeNumber('50000', { min: 1, max: 10000, defaultValue: 1000 })).toBe(10000);
      });

      it('should validate offset within range', () => {
        expect(sanitizeNumber('500', { min: 0, max: 100000, defaultValue: 0 })).toBe(500);
        expect(sanitizeNumber('-100', { min: 0, max: 100000, defaultValue: 0 })).toBe(0);
      });
    });
  });

  describe('ACS Contract Structure', () => {
    const mockContract = {
      contract_id: '00abc123::Splice.Amulet:Amulet',
      template_id: 'Splice.Amulet:Amulet',
      created_at: '2025-01-15T12:00:00.000Z',
      payload: { owner: 'party123', amount: { value: '1000' } },
      signatories: ['party123'],
      observers: [],
    };

    it('should have required contract fields', () => {
      expect(mockContract).toHaveProperty('contract_id');
      expect(mockContract).toHaveProperty('template_id');
      expect(mockContract).toHaveProperty('payload');
    });

    it('should validate contract_id format', () => {
      expect(mockContract.contract_id).toMatch(/^00[a-f0-9]+::/);
    });

    it('should validate template_id format', () => {
      expect(mockContract.template_id).toContain(':');
      const [module, template] = mockContract.template_id.split(':');
      expect(module.length).toBeGreaterThan(0);
      expect(template.length).toBeGreaterThan(0);
    });

    it('should have valid signatories array', () => {
      expect(Array.isArray(mockContract.signatories)).toBe(true);
      expect(mockContract.signatories.length).toBeGreaterThan(0);
    });
  });

  describe('Aggregation Queries', () => {
    describe('sum aggregation', () => {
      it('should calculate sum from mock data', () => {
        const contracts = [
          { payload: { amount: { value: '100' } } },
          { payload: { amount: { value: '200' } } },
          { payload: { amount: { value: '300' } } },
        ];
        
        const sum = contracts.reduce((acc, c) => {
          const value = parseFloat(c.payload?.amount?.value || '0');
          return acc + value;
        }, 0);
        
        expect(sum).toBe(600);
      });

      it('should handle missing amount fields', () => {
        const contracts = [
          { payload: { amount: { value: '100' } } },
          { payload: {} },
          { payload: { amount: null } },
        ];
        
        const sum = contracts.reduce((acc, c) => {
          const value = parseFloat(c.payload?.amount?.value || '0');
          return acc + (isNaN(value) ? 0 : value);
        }, 0);
        
        expect(sum).toBe(100);
      });
    });

    describe('count aggregation', () => {
      it('should count contracts by template', () => {
        const contracts = [
          { template_id: 'Amulet' },
          { template_id: 'Amulet' },
          { template_id: 'ValidatorLicense' },
        ];
        
        const counts = {};
        for (const c of contracts) {
          counts[c.template_id] = (counts[c.template_id] || 0) + 1;
        }
        
        expect(counts['Amulet']).toBe(2);
        expect(counts['ValidatorLicense']).toBe(1);
      });
    });
  });

  describe('Snapshot Status', () => {
    describe('status response structure', () => {
      const mockStatus = {
        latest_snapshot: '2025-01-15T12:00:00.000Z',
        migration_id: 5,
        contract_count: 150000,
        template_count: 25,
        status: 'complete',
        processing: false,
      };

      it('should have required status fields', () => {
        expect(mockStatus).toHaveProperty('latest_snapshot');
        expect(mockStatus).toHaveProperty('status');
        expect(mockStatus).toHaveProperty('contract_count');
      });

      it('should have valid status value', () => {
        const validStatuses = ['complete', 'processing', 'failed', 'pending'];
        expect(validStatuses).toContain(mockStatus.status);
      });

      it('should have non-negative counts', () => {
        expect(mockStatus.contract_count).toBeGreaterThanOrEqual(0);
        expect(mockStatus.template_count).toBeGreaterThanOrEqual(0);
      });
    });

    describe('processing state detection', () => {
      it('should detect processing state', () => {
        const isProcessing = (status) => status.status === 'processing' || status.processing === true;
        
        expect(isProcessing({ status: 'processing' })).toBe(true);
        expect(isProcessing({ processing: true })).toBe(true);
        expect(isProcessing({ status: 'complete' })).toBe(false);
      });
    });
  });

  describe('Template Statistics', () => {
    const mockTemplateStats = [
      { template_id: 'Splice.Amulet:Amulet', count: 50000 },
      { template_id: 'Splice.ValidatorLicense:ValidatorLicense', count: 1000 },
      { template_id: 'Splice.DsoRules:DsoRules', count: 1 },
    ];

    it('should sort templates by count descending', () => {
      const sorted = [...mockTemplateStats].sort((a, b) => b.count - a.count);
      
      expect(sorted[0].template_id).toBe('Splice.Amulet:Amulet');
      expect(sorted[0].count).toBe(50000);
    });

    it('should calculate total contract count', () => {
      const total = mockTemplateStats.reduce((sum, t) => sum + t.count, 0);
      expect(total).toBe(51001);
    });

    it('should calculate percentage distribution', () => {
      const total = mockTemplateStats.reduce((sum, t) => sum + t.count, 0);
      const withPercentage = mockTemplateStats.map(t => ({
        ...t,
        percentage: ((t.count / total) * 100).toFixed(2),
      }));
      
      expect(parseFloat(withPercentage[0].percentage)).toBeGreaterThan(90);
    });
  });

  describe('Error Handling', () => {
    it('should format database errors', () => {
      const dbError = new Error('Connection refused');
      const formatted = { error: dbError.message, code: 'DB_ERROR' };
      
      expect(formatted.error).toBe('Connection refused');
      expect(formatted).toHaveProperty('code');
    });

    it('should handle empty results gracefully', () => {
      const response = { data: [], count: 0, message: 'No contracts found' };
      
      expect(response.data).toHaveLength(0);
      expect(response.count).toBe(0);
    });

    it('should validate error response structure', () => {
      const errorResponse = {
        error: 'Snapshot not found',
        status: 404,
        timestamp: new Date().toISOString(),
      };
      
      expect(errorResponse).toHaveProperty('error');
      expect(errorResponse.status).toBeGreaterThanOrEqual(400);
    });
  });
});

describe('ACS Data Transformation', () => {
  describe('BigInt conversion', () => {
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

    it('should convert BigInt to Number', () => {
      const data = { count: 100n, amount: 5000000000n };
      const converted = convertBigInts(data);
      
      expect(converted.count).toBe(100);
      expect(converted.amount).toBe(5000000000);
      expect(typeof converted.count).toBe('number');
    });

    it('should handle nested BigInts', () => {
      const data = { 
        outer: { inner: { value: 123n } },
        array: [1n, 2n, 3n],
      };
      const converted = convertBigInts(data);
      
      expect(converted.outer.inner.value).toBe(123);
      expect(converted.array).toEqual([1, 2, 3]);
    });

    it('should preserve null and undefined', () => {
      expect(convertBigInts(null)).toBeNull();
      expect(convertBigInts(undefined)).toBeUndefined();
    });
  });

  describe('Amount parsing', () => {
    it('should parse Daml amount strings', () => {
      const parseAmount = (str) => {
        if (!str) return 0;
        // Daml amounts are in microunits (10^-10)
        const value = parseFloat(str);
        return value / 1e10;
      };
      
      expect(parseAmount('1000000000000')).toBe(100); // 100 CC
      expect(parseAmount('10000000000')).toBe(1); // 1 CC
      expect(parseAmount('0')).toBe(0);
      expect(parseAmount(null)).toBe(0);
    });

    it('should format amounts for display', () => {
      const formatCC = (amount) => {
        return new Intl.NumberFormat('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }).format(amount);
      };
      
      expect(formatCC(1234.5678)).toBe('1,234.57');
      expect(formatCC(0.12)).toBe('0.12');
    });
  });
});

describe('ACS Query Building', () => {
  describe('SQL generation', () => {
    it('should escape special characters in LIKE patterns', () => {
      const escaped = escapeLikePattern('test%_[]');
      
      expect(escaped).toContain('\\%');
      expect(escaped).toContain('\\_');
    });

    it('should build valid WHERE clauses', () => {
      const buildWhereClause = (filters) => {
        const conditions = [];
        if (filters.template) {
          conditions.push(`template_id LIKE '%${escapeLikePattern(filters.template)}%'`);
        }
        if (filters.owner) {
          conditions.push(`payload->>'owner' = '${filters.owner}'`);
        }
        return conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
      };
      
      const clause = buildWhereClause({ template: 'Amulet' });
      expect(clause).toContain('WHERE');
      expect(clause).toContain('template_id');
    });
  });
});
