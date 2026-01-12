import { describe, it, expect } from 'vitest';
import {
  paginationSchema,
  eventQuerySchema,
  searchQuerySchema,
  contractIdSchema,
  contractIdParamSchema,
  timestampSchema,
  dateRangeSchema,
  templateIdSchema,
  partyIdSchema,
  statsQuerySchema,
  governanceQuerySchema,
  backfillCursorSchema,
  acsQuerySchema,
} from './validation-schemas.js';

describe('validation-schemas', () => {
  describe('paginationSchema', () => {
    it('accepts valid pagination params', () => {
      const result = paginationSchema.safeParse({ limit: '50', offset: '100' });
      expect(result.success).toBe(true);
      expect(result.data).toEqual({ limit: 50, offset: 100 });
    });

    it('applies default values', () => {
      const result = paginationSchema.safeParse({});
      expect(result.success).toBe(true);
      expect(result.data).toEqual({ limit: 100, offset: 0 });
    });

    it('coerces string numbers to integers', () => {
      const result = paginationSchema.safeParse({ limit: '25', offset: '10' });
      expect(result.success).toBe(true);
      expect(result.data.limit).toBe(25);
      expect(result.data.offset).toBe(10);
    });

    it('rejects limit below minimum', () => {
      const result = paginationSchema.safeParse({ limit: 0 });
      expect(result.success).toBe(false);
    });

    it('rejects limit above maximum', () => {
      const result = paginationSchema.safeParse({ limit: 1001 });
      expect(result.success).toBe(false);
    });

    it('rejects negative offset', () => {
      const result = paginationSchema.safeParse({ offset: -1 });
      expect(result.success).toBe(false);
    });

    it('rejects offset above maximum', () => {
      const result = paginationSchema.safeParse({ offset: 100001 });
      expect(result.success).toBe(false);
    });

    it('rejects non-integer values', () => {
      const result = paginationSchema.safeParse({ limit: 'abc' });
      expect(result.success).toBe(false);
    });
  });

  describe('eventQuerySchema', () => {
    it('accepts valid event query with all fields', () => {
      const result = eventQuerySchema.safeParse({
        limit: 50,
        offset: 0,
        type: 'created',
        template: 'Splice.Amulet:Amulet',
        start: '2024-01-01T00:00:00Z',
        end: '2024-12-31T23:59:59Z',
      });
      expect(result.success).toBe(true);
    });

    it('accepts valid event type values', () => {
      for (const type of ['created', 'archived', 'exercised', 'CreatedEvent', 'ArchivedEvent', 'ExercisedEvent']) {
        const result = eventQuerySchema.safeParse({ type });
        expect(result.success).toBe(true);
        expect(result.data.type).toBe(type);
      }
    });

    it('rejects invalid event type', () => {
      const result = eventQuerySchema.safeParse({ type: 'invalid' });
      expect(result.success).toBe(false);
    });

    it('rejects template exceeding max length', () => {
      const result = eventQuerySchema.safeParse({ template: 'x'.repeat(501) });
      expect(result.success).toBe(false);
    });

    it('rejects invalid datetime format', () => {
      const result = eventQuerySchema.safeParse({ start: 'not-a-date' });
      expect(result.success).toBe(false);
    });
  });

  describe('searchQuerySchema', () => {
    it('accepts valid search query', () => {
      const result = searchQuerySchema.safeParse({ q: 'test query' });
      expect(result.success).toBe(true);
      expect(result.data.q).toBe('test query');
    });

    it('requires search query', () => {
      const result = searchQuerySchema.safeParse({});
      expect(result.success).toBe(false);
      expect(result.error.issues[0].message).toContain('required');
    });

    it('rejects empty search query', () => {
      const result = searchQuerySchema.safeParse({ q: '' });
      expect(result.success).toBe(false);
    });

    it('rejects query exceeding max length', () => {
      const result = searchQuerySchema.safeParse({ q: 'x'.repeat(201) });
      expect(result.success).toBe(false);
      expect(result.error.issues[0].message).toContain('too long');
    });

    it('accepts optional filter fields', () => {
      const result = searchQuerySchema.safeParse({
        q: 'test',
        type: 'created',
        template: 'Splice.Amulet:Amulet',
        party: 'validator::123',
        limit: 50,
      });
      expect(result.success).toBe(true);
    });
  });

  describe('contractIdSchema', () => {
    it('accepts valid hex contract ID', () => {
      const result = contractIdSchema.safeParse('00abc123def456');
      expect(result.success).toBe(true);
    });

    it('accepts contract ID with template path', () => {
      const result = contractIdSchema.safeParse('00abc123::Splice.Amulet:Amulet');
      expect(result.success).toBe(true);
    });

    it('accepts contract ID with hash suffix', () => {
      const result = contractIdSchema.safeParse('00abc123::Module:Template#suffix');
      expect(result.success).toBe(true);
    });

    it('accepts contract ID with @ suffix', () => {
      const result = contractIdSchema.safeParse('00abc123@def456');
      expect(result.success).toBe(true);
    });

    it('rejects empty contract ID', () => {
      const result = contractIdSchema.safeParse('');
      expect(result.success).toBe(false);
    });

    it('rejects contract ID with invalid characters', () => {
      const result = contractIdSchema.safeParse('abc; DROP TABLE users;');
      expect(result.success).toBe(false);
    });

    it('rejects contract ID exceeding max length', () => {
      const result = contractIdSchema.safeParse('00' + 'a'.repeat(500));
      expect(result.success).toBe(false);
    });
  });

  describe('contractIdParamSchema', () => {
    it('validates id param', () => {
      const result = contractIdParamSchema.safeParse({ id: '00abc123' });
      expect(result.success).toBe(true);
    });

    it('rejects missing id', () => {
      const result = contractIdParamSchema.safeParse({});
      expect(result.success).toBe(false);
    });
  });

  describe('timestampSchema', () => {
    it('accepts ISO date format', () => {
      const result = timestampSchema.safeParse('2024-01-15');
      expect(result.success).toBe(true);
    });

    it('accepts ISO datetime format', () => {
      const result = timestampSchema.safeParse('2024-01-15T10:30:00Z');
      expect(result.success).toBe(true);
    });

    it('accepts datetime with milliseconds', () => {
      const result = timestampSchema.safeParse('2024-01-15T10:30:00.123Z');
      expect(result.success).toBe(true);
    });

    it('accepts datetime with timezone offset', () => {
      const result = timestampSchema.safeParse('2024-01-15T10:30:00+05:30');
      expect(result.success).toBe(true);
    });

    it('rejects invalid date format', () => {
      const result = timestampSchema.safeParse('15/01/2024');
      expect(result.success).toBe(false);
    });

    it('rejects non-existent date', () => {
      const result = timestampSchema.safeParse('2024-02-30T00:00:00Z');
      expect(result.success).toBe(false);
    });

    it('rejects too long timestamp', () => {
      const result = timestampSchema.safeParse('x'.repeat(51));
      expect(result.success).toBe(false);
    });
  });

  describe('dateRangeSchema', () => {
    it('accepts valid date range', () => {
      const result = dateRangeSchema.safeParse({
        start: '2024-01-01T00:00:00Z',
        end: '2024-12-31T23:59:59Z',
      });
      expect(result.success).toBe(true);
    });

    it('accepts missing dates', () => {
      const result = dateRangeSchema.safeParse({});
      expect(result.success).toBe(true);
    });

    it('accepts only start date', () => {
      const result = dateRangeSchema.safeParse({ start: '2024-01-01T00:00:00Z' });
      expect(result.success).toBe(true);
    });

    it('accepts only end date', () => {
      const result = dateRangeSchema.safeParse({ end: '2024-12-31T23:59:59Z' });
      expect(result.success).toBe(true);
    });

    it('rejects start after end', () => {
      const result = dateRangeSchema.safeParse({
        start: '2024-12-31T23:59:59Z',
        end: '2024-01-01T00:00:00Z',
      });
      expect(result.success).toBe(false);
    });
  });

  describe('templateIdSchema', () => {
    it('accepts valid template ID', () => {
      const result = templateIdSchema.safeParse('Splice.Amulet:Amulet');
      expect(result.success).toBe(true);
    });

    it('accepts template with dots and colons', () => {
      const result = templateIdSchema.safeParse('Splice.AmuletRules:AmuletRules');
      expect(result.success).toBe(true);
    });

    it('rejects empty template ID', () => {
      const result = templateIdSchema.safeParse('');
      expect(result.success).toBe(false);
    });

    it('rejects template with SQL injection', () => {
      const result = templateIdSchema.safeParse("'; DROP TABLE--");
      expect(result.success).toBe(false);
    });
  });

  describe('partyIdSchema', () => {
    it('accepts valid party ID', () => {
      const result = partyIdSchema.safeParse('validator::1220abc');
      expect(result.success).toBe(true);
    });

    it('rejects empty party ID', () => {
      const result = partyIdSchema.safeParse('');
      expect(result.success).toBe(false);
    });
  });

  describe('statsQuerySchema', () => {
    it('accepts valid stats query', () => {
      const result = statsQuerySchema.safeParse({
        template: 'Splice.Amulet:Amulet',
        groupBy: 'day',
        start: '2024-01-01T00:00:00Z',
        end: '2024-12-31T23:59:59Z',
      });
      expect(result.success).toBe(true);
    });

    it('accepts valid groupBy values', () => {
      for (const groupBy of ['day', 'week', 'month']) {
        const result = statsQuerySchema.safeParse({ groupBy });
        expect(result.success).toBe(true);
      }
    });

    it('rejects invalid groupBy', () => {
      const result = statsQuerySchema.safeParse({ groupBy: 'year' });
      expect(result.success).toBe(false);
    });
  });

  describe('governanceQuerySchema', () => {
    it('accepts valid governance query', () => {
      const result = governanceQuerySchema.safeParse({
        limit: 50,
        status: 'accepted',
        actionType: 'CRARC_AddFutureAmuletConfigSchedule',
      });
      expect(result.success).toBe(true);
    });

    it('accepts all valid status values', () => {
      for (const status of ['pending', 'accepted', 'rejected', 'expired']) {
        const result = governanceQuerySchema.safeParse({ status });
        expect(result.success).toBe(true);
      }
    });

    it('rejects invalid status', () => {
      const result = governanceQuerySchema.safeParse({ status: 'invalid' });
      expect(result.success).toBe(false);
    });
  });

  describe('backfillCursorSchema', () => {
    it('accepts valid backfill cursor', () => {
      const result = backfillCursorSchema.safeParse({
        migrationId: 1,
        synchronizerId: 'global-sync::abc123',
      });
      expect(result.success).toBe(true);
    });

    it('coerces string migrationId', () => {
      const result = backfillCursorSchema.safeParse({ migrationId: '5' });
      expect(result.success).toBe(true);
      expect(result.data.migrationId).toBe(5);
    });

    it('rejects negative migrationId', () => {
      const result = backfillCursorSchema.safeParse({ migrationId: -1 });
      expect(result.success).toBe(false);
    });
  });

  describe('acsQuerySchema', () => {
    it('accepts valid ACS query', () => {
      const result = acsQuerySchema.safeParse({
        limit: 100,
        offset: 0,
        template: 'Splice.Amulet:Amulet',
        filter: 'active',
      });
      expect(result.success).toBe(true);
    });

    it('applies pagination defaults', () => {
      const result = acsQuerySchema.safeParse({});
      expect(result.success).toBe(true);
      expect(result.data.limit).toBe(100);
      expect(result.data.offset).toBe(0);
    });
  });
});
