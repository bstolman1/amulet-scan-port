import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

// Mock dependencies
vi.mock('../duckdb/connection.js', () => ({
  safeQuery: vi.fn(),
  getPool: vi.fn(() => ({
    query: vi.fn(),
  })),
}));

vi.mock('fs', () => ({
  existsSync: vi.fn(),
  readdirSync: vi.fn(),
  readFileSync: vi.fn(),
  statSync: vi.fn(),
}));

// Import after mocking
import { safeQuery } from '../duckdb/connection.js';

describe('Ingest Module SQL Helpers', () => {
  // Test the SQL helper functions that are commonly used in ingest

  describe('SQL String Escaping', () => {
    /**
     * sqlStr helper - escapes single quotes for SQL strings
     */
    function sqlStr(value) {
      if (value === null || value === undefined) return 'NULL';
      return `'${String(value).replace(/'/g, "''")}'`;
    }

    it('wraps string in single quotes', () => {
      expect(sqlStr('hello')).toBe("'hello'");
    });

    it('escapes single quotes in string', () => {
      expect(sqlStr("it's")).toBe("'it''s'");
    });

    it('handles multiple single quotes', () => {
      expect(sqlStr("it's Bob's")).toBe("'it''s Bob''s'");
    });

    it('returns NULL for null values', () => {
      expect(sqlStr(null)).toBe('NULL');
    });

    it('returns NULL for undefined values', () => {
      expect(sqlStr(undefined)).toBe('NULL');
    });

    it('converts numbers to strings', () => {
      expect(sqlStr(123)).toBe("'123'");
    });

    it('handles empty string', () => {
      expect(sqlStr('')).toBe("''");
    });

    it('handles special characters', () => {
      expect(sqlStr('hello\nworld')).toBe("'hello\nworld'");
      expect(sqlStr('tab\there')).toBe("'tab\there'");
    });
  });

  describe('SQL Timestamp Formatting', () => {
    /**
     * sqlTs helper - formats dates as SQL timestamps
     */
    function sqlTs(value) {
      if (!value) return 'NULL';
      const date = value instanceof Date ? value : new Date(value);
      if (isNaN(date.getTime())) return 'NULL';
      return `TIMESTAMP '${date.toISOString().replace('T', ' ').replace('Z', '')}'`;
    }

    it('formats Date object correctly', () => {
      const date = new Date('2025-01-10T12:30:45.000Z');
      expect(sqlTs(date)).toBe("TIMESTAMP '2025-01-10 12:30:45.000'");
    });

    it('formats ISO string correctly', () => {
      expect(sqlTs('2025-01-10T12:30:45.000Z')).toBe("TIMESTAMP '2025-01-10 12:30:45.000'");
    });

    it('returns NULL for null value', () => {
      expect(sqlTs(null)).toBe('NULL');
    });

    it('returns NULL for undefined value', () => {
      expect(sqlTs(undefined)).toBe('NULL');
    });

    it('returns NULL for invalid date string', () => {
      expect(sqlTs('not-a-date')).toBe('NULL');
    });

    it('handles epoch timestamps', () => {
      const epoch = new Date(0);
      expect(sqlTs(epoch)).toBe("TIMESTAMP '1970-01-01 00:00:00.000'");
    });

    it('handles milliseconds correctly', () => {
      const date = new Date('2025-01-10T12:30:45.123Z');
      expect(sqlTs(date)).toBe("TIMESTAMP '2025-01-10 12:30:45.123'");
    });
  });

  describe('SQL JSON Formatting', () => {
    /**
     * sqlJson helper - converts objects to JSON for SQL
     */
    function sqlJson(value) {
      if (value === null || value === undefined) return 'NULL';
      try {
        const json = JSON.stringify(value);
        return `'${json.replace(/'/g, "''")}'`;
      } catch {
        return 'NULL';
      }
    }

    it('converts object to JSON string', () => {
      expect(sqlJson({ key: 'value' })).toBe(`'{"key":"value"}'`);
    });

    it('handles nested objects', () => {
      const obj = { a: { b: { c: 1 } } };
      expect(sqlJson(obj)).toBe(`'{"a":{"b":{"c":1}}}'`);
    });

    it('handles arrays', () => {
      expect(sqlJson([1, 2, 3])).toBe("'[1,2,3]'");
    });

    it('escapes quotes in JSON values', () => {
      const obj = { key: "it's a test" };
      // The single quote in the value should be escaped
      const result = sqlJson(obj);
      expect(result).toContain("''");
    });

    it('returns NULL for null', () => {
      expect(sqlJson(null)).toBe('NULL');
    });

    it('returns NULL for undefined', () => {
      expect(sqlJson(undefined)).toBe('NULL');
    });

    it('handles circular reference gracefully', () => {
      const obj = {};
      obj.self = obj;
      expect(sqlJson(obj)).toBe('NULL');
    });

    it('handles primitive values', () => {
      expect(sqlJson(123)).toBe("'123'");
      expect(sqlJson('test')).toBe(`'"test"'`);
      expect(sqlJson(true)).toBe("'true'");
    });
  });
});

describe('Batch Insertion Logic', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Batch Size Handling', () => {
    const BATCH_SIZE = 1000;

    function createBatches(items, batchSize) {
      const batches = [];
      for (let i = 0; i < items.length; i += batchSize) {
        batches.push(items.slice(i, i + batchSize));
      }
      return batches;
    }

    it('creates single batch for small arrays', () => {
      const items = Array.from({ length: 100 }, (_, i) => ({ id: i }));
      const batches = createBatches(items, BATCH_SIZE);
      
      expect(batches).toHaveLength(1);
      expect(batches[0]).toHaveLength(100);
    });

    it('creates multiple batches for large arrays', () => {
      const items = Array.from({ length: 2500 }, (_, i) => ({ id: i }));
      const batches = createBatches(items, BATCH_SIZE);
      
      expect(batches).toHaveLength(3);
      expect(batches[0]).toHaveLength(1000);
      expect(batches[1]).toHaveLength(1000);
      expect(batches[2]).toHaveLength(500);
    });

    it('handles empty array', () => {
      const batches = createBatches([], BATCH_SIZE);
      expect(batches).toHaveLength(0);
    });

    it('handles exact batch size', () => {
      const items = Array.from({ length: 1000 }, (_, i) => ({ id: i }));
      const batches = createBatches(items, BATCH_SIZE);
      
      expect(batches).toHaveLength(1);
      expect(batches[0]).toHaveLength(1000);
    });
  });

  describe('Event Deduplication', () => {
    function deduplicateEvents(events) {
      const seen = new Set();
      return events.filter(event => {
        const key = `${event.event_id}:${event.offset}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    it('removes duplicate events by event_id and offset', () => {
      const events = [
        { event_id: 'a', offset: 0, data: 'first' },
        { event_id: 'a', offset: 0, data: 'duplicate' },
        { event_id: 'b', offset: 1, data: 'second' },
      ];
      
      const result = deduplicateEvents(events);
      
      expect(result).toHaveLength(2);
      expect(result[0].data).toBe('first');
      expect(result[1].data).toBe('second');
    });

    it('keeps events with same id but different offset', () => {
      const events = [
        { event_id: 'a', offset: 0 },
        { event_id: 'a', offset: 1 },
      ];
      
      const result = deduplicateEvents(events);
      
      expect(result).toHaveLength(2);
    });

    it('handles empty array', () => {
      expect(deduplicateEvents([])).toHaveLength(0);
    });

    it('handles all unique events', () => {
      const events = [
        { event_id: 'a', offset: 0 },
        { event_id: 'b', offset: 1 },
        { event_id: 'c', offset: 2 },
      ];
      
      const result = deduplicateEvents(events);
      
      expect(result).toHaveLength(3);
    });
  });
});

describe('File Processing', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('File Discovery', () => {
    function discoverFiles(directory, extension) {
      if (!fs.existsSync(directory)) return [];
      
      const files = fs.readdirSync(directory);
      return files
        .filter(f => f.endsWith(extension))
        .map(f => path.join(directory, f))
        .sort();
    }

    it('returns empty array for non-existent directory', () => {
      fs.existsSync.mockReturnValue(false);
      
      const result = discoverFiles('/fake/path', '.jsonl');
      
      expect(result).toEqual([]);
    });

    it('filters files by extension', () => {
      fs.existsSync.mockReturnValue(true);
      fs.readdirSync.mockReturnValue([
        'data.jsonl',
        'data.parquet',
        'other.jsonl',
        'readme.txt',
      ]);
      
      const result = discoverFiles('/data', '.jsonl');
      
      expect(result).toHaveLength(2);
      expect(result[0]).toContain('data.jsonl');
      expect(result[1]).toContain('other.jsonl');
    });

    it('returns sorted file list', () => {
      fs.existsSync.mockReturnValue(true);
      fs.readdirSync.mockReturnValue(['z.jsonl', 'a.jsonl', 'm.jsonl']);
      
      const result = discoverFiles('/data', '.jsonl');
      
      expect(result[0]).toContain('a.jsonl');
      expect(result[1]).toContain('m.jsonl');
      expect(result[2]).toContain('z.jsonl');
    });
  });

  describe('File Status Tracking', () => {
    function getProcessedFiles() {
      return new Map();
    }

    function markFileProcessed(processedFiles, filePath, stats) {
      processedFiles.set(filePath, {
        processedAt: new Date().toISOString(),
        eventCount: stats.eventCount,
        size: stats.size,
      });
    }

    function isFileProcessed(processedFiles, filePath) {
      return processedFiles.has(filePath);
    }

    it('tracks processed files', () => {
      const processed = getProcessedFiles();
      
      markFileProcessed(processed, '/data/file1.jsonl', { eventCount: 100, size: 1024 });
      
      expect(isFileProcessed(processed, '/data/file1.jsonl')).toBe(true);
      expect(isFileProcessed(processed, '/data/file2.jsonl')).toBe(false);
    });

    it('stores processing metadata', () => {
      const processed = getProcessedFiles();
      
      markFileProcessed(processed, '/data/file1.jsonl', { eventCount: 500, size: 2048 });
      
      const info = processed.get('/data/file1.jsonl');
      expect(info.eventCount).toBe(500);
      expect(info.size).toBe(2048);
      expect(info.processedAt).toBeDefined();
    });
  });
});

describe('Error Handling', () => {
  describe('Corrupt File Handling', () => {
    function parseEventLine(line) {
      try {
        return JSON.parse(line);
      } catch (error) {
        return { error: `Failed to parse: ${error.message}`, line: line.slice(0, 100) };
      }
    }

    it('returns error object for invalid JSON', () => {
      const result = parseEventLine('not valid json');
      
      expect(result.error).toBeDefined();
      expect(result.error).toContain('Failed to parse');
    });

    it('truncates long lines in error', () => {
      const longLine = 'x'.repeat(200);
      const result = parseEventLine(longLine);
      
      expect(result.line.length).toBe(100);
    });

    it('parses valid JSON correctly', () => {
      const result = parseEventLine('{"event_id": "123", "type": "created"}');
      
      expect(result.event_id).toBe('123');
      expect(result.type).toBe('created');
    });

    it('handles empty line', () => {
      const result = parseEventLine('');
      
      expect(result.error).toBeDefined();
    });
  });

  describe('Database Error Recovery', () => {
    async function insertWithRetry(insertFn, maxRetries = 3) {
      let lastError;
      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          return await insertFn();
        } catch (error) {
          lastError = error;
          if (attempt < maxRetries) {
            await new Promise(r => setTimeout(r, 100 * attempt));
          }
        }
      }
      throw lastError;
    }

    it('succeeds on first try', async () => {
      const fn = vi.fn().mockResolvedValue({ rows: 100 });
      
      const result = await insertWithRetry(fn);
      
      expect(result.rows).toBe(100);
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('retries on failure and succeeds', async () => {
      const fn = vi.fn()
        .mockRejectedValueOnce(new Error('Temp error'))
        .mockResolvedValue({ rows: 50 });
      
      const result = await insertWithRetry(fn);
      
      expect(result.rows).toBe(50);
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('throws after max retries', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('Persistent error'));
      
      await expect(insertWithRetry(fn, 3)).rejects.toThrow('Persistent error');
      expect(fn).toHaveBeenCalledTimes(3);
    });
  });
});

describe('Progress Tracking', () => {
  describe('Progress Calculation', () => {
    function calculateProgress(processed, total) {
      if (total === 0) return 100;
      return Math.round((processed / total) * 100);
    }

    it('calculates percentage correctly', () => {
      expect(calculateProgress(50, 100)).toBe(50);
      expect(calculateProgress(25, 100)).toBe(25);
      expect(calculateProgress(100, 100)).toBe(100);
    });

    it('returns 100 for empty total', () => {
      expect(calculateProgress(0, 0)).toBe(100);
    });

    it('rounds to nearest integer', () => {
      expect(calculateProgress(1, 3)).toBe(33);
      expect(calculateProgress(2, 3)).toBe(67);
    });

    it('handles large numbers', () => {
      expect(calculateProgress(500000, 1000000)).toBe(50);
    });
  });

  describe('ETA Calculation', () => {
    function calculateETA(startTime, processed, total) {
      if (processed === 0) return null;
      
      const elapsed = Date.now() - startTime;
      const rate = processed / elapsed;
      const remaining = total - processed;
      const etaMs = remaining / rate;
      
      return Math.round(etaMs / 1000); // Return seconds
    }

    it('returns null when nothing processed yet', () => {
      expect(calculateETA(Date.now(), 0, 100)).toBeNull();
    });

    it('calculates remaining time based on rate', () => {
      const startTime = Date.now() - 10000; // 10 seconds ago
      
      // Processed 50 of 100 in 10 seconds = 5/sec
      // Remaining 50 at 5/sec = 10 more seconds
      const eta = calculateETA(startTime, 50, 100);
      
      expect(eta).toBeCloseTo(10, 0);
    });
  });
});
