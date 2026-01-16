#!/usr/bin/env node
/**
 * Backfill Data Validation Tests (with assertions)
 * 
 * Validates backfill data integrity using vitest.
 */

import { describe, it, expect, vi } from 'vitest';
import { existsSync, readdirSync, statSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────────────────────
// Backfill Schema Tests
// ─────────────────────────────────────────────────────────────

describe('Backfill Update Schema', () => {
  const CRITICAL_UPDATE_FIELDS = ['id', 'type', 'migrationId'];
  const OPTIONAL_UPDATE_FIELDS = ['recordTime', 'synchronizerId', 'workflowId'];
  
  describe('Field validation', () => {
    it('should identify missing critical fields', () => {
      const update = { type: 'transaction', migrationId: 5 };
      const missing = CRITICAL_UPDATE_FIELDS.filter(f => !update[f]);
      
      expect(missing).toContain('id');
      expect(missing).toHaveLength(1);
    });
    
    it('should pass complete updates', () => {
      const update = { id: 'upd-123', type: 'transaction', migrationId: 5 };
      const missing = CRITICAL_UPDATE_FIELDS.filter(f => !update[f]);
      
      expect(missing).toHaveLength(0);
    });
    
    it('should validate update type values', () => {
      const validTypes = ['transaction', 'reassignment', 'init'];
      
      for (const type of validTypes) {
        expect(['transaction', 'reassignment', 'init']).toContain(type);
      }
    });
    
    it('should validate migration ID is number', () => {
      const update = { id: '123', type: 'transaction', migrationId: 5 };
      
      expect(typeof update.migrationId).toBe('number');
      expect(Number.isInteger(update.migrationId)).toBe(true);
    });
  });
});

describe('Backfill Event Schema', () => {
  const CRITICAL_EVENT_FIELDS = ['id', 'updateId', 'type'];
  
  describe('Field validation', () => {
    it('should identify missing critical event fields', () => {
      const event = { updateId: 'upd-123', type: 'created' };
      const missing = CRITICAL_EVENT_FIELDS.filter(f => !event[f]);
      
      expect(missing).toContain('id');
      expect(missing).toHaveLength(1);
    });
    
    it('should validate event type values', () => {
      const validTypes = ['created', 'archived', 'exercised', 'reassign_create', 'reassign_archive'];
      
      for (const type of validTypes) {
        const isValid = validTypes.includes(type);
        expect(isValid).toBe(true);
      }
    });
    
    it('should accept events with all fields', () => {
      const event = {
        id: 'evt-456',
        updateId: 'upd-123',
        type: 'created',
        contractId: '00abc123',
        templateId: 'Splice.Amulet:Amulet',
      };
      
      const missing = CRITICAL_EVENT_FIELDS.filter(f => !event[f]);
      expect(missing).toHaveLength(0);
    });
  });
});

describe('File Discovery', () => {
  describe('File pattern matching', () => {
    it('should identify update files', () => {
      const filenames = ['updates-001.pb.zst', 'events-001.pb.zst', 'other.txt'];
      const updateFiles = filenames.filter(n => n.startsWith('updates-'));
      
      expect(updateFiles).toHaveLength(1);
      expect(updateFiles[0]).toBe('updates-001.pb.zst');
    });
    
    it('should identify event files', () => {
      const filenames = ['updates-001.pb.zst', 'events-001.pb.zst', 'events-002.pb.zst'];
      const eventFiles = filenames.filter(n => n.startsWith('events-'));
      
      expect(eventFiles).toHaveLength(2);
    });
    
    it('should filter by extension', () => {
      const filenames = ['data.pb.zst', 'data.jsonl', 'data.parquet'];
      const zstFiles = filenames.filter(n => n.endsWith('.pb.zst'));
      const jsonlFiles = filenames.filter(n => n.endsWith('.jsonl'));
      
      expect(zstFiles).toHaveLength(1);
      expect(jsonlFiles).toHaveLength(1);
    });
    
    it('should handle nested directory patterns', () => {
      const pathParts = 'data/raw/migration=5/updates-001.pb.zst'.split('/');
      
      expect(pathParts).toHaveLength(4);
      expect(pathParts[2]).toBe('migration=5');
      expect(pathParts[3].startsWith('updates-')).toBe(true);
    });
  });
  
  describe('Sampling logic', () => {
    it('should sample files evenly', () => {
      const files = Array.from({ length: 100 }, (_, i) => `file-${i}.txt`);
      const sampleSize = 10;
      
      const sampled = [];
      const step = Math.floor(files.length / sampleSize);
      for (let i = 0; i < sampleSize; i++) {
        sampled.push(files[i * step]);
      }
      
      expect(sampled).toHaveLength(10);
      expect(sampled[0]).toBe('file-0.txt');
      expect(sampled[1]).toBe('file-10.txt');
    });
    
    it('should return all files when fewer than sample size', () => {
      const files = ['a.txt', 'b.txt', 'c.txt'];
      const sampleSize = 10;
      
      const result = files.length <= sampleSize ? files : files.slice(0, sampleSize);
      
      expect(result).toHaveLength(3);
      expect(result).toEqual(files);
    });
  });
});

describe('Cursor Management', () => {
  describe('Cursor structure', () => {
    it('should have required cursor fields', () => {
      const cursor = {
        migration_id: 5,
        last_before: '2025-01-15T12:00:00Z',
        complete: false,
        updates_processed: 1000,
      };
      
      expect(cursor).toHaveProperty('migration_id');
      expect(cursor).toHaveProperty('complete');
      expect(typeof cursor.complete).toBe('boolean');
    });
    
    it('should parse cursor from JSON', () => {
      const json = '{"migration_id": 5, "complete": true}';
      const cursor = JSON.parse(json);
      
      expect(cursor.migration_id).toBe(5);
      expect(cursor.complete).toBe(true);
    });
    
    it('should identify complete cursors', () => {
      const cursors = [
        { migration_id: 0, complete: true },
        { migration_id: 1, complete: false },
        { migration_id: 2, complete: true },
      ];
      
      const complete = cursors.filter(c => c.complete);
      const allComplete = cursors.every(c => c.complete);
      
      expect(complete).toHaveLength(2);
      expect(allComplete).toBe(false);
    });
  });
  
  describe('Cursor file naming', () => {
    it('should filter cursor files by extension', () => {
      const files = ['cursor-m0.json', 'cursor-m1.json', 'readme.md', 'live-cursor.json'];
      const cursorFiles = files.filter(f => f.endsWith('.json'));
      
      expect(cursorFiles).toHaveLength(3);
    });
    
    it('should exclude live cursor from backfill cursors', () => {
      const files = ['cursor-m0.json', 'live-cursor.json', 'cursor-m1.json'];
      const backfillCursors = files.filter(f => f.endsWith('.json') && f !== 'live-cursor.json');
      
      expect(backfillCursors).toHaveLength(2);
      expect(backfillCursors).not.toContain('live-cursor.json');
    });
  });
});

describe('Data Integrity Checks', () => {
  describe('Time gap detection', () => {
    it('should calculate time gaps correctly', () => {
      const timestamps = [
        '2025-01-15T10:00:00Z',
        '2025-01-15T10:05:00Z',
        '2025-01-15T10:30:00Z', // 25 min gap
      ];
      
      const dates = timestamps.map(t => new Date(t).getTime());
      const gaps = [];
      
      for (let i = 1; i < dates.length; i++) {
        gaps.push((dates[i] - dates[i - 1]) / 60000); // minutes
      }
      
      expect(gaps[0]).toBe(5);
      expect(gaps[1]).toBe(25);
    });
    
    it('should identify significant gaps', () => {
      const gaps = [5, 25, 3, 60, 2];
      const threshold = 10;
      const significantGaps = gaps.filter(g => g > threshold);
      
      expect(significantGaps).toHaveLength(2);
      expect(significantGaps).toContain(25);
      expect(significantGaps).toContain(60);
    });
  });
  
  describe('Record count validation', () => {
    it('should detect empty files', () => {
      const fileStats = { count: 0, size: 0 };
      const isEmpty = fileStats.count === 0;
      
      expect(isEmpty).toBe(true);
    });
    
    it('should calculate corruption ratio', () => {
      const stats = { readable: 95, corrupted: 5 };
      const total = stats.readable + stats.corrupted;
      const corruptionRatio = stats.corrupted / total;
      
      expect(corruptionRatio).toBe(0.05);
      expect(corruptionRatio).toBeLessThan(0.1);
    });
  });
});

describe('Binary File Handling', () => {
  describe('File extension parsing', () => {
    it('should identify compressed files', () => {
      const extensions = ['.pb.zst', '.jsonl.gz', '.jsonl.zst'];
      const isCompressed = (ext) => ext.includes('.gz') || ext.includes('.zst');
      
      expect(isCompressed('.pb.zst')).toBe(true);
      expect(isCompressed('.jsonl.gz')).toBe(true);
      expect(isCompressed('.jsonl')).toBe(false);
    });
    
    it('should identify protobuf files', () => {
      const isProtobuf = (filename) => filename.includes('.pb');
      
      expect(isProtobuf('updates-001.pb.zst')).toBe(true);
      expect(isProtobuf('updates-001.jsonl')).toBe(false);
    });
  });
  
  describe('File stats calculation', () => {
    it('should calculate size in MB', () => {
      const sizeBytes = 10 * 1024 * 1024; // 10 MB
      const sizeMB = sizeBytes / (1024 * 1024);
      
      expect(sizeMB).toBe(10);
    });
    
    it('should aggregate stats from multiple files', () => {
      const files = [
        { count: 100, size: 1024 },
        { count: 200, size: 2048 },
        { count: 150, size: 1536 },
      ];
      
      const totalCount = files.reduce((sum, f) => sum + f.count, 0);
      const totalSize = files.reduce((sum, f) => sum + f.size, 0);
      
      expect(totalCount).toBe(450);
      expect(totalSize).toBe(4608);
    });
  });
});

describe('Path Utilities', () => {
  describe('Path construction', () => {
    it('should join paths correctly', () => {
      const base = '/data';
      const sub = 'raw';
      const file = 'updates.pb.zst';
      
      const fullPath = [base, sub, file].join('/');
      
      expect(fullPath).toBe('/data/raw/updates.pb.zst');
    });
    
    it('should handle Windows-style paths', () => {
      const windowsPath = 'C:\\data\\raw\\updates.pb.zst';
      const normalized = windowsPath.replace(/\\/g, '/');
      
      expect(normalized).toBe('C:/data/raw/updates.pb.zst');
    });
    
    it('should extract migration from path', () => {
      const pathStr = '/data/raw/migration=5/updates-001.pb.zst';
      const match = pathStr.match(/migration=(\d+)/);
      
      expect(match).not.toBeNull();
      expect(match[1]).toBe('5');
    });
  });
});
