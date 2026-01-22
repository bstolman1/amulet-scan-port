/**
 * Hive Partition Path Tests
 * 
 * Tests partition path generation critical for:
 * - BigQuery/DuckDB type inference
 * - Data organization consistency
 * - Cross-migration data isolation
 */

import { describe, it, expect } from 'vitest';
import { getPartitionPath } from '../data-schema.js';
import { getACSPartitionPath } from '../acs-schema.js';

describe('Hive Partition Paths', () => {
  
  describe('Backfill Partition Paths', () => {
    it('should generate correct path structure for updates (default)', () => {
      const path = getPartitionPath('2024-06-15T10:30:00Z', 0);
      
      expect(path).toBe('backfill/updates/migration=0/year=2024/month=6/day=15');
    });
    
    it('should generate correct path structure for updates (explicit)', () => {
      const path = getPartitionPath('2024-06-15T10:30:00Z', 0, 'updates');
      
      expect(path).toBe('backfill/updates/migration=0/year=2024/month=6/day=15');
    });
    
    it('should generate correct path structure for events', () => {
      const path = getPartitionPath('2024-06-15T10:30:00Z', 0, 'events');
      
      expect(path).toBe('backfill/events/migration=0/year=2024/month=6/day=15');
    });
    
    it('should nest updates and events under backfill/', () => {
      const updatesPath = getPartitionPath('2024-06-15T10:30:00Z', 0, 'updates');
      const eventsPath = getPartitionPath('2024-06-15T10:30:00Z', 0, 'events');
      
      expect(updatesPath).toMatch(/^backfill\/updates\//);
      expect(eventsPath).toMatch(/^backfill\/events\//);
      expect(updatesPath).not.toBe(eventsPath);
    });
    
    it('should use numeric (unpadded) month and day', () => {
      // Single-digit month and day should NOT be zero-padded
      const path = getPartitionPath('2024-01-05T10:30:00Z', 0);
      
      expect(path).toContain('month=1');
      expect(path).toContain('day=5');
      expect(path).not.toContain('month=01');
      expect(path).not.toContain('day=05');
    });
    
    it('should handle migration_id = 0', () => {
      const path = getPartitionPath('2024-06-15T10:30:00Z', 0);
      
      expect(path).toContain('migration=0');
    });
    
    it('should handle higher migration IDs', () => {
      const path = getPartitionPath('2024-06-15T10:30:00Z', 5);
      
      expect(path).toContain('migration=5');
    });
    
    it('should default migration to 0 when null', () => {
      const path = getPartitionPath('2024-06-15T10:30:00Z', null);
      
      expect(path).toContain('migration=0');
    });
    
    it('should handle end-of-year dates', () => {
      const path = getPartitionPath('2024-12-31T23:59:59Z', 0, 'events');
      
      expect(path).toBe('backfill/events/migration=0/year=2024/month=12/day=31');
    });
    
    it('should handle leap year February', () => {
      const path = getPartitionPath('2024-02-29T12:00:00Z', 0, 'updates');
      
      expect(path).toBe('backfill/updates/migration=0/year=2024/month=2/day=29');
    });
    
    it('should handle different years', () => {
      const path2023 = getPartitionPath('2023-06-15T10:30:00Z', 0);
      const path2025 = getPartitionPath('2025-06-15T10:30:00Z', 0);
      
      expect(path2023).toContain('year=2023');
      expect(path2025).toContain('year=2025');
    });
  });
  
  describe('ACS Partition Paths', () => {
    it('should generate correct path structure with snapshot_id', () => {
      const path = getACSPartitionPath('2024-06-15T10:30:45Z', 0);
      
      expect(path).toBe('acs/migration=0/year=2024/month=6/day=15/snapshot_id=103045');
    });
    
    it('should use numeric (unpadded) month and day', () => {
      const path = getACSPartitionPath('2024-01-05T08:05:09Z', 0);
      
      expect(path).toContain('month=1');
      expect(path).toContain('day=5');
      expect(path).not.toContain('month=01');
      expect(path).not.toContain('day=05');
    });
    
    it('should use padded snapshot_id (HHMMSS)', () => {
      const path = getACSPartitionPath('2024-06-15T08:05:09Z', 0);
      
      // Hour, minute, second should be zero-padded
      expect(path).toContain('snapshot_id=080509');
    });
    
    it('should handle migration_id = 0', () => {
      const path = getACSPartitionPath('2024-06-15T10:30:00Z', 0);
      
      expect(path).toContain('migration=0');
    });
    
    it('should default migration to 0 when null', () => {
      const path = getACSPartitionPath('2024-06-15T10:30:00Z', null);
      
      expect(path).toContain('migration=0');
    });
    
    it('should NOT start with acs/acs/ (avoid double nesting)', () => {
      const path = getACSPartitionPath('2024-06-15T10:30:00Z', 0);
      
      expect(path).toMatch(/^acs\//);
      expect(path).not.toMatch(/^acs\/acs\//);
    });
    
    it('should generate unique snapshot_id for different times', () => {
      const path1 = getACSPartitionPath('2024-06-15T10:30:00Z', 0);
      const path2 = getACSPartitionPath('2024-06-15T10:30:01Z', 0);
      const path3 = getACSPartitionPath('2024-06-15T10:31:00Z', 0);
      
      expect(path1).toContain('snapshot_id=103000');
      expect(path2).toContain('snapshot_id=103001');
      expect(path3).toContain('snapshot_id=103100');
    });
    
    it('should handle midnight correctly', () => {
      const path = getACSPartitionPath('2024-06-15T00:00:00Z', 0);
      
      expect(path).toContain('snapshot_id=000000');
    });
    
    it('should handle end of day correctly', () => {
      const path = getACSPartitionPath('2024-06-15T23:59:59Z', 0);
      
      expect(path).toContain('snapshot_id=235959');
    });
  });
  
  describe('BigQuery/DuckDB Type Inference', () => {
    it('should generate INT64-compatible partition values', () => {
      // BigQuery and DuckDB infer types from partition values
      // Numeric values (6, 15) are inferred as INT64
      // Padded strings ("06", "15") are inferred as STRING/BYTE_ARRAY
      
      const path = getPartitionPath('2024-06-15T10:30:00Z', 0, 'events');
      
      // Extract partition values
      const monthMatch = path.match(/month=(\d+)/);
      const dayMatch = path.match(/day=(\d+)/);
      
      // Values should be parseable as integers without leading zeros
      expect(monthMatch[1]).toBe('6');
      expect(dayMatch[1]).toBe('15');
      
      // Verify they're truly numeric (not strings with leading zeros)
      expect(parseInt(monthMatch[1]).toString()).toBe(monthMatch[1]);
      expect(parseInt(dayMatch[1]).toString()).toBe(dayMatch[1]);
    });
    
    it('should maintain consistent types across months', () => {
      const paths = [
        getPartitionPath('2024-01-15T10:30:00Z', 0),
        getPartitionPath('2024-06-15T10:30:00Z', 0),
        getPartitionPath('2024-12-15T10:30:00Z', 0),
      ];
      
      const months = paths.map(p => p.match(/month=(\d+)/)[1]);
      
      // All should be numeric without leading zeros
      expect(months).toEqual(['1', '6', '12']);
    });
    
    it('should maintain consistent types across days', () => {
      const paths = [
        getPartitionPath('2024-06-01T10:30:00Z', 0),
        getPartitionPath('2024-06-09T10:30:00Z', 0),
        getPartitionPath('2024-06-15T10:30:00Z', 0),
        getPartitionPath('2024-06-31T10:30:00Z', 0), // Invalid date, but tests format
      ];
      
      const days = paths.map(p => p.match(/day=(\d+)/)[1]);
      
      // Day 1 and 9 should NOT have leading zeros
      expect(days[0]).toBe('1');
      expect(days[1]).toBe('9');
    });
  });
  
  describe('Path Consistency', () => {
    it('should produce deterministic paths for same input', () => {
      const timestamp = '2024-06-15T10:30:00Z';
      const migration = 0;
      
      const path1 = getPartitionPath(timestamp, migration);
      const path2 = getPartitionPath(timestamp, migration);
      const path3 = getPartitionPath(timestamp, migration);
      
      expect(path1).toBe(path2);
      expect(path2).toBe(path3);
    });
    
    it('should isolate data by migration', () => {
      const timestamp = '2024-06-15T10:30:00Z';
      
      const path0 = getPartitionPath(timestamp, 0);
      const path1 = getPartitionPath(timestamp, 1);
      const path5 = getPartitionPath(timestamp, 5);
      
      expect(path0).not.toBe(path1);
      expect(path1).not.toBe(path5);
      
      expect(path0).toContain('migration=0');
      expect(path1).toContain('migration=1');
      expect(path5).toContain('migration=5');
    });
  });
});
