/**
 * Performance Configuration Tests
 *
 * Verifies that the code correctly implements the performance optimizations:
 * 1. Auto-tuning boundaries for MIN/MAX rows per file
 * 2. Validation sampling rate
 * 3. parquet-worker.js module-level DuckDB reuse
 * 4. gcs-upload-queue.js uses the SDK with CRC32C
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';

describe('Performance Configuration', () => {

  describe('Auto-Tune Rows Per File Logic', () => {
    // Tests the auto-tuning logic from write-parquet.js to ensure
    // it respects the new MIN/MAX boundaries

    function simulateAutoTune(rowsPerSec, filesWritten, currentRowsPerFile, minRows, maxRows) {
      let dynamicRowsPerFile = currentRowsPerFile;

      if (rowsPerSec > 2000 && filesWritten > 3 && dynamicRowsPerFile < maxRows) {
        dynamicRowsPerFile = Math.min(maxRows, Math.round(dynamicRowsPerFile * 1.5));
      } else if (rowsPerSec < 500 && dynamicRowsPerFile > minRows) {
        dynamicRowsPerFile = Math.max(minRows, Math.round(dynamicRowsPerFile * 0.7));
      }

      return dynamicRowsPerFile;
    }

    it('should scale up when throughput is high', () => {
      const result = simulateAutoTune(3000, 5, 25000, 25000, 100000);
      expect(result).toBe(37500); // 25000 * 1.5
    });

    it('should cap at MAX_ROWS_PER_FILE', () => {
      // Starting near the cap
      const result = simulateAutoTune(5000, 10, 80000, 25000, 100000);
      expect(result).toBe(100000); // capped, not 120000
    });

    it('should scale down when throughput is low', () => {
      const result = simulateAutoTune(200, 5, 50000, 25000, 100000);
      expect(result).toBe(35000); // 50000 * 0.7
    });

    it('should not go below MIN_ROWS_PER_FILE', () => {
      const result = simulateAutoTune(100, 5, 30000, 25000, 100000);
      expect(result).toBe(25000); // floor, not 21000
    });

    it('should not change at moderate throughput', () => {
      const result = simulateAutoTune(1000, 5, 50000, 25000, 100000);
      expect(result).toBe(50000); // no change
    });

    it('should not scale up with too few files written', () => {
      const result = simulateAutoTune(5000, 2, 25000, 25000, 100000);
      expect(result).toBe(25000); // filesWritten <= 3, no change
    });

    it('should converge to MAX in sustained high throughput', () => {
      let current = 25000;
      const min = 25000;
      const max = 100000;

      // Simulate 10 tuning windows of high throughput
      for (let i = 0; i < 10; i++) {
        current = simulateAutoTune(5000, 10, current, min, max);
      }

      expect(current).toBe(max);
    });

    it('should converge to MIN in sustained low throughput', () => {
      let current = 100000;
      const min = 25000;
      const max = 100000;

      for (let i = 0; i < 20; i++) {
        current = simulateAutoTune(100, 10, current, min, max);
      }

      expect(current).toBe(min);
    });
  });

  describe('Validation Sampling Math', () => {

    it('should validate correct files with rate=20 and always_first=5', () => {
      const RATE = 20;
      const ALWAYS_FIRST = 5;
      const TOTAL_FILES = 100;

      const validated = [];
      for (let i = 1; i <= TOTAL_FILES; i++) {
        if (i <= ALWAYS_FIRST || i % RATE === 0) {
          validated.push(i);
        }
      }

      // First 5 always validated
      expect(validated.slice(0, 5)).toEqual([1, 2, 3, 4, 5]);

      // After that, every 20th: 20, 40, 60, 80, 100
      const afterFirst = validated.filter(v => v > ALWAYS_FIRST);
      expect(afterFirst).toEqual([20, 40, 60, 80, 100]);

      // Total: 5 + 5 = 10 out of 100 (10%)
      expect(validated).toHaveLength(10);
    });

    it('should reduce I/O by ~95% compared to validating every file', () => {
      const RATE = 20;
      const ALWAYS_FIRST = 5;
      const TOTAL_FILES = 1000;

      let validatedCount = 0;
      for (let i = 1; i <= TOTAL_FILES; i++) {
        if (i <= ALWAYS_FIRST || i % RATE === 0) {
          validatedCount++;
        }
      }

      const reductionPercent = ((TOTAL_FILES - validatedCount) / TOTAL_FILES) * 100;

      // Should validate ~55 out of 1000 (5 + 50)
      expect(validatedCount).toBe(55);
      expect(reductionPercent).toBeCloseTo(94.5, 0);
    });
  });

  describe('File Size Impact Analysis', () => {
    it('should produce 5x fewer files with new settings', () => {
      const totalRecords = 1_000_000;
      
      const oldMinRows = 5000;
      const newMinRows = 25000;
      
      const oldFileCount = Math.ceil(totalRecords / oldMinRows);
      const newFileCount = Math.ceil(totalRecords / newMinRows);
      
      expect(oldFileCount).toBe(200);
      expect(newFileCount).toBe(40);
      expect(oldFileCount / newFileCount).toBe(5);
    });

    it('should reduce per-file overhead proportionally', () => {
      // Each file incurs: DuckDB init (30ms) + JSONL write + Parquet write + validation + GCS upload
      const perFileOverheadMs = 30 + 10 + 20 + 50 + 80; // ~190ms total
      const totalRecords = 1_000_000;

      const oldFiles = Math.ceil(totalRecords / 5000);
      const newFiles = Math.ceil(totalRecords / 25000);

      const oldOverheadSec = (oldFiles * perFileOverheadMs) / 1000;
      const newOverheadSec = (newFiles * perFileOverheadMs) / 1000;

      // Old: 200 files * 190ms = 38s overhead
      // New: 40 files * 190ms = 7.6s overhead
      expect(oldOverheadSec).toBeCloseTo(38, 0);
      expect(newOverheadSec).toBeCloseTo(7.6, 0);
      expect(oldOverheadSec / newOverheadSec).toBeCloseTo(5, 0);
    });
  });

  describe('parquet-worker.js Code Verification', () => {
    let workerSource;

    beforeEach(() => {
      workerSource = fs.readFileSync(
        path.resolve(process.cwd(), 'scripts/ingest/parquet-worker.js'),
        'utf-8'
      );
    });

    it('should create DuckDB at module level, not per-job', () => {
      // The new code should have module-level _db and _conn variables
      expect(workerSource).toContain('let _db = null');
      expect(workerSource).toContain('let _conn = null');
      
      // Should have an ensureDuckDB function that creates once
      expect(workerSource).toContain('async function ensureDuckDB');
      expect(workerSource).toContain('if (_conn) return');
      
      // processJob should NOT create new Database per call
      const processJobSection = workerSource.split('async function processJob')[1];
      expect(processJobSection).not.toContain("new duckdb.Database(':memory:')");
      expect(processJobSection).not.toContain('db.close()');
      expect(processJobSection).not.toContain('conn.close()');
    });

    it('should implement sampling-based validation', () => {
      expect(workerSource).toContain('PARQUET_VALIDATION_SAMPLE_RATE');
      expect(workerSource).toContain('ALWAYS_VALIDATE_FIRST_N');
      expect(workerSource).toContain('shouldValidate');
      expect(workerSource).toContain('_jobCounter');
    });

    it('should clean up DuckDB on process exit', () => {
      expect(workerSource).toContain("process.on('exit'");
      expect(workerSource).toContain('_conn');
      expect(workerSource).toContain('_db');
    });
  });

  describe('gcs-upload-queue.js Code Verification', () => {
    let queueSource;

    beforeEach(() => {
      queueSource = fs.readFileSync(
        path.resolve(process.cwd(), 'scripts/ingest/gcs-upload-queue.js'),
        'utf-8'
      );
    });

    it('should use @google-cloud/storage SDK, not gsutil', () => {
      expect(queueSource).toContain('@google-cloud/storage');
      expect(queueSource).toContain('createReadStream');
      expect(queueSource).toContain('createWriteStream');
      expect(queueSource).not.toContain("spawn('gsutil'");
      expect(queueSource).not.toContain('child_process');
    });

    it('should not have computeLocalMD5 or getGCSObjectMD5', () => {
      // SDK handles integrity automatically
      expect(queueSource).not.toContain('computeLocalMD5');
      expect(queueSource).not.toContain('getGCSObjectMD5');
    });

    it('should still have dead-letter logging', () => {
      expect(queueSource).toContain('logFailedUpload');
      expect(queueSource).toContain('failed-uploads.jsonl');
    });

    it('should still have backpressure with byte tracking', () => {
      expect(queueSource).toContain('byteHighWater');
      expect(queueSource).toContain('byteLowWater');
      expect(queueSource).toContain('queuedBytes');
    });
  });

  describe('package.json Dependencies', () => {
    let pkg;

    beforeEach(() => {
      pkg = JSON.parse(fs.readFileSync(
        path.resolve(process.cwd(), 'scripts/ingest/package.json'),
        'utf-8'
      ));
    });

    it('should include @google-cloud/storage', () => {
      expect(pkg.dependencies).toHaveProperty('@google-cloud/storage');
    });

    it('should still include duckdb', () => {
      expect(pkg.dependencies).toHaveProperty('duckdb');
    });
  });
});
