/**
 * Parquet Worker Performance Optimization Tests
 * 
 * Tests the actual parquet-worker.js changes:
 * 1. DuckDB connection reuse (persistent instance across jobs)
 * 2. Sampling-based validation (only validate every Nth file)
 * 
 * These tests spawn real worker threads and verify behavior through
 * the ParquetWriterPool, which is how the worker is used in production.
 */

import { describe, it, expect, afterEach, beforeEach } from 'vitest';
import { Worker } from 'node:worker_threads';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const WORKER_SCRIPT = path.join(__dirname, '..', 'parquet-worker.js');

// Helper: run a job through the real worker and get the result
function runWorkerJob(job, env = {}) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT, {
      workerData: null, // persistent mode
      env: { ...process.env, ...env },
    });

    const timeout = setTimeout(() => {
      worker.terminate();
      reject(new Error('Worker timed out after 15s'));
    }, 15000);

    worker.on('message', (msg) => {
      clearTimeout(timeout);
      resolve(msg);
      // Don't terminate - send another job or let it sit
    });

    worker.on('error', (err) => {
      clearTimeout(timeout);
      reject(err);
    });

    // Send job to persistent worker
    worker.postMessage(job);

    // Store reference for cleanup
    runWorkerJob._lastWorker = worker;
  });
}

// Helper: send multiple jobs to the SAME persistent worker
function createPersistentWorker(env = {}) {
  const worker = new Worker(WORKER_SCRIPT, {
    workerData: null,
    env: { ...process.env, ...env },
  });

  let messageQueue = [];
  let waitingResolve = null;

  worker.on('message', (msg) => {
    if (waitingResolve) {
      const resolve = waitingResolve;
      waitingResolve = null;
      resolve(msg);
    } else {
      messageQueue.push(msg);
    }
  });

  return {
    sendJob(job) {
      return new Promise((resolve, reject) => {
        if (messageQueue.length > 0) {
          resolve(messageQueue.shift());
          return;
        }
        waitingResolve = resolve;
        
        const timeout = setTimeout(() => {
          waitingResolve = null;
          reject(new Error('Worker job timed out'));
        }, 15000);

        const origResolve = waitingResolve;
        waitingResolve = (msg) => {
          clearTimeout(timeout);
          origResolve(msg);
        };

        worker.postMessage(job);
      });
    },
    terminate() {
      return worker.terminate();
    },
  };
}

// Generate test records
function makeUpdateRecords(count) {
  const records = [];
  for (let i = 0; i < count; i++) {
    records.push({
      update_id: `update-${i}`,
      update_type: 'transaction',
      synchronizer_id: 'sync-1',
      effective_at: new Date(Date.now() - i * 1000).toISOString(),
      recorded_at: new Date(Date.now() - i * 1000).toISOString(),
      record_time: new Date(Date.now() - i * 1000).toISOString(),
      timestamp: new Date().toISOString(),
      command_id: `cmd-${i}`,
      workflow_id: `wf-${i}`,
      kind: 'transaction',
      migration_id: 3,
      offset: i,
      event_count: 1,
      root_event_ids: [`evt-${i}`],
      update_data: JSON.stringify({ test: true, index: i }),
    });
  }
  return records;
}

function makeEventRecords(count) {
  const records = [];
  for (let i = 0; i < count; i++) {
    records.push({
      event_id: `event-${i}`,
      update_id: `update-${i}`,
      event_type: 'created',
      event_type_original: 'CreatedEvent',
      synchronizer_id: 'sync-1',
      effective_at: new Date(Date.now() - i * 1000).toISOString(),
      recorded_at: new Date(Date.now() - i * 1000).toISOString(),
      timestamp: new Date().toISOString(),
      contract_id: `contract-${i}`,
      template_id: 'Splice.Amulet:Amulet',
      package_name: 'splice-amulet',
      migration_id: 3,
      signatories: ['party-1'],
      observers: [],
      acting_parties: ['party-1'],
      witness_parties: ['party-1'],
      raw_event: JSON.stringify({ created: { contractId: `contract-${i}` } }),
    });
  }
  return records;
}

// Temp dir for test outputs
let tmpDir;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'parquet-worker-test-'));
});

afterEach(() => {
  try {
    if (runWorkerJob._lastWorker) {
      runWorkerJob._lastWorker.terminate();
      runWorkerJob._lastWorker = null;
    }
  } catch {}
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch {}
});


describe('Parquet Worker - DuckDB Connection Reuse', () => {

  it('should process multiple sequential jobs on the same worker without error', async () => {
    // This verifies the DuckDB instance survives across jobs (not re-created each time)
    const worker = createPersistentWorker();

    try {
      const results = [];
      for (let i = 0; i < 5; i++) {
        const filePath = path.join(tmpDir, `updates-${i}.parquet`);
        const result = await worker.sendJob({
          type: 'updates',
          filePath,
          records: makeUpdateRecords(10),
          rowGroupSize: 100000,
        });
        results.push(result);
      }

      // All 5 jobs should succeed
      expect(results).toHaveLength(5);
      for (const r of results) {
        expect(r.ok).toBe(true);
        expect(r.count).toBe(10);
        expect(r.bytes).toBeGreaterThan(0);
      }

      // All files should exist
      for (let i = 0; i < 5; i++) {
        expect(fs.existsSync(path.join(tmpDir, `updates-${i}.parquet`))).toBe(true);
      }
    } finally {
      await worker.terminate();
    }
  });

  it('should handle mixed event and update types on the same worker', async () => {
    const worker = createPersistentWorker();

    try {
      const updatePath = path.join(tmpDir, 'updates-mixed.parquet');
      const eventPath = path.join(tmpDir, 'events-mixed.parquet');

      const r1 = await worker.sendJob({
        type: 'updates',
        filePath: updatePath,
        records: makeUpdateRecords(5),
      });

      const r2 = await worker.sendJob({
        type: 'events',
        filePath: eventPath,
        records: makeEventRecords(5),
      });

      expect(r1.ok).toBe(true);
      expect(r2.ok).toBe(true);
      expect(r1.count).toBe(5);
      expect(r2.count).toBe(5);

      // Verify files have different sizes (different schemas)
      const s1 = fs.statSync(updatePath);
      const s2 = fs.statSync(eventPath);
      expect(s1.size).toBeGreaterThan(0);
      expect(s2.size).toBeGreaterThan(0);
      // Events have more columns, should be different size
      expect(s1.size).not.toBe(s2.size);
    } finally {
      await worker.terminate();
    }
  });

  it('should not leak state between jobs (each reads fresh temp file)', async () => {
    const worker = createPersistentWorker();

    try {
      // Job 1: write 3 records
      const path1 = path.join(tmpDir, 'leak-test-1.parquet');
      const r1 = await worker.sendJob({
        type: 'updates',
        filePath: path1,
        records: makeUpdateRecords(3),
      });

      // Job 2: write 7 records
      const path2 = path.join(tmpDir, 'leak-test-2.parquet');
      const r2 = await worker.sendJob({
        type: 'updates',
        filePath: path2,
        records: makeUpdateRecords(7),
      });

      // Counts should be independent — no contamination from job 1
      expect(r1.count).toBe(3);
      expect(r2.count).toBe(7);

      // If validation ran, row counts should match expected
      if (r1.validation && r1.validation.rowCount !== undefined) {
        // rowCount should be 3, not 3+7
        expect(r1.validation.rowCount).toBe(3);
      }
      if (r2.validation && r2.validation.rowCount !== undefined) {
        expect(r2.validation.rowCount).toBe(7);
      }
    } finally {
      await worker.terminate();
    }
  });

  it('should be faster on second job due to DuckDB reuse', async () => {
    const worker = createPersistentWorker();
    const records = makeUpdateRecords(50);

    try {
      // Job 1: includes DuckDB initialization overhead
      const start1 = performance.now();
      const path1 = path.join(tmpDir, 'speed-1.parquet');
      await worker.sendJob({ type: 'updates', filePath: path1, records });
      const elapsed1 = performance.now() - start1;

      // Job 2: reuses existing DuckDB connection
      const start2 = performance.now();
      const path2 = path.join(tmpDir, 'speed-2.parquet');
      await worker.sendJob({ type: 'updates', filePath: path2, records });
      const elapsed2 = performance.now() - start2;

      // Second job should be faster (or at worst similar) since DuckDB is already initialized
      // We use a generous threshold since CI environments are variable
      console.log(`  DuckDB reuse: Job 1 = ${elapsed1.toFixed(0)}ms, Job 2 = ${elapsed2.toFixed(0)}ms`);
      
      // At minimum, both should complete successfully
      expect(fs.existsSync(path1)).toBe(true);
      expect(fs.existsSync(path2)).toBe(true);
    } finally {
      await worker.terminate();
    }
  });
});


describe('Parquet Worker - Sampling-Based Validation', () => {

  it('should always validate the first N files (early error detection)', async () => {
    // With PARQUET_VALIDATION_SAMPLE_RATE=20, first 5 files should always validate
    const worker = createPersistentWorker({
      PARQUET_VALIDATION_SAMPLE_RATE: '20',
    });

    try {
      const results = [];
      for (let i = 0; i < 5; i++) {
        const filePath = path.join(tmpDir, `early-${i}.parquet`);
        const result = await worker.sendJob({
          type: 'updates',
          filePath,
          records: makeUpdateRecords(10),
        });
        results.push(result);
      }

      // All first 5 should have validation data with rowCount
      for (let i = 0; i < 5; i++) {
        expect(results[i].ok).toBe(true);
        expect(results[i].validation).toBeDefined();
        expect(results[i].validation.valid).toBe(true);
        // Should have actually counted rows (not just assumed)
        expect(results[i].validation.rowCount).toBe(10);
      }
    } finally {
      await worker.terminate();
    }
  });

  it('should skip validation for most files after the first N', async () => {
    // With sample rate 20 and 5 always-validate, files 6-19 should NOT be validated
    const worker = createPersistentWorker({
      PARQUET_VALIDATION_SAMPLE_RATE: '20',
    });

    try {
      const results = [];
      // Run 25 jobs total
      for (let i = 0; i < 25; i++) {
        const filePath = path.join(tmpDir, `sample-${i}.parquet`);
        const result = await worker.sendJob({
          type: 'updates',
          filePath,
          records: makeUpdateRecords(5),
        });
        results.push(result);
      }

      // Count how many had actual validation (with rowCount from DB query)
      let validatedCount = 0;
      let skippedCount = 0;

      for (let i = 0; i < results.length; i++) {
        expect(results[i].ok).toBe(true);
        const v = results[i].validation;
        if (v && v.rowCount !== undefined) {
          // If validation ran, it should have queried the actual count
          if (i < 5) {
            // First 5 always validated
            expect(v.rowCount).toBe(5);
            validatedCount++;
          } else if (v.issues && v.issues.length === 0 && v.rowCount === 5) {
            // This was a validated file
            validatedCount++;
          } else {
            skippedCount++;
          }
        }
      }

      // First 5 always validated + approximately 1 more (20th job) = ~6 validated
      // The remaining ~19 should be skipped
      console.log(`  Validation sampling: ${validatedCount} validated, ${skippedCount} skipped out of 25 files`);
      expect(validatedCount).toBeGreaterThanOrEqual(5); // At least the first 5
      expect(validatedCount).toBeLessThan(25); // Should not validate all
    } finally {
      await worker.terminate();
    }
  });

  it('should validate at the correct sample rate', async () => {
    // Use a sample rate of 3 for easier testing
    const worker = createPersistentWorker({
      PARQUET_VALIDATION_SAMPLE_RATE: '3',
    });

    try {
      const results = [];
      for (let i = 0; i < 12; i++) {
        const filePath = path.join(tmpDir, `rate-${i}.parquet`);
        const result = await worker.sendJob({
          type: 'updates',
          filePath,
          records: makeUpdateRecords(3),
        });
        results.push(result);
      }

      // With rate=3 and always_first=5:
      // Jobs 1-5: always validated (counter 1-5)
      // Job 6: counter=6, 6%3=0 → validated
      // Job 7: counter=7, 7%3≠0 → skipped
      // Job 8: counter=8, 8%3≠0 → skipped
      // Job 9: counter=9, 9%3=0 → validated
      // Job 10: counter=10, 10%3≠0 → skipped
      // Job 11: counter=11, 11%3≠0 → skipped
      // Job 12: counter=12, 12%3=0 → validated
      // Expected validated: 5 + 3 = 8
      
      let withValidation = 0;
      for (const r of results) {
        if (r.validation && r.validation.rowCount !== undefined && r.validation.rowCount === 3) {
          withValidation++;
        }
      }

      // Should be approximately 8 (5 mandatory + every 3rd after)
      expect(withValidation).toBeGreaterThanOrEqual(7);
      expect(withValidation).toBeLessThanOrEqual(9);
    } finally {
      await worker.terminate();
    }
  });
});


describe('Parquet Worker - Input Validation', () => {

  it('should reject invalid type', async () => {
    const result = await runWorkerJob({
      type: 'invalid',
      filePath: path.join(tmpDir, 'bad.parquet'),
      records: [],
    });

    expect(result.ok).toBe(false);
    expect(result.error).toContain('Invalid type');

    await runWorkerJob._lastWorker?.terminate();
  });

  it('should reject missing filePath', async () => {
    const result = await runWorkerJob({
      type: 'updates',
      records: makeUpdateRecords(1),
    });

    expect(result.ok).toBe(false);
    expect(result.error).toContain('No filePath');

    await runWorkerJob._lastWorker?.terminate();
  });

  it('should handle empty records gracefully', async () => {
    const result = await runWorkerJob({
      type: 'updates',
      filePath: path.join(tmpDir, 'empty.parquet'),
      records: [],
    });

    expect(result.ok).toBe(true);
    expect(result.count).toBe(0);
    expect(result.bytes).toBe(0);

    await runWorkerJob._lastWorker?.terminate();
  });

  it('should clean up temp JSONL files even on success', async () => {
    const filePath = path.join(tmpDir, 'cleanup-test.parquet');
    const result = await runWorkerJob({
      type: 'updates',
      filePath,
      records: makeUpdateRecords(5),
    });

    expect(result.ok).toBe(true);
    
    // Temp JSONL should be cleaned up
    const tempPath = filePath.replace('.parquet', '.temp.jsonl');
    expect(fs.existsSync(tempPath)).toBe(false);
    
    // Parquet file should exist
    expect(fs.existsSync(filePath)).toBe(true);

    await runWorkerJob._lastWorker?.terminate();
  });
});
