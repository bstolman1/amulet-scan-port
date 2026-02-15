/**
 * Tests for persistent worker pool in parquet-writer-pool.js
 * 
 * Uses a lightweight mock worker script to verify:
 * - Workers are spawned at init() and reused across jobs
 * - shutdown() terminates all workers
 * - Stats are tracked correctly
 */

import { describe, it, expect, afterEach } from 'vitest';
import { ParquetWriterPool } from '../parquet-writer-pool.js';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Create a mock worker script using CommonJS-compatible code
const MOCK_WORKER_PATH = path.join(os.tmpdir(), 'test-parquet-worker.mjs');

function createMockWorkerScript() {
  const script = `
import { parentPort, workerData } from 'node:worker_threads';

if (workerData === null || workerData === undefined) {
  parentPort.on('message', (job) => {
    parentPort.postMessage({
      ok: true,
      filePath: job.filePath,
      count: job.records ? job.records.length : 0,
      bytes: 1024,
      validation: { valid: true, rowCount: job.records ? job.records.length : 0, issues: [] },
    });
  });
}
`;
  fs.writeFileSync(MOCK_WORKER_PATH, script);
}

function cleanupMockWorkerScript() {
  try { fs.unlinkSync(MOCK_WORKER_PATH); } catch {}
}

describe('ParquetWriterPool (persistent workers)', () => {
  let pool;

  afterEach(async () => {
    if (pool) {
      try { await pool.shutdown(); } catch {}
      pool = null;
    }
    cleanupMockWorkerScript();
  }, 15000);

  it('spawns persistent workers on init()', async () => {
    createMockWorkerScript();
    pool = new ParquetWriterPool(3, MOCK_WORKER_PATH);
    await pool.init();

    expect(pool._persistentWorkers.length).toBe(3);
    expect(pool._idleWorkers.length).toBe(3);
    expect(pool.stats.workersSpawned).toBe(3);
  });

  it('reuses workers across multiple jobs (no new workers spawned)', async () => {
    createMockWorkerScript();
    pool = new ParquetWriterPool(2, MOCK_WORKER_PATH);
    await pool.init();

    const initialSpawned = pool.stats.workersSpawned;

    for (let i = 0; i < 5; i++) {
      await pool.writeJob({
        type: 'updates',
        filePath: `/tmp/test-${i}.parquet`,
        records: [{ update_id: `u${i}`, update_type: 'tx', update_data: '{}' }],
      });
    }

    expect(pool.stats.workersSpawned).toBe(initialSpawned);
    expect(pool.stats.completedJobs).toBe(5);
  }, 15000);

  it('tracks stats correctly across jobs', async () => {
    createMockWorkerScript();
    pool = new ParquetWriterPool(2, MOCK_WORKER_PATH);
    await pool.init();

    await pool.writeJob({
      type: 'events',
      filePath: '/tmp/test-a.parquet',
      records: [{ event_id: 'e1' }, { event_id: 'e2' }],
    });

    await pool.writeJob({
      type: 'updates',
      filePath: '/tmp/test-b.parquet',
      records: [{ update_id: 'u1' }],
    });

    const stats = pool.getStats();
    expect(stats.completedJobs).toBe(2);
    expect(stats.totalRecords).toBe(3);
    expect(stats.totalBytes).toBe(2048);
    expect(stats.failedJobs).toBe(0);
  }, 15000);

  it('handles concurrent jobs with limited workers', async () => {
    createMockWorkerScript();
    pool = new ParquetWriterPool(2, MOCK_WORKER_PATH);
    await pool.init();

    const jobs = Array.from({ length: 4 }, (_, i) =>
      pool.writeJob({
        type: 'updates',
        filePath: `/tmp/test-concurrent-${i}.parquet`,
        records: [{ update_id: `u${i}` }],
      })
    );

    const results = await Promise.all(jobs);
    
    expect(results).toHaveLength(4);
    results.forEach(r => expect(r.ok).toBe(true));
    expect(pool.stats.completedJobs).toBe(4);
  }, 15000);

  it('terminates all workers on shutdown()', async () => {
    createMockWorkerScript();
    pool = new ParquetWriterPool(3, MOCK_WORKER_PATH);
    await pool.init();

    expect(pool._persistentWorkers.length).toBe(3);

    await pool.shutdown();

    expect(pool._persistentWorkers.length).toBe(0);
    expect(pool._idleWorkers.length).toBe(0);
    pool = null;
  });
});
