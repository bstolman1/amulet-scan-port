/**
 * Tests for byte-aware backpressure in gcs-upload-queue.js
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const {
  mockExistsSync, mockStatSync, mockUnlinkSync, mockReadFileSync,
  mockAppendFileSync, mockMkdirSync,
  mockSpawn, mockExecSync,
} = vi.hoisted(() => ({
  mockExistsSync: vi.fn(),
  mockStatSync: vi.fn(),
  mockUnlinkSync: vi.fn(),
  mockReadFileSync: vi.fn(),
  mockAppendFileSync: vi.fn(),
  mockMkdirSync: vi.fn(),
  mockSpawn: vi.fn(),
  mockExecSync: vi.fn(),
}));

vi.mock('fs', async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    default: {
      ...actual,
      existsSync: mockExistsSync, statSync: mockStatSync, unlinkSync: mockUnlinkSync,
      readFileSync: mockReadFileSync, appendFileSync: mockAppendFileSync, mkdirSync: mockMkdirSync,
    },
    existsSync: mockExistsSync,
    statSync: mockStatSync,
    unlinkSync: mockUnlinkSync,
    readFileSync: mockReadFileSync,
    appendFileSync: mockAppendFileSync,
    mkdirSync: mockMkdirSync,
  };
});

vi.mock('child_process', () => ({
  spawn: mockSpawn,
  execSync: mockExecSync,
  default: { spawn: mockSpawn, execSync: mockExecSync },
}));

describe('byte-aware backpressure', () => {
  let GCSUploadQueue;
  let origPump;

  beforeEach(async () => {
    vi.clearAllMocks();
    process.env.GCS_QUEUE_HIGH_WATER = '1000';
    process.env.GCS_QUEUE_LOW_WATER = '5';
    process.env.GCS_BYTE_HIGH_WATER = '1048576'; // 1MB
    process.env.GCS_BYTE_LOW_WATER = '524288';   // 512KB

    const mod = await import('../gcs-upload-queue.js');
    GCSUploadQueue = mod.default;

    // Save and replace _pump so enqueue() never dequeues or processes uploads
    origPump = GCSUploadQueue.prototype._pump;
    GCSUploadQueue.prototype._pump = function () {
      // Preserve backpressure-off and drain logic only
      if (this.isPaused && this.queue.length <= this.queueLowWater && this.queuedBytes <= this.byteLowWater) {
        this.isPaused = false;
      }
      if (this.drainResolve && this.activeUploads === 0 && this.queue.length === 0) {
        this.drainResolve();
        this.drainResolve = null;
        this.drainPromise = null;
      }
    };
  });

  afterEach(() => {
    // Restore original _pump
    if (GCSUploadQueue && origPump) {
      GCSUploadQueue.prototype._pump = origPump;
    }
    delete process.env.GCS_QUEUE_HIGH_WATER;
    delete process.env.GCS_QUEUE_LOW_WATER;
    delete process.env.GCS_BYTE_HIGH_WATER;
    delete process.env.GCS_BYTE_LOW_WATER;
  });

  it('triggers backpressure when queued bytes exceed byte high water', () => {
    const queue = new GCSUploadQueue(8);

    mockExistsSync.mockReturnValue(true);
    mockStatSync.mockReturnValue({ size: 600 * 1024 }); // 600KB each

    queue.enqueue('/tmp/big1.parquet', 'gs://b/big1.parquet');
    expect(queue.shouldPause()).toBe(false);

    queue.enqueue('/tmp/big2.parquet', 'gs://b/big2.parquet');
    // 1200KB > 1MB threshold
    expect(queue.shouldPause()).toBe(true);
  });

  it('does not trigger backpressure for many small files under byte threshold', () => {
    const queue = new GCSUploadQueue(8);

    mockExistsSync.mockReturnValue(true);
    mockStatSync.mockReturnValue({ size: 100 });

    for (let i = 0; i < 10; i++) {
      queue.enqueue(`/tmp/small${i}.parquet`, `gs://b/small${i}.parquet`);
    }

    expect(queue.shouldPause()).toBe(false);
  });

  it('tracks peak queue bytes in stats', () => {
    const queue = new GCSUploadQueue(8);

    mockExistsSync.mockReturnValue(true);
    mockStatSync.mockReturnValue({ size: 256 * 1024 }); // 256KB

    queue.enqueue('/tmp/a.parquet', 'gs://b/a.parquet');
    queue.enqueue('/tmp/b.parquet', 'gs://b/b.parquet');

    const stats = queue.getStats();
    expect(stats.peakQueueBytes).toBeGreaterThanOrEqual(512 * 1024);
  });

  it('handles statSync failure gracefully (fileSize = 0)', () => {
    const queue = new GCSUploadQueue(8);

    mockExistsSync.mockReturnValue(true);
    mockStatSync.mockImplementation(() => { throw new Error('stat failed'); });

    // Should not throw — fileSize defaults to 0
    queue.enqueue('/tmp/broken.parquet', 'gs://b/broken.parquet');
    expect(queue.shouldPause()).toBe(false);
  });
});
