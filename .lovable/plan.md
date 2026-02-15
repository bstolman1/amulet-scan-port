

# Fix Ingestion Pipeline Issues (Critical to Least Critical)

This plan addresses 6 confirmed issues in the ingestion pipeline, ordered by severity. Each fix includes meaningful tests that exercise the actual production code (not duplicated logic).

---

## Issue 1: Event Loop Blocking via Synchronous Busy-Wait Sleep (CRITICAL)

**File:** `scripts/ingest/gcs-upload.js` (lines 96-101)

**Problem:** The `sleep()` function uses a `while (Date.now() < end)` busy-wait loop, which blocks the entire Node.js event loop during retry backoff. During a 30-second max delay, the process is completely unresponsive -- no uploads, no heartbeats, no signal handling.

**Fix:** Replace the synchronous busy-wait `sleep()` with `Atomics.wait()` on a `SharedArrayBuffer`, which blocks the thread without spinning the CPU. This keeps the function synchronous (required by `uploadAndCleanupSync`) but does not burn CPU cycles.

```javascript
function sleep(ms) {
  const sharedBuf = new SharedArrayBuffer(4);
  const view = new Int32Array(sharedBuf);
  Atomics.wait(view, 0, 0, ms);
}
```

**Test file:** `scripts/ingest/test/gcs-upload-sleep.test.js`
- Verify `sleep()` blocks for approximately the requested duration (within a tolerance)
- Verify it does NOT consume excessive CPU (measure elapsed time vs wall time)
- Verify the synchronous `uploadAndCleanupSync` path uses the non-blocking sleep by exporting `sleep` and `calculateBackoffDelay` for testability

---

## Issue 2: TLS Verification Disabled in Production (CRITICAL)

**File:** `scripts/ingest/fetch-updates.js` (line 142)

**Problem:** `rejectUnauthorized: false` on the HTTPS agent disables certificate validation for ALL API calls, including the main ingestion loop and all 13 endpoint probes. This exposes the pipeline to man-in-the-middle attacks.

**Fix:** Default to `rejectUnauthorized: true`. Only disable when the `INSECURE_TLS` environment variable is explicitly set to `'true'` (for development/testing against self-signed certs).

```javascript
const client = axios.create({
  baseURL: activeScanUrl,
  timeout: FETCH_TIMEOUT_MS,
  httpsAgent: new https.Agent({ 
    rejectUnauthorized: process.env.INSECURE_TLS !== 'true'
  }),
});
```

Also update the probe function (line 1000) to use the same logic:
```javascript
httpsAgent: new https.Agent({ rejectUnauthorized: process.env.INSECURE_TLS !== 'true' }),
```

**Test file:** `scripts/ingest/test/tls-config.test.js`
- Test that with `INSECURE_TLS` unset, `rejectUnauthorized` resolves to `true`
- Test that with `INSECURE_TLS=true`, `rejectUnauthorized` resolves to `false`
- Test that with `INSECURE_TLS=false`, `rejectUnauthorized` resolves to `true`
- Tests use the extracted helper function `getTLSRejectUnauthorized()` rather than duplicating logic

---

## Issue 3: Graceful Shutdown Missing in `ingest-all.js` (HIGH)

**File:** `scripts/ingest/ingest-all.js` (lines 157-165)

**Problem:** The SIGINT/SIGTERM handlers call `process.exit(0)` immediately without killing the child process spawned by `runScript()`. This means:
- The child script (backfill or live updates) becomes an orphan process
- No buffers are flushed, no cursors saved in the child

**Fix:** Track the active child process and send it the signal before exiting. Wait up to 5 seconds for clean exit, then SIGKILL.

```javascript
let activeChild = null;

function runScript(scriptPath, scriptArgs = []) {
  return new Promise((resolve, reject) => {
    // ... existing code ...
    const child = spawn('node', [scriptPath, ...scriptArgs], { ... });
    activeChild = child;
    
    child.on('exit', (code) => {
      activeChild = null;
      // ... existing logic ...
    });
  });
}

async function gracefulShutdown(signal) {
  console.log(`\n\n[ingest-all] Received ${signal} - forwarding to child...`);
  if (activeChild) {
    activeChild.kill(signal);
    // Wait up to 5s for child to exit
    const timeout = setTimeout(() => {
      console.log('[ingest-all] Child did not exit in time, sending SIGKILL');
      activeChild?.kill('SIGKILL');
    }, 5000);
    activeChild.on('exit', () => clearTimeout(timeout));
  } else {
    process.exit(0);
  }
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
```

**Test file:** `scripts/ingest/test/ingest-all-shutdown.test.js`
- Test that `gracefulShutdown` sends the signal to the active child process
- Test that when no child is active, `process.exit(0)` is called
- Test the 5-second SIGKILL escalation timer logic
- Uses extracted `gracefulShutdown` function with dependency injection for the child process reference

---

## Issue 4: Worker-per-Job in Parquet Writer Pool (MODERATE)

**File:** `scripts/ingest/parquet-writer-pool.js` (line 159)

**Problem:** Every `_pump()` call spawns `new Worker(WORKER_SCRIPT, { workerData: job })`. Workers are created and destroyed for each job, which incurs significant thread-creation overhead and GC pressure during high-throughput ingestion (hundreds of files per session).

**Fix:** Pre-spawn a fixed pool of persistent workers at `init()` time. Each worker listens for job messages on its `parentPort` and posts results back. The pool sends jobs via `worker.postMessage()` instead of spawning new threads.

Changes to `parquet-writer-pool.js`:
- Add `_workers` array, spawn workers in `init()`
- Each worker stays alive and processes jobs via message passing
- `_pump()` picks an idle worker and sends the job via `postMessage`
- `shutdown()` terminates all persistent workers

Changes to `parquet-worker.js`:
- If started without `workerData`, enter message-listening mode (`parentPort.on('message', ...)`)
- Process the job and post result back via `parentPort.postMessage()`
- If started with `workerData` (legacy), process once and exit (backward compatibility)

**Test file:** `scripts/ingest/test/parquet-pool-persistent.test.js`
- Test that `init()` creates the expected number of workers
- Test that submitting multiple jobs reuses workers (worker count stays constant)
- Test that `shutdown()` terminates all workers
- Test that stats are tracked correctly across multiple jobs
- These tests use the actual `ParquetWriterPool` class with a mock worker script

---

## Issue 5: No MD5 Verification in Retry Script (MODERATE)

**File:** `scripts/ingest/retry-failed-uploads.js` (lines 51-66)

**Problem:** The `retryUpload()` function uploads via `gsutil cp` but never verifies the upload integrity. The main upload path (`gcs-upload-queue.js`) performs MD5 verification, but the retry path skips it entirely. A corrupted retry upload would be silently accepted.

**Fix:** After a successful `gsutil cp`, call `verifyUploadIntegrity()` from `gcs-upload-queue.js` to compare local and remote MD5 hashes.

```javascript
import { verifyUploadIntegrity } from '../gcs-upload-queue.js';

export function retryUpload(localPath, gcsPath, timeout = 300000) {
  if (!existsSync(localPath)) {
    return { ok: false, error: 'Local file no longer exists', recoverable: false };
  }

  try {
    execSync(`gsutil -q cp "${localPath}" "${gcsPath}"`, { ... });
    
    // Verify integrity (same as primary upload path)
    const verification = verifyUploadIntegrity(localPath, gcsPath);
    if (!verification.ok) {
      return { ok: false, error: `Integrity check failed: ${verification.error}`, recoverable: true };
    }
    
    return { ok: true, localMD5: verification.localMD5 };
  } catch (err) {
    return { ok: false, error: err.message, recoverable: true };
  }
}
```

**Test file:** `scripts/ingest/test/retry-integrity.test.js`
- Test that a successful upload followed by matching MD5 returns `ok: true`
- Test that a successful upload followed by mismatched MD5 returns `ok: false, recoverable: true`
- Test that failed MD5 retrieval (gsutil stat failure) returns `ok: false, recoverable: true`
- Tests extract the integrity-check logic into a testable pure function

---

## Issue 6: No Backpressure Check Before Flushing to Parquet (LOW)

**File:** `scripts/ingest/write-parquet.js` (lines 454-473)

**Problem:** `bufferUpdates()` and `bufferEvents()` flush based solely on row count (`MAX_ROWS_PER_FILE`) without checking if the GCS upload queue is saturated. When the queue is paused due to backpressure, flushing creates more local files that pile up in `/tmp`, risking disk exhaustion.

**Fix:** Check `shouldPauseWrites()` before flushing. If paused, wait for the queue to drain below the low-water mark before writing.

```javascript
export async function bufferUpdates(updates) {
  updatesBuffer.push(...updates);
  
  if (updatesBuffer.length >= MAX_ROWS_PER_FILE) {
    // Wait for upload queue backpressure to clear before creating more files
    if (getGCSMode() && shouldPauseWrites()) {
      console.log(`⏳ [write-parquet] Waiting for upload queue backpressure to clear...`);
      await drainUploads();
    }
    return await flushUpdates();
  }
  return null;
}
```

Same change for `bufferEvents()`.

**Test file:** `scripts/ingest/test/write-parquet-backpressure.test.js`
- Test that when `shouldPauseWrites()` returns false, flush proceeds immediately
- Test that when `shouldPauseWrites()` returns true, `drainUploads()` is awaited before flush
- Test that in local mode (no GCS), backpressure check is skipped entirely
- Tests use dependency injection to mock the queue state functions

---

## Summary of Changes

| # | Severity | File | Change |
|---|----------|------|--------|
| 1 | CRITICAL | `gcs-upload.js` | Replace busy-wait sleep with `Atomics.wait` |
| 2 | CRITICAL | `fetch-updates.js` | Enable TLS verification by default |
| 3 | HIGH | `ingest-all.js` | Forward signals to child, wait before SIGKILL |
| 4 | MODERATE | `parquet-writer-pool.js` + `parquet-worker.js` | Persistent worker pool instead of spawn-per-job |
| 5 | MODERATE | `retry-failed-uploads.js` | Add MD5 verification after retry upload |
| 6 | LOW | `write-parquet.js` | Check upload queue backpressure before flush |

**New test files (6):**
1. `scripts/ingest/test/gcs-upload-sleep.test.js`
2. `scripts/ingest/test/tls-config.test.js`
3. `scripts/ingest/test/ingest-all-shutdown.test.js`
4. `scripts/ingest/test/parquet-pool-persistent.test.js`
5. `scripts/ingest/test/retry-integrity.test.js`
6. `scripts/ingest/test/write-parquet-backpressure.test.js`

