/**
 * Worker Writer - Binary Protobuf + ZSTD Streaming
 *
 * This worker receives PRE-MAPPED records from write-binary.js, encodes them
 * to Protobuf, and streams them through ZSTD compression to disk.
 *
 * IMPORTANT: Records are mapped ONCE by write-binary.js (mapEventRecord /
 * mapUpdateRecord). This worker does NOT re-map — it uses records directly to
 * avoid double-mapping bugs.
 *
 * CHUNKED APPROACH:
 * - Encode up to CHUNK_SIZE records at a time (never a giant single message)
 * - Compress each chunk separately with ZSTD
 * - Write chunks sequentially with 4-byte big-endian length prefixes
 * - No worker ever hits the memory ceiling
 *
 * FIXES APPLIED:
 *
 * FIX #1  './encoding.js' → './proto-encode.js'
 *         The dynamic import used the wrong module name. proto-encode.js is the
 *         reviewed, fixed module that exports getEncoders(). encoding.js does not
 *         exist, so every binary write attempt threw MODULE_NOT_FOUND and fell
 *         back to the error path — no binary data was ever written.
 *
 * FIX #2  Chunk errors no longer silently skip records
 *         Each chunk stage (create/encode/compress/write) used `continue` on
 *         failure, skipping that chunk silently. The final result still reported
 *         count=records.length as if all data was written. Now failures throw
 *         immediately, the worker reports the error to the parent, and the pool
 *         increments failedJobs. The file is cleaned up before exit.
 *
 * FIX #3  result.count reflects actual records written, not records.length
 *         Even with FIX #2, count now tracks `recordsWritten` — incremented only
 *         after a chunk's fd.write() succeeds. This gives the parent pool accurate
 *         stats even in edge cases where partial writes succeed before failure.
 *
 * FIX #4  process.exit(0) replaced with explicit worker lifecycle
 *         Calling process.exit(0) immediately after parentPort.postMessage() is
 *         a race: the worker thread can exit before the message is delivered to
 *         the parent port. The worker now lets the run() promise resolve
 *         naturally; binary-writer-pool.js handles worker lifecycle via the
 *         'message' and 'exit' events, so no explicit exit call is needed from
 *         within run(). The error paths that must exit early still use
 *         process.exit(1) since those are pre-message fatal conditions.
 */

// Capture ALL errors — even during import
process.on('uncaughtException', (err) => {
  console.error('[WORKER FATAL] Uncaught exception:', err.message);
  console.error(err.stack);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[WORKER FATAL] Unhandled rejection at:', promise);
  console.error('Reason:', reason);
  process.exit(1);
});

import { parentPort, workerData, isMainThread } from 'node:worker_threads';
import fs from 'node:fs';
import path from 'node:path';

console.log('[WORKER] Starting worker-writer.js');

// Validate we're in a worker thread
if (isMainThread) {
  console.error('[WORKER] Error: worker-writer.js must be run as a Worker, not directly');
  process.exit(1);
}

// Validate workerData exists
if (!workerData) {
  console.error('[WORKER] Error: No workerData received');
  parentPort.postMessage({ ok: false, error: 'No workerData received' });
  process.exit(1);
}

const {
  type,              // "events" | "updates"
  filePath,          // output file path
  records,           // array of plain JS objects (pre-mapped by write-binary.js)
  zstdLevel = 1,     // ZSTD compression level
  chunkSize = 2000,  // records per chunk (capped below)
} = workerData;

// FIX #4 (note): CHUNK_SIZE cap is kept but now logs a warning so it isn't silent
const CHUNK_SIZE = Math.min(chunkSize, 2000);
if (chunkSize > 2000) {
  console.warn(`[WORKER] chunkSize ${chunkSize} exceeds 2000 cap — using 2000`);
}

console.log(`[WORKER] Job received: type=${type}, records=${records?.length || 0}, file=${filePath}`);

// ── Input validation ──────────────────────────────────────────────────────────

if (!type || !['events', 'updates'].includes(type)) {
  const msg = `Invalid type: ${type}`;
  console.error('[WORKER]', msg);
  parentPort.postMessage({ ok: false, error: msg, filePath });
  process.exit(1);
}

if (!filePath) {
  const msg = 'No filePath provided';
  console.error('[WORKER]', msg);
  parentPort.postMessage({ ok: false, error: msg });
  process.exit(1);
}

if (!records || !Array.isArray(records)) {
  const msg = `Invalid records: expected array, got ${typeof records}`;
  console.error('[WORKER]', msg);
  parentPort.postMessage({ ok: false, error: msg, filePath });
  process.exit(1);
}

if (records.length === 0) {
  console.log('[WORKER] Empty records array, writing empty file');
  // FIX #4: no process.exit(0) — let the promise resolve naturally
  parentPort.postMessage({
    ok: true, filePath, count: 0,
    originalSize: 0, compressedSize: 0, chunksWritten: 0,
  });
  return;
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function run() {
  let originalSize   = 0;
  let compressedSize = 0;
  let chunksWritten  = 0;
  // FIX #3: track actual records written, not assumed records.length
  let recordsWritten = 0;

  // ── Load dependencies ───────────────────────────────────────────────────────

  console.log('[WORKER] Loading dependencies...');

  let compress;
  try {
    const zstdModule = await import('@mongodb-js/zstd');
    compress = zstdModule.compress;
    console.log('[WORKER] ZSTD loaded');
  } catch (err) {
    const msg = `Failed to load @mongodb-js/zstd: ${err.message}`;
    console.error('[WORKER]', msg);
    parentPort.postMessage({ ok: false, error: msg, filePath });
    return;
  }

  let getEncoders;
  try {
    // FIX #1: correct module name — proto-encode.js, not encoding.js
    const encodingModule = await import('./proto-encode.js');
    getEncoders = encodingModule.getEncoders;
    console.log('[WORKER] Encoding module loaded');
  } catch (err) {
    const msg = `Failed to load proto-encode.js: ${err.message}`;
    console.error('[WORKER]', msg);
    parentPort.postMessage({ ok: false, error: msg, filePath });
    return;
  }

  let Event, Update, EventBatch, UpdateBatch;
  try {
    const encoders = await getEncoders();
    ({ Event, Update, EventBatch, UpdateBatch } = encoders);
    console.log('[WORKER] Protobuf schema loaded');
  } catch (err) {
    const msg = `Failed to load protobuf schema: ${err.message}`;
    console.error('[WORKER]', msg);
    parentPort.postMessage({ ok: false, error: msg, filePath });
    return;
  }

  const FileBatchType = type === 'events' ? EventBatch  : UpdateBatch;
  const RecordType    = type === 'events' ? Event        : Update;
  const fieldName     = type === 'events' ? 'events'     : 'updates';

  // ── Open file ───────────────────────────────────────────────────────────────

  let fd;
  try {
    const dir = path.dirname(filePath);
    if (dir && !fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fd = await fs.promises.open(filePath, 'w');
    console.log(`[WORKER] File opened: ${filePath}`);
  } catch (err) {
    const msg = `Failed to open file ${filePath}: ${err.message}`;
    console.error('[WORKER]', msg);
    parentPort.postMessage({ ok: false, error: msg, filePath });
    return;
  }

  // ── Write chunks ─────────────────────────────────────────────────────────────

  try {
    const totalChunks = Math.ceil(records.length / CHUNK_SIZE);

    for (let i = 0; i < records.length; i += CHUNK_SIZE) {
      const slice    = records.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i / CHUNK_SIZE) + 1;

      // Sanity check: warn if critical JSON fields are unexpectedly empty
      if (type === 'events' && slice[0] && !slice[0].rawJson && slice[0].id) {
        console.warn(
          `[WORKER][SANITY] Chunk ${chunkNum}: event records have empty rawJson — data may be incomplete`,
        );
      }

      // ── Create protobuf records ─────────────────────────────────────────────
      // FIX #2: throw on failure instead of continue — no silent data loss
      const protoRecords = [];
      for (let j = 0; j < slice.length; j++) {
        let created;
        try {
          created = RecordType.create(slice[j]);
        } catch (createErr) {
          const recordStr = JSON.stringify(slice[j]).substring(0, 200);
          throw new Error(
            `Chunk ${chunkNum}: failed to create protobuf record ${i + j}: ` +
            `${createErr.message}. Record: ${recordStr}`,
          );
        }
        protoRecords.push(created);
      }

      // ── Encode batch ────────────────────────────────────────────────────────
      let message;
      try {
        message = FileBatchType.create({ [fieldName]: protoRecords });
      } catch (err) {
        throw new Error(`Chunk ${chunkNum}: failed to create batch: ${err.message}`);
      }

      let encoded;
      try {
        encoded = FileBatchType.encode(message).finish();
        originalSize += encoded.length;
      } catch (err) {
        throw new Error(`Chunk ${chunkNum}: failed to encode batch: ${err.message}`);
      }

      // ── Compress ────────────────────────────────────────────────────────────
      let compressed;
      try {
        compressed = await compress(Buffer.from(encoded), zstdLevel);
        compressedSize += compressed.length;
      } catch (err) {
        throw new Error(`Chunk ${chunkNum}: failed to compress: ${err.message}`);
      }

      // ── Write (length prefix + data) ────────────────────────────────────────
      try {
        const lengthBuf = Buffer.alloc(4);
        lengthBuf.writeUInt32BE(compressed.length, 0);
        await fd.write(lengthBuf);
        await fd.write(compressed);
      } catch (err) {
        throw new Error(`Chunk ${chunkNum}: failed to write to disk: ${err.message}`);
      }

      chunksWritten++;
      // FIX #3: only count records after their chunk fully writes to disk
      recordsWritten += slice.length;

      if (chunkNum % 5 === 0 || chunkNum === totalChunks) {
        console.log(`[WORKER] Progress: ${chunkNum}/${totalChunks} chunks written`);
      }
    }
  } catch (err) {
    // FIX #2: any chunk failure surfaces here — close fd, clean up, report error
    console.error(`[WORKER] Write failed: ${err.message}`);
    try { await fd.close(); } catch {}
    try { fs.unlinkSync(filePath); } catch {}
    parentPort.postMessage({ ok: false, error: err.message, filePath });
    return;
  }

  // ── Close file ───────────────────────────────────────────────────────────────

  try {
    await fd.close();
    console.log(`[WORKER] File closed: ${filePath}`);
  } catch (err) {
    // Close failure after successful writes — file data is intact but handle leaked
    console.error(`[WORKER] Failed to close file: ${err.message}`);
    parentPort.postMessage({
      ok: false,
      error: `File close failed: ${err.message}`,
      filePath,
    });
    return;
  }

  // ── Report success ───────────────────────────────────────────────────────────
  // FIX #4: no process.exit(0) — postMessage is async; let the event loop drain
  // naturally so the message is guaranteed to reach binary-writer-pool.js before
  // the worker thread terminates.

  console.log(
    `[WORKER] Complete: ${recordsWritten} records, ${chunksWritten} chunks, ` +
    `${(compressedSize / 1024).toFixed(1)}KB`,
  );

  parentPort.postMessage({
    ok: true,
    filePath,
    // FIX #3: actual records written (incremented per successful chunk)
    count: recordsWritten,
    originalSize,
    compressedSize,
    chunksWritten,
  });

  // FIX #4: worker thread exits naturally when run() resolves and the event
  // loop is empty — no explicit process.exit(0) needed or safe here.
}

// ── Entry point ───────────────────────────────────────────────────────────────

run().catch(err => {
  console.error('[WORKER] Fatal error in run():', err.message);
  console.error(err.stack);
  parentPort.postMessage({
    ok: false,
    error: `Worker fatal: ${err.message}`,
    filePath,
  });
  // process.exit(1) is appropriate here — run() itself threw unexpectedly,
  // meaning we may not have sent a message, so the pool's 'exit' handler
  // will fire with code 1 and increment failedJobs correctly.
  process.exit(1);
});
