/**
 * Worker Writer - Binary Protobuf + ZSTD Streaming
 * 
 * This worker receives batches of records, encodes them to Protobuf,
 * and streams them through ZSTD compression to disk.
 * 
 * CHUNKED APPROACH:
 * - Encode 2000 records at a time (never a giant single message)
 * - Compress each chunk separately
 * - Write chunks sequentially with length prefixes
 * - No worker ever hits memory ceiling
 */

// Capture ALL errors - even during import
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
  type,           // "events" | "updates"
  filePath,       // output file path
  records,        // array of plain JS objects
  zstdLevel = 1,  // compression level
  chunkSize = 2000 // records per chunk - 2000 is safe default
} = workerData;

console.log(`[WORKER] Job received: type=${type}, records=${records?.length || 0}, file=${filePath}`);

// Validate inputs
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
  parentPort.postMessage({ ok: true, filePath, count: 0, originalSize: 0, compressedSize: 0, chunksWritten: 0 });
  process.exit(0);
}

const CHUNK_SIZE = Math.min(chunkSize, 2000); // Cap at 2000 for safety

async function run() {
  let originalSize = 0;
  let compressedSize = 0;
  let chunksWritten = 0;
  
  // Dynamic imports with error handling
  console.log('[WORKER] Loading dependencies...');
  
  let compress, getEncoders, mapEvent, mapUpdate;
  
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
  
  try {
    const encodingModule = await import('./encoding.js');
    getEncoders = encodingModule.getEncoders;
    mapEvent = encodingModule.mapEvent;
    mapUpdate = encodingModule.mapUpdate;
    console.log('[WORKER] Encoding module loaded');
  } catch (err) {
    const msg = `Failed to load encoding.js: ${err.message}`;
    console.error('[WORKER]', msg);
    parentPort.postMessage({ ok: false, error: msg, filePath });
    return;
  }
  
  let Event, Update, EventBatch, UpdateBatch;
  try {
    const encoders = await getEncoders();
    Event = encoders.Event;
    Update = encoders.Update;
    EventBatch = encoders.EventBatch;
    UpdateBatch = encoders.UpdateBatch;
    console.log('[WORKER] Protobuf schema loaded');
  } catch (err) {
    const msg = `Failed to load protobuf schema: ${err.message}`;
    console.error('[WORKER]', msg);
    parentPort.postMessage({ ok: false, error: msg, filePath });
    return;
  }

  const FileBatchType = type === 'events' ? EventBatch : UpdateBatch;
  const RecordType = type === 'events' ? Event : Update;
  const mapFn = type === 'events' ? mapEvent : mapUpdate;
  const fieldName = type === 'events' ? 'events' : 'updates';

  // Open file handle for sequential writes
  let fd;
  try {
    // Ensure directory exists - use path module for cross-platform support
    const path = await import('node:path');
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
  
  try {
    // Process records in chunks - never build giant buffers
    for (let i = 0; i < records.length; i += CHUNK_SIZE) {
      const slice = records.slice(i, Math.min(i + CHUNK_SIZE, records.length));
      const chunkNum = Math.floor(i / CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(records.length / CHUNK_SIZE);
      
      // Map records with individual error handling
      const mappedRecords = [];
      for (let j = 0; j < slice.length; j++) {
        try {
          const mapped = mapFn(slice[j]);
          const created = RecordType.create(mapped);
          mappedRecords.push(created);
        } catch (mapErr) {
          console.error(`[WORKER] Skipping record ${i + j}: ${mapErr.message}`);
          // Log the problematic record (truncated)
          const recordStr = JSON.stringify(slice[j]).substring(0, 200);
          console.error(`[WORKER] Problematic record: ${recordStr}...`);
        }
      }
      
      if (mappedRecords.length === 0) {
        console.warn(`[WORKER] Chunk ${chunkNum}/${totalChunks}: all records skipped`);
        continue;
      }
      
      // Create batch message
      let message;
      try {
        message = FileBatchType.create({ [fieldName]: mappedRecords });
      } catch (err) {
        console.error(`[WORKER] Chunk ${chunkNum}: Failed to create batch: ${err.message}`);
        continue;
      }
      
      // Encode to protobuf buffer
      let encoded;
      try {
        encoded = FileBatchType.encode(message).finish();
        originalSize += encoded.length;
      } catch (err) {
        console.error(`[WORKER] Chunk ${chunkNum}: Failed to encode: ${err.message}`);
        continue;
      }
      
      // Compress this chunk
      let compressed;
      try {
        compressed = await compress(Buffer.from(encoded), zstdLevel);
        compressedSize += compressed.length;
      } catch (err) {
        console.error(`[WORKER] Chunk ${chunkNum}: Failed to compress: ${err.message}`);
        continue;
      }
      
      // Write length prefix (4 bytes, big endian) + compressed chunk
      try {
        const lengthBuf = Buffer.alloc(4);
        lengthBuf.writeUInt32BE(compressed.length, 0);
        
        await fd.write(lengthBuf);
        await fd.write(compressed);
        chunksWritten++;
        
        if (chunkNum % 5 === 0 || chunkNum === totalChunks) {
          console.log(`[WORKER] Progress: ${chunkNum}/${totalChunks} chunks written`);
        }
      } catch (err) {
        console.error(`[WORKER] Chunk ${chunkNum}: Failed to write: ${err.message}`);
        continue;
      }
    }
  } finally {
    try {
      await fd.close();
      console.log(`[WORKER] File closed: ${filePath}`);
    } catch (err) {
      console.error(`[WORKER] Failed to close file: ${err.message}`);
    }
  }

  // Report success
  const result = {
    ok: true,
    filePath,
    count: records.length,
    originalSize,
    compressedSize,
    chunksWritten,
  };
  
  console.log(`[WORKER] Complete: ${records.length} records, ${chunksWritten} chunks, ${(compressedSize/1024).toFixed(1)}KB`);
  parentPort.postMessage(result);
  
  // Explicitly exit with success code
  process.exit(0);
}

// Run with top-level error handling
run().catch(err => {
  console.error('[WORKER] Fatal error in run():', err.message);
  console.error(err.stack);
  parentPort.postMessage({
    ok: false,
    error: `Worker fatal: ${err.message}`,
    filePath,
  });
  process.exit(1);
});
