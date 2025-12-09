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

import { parentPort, workerData, isMainThread } from 'node:worker_threads';
import fs from 'node:fs';
import { compress } from '@mongodb-js/zstd';
import { getEncoders, mapEvent, mapUpdate } from './encoding.js';

if (isMainThread) {
  throw new Error('worker-writer.js must be run as a Worker, not directly');
}

const {
  type,           // "events" | "updates"
  filePath,       // output file path
  records,        // array of plain JS objects
  zstdLevel = 1,  // compression level
  chunkSize = 2000 // records per chunk - 2000 is safe default
} = workerData;

const CHUNK_SIZE = Math.min(chunkSize, 2000); // Cap at 2000 for safety

(async () => {
  let originalSize = 0;
  let compressedSize = 0;
  
  try {
    const { Event, Update, EventBatch, UpdateBatch } = await getEncoders();

    const FileBatchType = type === 'events' ? EventBatch : UpdateBatch;
    const RecordType = type === 'events' ? Event : Update;
    const mapFn = type === 'events' ? mapEvent : mapUpdate;
    const fieldName = type === 'events' ? 'events' : 'updates';

    // Open file handle for sequential writes
    const fd = await fs.promises.open(filePath, 'w');
    
    try {
      // Process records in chunks - never build giant buffers
      for (let i = 0; i < records.length; i += CHUNK_SIZE) {
        const slice = records.slice(i, Math.min(i + CHUNK_SIZE, records.length));
        
        // Map and encode this chunk only
        const mappedRecords = [];
        for (const r of slice) {
          try {
            mappedRecords.push(RecordType.create(mapFn(r)));
          } catch (mapErr) {
            // Skip malformed records instead of crashing
            console.error(`Skipping malformed record: ${mapErr.message}`);
          }
        }
        
        if (mappedRecords.length === 0) continue;
        
        const message = FileBatchType.create({
          [fieldName]: mappedRecords,
        });
        
        // Encode to protobuf buffer
        const encoded = FileBatchType.encode(message).finish();
        originalSize += encoded.length;
        
        // Compress this chunk
        const compressed = await compress(Buffer.from(encoded), zstdLevel);
        compressedSize += compressed.length;
        
        // Write length prefix (4 bytes, big endian) + compressed chunk
        const lengthBuf = Buffer.alloc(4);
        lengthBuf.writeUInt32BE(compressed.length, 0);
        
        await fd.write(lengthBuf);
        await fd.write(compressed);
        
        // Allow GC to reclaim memory between chunks
        if (i % (CHUNK_SIZE * 5) === 0 && global.gc) {
          global.gc();
        }
      }
    } finally {
      await fd.close();
    }

    // Report success
    parentPort.postMessage({
      ok: true,
      filePath,
      count: records.length,
      originalSize,
      compressedSize,
      chunksWritten: Math.ceil(records.length / CHUNK_SIZE),
    });
  } catch (err) {
    console.error('Worker write error:', err.message, err.stack);
    parentPort.postMessage({
      ok: false,
      error: err.message,
      filePath,
    });
  }
})();
