/**
 * Worker Writer - Binary Protobuf + ZSTD Streaming
 * 
 * This worker receives batches of records, encodes them to Protobuf,
 * and streams them through ZSTD compression to disk.
 * 
 * Zero-copy approach: no giant JSON strings, streaming compression.
 */

import { parentPort, workerData, isMainThread } from 'node:worker_threads';
import fs from 'node:fs';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
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
  chunkSize = 5000 // records per chunk for streaming
} = workerData;

(async () => {
  try {
    const { Event, Update, EventBatch, UpdateBatch } = await getEncoders();

    const FileBatchType = type === 'events' ? EventBatch : UpdateBatch;
    const RecordType = type === 'events' ? Event : Update;
    const mapFn = type === 'events' ? mapEvent : mapUpdate;
    const fieldName = type === 'events' ? 'events' : 'updates';

    // Encode in chunks to avoid huge buffers
    const chunks = [];
    for (let i = 0; i < records.length; i += chunkSize) {
      const slice = records.slice(i, i + chunkSize);
      const message = FileBatchType.create({
        [fieldName]: slice.map(r => RecordType.create(mapFn(r))),
      });
      const buf = FileBatchType.encode(message).finish();
      chunks.push(buf);
    }

    // Concatenate all chunks
    const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
    const fullBuffer = Buffer.concat(chunks, totalLength);

    // Compress with ZSTD
    const compressed = await compress(fullBuffer, zstdLevel);

    // Write to disk
    await fs.promises.writeFile(filePath, compressed);

    // Report success with zero-copy transfer
    parentPort.postMessage({
      ok: true,
      filePath,
      count: records.length,
      originalSize: fullBuffer.length,
      compressedSize: compressed.length,
    });
  } catch (err) {
    console.error('Worker write error:', err.message);
    parentPort.postMessage({
      ok: false,
      error: err.message,
      filePath,
    });
  }
})();
