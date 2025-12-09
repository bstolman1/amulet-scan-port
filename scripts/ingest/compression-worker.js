/**
 * Compression Worker
 * 
 * Runs in a separate thread to handle CPU-intensive gzip compression
 * without blocking the main event loop.
 */

import { parentPort, workerData } from 'worker_threads';
import { gzipSync } from 'zlib';

// Worker receives messages with rows to compress
parentPort.on('message', (task) => {
  try {
    const { id, rows, level } = task;
    
    // Build JSONL content
    let content = '';
    for (const row of rows) {
      content += JSON.stringify(row) + '\n';
    }
    
    // Compress with gzip (sync is fine in worker - doesn't block main thread)
    const compressed = gzipSync(content, { level: level || 1 });
    
    // Send back compressed buffer
    parentPort.postMessage({
      id,
      success: true,
      data: compressed,
      originalRows: rows.length
    });
  } catch (err) {
    parentPort.postMessage({
      id: task.id,
      success: false,
      error: err.message
    });
  }
});
