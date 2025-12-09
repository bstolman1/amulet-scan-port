#!/usr/bin/env node
/**
 * Binary Reader Utility
 * 
 * Decodes .pb.zst files (Protobuf + ZSTD with chunked format) back to JSON for:
 * - Debugging and inspection
 * - DuckDB ingestion
 * - Data validation
 * 
 * File format: sequence of [4-byte length][compressed chunk]...
 * Each chunk is a separate Protobuf batch compressed with ZSTD.
 * 
 * Usage:
 *   node read-binary.js <file.pb.zst>              # Print to stdout as JSON
 *   node read-binary.js <file.pb.zst> --jsonl      # Print as JSON lines
 *   node read-binary.js <file.pb.zst> -o out.json  # Write to file
 *   node read-binary.js <dir> --convert            # Convert all .pb.zst to .jsonl
 */

import fs from 'fs';
import path from 'path';
import { decompress } from '@mongodb-js/zstd';
import { getEncoders } from './encoding.js';

/**
 * Read and decode a single .pb.zst file (chunked format)
 */
export async function readBinaryFile(filePath) {
  const { EventBatch, UpdateBatch } = await getEncoders();
  
  // Determine type from filename
  const basename = path.basename(filePath);
  const isEvents = basename.startsWith('events-');
  const isUpdates = basename.startsWith('updates-');
  
  if (!isEvents && !isUpdates) {
    throw new Error(`Cannot determine type from filename: ${basename}. Expected 'events-*.pb.zst' or 'updates-*.pb.zst'`);
  }
  
  const BatchType = isEvents ? EventBatch : UpdateBatch;
  const recordKey = isEvents ? 'events' : 'updates';
  
  // Read file
  const fileBuffer = fs.readFileSync(filePath);
  const allRecords = [];
  let originalSize = 0;
  let offset = 0;
  let chunksRead = 0;
  
  // Read chunks: [4-byte length][compressed data]...
  while (offset < fileBuffer.length) {
    if (offset + 4 > fileBuffer.length) {
      console.warn(`Incomplete length header at offset ${offset}`);
      break;
    }
    
    const chunkLength = fileBuffer.readUInt32BE(offset);
    offset += 4;
    
    if (offset + chunkLength > fileBuffer.length) {
      console.warn(`Incomplete chunk at offset ${offset}, expected ${chunkLength} bytes`);
      break;
    }
    
    const compressedChunk = fileBuffer.subarray(offset, offset + chunkLength);
    offset += chunkLength;
    
    // Decompress chunk
    const decompressed = await decompress(compressedChunk);
    originalSize += decompressed.length;
    
    // Decode protobuf
    const message = BatchType.decode(decompressed);
    const records = message[recordKey] || [];
    
    for (const r of records) {
      allRecords.push(toPlainObject(r, isEvents));
    }
    
    chunksRead++;
  }
  
  return {
    type: recordKey,
    count: allRecords.length,
    chunksRead,
    originalSize,
    compressedSize: fileBuffer.length,
    compressionRatio: ((fileBuffer.length / originalSize) * 100).toFixed(1) + '%',
    records: allRecords
  };
}

/**
 * Convert protobuf message to plain object with readable timestamps
 */
function toPlainObject(record, isEvent) {
  if (isEvent) {
    return {
      id: record.id || null,
      update_id: record.updateId || record.update_id || null,
      type: record.type || null,
      synchronizer: record.synchronizer || null,
      effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
      recorded_at: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
      contract_id: record.contractId || record.contract_id || null,
      party: record.party || null,
      template: record.template || null,
      payload: record.payloadJson ? tryParseJson(record.payloadJson) : null,
      signatories: record.signatories || [],
      observers: record.observers || [],
      package_name: record.packageName || null,
      raw_json: record.rawJson ? tryParseJson(record.rawJson) : null,
    };
  }
  
  // Update record
  return {
    id: record.id || null,
    synchronizer: record.synchronizer || null,
    effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
    recorded_at: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
    transaction_id: record.transactionId || record.transaction_id || null,
    command_id: record.commandId || record.command_id || null,
    workflow_id: record.workflowId || record.workflow_id || null,
    status: record.status || null,
  };
}

function tryParseJson(str) {
  if (!str) return null;
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
}

/**
 * Convert a .pb.zst file to .jsonl format
 */
export async function convertToJsonl(inputPath, outputPath) {
  const result = await readBinaryFile(inputPath);
  
  const lines = result.records.map(r => JSON.stringify(r)).join('\n');
  fs.writeFileSync(outputPath, lines + '\n');
  
  return {
    input: inputPath,
    output: outputPath,
    count: result.count,
    type: result.type,
    chunksRead: result.chunksRead
  };
}

/**
 * Convert all .pb.zst files in a directory to .jsonl
 */
export async function convertDirectory(dirPath, options = {}) {
  const { recursive = true, deleteOriginal = false } = options;
  const results = [];
  
  async function processDir(dir) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory() && recursive) {
        await processDir(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.pb.zst')) {
        const outputPath = fullPath.replace('.pb.zst', '.jsonl');
        
        try {
          const result = await convertToJsonl(fullPath, outputPath);
          results.push(result);
          console.log(`✅ Converted: ${entry.name} (${result.count} ${result.type}, ${result.chunksRead} chunks)`);
          
          if (deleteOriginal) {
            fs.unlinkSync(fullPath);
            console.log(`   🗑️ Deleted original: ${entry.name}`);
          }
        } catch (err) {
          console.error(`❌ Failed: ${entry.name} - ${err.message}`);
          results.push({ input: fullPath, error: err.message });
        }
      }
    }
  }
  
  await processDir(dirPath);
  return results;
}

/**
 * Get stats for a .pb.zst file without fully decoding records
 */
export async function getFileStats(filePath) {
  const { EventBatch, UpdateBatch } = await getEncoders();
  
  const basename = path.basename(filePath);
  const isEvents = basename.startsWith('events-');
  const BatchType = isEvents ? EventBatch : UpdateBatch;
  const recordKey = isEvents ? 'events' : 'updates';
  
  const fileBuffer = fs.readFileSync(filePath);
  let offset = 0;
  let chunksRead = 0;
  let totalRecords = 0;
  let originalSize = 0;
  
  while (offset < fileBuffer.length) {
    if (offset + 4 > fileBuffer.length) break;
    
    const chunkLength = fileBuffer.readUInt32BE(offset);
    offset += 4;
    
    if (offset + chunkLength > fileBuffer.length) break;
    
    const compressedChunk = fileBuffer.subarray(offset, offset + chunkLength);
    offset += chunkLength;
    
    const decompressed = await decompress(compressedChunk);
    originalSize += decompressed.length;
    
    const message = BatchType.decode(decompressed);
    totalRecords += (message[recordKey] || []).length;
    chunksRead++;
  }
  
  return {
    file: basename,
    type: recordKey,
    count: totalRecords,
    chunks: chunksRead,
    originalSize: formatBytes(originalSize),
    compressedSize: formatBytes(fileBuffer.length),
    compressionRatio: ((fileBuffer.length / originalSize) * 100).toFixed(1) + '%'
  };
}

function formatBytes(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

/**
 * Stream records from a .pb.zst file (memory-efficient for large files)
 */
export async function* streamRecords(filePath) {
  const result = await readBinaryFile(filePath);
  for (const record of result.records) {
    yield record;
  }
}

// CLI interface
const scriptPath = process.argv[1];
if (scriptPath && (scriptPath.endsWith('read-binary.js') || scriptPath.includes('read-binary'))) {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log(`
Binary Reader Utility - Decode chunked .pb.zst files

Usage:
  node read-binary.js <file.pb.zst>              Print as JSON
  node read-binary.js <file.pb.zst> --jsonl      Print as JSON lines
  node read-binary.js <file.pb.zst> --stats      Show file stats only
  node read-binary.js <file.pb.zst> -o out.json  Write to file
  node read-binary.js <dir> --convert            Convert all .pb.zst to .jsonl
  node read-binary.js <dir> --convert --delete   Convert and delete originals
`);
    process.exit(0);
  }
  
  const inputPath = args[0];
  const hasJsonl = args.includes('--jsonl');
  const hasStats = args.includes('--stats');
  const hasConvert = args.includes('--convert');
  const hasDelete = args.includes('--delete');
  const outputIdx = args.indexOf('-o');
  const outputPath = outputIdx >= 0 ? args[outputIdx + 1] : null;
  
  (async () => {
    try {
      const stat = fs.statSync(inputPath);
      
      if (stat.isDirectory()) {
        if (hasConvert) {
          console.log(`\n🔄 Converting .pb.zst files in ${inputPath}...\n`);
          const results = await convertDirectory(inputPath, { deleteOriginal: hasDelete });
          console.log(`\n✅ Converted ${results.filter(r => !r.error).length} files`);
          if (results.some(r => r.error)) {
            console.log(`❌ Failed: ${results.filter(r => r.error).length} files`);
          }
        } else {
          console.log('Use --convert flag to convert directory');
        }
      } else {
        if (hasStats) {
          const stats = await getFileStats(inputPath);
          console.log(JSON.stringify(stats, null, 2));
        } else {
          const result = await readBinaryFile(inputPath);
          
          let output;
          if (hasJsonl) {
            output = result.records.map(r => JSON.stringify(r)).join('\n');
          } else {
            output = JSON.stringify(result, null, 2);
          }
          
          if (outputPath) {
            fs.writeFileSync(outputPath, output + '\n');
            console.log(`✅ Written to ${outputPath} (${result.count} records, ${result.chunksRead} chunks)`);
          } else {
            console.log(output);
          }
        }
      }
    } catch (err) {
      console.error('❌ Error:', err.message);
      process.exit(1);
    }
  })();
}

export default {
  readBinaryFile,
  convertToJsonl,
  convertDirectory,
  getFileStats,
  streamRecords
};
