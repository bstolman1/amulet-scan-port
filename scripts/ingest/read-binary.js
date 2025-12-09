#!/usr/bin/env node
/**
 * Binary Reader Utility
 * 
 * Decodes .pb.zst files (Protobuf + ZSTD) back to JSON for:
 * - Debugging and inspection
 * - DuckDB ingestion
 * - Data validation
 * 
 * Usage:
 *   node read-binary.js <file.pb.zst>              # Print to stdout as JSON
 *   node read-binary.js <file.pb.zst> --jsonl      # Print as JSON lines
 *   node read-binary.js <file.pb.zst> -o out.json  # Write to file
 *   node read-binary.js <dir> --convert            # Convert all .pb.zst to .jsonl
 */

import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import zstd from '@mongodb-js/zstd';
import { getEncoders } from './encoding.js';

/**
 * Decompress ZSTD buffer
 */
async function decompressZstd(compressedBuffer) {
  return await zstd.decompress(compressedBuffer);
}

/**
 * Read and decode a single .pb.zst file
 */
export async function readBinaryFile(filePath) {
  const { EventBatch, UpdateBatch } = await getEncoders();
  
  // Read compressed file
  const compressed = fs.readFileSync(filePath);
  
  // Decompress
  const buffer = await decompressZstd(compressed);
  
  // Determine type from filename
  const basename = path.basename(filePath);
  const isEvents = basename.startsWith('events-');
  const isUpdates = basename.startsWith('updates-');
  
  if (!isEvents && !isUpdates) {
    throw new Error(`Cannot determine type from filename: ${basename}. Expected 'events-*.pb.zst' or 'updates-*.pb.zst'`);
  }
  
  const BatchType = isEvents ? EventBatch : UpdateBatch;
  const recordKey = isEvents ? 'events' : 'updates';
  
  // Decode protobuf
  const message = BatchType.decode(buffer);
  const records = message[recordKey] || [];
  
  // Convert to plain objects with proper types
  return {
    type: recordKey,
    count: records.length,
    originalSize: buffer.length,
    compressedSize: compressed.length,
    compressionRatio: ((compressed.length / buffer.length) * 100).toFixed(1) + '%',
    records: records.map(r => toPlainObject(r, isEvents))
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
    type: result.type
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
          console.log(`✅ Converted: ${entry.name} (${result.count} ${result.type})`);
          
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
 * Get stats for a .pb.zst file without fully decoding
 */
export async function getFileStats(filePath) {
  const result = await readBinaryFile(filePath);
  return {
    file: path.basename(filePath),
    type: result.type,
    count: result.count,
    originalSize: formatBytes(result.originalSize),
    compressedSize: formatBytes(result.compressedSize),
    compressionRatio: result.compressionRatio
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
if (process.argv[1] && process.argv[1].endsWith('read-binary.js')) {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log(`
Binary Reader Utility - Decode .pb.zst files

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
            console.log(`✅ Written to ${outputPath} (${result.count} records)`);
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
