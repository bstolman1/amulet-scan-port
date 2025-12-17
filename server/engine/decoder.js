/**
 * Decoder - Streaming decoder for .pb.zst files
 * 
 * Uses the same protobuf schema as the backfill writers.
 * STREAMING-ONLY: Yields records one at a time to avoid memory issues.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { decompress } from '@mongodb-js/zstd';
import protobuf from 'protobufjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Schema path relative to this file
const SCHEMA_PATH = path.resolve(__dirname, '../../scripts/ingest/schema/ledger.proto');

let cachedRoot = null;

/**
 * Load protobuf schema
 */
async function getRoot() {
  if (!cachedRoot) {
    cachedRoot = await protobuf.load(SCHEMA_PATH);
  }
  return cachedRoot;
}

/**
 * Get batch decoders
 */
async function getDecoders() {
  const root = await getRoot();
  return {
    EventBatch: root.lookupType('ledger.EventBatch'),
    UpdateBatch: root.lookupType('ledger.UpdateBatch'),
  };
}

/**
 * Convert protobuf event to plain object
 */
function eventToPlain(record) {
  return {
    id: record.id || null,
    update_id: record.updateId || null,
    type: record.type || null,
    type_original: record.typeOriginal || null,
    synchronizer: record.synchronizer || null,
    effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
    recorded_at: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
    created_at_ts: record.createdAtTs ? new Date(Number(record.createdAtTs)).toISOString() : null,
    contract_id: record.contractId || null,
    party: record.party || null,
    template: record.template || null,
    package_name: record.packageName || null,
    migration_id: record.migrationId || null,
    signatories: record.signatories || [],
    observers: record.observers || [],
    acting_parties: record.actingParties || [],
    witness_parties: record.witnessParties || [],
    payload: record.payloadJson ? tryParseJson(record.payloadJson) : null,
    contract_key: record.contractKeyJson ? tryParseJson(record.contractKeyJson) : null,
    // Exercised event fields
    choice: record.choice || null,
    consuming: record.consuming ?? null,
    interface_id: record.interfaceId || null,
    child_event_ids: record.childEventIds || [],
    exercise_result: record.exerciseResultJson ? tryParseJson(record.exerciseResultJson) : null,
    // Reassignment fields
    source_synchronizer: record.sourceSynchronizer || null,
    target_synchronizer: record.targetSynchronizer || null,
    unassign_id: record.unassignId || null,
    submitter: record.submitter || null,
    reassignment_counter: record.reassignmentCounter || null,
    raw_json: record.rawJson ? tryParseJson(record.rawJson) : null,
  };
}

/**
 * Convert protobuf update to plain object
 */
function updateToPlain(record) {
  return {
    id: record.id || null,
    synchronizer: record.synchronizer || null,
    effective_at: record.effectiveAt ? new Date(Number(record.effectiveAt)).toISOString() : null,
    recorded_at: record.recordedAt ? new Date(Number(record.recordedAt)).toISOString() : null,
    type: record.type || null,
    command_id: record.commandId || null,
    workflow_id: record.workflowId || null,
    kind: record.kind || null,
    migration_id: record.migrationId ? Number(record.migrationId) : null,
    offset_val: record.offset ? Number(record.offset) : null,
    event_count: record.eventCount || 0,
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
 * Streaming decode a .pb.zst file - yields records one at a time
 * 
 * File format: sequence of [4-byte BE length][zstd-compressed protobuf batch]
 */
export async function* decodeFile(filePath) {
  const { EventBatch, UpdateBatch } = await getDecoders();
  
  const basename = path.basename(filePath);
  const isEvents = basename.startsWith('events-');
  const BatchType = isEvents ? EventBatch : UpdateBatch;
  const recordKey = isEvents ? 'events' : 'updates';
  const toPlain = isEvents ? eventToPlain : updateToPlain;
  
  // Read file in chunks to avoid loading entire file
  const fd = fs.openSync(filePath, 'r');
  const fileSize = fs.fstatSync(fd).size;
  let offset = 0;
  
  try {
    while (offset < fileSize) {
      // Read 4-byte length header
      const lenBuf = Buffer.alloc(4);
      const lenRead = fs.readSync(fd, lenBuf, 0, 4, offset);
      if (lenRead < 4) break;
      
      const chunkLength = lenBuf.readUInt32BE(0);
      offset += 4;
      
      if (offset + chunkLength > fileSize) break;
      
      // Read compressed chunk
      const compressedChunk = Buffer.alloc(chunkLength);
      fs.readSync(fd, compressedChunk, 0, chunkLength, offset);
      offset += chunkLength;
      
      // Decompress and decode batch
      const decompressed = await decompress(compressedChunk);
      const message = BatchType.decode(decompressed);
      const records = message[recordKey] || [];
      
      // Yield records one at a time
      for (const r of records) {
        yield toPlain(r);
      }
    }
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Streaming decode with stats tracking - yields { record, stats } 
 * Stats are updated incrementally as records are yielded
 */
export async function* decodeFileStreaming(filePath) {
  let count = 0;
  let minTs = null;
  let maxTs = null;
  
  for await (const record of decodeFile(filePath)) {
    count++;
    
    const ts = record.recorded_at || record.effective_at;
    if (ts) {
      const d = new Date(ts);
      if (!minTs || d < minTs) minTs = d;
      if (!maxTs || d > maxTs) maxTs = d;
    }
    
    yield {
      record,
      stats: { count, minTs, maxTs }
    };
  }
}

/**
 * Get file type from path
 */
export function getFileType(filePath) {
  const basename = path.basename(filePath);
  if (basename.startsWith('events-')) return 'events';
  if (basename.startsWith('updates-')) return 'updates';
  return null;
}
