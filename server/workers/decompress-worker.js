/**
 * Decompress Worker
 * 
 * A worker thread that handles ZSTD decompression and protobuf decoding in parallel.
 * This enables true parallelism for CPU-bound operations across multiple cores.
 */

import { parentPort, workerData } from 'worker_threads';
import fs from 'fs';
import protobuf from 'protobufjs';
import { decompress } from '@mongodb-js/zstd';

// Proto schema definition - MUST MATCH binary-reader.js exactly!
const PROTO_SCHEMA = `
syntax = "proto3";
package ledger;

message Event {
  string id = 1;
  string update_id = 2;
  string type = 3;
  string synchronizer = 4;

  int64 effective_at = 5;
  int64 recorded_at = 6;
  int64 created_at_ts = 7;

  string contract_id = 8;
  string template = 9;
  string package_name = 10;
  int64 migration_id = 11;

  repeated string signatories = 12;
  repeated string observers = 13;
  repeated string acting_parties = 14;
  repeated string witness_parties = 15;

  string payload_json = 16;
  
  string contract_key_json = 17;
  
  string choice = 18;
  bool consuming = 19;
  string interface_id = 20;
  repeated string child_event_ids = 21;
  string exercise_result_json = 22;
  
  string source_synchronizer = 23;
  string target_synchronizer = 24;
  string unassign_id = 25;
  string submitter = 26;
  int64 reassignment_counter = 27;
  
  string raw_json = 28;
  
  string party = 29;
  
  string type_original = 30;
}

message EventBatch {
  repeated Event events = 1;
}
`;

let EventBatch = null;

async function initProto() {
  if (!EventBatch) {
    const root = protobuf.parse(PROTO_SCHEMA).root;
    EventBatch = root.lookupType('ledger.EventBatch');
  }
  return EventBatch;
}

function tryParseJson(str) {
  if (!str) return null;
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
}

function extractMigrationIdFromPath(filePath) {
  const m = filePath.match(/migration=(\d+)/);
  return m ? parseInt(m[1], 10) : null;
}

function toLong(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'number') return val;
  if (typeof val === 'object' && 'low' in val) {
    return val.toNumber ? val.toNumber() : Number(val.low);
  }
  return Number(val);
}

/**
 * Process a single file: read, decompress, decode, and filter for VoteRequest events
 */
async function processFile(filePath, filterTemplate = null) {
  const BatchType = await initProto();
  const pathMigrationId = extractMigrationIdFromPath(filePath);
  
  const fileBuffer = await fs.promises.readFile(filePath);
  const events = [];
  let offset = 0;
  
  // Read chunks: [4-byte length][compressed data]...
  while (offset < fileBuffer.length) {
    if (offset + 4 > fileBuffer.length) break;
    
    const chunkLength = fileBuffer.readUInt32BE(offset);
    offset += 4;
    
    if (offset + chunkLength > fileBuffer.length) break;
    
    const compressedChunk = fileBuffer.subarray(offset, offset + chunkLength);
    offset += chunkLength;
    
    // Decompress chunk
    const decompressed = await decompress(compressedChunk);
    
    // Decode protobuf
    const message = BatchType.decode(decompressed);
    const records = message.events || [];
    
    for (const r of records) {
      const templateId = r.template || '';
      const eventType = r.type || '';
      
      // If filter is specified, only include matching events
      if (filterTemplate) {
        if (!templateId.includes(filterTemplate)) continue;
        if (eventType !== 'created') continue;
      }
      
      // Convert to plain object
      const migrationId = toLong(r.migrationId ?? r.migration_id) ?? pathMigrationId;
      
      events.push({
        event_id: r.id || null,
        update_id: r.updateId || r.update_id || null,
        event_type: r.type || null,
        synchronizer_id: r.synchronizer || null,
        migration_id: migrationId,
        timestamp: r.recordedAt ? new Date(Number(r.recordedAt)).toISOString() : null,
        effective_at: r.effectiveAt ? new Date(Number(r.effectiveAt)).toISOString() : null,
        contract_id: r.contractId || r.contract_id || null,
        template_id: templateId,
        payload: r.payloadJson ? tryParseJson(r.payloadJson) : null,
        signatories: r.signatories || [],
        observers: r.observers || [],
        acting_parties: r.actingParties || r.acting_parties || [],
      });
    }
  }
  
  return events;
}

// Handle messages from main thread
parentPort.on('message', async (msg) => {
  const { id, filePath, filterTemplate } = msg;
  
  try {
    const events = await processFile(filePath, filterTemplate);
    parentPort.postMessage({ id, success: true, events, filePath });
  } catch (err) {
    parentPort.postMessage({ id, success: false, error: err.message, filePath, events: [] });
  }
});

// Signal ready
parentPort.postMessage({ ready: true });
