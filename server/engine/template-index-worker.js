/**
 * Template Index Worker - Processes binary files in parallel for template extraction
 * 
 * This worker receives a list of file paths, reads each file, extracts template
 * information, and returns the results to the main thread.
 */

import { parentPort, workerData } from 'worker_threads';
import fs from 'fs';
import path from 'path';
import protobuf from 'protobufjs';
import { decompress } from '@mongodb-js/zstd';

// Proto schema (same as binary-reader.js)
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
}

/**
 * Extract simple template name from full template_id
 */
function extractTemplateName(templateId) {
  if (!templateId) return null;
  const parts = templateId.split(':');
  return parts[parts.length - 1];
}

/**
 * Read a single binary file and extract template stats
 */
async function processFile(filePath) {
  try {
    const fileBuffer = fs.readFileSync(filePath);
    const templateStats = new Map();
    let offset = 0;
    
    while (offset < fileBuffer.length) {
      if (offset + 4 > fileBuffer.length) break;
      
      const chunkLength = fileBuffer.readUInt32BE(offset);
      offset += 4;
      
      if (offset + chunkLength > fileBuffer.length) break;
      
      const compressedChunk = fileBuffer.subarray(offset, offset + chunkLength);
      offset += chunkLength;
      
      const decompressed = await decompress(compressedChunk);
      const message = EventBatch.decode(decompressed);
      const events = message.events || [];
      
      for (const event of events) {
        const templateName = extractTemplateName(event.template);
        if (!templateName) continue;
        
        const effectiveAt = event.effectiveAt 
          ? new Date(Number(event.effectiveAt)) 
          : null;
        
        if (!templateStats.has(templateName)) {
          templateStats.set(templateName, {
            count: 0,
            firstEventAt: effectiveAt,
            lastEventAt: effectiveAt,
          });
        }
        
        const stats = templateStats.get(templateName);
        stats.count++;
        if (effectiveAt) {
          if (!stats.firstEventAt || effectiveAt < stats.firstEventAt) {
            stats.firstEventAt = effectiveAt;
          }
          if (!stats.lastEventAt || effectiveAt > stats.lastEventAt) {
            stats.lastEventAt = effectiveAt;
          }
        }
      }
    }
    
    // Convert Map to array of results
    const results = [];
    for (const [templateName, stats] of templateStats) {
      results.push({
        file_path: filePath,
        template_name: templateName,
        event_count: stats.count,
        first_event_at: stats.firstEventAt?.toISOString() || null,
        last_event_at: stats.lastEventAt?.toISOString() || null,
      });
    }
    
    return { success: true, results };
  } catch (err) {
    return { success: false, error: err.message, file: filePath };
  }
}

// Handle messages from main thread
parentPort.on('message', async (message) => {
  const { id, files } = message;
  
  try {
    await initProto();
    
    const allResults = [];
    let processed = 0;
    let errors = 0;
    
    for (const filePath of files) {
      const result = await processFile(filePath);
      if (result.success) {
        allResults.push(...result.results);
      } else {
        errors++;
      }
      processed++;
    }
    
    parentPort.postMessage({
      id,
      success: true,
      results: allResults,
      processed,
      errors,
    });
  } catch (err) {
    parentPort.postMessage({
      id,
      success: false,
      error: err.message,
    });
  }
});
