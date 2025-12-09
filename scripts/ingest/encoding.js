/**
 * Protobuf Encoding for Ledger Records
 * 
 * Provides binary encoding for events and updates using Protocol Buffers.
 * Much more efficient than JSON for large batches.
 */

import path from 'node:path';
import { fileURLToPath } from 'node:url';
import protobuf from 'protobufjs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const schemaPath = path.join(__dirname, 'schema', 'ledger.proto');

let rootPromise = null;

async function getRoot() {
  if (!rootPromise) {
    rootPromise = protobuf.load(schemaPath);
  }
  return rootPromise;
}

export async function getEncoders() {
  const root = await getRoot();
  const Event = root.lookupType('ledger.Event');
  const Update = root.lookupType('ledger.Update');
  const EventBatch = root.lookupType('ledger.EventBatch');
  const UpdateBatch = root.lookupType('ledger.UpdateBatch');

  return { Event, Update, EventBatch, UpdateBatch };
}

/**
 * Map a raw event object to protobuf-compatible format
 * NOTE: protobufjs uses camelCase field names internally
 */
export function mapEvent(r) {
  return {
    id: String(r.id ?? r.event_id ?? ''),
    updateId: String(r.update_id ?? ''),
    type: String(r.type ?? r.event_type ?? ''),
    synchronizer: String(r.synchronizer ?? r.synchronizer_id ?? ''),
    effectiveAt: safeTimestamp(r.effective_at),
    recordedAt: safeTimestamp(r.recorded_at ?? r.timestamp),
    createdAtTs: safeTimestamp(r.created_at_ts),
    contractId: String(r.contract_id ?? ''),
    template: String(r.template ?? r.template_id ?? ''),
    packageName: String(r.package_name ?? ''),
    migrationId: safeInt64(r.migration_id),
    signatories: safeStringArray(r.signatories),
    observers: safeStringArray(r.observers),
    actingParties: safeStringArray(r.acting_parties),
    payloadJson: r.payload_json || (r.payload ? safeStringify(r.payload) : ''),
    // Exercised event specific fields
    choice: String(r.choice ?? ''),
    consuming: Boolean(r.consuming ?? false),
    interfaceId: String(r.interface_id ?? ''),
    childEventIds: safeStringArray(r.child_event_ids),
    exerciseResultJson: r.exercise_result_json || (r.exercise_result ? safeStringify(r.exercise_result) : ''),
    // Reassignment event specific fields
    sourceSynchronizer: String(r.source_synchronizer ?? ''),
    targetSynchronizer: String(r.target_synchronizer ?? ''),
    unassignId: String(r.unassign_id ?? ''),
    submitter: String(r.submitter ?? ''),
    reassignmentCounter: safeInt64(r.reassignment_counter),
    rawJson: r.raw_json || (r.raw ? safeStringify(r.raw) : ''),
    // Deprecated
    party: String(r.party ?? ''),
  };
}

/**
 * Map a raw update object to protobuf-compatible format
 * NOTE: protobufjs uses camelCase field names internally
 */
export function mapUpdate(r) {
  return {
    id: String(r.id ?? r.update_id ?? ''),
    type: String(r.type ?? r.update_type ?? ''),
    synchronizer: String(r.synchronizer ?? r.synchronizer_id ?? ''),
    effectiveAt: safeTimestamp(r.effective_at),
    recordedAt: safeTimestamp(r.recorded_at ?? r.timestamp),
    recordTime: safeTimestamp(r.record_time),
    commandId: String(r.command_id ?? ''),
    workflowId: String(r.workflow_id ?? ''),
    kind: String(r.kind ?? ''),
    migrationId: safeInt64(r.migration_id),
    offset: safeInt64(r.offset),
    rootEventIds: safeStringArray(r.root_event_ids),
    eventCount: parseInt(r.event_count) || 0,
    updateDataJson: r.update_data_json || (r.update_data ? safeStringify(r.update_data) : ''),
  };
}

/**
 * Safely convert to timestamp (int64)
 */
function safeTimestamp(value) {
  if (!value) return 0;
  if (typeof value === 'number') return value;
  try {
    const ts = new Date(value).getTime();
    return isNaN(ts) ? 0 : ts;
  } catch {
    return 0;
  }
}

/**
 * Safely convert to int64
 */
function safeInt64(value) {
  if (!value) return 0;
  const num = parseInt(value);
  return isNaN(num) ? 0 : num;
}

/**
 * Safely convert to string array
 */
function safeStringArray(arr) {
  if (!Array.isArray(arr)) return [];
  return arr.map(String);
}

/**
 * Safely stringify payload
 */
function safeStringify(obj) {
  try {
    return typeof obj === 'string' ? obj : JSON.stringify(obj);
  } catch {
    return '';
  }
}