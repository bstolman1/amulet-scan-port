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
 * 
 * IMPORTANT per Scan API docs:
 * - event_id should be original API value (format: <update_id>:<event_index>)
 * - child_event_ids is critical for tree traversal
 * - Many fields are optional (signatories only on created, etc.)
 */
export function mapEvent(r) {
  return {
    id: String(r.id ?? r.event_id ?? ''),
    updateId: String(r.update_id ?? ''),
    type: String(r.type ?? r.event_type ?? ''),
    typeOriginal: String(r.type_original ?? r.event_type_original ?? ''),
    synchronizer: String(r.synchronizer ?? r.synchronizer_id ?? ''),
    effectiveAt: safeTimestamp(r.effective_at),
    recordedAt: safeTimestamp(r.recorded_at ?? r.timestamp),
    createdAtTs: safeTimestamp(r.created_at_ts),
    contractId: String(r.contract_id ?? ''),
    template: String(r.template ?? r.template_id ?? ''),
    packageName: String(r.package_name ?? ''),
    migrationId: safeInt64(r.migration_id),
    // Optional arrays - may be null/undefined for certain event types
    signatories: safeStringArray(r.signatories),
    observers: safeStringArray(r.observers),
    actingParties: safeStringArray(r.acting_parties),
    witnessParties: safeStringArray(r.witness_parties),
    payloadJson: r.payload_json || (r.payload ? safeStringify(r.payload) : ''),
    // Created event specific fields
    contractKeyJson: r.contract_key_json || (r.contract_key ? safeStringify(r.contract_key) : ''),
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
    // CRITICAL: Complete original event for recovery/future-proofing
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
    // Reassignment-specific update fields
    sourceSynchronizer: String(r.source_synchronizer ?? ''),
    targetSynchronizer: String(r.target_synchronizer ?? ''),
    unassignId: String(r.unassign_id ?? ''),
    submitter: String(r.submitter ?? ''),
    reassignmentCounter: safeInt64(r.reassignment_counter),
    // Tracing
    traceContextJson: r.trace_context_json || (r.trace_context ? safeStringify(r.trace_context) : ''),
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
  if (value === null || value === undefined) return 0;
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