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
  // Accept both snake_case (API/normalizer) and camelCase (internal)
  const effectiveAt = r.effective_at ?? r.effectiveAt;
  const recordedAt = r.recorded_at ?? r.recordedAt ?? r.timestamp;
  const createdAtTs = r.created_at_ts ?? r.createdAtTs;

  const eventType = r.type ?? r.event_type ?? r.eventType;
  const eventTypeOriginal =
    r.type_original ?? r.event_type_original ?? r.typeOriginal ?? r.eventTypeOriginal ?? eventType;

  const contractId =
    r.contract_id ??
    r.contractId ??
    r.created?.contract_id ??
    r.created?.contractId ??
    r.exercised?.contract_id ??
    r.exercised?.contractId ??
    r.reassignment?.contract_id ??
    r.reassignment?.contractId ??
    '';

  const childEventIds = r.child_event_ids ?? r.childEventIds;

  return {
    id: String(r.id ?? r.event_id ?? r.eventId ?? ''),
    updateId: String(r.update_id ?? r.updateId ?? ''),
    type: String(eventType ?? ''),
    typeOriginal: String(eventTypeOriginal ?? ''),
    synchronizer: String(r.synchronizer ?? r.synchronizer_id ?? r.synchronizerId ?? ''),
    effectiveAt: safeTimestamp(effectiveAt),
    recordedAt: safeTimestamp(recordedAt),
    createdAtTs: safeTimestamp(createdAtTs),
    contractId: String(contractId ?? ''),
    template: String(r.template ?? r.template_id ?? r.templateId ?? ''),
    packageName: String(r.package_name ?? r.packageName ?? ''),
    migrationId: safeInt64(r.migration_id ?? r.migrationId),
    // Optional arrays - may be null/undefined for certain event types
    signatories: safeStringArray(r.signatories),
    observers: safeStringArray(r.observers),
    actingParties: safeStringArray(r.acting_parties ?? r.actingParties),
    witnessParties: safeStringArray(r.witness_parties ?? r.witnessParties),
    payloadJson: r.payloadJson || r.payload_json || (r.payload ? safeStringify(r.payload) : ''),
    // Created event specific fields
    contractKeyJson: r.contractKeyJson || r.contract_key_json || (r.contract_key ? safeStringify(r.contract_key) : ''),
    // Exercised event specific fields
    choice: String(r.choice ?? ''),
    consuming: Boolean(r.consuming ?? false),
    interfaceId: String(r.interface_id ?? r.interfaceId ?? ''),
    childEventIds: safeStringArray(childEventIds),
    exerciseResultJson: r.exerciseResultJson || r.exercise_result_json || (r.exercise_result ? safeStringify(r.exercise_result) : ''),
    // Reassignment event specific fields
    sourceSynchronizer: String(r.source_synchronizer ?? r.sourceSynchronizer ?? ''),
    targetSynchronizer: String(r.target_synchronizer ?? r.targetSynchronizer ?? ''),
    unassignId: String(r.unassign_id ?? r.unassignId ?? ''),
    submitter: String(r.submitter ?? ''),
    reassignmentCounter: safeInt64(r.reassignment_counter ?? r.reassignmentCounter),
    // CRITICAL: Complete original event for recovery/future-proofing
    // Support both old (raw/raw_json) and new (raw_event) field names
    rawJson: r.rawJson || r.raw_json || r.raw_event || (r.raw ? safeStringify(r.raw) : ''),
    rawEvent: r.raw_event || r.rawJson || r.raw_json || (r.raw ? safeStringify(r.raw) : ''),
    // Deprecated
    party: String(r.party ?? ''),
  };
}

/**
 * Map a raw update object to protobuf-compatible format
 * NOTE: protobufjs uses camelCase field names internally
 */
export function mapUpdate(r) {
  const effectiveAt = r.effective_at ?? r.effectiveAt;
  const recordedAt = r.recorded_at ?? r.recordedAt ?? r.timestamp;
  const recordTime = r.record_time ?? r.recordTime;

  const rootEventIds =
    r.root_event_ids ??
    r.rootEventIds ??
    r.update_data?.root_event_ids ??
    r.update_data?.rootEventIds;

  return {
    id: String(r.id ?? r.update_id ?? r.updateId ?? ''),
    type: String(r.type ?? r.update_type ?? r.updateType ?? ''),
    synchronizer: String(r.synchronizer ?? r.synchronizer_id ?? r.synchronizerId ?? ''),
    effectiveAt: safeTimestamp(effectiveAt),
    recordedAt: safeTimestamp(recordedAt),
    recordTime: safeTimestamp(recordTime),
    commandId: String(r.command_id ?? r.commandId ?? ''),
    workflowId: String(r.workflow_id ?? r.workflowId ?? ''),
    kind: String(r.kind ?? ''),
    migrationId: safeInt64(r.migration_id ?? r.migrationId),
    offset: safeInt64(r.offset),
    rootEventIds: safeStringArray(rootEventIds),
    eventCount: parseInt(r.event_count ?? r.eventCount) || 0,
    // Reassignment-specific update fields
    sourceSynchronizer: String(r.source_synchronizer ?? r.sourceSynchronizer ?? ''),
    targetSynchronizer: String(r.target_synchronizer ?? r.targetSynchronizer ?? ''),
    unassignId: String(r.unassign_id ?? r.unassignId ?? ''),
    submitter: String(r.submitter ?? ''),
    reassignmentCounter: safeInt64(r.reassignment_counter ?? r.reassignmentCounter),
    // Tracing
    traceContextJson: r.traceContextJson || r.trace_context_json || (r.trace_context ? safeStringify(r.trace_context) : ''),
    updateDataJson: r.updateDataJson || r.update_data_json || (r.update_data ? safeStringify(r.update_data) : ''),
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