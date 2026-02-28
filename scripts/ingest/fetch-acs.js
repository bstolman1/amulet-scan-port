/**
 * Protobuf Encoding for Ledger Records
 *
 * Provides binary encoding for events and updates using Protocol Buffers.
 * Much more efficient than JSON for large batches.
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import protobuf from 'protobufjs';
import Long from 'long'; // bundled with protobufjs

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const schemaPath = path.join(__dirname, 'schema', 'ledger.proto');

// FIX #8: Validate schema path at module load time so a missing file surfaces
// immediately on import rather than lazily during the first encode operation
// (which could be deep inside a batch, making the error hard to trace).
if (!fs.existsSync(schemaPath)) {
  throw new Error(
    `proto-encode: schema file not found at "${schemaPath}". ` +
    `Check your build output and ensure ledger.proto is included.`
  );
}

let rootPromise = null;

/**
 * Load and cache the protobuf root.
 *
 * FIX #1: If protobuf.load() rejects (missing file, parse error), the cached
 * promise is cleared so the next call retries rather than re-throwing the same
 * stale rejection forever.
 */
async function getRoot() {
  if (!rootPromise) {
    rootPromise = protobuf.load(schemaPath).catch(err => {
      // FIX #1: Clear so the next call to getRoot() attempts a fresh load
      rootPromise = null;
      throw err;
    });
  }
  return rootPromise;
}

/**
 * Look up a protobuf type by name, throwing a descriptive error on failure.
 *
 * FIX #6: protobufjs lookupType() throws a generic TypeError when a type is
 * not found. Wrapping it here adds the schema path and type name to the error
 * so schema drift (renamed/removed types) is immediately actionable.
 *
 * @param {protobuf.Root} root
 * @param {string} typeName - Fully-qualified type name, e.g. 'ledger.Event'
 * @returns {protobuf.Type}
 */
function safelyLookupType(root, typeName) {
  try {
    return root.lookupType(typeName);
  } catch (err) {
    throw new Error(
      `proto-encode: type "${typeName}" not found in schema "${schemaPath}". ` +
      `If the .proto file was recently updated, check for renamed or removed types. ` +
      `Original error: ${err.message}`
    );
  }
}

export async function getEncoders() {
  const root = await getRoot();
  // FIX #6: Use safelyLookupType so missing types produce actionable errors
  const Event       = safelyLookupType(root, 'ledger.Event');
  const Update      = safelyLookupType(root, 'ledger.Update');
  const EventBatch  = safelyLookupType(root, 'ledger.EventBatch');
  const UpdateBatch = safelyLookupType(root, 'ledger.UpdateBatch');

  return { Event, Update, EventBatch, UpdateBatch };
}

/**
 * Map a raw event object to protobuf-compatible format.
 * NOTE: protobufjs uses camelCase field names internally.
 *
 * IMPORTANT per Scan API docs:
 * - event_id should be original API value (format: <update_id>:<event_index>)
 * - child_event_ids is critical for tree traversal
 * - Many fields are optional (signatories only on created, etc.)
 */
export function mapEvent(r) {
  // Accept both snake_case (API/normalizer) and camelCase (internal)
  const effectiveAt = r.effective_at ?? r.effectiveAt;
  const recordedAt  = r.recorded_at  ?? r.recordedAt  ?? r.timestamp;
  const createdAtTs = r.created_at_ts ?? r.createdAtTs;

  const eventType = r.type ?? r.event_type ?? r.eventType;
  const eventTypeOriginal =
    r.type_original ?? r.event_type_original ?? r.typeOriginal ?? r.eventTypeOriginal ?? eventType;

  const contractId =
    r.contract_id             ??
    r.contractId              ??
    r.created?.contract_id   ??
    r.created?.contractId    ??
    r.exercised?.contract_id ??
    r.exercised?.contractId  ??
    r.reassignment?.contract_id ??
    r.reassignment?.contractId  ??
    '';

  const childEventIds = r.child_event_ids ?? r.childEventIds;

  // FIX #2: rawJson and rawEvent are resolved from their own distinct source
  // fields only — no cross-referencing between the two output fields.
  // Previously rawJson fell back to r.raw_event and rawEvent fell back to
  // r.rawJson, silently aliasing them. Downstream consumers now always get the
  // field they asked for or an empty string, never a silent substitute.
  const rawJson  = r.rawJson   || r.raw_json  || (r.raw ? safeStringify(r.raw) : '');
  const rawEvent = r.raw_event || (r.raw ? safeStringify(r.raw) : '');

  return {
    id:               String(r.id ?? r.event_id ?? r.eventId ?? ''),
    updateId:         String(r.update_id ?? r.updateId ?? ''),
    type:             String(eventType ?? ''),
    typeOriginal:     String(eventTypeOriginal ?? ''),
    synchronizer:     String(r.synchronizer ?? r.synchronizer_id ?? r.synchronizerId ?? ''),
    effectiveAt:      safeTimestamp(effectiveAt),
    recordedAt:       safeTimestamp(recordedAt),
    createdAtTs:      safeTimestamp(createdAtTs),
    contractId:       String(contractId ?? ''),
    template:         String(r.template ?? r.template_id ?? r.templateId ?? ''),
    packageName:      String(r.package_name ?? r.packageName ?? ''),
    migrationId:      safeInt64(r.migration_id ?? r.migrationId),
    // Optional arrays — may be null/undefined for certain event types
    signatories:      safeStringArray(r.signatories),
    observers:        safeStringArray(r.observers),
    actingParties:    safeStringArray(r.acting_parties ?? r.actingParties),
    witnessParties:   safeStringArray(r.witness_parties ?? r.witnessParties),
    payloadJson:      r.payloadJson || r.payload_json || (r.payload ? safeStringify(r.payload) : ''),
    // Created event specific fields
    contractKeyJson:  r.contractKeyJson || r.contract_key_json || (r.contract_key ? safeStringify(r.contract_key) : ''),
    // Exercised event specific fields
    choice:           String(r.choice ?? ''),
    // FIX #5: Boolean(string) coerces "false" → true. Use explicit comparison
    // to handle values arriving as strings from CSV/parquet sources.
    consuming:        r.consuming === true || r.consuming === 1 || r.consuming === 'true',
    interfaceId:      String(r.interface_id ?? r.interfaceId ?? ''),
    childEventIds:    safeStringArray(childEventIds),
    exerciseResultJson: r.exerciseResultJson || r.exercise_result_json || (r.exercise_result ? safeStringify(r.exercise_result) : ''),
    // Reassignment event specific fields
    sourceSynchronizer: String(r.source_synchronizer ?? r.sourceSynchronizer ?? ''),
    targetSynchronizer: String(r.target_synchronizer ?? r.targetSynchronizer ?? ''),
    unassignId:       String(r.unassign_id ?? r.unassignId ?? ''),
    submitter:        String(r.submitter ?? ''),
    reassignmentCounter: safeInt64(r.reassignment_counter ?? r.reassignmentCounter),
    // FIX #2: rawJson and rawEvent resolved independently (see above)
    rawJson,
    rawEvent,
    // Deprecated
    party:            String(r.party ?? ''),
  };
}

/**
 * Map a raw update object to protobuf-compatible format.
 * NOTE: protobufjs uses camelCase field names internally.
 */
export function mapUpdate(r) {
  const effectiveAt = r.effective_at ?? r.effectiveAt;
  const recordedAt  = r.recorded_at  ?? r.recordedAt  ?? r.timestamp;
  const recordTime  = r.record_time  ?? r.recordTime;

  // FIX #7: Warn when the deep update_data fallback is used for rootEventIds.
  // This indicates the caller's normalization step did not extract root_event_ids
  // to the top level, which should be fixed upstream rather than silently patched.
  let rootEventIds = r.root_event_ids ?? r.rootEventIds;
  if (rootEventIds == null) {
    const deepValue = r.update_data?.root_event_ids ?? r.update_data?.rootEventIds;
    if (deepValue != null) {
      console.warn(
        `[proto-encode] mapUpdate: root_event_ids missing at top level for update ` +
        `"${r.update_id ?? r.updateId ?? 'UNKNOWN'}" — falling back to update_data.root_event_ids. ` +
        `Fix the upstream normalizer to extract root_event_ids to the top level.`
      );
      rootEventIds = deepValue;
    }
  }

  return {
    id:               String(r.id ?? r.update_id ?? r.updateId ?? ''),
    type:             String(r.type ?? r.update_type ?? r.updateType ?? ''),
    synchronizer:     String(r.synchronizer ?? r.synchronizer_id ?? r.synchronizerId ?? ''),
    effectiveAt:      safeTimestamp(effectiveAt),
    recordedAt:       safeTimestamp(recordedAt),
    recordTime:       safeTimestamp(recordTime),
    commandId:        String(r.command_id ?? r.commandId ?? ''),
    workflowId:       String(r.workflow_id ?? r.workflowId ?? ''),
    kind:             String(r.kind ?? ''),
    migrationId:      safeInt64(r.migration_id ?? r.migrationId),
    offset:           safeInt64(r.offset),
    rootEventIds:     safeStringArray(rootEventIds),
    eventCount:       parseInt(r.event_count ?? r.eventCount) || 0,
    // Reassignment-specific update fields
    sourceSynchronizer: String(r.source_synchronizer ?? r.sourceSynchronizer ?? ''),
    targetSynchronizer: String(r.target_synchronizer ?? r.targetSynchronizer ?? ''),
    unassignId:       String(r.unassign_id ?? r.unassignId ?? ''),
    submitter:        String(r.submitter ?? ''),
    reassignmentCounter: safeInt64(r.reassignment_counter ?? r.reassignmentCounter),
    // Tracing
    traceContextJson: r.traceContextJson || r.trace_context_json || (r.trace_context ? safeStringify(r.trace_context) : ''),
    updateDataJson:   r.updateDataJson || r.update_data_json   || (r.update_data  ? safeStringify(r.update_data)  : ''),
  };
}

/**
 * Safely convert a value to a protobuf timestamp (milliseconds since epoch).
 *
 * FIX #3: Invalid or missing timestamps previously returned 0 silently,
 * storing them as Unix epoch (1970-01-01). Downstream date-range queries
 * would then silently include these records in 1970 results. We now log a
 * warning on invalid values so the caller can investigate the source data.
 * 0 remains the stored value since protobuf int64 cannot represent null,
 * but the warning makes the situation visible.
 *
 * @param {*} value
 * @returns {number} Milliseconds since epoch, or 0 if missing/invalid
 */
function safeTimestamp(value) {
  if (value === null || value === undefined || value === '' || value === 0) return 0;
  if (typeof value === 'number') {
    if (isNaN(value)) {
      console.warn(`[proto-encode] safeTimestamp: NaN number — storing as 0`);
      return 0;
    }
    return value;
  }
  try {
    const ts = new Date(value).getTime();
    if (isNaN(ts)) {
      console.warn(`[proto-encode] safeTimestamp: invalid timestamp "${value}" — storing as 0 (epoch). Check source data.`);
      return 0;
    }
    return ts;
  } catch {
    console.warn(`[proto-encode] safeTimestamp: threw on value "${value}" — storing as 0`);
    return 0;
  }
}

/**
 * Safely convert a value to a protobuf int64.
 *
 * FIX #4: JavaScript parseInt() returns a float64, which can only safely
 * represent integers up to Number.MAX_SAFE_INTEGER (2^53 - 1). Protobuf
 * int64 supports values up to 2^63 - 1. Fields like reassignment_counter
 * and offset could exceed the safe integer range, producing silently wrong
 * values. We now use Long.fromValue() (bundled with protobufjs) which
 * handles arbitrarily large int64 values correctly.
 *
 * @param {*} value
 * @returns {Long} protobufjs Long, or Long.ZERO if missing/invalid
 */
function safeInt64(value) {
  if (value === null || value === undefined) return Long.ZERO;
  try {
    return Long.fromValue(value);
  } catch {
    console.warn(`[proto-encode] safeInt64: cannot convert "${value}" to int64 — storing as 0`);
    return Long.ZERO;
  }
}

/**
 * Safely convert a value to a string array.
 *
 * @param {*} arr
 * @returns {string[]}
 */
function safeStringArray(arr) {
  if (!Array.isArray(arr)) return [];
  return arr.map(String);
}

/**
 * Safely JSON-stringify an object.
 *
 * @param {*} obj
 * @returns {string} JSON string, or '' if serialization fails
 */
function safeStringify(obj) {
  try {
    return typeof obj === 'string' ? obj : JSON.stringify(obj);
  } catch {
    return '';
  }
}
