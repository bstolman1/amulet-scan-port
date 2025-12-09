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
    contractId: String(r.contract_id ?? ''),
    party: String(r.party ?? ''),
    template: String(r.template ?? r.template_id ?? ''),
    payloadJson: r.payload_json || (r.payload ? safeStringify(r.payload) : ''),
    signatories: Array.isArray(r.signatories) ? r.signatories.map(String) : [],
    observers: Array.isArray(r.observers) ? r.observers.map(String) : [],
    packageName: String(r.package_name ?? ''),
    rawJson: r.raw_json || (r.raw ? safeStringify(r.raw) : ''),
  };
}

/**
 * Map a raw update object to protobuf-compatible format
 * NOTE: protobufjs uses camelCase field names internally
 */
export function mapUpdate(r) {
  return {
    id: String(r.id ?? r.update_id ?? ''),
    synchronizer: String(r.synchronizer ?? r.synchronizer_id ?? ''),
    effectiveAt: safeTimestamp(r.effective_at),
    recordedAt: safeTimestamp(r.recorded_at ?? r.timestamp),
    transactionId: String(r.transaction_id ?? ''),
    commandId: String(r.command_id ?? ''),
    workflowId: String(r.workflow_id ?? ''),
    status: String(r.status ?? ''),
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
 * Safely stringify payload
 */
function safeStringify(obj) {
  try {
    return JSON.stringify(obj);
  } catch {
    return '';
  }
}
