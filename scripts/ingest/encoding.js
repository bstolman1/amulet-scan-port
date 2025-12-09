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
 */
export function mapEvent(r) {
  return {
    id: r.id ?? '',
    update_id: r.update_id ?? '',
    type: r.type ?? '',
    synchronizer: r.synchronizer ?? '',
    effective_at: r.effective_at ? new Date(r.effective_at).getTime() : 0,
    recorded_at: r.recorded_at ? new Date(r.recorded_at).getTime() : 0,
    contract_id: r.contract_id ?? '',
    party: r.party ?? '',
    template: r.template ?? '',
    payload_json: r.payload ? JSON.stringify(r.payload) : '',
  };
}

/**
 * Map a raw update object to protobuf-compatible format
 */
export function mapUpdate(r) {
  return {
    id: r.id ?? '',
    synchronizer: r.synchronizer ?? '',
    effective_at: r.effective_at ? new Date(r.effective_at).getTime() : 0,
    recorded_at: r.recorded_at ? new Date(r.recorded_at).getTime() : 0,
    transaction_id: r.transaction_id ?? '',
    command_id: r.command_id ?? '',
    workflow_id: r.workflow_id ?? '',
    status: r.status ?? '',
  };
}
