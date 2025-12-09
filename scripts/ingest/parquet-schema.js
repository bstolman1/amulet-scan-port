/**
 * Parquet Schema Definitions for Canton Ledger Data
 * 
 * These schemas define the structure of parquet files for:
 * - ledger_updates: Transaction/reassignment updates
 * - ledger_events: Individual contract events (created, archived, etc.)
 */

export const LEDGER_UPDATES_SCHEMA = {
  update_id: 'STRING',
  update_type: 'STRING',
  migration_id: 'INT64',
  synchronizer_id: 'STRING',
  workflow_id: 'STRING',
  offset: 'INT64',
  record_time: 'TIMESTAMP',
  effective_at: 'TIMESTAMP',
  timestamp: 'TIMESTAMP',
  kind: 'STRING',
  update_data: 'JSON',  // Stored as JSON string
};

export const LEDGER_EVENTS_SCHEMA = {
  event_id: 'STRING',
  update_id: 'STRING',
  event_type: 'STRING',  // 'created', 'archived', 'exercised'
  contract_id: 'STRING',
  template_id: 'STRING',
  package_name: 'STRING',
  migration_id: 'INT64',
  timestamp: 'TIMESTAMP',
  created_at_ts: 'TIMESTAMP',
  signatories: 'LIST<STRING>',
  observers: 'LIST<STRING>',
  payload: 'JSON',  // Contract payload as JSON string
};

// Column order for parquet files
export const UPDATES_COLUMNS = [
  'update_id',
  'update_type',
  'migration_id',
  'synchronizer_id',
  'workflow_id',
  'offset',
  'record_time',
  'effective_at',
  'timestamp',
  'kind',
  'update_data',
];

export const EVENTS_COLUMNS = [
  'event_id',
  'update_id',
  'event_type',
  'contract_id',
  'template_id',
  'package_name',
  'migration_id',
  'timestamp',
  'created_at_ts',
  'signatories',
  'observers',
  'payload',
];

/**
 * Normalize a ledger update for parquet storage
 */
export function normalizeUpdate(raw) {
  const update = raw.transaction || raw.reassignment || raw;
  
  return {
    update_id: update.update_id || raw.update_id,
    update_type: raw.transaction ? 'transaction' : raw.reassignment ? 'reassignment' : 'unknown',
    migration_id: parseInt(raw.migration_id) || null,
    synchronizer_id: update.synchronizer_id || null,
    workflow_id: update.workflow_id || null,
    offset: parseInt(update.offset) || null,
    record_time: update.record_time ? new Date(update.record_time) : null,
    effective_at: update.effective_at ? new Date(update.effective_at) : null,
    timestamp: new Date(),
    kind: update.kind || null,
    update_data: JSON.stringify(update),
  };
}

/**
 * Normalize a ledger event for parquet storage
 */
export function normalizeEvent(event, updateId, migrationId, rawEvent = null, updateInfo = null) {
  const templateId = event.template_id || 
    event.created_event?.template_id || 
    event.archived_event?.template_id ||
    event.exercised_event?.template_id ||
    null;
  
  const contractId = event.contract_id ||
    event.created_event?.contract_id ||
    event.archived_event?.contract_id ||
    event.exercised_event?.contract_id ||
    null;
  
  const payload = event.created_event?.create_arguments ||
    event.exercised_event?.choice_argument ||
    event.choice_argument ||
    event.payload ||
    event.create_arguments ||
    null;
  
  // Determine event type
  let eventType = 'unknown';
  if (event.created_event) eventType = 'created';
  else if (event.archived_event) eventType = 'archived';
  else if (event.exercised_event) eventType = 'exercised';
  else if (event.event_type) eventType = event.event_type;
  
  // Extract effective_at from multiple sources:
  // 1. Event's own created_at (for created events)
  // 2. Update's record_time (transaction level)
  // 3. Update's effective_at
  const createdAt = event.created_at || event.created_event?.created_at;
  const effectiveAt = createdAt 
    ? new Date(createdAt) 
    : (updateInfo?.record_time ? new Date(updateInfo.record_time) : null);
  
  // Get synchronizer from update info
  const synchronizer = updateInfo?.synchronizer_id || null;
  
  return {
    event_id: event.event_id || `${updateId}-${contractId}`,
    update_id: updateId,
    event_type: eventType,
    contract_id: contractId,
    template_id: templateId,
    package_name: extractPackageName(templateId),
    migration_id: parseInt(migrationId) || null,
    synchronizer_id: synchronizer,
    effective_at: effectiveAt,
    recorded_at: new Date(), // When we recorded this event
    timestamp: new Date(),
    created_at_ts: effectiveAt,
    signatories: event.signatories || event.created_event?.signatories || event.exercised_event?.acting_parties || event.acting_parties || [],
    observers: event.observers || event.created_event?.observers || [],
    payload: payload ? JSON.stringify(payload) : null,
    raw: rawEvent || event, // Store complete original event
  };
}

/**
 * Extract package name from template ID
 */
function extractPackageName(templateId) {
  if (!templateId) return null;
  const parts = templateId.split(':');
  return parts.length > 1 ? parts[0] : null;
}

/**
 * Get partition path for a timestamp and optional migration ID
 */
export function getPartitionPath(timestamp, migrationId = null) {
  const d = new Date(timestamp);
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  
  // Include migration_id in path to keep migrations separate
  if (migrationId) {
    return `migration=${migrationId}/year=${year}/month=${month}/day=${day}`;
  }
  return `year=${year}/month=${month}/day=${day}`;
}
