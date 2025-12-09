/**
 * Parquet Schema Definitions for Canton Ledger Data
 * 
 * These schemas define the structure of parquet files for:
 * - ledger_updates: Transaction/reassignment updates
 * - ledger_events: Individual contract events (created, archived, etc.)
 */

export const LEDGER_UPDATES_SCHEMA = {
  update_id: 'STRING',
  update_type: 'STRING',  // 'transaction' or 'reassignment'
  migration_id: 'INT64',
  synchronizer_id: 'STRING',
  workflow_id: 'STRING',
  command_id: 'STRING',
  offset: 'INT64',
  record_time: 'TIMESTAMP',
  effective_at: 'TIMESTAMP',
  recorded_at: 'TIMESTAMP',  // When we recorded this update
  timestamp: 'TIMESTAMP',
  kind: 'STRING',  // For reassignments: 'assign' or 'unassign'
  root_event_ids: 'LIST<STRING>',  // Root event IDs for this transaction
  event_count: 'INT32',  // Number of events in this update
  update_data: 'JSON',  // Full update data as JSON string
};

export const LEDGER_EVENTS_SCHEMA = {
  event_id: 'STRING',
  update_id: 'STRING',
  event_type: 'STRING',  // 'created', 'archived', 'exercised', 'reassign_create', 'reassign_archive'
  contract_id: 'STRING',
  template_id: 'STRING',
  package_name: 'STRING',
  migration_id: 'INT64',
  synchronizer_id: 'STRING',
  effective_at: 'TIMESTAMP',
  recorded_at: 'TIMESTAMP',
  timestamp: 'TIMESTAMP',
  created_at_ts: 'TIMESTAMP',
  signatories: 'LIST<STRING>',
  observers: 'LIST<STRING>',
  acting_parties: 'LIST<STRING>',  // For exercised events
  payload: 'JSON',  // Contract create_arguments or choice_argument
  // Exercised event fields
  choice: 'STRING',
  consuming: 'BOOLEAN',
  interface_id: 'STRING',
  child_event_ids: 'LIST<STRING>',
  exercise_result: 'JSON',
  // Reassignment event fields
  source_synchronizer: 'STRING',
  target_synchronizer: 'STRING',
  unassign_id: 'STRING',
  submitter: 'STRING',
  reassignment_counter: 'INT64',
};

// Column order for parquet files
export const UPDATES_COLUMNS = [
  'update_id',
  'update_type',
  'migration_id',
  'synchronizer_id',
  'workflow_id',
  'command_id',
  'offset',
  'record_time',
  'effective_at',
  'recorded_at',
  'timestamp',
  'kind',
  'root_event_ids',
  'event_count',
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
  'synchronizer_id',
  'effective_at',
  'recorded_at',
  'timestamp',
  'created_at_ts',
  'signatories',
  'observers',
  'acting_parties',
  'payload',
  'choice',
  'consuming',
  'interface_id',
  'child_event_ids',
  'exercise_result',
  'source_synchronizer',
  'target_synchronizer',
  'unassign_id',
  'submitter',
  'reassignment_counter',
];

/**
 * Normalize a ledger update for parquet storage
 */
export function normalizeUpdate(raw) {
  const update = raw.transaction || raw.reassignment || raw;
  
  // Extract root event IDs
  const rootEventIds = update.root_event_ids || [];
  
  // Count events
  const eventsById = update.events_by_id || {};
  const eventCount = Object.keys(eventsById).length;
  
  return {
    update_id: update.update_id || raw.update_id,
    update_type: raw.transaction ? 'transaction' : raw.reassignment ? 'reassignment' : 'unknown',
    migration_id: parseInt(raw.migration_id) || null,
    synchronizer_id: update.synchronizer_id || null,
    workflow_id: update.workflow_id || null,
    command_id: update.command_id || null,
    offset: parseInt(update.offset) || null,
    record_time: update.record_time ? new Date(update.record_time) : null,
    effective_at: update.effective_at ? new Date(update.effective_at) : null,
    recorded_at: new Date(), // When we recorded this update
    timestamp: new Date(),
    kind: update.kind || null,
    root_event_ids: rootEventIds,
    event_count: eventCount,
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
  
  // Extract exercise-specific fields
  const actingParties = event.acting_parties || event.exercised_event?.acting_parties || [];
  const choice = event.choice || event.exercised_event?.choice || null;
  const consuming = event.consuming ?? event.exercised_event?.consuming ?? null;
  const interfaceId = event.interface_id || event.exercised_event?.interface_id || null;
  const childEventIds = event.child_event_ids || event.exercised_event?.child_event_ids || [];
  const exerciseResult = event.exercise_result || event.exercised_event?.exercise_result || null;
  
  // Extract reassignment-specific fields
  const sourceSynchronizer = event.source || updateInfo?.source || null;
  const targetSynchronizer = event.target || updateInfo?.target || null;
  const unassignId = event.unassign_id || updateInfo?.unassign_id || null;
  const submitter = event.submitter || updateInfo?.submitter || null;
  const reassignmentCounter = event.counter ?? updateInfo?.counter ?? null;
  
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
    signatories: event.signatories || event.created_event?.signatories || [],
    observers: event.observers || event.created_event?.observers || [],
    acting_parties: actingParties,
    payload: payload ? JSON.stringify(payload) : null,
    choice: choice,
    consuming: consuming,
    interface_id: interfaceId,
    child_event_ids: childEventIds,
    exercise_result: exerciseResult ? JSON.stringify(exerciseResult) : null,
    // Reassignment fields
    source_synchronizer: sourceSynchronizer,
    target_synchronizer: targetSynchronizer,
    unassign_id: unassignId,
    submitter: submitter,
    reassignment_counter: reassignmentCounter,
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
