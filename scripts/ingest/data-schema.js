/**
 * Parquet Schema Definitions for Canton Ledger Data
 * 
 * These schemas define the structure of parquet files for:
 * - ledger_updates: Transaction/reassignment updates
 * - ledger_events: Individual contract events (created, archived, etc.)
 * 
 * IMPORTANT: Per Scan API documentation:
 * - record_time is the primary ordering key (monotonic within migration+synchronizer)
 * - event_id format is "<update_id>:<event_index>" - DO NOT synthesize
 * - Events form a tree structure via root_event_ids and child_event_ids
 * - Unknown fields should be preserved, not rejected
 * 
 * @see https://docs.dev.sync.global
 */

export const LEDGER_UPDATES_SCHEMA = {
  update_id: 'STRING',
  update_type: 'STRING',  // 'transaction' or 'reassignment'
  migration_id: 'INT64',
  synchronizer_id: 'STRING',
  workflow_id: 'STRING',       // Optional - may be empty
  command_id: 'STRING',        // Optional - may be empty
  offset: 'INT64',
  record_time: 'TIMESTAMP',    // PRIMARY ordering key per API docs
  effective_at: 'TIMESTAMP',   // When ledger action takes effect
  recorded_at: 'TIMESTAMP',    // When we recorded this update
  timestamp: 'TIMESTAMP',
  kind: 'STRING',              // For reassignments: 'assign' or 'unassign'
  root_event_ids: 'LIST<STRING>',  // Root event IDs for tree traversal
  event_count: 'INT32',        // Number of events in this update
  // Reassignment-specific update fields
  source_synchronizer: 'STRING',
  target_synchronizer: 'STRING',
  unassign_id: 'STRING',
  submitter: 'STRING',
  reassignment_counter: 'INT64',
  // Tracing
  trace_context: 'JSON',
  update_data: 'JSON',  // Full update data as JSON string - CANONICAL SOURCE
};

export const LEDGER_EVENTS_SCHEMA = {
  event_id: 'STRING',          // MUST be original API value: "<update_id>:<event_index>"
  update_id: 'STRING',
  event_type: 'STRING',        // Normalized: 'created', 'archived', 'exercised', etc.
  event_type_original: 'STRING', // Original API type: 'created_event', 'archived_event', etc.
  contract_id: 'STRING',
  template_id: 'STRING',
  package_name: 'STRING',      // Also provided by API - don't rely solely on extraction
  migration_id: 'INT64',
  synchronizer_id: 'STRING',
  effective_at: 'TIMESTAMP',
  recorded_at: 'TIMESTAMP',
  timestamp: 'TIMESTAMP',
  created_at_ts: 'TIMESTAMP',
  signatories: 'LIST<STRING>',     // Optional - only for created events
  observers: 'LIST<STRING>',       // Optional - only for created events
  acting_parties: 'LIST<STRING>',  // Optional - only for exercised events
  witness_parties: 'LIST<STRING>', // Optional - parties that witnessed the event
  payload: 'JSON',  // Contract create_arguments or choice_argument
  // Created event specific fields
  contract_key: 'JSON',  // Optional - contract key if defined
  // Exercised event fields
  choice: 'STRING',
  consuming: 'BOOLEAN',
  interface_id: 'STRING',
  child_event_ids: 'LIST<STRING>', // CRITICAL for tree traversal
  exercise_result: 'JSON',
  // Reassignment event fields
  source_synchronizer: 'STRING',
  target_synchronizer: 'STRING',
  unassign_id: 'STRING',
  submitter: 'STRING',
  reassignment_counter: 'INT64',
  raw_event: 'JSON',  // Full original event as JSON string - CANONICAL SOURCE
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
  'source_synchronizer',
  'target_synchronizer',
  'unassign_id',
  'submitter',
  'reassignment_counter',
  'trace_context',
  'update_data',
];

export const EVENTS_COLUMNS = [
  'event_id',
  'update_id',
  'event_type',
  'event_type_original',
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
  'witness_parties',
  'payload',
  'contract_key',
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
  'raw_event',
];

/**
 * Custom error for schema validation failures
 */
export class SchemaValidationError extends Error {
  constructor(message, context = {}) {
    super(message);
    this.name = 'SchemaValidationError';
    this.context = context;
  }
}

/**
 * Normalize a ledger update for parquet storage
 * 
 * IMPORTANT: record_time is the canonical ordering key per API docs.
 * It is monotonically increasing within a given migration_id + synchronizer_id.
 * 
 * @param {object} raw - Raw update from Scan API
 * @param {object} options - Validation options
 * @param {boolean} options.strict - If true (default), throws on unknown update_type
 * @param {boolean} options.warnOnly - If true, logs warning instead of throwing
 * @returns {object} Normalized update object
 * @throws {SchemaValidationError} If update_type is 'unknown' and strict mode enabled
 */
export function normalizeUpdate(raw, options = {}) {
  const { strict = true, warnOnly = false } = options;
  
  const update = raw.transaction || raw.reassignment || raw;
  const isReassignment = !!raw.reassignment;
  
  // Detect if this is a transaction when no wrapper exists
  // Transactions have events_by_id, reassignments don't
  const isTransaction = !!raw.transaction || (!isReassignment && !!update.events_by_id);
  
  // Determine update type
  const updateType = isTransaction ? 'transaction' : isReassignment ? 'reassignment' : 'unknown';
  
  // Validate update_type - catch schema mismatches early
  if (updateType === 'unknown') {
    const updateId = update.update_id || raw.update_id || 'NO_ID';
    const context = {
      update_id: updateId,
      has_transaction_wrapper: !!raw.transaction,
      has_reassignment_wrapper: !!raw.reassignment,
      has_events_by_id: !!update.events_by_id,
      top_level_keys: Object.keys(raw).slice(0, 10),
    };
    
    const message = `Unknown update_type detected for update ${updateId}. ` +
      `This indicates a schema mismatch - the update is neither a transaction (no events_by_id) ` +
      `nor a reassignment (no reassignment wrapper). Keys: [${context.top_level_keys.join(', ')}]`;
    
    if (strict && !warnOnly) {
      throw new SchemaValidationError(message, context);
    } else if (warnOnly) {
      console.warn(`[SCHEMA WARNING] ${message}`);
    }
  }
  
  // Extract root event IDs - CRITICAL for tree traversal
  const rootEventIds = update.root_event_ids || [];
  
  // Count events - handle both object and array formats
  const eventsById = update.events_by_id || {};
  const eventCount = Object.keys(eventsById).length;
  
  // Reassignment-specific fields (all optional)
  const sourceSynchronizer = update.source || null;
  const targetSynchronizer = update.target || null;
  const unassignId = update.unassign_id || null;
  const submitter = update.submitter || null;
  const reassignmentCounter = update.counter ?? null;
  
  return {
    update_id: update.update_id || raw.update_id,
    update_type: updateType,
    migration_id: raw.migration_id != null ? parseInt(raw.migration_id) : null,
    synchronizer_id: update.synchronizer_id || null,
    // These fields are optional per API docs
    workflow_id: update.workflow_id || null,
    command_id: update.command_id || null,
    offset: parseInt(update.offset) || null,
    // record_time is PRIMARY ordering key
    record_time: update.record_time ? new Date(update.record_time) : null,
    effective_at: update.effective_at ? new Date(update.effective_at) : null,
    recorded_at: new Date(), // When we recorded this update
    timestamp: new Date(),
    kind: update.kind || null,
    root_event_ids: rootEventIds,
    event_count: eventCount,
    // Reassignment fields at update level
    source_synchronizer: sourceSynchronizer,
    target_synchronizer: targetSynchronizer,
    unassign_id: unassignId,
    submitter: submitter,
    reassignment_counter: reassignmentCounter,
    // Tracing (optional)
    trace_context: update.trace_context ? JSON.stringify(update.trace_context) : null,
    // CRITICAL: Full original data for future-proofing
    update_data: JSON.stringify(update),
  };
}

/**
 * Determine original event type name from event structure
 * Returns the API's original type name (with _event suffix)
 */
function determineOriginalEventType(event) {
  if (event.created_event) return 'created_event';
  if (event.archived_event) return 'archived_event';
  if (event.exercised_event) return 'exercised_event';
  // For reassignment events, the type comes from update context
  // The API doesn't wrap these in *_event objects
  return event.event_type || null;
}

/**
 * Determine normalized event type for internal use
 * Maps API types to shorter internal names
 */
function determineNormalizedEventType(event) {
  if (event.created_event) return 'created';
  if (event.archived_event) return 'archived';
  if (event.exercised_event) return 'exercised';
  // Handle direct event_type strings from API
  const originalType = event.event_type || '';
  if (originalType.includes('created')) return 'created';
  if (originalType.includes('archived')) return 'archived';
  if (originalType.includes('exercised')) return 'exercised';
  return originalType || 'unknown';
}

/**
 * Normalize a ledger event for parquet storage
 * 
 * IMPORTANT per Scan API docs:
 * - event_id format is "<update_id>:<event_index>" - preserve original, don't synthesize
 * - Events form a tree via root_event_ids and child_event_ids
 * - Many fields are optional (signatories only on created, acting_parties only on exercised)
 * 
 * @param {object} event - Event object from Scan API
 * @param {string} updateId - Parent update's ID
 * @param {number} migrationId - Migration ID from backfill context
 * @param {object} rawEvent - Original raw event for preservation
 * @param {object} updateInfo - Parent update info (for synchronizer, timestamps)
 * @returns {object} Normalized event object
 */
export function normalizeEvent(event, updateId, migrationId, rawEvent = null, updateInfo = null) {
  // Unwrap nested event structure if present
  const createdEvent = event.created_event;
  const archivedEvent = event.archived_event;
  const exercisedEvent = event.exercised_event;
  const innerEvent = createdEvent || archivedEvent || exercisedEvent || event;
  
  // Template ID - check all possible sources
  const templateId = innerEvent.template_id || 
    event.template_id ||
    null;
  
  // Contract ID - check all possible sources
  const contractId = innerEvent.contract_id ||
    event.contract_id ||
    null;
  
  // Payload - depends on event type
  const payload = createdEvent?.create_arguments ||
    exercisedEvent?.choice_argument ||
    event.create_arguments ||
    event.choice_argument ||
    event.payload ||
    null;
  
  // Package name - prefer API-provided value, fall back to extraction
  const packageName = innerEvent.package_name || 
    event.package_name ||
    extractPackageName(templateId);
  
  // Event types - preserve both original and normalized
  const eventTypeOriginal = determineOriginalEventType(event);
  const eventType = determineNormalizedEventType(event);
  
  // Event ID - CRITICAL: Use original API value only
  // Per API docs, format is "<update_id>:<event_index>"
  // DO NOT synthesize - if missing, leave as null and log warning
  const eventId = event.event_id || innerEvent.event_id || null;
  if (!eventId && contractId) {
    console.warn(`Event missing event_id: update=${updateId}, contract=${contractId}`);
  }
  
  // Timestamps - prefer created_at for created events, fall back to update's record_time
  // IMPORTANT: Scan API can sometimes return timestamps without timezone suffix.
  // If missing, treat as UTC to avoid local timezone shifts (e.g., appearing ~5h behind).
  const asUtcDate = (v) => {
    if (!v) return null;
    if (v instanceof Date) return v;
    if (typeof v === 'number') return new Date(v);
    if (typeof v === 'string') {
      const hasTz = /([zZ]|[+-]\d{2}:?\d{2})$/.test(v);
      return new Date(hasTz ? v : `${v}Z`);
    }
    return new Date(v);
  };

  const createdAt = innerEvent.created_at || event.created_at;
  const effectiveAt = createdAt
    ? asUtcDate(createdAt)
    : (updateInfo?.record_time ? asUtcDate(updateInfo.record_time) : null);
  
  // Synchronizer from update context
  const synchronizer = updateInfo?.synchronizer_id || null;
  
  // Created event specific fields (optional for other event types)
  const signatories = createdEvent?.signatories || event.signatories || null;
  const observers = createdEvent?.observers || event.observers || null;
  // witness_parties can be at multiple locations in the API response
  const witnessParties = createdEvent?.witness_parties || 
    innerEvent?.witness_parties || 
    event.witness_parties || 
    null;
  const contractKey = createdEvent?.contract_key || innerEvent?.contract_key || event.contract_key || null;
  
  // Exercised event specific fields (optional for other event types)
  const actingParties = exercisedEvent?.acting_parties || innerEvent?.acting_parties || event.acting_parties || null;
  // CRITICAL: choice field must be extracted for governance analysis
  const choice = exercisedEvent?.choice || innerEvent?.choice || event.choice || null;
  const consuming = exercisedEvent?.consuming ?? innerEvent?.consuming ?? event.consuming ?? null;
  // interface_id for interface-based exercises
  const interfaceId = exercisedEvent?.interface_id || innerEvent?.interface_id || event.interface_id || null;
  // CRITICAL: child_event_ids for tree traversal
  const childEventIds = exercisedEvent?.child_event_ids || innerEvent?.child_event_ids || event.child_event_ids || null;
  const exerciseResult = exercisedEvent?.exercise_result || innerEvent?.exercise_result || event.exercise_result || null;
  
  // Reassignment-specific fields - PRIORITY: updateInfo (from reassignment wrapper) over event
  // For reassignment events, the fields are on the reassignment wrapper, not the inner created/archived event
  const sourceSynchronizer = updateInfo?.source || event.source || null;
  const targetSynchronizer = updateInfo?.target || event.target || null;
  const unassignId = updateInfo?.unassign_id || event.unassign_id || null;
  const submitter = updateInfo?.submitter || event.submitter || null;
  const reassignmentCounter = updateInfo?.counter ?? event.counter ?? null;
  
  return {
    event_id: eventId,
    update_id: updateId,
    event_type: eventType,
    event_type_original: eventTypeOriginal,
    contract_id: contractId,
    template_id: templateId,
    package_name: packageName,
    migration_id: migrationId != null ? parseInt(migrationId) : null,
    synchronizer_id: synchronizer,
    effective_at: effectiveAt,
    recorded_at: new Date(),
    timestamp: new Date(),
    created_at_ts: effectiveAt,
    // Optional arrays - use null if not present (don't default to empty array)
    signatories: signatories,
    observers: observers,
    acting_parties: actingParties,
    witness_parties: witnessParties,
    payload: payload ? JSON.stringify(payload) : null,
    contract_key: contractKey ? JSON.stringify(contractKey) : null,
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
    // CRITICAL: Store complete original event for recovery (stringified for DuckDB/Parquet compatibility)
    raw_event: JSON.stringify(rawEvent || event),
  };
}

/**
 * Extract package name from template ID
 * Note: API also provides package_name directly on events - prefer that when available
 */
function extractPackageName(templateId) {
  if (!templateId) return null;
  const parts = templateId.split(':');
  return parts.length > 1 ? parts[0] : null;
}

/**
 * Get partition path for a timestamp and optional migration ID
 * 
 * Uses migration_id in path because record_time can overlap across migrations
 * (per API docs, record_time is only monotonic within a migration+synchronizer)
 * 
 * IMPORTANT: Partition values are numeric (not zero-padded strings) to ensure
 * BigQuery/DuckDB infer them as INT64 rather than STRING/BYTE_ARRAY.
 * 
 * Structure: raw/backfill/migration=X/year=YYYY/month=M/day=D/
 */
export function getPartitionPath(timestamp, migrationId = null) {
  const d = new Date(timestamp);
  const year = d.getFullYear();
  const month = d.getMonth() + 1;  // 1-12, no padding for INT64 inference
  const day = d.getDate();          // 1-31, no padding for INT64 inference
  
  // Always include migration_id in path (default to 0 if not provided)
  const mig = migrationId ?? 0;
  return `backfill/migration=${mig}/year=${year}/month=${month}/day=${day}`;
}

/**
 * Flatten events from events_by_id maintaining tree order
 * 
 * Per API docs, events should be traversed in preorder using root_event_ids
 * and child_event_ids. This function flattens while preserving order.
 * 
 * @param {object} eventsById - events_by_id object from update
 * @param {string[]} rootEventIds - root_event_ids array
 * @returns {object[]} Flattened array of events in tree order
 */
export function flattenEventsInTreeOrder(eventsById, rootEventIds) {
  if (!eventsById || !rootEventIds) return [];
  
  const result = [];
  const visited = new Set();
  
  function traverse(eventId) {
    if (visited.has(eventId)) return;
    visited.add(eventId);
    
    const event = eventsById[eventId];
    if (!event) return;
    
    // Add event to result with its ID
    result.push({ ...event, event_id: eventId });
    
    // Traverse children in order
    const childIds = event.child_event_ids || 
      event.exercised_event?.child_event_ids || 
      [];
    for (const childId of childIds) {
      traverse(childId);
    }
  }
  
  // Start from root events
  for (const rootId of rootEventIds) {
    traverse(rootId);
  }
  
  return result;
}
