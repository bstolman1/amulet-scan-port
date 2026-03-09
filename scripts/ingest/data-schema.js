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
 * NOTE: Schema objects (LEDGER_UPDATES_SCHEMA, LEDGER_EVENTS_SCHEMA) map field
 * names to type label strings for documentation and tooling reference only.
 * They are NOT enforced at runtime — do not write validators that trust these
 * strings as authoritative contracts.
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
  recorded_at: 'TIMESTAMP',    // When we recorded this batch (set by caller, not per-record)
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
  package_name: 'STRING',      // API-provided preferred; fallback may be a package hash (see extractPackageName)
  migration_id: 'INT64',
  synchronizer_id: 'STRING',
  effective_at: 'TIMESTAMP',
  recorded_at: 'TIMESTAMP',    // Set by caller at batch start — not per-record new Date()
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
//
// ADDING A NEW FIELD — update all three places or the field will be silently
// absent from structured Parquet columns (still recoverable from raw_event /
// update_data, but not queryable directly):
//
//   1. normalizeUpdate() / normalizeEvent() return object  (data-schema.js)
//   2. UPDATES_COLUMNS / EVENTS_COLUMNS list below         (data-schema.js)
//   3. read_json_auto columns={...} in writeToParquetCLI   (write-parquet.js)
//
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
  'trace_context',  // present on events parquet writer; must stay in sync with write-parquet.js
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
 * @param {object} raw            - Raw update from Scan API
 * @param {object} options        - Validation options
 * @param {boolean} options.strict   - If true (default), throws on unknown update_type.
 *                                     If false, logs a warning and continues — never silently swallows.
 * @param {Date|null} options.batchTimestamp - Shared timestamp for recorded_at/timestamp across
 *                                     all records in this batch. Pass once at batch start to prevent
 *                                     drift. Defaults to new Date() if omitted (single-record use).
 * @returns {object} Normalized update object
 * @throws {SchemaValidationError} If update_type is 'unknown' and strict mode enabled
 */
export function normalizeUpdate(raw, options = {}) {
  // FIX #1: Removed the `warnOnly` option — its interaction with `strict` created
  // a silent-swallow path when both were false. Now: strict=true throws, strict=false
  // always warns. There is no silent path.
  const { strict = true, batchTimestamp = null } = options;

  // FIX #7: Use caller-supplied batchTimestamp so all records in a batch share
  // the same recorded_at/timestamp value, making it a meaningful batch marker.
  const recordedAt = batchTimestamp instanceof Date ? batchTimestamp : new Date();

  const update = raw.transaction || raw.reassignment || raw;
  const isReassignment = !!raw.reassignment;
  const isTransaction = !!raw.transaction || (!isReassignment && !!update.events_by_id);
  const updateType = isTransaction ? 'transaction' : isReassignment ? 'reassignment' : 'unknown';

  // FIX #1: Always warn when not strict — no silent swallow
  if (updateType === 'unknown') {
    const updateId = update.update_id || raw.update_id || 'NO_ID';
    const context = {
      update_id: updateId,
      has_transaction_wrapper: !!raw.transaction,
      has_reassignment_wrapper: !!raw.reassignment,
      has_events_by_id: !!update.events_by_id,
      top_level_keys: Object.keys(raw).slice(0, 10),
    };

    const message =
      `Unknown update_type detected for update ${updateId}. ` +
      `This indicates a schema mismatch - the update is neither a transaction (no events_by_id) ` +
      `nor a reassignment (no reassignment wrapper). Keys: [${context.top_level_keys.join(', ')}]`;

    if (strict) {
      throw new SchemaValidationError(message, context);
    }
    // FIX #1: Always warn — never silently swallow unknown updates
    console.warn(`[SCHEMA WARNING] ${message}`);
  }

  const rootEventIds = update.root_event_ids || [];
  const eventsById = update.events_by_id || {};
  const eventCount = Object.keys(eventsById).length;

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
    workflow_id: update.workflow_id || null,
    command_id: update.command_id || null,
    offset: (() => { const o = parseInt(update.offset); return isNaN(o) ? null : o; })(),
    record_time: update.record_time ? new Date(update.record_time) : null,
    effective_at: update.effective_at ? new Date(update.effective_at) : null,
    // FIX #7: Use shared batch timestamp — not a per-record new Date()
    recorded_at: recordedAt,
    timestamp: recordedAt,
    kind: update.kind || null,
    root_event_ids: rootEventIds,
    event_count: eventCount,
    source_synchronizer: sourceSynchronizer,
    target_synchronizer: targetSynchronizer,
    unassign_id: unassignId,
    submitter: submitter,
    reassignment_counter: reassignmentCounter,
    trace_context: update.trace_context ? JSON.stringify(update.trace_context) : null,
    update_data: JSON.stringify(update),
  };
}

/**
 * Determine original event type name from event structure.
 * Returns the API's original type name (with _event suffix).
 */
function determineOriginalEventType(event) {
  if (event.created_event) return 'created_event';
  if (event.archived_event) return 'archived_event';
  if (event.exercised_event) return 'exercised_event';
  return event.event_type || null;
}

/**
 * Determine normalized event type for internal use.
 * Maps API types to shorter internal names.
 */
function determineNormalizedEventType(event) {
  if (event.created_event) return 'created';
  if (event.archived_event) return 'archived';
  if (event.exercised_event) return 'exercised';
  const originalType = event.event_type || '';
  if (originalType.includes('created')) return 'created';
  if (originalType.includes('archived')) return 'archived';
  if (originalType.includes('exercised')) return 'exercised';
  return originalType || 'unknown';
}

/**
 * Parse and normalize a timestamp string to a UTC Date.
 *
 * Scan API can sometimes return timestamps without a timezone suffix.
 * When absent, we treat the value as UTC to avoid local-timezone shifts.
 *
 * @param {string|number|Date|null} v
 * @returns {Date|null}
 */
function asUtcDate(v) {
  if (!v) return null;
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
  if (typeof v === 'number') return new Date(v);
  if (typeof v === 'string') {
    const normalized = v.includes('T') ? v : v.replace(' ', 'T');
    const hasTz = /([zZ]|[+-]\d{2}:?\d{2})$/.test(normalized);
    const result = new Date(hasTz ? normalized : `${normalized}Z`);
    if (isNaN(result.getTime())) {
      console.warn(`[data-schema] asUtcDate: invalid timestamp "${v}"`);
      return null;
    }
    return result;
  }
  return new Date(v);
}

/**
 * Normalize a ledger event for parquet storage
 *
 * IMPORTANT per Scan API docs:
 * - event_id format is "<update_id>:<event_index>" — preserve original, don't synthesize
 * - Events form a tree via root_event_ids and child_event_ids
 * - Many fields are optional (signatories only on created, acting_parties only on exercised)
 *
 * @param {object}      event          - Event object from Scan API
 * @param {string}      updateId       - Parent update's ID
 * @param {number}      migrationId    - Migration ID from backfill context
 * @param {object|null} rawEvent       - Original raw event for preservation
 * @param {object|null} updateInfo     - Parent update info (synchronizer, timestamps)
 * @param {object}      options
 * @param {Date|null}   options.batchTimestamp - Shared timestamp for recorded_at/timestamp.
 *                                     Pass once at batch start to prevent per-record drift.
 * @returns {object} Normalized event object
 * @throws {Error} If effective_at cannot be determined (required for partitioning)
 */
export function normalizeEvent(event, updateId, migrationId, rawEvent = null, updateInfo = null, options = {}) {
  // FIX #7: Use caller-supplied batchTimestamp so all records in a batch share
  // the same recorded_at/timestamp value.
  const { batchTimestamp = null } = options;
  const recordedAt = batchTimestamp instanceof Date ? batchTimestamp : new Date();

  const createdEvent = event.created_event;
  const archivedEvent = event.archived_event;
  const exercisedEvent = event.exercised_event;
  const innerEvent = createdEvent || archivedEvent || exercisedEvent || event;

  const templateId = innerEvent.template_id || event.template_id || null;
  const contractId = innerEvent.contract_id || event.contract_id || null;

  const payload =
    createdEvent?.create_arguments ||
    exercisedEvent?.choice_argument ||
    event.create_arguments ||
    event.choice_argument ||
    event.payload ||
    null;

  // FIX #4: Log a warning when falling back to package hash extraction so callers
  // know the value may be a hash rather than a human-readable name.
  const packageName =
    innerEvent.package_name ||
    event.package_name ||
    extractPackageName(templateId, /* warnOnFallback */ true);

  const eventTypeOriginal = determineOriginalEventType(event);
  const eventType = determineNormalizedEventType(event);

  const eventId = event.event_id || innerEvent.event_id || null;
  if (!eventId && contractId) {
    console.warn(`Event missing event_id: update=${updateId}, contract=${contractId}`);
  }

  // FIX #2: effective_at priority is now event-type-aware.
  //
  // For 'created' events: created_at is the correct timestamp (when the contract
  // came into existence), so prefer it first.
  //
  // For 'archived' and 'exercised' events: created_at refers to the *contract's*
  // creation time, which is wrong for effective_at of the event itself. Prefer
  // the update's effective_at or record_time instead.
  //
  // This prevents archived/exercised events from being partitioned into the
  // wrong (contract-creation-date) folder.
  const createdAt = innerEvent.created_at || event.created_at;

  let effectiveAt;
  if (eventType === 'created' && createdAt) {
    effectiveAt = asUtcDate(createdAt);
  } else {
    // For all non-created events: use the update's effective_at or record_time.
    // Fall back to created_at only as a last resort (with a warning).
    const updateEffectiveAt = updateInfo?.effective_at || updateInfo?.record_time;
    if (updateEffectiveAt) {
      effectiveAt = asUtcDate(updateEffectiveAt);
    } else if (createdAt) {
      console.warn(
        `[data-schema] normalizeEvent: using created_at as effective_at for ` +
        `${eventType} event (update=${updateId}, contract=${contractId}) — ` +
        `updateInfo.effective_at and updateInfo.record_time were both absent. ` +
        `This may produce incorrect partition placement.`
      );
      effectiveAt = asUtcDate(createdAt);
    } else {
      effectiveAt = null;
    }
  }

  // FIX #3: Validate effective_at here at normalization time so the error is
  // reported with full event context (updateId, contractId, eventType) rather
  // than as a cryptic partition failure later in groupByPartition.
  if (!effectiveAt) {
    throw new Error(
      `normalizeEvent: could not determine effective_at for event ` +
      `(update=${updateId}, contract=${contractId}, type=${eventType}). ` +
      `Provide updateInfo.effective_at or updateInfo.record_time.`
    );
  }

  const synchronizer = updateInfo?.synchronizer_id || null;

  const signatories = createdEvent?.signatories || event.signatories || null;
  const observers = createdEvent?.observers || event.observers || null;
  const witnessParties =
    createdEvent?.witness_parties ||
    innerEvent?.witness_parties ||
    event.witness_parties ||
    null;
  const contractKey =
    createdEvent?.contract_key || innerEvent?.contract_key || event.contract_key || null;

  const actingParties =
    exercisedEvent?.acting_parties || innerEvent?.acting_parties || event.acting_parties || null;
  const choice =
    exercisedEvent?.choice || innerEvent?.choice || event.choice || null;
  const consuming =
    exercisedEvent?.consuming ?? innerEvent?.consuming ?? event.consuming ?? null;
  const interfaceId =
    exercisedEvent?.interface_id || innerEvent?.interface_id || event.interface_id || null;
  const childEventIds =
    exercisedEvent?.child_event_ids || innerEvent?.child_event_ids || event.child_event_ids || null;
  const exerciseResult =
    exercisedEvent?.exercise_result || innerEvent?.exercise_result || event.exercise_result || null;

  // Reassignment fields — PRIORITY: updateInfo (from reassignment wrapper) over event
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
    // FIX #7: Use shared batch timestamp — not per-record new Date()
    recorded_at: recordedAt,
    timestamp: recordedAt,
    created_at_ts: eventType === 'created' ? effectiveAt : (createdAt ? asUtcDate(createdAt) : null),
    signatories,
    observers,
    acting_parties: actingParties,
    witness_parties: witnessParties,
    payload: payload ? JSON.stringify(payload) : null,
    contract_key: contractKey ? JSON.stringify(contractKey) : null,
    choice,
    consuming,
    interface_id: interfaceId,
    child_event_ids: childEventIds,
    exercise_result: exerciseResult ? JSON.stringify(exerciseResult) : null,
    source_synchronizer: sourceSynchronizer,
    target_synchronizer: targetSynchronizer,
    unassign_id: unassignId,
    submitter,
    reassignment_counter: reassignmentCounter,
    raw_event: JSON.stringify(rawEvent || event),
  };
}

/**
 * Extract package name from template ID.
 *
 * Canton template IDs have the format: `packageHash:ModuleName:EntityName`.
 * Splitting on ':' and taking parts[0] returns the package *hash*, not a
 * human-readable name. The API provides `package_name` directly on events —
 * always prefer that value. This function is a last-resort fallback only.
 *
 * FIX #4: Added warnOnFallback parameter so callers are alerted when the
 * hash-based fallback is used and the value may not be human-readable.
 *
 * @param {string|null} templateId
 * @param {boolean} warnOnFallback - If true, logs a warning when extraction is used
 * @returns {string|null} Package hash (not name) or null
 */
function extractPackageName(templateId, warnOnFallback = false) {
  if (!templateId) return null;
  const parts = templateId.split(':');
  if (parts.length <= 1) return null;
  const hash = parts[0];
  if (warnOnFallback) {
    console.warn(
      `[data-schema] extractPackageName: package_name absent from API response for ` +
      `template "${templateId}" — falling back to package hash "${hash}". ` +
      `This value is a hash, not a human-readable name.`
    );
  }
  return hash;
}

/**
 * Extract UTC year/month/day from an effective_at timestamp.
 *
 * effective_at is the ONLY acceptable partitioning timestamp — no fallbacks.
 * Throws if the value is missing or not a valid date.
 *
 * @param {string} effectiveAt - ISO 8601 timestamp (must be valid)
 * @returns {{ year: number, month: number, day: number }}
 */
export function getUtcPartition(effectiveAt) {
  if (!effectiveAt) {
    throw new Error('getUtcPartition: effective_at is required for partitioning — no fallbacks allowed');
  }
  const d = new Date(effectiveAt);
  if (isNaN(d.getTime())) {
    throw new Error(`getUtcPartition: invalid timestamp "${effectiveAt}"`);
  }
  const year = d.getUTCFullYear();
  const month = d.getUTCMonth() + 1;
  const day = d.getUTCDate();

  const yearMin = parseInt(process.env.PARTITION_YEAR_MIN) || 2020;
  // OPERATIONAL NOTE: bump PARTITION_YEAR_MAX in your env before this date or
  // all partition writes will throw. The default is 2035 — add it to your
  // deployment runbook now so it isn't forgotten.
  const yearMax = parseInt(process.env.PARTITION_YEAR_MAX) || 2035;
  if (year < yearMin || year > yearMax) {
    throw new Error(
      `getUtcPartition: year ${year} out of range [${yearMin}-${yearMax}] from "${effectiveAt}" — likely microsecond timestamp`
    );
  }
  if (month < 1 || month > 12) {
    throw new Error(`getUtcPartition: month ${month} out of range [1-12] from "${effectiveAt}"`);
  }
  if (day < 1 || day > 31) {
    throw new Error(`getUtcPartition: day ${day} out of range [1-31] from "${effectiveAt}"`);
  }

  return { year, month, day };
}

/**
 * Get partition path for a timestamp and optional migration ID.
 *
 * Uses migration_id in path because record_time can overlap across migrations.
 * Partition values are numeric (not zero-padded) for INT64 inference in BigQuery/DuckDB.
 *
 * Structure: {source}/{type}/migration=X/year=YYYY/month=M/day=D
 *
 * FIX #5: Invalid `source` values now throw instead of silently redirecting to
 * 'backfill', which previously caused data intended for the 'updates' partition
 * to land in 'backfill' with no error or warning.
 *
 * @param {Date|string|number} timestamp
 * @param {number|null} migrationId - Defaults to 0 if not provided
 * @param {string} type   - 'updates' or 'events'
 * @param {string} source - 'backfill' or 'updates'
 */
export function getPartitionPath(timestamp, migrationId = null, type = 'updates', source = 'backfill') {
  const { year, month, day } = getUtcPartition(timestamp);
  const mig = migrationId ?? 0;

  // FIX #5: Throw on invalid source rather than silently redirecting to 'backfill'
  const validSources = ['backfill', 'updates'];
  if (!validSources.includes(source)) {
    throw new Error(
      `getPartitionPath: invalid source "${source}". Must be one of: ${validSources.join(', ')}. ` +
      `Refusing to silently redirect — data would land in the wrong partition.`
    );
  }

  const validTypes = ['updates', 'events'];
  if (!validTypes.includes(type)) {
    throw new Error(
      `getPartitionPath: invalid type "${type}". Must be one of: ${validTypes.join(', ')}.`
    );
  }

  return `${source}/${type}/migration=${mig}/year=${year}/month=${month}/day=${day}`;
}

/**
 * Group records by their individual effective_at partition path.
 *
 * Each record is routed to its own correct UTC-based partition, preventing
 * cross-midnight buffers from landing in the wrong folder.
 *
 * FIX #3: Missing effective_at is caught here with a clear error message.
 * It should never reach this point because normalizeEvent now validates and
 * throws earlier with richer context — but this provides a second safety net.
 *
 * @param {object[]} records    - Array of normalized records (must have effective_at)
 * @param {string}   type       - 'updates' or 'events'
 * @param {string}   source     - 'backfill' or 'updates'
 * @param {number|null} migrationId - Migration ID override (falls back to record.migration_id)
 * @returns {Object.<string, object[]>} Map of partition path → records
 */
export function groupByPartition(records, type = 'updates', source = 'backfill', migrationId = null) {
  const groups = {};

  for (const record of records) {
    const effectiveAt = record.effective_at;
    // FIX #3: Clearer error at groupByPartition level as a second safety net.
    // Primary validation should have already thrown in normalizeEvent.
    if (!effectiveAt) {
      throw new Error(
        `groupByPartition: record ${record.update_id || record.event_id || 'unknown'} ` +
        `has no effective_at — cannot partition. This should have been caught in normalizeEvent.`
      );
    }
    const mig = migrationId ?? record.migration_id ?? 0;
    const partition = getPartitionPath(effectiveAt, mig, type, source);

    if (!groups[partition]) {
      groups[partition] = [];
    }
    groups[partition].push(record);
  }

  return groups;
}

/**
 * Flatten events from events_by_id maintaining tree order.
 *
 * Per API docs, events should be traversed in preorder using root_event_ids
 * and child_event_ids. This function flattens while preserving order.
 *
 * FIX #6: After traversal, any events in eventsById that were not reachable
 * from rootEventIds are logged as orphans. They are appended to the result
 * so no data is silently dropped, but the warning lets operators investigate
 * whether the API response has a structural problem.
 *
 * @param {object}   eventsById   - events_by_id object from update
 * @param {string[]} rootEventIds - root_event_ids array
 * @returns {object[]} Flattened array of events in tree order (orphans appended)
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

    result.push({ ...event, event_id: eventId });

    const childIds =
      event.child_event_ids ||
      event.exercised_event?.child_event_ids ||
      [];
    for (const childId of childIds) {
      traverse(childId);
    }
  }

  for (const rootId of rootEventIds) {
    traverse(rootId);
  }

  // FIX #6: Detect and warn about events not reachable from rootEventIds.
  // Append them so no data is silently dropped.
  const orphans = Object.keys(eventsById).filter(id => !visited.has(id));
  if (orphans.length > 0) {
    console.warn(
      `flattenEventsInTreeOrder: ${orphans.length} orphaned event(s) not reachable ` +
      `from root_event_ids — appending to result. IDs: [${orphans.slice(0, 10).join(', ')}` +
      `${orphans.length > 10 ? `, ...+${orphans.length - 10} more` : ''}]. ` +
      `This may indicate a malformed API response.`
    );
    for (const id of orphans) {
      result.push({ ...eventsById[id], event_id: id });
    }
  }

  return result;
}
