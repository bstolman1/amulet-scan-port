/**
 * ACS (Active Contract Set) Schema Definitions for Parquet Storage
 *
 * Defines the structure for storing ACS snapshot data locally.
 */

/**
 * Expected Canton Network templates - used for validation
 * These are the core templates we expect to find in ACS snapshots
 */
export const EXPECTED_TEMPLATES = {
  // Core Amulet templates
  'Splice.Amulet:Amulet': { required: true, description: 'Amulet tokens' },
  'Splice.Amulet:LockedAmulet': { required: false, description: 'Locked amulet tokens' },
  'Splice.Amulet:FeaturedAppRight': { required: false, description: 'Featured app rights' },
  'Splice.Amulet:FeaturedAppActivityMarker': { required: false, description: 'Featured app activity markers' },
  'Splice.Amulet:ValidatorRight': { required: false, description: 'Validator rights' },
  'Splice.Amulet:AppRewardCoupon': { required: false, description: 'App reward coupons' },
  'Splice.Amulet:SvRewardCoupon': { required: false, description: 'SV reward coupons' },
  'Splice.Amulet:ValidatorRewardCoupon': { required: false, description: 'Validator reward coupons' },
  'Splice.Amulet:UnclaimedReward': { required: false, description: 'Unclaimed rewards' },

  // Validator templates
  'Splice.ValidatorLicense:ValidatorLicense': { required: true, description: 'Validator licenses' },
  'Splice.ValidatorLicense:ValidatorFaucetCoupon': { required: false, description: 'Validator faucet coupons' },
  'Splice.ValidatorLicense:ValidatorLivenessActivityRecord': { required: false, description: 'Validator liveness records' },

  // DSO/Governance templates
  'Splice.DsoRules:DsoRules': { required: false, description: 'DSO rules configuration' },
  'Splice.DsoRules:VoteRequest': { required: false, description: 'Governance vote requests' },
  'Splice.DsoRules:Confirmation': { required: false, description: 'Governance confirmations' },
  'Splice.DsoRules:ElectionRequest': { required: false, description: 'Election requests' },

  // DSO SV State templates
  'Splice.DSO.SvState:SvNodeState': { required: false, description: 'SV node state' },
  'Splice.DSO.SvState:SvRewardState': { required: false, description: 'SV reward state' },
  'Splice.DSO.SvState:SvStatusReport': { required: false, description: 'SV status reports' },

  // DSO Amulet Price templates
  'Splice.DSO.AmuletPrice:AmuletPriceVote': { required: false, description: 'Amulet price votes' },

  // Amulet Rules templates
  'Splice.AmuletRules:AmuletRules': { required: false, description: 'Amulet rules configuration' },
  'Splice.AmuletRules:TransferPreapproval': { required: false, description: 'Transfer pre-approvals' },
  'Splice.AmuletRules:ExternalPartySetupProposal': { required: false, description: 'External party setup' },

  // External Party templates
  'Splice.ExternalPartyAmuletRules:ExternalPartyAmuletRules': { required: false, description: 'External party amulet rules' },

  // Round templates
  'Splice.Round:OpenMiningRound': { required: false, description: 'Open mining rounds' },
  'Splice.Round:ClosedMiningRound': { required: false, description: 'Closed mining rounds' },
  'Splice.Round:IssuingMiningRound': { required: false, description: 'Issuing mining rounds' },
  'Splice.Round:SummarizingMiningRound': { required: false, description: 'Summarizing mining rounds' },

  // ANS templates
  'Splice.Ans:AnsEntry': { required: false, description: 'ANS name entries' },
  'Splice.Ans:AnsEntryContext': { required: false, description: 'ANS entry contexts' },
  'Splice.Ans:AnsRules': { required: false, description: 'ANS rules configuration' },
  'Splice.Ans.AmuletConversionRateFeed:AmuletConversionRateFeed': { required: false, description: 'Amulet conversion rate feed' },

  // Traffic templates
  'Splice.DecentralizedSynchronizer:MemberTraffic': { required: false, description: 'Member traffic records' },

  // Subscription templates
  'Splice.Wallet.Subscriptions:Subscription': { required: false, description: 'Subscriptions' },
  'Splice.Wallet.Subscriptions:SubscriptionRequest': { required: false, description: 'Subscription requests' },
  'Splice.Wallet.Subscriptions:SubscriptionIdleState': { required: false, description: 'Idle subscriptions' },

  // Transfer templates
  'Splice.ExternalPartyAmuletRules:TransferCommand': { required: false, description: 'Transfer commands' },
  'Splice.ExternalPartyAmuletRules:TransferCommandCounter': { required: false, description: 'Transfer counters' },
  'Splice.AmuletTransferInstruction:AmuletTransferInstruction': { required: false, description: 'Transfer instructions' },

  // Allocation templates
  'Splice.AmuletAllocation:AmuletAllocation': { required: false, description: 'Amulet allocations' },
};

export const ACS_CONTRACTS_SCHEMA = {
  contract_id:   'STRING',
  event_id:      'STRING',
  template_id:   'STRING',
  package_name:  'STRING',
  module_name:   'STRING',
  entity_name:   'STRING',
  migration_id:  'INT64',
  record_time:   'TIMESTAMP',
  snapshot_time: 'TIMESTAMP',
  signatories:   'LIST<STRING>',
  observers:     'LIST<STRING>',
  // FIX: 'JSON' is not a valid DuckDB type in versions < 0.9.0 and is only a
  // VARCHAR alias in >= 0.9.0. Using 'VARCHAR' is explicit and safe across all
  // versions. Content is a JSON-serialized string; callers should use
  // DuckDB's json_extract() / -> operator to query fields within it.
  payload: 'VARCHAR',
  raw:     'VARCHAR',
};

export const ACS_COLUMNS = [
  'contract_id',
  'event_id',
  'template_id',
  'package_name',
  'module_name',
  'entity_name',
  'migration_id',
  'record_time',
  'snapshot_time',
  'signatories',
  'observers',
  'payload',
  'raw',
];

// Field validation - critical fields that MUST have values
export const CRITICAL_CONTRACT_FIELDS = [
  'contract_id',
  'template_id',
  'migration_id',
  'record_time',
];

// Important fields that SHOULD have values
export const IMPORTANT_CONTRACT_FIELDS = [
  'module_name',
  'entity_name',
  'signatories',
  'payload',
];

/**
 * Parse template ID into components.
 *
 * Handles all Canton Network template ID formats:
 *   colon-dot  "hash:Module.Path:EntityName"       — standard (most common)
 *   all-colon  "hash:Module:Path:EntityName"        — all segments colon-separated
 *   underscore "hash_Module_Path_EntityName"        — all segments underscore-separated
 *   simple     "Module.Path:EntityName"             — no package hash (e.g. in EXPECTED_TEMPLATES keys)
 *
 * FIX: the original implementation used two pop() calls, which only handled
 * exactly 3-part inputs correctly. For "hash:Splice:Amulet:Amulet" (4 parts),
 * pop() gave entity="Amulet" and module="Amulet" — losing "Splice" entirely.
 * For "hash:Splice:DSO:SvState:SvNodeState" (5 parts), module became "SvState"
 * instead of "Splice.DSO.SvState". These mapped to wrong EXPECTED_TEMPLATES
 * keys, causing every such template to appear as both "unexpected" and "missing".
 *
 * Fix: treat parts[0] as the package hash, parts[-1] as the entity name, and
 * everything in between (joined with '.') as the module path. For the
 * simple-colon format (no hash), parts[0] IS the start of the module path —
 * detected by the absence of a long hex-like hash segment.
 */
/**
 * Returns true if a segment looks like part of a Canton package hash.
 * Canton package hashes are lowercase hexadecimal strings (e.g. "a3b4c5d6").
 * Module and entity names start with an uppercase letter (TitleCase).
 * Used to locate the hash/module boundary in the all-underscore format.
 */
function isHexSegment(s) {
  return /^[0-9a-f]+$/.test(s);
}

export function parseTemplateId(templateId) {
  if (!templateId) return { packageName: null, moduleName: null, entityName: null };

  const hasColon      = templateId.includes(':');
  const hasUnderscore = templateId.includes('_') && !hasColon;

  let parts;
  if (hasUnderscore) {
    parts = templateId.split('_');
  } else {
    parts = templateId.split(':');
  }

  if (parts.length < 2) {
    // Single-segment input (e.g. a bare hash or the string "unknown")
    return { packageName: null, moduleName: null, entityName: parts[0] || null };
  }

  if (parts.length === 2) {
    // simple-colon "Module.Path:EntityName" — no package hash
    // Normalise underscores in module segment to dots for consistency
    const moduleName = parts[0].replace(/_/g, '.') || null;
    const entityName = parts[1] || null;
    return { packageName: null, moduleName, entityName };
  }

  // 3+ parts: resolve package hash, module path, and entity name.
  //
  // Colon formats (hasColon=true): parts[0] is always the hash.
  //   colon-dot  "hash:Splice.Amulet:Amulet"          → middle = ["Splice.Amulet"]
  //   all-colon  "hash:Splice:Amulet:Amulet"           → middle = ["Splice","Amulet"]
  //   5-part     "hash:Splice:DSO:SvState:SvNodeState" → middle = ["Splice","DSO","SvState"]
  //
  // Underscore format (hasColon=false, hasUnderscore=true):
  //   "hash_Splice_Amulet_Amulet" — the hash may itself span multiple segments
  //   if it contains underscores (e.g. "abc123_def456_Splice_Amulet_Amulet").
  //   Canton hashes are lowercase hex; module/entity names are TitleCase.
  //   Walk from the left and treat all consecutive lowercase-hex segments as
  //   the hash boundary.

  let packageName, remainder;

  if (hasUnderscore) {
    let hashEnd = 0;
    for (let i = 0; i < parts.length; i++) {
      if (isHexSegment(parts[i])) {
        hashEnd = i + 1;
      } else {
        break;
      }
    }
    // If every segment looks like hex (or none do), fall back to treating
    // just the first segment as the hash to avoid consuming the module/entity.
    if (hashEnd === 0 || hashEnd >= parts.length) hashEnd = 1;
    packageName = parts.slice(0, hashEnd).join('_') || null;
    remainder   = parts.slice(hashEnd);
  } else {
    packageName = parts[0] || null;
    remainder   = parts.slice(1);
  }

  if (remainder.length < 2) {
    // Only one segment left after the hash — treat it as entity with no module
    return { packageName, moduleName: null, entityName: remainder[0] || null };
  }

  // Middle segments form the module path; last segment is the entity.
  // Joining with '.' normalises both all-colon ("Splice","Amulet" → "Splice.Amulet")
  // and underscore ("Splice","Amulet" → "Splice.Amulet") into canonical dot form.
  const entityName  = remainder[remainder.length - 1] || null;
  const middleParts = remainder.slice(0, -1);
  const moduleName  = middleParts.join('.') || null;

  return { packageName, moduleName, entityName };
}

/**
 * Normalize a template ID to canonical form: "Module.Path:EntityName"
 * Strips the package hash; always uses dot for module path separators.
 */
export function normalizeTemplateKey(templateId) {
  if (!templateId) return null;

  const { moduleName, entityName } = parseTemplateId(templateId);
  if (!moduleName || !entityName) return templateId;

  // moduleName is already dot-joined by parseTemplateId, but normalise any
  // residual underscores that survive (e.g. from underscore-format middle parts)
  const normalizedModule = moduleName.replace(/_/g, '.');
  return `${normalizedModule}:${entityName}`;
}

/**
 * Detect the format of a template ID
 */
export function detectTemplateFormat(templateId) {
  if (!templateId) return 'unknown';

  const colonCount     = (templateId.match(/:/g)  || []).length;
  const underscoreCount = (templateId.match(/_/g) || []).length;
  const dotCount       = (templateId.match(/\./g) || []).length;

  if (colonCount >= 2 && dotCount > 0)  return 'colon-dot';
  if (colonCount >= 2 && dotCount === 0) return 'all-colon';
  if (underscoreCount >= 2 && colonCount === 0) return 'underscore';
  if (colonCount === 1) return 'simple-colon';
  return 'unknown';
}

/**
 * Custom error for ACS validation failures
 */
export class ACSValidationError extends Error {
  constructor(message, context = {}) {
    super(message);
    this.name = 'ACSValidationError';
    this.context = context;
  }
}

/**
 * Normalize an ACS contract event for storage
 *
 * @param {object} event         - Raw contract event from Scan API
 * @param {number} migrationId   - Migration ID
 * @param {string} recordTime    - Record time from API
 * @param {string} snapshotTime  - Snapshot time (MUST be provided by caller)
 * @param {object} options       - Validation options
 * @param {boolean} options.strict   - If true, throws on missing critical fields
 * @param {boolean} options.warnOnly - If true, logs warning instead of throwing
 * @returns {object} Normalized contract object
 * @throws {ACSValidationError} If critical fields missing and strict mode enabled
 */
export function normalizeACSContract(event, migrationId, recordTime, snapshotTime, options = {}) {
  const { strict = false, warnOnly = false } = options;

  // FIX: use null instead of the string 'unknown' when template_id is absent.
  // Previously `event.template_id || 'unknown'` caused:
  //   1. parseTemplateId('unknown') → entity_name='unknown', module_name=null
  //   2. validateContractFields checked for null/'' but not 'unknown', so the
  //      record silently passed critical field validation with bad data.
  // null is caught correctly by validateContractFields and strict mode.
  const templateId = event.template_id || null;
  const { packageName, moduleName, entityName } = parseTemplateId(templateId);

  const contractId = event.contract_id || event.event_id;

  if (strict) {
    const missingCritical = [];
    if (!contractId)    missingCritical.push('contract_id');
    if (!templateId)    missingCritical.push('template_id');

    if (missingCritical.length > 0) {
      const context = {
        missingCritical,
        event_id:         event.event_id || 'NO_ID',
        has_contract_id:  !!event.contract_id,
        has_template_id:  !!event.template_id,
        top_level_keys:   Object.keys(event).slice(0, 10),
      };

      const message =
        `ACS contract missing critical fields: [${missingCritical.join(', ')}]. ` +
        `Event ID: ${context.event_id}. Keys: [${context.top_level_keys.join(', ')}]`;

      if (!warnOnly) {
        throw new ACSValidationError(message, context);
      } else {
        console.warn(`[ACS SCHEMA WARNING] ${message}`);
      }
    }
  }

  // FIX: snapshot_time defaults to null, not new Date().
  // Previously `snapshotTime ? new Date(snapshotTime) : new Date()` set each
  // contract's snapshot_time to the wall-clock moment normalizeACSContract ran
  // for that record. A 50,000-contract batch would produce 50,000 distinct
  // snapshot_time values spanning several seconds, breaking any query that
  // groups or filters by snapshot_time to identify a snapshot cohort.
  // Callers MUST always pass a snapshotTime; null here makes the omission
  // visible rather than silently wrong.
  const snapshotTimeValue = snapshotTime ? new Date(snapshotTime) : null;

  return {
    // Preserve BOTH contract_id and event_id (API returns both)
    contract_id:   contractId,
    event_id:      event.event_id || null,
    template_id:   templateId,
    package_name:  packageName,
    module_name:   moduleName,
    entity_name:   entityName,
    migration_id:  migrationId != null ? parseInt(migrationId) : null,
    record_time:   recordTime ? new Date(recordTime) : null,
    snapshot_time: snapshotTimeValue,
    signatories:   event.signatories || [],
    observers:     event.observers   || [],
    // ACS = Active Contract Set = created contracts only.
    // create_arguments is the correct field; choice_argument only appears on
    // exercised events, which archive contracts and remove them from the ACS.
    payload: event.create_arguments ? JSON.stringify(event.create_arguments) : null,
    // CRITICAL: Preserve raw API response for full data recovery
    raw: JSON.stringify(event),
  };
}

/**
 * Check if event matches a specific template
 */
export function isTemplate(event, moduleName, entityName) {
  const templateId = event?.template_id;
  if (!templateId) return false;

  const parsed = parseTemplateId(templateId);

  // moduleName is already dot-normalised by parseTemplateId; replace residual
  // underscores for safety, and normalise the caller's target the same way.
  const normalizedModule = parsed.moduleName?.replace(/_/g, '.') || '';
  const targetModule     = moduleName.replace(/_/g, '.');

  return normalizedModule === targetModule && parsed.entityName === entityName;
}

/**
 * Validate a single contract's fields.
 * Returns object with arrays of missing critical and important fields.
 *
 * FIX: TIMESTAMP fields (record_time, snapshot_time) are now checked for
 * Invalid Date in addition to null/undefined/''.
 * Previously `new Date('bad-string')` returned a truthy Invalid Date object
 * that passed the null/'' checks silently. The Invalid Date would then reach
 * the Parquet writer and produce a NaN timestamp or a write error.
 */
export function validateContractFields(contract) {
  const missingCritical  = [];
  const missingImportant = [];

  // Fields whose type in ACS_CONTRACTS_SCHEMA is TIMESTAMP
  const timestampFields = new Set(['record_time', 'snapshot_time']);

  for (const field of CRITICAL_CONTRACT_FIELDS) {
    const value = contract[field];
    const isNullish = value === null || value === undefined || value === '';
    const isInvalidDate =
      timestampFields.has(field) &&
      value instanceof Date &&
      isNaN(value.getTime());
    // Also catch the sentinel 'unknown' string left by old callers
    const isSentinel = value === 'unknown';

    if (isNullish || isInvalidDate || isSentinel) {
      missingCritical.push(field);
    }
  }

  for (const field of IMPORTANT_CONTRACT_FIELDS) {
    const value = contract[field];
    const isEmpty =
      value === null || value === undefined || value === '' ||
      (Array.isArray(value) && value.length === 0);
    if (isEmpty) missingImportant.push(field);
  }

  return { missingCritical, missingImportant };
}

/**
 * Validate templates against expected registry.
 * Returns validation report with found, missing, and unexpected templates.
 */
export function validateTemplates(templateCounts) {
  const report = {
    found:            [],
    missing:          [],
    unexpected:       [],
    formatVariations: {},
    warnings:         [],
  };

  const normalizedCounts = {};

  // Normalize all template IDs and track format variations
  for (const [templateId, count] of Object.entries(templateCounts)) {
    const format = detectTemplateFormat(templateId);
    const key    = normalizeTemplateKey(templateId);

    if (!normalizedCounts[key]) {
      normalizedCounts[key] = { count: 0, rawIds: [], formats: new Set() };
    }
    normalizedCounts[key].count += count;
    normalizedCounts[key].rawIds.push(templateId);
    normalizedCounts[key].formats.add(format);

    if (!report.formatVariations[format]) report.formatVariations[format] = 0;
    report.formatVariations[format]++;
  }

  // Check expected templates
  for (const [expectedKey, config] of Object.entries(EXPECTED_TEMPLATES)) {
    const normalized = normalizedCounts[expectedKey];

    if (normalized) {
      report.found.push({
        key:         expectedKey,
        count:       normalized.count,
        rawIds:      normalized.rawIds,
        formats:     [...normalized.formats],
        description: config.description,
      });
    } else if (config.required) {
      report.missing.push({ key: expectedKey, required: true,  description: config.description });
      report.warnings.push(`⚠️  MISSING REQUIRED: ${expectedKey} (${config.description})`);
    } else {
      report.missing.push({ key: expectedKey, required: false, description: config.description });
    }
  }

  // Find unexpected templates (not in registry)
  for (const [key, data] of Object.entries(normalizedCounts)) {
    if (!EXPECTED_TEMPLATES[key]) {
      report.unexpected.push({
        key,
        count:   data.count,
        rawIds:  data.rawIds,
        formats: [...data.formats],
      });
    }
  }

  // Warn about format inconsistencies
  const formats = Object.keys(report.formatVariations);
  if (formats.length > 1) {
    report.warnings.push(`⚠️  Multiple template ID formats detected: ${formats.join(', ')}`);
  }

  return report;
}

/**
 * Get partition path for ACS snapshots (by date, time, and migration).
 * Each snapshot gets its own partition to preserve historical snapshots.
 * Uses Hive-compliant key=value folder names for BigQuery/DuckDB compatibility.
 *
 * Structure: acs/migration=X/year=YYYY/month=MM/day=DD/snapshot_id=HHMMSSmmm/
 *
 * FIX 1: month and day are now zero-padded (01..12, 01..31).
 * The original comment claimed padding would prevent BigQuery/DuckDB from
 * inferring INT64 — this is incorrect. Both engines parse the integer value
 * from the partition string regardless of zero-padding; "01" and "1" both
 * infer as INT64. Without padding, string sort order is broken:
 * month=1, month=10, month=11, month=12, month=2 ...
 * This affects gsutil ls output, shell globs, find, and any tool that
 * sorts partition paths as strings rather than parsing the integer values.
 *
 * FIX 2: snapshot_id now includes milliseconds (HHMMSSmmm, 9 chars).
 * The original HHMMSS format had second-level collision: a --run-now snapshot
 * starting at exactly 00:00:00 and a cron trigger at the same second both
 * produced snapshot_id=000000, causing the second writer to silently overwrite
 * the first's Parquet files. Adding milliseconds makes same-second collisions
 * astronomically unlikely in practice.
 */
export function getACSPartitionPath(timestamp, migrationId = null) {
  if (timestamp == null) {
    throw new Error(`getACSPartitionPath: invalid timestamp "${timestamp}"`);
  }
  const d = new Date(timestamp);
  if (isNaN(d.getTime())) {
    throw new Error(`getACSPartitionPath: invalid timestamp "${timestamp}"`);
  }

  const year   = d.getUTCFullYear();
  const month  = d.getUTCMonth() + 1;
  const day    = d.getUTCDate();
  const hour   = String(d.getUTCHours()).padStart(2, '0');
  const minute = String(d.getUTCMinutes()).padStart(2, '0');
  const second = String(d.getUTCSeconds()).padStart(2, '0');
  // FIX 2: include milliseconds to prevent second-level snapshot_id collisions
  const ms     = String(d.getUTCMilliseconds()).padStart(3, '0');

  const snapshotId = `${hour}${minute}${second}${ms}`;

  // migrationId ?? 0: null/undefined migrationId uses migration=0 for
  // consistency with backfill structure — caller should always provide it.
  const mig = migrationId ?? 0;
  return `acs/migration=${mig}/year=${year}/month=${month}/day=${day}/snapshot_id=${snapshotId}`;
}
