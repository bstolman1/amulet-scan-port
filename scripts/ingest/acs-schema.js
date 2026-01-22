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
  contract_id: 'STRING',
  event_id: 'STRING',
  template_id: 'STRING',
  package_name: 'STRING',
  module_name: 'STRING',
  entity_name: 'STRING',
  migration_id: 'INT64',
  record_time: 'TIMESTAMP',
  snapshot_time: 'TIMESTAMP',
  signatories: 'LIST<STRING>',
  observers: 'LIST<STRING>',
  payload: 'JSON',
  raw: 'JSON',
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
 * Parse template ID into components
 * Handles multiple formats:
 * - "hash:Module.Path:EntityName" (colon + dot)
 * - "hash_Module_Path_EntityName" (underscore)
 * - "hash:Module:Path:EntityName" (all colons)
 */
export function parseTemplateId(templateId) {
  if (!templateId) return { packageName: null, moduleName: null, entityName: null };
  
  // Detect separator format
  const hasColon = templateId.includes(':');
  const hasUnderscore = templateId.includes('_') && !hasColon;
  
  let parts;
  if (hasUnderscore) {
    // Underscore-separated format: hash_Module_Path_EntityName
    parts = templateId.split('_');
  } else {
    // Colon-separated format (standard)
    parts = templateId.split(':');
  }
  
  const entityName = parts.pop() || null;
  const moduleName = parts.pop() || null;
  const packageName = parts.join(hasUnderscore ? '_' : ':') || null;
  
  return { packageName, moduleName, entityName };
}

/**
 * Normalize a template ID to canonical format: "Module.Path:EntityName"
 * This strips the package hash and uses dot for module path
 */
export function normalizeTemplateKey(templateId) {
  if (!templateId) return null;
  
  const { moduleName, entityName } = parseTemplateId(templateId);
  if (!moduleName || !entityName) return templateId;
  
  // Normalize module path to use dots
  const normalizedModule = moduleName.replace(/_/g, '.');
  return `${normalizedModule}:${entityName}`;
}

/**
 * Detect the format of a template ID
 */
export function detectTemplateFormat(templateId) {
  if (!templateId) return 'unknown';
  
  const colonCount = (templateId.match(/:/g) || []).length;
  const underscoreCount = (templateId.match(/_/g) || []).length;
  const dotCount = (templateId.match(/\./g) || []).length;
  
  if (colonCount >= 2 && dotCount > 0) return 'colon-dot'; // hash:Module.Path:Entity
  if (colonCount >= 2 && dotCount === 0) return 'all-colon'; // hash:Module:Path:Entity
  if (underscoreCount >= 2 && colonCount === 0) return 'underscore'; // hash_Module_Entity
  if (colonCount === 1) return 'simple-colon'; // Module:Entity (no hash)
  
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
 * @param {object} event - Raw contract event from Scan API
 * @param {number} migrationId - Migration ID
 * @param {string} recordTime - Record time from API
 * @param {string} snapshotTime - Snapshot time
 * @param {object} options - Validation options
 * @param {boolean} options.strict - If true, throws on missing critical fields
 * @param {boolean} options.warnOnly - If true, logs warning instead of throwing
 * @returns {object} Normalized contract object
 * @throws {ACSValidationError} If critical fields missing and strict mode enabled
 */
export function normalizeACSContract(event, migrationId, recordTime, snapshotTime, options = {}) {
  const { strict = false, warnOnly = false } = options;
  
  const templateId = event.template_id || 'unknown';
  const { packageName, moduleName, entityName } = parseTemplateId(templateId);
  
  const contractId = event.contract_id || event.event_id;
  
  // Strict validation for critical fields
  if (strict) {
    const missingCritical = [];
    
    if (!contractId) missingCritical.push('contract_id');
    if (!event.template_id || event.template_id === 'unknown') missingCritical.push('template_id');
    
    if (missingCritical.length > 0) {
      const context = {
        missingCritical,
        event_id: event.event_id || 'NO_ID',
        has_contract_id: !!event.contract_id,
        has_template_id: !!event.template_id,
        top_level_keys: Object.keys(event).slice(0, 10),
      };
      
      const message = `ACS contract missing critical fields: [${missingCritical.join(', ')}]. ` +
        `Event ID: ${context.event_id}. Keys: [${context.top_level_keys.join(', ')}]`;
      
      if (!warnOnly) {
        throw new ACSValidationError(message, context);
      } else {
        console.warn(`[ACS SCHEMA WARNING] ${message}`);
      }
    }
  }
  
  return {
    // Preserve BOTH contract_id and event_id (API returns both)
    contract_id: contractId,
    event_id: event.event_id || null,
    template_id: templateId,
    package_name: packageName,
    module_name: moduleName,
    entity_name: entityName,
    migration_id: migrationId != null ? parseInt(migrationId) : null,
    record_time: recordTime ? new Date(recordTime) : null,
    snapshot_time: snapshotTime ? new Date(snapshotTime) : new Date(),
    signatories: event.signatories || [],
    observers: event.observers || [],
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
  
  // Normalize module name comparison (handle dots vs underscores)
  const normalizedModule = parsed.moduleName?.replace(/_/g, '.') || '';
  const targetModule = moduleName.replace(/_/g, '.');
  
  return normalizedModule === targetModule && parsed.entityName === entityName;
}

/**
 * Validate a single contract's fields
 * Returns object with arrays of missing critical and important fields
 */
export function validateContractFields(contract) {
  const missingCritical = [];
  const missingImportant = [];
  
  for (const field of CRITICAL_CONTRACT_FIELDS) {
    const value = contract[field];
    if (value === null || value === undefined || value === '') {
      missingCritical.push(field);
    }
  }
  
  for (const field of IMPORTANT_CONTRACT_FIELDS) {
    const value = contract[field];
    const isEmpty = value === null || value === undefined || value === '' ||
      (Array.isArray(value) && value.length === 0);
    if (isEmpty) {
      missingImportant.push(field);
    }
  }
  
  return { missingCritical, missingImportant };
}

/**
 * Validate templates against expected registry
 * Returns validation report with found, missing, and unexpected templates
 */
export function validateTemplates(templateCounts) {
  const report = {
    found: [],
    missing: [],
    unexpected: [],
    formatVariations: {},
    warnings: [],
  };
  
  const normalizedCounts = {};
  
  // Normalize all template IDs and track format variations
  for (const [templateId, count] of Object.entries(templateCounts)) {
    const format = detectTemplateFormat(templateId);
    const key = normalizeTemplateKey(templateId);
    
    if (!normalizedCounts[key]) {
      normalizedCounts[key] = { count: 0, rawIds: [], formats: new Set() };
    }
    normalizedCounts[key].count += count;
    normalizedCounts[key].rawIds.push(templateId);
    normalizedCounts[key].formats.add(format);
    
    // Track format variations
    if (!report.formatVariations[format]) {
      report.formatVariations[format] = 0;
    }
    report.formatVariations[format]++;
  }
  
  // Check expected templates
  for (const [expectedKey, config] of Object.entries(EXPECTED_TEMPLATES)) {
    const normalized = normalizedCounts[expectedKey];
    
    if (normalized) {
      report.found.push({
        key: expectedKey,
        count: normalized.count,
        rawIds: normalized.rawIds,
        formats: [...normalized.formats],
        description: config.description,
      });
    } else if (config.required) {
      report.missing.push({
        key: expectedKey,
        required: true,
        description: config.description,
      });
      report.warnings.push(`⚠️  MISSING REQUIRED: ${expectedKey} (${config.description})`);
    } else {
      report.missing.push({
        key: expectedKey,
        required: false,
        description: config.description,
      });
    }
  }
  
  // Find unexpected templates (not in registry)
  for (const [key, data] of Object.entries(normalizedCounts)) {
    if (!EXPECTED_TEMPLATES[key]) {
      report.unexpected.push({
        key,
        count: data.count,
        rawIds: data.rawIds,
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
 * Get partition path for ACS snapshots (by date, time, and migration)
 * Each snapshot gets its own partition to preserve historical snapshots
 * Uses Hive-compliant key=value folder names for BigQuery compatibility
 * 
 * IMPORTANT: Partition values are numeric (not zero-padded strings) to ensure
 * BigQuery/DuckDB infer them as INT64 rather than STRING/BYTE_ARRAY.
 * Only snapshot_id uses padding since it's meant to be a string identifier.
 * 
 * Structure: raw/acs/migration=X/year=YYYY/month=M/day=D/snapshot_id=HHMMSS/
 * Note: The path does NOT include a leading "acs/" since that would cause
 * double-nesting when combined with base paths that already include "acs/".
 */
export function getACSPartitionPath(timestamp, migrationId = null) {
  const d = new Date(timestamp);
  const year = d.getFullYear();
  const month = d.getMonth() + 1;  // 1-12, no padding for INT64 inference
  const day = d.getDate();          // 1-31, no padding for INT64 inference
  const hour = String(d.getHours()).padStart(2, '0');
  const minute = String(d.getMinutes()).padStart(2, '0');
  const second = String(d.getSeconds()).padStart(2, '0');
  
  // Include full timestamp with seconds to keep each snapshot separate
  // snapshot_id is padded because it's a string identifier, not a numeric partition
  const snapshotId = `${hour}${minute}${second}`;
  
  // Always include migration in path for consistency with backfill structure
  const mig = migrationId ?? 0;
  return `acs/migration=${mig}/year=${year}/month=${month}/day=${day}/snapshot_id=${snapshotId}`;
}
