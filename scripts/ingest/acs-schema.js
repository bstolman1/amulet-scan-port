/**
 * ACS (Active Contract Set) Schema Definitions for Parquet Storage
 * 
 * Defines the structure for storing ACS snapshot data locally.
 */

export const ACS_CONTRACTS_SCHEMA = {
  contract_id: 'STRING',
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
};

export const ACS_COLUMNS = [
  'contract_id',
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
];

/**
 * Parse template ID into components
 */
export function parseTemplateId(templateId) {
  if (!templateId) return { packageName: null, moduleName: null, entityName: null };
  
  const parts = templateId.split(':');
  const entityName = parts.pop() || null;
  const moduleName = parts.pop() || null;
  const packageName = parts.join(':') || null;
  
  return { packageName, moduleName, entityName };
}

/**
 * Normalize an ACS contract event for storage
 */
export function normalizeACSContract(event, migrationId, recordTime, snapshotTime) {
  const templateId = event.template_id || 'unknown';
  const { packageName, moduleName, entityName } = parseTemplateId(templateId);
  
  return {
    contract_id: event.contract_id || event.event_id,
    template_id: templateId,
    package_name: packageName,
    module_name: moduleName,
    entity_name: entityName,
    migration_id: parseInt(migrationId) || null,
    record_time: recordTime ? new Date(recordTime) : null,
    snapshot_time: snapshotTime ? new Date(snapshotTime) : new Date(),
    signatories: event.signatories || [],
    observers: event.observers || [],
    payload: event.create_arguments ? JSON.stringify(event.create_arguments) : null,
  };
}

/**
 * Check if event matches a specific template
 */
export function isTemplate(event, moduleName, entityName) {
  const templateId = event?.template_id;
  if (!templateId) return false;
  
  const parts = templateId.split(':');
  const entity = parts.pop();
  const module = parts.pop();
  
  return module === moduleName && entity === entityName;
}

/**
 * Get partition path for ACS snapshots (by date and migration)
 */
export function getACSPartitionPath(timestamp, migrationId = null) {
  const d = new Date(timestamp);
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  
  // Include migration_id in path to keep migrations separate
  if (migrationId) {
    return `acs/migration=${migrationId}/year=${year}/month=${month}/day=${day}`;
  }
  return `acs/year=${year}/month=${month}/day=${day}`;
}
