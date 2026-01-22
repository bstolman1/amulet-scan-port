/**
 * ACS Pipeline Integration Tests
 * 
 * End-to-end tests that run actual ACS pipeline components against mock data
 * to verify data integrity from API response through normalization to output.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  normalizeACSContract,
  parseTemplateId,
  normalizeTemplateKey,
  isTemplate,
  validateTemplates,
  validateContractFields,
  getACSPartitionPath,
  ACSValidationError,
} from '../acs-schema.js';
import {
  MOCK_ACS_AMULET,
  MOCK_ACS_VALIDATOR_LICENSE,
  MOCK_ACS_LOCKED_AMULET,
  MOCK_ACS_DSO_RULES,
  MOCK_ACS_VOTE_REQUEST,
  MOCK_ACS_BATCH,
  createACSResponse,
} from './fixtures/mock-api-responses.js';

describe('ACS Pipeline Integration', () => {
  const MOCK_MIGRATION_ID = 0;
  const MOCK_RECORD_TIME = '2024-06-15T10:30:00.000Z';
  const MOCK_SNAPSHOT_TIME = '2024-06-15T12:00:00.000Z';
  
  describe('Contract Normalization', () => {
    it('should normalize Amulet contract with all fields', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_AMULET,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      // Core fields
      expect(normalized.contract_id).toBe('00acs001::amulet-1');
      expect(normalized.event_id).toBe('acs-evt-001');
      expect(normalized.template_id).toBe('splice-amulet:Splice.Amulet:Amulet');
      expect(normalized.package_name).toBe('splice-amulet');
      expect(normalized.module_name).toBe('Splice.Amulet');
      expect(normalized.entity_name).toBe('Amulet');
      expect(normalized.migration_id).toBe(0);
      
      // Timestamps
      expect(normalized.record_time).toBeInstanceOf(Date);
      expect(normalized.snapshot_time).toBeInstanceOf(Date);
      
      // Parties
      expect(normalized.signatories).toEqual(['DSO::owner-party']);
      expect(normalized.observers).toEqual(['witness-party']);
      
      // Payload preserved
      const payload = JSON.parse(normalized.payload);
      expect(payload.owner).toBe('DSO::owner-party');
      expect(payload.amount.initialAmount).toBe('2500000000');
      
      // Raw preserved
      const raw = JSON.parse(normalized.raw);
      expect(raw.witness_parties).toEqual(['witness-party']);
      expect(raw.contract_key).toEqual({ owner: 'DSO::owner-party' });
    });
    
    it('should normalize ValidatorLicense contract', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_VALIDATOR_LICENSE,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      expect(normalized.template_id).toBe('splice-validator-license:Splice.ValidatorLicense:ValidatorLicense');
      expect(normalized.module_name).toBe('Splice.ValidatorLicense');
      expect(normalized.entity_name).toBe('ValidatorLicense');
      
      const payload = JSON.parse(normalized.payload);
      expect(payload.validator).toBe('validator-party');
      expect(payload.validatorVersion).toBe('0.3.1');
    });
    
    it('should normalize LockedAmulet with nested amount', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_LOCKED_AMULET,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      expect(normalized.entity_name).toBe('LockedAmulet');
      
      const payload = JSON.parse(normalized.payload);
      expect(payload.amulet.amount.initialAmount).toBe('500000000');
      expect(payload.lock.holders).toEqual(['lock-holder-party']);
    });
    
    it('should normalize DsoRules contract', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_DSO_RULES,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      expect(normalized.template_id).toBe('splice-dso:Splice.DsoRules:DsoRules');
      expect(normalized.module_name).toBe('Splice.DsoRules');
      expect(normalized.entity_name).toBe('DsoRules');
      
      const payload = JSON.parse(normalized.payload);
      expect(payload.svs).toHaveLength(3);
    });
    
    it('should normalize VoteRequest with governance data', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_VOTE_REQUEST,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      expect(normalized.entity_name).toBe('VoteRequest');
      
      const payload = JSON.parse(normalized.payload);
      expect(payload.requestor).toBe('sv-1');
      expect(payload.action.tag).toBe('ARC_DsoRules');
      expect(payload.votes).toHaveLength(1);
      expect(payload.votes[0].accept).toBe(true);
    });
  });
  
  describe('Template Matching', () => {
    it('should match Amulet template correctly', () => {
      expect(isTemplate(MOCK_ACS_AMULET, 'Splice.Amulet', 'Amulet')).toBe(true);
      expect(isTemplate(MOCK_ACS_AMULET, 'Splice.Amulet', 'LockedAmulet')).toBe(false);
    });
    
    it('should match LockedAmulet template', () => {
      expect(isTemplate(MOCK_ACS_LOCKED_AMULET, 'Splice.Amulet', 'LockedAmulet')).toBe(true);
      expect(isTemplate(MOCK_ACS_LOCKED_AMULET, 'Splice.Amulet', 'Amulet')).toBe(false);
    });
    
    it('should match ValidatorLicense template', () => {
      expect(isTemplate(MOCK_ACS_VALIDATOR_LICENSE, 'Splice.ValidatorLicense', 'ValidatorLicense')).toBe(true);
    });
    
    it('should match DsoRules template', () => {
      expect(isTemplate(MOCK_ACS_DSO_RULES, 'Splice.DsoRules', 'DsoRules')).toBe(true);
    });
    
    it('should match VoteRequest template', () => {
      expect(isTemplate(MOCK_ACS_VOTE_REQUEST, 'Splice.DsoRules', 'VoteRequest')).toBe(true);
    });
  });
  
  describe('Batch Processing', () => {
    it('should process entire ACS batch without data loss', () => {
      const contracts = [];
      
      for (const event of MOCK_ACS_BATCH) {
        const normalized = normalizeACSContract(
          event,
          MOCK_MIGRATION_ID,
          MOCK_RECORD_TIME,
          MOCK_SNAPSHOT_TIME
        );
        contracts.push(normalized);
      }
      
      expect(contracts).toHaveLength(5);
      
      // Verify all have contract_id
      expect(contracts.every(c => c.contract_id != null)).toBe(true);
      
      // Verify all have template_id
      expect(contracts.every(c => c.template_id != null)).toBe(true);
      
      // Verify all have raw preserved
      expect(contracts.every(c => c.raw != null)).toBe(true);
      
      // Verify all have payload preserved
      expect(contracts.every(c => c.payload != null)).toBe(true);
    });
    
    it('should generate valid template counts for validation', () => {
      const templateCounts = {};
      
      for (const event of MOCK_ACS_BATCH) {
        const templateId = event.template_id;
        templateCounts[templateId] = (templateCounts[templateId] || 0) + 1;
      }
      
      expect(Object.keys(templateCounts)).toHaveLength(5);
      
      const validation = validateTemplates(templateCounts);
      
      // Should find expected templates
      expect(validation.found.length).toBeGreaterThan(0);
      
      // All our mock templates should be in expected registry
      expect(validation.unexpected).toHaveLength(0);
    });
    
    it('should calculate Amulet and LockedAmulet totals correctly', () => {
      let amuletTotal = 0n;
      let lockedTotal = 0n;
      
      for (const event of MOCK_ACS_BATCH) {
        const normalized = normalizeACSContract(event, MOCK_MIGRATION_ID, MOCK_RECORD_TIME, MOCK_SNAPSHOT_TIME);
        const payload = JSON.parse(normalized.payload);
        
        if (isTemplate(event, 'Splice.Amulet', 'Amulet')) {
          amuletTotal += BigInt(payload.amount?.initialAmount || '0');
        } else if (isTemplate(event, 'Splice.Amulet', 'LockedAmulet')) {
          lockedTotal += BigInt(payload.amulet?.amount?.initialAmount || '0');
        }
      }
      
      expect(amuletTotal).toBe(2500000000n);
      expect(lockedTotal).toBe(500000000n);
    });
  });
  
  describe('Field Validation', () => {
    it('should validate contract with all fields', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_AMULET,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      const { missingCritical, missingImportant } = validateContractFields(normalized);
      
      expect(missingCritical).toHaveLength(0);
      expect(missingImportant).toHaveLength(0);
    });
    
    it('should detect missing critical fields', () => {
      const incompleteEvent = {
        event_id: 'incomplete-001',
        // Missing contract_id, template_id
      };
      
      // In non-strict mode, normalization fills in defaults
      const normalized = normalizeACSContract(
        incompleteEvent,
        null, // No migration_id
        null, // No record_time
        MOCK_SNAPSHOT_TIME
      );
      
      const { missingCritical } = validateContractFields(normalized);
      
      // contract_id and template_id might be inferred, but migration_id and record_time are null
      expect(missingCritical).toContain('migration_id');
      expect(missingCritical).toContain('record_time');
    });
    
    it('should throw ACSValidationError in strict mode for missing critical fields', () => {
      const incompleteEvent = {
        event_id: 'incomplete-001',
        // Missing contract_id, template_id
      };
      
      expect(() => normalizeACSContract(
        incompleteEvent,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME,
        { strict: true }
      )).toThrow(ACSValidationError);
    });
  });
  
  describe('Template ID Parsing', () => {
    it('should parse colon-dot format', () => {
      const result = parseTemplateId('splice-amulet:Splice.Amulet:Amulet');
      
      expect(result.packageName).toBe('splice-amulet');
      expect(result.moduleName).toBe('Splice.Amulet');
      expect(result.entityName).toBe('Amulet');
    });
    
    it('should parse underscore format', () => {
      // Underscore format parsing pops entity, then module, joins rest as package
      // Use 3-part format to align with acs-schema.test.js expectations
      const result = parseTemplateId('hash_Splice_Amulet');
      
      expect(result.packageName).toBe('hash');
      expect(result.moduleName).toBe('Splice');
      expect(result.entityName).toBe('Amulet');
    });
    
    it('should normalize template keys for comparison', () => {
      const key1 = normalizeTemplateKey('splice-amulet:Splice.Amulet:Amulet');
      const key2 = normalizeTemplateKey('other-hash:Splice.Amulet:Amulet');
      
      expect(key1).toBe('Splice.Amulet:Amulet');
      expect(key2).toBe('Splice.Amulet:Amulet');
      expect(key1).toBe(key2);
    });
  });
  
  describe('Partition Path Generation', () => {
    it('should generate correct ACS partition path', () => {
      const path = getACSPartitionPath(MOCK_SNAPSHOT_TIME, MOCK_MIGRATION_ID);
      
      expect(path).toBe('acs/migration=0/year=2024/month=6/day=15/snapshot_id=120000');
    });
    
    it('should use numeric (unpadded) month and day', () => {
      const path = getACSPartitionPath('2024-01-05T08:05:09Z', 0);
      
      expect(path).toContain('month=1');
      expect(path).toContain('day=5');
      expect(path).not.toContain('month=01');
      expect(path).not.toContain('day=05');
    });
    
    it('should generate unique snapshot_id per timestamp', () => {
      const path1 = getACSPartitionPath('2024-06-15T10:30:00Z', 0);
      const path2 = getACSPartitionPath('2024-06-15T10:30:01Z', 0);
      const path3 = getACSPartitionPath('2024-06-15T10:31:00Z', 0);
      
      expect(path1).toContain('snapshot_id=103000');
      expect(path2).toContain('snapshot_id=103001');
      expect(path3).toContain('snapshot_id=103100');
    });
    
    it('should handle different migrations', () => {
      const path0 = getACSPartitionPath(MOCK_SNAPSHOT_TIME, 0);
      const path1 = getACSPartitionPath(MOCK_SNAPSHOT_TIME, 1);
      
      expect(path0).toContain('migration=0');
      expect(path1).toContain('migration=1');
      expect(path0).not.toBe(path1);
    });
  });
  
  describe('Data Preservation', () => {
    it('should preserve witness_parties in raw', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_AMULET,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      const raw = JSON.parse(normalized.raw);
      expect(raw.witness_parties).toEqual(['witness-party']);
    });
    
    it('should preserve contract_key in raw', () => {
      const normalized = normalizeACSContract(
        MOCK_ACS_AMULET,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      const raw = JSON.parse(normalized.raw);
      expect(raw.contract_key).toEqual({ owner: 'DSO::owner-party' });
    });
    
    it('should preserve unknown future fields in raw', () => {
      const eventWithUnknown = {
        ...MOCK_ACS_AMULET,
        future_api_field: 'should_be_preserved',
        nested_unknown: { deeply: { nested: 'value' } },
      };
      
      const normalized = normalizeACSContract(
        eventWithUnknown,
        MOCK_MIGRATION_ID,
        MOCK_RECORD_TIME,
        MOCK_SNAPSHOT_TIME
      );
      
      const raw = JSON.parse(normalized.raw);
      expect(raw.future_api_field).toBe('should_be_preserved');
      expect(raw.nested_unknown.deeply.nested).toBe('value');
    });
  });
  
  describe('API Response Handling', () => {
    it('should process paginated ACS response', () => {
      const page1 = createACSResponse(MOCK_ACS_BATCH.slice(0, 3), 'cursor-page-2');
      const page2 = createACSResponse(MOCK_ACS_BATCH.slice(3), null);
      
      const allContracts = [];
      
      // Simulate pagination
      for (const event of page1.created_events) {
        const normalized = normalizeACSContract(event, MOCK_MIGRATION_ID, MOCK_RECORD_TIME, MOCK_SNAPSHOT_TIME);
        allContracts.push(normalized);
      }
      
      expect(page1.next_page_token).toBe('cursor-page-2');
      
      for (const event of page2.created_events) {
        const normalized = normalizeACSContract(event, MOCK_MIGRATION_ID, MOCK_RECORD_TIME, MOCK_SNAPSHOT_TIME);
        allContracts.push(normalized);
      }
      
      expect(page2.next_page_token).toBeNull();
      expect(allContracts).toHaveLength(5);
    });
    
    it('should deduplicate contracts by contract_id', () => {
      const duplicateEvents = [
        MOCK_ACS_AMULET,
        MOCK_ACS_AMULET, // Duplicate
        MOCK_ACS_VALIDATOR_LICENSE,
      ];
      
      const seen = new Set();
      const uniqueContracts = [];
      
      for (const event of duplicateEvents) {
        const id = event.contract_id || event.event_id;
        if (seen.has(id)) continue;
        seen.add(id);
        
        const normalized = normalizeACSContract(event, MOCK_MIGRATION_ID, MOCK_RECORD_TIME, MOCK_SNAPSHOT_TIME);
        uniqueContracts.push(normalized);
      }
      
      expect(uniqueContracts).toHaveLength(2);
    });
  });
});
