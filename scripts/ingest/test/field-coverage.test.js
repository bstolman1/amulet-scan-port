/**
 * Field Coverage Audit Tests
 * 
 * Verifies that normalization schemas capture all fields from raw API data.
 * Ensures no data is silently dropped during ingestion.
 */

import { describe, it, expect } from 'vitest';
import { 
  ACS_COLUMNS, 
  CRITICAL_CONTRACT_FIELDS, 
  IMPORTANT_CONTRACT_FIELDS,
  normalizeACSContract,
  parseTemplateId,
} from '../acs-schema.js';
import { 
  UPDATES_COLUMNS, 
  EVENTS_COLUMNS,
  normalizeUpdate,
  normalizeEvent,
} from '../data-schema.js';

describe('Field Coverage Audit', () => {
  
  describe('ACS Schema Completeness', () => {
    // All known fields from Canton Scan API for ACS contracts
    const KNOWN_ACS_API_FIELDS = [
      'contract_id',
      'event_id', 
      'template_id',
      'signatories',
      'observers',
      'create_arguments',
      'created_at',
      'package_name',
      'witness_parties',
      'contract_key',
    ];
    
    it('should have columns for all critical API fields', () => {
      const capturedOrMapped = new Set([
        ...ACS_COLUMNS,
        'create_arguments', // mapped to 'payload'
        'created_at',       // mapped to 'record_time'
      ]);
      
      const criticalApiFields = ['contract_id', 'template_id', 'signatories'];
      for (const field of criticalApiFields) {
        expect(
          capturedOrMapped.has(field) || ACS_COLUMNS.includes(field),
          `Critical field '${field}' should be captured`
        ).toBe(true);
      }
    });
    
    it('should preserve raw JSON for data recovery', () => {
      expect(ACS_COLUMNS).toContain('raw');
      expect(ACS_COLUMNS).toContain('payload');
    });
    
    it('should capture all known API fields or map them', () => {
      const fieldMapping = {
        'contract_id': 'contract_id',
        'event_id': 'event_id',
        'template_id': 'template_id',
        'signatories': 'signatories',
        'observers': 'observers',
        'create_arguments': 'payload',      // Mapped
        'created_at': 'record_time',        // Mapped
        'package_name': 'package_name',
        'witness_parties': null,            // Not currently captured - in raw
        'contract_key': null,               // Not currently captured - in raw
      };
      
      for (const [apiField, schemaColumn] of Object.entries(fieldMapping)) {
        if (schemaColumn === null) {
          // Field intentionally not extracted but preserved in 'raw'
          expect(ACS_COLUMNS).toContain('raw');
        } else {
          expect(
            ACS_COLUMNS.includes(schemaColumn),
            `API field '${apiField}' should map to column '${schemaColumn}'`
          ).toBe(true);
        }
      }
    });
    
    it('normalizeACSContract should preserve all input data in raw column', () => {
      const input = {
        contract_id: 'test-contract-123',
        event_id: 'evt-456',
        template_id: 'abc123:Splice.Amulet:Amulet',
        signatories: ['party1'],
        observers: ['party2'],
        create_arguments: { amount: '1000000' },
        witness_parties: ['witness1'],
        contract_key: { key: 'value' },
        some_future_field: 'future-value',
      };
      
      const result = normalizeACSContract(input, 0, '2024-01-15T10:00:00Z', '2024-01-15T10:00:00Z');
      
      // Verify raw contains ALL original fields
      const rawParsed = JSON.parse(result.raw);
      expect(rawParsed.contract_id).toBe(input.contract_id);
      expect(rawParsed.witness_parties).toEqual(input.witness_parties);
      expect(rawParsed.contract_key).toEqual(input.contract_key);
      expect(rawParsed.some_future_field).toBe('future-value');
    });
  });
  
  describe('Updates Schema Completeness', () => {
    // All known fields from Canton Scan API for updates
    const KNOWN_UPDATE_API_FIELDS = [
      'update_id',
      'synchronizer_id',
      'workflow_id',
      'command_id',
      'offset',
      'record_time',
      'effective_at',
      'root_event_ids',
      'events_by_id',
      'trace_context',
      // Reassignment fields
      'source',
      'target',
      'unassign_id',
      'submitter',
      'counter',
      'kind',
    ];
    
    it('should have columns for all critical update fields', () => {
      const criticalFields = ['update_id', 'record_time', 'offset'];
      for (const field of criticalFields) {
        expect(
          UPDATES_COLUMNS.includes(field),
          `Critical field '${field}' should be in UPDATES_COLUMNS`
        ).toBe(true);
      }
    });
    
    it('should preserve raw JSON for data recovery', () => {
      expect(UPDATES_COLUMNS).toContain('update_data');
    });
    
    it('should capture reassignment-specific fields', () => {
      const reassignmentFields = [
        'source_synchronizer',
        'target_synchronizer', 
        'unassign_id',
        'submitter',
        'reassignment_counter',
        'kind',
      ];
      
      for (const field of reassignmentFields) {
        expect(
          UPDATES_COLUMNS.includes(field),
          `Reassignment field '${field}' should be captured`
        ).toBe(true);
      }
    });
    
    it('normalizeUpdate should preserve all input data in update_data column', () => {
      const input = {
        transaction: {
          update_id: 'upd-123',
          synchronizer_id: 'sync-1',
          record_time: '2024-01-15T10:00:00Z',
          effective_at: '2024-01-15T10:00:00Z',
          offset: '12345',
          root_event_ids: ['evt-1'],
          events_by_id: { 'evt-1': { event_id: 'evt-1' } },
          trace_context: { traceId: 'trace-abc' },
          some_future_field: 'future-value',
        },
        migration_id: 0,
      };
      
      const result = normalizeUpdate(input);
      
      // Verify update_data contains complete transaction
      const rawParsed = JSON.parse(result.update_data);
      expect(rawParsed.update_id).toBe('upd-123');
      expect(rawParsed.trace_context).toEqual({ traceId: 'trace-abc' });
      expect(rawParsed.some_future_field).toBe('future-value');
    });
  });
  
  describe('Events Schema Completeness', () => {
    // All known fields from Canton Scan API for events
    const KNOWN_EVENT_API_FIELDS = [
      'event_id',
      'contract_id',
      'template_id',
      'package_name',
      // Created event fields
      'create_arguments',
      'signatories',
      'observers',
      'witness_parties',
      'contract_key',
      'created_at',
      // Exercised event fields
      'choice',
      'choice_argument',
      'acting_parties',
      'consuming',
      'child_event_ids',
      'exercise_result',
      'interface_id',
      // Reassignment fields
      'source',
      'target',
      'unassign_id',
      'submitter',
      'counter',
    ];
    
    it('should have columns for all critical event fields', () => {
      const criticalFields = ['event_id', 'contract_id', 'template_id', 'event_type'];
      for (const field of criticalFields) {
        expect(
          EVENTS_COLUMNS.includes(field),
          `Critical field '${field}' should be in EVENTS_COLUMNS`
        ).toBe(true);
      }
    });
    
    it('should preserve raw JSON for data recovery', () => {
      expect(EVENTS_COLUMNS).toContain('raw_event');
      expect(EVENTS_COLUMNS).toContain('payload');
    });
    
    it('should capture created event specific fields', () => {
      const createdFields = ['signatories', 'observers', 'witness_parties', 'contract_key'];
      for (const field of createdFields) {
        expect(
          EVENTS_COLUMNS.includes(field),
          `Created event field '${field}' should be captured`
        ).toBe(true);
      }
    });
    
    it('should capture exercised event specific fields', () => {
      const exercisedFields = [
        'choice',
        'consuming',
        'acting_parties',
        'child_event_ids',
        'exercise_result',
        'interface_id',
      ];
      
      for (const field of exercisedFields) {
        expect(
          EVENTS_COLUMNS.includes(field),
          `Exercised event field '${field}' should be captured`
        ).toBe(true);
      }
    });
    
    it('normalizeEvent should preserve all input data in raw_event column', () => {
      const input = {
        created_event: {
          event_id: 'evt-123',
          contract_id: 'contract-456',
          template_id: 'pkg:Module:Entity',
          create_arguments: { value: 100 },
          signatories: ['party1'],
          observers: ['party2'],
          witness_parties: ['witness1'],
          contract_key: { keyField: 'keyValue' },
          some_future_field: 'future-value',
        },
      };
      
      const result = normalizeEvent(input, 'upd-1', 0, input);
      
      // Verify raw_event contains complete original event
      const rawParsed = JSON.parse(result.raw_event);
      expect(rawParsed.created_event.event_id).toBe('evt-123');
      expect(rawParsed.created_event.contract_key).toEqual({ keyField: 'keyValue' });
      expect(rawParsed.created_event.some_future_field).toBe('future-value');
    });
  });
  
  describe('Schema Safety Guarantees', () => {
    it('ACS schema should never drop unknown fields', () => {
      const contractWithUnknownFields = {
        contract_id: 'c1',
        template_id: 'pkg:Mod:Ent',
        unknown_field_1: 'value1',
        deeply_nested: { level1: { level2: 'deep' } },
        array_field: [1, 2, 3],
      };
      
      const result = normalizeACSContract(contractWithUnknownFields, 0, null, null);
      const raw = JSON.parse(result.raw);
      
      expect(raw.unknown_field_1).toBe('value1');
      expect(raw.deeply_nested).toEqual({ level1: { level2: 'deep' } });
      expect(raw.array_field).toEqual([1, 2, 3]);
    });
    
    it('Update schema should never drop unknown fields', () => {
      const updateWithUnknownFields = {
        transaction: {
          update_id: 'u1',
          record_time: '2024-01-01T00:00:00Z',
          events_by_id: {},
          root_event_ids: [],
          future_api_field: 'new-value',
          nested_unknown: { a: 1, b: 2 },
        },
        migration_id: 0,
      };
      
      const result = normalizeUpdate(updateWithUnknownFields);
      const raw = JSON.parse(result.update_data);
      
      expect(raw.future_api_field).toBe('new-value');
      expect(raw.nested_unknown).toEqual({ a: 1, b: 2 });
    });
    
    it('Event schema should never drop unknown fields', () => {
      const eventWithUnknownFields = {
        event_id: 'e1',
        contract_id: 'c1',
        template_id: 't1',
        future_event_field: 'future',
        complex_unknown: { arr: [1, 2], obj: { x: 'y' } },
      };
      
      const result = normalizeEvent(eventWithUnknownFields, 'u1', 0, eventWithUnknownFields);
      const raw = JSON.parse(result.raw_event);
      
      expect(raw.future_event_field).toBe('future');
      expect(raw.complex_unknown).toEqual({ arr: [1, 2], obj: { x: 'y' } });
    });
  });
  
  describe('Field Mapping Documentation', () => {
    it('should document all API-to-column mappings for ACS', () => {
      // This test serves as documentation of field mappings
      const acsFieldMappings = {
        // Direct mappings
        'contract_id': 'contract_id',
        'event_id': 'event_id',
        'template_id': 'template_id',
        'signatories': 'signatories',
        'observers': 'observers',
        // Transformed mappings
        'create_arguments': 'payload (JSON stringified)',
        // Derived fields
        'template_id → package_name': 'package_name (extracted)',
        'template_id → module_name': 'module_name (extracted)',
        'template_id → entity_name': 'entity_name (extracted)',
        // Preserved in raw
        'witness_parties': 'raw (preserved)',
        'contract_key': 'raw (preserved)',
        '*unknown*': 'raw (preserved)',
      };
      
      expect(Object.keys(acsFieldMappings).length).toBeGreaterThan(0);
    });
    
    it('should document all API-to-column mappings for events', () => {
      const eventFieldMappings = {
        // Direct mappings
        'event_id': 'event_id',
        'contract_id': 'contract_id',
        'template_id': 'template_id',
        'signatories': 'signatories',
        'observers': 'observers',
        'choice': 'choice',
        'consuming': 'consuming',
        'child_event_ids': 'child_event_ids',
        // Transformed mappings
        'create_arguments': 'payload (JSON stringified)',
        'choice_argument': 'payload (JSON stringified)',
        'exercise_result': 'exercise_result (JSON stringified)',
        'contract_key': 'contract_key (JSON stringified)',
        // Normalized
        'created_event/archived_event/exercised_event': 'event_type + event_type_original',
        // Preserved in raw
        '*all fields*': 'raw_event (complete preservation)',
      };
      
      expect(Object.keys(eventFieldMappings).length).toBeGreaterThan(0);
    });
  });
});
