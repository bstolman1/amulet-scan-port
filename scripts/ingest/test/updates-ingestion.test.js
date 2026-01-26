/**
 * Live Updates Ingestion Tests
 * 
 * Comprehensive tests for the v2/updates ingestion pipeline ensuring:
 * - JSON payload parsing correctness
 * - Schema field extraction
 * - Data source separation (updates vs backfill)
 * - Field population and type correctness
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  normalizeUpdate,
  normalizeEvent,
  flattenEventsInTreeOrder,
  getPartitionPath,
  SchemaValidationError,
  LEDGER_UPDATES_SCHEMA,
  LEDGER_EVENTS_SCHEMA,
} from '../data-schema.js';

describe('Live Updates Ingestion', () => {
  
  describe('v2/updates JSON parsing', () => {
    it('should parse transaction update from v2/updates response', () => {
      // Sample from actual v2/updates API response
      const raw = {
        migration_id: 4,
        update_id: '1220abcd1234567890abcdef',
        workflow_id: 'workflow-123',
        record_time: '2025-01-20T10:30:12.411860Z',
        synchronizer_id: 'global-domain::1220b1431ef217342db44d516bb9befde802be7d8899637d290895fa58880f19accc',
        effective_at: '2025-01-20T10:30:11.903408Z',
        offset: '000000000012345678',
        root_event_ids: ['1220abcd1234567890abcdef:0'],
        events_by_id: {
          '1220abcd1234567890abcdef:0': {
            event_type: 'created_event',
            event_id: '1220abcd1234567890abcdef:0',
            contract_id: '00cb8de49bd613f7ee386baa1ad72d0032a04a86b8b69f737c743a0c12d378a013ca',
            template_id: 'a1b2c3d4e5f6:Splice.Amulet:Amulet',
            package_name: 'splice-amulet-0.1.0',
            create_arguments: {
              owner: 'party::user123',
              amount: { initialAmount: '1000.0' },
            },
            signatories: ['party::user123'],
            observers: [],
            witness_parties: ['party::user123', 'party::sv1'],
          },
        },
        trace_context: { trace_id: 'abc123', span_id: 'def456' },
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_type).toBe('transaction');
      expect(result.update_id).toBe('1220abcd1234567890abcdef');
      expect(result.migration_id).toBe(4);
      expect(result.synchronizer_id).toContain('global-domain');
      expect(result.record_time).toBeInstanceOf(Date);
      expect(result.effective_at).toBeInstanceOf(Date);
      expect(result.offset).toBe(12345678);
      expect(result.event_count).toBe(1);
      expect(result.root_event_ids).toEqual(['1220abcd1234567890abcdef:0']);
    });

    it('should extract events from update correctly', () => {
      const raw = {
        migration_id: 4,
        update_id: 'test-update-123',
        record_time: '2025-01-20T10:30:12Z',
        synchronizer_id: 'global-domain::sync1',
        effective_at: '2025-01-20T10:30:11Z',
        root_event_ids: ['test-update-123:0', 'test-update-123:1'],
        events_by_id: {
          'test-update-123:0': {
            event_type: 'exercised_event',
            event_id: 'test-update-123:0',
            contract_id: 'contract-abc',
            template_id: 'pkg:Module:Template',
            choice: 'Transfer',
            consuming: true,
            acting_parties: ['party1'],
            choice_argument: { amount: 500 },
            child_event_ids: ['test-update-123:2'],
            exercise_result: { success: true },
          },
          'test-update-123:1': {
            event_type: 'created_event',
            event_id: 'test-update-123:1',
            contract_id: 'contract-def',
            template_id: 'pkg:Module:Template',
            signatories: ['party2'],
          },
          'test-update-123:2': {
            event_type: 'archived_event',
            event_id: 'test-update-123:2',
            contract_id: 'contract-ghi',
          },
        },
      };

      const normalized = normalizeUpdate(raw);
      const events = flattenEventsInTreeOrder(raw.events_by_id, raw.root_event_ids);
      
      expect(events).toHaveLength(3);
      expect(events[0].event_id).toBe('test-update-123:0');
      expect(events[1].event_id).toBe('test-update-123:2'); // Child of :0
      expect(events[2].event_id).toBe('test-update-123:1');
    });
  });

  describe('Schema field population', () => {
    it('should populate all LEDGER_UPDATES_SCHEMA fields', () => {
      const raw = {
        migration_id: 4,
        update_id: 'update-xyz',
        record_time: '2025-01-20T10:30:12.411860Z',
        synchronizer_id: 'global-domain::sync123',
        effective_at: '2025-01-20T10:30:11.903408Z',
        workflow_id: 'workflow-abc',
        command_id: 'command-def',
        offset: '000000000000000042',
        root_event_ids: ['update-xyz:0'],
        events_by_id: { 'update-xyz:0': {} },
        trace_context: { trace_id: 'trace1' },
      };

      const result = normalizeUpdate(raw);
      
      // Verify all schema fields are present
      const schemaFields = Object.keys(LEDGER_UPDATES_SCHEMA);
      for (const field of schemaFields) {
        expect(result).toHaveProperty(field);
      }
      
      // Verify specific values
      expect(result.update_id).toBe('update-xyz');
      expect(result.update_type).toBe('transaction');
      expect(result.migration_id).toBe(4);
      expect(result.synchronizer_id).toBe('global-domain::sync123');
      expect(result.workflow_id).toBe('workflow-abc');
      expect(result.command_id).toBe('command-def');
      expect(result.offset).toBe(42);
      expect(result.record_time).toBeInstanceOf(Date);
      expect(result.effective_at).toBeInstanceOf(Date);
      expect(result.event_count).toBe(1);
      expect(result.root_event_ids).toEqual(['update-xyz:0']);
    });

    it('should populate all LEDGER_EVENTS_SCHEMA fields for created event', () => {
      const event = {
        created_event: {
          event_id: 'event:0',
          contract_id: 'contract123',
          template_id: 'pkg:Splice.Amulet:Amulet',
          package_name: 'splice-amulet-0.1.0',
          create_arguments: { owner: 'party1', amount: { initialAmount: '100' } },
          signatories: ['party1'],
          observers: ['party2'],
          witness_parties: ['party1', 'party2'],
          created_at: '2025-01-20T10:30:11Z',
          contract_key: { key: 'value' },
        },
      };
      const updateInfo = {
        record_time: '2025-01-20T10:30:12Z',
        synchronizer_id: 'sync1',
      };

      const result = normalizeEvent(event, 'update1', 4, event, updateInfo);
      
      expect(result.event_id).toBe('event:0');
      expect(result.update_id).toBe('update1');
      expect(result.event_type).toBe('created');
      expect(result.event_type_original).toBe('created_event');
      expect(result.contract_id).toBe('contract123');
      expect(result.template_id).toBe('pkg:Splice.Amulet:Amulet');
      expect(result.package_name).toBe('splice-amulet-0.1.0');
      expect(result.migration_id).toBe(4);
      expect(result.signatories).toEqual(['party1']);
      expect(result.observers).toEqual(['party2']);
      expect(result.witness_parties).toEqual(['party1', 'party2']);
      expect(result.payload).toContain('owner');
      expect(result.contract_key).toContain('key');
      expect(result.raw_event).not.toBeNull();
    });

    it('should populate all LEDGER_EVENTS_SCHEMA fields for exercised event', () => {
      const event = {
        exercised_event: {
          event_id: 'event:0',
          contract_id: 'contract123',
          template_id: 'pkg:Splice.Amulet:Amulet',
          choice: 'Amulet_Send',
          interface_id: 'pkg:Interface:IAmulet',
          consuming: true,
          acting_parties: ['party1'],
          child_event_ids: ['event:1', 'event:2'],
          choice_argument: { recipient: 'party2', amount: 100 },
          exercise_result: { success: true, newContractId: 'contract456' },
        },
      };

      const result = normalizeEvent(event, 'update1', 4);
      
      expect(result.event_type).toBe('exercised');
      expect(result.event_type_original).toBe('exercised_event');
      expect(result.choice).toBe('Amulet_Send');
      expect(result.interface_id).toBe('pkg:Interface:IAmulet');
      expect(result.consuming).toBe(true);
      expect(result.acting_parties).toEqual(['party1']);
      expect(result.child_event_ids).toEqual(['event:1', 'event:2']);
      expect(result.payload).toContain('recipient');
      expect(result.exercise_result).toContain('success');
    });
  });

  describe('Data source separation', () => {
    it('should write to updates folder for live ingestion', () => {
      const timestamp = '2025-01-20T10:30:00Z';
      const migration = 4;
      
      const liveUpdatesPath = getPartitionPath(timestamp, migration, 'updates', 'updates');
      const liveEventsPath = getPartitionPath(timestamp, migration, 'events', 'updates');
      
      expect(liveUpdatesPath).toBe('updates/updates/migration=4/year=2025/month=1/day=20');
      expect(liveEventsPath).toBe('updates/events/migration=4/year=2025/month=1/day=20');
    });

    it('should keep backfill data separate from live updates', () => {
      const timestamp = '2025-01-20T10:30:00Z';
      
      const backfillPath = getPartitionPath(timestamp, 3, 'updates', 'backfill');
      const livePath = getPartitionPath(timestamp, 4, 'updates', 'updates');
      
      // They should be in completely different top-level folders
      expect(backfillPath.split('/')[0]).toBe('backfill');
      expect(livePath.split('/')[0]).toBe('updates');
      expect(backfillPath).not.toEqual(livePath);
    });
  });

  describe('Reassignment updates', () => {
    it('should handle reassignment update from v2/updates', () => {
      const raw = {
        migration_id: 4,
        reassignment: {
          update_id: 'reassign-123',
          synchronizer_id: 'global-domain::sync1',
          record_time: '2025-01-20T10:30:12Z',
          kind: 'assign',
          source: 'source-synchronizer-id',
          target: 'target-synchronizer-id',
          unassign_id: 'unassign-abc',
          submitter: 'party::submitter1',
          counter: 5,
        },
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_type).toBe('reassignment');
      expect(result.kind).toBe('assign');
      expect(result.source_synchronizer).toBe('source-synchronizer-id');
      expect(result.target_synchronizer).toBe('target-synchronizer-id');
      expect(result.unassign_id).toBe('unassign-abc');
      expect(result.submitter).toBe('party::submitter1');
      expect(result.reassignment_counter).toBe(5);
    });

    it('should propagate reassignment fields to events', () => {
      const event = {
        created_event: {
          event_id: 'reassign:0',
          contract_id: 'contract123',
          template_id: 'pkg:Module:Template',
        },
      };
      const updateInfo = {
        record_time: '2025-01-20T10:30:12Z',
        synchronizer_id: 'sync1',
        source: 'source-sync',
        target: 'target-sync',
        unassign_id: 'unassign-xyz',
        submitter: 'party1',
        counter: 10,
      };

      const result = normalizeEvent(event, 'reassign', 4, event, updateInfo);
      
      expect(result.source_synchronizer).toBe('source-sync');
      expect(result.target_synchronizer).toBe('target-sync');
      expect(result.unassign_id).toBe('unassign-xyz');
      expect(result.submitter).toBe('party1');
      expect(result.reassignment_counter).toBe(10);
    });
  });

  describe('Edge cases and error handling', () => {
    it('should handle empty events_by_id gracefully', () => {
      const raw = {
        migration_id: 4,
        update_id: 'empty-update',
        record_time: '2025-01-20T10:30:12Z',
        synchronizer_id: 'sync1',
        root_event_ids: [],
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_type).toBe('transaction');
      expect(result.event_count).toBe(0);
    });

    it('should reject unknown update format in strict mode', () => {
      const raw = {
        migration_id: 4,
        update_id: 'mystery-update',
        record_time: '2025-01-20T10:30:12Z',
        // No events_by_id, no transaction wrapper, no reassignment wrapper
      };

      expect(() => normalizeUpdate(raw)).toThrow(SchemaValidationError);
      expect(() => normalizeUpdate(raw)).toThrow(/Unknown update_type/);
    });

    it('should handle missing optional fields gracefully', () => {
      const raw = {
        migration_id: 4,
        update_id: 'minimal-update',
        record_time: '2025-01-20T10:30:12Z',
        synchronizer_id: 'sync1',
        events_by_id: {},
        // workflow_id, command_id, trace_context intentionally missing
      };

      const result = normalizeUpdate(raw);
      
      expect(result.workflow_id).toBeNull();
      expect(result.command_id).toBeNull();
      expect(result.trace_context).toBeNull();
    });

    it('should preserve full JSON in update_data for debugging', () => {
      const raw = {
        migration_id: 4,
        update_id: 'debug-update',
        record_time: '2025-01-20T10:30:12Z',
        synchronizer_id: 'sync1',
        events_by_id: {},
        custom_field: 'important_for_debugging',
        nested: { deep: { data: 'preserved' } },
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_data).toContain('custom_field');
      expect(result.update_data).toContain('important_for_debugging');
      expect(result.update_data).toContain('preserved');
    });

    it('should preserve full JSON in raw_event for events', () => {
      const event = {
        event_type: 'created_event',
        event_id: 'debug:0',
        contract_id: 'c1',
        custom_event_field: 'preserved_in_raw',
      };

      const result = normalizeEvent(event, 'update1', 4, event);
      
      expect(result.raw_event).toContain('custom_event_field');
      expect(result.raw_event).toContain('preserved_in_raw');
    });
  });

  describe('Timestamp handling', () => {
    it('should parse microsecond precision timestamps', () => {
      const raw = {
        migration_id: 4,
        update_id: 'timestamp-test',
        record_time: '2025-01-20T10:30:12.411860Z',
        synchronizer_id: 'sync1',
        effective_at: '2025-01-20T10:30:11.903408Z',
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.record_time).toBeInstanceOf(Date);
      expect(result.effective_at).toBeInstanceOf(Date);
      expect(result.record_time.toISOString()).toContain('2025-01-20');
    });

    it('should assume UTC for timestamps without timezone', () => {
      const event = {
        event_type: 'created_event',
        event_id: 'tz:0',
        contract_id: 'c1',
        created_at: '2025-01-20T10:30:11.903408', // No Z suffix
      };

      const result = normalizeEvent(event, 'update1', 4);
      
      expect(result.effective_at.toISOString()).toBe('2025-01-20T10:30:11.903Z');
    });
  });

  describe('Migration continuity', () => {
    it('should correctly partition across migration boundaries', () => {
      const mig3EndPath = getPartitionPath('2025-12-10T23:59:59Z', 3, 'events', 'backfill');
      const mig4StartPath = getPartitionPath('2025-12-11T00:00:01Z', 4, 'events', 'updates');
      
      expect(mig3EndPath).toContain('migration=3');
      expect(mig3EndPath).toContain('backfill/');
      
      expect(mig4StartPath).toContain('migration=4');
      expect(mig4StartPath).toContain('updates/');
    });

    it('should handle migration_id correctly in all normalized outputs', () => {
      const raw = {
        migration_id: 4,
        update_id: 'mig4-update',
        record_time: '2025-01-20T10:30:12Z',
        synchronizer_id: 'sync1',
        events_by_id: {
          'mig4-update:0': {
            event_type: 'created_event',
            contract_id: 'c1',
          },
        },
        root_event_ids: ['mig4-update:0'],
      };

      const update = normalizeUpdate(raw);
      const events = flattenEventsInTreeOrder(raw.events_by_id, raw.root_event_ids);
      const normalizedEvent = normalizeEvent(events[0], raw.update_id, raw.migration_id);
      
      expect(update.migration_id).toBe(4);
      expect(normalizedEvent.migration_id).toBe(4);
    });
  });
});
