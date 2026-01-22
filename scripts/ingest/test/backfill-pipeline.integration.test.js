/**
 * Backfill Pipeline Integration Tests
 * 
 * End-to-end tests that run actual pipeline components against mock data
 * to verify data integrity from API response through normalization to output.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder, getPartitionPath } from '../data-schema.js';
import {
  MOCK_BACKFILL_TRANSACTION,
  MOCK_BACKFILL_EXERCISE,
  MOCK_BACKFILL_REASSIGNMENT,
  MOCK_GOVERNANCE_UPDATE,
  MOCK_BACKFILL_BATCH,
  createBackfillResponse,
} from './fixtures/mock-api-responses.js';

describe('Backfill Pipeline Integration', () => {
  
  describe('Transaction Processing', () => {
    it('should normalize transaction update with all metadata', () => {
      const normalized = normalizeUpdate(MOCK_BACKFILL_TRANSACTION);
      
      // Core fields
      expect(normalized.update_id).toBe('upd-txn-001');
      expect(normalized.update_type).toBe('transaction');
      expect(normalized.migration_id).toBe(0);
      expect(normalized.synchronizer_id).toBe('sync-global-001');
      
      // Optional fields
      expect(normalized.workflow_id).toBe('wf-001');
      expect(normalized.command_id).toBe('cmd-001');
      expect(normalized.offset).toBe(12345);
      
      // Timestamps
      expect(normalized.record_time).toBeInstanceOf(Date);
      expect(normalized.effective_at).toBeInstanceOf(Date);
      expect(normalized.recorded_at).toBeInstanceOf(Date);
      
      // Tree structure
      expect(normalized.root_event_ids).toEqual(['upd-txn-001:0', 'upd-txn-001:1']);
      expect(normalized.event_count).toBe(2);
      
      // Trace context preserved
      const traceContext = JSON.parse(normalized.trace_context);
      expect(traceContext.traceId).toBe('trace-abc');
      
      // Full update data preserved
      const updateData = JSON.parse(normalized.update_data);
      expect(updateData.events_by_id).toBeDefined();
    });
    
    it('should extract all events from transaction in tree order', () => {
      const update = MOCK_BACKFILL_TRANSACTION.transaction;
      const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids);
      
      expect(events).toHaveLength(2);
      expect(events[0].event_id).toBe('upd-txn-001:0');
      expect(events[1].event_id).toBe('upd-txn-001:1');
    });
    
    it('should normalize created events with full payload', () => {
      const update = MOCK_BACKFILL_TRANSACTION.transaction;
      const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids);
      
      const normalized = normalizeEvent(
        events[0],
        update.update_id,
        MOCK_BACKFILL_TRANSACTION.migration_id,
        events[0],
        { synchronizer_id: update.synchronizer_id, record_time: update.record_time }
      );
      
      // Core fields
      expect(normalized.event_id).toBe('upd-txn-001:0');
      expect(normalized.update_id).toBe('upd-txn-001');
      expect(normalized.event_type).toBe('created');
      expect(normalized.contract_id).toBe('00abc123::amulet-contract-1');
      expect(normalized.template_id).toBe('splice-amulet:Splice.Amulet:Amulet');
      expect(normalized.package_name).toBe('splice-amulet');
      
      // Parties
      expect(normalized.signatories).toEqual(['DSO::party1']);
      expect(normalized.observers).toEqual(['party2']);
      
      // Payload preserved
      const payload = JSON.parse(normalized.payload);
      expect(payload.owner).toBe('DSO::party1');
      expect(payload.amount.initialAmount).toBe('1000000000');
      
      // Raw event preserved
      const rawEvent = JSON.parse(normalized.raw_event);
      expect(rawEvent.created_event).toBeDefined();
    });
  });
  
  describe('Exercise Event Processing', () => {
    it('should normalize exercised events with choice data', () => {
      const update = MOCK_BACKFILL_EXERCISE.transaction;
      const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids);
      
      // First event should be the exercised event
      const exercised = events.find(e => e.exercised_event);
      const normalized = normalizeEvent(
        exercised,
        update.update_id,
        MOCK_BACKFILL_EXERCISE.migration_id,
        exercised,
        { synchronizer_id: update.synchronizer_id }
      );
      
      expect(normalized.event_type).toBe('exercised');
      expect(normalized.choice).toBe('Amulet_Transfer');
      expect(normalized.consuming).toBe(true);
      expect(normalized.acting_parties).toEqual(['DSO::party1']);
      expect(normalized.child_event_ids).toEqual(['upd-txn-002:1', 'upd-txn-002:2']);
      
      // Choice argument as payload
      const payload = JSON.parse(normalized.payload);
      expect(payload.recipient).toBe('party2');
      expect(payload.amount).toBe('500000000');
      
      // Exercise result
      const result = JSON.parse(normalized.exercise_result);
      expect(result.success).toBe(true);
    });
    
    it('should process child events (archived + created)', () => {
      const update = MOCK_BACKFILL_EXERCISE.transaction;
      const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids);
      
      // Should have 3 events: exercised, archived, created
      expect(events).toHaveLength(3);
      
      // Check archived event
      const archived = events.find(e => e.archived_event);
      const normalizedArchived = normalizeEvent(
        archived,
        update.update_id,
        MOCK_BACKFILL_EXERCISE.migration_id,
        archived,
        {}
      );
      expect(normalizedArchived.event_type).toBe('archived');
      expect(normalizedArchived.contract_id).toBe('00abc123::amulet-contract-1');
      
      // Check created event (new amulet)
      const created = events.find(e => e.created_event && e.event_id === 'upd-txn-002:2');
      const normalizedCreated = normalizeEvent(
        created,
        update.update_id,
        MOCK_BACKFILL_EXERCISE.migration_id,
        created,
        {}
      );
      expect(normalizedCreated.event_type).toBe('created');
      expect(normalizedCreated.contract_id).toBe('00abc123::amulet-contract-2');
      expect(normalizedCreated.signatories).toEqual(['party2']);
    });
  });
  
  describe('Reassignment Processing', () => {
    it('should normalize reassignment update', () => {
      const normalized = normalizeUpdate(MOCK_BACKFILL_REASSIGNMENT);
      
      expect(normalized.update_id).toBe('upd-reassign-001');
      expect(normalized.update_type).toBe('reassignment');
      expect(normalized.kind).toBe('assign');
      expect(normalized.source_synchronizer).toBe('sync-source-001');
      expect(normalized.target_synchronizer).toBe('sync-target-001');
      expect(normalized.unassign_id).toBe('unassign-123');
      expect(normalized.submitter).toBe('party1');
      expect(normalized.reassignment_counter).toBe(5);
    });
  });
  
  describe('Governance Event Processing', () => {
    it('should process VoteRequest creation', () => {
      const update = MOCK_GOVERNANCE_UPDATE.transaction;
      const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids);
      
      const normalized = normalizeEvent(
        events[0],
        update.update_id,
        MOCK_GOVERNANCE_UPDATE.migration_id,
        events[0],
        { synchronizer_id: update.synchronizer_id }
      );
      
      expect(normalized.template_id).toBe('splice-dso:Splice.DsoRules:VoteRequest');
      
      const payload = JSON.parse(normalized.payload);
      expect(payload.requestor).toBe('sv-party-1');
      expect(payload.action.tag).toBe('ARC_AmuletRules');
      expect(payload.reason.url).toBe('https://governance.example.com/proposal/1');
    });
  });
  
  describe('Batch Processing', () => {
    it('should process entire batch without data loss', () => {
      const updates = [];
      const events = [];
      
      for (const raw of MOCK_BACKFILL_BATCH) {
        const normalizedUpdate = normalizeUpdate(raw);
        updates.push(normalizedUpdate);
        
        const update = raw.transaction || raw.reassignment || raw;
        if (update.events_by_id) {
          const rootIds = update.root_event_ids || [];
          const flatEvents = flattenEventsInTreeOrder(update.events_by_id, rootIds);
          
          for (const event of flatEvents) {
            const normalizedEvent = normalizeEvent(
              event,
              normalizedUpdate.update_id,
              normalizedUpdate.migration_id,
              event,
              { synchronizer_id: normalizedUpdate.synchronizer_id }
            );
            events.push(normalizedEvent);
          }
        }
      }
      
      // Verify counts
      expect(updates).toHaveLength(4);
      expect(events).toHaveLength(6); // 2 + 3 + 0 + 1
      
      // Verify update types
      const types = updates.map(u => u.update_type);
      expect(types.filter(t => t === 'transaction')).toHaveLength(3);
      expect(types.filter(t => t === 'reassignment')).toHaveLength(1);
      
      // Verify no null IDs
      expect(updates.every(u => u.update_id != null)).toBe(true);
      expect(events.every(e => e.event_id != null)).toBe(true);
      
      // Verify all events have raw_event preserved
      expect(events.every(e => e.raw_event != null)).toBe(true);
      
      // Verify all updates have update_data preserved
      expect(updates.every(u => u.update_data != null)).toBe(true);
    });
    
    it('should maintain referential integrity (event.update_id matches update)', () => {
      const updateIds = new Set();
      const eventUpdateRefs = new Set();
      
      for (const raw of MOCK_BACKFILL_BATCH) {
        const normalizedUpdate = normalizeUpdate(raw);
        updateIds.add(normalizedUpdate.update_id);
        
        const update = raw.transaction || raw.reassignment || raw;
        if (update.events_by_id) {
          const flatEvents = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids || []);
          for (const event of flatEvents) {
            const normalizedEvent = normalizeEvent(event, normalizedUpdate.update_id, normalizedUpdate.migration_id, event, {});
            eventUpdateRefs.add(normalizedEvent.update_id);
          }
        }
      }
      
      // All event update_ids should reference valid updates
      for (const ref of eventUpdateRefs) {
        expect(updateIds.has(ref)).toBe(true);
      }
    });
  });
  
  describe('Partition Path Generation', () => {
    it('should generate correct paths for batch items', () => {
      const paths = new Set();
      
      for (const raw of MOCK_BACKFILL_BATCH) {
        const update = raw.transaction || raw.reassignment || raw;
        const recordTime = update.record_time;
        const migrationId = raw.migration_id;
        
        const path = getPartitionPath(recordTime, migrationId, 'updates');
        paths.add(path);
      }
      
      // All items are from same date, should have same partition
      expect(paths.size).toBe(1);
      expect([...paths][0]).toBe('backfill/updates/migration=0/year=2024/month=6/day=15');
    });
    
    it('should separate items by migration', () => {
      const path0 = getPartitionPath('2024-06-15T10:00:00Z', 0, 'updates');
      const path1 = getPartitionPath('2024-06-15T10:00:00Z', 1, 'updates');
      
      expect(path0).toContain('migration=0');
      expect(path1).toContain('migration=1');
      expect(path0).not.toBe(path1);
    });
  });
  
  describe('Error Handling', () => {
    it('should throw SchemaValidationError for unknown update type in strict mode', () => {
      const invalidUpdate = { update_id: 'invalid-001', unknown_field: true };
      
      expect(() => normalizeUpdate(invalidUpdate, { strict: true }))
        .toThrow('Unknown update_type');
    });
    
    it('should warn but not throw in warnOnly mode', () => {
      const invalidUpdate = { update_id: 'invalid-001', unknown_field: true };
      const consoleSpy = { warn: [] };
      const originalWarn = console.warn;
      console.warn = (msg) => consoleSpy.warn.push(msg);
      
      const result = normalizeUpdate(invalidUpdate, { strict: true, warnOnly: true });
      
      console.warn = originalWarn;
      
      expect(result.update_type).toBe('unknown');
      expect(consoleSpy.warn.length).toBeGreaterThan(0);
    });
    
    it('should handle events with missing event_id gracefully', () => {
      const eventMissingId = {
        created_event: {
          contract_id: 'test-contract',
          template_id: 'test:Template',
        },
      };
      
      const consoleSpy = { warn: [] };
      const originalWarn = console.warn;
      console.warn = (msg) => consoleSpy.warn.push(msg);
      
      const normalized = normalizeEvent(eventMissingId, 'upd-test', 0, eventMissingId, {});
      
      console.warn = originalWarn;
      
      expect(normalized.event_id).toBeNull();
      expect(normalized.contract_id).toBe('test-contract');
      expect(consoleSpy.warn.length).toBeGreaterThan(0);
    });
  });
  
  describe('Data Preservation', () => {
    it('should preserve unknown fields in raw_event and update_data', () => {
      const updateWithUnknown = {
        ...MOCK_BACKFILL_TRANSACTION,
        transaction: {
          ...MOCK_BACKFILL_TRANSACTION.transaction,
          future_field: 'preserved',
          nested_unknown: { deeply: { nested: 'value' } },
        },
      };
      
      const normalized = normalizeUpdate(updateWithUnknown);
      const updateData = JSON.parse(normalized.update_data);
      
      expect(updateData.future_field).toBe('preserved');
      expect(updateData.nested_unknown.deeply.nested).toBe('value');
    });
    
    it('should preserve witness_parties in raw_event', () => {
      const eventWithWitness = {
        created_event: {
          event_id: 'evt-witness-test',
          contract_id: 'contract-test',
          template_id: 'test:Template',
          signatories: ['party1'],
          observers: ['party2'],
          witness_parties: ['witness1', 'witness2'],
          create_arguments: { test: true },
        },
      };
      
      const normalized = normalizeEvent(eventWithWitness, 'upd-test', 0, eventWithWitness, {});
      const rawEvent = JSON.parse(normalized.raw_event);
      
      expect(rawEvent.created_event.witness_parties).toEqual(['witness1', 'witness2']);
    });
  });
});
