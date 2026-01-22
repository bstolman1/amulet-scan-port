/**
 * Data Quality Rules Tests
 * 
 * Validates that fields are populated correctly based on update_type:
 * - Reassignment-specific fields are populated when update_type='reassignment'
 * - Reassignment-specific fields are null when update_type='transaction'
 */

import { describe, it, expect } from 'vitest';
import { normalizeUpdate, normalizeEvent } from '../data-schema.js';

// Fields that should ONLY be populated for reassignments (at update level)
const REASSIGNMENT_ONLY_UPDATE_FIELDS = [
  'source_synchronizer',
  'target_synchronizer',
  'unassign_id',
  'submitter',
  'reassignment_counter',
  'kind',
];

// Fields that should ONLY be populated for reassignments (at event level)
const REASSIGNMENT_ONLY_EVENT_FIELDS = [
  'source_synchronizer',
  'target_synchronizer',
  'unassign_id',
  'submitter',
  'reassignment_counter',
];

// Fields that should ONLY be populated for exercised events
const EXERCISED_ONLY_EVENT_FIELDS = [
  'choice',
  'acting_parties',
  'child_event_ids',
  'exercise_result',
  'consuming',
];

// Fields that should ONLY be populated for created events
const CREATED_ONLY_EVENT_FIELDS = [
  'signatories',
  'observers',
];

describe('Data Quality Rules', () => {
  describe('Update-level field population', () => {
    describe('Transaction updates', () => {
      const transactionRaw = {
        update_id: 'tx123',
        migration_id: 0,
        workflow_id: 'wf-001',
        record_time: '2024-10-07T11:30:12.411Z',
        synchronizer_id: 'global-domain::abc',
        effective_at: '2024-10-07T11:30:11.903Z',
        offset: '42',
        events_by_id: {
          'tx123:0': { event_type: 'created_event' },
          'tx123:1': { event_type: 'exercised_event' },
        },
        root_event_ids: ['tx123:0'],
      };

      it('should have update_type = "transaction"', () => {
        const result = normalizeUpdate(transactionRaw);
        expect(result.update_type).toBe('transaction');
      });

      it('should have all reassignment-specific fields as null', () => {
        const result = normalizeUpdate(transactionRaw);
        
        for (const field of REASSIGNMENT_ONLY_UPDATE_FIELDS) {
          expect(result[field], `Field ${field} should be null for transactions`).toBeNull();
        }
      });

      it('should have transaction fields populated', () => {
        const result = normalizeUpdate(transactionRaw);
        
        expect(result.update_id).toBe('tx123');
        expect(result.synchronizer_id).toBe('global-domain::abc');
        expect(result.event_count).toBe(2);
        expect(result.root_event_ids).toEqual(['tx123:0']);
        expect(result.offset).toBe(42);
      });
    });

    describe('Reassignment updates', () => {
      const reassignmentRaw = {
        migration_id: 0,
        reassignment: {
          update_id: 'reassign456',
          synchronizer_id: 'global-domain::abc',
          record_time: '2024-10-07T11:30:12.411Z',
          kind: 'assign',
          source: 'source-sync::123',
          target: 'target-sync::456',
          unassign_id: 'unassign-789',
          submitter: 'party::submitter',
          counter: 5,
        },
      };

      it('should have update_type = "reassignment"', () => {
        const result = normalizeUpdate(reassignmentRaw);
        expect(result.update_type).toBe('reassignment');
      });

      it('should have all reassignment-specific fields populated', () => {
        const result = normalizeUpdate(reassignmentRaw);
        
        expect(result.kind).toBe('assign');
        expect(result.source_synchronizer).toBe('source-sync::123');
        expect(result.target_synchronizer).toBe('target-sync::456');
        expect(result.unassign_id).toBe('unassign-789');
        expect(result.submitter).toBe('party::submitter');
        expect(result.reassignment_counter).toBe(5);
      });

      it('should handle reassignment_counter = 0 correctly', () => {
        const rawWithZeroCounter = {
          migration_id: 0,
          reassignment: {
            ...reassignmentRaw.reassignment,
            counter: 0,
          },
        };
        
        const result = normalizeUpdate(rawWithZeroCounter);
        expect(result.reassignment_counter).toBe(0);
      });

      it('should have event_count = 0 for reassignments (no events_by_id)', () => {
        const result = normalizeUpdate(reassignmentRaw);
        expect(result.event_count).toBe(0);
      });
    });

    describe('Unassign reassignment updates', () => {
      const unassignRaw = {
        migration_id: 0,
        reassignment: {
          update_id: 'unassign123',
          synchronizer_id: 'global-domain::abc',
          record_time: '2024-10-07T11:30:12.411Z',
          kind: 'unassign',
          source: 'source-sync::123',
          target: 'target-sync::456',
          unassign_id: 'unassign-self',
          submitter: 'party::actor',
          counter: 0,
        },
      };

      it('should have kind = "unassign"', () => {
        const result = normalizeUpdate(unassignRaw);
        expect(result.kind).toBe('unassign');
        expect(result.update_type).toBe('reassignment');
      });
    });
  });

  describe('Event-level field population', () => {
    describe('Created events from transactions', () => {
      const createdEvent = {
        created_event: {
          event_id: 'tx123:0',
          contract_id: 'contract-abc',
          template_id: 'pkg:Module:Template',
          create_arguments: { field: 'value' },
          signatories: ['party1'],
          observers: ['party2'],
        },
      };
      const updateInfo = {
        record_time: '2024-10-07T11:30:12.411Z',
        synchronizer_id: 'global-domain::abc',
      };

      it('should have event_type = "created"', () => {
        const result = normalizeEvent(createdEvent, 'tx123', 0, createdEvent, updateInfo);
        expect(result.event_type).toBe('created');
      });

      it('should have all reassignment-specific fields as null', () => {
        const result = normalizeEvent(createdEvent, 'tx123', 0, createdEvent, updateInfo);
        
        for (const field of REASSIGNMENT_ONLY_EVENT_FIELDS) {
          expect(result[field], `Field ${field} should be null for transaction events`).toBeNull();
        }
      });

      it('should have created-event fields populated', () => {
        const result = normalizeEvent(createdEvent, 'tx123', 0, createdEvent, updateInfo);
        
        expect(result.signatories).toEqual(['party1']);
        expect(result.observers).toEqual(['party2']);
        expect(result.contract_id).toBe('contract-abc');
      });

      it('should have exercised-event fields as null', () => {
        const result = normalizeEvent(createdEvent, 'tx123', 0, createdEvent, updateInfo);
        
        for (const field of EXERCISED_ONLY_EVENT_FIELDS) {
          expect(result[field], `Field ${field} should be null for created events`).toBeNull();
        }
      });
    });

    describe('Exercised events from transactions', () => {
      const exercisedEvent = {
        exercised_event: {
          event_id: 'tx123:1',
          contract_id: 'contract-abc',
          template_id: 'pkg:Module:Template',
          choice: 'Execute',
          choice_argument: { amount: 100 },
          consuming: true,
          acting_parties: ['party1'],
          child_event_ids: ['tx123:2'],
          exercise_result: { success: true },
        },
      };
      const updateInfo = {
        record_time: '2024-10-07T11:30:12.411Z',
        synchronizer_id: 'global-domain::abc',
      };

      it('should have event_type = "exercised"', () => {
        const result = normalizeEvent(exercisedEvent, 'tx123', 0, exercisedEvent, updateInfo);
        expect(result.event_type).toBe('exercised');
      });

      it('should have all reassignment-specific fields as null', () => {
        const result = normalizeEvent(exercisedEvent, 'tx123', 0, exercisedEvent, updateInfo);
        
        for (const field of REASSIGNMENT_ONLY_EVENT_FIELDS) {
          expect(result[field], `Field ${field} should be null for transaction events`).toBeNull();
        }
      });

      it('should have exercised-event fields populated', () => {
        const result = normalizeEvent(exercisedEvent, 'tx123', 0, exercisedEvent, updateInfo);
        
        expect(result.choice).toBe('Execute');
        expect(result.acting_parties).toEqual(['party1']);
        expect(result.child_event_ids).toEqual(['tx123:2']);
        expect(result.exercise_result).toContain('success');
        expect(result.consuming).toBe(true);
      });

      it('should have created-event fields as null', () => {
        const result = normalizeEvent(exercisedEvent, 'tx123', 0, exercisedEvent, updateInfo);
        
        for (const field of CREATED_ONLY_EVENT_FIELDS) {
          expect(result[field], `Field ${field} should be null for exercised events`).toBeNull();
        }
      });
    });

    describe('Created events from reassignments (assign)', () => {
      const assignCreatedEvent = {
        created_event: {
          event_id: 'reassign456:0',
          contract_id: 'contract-xyz',
          template_id: 'pkg:Module:Template',
          create_arguments: { owner: 'party1' },
          signatories: ['party1'],
        },
      };
      const reassignmentUpdateInfo = {
        record_time: '2024-10-07T11:30:12.411Z',
        synchronizer_id: 'global-domain::abc',
        source: 'source-sync::123',
        target: 'target-sync::456',
        unassign_id: 'unassign-789',
        submitter: 'party::submitter',
        counter: 5,
      };

      it('should have event_type = "created"', () => {
        const result = normalizeEvent(assignCreatedEvent, 'reassign456', 0, assignCreatedEvent, reassignmentUpdateInfo);
        expect(result.event_type).toBe('created');
      });

      it('should have all reassignment-specific fields populated from updateInfo', () => {
        const result = normalizeEvent(assignCreatedEvent, 'reassign456', 0, assignCreatedEvent, reassignmentUpdateInfo);
        
        expect(result.source_synchronizer).toBe('source-sync::123');
        expect(result.target_synchronizer).toBe('target-sync::456');
        expect(result.unassign_id).toBe('unassign-789');
        expect(result.submitter).toBe('party::submitter');
        expect(result.reassignment_counter).toBe(5);
      });

      it('should also have created-event fields populated', () => {
        const result = normalizeEvent(assignCreatedEvent, 'reassign456', 0, assignCreatedEvent, reassignmentUpdateInfo);
        
        expect(result.signatories).toEqual(['party1']);
        expect(result.contract_id).toBe('contract-xyz');
      });
    });

    describe('Archived events from reassignments (unassign)', () => {
      const unassignArchivedEvent = {
        archived_event: {
          event_id: 'unassign789:0',
          contract_id: 'contract-xyz',
          template_id: 'pkg:Module:Template',
        },
      };
      const unassignUpdateInfo = {
        record_time: '2024-10-07T11:30:12.411Z',
        synchronizer_id: 'global-domain::abc',
        source: 'source-sync::123',
        target: 'target-sync::456',
        unassign_id: 'unassign-self',
        submitter: 'party::actor',
        counter: 0,
      };

      it('should have event_type = "archived"', () => {
        const result = normalizeEvent(unassignArchivedEvent, 'unassign789', 0, unassignArchivedEvent, unassignUpdateInfo);
        expect(result.event_type).toBe('archived');
      });

      it('should have all reassignment-specific fields populated from updateInfo', () => {
        const result = normalizeEvent(unassignArchivedEvent, 'unassign789', 0, unassignArchivedEvent, unassignUpdateInfo);
        
        expect(result.source_synchronizer).toBe('source-sync::123');
        expect(result.target_synchronizer).toBe('target-sync::456');
        expect(result.unassign_id).toBe('unassign-self');
        expect(result.submitter).toBe('party::actor');
        expect(result.reassignment_counter).toBe(0);
      });
    });
  });

  describe('Cross-validation: field presence consistency', () => {
    it('normalizeUpdate and normalizeEvent should agree on reassignment field presence', () => {
      // For a transaction update, both update and events should have null reassignment fields
      const txRaw = {
        update_id: 'tx123',
        migration_id: 0,
        events_by_id: {
          'tx123:0': {
            event_type: 'created_event',
            event_id: 'tx123:0',
            contract_id: 'c1',
            signatories: ['p1'],
          },
        },
        root_event_ids: ['tx123:0'],
        record_time: '2024-10-07T11:30:12.411Z',
        synchronizer_id: 'sync1',
      };

      const update = normalizeUpdate(txRaw);
      const event = normalizeEvent(
        txRaw.events_by_id['tx123:0'],
        'tx123',
        0,
        txRaw.events_by_id['tx123:0'],
        { record_time: txRaw.record_time, synchronizer_id: txRaw.synchronizer_id }
      );

      // Both should have null reassignment fields
      expect(update.source_synchronizer).toBeNull();
      expect(event.source_synchronizer).toBeNull();
      expect(update.target_synchronizer).toBeNull();
      expect(event.target_synchronizer).toBeNull();
    });

    it('reassignment update and event should both have reassignment fields populated', () => {
      const reassignRaw = {
        migration_id: 0,
        reassignment: {
          update_id: 'reassign123',
          synchronizer_id: 'sync1',
          record_time: '2024-10-07T11:30:12.411Z',
          kind: 'assign',
          source: 'source-sync',
          target: 'target-sync',
          unassign_id: 'unassign-abc',
          submitter: 'party1',
          counter: 3,
        },
      };

      // Mock a created event that would be part of this reassignment
      const createdEvent = {
        created_event: {
          event_id: 'reassign123:0',
          contract_id: 'c1',
          template_id: 'pkg:Mod:Tmpl',
        },
      };

      const update = normalizeUpdate(reassignRaw);
      const event = normalizeEvent(
        createdEvent,
        'reassign123',
        0,
        createdEvent,
        reassignRaw.reassignment // Pass the reassignment wrapper as updateInfo
      );

      // Both should have populated reassignment fields
      expect(update.source_synchronizer).toBe('source-sync');
      expect(event.source_synchronizer).toBe('source-sync');
      expect(update.target_synchronizer).toBe('target-sync');
      expect(event.target_synchronizer).toBe('target-sync');
      expect(update.unassign_id).toBe('unassign-abc');
      expect(event.unassign_id).toBe('unassign-abc');
      expect(update.reassignment_counter).toBe(3);
      expect(event.reassignment_counter).toBe(3);
    });
  });

  describe('Edge cases', () => {
    it('should handle missing optional fields gracefully', () => {
      const minimalTx = {
        update_id: 'minimal',
        migration_id: 0,
        events_by_id: {},
      };

      const result = normalizeUpdate(minimalTx);
      
      expect(result.update_type).toBe('transaction');
      expect(result.workflow_id).toBeNull();
      expect(result.command_id).toBeNull();
      expect(result.trace_context).toBeNull();
    });

    it('should handle partial reassignment data', () => {
      // Reassignment with only some optional fields populated
      const partialReassign = {
        migration_id: 0,
        reassignment: {
          update_id: 'partial123',
          synchronizer_id: 'sync1',
          record_time: '2024-10-07T11:30:12.411Z',
          kind: 'assign',
          source: 'source-sync',
          // target, unassign_id, submitter, counter are missing
        },
      };

      const result = normalizeUpdate(partialReassign);
      
      expect(result.update_type).toBe('reassignment');
      expect(result.source_synchronizer).toBe('source-sync');
      expect(result.target_synchronizer).toBeNull();
      expect(result.unassign_id).toBeNull();
      expect(result.submitter).toBeNull();
      expect(result.reassignment_counter).toBeNull();
    });
  });
});
