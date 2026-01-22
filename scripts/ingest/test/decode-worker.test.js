/**
 * Decode Worker Tests
 * 
 * Tests the CPU-intensive transaction normalization logic.
 */

import { describe, it, expect } from 'vitest';
import { normalizeUpdate, normalizeEvent } from '../data-schema.js';

describe('Decode Worker Logic', () => {
  
  describe('Transaction Decoding', () => {
    it('should decode a standard transaction with events', () => {
      const tx = {
        transaction: {
          update_id: 'upd-123',
          record_time: '2024-01-15T10:00:00Z',
          effective_at: '2024-01-15T10:00:00Z',
          synchronizer_id: 'sync-1',
          offset: '12345',
          root_event_ids: ['evt-1', 'evt-2'],
          events_by_id: {
            'evt-1': {
              created_event: {
                event_id: 'evt-1',
                contract_id: 'contract-1',
                template_id: 'pkg:Splice.Amulet:Amulet',
                create_arguments: { amount: '1000000' },
                signatories: ['party1'],
              }
            },
            'evt-2': {
              exercised_event: {
                event_id: 'evt-2',
                contract_id: 'contract-2',
                template_id: 'pkg:Splice.Amulet:Amulet',
                choice: 'Transfer',
                choice_argument: { recipient: 'party2' },
                consuming: true,
                child_event_ids: [],
              }
            }
          }
        },
        migration_id: 0,
      };
      
      const update = normalizeUpdate(tx);
      
      expect(update.update_id).toBe('upd-123');
      expect(update.update_type).toBe('transaction');
      expect(update.migration_id).toBe(0);
      expect(update.event_count).toBe(2);
      expect(update.root_event_ids).toEqual(['evt-1', 'evt-2']);
    });
    
    it('should decode events from events_by_id', () => {
      const tx = {
        transaction: {
          update_id: 'upd-456',
          record_time: '2024-01-15T10:00:00Z',
          synchronizer_id: 'sync-1',
          events_by_id: {
            'evt-1': {
              created_event: {
                event_id: 'evt-1',
                contract_id: 'c-1',
                template_id: 'pkg:Mod:Entity',
                create_arguments: { value: 100 },
                signatories: ['p1'],
                observers: ['p2'],
              }
            }
          },
          root_event_ids: ['evt-1'],
        },
        migration_id: 0,
      };
      
      const txData = tx.transaction;
      const eventsById = txData.events_by_id || {};
      const events = [];
      
      for (const [eventId, rawEvent] of Object.entries(eventsById)) {
        const ev = normalizeEvent(rawEvent, 'upd-456', 0, rawEvent, {
          record_time: txData.record_time,
          synchronizer_id: txData.synchronizer_id,
        });
        ev.event_id = eventId;
        events.push(ev);
      }
      
      expect(events).toHaveLength(1);
      expect(events[0].event_id).toBe('evt-1');
      expect(events[0].event_type).toBe('created');
      expect(events[0].contract_id).toBe('c-1');
      expect(events[0].signatories).toEqual(['p1']);
    });
  });
  
  describe('Reassignment Decoding', () => {
    it('should decode reassignment with created event', () => {
      const tx = {
        reassignment: {
          update_id: 'upd-reassign-1',
          record_time: '2024-01-15T11:00:00Z',
          synchronizer_id: 'sync-target',
          source: 'sync-source',
          target: 'sync-target',
          unassign_id: 'unassign-123',
          submitter: 'party1',
          counter: 5,
        },
        event: {
          created_event: {
            event_id: 'evt-reassign-1',
            contract_id: 'contract-reassign',
            template_id: 'pkg:Mod:Entity',
            create_arguments: { data: 'value' },
          }
        },
        migration_id: 0,
      };
      
      const update = normalizeUpdate(tx);
      
      expect(update.update_type).toBe('reassignment');
      expect(update.source_synchronizer).toBe('sync-source');
      expect(update.target_synchronizer).toBe('sync-target');
      expect(update.unassign_id).toBe('unassign-123');
      expect(update.submitter).toBe('party1');
      expect(update.reassignment_counter).toBe(5);
    });
    
    it('should extract reassignment created event', () => {
      const createdEvent = {
        event_id: 'evt-reassign-1',
        contract_id: 'contract-reassign',
        template_id: 'pkg:Mod:Entity',
        create_arguments: { data: 'value' },
        signatories: ['party1'],
      };
      
      const updateInfo = {
        record_time: '2024-01-15T11:00:00Z',
        synchronizer_id: 'sync-target',
        source: 'sync-source',
        target: 'sync-target',
      };
      
      const ev = normalizeEvent(
        { created_event: createdEvent },
        'upd-reassign-1',
        0,
        { created_event: createdEvent },
        updateInfo
      );
      
      expect(ev.event_type).toBe('created');
      expect(ev.contract_id).toBe('contract-reassign');
      expect(ev.source_synchronizer).toBe('sync-source');
      expect(ev.target_synchronizer).toBe('sync-target');
    });
    
    it('should extract reassignment archived event', () => {
      const archivedEvent = {
        event_id: 'evt-archive-1',
        contract_id: 'contract-archived',
        template_id: 'pkg:Mod:Entity',
      };
      
      const updateInfo = {
        record_time: '2024-01-15T11:00:00Z',
        synchronizer_id: 'sync-source',
        source: 'sync-source',
        target: 'sync-target',
      };
      
      const ev = normalizeEvent(
        { archived_event: archivedEvent },
        'upd-reassign-1',
        0,
        { archived_event: archivedEvent },
        updateInfo
      );
      
      expect(ev.event_type).toBe('archived');
      expect(ev.contract_id).toBe('contract-archived');
    });
  });
  
  describe('Flat Transaction Format', () => {
    it('should handle transactions without wrapper', () => {
      // Some API responses don't have the transaction/reassignment wrapper
      const tx = {
        update_id: 'upd-flat-1',
        record_time: '2024-01-15T12:00:00Z',
        synchronizer_id: 'sync-1',
        events_by_id: {
          'evt-1': {
            created_event: {
              event_id: 'evt-1',
              contract_id: 'c-1',
              template_id: 'pkg:Mod:Entity',
            }
          }
        },
        root_event_ids: ['evt-1'],
        migration_id: 0,
      };
      
      const update = normalizeUpdate(tx);
      
      // Should detect as transaction because it has events_by_id
      expect(update.update_type).toBe('transaction');
      expect(update.update_id).toBe('upd-flat-1');
      expect(update.event_count).toBe(1);
    });
  });
  
  describe('Exercised Event with Children', () => {
    it('should preserve child_event_ids for tree traversal', () => {
      const exercisedEvent = {
        event_id: 'evt-exercise-1',
        contract_id: 'c-1',
        template_id: 'pkg:Mod:Entity',
        choice: 'Execute',
        choice_argument: { param: 'value' },
        consuming: true,
        child_event_ids: ['evt-child-1', 'evt-child-2', 'evt-child-3'],
        exercise_result: { status: 'success' },
      };
      
      const ev = normalizeEvent(
        { exercised_event: exercisedEvent },
        'upd-1',
        0,
        { exercised_event: exercisedEvent },
        { record_time: '2024-01-15T10:00:00Z' }
      );
      
      expect(ev.event_type).toBe('exercised');
      expect(ev.choice).toBe('Execute');
      expect(ev.consuming).toBe(true);
      expect(ev.child_event_ids).toEqual(['evt-child-1', 'evt-child-2', 'evt-child-3']);
      expect(JSON.parse(ev.exercise_result)).toEqual({ status: 'success' });
    });
  });
  
  describe('Timestamp Handling', () => {
    it('should treat timestamps without timezone as UTC', () => {
      const event = {
        event_id: 'evt-1',
        contract_id: 'c-1',
        template_id: 'pkg:Mod:Entity',
        created_at: '2024-01-15T10:00:00', // No Z suffix
      };
      
      const ev = normalizeEvent(
        { created_event: event },
        'upd-1',
        0,
        { created_event: event },
        {}
      );
      
      // Should be interpreted as UTC
      expect(ev.effective_at.toISOString()).toBe('2024-01-15T10:00:00.000Z');
    });
    
    it('should preserve explicit timezone', () => {
      const event = {
        event_id: 'evt-1',
        contract_id: 'c-1',
        template_id: 'pkg:Mod:Entity',
        created_at: '2024-01-15T10:00:00+05:00',
      };
      
      const ev = normalizeEvent(
        { created_event: event },
        'upd-1',
        0,
        { created_event: event },
        {}
      );
      
      // Should convert to UTC
      expect(ev.effective_at.toISOString()).toBe('2024-01-15T05:00:00.000Z');
    });
  });
  
  describe('Migration ID Edge Cases', () => {
    it('should handle migration_id = 0 correctly', () => {
      const tx = {
        transaction: {
          update_id: 'upd-1',
          events_by_id: {},
          root_event_ids: [],
        },
        migration_id: 0,
      };
      
      const update = normalizeUpdate(tx);
      
      expect(update.migration_id).toBe(0);
      expect(update.migration_id).not.toBeNull();
    });
    
    it('should handle missing migration_id as null', () => {
      const tx = {
        transaction: {
          update_id: 'upd-1',
          events_by_id: {},
          root_event_ids: [],
        },
        // No migration_id
      };
      
      const update = normalizeUpdate(tx);
      
      expect(update.migration_id).toBeNull();
    });
  });
});
