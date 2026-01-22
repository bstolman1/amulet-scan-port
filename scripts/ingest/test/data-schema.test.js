/**
 * Tests for data-schema.js normalization functions
 * 
 * Critical tests ensuring Scan API payloads are correctly parsed
 * regardless of wrapper format (flat vs. transaction/reassignment wrapper)
 */

import { describe, it, expect } from 'vitest';
import {
  normalizeUpdate,
  normalizeEvent,
  flattenEventsInTreeOrder,
  getPartitionPath,
} from '../data-schema.js';

describe('normalizeUpdate', () => {
  describe('update_type detection', () => {
    it('should detect transaction when wrapped in transaction property', () => {
      const raw = {
        migration_id: 0,
        transaction: {
          update_id: 'abc123',
          synchronizer_id: 'global-domain::xyz',
          record_time: '2024-10-07T11:30:12.411Z',
          effective_at: '2024-10-07T11:30:11.903Z',
          offset: '1',
          events_by_id: { 'abc123:0': { event_type: 'created_event' } },
          root_event_ids: ['abc123:0'],
        },
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_type).toBe('transaction');
      expect(result.update_id).toBe('abc123');
      expect(result.event_count).toBe(1);
    });

    it('should detect transaction when flat with events_by_id (no wrapper)', () => {
      // This is the actual format from the Canton API that was causing nulls
      const raw = {
        update_id: '12204890912c8a0ff171c05370cbf84ebdd061943c3332ecf3b2612d4117d9a7ad17',
        migration_id: 0,
        workflow_id: '',
        record_time: '2024-10-07T11:30:12.411860Z',
        synchronizer_id: 'global-domain::1220b1431ef217342db44d516bb9befde802be7d8899637d290895fa58880f19accc',
        effective_at: '2024-10-07T11:30:11.903408Z',
        offset: '000000000000000001',
        root_event_ids: ['12204890912c8a0ff171c05370cbf84ebdd061943c3332ecf3b2612d4117d9a7ad17:0'],
        events_by_id: {
          '12204890912c8a0ff171c05370cbf84ebdd061943c3332ecf3b2612d4117d9a7ad17:0': {
            event_type: 'exercised_event',
            event_id: '12204890912c8a0ff171c05370cbf84ebdd061943c3332ecf3b2612d4117d9a7ad17:0',
            contract_id: '00cb8de49bd613f7ee386baa1ad72d0032a04a86b8b69f737c743a0c12d378a013ca',
          },
        },
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_type).toBe('transaction');
      expect(result.update_id).toBe('12204890912c8a0ff171c05370cbf84ebdd061943c3332ecf3b2612d4117d9a7ad17');
      expect(result.synchronizer_id).toBe('global-domain::1220b1431ef217342db44d516bb9befde802be7d8899637d290895fa58880f19accc');
      expect(result.event_count).toBe(1);
    });

    it('should detect reassignment when wrapped in reassignment property', () => {
      const raw = {
        migration_id: 0,
        reassignment: {
          update_id: 'reassign123',
          synchronizer_id: 'global-domain::xyz',
          record_time: '2024-10-07T11:30:12.411Z',
          kind: 'assign',
          source: 'source-sync',
          target: 'target-sync',
        },
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_type).toBe('reassignment');
      expect(result.kind).toBe('assign');
      expect(result.source_synchronizer).toBe('source-sync');
      expect(result.target_synchronizer).toBe('target-sync');
    });

    it('should return unknown for unrecognized format', () => {
      const raw = {
        update_id: 'mystery',
        migration_id: 0,
        // No transaction wrapper, no reassignment wrapper, no events_by_id
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_type).toBe('unknown');
    });
  });

  describe('field extraction', () => {
    it('should parse record_time as Date', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        record_time: '2024-10-07T11:30:12.411860Z',
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.record_time).toBeInstanceOf(Date);
      expect(result.record_time.toISOString()).toContain('2024-10-07');
    });

    it('should parse offset as integer', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        offset: '000000000000000042',
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.offset).toBe(42);
    });

    it('should extract root_event_ids as array', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        root_event_ids: ['event1:0', 'event1:1', 'event1:2'],
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.root_event_ids).toEqual(['event1:0', 'event1:1', 'event1:2']);
      expect(result.root_event_ids).toHaveLength(3);
    });

    it('should count events from events_by_id', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        events_by_id: {
          'test:0': {},
          'test:1': {},
          'test:2': {},
          'test:3': {},
          'test:4': {},
        },
      };

      const result = normalizeUpdate(raw);
      
      expect(result.event_count).toBe(5);
    });

    it('should stringify update_data for full preservation', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        custom_field: 'preserved',
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.update_data).toContain('custom_field');
      expect(result.update_data).toContain('preserved');
    });
  });

  describe('optional fields handling', () => {
    it('should handle missing workflow_id and command_id gracefully', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        events_by_id: {},
        // workflow_id and command_id intentionally missing
      };

      const result = normalizeUpdate(raw);
      
      expect(result.workflow_id).toBeNull();
      expect(result.command_id).toBeNull();
    });

    it('should handle empty string workflow_id as null', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        workflow_id: '',
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.workflow_id).toBeNull();
    });

    it('should handle missing trace_context', () => {
      const raw = {
        update_id: 'test',
        migration_id: 0,
        events_by_id: {},
      };

      const result = normalizeUpdate(raw);
      
      expect(result.trace_context).toBeNull();
    });
  });
});

describe('normalizeEvent', () => {
  describe('event type detection', () => {
    it('should detect created_event with nested structure', () => {
      const event = {
        created_event: {
          event_id: 'test:0',
          contract_id: 'contract123',
          template_id: 'pkg:Module:Template',
          create_arguments: { field: 'value' },
          signatories: ['party1'],
          observers: ['party2'],
        },
      };

      const result = normalizeEvent(event, 'test', 0);
      
      expect(result.event_type).toBe('created');
      expect(result.event_type_original).toBe('created_event');
      expect(result.contract_id).toBe('contract123');
      expect(result.signatories).toEqual(['party1']);
    });

    it('should detect flat created event (event_type property)', () => {
      const event = {
        event_type: 'created_event',
        event_id: 'test:0',
        contract_id: 'contract123',
        template_id: 'pkg:Module:Template',
        create_arguments: { field: 'value' },
        signatories: ['party1'],
      };

      const result = normalizeEvent(event, 'test', 0);
      
      expect(result.event_type).toBe('created');
      expect(result.contract_id).toBe('contract123');
    });

    it('should detect exercised_event with nested structure', () => {
      const event = {
        exercised_event: {
          event_id: 'test:0',
          contract_id: 'contract123',
          template_id: 'pkg:Module:Template',
          choice: 'Transfer',
          choice_argument: { amount: 100 },
          consuming: true,
          acting_parties: ['party1'],
          child_event_ids: ['test:1', 'test:2'],
          exercise_result: { success: true },
        },
      };

      const result = normalizeEvent(event, 'test', 0);
      
      expect(result.event_type).toBe('exercised');
      expect(result.event_type_original).toBe('exercised_event');
      expect(result.choice).toBe('Transfer');
      expect(result.consuming).toBe(true);
      expect(result.child_event_ids).toEqual(['test:1', 'test:2']);
      expect(result.acting_parties).toEqual(['party1']);
    });
  });

  describe('payload extraction', () => {
    it('should extract create_arguments from created event', () => {
      const event = {
        created_event: {
          event_id: 'test:0',
          contract_id: 'c1',
          create_arguments: { record: { fields: [{ value: { party: 'DSO' } }] } },
        },
      };

      const result = normalizeEvent(event, 'test', 0);
      
      expect(result.payload).toContain('DSO');
      expect(JSON.parse(result.payload).record.fields).toHaveLength(1);
    });

    it('should extract choice_argument from exercised event', () => {
      const event = {
        exercised_event: {
          event_id: 'test:0',
          contract_id: 'c1',
          choice: 'Execute',
          choice_argument: { amount: 1000 },
        },
      };

      const result = normalizeEvent(event, 'test', 0);
      
      expect(result.payload).toContain('1000');
      expect(JSON.parse(result.payload).amount).toBe(1000);
    });
  });

  describe('timestamp handling', () => {
    it('should parse created_at as effective_at', () => {
      const event = {
        event_type: 'created_event',
        event_id: 'test:0',
        contract_id: 'c1',
        created_at: '2024-10-07T11:30:11.903408Z',
      };

      const result = normalizeEvent(event, 'test', 0);
      
      expect(result.effective_at).toBeInstanceOf(Date);
      expect(result.effective_at.toISOString()).toContain('2024-10-07');
    });

    it('should assume UTC when timezone missing', () => {
      const event = {
        event_type: 'created_event',
        event_id: 'test:0',
        contract_id: 'c1',
        created_at: '2024-10-07T11:30:11.903408', // No Z suffix
      };

      const result = normalizeEvent(event, 'test', 0);
      
      // Should be parsed as UTC, not local time
      expect(result.effective_at.toISOString()).toBe('2024-10-07T11:30:11.903Z');
    });

    it('should fall back to updateInfo.record_time when created_at missing', () => {
      const event = {
        event_type: 'created_event',
        event_id: 'test:0',
        contract_id: 'c1',
      };
      const updateInfo = {
        record_time: '2024-10-08T12:00:00Z',
        synchronizer_id: 'sync1',
      };

      const result = normalizeEvent(event, 'test', 0, null, updateInfo);
      
      expect(result.effective_at).toBeInstanceOf(Date);
      expect(result.effective_at.toISOString()).toContain('2024-10-08');
    });
  });

  describe('raw_event preservation', () => {
    it('should stringify raw event for DuckDB compatibility', () => {
      const event = {
        event_type: 'created_event',
        event_id: 'test:0',
        custom_field: 'important_data',
      };

      const result = normalizeEvent(event, 'test', 0, event);
      
      expect(result.raw_event).toContain('important_data');
      expect(typeof result.raw_event).toBe('string');
    });
  });
});

describe('flattenEventsInTreeOrder', () => {
  it('should flatten events in preorder traversal', () => {
    const eventsById = {
      'root:0': { child_event_ids: ['root:1', 'root:2'] },
      'root:1': { child_event_ids: ['root:3'] },
      'root:2': { child_event_ids: [] },
      'root:3': { child_event_ids: [] },
    };
    const rootEventIds = ['root:0'];

    const result = flattenEventsInTreeOrder(eventsById, rootEventIds);
    
    // Preorder: root, then left subtree, then right subtree
    expect(result.map(e => e.event_id)).toEqual(['root:0', 'root:1', 'root:3', 'root:2']);
  });

  it('should handle multiple root events', () => {
    const eventsById = {
      'a:0': { child_event_ids: [] },
      'a:1': { child_event_ids: ['a:2'] },
      'a:2': { child_event_ids: [] },
    };
    const rootEventIds = ['a:0', 'a:1'];

    const result = flattenEventsInTreeOrder(eventsById, rootEventIds);
    
    expect(result.map(e => e.event_id)).toEqual(['a:0', 'a:1', 'a:2']);
  });

  it('should handle exercised_event wrapper for child_event_ids', () => {
    const eventsById = {
      'root:0': { 
        exercised_event: { child_event_ids: ['root:1'] } 
      },
      'root:1': {},
    };
    const rootEventIds = ['root:0'];

    const result = flattenEventsInTreeOrder(eventsById, rootEventIds);
    
    expect(result.map(e => e.event_id)).toEqual(['root:0', 'root:1']);
  });

  it('should handle empty input gracefully', () => {
    expect(flattenEventsInTreeOrder({}, [])).toEqual([]);
    expect(flattenEventsInTreeOrder(null, null)).toEqual([]);
  });
});

describe('getPartitionPath', () => {
  it('should generate correct Hive partition path', () => {
    const timestamp = new Date('2024-10-07T11:30:12Z');
    const migrationId = 0;

    const result = getPartitionPath(timestamp, migrationId);
    
    expect(result).toBe('backfill/migration=0/year=2024/month=10/day=7');
  });

  it('should use numeric (non-padded) values for BigQuery INT64 inference', () => {
    const timestamp = new Date('2024-01-05T11:30:12Z');
    
    const result = getPartitionPath(timestamp, 1);
    
    // month=1 not month=01, day=5 not day=05
    expect(result).toBe('backfill/migration=1/year=2024/month=1/day=5');
  });

  it('should default migration to 0 when not provided', () => {
    const timestamp = new Date('2024-06-15T00:00:00Z');

    const result = getPartitionPath(timestamp);
    
    expect(result).toContain('migration=0');
  });
});
