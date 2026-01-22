/**
 * Encoding Tests
 * 
 * Tests protobuf mapping and type conversion functions.
 * Critical for data integrity during binary serialization.
 */

import { describe, it, expect } from 'vitest';
import { mapEvent, mapUpdate } from '../encoding.js';

describe('Encoding', () => {
  
  describe('mapEvent', () => {
    it('should map snake_case event fields to camelCase', () => {
      const input = {
        event_id: 'evt-123',
        update_id: 'upd-456',
        event_type: 'created',
        contract_id: 'contract-789',
        template_id: 'pkg:Splice.Amulet:Amulet',
        effective_at: '2024-01-15T10:00:00Z',
        recorded_at: '2024-01-15T10:00:01Z',
      };
      
      const result = mapEvent(input);
      
      expect(result.id).toBe('evt-123');
      expect(result.updateId).toBe('upd-456');
      expect(result.type).toBe('created');
      expect(result.contractId).toBe('contract-789');
      expect(result.template).toBe('pkg:Splice.Amulet:Amulet');
    });
    
    it('should map camelCase event fields', () => {
      const input = {
        eventId: 'evt-123',
        updateId: 'upd-456',
        eventType: 'exercised',
        contractId: 'contract-789',
        templateId: 'pkg:Mod:Entity',
      };
      
      const result = mapEvent(input);
      
      expect(result.id).toBe('evt-123');
      expect(result.updateId).toBe('upd-456');
      expect(result.type).toBe('exercised');
      expect(result.contractId).toBe('contract-789');
    });
    
    it('should handle signatories and observers arrays', () => {
      const input = {
        event_id: 'evt-1',
        signatories: ['party1', 'party2'],
        observers: ['observer1'],
        acting_parties: ['actor1'],
        witness_parties: ['witness1', 'witness2'],
      };
      
      const result = mapEvent(input);
      
      expect(result.signatories).toEqual(['party1', 'party2']);
      expect(result.observers).toEqual(['observer1']);
      expect(result.actingParties).toEqual(['actor1']);
      expect(result.witnessParties).toEqual(['witness1', 'witness2']);
    });
    
    it('should handle null/undefined arrays', () => {
      const input = {
        event_id: 'evt-1',
        signatories: null,
        observers: undefined,
      };
      
      const result = mapEvent(input);
      
      expect(result.signatories).toEqual([]);
      expect(result.observers).toEqual([]);
    });
    
    it('should handle child_event_ids for exercised events', () => {
      const input = {
        event_id: 'evt-1',
        child_event_ids: ['child-1', 'child-2', 'child-3'],
      };
      
      const result = mapEvent(input);
      
      expect(result.childEventIds).toEqual(['child-1', 'child-2', 'child-3']);
    });
    
    it('should serialize payload to JSON string', () => {
      const input = {
        event_id: 'evt-1',
        payload: { amount: '1000000', recipient: 'party1' },
      };
      
      const result = mapEvent(input);
      
      expect(result.payloadJson).toBe('{"amount":"1000000","recipient":"party1"}');
    });
    
    it('should preserve pre-stringified payload', () => {
      const input = {
        event_id: 'evt-1',
        payloadJson: '{"preStringified":true}',
      };
      
      const result = mapEvent(input);
      
      expect(result.payloadJson).toBe('{"preStringified":true}');
    });
    
    it('should handle reassignment fields', () => {
      const input = {
        event_id: 'evt-1',
        source_synchronizer: 'sync-source',
        target_synchronizer: 'sync-target',
        unassign_id: 'unassign-123',
        submitter: 'party1',
        reassignment_counter: 5,
      };
      
      const result = mapEvent(input);
      
      expect(result.sourceSynchronizer).toBe('sync-source');
      expect(result.targetSynchronizer).toBe('sync-target');
      expect(result.unassignId).toBe('unassign-123');
      expect(result.submitter).toBe('party1');
      expect(result.reassignmentCounter).toBe(5);
    });
    
    it('should preserve raw event for recovery', () => {
      const input = {
        event_id: 'evt-1',
        raw_event: '{"original":"data"}',
      };
      
      const result = mapEvent(input);
      
      expect(result.rawEvent).toBe('{"original":"data"}');
      expect(result.rawJson).toBe('{"original":"data"}');
    });
    
    it('should handle migration_id = 0', () => {
      const input = {
        event_id: 'evt-1',
        migration_id: 0,
      };
      
      const result = mapEvent(input);
      
      expect(result.migrationId).toBe(0);
    });
  });
  
  describe('mapUpdate', () => {
    it('should map snake_case update fields to camelCase', () => {
      const input = {
        update_id: 'upd-123',
        update_type: 'transaction',
        synchronizer_id: 'sync-1',
        record_time: '2024-01-15T10:00:00Z',
        command_id: 'cmd-456',
        workflow_id: 'wf-789',
      };
      
      const result = mapUpdate(input);
      
      expect(result.id).toBe('upd-123');
      expect(result.type).toBe('transaction');
      expect(result.synchronizer).toBe('sync-1');
      expect(result.commandId).toBe('cmd-456');
      expect(result.workflowId).toBe('wf-789');
    });
    
    it('should handle root_event_ids', () => {
      const input = {
        update_id: 'upd-1',
        root_event_ids: ['evt-1', 'evt-2', 'evt-3'],
        event_count: 5,
      };
      
      const result = mapUpdate(input);
      
      expect(result.rootEventIds).toEqual(['evt-1', 'evt-2', 'evt-3']);
      expect(result.eventCount).toBe(5);
    });
    
    it('should handle reassignment update fields', () => {
      const input = {
        update_id: 'upd-1',
        update_type: 'reassignment',
        kind: 'assign',
        source_synchronizer: 'sync-source',
        target_synchronizer: 'sync-target',
        unassign_id: 'unassign-123',
        submitter: 'party1',
        reassignment_counter: 10,
      };
      
      const result = mapUpdate(input);
      
      expect(result.type).toBe('reassignment');
      expect(result.kind).toBe('assign');
      expect(result.sourceSynchronizer).toBe('sync-source');
      expect(result.targetSynchronizer).toBe('sync-target');
      expect(result.unassignId).toBe('unassign-123');
      expect(result.reassignmentCounter).toBe(10);
    });
    
    it('should serialize trace_context and update_data', () => {
      const input = {
        update_id: 'upd-1',
        trace_context: { traceId: 'trace-abc', spanId: 'span-123' },
        update_data: { events_by_id: {}, root_event_ids: [] },
      };
      
      const result = mapUpdate(input);
      
      expect(result.traceContextJson).toBe('{"traceId":"trace-abc","spanId":"span-123"}');
      expect(result.updateDataJson).toBe('{"events_by_id":{},"root_event_ids":[]}');
    });
    
    it('should handle offset as int64', () => {
      const input = {
        update_id: 'upd-1',
        offset: '123456789',
      };
      
      const result = mapUpdate(input);
      
      expect(result.offset).toBe(123456789);
    });
  });
  
  describe('Timestamp Conversion', () => {
    function safeTimestamp(value) {
      if (!value) return 0;
      if (typeof value === 'number') return value;
      try {
        const ts = new Date(value).getTime();
        return isNaN(ts) ? 0 : ts;
      } catch {
        return 0;
      }
    }
    
    it('should convert ISO string to epoch ms', () => {
      expect(safeTimestamp('2024-01-15T10:00:00Z')).toBe(1705312800000);
    });
    
    it('should preserve numeric timestamps', () => {
      expect(safeTimestamp(1705312800000)).toBe(1705312800000);
    });
    
    it('should return 0 for null/undefined', () => {
      expect(safeTimestamp(null)).toBe(0);
      expect(safeTimestamp(undefined)).toBe(0);
    });
    
    it('should return 0 for invalid dates', () => {
      expect(safeTimestamp('not-a-date')).toBe(0);
      expect(safeTimestamp('')).toBe(0);
    });
    
    it('should handle Date objects', () => {
      const date = new Date('2024-01-15T10:00:00Z');
      expect(safeTimestamp(date.toISOString())).toBe(1705312800000);
    });
  });
  
  describe('Int64 Conversion', () => {
    function safeInt64(value) {
      if (value === null || value === undefined) return 0;
      const num = parseInt(value);
      return isNaN(num) ? 0 : num;
    }
    
    it('should parse string numbers', () => {
      expect(safeInt64('123456')).toBe(123456);
    });
    
    it('should preserve number values', () => {
      expect(safeInt64(123456)).toBe(123456);
    });
    
    it('should return 0 for null/undefined', () => {
      expect(safeInt64(null)).toBe(0);
      expect(safeInt64(undefined)).toBe(0);
    });
    
    it('should handle 0 correctly', () => {
      expect(safeInt64(0)).toBe(0);
      expect(safeInt64('0')).toBe(0);
    });
    
    it('should return 0 for NaN', () => {
      expect(safeInt64('not-a-number')).toBe(0);
    });
  });
  
  describe('String Array Conversion', () => {
    function safeStringArray(arr) {
      if (!Array.isArray(arr)) return [];
      return arr.map(String);
    }
    
    it('should convert all elements to strings', () => {
      expect(safeStringArray([1, 2, 3])).toEqual(['1', '2', '3']);
      expect(safeStringArray(['a', 'b'])).toEqual(['a', 'b']);
    });
    
    it('should return empty array for non-array', () => {
      expect(safeStringArray(null)).toEqual([]);
      expect(safeStringArray(undefined)).toEqual([]);
      expect(safeStringArray('string')).toEqual([]);
      expect(safeStringArray({})).toEqual([]);
    });
    
    it('should handle mixed types', () => {
      expect(safeStringArray(['a', 1, true, null])).toEqual(['a', '1', 'true', 'null']);
    });
  });
  
  describe('JSON Stringify Safety', () => {
    function safeStringify(obj) {
      try {
        return typeof obj === 'string' ? obj : JSON.stringify(obj);
      } catch {
        return '';
      }
    }
    
    it('should stringify objects', () => {
      expect(safeStringify({ key: 'value' })).toBe('{"key":"value"}');
    });
    
    it('should preserve already-stringified JSON', () => {
      expect(safeStringify('{"already":"stringified"}')).toBe('{"already":"stringified"}');
    });
    
    it('should handle nested objects', () => {
      const nested = { a: { b: { c: 'deep' } } };
      expect(safeStringify(nested)).toBe('{"a":{"b":{"c":"deep"}}}');
    });
    
    it('should handle arrays', () => {
      expect(safeStringify([1, 2, 3])).toBe('[1,2,3]');
    });
    
    it('should handle circular references gracefully', () => {
      const circular = { a: 1 };
      circular.self = circular;
      
      // Should return empty string on error
      expect(safeStringify(circular)).toBe('');
    });
  });
});
