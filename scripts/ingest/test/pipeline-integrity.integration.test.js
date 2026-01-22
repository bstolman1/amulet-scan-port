/**
 * Pipeline Integrity Integration Tests
 * 
 * Cross-cutting tests that verify data integrity across the entire
 * ingestion pipeline, including consistency between backfill and ACS data.
 */

import { describe, it, expect } from 'vitest';
import { normalizeUpdate, normalizeEvent, flattenEventsInTreeOrder, getPartitionPath } from '../data-schema.js';
import { normalizeACSContract, getACSPartitionPath, isTemplate } from '../acs-schema.js';
import {
  MOCK_BACKFILL_BATCH,
  MOCK_ACS_BATCH,
  MOCK_BACKFILL_TRANSACTION,
  MOCK_ACS_AMULET,
} from './fixtures/mock-api-responses.js';

describe('Pipeline Integrity', () => {
  
  describe('Cross-Pipeline Consistency', () => {
    it('should produce consistent template_id format between backfill and ACS', () => {
      // Extract templates from backfill
      const backfillTemplates = new Set();
      for (const raw of MOCK_BACKFILL_BATCH) {
        const update = raw.transaction || raw.reassignment || raw;
        if (update.events_by_id) {
          const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids || []);
          for (const event of events) {
            const inner = event.created_event || event.archived_event || event.exercised_event || event;
            if (inner.template_id) {
              backfillTemplates.add(inner.template_id);
            }
          }
        }
      }
      
      // Extract templates from ACS
      const acsTemplates = new Set();
      for (const event of MOCK_ACS_BATCH) {
        if (event.template_id) {
          acsTemplates.add(event.template_id);
        }
      }
      
      // Both should use same format (colon-dot)
      for (const t of backfillTemplates) {
        expect(t).toMatch(/:/);
        expect(t).toMatch(/\./);
      }
      for (const t of acsTemplates) {
        expect(t).toMatch(/:/);
        expect(t).toMatch(/\./);
      }
    });
    
    it('should use consistent partition path structure', () => {
      const backfillPath = getPartitionPath('2024-06-15T10:30:00Z', 0);
      const acsPath = getACSPartitionPath('2024-06-15T10:30:00Z', 0);
      
      // Both should use numeric (unpadded) date components
      expect(backfillPath).toContain('month=6');
      expect(acsPath).toContain('month=6');
      
      expect(backfillPath).toContain('day=15');
      expect(acsPath).toContain('day=15');
      
      // Both should include migration
      expect(backfillPath).toContain('migration=0');
      expect(acsPath).toContain('migration=0');
    });
    
    it('should preserve identical payload structure for same templates', () => {
      // Get an Amulet from backfill
      const backfillUpdate = MOCK_BACKFILL_TRANSACTION.transaction;
      const backfillEvents = flattenEventsInTreeOrder(backfillUpdate.events_by_id, backfillUpdate.root_event_ids);
      const backfillAmulet = backfillEvents.find(e => e.created_event?.template_id?.includes('Splice.Amulet:Amulet'));
      
      const backfillNormalized = normalizeEvent(
        backfillAmulet,
        backfillUpdate.update_id,
        0,
        backfillAmulet,
        {}
      );
      
      // Get Amulet from ACS
      const acsNormalized = normalizeACSContract(
        MOCK_ACS_AMULET,
        0,
        '2024-06-15T10:30:00Z',
        '2024-06-15T12:00:00Z'
      );
      
      // Both should have payload with same structure
      const backfillPayload = JSON.parse(backfillNormalized.payload);
      const acsPayload = JSON.parse(acsNormalized.payload);
      
      // Both should have owner and amount fields
      expect(backfillPayload).toHaveProperty('owner');
      expect(acsPayload).toHaveProperty('owner');
      
      expect(backfillPayload).toHaveProperty('amount');
      expect(acsPayload).toHaveProperty('amount');
      
      expect(backfillPayload.amount).toHaveProperty('initialAmount');
      expect(acsPayload.amount).toHaveProperty('initialAmount');
    });
  });
  
  describe('Migration ID Handling', () => {
    it('should preserve migration_id = 0 correctly', () => {
      // Backfill
      const backfillNormalized = normalizeUpdate(MOCK_BACKFILL_TRANSACTION);
      expect(backfillNormalized.migration_id).toBe(0);
      expect(backfillNormalized.migration_id).not.toBeNull();
      
      // ACS
      const acsNormalized = normalizeACSContract(MOCK_ACS_AMULET, 0, '2024-06-15T10:30:00Z', '2024-06-15T12:00:00Z');
      expect(acsNormalized.migration_id).toBe(0);
      expect(acsNormalized.migration_id).not.toBeNull();
    });
    
    it('should handle null migration_id with default', () => {
      const path = getPartitionPath('2024-06-15T10:30:00Z', null);
      expect(path).toContain('migration=0');
      
      const acsPath = getACSPartitionPath('2024-06-15T10:30:00Z', null);
      expect(acsPath).toContain('migration=0');
    });
    
    it('should isolate data by migration in partition paths', () => {
      const paths = [];
      for (let mig = 0; mig < 3; mig++) {
        paths.push(getPartitionPath('2024-06-15T10:30:00Z', mig));
      }
      
      expect(new Set(paths).size).toBe(3);
      expect(paths[0]).toContain('migration=0');
      expect(paths[1]).toContain('migration=1');
      expect(paths[2]).toContain('migration=2');
    });
  });
  
  describe('Timestamp Consistency', () => {
    it('should handle timestamps without timezone suffix', () => {
      // Some API responses may omit timezone
      const eventWithoutTz = {
        created_event: {
          event_id: 'evt-no-tz',
          contract_id: 'contract-no-tz',
          template_id: 'test:Module:Entity',
          created_at: '2024-06-15T10:30:00', // No Z suffix
        },
      };
      
      const normalized = normalizeEvent(eventWithoutTz, 'upd-test', 0, eventWithoutTz, {});
      
      // Should be treated as UTC and not shifted to local time
      expect(normalized.effective_at).toBeInstanceOf(Date);
      expect(normalized.effective_at.getUTCHours()).toBe(10);
    });
    
    it('should generate consistent partition paths for same timestamp', () => {
      const timestamp = '2024-06-15T10:30:00.000Z';
      
      const path1 = getPartitionPath(timestamp, 0);
      const path2 = getPartitionPath(timestamp, 0);
      const path3 = getPartitionPath(timestamp, 0);
      
      expect(path1).toBe(path2);
      expect(path2).toBe(path3);
    });
  });
  
  describe('Data Completeness', () => {
    it('should ensure all updates have update_data for recovery', () => {
      for (const raw of MOCK_BACKFILL_BATCH) {
        const normalized = normalizeUpdate(raw);
        
        expect(normalized.update_data).not.toBeNull();
        expect(normalized.update_data).not.toBe('');
        
        // Should be valid JSON
        const parsed = JSON.parse(normalized.update_data);
        expect(parsed).toBeDefined();
      }
    });
    
    it('should ensure all ACS contracts have raw for recovery', () => {
      for (const event of MOCK_ACS_BATCH) {
        const normalized = normalizeACSContract(event, 0, '2024-06-15T10:30:00Z', '2024-06-15T12:00:00Z');
        
        expect(normalized.raw).not.toBeNull();
        expect(normalized.raw).not.toBe('');
        
        // Should be valid JSON
        const parsed = JSON.parse(normalized.raw);
        expect(parsed).toBeDefined();
        
        // Should contain original event data
        expect(parsed.template_id).toBe(event.template_id);
        expect(parsed.contract_id || parsed.event_id).toBe(event.contract_id || event.event_id);
      }
    });
    
    it('should ensure all events have raw_event for recovery', () => {
      for (const raw of MOCK_BACKFILL_BATCH) {
        const update = raw.transaction || raw.reassignment || raw;
        if (!update.events_by_id) continue;
        
        const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids || []);
        
        for (const event of events) {
          const normalized = normalizeEvent(event, raw.update_id, raw.migration_id, event, {});
          
          expect(normalized.raw_event).not.toBeNull();
          expect(normalized.raw_event).not.toBe('');
          
          // Should be valid JSON
          const parsed = JSON.parse(normalized.raw_event);
          expect(parsed).toBeDefined();
        }
      }
    });
  });
  
  describe('ID Uniqueness', () => {
    it('should produce unique update_ids in a batch', () => {
      const ids = [];
      for (const raw of MOCK_BACKFILL_BATCH) {
        const normalized = normalizeUpdate(raw);
        ids.push(normalized.update_id);
      }
      
      expect(new Set(ids).size).toBe(ids.length);
    });
    
    it('should produce unique event_ids in a batch', () => {
      const ids = [];
      for (const raw of MOCK_BACKFILL_BATCH) {
        const update = raw.transaction || raw.reassignment || raw;
        if (!update.events_by_id) continue;
        
        const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids || []);
        for (const event of events) {
          const normalized = normalizeEvent(event, raw.update_id, raw.migration_id, event, {});
          if (normalized.event_id) {
            ids.push(normalized.event_id);
          }
        }
      }
      
      expect(new Set(ids).size).toBe(ids.length);
    });
    
    it('should produce unique contract_ids in ACS batch', () => {
      const ids = [];
      for (const event of MOCK_ACS_BATCH) {
        const normalized = normalizeACSContract(event, 0, '2024-06-15T10:30:00Z', '2024-06-15T12:00:00Z');
        ids.push(normalized.contract_id);
      }
      
      expect(new Set(ids).size).toBe(ids.length);
    });
  });
  
  describe('Type Consistency', () => {
    it('should always return Date objects for timestamps', () => {
      const normalized = normalizeUpdate(MOCK_BACKFILL_TRANSACTION);
      
      expect(normalized.record_time).toBeInstanceOf(Date);
      expect(normalized.effective_at).toBeInstanceOf(Date);
      expect(normalized.recorded_at).toBeInstanceOf(Date);
      expect(normalized.timestamp).toBeInstanceOf(Date);
    });
    
    it('should always return arrays for list fields', () => {
      const normalized = normalizeUpdate(MOCK_BACKFILL_TRANSACTION);
      
      expect(Array.isArray(normalized.root_event_ids)).toBe(true);
    });
    
    it('should always return number for migration_id', () => {
      const normalized = normalizeUpdate(MOCK_BACKFILL_TRANSACTION);
      
      expect(typeof normalized.migration_id).toBe('number');
    });
    
    it('should always return string for JSON fields', () => {
      const normalized = normalizeUpdate(MOCK_BACKFILL_TRANSACTION);
      
      expect(typeof normalized.update_data).toBe('string');
      if (normalized.trace_context) {
        expect(typeof normalized.trace_context).toBe('string');
      }
    });
  });
  
  describe('Event Tree Integrity', () => {
    it('should preserve parent-child relationships in tree order', () => {
      const update = MOCK_BACKFILL_BATCH[1].transaction; // Exercise with children
      const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids);
      
      // Root event should come first
      expect(events[0].event_id).toBe('upd-txn-002:0');
      
      // Child events should follow
      const rootEvent = events[0];
      const exercised = rootEvent.exercised_event;
      const childIds = exercised?.child_event_ids || [];
      
      // Verify children appear after parent
      const rootIndex = events.findIndex(e => e.event_id === 'upd-txn-002:0');
      for (const childId of childIds) {
        const childIndex = events.findIndex(e => e.event_id === childId);
        expect(childIndex).toBeGreaterThan(rootIndex);
      }
    });
    
    it('should preserve all events from events_by_id', () => {
      const update = MOCK_BACKFILL_BATCH[1].transaction;
      const events = flattenEventsInTreeOrder(update.events_by_id, update.root_event_ids);
      
      const originalCount = Object.keys(update.events_by_id).length;
      expect(events).toHaveLength(originalCount);
    });
  });
});
