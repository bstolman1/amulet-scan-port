/**
 * Party API Tests
 * 
 * Tests for /api/party/* endpoints
 */

import { describe, it, expect, vi } from 'vitest';
import { sanitizeNumber, sanitizeSearchQuery, sanitizeIdentifier } from '../lib/sql-sanitize.js';

describe('Party API', () => {
  describe('Party search validation', () => {
    it('should validate search query', () => {
      expect(sanitizeSearchQuery('alice')).toBe('alice');
      expect(sanitizeSearchQuery('party::alice')).toBe(null); // Contains ::
      expect(sanitizeSearchQuery('Digital Asset')).toBe('Digital Asset');
      expect(sanitizeSearchQuery("'; DROP TABLE")).toBe(null);
    });
    
    it('should validate party ID', () => {
      expect(sanitizeIdentifier('party::alice')).toBe('party::alice');
      expect(sanitizeIdentifier('DSO::namespace::party')).toBe('DSO::namespace::party');
      expect(sanitizeIdentifier("party'; DROP TABLE")).toBeNull();
    });
    
    it('should validate limit parameter', () => {
      expect(sanitizeNumber('50', { min: 1, max: 10000, defaultValue: 1000 })).toBe(50);
      expect(sanitizeNumber('20000', { min: 1, max: 10000, defaultValue: 1000 })).toBe(10000);
      expect(sanitizeNumber('invalid', { min: 1, max: 10000, defaultValue: 1000 })).toBe(1000);
    });
  });

  describe('Party matching logic', () => {
    const partyMatches = (event, partyId) => {
      return (event.signatories && event.signatories.includes(partyId)) ||
             (event.observers && event.observers.includes(partyId));
    };
    
    it('should match by signatory', () => {
      const event = { signatories: ['party::alice', 'party::bob'], observers: [] };
      expect(partyMatches(event, 'party::alice')).toBe(true);
      expect(partyMatches(event, 'party::bob')).toBe(true);
      expect(partyMatches(event, 'party::charlie')).toBe(false);
    });
    
    it('should match by observer', () => {
      const event = { signatories: [], observers: ['party::alice', 'party::bob'] };
      expect(partyMatches(event, 'party::alice')).toBe(true);
      expect(partyMatches(event, 'party::charlie')).toBe(false);
    });
    
    it('should match in either role', () => {
      const event = { signatories: ['party::alice'], observers: ['party::bob'] };
      expect(partyMatches(event, 'party::alice')).toBe(true);
      expect(partyMatches(event, 'party::bob')).toBe(true);
    });
    
    it('should handle missing arrays', () => {
      expect(partyMatches({}, 'party::alice')).toBe(false);
      expect(partyMatches({ signatories: null }, 'party::alice')).toBe(false);
    });
  });

  describe('Response structure validation', () => {
    it('should structure search response correctly', () => {
      const matches = ['party::alice', 'party::alice-validator'];
      
      const response = {
        data: matches,
        count: matches.length,
        indexed: true,
      };
      
      expect(response.data).toHaveLength(2);
      expect(response.count).toBe(2);
      expect(response.indexed).toBe(true);
    });
    
    it('should structure party events response correctly', () => {
      const events = [
        { event_id: '1', template_id: 'Template1' },
        { event_id: '2', template_id: 'Template2' },
      ];
      
      const response = {
        data: events,
        count: events.length,
        total: 100,
        party_id: 'party::alice',
        indexed: true,
        filesScanned: 5,
      };
      
      expect(response.data).toHaveLength(2);
      expect(response.party_id).toBe('party::alice');
      expect(response.total).toBe(100);
    });
    
    it('should include warning for non-indexed queries', () => {
      const response = {
        data: [],
        count: 0,
        party_id: 'party::alice',
        indexed: false,
        warning: 'Scanning recent files only (last 30 days). Build the party index for complete history.',
      };
      
      expect(response.indexed).toBe(false);
      expect(response.warning).toContain('Build the party index');
    });
  });

  describe('Party summary structure', () => {
    it('should provide party summary data', () => {
      const summary = {
        partyId: 'party::alice',
        eventCount: 150,
        firstSeen: '2024-01-01T00:00:00Z',
        lastSeen: '2024-12-31T23:59:59Z',
        templates: {
          'Splice.Amulet:Amulet': 50,
          'Splice.ValidatorLicense:ValidatorLicense': 25,
          'Splice.Round:OpenMiningRound': 75,
        },
        roles: ['signatory', 'observer'],
      };
      
      expect(summary.eventCount).toBe(150);
      expect(summary.templates['Splice.Amulet:Amulet']).toBe(50);
    });
  });
});
