/**
 * Governance Lifecycle API Tests
 * 
 * Tests for governance flow classification and entity extraction.
 * Critical for accurate governance proposal tracking.
 */

import { describe, it, expect } from 'vitest';

describe('Governance Lifecycle', () => {
  describe('Entity name extraction', () => {
    // Core extraction logic
    const extractPrimaryEntityName = (text) => {
      if (!text) return { name: null, isMultiEntity: false };
      
      let cleanText = text
        .replace(/^(re:|fwd:|fw:)\s*/i, '')
        .replace(/\s*-\s*vote\s*(proposal)?\s*$/i, '')
        .replace(/\s*vote\s*(proposal)?\s*$/i, '')
        .trim();
      
      // Batch approval detection
      const batchApprovalMatch = cleanText.match(/(?:validator\s*operators?|validators|featured\s*apps?)\s*approved[:\s-]+(.+?)$/i);
      if (batchApprovalMatch) {
        const content = batchApprovalMatch[1].trim();
        if (content.includes(',') || /\s+and\s+/i.test(content)) {
          return { name: null, isMultiEntity: true };
        }
      }
      
      // Validator Approved: EntityName
      const validatorApprovedMatch = cleanText.match(/validator\s*(?:approved|operator\s+approved)[:\s-]+(.+?)$/i);
      if (validatorApprovedMatch) {
        let name = validatorApprovedMatch[1].trim();
        if (name.includes(',') || /\s+and\s+/i.test(name)) {
          return { name: null, isMultiEntity: true };
        }
        if (name.length > 1) return { name, isMultiEntity: false };
      }
      
      // Featured App Approved: AppName
      const featuredApprovedMatch = cleanText.match(/featured\s*app\s*approved[:\s-]+(.+?)$/i);
      if (featuredApprovedMatch) {
        let name = featuredApprovedMatch[1].trim();
        if (name.length > 1) return { name, isMultiEntity: false };
      }
      
      // to Feature AppName
      const toFeatureMatch = cleanText.match(/to\s+feature\s+(?:the\s+)?([A-Za-z0-9][\w\s-]*?)$/i);
      if (toFeatureMatch) {
        let name = toFeatureMatch[1].trim().replace(/\s+by\s+.*$/i, '').trim();
        if (name.length > 1) return { name, isMultiEntity: false };
      }
      
      // Request: AppName / Proposal: AppName
      const requestMatch = cleanText.match(/(?:request|proposal|onboarding|tokenomics|announce|announcement|approved|vote\s*proposal)[:\s-]+(.+?)$/i);
      if (requestMatch) {
        let name = requestMatch[1].trim()
          .replace(/^(?:mainnet|testnet|main\s*net|test\s*net)[:\s]+/i, '')
          .replace(/\s+by\s+.*$/i, '')
          .trim();
        if (name.length > 2 && !/^(the|this|new|our)$/i.test(name)) {
          return { name, isMultiEntity: false };
        }
      }
      
      return { name: null, isMultiEntity: false };
    };
    
    it('should extract validator approval names', () => {
      const result = extractPrimaryEntityName('Validator Approved: Digital Asset');
      expect(result.name).toBe('Digital Asset');
      expect(result.isMultiEntity).toBe(false);
    });
    
    it('should extract featured app approval names', () => {
      const result = extractPrimaryEntityName('Featured App Approved: Akascan');
      expect(result.name).toBe('Akascan');
      expect(result.isMultiEntity).toBe(false);
    });
    
    it('should detect multi-entity batch approvals', () => {
      const result = extractPrimaryEntityName('Validator Operators Approved: Company A, Company B, Company C');
      expect(result.name).toBeNull();
      expect(result.isMultiEntity).toBe(true);
    });
    
    it('should extract "to Feature" patterns', () => {
      const result = extractPrimaryEntityName('to Feature Console Wallet');
      expect(result.name).toBe('Console Wallet');
    });
    
    it('should extract from request patterns', () => {
      const result = extractPrimaryEntityName('Request: MyApp by MyCompany');
      expect(result.name).toBe('MyApp');
    });
    
    it('should handle empty/null input', () => {
      expect(extractPrimaryEntityName(null)).toEqual({ name: null, isMultiEntity: false });
      expect(extractPrimaryEntityName('')).toEqual({ name: null, isMultiEntity: false });
    });
    
    it('should strip reply prefixes', () => {
      const result = extractPrimaryEntityName('Re: Featured App Approved: TestApp');
      expect(result.name).toBe('TestApp');
    });
  });

  describe('URL extraction', () => {
    const extractUrls = (text) => {
      if (!text) return [];
      const urlRegex = /(https?:\/\/[^\s<>"{}|\\^`\[\]]+)/g;
      const matches = text.match(urlRegex) || [];
      return [...new Set(matches)];
    };
    
    it('should extract HTTP URLs', () => {
      const urls = extractUrls('Check out http://example.com for details');
      expect(urls).toContain('http://example.com');
    });
    
    it('should extract HTTPS URLs', () => {
      const urls = extractUrls('Visit https://governance.example.org/proposal/123');
      expect(urls).toContain('https://governance.example.org/proposal/123');
    });
    
    it('should extract multiple URLs', () => {
      const urls = extractUrls('See https://a.com and https://b.com');
      expect(urls).toHaveLength(2);
      expect(urls).toContain('https://a.com');
      expect(urls).toContain('https://b.com');
    });
    
    it('should deduplicate URLs', () => {
      const urls = extractUrls('Link: https://example.com mentioned twice https://example.com');
      expect(urls).toHaveLength(1);
    });
    
    it('should handle empty input', () => {
      expect(extractUrls(null)).toEqual([]);
      expect(extractUrls('')).toEqual([]);
    });
  });

  describe('Workflow stages', () => {
    const WORKFLOW_STAGES = {
      cip: ['cip-discuss', 'cip-vote', 'cip-announce', 'sv-announce'],
      'featured-app': ['tokenomics', 'tokenomics-announce', 'sv-announce'],
      validator: ['tokenomics', 'sv-announce'],
      'protocol-upgrade': ['tokenomics', 'sv-announce'],
      outcome: ['sv-announce'],
      other: ['tokenomics', 'sv-announce'],
    };
    
    it('should define CIP workflow correctly', () => {
      expect(WORKFLOW_STAGES.cip).toEqual(['cip-discuss', 'cip-vote', 'cip-announce', 'sv-announce']);
    });
    
    it('should define featured-app workflow correctly', () => {
      expect(WORKFLOW_STAGES['featured-app']).toContain('tokenomics');
      expect(WORKFLOW_STAGES['featured-app']).toContain('sv-announce');
    });
    
    it('should have sv-announce as final stage for all types', () => {
      for (const stages of Object.values(WORKFLOW_STAGES)) {
        expect(stages[stages.length - 1]).toBe('sv-announce');
      }
    });
  });

  describe('Confidence decay', () => {
    const DECAY_HALF_LIFE_DAYS = 30;
    const MIN_CONFIDENCE = 0.1;
    
    const calculatePatternConfidence = (createdAt, lastReinforced, baseConfidence = 1.0) => {
      if (!createdAt) return 1.0;
      
      const createdAtMs = new Date(createdAt).getTime();
      const lastReinforcedMs = lastReinforced ? new Date(lastReinforced).getTime() : createdAtMs;
      const now = Date.now();
      
      const daysSinceReinforcement = (now - lastReinforcedMs) / (1000 * 60 * 60 * 24);
      const decayFactor = Math.pow(0.5, daysSinceReinforcement / DECAY_HALF_LIFE_DAYS);
      
      return Math.max(MIN_CONFIDENCE, baseConfidence * decayFactor);
    };
    
    it('should have full confidence for new patterns', () => {
      const now = new Date().toISOString();
      const confidence = calculatePatternConfidence(now, now, 1.0);
      expect(confidence).toBeCloseTo(1.0, 1);
    });
    
    it('should decay to ~50% after half-life', () => {
      const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
      const confidence = calculatePatternConfidence(thirtyDaysAgo, thirtyDaysAgo, 1.0);
      expect(confidence).toBeCloseTo(0.5, 1);
    });
    
    it('should not go below minimum confidence', () => {
      const yearAgo = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString();
      const confidence = calculatePatternConfidence(yearAgo, yearAgo, 1.0);
      expect(confidence).toBeGreaterThanOrEqual(MIN_CONFIDENCE);
    });
    
    it('should reset decay on reinforcement', () => {
      const monthAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
      const now = new Date().toISOString();
      
      // Created a month ago but reinforced now
      const confidence = calculatePatternConfidence(monthAgo, now, 1.0);
      expect(confidence).toBeCloseTo(1.0, 1);
    });
  });

  describe('Governance group mapping', () => {
    const GOVERNANCE_GROUPS = {
      'cip-discuss': { stage: 'cip-discuss', flow: 'cip', label: 'CIP Discussion' },
      'cip-vote': { stage: 'cip-vote', flow: 'cip', label: 'CIP Vote' },
      'cip-announce': { stage: 'cip-announce', flow: 'cip', label: 'CIP Announcement' },
      'tokenomics': { stage: 'tokenomics', flow: 'shared', label: 'Tokenomics Discussion' },
      'tokenomics-announce': { stage: 'tokenomics-announce', flow: 'featured-app', label: 'Tokenomics Announcement' },
      'supervalidator-announce': { stage: 'sv-announce', flow: 'shared', label: 'SV Announcement' },
    };
    
    it('should map CIP groups correctly', () => {
      expect(GOVERNANCE_GROUPS['cip-discuss'].flow).toBe('cip');
      expect(GOVERNANCE_GROUPS['cip-vote'].flow).toBe('cip');
      expect(GOVERNANCE_GROUPS['cip-announce'].flow).toBe('cip');
    });
    
    it('should map tokenomics as shared flow', () => {
      expect(GOVERNANCE_GROUPS['tokenomics'].flow).toBe('shared');
    });
    
    it('should map sv-announce as shared flow', () => {
      expect(GOVERNANCE_GROUPS['supervalidator-announce'].stage).toBe('sv-announce');
      expect(GOVERNANCE_GROUPS['supervalidator-announce'].flow).toBe('shared');
    });
  });
});
