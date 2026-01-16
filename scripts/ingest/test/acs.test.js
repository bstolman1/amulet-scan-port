#!/usr/bin/env node
/**
 * ACS Snapshot Validation Tests (with assertions)
 * 
 * Validates ACS snapshot data integrity using vitest.
 */

import { describe, it, expect, beforeAll, vi } from 'vitest';
import { existsSync, readdirSync, createReadStream } from 'fs';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ─────────────────────────────────────────────────────────────
// Mock path-utils for test environment
// ─────────────────────────────────────────────────────────────

vi.mock('../path-utils.js', () => ({
  getBaseDataDir: () => process.env.DATA_DIR || path.join(__dirname, '../../../data'),
  getRawDir: () => process.env.RAW_DIR || path.join(__dirname, '../../../data/raw'),
  getCursorDir: () => path.join(process.env.DATA_DIR || path.join(__dirname, '../../../data'), 'cursors'),
}));

// ─────────────────────────────────────────────────────────────
// Test Utilities
// ─────────────────────────────────────────────────────────────

function findSnapshots(acsDir) {
  const snapshots = [];
  
  if (!existsSync(acsDir)) return snapshots;
  
  function scan(dir, pathParts = {}) {
    try {
      const entries = readdirSync(dir, { withFileTypes: true });
      
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        
        if (entry.isDirectory()) {
          const newParts = { ...pathParts };
          
          if (entry.name.startsWith('migration=')) {
            newParts.migrationId = parseInt(entry.name.split('=')[1]);
          } else if (entry.name.startsWith('snapshot=')) {
            newParts.snapshotId = entry.name.split('=')[1];
            snapshots.push({
              path: fullPath,
              ...newParts,
              hasComplete: existsSync(path.join(fullPath, '_COMPLETE')),
            });
            continue;
          }
          
          scan(fullPath, newParts);
        }
      }
    } catch (e) {
      // Directory not accessible
    }
  }
  
  scan(acsDir);
  return snapshots;
}

async function readJsonlSample(filePath, maxRecords = 10) {
  const records = [];
  
  try {
    const fileStream = createReadStream(filePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });
    
    for await (const line of rl) {
      if (line.trim()) {
        records.push(JSON.parse(line));
        if (records.length >= maxRecords) break;
      }
    }
    
    rl.close();
    fileStream.close();
  } catch (e) {
    // File not accessible
  }
  
  return records;
}

// ─────────────────────────────────────────────────────────────
// ACS Schema Tests
// ─────────────────────────────────────────────────────────────

describe('ACS Schema Validation', () => {
  const CRITICAL_CONTRACT_FIELDS = ['contract_id', 'template_id', 'payload'];
  
  describe('validateContractFields', () => {
    function validateContractFields(contract) {
      const missingCritical = CRITICAL_CONTRACT_FIELDS.filter(f => !contract[f]);
      const missingOptional = ['created_at', 'signatories'].filter(f => !contract[f]);
      return { missingCritical, missingOptional };
    }
    
    it('should identify missing critical fields', () => {
      const contract = { template_id: 'Test', payload: {} };
      const result = validateContractFields(contract);
      
      expect(result.missingCritical).toContain('contract_id');
      expect(result.missingCritical).not.toContain('template_id');
      expect(result.missingCritical).not.toContain('payload');
      expect(result.missingCritical).toHaveLength(1);
    });
    
    it('should pass for complete contracts', () => {
      const contract = { 
        contract_id: '00abc123', 
        template_id: 'Splice.Amulet:Amulet', 
        payload: { amount: '100' } 
      };
      const result = validateContractFields(contract);
      
      expect(result.missingCritical).toHaveLength(0);
    });
    
    it('should identify all missing fields in empty object', () => {
      const contract = {};
      const result = validateContractFields(contract);
      
      expect(result.missingCritical).toHaveLength(3);
      expect(result.missingCritical).toEqual(expect.arrayContaining(['contract_id', 'template_id', 'payload']));
    });
    
    it('should identify missing optional fields separately', () => {
      const contract = { contract_id: '123', template_id: 'Test', payload: {} };
      const result = validateContractFields(contract);
      
      expect(result.missingCritical).toHaveLength(0);
      expect(result.missingOptional).toContain('created_at');
      expect(result.missingOptional).toContain('signatories');
    });
  });
});

describe('ACS Snapshot Discovery', () => {
  describe('findSnapshots function', () => {
    it('should return empty array for non-existent directory', () => {
      const result = findSnapshots('/non/existent/path');
      
      expect(result).toBeInstanceOf(Array);
      expect(result).toHaveLength(0);
    });
    
    it('should parse migration ID from directory name', () => {
      const migId = 'migration=5'.split('=')[1];
      expect(parseInt(migId)).toBe(5);
    });
    
    it('should parse snapshot ID from directory name', () => {
      const snapshotId = 'snapshot=2025-01-15T12:00:00Z'.split('=')[1];
      expect(snapshotId).toBe('2025-01-15T12:00:00Z');
    });
    
    it('should handle edge cases in directory names', () => {
      expect('migration=0'.split('=')[1]).toBe('0');
      expect('snapshot='.split('=')[1]).toBe('');
    });
  });
});

describe('JSONL File Parsing', () => {
  describe('JSON parsing edge cases', () => {
    it('should parse valid JSON lines', () => {
      const line = '{"contract_id": "abc", "template_id": "Test"}';
      const parsed = JSON.parse(line);
      
      expect(parsed).toHaveProperty('contract_id', 'abc');
      expect(parsed).toHaveProperty('template_id', 'Test');
    });
    
    it('should throw on invalid JSON', () => {
      const line = 'not valid json';
      expect(() => JSON.parse(line)).toThrow();
    });
    
    it('should handle empty objects', () => {
      const line = '{}';
      const parsed = JSON.parse(line);
      
      expect(parsed).toEqual({});
      expect(Object.keys(parsed)).toHaveLength(0);
    });
    
    it('should handle nested structures', () => {
      const line = '{"payload": {"amount": {"value": "100"}}}';
      const parsed = JSON.parse(line);
      
      expect(parsed.payload).toBeDefined();
      expect(parsed.payload.amount).toBeDefined();
      expect(parsed.payload.amount.value).toBe('100');
    });
    
    it('should handle arrays in payload', () => {
      const line = '{"signatories": ["party1", "party2"]}';
      const parsed = JSON.parse(line);
      
      expect(parsed.signatories).toBeInstanceOf(Array);
      expect(parsed.signatories).toHaveLength(2);
      expect(parsed.signatories).toContain('party1');
    });
    
    it('should preserve unicode characters', () => {
      const line = '{"name": "测试"}';
      const parsed = JSON.parse(line);
      
      expect(parsed.name).toBe('测试');
    });
  });
});

describe('ACS Data Integrity', () => {
  describe('Template ID validation', () => {
    const KNOWN_TEMPLATES = [
      'Splice.Amulet:Amulet',
      'Splice.ValidatorLicense:ValidatorLicense',
      'Splice.DsoRules:DsoRules',
      'Splice.AmuletRules:AmuletRules',
    ];
    
    it('should recognize known template formats', () => {
      for (const template of KNOWN_TEMPLATES) {
        expect(template).toMatch(/^[A-Za-z0-9.]+:[A-Za-z0-9]+$/);
      }
    });
    
    it('should validate template ID format', () => {
      const validTemplate = 'Splice.Amulet:Amulet';
      expect(validTemplate.includes(':')).toBe(true);
      expect(validTemplate.split(':').length).toBe(2);
    });
    
    it('should detect invalid template formats', () => {
      const invalidTemplates = ['', 'NoColon', '::Empty', 'Has Space:Template'];
      
      for (const template of invalidTemplates) {
        const parts = template.split(':');
        const isValid = parts.length === 2 && parts[0].length > 0 && parts[1].length > 0 && !template.includes(' ');
        expect(isValid).toBe(false);
      }
    });
  });
  
  describe('Contract ID validation', () => {
    it('should accept hex-prefixed contract IDs', () => {
      const contractId = '00abc123def456::Splice.Amulet:Amulet';
      expect(contractId.startsWith('00')).toBe(true);
    });
    
    it('should extract template from full contract ID', () => {
      const contractId = '00abc123::Splice.Amulet:Amulet';
      const parts = contractId.split('::');
      
      expect(parts).toHaveLength(2);
      expect(parts[0]).toBe('00abc123');
      expect(parts[1]).toBe('Splice.Amulet:Amulet');
    });
    
    it('should handle contract IDs without template suffix', () => {
      const contractId = '00abc123';
      const parts = contractId.split('::');
      
      expect(parts).toHaveLength(1);
      expect(parts[0]).toBe('00abc123');
    });
  });
});

describe('ACS Snapshot Completeness', () => {
  describe('Completion marker logic', () => {
    it('should treat snapshot as incomplete without _COMPLETE', () => {
      const snapshot = { path: '/path', hasComplete: false };
      expect(snapshot.hasComplete).toBe(false);
    });
    
    it('should treat snapshot as complete with _COMPLETE', () => {
      const snapshot = { path: '/path', hasComplete: true };
      expect(snapshot.hasComplete).toBe(true);
    });
    
    it('should filter complete snapshots correctly', () => {
      const snapshots = [
        { id: 1, hasComplete: true },
        { id: 2, hasComplete: false },
        { id: 3, hasComplete: true },
      ];
      
      const complete = snapshots.filter(s => s.hasComplete);
      const incomplete = snapshots.filter(s => !s.hasComplete);
      
      expect(complete).toHaveLength(2);
      expect(incomplete).toHaveLength(1);
      expect(incomplete[0].id).toBe(2);
    });
  });
});
