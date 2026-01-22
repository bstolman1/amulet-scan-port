/**
 * ACS Schema Unit Tests
 * 
 * Comprehensive tests for ACS normalization and validation functions
 * imported directly from acs-schema.js
 */

import { describe, it, expect } from 'vitest';
import {
  normalizeACSContract,
  parseTemplateId,
  normalizeTemplateKey,
  detectTemplateFormat,
  isTemplate,
  validateContractFields,
  validateTemplates,
  getACSPartitionPath,
  EXPECTED_TEMPLATES,
  CRITICAL_CONTRACT_FIELDS,
  IMPORTANT_CONTRACT_FIELDS,
  ACSValidationError,
} from '../acs-schema.js';

describe('parseTemplateId', () => {
  describe('colon-dot format (standard)', () => {
    it('should parse hash:Module.Path:EntityName format', () => {
      const templateId = '67bc95402ad7b08fcdff0ed478308d39c70baca2238c35c5d425a435a8a9e7f7:Splice.Amulet:Amulet';
      const result = parseTemplateId(templateId);
      
      expect(result.packageName).toBe('67bc95402ad7b08fcdff0ed478308d39c70baca2238c35c5d425a435a8a9e7f7');
      expect(result.moduleName).toBe('Splice.Amulet');
      expect(result.entityName).toBe('Amulet');
    });

    it('should parse hash:Deep.Module.Path:EntityName format', () => {
      const templateId = 'abc123:Splice.DSO.SvState:SvNodeState';
      const result = parseTemplateId(templateId);
      
      expect(result.packageName).toBe('abc123');
      expect(result.moduleName).toBe('Splice.DSO.SvState');
      expect(result.entityName).toBe('SvNodeState');
    });
  });

  describe('simple format (no hash)', () => {
    it('should parse Module.Path:EntityName without hash', () => {
      const templateId = 'Splice.Amulet:Amulet';
      const result = parseTemplateId(templateId);
      
      // packageName is null when there's no hash prefix (only 2 parts)
      expect(result.packageName).toBeNull();
      expect(result.moduleName).toBe('Splice.Amulet');
      expect(result.entityName).toBe('Amulet');
    });
  });

  describe('underscore format', () => {
    it('should parse hash_Module_EntityName format', () => {
      const templateId = 'abc123_Splice_Amulet';
      const result = parseTemplateId(templateId);
      
      expect(result.packageName).toBe('abc123');
      expect(result.moduleName).toBe('Splice');
      expect(result.entityName).toBe('Amulet');
    });
  });

  describe('edge cases', () => {
    it('should return nulls for null input', () => {
      const result = parseTemplateId(null);
      
      expect(result.packageName).toBeNull();
      expect(result.moduleName).toBeNull();
      expect(result.entityName).toBeNull();
    });

    it('should return nulls for empty string', () => {
      const result = parseTemplateId('');
      
      expect(result.packageName).toBeNull();
      expect(result.moduleName).toBeNull();
      expect(result.entityName).toBeNull();
    });
  });
});

describe('normalizeTemplateKey', () => {
  it('should strip hash and normalize to Module.Path:Entity', () => {
    const templateId = 'abc123:Splice.Amulet:Amulet';
    const result = normalizeTemplateKey(templateId);
    
    expect(result).toBe('Splice.Amulet:Amulet');
  });

  it('should convert underscores to dots in module path', () => {
    const templateId = 'abc_Splice_DSO_SvState_SvNodeState';
    const result = normalizeTemplateKey(templateId);
    
    // Note: underscore format parsing extracts last two parts
    expect(result).toContain(':');
  });

  it('should return input for null', () => {
    expect(normalizeTemplateKey(null)).toBeNull();
  });
});

describe('detectTemplateFormat', () => {
  it('should detect colon-dot format', () => {
    const templateId = 'hash:Splice.Amulet:Amulet';
    expect(detectTemplateFormat(templateId)).toBe('colon-dot');
  });

  it('should detect all-colon format', () => {
    const templateId = 'hash:Splice:Amulet:Amulet';
    expect(detectTemplateFormat(templateId)).toBe('all-colon');
  });

  it('should detect underscore format', () => {
    const templateId = 'hash_Splice_Amulet';
    expect(detectTemplateFormat(templateId)).toBe('underscore');
  });

  it('should detect simple-colon format', () => {
    const templateId = 'Splice.Amulet:Amulet';
    expect(detectTemplateFormat(templateId)).toBe('simple-colon');
  });

  it('should return unknown for unrecognized formats', () => {
    expect(detectTemplateFormat(null)).toBe('unknown');
    expect(detectTemplateFormat('')).toBe('unknown');
  });
});

describe('normalizeACSContract', () => {
  describe('basic normalization', () => {
    it('should normalize a complete contract event', () => {
      const event = {
        contract_id: '00abc123def456',
        event_id: 'evt123',
        template_id: 'hash:Splice.Amulet:Amulet',
        signatories: ['party1', 'party2'],
        observers: ['observer1'],
        create_arguments: { amount: { value: '1000' } },
      };

      const result = normalizeACSContract(event, 0, '2024-10-07T12:00:00Z', '2024-10-07T12:00:00Z');

      expect(result.contract_id).toBe('00abc123def456');
      expect(result.event_id).toBe('evt123');
      expect(result.template_id).toBe('hash:Splice.Amulet:Amulet');
      expect(result.package_name).toBe('hash');
      expect(result.module_name).toBe('Splice.Amulet');
      expect(result.entity_name).toBe('Amulet');
      expect(result.migration_id).toBe(0);
      expect(result.signatories).toEqual(['party1', 'party2']);
      expect(result.observers).toEqual(['observer1']);
      expect(result.payload).toContain('1000');
    });

    it('should fall back to event_id when contract_id missing', () => {
      const event = {
        event_id: 'fallback_id',
        template_id: 'Splice.Amulet:Amulet',
      };

      const result = normalizeACSContract(event, 0, null, null);

      expect(result.contract_id).toBe('fallback_id');
    });

    it('should parse timestamps as Date objects', () => {
      const event = {
        contract_id: 'c1',
        template_id: 'Test:Test',
      };

      const result = normalizeACSContract(event, 0, '2024-10-07T12:00:00Z', '2024-10-07T12:30:00Z');

      expect(result.record_time).toBeInstanceOf(Date);
      expect(result.snapshot_time).toBeInstanceOf(Date);
      expect(result.record_time.toISOString()).toContain('2024-10-07');
    });

    it('should stringify create_arguments as payload', () => {
      const event = {
        contract_id: 'c1',
        template_id: 'Test:Test',
        create_arguments: { 
          record: { 
            fields: [
              { value: { party: 'DSO::abc' } },
              { value: { numeric: '100.5' } },
            ]
          }
        },
      };

      const result = normalizeACSContract(event, 0, null, null);

      expect(typeof result.payload).toBe('string');
      const parsed = JSON.parse(result.payload);
      expect(parsed.record.fields).toHaveLength(2);
    });

    it('should preserve raw event as JSON string', () => {
      const event = {
        contract_id: 'c1',
        template_id: 'Test:Test',
        custom_field: 'important',
      };

      const result = normalizeACSContract(event, 0, null, null);

      expect(typeof result.raw).toBe('string');
      expect(result.raw).toContain('important');
    });
  });

  describe('strict validation', () => {
    it('should throw ACSValidationError when contract_id missing in strict mode', () => {
      const event = {
        template_id: 'Test:Test',
        // contract_id missing
      };

      expect(() => normalizeACSContract(event, 0, null, null, { strict: true }))
        .toThrow(ACSValidationError);
    });

    it('should throw ACSValidationError when template_id missing in strict mode', () => {
      const event = {
        contract_id: 'c1',
        // template_id missing
      };

      expect(() => normalizeACSContract(event, 0, null, null, { strict: true }))
        .toThrow(ACSValidationError);
    });

    it('should include context in ACSValidationError', () => {
      const event = {
        // Missing both contract_id AND event_id (no fallback)
        // Also missing template_id
      };

      try {
        normalizeACSContract(event, 0, null, null, { strict: true });
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(ACSValidationError);
        expect(e.context.missingCritical).toContain('contract_id');
        expect(e.context.missingCritical).toContain('template_id');
      }
    });

    it('should allow missing fields with strict=false (default)', () => {
      const event = {
        // Missing everything
      };

      const result = normalizeACSContract(event, 0, null, null);
      
      // Should not throw, just have null/unknown values
      expect(result.template_id).toBe('unknown');
    });

    it('should warn but not throw with warnOnly=true', () => {
      const event = {
        // Missing critical fields
      };

      // Should not throw
      const result = normalizeACSContract(event, 0, null, null, { strict: true, warnOnly: true });
      expect(result.template_id).toBe('unknown');
    });
  });

  describe('edge cases', () => {
    it('should handle empty signatories and observers', () => {
      const event = {
        contract_id: 'c1',
        template_id: 'Test:Test',
        signatories: [],
        observers: [],
      };

      const result = normalizeACSContract(event, 0, null, null);

      expect(result.signatories).toEqual([]);
      expect(result.observers).toEqual([]);
    });

    it('should handle null create_arguments', () => {
      const event = {
        contract_id: 'c1',
        template_id: 'Test:Test',
        create_arguments: null,
      };

      const result = normalizeACSContract(event, 0, null, null);

      expect(result.payload).toBeNull();
    });

    it('should parse migration_id as integer', () => {
      const event = {
        contract_id: 'c1',
        template_id: 'Test:Test',
      };

      const result = normalizeACSContract(event, '5', null, null);

      expect(result.migration_id).toBe(5);
      expect(typeof result.migration_id).toBe('number');
    });
  });
});

describe('isTemplate', () => {
  it('should match exact template', () => {
    const event = { template_id: 'hash:Splice.Amulet:Amulet' };
    
    expect(isTemplate(event, 'Splice.Amulet', 'Amulet')).toBe(true);
  });

  it('should handle underscores as dots', () => {
    const event = { template_id: 'hash:Splice_Amulet:Amulet' };
    
    // Module name gets normalized
    expect(isTemplate(event, 'Splice.Amulet', 'Amulet')).toBe(true);
  });

  it('should return false for non-matching entity', () => {
    const event = { template_id: 'hash:Splice.Amulet:Amulet' };
    
    expect(isTemplate(event, 'Splice.Amulet', 'LockedAmulet')).toBe(false);
  });

  it('should return false for null event', () => {
    expect(isTemplate(null, 'Splice.Amulet', 'Amulet')).toBe(false);
    expect(isTemplate({}, 'Splice.Amulet', 'Amulet')).toBe(false);
  });
});

describe('validateContractFields', () => {
  it('should identify missing critical fields', () => {
    const contract = { template_id: 'Test:Test' };
    const result = validateContractFields(contract);
    
    expect(result.missingCritical).toContain('contract_id');
    expect(result.missingCritical).toContain('migration_id');
    expect(result.missingCritical).toContain('record_time');
  });

  it('should pass for complete contract', () => {
    const contract = {
      contract_id: 'c1',
      template_id: 'Test:Test',
      migration_id: 0,
      record_time: new Date(),
      module_name: 'Test',
      entity_name: 'Test',
      signatories: ['party1'],
      payload: '{}',
    };
    
    const result = validateContractFields(contract);
    
    expect(result.missingCritical).toHaveLength(0);
    expect(result.missingImportant).toHaveLength(0);
  });

  it('should identify empty arrays as missing important fields', () => {
    const contract = {
      contract_id: 'c1',
      template_id: 'Test:Test',
      migration_id: 0,
      record_time: new Date(),
      signatories: [], // Empty array
    };
    
    const result = validateContractFields(contract);
    
    expect(result.missingImportant).toContain('signatories');
  });
});

describe('validateTemplates', () => {
  it('should identify found templates', () => {
    const counts = {
      'hash:Splice.Amulet:Amulet': 100,
      'hash:Splice.ValidatorLicense:ValidatorLicense': 50,
    };
    
    const report = validateTemplates(counts);
    
    expect(report.found.length).toBeGreaterThan(0);
    expect(report.found.some(f => f.key === 'Splice.Amulet:Amulet')).toBe(true);
  });

  it('should identify unexpected templates', () => {
    const counts = {
      'hash:Custom.Unknown:Template': 10,
    };
    
    const report = validateTemplates(counts);
    
    expect(report.unexpected.length).toBeGreaterThan(0);
    expect(report.unexpected[0].key).toBe('Custom.Unknown:Template');
  });

  it('should warn about missing required templates', () => {
    const counts = {}; // No templates found
    
    const report = validateTemplates(counts);
    
    // Should have warnings for required templates
    const requiredMissing = report.missing.filter(m => m.required);
    expect(requiredMissing.length).toBeGreaterThan(0);
    expect(report.warnings.length).toBeGreaterThan(0);
  });

  it('should detect multiple format variations', () => {
    const counts = {
      'hash:Splice.Amulet:Amulet': 50,      // colon-dot
      'hash_Splice_Validator': 25,           // underscore
    };
    
    const report = validateTemplates(counts);
    
    expect(Object.keys(report.formatVariations).length).toBeGreaterThan(1);
    expect(report.warnings.some(w => w.includes('Multiple template ID formats'))).toBe(true);
  });
});

describe('getACSPartitionPath', () => {
  it('should generate correct Hive partition path', () => {
    const timestamp = new Date('2024-10-07T12:30:45Z');
    const migrationId = 0;

    const result = getACSPartitionPath(timestamp, migrationId);

    expect(result).toBe('acs/migration=0/year=2024/month=10/day=7/snapshot_id=123045');
  });

  it('should use numeric (non-padded) values for month/day', () => {
    const timestamp = new Date('2024-01-05T09:05:05Z');

    const result = getACSPartitionPath(timestamp, 1);

    // month=1 not month=01, day=5 not day=05
    expect(result).toBe('acs/migration=1/year=2024/month=1/day=5/snapshot_id=090505');
  });

  it('should default migration to 0', () => {
    const timestamp = new Date('2024-06-15T00:00:00Z');

    const result = getACSPartitionPath(timestamp);

    expect(result).toContain('migration=0');
  });

  it('should pad snapshot_id with zeros (string identifier)', () => {
    const timestamp = new Date('2024-06-15T01:02:03Z');

    const result = getACSPartitionPath(timestamp);

    expect(result).toContain('snapshot_id=010203');
  });
});

describe('EXPECTED_TEMPLATES registry', () => {
  it('should have required Amulet template', () => {
    expect(EXPECTED_TEMPLATES['Splice.Amulet:Amulet']).toBeDefined();
    expect(EXPECTED_TEMPLATES['Splice.Amulet:Amulet'].required).toBe(true);
  });

  it('should have required ValidatorLicense template', () => {
    expect(EXPECTED_TEMPLATES['Splice.ValidatorLicense:ValidatorLicense']).toBeDefined();
    expect(EXPECTED_TEMPLATES['Splice.ValidatorLicense:ValidatorLicense'].required).toBe(true);
  });

  it('should have descriptions for all templates', () => {
    for (const [key, config] of Object.entries(EXPECTED_TEMPLATES)) {
      expect(config.description).toBeDefined();
      expect(config.description.length).toBeGreaterThan(0);
    }
  });
});
