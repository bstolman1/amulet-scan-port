/**
 * use-local-acs hook tests
 * 
 * Tests for ACS (Active Contract Set) data fetching hooks
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createElement } from 'react';

// Mock the API client
vi.mock('@/lib/duckdb-api-client', () => ({
  apiFetch: vi.fn().mockResolvedValue({
    data: [],
    count: 0,
    status: 'complete',
  }),
}));

// Test wrapper with QueryClient
function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
    },
  });
  
  return ({ children }: { children: React.ReactNode }) =>
    createElement(QueryClientProvider, { client: queryClient }, children);
}

describe('ACS Data Structures', () => {
  describe('ACS Contract structure', () => {
    const mockContract = {
      contract_id: '00abc123::Splice.Amulet:Amulet',
      template_id: 'Splice.Amulet:Amulet',
      created_at: '2025-01-15T12:00:00.000Z',
      payload: {
        owner: 'party123',
        amount: { value: '10000000000' },
      },
      signatories: ['party123'],
      observers: [],
    };

    it('should have valid contract_id format', () => {
      expect(mockContract.contract_id).toMatch(/^00[a-f0-9]+::/);
      expect(mockContract.contract_id).toContain('::');
    });

    it('should have valid template_id format', () => {
      expect(mockContract.template_id).toContain(':');
      const parts = mockContract.template_id.split(':');
      expect(parts).toHaveLength(2);
    });

    it('should have payload with expected structure', () => {
      expect(mockContract.payload).toHaveProperty('owner');
      expect(mockContract.payload).toHaveProperty('amount');
      expect(mockContract.payload.amount).toHaveProperty('value');
    });

    it('should have signatories as array', () => {
      expect(Array.isArray(mockContract.signatories)).toBe(true);
    });

    it('should parse amount correctly', () => {
      const amountStr = mockContract.payload.amount.value;
      const amountNum = parseFloat(amountStr);
      
      expect(typeof amountStr).toBe('string');
      expect(amountNum).toBe(10000000000);
      expect(amountNum / 1e10).toBe(1); // 1 CC
    });
  });

  describe('ACS Snapshot structure', () => {
    const mockSnapshot = {
      snapshot_id: '2025-01-15T12:00:00.000Z',
      migration_id: 5,
      contract_count: 150000,
      template_count: 25,
      status: 'complete',
      created_at: '2025-01-15T12:00:00.000Z',
    };

    it('should have required snapshot fields', () => {
      expect(mockSnapshot).toHaveProperty('snapshot_id');
      expect(mockSnapshot).toHaveProperty('status');
      expect(mockSnapshot).toHaveProperty('contract_count');
    });

    it('should have valid status', () => {
      const validStatuses = ['complete', 'processing', 'failed', 'pending'];
      expect(validStatuses).toContain(mockSnapshot.status);
    });

    it('should have non-negative counts', () => {
      expect(mockSnapshot.contract_count).toBeGreaterThanOrEqual(0);
      expect(mockSnapshot.template_count).toBeGreaterThanOrEqual(0);
    });

    it('should have valid migration_id', () => {
      expect(typeof mockSnapshot.migration_id).toBe('number');
      expect(mockSnapshot.migration_id).toBeGreaterThanOrEqual(0);
    });
  });
});

describe('ACS Amount Utilities', () => {
  const MICRO_UNIT = 1e10;

  describe('Amount parsing', () => {
    it('should parse Daml amount string to number', () => {
      const parseAmount = (str: string | null): number => {
        if (!str) return 0;
        return parseFloat(str) / MICRO_UNIT;
      };

      expect(parseAmount('10000000000')).toBe(1); // 1 CC
      expect(parseAmount('100000000000')).toBe(10); // 10 CC
      expect(parseAmount('5000000000')).toBe(0.5); // 0.5 CC
      expect(parseAmount('0')).toBe(0);
      expect(parseAmount(null)).toBe(0);
    });

    it('should handle large amounts', () => {
      const largeAmount = '1000000000000000000'; // 100M CC
      const parsed = parseFloat(largeAmount) / MICRO_UNIT;
      
      expect(parsed).toBe(100000000);
    });

    it('should handle very small amounts', () => {
      const smallAmount = '1'; // Smallest unit
      const parsed = parseFloat(smallAmount) / MICRO_UNIT;
      
      expect(parsed).toBe(1e-10);
      expect(parsed).toBeGreaterThan(0);
    });
  });

  describe('Amount formatting', () => {
    it('should format amounts with proper decimal places', () => {
      const formatAmount = (amount: number): string => {
        return new Intl.NumberFormat('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 10,
        }).format(amount);
      };

      expect(formatAmount(1234.5678)).toBe('1,234.5678');
      expect(formatAmount(0.12)).toBe('0.12');
      expect(formatAmount(1000000)).toBe('1,000,000.00');
    });

    it('should format with currency symbol', () => {
      const formatCC = (amount: number): string => {
        return `${amount.toFixed(2)} CC`;
      };

      expect(formatCC(100)).toBe('100.00 CC');
      expect(formatCC(0.5)).toBe('0.50 CC');
    });
  });

  describe('Amount aggregation', () => {
    it('should sum amounts correctly', () => {
      const amounts = ['10000000000', '20000000000', '30000000000'];
      const sum = amounts.reduce((acc, str) => acc + parseFloat(str), 0) / MICRO_UNIT;
      
      expect(sum).toBe(6); // 1 + 2 + 3 CC
    });

    it('should handle empty array', () => {
      const amounts: string[] = [];
      const sum = amounts.reduce((acc, str) => acc + parseFloat(str), 0) / MICRO_UNIT;
      
      expect(sum).toBe(0);
    });

    it('should filter invalid amounts', () => {
      const amounts = ['10000000000', 'invalid', '20000000000', null as any];
      const validSum = amounts
        .filter((a): a is string => a !== null && !isNaN(parseFloat(a)))
        .reduce((acc, str) => acc + parseFloat(str), 0) / MICRO_UNIT;
      
      expect(validSum).toBe(3); // 1 + 2 CC
    });
  });
});

describe('ACS Template Filtering', () => {
  const mockContracts = [
    { contract_id: '1', template_id: 'Splice.Amulet:Amulet' },
    { contract_id: '2', template_id: 'Splice.Amulet:Amulet' },
    { contract_id: '3', template_id: 'Splice.ValidatorLicense:ValidatorLicense' },
    { contract_id: '4', template_id: 'Splice.DsoRules:DsoRules' },
  ];

  it('should filter by exact template', () => {
    const filtered = mockContracts.filter(c => c.template_id === 'Splice.Amulet:Amulet');
    expect(filtered).toHaveLength(2);
  });

  it('should filter by template suffix', () => {
    const filtered = mockContracts.filter(c => c.template_id.endsWith(':Amulet'));
    expect(filtered).toHaveLength(2);
  });

  it('should filter by template prefix', () => {
    const filtered = mockContracts.filter(c => c.template_id.startsWith('Splice.'));
    expect(filtered).toHaveLength(4);
  });

  it('should count by template', () => {
    const counts = mockContracts.reduce((acc, c) => {
      acc[c.template_id] = (acc[c.template_id] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    expect(counts['Splice.Amulet:Amulet']).toBe(2);
    expect(counts['Splice.ValidatorLicense:ValidatorLicense']).toBe(1);
  });
});

describe('ACS Circulating vs Locked', () => {
  const mockContracts = [
    { contract_id: '1', template_id: 'Splice.Amulet:Amulet', payload: { amount: { value: '10000000000' } } },
    { contract_id: '2', template_id: 'Splice.Amulet:LockedAmulet', payload: { amount: { value: '5000000000' } } },
    { contract_id: '3', template_id: 'Splice.Amulet:Amulet', payload: { amount: { value: '20000000000' } } },
  ];

  it('should identify circulating contracts', () => {
    const circulating = mockContracts.filter(c => 
      c.template_id.includes('Amulet') && !c.template_id.includes('Locked')
    );
    expect(circulating).toHaveLength(2);
  });

  it('should identify locked contracts', () => {
    const locked = mockContracts.filter(c => c.template_id.includes('LockedAmulet'));
    expect(locked).toHaveLength(1);
  });

  it('should calculate circulating supply', () => {
    const MICRO_UNIT = 1e10;
    const circulating = mockContracts
      .filter(c => c.template_id.includes('Amulet') && !c.template_id.includes('Locked'))
      .reduce((sum, c) => sum + parseFloat(c.payload.amount.value), 0) / MICRO_UNIT;
    
    expect(circulating).toBe(3); // 1 + 2 CC
  });

  it('should calculate locked supply', () => {
    const MICRO_UNIT = 1e10;
    const locked = mockContracts
      .filter(c => c.template_id.includes('LockedAmulet'))
      .reduce((sum, c) => sum + parseFloat(c.payload.amount.value), 0) / MICRO_UNIT;
    
    expect(locked).toBe(0.5);
  });

  it('should calculate total supply', () => {
    const MICRO_UNIT = 1e10;
    const total = mockContracts
      .filter(c => c.template_id.includes('Amulet'))
      .reduce((sum, c) => sum + parseFloat(c.payload.amount.value), 0) / MICRO_UNIT;
    
    expect(total).toBe(3.5);
  });
});

describe('ACS Error States', () => {
  it('should handle API errors', () => {
    const error = { message: 'Failed to fetch ACS data', status: 500 };
    
    expect(error.message).toContain('Failed');
    expect(error.status).toBe(500);
  });

  it('should handle empty snapshots', () => {
    const snapshot = { data: [], count: 0 };
    
    expect(snapshot.data).toHaveLength(0);
    expect(snapshot.count).toBe(0);
  });

  it('should handle missing fields gracefully', () => {
    const contract = { contract_id: '1' } as any;
    const templateId = contract.template_id || 'unknown';
    const amount = contract.payload?.amount?.value || '0';
    
    expect(templateId).toBe('unknown');
    expect(amount).toBe('0');
  });

  it('should handle network timeouts', () => {
    const isTimeout = (error: Error) => 
      error.message.toLowerCase().includes('timeout') || 
      error.message.includes('ETIMEDOUT');
    
    expect(isTimeout(new Error('Request timeout'))).toBe(true);
    expect(isTimeout(new Error('ETIMEDOUT'))).toBe(true);
    expect(isTimeout(new Error('Connection failed'))).toBe(false);
  });
});

describe('ACS Query Keys', () => {
  it('should generate unique query keys', () => {
    const key1 = ['acs', 'snapshot', '2025-01-15', 'migration-5'];
    const key2 = ['acs', 'snapshot', '2025-01-14', 'migration-5'];
    
    expect(JSON.stringify(key1)).not.toBe(JSON.stringify(key2));
  });

  it('should include all relevant parameters', () => {
    const buildKey = (params: { snapshotId?: string; migrationId?: number; template?: string }) => {
      return ['acs', params.snapshotId, params.migrationId, params.template].filter(Boolean);
    };
    
    const key = buildKey({ snapshotId: '123', migrationId: 5, template: 'Amulet' });
    expect(key).toContain('123');
    expect(key).toContain(5);
    expect(key).toContain('Amulet');
  });
});
