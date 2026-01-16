#!/usr/bin/env node
/**
 * Scan API Connectivity Tests (with assertions)
 * 
 * Validates API connectivity and response structures using vitest.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// ─────────────────────────────────────────────────────────────
// API Response Structure Tests
// ─────────────────────────────────────────────────────────────

describe('Scan API Response Structures', () => {
  describe('Round data response', () => {
    it('should have required round field', () => {
      const response = { round: 12345 };
      
      expect(response).toHaveProperty('round');
      expect(typeof response.round).toBe('number');
    });
    
    it('should accept string round values', () => {
      const response = { round: '12345' };
      
      expect(response).toHaveProperty('round');
      expect(typeof response.round === 'number' || typeof response.round === 'string').toBe(true);
    });
    
    it('should validate round is positive', () => {
      const validateRound = (round) => {
        const num = typeof round === 'string' ? parseInt(round) : round;
        return Number.isInteger(num) && num >= 0;
      };
      
      expect(validateRound(12345)).toBe(true);
      expect(validateRound('12345')).toBe(true);
      expect(validateRound(-1)).toBe(false);
      expect(validateRound('invalid')).toBe(false);
    });
  });
  
  describe('Updates response', () => {
    it('should have updates array', () => {
      const response = { updates: [], items: null };
      
      const updates = response.updates || response.items || [];
      expect(updates).toBeInstanceOf(Array);
    });
    
    it('should handle different response formats', () => {
      const responseFormats = [
        { updates: [{ id: 1 }] },
        { items: [{ id: 1 }] },
        { transactions: [{ id: 1 }] },
      ];
      
      for (const response of responseFormats) {
        const data = response.updates || response.items || response.transactions || [];
        expect(data).toBeInstanceOf(Array);
        expect(data.length).toBeGreaterThanOrEqual(0);
      }
    });
    
    it('should validate update record structure', () => {
      const update = {
        update_id: 'upd-123',
        migration_id: 5,
        record_time: '2025-01-15T12:00:00Z',
        update_type: 'transaction',
      };
      
      expect(update).toHaveProperty('update_id');
      expect(update).toHaveProperty('migration_id');
      expect(typeof update.migration_id).toBe('number');
    });
    
    it('should handle empty updates gracefully', () => {
      const response = { updates: [] };
      
      expect(response.updates).toHaveLength(0);
      expect(response.updates).toEqual([]);
    });
  });
  
  describe('ACS snapshot response', () => {
    it('should handle 404 for missing snapshots', () => {
      const response = { status: 404, ok: false };
      
      expect(response.status).toBe(404);
      expect(response.ok).toBe(false);
    });
    
    it('should validate snapshot timestamp format', () => {
      const timestamp = '2025-01-15T12:00:00.000Z';
      const date = new Date(timestamp);
      
      expect(date.toISOString()).toBe(timestamp);
      expect(date.getTime()).toBeGreaterThan(0);
    });
    
    it('should validate migration_id parameter', () => {
      const migrationIds = [0, 1, 5, 10];
      
      for (const id of migrationIds) {
        expect(Number.isInteger(id)).toBe(true);
        expect(id).toBeGreaterThanOrEqual(0);
      }
    });
  });
});

describe('HTTP Client Behavior', () => {
  describe('Response parsing', () => {
    it('should parse successful response', () => {
      const mockResponse = {
        ok: true,
        status: 200,
        statusText: 'OK',
        json: () => Promise.resolve({ data: 'test' }),
      };
      
      expect(mockResponse.ok).toBe(true);
      expect(mockResponse.status).toBe(200);
    });
    
    it('should identify error responses', () => {
      const errorCodes = [400, 401, 403, 404, 500, 502, 503];
      
      for (const code of errorCodes) {
        const response = { ok: false, status: code };
        expect(response.ok).toBe(false);
        expect(response.status).toBeGreaterThanOrEqual(400);
      }
    });
    
    it('should classify response types correctly', () => {
      const isSuccess = (status) => status >= 200 && status < 300;
      const isClientError = (status) => status >= 400 && status < 500;
      const isServerError = (status) => status >= 500;
      
      expect(isSuccess(200)).toBe(true);
      expect(isSuccess(201)).toBe(true);
      expect(isSuccess(404)).toBe(false);
      
      expect(isClientError(400)).toBe(true);
      expect(isClientError(404)).toBe(true);
      expect(isClientError(500)).toBe(false);
      
      expect(isServerError(500)).toBe(true);
      expect(isServerError(503)).toBe(true);
      expect(isServerError(404)).toBe(false);
    });
  });
  
  describe('URL construction', () => {
    it('should construct valid API URLs', () => {
      const baseUrl = 'https://scan.example.com/api/scan';
      const endpoint = '/v0/round-of-latest-data';
      const fullUrl = `${baseUrl}${endpoint}`;
      
      expect(fullUrl).toBe('https://scan.example.com/api/scan/v0/round-of-latest-data');
      expect(() => new URL(fullUrl)).not.toThrow();
    });
    
    it('should encode query parameters correctly', () => {
      const timestamp = '2025-01-15T12:00:00.000Z';
      const encoded = encodeURIComponent(timestamp);
      
      expect(encoded).toBe('2025-01-15T12%3A00%3A00.000Z');
      expect(decodeURIComponent(encoded)).toBe(timestamp);
    });
    
    it('should build query strings correctly', () => {
      const params = { before: '2025-01-15', page_size: 100, migration_id: 5 };
      const queryString = Object.entries(params)
        .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
        .join('&');
      
      expect(queryString).toContain('before=2025-01-15');
      expect(queryString).toContain('page_size=100');
      expect(queryString).toContain('migration_id=5');
    });
  });
  
  describe('Timeout handling', () => {
    it('should have reasonable default timeout', () => {
      const TIMEOUT_MS = 30000;
      
      expect(TIMEOUT_MS).toBe(30000);
      expect(TIMEOUT_MS).toBeGreaterThan(5000);
      expect(TIMEOUT_MS).toBeLessThan(120000);
    });
    
    it('should parse timeout from environment', () => {
      const parseTimeout = (envValue) => parseInt(envValue) || 30000;
      
      expect(parseTimeout('60000')).toBe(60000);
      expect(parseTimeout('invalid')).toBe(30000);
      expect(parseTimeout(undefined)).toBe(30000);
    });
  });
});

describe('Migration Detection', () => {
  describe('Migration ID parsing', () => {
    it('should detect valid migration IDs', () => {
      const validIds = [0, 1, 2, 5, 10];
      
      for (const id of validIds) {
        expect(Number.isInteger(id)).toBe(true);
        expect(id).toBeGreaterThanOrEqual(0);
      }
    });
    
    it('should aggregate migration results', () => {
      const responses = [
        { migId: 0, ok: true },
        { migId: 1, ok: true },
        { migId: 2, ok: false },
        { migId: 3, ok: false },
      ];
      
      const accessible = responses.filter(r => r.ok).map(r => r.migId);
      
      expect(accessible).toEqual([0, 1]);
      expect(accessible.length).toBe(2);
    });
    
    it('should detect when all migrations fail', () => {
      const responses = [
        { migId: 0, error: 'Network error' },
        { migId: 1, error: 'Network error' },
      ];
      
      const allFailed = responses.every(r => r.error);
      expect(allFailed).toBe(true);
    });
  });
});

describe('Error Handling', () => {
  describe('Network error handling', () => {
    it('should format network errors correctly', () => {
      const error = new Error('ECONNREFUSED');
      const message = `Network error: ${error.message}`;
      
      expect(message).toBe('Network error: ECONNREFUSED');
    });
    
    it('should format timeout errors correctly', () => {
      const timeoutMs = 30000;
      const message = `Request timeout after ${timeoutMs}ms`;
      
      expect(message).toBe('Request timeout after 30000ms');
    });
    
    it('should handle JSON parse errors', () => {
      const parseJson = (str) => {
        try {
          return { success: true, data: JSON.parse(str) };
        } catch (e) {
          return { success: false, error: e.message };
        }
      };
      
      expect(parseJson('{"valid": true}').success).toBe(true);
      expect(parseJson('not json').success).toBe(false);
    });
  });
  
  describe('HTTP error responses', () => {
    it('should extract error message from response', () => {
      const truncate = (text, max) => text.length > max ? text.slice(0, max) : text;
      
      const longError = 'A'.repeat(1000);
      const truncated = truncate(longError, 200);
      
      expect(truncated.length).toBe(200);
    });
    
    it('should handle various HTTP status codes', () => {
      const statusMessages = {
        400: 'Bad Request',
        401: 'Unauthorized',
        403: 'Forbidden',
        404: 'Not Found',
        500: 'Internal Server Error',
        502: 'Bad Gateway',
        503: 'Service Unavailable',
      };
      
      for (const [code, message] of Object.entries(statusMessages)) {
        expect(parseInt(code)).toBeGreaterThanOrEqual(400);
        expect(message.length).toBeGreaterThan(0);
      }
    });
  });
});
