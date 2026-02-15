/**
 * Tests for TLS configuration across the ingestion pipeline.
 * 
 * Tests the ACTUAL getTLSRejectUnauthorized() export from fetch-updates.js
 * and verifies fetch-backfill.js uses the same INSECURE_TLS semantics.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { getTLSRejectUnauthorized } from '../fetch-updates.js';
import fs from 'fs';

describe('getTLSRejectUnauthorized (from fetch-updates.js)', () => {
  let originalInsecureTLS;

  beforeEach(() => {
    originalInsecureTLS = process.env.INSECURE_TLS;
  });

  afterEach(() => {
    if (originalInsecureTLS === undefined) {
      delete process.env.INSECURE_TLS;
    } else {
      process.env.INSECURE_TLS = originalInsecureTLS;
    }
  });

  it('returns true (secure) when INSECURE_TLS is not set', () => {
    delete process.env.INSECURE_TLS;
    expect(getTLSRejectUnauthorized()).toBe(true);
  });

  it('returns false when INSECURE_TLS is "true"', () => {
    process.env.INSECURE_TLS = 'true';
    expect(getTLSRejectUnauthorized()).toBe(false);
  });

  it('returns true when INSECURE_TLS is "false"', () => {
    process.env.INSECURE_TLS = 'false';
    expect(getTLSRejectUnauthorized()).toBe(true);
  });

  it('returns true when INSECURE_TLS is empty string', () => {
    process.env.INSECURE_TLS = '';
    expect(getTLSRejectUnauthorized()).toBe(true);
  });

  it('returns true when INSECURE_TLS is "TRUE" (case-sensitive, only lowercase "true" disables)', () => {
    process.env.INSECURE_TLS = 'TRUE';
    expect(getTLSRejectUnauthorized()).toBe(true);
  });

  it('returns true when INSECURE_TLS is "1" (only "true" disables)', () => {
    process.env.INSECURE_TLS = '1';
    expect(getTLSRejectUnauthorized()).toBe(true);
  });

  it('returns true when INSECURE_TLS is "yes" (only "true" disables)', () => {
    process.env.INSECURE_TLS = 'yes';
    expect(getTLSRejectUnauthorized()).toBe(true);
  });
});

describe('fetch-backfill.js TLS consistency', () => {
  it('uses per-agent INSECURE_TLS check, not global NODE_TLS_REJECT_UNAUTHORIZED', () => {
    const source = fs.readFileSync('scripts/ingest/fetch-backfill.js', 'utf8');
    
    // Must NOT set the global override
    expect(source).not.toContain('NODE_TLS_REJECT_UNAUTHORIZED');
    
    // Must use the same strict check as fetch-updates.js
    expect(source).toContain("process.env.INSECURE_TLS === 'true'");
  });

  it('uses rejectUnauthorized on the httpsAgent (per-agent, not global)', () => {
    const source = fs.readFileSync('scripts/ingest/fetch-backfill.js', 'utf8');
    
    // The httpsAgent should use rejectUnauthorized: !INSECURE_TLS
    expect(source).toContain('rejectUnauthorized: !INSECURE_TLS');
  });

  it('does not accept "1" or "yes" as valid INSECURE_TLS values', () => {
    const source = fs.readFileSync('scripts/ingest/fetch-backfill.js', 'utf8');
    
    // Must NOT have the old permissive check
    expect(source).not.toMatch(/\['1',\s*'true',\s*'yes'\]/);
  });
});
