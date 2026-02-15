/**
 * Upload Integrity Verification Tests
 * 
 * Tests hash computation and comparison logic using
 * extracted pure functions (no module-level mocks needed).
 */

import { describe, it, expect } from 'vitest';
import { createHash } from 'crypto';

describe('Upload Integrity Verification', () => {

  describe('MD5 hash computation', () => {
    it('should compute base64-encoded MD5 matching GCS format', () => {
      const data = Buffer.from('test-parquet-data');
      const expected = createHash('md5').update(data).digest('base64');

      // Verify it produces a valid base64 string
      expect(expected).toMatch(/^[A-Za-z0-9+/]+=*$/);
      // Verify deterministic
      const again = createHash('md5').update(data).digest('base64');
      expect(again).toBe(expected);
    });

    it('should produce different hashes for different data', () => {
      const hash1 = createHash('md5').update(Buffer.from('file-a')).digest('base64');
      const hash2 = createHash('md5').update(Buffer.from('file-b')).digest('base64');
      expect(hash1).not.toBe(hash2);
    });
  });

  describe('gsutil stat MD5 parsing', () => {
    function parseGCSMD5(statOutput) {
      if (!statOutput) return null;
      const match = statOutput.match(/Hash \(md5\):\s+(\S+)/);
      return match ? match[1] : null;
    }

    it('should parse MD5 from standard gsutil stat output', () => {
      const output =
        'gs://bucket/test.parquet:\n' +
        '    Creation time:    Mon, 01 Jan 2026 00:00:00 GMT\n' +
        '    Content-Length:   1024\n' +
        '    Hash (crc32c):    abc123==\n' +
        '    Hash (md5):       XrY7u+Ae7tCTyyK7j1rNww==\n';

      expect(parseGCSMD5(output)).toBe('XrY7u+Ae7tCTyyK7j1rNww==');
    });

    it('should return null when output has no md5 line', () => {
      const output = 'gs://bucket/test.parquet:\n    Content-Length: 1024\n';
      expect(parseGCSMD5(output)).toBeNull();
    });

    it('should return null for empty/null output', () => {
      expect(parseGCSMD5(null)).toBeNull();
      expect(parseGCSMD5('')).toBeNull();
    });

    it('should handle hash with special base64 characters', () => {
      const output = '    Hash (md5):       A+B/c+d/E=\n';
      expect(parseGCSMD5(output)).toBe('A+B/c+d/E=');
    });
  });

  describe('integrity verification logic', () => {
    function checkIntegrity(localMD5, remoteMD5) {
      if (!remoteMD5) {
        return { ok: false, localMD5, error: 'Could not retrieve GCS object hash' };
      }
      if (localMD5 !== remoteMD5) {
        return { ok: false, localMD5, remoteMD5, error: `Hash mismatch: local=${localMD5} remote=${remoteMD5}` };
      }
      return { ok: true, localMD5, remoteMD5 };
    }

    it('should return ok:true when hashes match', () => {
      const hash = createHash('md5').update(Buffer.from('data')).digest('base64');
      const result = checkIntegrity(hash, hash);
      expect(result.ok).toBe(true);
      expect(result.localMD5).toBe(hash);
      expect(result.remoteMD5).toBe(hash);
    });

    it('should return ok:false when hashes mismatch', () => {
      const result = checkIntegrity('localHash==', 'remoteHash==');
      expect(result.ok).toBe(false);
      expect(result.error).toContain('Hash mismatch');
    });

    it('should return ok:false when remote hash is null', () => {
      const result = checkIntegrity('localHash==', null);
      expect(result.ok).toBe(false);
      expect(result.error).toContain('Could not retrieve');
    });

    it('should detect single-bit difference', () => {
      const data1 = Buffer.from([0x00, 0x01, 0x02]);
      const data2 = Buffer.from([0x00, 0x01, 0x03]); // 1 bit different
      const hash1 = createHash('md5').update(data1).digest('base64');
      const hash2 = createHash('md5').update(data2).digest('base64');

      const result = checkIntegrity(hash1, hash2);
      expect(result.ok).toBe(false);
    });
  });
});
