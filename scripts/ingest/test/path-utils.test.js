/**
 * Path Utilities Tests
 * 
 * Tests cross-platform path handling critical for:
 * - Windows/Linux compatibility
 * - GCS mode detection
 * - Directory structure consistency
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  isWindows,
  isWindowsPath,
  isLinuxPath,
  WIN_DEFAULT,
  LINUX_DEFAULT,
  TMP_DIR,
  normalizeCrossPlatform,
  getBaseDataDir,
  getRawDir,
  getCursorDir,
  getTmpDir,
  getTmpRawDir,
  toDuckDBPath,
  isGCSMode,
  validateGCSBucket,
  getGCSBucket,
  logPathConfig,
} from '../path-utils.js';

describe('Path Utilities', () => {
  // Store original env values
  const originalEnv = { ...process.env };
  
  beforeEach(() => {
    // Reset env vars before each test
    delete process.env.DATA_DIR;
    delete process.env.CURSOR_DIR;
    delete process.env.GCS_BUCKET;
    delete process.env.GCS_ENABLED;
  });
  
  afterEach(() => {
    // Restore original env
    process.env = { ...originalEnv };
  });
  
  describe('Platform Detection', () => {
    it('should detect Windows paths correctly', () => {
      expect(isWindowsPath('C:\\ledger_data')).toBe(true);
      expect(isWindowsPath('D:/data/raw')).toBe(true);
      expect(isWindowsPath('E:\\Users\\test')).toBe(true);
      expect(isWindowsPath('/home/user/data')).toBe(false);
      expect(isWindowsPath('./relative/path')).toBe(false);
      expect(isWindowsPath('')).toBe(false);
    });
    
    it('should detect Linux paths correctly', () => {
      expect(isLinuxPath('/home/user/data')).toBe(true);
      expect(isLinuxPath('/tmp/ledger_raw')).toBe(true);
      expect(isLinuxPath('/var/log')).toBe(true);
      expect(isLinuxPath('C:\\Windows')).toBe(false);
      expect(isLinuxPath('./relative')).toBe(false);
      expect(isLinuxPath('')).toBe(false);
      expect(isLinuxPath(null)).toBe(false);
    });
    
    it('should export platform constants', () => {
      expect(typeof isWindows).toBe('boolean');
      expect(WIN_DEFAULT).toBe('C:\\ledger_raw');
      expect(LINUX_DEFAULT).toBe('/home/ben/ledger_data');
      expect(TMP_DIR).toBe('/tmp/ledger_raw');
    });
  });
  
  describe('Cross-Platform Normalization', () => {
    it('should return default when no input provided', () => {
      const result = normalizeCrossPlatform(null);
      // Should return platform-appropriate default
      expect(result).toBe(isWindows ? WIN_DEFAULT : LINUX_DEFAULT);
    });
    
    it('should return empty string default when no input provided', () => {
      const result = normalizeCrossPlatform('');
      expect(result).toBe(isWindows ? WIN_DEFAULT : LINUX_DEFAULT);
    });
    
    it('should use custom defaults when provided', () => {
      const customLinux = '/var/ledger';
      const customWin = 'E:\\ledger';
      
      const result = normalizeCrossPlatform(null, customLinux, customWin);
      expect(result).toBe(isWindows ? customWin : customLinux);
    });
    
    it('should preserve valid paths for current platform', () => {
      if (isWindows) {
        expect(normalizeCrossPlatform('D:\\custom\\path')).toBe('D:\\custom\\path');
      } else {
        expect(normalizeCrossPlatform('/custom/path')).toBe('/custom/path');
      }
    });
    
    it('should handle cross-platform path mismatch', () => {
      // On Linux, Windows paths should fallback to Linux default
      // On Windows, Linux paths should fallback to Windows default
      if (!isWindows) {
        // We're on Linux, test Windows path input
        const spy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const result = normalizeCrossPlatform('C:\\Users\\data');
        expect(result).toBe(LINUX_DEFAULT);
        spy.mockRestore();
      } else {
        // We're on Windows, test Linux path input
        const spy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const result = normalizeCrossPlatform('/home/user/data');
        expect(result).toBe(WIN_DEFAULT);
        spy.mockRestore();
      }
    });
  });
  
  describe('Directory Functions', () => {
    it('should get base data dir with default', () => {
      const result = getBaseDataDir();
      expect(typeof result).toBe('string');
      expect(result.length).toBeGreaterThan(0);
    });
    
    it('should get base data dir with custom env', () => {
      const customPath = isWindows ? 'D:\\custom' : '/custom/data';
      const result = getBaseDataDir(customPath);
      expect(result).toContain('custom');
    });
    
    it('should derive raw directory from base', () => {
      const result = getRawDir();
      expect(result).toContain('raw');
    });
    
    it('should get cursor dir with fallback to DATA_DIR', () => {
      const result = getCursorDir();
      expect(result).toContain('cursors');
    });
    
    it('should use CURSOR_DIR when provided', () => {
      const customCursor = isWindows ? 'D:\\cursors' : '/custom/cursors';
      const result = getCursorDir(customCursor);
      expect(result).toContain('cursors');
    });
    
    it('should get tmp directories', () => {
      expect(getTmpDir()).toBe('/tmp/ledger_raw');
      expect(getTmpRawDir()).toBe('/tmp/ledger_raw/raw');
    });
  });
  
  describe('DuckDB Path Conversion', () => {
    it('should convert Windows backslashes to forward slashes', () => {
      expect(toDuckDBPath('C:\\data\\raw\\file.parquet')).toBe('C:/data/raw/file.parquet');
      expect(toDuckDBPath('D:\\ledger\\migration=0\\updates.parquet')).toBe('D:/ledger/migration=0/updates.parquet');
    });
    
    it('should preserve Linux paths unchanged', () => {
      expect(toDuckDBPath('/home/user/data/file.parquet')).toBe('/home/user/data/file.parquet');
    });
    
    it('should handle mixed separators', () => {
      expect(toDuckDBPath('C:/data\\raw/file.parquet')).toBe('C:/data/raw/file.parquet');
    });
  });
  
  describe('GCS Mode Detection', () => {
    it('should be disabled when GCS_BUCKET is not set', () => {
      delete process.env.GCS_BUCKET;
      expect(isGCSMode()).toBe(false);
    });
    
    it('should be enabled when GCS_BUCKET is set', () => {
      process.env.GCS_BUCKET = 'my-bucket';
      expect(isGCSMode()).toBe(true);
    });
    
    it('should be disabled when GCS_ENABLED is false', () => {
      process.env.GCS_BUCKET = 'my-bucket';
      process.env.GCS_ENABLED = 'false';
      expect(isGCSMode()).toBe(false);
    });
    
    it('should remain enabled when GCS_ENABLED is true', () => {
      process.env.GCS_BUCKET = 'my-bucket';
      process.env.GCS_ENABLED = 'true';
      expect(isGCSMode()).toBe(true);
    });
  });
  
  describe('GCS Bucket Validation', () => {
    it('should throw when required but not set', () => {
      delete process.env.GCS_BUCKET;
      expect(() => validateGCSBucket(true)).toThrow('GCS_BUCKET environment variable is required');
    });
    
    it('should throw when GCS_ENABLED is true but bucket not set', () => {
      delete process.env.GCS_BUCKET;
      process.env.GCS_ENABLED = 'true';
      expect(() => validateGCSBucket(false)).toThrow('GCS_BUCKET environment variable is required');
    });
    
    it('should return null when not required and not set', () => {
      delete process.env.GCS_BUCKET;
      expect(validateGCSBucket(false)).toBeNull();
    });
    
    it('should return bucket name when set', () => {
      process.env.GCS_BUCKET = 'my-bucket';
      expect(validateGCSBucket(false)).toBe('my-bucket');
      expect(validateGCSBucket(true)).toBe('my-bucket');
    });
    
    it('getGCSBucket should return bucket via validateGCSBucket', () => {
      process.env.GCS_BUCKET = 'test-bucket';
      expect(getGCSBucket()).toBe('test-bucket');
    });
  });
  
  describe('Log Path Config', () => {
    it('should log configuration without throwing', () => {
      const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
      
      expect(() => logPathConfig()).not.toThrow();
      expect(() => logPathConfig('test-module')).not.toThrow();
      
      // Should have logged multiple lines
      expect(spy).toHaveBeenCalled();
      spy.mockRestore();
    });
    
    it('should log GCS mode info when bucket is set', () => {
      process.env.GCS_BUCKET = 'test-bucket';
      const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
      
      logPathConfig('gcs-test');
      
      // Verify GCS-related logs were made
      const calls = spy.mock.calls.map(c => c[0]);
      expect(calls.some(c => c.includes('GCS_BUCKET'))).toBe(true);
      
      spy.mockRestore();
    });
  });
});
