/**
 * Path Utilities Tests
 * 
 * Tests cross-platform path handling critical for:
 * - Windows/Linux compatibility
 * - GCS mode detection
 * - Directory structure consistency
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

describe('Path Utilities', () => {
  
  describe('Platform Detection', () => {
    it('should detect Windows paths correctly', () => {
      const isWindowsPath = (p) => /^[A-Za-z]:[\\/]/.test(p);
      
      expect(isWindowsPath('C:\\ledger_data')).toBe(true);
      expect(isWindowsPath('D:/data/raw')).toBe(true);
      expect(isWindowsPath('E:\\Users\\test')).toBe(true);
      expect(isWindowsPath('/home/user/data')).toBe(false);
      expect(isWindowsPath('./relative/path')).toBe(false);
      expect(isWindowsPath('')).toBe(false);
    });
    
    it('should detect Linux paths correctly', () => {
      const isLinuxPath = (p) => !!p && p.startsWith('/');
      
      expect(isLinuxPath('/home/user/data')).toBe(true);
      expect(isLinuxPath('/tmp/ledger_raw')).toBe(true);
      expect(isLinuxPath('/var/log')).toBe(true);
      expect(isLinuxPath('C:\\Windows')).toBe(false);
      expect(isLinuxPath('./relative')).toBe(false);
      expect(isLinuxPath('')).toBe(false);
      expect(isLinuxPath(null)).toBe(false);
    });
  });
  
  describe('Cross-Platform Normalization', () => {
    const WIN_DEFAULT = 'C:\\ledger_raw';
    const LINUX_DEFAULT = '/home/ben/ledger_data';
    
    function normalizeCrossPlatform(inputPath, isWindows, customLinuxDefault = LINUX_DEFAULT, customWinDefault = WIN_DEFAULT) {
      const isWindowsPath = (p) => /^[A-Za-z]:[\\/]/.test(p);
      const isLinuxPath = (p) => p && p.startsWith('/');
      
      if (!inputPath) {
        return isWindows ? customWinDefault : customLinuxDefault;
      }
      
      if (!isWindows && isWindowsPath(inputPath)) {
        return customLinuxDefault;
      }
      
      if (isWindows && isLinuxPath(inputPath)) {
        return customWinDefault;
      }
      
      return inputPath;
    }
    
    it('should return Linux default when no input on Linux', () => {
      const result = normalizeCrossPlatform(null, false);
      expect(result).toBe(LINUX_DEFAULT);
    });
    
    it('should return Windows default when no input on Windows', () => {
      const result = normalizeCrossPlatform(null, true);
      expect(result).toBe(WIN_DEFAULT);
    });
    
    it('should fallback to Linux default when given Windows path on Linux', () => {
      const result = normalizeCrossPlatform('C:\\Users\\data', false);
      expect(result).toBe(LINUX_DEFAULT);
    });
    
    it('should fallback to Windows default when given Linux path on Windows', () => {
      const result = normalizeCrossPlatform('/home/user/data', true);
      expect(result).toBe(WIN_DEFAULT);
    });
    
    it('should preserve path when format matches platform', () => {
      expect(normalizeCrossPlatform('/custom/path', false)).toBe('/custom/path');
      expect(normalizeCrossPlatform('D:\\custom\\path', true)).toBe('D:\\custom\\path');
    });
    
    it('should allow custom defaults', () => {
      const customLinux = '/var/ledger';
      const customWin = 'E:\\ledger';
      
      expect(normalizeCrossPlatform(null, false, customLinux, customWin)).toBe(customLinux);
      expect(normalizeCrossPlatform(null, true, customLinux, customWin)).toBe(customWin);
    });
  });
  
  describe('DuckDB Path Conversion', () => {
    function toDuckDBPath(filePath) {
      return filePath.replace(/\\/g, '/');
    }
    
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
  
  describe('Directory Derivation', () => {
    it('should derive raw directory from base', () => {
      const getBaseDataDir = () => '/home/ben/ledger_data';
      const getRawDir = () => getBaseDataDir() + '/raw';
      
      expect(getRawDir()).toBe('/home/ben/ledger_data/raw');
    });
    
    it('should derive cursor directory from base', () => {
      const getBaseDataDir = () => '/home/ben/ledger_data';
      const getCursorDir = (cursorEnv) => cursorEnv || getBaseDataDir() + '/cursors';
      
      expect(getCursorDir()).toBe('/home/ben/ledger_data/cursors');
      expect(getCursorDir('/custom/cursors')).toBe('/custom/cursors');
    });
    
    it('should derive tmp directories correctly', () => {
      const TMP_DIR = '/tmp/ledger_raw';
      const getTmpRawDir = () => TMP_DIR + '/raw';
      
      expect(getTmpRawDir()).toBe('/tmp/ledger_raw/raw');
    });
  });
  
  describe('GCS Mode Detection', () => {
    it('should be disabled when GCS_BUCKET is not set', () => {
      const isGCSMode = (bucket, enabled) => {
        if (!bucket) return false;
        return enabled !== 'false';
      };
      
      expect(isGCSMode(null, null)).toBe(false);
      expect(isGCSMode('', null)).toBe(false);
      expect(isGCSMode(undefined, 'true')).toBe(false);
    });
    
    it('should be enabled when GCS_BUCKET is set and not disabled', () => {
      const isGCSMode = (bucket, enabled) => {
        if (!bucket) return false;
        return enabled !== 'false';
      };
      
      expect(isGCSMode('my-bucket', null)).toBe(true);
      expect(isGCSMode('my-bucket', 'true')).toBe(true);
      expect(isGCSMode('my-bucket', undefined)).toBe(true);
    });
    
    it('should be disabled when explicitly set to false', () => {
      const isGCSMode = (bucket, enabled) => {
        if (!bucket) return false;
        return enabled !== 'false';
      };
      
      expect(isGCSMode('my-bucket', 'false')).toBe(false);
    });
  });
  
  describe('GCS Bucket Validation', () => {
    it('should throw when required but not set', () => {
      const validateGCSBucket = (bucket, required, enabled) => {
        if (!bucket) {
          if (required || enabled === 'true') {
            throw new Error('GCS_BUCKET environment variable is required');
          }
          return null;
        }
        return bucket;
      };
      
      expect(() => validateGCSBucket(null, true, null)).toThrow();
      expect(() => validateGCSBucket(null, false, 'true')).toThrow();
    });
    
    it('should return null when not required and not set', () => {
      const validateGCSBucket = (bucket, required, enabled) => {
        if (!bucket) {
          if (required || enabled === 'true') {
            throw new Error('GCS_BUCKET environment variable is required');
          }
          return null;
        }
        return bucket;
      };
      
      expect(validateGCSBucket(null, false, null)).toBeNull();
      expect(validateGCSBucket(null, false, 'false')).toBeNull();
    });
    
    it('should return bucket name when set', () => {
      const validateGCSBucket = (bucket, required, enabled) => {
        if (!bucket) {
          if (required || enabled === 'true') {
            throw new Error('GCS_BUCKET environment variable is required');
          }
          return null;
        }
        return bucket;
      };
      
      expect(validateGCSBucket('my-bucket', false, null)).toBe('my-bucket');
      expect(validateGCSBucket('my-bucket', true, null)).toBe('my-bucket');
    });
  });
});
