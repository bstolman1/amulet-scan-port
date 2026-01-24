/**
 * Crash Logger Tests
 * 
 * Tests crash logging functionality including file writing and error formatting.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import fs from 'fs';

// Create spies on fs methods
const existsSyncSpy = vi.spyOn(fs, 'existsSync');
const mkdirSyncSpy = vi.spyOn(fs, 'mkdirSync');
const appendFileSyncSpy = vi.spyOn(fs, 'appendFileSync');

// Set default behavior
existsSyncSpy.mockReturnValue(true);
mkdirSyncSpy.mockImplementation(() => undefined);
appendFileSyncSpy.mockImplementation(() => undefined);

// Dynamic import after spies are set up
const { logCrash, logError, LOG_PATHS } = await import('./crash-logger.js');

describe('Crash Logger', () => {
  
  beforeEach(() => {
    vi.clearAllMocks();
    existsSyncSpy.mockReturnValue(true);
    appendFileSyncSpy.mockImplementation(() => undefined);
  });
  
  describe('LOG_PATHS', () => {
    it('should export log paths object', () => {
      expect(LOG_PATHS).toBeDefined();
      expect(LOG_PATHS).toHaveProperty('dir');
      expect(LOG_PATHS).toHaveProperty('crash');
      expect(LOG_PATHS).toHaveProperty('error');
    });
    
    it('dir should be a string path', () => {
      expect(typeof LOG_PATHS.dir).toBe('string');
      expect(LOG_PATHS.dir.length).toBeGreaterThan(0);
    });
    
    it('crash log should end with crash.log', () => {
      expect(LOG_PATHS.crash).toMatch(/crash\.log$/);
    });
    
    it('error log should end with error.log', () => {
      expect(LOG_PATHS.error).toMatch(/error\.log$/);
    });
  });
  
  describe('logCrash', () => {
    it('should write to crash.log file', () => {
      logCrash(new Error('Test'));
      
      expect(appendFileSyncSpy).toHaveBeenCalled();
      const [filePath] = appendFileSyncSpy.mock.calls[0];
      expect(filePath).toMatch(/crash\.log$/);
    });
    
    it('should include error message', () => {
      logCrash(new Error('Specific crash message'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Specific crash message');
    });
    
    it('should include custom type parameter', () => {
      logCrash(new Error('Test'), 'CUSTOM_CRASH_TYPE');
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('CUSTOM_CRASH_TYPE');
    });
    
    it('should default to UNCAUGHT_EXCEPTION type', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('UNCAUGHT_EXCEPTION');
    });
    
    it('should include stack trace', () => {
      const error = new Error('Stack test');
      logCrash(error);
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Stack:');
      expect(content).toContain('Error: Stack test');
    });
    
    it('should include process.pid', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Process PID:');
      expect(content).toContain(String(process.pid));
    });
    
    it('should include Node version', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Node Version:');
      expect(content).toContain(process.version);
    });
    
    it('should include platform', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Platform:');
      expect(content).toContain(process.platform);
    });
    
    it('should include memory usage JSON', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Memory Usage:');
      expect(content).toContain('heapUsed');
    });
    
    it('should include uptime', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Uptime:');
      expect(content).toContain('seconds');
    });
    
    it('should handle null error gracefully', () => {
      expect(() => logCrash(null)).not.toThrow();
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Unknown error');
    });
    
    it('should handle undefined error gracefully', () => {
      expect(() => logCrash(undefined)).not.toThrow();
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Unknown error');
    });
    
    it('should handle error without stack', () => {
      const errorLike = { message: 'No stack error' };
      logCrash(errorLike);
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('No stack error');
      expect(content).toContain('No stack trace');
    });
  });
  
  describe('logError', () => {
    it('should write to error.log file', () => {
      logError(new Error('Test'));
      
      expect(fs.appendFileSync).toHaveBeenCalled();
      const [filePath] = fs.appendFileSync.mock.calls[0];
      expect(filePath).toMatch(/error\.log$/);
    });
    
    it('should include custom context', () => {
      logError(new Error('Test'), 'API_HANDLER');
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('API_HANDLER');
    });
    
    it('should default to ERROR context', () => {
      logError(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('ERROR');
    });
    
    it('should include error message', () => {
      logError(new Error('Specific error details'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Specific error details');
    });
  });
  
  describe('Log Directory Creation', () => {
    it('should create directory if it does not exist', () => {
      fs.existsSync.mockReturnValue(false);
      
      logCrash(new Error('Test'));
      
      expect(fs.mkdirSync).toHaveBeenCalled();
    });
    
    it('should use recursive option', () => {
      fs.existsSync.mockReturnValue(false);
      
      logCrash(new Error('Test'));
      
      expect(fs.mkdirSync).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ recursive: true })
      );
    });
    
    it('should not create directory if it exists', () => {
      fs.existsSync.mockReturnValue(true);
      
      logCrash(new Error('Test'));
      
      expect(fs.mkdirSync).not.toHaveBeenCalled();
    });
  });
  
  describe('Write Failure Handling', () => {
    it('should write to stderr if appendFileSync fails', () => {
      const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
      fs.appendFileSync.mockImplementation(() => {
        throw new Error('Disk full');
      });
      
      expect(() => logCrash(new Error('Test'))).not.toThrow();
      
      expect(stderrSpy).toHaveBeenCalled();
      const stderrOutput = stderrSpy.mock.calls.map(c => c[0]).join('');
      expect(stderrOutput).toContain('Failed to write');
      
      stderrSpy.mockRestore();
    });
  });
});
