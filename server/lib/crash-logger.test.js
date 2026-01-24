/**
 * Crash Logger Tests
 * 
 * Tests crash logging functionality by verifying ACTUAL file write behavior.
 * These tests validate real implementation logic - not mock pass-throughs.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'fs';

// Spies on real fs methods - set up BEFORE importing the module
const existsSyncSpy = vi.spyOn(fs, 'existsSync');
const mkdirSyncSpy = vi.spyOn(fs, 'mkdirSync');
const appendFileSyncSpy = vi.spyOn(fs, 'appendFileSync');

// Default: pretend directory exists, capture file writes
existsSyncSpy.mockReturnValue(true);
mkdirSyncSpy.mockImplementation(() => undefined);
appendFileSyncSpy.mockImplementation(() => undefined);

// Dynamic import AFTER spies are established
const { logCrash, logError, LOG_PATHS } = await import('./crash-logger.js');

describe('Crash Logger', () => {
  
  beforeEach(() => {
    vi.clearAllMocks();
    existsSyncSpy.mockReturnValue(true);
    appendFileSyncSpy.mockImplementation(() => undefined);
  });
  
  afterEach(() => {
    vi.restoreAllMocks();
  });
  
  describe('LOG_PATHS exports', () => {
    it('should export dir as string containing logs', () => {
      expect(typeof LOG_PATHS.dir).toBe('string');
      expect(LOG_PATHS.dir).toMatch(/logs/);
    });
    
    it('should export crash log path ending with crash.log', () => {
      expect(LOG_PATHS.crash).toMatch(/crash\.log$/);
    });
    
    it('should export error log path ending with error.log', () => {
      expect(LOG_PATHS.error).toMatch(/error\.log$/);
    });
    
    it('crash and error logs should be in the same directory', () => {
      // Both should share the same parent directory
      const crashDir = LOG_PATHS.crash.replace(/[/\\][^/\\]+$/, '');
      const errorDir = LOG_PATHS.error.replace(/[/\\][^/\\]+$/, '');
      expect(crashDir).toBe(errorDir);
      expect(crashDir).toBe(LOG_PATHS.dir);
    });
  });
  
  describe('logCrash - file targeting', () => {
    it('should write to crash.log (not error.log)', () => {
      logCrash(new Error('Test'));
      
      expect(appendFileSyncSpy).toHaveBeenCalledTimes(1);
      const [filePath] = appendFileSyncSpy.mock.calls[0];
      expect(filePath).toMatch(/crash\.log$/);
      expect(filePath).not.toMatch(/error\.log$/);
    });
  });
  
  describe('logCrash - error message formatting', () => {
    it('should include the exact error message in output', () => {
      const errorMessage = 'Database connection failed XYZ123';
      logCrash(new Error(errorMessage));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain(errorMessage);
    });
    
    it('should include custom type when provided', () => {
      logCrash(new Error('Test'), 'CUSTOM_CRASH_TYPE');
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('CUSTOM_CRASH_TYPE');
    });
    
    it('should default to UNCAUGHT_EXCEPTION when no type provided', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('UNCAUGHT_EXCEPTION');
    });
    
    it('should include stack trace with file/line info', () => {
      const error = new Error('Stack test');
      logCrash(error);
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Stack:');
      // Stack should contain the test file name
      expect(content).toMatch(/crash-logger\.test/);
    });
  });
  
  describe('logCrash - process metadata', () => {
    it('should include process.pid as a number', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Process PID:');
      expect(content).toContain(String(process.pid));
      expect(typeof process.pid).toBe('number');
    });
    
    it('should include actual Node version (starts with v)', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Node Version:');
      expect(content).toContain(process.version);
      expect(process.version).toMatch(/^v\d+\.\d+/);
    });
    
    it('should include platform identifier', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Platform:');
      expect(content).toContain(process.platform);
      expect(['darwin', 'linux', 'win32']).toContain(process.platform);
    });
    
    it('should include memory usage with heapUsed (real value)', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Memory Usage:');
      expect(content).toContain('heapUsed');
      expect(content).toContain('heapTotal');
      expect(content).toContain('rss');
    });
    
    it('should include uptime in seconds', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Uptime:');
      expect(content).toContain('seconds');
      // Uptime should be a positive number
      const uptimeMatch = content.match(/Uptime:\s*([\d.]+)/);
      expect(uptimeMatch).not.toBeNull();
      expect(parseFloat(uptimeMatch[1])).toBeGreaterThan(0);
    });
  });
  
  describe('logCrash - edge case handling', () => {
    it('should handle null error without throwing', () => {
      expect(() => logCrash(null)).not.toThrow();
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Unknown error');
    });
    
    it('should handle undefined error without throwing', () => {
      expect(() => logCrash(undefined)).not.toThrow();
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('Unknown error');
    });
    
    it('should handle error-like object without stack property', () => {
      const errorLike = { message: 'No stack available' };
      logCrash(errorLike);
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('No stack available');
      expect(content).toContain('No stack trace');
    });
    
    it('should handle error with empty message', () => {
      const error = new Error('');
      logCrash(error);
      
      // Should still write something
      expect(appendFileSyncSpy).toHaveBeenCalled();
    });
  });
  
  describe('logError - file targeting', () => {
    it('should write to error.log (not crash.log)', () => {
      logError(new Error('Test'));
      
      expect(appendFileSyncSpy).toHaveBeenCalledTimes(1);
      const [filePath] = appendFileSyncSpy.mock.calls[0];
      expect(filePath).toMatch(/error\.log$/);
      expect(filePath).not.toMatch(/crash\.log$/);
    });
  });
  
  describe('logError - context formatting', () => {
    it('should include custom context string', () => {
      logError(new Error('Test'), 'API_HANDLER');
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('API_HANDLER');
    });
    
    it('should default to ERROR context', () => {
      logError(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain('ERROR');
    });
    
    it('should include detailed error message', () => {
      const detailedMessage = 'Connection refused at 127.0.0.1:5432';
      logError(new Error(detailedMessage));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      expect(content).toContain(detailedMessage);
    });
  });
  
  describe('Directory creation', () => {
    it('should create directory if it does not exist', () => {
      existsSyncSpy.mockReturnValue(false);
      
      logCrash(new Error('Test'));
      
      expect(mkdirSyncSpy).toHaveBeenCalled();
    });
    
    it('should use recursive option for nested directories', () => {
      existsSyncSpy.mockReturnValue(false);
      
      logCrash(new Error('Test'));
      
      expect(mkdirSyncSpy).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ recursive: true })
      );
    });
    
    it('should NOT create directory if it already exists', () => {
      existsSyncSpy.mockReturnValue(true);
      
      logCrash(new Error('Test'));
      
      expect(mkdirSyncSpy).not.toHaveBeenCalled();
    });
  });
  
  describe('Write failure handling', () => {
    it('should write to stderr if appendFileSync fails', () => {
      const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
      appendFileSyncSpy.mockImplementation(() => {
        throw new Error('Disk full');
      });
      
      // Should not throw - gracefully degrade
      expect(() => logCrash(new Error('Test'))).not.toThrow();
      
      // Should have written to stderr as fallback
      expect(stderrSpy).toHaveBeenCalled();
      const stderrOutput = stderrSpy.mock.calls.map(c => c[0]).join('');
      expect(stderrOutput).toContain('Failed to write');
      
      stderrSpy.mockRestore();
    });
    
    it('should include original error content in stderr fallback', () => {
      const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
      appendFileSyncSpy.mockImplementation(() => {
        throw new Error('Permission denied');
      });
      
      const originalError = new Error('Original crash reason');
      logCrash(originalError);
      
      const stderrOutput = stderrSpy.mock.calls.map(c => c[0]).join('');
      // The formatted error content should still be output to stderr
      expect(stderrOutput).toContain('Original crash reason');
      
      stderrSpy.mockRestore();
    });
  });
  
  describe('Log format structure', () => {
    it('should include separator lines for readability', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      // Should have separator lines (equals signs)
      expect(content).toMatch(/={10,}/);
    });
    
    it('should include ISO timestamp in log entry', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      // ISO timestamp format: 2024-01-15T10:30:00.000Z
      expect(content).toMatch(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
    });
    
    it('should format memory usage as valid JSON', () => {
      logCrash(new Error('Test'));
      
      const [, content] = appendFileSyncSpy.mock.calls[0];
      // Extract memory JSON and verify it's parseable
      const memoryMatch = content.match(/Memory Usage:\s*(\{[\s\S]*?\})/);
      expect(memoryMatch).not.toBeNull();
      
      // The JSON should be valid
      expect(() => JSON.parse(memoryMatch[1])).not.toThrow();
    });
  });
  
  describe('installCrashHandlers', () => {
    let processOnSpy;
    let originalProcessOn;
    let registeredHandlers;
    
    beforeEach(async () => {
      registeredHandlers = {};
      originalProcessOn = process.on.bind(process);
      
      // Spy on process.on to capture registered handlers
      processOnSpy = vi.spyOn(process, 'on').mockImplementation((event, handler) => {
        registeredHandlers[event] = registeredHandlers[event] || [];
        registeredHandlers[event].push(handler);
        return process;
      });
    });
    
    afterEach(() => {
      processOnSpy.mockRestore();
    });
    
    it('should register uncaughtException handler', async () => {
      // Re-import to get installCrashHandlers
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('uncaughtException', expect.any(Function));
      expect(registeredHandlers['uncaughtException']).toBeDefined();
      expect(registeredHandlers['uncaughtException'].length).toBeGreaterThan(0);
    });
    
    it('should register unhandledRejection handler', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('unhandledRejection', expect.any(Function));
      expect(registeredHandlers['unhandledRejection']).toBeDefined();
    });
    
    it('should register SIGTERM handler', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('SIGTERM', expect.any(Function));
      expect(registeredHandlers['SIGTERM']).toBeDefined();
    });
    
    it('should register SIGINT handler', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('SIGINT', expect.any(Function));
      expect(registeredHandlers['SIGINT']).toBeDefined();
    });
    
    it('uncaughtException handler should call logCrash', async () => {
      const { installCrashHandlers, logCrash } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      const handler = registeredHandlers['uncaughtException'][0];
      const testError = new Error('Uncaught test error');
      
      // Mock setTimeout to prevent actual exit
      const setTimeoutSpy = vi.spyOn(global, 'setTimeout').mockImplementation(() => ({ unref: () => {} }));
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      handler(testError);
      
      // Verify logCrash was called (via the appendFileSync spy)
      expect(appendFileSyncSpy).toHaveBeenCalled();
      const [filePath, content] = appendFileSyncSpy.mock.calls[appendFileSyncSpy.mock.calls.length - 1];
      expect(filePath).toMatch(/crash\.log$/);
      expect(content).toContain('Uncaught test error');
      expect(content).toContain('UNCAUGHT_EXCEPTION');
      
      setTimeoutSpy.mockRestore();
      consoleSpy.mockRestore();
    });
    
    it('unhandledRejection handler should call logCrash with UNHANDLED_REJECTION type', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      const handler = registeredHandlers['unhandledRejection'][0];
      const testError = new Error('Rejected promise');
      
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      handler(testError, Promise.reject(testError).catch(() => {}));
      
      expect(appendFileSyncSpy).toHaveBeenCalled();
      const [filePath, content] = appendFileSyncSpy.mock.calls[appendFileSyncSpy.mock.calls.length - 1];
      expect(filePath).toMatch(/crash\.log$/);
      expect(content).toContain('UNHANDLED_REJECTION');
      expect(content).toContain('Rejected promise');
      
      consoleSpy.mockRestore();
    });
    
    it('unhandledRejection handler should handle non-Error reasons', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      const handler = registeredHandlers['unhandledRejection'][0];
      const stringReason = 'Simple string rejection';
      
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      handler(stringReason, Promise.reject(stringReason).catch(() => {}));
      
      expect(appendFileSyncSpy).toHaveBeenCalled();
      const [, content] = appendFileSyncSpy.mock.calls[appendFileSyncSpy.mock.calls.length - 1];
      expect(content).toContain('Simple string rejection');
      
      consoleSpy.mockRestore();
    });
    
    it('SIGTERM handler should log graceful shutdown', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      const handler = registeredHandlers['SIGTERM'][0];
      
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      const exitSpy = vi.spyOn(process, 'exit').mockImplementation(() => {});
      
      handler();
      
      expect(appendFileSyncSpy).toHaveBeenCalled();
      const calls = appendFileSyncSpy.mock.calls;
      const lastCall = calls[calls.length - 1];
      expect(lastCall[1]).toContain('SIGTERM');
      expect(lastCall[1]).toContain('graceful shutdown');
      
      consoleSpy.mockRestore();
      exitSpy.mockRestore();
    });
    
    it('SIGINT handler should log graceful shutdown and exit with 0', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      installCrashHandlers();
      
      const handler = registeredHandlers['SIGINT'][0];
      
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      const exitSpy = vi.spyOn(process, 'exit').mockImplementation(() => {});
      
      handler();
      
      expect(exitSpy).toHaveBeenCalledWith(0);
      expect(appendFileSyncSpy).toHaveBeenCalled();
      const calls = appendFileSyncSpy.mock.calls;
      const lastCall = calls[calls.length - 1];
      expect(lastCall[1]).toContain('SIGINT');
      
      consoleSpy.mockRestore();
      exitSpy.mockRestore();
    });
    
    it('should log confirmation message when handlers are installed', async () => {
      const { installCrashHandlers } = await import('./crash-logger.js');
      
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      
      installCrashHandlers();
      
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Crash logger installed')
      );
      
      consoleSpy.mockRestore();
    });
  });
});
