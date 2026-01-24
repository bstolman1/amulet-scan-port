/**
 * Crash Logger Tests
 * 
 * Tests crash logging functionality by verifying ACTUAL file write behavior.
 * These tests validate real implementation logic - not mock pass-throughs.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// IMPORTANT: Mock fs *before* importing crash-logger.js.
// Using vi.mock here is more reliable than vi.spyOn(fs, ...) because it guarantees
// crash-logger.js imports the mocked module instance.
vi.mock('fs', async () => {
  const actual = await vi.importActual<typeof import('fs')>('fs');
  return {
    default: {
      ...actual,
      existsSync: vi.fn(),
      mkdirSync: vi.fn(),
      appendFileSync: vi.fn(),
    },
  };
});

/** @type {any} */
const fs = (await import('fs')).default;

const { logCrash, logError, installCrashHandlers, LOG_PATHS } = await import('./crash-logger.js');

describe('Crash Logger', () => {
  
  beforeEach(() => {
    // Clear call history but keep the spy implementations intact
    vi.clearAllMocks();
    // Reset default mock behavior
    fs.existsSync.mockReturnValue(true);
    fs.mkdirSync.mockImplementation(() => undefined);
    fs.appendFileSync.mockImplementation(() => undefined);
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
      const crashDir = LOG_PATHS.crash.replace(/[/\\][^/\\]+$/, '');
      const errorDir = LOG_PATHS.error.replace(/[/\\][^/\\]+$/, '');
      expect(crashDir).toBe(errorDir);
      expect(crashDir).toBe(LOG_PATHS.dir);
    });
  });
  
  describe('logCrash - file targeting', () => {
    it('should write to crash.log (not error.log)', () => {
      logCrash(new Error('Test'));
      
      expect(fs.appendFileSync).toHaveBeenCalledTimes(1);
      const [filePath] = fs.appendFileSync.mock.calls[0];
      expect(filePath).toMatch(/crash\.log$/);
      expect(filePath).not.toMatch(/error\.log$/);
    });
  });
  
  describe('logCrash - error message formatting', () => {
    it('should include the exact error message in output', () => {
      const errorMessage = 'Database connection failed XYZ123';
      logCrash(new Error(errorMessage));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain(errorMessage);
    });
    
    it('should include custom type when provided', () => {
      logCrash(new Error('Test'), 'CUSTOM_CRASH_TYPE');
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('CUSTOM_CRASH_TYPE');
    });
    
    it('should default to UNCAUGHT_EXCEPTION when no type provided', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('UNCAUGHT_EXCEPTION');
    });
    
    it('should include stack trace with file/line info', () => {
      const error = new Error('Stack test');
      logCrash(error);
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Stack:');
      expect(content).toMatch(/crash-logger\.test/);
    });
  });
  
  describe('logCrash - process metadata', () => {
    it('should include process.pid as a number', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Process PID:');
      expect(content).toContain(String(process.pid));
    });
    
    it('should include actual Node version (starts with v)', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Node Version:');
      expect(content).toContain(process.version);
    });
    
    it('should include platform identifier', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Platform:');
      expect(content).toContain(process.platform);
    });
    
    it('should include memory usage with heapUsed', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Memory Usage:');
      expect(content).toContain('heapUsed');
    });
    
    it('should include uptime in seconds', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Uptime:');
      expect(content).toContain('seconds');
    });
  });
  
  describe('logCrash - edge case handling', () => {
    it('should handle null error without throwing', () => {
      expect(() => logCrash(null)).not.toThrow();
      expect(fs.appendFileSync).toHaveBeenCalled();
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Unknown error');
    });
    
    it('should handle undefined error without throwing', () => {
      expect(() => logCrash(undefined)).not.toThrow();
      expect(fs.appendFileSync).toHaveBeenCalled();
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('Unknown error');
    });
    
    it('should handle error-like object without stack property', () => {
      const errorLike = { message: 'No stack available' };
      logCrash(errorLike);
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('No stack available');
      expect(content).toContain('No stack trace');
    });
    
    it('should handle error with empty message', () => {
      const error = new Error('');
      logCrash(error);
      
      expect(fs.appendFileSync).toHaveBeenCalled();
    });
  });
  
  describe('logError - file targeting', () => {
    it('should write to error.log (not crash.log)', () => {
      logError(new Error('Test'));
      
      expect(fs.appendFileSync).toHaveBeenCalledTimes(1);
      const [filePath] = fs.appendFileSync.mock.calls[0];
      expect(filePath).toMatch(/error\.log$/);
      expect(filePath).not.toMatch(/crash\.log$/);
    });
  });
  
  describe('logError - context formatting', () => {
    it('should include custom context string', () => {
      logError(new Error('Test'), 'API_HANDLER');
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('API_HANDLER');
    });
    
    it('should default to ERROR context', () => {
      logError(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain('ERROR');
    });
    
    it('should include detailed error message', () => {
      const detailedMessage = 'Connection refused at 127.0.0.1:5432';
      logError(new Error(detailedMessage));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toContain(detailedMessage);
    });
  });
  
  describe('Directory creation', () => {
    it('should create directory if it does not exist', () => {
      fs.existsSync.mockReturnValue(false);
      
      logCrash(new Error('Test'));
      
      expect(fs.mkdirSync).toHaveBeenCalled();
    });
    
    it('should use recursive option for nested directories', () => {
      fs.existsSync.mockReturnValue(false);
      
      logCrash(new Error('Test'));
      
      expect(fs.mkdirSync).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ recursive: true })
      );
    });
    
    it('should NOT create directory if it already exists', () => {
      fs.existsSync.mockReturnValue(true);
      
      logCrash(new Error('Test'));
      
      expect(fs.mkdirSync).not.toHaveBeenCalled();
    });
  });
  
  describe('Write failure handling', () => {
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
    
    it('should include original error content in stderr fallback', () => {
      const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
      fs.appendFileSync.mockImplementation(() => {
        throw new Error('Permission denied');
      });
      
      const originalError = new Error('Original crash reason');
      logCrash(originalError);
      
      const stderrOutput = stderrSpy.mock.calls.map(c => c[0]).join('');
      expect(stderrOutput).toContain('Original crash reason');
      
      stderrSpy.mockRestore();
    });
  });
  
  describe('Log format structure', () => {
    it('should include separator lines for readability', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toMatch(/={10,}/);
    });
    
    it('should include ISO timestamp in log entry', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      expect(content).toMatch(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
    });
    
    it('should format memory usage as valid JSON', () => {
      logCrash(new Error('Test'));
      
      const [, content] = fs.appendFileSync.mock.calls[0];
      const memoryMatch = content.match(/Memory Usage:\s*(\{[\s\S]*?\})/);
      expect(memoryMatch).not.toBeNull();
      expect(() => JSON.parse(memoryMatch[1])).not.toThrow();
    });
  });
  
  describe('installCrashHandlers', () => {
    let processOnSpy;
    let registeredHandlers;
    
    beforeEach(() => {
      registeredHandlers = {};
      processOnSpy = vi.spyOn(process, 'on').mockImplementation((event, handler) => {
        registeredHandlers[event] = registeredHandlers[event] || [];
        registeredHandlers[event].push(handler);
        return process;
      });
    });
    
    it('should register uncaughtException handler', () => {
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('uncaughtException', expect.any(Function));
      expect(registeredHandlers['uncaughtException']).toBeDefined();
    });
    
    it('should register unhandledRejection handler', () => {
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('unhandledRejection', expect.any(Function));
      expect(registeredHandlers['unhandledRejection']).toBeDefined();
    });
    
    it('should register SIGTERM handler', () => {
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('SIGTERM', expect.any(Function));
      expect(registeredHandlers['SIGTERM']).toBeDefined();
    });
    
    it('should register SIGINT handler', () => {
      installCrashHandlers();
      
      expect(processOnSpy).toHaveBeenCalledWith('SIGINT', expect.any(Function));
      expect(registeredHandlers['SIGINT']).toBeDefined();
    });
    
    it('uncaughtException handler should call logCrash', () => {
      installCrashHandlers();
      
      const handler = registeredHandlers['uncaughtException'][0];
      const testError = new Error('Uncaught test error');
      
      const setTimeoutSpy = vi.spyOn(global, 'setTimeout').mockImplementation(() => ({ unref: () => {} }));
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      handler(testError);
      
      expect(fs.appendFileSync).toHaveBeenCalled();
      const lastCall = fs.appendFileSync.mock.calls[fs.appendFileSync.mock.calls.length - 1];
      expect(lastCall[0]).toMatch(/crash\.log$/);
      expect(lastCall[1]).toContain('Uncaught test error');
      expect(lastCall[1]).toContain('UNCAUGHT_EXCEPTION');
      
      setTimeoutSpy.mockRestore();
      consoleSpy.mockRestore();
    });
    
    it('unhandledRejection handler should call logCrash with UNHANDLED_REJECTION', () => {
      installCrashHandlers();
      
      const handler = registeredHandlers['unhandledRejection'][0];
      const testError = new Error('Rejected promise');
      
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      handler(testError, Promise.reject(testError).catch(() => {}));
      
      expect(fs.appendFileSync).toHaveBeenCalled();
      const lastCall = fs.appendFileSync.mock.calls[fs.appendFileSync.mock.calls.length - 1];
      expect(lastCall[0]).toMatch(/crash\.log$/);
      expect(lastCall[1]).toContain('UNHANDLED_REJECTION');
      expect(lastCall[1]).toContain('Rejected promise');
      
      consoleSpy.mockRestore();
    });
    
    it('unhandledRejection handler should handle non-Error reasons', () => {
      installCrashHandlers();
      
      const handler = registeredHandlers['unhandledRejection'][0];
      const stringReason = 'Simple string rejection';
      
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      
      handler(stringReason, Promise.reject(stringReason).catch(() => {}));
      
      expect(fs.appendFileSync).toHaveBeenCalled();
      const lastCall = fs.appendFileSync.mock.calls[fs.appendFileSync.mock.calls.length - 1];
      expect(lastCall[1]).toContain('Simple string rejection');
      
      consoleSpy.mockRestore();
    });
    
    it('SIGTERM handler should log graceful shutdown', () => {
      installCrashHandlers();
      
      const handler = registeredHandlers['SIGTERM'][0];
      
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      const exitSpy = vi.spyOn(process, 'exit').mockImplementation(() => {});
      
      handler();
      
      expect(fs.appendFileSync).toHaveBeenCalled();
      const lastCall = fs.appendFileSync.mock.calls[fs.appendFileSync.mock.calls.length - 1];
      expect(lastCall[1]).toContain('SIGTERM');
      expect(lastCall[1]).toContain('graceful shutdown');
      
      consoleSpy.mockRestore();
      exitSpy.mockRestore();
    });
    
    it('SIGINT handler should log graceful shutdown and exit with 0', () => {
      installCrashHandlers();
      
      const handler = registeredHandlers['SIGINT'][0];
      
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      const exitSpy = vi.spyOn(process, 'exit').mockImplementation(() => {});
      
      handler();
      
      expect(exitSpy).toHaveBeenCalledWith(0);
      expect(fs.appendFileSync).toHaveBeenCalled();
      const lastCall = fs.appendFileSync.mock.calls[fs.appendFileSync.mock.calls.length - 1];
      expect(lastCall[1]).toContain('SIGINT');
      
      consoleSpy.mockRestore();
      exitSpy.mockRestore();
    });
    
    it('should log confirmation message when handlers are installed', () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      
      installCrashHandlers();
      
      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('Crash logger installed')
      );
      
      consoleSpy.mockRestore();
    });
  });
});
