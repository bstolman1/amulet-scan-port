/**
 * Crash Logger Tests
 * 
 * Tests crash logging functionality including file writing and error formatting.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import path from 'path';
import fs from 'fs';

// Spy on fs methods instead of mocking the module
vi.spyOn(fs, 'existsSync').mockReturnValue(true);
vi.spyOn(fs, 'mkdirSync').mockImplementation(() => undefined);
vi.spyOn(fs, 'appendFileSync').mockImplementation(() => undefined);

import { logCrash, logError, LOG_PATHS } from './crash-logger.js';

describe('Crash Logger', () => {
  
  beforeEach(() => {
    vi.clearAllMocks();
  });
  
  describe('LOG_PATHS', () => {
    it('should export log paths', () => {
      expect(LOG_PATHS).toBeDefined();
      expect(LOG_PATHS).toHaveProperty('dir');
      expect(LOG_PATHS).toHaveProperty('crash');
      expect(LOG_PATHS).toHaveProperty('error');
    });
    
    it('should have valid path strings', () => {
      expect(typeof LOG_PATHS.dir).toBe('string');
      expect(typeof LOG_PATHS.crash).toBe('string');
      expect(typeof LOG_PATHS.error).toBe('string');
    });
    
    it('crash log should be in log directory', () => {
      expect(LOG_PATHS.crash).toContain('crash.log');
    });
    
    it('error log should be in log directory', () => {
      expect(LOG_PATHS.error).toContain('error.log');
    });
  });
  
  describe('logCrash', () => {
    it('should write crash to file', () => {
      const error = new Error('Test crash');
      
      logCrash(error);
      
      expect(fs.appendFileSync).toHaveBeenCalled();
    });
    
    it('should include error message in log', () => {
      const error = new Error('Specific error message');
      
      logCrash(error);
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('Specific error message');
    });
    
    it('should include error type in log', () => {
      const error = new Error('Test');
      
      logCrash(error, 'CUSTOM_TYPE');
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('CUSTOM_TYPE');
    });
    
    it('should default to UNCAUGHT_EXCEPTION type', () => {
      const error = new Error('Test');
      
      logCrash(error);
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('UNCAUGHT_EXCEPTION');
    });
    
    it('should handle null error gracefully', () => {
      expect(() => logCrash(null)).not.toThrow();
    });
    
    it('should handle undefined error gracefully', () => {
      expect(() => logCrash(undefined)).not.toThrow();
    });
    
    it('should include stack trace if available', () => {
      const error = new Error('Test error');
      
      logCrash(error);
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('Stack:');
    });
    
    it('should include process metadata', () => {
      const error = new Error('Test');
      
      logCrash(error);
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('Process PID:');
      expect(callArgs[1]).toContain('Node Version:');
      expect(callArgs[1]).toContain('Platform:');
      expect(callArgs[1]).toContain('Memory Usage:');
      expect(callArgs[1]).toContain('Uptime:');
    });
  });
  
  describe('logError', () => {
    it('should write error to file', () => {
      const error = new Error('Test error');
      
      logError(error);
      
      expect(fs.appendFileSync).toHaveBeenCalled();
    });
    
    it('should write to error log file', () => {
      const error = new Error('Test error');
      
      logError(error);
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[0]).toContain('error.log');
    });
    
    it('should include context in log', () => {
      const error = new Error('Test');
      
      logError(error, 'API_ERROR');
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('API_ERROR');
    });
    
    it('should default to ERROR context', () => {
      const error = new Error('Test');
      
      logError(error);
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('ERROR');
    });
    
    it('should handle error without stack', () => {
      const error = { message: 'No stack error' };
      
      expect(() => logError(error)).not.toThrow();
      
      const callArgs = fs.appendFileSync.mock.calls[0];
      expect(callArgs[1]).toContain('No stack trace');
    });
  });
  
  describe('Log Directory Creation', () => {
    it('should create log directory if missing', () => {
      fs.existsSync.mockReturnValue(false);
      
      const error = new Error('Test');
      logCrash(error);
      
      expect(fs.mkdirSync).toHaveBeenCalled();
    });
    
    it('should use recursive mkdir', () => {
      fs.existsSync.mockReturnValue(false);
      
      const error = new Error('Test');
      logCrash(error);
      
      expect(fs.mkdirSync).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({ recursive: true })
      );
    });
    
    it('should not create directory if exists', () => {
      fs.existsSync.mockReturnValue(true);
      fs.mkdirSync.mockClear();
      
      const error = new Error('Test');
      logCrash(error);
      
      expect(fs.mkdirSync).not.toHaveBeenCalled();
    });
  });
  
  describe('Error Handling', () => {
    it('should handle appendFileSync failure', () => {
      const stderrSpy = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
      fs.appendFileSync.mockImplementation(() => {
        throw new Error('Disk full');
      });
      
      const error = new Error('Test');
      
      // Should not throw
      expect(() => logCrash(error)).not.toThrow();
      
      // Should write to stderr
      expect(stderrSpy).toHaveBeenCalled();
      
      stderrSpy.mockRestore();
      fs.appendFileSync.mockImplementation(() => {});
    });
  });
});
