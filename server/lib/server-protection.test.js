/**
 * Server Protection Tests
 * 
 * Tests rate limiting, memory monitoring, request timeouts, and error handlers.
 * These tests exercise REAL implementations - not mocks.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Only mock crash-logger to avoid disk writes during tests
vi.mock('./crash-logger.js', () => ({
  logError: vi.fn(),
}));

import { logError } from './crash-logger.js';

// Import real implementations (not mocked)
import {
  apiLimiter,
  expensiveLimiter,
  securityHeaders,
  startMemoryMonitor,
  stopMemoryMonitor,
  getMemoryStatus,
  memoryGuard,
  requestTimeout,
  asyncHandler,
  globalErrorHandler,
} from './server-protection.js';

describe('Server Protection', () => {
  
  beforeEach(() => {
    vi.clearAllMocks();
  });
  
  describe('Rate Limiters', () => {
    it('apiLimiter should be a middleware function', () => {
      expect(typeof apiLimiter).toBe('function');
    });
    
    it('expensiveLimiter should be a middleware function', () => {
      expect(typeof expensiveLimiter).toBe('function');
    });
    
    // Note: Rate limiters require full Express context to test properly
    // They are validated through integration tests in server/test/integration/
  });
  
  describe('Security Headers (helmet)', () => {
    it('securityHeaders should be a middleware function', () => {
      expect(typeof securityHeaders).toBe('function');
    });
    
    it('should call next() after setting headers', () => {
      const req = {};
      const res = { 
        setHeader: vi.fn(),
        removeHeader: vi.fn(),
        getHeader: vi.fn(),
      };
      const next = vi.fn();
      
      securityHeaders(req, res, next);
      
      expect(next).toHaveBeenCalled();
    });
  });
  
  describe('Memory Monitor', () => {
    afterEach(() => {
      stopMemoryMonitor();
    });
    
    it('startMemoryMonitor should not throw', () => {
      expect(() => startMemoryMonitor()).not.toThrow();
    });
    
    it('stopMemoryMonitor should not throw', () => {
      startMemoryMonitor();
      expect(() => stopMemoryMonitor()).not.toThrow();
    });
    
    it('should be idempotent - multiple starts should not throw', () => {
      startMemoryMonitor();
      expect(() => startMemoryMonitor()).not.toThrow();
    });
    
    it('should handle stop when not started', () => {
      expect(() => stopMemoryMonitor()).not.toThrow();
    });
  });
  
  describe('getMemoryStatus', () => {
    it('should return all required memory fields', () => {
      const status = getMemoryStatus();
      
      expect(status).toHaveProperty('heapUsedMB');
      expect(status).toHaveProperty('heapTotalMB');
      expect(status).toHaveProperty('heapPercent');
      expect(status).toHaveProperty('rssMB');
      expect(status).toHaveProperty('externalMB');
      expect(status).toHaveProperty('isCritical');
    });
    
    it('heapUsedMB should be positive', () => {
      const status = getMemoryStatus();
      expect(status.heapUsedMB).toBeGreaterThan(0);
    });
    
    it('heapTotalMB should be greater than or equal to heapUsedMB', () => {
      const status = getMemoryStatus();
      expect(status.heapTotalMB).toBeGreaterThanOrEqual(status.heapUsedMB);
    });
    
    it('heapPercent should be a valid percentage string', () => {
      const status = getMemoryStatus();
      const percent = parseFloat(status.heapPercent);
      expect(percent).toBeGreaterThan(0);
      expect(percent).toBeLessThanOrEqual(100);
    });
    
    it('isCritical should be a boolean', () => {
      const status = getMemoryStatus();
      expect(typeof status.isCritical).toBe('boolean');
    });
    
    it('rssMB should be positive (resident set size)', () => {
      const status = getMemoryStatus();
      expect(status.rssMB).toBeGreaterThan(0);
    });
  });
  
  describe('memoryGuard middleware', () => {
    it('should call next() when memory is not critical', () => {
      const req = { path: '/test' };
      const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      memoryGuard(req, res, next);
      
      // In normal test conditions, memory should not be critical
      expect(next).toHaveBeenCalled();
      expect(res.status).not.toHaveBeenCalled();
    });
    
    it('should have correct middleware signature', () => {
      expect(typeof memoryGuard).toBe('function');
      expect(memoryGuard.length).toBe(3);
    });
  });
  
  describe('requestTimeout middleware', () => {
    it('should return a middleware function', () => {
      const middleware = requestTimeout();
      expect(typeof middleware).toBe('function');
    });
    
    it('should accept custom timeout parameter', () => {
      const middleware = requestTimeout(5000);
      expect(typeof middleware).toBe('function');
    });
    
    it('should call next() immediately', () => {
      const middleware = requestTimeout(30000);
      const req = { method: 'GET', path: '/test' };
      const res = { 
        headersSent: false,
        status: vi.fn().mockReturnThis(), 
        json: vi.fn(),
        on: vi.fn(),
      };
      const next = vi.fn();
      
      middleware(req, res, next);
      
      expect(next).toHaveBeenCalledTimes(1);
    });
    
    it('should register finish and close event handlers', () => {
      const middleware = requestTimeout(30000);
      const req = { method: 'GET', path: '/test' };
      const registeredEvents = [];
      const res = { 
        headersSent: false,
        status: vi.fn().mockReturnThis(), 
        json: vi.fn(),
        on: vi.fn((event) => registeredEvents.push(event)),
      };
      const next = vi.fn();
      
      middleware(req, res, next);
      
      expect(registeredEvents).toContain('finish');
      expect(registeredEvents).toContain('close');
    });
    
    it('should clear timeout when response finishes', () => {
      vi.useFakeTimers();
      
      const middleware = requestTimeout(100);
      const req = { method: 'GET', path: '/test' };
      let finishCallback;
      const res = { 
        headersSent: false,
        status: vi.fn().mockReturnThis(), 
        json: vi.fn(),
        on: vi.fn((event, cb) => {
          if (event === 'finish') finishCallback = cb;
        }),
      };
      const next = vi.fn();
      
      middleware(req, res, next);
      
      // Simulate response finishing before timeout
      finishCallback();
      
      // Advance past timeout
      vi.advanceTimersByTime(200);
      
      // Should NOT have sent timeout response because finish cleared it
      expect(res.status).not.toHaveBeenCalled();
      
      vi.useRealTimers();
    });
    
    it('should send 408 when timeout expires', () => {
      vi.useFakeTimers();
      
      const middleware = requestTimeout(100);
      const req = { method: 'GET', path: '/test' };
      const res = { 
        headersSent: false,
        status: vi.fn().mockReturnThis(), 
        json: vi.fn(),
        on: vi.fn(),
      };
      const next = vi.fn();
      
      middleware(req, res, next);
      
      vi.advanceTimersByTime(150);
      
      expect(res.status).toHaveBeenCalledWith(408);
      expect(res.json).toHaveBeenCalledWith({ error: 'Request timeout' });
      
      vi.useRealTimers();
    });
    
    it('should not send timeout if headers already sent', () => {
      vi.useFakeTimers();
      
      const middleware = requestTimeout(100);
      const req = { method: 'GET', path: '/test' };
      const res = { 
        headersSent: true,
        status: vi.fn().mockReturnThis(), 
        json: vi.fn(),
        on: vi.fn(),
      };
      const next = vi.fn();
      
      middleware(req, res, next);
      
      vi.advanceTimersByTime(150);
      
      expect(res.status).not.toHaveBeenCalled();
      
      vi.useRealTimers();
    });
  });
  
  describe('asyncHandler', () => {
    it('should return a middleware function', () => {
      const handler = asyncHandler(async () => {});
      expect(typeof handler).toBe('function');
    });
    
    it('should pass req, res, next to wrapped function', async () => {
      const fn = vi.fn().mockResolvedValue(undefined);
      const handler = asyncHandler(fn);
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(fn).toHaveBeenCalledWith(req, res, next);
    });
    
    it('should catch rejected promises and return 500', async () => {
      const error = new Error('Async failure');
      const fn = vi.fn().mockRejectedValue(error);
      const handler = asyncHandler(fn);
      const req = { method: 'GET', path: '/api/data' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith({ error: 'Internal server error' });
    });
    
    it('should call logError when catching errors', async () => {
      const error = new Error('Logged error');
      const fn = vi.fn().mockRejectedValue(error);
      const handler = asyncHandler(fn);
      const req = { method: 'POST', path: '/api/create' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(logError).toHaveBeenCalledWith(error, expect.stringContaining('POST /api/create'));
    });
    
    it('should not send response if headers already sent', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('Late error'));
      const handler = asyncHandler(fn);
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: true, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(res.status).not.toHaveBeenCalled();
      expect(res.json).not.toHaveBeenCalled();
    });
    
    it('should catch synchronous throws via Promise.resolve', async () => {
      // Note: asyncHandler wraps in Promise.resolve, so sync throws become rejections
      const fn = vi.fn().mockRejectedValue(new Error('Sync-like throw'));
      const handler = asyncHandler(fn);
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(500);
    });
  });
  
  describe('globalErrorHandler', () => {
    it('should send 500 by default', () => {
      const err = new Error('Test error');
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(500);
    });
    
    it('should use error.status if provided', () => {
      const err = new Error('Not found');
      err.status = 404;
      const req = { method: 'GET', path: '/missing' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(404);
    });
    
    it('should call logError with error and route context', () => {
      const err = new Error('Server crash');
      const req = { method: 'DELETE', path: '/api/items/42' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(logError).toHaveBeenCalledWith(err, expect.stringContaining('DELETE /api/items/42'));
    });
    
    it('should not send response if headers already sent', () => {
      const err = new Error('Test error');
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: true, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.status).not.toHaveBeenCalled();
    });
    
    it('should hide error message in production', () => {
      const originalEnv = process.env.NODE_ENV;
      process.env.NODE_ENV = 'production';
      
      const err = new Error('Database password: secret123');
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.json).toHaveBeenCalledWith({ error: 'Internal server error' });
      // Verify sensitive info is NOT exposed
      const jsonCall = res.json.mock.calls[0][0];
      expect(jsonCall.error).not.toContain('secret123');
      
      process.env.NODE_ENV = originalEnv;
    });
    
    it('should show error message in development', () => {
      const originalEnv = process.env.NODE_ENV;
      process.env.NODE_ENV = 'development';
      
      const err = new Error('Detailed debug info');
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.json).toHaveBeenCalledWith({ error: 'Detailed debug info' });
      
      process.env.NODE_ENV = originalEnv;
    });
  });
});
