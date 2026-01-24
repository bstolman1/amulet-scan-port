/**
 * Server Protection Tests
 * 
 * Tests rate limiting, memory monitoring, request timeouts, and error handlers.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock dependencies
vi.mock('./crash-logger.js', () => ({
  logError: vi.fn(),
}));

vi.mock('express-rate-limit', () => ({
  default: vi.fn((options) => {
    const middleware = (req, res, next) => next();
    middleware.options = options;
    return middleware;
  }),
}));

vi.mock('helmet', () => ({
  default: vi.fn((options) => {
    const middleware = (req, res, next) => next();
    middleware.options = options;
    return middleware;
  }),
}));

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
  
  describe('Rate Limiters', () => {
    it('should have apiLimiter configured', () => {
      expect(apiLimiter).toBeDefined();
      expect(typeof apiLimiter).toBe('function');
    });
    
    it('should have expensiveLimiter configured', () => {
      expect(expensiveLimiter).toBeDefined();
      expect(typeof expensiveLimiter).toBe('function');
    });
    
    it('apiLimiter should allow 100 requests per minute', () => {
      expect(apiLimiter.options.max).toBe(100);
      expect(apiLimiter.options.windowMs).toBe(60 * 1000);
    });
    
    it('expensiveLimiter should allow 20 requests per minute', () => {
      expect(expensiveLimiter.options.max).toBe(20);
      expect(expensiveLimiter.options.windowMs).toBe(60 * 1000);
    });
  });
  
  describe('Security Headers', () => {
    it('should have securityHeaders configured', () => {
      expect(securityHeaders).toBeDefined();
      expect(typeof securityHeaders).toBe('function');
    });
    
    it('should call next() when invoked', () => {
      const next = vi.fn();
      securityHeaders({}, {}, next);
      expect(next).toHaveBeenCalled();
    });
  });
  
  describe('Memory Monitor', () => {
    beforeEach(() => {
      stopMemoryMonitor();
    });
    
    afterEach(() => {
      stopMemoryMonitor();
    });
    
    it('should start memory monitor without error', () => {
      expect(() => startMemoryMonitor()).not.toThrow();
    });
    
    it('should stop memory monitor without error', () => {
      startMemoryMonitor();
      expect(() => stopMemoryMonitor()).not.toThrow();
    });
    
    it('should not start duplicate monitors', () => {
      startMemoryMonitor();
      startMemoryMonitor(); // Should not throw
      stopMemoryMonitor();
    });
    
    it('should handle stop when not started', () => {
      expect(() => stopMemoryMonitor()).not.toThrow();
    });
  });
  
  describe('getMemoryStatus', () => {
    it('should return memory status object', () => {
      const status = getMemoryStatus();
      
      expect(status).toHaveProperty('heapUsedMB');
      expect(status).toHaveProperty('heapTotalMB');
      expect(status).toHaveProperty('heapPercent');
      expect(status).toHaveProperty('rssMB');
      expect(status).toHaveProperty('externalMB');
      expect(status).toHaveProperty('isCritical');
    });
    
    it('should return numeric values for memory', () => {
      const status = getMemoryStatus();
      
      expect(typeof status.heapUsedMB).toBe('number');
      expect(typeof status.heapTotalMB).toBe('number');
      expect(typeof status.rssMB).toBe('number');
      expect(typeof status.externalMB).toBe('number');
    });
    
    it('should return string percentage', () => {
      const status = getMemoryStatus();
      expect(typeof status.heapPercent).toBe('string');
      expect(parseFloat(status.heapPercent)).toBeGreaterThan(0);
    });
    
    it('should return boolean for isCritical', () => {
      const status = getMemoryStatus();
      expect(typeof status.isCritical).toBe('boolean');
    });
  });
  
  describe('memoryGuard', () => {
    it('should call next() when memory is not critical', () => {
      const req = { path: '/test' };
      const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      memoryGuard(req, res, next);
      
      expect(next).toHaveBeenCalled();
      expect(res.status).not.toHaveBeenCalled();
    });
    
    it('should be a function', () => {
      expect(typeof memoryGuard).toBe('function');
    });
  });
  
  describe('requestTimeout', () => {
    it('should return a middleware function', () => {
      const middleware = requestTimeout();
      expect(typeof middleware).toBe('function');
    });
    
    it('should return middleware with custom timeout', () => {
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
      
      expect(next).toHaveBeenCalled();
    });
    
    it('should register finish and close handlers', () => {
      const middleware = requestTimeout(30000);
      const req = { method: 'GET', path: '/test' };
      const onHandlers = {};
      const res = { 
        headersSent: false,
        status: vi.fn().mockReturnThis(), 
        json: vi.fn(),
        on: vi.fn((event, handler) => { onHandlers[event] = handler; }),
      };
      const next = vi.fn();
      
      middleware(req, res, next);
      
      expect(res.on).toHaveBeenCalledWith('finish', expect.any(Function));
      expect(res.on).toHaveBeenCalledWith('close', expect.any(Function));
    });
  });
  
  describe('asyncHandler', () => {
    it('should return a middleware function', () => {
      const handler = asyncHandler(async () => {});
      expect(typeof handler).toBe('function');
    });
    
    it('should call the wrapped function', async () => {
      const fn = vi.fn().mockResolvedValue(undefined);
      const handler = asyncHandler(fn);
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(fn).toHaveBeenCalledWith(req, res, next);
    });
    
    it('should catch errors and call json with error', async () => {
      const error = new Error('Test error');
      const fn = vi.fn().mockRejectedValue(error);
      const handler = asyncHandler(fn);
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith({ error: 'Internal server error' });
    });
    
    it('should not send response if headers already sent', async () => {
      const error = new Error('Test error');
      const fn = vi.fn().mockRejectedValue(error);
      const handler = asyncHandler(fn);
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: true, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      await handler(req, res, next);
      
      expect(res.status).not.toHaveBeenCalled();
      expect(res.json).not.toHaveBeenCalled();
    });
  });
  
  describe('globalErrorHandler', () => {
    it('should send 500 status by default', () => {
      const err = new Error('Test error');
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(500);
    });
    
    it('should use error status if provided', () => {
      const err = new Error('Not found');
      err.status = 404;
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(404);
    });
    
    it('should not send response if headers already sent', () => {
      const err = new Error('Test error');
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: true, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.status).not.toHaveBeenCalled();
      expect(res.json).not.toHaveBeenCalled();
    });
    
    it('should hide error message in production', () => {
      const originalEnv = process.env.NODE_ENV;
      process.env.NODE_ENV = 'production';
      
      const err = new Error('Sensitive error details');
      const req = { method: 'GET', path: '/test' };
      const res = { headersSent: false, status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();
      
      globalErrorHandler(err, req, res, next);
      
      expect(res.json).toHaveBeenCalledWith({ error: 'Internal server error' });
      
      process.env.NODE_ENV = originalEnv;
    });
  });
});
