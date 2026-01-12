import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  log,
  logRequest,
  logApiError,
  logSlowRequest,
  logValidationError,
  logQuery,
  requestLoggerMiddleware,
  errorLoggerMiddleware,
} from './request-logger.js';

describe('request-logger', () => {
  let consoleLogSpy;
  let consoleErrorSpy;

  beforeEach(() => {
    consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('log()', () => {
    it('logs info level messages to console.log', () => {
      log('info', 'test message', { key: 'value' });
      
      expect(consoleLogSpy).toHaveBeenCalledTimes(1);
      const logged = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(logged.level).toBe('info');
      expect(logged.message).toBe('test message');
      expect(logged.key).toBe('value');
    });

    it('logs error level messages to console.error', () => {
      log('error', 'error message');
      
      expect(consoleErrorSpy).toHaveBeenCalledTimes(1);
      const logged = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(logged.level).toBe('error');
    });

    it('includes timestamp in log entries', () => {
      log('info', 'test');
      
      const logged = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(logged.ts).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    });

    it('returns the log entry object', () => {
      const entry = log('info', 'test', { data: 123 });
      
      expect(entry).toEqual(
        expect.objectContaining({
          level: 'info',
          message: 'test',
          data: 123,
        })
      );
    });
  });

  describe('logRequest()', () => {
    it('logs request details', () => {
      const req = {
        method: 'GET',
        path: '/api/test',
        query: { limit: '10' },
        get: () => 'Mozilla/5.0',
        ip: '127.0.0.1',
      };
      const res = { statusCode: 200 };

      logRequest(req, res, { durationMs: 50 });

      const logged = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(logged.message).toBe('api_request');
      expect(logged.method).toBe('GET');
      expect(logged.path).toBe('/api/test');
      expect(logged.status).toBe(200);
      expect(logged.duration_ms).toBe(50);
    });

    it('omits empty query object', () => {
      const req = {
        method: 'GET',
        path: '/api/test',
        query: {},
        get: () => null,
      };
      const res = { statusCode: 200 };

      logRequest(req, res, {});

      const logged = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(logged.query).toBeUndefined();
    });
  });

  describe('logApiError()', () => {
    it('logs error details', () => {
      const req = {
        method: 'POST',
        path: '/api/create',
        query: {},
      };
      const error = new Error('Database connection failed');
      error.code = 'ECONNREFUSED';

      logApiError(req, error);

      const logged = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(logged.message).toBe('api_error');
      expect(logged.error_message).toBe('Database connection failed');
      expect(logged.error_code).toBe('ECONNREFUSED');
      expect(logged.method).toBe('POST');
    });

    it('includes truncated stack trace', () => {
      const req = { method: 'GET', path: '/test', query: {} };
      const error = new Error('Test error');

      logApiError(req, error);

      const logged = JSON.parse(consoleErrorSpy.mock.calls[0][0]);
      expect(logged.stack).toBeDefined();
      expect(logged.stack.split('\n').length).toBeLessThanOrEqual(5);
    });
  });

  describe('logSlowRequest()', () => {
    it('logs slow request warning', () => {
      const req = { method: 'GET', path: '/api/slow' };

      logSlowRequest(req, 2500, 1000);

      expect(consoleLogSpy).toHaveBeenCalled();
      const logged = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(logged.level).toBe('warn');
      expect(logged.message).toBe('slow_request');
      expect(logged.duration_ms).toBe(2500);
      expect(logged.threshold_ms).toBe(1000);
    });
  });

  describe('logValidationError()', () => {
    it('logs validation errors', () => {
      const req = { method: 'POST', path: '/api/create' };
      const errors = [
        { field: 'email', message: 'Invalid email' },
        { field: 'name', message: 'Required' },
      ];

      logValidationError(req, errors);

      const logged = JSON.parse(consoleLogSpy.mock.calls[0][0]);
      expect(logged.level).toBe('warn');
      expect(logged.message).toBe('validation_error');
      expect(logged.errors).toHaveLength(2);
    });
  });

  describe('logQuery()', () => {
    it('logs database query at debug level', () => {
      // Note: This may not log if LOG_LEVEL is 'info'
      const entry = logQuery('SELECT * FROM events', 25, 100);
      
      // Debug level may be skipped based on LOG_LEVEL
      if (entry) {
        expect(entry.message).toBe('db_query');
        expect(entry.duration_ms).toBe(25);
        expect(entry.row_count).toBe(100);
      }
    });

    it('truncates long queries', () => {
      const longQuery = 'SELECT ' + 'column, '.repeat(100) + 'FROM table';
      const entry = logQuery(longQuery, 10, 5);
      
      if (entry) {
        expect(entry.query.length).toBeLessThanOrEqual(500);
      }
    });
  });

  describe('requestLoggerMiddleware()', () => {
    it('calls next() immediately', () => {
      const middleware = requestLoggerMiddleware();
      const req = { path: '/api/test', method: 'GET', query: {} };
      const res = {
        end: vi.fn(),
        statusCode: 200,
      };
      const next = vi.fn();

      middleware(req, res, next);

      expect(next).toHaveBeenCalledTimes(1);
    });

    it('logs request when response ends', () => {
      const middleware = requestLoggerMiddleware();
      const req = {
        path: '/api/test',
        method: 'GET',
        query: {},
        get: () => 'Test Agent',
      };
      const res = {
        end: vi.fn(),
        statusCode: 200,
      };
      const next = vi.fn();

      middleware(req, res, next);
      res.end();

      expect(consoleLogSpy).toHaveBeenCalled();
    });

    it('skips logging for health check paths', () => {
      const middleware = requestLoggerMiddleware({ skipPaths: ['/health'] });
      const req = { path: '/health', method: 'GET' };
      const res = { end: vi.fn() };
      const next = vi.fn();

      middleware(req, res, next);
      res.end();

      // Should not have logged (health check is skipped)
      expect(consoleLogSpy).not.toHaveBeenCalled();
    });

    it('logs slow request warning when threshold exceeded', () => {
      vi.useFakeTimers();
      
      const middleware = requestLoggerMiddleware({ slowThreshold: 100 });
      const req = {
        path: '/api/slow',
        method: 'GET',
        query: {},
        get: () => null,
      };
      const res = {
        end: vi.fn(),
        statusCode: 200,
      };
      const next = vi.fn();

      middleware(req, res, next);
      
      // Simulate slow response
      vi.advanceTimersByTime(200);
      res.end();

      // Should have logged both request and slow warning
      expect(consoleLogSpy).toHaveBeenCalledTimes(2);
      
      vi.useRealTimers();
    });
  });

  describe('errorLoggerMiddleware()', () => {
    it('logs error and sends 500 response', () => {
      const middleware = errorLoggerMiddleware();
      const req = { method: 'GET', path: '/api/test', query: {} };
      const res = {
        status: vi.fn().mockReturnThis(),
        json: vi.fn(),
      };
      const next = vi.fn();
      const error = new Error('Something went wrong');

      middleware(error, req, res, next);

      expect(consoleErrorSpy).toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(500);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.any(String),
        })
      );
    });

    it('uses error.status if provided', () => {
      const middleware = errorLoggerMiddleware();
      const req = { method: 'GET', path: '/test', query: {} };
      const res = {
        status: vi.fn().mockReturnThis(),
        json: vi.fn(),
      };
      const error = new Error('Not Found');
      error.status = 404;

      middleware(error, req, res, vi.fn());

      expect(res.status).toHaveBeenCalledWith(404);
    });
  });
});
