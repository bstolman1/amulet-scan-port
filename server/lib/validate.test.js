import { describe, it, expect, vi } from 'vitest';
import { z } from 'zod';
import { validate, validateAll, parseData } from './validate.js';

describe('validate middleware', () => {
  const mockSchema = z.object({
    name: z.string().min(1),
    count: z.coerce.number().int().positive(),
  });

  const createMockReqRes = (data, source = 'query') => {
    const req = { [source]: data };
    const res = {
      status: vi.fn().mockReturnThis(),
      json: vi.fn().mockReturnThis(),
    };
    const next = vi.fn();
    return { req, res, next };
  };

  describe('validate()', () => {
    it('passes valid data and sets req.validated', () => {
      const { req, res, next } = createMockReqRes({ name: 'test', count: '5' });
      
      validate(mockSchema)(req, res, next);
      
      expect(next).toHaveBeenCalledTimes(1);
      expect(req.validated).toEqual({ name: 'test', count: 5 });
      expect(res.status).not.toHaveBeenCalled();
    });

    it('returns 400 for invalid data', () => {
      const { req, res, next } = createMockReqRes({ name: '', count: 'abc' });
      
      validate(mockSchema)(req, res, next);
      
      expect(next).not.toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(400);
      expect(res.json).toHaveBeenCalledWith(
        expect.objectContaining({
          error: 'Validation failed',
          details: expect.any(Array),
        })
      );
    });

    it('includes field path in error details', () => {
      const { req, res, next } = createMockReqRes({ name: '' });
      
      validate(mockSchema)(req, res, next);
      
      const errorResponse = res.json.mock.calls[0][0];
      expect(errorResponse.details).toContainEqual(
        expect.objectContaining({
          field: 'name',
          code: expect.any(String),
        })
      );
    });

    it('validates body source', () => {
      const { req, res, next } = createMockReqRes({ name: 'test', count: 10 }, 'body');
      
      validate(mockSchema, 'body')(req, res, next);
      
      expect(next).toHaveBeenCalled();
      expect(req.validated).toEqual({ name: 'test', count: 10 });
    });

    it('validates params source', () => {
      const idSchema = z.object({ id: z.string().min(1) });
      const { req, res, next } = createMockReqRes({ id: 'abc123' }, 'params');
      
      validate(idSchema, 'params')(req, res, next);
      
      expect(next).toHaveBeenCalled();
      expect(req.validated).toEqual({ id: 'abc123' });
    });

    it('handles missing required fields', () => {
      const { req, res, next } = createMockReqRes({});
      
      validate(mockSchema)(req, res, next);
      
      expect(res.status).toHaveBeenCalledWith(400);
      const errorResponse = res.json.mock.calls[0][0];
      expect(errorResponse.details.length).toBeGreaterThan(0);
    });
  });

  describe('validateAll()', () => {
    const paramsSchema = z.object({ id: z.string().min(1) });
    const querySchema = z.object({ limit: z.coerce.number().default(10) });

    it('validates multiple sources', () => {
      const req = {
        params: { id: 'abc123' },
        query: { limit: '25' },
      };
      const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();

      validateAll({ params: paramsSchema, query: querySchema })(req, res, next);

      expect(next).toHaveBeenCalled();
      expect(req.validated).toEqual({
        params: { id: 'abc123' },
        query: { limit: 25 },
      });
    });

    it('returns combined errors from multiple sources', () => {
      const req = {
        params: { id: '' },
        query: { limit: 'invalid' },
      };
      const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();

      validateAll({ params: paramsSchema, query: querySchema })(req, res, next);

      expect(next).not.toHaveBeenCalled();
      expect(res.status).toHaveBeenCalledWith(400);
      const errorResponse = res.json.mock.calls[0][0];
      expect(errorResponse.details.some(e => e.source === 'params')).toBe(true);
    });

    it('includes source name in error details', () => {
      const req = {
        params: { id: '' },
        query: {},
      };
      const res = { status: vi.fn().mockReturnThis(), json: vi.fn() };
      const next = vi.fn();

      validateAll({ params: paramsSchema, query: querySchema })(req, res, next);

      const errorResponse = res.json.mock.calls[0][0];
      expect(errorResponse.details[0].source).toBe('params');
    });
  });

  describe('parseData()', () => {
    const schema = z.object({
      email: z.string().email(),
      age: z.number().min(0),
    });

    it('returns success true with parsed data for valid input', () => {
      const result = parseData(schema, { email: 'test@example.com', age: 25 });
      
      expect(result.success).toBe(true);
      expect(result.data).toEqual({ email: 'test@example.com', age: 25 });
      expect(result.errors).toBeUndefined();
    });

    it('returns success false with errors for invalid input', () => {
      const result = parseData(schema, { email: 'invalid', age: -5 });
      
      expect(result.success).toBe(false);
      expect(result.data).toBeUndefined();
      expect(result.errors).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ field: 'email' }),
          expect.objectContaining({ field: 'age' }),
        ])
      );
    });

    it('handles nested object validation', () => {
      const nestedSchema = z.object({
        user: z.object({
          name: z.string(),
          email: z.string().email(),
        }),
      });

      const result = parseData(nestedSchema, { user: { name: 'Test', email: 'invalid' } });
      
      expect(result.success).toBe(false);
      expect(result.errors[0].field).toBe('user.email');
    });

    it('handles array validation', () => {
      const arraySchema = z.object({
        tags: z.array(z.string().min(1)),
      });

      const result = parseData(arraySchema, { tags: ['valid', ''] });
      
      expect(result.success).toBe(false);
      expect(result.errors[0].field).toContain('tags');
    });
  });
});
