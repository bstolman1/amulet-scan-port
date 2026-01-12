/**
 * Zod Validation Schemas for API Endpoints
 * 
 * Provides type-safe input validation for all API endpoints.
 * These schemas ensure data integrity and prevent malformed requests.
 */

import { z } from 'zod';

/**
 * Pagination schema for list endpoints
 */
export const paginationSchema = z.object({
  limit: z.coerce.number().int().min(1).max(1000).default(100),
  offset: z.coerce.number().int().min(0).max(100000).default(0),
});

/**
 * Event query schema for filtering events
 */
export const eventQuerySchema = paginationSchema.extend({
  type: z.enum(['created', 'archived', 'exercised', 'CreatedEvent', 'ArchivedEvent', 'ExercisedEvent']).optional(),
  template: z.string().max(500).optional(),
  start: z.string().datetime().optional(),
  end: z.string().datetime().optional(),
});

/**
 * Search query schema for full-text search
 */
export const searchQuerySchema = z.object({
  q: z.string().min(1, 'Search query is required').max(200, 'Search query too long'),
  type: z.enum(['created', 'archived', 'exercised']).optional(),
  template: z.string().max(500).optional(),
  party: z.string().max(500).optional(),
  limit: z.coerce.number().int().min(1).max(1000).default(100),
  offset: z.coerce.number().int().min(0).max(100000).default(0),
});

/**
 * Contract ID schema - validates Daml contract ID format
 * Format: 00hex::Package.Module:Template#suffix
 */
export const contractIdSchema = z.string()
  .min(1, 'Contract ID is required')
  .max(500, 'Contract ID too long')
  .regex(
    /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/,
    'Invalid contract ID format'
  );

/**
 * Contract ID param schema for URL params
 */
export const contractIdParamSchema = z.object({
  id: contractIdSchema,
});

/**
 * Timestamp schema - validates ISO 8601 format
 */
export const timestampSchema = z.string()
  .max(50, 'Timestamp too long')
  .refine((val) => {
    const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/;
    if (!isoPattern.test(val)) return false;
    const parsed = Date.parse(val);
    return !isNaN(parsed);
  }, 'Invalid timestamp format');

/**
 * Date range schema for filtering by time period
 */
export const dateRangeSchema = z.object({
  start: timestampSchema.optional(),
  end: timestampSchema.optional(),
}).refine((data) => {
  if (data.start && data.end) {
    return new Date(data.start) <= new Date(data.end);
  }
  return true;
}, 'Start date must be before end date');

/**
 * Template ID schema - validates Daml template identifier format
 */
export const templateIdSchema = z.string()
  .min(1, 'Template ID is required')
  .max(500, 'Template ID too long')
  .regex(/^[\w.:@-]+$/i, 'Invalid template ID format');

/**
 * Party ID schema - validates Daml party identifier format
 */
export const partyIdSchema = z.string()
  .min(1, 'Party ID is required')
  .max(500, 'Party ID too long')
  .regex(/^[\w.:@-]+$/i, 'Invalid party ID format');

/**
 * Stats query schema for statistics endpoints
 */
export const statsQuerySchema = z.object({
  template: templateIdSchema.optional(),
  groupBy: z.enum(['day', 'week', 'month']).optional(),
  ...dateRangeSchema.shape,
});

/**
 * Governance query schema for governance lifecycle endpoints
 */
export const governanceQuerySchema = paginationSchema.extend({
  status: z.enum(['pending', 'accepted', 'rejected', 'expired']).optional(),
  actionType: z.string().max(200).optional(),
});

/**
 * Backfill cursor schema
 */
export const backfillCursorSchema = z.object({
  migrationId: z.coerce.number().int().min(0),
  synchronizerId: z.string().max(200).optional(),
});

/**
 * ACS snapshot query schema
 */
export const acsQuerySchema = paginationSchema.extend({
  template: templateIdSchema.optional(),
  filter: z.string().max(200).optional(),
});

export default {
  paginationSchema,
  eventQuerySchema,
  searchQuerySchema,
  contractIdSchema,
  contractIdParamSchema,
  timestampSchema,
  dateRangeSchema,
  templateIdSchema,
  partyIdSchema,
  statsQuerySchema,
  governanceQuerySchema,
  backfillCursorSchema,
  acsQuerySchema,
};
