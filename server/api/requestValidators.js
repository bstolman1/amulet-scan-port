/**
 * Request validators.
 *
 * Lightweight schema validation using plain functions — no heavy dependency.
 * Each validator returns `{ valid: boolean, error?: string }`.
 *
 * Route handlers call `validate(req.body, schema)` and immediately 400 on
 * invalid input, preventing scattered inline checks.
 */

import { VALID_TYPES } from './constants.js';

// ── Primitive checks ──────────────────────────────────────────────────────

const isNonEmptyString = v => typeof v === 'string' && v.trim().length > 0;
const isValidType = v => VALID_TYPES.includes(v);
const isBoolean = v => typeof v === 'boolean';
const isPositiveInt = v => Number.isInteger(v) && v > 0;

// ── Schema definitions ────────────────────────────────────────────────────

export const schemas = {
  setItemOverride: {
    required: [],
    fields: {
      itemId: { optional: true, check: isNonEmptyString, message: 'itemId must be a non-empty string' },
      primaryId: { optional: true, check: isNonEmptyString, message: 'primaryId must be a non-empty string' },
      type: { check: isValidType, message: `type must be one of: ${VALID_TYPES.join(', ')}` },
      reason: { optional: true, check: isNonEmptyString, message: 'reason must be a non-empty string' },
    },
    custom(body) {
      if (!body.itemId && !body.primaryId) {
        return 'Either itemId or primaryId is required';
      }
      return null;
    },
  },

  setTopicOverride: {
    fields: {
      topicId: { check: v => isNonEmptyString(String(v)), message: 'topicId is required' },
      newType: { check: isValidType, message: `newType must be one of: ${VALID_TYPES.join(', ')}` },
      reason: { optional: true, check: isNonEmptyString, message: 'reason must be a non-empty string' },
    },
  },

  setExtractOverride: {
    fields: {
      topicId: { check: v => v != null, message: 'topicId is required' },
      customName: { optional: true, check: isNonEmptyString, message: 'customName must be a non-empty string' },
      reason: { optional: true, check: isNonEmptyString, message: 'reason must be a non-empty string' },
    },
  },

  setMergeOverride: {
    fields: {
      mergeInto: {
        check: v => isNonEmptyString(v) || (Array.isArray(v) && v.length > 0 && v.every(isNonEmptyString)),
        message: 'mergeInto must be a non-empty string or array of strings',
      },
      reason: { optional: true, check: isNonEmptyString, message: 'reason must be a non-empty string' },
    },
    custom(body) {
      if (!body.sourceId && !body.sourcePrimaryId) {
        return 'Either sourceId or sourcePrimaryId is required';
      }
      return null;
    },
  },

  setMoveOverride: {
    fields: {
      topicId: { check: v => v != null, message: 'topicId is required' },
      targetCardId: { check: v => isNonEmptyString(String(v)), message: 'targetCardId is required' },
      reason: { optional: true, check: isNonEmptyString, message: 'reason must be a non-empty string' },
    },
  },

  applyImprovements: {
    fields: {
      dryRun: { optional: true, check: isBoolean, message: 'dryRun must be a boolean' },
    },
  },

  testProposals: {
    fields: {
      sampleSize: { optional: true, check: isPositiveInt, message: 'sampleSize must be a positive integer' },
    },
  },

  rollback: {
    fields: {
      targetVersion: { check: isNonEmptyString, message: 'targetVersion is required' },
    },
  },

  learningMode: {
    fields: {
      enabled: { check: isBoolean, message: 'enabled must be a boolean' },
    },
  },
};

// ── Validate function ─────────────────────────────────────────────────────

/**
 * Validate a request body against a named schema.
 * @param {object} body
 * @param {keyof typeof schemas} schemaName
 * @returns {{ valid: boolean, error?: string }}
 */
export function validate(body, schemaName) {
  const schema = schemas[schemaName];
  if (!schema) return { valid: false, error: `Unknown schema: ${schemaName}` };

  for (const [field, spec] of Object.entries(schema.fields ?? {})) {
    const value = body[field];
    if (value === undefined || value === null) {
      if (!spec.optional) return { valid: false, error: `${field} is required` };
      continue;
    }
    if (!spec.check(value)) return { valid: false, error: spec.message };
  }

  if (schema.custom) {
    const err = schema.custom(body);
    if (err) return { valid: false, error: err };
  }

  return { valid: true };
}

/**
 * Express middleware factory for body validation.
 * Usage: router.post('/foo', validateBody('setItemOverride'), handler)
 */
export function validateBody(schemaName) {
  return (req, res, next) => {
    const result = validate(req.body, schemaName);
    if (!result.valid) {
      return res.status(400).json({ error: result.error });
    }
    next();
  };
}
