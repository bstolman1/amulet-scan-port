/**
 * Validation Middleware for Express
 * 
 * Provides middleware functions for validating request data using Zod schemas.
 * Returns 400 Bad Request with detailed error information on validation failure.
 */

/**
 * Create validation middleware for a given schema
 * 
 * @param {import('zod').ZodSchema} schema - The Zod schema to validate against
 * @param {string} source - The request property to validate ('query', 'body', 'params')
 * @returns {import('express').RequestHandler} Express middleware function
 * 
 * @example
 * import { paginationSchema } from './validation-schemas.js';
 * import { validate } from './validate.js';
 * 
 * router.get('/items', validate(paginationSchema), (req, res) => {
 *   const { limit, offset } = req.validated;
 *   // limit and offset are guaranteed to be valid numbers
 * });
 */
export function validate(schema, source = 'query') {
  return (req, res, next) => {
    const data = req[source];
    const result = schema.safeParse(data);
    
    if (!result.success) {
      const errors = result.error.issues.map(issue => ({
        field: issue.path.join('.'),
        message: issue.message,
        code: issue.code,
      }));
      
      return res.status(400).json({
        error: 'Validation failed',
        details: errors,
      });
    }
    
    // Store validated and transformed data on req.validated
    req.validated = result.data;
    next();
  };
}

/**
 * Validate multiple sources at once
 * 
 * @param {Object} schemas - Object mapping source names to Zod schemas
 * @returns {import('express').RequestHandler} Express middleware function
 * 
 * @example
 * router.get('/items/:id', validateAll({
 *   params: idParamSchema,
 *   query: paginationSchema,
 * }), handler);
 */
export function validateAll(schemas) {
  return (req, res, next) => {
    const errors = [];
    const validated = {};
    
    for (const [source, schema] of Object.entries(schemas)) {
      const data = req[source];
      const result = schema.safeParse(data);
      
      if (!result.success) {
        errors.push(...result.error.issues.map(issue => ({
          source,
          field: issue.path.join('.'),
          message: issue.message,
          code: issue.code,
        })));
      } else {
        validated[source] = result.data;
      }
    }
    
    if (errors.length > 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: errors,
      });
    }
    
    req.validated = validated;
    next();
  };
}

/**
 * Parse and validate data directly (non-middleware usage)
 * 
 * @param {import('zod').ZodSchema} schema - The Zod schema to validate against
 * @param {any} data - The data to validate
 * @returns {{ success: boolean, data?: any, errors?: Array }} Validation result
 */
export function parseData(schema, data) {
  const result = schema.safeParse(data);
  
  if (!result.success) {
    return {
      success: false,
      errors: result.error.issues.map(issue => ({
        field: issue.path.join('.'),
        message: issue.message,
        code: issue.code,
      })),
    };
  }
  
  return {
    success: true,
    data: result.data,
  };
}

export default {
  validate,
  validateAll,
  parseData,
};
