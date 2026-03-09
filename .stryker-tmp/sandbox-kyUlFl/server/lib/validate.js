/**
 * Validation Middleware for Express
 * 
 * Provides middleware functions for validating request data using Zod schemas.
 * Returns 400 Bad Request with detailed error information on validation failure.
 */
// @ts-nocheck


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
 */function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
export function validate(schema, source = 'query') {
  if (stryMutAct_9fa48("714")) {
    {}
  } else {
    stryCov_9fa48("714");
    return (req, res, next) => {
      if (stryMutAct_9fa48("715")) {
        {}
      } else {
        stryCov_9fa48("715");
        const data = req[source];
        const result = schema.safeParse(data);
        if (stryMutAct_9fa48("718") ? false : stryMutAct_9fa48("717") ? true : stryMutAct_9fa48("716") ? result.success : (stryCov_9fa48("716", "717", "718"), !result.success)) {
          if (stryMutAct_9fa48("719")) {
            {}
          } else {
            stryCov_9fa48("719");
            const errors = result.error.issues.map(stryMutAct_9fa48("720") ? () => undefined : (stryCov_9fa48("720"), issue => ({
              field: issue.path.join('.'),
              message: issue.message,
              code: issue.code
            })));
            return res.status(400).json({
              error: 'Validation failed',
              details: errors
            });
          }
        }

        // Store validated and transformed data on req.validated
        req.validated = result.data;
        next();
      }
    };
  }
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
  if (stryMutAct_9fa48("725")) {
    {}
  } else {
    stryCov_9fa48("725");
    return (req, res, next) => {
      if (stryMutAct_9fa48("726")) {
        {}
      } else {
        stryCov_9fa48("726");
        const errors = stryMutAct_9fa48("727") ? ["Stryker was here"] : (stryCov_9fa48("727"), []);
        const validated = {};
        for (const [source, schema] of Object.entries(schemas)) {
          if (stryMutAct_9fa48("728")) {
            {}
          } else {
            stryCov_9fa48("728");
            const data = req[source];
            const result = schema.safeParse(data);
            if (stryMutAct_9fa48("731") ? false : stryMutAct_9fa48("730") ? true : stryMutAct_9fa48("729") ? result.success : (stryCov_9fa48("729", "730", "731"), !result.success)) {
              if (stryMutAct_9fa48("732")) {
                {}
              } else {
                stryCov_9fa48("732");
                errors.push(...result.error.issues.map(stryMutAct_9fa48("733") ? () => undefined : (stryCov_9fa48("733"), issue => ({
                  source,
                  field: issue.path.join('.'),
                  message: issue.message,
                  code: issue.code
                }))));
              }
            } else {
              if (stryMutAct_9fa48("736")) {
                {}
              } else {
                stryCov_9fa48("736");
                validated[source] = result.data;
              }
            }
          }
        }
        if (stryMutAct_9fa48("740") ? errors.length <= 0 : stryMutAct_9fa48("739") ? errors.length >= 0 : stryMutAct_9fa48("738") ? false : stryMutAct_9fa48("737") ? true : (stryCov_9fa48("737", "738", "739", "740"), errors.length > 0)) {
          if (stryMutAct_9fa48("741")) {
            {}
          } else {
            stryCov_9fa48("741");
            return res.status(400).json({
              error: 'Validation failed',
              details: errors
            });
          }
        }
        req.validated = validated;
        next();
      }
    };
  }
}

/**
 * Parse and validate data directly (non-middleware usage)
 * 
 * @param {import('zod').ZodSchema} schema - The Zod schema to validate against
 * @param {any} data - The data to validate
 * @returns {{ success: boolean, data?: any, errors?: Array }} Validation result
 */
export function parseData(schema, data) {
  if (stryMutAct_9fa48("744")) {
    {}
  } else {
    stryCov_9fa48("744");
    const result = schema.safeParse(data);
    if (stryMutAct_9fa48("747") ? false : stryMutAct_9fa48("746") ? true : stryMutAct_9fa48("745") ? result.success : (stryCov_9fa48("745", "746", "747"), !result.success)) {
      if (stryMutAct_9fa48("748")) {
        {}
      } else {
        stryCov_9fa48("748");
        return {
          success: stryMutAct_9fa48("750") ? true : (stryCov_9fa48("750"), false),
          errors: result.error.issues.map(stryMutAct_9fa48("751") ? () => undefined : (stryCov_9fa48("751"), issue => ({
            field: issue.path.join('.'),
            message: issue.message,
            code: issue.code
          })))
        };
      }
    }
    return {
      success: stryMutAct_9fa48("755") ? false : (stryCov_9fa48("755"), true),
      data: result.data
    };
  }
}
export default {
  validate,
  validateAll,
  parseData
};