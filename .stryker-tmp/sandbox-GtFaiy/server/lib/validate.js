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
  if (stryMutAct_9fa48("301")) {
    {}
  } else {
    stryCov_9fa48("301");
    return (req, res, next) => {
      if (stryMutAct_9fa48("302")) {
        {}
      } else {
        stryCov_9fa48("302");
        const data = req[source];
        const result = schema.safeParse(data);
        if (stryMutAct_9fa48("305") ? false : stryMutAct_9fa48("304") ? true : stryMutAct_9fa48("303") ? result.success : (stryCov_9fa48("303", "304", "305"), !result.success)) {
          if (stryMutAct_9fa48("306")) {
            {}
          } else {
            stryCov_9fa48("306");
            const errors = result.error.issues.map(stryMutAct_9fa48("307") ? () => undefined : (stryCov_9fa48("307"), issue => ({
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
  if (stryMutAct_9fa48("312")) {
    {}
  } else {
    stryCov_9fa48("312");
    return (req, res, next) => {
      if (stryMutAct_9fa48("313")) {
        {}
      } else {
        stryCov_9fa48("313");
        const errors = stryMutAct_9fa48("314") ? ["Stryker was here"] : (stryCov_9fa48("314"), []);
        const validated = {};
        for (const [source, schema] of Object.entries(schemas)) {
          if (stryMutAct_9fa48("315")) {
            {}
          } else {
            stryCov_9fa48("315");
            const data = req[source];
            const result = schema.safeParse(data);
            if (stryMutAct_9fa48("318") ? false : stryMutAct_9fa48("317") ? true : stryMutAct_9fa48("316") ? result.success : (stryCov_9fa48("316", "317", "318"), !result.success)) {
              if (stryMutAct_9fa48("319")) {
                {}
              } else {
                stryCov_9fa48("319");
                errors.push(...result.error.issues.map(stryMutAct_9fa48("320") ? () => undefined : (stryCov_9fa48("320"), issue => ({
                  source,
                  field: issue.path.join('.'),
                  message: issue.message,
                  code: issue.code
                }))));
              }
            } else {
              if (stryMutAct_9fa48("323")) {
                {}
              } else {
                stryCov_9fa48("323");
                validated[source] = result.data;
              }
            }
          }
        }
        if (stryMutAct_9fa48("327") ? errors.length <= 0 : stryMutAct_9fa48("326") ? errors.length >= 0 : stryMutAct_9fa48("325") ? false : stryMutAct_9fa48("324") ? true : (stryCov_9fa48("324", "325", "326", "327"), errors.length > 0)) {
          if (stryMutAct_9fa48("328")) {
            {}
          } else {
            stryCov_9fa48("328");
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
  if (stryMutAct_9fa48("331")) {
    {}
  } else {
    stryCov_9fa48("331");
    const result = schema.safeParse(data);
    if (stryMutAct_9fa48("334") ? false : stryMutAct_9fa48("333") ? true : stryMutAct_9fa48("332") ? result.success : (stryCov_9fa48("332", "333", "334"), !result.success)) {
      if (stryMutAct_9fa48("335")) {
        {}
      } else {
        stryCov_9fa48("335");
        return {
          success: stryMutAct_9fa48("337") ? true : (stryCov_9fa48("337"), false),
          errors: result.error.issues.map(stryMutAct_9fa48("338") ? () => undefined : (stryCov_9fa48("338"), issue => ({
            field: issue.path.join('.'),
            message: issue.message,
            code: issue.code
          })))
        };
      }
    }
    return {
      success: stryMutAct_9fa48("342") ? false : (stryCov_9fa48("342"), true),
      data: result.data
    };
  }
}
export default {
  validate,
  validateAll,
  parseData
};