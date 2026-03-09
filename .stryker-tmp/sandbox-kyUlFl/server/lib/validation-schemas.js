/**
 * Zod Validation Schemas for API Endpoints
 * 
 * Provides type-safe input validation for all API endpoints.
 * These schemas ensure data integrity and prevent malformed requests.
 */
// @ts-nocheck
function stryNS_9fa48() {
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
import { z } from 'zod';

/**
 * Pagination schema for list endpoints
 */
export const paginationSchema = z.object({
  limit: stryMutAct_9fa48("759") ? z.coerce.number().int().max(1).max(1000).default(100) : stryMutAct_9fa48("758") ? z.coerce.number().int().min(1).min(1000).default(100) : (stryCov_9fa48("758", "759"), z.coerce.number().int().min(1).max(1000).default(100)),
  offset: stryMutAct_9fa48("761") ? z.coerce.number().int().max(0).max(100000).default(0) : stryMutAct_9fa48("760") ? z.coerce.number().int().min(0).min(100000).default(0) : (stryCov_9fa48("760", "761"), z.coerce.number().int().min(0).max(100000).default(0))
});

/**
 * Event query schema for filtering events
 */
export const eventQuerySchema = paginationSchema.extend({
  type: z.enum(stryMutAct_9fa48("763") ? [] : (stryCov_9fa48("763"), ['created', 'archived', 'exercised', 'CreatedEvent', 'ArchivedEvent', 'ExercisedEvent'])).optional(),
  template: stryMutAct_9fa48("770") ? z.string().min(500).optional() : (stryCov_9fa48("770"), z.string().max(500).optional()),
  start: z.string().datetime().optional(),
  end: z.string().datetime().optional()
});

/**
 * Search query schema for full-text search
 */
export const searchQuerySchema = z.object({
  q: stryMutAct_9fa48("773") ? z.string().max(1, 'Search query is required').max(200, 'Search query too long') : stryMutAct_9fa48("772") ? z.string().min(1, 'Search query is required').min(200, 'Search query too long') : (stryCov_9fa48("772", "773"), z.string().min(1, 'Search query is required').max(200, 'Search query too long')),
  type: z.enum(stryMutAct_9fa48("776") ? [] : (stryCov_9fa48("776"), ['created', 'archived', 'exercised'])).optional(),
  template: stryMutAct_9fa48("780") ? z.string().min(500).optional() : (stryCov_9fa48("780"), z.string().max(500).optional()),
  party: stryMutAct_9fa48("781") ? z.string().min(500).optional() : (stryCov_9fa48("781"), z.string().max(500).optional()),
  limit: stryMutAct_9fa48("783") ? z.coerce.number().int().max(1).max(1000).default(100) : stryMutAct_9fa48("782") ? z.coerce.number().int().min(1).min(1000).default(100) : (stryCov_9fa48("782", "783"), z.coerce.number().int().min(1).max(1000).default(100)),
  offset: stryMutAct_9fa48("785") ? z.coerce.number().int().max(0).max(100000).default(0) : stryMutAct_9fa48("784") ? z.coerce.number().int().min(0).min(100000).default(0) : (stryCov_9fa48("784", "785"), z.coerce.number().int().min(0).max(100000).default(0))
});

/**
 * Contract ID schema - validates Daml contract ID format
 * Format: 00hex::Package.Module:Template#suffix
 */
export const contractIdSchema = stryMutAct_9fa48("787") ? z.string().max(1, 'Contract ID is required').max(500, 'Contract ID too long').regex(/^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/, 'Invalid contract ID format') : stryMutAct_9fa48("786") ? z.string().min(1, 'Contract ID is required').min(500, 'Contract ID too long').regex(/^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/, 'Invalid contract ID format') : (stryCov_9fa48("786", "787"), z.string().min(1, 'Contract ID is required').max(500, 'Contract ID too long').regex(stryMutAct_9fa48("802") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[^a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("801") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_])?$/ : stryMutAct_9fa48("800") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)$/ : stryMutAct_9fa48("799") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[^a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("798") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9])?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("797") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("796") ? /^[a-fA-F0-9]+(::[^a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("795") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-])?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("794") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("793") ? /^[^a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("792") ? /^[a-fA-F0-9](::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : stryMutAct_9fa48("791") ? /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?/ : stryMutAct_9fa48("790") ? /[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/ : (stryCov_9fa48("790", "791", "792", "793", "794", "795", "796", "797", "798", "799", "800", "801", "802"), /^[a-fA-F0-9]+(::[a-zA-Z0-9_.:-]+)?(@[a-fA-F0-9]+)?(#[a-zA-Z0-9_]+)?$/), 'Invalid contract ID format'));

/**
 * Contract ID param schema for URL params
 */
export const contractIdParamSchema = z.object({
  id: contractIdSchema
});

/**
 * Timestamp schema - validates ISO 8601 format
 */
export const timestampSchema = stryMutAct_9fa48("805") ? z.string().min(50, 'Timestamp too long').refine(val => {
  const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/;
  if (!isoPattern.test(val)) return false;
  const parsed = Date.parse(val);
  return !isNaN(parsed);
}, 'Invalid timestamp format') : (stryCov_9fa48("805"), z.string().max(50, 'Timestamp too long').refine(val => {
  if (stryMutAct_9fa48("807")) {
    {}
  } else {
    stryCov_9fa48("807");
    const isoPattern = stryMutAct_9fa48("832") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\D{2})?)?$/ : stryMutAct_9fa48("831") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d)?)?$/ : stryMutAct_9fa48("830") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\D{2}:\d{2})?)?$/ : stryMutAct_9fa48("829") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d:\d{2})?)?$/ : stryMutAct_9fa48("828") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[^+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("827") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2}))?$/ : stryMutAct_9fa48("826") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\D+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("825") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("824") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("823") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\D{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("822") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d)?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("821") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("820") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\D{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("819") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("818") ? /^\d{4}-\d{2}-\d{2}(T\D{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("817") ? /^\d{4}-\d{2}-\d{2}(T\d:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("816") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)$/ : stryMutAct_9fa48("815") ? /^\d{4}-\d{2}-\D{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("814") ? /^\d{4}-\d{2}-\d(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("813") ? /^\d{4}-\D{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("812") ? /^\d{4}-\d-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("811") ? /^\D{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("810") ? /^\d-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : stryMutAct_9fa48("809") ? /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?/ : stryMutAct_9fa48("808") ? /\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/ : (stryCov_9fa48("808", "809", "810", "811", "812", "813", "814", "815", "816", "817", "818", "819", "820", "821", "822", "823", "824", "825", "826", "827", "828", "829", "830", "831", "832"), /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$/);
    if (stryMutAct_9fa48("835") ? false : stryMutAct_9fa48("834") ? true : stryMutAct_9fa48("833") ? isoPattern.test(val) : (stryCov_9fa48("833", "834", "835"), !isoPattern.test(val))) return stryMutAct_9fa48("836") ? true : (stryCov_9fa48("836"), false);
    const parsed = Date.parse(val);
    return stryMutAct_9fa48("837") ? isNaN(parsed) : (stryCov_9fa48("837"), !isNaN(parsed));
  }
}, 'Invalid timestamp format'));

/**
 * Date range schema for filtering by time period
 */
export const dateRangeSchema = z.object({
  start: timestampSchema.optional(),
  end: timestampSchema.optional()
}).refine(data => {
  if (stryMutAct_9fa48("840")) {
    {}
  } else {
    stryCov_9fa48("840");
    if (stryMutAct_9fa48("843") ? data.start || data.end : stryMutAct_9fa48("842") ? false : stryMutAct_9fa48("841") ? true : (stryCov_9fa48("841", "842", "843"), data.start && data.end)) {
      if (stryMutAct_9fa48("844")) {
        {}
      } else {
        stryCov_9fa48("844");
        return stryMutAct_9fa48("848") ? new Date(data.start) > new Date(data.end) : stryMutAct_9fa48("847") ? new Date(data.start) < new Date(data.end) : stryMutAct_9fa48("846") ? false : stryMutAct_9fa48("845") ? true : (stryCov_9fa48("845", "846", "847", "848"), new Date(data.start) <= new Date(data.end));
      }
    }
    return stryMutAct_9fa48("849") ? false : (stryCov_9fa48("849"), true);
  }
}, 'Start date must be before end date');

/**
 * Template ID schema - validates Daml template identifier format
 */
export const templateIdSchema = stryMutAct_9fa48("852") ? z.string().max(1, 'Template ID is required').max(500, 'Template ID too long').regex(/^[\w.:@-]+$/i, 'Invalid template ID format') : stryMutAct_9fa48("851") ? z.string().min(1, 'Template ID is required').min(500, 'Template ID too long').regex(/^[\w.:@-]+$/i, 'Invalid template ID format') : (stryCov_9fa48("851", "852"), z.string().min(1, 'Template ID is required').max(500, 'Template ID too long').regex(stryMutAct_9fa48("859") ? /^[\W.:@-]+$/i : stryMutAct_9fa48("858") ? /^[^\w.:@-]+$/i : stryMutAct_9fa48("857") ? /^[\w.:@-]$/i : stryMutAct_9fa48("856") ? /^[\w.:@-]+/i : stryMutAct_9fa48("855") ? /[\w.:@-]+$/i : (stryCov_9fa48("855", "856", "857", "858", "859"), /^[\w.:@-]+$/i), 'Invalid template ID format'));

/**
 * Party ID schema - validates Daml party identifier format
 */
export const partyIdSchema = stryMutAct_9fa48("862") ? z.string().max(1, 'Party ID is required').max(500, 'Party ID too long').regex(/^[\w.:@-]+$/i, 'Invalid party ID format') : stryMutAct_9fa48("861") ? z.string().min(1, 'Party ID is required').min(500, 'Party ID too long').regex(/^[\w.:@-]+$/i, 'Invalid party ID format') : (stryCov_9fa48("861", "862"), z.string().min(1, 'Party ID is required').max(500, 'Party ID too long').regex(stryMutAct_9fa48("869") ? /^[\W.:@-]+$/i : stryMutAct_9fa48("868") ? /^[^\w.:@-]+$/i : stryMutAct_9fa48("867") ? /^[\w.:@-]$/i : stryMutAct_9fa48("866") ? /^[\w.:@-]+/i : stryMutAct_9fa48("865") ? /[\w.:@-]+$/i : (stryCov_9fa48("865", "866", "867", "868", "869"), /^[\w.:@-]+$/i), 'Invalid party ID format'));

/**
 * Stats query schema for statistics endpoints
 */
export const statsQuerySchema = z.object({
  template: templateIdSchema.optional(),
  groupBy: z.enum(stryMutAct_9fa48("872") ? [] : (stryCov_9fa48("872"), ['day', 'week', 'month'])).optional(),
  ...dateRangeSchema.shape
});

/**
 * Governance query schema for governance lifecycle endpoints
 */
export const governanceQuerySchema = paginationSchema.extend({
  status: z.enum(stryMutAct_9fa48("877") ? [] : (stryCov_9fa48("877"), ['pending', 'accepted', 'rejected', 'expired'])).optional(),
  actionType: stryMutAct_9fa48("882") ? z.string().min(200).optional() : (stryCov_9fa48("882"), z.string().max(200).optional())
});

/**
 * Backfill cursor schema
 */
export const backfillCursorSchema = z.object({
  migrationId: stryMutAct_9fa48("884") ? z.coerce.number().int().max(0) : (stryCov_9fa48("884"), z.coerce.number().int().min(0)),
  synchronizerId: stryMutAct_9fa48("885") ? z.string().min(200).optional() : (stryCov_9fa48("885"), z.string().max(200).optional())
});

/**
 * ACS snapshot query schema
 */
export const acsQuerySchema = paginationSchema.extend({
  template: templateIdSchema.optional(),
  filter: stryMutAct_9fa48("887") ? z.string().min(200).optional() : (stryCov_9fa48("887"), z.string().max(200).optional())
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
  acsQuerySchema
};