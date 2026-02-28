/**
 * Fetch Result Types - Explicit State Machine for API Responses
 *
 * CRITICAL: Every API fetch MUST return one of these three states:
 * 1. SUCCESS_DATA   - Got data, process it
 * 2. SUCCESS_EMPTY  - Legit empty (no data in range), advance cursor
 * 3. FAILURE        - Network/decode error, MUST retry or fail run
 *
 * This eliminates the "silent success on error" bug where network failures
 * were incorrectly treated as "no data".
 */

// FIX #1: Module-level Sets for O(1) lookup instead of arrays rebuilt per call.
//
// IMPORTANT: Keep RETRYABLE_CODES in sync with TRANSIENT_NETWORK_CODES in
// fetch-backfill.js. They cover the same semantic space — one is used inside
// the fetch pipeline (here), the other in the outer orchestration layer.
// If you add a code to one, add it to the other.
//
// Differences from the old array:
//   Added:  ECONNABORTED (connection aborted mid-flight, definitely transient)
//           ERR_BAD_RESPONSE (axios response parsing failure, often transient)
//   Kept:   ECONNRESET, ETIMEDOUT, ECONNREFUSED, EPIPE, ENOTFOUND, EAI_AGAIN, EPROTO
export const RETRYABLE_CODES = new Set([
  'ECONNRESET',
  'ETIMEDOUT',
  'ECONNREFUSED',
  'ECONNABORTED',   // FIX #1: added — matches fetch-backfill.js TRANSIENT_NETWORK_CODES
  'EPIPE',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EPROTO',
  'ERR_BAD_RESPONSE', // FIX #1: added — matches fetch-backfill.js TRANSIENT_NETWORK_CODES
]);

const RETRYABLE_STATUSES = new Set([429, 500, 502, 503, 504]);
const PERMANENT_STATUSES = new Set([400, 401, 403, 404, 422]);

export const FetchResultType = {
  SUCCESS_DATA:  'SUCCESS_DATA',
  SUCCESS_EMPTY: 'SUCCESS_EMPTY',
  FAILURE:       'FAILURE',
};

/**
 * Create a success result with data.
 */
export function successData(data, metadata = {}) {
  return {
    type: FetchResultType.SUCCESS_DATA,
    success: true,
    hasData: true,
    data,
    metadata,
    error: null,
  };
}

/**
 * Create a success result with legitimately empty data.
 * (API responded successfully but no records in the requested range.)
 */
export function successEmpty(metadata = {}) {
  return {
    type: FetchResultType.SUCCESS_EMPTY,
    success: true,
    hasData: false,
    data: null,
    metadata,
    error: null,
  };
}

/**
 * Create a failure result.
 *
 * FIX #5: The original error object is now preserved in errorInfo.cause so
 * that the full stack trace is available for debugging. Previously only
 * message/code/status were retained and the stack was silently discarded,
 * making it impossible to trace where in the call graph the failure occurred.
 *
 * FIX #4: An `attempts` field is always included in metadata (defaulting to
 * null when unknown) so assertSuccess can always render a meaningful message
 * rather than falling back to "after ? attempts".
 *
 * @param {Error|*}  error
 * @param {boolean}  retryable
 * @param {object}   metadata
 */
export function failure(error, retryable = true, metadata = {}) {
  const errorInfo = {
    message:  error?.message  || String(error),
    code:     error?.code     || null,
    status:   error?.response?.status || null,
    stack:    error?.stack    || null,  // FIX #5: preserve stack trace
    retryable,
    // FIX #5: preserve original error for instanceof checks and full context
    cause:    error instanceof Error ? error : null,
  };

  return {
    type:     FetchResultType.FAILURE,
    success:  false,
    hasData:  false,
    data:     null,
    // FIX #4: always include attempts in metadata so assertSuccess message is accurate
    metadata: { attempts: null, ...metadata },
    error:    errorInfo,
  };
}

/**
 * Classify an error as 'retryable', 'permanent', or 'unknown'.
 *
 * FIX #7: Replaces the two separate boolean predicates isRetryableError /
 * isPermanentError. Those functions left a gap — errors that were neither
 * (e.g. HTTP 405, 408, 409) fell through both checks silently, so callers
 * using if/else chains would hit neither branch.
 *
 * A single classification function with an exhaustive return type makes the
 * gap visible. 'unknown' is returned for errors that don't match any known
 * pattern; callers should treat 'unknown' conservatively (i.e. as retryable
 * unless the context demands otherwise).
 *
 * @param {Error|*} error
 * @returns {'retryable'|'permanent'|'unknown'}
 */
export function classifyError(error) {
  const status = error?.response?.status;
  const code   = error?.code ? String(error.code).toUpperCase() : null;
  const msg    = error?.message || '';

  // Permanent HTTP status codes — do not retry
  if (Number.isFinite(status) && PERMANENT_STATUSES.has(status)) return 'permanent';

  // Retryable HTTP status codes
  if (Number.isFinite(status) && RETRYABLE_STATUSES.has(status)) return 'retryable';

  // Retryable network error codes (FIX #1: O(1) Set lookup)
  if (code && RETRYABLE_CODES.has(code)) return 'retryable';

  // Retryable by message pattern (SSL/TLS, socket, network)
  if (/socket disconnected|ECONNRESET|ssl3_get_record|wrong version number/i.test(msg)) return 'retryable';

  // Nothing matched — unknown classification
  return 'unknown';
}

/**
 * Check if an error is retryable (transient network/server issues).
 *
 * FIX #7: Now delegates to classifyError. 'unknown' errors are treated as
 * retryable by default (conservative — better to retry than to silently drop).
 * Call classifyError() directly when you need to distinguish 'unknown'.
 *
 * FIX #1: Delegates to module-level Set — no per-call array allocation.
 */
export function isRetryableError(error) {
  return classifyError(error) !== 'permanent';
}

/**
 * Check if an error is a permanent failure (should not retry).
 *
 * FIX #7: Now delegates to classifyError.
 */
export function isPermanentError(error) {
  return classifyError(error) === 'permanent';
}

/**
 * Wrap an async fetch function with explicit result typing.
 *
 * Single-attempt wrapper — does NOT retry. Use retryFetch() when retries
 * are needed.
 *
 * FIX #3: Docstring now explicitly states this is a single-attempt wrapper.
 * The `retryable` flag in the returned FAILURE result is informational only —
 * wrapFetch itself will not act on it. Callers that want automatic retries
 * should use retryFetch() instead.
 *
 * @param {Function} fetchFn   - Async function that performs the fetch
 * @param {object}   options
 * @param {Function} options.emptyCheck - Returns true if data should be treated as empty
 * @returns {Promise<FetchResult>} Always resolves — never throws
 */
export async function wrapFetch(fetchFn, options = {}) {
  const { emptyCheck = (data) => !data || (Array.isArray(data) && data.length === 0) } = options;

  try {
    const data = await fetchFn();
    if (emptyCheck(data)) return successEmpty({ source: 'api', attempts: 1 });
    return successData(data, { source: 'api', attempts: 1 });
  } catch (error) {
    // FIX #3: retryable flag is informational — wrapFetch does not retry.
    // FIX #4: include attempts: 1 so assertSuccess message is accurate.
    return failure(error, isRetryableError(error), { source: 'api', attempts: 1 });
  }
}

/**
 * Retry a fetch with exponential backoff, returning a FetchResult.
 *
 * CRITICAL DIFFERENCE from old retry logic:
 * - Returns FAILURE result instead of throwing on max retries
 * - Caller MUST handle failure explicitly
 * - Never silently continues on error
 *
 * @param {Function} fetchFn
 * @param {object}   options
 * @param {number}   options.maxRetries
 * @param {number}   options.baseDelay    ms
 * @param {number}   options.maxDelay     ms
 * @param {Function} options.onRetry      (attempt, maxRetries, delayMs, error) => void
 * @param {Function} options.emptyCheck
 * @returns {Promise<FetchResult>} Always resolves — never throws
 */
export async function retryFetch(fetchFn, options = {}) {
  const {
    maxRetries  = 5,
    baseDelay   = 1000,
    maxDelay    = 30000,
    onRetry     = null,
    emptyCheck  = (data) => !data || (Array.isArray(data) && data.length === 0),
  } = options;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const data = await fetchFn();

      if (emptyCheck(data)) return successEmpty({ attempts: attempt + 1 });
      return successData(data, { attempts: attempt + 1 });

    } catch (error) {
      const classification = classifyError(error);  // FIX #7: use unified classifier

      // Permanent error — fail immediately, no retry budget spent
      if (classification === 'permanent') {
        return failure(error, false, { attempts: attempt + 1, permanent: true });
      }

      // Exhausted retries
      if (attempt === maxRetries) {
        return failure(error, true, { attempts: attempt + 1, exhausted: true });
      }

      // FIX #7: 'unknown' classification treated as retryable (conservative)
      // Log so operators can see unrecognized error shapes
      if (classification === 'unknown') {
        console.warn(
          `[fetch-result] retryFetch: unrecognized error classification ` +
          `(status=${error?.response?.status ?? 'n/a'}, code=${error?.code ?? 'n/a'}) ` +
          `— treating as retryable. Add to RETRYABLE_CODES or PERMANENT_STATUSES if this is expected.`
        );
      }

      const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
      const delay = Math.round(exponentialDelay + Math.random() * exponentialDelay * 0.3);

      if (onRetry) onRetry(attempt + 1, maxRetries, delay, error);

      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  // FIX #2: This line is now truly unreachable — the loop always returns on
  // the final attempt via the exhausted-retries branch above. Removed the
  // dead fallback return that previously implied there was a path to reach here.
  //
  // Note: we keep the loop structure as-is (attempt <= maxRetries) so the
  // final attempt at attempt===maxRetries still executes fetchFn once before
  // the exhausted check fires in the catch block.
}

/**
 * Assert that a fetch result is successful, throwing if not.
 * Use this when failure should abort the entire run.
 *
 * FIX #4: Error message now always shows a real attempt count because
 * failure() guarantees metadata.attempts is present (null if unknown).
 *
 * FIX #5: The reconstructed error includes the original stack via `cause`
 * (Node 16+ standard) so the full call chain is visible in stack traces.
 *
 * @param {FetchResult} result
 * @param {string}      context  - Label for error message prefix
 * @returns {FetchResult} The original result if successful
 * @throws {Error} If result is FAILURE
 */
export function assertSuccess(result, context = '') {
  if (result.type === FetchResultType.FAILURE) {
    const prefix = context ? `[${context}] ` : '';
    // FIX #4: attempts is always present in metadata (may be null for unknown)
    const attemptStr = result.metadata.attempts != null ? result.metadata.attempts : '?';
    const error = new Error(
      `${prefix}Fetch failed after ${attemptStr} attempt(s): ${result.error.message}`
    );
    error.fetchResult = result;
    error.code        = result.error.code;
    error.status      = result.error.status;
    // FIX #5: chain original error as cause for full stack trace preservation
    if (result.error.cause) error.cause = result.error.cause;
    throw error;
  }
  return result;
}

/**
 * Process a batch of fetch results, throwing if any failed.
 *
 * FIX #6: Failures are now partitioned into retryable vs permanent buckets.
 * Previously a single permanent 404 was indistinguishable from a batch of
 * transient 503s. Callers can now decide whether to retry the batch (only
 * retryable failures), abort (any permanent failure), or handle each bucket
 * differently.
 *
 * @param {FetchResult[]} results
 * @param {string}        context
 * @returns {{ successful: FetchResult[], empty: FetchResult[] }}
 * @throws {Error} with .retryableFailures and .permanentFailures attached
 */
export function validateBatchResults(results, context = '') {
  const failures          = results.filter(r => r.type === FetchResultType.FAILURE);
  // FIX #6: Partition by retryability so callers can act on each bucket
  const retryableFailures = failures.filter(f => f.error.retryable);
  const permanentFailures = failures.filter(f => !f.error.retryable);

  if (failures.length > 0) {
    const prefix = context ? `[${context}] ` : '';
    const errorMessages = failures.map(f => f.error.message).join('; ');
    const error = new Error(
      `${prefix}${failures.length}/${results.length} fetches failed ` +
      `(${retryableFailures.length} retryable, ${permanentFailures.length} permanent): ${errorMessages}`
    );
    // FIX #6: attach partitioned failure lists for caller inspection
    error.failures          = failures;          // all failures (backward compat)
    error.retryableFailures = retryableFailures;  // can be retried
    error.permanentFailures = permanentFailures;  // should not retry
    throw error;
  }

  return {
    successful: results.filter(r => r.type === FetchResultType.SUCCESS_DATA),
    empty:      results.filter(r => r.type === FetchResultType.SUCCESS_EMPTY),
  };
}

export default {
  FetchResultType,
  RETRYABLE_CODES,
  successData,
  successEmpty,
  failure,
  classifyError,
  isRetryableError,
  isPermanentError,
  wrapFetch,
  retryFetch,
  assertSuccess,
  validateBatchResults,
};
