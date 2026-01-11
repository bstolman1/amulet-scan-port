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

export const FetchResultType = {
  SUCCESS_DATA: 'SUCCESS_DATA',
  SUCCESS_EMPTY: 'SUCCESS_EMPTY',
  FAILURE: 'FAILURE',
};

/**
 * Create a success result with data
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
 * Create a success result with legitimately empty data
 * (API responded successfully but no records in the requested range)
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
 * Create a failure result
 * MUST be handled - cannot be silently ignored
 */
export function failure(error, retryable = true, metadata = {}) {
  const errorInfo = {
    message: error.message || String(error),
    code: error.code || null,
    status: error.response?.status || null,
    retryable,
  };
  
  return {
    type: FetchResultType.FAILURE,
    success: false,
    hasData: false,
    data: null,
    metadata,
    error: errorInfo,
  };
}

/**
 * Check if an error is retryable (transient network/server issues)
 */
export function isRetryableError(error) {
  const retryableCodes = ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN'];
  const retryableStatuses = [429, 500, 502, 503, 504];
  
  if (error.code && retryableCodes.includes(error.code)) return true;
  if (error.response?.status && retryableStatuses.includes(error.response.status)) return true;
  
  // Network socket disconnected
  if (error.message?.includes('socket disconnected')) return true;
  if (error.message?.includes('ECONNRESET')) return true;
  
  return false;
}

/**
 * Check if an error is a permanent failure (should not retry)
 */
export function isPermanentError(error) {
  const permanentStatuses = [400, 401, 403, 404, 422];
  
  if (error.response?.status && permanentStatuses.includes(error.response.status)) return true;
  
  return false;
}

/**
 * Wrap an async fetch function with explicit result typing
 * Returns FetchResult instead of throwing
 */
export async function wrapFetch(fetchFn, options = {}) {
  const { emptyCheck = (data) => !data || (Array.isArray(data) && data.length === 0) } = options;
  
  try {
    const data = await fetchFn();
    
    if (emptyCheck(data)) {
      return successEmpty({ source: 'api' });
    }
    
    return successData(data, { source: 'api' });
  } catch (error) {
    return failure(error, isRetryableError(error), { source: 'api' });
  }
}

/**
 * Retry a fetch with exponential backoff, returning FetchResult
 * 
 * CRITICAL DIFFERENCE from old retry logic:
 * - Returns FAILURE result instead of throwing on max retries
 * - Caller MUST handle failure explicitly
 * - Never silently continues on error
 */
export async function retryFetch(fetchFn, options = {}) {
  const {
    maxRetries = 5,
    baseDelay = 1000,
    maxDelay = 30000,
    onRetry = null,
    emptyCheck = (data) => !data || (Array.isArray(data) && data.length === 0),
  } = options;

  let lastError = null;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const data = await fetchFn();
      
      // Success - check if empty or has data
      if (emptyCheck(data)) {
        return successEmpty({ attempts: attempt + 1 });
      }
      
      return successData(data, { attempts: attempt + 1 });
    } catch (error) {
      lastError = error;
      
      // Check if error is retryable
      if (!isRetryableError(error)) {
        // Permanent error - fail immediately
        return failure(error, false, { attempts: attempt + 1, permanent: true });
      }
      
      // Check if we've exhausted retries
      if (attempt === maxRetries) {
        return failure(error, true, { attempts: attempt + 1, exhausted: true });
      }
      
      // Calculate backoff delay
      const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
      const jitter = Math.random() * exponentialDelay * 0.3;
      const delay = Math.round(exponentialDelay + jitter);
      
      // Notify caller of retry
      if (onRetry) {
        onRetry(attempt + 1, maxRetries, delay, error);
      }
      
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  // Should never reach here, but just in case
  return failure(lastError || new Error('Unknown error'), true, { exhausted: true });
}

/**
 * Assert that a fetch result is successful, throwing if not
 * Use this when failure should abort the entire run
 */
export function assertSuccess(result, context = '') {
  if (result.type === FetchResultType.FAILURE) {
    const prefix = context ? `[${context}] ` : '';
    const error = new Error(
      `${prefix}Fetch failed after ${result.metadata.attempts || '?'} attempts: ${result.error.message}`
    );
    error.fetchResult = result;
    error.code = result.error.code;
    error.status = result.error.status;
    throw error;
  }
  return result;
}

/**
 * Process a batch of fetch results, failing if any failed
 */
export function validateBatchResults(results, context = '') {
  const failures = results.filter(r => r.type === FetchResultType.FAILURE);
  
  if (failures.length > 0) {
    const prefix = context ? `[${context}] ` : '';
    const errorMessages = failures.map(f => f.error.message).join('; ');
    const error = new Error(
      `${prefix}${failures.length}/${results.length} fetches failed: ${errorMessages}`
    );
    error.failures = failures;
    throw error;
  }
  
  return {
    successful: results.filter(r => r.type === FetchResultType.SUCCESS_DATA),
    empty: results.filter(r => r.type === FetchResultType.SUCCESS_EMPTY),
  };
}

export default {
  FetchResultType,
  successData,
  successEmpty,
  failure,
  isRetryableError,
  isPermanentError,
  wrapFetch,
  retryFetch,
  assertSuccess,
  validateBatchResults,
};
