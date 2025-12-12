/**
 * Simple in-memory cache with TTL for expensive aggregations
 * This prevents re-scanning ACS files on every API call
 */

const cache = new Map();
const DEFAULT_TTL = 5 * 60 * 1000; // 5 minutes

/**
 * Get cached value if exists and not expired
 */
export function getCached(key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    cache.delete(key);
    return null;
  }
  return entry.data;
}

/**
 * Set cache with optional TTL
 */
export function setCache(key, data, ttlMs = DEFAULT_TTL) {
  cache.set(key, { 
    data, 
    expiresAt: Date.now() + ttlMs,
    cachedAt: Date.now()
  });
}

/**
 * Invalidate all cache entries with a given prefix
 */
export function invalidateCache(prefix = '') {
  if (!prefix) {
    cache.clear();
    console.log('🗑️ Cache cleared completely');
    return;
  }
  
  let count = 0;
  for (const key of cache.keys()) {
    if (key.startsWith(prefix)) {
      cache.delete(key);
      count++;
    }
  }
  console.log(`🗑️ Invalidated ${count} cache entries with prefix "${prefix}"`);
}

/**
 * Get cache stats for debugging
 */
export function getCacheStats() {
  const now = Date.now();
  const entries = [];
  
  for (const [key, entry] of cache.entries()) {
    const isExpired = now > entry.expiresAt;
    entries.push({
      key,
      isExpired,
      ttlRemaining: isExpired ? 0 : Math.round((entry.expiresAt - now) / 1000),
      ageSeconds: Math.round((now - entry.cachedAt) / 1000),
    });
  }
  
  return {
    totalEntries: cache.size,
    entries,
  };
}

export default { getCached, setCache, invalidateCache, getCacheStats };
