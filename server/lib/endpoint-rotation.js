/**
 * Scan API Endpoint Rotation with Automatic Failover
 * 
 * Maintains a list of Canton Scan API endpoints and automatically
 * rotates to a healthy endpoint when the current one fails.
 */

// All available Canton Scan API endpoints (extracted from SV info)
const SCAN_ENDPOINTS = [
  { name: 'Global-Synchronizer-Foundation', url: 'https://scan.sv-1.global.canton.network.sync.global/api/scan' },
  { name: 'Digital-Asset-1', url: 'https://scan.sv-1.global.canton.network.digitalasset.com/api/scan' },
  { name: 'Digital-Asset-2', url: 'https://scan.sv-2.global.canton.network.digitalasset.com/api/scan' },
  { name: 'Cumberland-1', url: 'https://scan.sv-1.global.canton.network.cumberland.io/api/scan' },
  { name: 'Cumberland-2', url: 'https://scan.sv-2.global.canton.network.cumberland.io/api/scan' },
  { name: 'Five-North-1', url: 'https://scan.sv-1.global.canton.network.fivenorth.io/api/scan' },
  { name: 'Tradeweb-Markets-1', url: 'https://scan.sv-1.global.canton.network.tradeweb.com/api/scan' },
  { name: 'Proof-Group-1', url: 'https://scan.sv-1.global.canton.network.proofgroup.xyz/api/scan' },
  { name: 'Liberty-City-Ventures-1', url: 'https://scan.sv-1.global.canton.network.lcv.mpch.io/api/scan' },
  { name: 'MPC-Holding-Inc', url: 'https://scan.sv-1.global.canton.network.mpch.io/api/scan' },
  { name: 'Orb-1-LP-1', url: 'https://scan.sv-1.global.canton.network.orb1lp.mpch.io/api/scan' },
  { name: 'SV-Nodeops-Limited', url: 'https://scan.sv.global.canton.network.sv-nodeops.com/api/scan' },
  { name: 'C7-Technology-Services-Limited', url: 'https://scan.sv-1.global.canton.network.c7.digital/api/scan' },
];

// Endpoint health tracking
const endpointHealth = new Map();
const HEALTH_CHECK_INTERVAL = 60000; // 1 minute
const FAILURE_THRESHOLD = 3; // Mark unhealthy after 3 consecutive failures
const RECOVERY_INTERVAL = 300000; // Try unhealthy endpoints every 5 minutes

let currentEndpointIndex = 0;
let lastHealthCheck = 0;

/**
 * Initialize health tracking for all endpoints
 */
function initHealthTracking() {
  for (const endpoint of SCAN_ENDPOINTS) {
    if (!endpointHealth.has(endpoint.url)) {
      endpointHealth.set(endpoint.url, {
        healthy: true,
        consecutiveFailures: 0,
        lastFailure: null,
        lastSuccess: null,
        totalRequests: 0,
        totalFailures: 0,
      });
    }
  }
}

initHealthTracking();

/**
 * Get the current active endpoint
 */
export function getCurrentEndpoint() {
  return SCAN_ENDPOINTS[currentEndpointIndex];
}

/**
 * Get all endpoints with their health status
 */
export function getAllEndpoints() {
  return SCAN_ENDPOINTS.map(endpoint => ({
    ...endpoint,
    health: endpointHealth.get(endpoint.url) || { healthy: true },
  }));
}

/**
 * Record a successful request to an endpoint
 */
export function recordSuccess(url) {
  const health = endpointHealth.get(url);
  if (health) {
    health.consecutiveFailures = 0;
    health.healthy = true;
    health.lastSuccess = Date.now();
    health.totalRequests++;
  }
}

/**
 * Record a failed request to an endpoint
 */
export function recordFailure(url, error) {
  const health = endpointHealth.get(url);
  if (health) {
    health.consecutiveFailures++;
    health.lastFailure = Date.now();
    health.totalRequests++;
    health.totalFailures++;
    
    if (health.consecutiveFailures >= FAILURE_THRESHOLD) {
      health.healthy = false;
      console.warn(`[Endpoint Rotation] Marking ${url} as unhealthy after ${health.consecutiveFailures} failures`);
    }
  }
}

/**
 * Find the next healthy endpoint, rotating through all options
 */
export function rotateToNextHealthy() {
  const startIndex = currentEndpointIndex;
  let attempts = 0;
  
  do {
    currentEndpointIndex = (currentEndpointIndex + 1) % SCAN_ENDPOINTS.length;
    const endpoint = SCAN_ENDPOINTS[currentEndpointIndex];
    const health = endpointHealth.get(endpoint.url);
    
    // Check if this endpoint is healthy or if enough time has passed to retry
    const shouldRetry = health && !health.healthy && 
      health.lastFailure && (Date.now() - health.lastFailure > RECOVERY_INTERVAL);
    
    if (health?.healthy || shouldRetry) {
      if (shouldRetry) {
        console.log(`[Endpoint Rotation] Retrying previously unhealthy endpoint: ${endpoint.name}`);
        health.healthy = true; // Give it another chance
        health.consecutiveFailures = 0;
      }
      console.log(`[Endpoint Rotation] Rotated to: ${endpoint.name}`);
      return endpoint;
    }
    
    attempts++;
  } while (currentEndpointIndex !== startIndex && attempts < SCAN_ENDPOINTS.length);
  
  // If all endpoints are unhealthy, reset all and try the first one
  console.warn('[Endpoint Rotation] All endpoints appear unhealthy, resetting health status');
  for (const endpoint of SCAN_ENDPOINTS) {
    const health = endpointHealth.get(endpoint.url);
    if (health) {
      health.healthy = true;
      health.consecutiveFailures = 0;
    }
  }
  
  currentEndpointIndex = 0;
  return SCAN_ENDPOINTS[0];
}

/**
 * Make a request with automatic failover
 * Tries the current endpoint, then rotates on failure
 */
export async function fetchWithFailover(path, options = {}) {
  const maxRetries = Math.min(3, SCAN_ENDPOINTS.length);
  let lastError = null;
  
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const endpoint = getCurrentEndpoint();
    const url = `${endpoint.url}/${path}`;
    
    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          ...options.headers,
        },
        signal: options.signal || AbortSignal.timeout(30000),
      });
      
      if (response.ok) {
        recordSuccess(endpoint.url);
        return response;
      }
      
      // Server returned an error status
      if (response.status >= 500 || response.status === 429) {
        // Server error or rate limit - rotate endpoint
        console.warn(`[Endpoint Rotation] ${endpoint.name} returned ${response.status}, rotating...`);
        recordFailure(endpoint.url, new Error(`HTTP ${response.status}`));
        rotateToNextHealthy();
        lastError = new Error(`HTTP ${response.status}`);
        continue;
      }
      
      // Client error (4xx except 429) - don't rotate, return as-is
      recordSuccess(endpoint.url); // The endpoint works, just bad request
      return response;
      
    } catch (err) {
      console.error(`[Endpoint Rotation] ${endpoint.name} failed:`, err.message);
      recordFailure(endpoint.url, err);
      rotateToNextHealthy();
      lastError = err;
    }
  }
  
  // All retries exhausted
  throw lastError || new Error('All endpoints failed');
}

/**
 * Get endpoint health statistics
 */
export function getHealthStats() {
  const stats = {
    current: getCurrentEndpoint().name,
    endpoints: [],
  };
  
  for (const endpoint of SCAN_ENDPOINTS) {
    const health = endpointHealth.get(endpoint.url);
    stats.endpoints.push({
      name: endpoint.name,
      healthy: health?.healthy ?? true,
      consecutiveFailures: health?.consecutiveFailures ?? 0,
      totalRequests: health?.totalRequests ?? 0,
      totalFailures: health?.totalFailures ?? 0,
      lastSuccess: health?.lastSuccess,
      lastFailure: health?.lastFailure,
    });
  }
  
  return stats;
}

/**
 * Manually set the active endpoint by name
 */
export function setEndpointByName(name) {
  const index = SCAN_ENDPOINTS.findIndex(e => e.name === name);
  if (index !== -1) {
    currentEndpointIndex = index;
    console.log(`[Endpoint Rotation] Manually set to: ${name}`);
    return true;
  }
  return false;
}

/**
 * Perform health check on all endpoints (background task)
 */
export async function checkAllEndpoints() {
  console.log('[Endpoint Rotation] Starting health check of all endpoints...');
  
  const results = await Promise.allSettled(
    SCAN_ENDPOINTS.map(async (endpoint) => {
      try {
        const response = await fetch(`${endpoint.url}/v0/dso`, {
          method: 'GET',
          headers: { 'Accept': 'application/json' },
          signal: AbortSignal.timeout(10000),
        });
        
        if (response.ok) {
          recordSuccess(endpoint.url);
          return { name: endpoint.name, healthy: true };
        } else {
          recordFailure(endpoint.url, new Error(`HTTP ${response.status}`));
          return { name: endpoint.name, healthy: false, status: response.status };
        }
      } catch (err) {
        recordFailure(endpoint.url, err);
        return { name: endpoint.name, healthy: false, error: err.message };
      }
    })
  );
  
  const healthy = results.filter(r => r.status === 'fulfilled' && r.value.healthy).length;
  console.log(`[Endpoint Rotation] Health check complete: ${healthy}/${SCAN_ENDPOINTS.length} healthy`);
  
  lastHealthCheck = Date.now();
  return results.map(r => r.status === 'fulfilled' ? r.value : { healthy: false, error: 'check failed' });
}

export default {
  getCurrentEndpoint,
  getAllEndpoints,
  recordSuccess,
  recordFailure,
  rotateToNextHealthy,
  fetchWithFailover,
  getHealthStats,
  setEndpointByName,
  checkAllEndpoints,
};
