import { Router } from 'express';
import https from 'https';
import { 
  getCurrentEndpoint, 
  getHealthStats,
  setEndpointByName,
  checkAllEndpoints,
  recordSuccess,
  recordFailure,
  rotateToNextHealthy,
} from '../lib/endpoint-rotation.js';

const router = Router();

/**
 * Scan API Proxy - Transparent Method-Preserving Relay
 * 
 * Routes: /api/scan-proxy/* → Scan API /api/scan/*
 * 
 * Rules:
 * 1. Preserve HTTP method (GET vs POST)
 * 2. GET requests: no body, query params only
 * 3. POST requests: forward JSON body unchanged
 * 4. CRITICAL: Override Host header for SCAN's host-based routing
 */

/**
 * Extract hostname from a full URL for the Host header
 */
function extractHostname(url) {
  try {
    return new URL(url).hostname;
  } catch {
    return undefined;
  }
}

/**
 * Create an HTTPS agent with proper SNI servername for the target host.
 * This ensures TLS handshake uses the correct hostname.
 */
function createAgent(hostname) {
  return new https.Agent({
    servername: hostname,
    keepAlive: true,
  });
}

// Health/status endpoint for monitoring
router.get('/_health', (req, res) => {
  const stats = getHealthStats();
  res.json({
    status: 'ok',
    currentEndpoint: stats.current,
    endpoints: stats.endpoints,
  });
});

// Manually trigger health check of all endpoints
router.post('/_health/check', async (req, res) => {
  try {
    const results = await checkAllEndpoints();
    res.json({ results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Manually set active endpoint
router.post('/_endpoint', (req, res) => {
  const { name } = req.body;
  if (!name) {
    return res.status(400).json({ error: 'name is required' });
  }
  
  const success = setEndpointByName(name);
  if (success) {
    res.json({ success: true, current: getCurrentEndpoint().name });
  } else {
    res.status(404).json({ error: 'Endpoint not found' });
  }
});

/**
 * Generic proxy handler - preserves HTTP method
 */
async function proxyRequest(req, res, method) {
  const path = req.params[0] || '';
  
  // Skip internal endpoints
  if (path.startsWith('_')) {
    return res.status(404).json({ error: 'Not found' });
  }

  const maxRetries = 3;
  let lastError = null;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const endpoint = getCurrentEndpoint();
    
    // Build full URL: endpoint.url already ends with /api/scan
    const queryString = new URLSearchParams(req.query).toString();
    const scanUrl = queryString 
      ? `${endpoint.url}/${path}?${queryString}`
      : `${endpoint.url}/${path}`;
    
    const hostname = extractHostname(endpoint.url);
    
    console.log(`[Scan Proxy] ${method} ${scanUrl} (Host: ${hostname})`);

    try {
      // Create agent with correct TLS SNI servername
      const agent = hostname ? createAgent(hostname) : undefined;
      
      const fetchOptions = {
        method,
        headers: {
          'Accept': 'application/json',
          // CRITICAL: Override Host header - SCAN uses host-based routing
          'Host': hostname,
        },
        signal: AbortSignal.timeout(30000),
      };

      // Add the agent for proper TLS SNI
      if (agent) {
        fetchOptions.agent = agent;
      }

      // Only add body for POST/PUT/PATCH requests
      if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
        fetchOptions.headers['Content-Type'] = 'application/json';
        fetchOptions.body = JSON.stringify(req.body);
      }

      const scanRes = await fetch(scanUrl, fetchOptions);
      
      console.log(`[Scan Proxy] ${method} response: ${scanRes.status}`);

      // Check for server errors that warrant rotation
      if (scanRes.status >= 500 || scanRes.status === 429) {
        recordFailure(endpoint.url, new Error(`HTTP ${scanRes.status}`));
        rotateToNextHealthy();
        lastError = new Error(`Scan API returned ${scanRes.status}`);
        continue;
      }

      // Success or client error - return as-is
      recordSuccess(endpoint.url);
      
      const text = await scanRes.text();
      
      res.set('X-Scan-Endpoint', endpoint.name);
      res.status(scanRes.status).send(text);
      return;

    } catch (err) {
      console.error(`[Scan Proxy] ${method} error (${endpoint.name}):`, err.message);
      recordFailure(endpoint.url, err);
      rotateToNextHealthy();
      lastError = err;
    }
  }

  // All retries exhausted
  res.status(502).json({ 
    error: lastError?.message || 'All Scan API endpoints failed',
    endpoint: getCurrentEndpoint().name,
  });
}

// GET requests - no body, query params preserved
router.get('/*', (req, res) => proxyRequest(req, res, 'GET'));

// POST requests - JSON body forwarded
router.post('/*', (req, res) => {
  // Skip internal routes (already handled above)
  if (req.params[0]?.startsWith('_')) {
    return res.status(404).json({ error: 'Not found' });
  }
  return proxyRequest(req, res, 'POST');
});

export default router;
