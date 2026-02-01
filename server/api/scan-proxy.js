import { Router } from 'express';
import { 
  fetchWithFailover, 
  getCurrentEndpoint, 
  getHealthStats,
  setEndpointByName,
  checkAllEndpoints,
} from '../lib/endpoint-rotation.js';

const router = Router();

/**
 * Scan API Proxy with Automatic Endpoint Rotation
 * 
 * All frontend Scan API calls go through this proxy to:
 * 1. Avoid CORS errors (browser → Scan API blocked)
 * 2. Avoid rate limiting (distributed across multiple frontend clients)
 * 3. Automatically failover to healthy endpoints
 * 
 * Rule: Browser → our API → Scan API (never browser → Scan directly)
 */

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

// Generic proxy handler for GET requests (no body, query params only)
router.get('/*', async (req, res) => {
  try {
    const path = req.params[0] || '';
    const queryString = new URLSearchParams(req.query).toString();
    const fullPath = queryString ? `${path}?${queryString}` : path;

    const endpoint = getCurrentEndpoint();
    const scanUrl = `${endpoint.url}/${fullPath}`;
    
    console.log(`[Scan Proxy] GET ${scanUrl}`);

    const scanRes = await fetch(scanUrl, {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
    });
    
    console.log(`[Scan Proxy] GET response: ${scanRes.status}`);

    const text = await scanRes.text();
    
    res.set('X-Scan-Endpoint', endpoint.name);
    res.status(scanRes.status).send(text);
  } catch (err) {
    console.error('[Scan Proxy] GET error:', err.message);
    res.status(500).json({ 
      error: err.message,
      endpoint: getCurrentEndpoint().name,
    });
  }
});

// Generic proxy handler for POST requests (with JSON body)
router.post('/*', async (req, res) => {
  // Skip internal endpoints (handled above)
  if (req.params[0]?.startsWith('_')) {
    return res.status(404).json({ error: 'Not found' });
  }
  
  try {
    const path = req.params[0] || '';
    const endpoint = getCurrentEndpoint();
    const scanUrl = `${endpoint.url}/${path}`;
    
    console.log(`[Scan Proxy] POST ${scanUrl}`);

    const scanRes = await fetch(scanUrl, {
      method: 'POST',
      headers: { 
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify(req.body),
    });
    
    console.log(`[Scan Proxy] POST response: ${scanRes.status}`);

    const text = await scanRes.text();

    res.set('X-Scan-Endpoint', endpoint.name);
    res.status(scanRes.status).send(text);
  } catch (err) {
    console.error('[Scan Proxy] POST error:', err.message);
    res.status(500).json({ 
      error: err.message,
      endpoint: getCurrentEndpoint().name,
    });
  }
});

export default router;
