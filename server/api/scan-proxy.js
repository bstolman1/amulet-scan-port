import { Router } from 'express';

const router = Router();

// Canton Scan API base URL
const SCAN_API_URL = process.env.SCAN_URL || 'https://scan.sv-1.global.canton.network.sync.global/api/scan';

/**
 * Scan API Proxy
 * 
 * All frontend Scan API calls go through this proxy to avoid:
 * 1. CORS errors (browser → Scan API blocked)
 * 2. Rate limiting (distributed across multiple frontend clients)
 * 
 * Rule: Browser → our API → Scan API (never browser → Scan directly)
 */

// Generic proxy handler for GET requests
router.get('/*', async (req, res) => {
  try {
    const path = req.params[0] || '';
    const queryString = new URLSearchParams(req.query).toString();
    const url = queryString 
      ? `${SCAN_API_URL}/${path}?${queryString}`
      : `${SCAN_API_URL}/${path}`;

    console.log(`[Scan Proxy] GET ${path}`);

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      signal: AbortSignal.timeout(30000),
    });

    if (!response.ok) {
      console.error(`[Scan Proxy] Error: ${response.status} for ${path}`);
      return res.status(response.status).json({ 
        error: `Scan API error: ${response.status}`,
        path,
      });
    }

    const data = await response.json();
    res.json(data);
  } catch (err) {
    console.error('[Scan Proxy] GET error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Generic proxy handler for POST requests
router.post('/*', async (req, res) => {
  try {
    const path = req.params[0] || '';
    const url = `${SCAN_API_URL}/${path}`;

    console.log(`[Scan Proxy] POST ${path}`);

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      body: JSON.stringify(req.body),
      signal: AbortSignal.timeout(30000),
    });

    if (!response.ok) {
      console.error(`[Scan Proxy] Error: ${response.status} for ${path}`);
      return res.status(response.status).json({ 
        error: `Scan API error: ${response.status}`,
        path,
      });
    }

    const data = await response.json();
    res.json(data);
  } catch (err) {
    console.error('[Scan Proxy] POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

export default router;
