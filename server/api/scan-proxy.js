import { Router } from 'express';
import { extractHostname, createDispatcher } from '../lib/undici-dispatcher.js';
import { 
  getCurrentEndpoint, 
  getAllEndpoints,
  getHealthStats,
  setEndpointByName,
  checkAllEndpoints,
  recordSuccess,
  recordFailure,
  rotateToNextHealthy,
} from '../lib/endpoint-rotation.js';

const router = Router();

// FIX: Cap the maximum bytes we will buffer from a proxied Scan API response.
// Without a limit, a misbehaving or compromised upstream can send an arbitrarily
// large body, exhausting server memory. 50 MB is generous for any known Scan API
// response but prevents runaway memory growth.
const MAX_PROXY_RESPONSE_BYTES = 50 * 1024 * 1024; // 50 MB

/**
 * Scan API Proxy - Transparent Method-Preserving Relay
 * 
 * Routes: /api/scan-proxy/* → Scan API /api/scan/*
 * 
 * Rules:
 * 1. Preserve HTTP method (GET vs POST)
 * 2. GET requests: no body, query params only
 * 3. POST requests: forward JSON body unchanged
 * 
 * NOTE: Node 18 fetch (Undici) handles TLS/Host correctly by default.
 * No manual Host override or https.Agent needed.
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

// All known SV nodes per environment (scan URLs only; mediator/sv URLs are derived)
const SV_NODES = {
  dev: [
    { name: 'C7-Technology-Services-Limited',    scanUrl: 'https://scan.sv-1.dev.global.canton.network.c7.digital' },
    { name: 'Cumberland-1',                       scanUrl: 'https://scan.sv-1.dev.global.canton.network.cumberland.io' },
    { name: 'Cumberland-2',                       scanUrl: 'https://scan.sv-2.dev.global.canton.network.cumberland.io' },
    { name: 'DA-Helm-Test-Node',                  scanUrl: 'https://scan.sv.dev.global.canton.network.digitalasset.com' },
    { name: 'Digital-Asset-1',                    scanUrl: 'https://scan.sv-1.dev.global.canton.network.digitalasset.com' },
    { name: 'Digital-Asset-2',                    scanUrl: 'https://scan.sv-2.dev.global.canton.network.digitalasset.com' },
    { name: 'Five-North-1',                       scanUrl: 'https://scan.sv-1.dev.global.canton.network.fivenorth.io' },
    { name: 'Global-Synchronizer-Foundation',     scanUrl: 'https://scan.sv-1.dev.global.canton.network.sync.global' },
    { name: 'Liberty-City-Ventures-1',            scanUrl: 'https://scan.sv-1.dev.global.canton.network.lcv.mpch.io' },
    { name: 'MPC-Holding-Inc',                    scanUrl: 'https://scan.sv-1.dev.global.canton.network.mpch.io' },
    { name: 'Orb-1-LP-1',                         scanUrl: 'https://scan.sv-1.dev.global.canton.network.orb1lp.mpch.io' },
    { name: 'Proof-Group-1',                      scanUrl: 'https://scan.sv-1.dev.global.canton.network.proofgroup.xyz' },
    { name: 'SV-Nodeops-Limited',                 scanUrl: 'https://scan.sv.dev.global.canton.network.sv-nodeops.com' },
    { name: 'Tradeweb-Markets-1',                 scanUrl: 'https://scan.sv-1.dev.global.canton.network.tradeweb.com' },
  ],
  test: [
    { name: 'C7-Technology-Services-Limited',    scanUrl: 'https://scan.sv-1.test.global.canton.network.c7.digital' },
    { name: 'Cumberland-1',                       scanUrl: 'https://scan.sv-1.test.global.canton.network.cumberland.io' },
    { name: 'Cumberland-2',                       scanUrl: 'https://scan.sv-2.test.global.canton.network.cumberland.io' },
    { name: 'Digital-Asset-1',                    scanUrl: 'https://scan.sv-1.test.global.canton.network.digitalasset.com' },
    { name: 'Digital-Asset-2',                    scanUrl: 'https://scan.sv-2.test.global.canton.network.digitalasset.com' },
    { name: 'Five-North-1',                       scanUrl: 'https://scan.sv-1.test.global.canton.network.fivenorth.io' },
    { name: 'Global-Synchronizer-Foundation',     scanUrl: 'https://scan.sv-1.test.global.canton.network.sync.global' },
    { name: 'Liberty-City-Ventures-1',            scanUrl: 'https://scan.sv-1.test.global.canton.network.lcv.mpch.io' },
    { name: 'MPC-Holding-Inc',                    scanUrl: 'https://scan.sv-1.test.global.canton.network.mpch.io' },
    { name: 'Orb-1-LP-1',                         scanUrl: 'https://scan.sv-1.test.global.canton.network.orb1lp.mpch.io' },
    { name: 'Proof-Group-1',                      scanUrl: 'https://scan.sv-1.test.global.canton.network.proofgroup.xyz' },
    { name: 'SV-Nodeops-Limited',                 scanUrl: 'https://scan.sv.test.global.canton.network.sv-nodeops.com' },
    { name: 'Tradeweb-Markets-1',                 scanUrl: 'https://scan.sv.test.global.canton.network.tradeweb.com' },
  ],
  main: [
    { name: 'C7-Technology-Services-Limited',    scanUrl: 'https://scan.sv-1.global.canton.network.c7.digital' },
    { name: 'Cumberland-1',                       scanUrl: 'https://scan.sv-1.global.canton.network.cumberland.io' },
    { name: 'Cumberland-2',                       scanUrl: 'https://scan.sv-2.global.canton.network.cumberland.io' },
    { name: 'Digital-Asset-1',                    scanUrl: 'https://scan.sv-1.global.canton.network.digitalasset.com' },
    { name: 'Digital-Asset-2',                    scanUrl: 'https://scan.sv-2.global.canton.network.digitalasset.com' },
    { name: 'Five-North-1',                       scanUrl: 'https://scan.sv-1.global.canton.network.fivenorth.io' },
    { name: 'Global-Synchronizer-Foundation',     scanUrl: 'https://scan.sv-1.global.canton.network.sync.global' },
    { name: 'Liberty-City-Ventures-1',            scanUrl: 'https://scan.sv-1.global.canton.network.lcv.mpch.io' },
    { name: 'MPC-Holding-Inc',                    scanUrl: 'https://scan.sv-1.global.canton.network.mpch.io' },
    { name: 'Orb-1-LP-1',                         scanUrl: 'https://scan.sv-1.global.canton.network.orb1lp.mpch.io' },
    { name: 'Proof-Group-1',                      scanUrl: 'https://scan.sv-1.global.canton.network.proofgroup.xyz' },
    { name: 'SV-Nodeops-Limited',                 scanUrl: 'https://scan.sv.global.canton.network.sv-nodeops.com' },
    { name: 'Tradeweb-Markets-1',                 scanUrl: 'https://scan.sv-1.global.canton.network.tradeweb.com' },
  ],
};

// Derive mediator/sv base URLs from a scan URL by replacing the leading 'scan' subdomain
function deriveServiceUrl(scanUrl, service) {
  return scanUrl.replace(/^https:\/\/scan\./, `https://${service}.`);
}

// Probe a single URL, returning { ok, version, latency }
async function probeUrl(url, versionPath) {
  const start = Date.now();
  try {
    const hostname = extractHostname(url);
    const dispatcher = hostname ? createDispatcher(hostname) : undefined;
    const fullUrl = `${url}${versionPath}`;
    const resp = await fetch(fullUrl, {
      method: 'GET',
      headers: { Accept: 'application/json', ...(hostname ? { Host: hostname } : {}) },
      ...(dispatcher ? { dispatcher } : {}),
      signal: AbortSignal.timeout(10000),
    });
    const latency = Date.now() - start;
    if (!resp.ok) return { ok: false, version: null, latency };
    let version = null;
    try {
      const text = await readBodyWithLimit(resp, 64 * 1024);
      const data = JSON.parse(text);
      version = data.version || null;
    } catch (_) { /* non-JSON health endpoints are fine */ }
    return { ok: true, version, latency };
  } catch (_) {
    return { ok: false, version: null, latency: Date.now() - start };
  }
}

// GET /_sv-node-status - Probe all SV nodes across dev/test/main for SCAN, MEDIATOR, SV health
router.get('/_sv-node-status', async (req, res) => {
  const envNames = ['dev', 'test', 'main'];
  console.log('[Scan Proxy] SV node status check across all environments');

  const envResults = await Promise.all(
    envNames.map(async (env) => {
      const nodes = SV_NODES[env];
      const nodeResults = await Promise.all(
        nodes.map(async (node) => {
          const mediatorUrl = deriveServiceUrl(node.scanUrl, 'mediator');
          const svUrl = deriveServiceUrl(node.scanUrl, 'sv');
          const [scan, mediator, sv] = await Promise.all([
            probeUrl(node.scanUrl, '/api/scan/version'),
            probeUrl(mediatorUrl, '/api/mediator/v0/health'),
            probeUrl(svUrl, '/api/sv/v0/health'),
          ]);
          return {
            name: node.name,
            scanUrl: node.scanUrl,
            scan:     { ok: scan.ok,     version: scan.version,     latency: scan.latency },
            mediator: { ok: mediator.ok, version: mediator.version, latency: mediator.latency },
            sv:       { ok: sv.ok,       version: sv.version,       latency: sv.latency },
          };
        })
      );
      return { env, nodes: nodeResults };
    })
  );

  console.log('[Scan Proxy] SV node status check complete');
  res.json({ environments: envResults, checked_at: new Date().toISOString() });
});

/**
 * Best-of-all-SVs handler for dev fund coupons.
 * Queries all healthy endpoints in parallel, returns the largest result set.
 */
router.get('/v0/unclaimed-development-fund-coupons', async (req, res) => {
  const endpoints = getAllEndpoints().filter(e => e.health?.healthy !== false);
  console.log(`[Scan Proxy] Best-of-all query across ${endpoints.length} healthy endpoints for dev fund coupons`);

  const results = await Promise.allSettled(
    endpoints.map(async (ep) => {
      const hostname = extractHostname(ep.url);
      const dispatcher = hostname ? createDispatcher(hostname) : undefined;
      const url = `${ep.url}/v0/unclaimed-development-fund-coupons`;
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/json',
          ...(hostname ? { Host: hostname } : {}),
        },
        ...(dispatcher ? { dispatcher } : {}),
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      // FIX: enforce size limit before buffering
      const text = await readBodyWithLimit(response, MAX_PROXY_RESPONSE_BYTES);
      const data = JSON.parse(text);
      const coupons = data['unclaimed-development-fund-coupons'] || [];
      console.log(`[Scan Proxy]   ${ep.name}: ${coupons.length} coupons`);
      recordSuccess(ep.url);
      return { name: ep.name, data, count: coupons.length };
    })
  );

  const successful = results
    .filter(r => r.status === 'fulfilled')
    .map(r => r.value);

  if (successful.length === 0) {
    return res.status(502).json({ error: 'All endpoints failed for dev fund coupons' });
  }

  // Return the result with the most coupons
  const best = successful.reduce((a, b) => a.count >= b.count ? a : b);
  console.log(`[Scan Proxy] Best result: ${best.name} with ${best.count} coupons`);
  res.set('X-Scan-Endpoint', best.name);
  res.set('X-Scan-Endpoints-Queried', successful.length.toString());
  res.json(best.data);
});

/**
 * Read a fetch Response body up to maxBytes, then discard the rest.
 * Throws if the body exceeds the limit so the caller can handle it.
 *
 * FIX: Using response.text() directly buffers the entire body with no upper
 * bound. For a proxy that is open to the internet, a single slow/large
 * upstream response can exhaust Node's heap. This helper streams the body
 * and aborts once MAX_PROXY_RESPONSE_BYTES is reached.
 */
async function readBodyWithLimit(response, maxBytes) {
  const reader = response.body?.getReader();
  if (!reader) {
    // Fallback for environments where body streaming isn't available
    return response.text();
  }

  const chunks = [];
  let totalBytes = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        reader.cancel();
        throw new Error(
          `Upstream response exceeds size limit (${maxBytes} bytes). Possible runaway response from Scan API.`
        );
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  // Concatenate all chunks into a single string
  const combined = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(combined);
}

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
    const baseUrl = path.startsWith("registry/") ? endpoint.url.replace(/\/api\/scan$/, "") : endpoint.url;
    const scanUrl = queryString 
      ? `${baseUrl}/${path}?${queryString}`
      : `${baseUrl}/${path}`;
    
    const reqStart = Date.now();
    console.log(`[Scan Proxy] ${method} → ${endpoint.name} | ${path}${queryString ? '?' + queryString : ''}`);
    console.log(`[Scan Proxy] Full URL: ${scanUrl}`);

    try {
      const hostname = extractHostname(endpoint.url);
      const dispatcher = hostname ? createDispatcher(hostname) : undefined;

      const fetchOptions = {
        method,
        headers: {
          'Accept': 'application/json',
          // Required for SCAN host-based routing
          ...(hostname ? { Host: hostname } : {}),
        },
        // CRITICAL: fetch() uses Undici's dispatcher, not https.Agent
        ...(dispatcher ? { dispatcher } : {}),
        signal: AbortSignal.timeout(30000),
      };

      // Only add body for POST/PUT/PATCH requests
      if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
        fetchOptions.headers['Content-Type'] = 'application/json';
        fetchOptions.body = JSON.stringify(req.body);
      }

      const scanRes = await fetch(scanUrl, fetchOptions);
      
      console.log(`[Scan Proxy] ✓ ${endpoint.name} responded ${scanRes.status} in ${Date.now() - reqStart}ms`);

      // Check for server errors that warrant rotation
      if (scanRes.status >= 500 || scanRes.status === 429) {
        console.warn(`[Scan Proxy] ⚠ ${endpoint.name} returned ${scanRes.status}, rotating to next endpoint...`);
        recordFailure(endpoint.url, new Error(`HTTP ${scanRes.status}`));
        const nextEndpoint = rotateToNextHealthy();
        console.log(`[Scan Proxy] → Now using: ${nextEndpoint.name}`);
        lastError = new Error(`Scan API returned ${scanRes.status}`);
        continue;
      }

      // Success or client error - return as-is
      recordSuccess(endpoint.url);

      // FIX: Read with size limit before forwarding to the client.
      // The original code used response.text() with no bound, allowing an
      // upstream to send gigabytes of data and OOM the server process.
      let text;
      try {
        text = await readBodyWithLimit(scanRes, MAX_PROXY_RESPONSE_BYTES);
      } catch (sizeErr) {
        console.error(`[Scan Proxy] ✗ Response too large from ${endpoint.name}: ${sizeErr.message}`);
        res.status(502).json({ error: 'Upstream response too large', details: sizeErr.message });
        return;
      }
      
      if (res.headersSent) return;
      res.set('X-Scan-Endpoint', endpoint.name);
      res.status(scanRes.status).send(text);
      return;

    } catch (err) {
      console.error(`[Scan Proxy] ✗ ${endpoint.name} failed: ${err.message}`);
      recordFailure(endpoint.url, err);
      const nextEndpoint = rotateToNextHealthy();
      console.log(`[Scan Proxy] → Rotating to: ${nextEndpoint.name}`);
      lastError = err;
    }
  }

  // All retries exhausted
  if (!res.headersSent) {
    res.status(502).json({ 
      error: lastError?.message || 'All Scan API endpoints failed',
      endpoint: getCurrentEndpoint().name,
    });
  }
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
