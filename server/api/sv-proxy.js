/**
 * SV Node API Proxy
 * 
 * Proxies requests to the SV Node API to avoid CORS issues.
 */

import { Router } from 'express';

const router = Router();

const SV_API_BASE = process.env.SV_API_BASE || 'https://sv-1.global.canton.network.sync.global/api/sv';

/**
 * GET /api/sv-proxy/voterequests
 * Proxy for GET /v0/admin/sv/voterequests - List all active VoteRequests
 */
router.get('/voterequests', async (req, res) => {
  try {
    const response = await fetch(`${SV_API_BASE}/v0/admin/sv/voterequests`, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
    });

    if (!response.ok) {
      return res.status(response.status).json({
        error: `SV API error: ${response.status} ${response.statusText}`,
      });
    }

    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error proxying vote requests:', error);
    res.status(500).json({
      error: 'Failed to fetch vote requests from SV Node',
      details: error.message,
    });
  }
});

/**
 * POST /api/sv-proxy/voteresults
 * Proxy for POST /v0/admin/sv/voteresults - Get vote results with filters
 */
router.post('/voteresults', async (req, res) => {
  try {
    const response = await fetch(`${SV_API_BASE}/v0/admin/sv/voteresults`, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(req.body),
    });

    if (!response.ok) {
      return res.status(response.status).json({
        error: `SV API error: ${response.status} ${response.statusText}`,
      });
    }

    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Error proxying vote results:', error);
    res.status(500).json({
      error: 'Failed to fetch vote results from SV Node',
      details: error.message,
    });
  }
});

export default router;
