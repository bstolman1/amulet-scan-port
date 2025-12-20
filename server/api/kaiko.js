import express from 'express';

const router = express.Router();

const KAIKO_API_KEY = process.env.KAIKO_API_KEY;
const KAIKO_BASE_URL = 'https://us.market-api.kaiko.io/v2/data/trades.v1';

/**
 * GET /api/kaiko/ohlcv
 * Fetches OHLCV + VWAP data from Kaiko API
 * 
 * Query params:
 * - exchange: Exchange code (default: cbse)
 * - instrument_class: Instrument class (default: spot)
 * - instrument: Trading pair (default: btc-usd)
 * - interval: Time interval (default: 1h)
 * - start_time: ISO 8601 start time
 * - end_time: ISO 8601 end time
 * - sort: asc or desc (default: desc)
 * - page_size: Number of results (default: 100)
 */
router.get('/ohlcv', async (req, res) => {
  if (!KAIKO_API_KEY) {
    return res.status(500).json({ 
      error: 'KAIKO_API_KEY not configured',
      message: 'Please set KAIKO_API_KEY in your server/.env file'
    });
  }

  const {
    exchange = 'cbse',
    instrument_class = 'spot',
    instrument = 'btc-usd',
    interval = '1h',
    start_time,
    end_time,
    sort = 'desc',
    page_size = '100',
  } = req.query;

  const url = new URL(
    `${KAIKO_BASE_URL}/exchanges/${exchange}/${instrument_class}/${instrument}/aggregations/count_ohlcv_vwap`
  );

  url.searchParams.set('interval', interval);
  url.searchParams.set('sort', sort);
  url.searchParams.set('page_size', page_size);
  
  if (start_time) url.searchParams.set('start_time', start_time);
  if (end_time) url.searchParams.set('end_time', end_time);

  console.log(`📊 Fetching Kaiko OHLCV: ${exchange}/${instrument_class}/${instrument} (${interval})`);

  try {
    const response = await fetch(url.toString(), {
      headers: {
        'Accept': 'application/json',
        'X-Api-Key': KAIKO_API_KEY,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`❌ Kaiko API error (${response.status}):`, errorText);
      return res.status(response.status).json({ 
        error: `Kaiko API error: ${response.status}`,
        message: errorText 
      });
    }

    const data = await response.json();
    console.log(`✅ Kaiko returned ${data.data?.length || 0} candles`);
    
    res.json(data);
  } catch (err) {
    console.error('❌ Kaiko fetch failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/kaiko/asset-metrics
 * Fetches asset metrics data from Kaiko API (volumes, trades, liquidity, supply)
 * 
 * Query params:
 * - asset: Asset code (required, e.g., btc, eth, sol)
 * - start_time: ISO 8601 start time (required)
 * - end_time: ISO 8601 end time (required)
 * - interval: Time interval (default: 1h)
 * - sources: Include exchange breakdown (default: true)
 * - page_size: Number of results (default: 100)
 */
router.get('/asset-metrics', async (req, res) => {
  if (!KAIKO_API_KEY) {
    return res.status(500).json({ 
      error: 'KAIKO_API_KEY not configured',
      message: 'Please set KAIKO_API_KEY in your server/.env file'
    });
  }

  const {
    asset = 'btc',
    start_time,
    end_time,
    interval = '1h',
    sources = 'true',
    page_size = '100',
  } = req.query;

  // Default to last 24 hours if no time range specified
  const now = new Date();
  const defaultEnd = now.toISOString();
  const defaultStart = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();

  const url = new URL('https://us.market-api.kaiko.io/v2/data/analytics.v2/asset_metrics');
  
  url.searchParams.set('asset', asset);
  url.searchParams.set('start_time', start_time || defaultStart);
  url.searchParams.set('end_time', end_time || defaultEnd);
  url.searchParams.set('interval', interval);
  url.searchParams.set('sources', sources);
  url.searchParams.set('page_size', page_size);

  console.log(`📊 Fetching Kaiko Asset Metrics: ${asset} (${interval})`);

  try {
    const response = await fetch(url.toString(), {
      headers: {
        'Accept': 'application/json',
        'X-Api-Key': KAIKO_API_KEY,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`❌ Kaiko API error (${response.status}):`, errorText);
      return res.status(response.status).json({ 
        error: `Kaiko API error: ${response.status}`,
        message: errorText 
      });
    }

    const data = await response.json();
    console.log(`✅ Kaiko returned ${data.data?.length || 0} asset metric records`);
    
    res.json(data);
  } catch (err) {
    console.error('❌ Kaiko asset metrics fetch failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/kaiko/status
 * Check if Kaiko API is configured
 */
router.get('/status', (req, res) => {
  res.json({
    configured: !!KAIKO_API_KEY,
    message: KAIKO_API_KEY ? 'Kaiko API key is configured' : 'KAIKO_API_KEY not set in environment',
  });
});

export default router;
