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
 * GET /api/kaiko/cc-market-overview
 * Fetches aggregated CC market data across all exchanges
 */
router.get('/cc-market-overview', async (req, res) => {
  if (!KAIKO_API_KEY) {
    return res.status(500).json({ 
      error: 'KAIKO_API_KEY not configured',
      message: 'Please set KAIKO_API_KEY in your server/.env file'
    });
  }

  // CC trading pairs and their exchanges
  const ccExchanges = [
    { exchange: 'krkn', instrument: 'cc-usd', class: 'spot' },
    { exchange: 'krkn', instrument: 'cc-usdt', class: 'spot' },
    { exchange: 'krkn', instrument: 'cc-usdc', class: 'spot' },
    { exchange: 'krkn', instrument: 'cc-eur', class: 'spot' },
    { exchange: 'gate', instrument: 'cc-usdt', class: 'spot' },
    { exchange: 'kcon', instrument: 'cc-usdt', class: 'spot' },
    { exchange: 'mexc', instrument: 'cc-usdt', class: 'spot' },
    { exchange: 'mexc', instrument: 'cc-usdc', class: 'spot' },
    { exchange: 'bbsp', instrument: 'cc-usdt', class: 'spot' },
    { exchange: 'bbsp', instrument: 'cc-usdc', class: 'spot' },
    { exchange: 'hitb', instrument: 'cc-usdt', class: 'spot' },
    { exchange: 'cnex', instrument: 'cc-usdt', class: 'spot' },
    // Derivatives
    { exchange: 'binc', instrument: 'cc-usdt', class: 'perpetual-future' },
    { exchange: 'okex', instrument: 'cc-usdt', class: 'perpetual-future' },
    { exchange: 'gtdm', instrument: 'cc-usdt', class: 'perpetual-future' },
    { exchange: 'bbit', instrument: 'cc-usdt', class: 'perpetual-future' },
    { exchange: 'hbdm', instrument: 'cc-usdt', class: 'perpetual-future' },
  ];

  console.log(`📊 Fetching CC market overview across ${ccExchanges.length} trading pairs`);

  try {
    const results = await Promise.allSettled(
      ccExchanges.map(async ({ exchange, instrument, class: instrumentClass }) => {
        const url = new URL(
          `${KAIKO_BASE_URL}/exchanges/${exchange}/${instrumentClass}/${instrument}/aggregations/count_ohlcv_vwap`
        );
        url.searchParams.set('interval', '1d');
        url.searchParams.set('page_size', '2');
        url.searchParams.set('sort', 'desc');

        const response = await fetch(url.toString(), {
          headers: {
            'Accept': 'application/json',
            'X-Api-Key': KAIKO_API_KEY,
          },
        });

        if (!response.ok) return null;
        
        const data = await response.json();
        const candles = data.data || [];
        const latest = candles[0];
        const previous = candles[1];

        if (!latest) return null;

        return {
          exchange,
          exchangeName: getExchangeName(exchange),
          instrument,
          instrumentClass,
          price: latest.close ? parseFloat(latest.close) : null,
          open: latest.open ? parseFloat(latest.open) : null,
          high: latest.high ? parseFloat(latest.high) : null,
          low: latest.low ? parseFloat(latest.low) : null,
          close: latest.close ? parseFloat(latest.close) : null,
          volume: parseFloat(latest.volume || '0'),
          vwap: latest.price ? parseFloat(latest.price) : null,
          tradeCount: latest.count || 0,
          previousClose: previous?.close ? parseFloat(previous.close) : null,
          change24h: latest.close && previous?.close 
            ? ((parseFloat(latest.close) - parseFloat(previous.close)) / parseFloat(previous.close)) * 100 
            : null,
        };
      })
    );

    const validResults = results
      .filter(r => r.status === 'fulfilled' && r.value !== null)
      .map(r => r.value);

    // Aggregate stats
    const totalVolume = validResults.reduce((sum, r) => sum + (r.volume || 0), 0);
    const totalTrades = validResults.reduce((sum, r) => sum + (r.tradeCount || 0), 0);
    
    // Get reference price from Kraken CC/USD
    const krakenUsd = validResults.find(r => r.exchange === 'krkn' && r.instrument === 'cc-usd');
    const referencePrice = krakenUsd?.price || validResults.find(r => r.price)?.price || null;
    const referenceChange = krakenUsd?.change24h || validResults.find(r => r.change24h !== null)?.change24h || null;

    // Volume-weighted average price across all pairs
    const volumeWeightedPrices = validResults.filter(r => r.vwap && r.volume > 0);
    const totalWeightedVolume = volumeWeightedPrices.reduce((sum, r) => sum + r.volume, 0);
    const vwap = totalWeightedVolume > 0
      ? volumeWeightedPrices.reduce((sum, r) => sum + (r.vwap * r.volume), 0) / totalWeightedVolume
      : null;

    res.json({
      result: 'success',
      timestamp: new Date().toISOString(),
      summary: {
        price: referencePrice,
        change24h: referenceChange,
        vwap,
        totalVolume,
        totalTrades,
        activeExchanges: validResults.length,
      },
      exchanges: validResults.sort((a, b) => (b.volume || 0) - (a.volume || 0)),
    });
  } catch (err) {
    console.error('❌ CC market overview fetch failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

function getExchangeName(code) {
  const names = {
    krkn: 'Kraken',
    binc: 'Binance',
    bbsp: 'Bybit Spot',
    gate: 'Gate.io',
    kcon: 'KuCoin',
    mexc: 'MEXC',
    okex: 'OKX',
    hitb: 'HitBTC',
    cnex: 'CoinEx',
    gtdm: 'Gate.io Derivatives',
    hbdm: 'Huobi Derivatives',
    bbit: 'Bybit Perps',
  };
  return names[code] || code.toUpperCase();
}

/**
 * GET /api/kaiko/twap
 * Computes Time-Weighted Average Price from OHLCV close prices
 * 
 * Query params:
 * - exchange: Exchange code (default: krkn)
 * - instrument_class: Instrument class (default: spot)
 * - instrument: Trading pair (default: cc-usd)
 * - interval: Candle interval used for TWAP calc (default: 5m)
 * - start_time: ISO 8601 start (required)
 * - end_time: ISO 8601 end (required)
 * - decimals: Number of decimal places (default: 5, max: 18)
 */
router.get('/twap', async (req, res) => {
  if (!KAIKO_API_KEY) {
    return res.status(500).json({ 
      error: 'KAIKO_API_KEY not configured',
      message: 'Please set KAIKO_API_KEY in your server/.env file'
    });
  }

  const {
    exchange = 'krkn',
    instrument_class = 'spot',
    instrument = 'cc-usd',
    interval = '5m',
    start_time,
    end_time,
    decimals = '5',
  } = req.query;

  if (!start_time || !end_time) {
    return res.status(400).json({ error: 'start_time and end_time are required' });
  }

  const decimalPlaces = Math.min(Math.max(parseInt(decimals) || 5, 0), 18);

  console.log(`📊 Computing TWAP: ${exchange}/${instrument_class}/${instrument} interval=${interval} from ${start_time} to ${end_time}`);

  try {
    // Fetch all candles in the window — paginate if needed
    let allCandles = [];
    let nextUrl = null;
    let page = 0;

    do {
      const url = nextUrl ? new URL(nextUrl) : new URL(
        `${KAIKO_BASE_URL}/exchanges/${exchange}/${instrument_class}/${instrument}/aggregations/count_ohlcv_vwap`
      );

      if (!nextUrl) {
        url.searchParams.set('interval', interval);
        url.searchParams.set('sort', 'asc');
        url.searchParams.set('page_size', '1000');
        url.searchParams.set('start_time', start_time);
        url.searchParams.set('end_time', end_time);
      }

      const response = await fetch(url.toString(), {
        headers: { 'Accept': 'application/json', 'X-Api-Key': KAIKO_API_KEY },
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`❌ Kaiko TWAP fetch error (${response.status}):`, errorText);
        return res.status(response.status).json({ error: `Kaiko API error: ${response.status}`, message: errorText });
      }

      const data = await response.json();
      allCandles = allCandles.concat(data.data || []);
      nextUrl = data.next_url || null;
      page++;
    } while (nextUrl && page < 20); // safety cap

    // Filter candles with valid close prices
    const validCandles = allCandles.filter(c => c.close !== null && c.close !== undefined);

    if (validCandles.length === 0) {
      return res.json({
        result: 'no_data',
        twap: null,
        candle_count: 0,
        message: 'No candles with close prices in the specified range',
      });
    }

    // TWAP = arithmetic mean of close prices (equal time weight per candle)
    const closes = validCandles.map(c => parseFloat(c.close));
    const twap = closes.reduce((sum, p) => sum + p, 0) / closes.length;

    res.json({
      result: 'success',
      twap: twap.toFixed(decimalPlaces),
      twap_raw: twap,
      candle_count: validCandles.length,
      total_candles_fetched: allCandles.length,
      interval,
      exchange,
      instrument,
      instrument_class,
      start_time,
      end_time,
      decimals: decimalPlaces,
      first_candle: validCandles[0]?.timestamp,
      last_candle: validCandles[validCandles.length - 1]?.timestamp,
    });

    console.log(`✅ TWAP computed: ${twap.toFixed(decimalPlaces)} from ${validCandles.length} candles`);
  } catch (err) {
    console.error('❌ TWAP computation failed:', err.message);
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
