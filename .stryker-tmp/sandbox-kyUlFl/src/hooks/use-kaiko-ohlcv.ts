// @ts-nocheck
function stryNS_9fa48() {
  var g = typeof globalThis === 'object' && globalThis && globalThis.Math === Math && globalThis || new Function("return this")();
  var ns = g.__stryker__ || (g.__stryker__ = {});
  if (ns.activeMutant === undefined && g.process && g.process.env && g.process.env.__STRYKER_ACTIVE_MUTANT__) {
    ns.activeMutant = g.process.env.__STRYKER_ACTIVE_MUTANT__;
  }
  function retrieveNS() {
    return ns;
  }
  stryNS_9fa48 = retrieveNS;
  return retrieveNS();
}
stryNS_9fa48();
function stryCov_9fa48() {
  var ns = stryNS_9fa48();
  var cov = ns.mutantCoverage || (ns.mutantCoverage = {
    static: {},
    perTest: {}
  });
  function cover() {
    var c = cov.static;
    if (ns.currentTestId) {
      c = cov.perTest[ns.currentTestId] = cov.perTest[ns.currentTestId] || {};
    }
    var a = arguments;
    for (var i = 0; i < a.length; i++) {
      c[a[i]] = (c[a[i]] || 0) + 1;
    }
  }
  stryCov_9fa48 = cover;
  cover.apply(null, arguments);
}
function stryMutAct_9fa48(id) {
  var ns = stryNS_9fa48();
  function isActive(id) {
    if (ns.activeMutant === id) {
      if (ns.hitCount !== void 0 && ++ns.hitCount > ns.hitLimit) {
        throw new Error('Stryker: Hit count limit reached (' + ns.hitCount + ')');
      }
      return true;
    }
    return false;
  }
  stryMutAct_9fa48 = isActive;
  return isActive(id);
}
import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";
export interface KaikoCandle {
  timestamp: number;
  open: string | null;
  high: string | null;
  low: string | null;
  close: string | null;
  volume: string;
  price: string | null;
  count: number;
}
export interface KaikoResponse {
  query: {
    exchange: string;
    instrument_class: string;
    instrument: string;
    interval: string;
    sort: string;
  };
  data: KaikoCandle[];
  result: string;
  continuation_token?: string;
  next_url?: string;
}
export interface KaikoParams {
  exchange?: string;
  instrumentClass?: string;
  instrument?: string;
  interval?: string;
  startTime?: string;
  endTime?: string;
  sort?: 'asc' | 'desc';
  pageSize?: number;
}
export function useKaikoOHLCV(params: KaikoParams = {}, enabled = stryMutAct_9fa48("2419") ? false : (stryCov_9fa48("2419"), true)) {
  if (stryMutAct_9fa48("2420")) {
    {}
  } else {
    stryCov_9fa48("2420");
    const {
      exchange = 'cbse',
      instrumentClass = 'spot',
      instrument = 'btc-usd',
      interval = '1h',
      startTime,
      endTime,
      sort = 'desc',
      pageSize = 100
    } = params;
    return useQuery<KaikoResponse, Error>({
      queryKey: stryMutAct_9fa48("2427") ? [] : (stryCov_9fa48("2427"), ['kaiko-ohlcv', exchange, instrumentClass, instrument, interval, startTime, endTime, sort, pageSize]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2429")) {
          {}
        } else {
          stryCov_9fa48("2429");
          const searchParams = new URLSearchParams({
            exchange,
            instrument_class: instrumentClass,
            instrument,
            interval,
            sort,
            page_size: String(pageSize)
          });
          if (stryMutAct_9fa48("2432") ? false : stryMutAct_9fa48("2431") ? true : (stryCov_9fa48("2431", "2432"), startTime)) searchParams.set('start_time', startTime);
          if (stryMutAct_9fa48("2435") ? false : stryMutAct_9fa48("2434") ? true : (stryCov_9fa48("2434", "2435"), endTime)) searchParams.set('end_time', endTime);
          return apiFetch<KaikoResponse>(`/api/kaiko/ohlcv?${searchParams.toString()}`);
        }
      },
      enabled,
      staleTime: 60_000,
      // 1 minute
      refetchInterval: 60_000 // Refresh every minute
    });
  }
}
export function useKaikoStatus() {
  if (stryMutAct_9fa48("2438")) {
    {}
  } else {
    stryCov_9fa48("2438");
    return useQuery({
      queryKey: stryMutAct_9fa48("2440") ? [] : (stryCov_9fa48("2440"), ['kaiko-status']),
      queryFn: stryMutAct_9fa48("2442") ? () => undefined : (stryCov_9fa48("2442"), () => apiFetch<{
        configured: boolean;
        message: string;
      }>('/api/kaiko/status')),
      staleTime: stryMutAct_9fa48("2444") ? 5 / 60_000 : (stryCov_9fa48("2444"), 5 * 60_000)
    });
  }
}

// Asset Metrics types
export interface AssetTradeData {
  exchange: string;
  volume_usd: number;
  volume_asset: number;
  trade_count: number;
}
export interface MarketDepth {
  exchange: string;
  volume_assets: Record<string, number>;
  volume_usds: Record<string, number>;
}
export interface TokenInfo {
  blockchain: string;
  token_address: string;
  nb_of_holders: number;
  main_holders: Array<{
    address: string;
    amount: number;
    percentage: number;
  }>;
  total_supply: number;
}
export interface AssetMetricData {
  timestamp: string;
  price: number | null;
  total_volume_usd: number;
  total_volume_asset: number;
  total_trade_count: number;
  off_chain_liquidity_data?: {
    total_off_chain_volume_usd: number;
    total_off_chain_volume_asset: number;
    total_off_chain_trade_count: number;
    trade_data: AssetTradeData[];
    buy_market_depths?: MarketDepth[];
    sell_market_depths?: MarketDepth[];
    total_buy_market_depth?: {
      volume_assets: Record<string, number>;
      volume_usds: Record<string, number>;
    };
    total_sell_market_depth?: {
      volume_assets: Record<string, number>;
      volume_usds: Record<string, number>;
    };
  };
  on_chain_liquidity_data?: {
    total_on_chain_volume_usd: number;
    total_on_chain_volume_asset: number;
    total_on_chain_trade_count: number;
    trades_data: AssetTradeData[];
    token_information?: TokenInfo[];
  };
}
export interface AssetMetricsResponse {
  data: AssetMetricData[];
  result?: string;
  continuation_token?: string;
  next_url?: string;
}
export interface AssetMetricsParams {
  asset?: string;
  startTime?: string;
  endTime?: string;
  interval?: string;
  sources?: boolean;
  pageSize?: number;
}
export function useKaikoAssetMetrics(params: AssetMetricsParams = {}, enabled = stryMutAct_9fa48("2445") ? false : (stryCov_9fa48("2445"), true)) {
  if (stryMutAct_9fa48("2446")) {
    {}
  } else {
    stryCov_9fa48("2446");
    const {
      asset = 'btc',
      startTime,
      endTime,
      interval = '1h',
      sources = stryMutAct_9fa48("2449") ? false : (stryCov_9fa48("2449"), true),
      pageSize = 100
    } = params;
    return useQuery<AssetMetricsResponse, Error>({
      queryKey: stryMutAct_9fa48("2451") ? [] : (stryCov_9fa48("2451"), ['kaiko-asset-metrics', asset, startTime, endTime, interval, sources, pageSize]),
      queryFn: async () => {
        if (stryMutAct_9fa48("2453")) {
          {}
        } else {
          stryCov_9fa48("2453");
          const searchParams = new URLSearchParams({
            asset,
            interval,
            sources: String(sources),
            page_size: String(pageSize)
          });
          if (stryMutAct_9fa48("2456") ? false : stryMutAct_9fa48("2455") ? true : (stryCov_9fa48("2455", "2456"), startTime)) searchParams.set('start_time', startTime);
          if (stryMutAct_9fa48("2459") ? false : stryMutAct_9fa48("2458") ? true : (stryCov_9fa48("2458", "2459"), endTime)) searchParams.set('end_time', endTime);
          return apiFetch<AssetMetricsResponse>(`/api/kaiko/asset-metrics?${searchParams.toString()}`);
        }
      },
      enabled,
      staleTime: 60_000,
      refetchInterval: 60_000
    });
  }
}

// CC Market Overview types
export interface CCExchangeData {
  exchange: string;
  exchangeName: string;
  instrument: string;
  instrumentClass: string;
  price: number | null;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number;
  vwap: number | null;
  tradeCount: number;
  previousClose: number | null;
  change24h: number | null;
}
export interface CCMarketOverview {
  result: string;
  timestamp: string;
  summary: {
    price: number | null;
    change24h: number | null;
    vwap: number | null;
    totalVolume: number;
    totalTrades: number;
    activeExchanges: number;
  };
  exchanges: CCExchangeData[];
}
export function useCCMarketOverview(enabled = stryMutAct_9fa48("2462") ? false : (stryCov_9fa48("2462"), true)) {
  if (stryMutAct_9fa48("2463")) {
    {}
  } else {
    stryCov_9fa48("2463");
    return useQuery<CCMarketOverview, Error>({
      queryKey: stryMutAct_9fa48("2465") ? [] : (stryCov_9fa48("2465"), ['cc-market-overview']),
      queryFn: stryMutAct_9fa48("2467") ? () => undefined : (stryCov_9fa48("2467"), () => apiFetch<CCMarketOverview>('/api/kaiko/cc-market-overview')),
      enabled,
      staleTime: 60_000,
      refetchInterval: 60_000
    });
  }
}