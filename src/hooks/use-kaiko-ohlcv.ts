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

export function useKaikoOHLCV(params: KaikoParams = {}, enabled = true) {
  const {
    exchange = 'cbse',
    instrumentClass = 'spot',
    instrument = 'btc-usd',
    interval = '1h',
    startTime,
    endTime,
    sort = 'desc',
    pageSize = 100,
  } = params;

  return useQuery<KaikoResponse, Error>({
    queryKey: ['kaiko-ohlcv', exchange, instrumentClass, instrument, interval, startTime, endTime, sort, pageSize],
    queryFn: async () => {
      const searchParams = new URLSearchParams({
        exchange,
        instrument_class: instrumentClass,
        instrument,
        interval,
        sort,
        page_size: String(pageSize),
      });
      
      if (startTime) searchParams.set('start_time', startTime);
      if (endTime) searchParams.set('end_time', endTime);

      return apiFetch<KaikoResponse>(`/api/kaiko/ohlcv?${searchParams.toString()}`);
    },
    enabled,
    staleTime: 60_000, // 1 minute
    refetchInterval: 60_000, // Refresh every minute
  });
}

export function useKaikoStatus() {
  return useQuery({
    queryKey: ['kaiko-status'],
    queryFn: () => apiFetch<{ configured: boolean; message: string }>('/api/kaiko/status'),
    staleTime: 5 * 60_000,
  });
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
    total_buy_market_depth?: { volume_assets: Record<string, number>; volume_usds: Record<string, number> };
    total_sell_market_depth?: { volume_assets: Record<string, number>; volume_usds: Record<string, number> };
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

export function useKaikoAssetMetrics(params: AssetMetricsParams = {}, enabled = true) {
  const {
    asset = 'btc',
    startTime,
    endTime,
    interval = '1h',
    sources = true,
    pageSize = 100,
  } = params;

  return useQuery<AssetMetricsResponse, Error>({
    queryKey: ['kaiko-asset-metrics', asset, startTime, endTime, interval, sources, pageSize],
    queryFn: async () => {
      const searchParams = new URLSearchParams({
        asset,
        interval,
        sources: String(sources),
        page_size: String(pageSize),
      });
      
      if (startTime) searchParams.set('start_time', startTime);
      if (endTime) searchParams.set('end_time', endTime);

      return apiFetch<AssetMetricsResponse>(`/api/kaiko/asset-metrics?${searchParams.toString()}`);
    },
    enabled,
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
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

export function useCCMarketOverview(enabled = true) {
  return useQuery<CCMarketOverview, Error>({
    queryKey: ['cc-market-overview'],
    queryFn: () => apiFetch<CCMarketOverview>('/api/kaiko/cc-market-overview'),
    enabled,
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
}
