import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "@/lib/duckdb-api-client";

export interface TwapResponse {
  result: string;
  twap: string | null;
  twap_raw: number | null;
  candle_count: number;
  total_candles_fetched: number;
  interval: string;
  exchange: string;
  instrument: string;
  instrument_class: string;
  start_time: string;
  end_time: string;
  decimals: number;
  first_candle?: number;
  last_candle?: number;
  message?: string;
}

export interface TwapParams {
  exchange?: string;
  instrumentClass?: string;
  instrument?: string;
  interval?: string;
  startTime: string;
  endTime: string;
  decimals?: number;
}

export function useKaikoTwap(params: TwapParams | null, enabled = true) {
  return useQuery<TwapResponse, Error>({
    queryKey: ['kaiko-twap', params],
    queryFn: async () => {
      if (!params) throw new Error('No params');
      const {
        exchange = 'krkn',
        instrumentClass = 'spot',
        instrument = 'cc-usd',
        interval = '5m',
        startTime,
        endTime,
        decimals = 5,
      } = params;

      const sp = new URLSearchParams({
        exchange,
        instrument_class: instrumentClass,
        instrument,
        interval,
        start_time: startTime,
        end_time: endTime,
        decimals: String(decimals),
      });

      return apiFetch<TwapResponse>(`/api/kaiko/twap?${sp.toString()}`);
    },
    enabled: enabled && !!params,
    staleTime: 30_000,
  });
}

// Volume-Weighted TWAP types
export interface VwTwapExchangeBreakdown {
  exchange: string;
  instrument: string;
  candle_count: number;
  total_volume: number;
}

export interface VwTwapResponse {
  result: string;
  twap: string | null;
  twap_raw: number | null;
  time_slices: number;
  exchanges_with_data: number;
  total_exchange_pairs: number;
  interval: string;
  start_time: string;
  end_time: string;
  decimals: number;
  first_slice?: number;
  last_slice?: number;
  exchange_breakdown?: VwTwapExchangeBreakdown[];
  message?: string;
}

export interface VwTwapParams {
  interval?: string;
  startTime: string;
  endTime: string;
  decimals?: number;
}

export function useKaikoVwTwap(params: VwTwapParams | null, enabled = true) {
  return useQuery<VwTwapResponse, Error>({
    queryKey: ['kaiko-vw-twap', params],
    queryFn: async () => {
      if (!params) throw new Error('No params');
      const { interval = '5m', startTime, endTime, decimals = 5 } = params;

      const sp = new URLSearchParams({
        interval,
        start_time: startTime,
        end_time: endTime,
        decimals: String(decimals),
      });

      return apiFetch<VwTwapResponse>(`/api/kaiko/vw-twap?${sp.toString()}`);
    },
    enabled: enabled && !!params,
    staleTime: 30_000,
  });
}
