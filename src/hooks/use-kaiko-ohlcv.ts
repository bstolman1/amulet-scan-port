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
