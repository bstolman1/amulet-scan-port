import { useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { KaikoCandle } from "@/hooks/use-kaiko-ohlcv";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import { TrendingUp, TrendingDown, BarChart3 } from "lucide-react";

interface CCPriceChartProps {
  candles: KaikoCandle[];
  isLoading: boolean;
  exchange?: string;
  instrument?: string;
}

export function CCPriceChart({ candles, isLoading, exchange, instrument }: CCPriceChartProps) {
  const chartData = useMemo(() => {
    if (!candles.length) return [];
    
    // Reverse to show oldest first (left to right)
    return [...candles].reverse().map((candle) => ({
      timestamp: candle.timestamp,
      time: new Date(candle.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      date: new Date(candle.timestamp).toLocaleDateString(),
      open: candle.open ? parseFloat(candle.open) : null,
      high: candle.high ? parseFloat(candle.high) : null,
      low: candle.low ? parseFloat(candle.low) : null,
      close: candle.close ? parseFloat(candle.close) : null,
      volume: parseFloat(candle.volume || '0'),
      vwap: candle.price ? parseFloat(candle.price) : null,
    }));
  }, [candles]);

  const priceChange = useMemo(() => {
    if (chartData.length < 2) return null;
    const first = chartData[0]?.close;
    const last = chartData[chartData.length - 1]?.close;
    if (!first || !last) return null;
    return ((last - first) / first) * 100;
  }, [chartData]);

  const minPrice = useMemo(() => {
    const prices = chartData.filter(d => d.low !== null).map(d => d.low as number);
    return prices.length ? Math.min(...prices) * 0.998 : 0;
  }, [chartData]);

  const maxPrice = useMemo(() => {
    const prices = chartData.filter(d => d.high !== null).map(d => d.high as number);
    return prices.length ? Math.max(...prices) * 1.002 : 0;
  }, [chartData]);

  const avgVwap = useMemo(() => {
    const vwaps = chartData.filter(d => d.vwap !== null).map(d => d.vwap as number);
    return vwaps.length ? vwaps.reduce((a, b) => a + b, 0) / vwaps.length : null;
  }, [chartData]);

  const isPositive = priceChange !== null && priceChange >= 0;
  const gradientId = isPositive ? 'priceGradientUp' : 'priceGradientDown';
  const strokeColor = isPositive ? 'hsl(var(--chart-2))' : 'hsl(var(--destructive))';

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="h-5 w-5" />
            CC Price Chart
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Skeleton className="h-[300px] w-full" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="h-5 w-5" />
            CC Price Chart
            {exchange && instrument && (
              <Badge variant="outline" className="ml-2">
                {exchange.toUpperCase()} / {instrument.toUpperCase()}
              </Badge>
            )}
          </CardTitle>
          {priceChange !== null && (
            <div className={`flex items-center gap-1 text-sm font-medium ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
              {isPositive ? <TrendingUp className="h-4 w-4" /> : <TrendingDown className="h-4 w-4" />}
              {isPositive ? '+' : ''}{priceChange.toFixed(2)}%
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {chartData.length === 0 ? (
          <div className="h-[300px] flex items-center justify-center text-muted-foreground">
            No chart data available
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="priceGradientUp" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(var(--chart-2))" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="hsl(var(--chart-2))" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="priceGradientDown" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(var(--destructive))" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="hsl(var(--destructive))" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis 
                dataKey="time" 
                tick={{ fontSize: 11 }}
                className="text-muted-foreground"
                tickLine={false}
                axisLine={false}
              />
              <YAxis 
                domain={[minPrice, maxPrice]}
                tick={{ fontSize: 11 }}
                className="text-muted-foreground"
                tickLine={false}
                axisLine={false}
                tickFormatter={(value) => `$${value.toFixed(2)}`}
                width={70}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'hsl(var(--background))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px',
                  padding: '12px',
                }}
                labelStyle={{ color: 'hsl(var(--foreground))' }}
                formatter={(value: number, name: string) => {
                  const labels: Record<string, string> = {
                    close: 'Close',
                    high: 'High',
                    low: 'Low',
                    vwap: 'VWAP',
                  };
                  return [`$${value?.toFixed(4) || '-'}`, labels[name] || name];
                }}
                labelFormatter={(label, payload) => {
                  if (payload?.[0]?.payload) {
                    return `${payload[0].payload.date} ${label}`;
                  }
                  return label;
                }}
              />
              {avgVwap && (
                <ReferenceLine 
                  y={avgVwap} 
                  stroke="hsl(var(--muted-foreground))" 
                  strokeDasharray="5 5"
                  label={{ 
                    value: `VWAP: $${avgVwap.toFixed(2)}`, 
                    position: 'right',
                    fill: 'hsl(var(--muted-foreground))',
                    fontSize: 10,
                  }}
                />
              )}
              <Area
                type="monotone"
                dataKey="close"
                stroke={strokeColor}
                strokeWidth={2}
                fill={`url(#${gradientId})`}
                dot={false}
                activeDot={{ r: 4, strokeWidth: 2 }}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}