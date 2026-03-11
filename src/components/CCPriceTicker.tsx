import { useMemo } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useCCMarketOverview } from "@/hooks/use-kaiko-ohlcv";
import { TrendingUp, TrendingDown, Activity } from "lucide-react";
import {
  AreaChart,
  Area,
  ResponsiveContainer,
} from "recharts";

interface CCPriceTickerProps {
  enabled?: boolean;
}

function safeNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function safeCurrency(value: unknown): string {
  const num = safeNumber(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function safeCompactVolume(value: unknown): string {
  const num = safeNumber(value, 0);
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
  return num.toFixed(0);
}

export function CCPriceTicker({ enabled = true }: CCPriceTickerProps) {
  const { data, isLoading, error } = useCCMarketOverview(enabled);

  const sparklineData = useMemo(() => {
    if (!data?.exchanges) return [];

    return data.exchanges
      .filter((ex) => safeNumber(ex.price, NaN) === safeNumber(ex.price, NaN))
      .map((ex, idx) => ({
        idx,
        price: safeNumber(ex.price, 0),
      }))
      .sort((a, b) => a.price - b.price);
  }, [data?.exchanges]);

  if (isLoading) {
    return (
      <Card className="bg-gradient-to-r from-primary/5 to-primary/10 border-primary/20">
        <CardContent className="p-4">
          <div className="flex items-center gap-4">
            <Skeleton className="h-10 w-10 rounded-full" />
            <div className="flex-1">
              <Skeleton className="h-4 w-20 mb-1" />
              <Skeleton className="h-6 w-32" />
            </div>
            <Skeleton className="h-12 w-24" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !data) {
    return null;
  }

  const summary = data.summary ?? {};
  const price = safeNumber(summary.price, NaN);
  const change24h =
    typeof summary.change24h === "number" && Number.isFinite(summary.change24h)
      ? summary.change24h
      : null;
  const activeExchanges = safeNumber(summary.activeExchanges, 0);
  const totalVolume = safeNumber(summary.totalVolume, 0);
  const isPositive = change24h !== null ? change24h >= 0 : true;

  return (
    <Card className="bg-gradient-to-r from-primary/5 via-primary/10 to-primary/5 border-primary/20 overflow-hidden">
      <CardContent className="p-4">
        <div className="flex items-center gap-4">
          <div className="relative">
            <div className="h-12 w-12 rounded-full bg-primary/20 flex items-center justify-center">
              <span className="text-xl font-bold text-primary">CC</span>
            </div>
            <div
              className={`absolute -bottom-1 -right-1 h-4 w-4 rounded-full flex items-center justify-center ${
                isPositive ? "bg-green-500" : "bg-red-500"
              }`}
            >
              {isPositive ? (
                <TrendingUp className="h-2.5 w-2.5 text-white" />
              ) : (
                <TrendingDown className="h-2.5 w-2.5 text-white" />
              )}
            </div>
          </div>

          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground font-medium">Canton Coin</span>
              <Badge
                variant="outline"
                className="text-[10px] px-1.5 py-0 h-4 border-primary/30 text-primary"
              >
                LIVE
              </Badge>
            </div>
            <div className="flex items-baseline gap-3">
              <span className="text-2xl font-bold">
                ${Number.isFinite(price) ? safeCurrency(price) : "-"}
              </span>
              {change24h !== null && (
                <span className={`text-sm font-semibold ${isPositive ? "text-green-500" : "text-red-500"}`}>
                  {isPositive ? "+" : ""}
                  {change24h.toFixed(2)}%
                </span>
              )}
            </div>
          </div>

          <div className="w-24 h-12">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={sparklineData}>
                <defs>
                  <linearGradient id="sparklineGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop
                      offset="5%"
                      stopColor={isPositive ? "hsl(142, 76%, 36%)" : "hsl(0, 84%, 60%)"}
                      stopOpacity={0.4}
                    />
                    <stop
                      offset="95%"
                      stopColor={isPositive ? "hsl(142, 76%, 36%)" : "hsl(0, 84%, 60%)"}
                      stopOpacity={0}
                    />
                  </linearGradient>
                </defs>
                <Area
                  type="monotone"
                  dataKey="price"
                  stroke={isPositive ? "hsl(142, 76%, 36%)" : "hsl(0, 84%, 60%)"}
                  strokeWidth={2}
                  fill="url(#sparklineGradient)"
                  dot={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          <div className="hidden sm:flex flex-col items-end gap-1">
            <div className="flex items-center gap-1 text-sm text-muted-foreground">
              <Activity className="h-3 w-3" />
              <span>{activeExchanges} markets</span>
            </div>
            <div className="text-xs text-muted-foreground">
              Vol: ${safeCompactVolume(totalVolume)}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
