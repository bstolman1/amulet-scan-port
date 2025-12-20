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

export function CCPriceTicker({ enabled = true }: CCPriceTickerProps) {
  const { data, isLoading, error } = useCCMarketOverview(enabled);

  // Generate sparkline data from exchange prices
  const sparklineData = useMemo(() => {
    if (!data?.exchanges) return [];
    
    // Use exchange prices as sparkline points for visual effect
    return data.exchanges
      .filter(ex => ex.price !== null)
      .map((ex, idx) => ({
        idx,
        price: ex.price,
      }))
      .sort((a, b) => (a.price || 0) - (b.price || 0));
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

  const { summary } = data;
  const isPositive = summary.change24h !== null && summary.change24h >= 0;

  return (
    <Card className="bg-gradient-to-r from-primary/5 via-primary/10 to-primary/5 border-primary/20 overflow-hidden">
      <CardContent className="p-4">
        <div className="flex items-center gap-4">
          {/* CC Logo/Icon */}
          <div className="relative">
            <div className="h-12 w-12 rounded-full bg-primary/20 flex items-center justify-center">
              <span className="text-xl font-bold text-primary">CC</span>
            </div>
            <div className={`absolute -bottom-1 -right-1 h-4 w-4 rounded-full flex items-center justify-center ${isPositive ? 'bg-green-500' : 'bg-red-500'}`}>
              {isPositive ? (
                <TrendingUp className="h-2.5 w-2.5 text-white" />
              ) : (
                <TrendingDown className="h-2.5 w-2.5 text-white" />
              )}
            </div>
          </div>

          {/* Price Info */}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground font-medium">Canton Coin</span>
              <Badge variant="outline" className="text-[10px] px-1.5 py-0 h-4 border-primary/30 text-primary">
                LIVE
              </Badge>
            </div>
            <div className="flex items-baseline gap-3">
              <span className="text-2xl font-bold">
                ${summary.price?.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 }) || '-'}
              </span>
              {summary.change24h !== null && (
                <span className={`text-sm font-semibold ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                  {isPositive ? '+' : ''}{summary.change24h.toFixed(2)}%
                </span>
              )}
            </div>
          </div>

          {/* Sparkline */}
          <div className="w-24 h-12">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={sparklineData}>
                <defs>
                  <linearGradient id="sparklineGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop 
                      offset="5%" 
                      stopColor={isPositive ? 'hsl(142, 76%, 36%)' : 'hsl(0, 84%, 60%)'} 
                      stopOpacity={0.4} 
                    />
                    <stop 
                      offset="95%" 
                      stopColor={isPositive ? 'hsl(142, 76%, 36%)' : 'hsl(0, 84%, 60%)'} 
                      stopOpacity={0} 
                    />
                  </linearGradient>
                </defs>
                <Area
                  type="monotone"
                  dataKey="price"
                  stroke={isPositive ? 'hsl(142, 76%, 36%)' : 'hsl(0, 84%, 60%)'}
                  strokeWidth={2}
                  fill="url(#sparklineGradient)"
                  dot={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {/* Volume & Activity */}
          <div className="hidden sm:flex flex-col items-end gap-1">
            <div className="flex items-center gap-1 text-sm text-muted-foreground">
              <Activity className="h-3 w-3" />
              <span>{summary.activeExchanges} markets</span>
            </div>
            <div className="text-xs text-muted-foreground">
              Vol: ${summary.totalVolume >= 1_000_000 
                ? `${(summary.totalVolume / 1_000_000).toFixed(1)}M` 
                : summary.totalVolume >= 1_000 
                  ? `${(summary.totalVolume / 1_000).toFixed(1)}K`
                  : summary.totalVolume.toFixed(0)
              }
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}