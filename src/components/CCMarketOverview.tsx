import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { useCCMarketOverview } from "@/hooks/use-kaiko-ohlcv";
import { TrendingUp, TrendingDown, DollarSign, Activity, BarChart3, Building2 } from "lucide-react";

function formatCurrency(value: number | null): string {
  if (value === null) return '-';
  return value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
}

function formatVolume(value: number): string {
  if (value >= 1_000_000_000) return `$${(value / 1_000_000_000).toFixed(2)}B`;
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `$${(value / 1_000).toFixed(2)}K`;
  return `$${value.toFixed(2)}`;
}

function formatNumber(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
  return value.toLocaleString();
}

interface StatItemProps {
  label: string;
  value: string;
  icon: React.ReactNode;
  subValue?: React.ReactNode;
}

function StatItem({ label, value, icon, subValue }: StatItemProps) {
  return (
    <div className="flex items-start gap-3 p-4 rounded-lg bg-muted/50">
      <div className="p-2 rounded-md bg-primary/10 text-primary">
        {icon}
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm text-muted-foreground">{label}</p>
        <p className="text-xl font-bold truncate">{value}</p>
        {subValue}
      </div>
    </div>
  );
}

interface CCMarketOverviewProps {
  enabled?: boolean;
}

export function CCMarketOverview({ enabled = true }: CCMarketOverviewProps) {
  const { data, isLoading, error } = useCCMarketOverview(enabled);

  if (isLoading) {
    return (
      <Card className="col-span-full">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <DollarSign className="h-5 w-5 text-primary" />
            Canton Coin Market Overview
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <Skeleton key={i} className="h-24" />
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !data) {
    return (
      <Card className="col-span-full">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <DollarSign className="h-5 w-5 text-primary" />
            Canton Coin Market Overview
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">Unable to load market data</p>
        </CardContent>
      </Card>
    );
  }

  const { summary } = data;
  const isPositive = summary.change24h !== null && summary.change24h >= 0;

  return (
    <Card className="col-span-full border-primary/20 bg-gradient-to-br from-background to-muted/20">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <DollarSign className="h-5 w-5 text-primary" />
            Canton Coin Market Overview
            <Badge variant="outline" className="ml-2 text-primary border-primary">
              CC
            </Badge>
          </CardTitle>
          <span className="text-xs text-muted-foreground">
            Updated: {new Date(data.timestamp).toLocaleTimeString()}
          </span>
        </div>
      </CardHeader>
      <CardContent>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <StatItem
            label="Price (USD)"
            value={`$${formatCurrency(summary.price)}`}
            icon={<DollarSign className="h-5 w-5" />}
            subValue={
              summary.change24h !== null && (
                <div className={`flex items-center gap-1 text-sm ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                  {isPositive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                  <span>{isPositive ? '+' : ''}{summary.change24h.toFixed(2)}% (24h)</span>
                </div>
              )
            }
          />
          <StatItem
            label="VWAP"
            value={`$${formatCurrency(summary.vwap)}`}
            icon={<BarChart3 className="h-5 w-5" />}
            subValue={
              <p className="text-xs text-muted-foreground mt-1">Volume-Weighted Avg</p>
            }
          />
          <StatItem
            label="24h Volume"
            value={formatVolume(summary.totalVolume)}
            icon={<Activity className="h-5 w-5" />}
            subValue={
              <p className="text-xs text-muted-foreground mt-1">
                {formatNumber(summary.totalTrades)} trades
              </p>
            }
          />
          <StatItem
            label="Active Markets"
            value={`${summary.activeExchanges} venues`}
            icon={<Building2 className="h-5 w-5" />}
            subValue={
              <div className="flex flex-wrap gap-1 mt-1">
                {data.exchanges.slice(0, 3).map(ex => (
                  <Badge key={ex.exchange} variant="secondary" className="text-[10px] px-1">
                    {ex.exchangeName}
                  </Badge>
                ))}
                {data.exchanges.length > 3 && (
                  <Badge variant="secondary" className="text-[10px] px-1">
                    +{data.exchanges.length - 3}
                  </Badge>
                )}
              </div>
            }
          />
        </div>
      </CardContent>
    </Card>
  );
}