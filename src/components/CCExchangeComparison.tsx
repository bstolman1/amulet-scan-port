import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useCCMarketOverview, useKaikoStatus } from "@/hooks/use-kaiko-ohlcv";
import { Building2, ArrowUpDown, AlertTriangle, TrendingUp, TrendingDown } from "lucide-react";
import { useState, useMemo } from "react";
import { Progress } from "@/components/ui/progress";

type SortKey = 'volume' | 'price' | 'change24h' | 'tradeCount';

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

interface CCExchangeComparisonProps {
  enabled?: boolean;
}

export function CCExchangeComparison({ enabled = true }: CCExchangeComparisonProps) {
  const { data, isLoading, error } = useCCMarketOverview(enabled);
  const kaikoStatus = useKaikoStatus();

  const [sortKey, setSortKey] = useState<SortKey>('volume');
  const [sortAsc, setSortAsc] = useState(false);

  // Filter out exchanges with no actual trading data (volume 0 or no price)
  const validExchanges = useMemo(() => {
    if (!data?.exchanges) return [];
    return data.exchanges.filter((e) => e.volume > 0 && e.tradeCount > 0 && e.price !== null);
  }, [data?.exchanges]);

  const sortedExchanges = useMemo(() => {
    if (!validExchanges.length) return [];

    return [...validExchanges].sort((a, b) => {
      let aVal: number, bVal: number;

      switch (sortKey) {
        case 'volume':
          aVal = a.volume || 0;
          bVal = b.volume || 0;
          break;
        case 'price':
          aVal = a.price || 0;
          bVal = b.price || 0;
          break;
        case 'change24h':
          aVal = a.change24h || 0;
          bVal = b.change24h || 0;
          break;
        case 'tradeCount':
          aVal = a.tradeCount || 0;
          bVal = b.tradeCount || 0;
          break;
        default:
          return 0;
      }

      return sortAsc ? aVal - bVal : bVal - aVal;
    });
  }, [validExchanges, sortKey, sortAsc]);

  const maxVolume = useMemo(() => {
    if (!validExchanges.length) return 0;
    return Math.max(...validExchanges.map((e) => e.volume || 0));
  }, [validExchanges]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortAsc(!sortAsc);
    } else {
      setSortKey(key);
      setSortAsc(false);
    }
  };

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Building2 className="h-5 w-5" />
            CC Exchange Comparison
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error || !data) {
    const statusConfigured = kaikoStatus.data?.configured;

    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Building2 className="h-5 w-5" />
            CC Exchange Comparison
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-start gap-3 rounded-lg border bg-muted/30 p-4">
            <div className="mt-0.5 text-muted-foreground">
              <AlertTriangle className="h-4 w-4" />
            </div>
            <div className="min-w-0">
              <p className="font-medium">Unable to load exchange data</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {statusConfigured === false
                  ? "Your local API server isn't configured with KAIKO_API_KEY. Set it in server/.env and restart the server."
                  : (error?.message || "Request failed. Make sure the local API server is running on http://localhost:3001.")}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  const SortableHeader = ({ label, sortKeyName }: { label: string; sortKeyName: SortKey }) => (
    <TableHead
      className="cursor-pointer hover:bg-muted/50 transition-colors text-right"
      onClick={() => handleSort(sortKeyName)}
    >
      <div className="flex items-center justify-end gap-1">
        {label}
        <ArrowUpDown
          className={`h-3 w-3 ${sortKey === sortKeyName ? 'text-primary' : 'text-muted-foreground'}`}
        />
      </div>
    </TableHead>
  );


  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Building2 className="h-5 w-5" />
          CC Exchange Comparison
          <Badge variant="secondary" className="ml-2">
            {sortedExchanges.length} Markets
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Exchange</TableHead>
                <TableHead>Pair</TableHead>
                <TableHead>Type</TableHead>
                <SortableHeader label="Price" sortKeyName="price" />
                <SortableHeader label="24h Change" sortKeyName="change24h" />
                <SortableHeader label="Volume" sortKeyName="volume" />
                <TableHead className="w-[150px]">Volume Share</TableHead>
                <SortableHeader label="Trades" sortKeyName="tradeCount" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {sortedExchanges.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center text-muted-foreground py-8">
                    No exchange data available
                  </TableCell>
                </TableRow>
              ) : (
                sortedExchanges.map((exchange, idx) => {
                  const isPositive = exchange.change24h !== null && exchange.change24h >= 0;
                  const volumePercent = maxVolume > 0 ? (exchange.volume / maxVolume) * 100 : 0;
                  
                  return (
                    <TableRow key={`${exchange.exchange}-${exchange.instrument}`}>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          {idx < 3 && (
                            <Badge variant="outline" className="text-xs px-1.5 py-0 h-5 bg-primary/10 text-primary border-primary/30">
                              #{idx + 1}
                            </Badge>
                          )}
                          <span className="font-medium">{exchange.exchangeName}</span>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="secondary" className="font-mono">
                          {exchange.instrument.toUpperCase()}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge 
                          variant={exchange.instrumentClass === 'spot' ? 'outline' : 'default'}
                          className="text-xs"
                        >
                          {exchange.instrumentClass === 'perpetual-future' ? 'Perp' : 'Spot'}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        ${formatCurrency(exchange.price)}
                      </TableCell>
                      <TableCell className="text-right">
                        {exchange.change24h !== null ? (
                          <div className={`flex items-center justify-end gap-1 ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                            {isPositive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                            <span className="font-mono">
                              {isPositive ? '+' : ''}{exchange.change24h.toFixed(2)}%
                            </span>
                          </div>
                        ) : (
                          <span className="text-muted-foreground">-</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {formatVolume(exchange.volume)}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Progress value={volumePercent} className="h-2 flex-1" />
                          <span className="text-xs text-muted-foreground w-10 text-right">
                            {volumePercent.toFixed(0)}%
                          </span>
                        </div>
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {exchange.tradeCount.toLocaleString()}
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}