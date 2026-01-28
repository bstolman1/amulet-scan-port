import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useCCMarketOverview, CCExchangeData } from "@/hooks/use-kaiko-ohlcv";
import { TrendingUp, TrendingDown, Building2, ArrowUpDown } from "lucide-react";
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
  const [sortKey, setSortKey] = useState<SortKey>('volume');
  const [sortAsc, setSortAsc] = useState(false);

  // Filter out exchanges with no actual trading data (volume 0 or no price)
  const validExchanges = useMemo(() => {
    if (!data?.exchanges) return [];
    return data.exchanges.filter(e => 
      e.volume > 0 && e.tradeCount > 0 && e.price !== null
    );
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
    return Math.max(...validExchanges.map(e => e.volume || 0));
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
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Building2 className="h-5 w-5" />
            CC Exchange Comparison
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground">Unable to load exchange data</p>
        </CardContent>
      </Card>
    );
  }

  const SortableHeader = ({ label, sortKeyName }: { label: string; sortKeyName: SortKey }) => (
    <TableHead 
      className="cursor-pointer hover:bg-muted/50 transition-colors text-right whitespace-nowrap px-4"
      onClick={() => handleSort(sortKeyName)}
    >
      <div className="flex items-center justify-end gap-1">
        {label}
        <ArrowUpDown className={`h-3 w-3 ${sortKey === sortKeyName ? 'text-primary' : 'text-muted-foreground'}`} />
      </div>
    </TableHead>
  );

  return (
    <Card>
      <CardHeader className="pb-4">
        <CardTitle className="flex items-center gap-2">
          <Building2 className="h-5 w-5" />
          CC Exchange Comparison
          <Badge variant="secondary" className="ml-2">
            {sortedExchanges.length} Markets
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="rounded-md border overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="whitespace-nowrap px-4 min-w-[140px]">Exchange</TableHead>
                <TableHead className="whitespace-nowrap px-4 min-w-[100px]">Pair</TableHead>
                <TableHead className="whitespace-nowrap px-4 min-w-[70px]">Type</TableHead>
                <SortableHeader label="Price" sortKeyName="price" />
                <SortableHeader label="24h Change" sortKeyName="change24h" />
                <SortableHeader label="Volume" sortKeyName="volume" />
                <TableHead className="whitespace-nowrap px-4 min-w-[180px]">Volume Share</TableHead>
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
                      <TableCell className="px-4">
                        <div className="flex items-center gap-2">
                          {idx < 3 && (
                            <Badge variant="outline" className="text-xs px-1.5 py-0 h-5 bg-primary/10 text-primary border-primary/30 shrink-0">
                              #{idx + 1}
                            </Badge>
                          )}
                          <span className="font-medium truncate">{exchange.exchangeName}</span>
                        </div>
                      </TableCell>
                      <TableCell className="px-4">
                        <Badge variant="secondary" className="font-mono text-xs">
                          {exchange.instrument.toUpperCase()}
                        </Badge>
                      </TableCell>
                      <TableCell className="px-4">
                        <Badge 
                          variant={exchange.instrumentClass === 'spot' ? 'outline' : 'default'}
                          className="text-xs whitespace-nowrap"
                        >
                          {exchange.instrumentClass === 'perpetual-future' ? 'Perp' : 'Spot'}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right font-mono px-4 whitespace-nowrap">
                        ${formatCurrency(exchange.price)}
                      </TableCell>
                      <TableCell className="text-right px-4">
                        {exchange.change24h !== null ? (
                          <div className={`flex items-center justify-end gap-1 whitespace-nowrap ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
                            {isPositive ? <TrendingUp className="h-3 w-3 shrink-0" /> : <TrendingDown className="h-3 w-3 shrink-0" />}
                            <span className="font-mono">
                              {isPositive ? '+' : ''}{exchange.change24h.toFixed(2)}%
                            </span>
                          </div>
                        ) : (
                          <span className="text-muted-foreground">-</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right font-mono px-4 whitespace-nowrap">
                        {formatVolume(exchange.volume)}
                      </TableCell>
                      <TableCell className="px-4">
                        <div className="flex items-center gap-3 min-w-[140px]">
                          <Progress value={volumePercent} className="h-2 flex-1" />
                          <span className="text-xs text-muted-foreground w-10 text-right shrink-0">
                            {volumePercent.toFixed(0)}%
                          </span>
                        </div>
                      </TableCell>
                      <TableCell className="text-right font-mono px-4 whitespace-nowrap">
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