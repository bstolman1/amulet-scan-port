import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { useCCMarketOverview } from "@/hooks/use-kaiko-ohlcv";
import { TrendingUp, TrendingDown, Building2, ArrowUpDown } from "lucide-react";
import { useState, useMemo } from "react";
import { Progress } from "@/components/ui/progress";

type SortKey = "volume" | "price" | "change24h" | "tradeCount";

function safeNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function formatCurrency(value: number | null | undefined): string {
  const num = safeNumber(value, NaN);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function formatVolume(value: number | null | undefined): string {
  const num = safeNumber(value, 0);
  if (num >= 1_000_000_000) return `$${(num / 1_000_000_000).toFixed(2)}B`;
  if (num >= 1_000_000) return `$${(num / 1_000_000).toFixed(2)}M`;
  if (num >= 1_000) return `$${(num / 1_000).toFixed(2)}K`;
  return `$${num.toFixed(2)}`;
}

function formatInteger(value: unknown): string {
  const num = safeNumber(value, NaN);
  return Number.isFinite(num) ? num.toLocaleString() : "0";
}

interface CCExchangeComparisonProps {
  enabled?: boolean;
}

export function CCExchangeComparison({ enabled = true }: CCExchangeComparisonProps) {
  const { data, isLoading, error } = useCCMarketOverview(enabled);
  const [sortKey, setSortKey] = useState<SortKey>("volume");
  const [sortAsc, setSortAsc] = useState(false);

  const validExchanges = useMemo(() => {
    if (!data?.exchanges) return [];
    return data.exchanges.filter((e) =>
      safeNumber(e.volume, 0) > 0 &&
      safeNumber(e.tradeCount, 0) > 0 &&
      e.price !== null &&
      e.price !== undefined
    );
  }, [data?.exchanges]);

  const sortedExchanges = useMemo(() => {
    if (!validExchanges.length) return [];

    return [...validExchanges].sort((a, b) => {
      let aVal = 0;
      let bVal = 0;

      switch (sortKey) {
        case "volume":
          aVal = safeNumber(a.volume, 0);
          bVal = safeNumber(b.volume, 0);
          break;
        case "price":
          aVal = safeNumber(a.price, 0);
          bVal = safeNumber(b.price, 0);
          break;
        case "change24h":
          aVal = safeNumber(a.change24h, 0);
          bVal = safeNumber(b.change24h, 0);
          break;
        case "tradeCount":
          aVal = safeNumber(a.tradeCount, 0);
          bVal = safeNumber(b.tradeCount, 0);
          break;
      }

      return sortAsc ? aVal - bVal : bVal - aVal;
    });
  }, [validExchanges, sortKey, sortAsc]);

  const maxVolume = useMemo(() => {
    if (!validExchanges.length) return 0;
    return Math.max(...validExchanges.map((e) => safeNumber(e.volume, 0)));
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
        <ArrowUpDown
          className={`h-3 w-3 ${sortKey === sortKeyName ? "text-primary" : "text-muted-foreground"}`}
        />
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
                  const change24h = exchange.change24h;
                  const isPositive = typeof change24h === "number" && Number.isFinite(change24h) ? change24h >= 0 : false;
                  const volume = safeNumber(exchange.volume, 0);
                  const volumePercent = maxVolume > 0 ? (volume / maxVolume) * 100 : 0;

                  return (
                    <TableRow key={`${exchange.exchange}-${exchange.instrument}`}>
                      <TableCell className="px-4">
                        <div className="flex items-center gap-2">
                          {idx < 3 && (
                            <Badge
                              variant="outline"
                              className="text-xs px-1.5 py-0 h-5 bg-primary/10 text-primary border-primary/30 shrink-0"
                            >
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
                          variant={exchange.instrumentClass === "spot" ? "outline" : "default"}
                          className="text-xs whitespace-nowrap"
                        >
                          {exchange.instrumentClass === "perpetual-future" ? "Perp" : "Spot"}
                        </Badge>
                      </TableCell>

                      <TableCell className="text-right font-mono px-4 whitespace-nowrap">
                        ${formatCurrency(exchange.price)}
                      </TableCell>

                      <TableCell className="text-right px-4">
                        {typeof change24h === "number" && Number.isFinite(change24h) ? (
                          <div
                            className={`flex items-center justify-end gap-1 whitespace-nowrap ${
                              isPositive ? "text-green-500" : "text-red-500"
                            }`}
                          >
                            {isPositive ? (
                              <TrendingUp className="h-3 w-3 shrink-0" />
                            ) : (
                              <TrendingDown className="h-3 w-3 shrink-0" />
                            )}
                            <span className="font-mono">
                              {isPositive ? "+" : ""}
                              {change24h.toFixed(2)}%
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
                            {Number.isFinite(volumePercent) ? volumePercent.toFixed(0) : "0"}%
                          </span>
                        </div>
                      </TableCell>

                      <TableCell className="text-right font-mono px-4 whitespace-nowrap">
                        {formatInteger(exchange.tradeCount)}
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
