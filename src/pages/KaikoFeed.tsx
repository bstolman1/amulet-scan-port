import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useKaikoOHLCV, useKaikoStatus, KaikoCandle } from "@/hooks/use-kaiko-ohlcv";
import { TrendingUp, TrendingDown, Activity, BarChart3, AlertCircle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

const EXCHANGES = [
  { value: 'cbse', label: 'Coinbase' },
  { value: 'bfnx', label: 'Bitfinex' },
  { value: 'krkn', label: 'Kraken' },
  { value: 'binc', label: 'Binance' },
];

const INSTRUMENTS = [
  { value: 'btc-usd', label: 'BTC/USD' },
  { value: 'eth-usd', label: 'ETH/USD' },
  { value: 'sol-usd', label: 'SOL/USD' },
  { value: 'btc-usdt', label: 'BTC/USDT' },
  { value: 'eth-usdt', label: 'ETH/USDT' },
];

const INTERVALS = [
  { value: '1m', label: '1 Minute' },
  { value: '5m', label: '5 Minutes' },
  { value: '15m', label: '15 Minutes' },
  { value: '1h', label: '1 Hour' },
  { value: '4h', label: '4 Hours' },
  { value: '1d', label: '1 Day' },
];

function formatPrice(value: string | null): string {
  if (!value) return '-';
  const num = parseFloat(value);
  return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatVolume(value: string): string {
  const num = parseFloat(value);
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
  if (num >= 1_000) return `${(num / 1_000).toFixed(2)}K`;
  return num.toFixed(2);
}

function formatTimestamp(ts: number): string {
  return new Date(ts).toLocaleString();
}

function PriceChange({ open, close }: { open: string | null; close: string | null }) {
  if (!open || !close) return <span className="text-muted-foreground">-</span>;
  
  const openNum = parseFloat(open);
  const closeNum = parseFloat(close);
  const change = ((closeNum - openNum) / openNum) * 100;
  const isPositive = change >= 0;

  return (
    <div className={`flex items-center gap-1 ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
      {isPositive ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
      <span>{isPositive ? '+' : ''}{change.toFixed(2)}%</span>
    </div>
  );
}

function StatCard({ title, value, icon: Icon }: { title: string; value: string; icon: React.ElementType }) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">{title}</CardTitle>
        <Icon className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value}</div>
      </CardContent>
    </Card>
  );
}

export default function KaikoFeed() {
  const [exchange, setExchange] = useState('cbse');
  const [instrument, setInstrument] = useState('btc-usd');
  const [interval, setInterval] = useState('1h');

  const { data: status } = useKaikoStatus();
  const { data, isLoading, error, refetch, isFetching } = useKaikoOHLCV({
    exchange,
    instrument,
    interval,
    pageSize: 50,
  }, status?.configured);

  const candles = data?.data || [];
  const latestCandle = candles[0];

  // Calculate stats from candles
  const totalVolume = candles.reduce((sum, c) => sum + parseFloat(c.volume || '0'), 0);
  const totalTrades = candles.reduce((sum, c) => sum + c.count, 0);
  const avgVWAP = candles.filter(c => c.price).length > 0
    ? candles.reduce((sum, c) => sum + parseFloat(c.price || '0'), 0) / candles.filter(c => c.price).length
    : 0;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h1 className="text-3xl font-bold">Kaiko Feed</h1>
            <p className="text-muted-foreground">Trade Count, OHLCV & VWAP Market Data</p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching}
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </div>

        {!status?.configured && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>API Key Not Configured</AlertTitle>
            <AlertDescription>
              Add KAIKO_API_KEY to your server/.env file to enable the Kaiko data feed.
            </AlertDescription>
          </Alert>
        )}

        {/* Filters */}
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Filters</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-4">
              <div className="space-y-1">
                <label className="text-sm text-muted-foreground">Exchange</label>
                <Select value={exchange} onValueChange={setExchange}>
                  <SelectTrigger className="w-[140px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {EXCHANGES.map((ex) => (
                      <SelectItem key={ex.value} value={ex.value}>{ex.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <label className="text-sm text-muted-foreground">Instrument</label>
                <Select value={instrument} onValueChange={setInstrument}>
                  <SelectTrigger className="w-[140px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {INSTRUMENTS.map((inst) => (
                      <SelectItem key={inst.value} value={inst.value}>{inst.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-1">
                <label className="text-sm text-muted-foreground">Interval</label>
                <Select value={interval} onValueChange={setInterval}>
                  <SelectTrigger className="w-[140px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {INTERVALS.map((int) => (
                      <SelectItem key={int.value} value={int.value}>{int.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Stats */}
        {status?.configured && (
          <div className="grid gap-4 md:grid-cols-4">
            <StatCard
              title="Latest Close"
              value={isLoading ? '...' : `$${formatPrice(latestCandle?.close)}`}
              icon={Activity}
            />
            <StatCard
              title="Avg VWAP"
              value={isLoading ? '...' : `$${formatPrice(String(avgVWAP))}`}
              icon={BarChart3}
            />
            <StatCard
              title="Total Volume"
              value={isLoading ? '...' : formatVolume(String(totalVolume))}
              icon={TrendingUp}
            />
            <StatCard
              title="Trade Count"
              value={isLoading ? '...' : totalTrades.toLocaleString()}
              icon={Activity}
            />
          </div>
        )}

        {/* Data Table */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <BarChart3 className="h-5 w-5" />
              OHLCV Data
              {data?.query && (
                <Badge variant="secondary" className="ml-2">
                  {data.query.exchange.toUpperCase()} / {data.query.instrument.toUpperCase()}
                </Badge>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {error && (
              <Alert variant="destructive" className="mb-4">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>{error.message}</AlertDescription>
              </Alert>
            )}

            {isLoading ? (
              <div className="space-y-2">
                {Array.from({ length: 10 }).map((_, i) => (
                  <Skeleton key={i} className="h-12 w-full" />
                ))}
              </div>
            ) : (
              <div className="rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Time</TableHead>
                      <TableHead className="text-right">Open</TableHead>
                      <TableHead className="text-right">High</TableHead>
                      <TableHead className="text-right">Low</TableHead>
                      <TableHead className="text-right">Close</TableHead>
                      <TableHead className="text-right">Change</TableHead>
                      <TableHead className="text-right">Volume</TableHead>
                      <TableHead className="text-right">VWAP</TableHead>
                      <TableHead className="text-right">Trades</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {candles.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={9} className="text-center text-muted-foreground py-8">
                          No data available
                        </TableCell>
                      </TableRow>
                    ) : (
                      candles.map((candle, idx) => (
                        <TableRow key={candle.timestamp}>
                          <TableCell className="font-mono text-sm">
                            {formatTimestamp(candle.timestamp)}
                          </TableCell>
                          <TableCell className="text-right font-mono">
                            ${formatPrice(candle.open)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-green-500">
                            ${formatPrice(candle.high)}
                          </TableCell>
                          <TableCell className="text-right font-mono text-red-500">
                            ${formatPrice(candle.low)}
                          </TableCell>
                          <TableCell className="text-right font-mono font-semibold">
                            ${formatPrice(candle.close)}
                          </TableCell>
                          <TableCell className="text-right">
                            <PriceChange open={candle.open} close={candle.close} />
                          </TableCell>
                          <TableCell className="text-right font-mono">
                            {formatVolume(candle.volume)}
                          </TableCell>
                          <TableCell className="text-right font-mono">
                            ${formatPrice(candle.price)}
                          </TableCell>
                          <TableCell className="text-right font-mono">
                            {candle.count.toLocaleString()}
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}
