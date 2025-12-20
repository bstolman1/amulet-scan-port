import { useState, useMemo, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useKaikoOHLCV, useKaikoStatus, useKaikoAssetMetrics, KaikoCandle, AssetMetricData } from "@/hooks/use-kaiko-ohlcv";
import { TrendingUp, TrendingDown, Activity, BarChart3, AlertCircle, RefreshCw, Coins, Users, Database, Building2, Bell } from "lucide-react";
import { Button } from "@/components/ui/button";
import { CCPriceChart } from "@/components/CCPriceChart";
import { CCMarketOverview } from "@/components/CCMarketOverview";
import { CCExchangeComparison } from "@/components/CCExchangeComparison";
import { CCCandlestickChart } from "@/components/CCCandlestickChart";
import { CCPriceTicker } from "@/components/CCPriceTicker";
import { CCPriceAlerts } from "@/components/CCPriceAlerts";
import { CCTimeframeComparison } from "@/components/CCTimeframeComparison";

// Exchange to instrument mapping based on Kaiko reference data
const EXCHANGE_INSTRUMENTS: Record<string, string[]> = {
  // CC Spot exchanges
  krkn: ['cc-usd', 'cc-usdt', 'cc-usdc', 'cc-eur', 'btc-usd', 'eth-usd', 'sol-usd'],
  gate: ['cc-usdt', 'btc-usdt', 'eth-usdt'],
  kcon: ['cc-usdt', 'btc-usdt', 'eth-usdt'],
  mexc: ['cc-usdt', 'cc-usdc', 'btc-usdt', 'eth-usdt'],
  bbsp: ['cc-usdt', 'cc-usdc', 'btc-usdt', 'eth-usdt'],
  hitb: ['cc-usdt', 'btc-usdt', 'eth-usdt'],
  cnex: ['cc-usdt', 'btc-usdt', 'eth-usdt'],
  // CC Derivatives
  binc: ['cc-usdt', 'btc-usdt', 'eth-usdt', 'sol-usdt'],
  okex: ['cc-usdt', 'btc-usdt', 'eth-usdt'],
  gtdm: ['cc-usdt', 'btc-usdt'],
  bbit: ['cc-usdt', 'btc-usdt', 'eth-usdt'],
  hbdm: ['cc-usdt', 'btc-usdt'],
  // Other exchanges (non-CC)
  cbse: ['btc-usd', 'eth-usd', 'sol-usd', 'btc-usdt', 'eth-usdt'],
  bfnx: ['btc-usd', 'eth-usd', 'btc-usdt', 'eth-usdt'],
  stmp: ['btc-usd', 'eth-usd', 'xrp-usd'],
  gmni: ['btc-usd', 'eth-usd'],
  huob: ['btc-usdt', 'eth-usdt'],
  polo: ['btc-usdt', 'eth-usdt'],
  btrx: ['btc-usdt', 'eth-usdt'],
  upbt: ['btc-usdt', 'eth-usdt'],
  bthb: ['btc-usdt', 'eth-usdt'],
  bybt: ['btc-usdt', 'eth-usdt'],
  drbt: ['btc-usd', 'eth-usd'],
  bvav: ['btc-usd', 'eth-usd'],
  bull: ['btc-usd', 'eth-usd'],
  whbt: ['btc-usdt', 'eth-usdt'],
  // DEXs
  usp3: ['eth-usdt', 'eth-usd'],
  usp2: ['eth-usdt'],
  sush: ['eth-usdt'],
  curv: ['eth-usdt'],
  blc2: ['eth-usdt'],
  pksp: ['eth-usdt', 'btc-usdt'],
  orca: ['sol-usd'],
  raya: ['sol-usd'],
  btmx: ['btc-usd', 'eth-usd'],
  dydx: ['btc-usd', 'eth-usd'],
};

// All available Kaiko exchanges
const EXCHANGES = [
  // Major exchanges with CC trading pairs (prioritized)
  { value: 'krkn', label: 'Kraken', hasCC: true },
  { value: 'binc', label: 'Binance', hasCC: true },
  { value: 'bbsp', label: 'Bybit Spot', hasCC: true },
  { value: 'gate', label: 'Gate.io', hasCC: true },
  { value: 'kcon', label: 'KuCoin', hasCC: true },
  { value: 'mexc', label: 'MEXC', hasCC: true },
  { value: 'okex', label: 'OKX', hasCC: true },
  { value: 'hitb', label: 'HitBTC', hasCC: true },
  { value: 'cnex', label: 'CoinEx', hasCC: true },
  // Other major exchanges
  { value: 'cbse', label: 'Coinbase' },
  { value: 'bfnx', label: 'Bitfinex' },
  { value: 'stmp', label: 'Bitstamp' },
  { value: 'gmni', label: 'Gemini' },
  { value: 'huob', label: 'Huobi' },
  { value: 'polo', label: 'Poloniex' },
  { value: 'btrx', label: 'Bittrex' },
  { value: 'upbt', label: 'UPbit' },
  { value: 'bthb', label: 'Bithumb' },
  { value: 'bybt', label: 'Bybit' },
  { value: 'drbt', label: 'Deribit' },
  { value: 'bvav', label: 'Bitvavo' },
  { value: 'bull', label: 'Bullish' },
  { value: 'whbt', label: 'WhiteBIT' },
  // DEXs and DeFi
  { value: 'usp3', label: 'Uniswap V3' },
  { value: 'usp2', label: 'Uniswap V2' },
  { value: 'sush', label: 'Sushiswap' },
  { value: 'curv', label: 'Curve' },
  { value: 'blc2', label: 'Balancer V2' },
  { value: 'pksp', label: 'Pancakeswap' },
  { value: 'orca', label: 'Orca' },
  { value: 'raya', label: 'Raydium' },
  // Derivative markets
  { value: 'gtdm', label: 'Gate.io Derivatives', hasCC: true },
  { value: 'hbdm', label: 'Huobi Derivatives', hasCC: true },
  { value: 'bbit', label: 'Bybit Perps', hasCC: true },
  { value: 'btmx', label: 'BitMEX' },
  { value: 'dydx', label: 'dYdX' },
];

// All instruments with labels
const ALL_INSTRUMENTS: Record<string, { label: string; isCC?: boolean }> = {
  'cc-usd': { label: 'CC/USD', isCC: true },
  'cc-usdt': { label: 'CC/USDT', isCC: true },
  'cc-usdc': { label: 'CC/USDC', isCC: true },
  'cc-eur': { label: 'CC/EUR', isCC: true },
  'btc-usd': { label: 'BTC/USD' },
  'btc-usdt': { label: 'BTC/USDT' },
  'eth-usd': { label: 'ETH/USD' },
  'eth-usdt': { label: 'ETH/USDT' },
  'sol-usd': { label: 'SOL/USD' },
  'sol-usdt': { label: 'SOL/USDT' },
  'xrp-usd': { label: 'XRP/USD' },
  'ada-usd': { label: 'ADA/USD' },
};

const INTERVALS = [
  { value: '1m', label: '1 Minute' },
  { value: '5m', label: '5 Minutes' },
  { value: '15m', label: '15 Minutes' },
  { value: '1h', label: '1 Hour' },
  { value: '4h', label: '4 Hours' },
  { value: '1d', label: '1 Day' },
];

// Assets including Canton Coin
const ASSETS = [
  { value: 'cc', label: 'Canton Coin (CC)', isCC: true },
  { value: 'btc', label: 'Bitcoin (BTC)' },
  { value: 'eth', label: 'Ethereum (ETH)' },
  { value: 'sol', label: 'Solana (SOL)' },
  { value: 'usdt', label: 'Tether (USDT)' },
  { value: 'usdc', label: 'USD Coin (USDC)' },
  { value: 'xrp', label: 'XRP' },
  { value: 'ada', label: 'Cardano (ADA)' },
  { value: 'matic', label: 'Polygon (MATIC)' },
];

const ASSET_INTERVALS = [
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
  // Default to Kraken + CC/USD for Canton Coin
  const [exchange, setExchange] = useState('krkn');
  const [instrument, setInstrument] = useState('cc-usd');
  const [interval, setInterval] = useState('1h');
  const [activeTab, setActiveTab] = useState('cc-overview');
  
  // Asset Metrics state - default to Canton Coin
  const [asset, setAsset] = useState('cc');
  const [assetInterval, setAssetInterval] = useState('1h');

  // Get available instruments for the selected exchange
  const availableInstruments = useMemo(() => {
    const instruments = EXCHANGE_INSTRUMENTS[exchange] || [];
    return instruments.map(code => ({
      value: code,
      label: ALL_INSTRUMENTS[code]?.label || code.toUpperCase(),
      isCC: ALL_INSTRUMENTS[code]?.isCC || false,
    }));
  }, [exchange]);

  // When exchange changes, update instrument if current one isn't available
  useEffect(() => {
    const available = EXCHANGE_INSTRUMENTS[exchange] || [];
    if (!available.includes(instrument)) {
      // Pick first CC pair if available, otherwise first pair
      const ccPair = available.find(i => i.startsWith('cc-'));
      setInstrument(ccPair || available[0] || 'btc-usd');
    }
  }, [exchange, instrument]);

  const { data: status } = useKaikoStatus();
  const { data, isLoading, error, refetch, isFetching } = useKaikoOHLCV({
    exchange,
    instrument,
    interval,
    pageSize: 50,
  }, status?.configured && (activeTab === 'ohlcv' || activeTab === 'cc-overview'));

  const { 
    data: assetData, 
    isLoading: assetLoading, 
    error: assetError, 
    refetch: refetchAsset, 
    isFetching: assetFetching 
  } = useKaikoAssetMetrics({
    asset,
    interval: assetInterval,
    pageSize: 50,
  }, status?.configured && activeTab === 'assets');

  const candles = data?.data || [];
  const latestCandle = candles[0];

  // Calculate stats from candles
  const totalVolume = candles.reduce((sum, c) => sum + parseFloat(c.volume || '0'), 0);
  const totalTrades = candles.reduce((sum, c) => sum + c.count, 0);
  const avgVWAP = candles.filter(c => c.price).length > 0
    ? candles.reduce((sum, c) => sum + parseFloat(c.price || '0'), 0) / candles.filter(c => c.price).length
    : 0;

  // Asset metrics data
  const assetMetrics = assetData?.data || [];
  const latestMetric = assetMetrics[0];

  const handleRefresh = () => {
    if (activeTab === 'ohlcv') {
      refetch();
    } else {
      refetchAsset();
    }
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h1 className="text-3xl font-bold">Kaiko Feed</h1>
            <p className="text-muted-foreground">Market Data: OHLCV, Asset Metrics & Analytics</p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={handleRefresh}
            disabled={isFetching || assetFetching}
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${(isFetching || assetFetching) ? 'animate-spin' : ''}`} />
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

        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
          <TabsList>
            <TabsTrigger value="cc-overview" className="gap-2">
              <Coins className="h-4 w-4" />
              CC Overview
            </TabsTrigger>
            <TabsTrigger value="ohlcv" className="gap-2">
              <BarChart3 className="h-4 w-4" />
              OHLCV
            </TabsTrigger>
            <TabsTrigger value="assets" className="gap-2">
              <Activity className="h-4 w-4" />
              Asset Metrics
            </TabsTrigger>
            <TabsTrigger value="exchanges" className="gap-2">
              <Building2 className="h-4 w-4" />
              Exchanges
            </TabsTrigger>
          </TabsList>

          {/* CC Overview Tab */}
          <TabsContent value="cc-overview" className="space-y-6">
            <CCPriceTicker enabled={status?.configured && activeTab === 'cc-overview'} />
            
            <CCTimeframeComparison enabled={status?.configured && activeTab === 'cc-overview'} />
            
            <CCMarketOverview enabled={status?.configured && activeTab === 'cc-overview'} />
            
            <CCCandlestickChart 
              candles={candles} 
              isLoading={isLoading}
              exchange={exchange}
              instrument={instrument}
              onExchangeChange={(newExchange, newInstrument) => {
                setExchange(newExchange);
                setInstrument(newInstrument);
              }}
            />
            
            <CCPriceAlerts enabled={status?.configured && activeTab === 'cc-overview'} />
            
            <CCExchangeComparison enabled={status?.configured && activeTab === 'cc-overview'} />
          </TabsContent>

          {/* OHLCV Tab */}
          <TabsContent value="ohlcv" className="space-y-6">
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
                      <SelectTrigger className="w-[180px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent className="max-h-[300px]">
                        <SelectItem value="__cc_header" disabled className="text-xs text-primary font-semibold">
                          — CC Trading Venues —
                        </SelectItem>
                        {EXCHANGES.filter(ex => ex.hasCC).map((ex) => (
                          <SelectItem key={ex.value} value={ex.value}>
                            <span className="flex items-center gap-2">
                              {ex.label}
                              <Badge variant="outline" className="text-[10px] px-1 py-0 h-4 text-primary border-primary">CC</Badge>
                            </span>
                          </SelectItem>
                        ))}
                        <SelectItem value="__other_header" disabled className="text-xs text-muted-foreground font-semibold mt-2">
                          — Other Exchanges —
                        </SelectItem>
                        {EXCHANGES.filter(ex => !ex.hasCC).map((ex) => (
                          <SelectItem key={ex.value} value={ex.value}>{ex.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm text-muted-foreground">
                      Instrument 
                      <span className="text-xs text-muted-foreground/70 ml-1">
                        ({availableInstruments.length} available)
                      </span>
                    </label>
                    <Select value={instrument} onValueChange={setInstrument}>
                      <SelectTrigger className="w-[160px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent className="bg-background border z-50">
                        {availableInstruments.filter(inst => inst.isCC).length > 0 && (
                          <>
                            <SelectItem value="__cc_pairs" disabled className="text-xs text-primary font-semibold">
                              — Canton Coin —
                            </SelectItem>
                            {availableInstruments.filter(inst => inst.isCC).map((inst) => (
                              <SelectItem key={inst.value} value={inst.value}>
                                <span className="font-semibold text-primary">{inst.label}</span>
                              </SelectItem>
                            ))}
                          </>
                        )}
                        {availableInstruments.filter(inst => !inst.isCC).length > 0 && (
                          <>
                            <SelectItem value="__other_pairs" disabled className="text-xs text-muted-foreground font-semibold mt-2">
                              — Other Pairs —
                            </SelectItem>
                            {availableInstruments.filter(inst => !inst.isCC).map((inst) => (
                              <SelectItem key={inst.value} value={inst.value}>{inst.label}</SelectItem>
                            ))}
                          </>
                        )}
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
                          candles.map((candle) => (
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
          </TabsContent>

          {/* Asset Metrics Tab */}
          <TabsContent value="assets" className="space-y-6">
            {/* Asset Filters */}
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Asset Selection</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-4">
                  <div className="space-y-1">
                    <label className="text-sm text-muted-foreground">Asset</label>
                    <Select value={asset} onValueChange={setAsset}>
                      <SelectTrigger className="w-[180px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {ASSETS.filter(a => a.isCC).map((a) => (
                          <SelectItem key={a.value} value={a.value}>
                            <span className="font-semibold text-primary">{a.label}</span>
                          </SelectItem>
                        ))}
                        <SelectItem value="__other_assets" disabled className="text-xs text-muted-foreground font-semibold mt-2">
                          — Other Assets —
                        </SelectItem>
                        {ASSETS.filter(a => !a.isCC).map((a) => (
                          <SelectItem key={a.value} value={a.value}>{a.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-1">
                    <label className="text-sm text-muted-foreground">Interval</label>
                    <Select value={assetInterval} onValueChange={setAssetInterval}>
                      <SelectTrigger className="w-[140px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {ASSET_INTERVALS.map((int) => (
                          <SelectItem key={int.value} value={int.value}>{int.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Asset Stats */}
            {status?.configured && latestMetric && (
              <div className="grid gap-4 md:grid-cols-4">
                <StatCard
                  title="Price (USD)"
                  value={assetLoading ? '...' : `$${latestMetric.price?.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) || '-'}`}
                  icon={Coins}
                />
                <StatCard
                  title="Total Volume (USD)"
                  value={assetLoading ? '...' : formatVolume(String(latestMetric.total_volume_usd || 0))}
                  icon={TrendingUp}
                />
                <StatCard
                  title="Total Trades"
                  value={assetLoading ? '...' : latestMetric.total_trade_count?.toLocaleString() || '0'}
                  icon={Activity}
                />
                <StatCard
                  title="Off-Chain Volume"
                  value={assetLoading ? '...' : formatVolume(String(latestMetric.off_chain_liquidity_data?.total_off_chain_volume_usd || 0))}
                  icon={Database}
                />
              </div>
            )}

            {/* Asset Metrics Table */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Coins className="h-5 w-5" />
                  Asset Metrics
                  <Badge variant="secondary" className="ml-2">
                    {asset.toUpperCase()}
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {assetError && (
                  <Alert variant="destructive" className="mb-4">
                    <AlertCircle className="h-4 w-4" />
                    <AlertDescription>{assetError.message}</AlertDescription>
                  </Alert>
                )}

                {assetLoading ? (
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
                          <TableHead className="text-right">Price (USD)</TableHead>
                          <TableHead className="text-right">Volume (USD)</TableHead>
                          <TableHead className="text-right">Volume (Asset)</TableHead>
                          <TableHead className="text-right">Total Trades</TableHead>
                          <TableHead className="text-right">Off-Chain Vol</TableHead>
                          <TableHead className="text-right">On-Chain Vol</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {assetMetrics.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                              No data available
                            </TableCell>
                          </TableRow>
                        ) : (
                          assetMetrics.map((metric) => (
                            <TableRow key={metric.timestamp}>
                              <TableCell className="font-mono text-sm">
                                {new Date(metric.timestamp).toLocaleString()}
                              </TableCell>
                              <TableCell className="text-right font-mono font-semibold">
                                ${metric.price?.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) || '-'}
                              </TableCell>
                              <TableCell className="text-right font-mono">
                                {formatVolume(String(metric.total_volume_usd || 0))}
                              </TableCell>
                              <TableCell className="text-right font-mono">
                                {formatVolume(String(metric.total_volume_asset || 0))}
                              </TableCell>
                              <TableCell className="text-right font-mono">
                                {metric.total_trade_count?.toLocaleString() || '0'}
                              </TableCell>
                              <TableCell className="text-right font-mono text-blue-500">
                                {formatVolume(String(metric.off_chain_liquidity_data?.total_off_chain_volume_usd || 0))}
                              </TableCell>
                              <TableCell className="text-right font-mono text-purple-500">
                                {formatVolume(String(metric.on_chain_liquidity_data?.total_on_chain_volume_usd || 0))}
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

            {/* Exchange Breakdown */}
            {latestMetric?.off_chain_liquidity_data?.trade_data && latestMetric.off_chain_liquidity_data.trade_data.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Database className="h-5 w-5" />
                    Exchange Breakdown (Off-Chain)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Exchange</TableHead>
                          <TableHead className="text-right">Volume (USD)</TableHead>
                          <TableHead className="text-right">Volume (Asset)</TableHead>
                          <TableHead className="text-right">Trade Count</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {latestMetric.off_chain_liquidity_data.trade_data.map((trade) => (
                          <TableRow key={trade.exchange}>
                            <TableCell className="font-mono font-semibold">
                              {trade.exchange.toUpperCase()}
                            </TableCell>
                            <TableCell className="text-right font-mono">
                              {formatVolume(String(trade.volume_usd))}
                            </TableCell>
                            <TableCell className="text-right font-mono">
                              {formatVolume(String(trade.volume_asset))}
                            </TableCell>
                            <TableCell className="text-right font-mono">
                              {trade.trade_count.toLocaleString()}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            )}
          </TabsContent>

          {/* Exchanges Tab */}
          <TabsContent value="exchanges" className="space-y-6">
            <CCExchangeComparison enabled={status?.configured && activeTab === 'exchanges'} />
          </TabsContent>
        </Tabs>
      </div>
    </DashboardLayout>
  );
}
