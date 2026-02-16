import { useState, useMemo } from "react";
import { subHours } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import { CalendarIcon, Clock, RefreshCw, Globe, Building2, ChevronDown, ChevronUp } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";
import { useKaikoTwap, useKaikoVwTwap } from "@/hooks/use-kaiko-twap";
import type { TwapCandle } from "@/hooks/use-kaiko-twap";

const TWAP_INTERVALS = [
  { value: '1m', label: '1 Min' },
  { value: '5m', label: '5 Min' },
  { value: '15m', label: '15 Min' },
  { value: '1h', label: '1 Hour' },
  { value: '4h', label: '4 Hours' },
];

const PRESET_RANGES = [
  { label: '1H', hours: 1 },
  { label: '4H', hours: 4 },
  { label: '12H', hours: 12 },
  { label: '24H', hours: 24 },
  { label: '7D', hours: 168 },
  { label: '30D', hours: 720 },
];

const EXCHANGES = [
  { value: 'krkn', label: 'Kraken', instrument: 'cc-usd' },
  { value: 'gate', label: 'Gate.io', instrument: 'cc-usdt' },
  { value: 'kcon', label: 'KuCoin', instrument: 'cc-usdt' },
  { value: 'mexc', label: 'MEXC', instrument: 'cc-usdt' },
  { value: 'bbsp', label: 'Bybit', instrument: 'cc-usdt' },
  { value: 'hitb', label: 'HitBTC', instrument: 'cc-usdt' },
  { value: 'cnex', label: 'CoinEx', instrument: 'cc-usdt' },
];

interface CCTwapCardProps {
  enabled?: boolean;
}

export function CCTwapCard({ enabled = true }: CCTwapCardProps) {
  const [mode, setMode] = useState<'single' | 'vw'>('single');
  const [selectedExchange, setSelectedExchange] = useState('krkn');
  const [twapInterval, setTwapInterval] = useState('5m');
  const [activePreset, setActivePreset] = useState<string>('24H');
  const [startDate, setStartDate] = useState<Date | undefined>(undefined);
  const [startHour, setStartHour] = useState<string>('0');
  const [endDate, setEndDate] = useState<Date | undefined>(undefined);
  const [endHour, setEndHour] = useState<string>('23');
  const [useCustomDates, setUseCustomDates] = useState(false);
  const [showCandles, setShowCandles] = useState(false);

  const UTC_HOURS = Array.from({ length: 24 }, (_, i) => String(i));

  const exchangeConfig = EXCHANGES.find(e => e.value === selectedExchange) || EXCHANGES[0];

  const { startTime, endTime } = useMemo(() => {
    if (useCustomDates && startDate && endDate) {
      const s = new Date(Date.UTC(startDate.getFullYear(), startDate.getMonth(), startDate.getDate(), parseInt(startHour, 10), 0, 0));
      const e = new Date(Date.UTC(endDate.getFullYear(), endDate.getMonth(), endDate.getDate(), parseInt(endHour, 10), 0, 0));
      return {
        startTime: s.toISOString(),
        endTime: e.toISOString(),
      };
    }
    const preset = PRESET_RANGES.find(p => p.label === activePreset);
    const now = new Date();
    return {
      startTime: subHours(now, preset?.hours || 24).toISOString(),
      endTime: now.toISOString(),
    };
  }, [useCustomDates, startDate, startHour, endDate, endHour, activePreset]);

  const singleParams = useMemo(() => ({
    exchange: selectedExchange,
    instrument: exchangeConfig.instrument,
    interval: twapInterval,
    startTime,
    endTime,
    decimals: 5,
  }), [selectedExchange, exchangeConfig.instrument, twapInterval, startTime, endTime]);

  const vwParams = useMemo(() => ({
    interval: twapInterval,
    startTime,
    endTime,
    decimals: 5,
  }), [twapInterval, startTime, endTime]);

  const singleQuery = useKaikoTwap(singleParams, enabled && mode === 'single');
  const vwQuery = useKaikoVwTwap(vwParams, enabled && mode === 'vw');

  const activeQuery = mode === 'single' ? singleQuery : vwQuery;
  const { data, isLoading, error, refetch, isFetching } = activeQuery;

  const handlePreset = (label: string) => {
    setActivePreset(label);
    setUseCustomDates(false);
    setStartDate(undefined);
    setEndDate(undefined);
    setStartHour('0');
    setEndHour('23');
  };

  const handleStartDateSelect = (date: Date | undefined) => {
    setStartDate(date);
    if (date) { setUseCustomDates(true); setActivePreset(''); }
  };

  const handleEndDateSelect = (date: Date | undefined) => {
    setEndDate(date);
    if (date) { setUseCustomDates(true); setActivePreset(''); }
  };

  // Type narrowing helpers
  const vwData = mode === 'vw' ? (data as import("@/hooks/use-kaiko-twap").VwTwapResponse | undefined) : undefined;
  const singleData = mode === 'single' ? (data as import("@/hooks/use-kaiko-twap").TwapResponse | undefined) : undefined;

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
        <div className="flex items-center gap-2">
          <Clock className="h-5 w-5 text-primary" />
          <CardTitle className="text-lg">TWAP — Time-Weighted Average Price</CardTitle>
          <Badge variant="secondary" className="text-[10px] font-mono">UTC</Badge>
        </div>
        <Button variant="ghost" size="icon" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={cn("h-4 w-4", isFetching && "animate-spin")} />
        </Button>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Mode toggle */}
        <Tabs value={mode} onValueChange={(v) => setMode(v as 'single' | 'vw')}>
          <TabsList className="h-8">
            <TabsTrigger value="single" className="text-xs gap-1.5 px-3">
              <Building2 className="h-3 w-3" /> Single Exchange
            </TabsTrigger>
            <TabsTrigger value="vw" className="text-xs gap-1.5 px-3">
              <Globe className="h-3 w-3" /> Volume-Weighted (All CC)
            </TabsTrigger>
          </TabsList>
        </Tabs>

        {/* Controls row */}
        <div className="flex flex-wrap gap-3 items-end">
          {/* Exchange - only for single mode */}
          {mode === 'single' && (
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Exchange</label>
              <Select value={selectedExchange} onValueChange={setSelectedExchange}>
                <SelectTrigger className="w-[130px] h-8 text-sm">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {EXCHANGES.map(ex => (
                    <SelectItem key={ex.value} value={ex.value}>{ex.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          {/* Interval */}
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Candle Interval</label>
            <Select value={twapInterval} onValueChange={setTwapInterval}>
              <SelectTrigger className="w-[110px] h-8 text-sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {TWAP_INTERVALS.map(i => (
                  <SelectItem key={i.value} value={i.value}>{i.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Start Date + Hour */}
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Start (UTC)</label>
            <div className="flex gap-1">
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" size="sm" className={cn(
                    "w-[130px] justify-start text-left font-normal h-8 text-sm",
                    !startDate && "text-muted-foreground"
                  )}>
                    <CalendarIcon className="mr-1 h-3 w-3" />
                    {startDate ? formatInTimeZone(startDate, 'UTC', "MMM d, yyyy") : "Pick date"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <Calendar mode="single" selected={startDate} onSelect={handleStartDateSelect}
                    disabled={(date) => date > new Date()} initialFocus className={cn("p-3 pointer-events-auto")} />
                </PopoverContent>
              </Popover>
              <Select value={startHour} onValueChange={(v) => { setStartHour(v); if (startDate) { setUseCustomDates(true); setActivePreset(''); } }}>
                <SelectTrigger className="w-[72px] h-8 text-sm font-mono">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {UTC_HOURS.map(h => (
                    <SelectItem key={h} value={h} className="font-mono">{h.padStart(2, '0')}:00</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* End Date + Hour */}
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">End (UTC)</label>
            <div className="flex gap-1">
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" size="sm" className={cn(
                    "w-[130px] justify-start text-left font-normal h-8 text-sm",
                    !endDate && "text-muted-foreground"
                  )}>
                    <CalendarIcon className="mr-1 h-3 w-3" />
                    {endDate ? formatInTimeZone(endDate, 'UTC', "MMM d, yyyy") : "Pick date"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <Calendar mode="single" selected={endDate} onSelect={handleEndDateSelect}
                    disabled={(date) => date > new Date() || (startDate ? date < startDate : false)}
                    initialFocus className={cn("p-3 pointer-events-auto")} />
                </PopoverContent>
              </Popover>
              <Select value={endHour} onValueChange={(v) => { setEndHour(v); if (endDate) { setUseCustomDates(true); setActivePreset(''); } }}>
                <SelectTrigger className="w-[72px] h-8 text-sm font-mono">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {UTC_HOURS.map(h => (
                    <SelectItem key={h} value={h} className="font-mono">{h.padStart(2, '0')}:00</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>

        {/* Preset range buttons */}
        <div className="flex flex-wrap gap-1.5">
          {PRESET_RANGES.map(p => (
            <Button key={p.label} variant={activePreset === p.label ? "default" : "outline"}
              size="sm" className="h-7 text-xs px-3" onClick={() => handlePreset(p.label)}>
              {p.label}
            </Button>
          ))}
        </div>

        {/* TWAP Result */}
        <div className="rounded-lg border bg-muted/30 p-4">
          {isLoading ? (
            <div className="space-y-2">
              <Skeleton className="h-8 w-48" />
              <Skeleton className="h-4 w-64" />
            </div>
          ) : error ? (
            <p className="text-sm text-destructive">Error: {error.message}</p>
          ) : data?.result === 'no_data' ? (
            <p className="text-sm text-muted-foreground">No trade data available for this range</p>
          ) : data?.twap ? (
            <div className="space-y-3">
              <div className="flex items-baseline gap-3">
                <span className="text-3xl font-mono font-bold tracking-tight">
                  ${data.twap}
                </span>
                <Badge variant="outline" className="text-xs">
                  {mode === 'single'
                    ? `${exchangeConfig.label} · ${exchangeConfig.instrument.toUpperCase()}`
                    : `${vwData?.exchanges_with_data || 0}/${vwData?.total_exchange_pairs || 0} CC pairs`
                  }
                </Badge>
                {mode === 'vw' && (
                  <Badge variant="secondary" className="text-xs">Volume-Weighted</Badge>
                )}
              </div>
              <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                <span>{mode === 'vw' ? 'Time slices' : 'Candles'}: <strong>
                  {mode === 'vw' ? vwData?.time_slices : singleData?.candle_count}
                </strong></span>
                <span>Interval: <strong>{data.interval}</strong></span>
                <span>Precision: <strong>{data.decimals} dp</strong></span>
                <span>Range: {formatInTimeZone(new Date(startTime), 'UTC', "MMM d HH:mm")} → {formatInTimeZone(new Date(endTime), 'UTC', "MMM d HH:mm")} UTC</span>
              </div>

              {/* VW exchange breakdown */}
              {mode === 'vw' && vwData?.exchange_breakdown && vwData.exchange_breakdown.length > 0 && (
                <div className="mt-2 pt-2 border-t">
                  <p className="text-xs text-muted-foreground mb-1.5">Volume breakdown by exchange:</p>
                  <div className="flex flex-wrap gap-2">
                    {vwData.exchange_breakdown.map(ex => {
                      const totalVol = vwData.exchange_breakdown!.reduce((s, e) => s + e.total_volume, 0);
                      const pct = totalVol > 0 ? ((ex.total_volume / totalVol) * 100).toFixed(1) : '0';
                      return (
                        <Badge key={`${ex.exchange}-${ex.instrument}`} variant="outline" className="text-[10px] font-mono">
                          {ex.exchange}/{ex.instrument} — {pct}%
                        </Badge>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Hourly candle drill-down */}
              {mode === 'single' && singleData?.candles && singleData.candles.length > 0 && (
                <div className="mt-2 pt-2 border-t">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-xs gap-1 px-0 h-6 text-muted-foreground hover:text-foreground"
                    onClick={() => setShowCandles(!showCandles)}
                  >
                    {showCandles ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
                    {showCandles ? 'Hide' : 'Show'} Candle Breakdown ({singleData.candles.length})
                  </Button>
                  {showCandles && (
                    <ScrollArea className="mt-2 max-h-[400px] rounded border">
                      <Table>
                        <TableHeader>
                          <TableRow className="text-[10px]">
                            <TableHead className="py-1 px-2">Time (UTC)</TableHead>
                            <TableHead className="py-1 px-2 text-right">Open</TableHead>
                            <TableHead className="py-1 px-2 text-right">High</TableHead>
                            <TableHead className="py-1 px-2 text-right">Low</TableHead>
                            <TableHead className="py-1 px-2 text-right">Close</TableHead>
                            <TableHead className="py-1 px-2 text-right">Volume</TableHead>
                            <TableHead className="py-1 px-2 text-right">Avg (OHLC/4)</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {singleData.candles.map((c: TwapCandle) => (
                            <TableRow key={c.timestamp} className="text-[11px] font-mono">
                              <TableCell className="py-1 px-2 whitespace-nowrap">
                                {formatInTimeZone(new Date(c.timestamp), 'UTC', "yyyy-MM-dd HH:mm")}
                              </TableCell>
                              <TableCell className="py-1 px-2 text-right">{c.open.toFixed(6)}</TableCell>
                              <TableCell className="py-1 px-2 text-right">{c.high.toFixed(6)}</TableCell>
                              <TableCell className="py-1 px-2 text-right">{c.low.toFixed(6)}</TableCell>
                              <TableCell className="py-1 px-2 text-right">{c.close.toFixed(6)}</TableCell>
                              <TableCell className="py-1 px-2 text-right">{c.volume.toLocaleString(undefined, { maximumFractionDigits: 1 })}</TableCell>
                              <TableCell className="py-1 px-2 text-right font-semibold">{c.typical_price.toFixed(6)}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </ScrollArea>
                  )}
                </div>
              )}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Select a time range to compute TWAP</p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
