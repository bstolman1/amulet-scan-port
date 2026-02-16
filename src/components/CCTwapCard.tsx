import { useState, useMemo } from "react";
import { format, subDays, subHours, startOfDay, endOfDay } from "date-fns";
import { CalendarIcon, Clock, BarChart3, RefreshCw } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { useKaikoTwap } from "@/hooks/use-kaiko-twap";

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
  const [selectedExchange, setSelectedExchange] = useState('krkn');
  const [twapInterval, setTwapInterval] = useState('5m');
  const [activePreset, setActivePreset] = useState<string>('24H');
  const [startDate, setStartDate] = useState<Date | undefined>(undefined);
  const [endDate, setEndDate] = useState<Date | undefined>(undefined);
  const [useCustomDates, setUseCustomDates] = useState(false);

  const exchangeConfig = EXCHANGES.find(e => e.value === selectedExchange) || EXCHANGES[0];

  const { startTime, endTime } = useMemo(() => {
    if (useCustomDates && startDate && endDate) {
      return {
        startTime: startOfDay(startDate).toISOString(),
        endTime: endOfDay(endDate).toISOString(),
      };
    }
    const preset = PRESET_RANGES.find(p => p.label === activePreset);
    const now = new Date();
    return {
      startTime: subHours(now, preset?.hours || 24).toISOString(),
      endTime: now.toISOString(),
    };
  }, [useCustomDates, startDate, endDate, activePreset]);

  const params = useMemo(() => ({
    exchange: selectedExchange,
    instrument: exchangeConfig.instrument,
    interval: twapInterval,
    startTime,
    endTime,
    decimals: 5,
  }), [selectedExchange, exchangeConfig.instrument, twapInterval, startTime, endTime]);

  const { data, isLoading, error, refetch, isFetching } = useKaikoTwap(params, enabled);

  const handlePreset = (label: string) => {
    setActivePreset(label);
    setUseCustomDates(false);
    setStartDate(undefined);
    setEndDate(undefined);
  };

  const handleStartDateSelect = (date: Date | undefined) => {
    setStartDate(date);
    if (date) {
      setUseCustomDates(true);
      setActivePreset('');
    }
  };

  const handleEndDateSelect = (date: Date | undefined) => {
    setEndDate(date);
    if (date) {
      setUseCustomDates(true);
      setActivePreset('');
    }
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
        <div className="flex items-center gap-2">
          <Clock className="h-5 w-5 text-primary" />
          <CardTitle className="text-lg">TWAP — Time-Weighted Average Price</CardTitle>
        </div>
        <Button variant="ghost" size="icon" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={cn("h-4 w-4", isFetching && "animate-spin")} />
        </Button>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Controls row */}
        <div className="flex flex-wrap gap-3 items-end">
          {/* Exchange */}
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

          {/* Start Date Picker */}
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Start Date</label>
            <Popover>
              <PopoverTrigger asChild>
                <Button variant="outline" size="sm" className={cn(
                  "w-[140px] justify-start text-left font-normal h-8 text-sm",
                  !startDate && "text-muted-foreground"
                )}>
                  <CalendarIcon className="mr-1 h-3 w-3" />
                  {startDate ? format(startDate, "MMM d, yyyy") : "Pick start"}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar
                  mode="single"
                  selected={startDate}
                  onSelect={handleStartDateSelect}
                  disabled={(date) => date > new Date()}
                  initialFocus
                  className={cn("p-3 pointer-events-auto")}
                />
              </PopoverContent>
            </Popover>
          </div>

          {/* End Date Picker */}
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">End Date</label>
            <Popover>
              <PopoverTrigger asChild>
                <Button variant="outline" size="sm" className={cn(
                  "w-[140px] justify-start text-left font-normal h-8 text-sm",
                  !endDate && "text-muted-foreground"
                )}>
                  <CalendarIcon className="mr-1 h-3 w-3" />
                  {endDate ? format(endDate, "MMM d, yyyy") : "Pick end"}
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar
                  mode="single"
                  selected={endDate}
                  onSelect={handleEndDateSelect}
                  disabled={(date) => date > new Date() || (startDate ? date < startDate : false)}
                  initialFocus
                  className={cn("p-3 pointer-events-auto")}
                />
              </PopoverContent>
            </Popover>
          </div>
        </div>

        {/* Preset range buttons */}
        <div className="flex flex-wrap gap-1.5">
          {PRESET_RANGES.map(p => (
            <Button
              key={p.label}
              variant={activePreset === p.label ? "default" : "outline"}
              size="sm"
              className="h-7 text-xs px-3"
              onClick={() => handlePreset(p.label)}
            >
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
            <div className="space-y-2">
              <div className="flex items-baseline gap-3">
                <span className="text-3xl font-mono font-bold tracking-tight">
                  ${data.twap}
                </span>
                <Badge variant="outline" className="text-xs">
                  {exchangeConfig.label} · {exchangeConfig.instrument.toUpperCase()}
                </Badge>
              </div>
              <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                <span>Candles: <strong>{data.candle_count}</strong></span>
                <span>Interval: <strong>{data.interval}</strong></span>
                <span>
                  Range: {data.first_candle ? format(new Date(data.first_candle), "MMM d HH:mm") : '—'} → {data.last_candle ? format(new Date(data.last_candle), "MMM d HH:mm") : '—'}
                </span>
                <span>Precision: <strong>{data.decimals} dp</strong></span>
              </div>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Select a time range to compute TWAP</p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
