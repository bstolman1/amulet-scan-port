import { useState, useMemo } from "react";
import { subHours } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import {
  Clock,
  RefreshCw,
  Globe,
  Building2,
} from "lucide-react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ScrollArea } from "@/components/ui/scroll-area";

import { cn } from "@/lib/utils";
import { useKaikoTwap, useKaikoVwTwap } from "@/hooks/use-kaiko-twap";
import type { TwapCandle, TwapResponse, VwTwapResponse } from "@/hooks/use-kaiko-twap";

const TWAP_INTERVALS = [
  { value: "1m", label: "1 Min" },
  { value: "5m", label: "5 Min" },
  { value: "15m", label: "15 Min" },
  { value: "1h", label: "1 Hour" },
  { value: "4h", label: "4 Hours" },
];

const PRESET_RANGES = [
  { label: "1H", hours: 1 },
  { label: "4H", hours: 4 },
  { label: "12H", hours: 12 },
  { label: "24H", hours: 24 },
  { label: "7D", hours: 168 },
  { label: "30D", hours: 720 },
];

const EXCHANGES = [
  { value: "krkn", label: "Kraken", instrument: "cc-usd" },
  { value: "gate", label: "Gate.io", instrument: "cc-usdt" },
  { value: "kcon", label: "KuCoin", instrument: "cc-usdt" },
  { value: "mexc", label: "MEXC", instrument: "cc-usdt" },
  { value: "bbsp", label: "Bybit", instrument: "cc-usdt" },
  { value: "hitb", label: "HitBTC", instrument: "cc-usdt" },
  { value: "cnex", label: "CoinEx", instrument: "cc-usdt" },
];

interface CCTwapCardProps {
  enabled?: boolean;
}

function safeNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function safeFixed(value: unknown, digits: number) {
  const v = safeNumber(value);
  return v === null ? "0".padEnd(digits + 2, "0") : v.toFixed(digits);
}

function safeLocale(value: unknown, opts?: Intl.NumberFormatOptions) {
  const v = safeNumber(value);
  return v === null ? "0" : v.toLocaleString(undefined, opts);
}

// Format Date to "YYYY-MM-DDTHH:mm" in UTC for datetime-local input
const toUTCInputValue = (date: Date) => {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}T${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}`;
};

// Parse datetime-local string as UTC
const parseUTC = (val: string) => (val ? new Date(val + ":00Z") : null);

export function CCTwapCard({ enabled = true }: CCTwapCardProps) {
  const [mode, setMode] = useState<"single" | "vw">("single");
  const [selectedExchange, setSelectedExchange] = useState("krkn");
  const [twapInterval, setTwapInterval] = useState("5m");
  const [activePreset, setActivePreset] = useState("24H");
  const [useCustomDates, setUseCustomDates] = useState(false);
  const [startInput, setStartInput] = useState(() => toUTCInputValue(subHours(new Date(), 24)));
  const [endInput, setEndInput] = useState(() => toUTCInputValue(new Date()));

  const exchangeConfig =
    EXCHANGES.find((e) => e.value === selectedExchange) || EXCHANGES[0];

  const { startTime, endTime } = useMemo(() => {
    if (useCustomDates) {
      const s = parseUTC(startInput);
      const e = parseUTC(endInput);
      if (s && e && e > s) {
        return { startTime: s.toISOString(), endTime: e.toISOString() };
      }
    }
    const preset = PRESET_RANGES.find((p) => p.label === activePreset);
    const now = new Date();
    return {
      startTime: subHours(now, preset?.hours || 24).toISOString(),
      endTime: now.toISOString(),
    };
  }, [useCustomDates, startInput, endInput, activePreset]);

  const customDateValid =
    !useCustomDates ||
    (!!parseUTC(startInput) &&
      !!parseUTC(endInput) &&
      parseUTC(endInput)! > parseUTC(startInput)!);

  const singleParams = useMemo(
    () => ({
      exchange: selectedExchange,
      instrument: exchangeConfig.instrument,
      interval: twapInterval,
      startTime,
      endTime,
      decimals: 5,
    }),
    [selectedExchange, exchangeConfig.instrument, twapInterval, startTime, endTime],
  );

  const vwParams = useMemo(
    () => ({ interval: twapInterval, startTime, endTime, decimals: 5 }),
    [twapInterval, startTime, endTime],
  );

  const singleQuery = useKaikoTwap(singleParams, enabled && mode === "single" && customDateValid);
  const vwQuery = useKaikoVwTwap(vwParams, enabled && mode === "vw" && customDateValid);

  const activeQuery = mode === "single" ? singleQuery : vwQuery;
  const { data, isLoading, error, refetch, isFetching } = activeQuery;

  const vwData = mode === "vw" ? (data as VwTwapResponse) : undefined;
  const singleData = mode === "single" ? (data as TwapResponse) : undefined;

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-3">
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

        {/* Mode tabs */}
        <Tabs value={mode} onValueChange={(v) => setMode(v as "single" | "vw")}>
          <TabsList className="h-8">
            <TabsTrigger value="single" className="text-xs gap-1.5 px-3">
              <Building2 className="h-3 w-3" />
              Single Exchange
            </TabsTrigger>
            <TabsTrigger value="vw" className="text-xs gap-1.5 px-3">
              <Globe className="h-3 w-3" />
              Volume Weighted
            </TabsTrigger>
          </TabsList>
        </Tabs>

        {/* Controls */}
        <div className="flex flex-wrap gap-3 items-end">

          {/* Exchange — only in single mode */}
          {mode === "single" && (
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Exchange</label>
              <Select value={selectedExchange} onValueChange={setSelectedExchange}>
                <SelectTrigger className="w-[140px]" style={{ backgroundColor: "#000", color: "#fff" }}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {EXCHANGES.map((ex) => (
                    <SelectItem key={ex.value} value={ex.value}>
                      <span className="flex items-center gap-2">
                        {ex.label}
                        <Badge variant="outline" className="text-[10px] px-1 py-0 h-4 text-primary border-primary">
                          CC
                        </Badge>
                      </span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

          {/* Candle interval */}
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground">Interval</label>
            <Select value={twapInterval} onValueChange={setTwapInterval}>
              <SelectTrigger className="w-[110px]" style={{ backgroundColor: "#000", color: "#fff" }}>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {TWAP_INTERVALS.map((int) => (
                  <SelectItem key={int.value} value={int.value}>{int.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Preset range buttons */}
          {!useCustomDates && (
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Range</label>
              <div className="flex gap-1">
                {PRESET_RANGES.map((p) => (
                  <Button
                    key={p.label}
                    variant={activePreset === p.label ? "default" : "outline"}
                    size="sm"
                    className="px-2 h-9 text-xs"
                    onClick={() => setActivePreset(p.label)}
                  >
                    {p.label}
                  </Button>
                ))}
              </div>
            </div>
          )}

          {/* Custom date range */}
          {useCustomDates && (
            <>
              <div className="space-y-1">
                <label className="text-xs text-muted-foreground">Start (UTC)</label>
                <Input
                  type="datetime-local"
                  value={startInput}
                  onChange={(e) => setStartInput(e.target.value)}
                  className="w-[200px] text-sm"
                  style={{ backgroundColor: "#000", color: "#fff" }}
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs text-muted-foreground">End (UTC)</label>
                <Input
                  type="datetime-local"
                  value={endInput}
                  onChange={(e) => setEndInput(e.target.value)}
                  className="w-[200px] text-sm"
                  style={{ backgroundColor: "#000", color: "#fff" }}
                />
              </div>
              {!customDateValid && (
                <p className="text-xs text-destructive pb-1">End must be after start</p>
              )}
            </>
          )}

          {/* Toggle custom dates */}
          <div className="space-y-1">
            <label className="text-xs text-muted-foreground opacity-0">toggle</label>
            <Button
              variant="outline"
              size="sm"
              className="h-9 text-xs"
              onClick={() => setUseCustomDates((v) => !v)}
            >
              {useCustomDates ? "Use Presets" : "Custom Range"}
            </Button>
          </div>

        </div>

        {/* Result */}
        <div className="rounded-lg border bg-muted/30 p-4">
          {isLoading ? (
            <Skeleton className="h-8 w-48" />
          ) : error ? (
            <p className="text-destructive text-sm">{error.message}</p>
          ) : data?.twap ? (
            <div className="space-y-3">
              <div className="flex items-baseline gap-3">
                <span className="text-3xl font-mono font-bold">${data.twap}</span>
                <Badge variant="outline">
                  {mode === "single"
                    ? `${exchangeConfig.label} · ${exchangeConfig.instrument.toUpperCase()}`
                    : `${vwData?.exchanges_with_data || 0}/${vwData?.total_exchange_pairs || 0} CC pairs`}
                </Badge>
                {singleData?.candle_count && (
                  <span className="text-xs text-muted-foreground">
                    {singleData.candle_count.toLocaleString()} candles
                  </span>
                )}
              </div>

              {data.pagination_truncated && (
                <p className="text-xs text-yellow-500">
                  ⚠ Result may be truncated — window exceeds data limit
                </p>
              )}

              {/* Candle table — single mode only */}
              {mode === "single" && singleData?.candles && singleData.candles.length > 0 && (
                <ScrollArea className="mt-2 max-h-[400px] rounded border">
                  <Table>
                    <TableHeader>
                      <TableRow className="text-[10px]">
                        <TableHead>Time (UTC)</TableHead>
                        <TableHead className="text-right">Open</TableHead>
                        <TableHead className="text-right">High</TableHead>
                        <TableHead className="text-right">Low</TableHead>
                        <TableHead className="text-right">Close</TableHead>
                        <TableHead className="text-right">Volume</TableHead>
                        <TableHead className="text-right">Avg</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {singleData.candles.map((c: TwapCandle) => (
                        <TableRow key={c.timestamp} className="text-[11px] font-mono">
                          <TableCell>
                            {formatInTimeZone(new Date(c.timestamp), "UTC", "yyyy-MM-dd HH:mm")}
                          </TableCell>
                          <TableCell className="text-right">{safeFixed(c.open, 5)}</TableCell>
                          <TableCell className="text-right">{safeFixed(c.high, 5)}</TableCell>
                          <TableCell className="text-right">{safeFixed(c.low, 5)}</TableCell>
                          <TableCell className="text-right">{safeFixed(c.close, 5)}</TableCell>
                          <TableCell className="text-right">
                            {safeLocale(c.volume, { maximumFractionDigits: 1 })}
                          </TableCell>
                          <TableCell className="text-right font-semibold">
                            {safeFixed(c.typical_price, 6)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </ScrollArea>
              )}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              {!customDateValid ? "Fix the date range to compute TWAP" : "Select a time range to compute TWAP"}
            </p>
          )}
        </div>

      </CardContent>
    </Card>
  );
}
