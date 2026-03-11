import { useState, useMemo } from "react";
import { subHours } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import {
  CalendarIcon,
  Clock,
  RefreshCw,
  Globe,
  Building2,
  ChevronDown,
  ChevronUp,
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
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
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
import type { TwapCandle } from "@/hooks/use-kaiko-twap";

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

/* ---------------- SAFE HELPERS ---------------- */

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

/* ---------------------------------------------- */

export function CCTwapCard({ enabled = true }: CCTwapCardProps) {
  const [mode, setMode] = useState<"single" | "vw">("single");
  const [selectedExchange, setSelectedExchange] = useState("krkn");
  const [twapInterval, setTwapInterval] = useState("5m");
  const [activePreset, setActivePreset] = useState("24H");

  const [startDate, setStartDate] = useState<Date>();
  const [startHour, setStartHour] = useState("0");

  const [endDate, setEndDate] = useState<Date>();
  const [endHour, setEndHour] = useState("23");

  const [useCustomDates, setUseCustomDates] = useState(false);
  const [showCandles, setShowCandles] = useState(false);

  const UTC_HOURS = Array.from({ length: 24 }, (_, i) => String(i));

  const exchangeConfig =
    EXCHANGES.find((e) => e.value === selectedExchange) || EXCHANGES[0];

  const { startTime, endTime } = useMemo(() => {
    if (useCustomDates && startDate && endDate) {
      const s = new Date(
        Date.UTC(
          startDate.getFullYear(),
          startDate.getMonth(),
          startDate.getDate(),
          parseInt(startHour),
        ),
      );

      const e = new Date(
        Date.UTC(
          endDate.getFullYear(),
          endDate.getMonth(),
          endDate.getDate(),
          parseInt(endHour),
        ),
      );

      return {
        startTime: s.toISOString(),
        endTime: e.toISOString(),
      };
    }

    const preset = PRESET_RANGES.find((p) => p.label === activePreset);
    const now = new Date();

    return {
      startTime: subHours(now, preset?.hours || 24).toISOString(),
      endTime: now.toISOString(),
    };
  }, [useCustomDates, startDate, startHour, endDate, endHour, activePreset]);

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
    () => ({
      interval: twapInterval,
      startTime,
      endTime,
      decimals: 5,
    }),
    [twapInterval, startTime, endTime],
  );

  const singleQuery = useKaikoTwap(singleParams, enabled && mode === "single");
  const vwQuery = useKaikoVwTwap(vwParams, enabled && mode === "vw");

  const activeQuery = mode === "single" ? singleQuery : vwQuery;
  const { data, isLoading, error, refetch, isFetching } = activeQuery;

  const vwData =
    mode === "vw"
      ? (data as import("@/hooks/use-kaiko-twap").VwTwapResponse)
      : undefined;

  const singleData =
    mode === "single"
      ? (data as import("@/hooks/use-kaiko-twap").TwapResponse)
      : undefined;

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-3">
        <div className="flex items-center gap-2">
          <Clock className="h-5 w-5 text-primary" />
          <CardTitle className="text-lg">TWAP — Time-Weighted Average Price</CardTitle>
          <Badge variant="secondary" className="text-[10px] font-mono">
            UTC
          </Badge>
        </div>

        <Button variant="ghost" size="icon" onClick={() => refetch()} disabled={isFetching}>
          <RefreshCw className={cn("h-4 w-4", isFetching && "animate-spin")} />
        </Button>
      </CardHeader>

      <CardContent className="space-y-4">

        <Tabs value={mode} onValueChange={(v) => setMode(v as any)}>
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

        {/* RESULT */}

        <div className="rounded-lg border bg-muted/30 p-4">
          {isLoading ? (
            <Skeleton className="h-8 w-48" />
          ) : error ? (
            <p className="text-destructive text-sm">{error.message}</p>
          ) : data?.twap ? (
            <div className="space-y-3">

              <div className="flex items-baseline gap-3">
                <span className="text-3xl font-mono font-bold">
                  ${data.twap}
                </span>

                <Badge variant="outline">
                  {mode === "single"
                    ? `${exchangeConfig.label} · ${exchangeConfig.instrument.toUpperCase()}`
                    : `${vwData?.exchanges_with_data || 0}/${vwData?.total_exchange_pairs || 0} CC pairs`}
                </Badge>
              </div>

              {/* CANDLE TABLE */}

              {mode === "single" &&
                singleData?.candles &&
                singleData.candles.length > 0 && (
                  <ScrollArea className="mt-2 max-h-[500px] rounded border">

                    <Table>

                      <TableHeader>
                        <TableRow className="text-[10px]">
                          <TableHead>Time</TableHead>
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
                              {formatInTimeZone(
                                new Date(c.timestamp),
                                "UTC",
                                "yyyy-MM-dd HH:mm",
                              )}
                            </TableCell>

                            <TableCell className="text-right">
                              {safeFixed(c.open, 5)}
                            </TableCell>

                            <TableCell className="text-right">
                              {safeFixed(c.high, 5)}
                            </TableCell>

                            <TableCell className="text-right">
                              {safeFixed(c.low, 5)}
                            </TableCell>

                            <TableCell className="text-right">
                              {safeFixed(c.close, 5)}
                            </TableCell>

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
              Select a time range to compute TWAP
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
