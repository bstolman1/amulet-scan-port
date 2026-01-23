import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { useKaikoOHLCV } from "@/hooks/use-kaiko-ohlcv";
import { Line, LineChart, ResponsiveContainer, Tooltip, YAxis } from "recharts";
import { CalendarRange, TrendingDown, TrendingUp } from "lucide-react";

type Timeframe = "1D" | "7D" | "30D";

const TIMEFRAMES: Array<{ key: Timeframe; label: string }> = [
  { key: "1D", label: "1D" },
  { key: "7D", label: "7D" },
  { key: "30D", label: "30D" },
];

function startTimeFor(timeframe: Timeframe): { startTime?: string; interval: string; pageSize: number } {
  const now = new Date();

  if (timeframe === "1D") {
    const start = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    return { startTime: start.toISOString(), interval: "1h", pageSize: 48 };
  }

  const days = timeframe === "7D" ? 7 : 30;
  const start = new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
  return { startTime: start.toISOString(), interval: "1d", pageSize: 200 };
}

function formatPct(value: number) {
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

export function CCTimeframeComparison({ enabled = true }: { enabled?: boolean }) {
  const [timeframe, setTimeframe] = useState<Timeframe>("30D");

  const { startTime, interval, pageSize } = useMemo(() => startTimeFor(timeframe), [timeframe]);

  const { data, isLoading } = useKaikoOHLCV(
    {
      exchange: "krkn",
      instrumentClass: "spot",
      instrument: "cc-usd",
      interval,
      startTime,
      sort: "desc",
      pageSize,
    },
    enabled,
  );

  const points = useMemo(() => {
    const candles = data?.data || [];
    // API returns desc; chart should be asc.
    return [...candles]
      .reverse()
      .map((c) => ({
        ts: c.timestamp,
        close: c.close ? Number(c.close) : null,
      }))
      .filter((p): p is { ts: number; close: number } => typeof p.close === "number" && !Number.isNaN(p.close));
  }, [data?.data]);

  const perf = useMemo(() => {
    if (points.length < 2) return null;
    const first = points[0].close;
    const last = points[points.length - 1].close;
    if (!first || !last) return null;
    const pct = ((last - first) / first) * 100;
    return { first, last, pct };
  }, [points]);

  const isPositive = (perf?.pct ?? 0) >= 0;

  return (
    <Card>
      <CardHeader className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <CardTitle className="flex items-center gap-2">
          <CalendarRange className="h-5 w-5" />
          CC Performance
          <Badge variant="outline" className="ml-2">CC/USD</Badge>
        </CardTitle>

        <div className="flex flex-wrap items-center gap-2">
          {TIMEFRAMES.map((t) => (
            <Button
              key={t.key}
              type="button"
              size="sm"
              variant={timeframe === t.key ? "secondary" : "outline"}
              onClick={() => setTimeframe(t.key)}
            >
              {t.label}
            </Button>
          ))}
        </div>
      </CardHeader>

      <CardContent>
        {isLoading ? (
          <Skeleton className="h-[120px] w-full" />
        ) : !perf ? (
          <div className="h-[120px] flex items-center justify-center text-muted-foreground">
            Not enough data for {timeframe}
          </div>
        ) : (
          <div className="grid gap-4 md:grid-cols-[220px_1fr]">
            <div className="rounded-md border bg-card p-4">
              <div className="text-sm text-muted-foreground">{timeframe} change</div>
              <div className="mt-1 flex items-center gap-2">
                {isPositive ? (
                  <TrendingUp className="h-4 w-4 text-primary" />
                ) : (
                  <TrendingDown className="h-4 w-4 text-destructive" />
                )}
                <div className="text-2xl font-semibold">
                  {formatPct(perf.pct)}
                </div>
              </div>
              <div className="mt-2 text-xs text-muted-foreground">
                ${perf.first.toFixed(4)} → ${perf.last.toFixed(4)}
              </div>
            </div>

            <div className="h-[120px] rounded-md border bg-card px-3 py-2">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={points} margin={{ top: 8, right: 8, left: 8, bottom: 0 }}>
                  <YAxis hide domain={["dataMin", "dataMax"]} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "hsl(var(--popover))",
                      border: "1px solid hsl(var(--border))",
                      borderRadius: 8,
                      boxShadow: "0 10px 30px -12px hsl(var(--foreground) / 0.25)",
                    }}
                    labelFormatter={(_, payload) => {
                      const ts = payload?.[0]?.payload?.ts;
                      return ts ? new Date(ts).toLocaleString() : "";
                    }}
                    formatter={(value: number) => [`$${value.toFixed(4)}`, "Close"]}
                  />
                  <Line
                    type="monotone"
                    dataKey="close"
                    stroke={isPositive ? "hsl(var(--chart-2))" : "hsl(var(--destructive))"}
                    strokeWidth={2}
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
