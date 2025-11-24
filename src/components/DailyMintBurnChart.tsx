import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Legend } from "recharts";
import { useBurnStats } from "@/hooks/use-burn-stats";

export const DailyMintBurnChart = () => {
  const { data: latestRound } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
    staleTime: 60_000,
  });

  const roundsPerDay = 144; // ~10 minutes per round
  const rangeDays = 30; // expanded for more data
  const roundsToFetch = Math.max(1, rangeDays * roundsPerDay);

  // Fetch comprehensive burn stats for 30 days
  const { data: burnStats, isPending: burnLoading } = useBurnStats({ days: rangeDays });

  // Fetch per-round totals for minting data
  const { data: yearlyTotals, isPending: mintLoading } = useQuery({
    queryKey: ["mintTotals", latestRound?.round, rangeDays],
    queryFn: async () => {
      if (!latestRound) return null;
      const startRound = Math.max(0, latestRound.round - roundsToFetch);
      const chunkSize = 200;
      const promises: Promise<{ entries: any[] }>[] = [];
      for (let start = startRound; start <= latestRound.round; start += chunkSize) {
        const end = Math.min(start + chunkSize - 1, latestRound.round);
        promises.push(scanApi.fetchRoundTotals({ start_round: start, end_round: end }));
      }
      const results = await Promise.all(promises);
      const entries = results.flatMap((r) => r?.entries ?? []);
      return { entries };
    },
    enabled: !!latestRound,
    staleTime: 60_000,
    retry: 1,
  });

  const chartData = (() => {
    const totalsLen = yearlyTotals?.entries?.length || 0;
    const burnByDay = burnStats?.byDay || {};

    console.info("DailyMintBurnChart START:", {
      totalsLen,
      burnDaysCount: Object.keys(burnByDay).length,
      mintLoading,
      burnLoading,
      sampleEntry: yearlyTotals?.entries?.[0]
    });

    if (!totalsLen && !Object.keys(burnByDay).length) {
      console.warn("DailyMintBurnChart: NO DATA AT ALL");
      return [] as Array<{ date: string; minted: number; burned: number }>;
    }

    const byDay: Record<string, { minted: number; burned: number; date: Date }> = {};

    // Process minting data
    let mintedTotal = 0;
    if (yearlyTotals?.entries?.length) {
      for (const e of yearlyTotals.entries) {
        const change = parseFloat(e.change_to_initial_amount_as_of_round_zero);
        const d = new Date(e.closed_round_effective_at);
        const key = d.toISOString().slice(0, 10);
        if (!byDay[key]) byDay[key] = { minted: 0, burned: 0, date: new Date(key) };
        if (!isNaN(change) && change > 0) {
          byDay[key].minted += change;
          mintedTotal += change;
        }
      }
      console.info("DailyMintBurnChart: processed minting", { mintedTotal, days: Object.keys(byDay).length });
    }

    // Use comprehensive burn stats from useBurnStats hook
    let burnedTotal = 0;
    for (const [dateKey, dayBurn] of Object.entries(burnByDay)) {
      if (!byDay[dateKey]) byDay[dateKey] = { minted: 0, burned: 0, date: new Date(dateKey) };
      byDay[dateKey].burned = dayBurn.totalBurn;
      burnedTotal += dayBurn.totalBurn;
    }
    console.info("DailyMintBurnChart: using comprehensive burn stats", { burnedTotal });

    const result = Object.values(byDay)
      .sort((a, b) => a.date.getTime() - b.date.getTime())
      .map((d) => ({
        date: d.date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        minted: Math.round(d.minted),
        burned: Math.round(d.burned),
      }));

    console.info("DailyMintBurnChart FINAL:", {
      points: result.length,
      totalsLen,
      burnDaysCount: Object.keys(burnByDay).length,
      mintedTotal,
      burnedTotal,
      firstPoint: result[0],
      lastPoint: result[result.length - 1]
    });
    return result;
  })();

  const isLoading = mintLoading || burnLoading;

  return (
    <Card className="glass-card">
      <div className="p-6">
        <h3 className="text-xl font-bold mb-4">Daily Mint & Burn Activity — Last 30 Days</h3>
        {isLoading ? (
          <Skeleton className="h-[400px] w-full" />
        ) : chartData.length === 0 ? (
          <div className="h-[400px] flex items-center justify-center">
            <div className="text-center space-y-2">
              <p className="text-muted-foreground">No data available for the last 30 days</p>
              <p className="text-xs text-muted-foreground">
                Check console for debug info (mintLoading: {String(mintLoading)}, burnLoading: {String(burnLoading)})
              </p>
            </div>
          </div>
        ) : (
          <div>
            <div className="mb-2 text-xs text-muted-foreground">
              Showing {chartData.length} days of data
            </div>
            <ChartContainer
              config={{
                minted: {
                  label: "Minted",
                  color: "hsl(var(--chart-2))",
                },
                burned: {
                  label: "Burned",
                  color: "hsl(var(--destructive))",
                },
              }}
              className="h-[400px] w-full"
            >
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis
                    dataKey="date"
                    className="text-xs"
                    tick={{ fill: 'hsl(var(--muted-foreground))' }}
                    angle={-45}
                    textAnchor="end"
                    height={80}
                  />
                  <YAxis
                    className="text-xs"
                    tick={{ fill: 'hsl(var(--muted-foreground))' }}
                    tickFormatter={(value) => {
                      if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
                      if (value >= 1000) return `${(value / 1000).toFixed(0)}K`;
                      return value.toString();
                    }}
                  />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Legend />
                  <Bar
                    dataKey="minted"
                    fill="hsl(var(--chart-2))"
                    radius={[4, 4, 0, 0]}
                    name="Minted"
                  />
                  <Bar
                    dataKey="burned"
                    fill="hsl(var(--destructive))"
                    radius={[4, 4, 0, 0]}
                    name="Burned"
                  />
                </BarChart>
              </ResponsiveContainer>
            </ChartContainer>
          </div>
        )}
      </div>
    </Card>
  );
};
