import { useSvWeightHistory } from "@/hooks/use-sv-weight-history";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, ResponsiveContainer } from "recharts";
import { TrendingUp, Users } from "lucide-react";

export const SVWeightHistoryChart = () => {
  const { data, isPending, isError } = useSvWeightHistory(200);

  const chartData = data?.dailyData?.map((entry) => ({
    date: new Date(entry.date).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    }),
    fullDate: entry.date,
    svCount: entry.svCount,
  })) || [];

  // Calculate stats
  const currentCount = chartData.length > 0 ? chartData[chartData.length - 1]?.svCount : 0;
  const earliestCount = chartData.length > 0 ? chartData[0]?.svCount : 0;
  const change = currentCount - earliestCount;
  const changePercent = earliestCount > 0 ? ((change / earliestCount) * 100).toFixed(1) : 0;

  return (
    <Card className="glass-card">
      <div className="p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Users className="w-5 h-5 text-primary" />
            <h3 className="text-xl font-bold">SV Count Over Time</h3>
          </div>
          {chartData.length > 0 && (
            <div className="flex items-center gap-2 text-sm">
              <span className="text-muted-foreground">Current:</span>
              <span className="font-semibold text-primary">{currentCount} SVs</span>
              {change !== 0 && (
                <span className={change > 0 ? "text-success" : "text-destructive"}>
                  ({change > 0 ? "+" : ""}{change})
                </span>
              )}
            </div>
          )}
        </div>

        {isPending ? (
          <Skeleton className="h-[300px] w-full" />
        ) : isError ? (
          <div className="h-[300px] flex items-center justify-center">
            <div className="text-center space-y-2">
              <p className="text-muted-foreground">Unable to load SV weight history</p>
              <p className="text-xs text-muted-foreground">
                Ensure the DSO Rules index is built (run /api/stats/sv-index/build)
              </p>
            </div>
          </div>
        ) : chartData.length === 0 ? (
          <div className="h-[300px] flex items-center justify-center">
            <div className="text-center space-y-2">
              <TrendingUp className="w-12 h-12 mx-auto text-muted-foreground/50" />
              <p className="text-muted-foreground">No SV history data available</p>
              <p className="text-xs text-muted-foreground">
                Build the DSO Rules index first via /api/events/dso-rules/build
              </p>
            </div>
          </div>
        ) : (
          <div>
            <div className="mb-2 text-xs text-muted-foreground">
              Showing {chartData.length} days of data • From {earliestCount} to {currentCount} SVs
            </div>
            <ChartContainer
              config={{
                svCount: {
                  label: "Active SVs",
                  color: "hsl(var(--primary))",
                },
              }}
              className="h-[300px] w-full"
            >
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData}>
                  <defs>
                    <linearGradient id="svCountGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.4} />
                      <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0.05} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis
                    dataKey="date"
                    className="text-xs"
                    tick={{ fill: "hsl(var(--muted-foreground))" }}
                    angle={-45}
                    textAnchor="end"
                    height={60}
                    interval="preserveStartEnd"
                  />
                  <YAxis
                    className="text-xs"
                    tick={{ fill: "hsl(var(--muted-foreground))" }}
                    domain={["dataMin - 1", "dataMax + 1"]}
                    allowDecimals={false}
                  />
                  <ChartTooltip
                    content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const data = payload[0].payload;
                      return (
                        <div className="bg-popover border border-border rounded-lg p-3 shadow-lg">
                          <p className="text-sm font-medium">{data.fullDate}</p>
                          <p className="text-sm text-primary">
                            {data.svCount} Active SVs
                          </p>
                        </div>
                      );
                    }}
                  />
                  <Area
                    type="stepAfter"
                    dataKey="svCount"
                    stroke="hsl(var(--primary))"
                    strokeWidth={2}
                    fill="url(#svCountGradient)"
                    name="Active SVs"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </ChartContainer>
          </div>
        )}
      </div>
    </Card>
  );
};
