import { useSvWeightHistory } from "@/hooks/use-sv-weight-history";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ChartContainer } from "@/components/ui/chart";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Tooltip, Legend } from "recharts";
import { Layers, Users } from "lucide-react";
import { useMemo, useState } from "react";

// Generate distinct colors for SVs using golden ratio for good distribution
const generateColors = (count: number): string[] => {
  const colors: string[] = [];
  const goldenRatio = 0.618033988749895;
  let hue = 0.1; // Start hue
  
  for (let i = 0; i < count; i++) {
    hue += goldenRatio;
    hue %= 1;
    // Use HSL with varying saturation and lightness for visual distinction
    const saturation = 60 + (i % 3) * 10; // 60-80%
    const lightness = 45 + (i % 4) * 8; // 45-69%
    colors.push(`hsl(${Math.floor(hue * 360)}, ${saturation}%, ${lightness}%)`);
  }
  return colors;
};

export const SVWeightStackedChart = () => {
  const { data, isPending, isError } = useSvWeightHistory(300);
  const [hoveredSv, setHoveredSv] = useState<string | null>(null);

  // Process data for stacked chart
  const { chartData, svNames, colors, chartConfig } = useMemo(() => {
    if (!data?.stackedData || !data?.svNames) {
      return { chartData: [], svNames: [], colors: [], chartConfig: {} };
    }

    const names = data.svNames.slice(0, 20); // Limit to top 20 for readability
    const colorPalette = generateColors(names.length);
    
    // Build chart config
    const config: Record<string, { label: string; color: string }> = {};
    names.forEach((name, i) => {
      config[name] = {
        label: name,
        color: colorPalette[i],
      };
    });

    // Format dates for display
    const formattedData = data.stackedData.map((entry) => ({
      ...entry,
      displayDate: new Date(entry.date).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      }),
    }));

    return {
      chartData: formattedData,
      svNames: names,
      colors: colorPalette,
      chartConfig: config,
    };
  }, [data]);

  // Calculate summary stats
  const latestEntry = chartData[chartData.length - 1];
  const totalSvs = latestEntry?.total || 0;

  return (
    <Card className="glass-card">
      <div className="p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Layers className="w-5 h-5 text-primary" />
            <h3 className="text-xl font-bold">SV Distribution Over Time</h3>
          </div>
          {chartData.length > 0 && (
            <div className="flex items-center gap-2 text-sm">
              <Users className="w-4 h-4 text-muted-foreground" />
              <span className="text-muted-foreground">Current:</span>
              <span className="font-semibold text-primary">{totalSvs} operators</span>
            </div>
          )}
        </div>

        {isPending ? (
          <Skeleton className="h-[400px] w-full" />
        ) : isError ? (
          <div className="h-[400px] flex items-center justify-center">
            <div className="text-center space-y-2">
              <p className="text-muted-foreground">Unable to load SV distribution data</p>
              <p className="text-xs text-muted-foreground">
                Build the DSO Rules index first
              </p>
            </div>
          </div>
        ) : chartData.length === 0 || svNames.length === 0 ? (
          <div className="h-[400px] flex items-center justify-center">
            <div className="text-center space-y-2">
              <Layers className="w-12 h-12 mx-auto text-muted-foreground/50" />
              <p className="text-muted-foreground">No SV distribution data available</p>
              <p className="text-xs text-muted-foreground">
                DSO Rules index may not contain SV party information
              </p>
            </div>
          </div>
        ) : (
          <div>
            <div className="mb-2 text-xs text-muted-foreground">
              Showing {chartData.length} days • {svNames.length} unique operators tracked
            </div>
            <ChartContainer config={chartConfig} className="h-[400px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 60 }}>
                  <defs>
                    {svNames.map((name, i) => (
                      <linearGradient key={name} id={`gradient-${i}`} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={colors[i]} stopOpacity={0.8} />
                        <stop offset="95%" stopColor={colors[i]} stopOpacity={0.3} />
                      </linearGradient>
                    ))}
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis
                    dataKey="displayDate"
                    className="text-xs"
                    tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 10 }}
                    angle={-45}
                    textAnchor="end"
                    height={60}
                    interval="preserveStartEnd"
                  />
                  <YAxis
                    className="text-xs"
                    tick={{ fill: "hsl(var(--muted-foreground))" }}
                    allowDecimals={false}
                    label={{ 
                      value: "Active SVs", 
                      angle: -90, 
                      position: "insideLeft",
                      style: { fill: "hsl(var(--muted-foreground))", fontSize: 11 }
                    }}
                  />
                  <Tooltip
                    content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      const activeSvs = payload.filter(p => (p.value as number) > 0);
                      return (
                        <div className="bg-popover border border-border rounded-lg p-3 shadow-lg max-w-xs">
                          <p className="text-sm font-medium mb-2">{label}</p>
                          <div className="space-y-1 max-h-48 overflow-y-auto">
                            {activeSvs.map((entry, i) => (
                              <div key={i} className="flex items-center gap-2 text-xs">
                                <div 
                                  className="w-2 h-2 rounded-full flex-shrink-0" 
                                  style={{ backgroundColor: entry.color as string }}
                                />
                                <span className="truncate">{entry.name}</span>
                              </div>
                            ))}
                          </div>
                          <p className="text-xs text-muted-foreground mt-2 pt-2 border-t border-border">
                            Total: {activeSvs.length} active SVs
                          </p>
                        </div>
                      );
                    }}
                  />
                  {svNames.map((name, i) => (
                    <Area
                      key={name}
                      type="stepAfter"
                      dataKey={name}
                      stackId="1"
                      stroke={colors[i]}
                      strokeWidth={hoveredSv === name ? 2 : 0.5}
                      fill={`url(#gradient-${i})`}
                      fillOpacity={hoveredSv && hoveredSv !== name ? 0.3 : 1}
                      onMouseEnter={() => setHoveredSv(name)}
                      onMouseLeave={() => setHoveredSv(null)}
                    />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            </ChartContainer>

            {/* Legend with scrollable SV list */}
            <div className="mt-4 border-t border-border pt-4">
              <p className="text-xs text-muted-foreground mb-2">Operators (hover to highlight):</p>
              <div className="flex flex-wrap gap-2 max-h-24 overflow-y-auto">
                {svNames.map((name, i) => (
                  <div
                    key={name}
                    className="flex items-center gap-1.5 px-2 py-1 rounded-md bg-muted/50 hover:bg-muted cursor-pointer transition-colors text-xs"
                    onMouseEnter={() => setHoveredSv(name)}
                    onMouseLeave={() => setHoveredSv(null)}
                    style={{
                      opacity: hoveredSv && hoveredSv !== name ? 0.5 : 1,
                      borderLeft: `3px solid ${colors[i]}`,
                    }}
                  >
                    <span className="truncate max-w-[100px]">{name}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </Card>
  );
};
