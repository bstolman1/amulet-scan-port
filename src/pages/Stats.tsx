import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { TrendingUp, Users, Calendar, Download, ChevronLeft, ChevronRight } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useToast } from "@/hooks/use-toast";
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, ResponsiveContainer, AreaChart, Area } from "recharts";
import { useUsageStats } from "@/hooks/use-usage-stats";
import { fetchConfigData, scheduleDailySync } from "@/lib/config-sync";
import { useEffect, useState } from "react";

const Stats = () => {
  // Schedule daily sync for config data
  useEffect(() => {
    const dispose = scheduleDailySync();
    return () => {
      dispose?.();
    };
  }, []);

  // Fetch real Super Validator configuration
  const { data: configData } = useQuery({
    queryKey: ["sv-config"],
    queryFn: () => fetchConfigData(),
    staleTime: 24 * 60 * 60 * 1000, // 24 hours
  });
  const { data: validators, isLoading: validatorsLoading } = useQuery({
    queryKey: ["topValidators"],
    queryFn: () => scanApi.fetchTopValidators(),
    retry: 1,
  });

  const { data: latestRound } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
  });

  const { data: roundTotals } = useQuery({
    queryKey: ["recentRoundTotals"],
    queryFn: async () => {
      if (!latestRound) return null;
      // Fetch last 30 rounds to get timing data
      return scanApi.fetchRoundTotals({
        start_round: Math.max(0, latestRound.round - 30),
        end_round: latestRound.round,
      });
    },
    enabled: !!latestRound,
  });

  // Usage statistics via transactions API
  const { data: usageChartData, isLoading: usageLoading, error: usageError } = useUsageStats(90);

  // Calculate rounds per day based on recent data using timestamps
  const roundsPerDay = (() => {
    const entries = roundTotals?.entries || [];
    if (entries.length >= 2) {
      const first = entries[0];
      const last = entries[entries.length - 1];
      const firstTime = new Date(first.closed_round_effective_at).getTime();
      const lastTime = new Date(last.closed_round_effective_at).getTime();
      const roundDiff = Math.max(1, last.closed_round - first.closed_round);
      const secondsPerRound = (lastTime - firstTime) / 1000 / roundDiff;
      const computed = secondsPerRound > 0 ? 86400 / secondsPerRound : 144;
      return Math.round(computed);
    }
    return 144; // Fallback estimate (10 min per round = 144/day)
  })();

  const currentRound = latestRound?.round || 0;
  const oneDayAgo = currentRound - roundsPerDay;
  const oneWeekAgo = currentRound - roundsPerDay * 7;
  const oneMonthAgo = currentRound - roundsPerDay * 30;
  const sixMonthsAgo = currentRound - roundsPerDay * 180;
  const oneYearAgo = currentRound - roundsPerDay * 365;

  // Get validator liveness data
  const validatorsList = validators?.validatorsAndRewards || [];

  // Get SV participant IDs to exclude them from regular validator counts
  const svParticipantIds = new Set(configData?.superValidators.map((sv) => sv.address) || []);

  // Filter validators by join period based on rounds collected (excluding SVs)
  const recentValidators = validatorsList.filter((v) => {
    const roundsCollected = parseFloat(v.rewards);
    return roundsCollected > 0 && !svParticipantIds.has(v.provider);
  });

  // Categorize validators by activity duration
  const newValidators = recentValidators.filter((v) => parseFloat(v.rewards) < roundsPerDay);
  const weeklyValidators = recentValidators.filter((v) => {
    const rounds = parseFloat(v.rewards);
    return rounds < roundsPerDay * 7 && rounds >= roundsPerDay;
  });
  const monthlyValidators = recentValidators.filter((v) => {
    const rounds = parseFloat(v.rewards);
    return rounds < roundsPerDay * 30 && rounds >= roundsPerDay * 7;
  });
  const sixMonthValidators = recentValidators.filter((v) => {
    const rounds = parseFloat(v.rewards);
    return rounds < roundsPerDay * 180 && rounds >= roundsPerDay * 30;
  });
  const yearlyValidators = recentValidators.filter((v) => {
    const rounds = parseFloat(v.rewards);
    return rounds < roundsPerDay * 365 && rounds >= roundsPerDay * 180;
  });
  const allTimeValidators = recentValidators;

  // Calculate monthly join data for all time since network launch
  const getMonthlyJoinData = () => {
    const monthlyData: { [key: string]: number } = {};
    const now = new Date();
    const networkStart = new Date("2024-06-01T00:00:00Z");

    // Helper function for consistent date formatting
    const formatMonth = (date: Date) => {
      const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
      return `${months[date.getMonth()]} ${date.getFullYear()}`;
    };

    // Initialize months from network start to now
    const iter = new Date(Date.UTC(networkStart.getFullYear(), networkStart.getMonth(), 1));
    const nowUTC = new Date(Date.UTC(now.getFullYear(), now.getMonth() + 1, 1));
    while (iter <= nowUTC) {
      const monthKey = formatMonth(iter);
      monthlyData[monthKey] = 0;
      iter.setUTCMonth(iter.getUTCMonth() + 1);
    }

    // Calculate join dates for validators using firstCollectedInRound
    recentValidators.forEach((validator) => {
      const firstRound = validator.firstCollectedInRound ?? 0;
      const roundsAgo = currentRound - firstRound;
      const daysAgo = roundsAgo / roundsPerDay;
      const joinDate = new Date(now.getTime() - daysAgo * 24 * 60 * 60 * 1000);

      if (joinDate >= networkStart) {
        const monthKey = formatMonth(joinDate);
        if (monthlyData.hasOwnProperty(monthKey)) {
          monthlyData[monthKey]++;
        }
      }
    });

    return Object.entries(monthlyData).map(([month, count]) => ({
      month,
      validators: count,
    }));
  };

  const monthlyChartData = getMonthlyJoinData();

  const { toast } = useToast();

  // Fetch validator liveness data for health/uptime metrics
  const { data: validatorLivenessData } = useQuery({
    queryKey: ["validatorLiveness", validatorsList.slice(0, 50).map((v) => v.provider)],
    queryFn: async () => {
      const validatorIds = validatorsList.slice(0, 50).map((v) => v.provider);
      if (validatorIds.length === 0) return null;
      return scanApi.fetchValidatorLiveness(validatorIds);
    },
    enabled: validatorsList.length > 0,
    retry: 1,
  });

  // Create a map of validator health data
  const validatorHealthMap = new Map(
    (validatorLivenessData?.validatorsReceivedFaucets || []).map((v) => [
      v.validator,
      {
        collected: v.numRoundsCollected,
        missed: v.numRoundsMissed,
        uptime: (v.numRoundsCollected / (v.numRoundsCollected + v.numRoundsMissed)) * 100,
      },
    ]),
  );

  // Get real Super Validator count from config
  const superValidatorCount = configData?.superValidators.length || 0;

  // Calculate inactive validators (missed more than 1 round)
  const inactiveValidators = recentValidators.filter((v) => {
    const healthData = validatorHealthMap.get(v.provider);
    return healthData && healthData.missed > 1;
  });

  // Calculate non-SV validator count
  const nonSvValidatorCount = recentValidators.length;

  const formatPartyId = (partyId: string) => {
    const parts = partyId.split("::");
    return parts[0] || partyId;
  };

  const exportToCSV = () => {
    try {
      // Prepare CSV content
      const csvRows = [];

      // Header
      csvRows.push(["Canton Network Validator Statistics"]);
      csvRows.push(["Generated:", new Date().toISOString()]);
      csvRows.push(["Current Round:", currentRound]);
      csvRows.push([]);

      // Summary statistics
      csvRows.push(["Summary Statistics"]);
      csvRows.push(["Period", "New Validators"]);
      csvRows.push(["Last 24 Hours", newValidators.length]);
      csvRows.push(["Last 7 Days", weeklyValidators.length + newValidators.length]);
      csvRows.push(["Last 30 Days", monthlyValidators.length + weeklyValidators.length + newValidators.length]);
      csvRows.push([
        "Last 6 Months",
        sixMonthValidators.length + monthlyValidators.length + weeklyValidators.length + newValidators.length,
      ]);
      csvRows.push([
        "Last Year",
        yearlyValidators.length +
          sixMonthValidators.length +
          monthlyValidators.length +
          weeklyValidators.length +
          newValidators.length,
      ]);
      csvRows.push(["All Time", allTimeValidators.length]);
      csvRows.push([]);

      // Detailed validator list
      csvRows.push(["All Active Validators"]);
      csvRows.push(["Provider Name", "Provider ID", "Rounds Collected"]);

      allTimeValidators.forEach((validator) => {
        csvRows.push([formatPartyId(validator.provider), validator.provider, parseFloat(validator.rewards).toFixed(0)]);
      });

      // Convert to CSV string
      const csvContent = csvRows.map((row) => row.map((cell) => `"${cell}"`).join(",")).join("\n");

      // Create and download file
      const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
      const link = document.createElement("a");
      const url = URL.createObjectURL(blob);
      link.setAttribute("href", url);
      link.setAttribute("download", `validator-stats-${new Date().toISOString().split("T")[0]}.csv`);
      link.style.visibility = "hidden";
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      toast({
        title: "Export successful",
        description: "Statistics have been exported to CSV",
      });
    } catch (error) {
      toast({
        title: "Export failed",
        description: "There was an error exporting the statistics",
        variant: "destructive",
      });
    }
  };

  const ValidatorList = ({ validators, title }: { validators: any[]; title: string }) => {
    const [currentPage, setCurrentPage] = useState(1);
    const itemsPerPage = 10;
    const totalPages = Math.ceil(validators.length / itemsPerPage);

    // Reset to page 1 when validators change
    useEffect(() => {
      setCurrentPage(1);
    }, [validators.length]);

    const paginatedValidators = validators.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);

    return (
      <div className="space-y-3">
        <h4 className="text-sm font-semibold text-muted-foreground">
          {title} ({validators.length})
        </h4>
        {validators.length === 0 ? (
          <p className="text-sm text-muted-foreground italic">No validators in this period</p>
        ) : (
          <>
            <div className="space-y-2">
              {paginatedValidators.map((validator, index) => {
                // Check if validator is actually a Super Validator by matching address
                const isSuperValidator =
                  configData?.superValidators.some((sv) => sv.address === validator.provider) || false;
                const healthData = validatorHealthMap.get(validator.provider);
                const uptime = healthData ? healthData.uptime : null;
                const healthColor =
                  uptime !== null
                    ? uptime >= 95
                      ? "text-success"
                      : uptime >= 85
                        ? "text-warning"
                        : "text-destructive"
                    : "text-muted-foreground";

                return (
                  <div
                    key={validator.provider}
                    className="p-3 rounded-lg bg-muted/30 flex items-center justify-between hover:bg-muted/50 transition-colors"
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <p className="font-medium truncate">{formatPartyId(validator.provider)}</p>
                        {isSuperValidator && (
                          <Badge variant="outline" className="bg-primary/10 text-primary border-primary/20 text-xs">
                            SV
                          </Badge>
                        )}
                      </div>
                      <p className="text-xs text-muted-foreground font-mono truncate">{validator.provider}</p>
                    </div>
                    <div className="flex items-center gap-3 ml-4">
                      {healthData && (
                        <>
                          <div className="text-right">
                            <p className="text-xs text-muted-foreground">Health</p>
                            <p className={`text-sm font-bold ${healthColor}`}>
                              {uptime !== null ? `${uptime.toFixed(1)}%` : "N/A"}
                            </p>
                          </div>
                          <div className="text-right">
                            <p className="text-xs text-muted-foreground">Missed</p>
                            <Badge
                              variant="outline"
                              className={
                                healthData.missed > 1 ? "bg-destructive/10 text-destructive border-destructive/20" : ""
                              }
                            >
                              {healthData.missed}
                            </Badge>
                          </div>
                        </>
                      )}
                      <div className="text-right">
                        <p className="text-xs text-muted-foreground">Rounds</p>
                        <Badge variant="outline" className="shrink-0">
                          {parseFloat(validator.rewards).toLocaleString()}
                        </Badge>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
            {totalPages > 1 && (
              <div className="flex items-center justify-between pt-4 border-t border-border/50">
                <p className="text-sm text-muted-foreground">
                  Showing {(currentPage - 1) * itemsPerPage + 1} to{" "}
                  {Math.min(currentPage * itemsPerPage, validators.length)} of {validators.length} validators
                </p>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
                    disabled={currentPage === 1}
                  >
                    <ChevronLeft className="h-4 w-4" />
                    Previous
                  </Button>
                  <span className="text-sm text-muted-foreground">
                    Page {currentPage} of {totalPages}
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setCurrentPage((prev) => Math.min(prev + 1, totalPages))}
                    disabled={currentPage === totalPages}
                  >
                    Next
                    <ChevronRight className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    );
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold mb-2">Validator Statistics</h2>
            <p className="text-muted-foreground">
              Track validator growth and onboarding trends • {nonSvValidatorCount} validators (excluding{" "}
              {superValidatorCount} Super Validators) • {inactiveValidators.length} inactive
            </p>
          </div>
          <Button onClick={exportToCSV} disabled={validatorsLoading} variant="outline" className="gap-2">
            <Download className="h-4 w-4" />
            Export CSV
          </Button>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
          <Card className="glass-card">
            <div className="p-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs font-medium text-muted-foreground">Last 24 Hours</h3>
                <Calendar className="h-4 w-4 text-primary" />
              </div>
              {validatorsLoading ? (
                <Skeleton className="h-10 w-16" />
              ) : (
                <>
                  <p className="text-3xl font-bold text-primary mb-1">{newValidators.length}</p>
                  <p className="text-xs text-muted-foreground">New validators</p>
                </>
              )}
            </div>
          </Card>

          <Card className="glass-card">
            <div className="p-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs font-medium text-muted-foreground">Last 7 Days</h3>
                <TrendingUp className="h-4 w-4 text-chart-2" />
              </div>
              {validatorsLoading ? (
                <Skeleton className="h-10 w-16" />
              ) : (
                <>
                  <p className="text-3xl font-bold text-chart-2 mb-1">
                    {weeklyValidators.length + newValidators.length}
                  </p>
                  <p className="text-xs text-muted-foreground">New validators</p>
                </>
              )}
            </div>
          </Card>

          <Card className="glass-card">
            <div className="p-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs font-medium text-muted-foreground">Last 30 Days</h3>
                <Users className="h-4 w-4 text-chart-3" />
              </div>
              {validatorsLoading ? (
                <Skeleton className="h-10 w-16" />
              ) : (
                <>
                  <p className="text-3xl font-bold text-chart-3 mb-1">
                    {monthlyValidators.length + weeklyValidators.length + newValidators.length}
                  </p>
                  <p className="text-xs text-muted-foreground">New validators</p>
                </>
              )}
            </div>
          </Card>

          <Card className="glass-card">
            <div className="p-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs font-medium text-muted-foreground">Last 6 Months</h3>
                <TrendingUp className="h-4 w-4 text-chart-4" />
              </div>
              {validatorsLoading ? (
                <Skeleton className="h-10 w-16" />
              ) : (
                <>
                  <p className="text-3xl font-bold text-chart-4 mb-1">
                    {sixMonthValidators.length +
                      monthlyValidators.length +
                      weeklyValidators.length +
                      newValidators.length}
                  </p>
                  <p className="text-xs text-muted-foreground">New validators</p>
                </>
              )}
            </div>
          </Card>

          <Card className="glass-card">
            <div className="p-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs font-medium text-muted-foreground">Last Year</h3>
                <TrendingUp className="h-4 w-4 text-chart-5" />
              </div>
              {validatorsLoading ? (
                <Skeleton className="h-10 w-16" />
              ) : (
                <>
                  <p className="text-3xl font-bold text-chart-5 mb-1">
                    {yearlyValidators.length +
                      sixMonthValidators.length +
                      monthlyValidators.length +
                      weeklyValidators.length +
                      newValidators.length}
                  </p>
                  <p className="text-xs text-muted-foreground">New validators</p>
                </>
              )}
            </div>
          </Card>

          <Card className="glass-card">
            <div className="p-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="text-xs font-medium text-muted-foreground">All Time</h3>
                <Users className="h-4 w-4 text-primary" />
              </div>
              {validatorsLoading ? (
                <Skeleton className="h-10 w-16" />
              ) : (
                <>
                  <p className="text-3xl font-bold gradient-text mb-1">{allTimeValidators.length}</p>
                  <p className="text-xs text-muted-foreground">Total validators</p>
                </>
              )}
            </div>
          </Card>
        </div>

        {/* Detailed Lists */}
        <Card className="glass-card">
          <div className="p-6">
            <h3 className="text-xl font-bold mb-6">Recently Joined Validators</h3>
            {validatorsLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-20 w-full" />
                ))}
              </div>
            ) : (
              <Tabs defaultValue="day" className="w-full">
                <TabsList className="grid w-full grid-cols-6">
                  <TabsTrigger value="day">Day</TabsTrigger>
                  <TabsTrigger value="week">Week</TabsTrigger>
                  <TabsTrigger value="month">Month</TabsTrigger>
                  <TabsTrigger value="6months">6 Months</TabsTrigger>
                  <TabsTrigger value="year">Year</TabsTrigger>
                  <TabsTrigger value="all">All Time</TabsTrigger>
                </TabsList>
                <TabsContent value="day" className="mt-6">
                  <ValidatorList validators={newValidators} title="Validators with < 1 day of activity" />
                </TabsContent>
                <TabsContent value="week" className="mt-6">
                  <ValidatorList
                    validators={[...newValidators, ...weeklyValidators]}
                    title="Validators with < 7 days of activity"
                  />
                </TabsContent>
                <TabsContent value="month" className="mt-6">
                  <ValidatorList
                    validators={[...newValidators, ...weeklyValidators, ...monthlyValidators]}
                    title="Validators with < 30 days of activity"
                  />
                </TabsContent>
                <TabsContent value="6months" className="mt-6">
                  <ValidatorList
                    validators={[...newValidators, ...weeklyValidators, ...monthlyValidators, ...sixMonthValidators]}
                    title="Validators with < 6 months of activity"
                  />
                </TabsContent>
                <TabsContent value="year" className="mt-6">
                  <ValidatorList
                    validators={[
                      ...newValidators,
                      ...weeklyValidators,
                      ...monthlyValidators,
                      ...sixMonthValidators,
                      ...yearlyValidators,
                    ]}
                    title="Validators with < 1 year of activity"
                  />
                </TabsContent>
                <TabsContent value="all" className="mt-6">
                  <ValidatorList validators={allTimeValidators} title="All active validators" />
                </TabsContent>
              </Tabs>
            )}
          </div>
        </Card>

        {/* Usage Statistics Section */}
        <div className="space-y-6">
          <div className="flex items-center justify-between">
            <h3 className="text-2xl font-bold">Usage Statistics</h3>
            {!usageLoading && !usageError && usageChartData && (
              <Badge variant="outline" className="bg-success/10 text-success border-success/20">
                ✓ Real-time data
              </Badge>
            )}
            {usageLoading && (
              <Badge variant="outline" className="bg-muted">
                Loading...
              </Badge>
            )}
            {usageError && (
              <Badge variant="outline" className="bg-destructive/10 text-destructive border-destructive/20">
                ⚠ API Error
              </Badge>
            )}
          </div>

          {usageError && (
            <Card className="glass-card border-destructive/20">
              <div className="p-4">
                <div className="flex items-start gap-3">
                  <div className="flex-shrink-0 w-2 h-2 mt-2 rounded-full bg-destructive animate-pulse" />
                  <div className="flex-1">
                    <p className="text-sm font-medium text-destructive mb-1">Unable to fetch usage statistics</p>
                    <p className="text-xs text-muted-foreground">
                      The transaction API is currently unavailable or timing out. This may be due to network
                      connectivity or API rate limits.
                    </p>
                  </div>
                </div>
              </div>
            </Card>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Cumulative Unique Parties */}
            <Card className="glass-card">
              <div className="p-6">
                <h4 className="text-lg font-semibold mb-4">Cumulative Unique Parties</h4>
                {usageLoading ? (
                  <Skeleton className="h-[250px] w-full" />
                ) : usageError ? (
                  <div className="h-[250px] flex items-center justify-center">
                    <div className="text-center">
                      <p className="text-sm text-destructive mb-2">Failed to load data</p>
                      <p className="text-xs text-muted-foreground">API connection issue</p>
                    </div>
                  </div>
                ) : usageChartData.cumulativeParties.length === 0 ? (
                  <div className="h-[250px] flex items-center justify-center">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground mb-2">No data available</p>
                      <p className="text-xs text-muted-foreground">Total Parties: {usageChartData.totalParties || 0}</p>
                    </div>
                  </div>
                ) : (
                  <ChartContainer
                    config={{
                      parties: {
                        label: "Cumulative Parties",
                        color: "hsl(var(--chart-1))",
                      },
                    }}
                    className="h-[250px] w-full"
                  >
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={usageChartData.cumulativeParties}>
                        <defs>
                          <linearGradient id="colorParties" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="hsl(var(--chart-1))" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="hsl(var(--chart-1))" stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" opacity={0.3} />
                        <XAxis
                          dataKey="date"
                          className="text-xs"
                          tick={{ fill: "hsl(var(--muted-foreground))" }}
                          tickFormatter={(value) => {
                            const date = new Date(value);
                            return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
                          }}
                        />
                        <YAxis
                          className="text-xs"
                          tick={{ fill: "hsl(var(--muted-foreground))" }}
                          tickFormatter={(value) => value.toLocaleString()}
                        />
                        <ChartTooltip
                          content={<ChartTooltipContent />}
                          labelFormatter={(value) => {
                            const date = new Date(value);
                            return date.toLocaleDateString("en-US", {
                              month: "short",
                              day: "numeric",
                              year: "numeric",
                            });
                          }}
                        />
                        <Area
                          type="monotone"
                          dataKey="parties"
                          stroke="hsl(var(--chart-1))"
                          fill="url(#colorParties)"
                          strokeWidth={2}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </ChartContainer>
                )}
              </div>
            </Card>

            {/* Daily Active Users */}
            <Card className="glass-card">
              <div className="p-6">
                <h4 className="text-lg font-semibold mb-4">Daily Active Users</h4>
                {usageLoading ? (
                  <Skeleton className="h-[250px] w-full" />
                ) : usageError ? (
                  <div className="h-[250px] flex items-center justify-center">
                    <div className="text-center">
                      <p className="text-sm text-destructive mb-2">Failed to load data</p>
                      <p className="text-xs text-muted-foreground">API connection issue</p>
                    </div>
                  </div>
                ) : usageChartData.dailyActiveUsers.length === 0 ? (
                  <div className="h-[250px] flex items-center justify-center">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground mb-2">No data available</p>
                      <p className="text-xs text-muted-foreground">Avg Users: {usageChartData.totalDailyUsers || 0}</p>
                    </div>
                  </div>
                ) : (
                  <ChartContainer
                    config={{
                      daily: {
                        label: "Users (Daily)",
                        color: "hsl(var(--chart-2))",
                      },
                      avg7d: {
                        label: "Users (7d Avg)",
                        color: "hsl(var(--chart-3))",
                      },
                    }}
                    className="h-[250px] w-full"
                  >
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={usageChartData.dailyActiveUsers}>
                        <defs>
                          <linearGradient id="colorDaily" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="hsl(var(--chart-2))" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="hsl(var(--chart-2))" stopOpacity={0} />
                          </linearGradient>
                          <linearGradient id="colorAvg" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="hsl(var(--chart-3))" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="hsl(var(--chart-3))" stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" opacity={0.3} />
                        <XAxis
                          dataKey="date"
                          className="text-xs"
                          tick={{ fill: "hsl(var(--muted-foreground))" }}
                          tickFormatter={(value) => {
                            const date = new Date(value);
                            return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
                          }}
                        />
                        <YAxis className="text-xs" tick={{ fill: "hsl(var(--muted-foreground))" }} />
                        <ChartTooltip
                          content={<ChartTooltipContent />}
                          labelFormatter={(value) => {
                            const date = new Date(value);
                            return date.toLocaleDateString("en-US", {
                              month: "short",
                              day: "numeric",
                              year: "numeric",
                            });
                          }}
                        />
                        <Area
                          type="monotone"
                          dataKey="daily"
                          stroke="hsl(var(--chart-2))"
                          fill="url(#colorDaily)"
                          strokeWidth={2}
                        />
                        <Area
                          type="monotone"
                          dataKey="avg7d"
                          stroke="hsl(var(--chart-3))"
                          fill="url(#colorAvg)"
                          strokeWidth={2}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </ChartContainer>
                )}
              </div>
            </Card>

            {/* Daily Transactions */}
            <Card className="glass-card">
              <div className="p-6">
                <h4 className="text-lg font-semibold mb-4">Daily Transactions</h4>
                {usageLoading ? (
                  <Skeleton className="h-[250px] w-full" />
                ) : usageError ? (
                  <div className="h-[250px] flex items-center justify-center">
                    <div className="text-center">
                      <p className="text-sm text-destructive mb-2">Failed to load data</p>
                      <p className="text-xs text-muted-foreground">API connection issue</p>
                    </div>
                  </div>
                ) : usageChartData.dailyTransactions.length === 0 ? (
                  <div className="h-[250px] flex items-center justify-center">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground mb-2">No data available</p>
                      <p className="text-xs text-muted-foreground">
                        Total TX: {usageChartData.totalTransactions?.toLocaleString() || 0}
                      </p>
                    </div>
                  </div>
                ) : (
                  <ChartContainer
                    config={{
                      daily: {
                        label: "TX (Daily)",
                        color: "hsl(var(--chart-4))",
                      },
                      avg7d: {
                        label: "TX (7d Avg)",
                        color: "hsl(var(--chart-5))",
                      },
                    }}
                    className="h-[250px] w-full"
                  >
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={usageChartData.dailyTransactions}>
                        <defs>
                          <linearGradient id="colorTxDaily" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="hsl(var(--chart-4))" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="hsl(var(--chart-4))" stopOpacity={0} />
                          </linearGradient>
                          <linearGradient id="colorTxAvg" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="hsl(var(--chart-5))" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="hsl(var(--chart-5))" stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" opacity={0.3} />
                        <XAxis
                          dataKey="date"
                          className="text-xs"
                          tick={{ fill: "hsl(var(--muted-foreground))" }}
                          tickFormatter={(value) => {
                            const date = new Date(value);
                            return date.toLocaleDateString("en-US", { month: "short", day: "numeric" });
                          }}
                        />
                        <YAxis
                          className="text-xs"
                          tick={{ fill: "hsl(var(--muted-foreground))" }}
                          tickFormatter={(value) => value.toLocaleString()}
                        />
                        <ChartTooltip
                          content={<ChartTooltipContent />}
                          labelFormatter={(value) => {
                            const date = new Date(value);
                            return date.toLocaleDateString("en-US", {
                              month: "short",
                              day: "numeric",
                              year: "numeric",
                            });
                          }}
                        />
                        <Area
                          type="monotone"
                          dataKey="daily"
                          stroke="hsl(var(--chart-4))"
                          fill="url(#colorTxDaily)"
                          strokeWidth={2}
                        />
                        <Area
                          type="monotone"
                          dataKey="avg7d"
                          stroke="hsl(var(--chart-5))"
                          fill="url(#colorTxAvg)"
                          strokeWidth={2}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </ChartContainer>
                )}
              </div>
            </Card>
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
};

export default Stats;
