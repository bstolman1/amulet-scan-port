import { DashboardLayout } from "@/components/DashboardLayout";
import { StatCard } from "@/components/StatCard";
import { Activity, Coins, TrendingUp, Users, Zap, Package, Database, Clock, Lock, FileText } from "lucide-react";
import { SearchBar } from "@/components/SearchBar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TriggerACSSnapshotButton } from "@/components/TriggerACSSnapshotButton";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { fetchConfigData } from "@/lib/config-sync";
import { useLocalOverviewStats, useLocalApiAvailable, useLocalDailyStats } from "@/hooks/use-local-stats";
import { Badge } from "@/components/ui/badge";
import { useACSStatus, useLocalACSStats, useLocalLatestACSSnapshot, useLocalACSTemplates } from "@/hooks/use-local-acs";

const Dashboard = () => {
  // Check if local API is available
  const { data: localApiAvailable } = useLocalApiAvailable();
  
  // Local DuckDB stats (from backfilled data)
  const { data: localStats, isLoading: localStatsLoading } = useLocalOverviewStats();
  const { data: dailyStats } = useLocalDailyStats(7);
  
  // Local ACS snapshot data
  const { data: acsStatus } = useACSStatus();
  const { data: acsStats, isLoading: acsStatsLoading } = useLocalACSStats();
  const { data: latestAcsSnapshot, isLoading: acsSnapshotLoading } = useLocalLatestACSSnapshot();
  const { data: acsTemplates, isLoading: acsTemplatesLoading } = useLocalACSTemplates(10);
  
  // Fetch real data from Canton Scan API
  const { data: latestRound } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
  });
  const {
    data: totalBalance,
    isError: balanceError,
    isLoading: balanceLoading,
  } = useQuery({
    queryKey: ["totalBalance"],
    queryFn: () => scanApi.fetchTotalBalance(),
    retry: 2,
    retryDelay: 1000,
  });
  const { data: topValidators, isError: validatorsError } = useQuery({
    queryKey: ["topValidators"],
    queryFn: () => scanApi.fetchTopValidators(),
    retry: 1,
  });
  const { data: topProviders } = useQuery({
    queryKey: ["topProviders"],
    queryFn: () => scanApi.fetchTopProviders(),
    retry: 1,
  });
  const { data: transactions } = useQuery({
    queryKey: ["recentTransactions"],
    queryFn: () =>
      scanApi.fetchTransactions({
        page_size: 5,
        sort_order: "desc",
      }),
  });
  const { data: configData } = useQuery({
    queryKey: ["sv-config"],
    queryFn: () => fetchConfigData(),
    staleTime: 24 * 60 * 60 * 1000,
  });

  // Calculate total rewards from validators (rounds collected) and providers (app rewards)
  const totalValidatorRounds =
    topValidators?.validatorsAndRewards.reduce((sum, v) => sum + parseFloat(v.rewards), 0) || 0;
  const totalAppRewards = topProviders?.providersAndRewards.reduce((sum, p) => sum + parseFloat(p.rewards), 0) || 0;
  const ccPrice = transactions?.transactions?.[0]?.amulet_price
    ? parseFloat(transactions.transactions[0].amulet_price)
    : undefined;
  const marketCap =
    totalBalance?.total_balance && ccPrice !== undefined
      ? (parseFloat(totalBalance.total_balance) * ccPrice).toLocaleString(undefined, {
          maximumFractionDigits: 0,
        })
      : "Loading...";
  const superValidatorCount = configData?.operators?.length || 0;

  const stats = {
    totalBalance: balanceLoading
      ? "Loading..."
      : balanceError
        ? "Connection Failed"
        : totalBalance?.total_balance
          ? parseFloat(totalBalance.total_balance).toLocaleString(undefined, {
              maximumFractionDigits: 2,
            })
          : "Loading...",
    marketCap: balanceLoading ? "Loading..." : balanceError ? "Connection Failed" : marketCap,
    superValidators: configData ? superValidatorCount.toString() : "Loading...",
    currentRound: latestRound?.round.toLocaleString() || "Loading...",
    coinPrice: ccPrice !== undefined ? `$${ccPrice.toFixed(4)}` : "Loading...",
    totalRewards:
      totalAppRewards > 0
        ? parseFloat(totalAppRewards.toString()).toLocaleString(undefined, {
            maximumFractionDigits: 2,
          })
        : "Connection Failed",
    networkHealth: "99.9%",
  };

  // Format local stats
  const formatLocalStats = () => {
    if (!localStats) return null;
    return {
      totalEvents: localStats.total_events?.toLocaleString() || "0",
      uniqueContracts: localStats.unique_contracts?.toLocaleString() || "0",
      uniqueTemplates: localStats.unique_templates?.toLocaleString() || "0",
      earliestEvent: localStats.earliest_event 
        ? new Date(localStats.earliest_event).toLocaleDateString() 
        : "N/A",
      latestEvent: localStats.latest_event 
        ? new Date(localStats.latest_event).toLocaleDateString() 
        : "N/A",
      dataSource: localStats.data_source || "unknown",
    };
  };

  const localStatsFormatted = formatLocalStats();

  // Calculate week's activity from daily stats
  const weekActivity = dailyStats?.reduce((sum, d) => sum + d.event_count, 0) || 0;

  return (
    <DashboardLayout>
      <div className="space-y-8">
        {/* Hero Section */}
        <div className="relative">
          <div className="absolute inset-0 gradient-primary rounded-2xl blur-3xl opacity-20" />
          <div className="relative glass-card p-8">
            <div className="flex justify-between items-start mb-4">
              <div className="flex items-center gap-2">
                {localApiAvailable && (
                  <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
                    <Database className="h-3 w-3 mr-1" />
                    Local Data Connected
                  </Badge>
                )}
              </div>
              <TriggerACSSnapshotButton />
            </div>
            <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
              <div>
                <h2 className="text-4xl font-bold mb-2">Welcome to SCANTON</h2>
                <p className="text-lg text-muted-foreground">
                  Explore transactions, validators, and network statistics
                </p>
              </div>
              <div className="w-full md:w-[420px]">
                {/* Local search beside hero title */}
                <SearchBar />
              </div>
            </div>
          </div>
        </div>

        {/* Live Network Stats Grid */}
        <div>
          <h3 className="text-xl font-semibold mb-4 flex items-center gap-2">
            <Activity className="h-5 w-5 text-primary" />
            Live Network Stats
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <StatCard title="Total Amulet Balance" value={stats.totalBalance} icon={Coins} gradient />
            <StatCard
              title="Canton Coin Price (USD)"
              value={stats.coinPrice}
              icon={Activity}
              trend={{
                value: "",
                positive: true,
              }}
            />
            <StatCard title="Market Cap (USD)" value={`$${stats.marketCap}`} icon={Users} />
            <StatCard title="Current Round" value={stats.currentRound} icon={Package} />
            <StatCard title="Super Validators" value={stats.superValidators} icon={Zap} />
            <StatCard title="Cumulative App Rewards" value={stats.totalRewards} icon={TrendingUp} gradient />
          </div>
        </div>

        {/* Local Backfill Stats - Only shown if local API is available */}
        {localApiAvailable && (
          <div>
            <h3 className="text-xl font-semibold mb-4 flex items-center gap-2">
              <Database className="h-5 w-5 text-primary" />
              Local Backfill Data
              {localStatsFormatted?.dataSource && (
                <Badge variant="secondary" className="text-xs">
                  Source: {localStatsFormatted.dataSource}
                </Badge>
              )}
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">Total Events</CardTitle>
                </CardHeader>
                <CardContent>
                  {localStatsLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <p className="text-2xl font-bold">{localStatsFormatted?.totalEvents || "0"}</p>
                  )}
                </CardContent>
              </Card>
              
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">Unique Contracts</CardTitle>
                </CardHeader>
                <CardContent>
                  {localStatsLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <p className="text-2xl font-bold">{localStatsFormatted?.uniqueContracts || "0"}</p>
                  )}
                </CardContent>
              </Card>
              
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">Unique Templates</CardTitle>
                </CardHeader>
                <CardContent>
                  {localStatsLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <p className="text-2xl font-bold">{localStatsFormatted?.uniqueTemplates || "0"}</p>
                  )}
                </CardContent>
              </Card>
              
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    Data Range
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {localStatsLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <div className="text-sm">
                      <p className="font-medium">{localStatsFormatted?.earliestEvent}</p>
                      <p className="text-muted-foreground">to {localStatsFormatted?.latestEvent}</p>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
            
            {weekActivity > 0 && (
              <Card className="glass-card mt-4">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">
                    Last 7 Days Activity
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-2xl font-bold">{weekActivity.toLocaleString()} events</p>
                  <div className="mt-2 flex gap-1">
                    {dailyStats?.slice(0, 7).reverse().map((day, i) => {
                      const maxCount = Math.max(...(dailyStats?.map(d => d.event_count) || [1]));
                      const height = Math.max(4, (day.event_count / maxCount) * 40);
                      return (
                        <div
                          key={day.date}
                          className="flex-1 bg-primary/20 rounded-t"
                          style={{ height: `${height}px` }}
                          title={`${day.date}: ${day.event_count} events`}
                        />
                      );
                    })}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Local ACS Snapshot Data - Only shown if ACS data is available */}
        {acsStatus?.available && (
          <div>
            <h3 className="text-xl font-semibold mb-4 flex items-center gap-2">
              <FileText className="h-5 w-5 text-primary" />
              Active Contract Set (ACS)
              {latestAcsSnapshot && (
                <Badge variant="secondary" className="text-xs">
                  Snapshot: {new Date(latestAcsSnapshot.record_time).toLocaleString()}
                </Badge>
              )}
            </h3>
            
            {/* ACS Overview Stats */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-4">
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                    <Package className="h-3 w-3" />
                    Total Contracts
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {acsStatsLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <p className="text-2xl font-bold text-primary">
                      {acsStats?.total_contracts?.toLocaleString() || "0"}
                    </p>
                  )}
                </CardContent>
              </Card>
              
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                    <FileText className="h-3 w-3" />
                    Total Templates
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {acsStatsLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <p className="text-2xl font-bold">
                      {acsStats?.total_templates?.toLocaleString() || "0"}
                    </p>
                  )}
                </CardContent>
              </Card>
              
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                    <Coins className="h-3 w-3" />
                    Migration ID
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {acsSnapshotLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <p className="text-2xl font-bold">
                      {latestAcsSnapshot?.migration_id ?? "N/A"}
                    </p>
                  )}
                </CardContent>
              </Card>
              
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    Entry Count
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {acsSnapshotLoading ? (
                    <Skeleton className="h-8 w-24" />
                  ) : (
                    <p className="text-2xl font-bold">
                      {latestAcsSnapshot?.entry_count?.toLocaleString() ?? "0"}
                    </p>
                  )}
                </CardContent>
              </Card>
            </div>
            
            {/* Top Templates */}
            {acsTemplates && acsTemplates.length > 0 && (
              <Card className="glass-card">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm font-medium text-muted-foreground">
                    Top Templates by Contract Count
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {acsTemplatesLoading ? (
                      <>
                        <Skeleton className="h-6 w-full" />
                        <Skeleton className="h-6 w-full" />
                        <Skeleton className="h-6 w-full" />
                      </>
                    ) : (
                      acsTemplates.slice(0, 5).map((template, i) => (
                        <div key={template.template_id || i} className="flex items-center justify-between text-sm">
                          <span className="font-mono text-xs truncate max-w-[60%]" title={template.template_id}>
                            {template.entity_name || template.template_id?.split(':').pop() || 'Unknown'}
                          </span>
                          <Badge variant="secondary">
                            {template.contract_count?.toLocaleString() || 0} contracts
                          </Badge>
                        </div>
                      ))
                    )}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        )}

        {/* Canton Coin Price placeholder */}
        <Card className="glass-card"></Card>
      </div>
    </DashboardLayout>
  );
};
export default Dashboard;
