import { DashboardLayout } from "@/components/DashboardLayout";
import { StatCard } from "@/components/StatCard";
import { Activity, Coins, TrendingUp, Users, Zap, Package } from "lucide-react";
import { SearchBar } from "@/components/SearchBar";
import { Card } from "@/components/ui/card";
import { TriggerACSSnapshotButton } from "@/components/TriggerACSSnapshotButton";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";
import { Skeleton } from "@/components/ui/skeleton";
import { fetchConfigData } from "@/lib/config-sync";
const Dashboard = () => {
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
  return (
    <DashboardLayout>
      <div className="space-y-8">
        {/* Hero Section */}
        <div className="relative">
          <div className="absolute inset-0 gradient-primary rounded-2xl blur-3xl opacity-20" />
          <div className="relative glass-card p-8">
            <div className="flex justify-end mb-4">
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

        {/* Stats Grid */}
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

        {/* Canton Coin Price */}
        <Card className="glass-card"></Card>
      </div>
    </DashboardLayout>
  );
};
export default Dashboard;
