import { DashboardLayout } from "@/components/DashboardLayout";
import { StatCard } from "@/components/StatCard";
import { Activity, Coins, Users, Package } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { scanApi } from "@/lib/api-client";

const Dashboard = () => {
  const { data: latestRound } = useQuery({
    queryKey: ["latestRound"],
    queryFn: () => scanApi.fetchLatestRound(),
  });
  const { data: dsoInfo } = useQuery({
    queryKey: ["dsoInfo"],
    queryFn: () => scanApi.fetchDsoInfo(),
    staleTime: 30000,
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
  const { data: transactions } = useQuery({
    queryKey: ["recentTransactions"],
    queryFn: () =>
      scanApi.fetchTransactions({
        page_size: 5,
        sort_order: "desc",
      }),
  });

  const ccPrice = (() => {
    const dsoPrice = (dsoInfo as any)?.latest_mining_round?.contract?.payload?.amuletPrice;
    if (dsoPrice) return parseFloat(dsoPrice);
    const txPrice = transactions?.transactions?.[0]?.amulet_price;
    if (txPrice) return parseFloat(txPrice);
    return undefined;
  })();

  const marketCap =
    totalBalance?.total_balance && ccPrice !== undefined
      ? (parseFloat(totalBalance.total_balance) * ccPrice).toLocaleString(undefined, {
          maximumFractionDigits: 0,
        })
      : "Loading...";

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
    currentRound: latestRound?.round.toLocaleString() || "Loading...",
    coinPrice: ccPrice !== undefined ? `$${ccPrice.toFixed(4)}` : "Loading...",
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="relative">
          <div className="absolute inset-0 gradient-primary rounded-xl blur-3xl opacity-20" />
          <div className="relative glass-card p-6 text-center">
            <h2 className="text-3xl font-bold mb-1">Welcome to SCANTON</h2>
            <p className="text-muted-foreground">
              Explore governance, validators, and network statistics
            </p>
          </div>
        </div>

        <div>
          <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
            <Activity className="h-4 w-4 text-primary" />
            Live Network Stats
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <StatCard title="Total Amulet Balance" value={stats.totalBalance} icon={Coins} gradient />
            <StatCard
              title="Canton Coin Price (USD)"
              value={stats.coinPrice}
              icon={Activity}
              trend={{ value: "", positive: true }}
            />
            <StatCard title="Market Cap (USD)" value={`$${stats.marketCap}`} icon={Users} />
            <StatCard title="Current Round" value={stats.currentRound} icon={Package} />
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
};

export default Dashboard;
