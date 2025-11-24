import DashboardLayout from "@/components/DashboardLayout";
import { useAcsSnapshots } from "@/hooks/use-acs-snapshots";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";
import { pickAmount, pickLockedAmount } from "@/lib/amount-utils";
import StatCard from "@/components/StatCard";

const Supply = () => {
  const { data: snapshots, isLoading } = useAcsSnapshots({ limit: 1 });

  const latestSnapshot = snapshots?.[0];
  const templates = latestSnapshot?.snapshot_data as any;

  const amulets = templates?.["Splice:Amulet:Amulet"] || [];
  const lockedAmulets = templates?.["Splice:Amulet:LockedAmulet"] || [];

  const totalSupply = amulets.reduce((sum: number, contract: any) => {
    return sum + pickAmount(contract);
  }, 0);

  const totalLocked = lockedAmulets.reduce((sum: number, contract: any) => {
    return sum + pickLockedAmount(contract);
  }, 0);

  const circulating = totalSupply - totalLocked;
  const lockPercentage = totalSupply > 0 ? (totalLocked / totalSupply) * 100 : 0;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Supply</h1>
          <p className="text-muted-foreground">
            Amulet supply breakdown and distribution
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
              <StatCard
                title="Total Supply"
                value={`${totalSupply.toLocaleString()} CC`}
                description="All Amulets"
              />
              <StatCard
                title="Circulating"
                value={`${circulating.toLocaleString()} CC`}
                description="Unlocked Amulets"
              />
              <StatCard
                title="Locked"
                value={`${totalLocked.toLocaleString()} CC`}
                description="Locked Amulets"
              />
              <StatCard
                title="Lock Rate"
                value={`${lockPercentage.toFixed(2)}%`}
                description="Percentage locked"
              />
            </div>

            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4">Supply Distribution</h3>
              <div className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-muted-foreground">Circulating Supply</span>
                  <div className="text-right">
                    <div className="font-medium">{circulating.toLocaleString()} CC</div>
                    <div className="text-sm text-muted-foreground">
                      {totalSupply > 0 ? ((circulating / totalSupply) * 100).toFixed(2) : 0}%
                    </div>
                  </div>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-muted-foreground">Locked Supply</span>
                  <div className="text-right">
                    <div className="font-medium">{totalLocked.toLocaleString()} CC</div>
                    <div className="text-sm text-muted-foreground">
                      {lockPercentage.toFixed(2)}%
                    </div>
                  </div>
                </div>
              </div>
            </Card>
          </>
        )}
      </div>
    </DashboardLayout>
  );
};

export default Supply;
