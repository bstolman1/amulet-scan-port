import { Card } from "@/components/ui/card";
import { useAcsSnapshots } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { pickAmount, pickLockedAmount } from "@/lib/amount-utils";
import { Loader2 } from "lucide-react";
import DashboardLayout from "@/components/DashboardLayout";
import StatCard from "@/components/StatCard";

const Dashboard = () => {
  const { data: snapshots, isLoading } = useAcsSnapshots({ limit: 1 });
  const { data: aggregatedData, isLoading: isLoadingAggregated } = useAggregatedTemplateData();

  if (isLoading || isLoadingAggregated) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center min-h-[400px]">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      </DashboardLayout>
    );
  }

  const latestSnapshot = snapshots?.[0];
  const templates = latestSnapshot?.snapshot_data as any;

  // Calculate total supply from Amulet and LockedAmulet templates
  const amulets = templates?.["Splice:Amulet:Amulet"] || [];
  const lockedAmulets = templates?.["Splice:Amulet:LockedAmulet"] || [];

  const totalSupply = amulets.reduce((sum: number, contract: any) => {
    return sum + pickAmount(contract);
  }, 0);

  const totalLocked = lockedAmulets.reduce((sum: number, contract: any) => {
    return sum + pickLockedAmount(contract);
  }, 0);

  const circulating = totalSupply - totalLocked;

  // Get round number
  const roundNumber = latestSnapshot?.round || 0;

  // Count validators from ValidatorLicense template
  const validators = templates?.["Splice:ValidatorLicense:ValidatorLicense"] || [];
  const validatorCount = validators.length;

  // Count active templates
  const activeTemplates = aggregatedData?.length || 0;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
          <p className="text-muted-foreground">
            Real-time overview of the Amulet network
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <StatCard
            title="Current Round"
            value={roundNumber.toLocaleString()}
            description="Latest processed round"
          />
          <StatCard
            title="Total Supply"
            value={`${totalSupply.toLocaleString()} CC`}
            description="All Amulets in circulation"
          />
          <StatCard
            title="Circulating Supply"
            value={`${circulating.toLocaleString()} CC`}
            description="Unlocked Amulets"
          />
          <StatCard
            title="Active Validators"
            value={validatorCount.toLocaleString()}
            description="Current validator licenses"
          />
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <Card className="p-6">
            <h3 className="text-lg font-semibold mb-2">Network Activity</h3>
            <div className="space-y-3">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Active Templates</span>
                <span className="font-medium">{activeTemplates}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Locked Supply</span>
                <span className="font-medium">{totalLocked.toLocaleString()} CC</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Lock Percentage</span>
                <span className="font-medium">
                  {totalSupply > 0 ? ((totalLocked / totalSupply) * 100).toFixed(2) : 0}%
                </span>
              </div>
            </div>
          </Card>

          <Card className="p-6">
            <h3 className="text-lg font-semibold mb-2">Quick Links</h3>
            <div className="space-y-2">
              <a href="/transactions" className="block text-primary hover:underline">
                View Recent Transactions
              </a>
              <a href="/validators" className="block text-primary hover:underline">
                Validator Details
              </a>
              <a href="/supply" className="block text-primary hover:underline">
                Supply Breakdown
              </a>
              <a href="/governance" className="block text-primary hover:underline">
                Governance Activity
              </a>
            </div>
          </Card>
        </div>
      </div>
    </DashboardLayout>
  );
};

export default Dashboard;
