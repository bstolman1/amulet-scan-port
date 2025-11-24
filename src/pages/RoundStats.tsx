import DashboardLayout from "@/components/DashboardLayout";
import { useAcsSnapshots } from "@/hooks/use-acs-snapshots";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";
import { pickAmount } from "@/lib/amount-utils";

const RoundStats = () => {
  const { data: snapshots, isLoading } = useAcsSnapshots({ limit: 10 });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Round Statistics</h1>
          <p className="text-muted-foreground">
            Historical data by round
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-4">
            {snapshots?.map((snapshot) => {
              const templates = snapshot.snapshot_data as any;
              const amulets = templates?.["Splice:Amulet:Amulet"] || [];
              const totalSupply = amulets.reduce((sum: number, contract: any) => {
                return sum + pickAmount(contract);
              }, 0);

              return (
                <Card key={snapshot.id} className="p-6">
                  <div className="flex justify-between items-start">
                    <div>
                      <div className="text-2xl font-bold">Round {snapshot.round}</div>
                      <div className="text-sm text-muted-foreground">
                        {new Date(snapshot.timestamp).toLocaleString()}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-lg font-semibold">
                        {totalSupply.toLocaleString()} CC
                      </div>
                      <div className="text-sm text-muted-foreground">Total Supply</div>
                    </div>
                  </div>
                </Card>
              );
            })}
          </div>
        )}
      </div>
    </DashboardLayout>
  );
};

export default RoundStats;
