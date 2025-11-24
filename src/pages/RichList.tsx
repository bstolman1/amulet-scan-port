import DashboardLayout from "@/components/DashboardLayout";
import { useAcsSnapshots } from "@/hooks/use-acs-snapshots";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";
import { pickAmount } from "@/lib/amount-utils";

const RichList = () => {
  const { data: snapshots, isLoading } = useAcsSnapshots({ limit: 1 });

  const latestSnapshot = snapshots?.[0];
  const templates = latestSnapshot?.snapshot_data as any;
  const amulets = templates?.["Splice:Amulet:Amulet"] || [];

  // Group by owner and sum amounts
  const balances = amulets.reduce((acc: any, contract: any) => {
    const owner = contract.contract?.payload?.owner || "Unknown";
    const amount = pickAmount(contract);
    acc[owner] = (acc[owner] || 0) + amount;
    return acc;
  }, {});

  // Convert to array and sort
  const richList = Object.entries(balances)
    .map(([owner, balance]) => ({ owner, balance: balance as number }))
    .sort((a, b) => b.balance - a.balance)
    .slice(0, 100);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Rich List</h1>
          <p className="text-muted-foreground">
            Top Amulet holders by balance
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-2">
            {richList.map((item, index) => (
              <Card key={index} className="p-4">
                <div className="flex justify-between items-center">
                  <div className="flex items-center gap-4">
                    <div className="text-2xl font-bold text-muted-foreground w-12">
                      #{index + 1}
                    </div>
                    <div className="font-mono text-sm break-all">
                      {item.owner}
                    </div>
                  </div>
                  <div className="text-lg font-semibold">
                    {item.balance.toLocaleString()} CC
                  </div>
                </div>
              </Card>
            ))}
          </div>
        )}
      </div>
    </DashboardLayout>
  );
};

export default RichList;
