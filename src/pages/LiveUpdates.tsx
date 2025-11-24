import DashboardLayout from "@/components/DashboardLayout";
import { useLedgerUpdates } from "@/hooks/use-ledger-updates";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";

const LiveUpdates = () => {
  const { data: updates, isLoading } = useLedgerUpdates({ limit: 20 });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Live Updates</h1>
          <p className="text-muted-foreground">
            Real-time ledger update stream
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-2">
            {updates?.map((update) => (
              <Card key={update.id} className="p-4">
                <div className="flex justify-between items-start">
                  <div className="space-y-1">
                    <div className="font-medium">{update.update_type}</div>
                    <div className="text-sm text-muted-foreground">
                      Round {update.round}
                    </div>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    {new Date(update.timestamp).toLocaleString()}
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

export default LiveUpdates;
