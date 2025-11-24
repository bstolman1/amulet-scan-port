import DashboardLayout from "@/components/DashboardLayout";
import { useAcsSnapshots } from "@/hooks/use-acs-snapshots";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";

const SnapshotProgress = () => {
  const { data: snapshots, isLoading } = useAcsSnapshots({ limit: 20 });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Snapshot Progress</h1>
          <p className="text-muted-foreground">
            Recent ACS snapshot history
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-2">
            {snapshots?.map((snapshot) => (
              <Card key={snapshot.id} className="p-4">
                <div className="flex justify-between items-center">
                  <div>
                    <div className="font-medium">Round {snapshot.round}</div>
                    <div className="text-sm text-muted-foreground">
                      {new Date(snapshot.timestamp).toLocaleString()}
                    </div>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    {new Date(snapshot.created_at).toLocaleString()}
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

export default SnapshotProgress;
