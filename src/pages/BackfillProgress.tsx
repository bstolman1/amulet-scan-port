import DashboardLayout from "@/components/DashboardLayout";
import { useBackfillCursors } from "@/hooks/use-backfill-cursors";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";

const BackfillProgress = () => {
  const { data: cursors, isLoading } = useBackfillCursors();

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Backfill Progress</h1>
          <p className="text-muted-foreground">
            Data backfill cursor tracking
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-2">
            {cursors?.map((cursor) => (
              <Card key={cursor.id} className="p-4">
                <div className="flex justify-between items-center">
                  <div>
                    <div className="font-medium">{cursor.cursor_name}</div>
                    <div className="text-sm text-muted-foreground">
                      Last Updated: {new Date(cursor.updated_at).toLocaleString()}
                    </div>
                  </div>
                  <div className="text-lg font-semibold">
                    Round {cursor.last_processed_round}
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

export default BackfillProgress;
