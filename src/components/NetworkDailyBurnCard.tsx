import { useBurnStats } from "@/hooks/use-burn-stats";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Flame } from "lucide-react";

export const NetworkDailyBurnCard = () => {
  const { data: burnStats, isPending, isError } = useBurnStats({ days: 1 });

  const dailyBurn = burnStats?.totalBurn || 0;
  const hasError = !isPending && isError;

  return (
    <Card className="glass-card p-6">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-muted-foreground">Daily Burned (24h)</h3>
        <Flame className="h-5 w-5 text-destructive" />
      </div>
      {isPending ? (
        <Skeleton className="h-10 w-full" />
      ) : hasError ? (
        <>
          <p className="text-3xl font-bold text-muted-foreground mb-1">--</p>
          <p className="text-xs text-muted-foreground">Data unavailable</p>
        </>
      ) : (
        <>
          <p className="text-3xl font-bold text-destructive mb-1">
            {dailyBurn.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </p>
          <p className="text-xs text-muted-foreground">
            CC burned in last 24h (all sources)
          </p>
        </>
      )}
    </Card>
  );
};
