import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Coins, Lock, TrendingUp } from "lucide-react";

export const ACSSnapshotCard = () => {
  const { data: snapshot, isPending } = useLatestACSSnapshot();

  if (isPending) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[...Array(3)].map((_, i) => (
          <Card key={i} className="glass-card p-6">
            <Skeleton className="h-8 w-full mb-2" />
            <Skeleton className="h-10 w-full" />
          </Card>
        ))}
      </div>
    );
  }

  if (!snapshot) {
    return (
      <Card className="glass-card p-6">
        <p className="text-muted-foreground text-center">
          No ACS snapshot data available. Trigger a snapshot to view supply metrics.
        </p>
      </Card>
    );
  }

  const amuletTotal = snapshot.amulet_total;
  const lockedTotal = snapshot.locked_total;
  const circulatingSupply = snapshot.circulating_supply;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold">ACS Snapshot</h3>
          <p className="text-sm text-muted-foreground">
            Latest: {new Date(snapshot.timestamp).toLocaleString()}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card className="glass-card p-6">
          <div className="flex items-center justify-between mb-3">
            <h4 className="text-sm font-medium text-muted-foreground">Total Amulet</h4>
            <Coins className="h-5 w-5 text-primary" />
          </div>
          <p className="text-3xl font-bold text-primary mb-1">
            {amuletTotal.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </p>
          <p className="text-xs text-muted-foreground">
            All Amulet contracts in ACS
          </p>
        </Card>

        <Card className="glass-card p-6">
          <div className="flex items-center justify-between mb-3">
            <h4 className="text-sm font-medium text-muted-foreground">Locked Amulet</h4>
            <Lock className="h-5 w-5 text-orange-500" />
          </div>
          <p className="text-3xl font-bold text-orange-500 mb-1">
            {lockedTotal.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </p>
          <p className="text-xs text-muted-foreground">
            Locked in LockedAmulet contracts
          </p>
        </Card>

        <Card className="glass-card p-6">
          <div className="flex items-center justify-between mb-3">
            <h4 className="text-sm font-medium text-muted-foreground">Circulating Supply</h4>
            <TrendingUp className="h-5 w-5 text-green-500" />
          </div>
          <p className="text-3xl font-bold text-green-500 mb-1">
            {circulatingSupply.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </p>
          <p className="text-xs text-muted-foreground">
            Total - Locked = Circulating
          </p>
        </Card>
      </div>

      <Card className="glass-card p-4">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div>
            <p className="text-muted-foreground">Migration ID</p>
            <p className="font-mono">{snapshot.migration_id}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Entry Count</p>
            <p className="font-mono">{snapshot.entry_count.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Canonical Package</p>
            <p className="font-mono text-xs truncate">{snapshot.canonical_package || 'N/A'}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Record Time</p>
            <p className="font-mono text-xs truncate" title={snapshot.record_time}>
              {new Date(snapshot.record_time).toLocaleTimeString()}
            </p>
          </div>
        </div>
      </Card>
    </div>
  );
};
