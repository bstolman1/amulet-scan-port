import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Coins, Lock, TrendingUp, RefreshCw, Loader2 } from "lucide-react";
import { useQueryClient } from "@tanstack/react-query";
import { useState, useEffect, useCallback } from "react";
import { toast } from "@/hooks/use-toast";
import { getDuckDBApiUrl } from "@/lib/backend-config";

const AUTO_REFRESH_INTERVAL = 30000; // 30 seconds

export const ACSSnapshotCard = () => {
  const { data: snapshot, isPending, refetch } = useLatestACSSnapshot();
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);

  const handleRefresh = useCallback(async (showToast = true) => {
    setIsRefreshing(true);
    try {
      // Invalidate server-side cache first
      const apiBase = getDuckDBApiUrl();
      await fetch(`${apiBase}/api/acs/cache/invalidate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prefix: 'acs:' }),
      }).catch(() => {}); // Ignore cache invalidation errors
      
      // Invalidate all ACS-related queries
      await queryClient.invalidateQueries({ queryKey: ['latestACSSnapshot'] });
      await queryClient.invalidateQueries({ queryKey: ['acsSnapshots'] });
      await queryClient.invalidateQueries({ queryKey: ['localACSStats'] });
      await queryClient.invalidateQueries({ queryKey: ['localACSTemplates'] });
      await queryClient.invalidateQueries({ queryKey: ['localLatestACSSnapshot'] });
      
      await refetch();
      if (showToast) {
        toast({ title: "ACS data refreshed", description: "Latest snapshot data loaded" });
      }
    } catch (err) {
      console.error('Refresh failed:', err);
      if (showToast) {
        toast({ title: "Refresh failed", description: "Could not refresh ACS data", variant: "destructive" });
      }
    } finally {
      setIsRefreshing(false);
    }
  }, [queryClient, refetch]);

  // Auto-refresh effect
  useEffect(() => {
    if (!autoRefresh) return;
    
    const interval = setInterval(() => {
      handleRefresh(false); // Silent refresh
    }, AUTO_REFRESH_INTERVAL);

    return () => clearInterval(interval);
  }, [autoRefresh, handleRefresh]);

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
            {autoRefresh && (
              <span className="ml-2 text-xs text-primary">
                <Loader2 className="inline h-3 w-3 animate-spin mr-1" />
                Auto-refreshing
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant={autoRefresh ? "default" : "outline"}
            size="sm"
            onClick={() => setAutoRefresh(!autoRefresh)}
            className="text-xs"
          >
            {autoRefresh ? 'Auto: ON' : 'Auto: OFF'}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => handleRefresh(true)}
            disabled={isRefreshing}
            className="gap-2"
          >
            <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
            {isRefreshing ? 'Refreshing...' : 'Refresh'}
          </Button>
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
