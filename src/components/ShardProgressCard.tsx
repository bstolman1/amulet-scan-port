import { useState, useEffect, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Layers, Activity, Clock, Zap, ChevronDown, ChevronRight } from "lucide-react";
import { formatDuration, intervalToDuration } from "date-fns";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";

interface Shard {
  shardIndex: number | null;
  isSharded: boolean;
  progress: number;
  throughput: number | null;
  eta: number | null;
  complete: boolean;
  totalUpdates: number;
  totalEvents: number;
  minTime: string;
  maxTime: string;
  lastBefore: string;
  updatedAt: string;
  startedAt: string;
  error?: string;
}

interface ShardGroupStats {
  migrationId: number;
  synchronizerId: string;
  shards: Shard[];
  totalUpdates: number;
  totalEvents: number;
  totalShards: number;
  completedShards: number;
  activeShards: number;
  overallProgress: number;
  combinedEta: number | null;
}

// Track previous totals to detect if data is still being written
const previousTotals = new Map<string, { updates: number; events: number; timestamp: number }>();

function isDataStillWriting(group: ShardGroupStats): boolean {
  const key = `${group.migrationId}-${group.synchronizerId}`;
  const prev = previousTotals.get(key);
  const now = Date.now();
  
  if (!prev) {
    previousTotals.set(key, { updates: group.totalUpdates, events: group.totalEvents, timestamp: now });
    return false;
  }
  
  const isWriting = group.totalUpdates > prev.updates || group.totalEvents > prev.events;
  previousTotals.set(key, { updates: group.totalUpdates, events: group.totalEvents, timestamp: now });
  
  return isWriting;
}


interface ShardProgressCardProps {
  refreshInterval?: number;
}

function formatEta(ms: number | null): string {
  if (!ms) return "-";
  if (ms <= 0) return "Almost done";
  
  const duration = intervalToDuration({ start: 0, end: Math.round(ms) });
  return formatDuration(duration, { format: ['hours', 'minutes'], zero: false }) || "< 1 min";
}

export function ShardProgressCard({ refreshInterval = 3000 }: ShardProgressCardProps) {
  const [shardData, setShardData] = useState<ShardGroupStats[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

  const fetchShardProgress = async () => {
    try {
      const apiUrl = import.meta.env.VITE_DUCKDB_API_URL || "http://localhost:3001";
      const response = await fetch(`${apiUrl}/api/backfill/shards`);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      
      const result = await response.json();
      setShardData(result.data || []);
      setError(null);
    } catch (err: any) {
      console.error("Failed to fetch shard progress:", err);
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchShardProgress();
    const interval = setInterval(fetchShardProgress, refreshInterval);
    return () => clearInterval(interval);
  }, [refreshInterval]);

  const toggleGroup = (key: string) => {
    setExpandedGroups(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  // Filter to only show sharded groups (more than 1 shard or has shard index)
  const shardedGroups = useMemo(() => {
    return shardData.filter(group => 
      group.shards.length > 1 || group.shards.some(s => s.isSharded)
    );
  }, [shardData]);

  // Overall stats across all shards
  const overallStats = useMemo(() => {
    const totalShards = shardedGroups.reduce((sum, g) => sum + g.totalShards, 0);
    const completedShards = shardedGroups.reduce((sum, g) => sum + g.completedShards, 0);
    const activeShards = shardedGroups.reduce((sum, g) => sum + g.activeShards, 0);
    const totalUpdates = shardedGroups.reduce((sum, g) => sum + g.totalUpdates, 0);
    const totalEvents = shardedGroups.reduce((sum, g) => sum + g.totalEvents, 0);
    const overallProgress = totalShards > 0
      ? shardedGroups.reduce((sum, g) => sum + g.overallProgress * g.totalShards, 0) / totalShards
      : 0;
    
    return { totalShards, completedShards, activeShards, totalUpdates, totalEvents, overallProgress };
  }, [shardedGroups]);

  if (isLoading) {
    return (
      <Card className="bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Layers className="w-5 h-5" />
            Shard Progress
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-center py-8">
            <Activity className="w-6 h-6 animate-spin text-muted-foreground" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card className="bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Layers className="w-5 h-5" />
            Shard Progress
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-4 text-muted-foreground">
            <p className="text-sm">Unable to fetch shard data</p>
            <p className="text-xs mt-1 text-destructive">{error}</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (shardedGroups.length === 0) {
    return (
      <Card className="bg-card/50 backdrop-blur">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Layers className="w-5 h-5" />
            Shard Progress
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center py-4 text-muted-foreground">
            <p className="text-sm">No sharded backfill jobs running</p>
            <p className="text-xs mt-1">Start a sharded backfill to see progress here</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="bg-card/50 backdrop-blur">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Layers className="w-5 h-5" />
            Shard Progress
          </CardTitle>
          <Badge variant="default" className="gap-1">
            <Activity className="w-3 h-3 animate-pulse" />
            {overallStats.activeShards} active
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Overall shard stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 p-3 rounded-lg bg-muted/30">
          <div className="text-center">
            <div className="text-2xl font-bold text-primary">{overallStats.totalShards}</div>
            <div className="text-xs text-muted-foreground">Total Shards</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-green-400">{overallStats.completedShards}</div>
            <div className="text-xs text-muted-foreground">Completed</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-blue-400">{overallStats.totalUpdates.toLocaleString()}</div>
            <div className="text-xs text-muted-foreground">Total Updates</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-yellow-400">{overallStats.overallProgress.toFixed(1)}%</div>
            <div className="text-xs text-muted-foreground">Overall Progress</div>
          </div>
        </div>

        {/* Per-group shard breakdown */}
        <div className="space-y-3">
          {shardedGroups.map(group => {
            const key = `${group.migrationId}-${group.synchronizerId}`;
            const isExpanded = expandedGroups.has(key);
            const syncShort = group.synchronizerId.length > 30 
              ? group.synchronizerId.substring(0, 30) + "..." 
              : group.synchronizerId;
            
            // Check if data is still being written even if "complete"
            const stillWriting = isDataStillWriting(group);
            const allComplete = group.completedShards === group.totalShards;
            const displayComplete = allComplete && !stillWriting;

            return (
              <Collapsible key={key} open={isExpanded} onOpenChange={() => toggleGroup(key)}>
                <div className="rounded-lg border border-border/50 overflow-hidden">
                  {/* Group header */}
                  <CollapsibleTrigger className="w-full p-3 bg-muted/20 hover:bg-muted/40 transition-colors">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        {isExpanded ? (
                          <ChevronDown className="w-4 h-4 text-muted-foreground" />
                        ) : (
                          <ChevronRight className="w-4 h-4 text-muted-foreground" />
                        )}
                        <span className="font-medium">Migration {group.migrationId}</span>
                        <span className="text-xs text-muted-foreground font-mono">{syncShort}</span>
                        {stillWriting && (
                          <Badge variant="default" className="text-xs bg-blue-600 animate-pulse">
                            <Activity className="w-3 h-3 mr-1" />
                            Writing
                          </Badge>
                        )}
                      </div>
                      <div className="flex items-center gap-3">
                        <Badge variant={displayComplete ? "default" : "secondary"} className={displayComplete ? "bg-green-600" : ""}>
                          {group.completedShards}/{group.totalShards} shards
                        </Badge>
                        <span className="text-sm font-medium text-primary">{group.overallProgress.toFixed(1)}%</span>
                      </div>
                    </div>
                    <div className="mt-2">
                      <Progress value={group.overallProgress} className="h-2" />
                    </div>
                  </CollapsibleTrigger>

                  {/* Expanded shard details */}
                  <CollapsibleContent>
                    <div className="p-3 space-y-2 bg-background/50">
                      {group.shards.map((shard, idx) => {
                        const shardLabel = shard.shardIndex !== null ? `Shard ${shard.shardIndex}` : `Shard ${idx}`;
                        const isActive = !shard.complete && shard.updatedAt && 
                          (Date.now() - new Date(shard.updatedAt).getTime() < 60000);

                        const isFinalizing = !shard.complete && shard.progress >= 99.5;

                        return (
                          <div key={idx} className="flex items-center gap-3 p-2 rounded bg-muted/30">
                            <div className="w-24 flex items-center gap-2">
                              {shard.complete && !stillWriting ? (
                                <Badge variant="default" className="text-xs bg-green-600">Done</Badge>
                              ) : shard.complete && stillWriting ? (
                                <Badge variant="default" className="text-xs bg-blue-600 animate-pulse">Writing</Badge>
                              ) : isFinalizing ? (
                                <Badge variant="default" className="text-xs bg-yellow-600 animate-pulse">Finalizing</Badge>
                              ) : isActive ? (
                                <Badge variant="default" className="text-xs animate-pulse">Active</Badge>
                              ) : (
                                <Badge variant="secondary" className="text-xs">Idle</Badge>
                              )}
                            </div>
                            <span className="font-mono text-sm w-20">{shardLabel}</span>
                            <div className="flex-1">
                              <Progress value={shard.progress} className="h-1.5" />
                            </div>
                            <span className="text-sm font-medium w-14 text-right">{shard.progress.toFixed(1)}%</span>
                            <div className="flex items-center gap-1 text-xs text-muted-foreground w-24">
                              <Zap className="w-3 h-3" />
                              {shard.throughput ? `${shard.throughput.toLocaleString()}/s` : "-"}
                            </div>
                            <div className="flex items-center gap-1 text-xs text-muted-foreground w-20">
                              <Clock className="w-3 h-3" />
                              {shard.complete ? "Done" : formatEta(shard.eta)}
                            </div>
                            <span className="text-xs text-green-400 w-24 text-right">
                              {shard.totalUpdates.toLocaleString()} upd
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  </CollapsibleContent>
                </div>
              </Collapsible>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}