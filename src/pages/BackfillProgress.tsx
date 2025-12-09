import { useState, useEffect, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { supabase } from "@/integrations/supabase/client";
import { Database, Activity, Trash2, FileText, Layers, Zap, Clock, Grid3X3 } from "lucide-react";
import { formatDistanceToNow, format, formatDuration, intervalToDuration } from "date-fns";
import { useBackfillCursors, useBackfillStats, BackfillCursor } from "@/hooks/use-backfill-cursors";
import { useToast } from "@/hooks/use-toast";

// Extend cursor type with shard info
interface ShardedCursor extends BackfillCursor {
  shard_index?: number | null;
}

/**
 * Parse shard information from synchronizer_id
 */
function parseShardInfo(cursor: ShardedCursor): { shardIndex: number | null; isSharded: boolean } {
  // First check if shard_index is already set from the API
  if (cursor.shard_index !== undefined && cursor.shard_index !== null) {
    return { shardIndex: cursor.shard_index, isSharded: true };
  }
  // Fallback: parse from synchronizer_id
  const match = cursor.synchronizer_id?.match(/-shard(\d+)$/);
  return match 
    ? { shardIndex: parseInt(match[1], 10), isSharded: true } 
    : { shardIndex: null, isSharded: false };
}

/**
 * Calculate progress percentage for a cursor
 */
function calculateProgress(cursor: BackfillCursor): number {
  if (cursor.complete) return 100;
  if (!cursor.min_time || !cursor.max_time || !cursor.last_before) return 0;
  
  const minTime = new Date(cursor.min_time).getTime();
  const maxTime = new Date(cursor.max_time).getTime();
  const currentTime = new Date(cursor.last_before).getTime();
  const totalRange = maxTime - minTime;
  const progressFromMax = maxTime - currentTime;
  
  return Math.min(100, Math.max(0, (progressFromMax / totalRange) * 100));
}

/**
 * Calculate ETA for a cursor based on throughput
 */
function calculateETA(cursor: BackfillCursor): { eta: string | null; throughput: number | null } {
  if (cursor.complete) return { eta: null, throughput: null };
  if (!cursor.min_time || !cursor.max_time || !cursor.last_before) return { eta: null, throughput: null };
  if (!cursor.started_at || !cursor.total_updates) return { eta: null, throughput: null };
  
  const startedAt = new Date(cursor.started_at).getTime();
  const now = new Date(cursor.updated_at).getTime();
  const elapsedMs = now - startedAt;
  
  if (elapsedMs <= 0) return { eta: null, throughput: null };
  
  // Calculate throughput based on time range covered
  const minTime = new Date(cursor.min_time).getTime();
  const maxTime = new Date(cursor.max_time).getTime();
  const currentTime = new Date(cursor.last_before).getTime();
  
  const totalRange = maxTime - minTime;
  const processedRange = maxTime - currentTime;
  const remainingRange = currentTime - minTime;
  
  if (processedRange <= 0 || totalRange <= 0) return { eta: null, throughput: null };
  
  // Time per millisecond of data range
  const msPerDataMs = elapsedMs / processedRange;
  const estimatedRemainingMs = remainingRange * msPerDataMs;
  
  // Throughput in updates per second
  const throughput = Math.round(cursor.total_updates / (elapsedMs / 1000));
  
  if (estimatedRemainingMs <= 0) return { eta: "Almost done", throughput };
  if (estimatedRemainingMs > 365 * 24 * 60 * 60 * 1000) return { eta: "> 1 year", throughput };
  
  const duration = intervalToDuration({ start: 0, end: Math.round(estimatedRemainingMs) });
  const eta = formatDuration(duration, { format: ['days', 'hours', 'minutes'], zero: false }) || "< 1 min";
  
  return { eta, throughput };
}

const BackfillProgress = () => {
  const { data: cursors = [], isLoading, refetch } = useBackfillCursors();
  const { data: stats, refetch: refetchStats } = useBackfillStats();
  const [realtimeCursors, setRealtimeCursors] = useState<BackfillCursor[]>([]);
  const [isPurging, setIsPurging] = useState(false);
  const { toast } = useToast();

  // Calculate merged cursors list
  const allCursors = useMemo(() => {
    return [...realtimeCursors, ...cursors.filter((c) => !realtimeCursors.some((rc) => rc.id === c.id))];
  }, [cursors, realtimeCursors]);

  // Group cursors by migration_id
  const cursorsByMigration = useMemo(() => {
    const grouped: Record<number, BackfillCursor[]> = {};
    for (const cursor of allCursors) {
      const migrationId = cursor.migration_id || 0;
      if (!grouped[migrationId]) grouped[migrationId] = [];
      grouped[migrationId].push(cursor);
    }
    // Sort by migration_id
    return Object.entries(grouped)
      .sort(([a], [b]) => Number(a) - Number(b))
      .map(([id, cursors]) => ({ migrationId: Number(id), cursors }));
  }, [allCursors]);

  // Group cursors by shard (only for sharded cursors)
  const cursorsByShard = useMemo(() => {
    const grouped: Record<number, ShardedCursor[]> = {};
    let hasShards = false;
    
    for (const cursor of allCursors as ShardedCursor[]) {
      const { shardIndex, isSharded } = parseShardInfo(cursor);
      if (isSharded && shardIndex !== null) {
        hasShards = true;
        if (!grouped[shardIndex]) grouped[shardIndex] = [];
        grouped[shardIndex].push(cursor);
      }
    }
    
    if (!hasShards) return null;
    
    return Object.entries(grouped)
      .sort(([a], [b]) => Number(a) - Number(b))
      .map(([shard, cursors]) => {
        const shardIndex = Number(shard);
        const completedCount = cursors.filter(c => c.complete).length;
        const totalProgress = cursors.reduce((sum, c) => sum + calculateProgress(c), 0) / cursors.length;
        const totalThroughput = cursors.reduce((sum, c) => {
          const { throughput } = calculateETA(c);
          return sum + (throughput || 0);
        }, 0);
        const totalUpdates = cursors.reduce((sum, c) => sum + (c.total_updates || 0), 0);
        const totalEvents = cursors.reduce((sum, c) => sum + (c.total_events || 0), 0);
        
        return {
          shardIndex,
          cursors,
          completedCount,
          totalProgress,
          totalThroughput,
          totalUpdates,
          totalEvents,
          isComplete: completedCount === cursors.length,
        };
      });
  }, [allCursors]);

  // Determine current active migration (first non-complete migration)
  const activeMigrationId = useMemo(() => {
    for (const { migrationId, cursors } of cursorsByMigration) {
      const allComplete = cursors.every(c => c.complete);
      if (!allComplete) return migrationId;
    }
    return null;
  }, [cursorsByMigration]);

  // Calculate cursor stats from allCursors (derived, not in useEffect)
  const cursorStats = useMemo(() => ({
    totalCursors: stats?.totalCursors || allCursors.length,
    completedCursors: stats?.completedCursors || allCursors.filter((c) => c.complete).length,
  }), [allCursors, stats]);

  // Calculate overall progress percentage
  const overallProgress = useMemo(() => {
    if (allCursors.length === 0) return 0;
    return allCursors.reduce((sum, c) => sum + calculateProgress(c), 0) / allCursors.length;
  }, [allCursors]);

  useEffect(() => {
    const channel = supabase
      .channel("backfill-progress")
      .on(
        "postgres_changes",
        {
          event: "*",
          schema: "public",
          table: "backfill_cursors",
        },
        (payload: any) => {
          console.log("Backfill cursor update:", payload);
          if (payload.eventType === "INSERT") {
            setRealtimeCursors((prev) => [payload.new as BackfillCursor, ...prev]);
          } else if (payload.eventType === "UPDATE") {
            setRealtimeCursors((prev) =>
              prev.map((c) => (c.id === payload.new.id ? (payload.new as BackfillCursor) : c)),
            );
          }
        },
      )
      .on(
        "postgres_changes",
        {
          event: "INSERT",
          schema: "public",
          table: "ledger_updates",
        },
        () => {
          refetchStats();
        },
      )
      .on(
        "postgres_changes",
        {
          event: "INSERT",
          schema: "public",
          table: "ledger_events",
        },
        () => {
          refetchStats();
        },
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, [refetchStats]);

  

  const handlePurgeAll = async () => {
    if (
      !confirm(
        "Are you sure you want to purge ALL backfill data? This will delete all backfill cursors and data files. This action cannot be undone.",
      )
    ) {
      return;
    }

    setIsPurging(true);
    try {
      // Try local DuckDB API first
      const localApiUrl = import.meta.env.VITE_DUCKDB_API_URL || "http://localhost:3001";
      try {
        const response = await fetch(`${localApiUrl}/api/backfill/purge`, { method: "DELETE" });
        if (response.ok) {
          const data = await response.json();
          toast({
            title: "Purge complete",
            description: `Deleted ${data.deleted_cursors} cursor files${data.deleted_data_dir ? " and raw data directory" : ""}`,
          });
          refetch();
          refetchStats();
          setRealtimeCursors([]);
          return;
        }
      } catch (localError) {
        console.log("Local API not available, falling back to Supabase edge function");
      }

      // Fallback to Supabase edge function
      const { data, error } = await supabase.functions.invoke("purge-backfill-data", {
        body: { purge_all: true },
      });

      if (error) throw error;

      toast({
        title: "Purge complete",
        description: `Deleted ${data.deleted_cursors} cursors, ${data.deleted_updates} updates, ${data.deleted_events} events`,
      });

      refetch();
      refetchStats();
      setRealtimeCursors([]);
    } catch (error: any) {
      console.error("Purge error:", error);
      toast({ title: "Purge failed", description: error.message || "Unknown error", variant: "destructive" });
    } finally {
      setIsPurging(false);
    }
  };

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-64">
          <Activity className="w-8 h-8 animate-spin text-primary" />
        </div>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-4">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <h1 className="text-2xl font-bold">Backfill Progress</h1>
              <Badge variant="default" className="gap-1">
                <Activity className="w-3 h-3 animate-pulse" />
                Live
              </Badge>
            </div>
            <p className="text-sm text-muted-foreground">Monitoring backfill data ingestion</p>
          </div>
          <div className="flex gap-2">
            <Button onClick={handlePurgeAll} disabled={isPurging} variant="destructive" size="sm">
              <Trash2 className={`h-4 w-4 mr-2 ${isPurging ? "animate-spin" : ""}`} />
              {isPurging ? "Purging..." : "Purge All"}
            </Button>
          </div>
        </div>

        {/* Overall Progress Bar */}
        {allCursors.length > 0 && (
          <Card className="bg-card/50 backdrop-blur border-primary/20">
            <CardContent className="p-4">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  <Activity className="w-4 h-4 text-primary" />
                  <span className="font-semibold">Overall Backfill Progress</span>
                </div>
                <span className="text-lg font-bold text-primary">{overallProgress.toFixed(1)}%</span>
              </div>
              <Progress value={overallProgress} className="h-3" />
              <div className="flex justify-between mt-2 text-xs text-muted-foreground">
                <span>{cursorStats.completedCursors} of {cursorStats.totalCursors} migrations complete</span>
                <span>{stats?.totalUpdates?.toLocaleString() || 0} updates • {stats?.totalEvents?.toLocaleString() || 0} events</span>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Stats Grid */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Database className="w-4 h-4" />
                Cursors Completed
              </div>
              <div className="text-2xl font-bold text-primary">{cursorStats.completedCursors}</div>
            </CardContent>
          </Card>

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <FileText className="w-4 h-4" />
                Total Cursors
              </div>
              <div className="text-2xl font-bold text-primary">{cursorStats.totalCursors}</div>
            </CardContent>
          </Card>

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Zap className="w-4 h-4" />
                Updates Received
              </div>
              <div className="text-2xl font-bold text-blue-400">{stats?.totalUpdates?.toLocaleString() || 0}</div>
            </CardContent>
          </Card>

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Layers className="w-4 h-4" />
                Events Received
              </div>
              <div className="text-2xl font-bold text-green-400">{stats?.totalEvents?.toLocaleString() || 0}</div>
            </CardContent>
          </Card>

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Database className="w-4 h-4" />
                Active Migrations
              </div>
              <div className="text-2xl font-bold text-primary">{stats?.activeMigrations || 0}</div>
            </CardContent>
          </Card>

        </div>

        {/* Shard Progress Grid - Only shown when sharding is active */}
        {cursorsByShard && cursorsByShard.length > 0 && (
          <Card className="bg-card/50 backdrop-blur border-primary/30">
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2">
                <Grid3X3 className="w-5 h-5 text-primary" />
                Shard Progress
                <Badge variant="secondary" className="ml-2">
                  {cursorsByShard.length} shards
                </Badge>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                {cursorsByShard.map(({ shardIndex, cursors, completedCount, totalProgress, totalThroughput, totalUpdates, totalEvents, isComplete }) => (
                  <div 
                    key={shardIndex}
                    className={`p-4 rounded-lg border ${
                      isComplete 
                        ? 'bg-green-500/10 border-green-500/30' 
                        : totalProgress > 0 
                          ? 'bg-primary/10 border-primary/30' 
                          : 'bg-muted/50 border-border/50'
                    }`}
                  >
                    {/* Shard Header */}
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <span className="text-lg font-bold">Shard {shardIndex}</span>
                        {isComplete ? (
                          <Badge className="bg-green-600 text-xs">✓</Badge>
                        ) : totalThroughput > 0 ? (
                          <Badge variant="default" className="text-xs animate-pulse">
                            <Zap className="w-3 h-3 mr-1" />
                            {totalThroughput.toLocaleString()}/s
                          </Badge>
                        ) : (
                          <Badge variant="secondary" className="text-xs">Idle</Badge>
                        )}
                      </div>
                    </div>
                    
                    {/* Progress Bar */}
                    <div className="space-y-1 mb-2">
                      <Progress value={totalProgress} className="h-2" />
                      <div className="flex justify-between text-xs text-muted-foreground">
                        <span>{totalProgress.toFixed(1)}%</span>
                        <span>{completedCount}/{cursors.length} cursors</span>
                      </div>
                    </div>
                    
                    {/* Stats */}
                    <div className="flex flex-wrap gap-2 text-xs">
                      {totalUpdates > 0 && (
                        <span className="text-blue-400">{totalUpdates.toLocaleString()} updates</span>
                      )}
                      {totalEvents > 0 && (
                        <span className="text-green-400">{totalEvents.toLocaleString()} events</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              
              {/* Combined Stats */}
              <div className="mt-4 pt-4 border-t border-border/50 flex flex-wrap gap-4 text-sm">
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">Combined Throughput:</span>
                  <span className="font-bold text-primary">
                    {cursorsByShard.reduce((sum, s) => sum + s.totalThroughput, 0).toLocaleString()}/s
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">Total Updates:</span>
                  <span className="font-bold text-blue-400">
                    {cursorsByShard.reduce((sum, s) => sum + s.totalUpdates, 0).toLocaleString()}
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">Total Events:</span>
                  <span className="font-bold text-green-400">
                    {cursorsByShard.reduce((sum, s) => sum + s.totalEvents, 0).toLocaleString()}
                  </span>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Backfill Cursors Progress */}
        <Card className="bg-card/50 backdrop-blur">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="w-5 h-5" />
              Backfill Cursors
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-6">
              {cursorsByMigration.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">No cursors found</div>
              ) : (
                cursorsByMigration.map(({ migrationId, cursors: migrationCursors }) => {
                  const allComplete = migrationCursors.every(c => c.complete);
                  const isActive = migrationId === activeMigrationId;
                  const completedCount = migrationCursors.filter(c => c.complete).length;
                  
                  return (
                    <div key={migrationId} className="space-y-3">
                      {/* Migration Header */}
                      <div className="flex items-center justify-between border-b border-border/50 pb-2">
                        <div className="flex items-center gap-3">
                          <span className="text-lg font-semibold">Migration {migrationId}</span>
                          {allComplete ? (
                            <Badge variant="default" className="bg-green-600">Complete</Badge>
                          ) : isActive ? (
                            <Badge variant="default" className="bg-primary animate-pulse">Active</Badge>
                          ) : (
                            <Badge variant="secondary">Waiting</Badge>
                          )}
                        </div>
                        <span className="text-sm text-muted-foreground">
                          {completedCount} / {migrationCursors.length} synchronizers
                        </span>
                      </div>
                      
                      {/* Cursors for this migration */}
                      <div className="space-y-3 pl-4 border-l-2 border-border/30">
                        {migrationCursors.map((cursor) => {
                          const progressPercent = calculateProgress(cursor);
                          const { eta, throughput } = calculateETA(cursor);
                          const { shardIndex, isSharded } = parseShardInfo(cursor as ShardedCursor);

                          return (
                            <div key={cursor.id} className="space-y-2 p-3 rounded-lg bg-muted/50">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                  <span className="font-mono text-sm font-medium">{cursor.cursor_name}</span>
                                  {isSharded && (
                                    <Badge variant="outline" className="text-xs">
                                      Shard {shardIndex}
                                    </Badge>
                                  )}
                                  <Badge variant={cursor.complete ? "default" : "secondary"} className="text-xs">
                                    {cursor.complete ? "Complete" : "In Progress"}
                                  </Badge>
                                </div>
                              <div className="flex items-center gap-3 text-sm text-muted-foreground">
                                  {throughput && (
                                    <span className="text-blue-400">{throughput.toLocaleString()}/s</span>
                                  )}
                                  {cursor.total_updates ? (
                                    <span className="text-green-400">{cursor.total_updates.toLocaleString()} updates</span>
                                  ) : null}
                                  {cursor.total_events ? (
                                    <span className="text-yellow-400">{cursor.total_events.toLocaleString()} events</span>
                                  ) : null}
                                </div>
                              </div>
                              {cursor.min_time && cursor.max_time && (
                                <div className="space-y-1">
                                  <div className="flex items-center justify-between text-xs">
                                    <span className="text-muted-foreground">Start: {format(new Date(cursor.min_time), "MMM d, yyyy HH:mm")}</span>
                                    <span className="text-muted-foreground">End: {format(new Date(cursor.max_time), "MMM d, yyyy HH:mm")}</span>
                                  </div>
                                  {cursor.last_before && (
                                    <div className="flex items-center justify-between text-xs">
                                      <span className="text-primary font-semibold">Current: {format(new Date(cursor.last_before), "MMM d, yyyy HH:mm:ss")}</span>
                                      <span className="text-primary font-semibold">{progressPercent.toFixed(1)}%</span>
                                    </div>
                                  )}
                                  <Progress value={progressPercent} className="h-2" />
                                  {eta && (
                                    <div className="flex items-center gap-1 text-xs text-amber-400">
                                      <Clock className="w-3 h-3" />
                                      <span>ETA: {eta}</span>
                                    </div>
                                  )}
                                </div>
                              )}
                              <div className="flex items-center justify-between text-xs text-muted-foreground">
                                <span>Updated {formatDistanceToNow(new Date(cursor.updated_at), { addSuffix: true })}</span>
                                {cursor.total_updates && (
                                  <span>{cursor.total_updates.toLocaleString()} updates processed</span>
                                )}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default BackfillProgress;
