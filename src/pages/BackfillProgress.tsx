import { useState, useEffect, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { supabase } from "@/integrations/supabase/client";
import { Database, Activity, Trash2, FileText, Layers, Zap, Clock } from "lucide-react";
import { formatDistanceToNow, format, formatDuration, intervalToDuration } from "date-fns";
import { useBackfillCursors, useBackfillStats, BackfillCursor } from "@/hooks/use-backfill-cursors";
import { useToast } from "@/hooks/use-toast";
import { ShardProgressCard } from "@/components/ShardProgressCard";
import { GapDetectionCard } from "@/components/GapDetectionCard";

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
  const [prevStats, setPrevStats] = useState<{ updates: number; events: number } | null>(null);
  const { toast } = useToast();

  // Track if data is still being written (counts increasing)
  const isStillWriting = useMemo(() => {
    if (!stats || !prevStats) return false;
    return stats.totalUpdates > prevStats.updates || stats.totalEvents > prevStats.events;
  }, [stats, prevStats]);

  // Update previous stats when stats change
  useEffect(() => {
    if (stats) {
      setPrevStats(prev => {
        // Only update after we've had a chance to compare
        if (prev === null) {
          return { updates: stats.totalUpdates, events: stats.totalEvents };
        }
        // Delay update to allow comparison
        setTimeout(() => {
          setPrevStats({ updates: stats.totalUpdates, events: stats.totalEvents });
        }, 100);
        return prev;
      });
    }
  }, [stats]);

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

  // Determine current active migration (first non-complete migration OR one still writing data)
  const activeMigrationId = useMemo(() => {
    for (const { migrationId, cursors } of cursorsByMigration) {
      const allComplete = cursors.every(c => c.complete);
      if (!allComplete) return migrationId;
    }
    // If all complete but still writing, return the last migration
    if (isStillWriting && cursorsByMigration.length > 0) {
      return cursorsByMigration[cursorsByMigration.length - 1].migrationId;
    }
    return null;
  }, [cursorsByMigration, isStillWriting]);

  // Calculate cursor stats from allCursors (derived, not in useEffect)
  const cursorStats = useMemo(() => ({
    totalCursors: stats?.totalCursors || allCursors.length,
    completedCursors: stats?.completedCursors || allCursors.filter((c) => c.complete).length,
  }), [allCursors, stats]);

  // Calculate overall progress percentage
  const overallProgress = useMemo(() => {
    if (allCursors.length === 0) return 0;
    
    let totalProgress = 0;
    let validCursors = 0;
    
    for (const cursor of allCursors) {
      if (cursor.complete) {
        totalProgress += 100;
        validCursors++;
      } else if (cursor.min_time && cursor.max_time && cursor.last_before) {
        const minTime = new Date(cursor.min_time).getTime();
        const maxTime = new Date(cursor.max_time).getTime();
        const currentTime = new Date(cursor.last_before).getTime();
        const totalRange = maxTime - minTime;
        if (totalRange > 0) {
          const progressFromMax = maxTime - currentTime;
          totalProgress += Math.min(100, Math.max(0, (progressFromMax / totalRange) * 100));
          validCursors++;
        }
      }
    }
    
    return validCursors > 0 ? totalProgress / validCursors : 0;
  }, [allCursors]);

  // While data is still being written, avoid showing a misleading 100%
  const displayOverallProgress = useMemo(() => {
    if (!isStillWriting) return overallProgress;
    return overallProgress >= 100 ? 99.9 : overallProgress;
  }, [isStillWriting, overallProgress]);

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
                  {isStillWriting && (
                    <Badge variant="default" className="gap-1 bg-accent text-accent-foreground animate-pulse">
                      <Activity className="w-3 h-3" />
                      Writing Data
                    </Badge>
                  )}
                </div>
                <span className="text-lg font-bold text-primary">{displayOverallProgress.toFixed(1)}%</span>
              </div>
              <Progress value={displayOverallProgress} className="h-3" />
              <div className="flex justify-between mt-2 text-xs text-muted-foreground">
                <span>
                  {cursorStats.completedCursors} of {cursorStats.totalCursors} migrations complete
                  {isStillWriting && " (data still being written)"}
                </span>
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

        {/* Gap Detection */}
        <GapDetectionCard refreshInterval={30000} />

        {/* Shard Progress */}
        <ShardProgressCard refreshInterval={3000} />

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
                          {allComplete && !isStillWriting ? (
                            <Badge variant="default" className="bg-green-600">Complete</Badge>
                          ) : allComplete && isStillWriting ? (
                            <Badge variant="default" className="bg-blue-600 animate-pulse">Writing</Badge>
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
                          let progressPercent = 0;
                          if (cursor.min_time && cursor.max_time && cursor.last_before) {
                            const minTime = new Date(cursor.min_time).getTime();
                            const maxTime = new Date(cursor.max_time).getTime();
                            const currentTime = new Date(cursor.last_before).getTime();
                            const totalRange = maxTime - minTime;
                            const progressFromMax = maxTime - currentTime;
                            progressPercent = Math.min(100, Math.max(0, (progressFromMax / totalRange) * 100));
                          } else if (cursor.complete) {
                            progressPercent = 100;
                          }

                          // If we're still writing data, don't render a misleading 100%.
                          const displayProgressPercent = isStillWriting && progressPercent >= 100 ? 99.9 : progressPercent;

                          const { eta, throughput } = calculateETA(cursor);

                          return (
                            <div key={cursor.id} className="space-y-2 p-3 rounded-lg bg-muted/50">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                  <span className="font-mono text-sm font-medium">{cursor.cursor_name}</span>
                                  {cursor.complete && !isStillWriting ? (
                                    <Badge variant="default" className="text-xs bg-primary text-primary-foreground">Complete</Badge>
                                  ) : cursor.complete && isStillWriting ? (
                                    <Badge variant="default" className="text-xs bg-accent text-accent-foreground animate-pulse">Writing</Badge>
                                  ) : (
                                    <Badge variant="secondary" className="text-xs">In Progress</Badge>
                                  )}
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
                                  {/* Direction explanation */}
                                  <div className="text-xs text-muted-foreground mb-1">
                                    Processing backwards: {format(new Date(cursor.max_time), "MMM d, yyyy")} → {format(new Date(cursor.min_time), "MMM d, yyyy")}
                                  </div>
                                  <div className="flex items-center justify-between text-xs">
                                    <span className="text-green-400 font-medium">Oldest: {format(new Date(cursor.min_time), "MMM d, yyyy HH:mm")}</span>
                                    <span className="text-muted-foreground">Newest: {format(new Date(cursor.max_time), "MMM d, yyyy HH:mm")}</span>
                                  </div>
                                  {cursor.last_before && (
                                    <div className="flex items-center justify-between text-xs">
                                      <span className="text-primary font-semibold">
                                        Position: {format(new Date(cursor.last_before), "MMM d, yyyy HH:mm:ss")}
                                      </span>
                                      <span className="text-primary font-semibold">{displayProgressPercent.toFixed(1)}%</span>
                                    </div>
                                  )}
                                  <div className="relative">
                                    <Progress value={displayProgressPercent} className="h-2" />
                                    {/* Direction arrow indicator */}
                                    <div className="absolute inset-0 flex items-center pointer-events-none">
                                      <div 
                                        className="w-0 h-0 border-t-[4px] border-t-transparent border-b-[4px] border-b-transparent border-r-[6px] border-r-primary transition-all duration-300"
                                        style={{ marginLeft: `calc(${Math.min(displayProgressPercent, 98)}% - 3px)` }}
                                      />
                                    </div>
                                  </div>
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
