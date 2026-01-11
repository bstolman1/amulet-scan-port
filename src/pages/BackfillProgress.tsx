import { useState, useEffect, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Database, Activity, Trash2, FileText, Layers, Zap, Clock, PlayCircle, PauseCircle, CheckCircle2 } from "lucide-react";
import { formatDistanceToNow, format, formatDuration, intervalToDuration } from "date-fns";
import { useBackfillCursors, useBackfillStats, useWriteActivity, useBackfillDebugInfo, BackfillCursor } from "@/hooks/use-backfill-cursors";
import { useToast } from "@/hooks/use-toast";
import { ShardProgressCard } from "@/components/ShardProgressCard";
import { GapRecoveryPanel } from "@/components/GapRecoveryPanel";
import { DataIntegrityValidator } from "@/components/DataIntegrityValidator";
import { apiFetch } from "@/lib/duckdb-api-client";

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
  const { data: writeActivity } = useWriteActivity();
  const { data: backfillDebug } = useBackfillDebugInfo();
  const [realtimeCursors, setRealtimeCursors] = useState<BackfillCursor[]>([]);
  const [isPurging, setIsPurging] = useState(false);
  const { toast } = useToast();

  // Calculate merged cursors list
  const allCursors = useMemo(() => {
    return [...realtimeCursors, ...cursors.filter((c) => !realtimeCursors.some((rc) => rc.id === c.id))];
  }, [cursors, realtimeCursors]);

  // Track if data is still being written (cursor buffers OR counts increasing OR file activity)
  const hasPendingWork = useMemo(() => {
    return allCursors.some((c) => (c.pending_writes || 0) > 0 || (c.buffered_records || 0) > 0);
  }, [allCursors]);

  // Check if any cursor was recently updated (within last 2 minutes)
  const hasRecentActivity = useMemo(() => {
    const twoMinutesAgo = Date.now() - 2 * 60 * 1000;
    return allCursors.some(c => {
      const updated = c.updated_at ? new Date(c.updated_at).getTime() : 0;
      return updated > twoMinutesAgo && !c.complete;
    });
  }, [allCursors]);

  const [prevStats, setPrevStats] = useState<{ updates: number; events: number } | null>(null);
  const [isStatsIncreasing, setIsStatsIncreasing] = useState(false);

  // Detect increasing counts across polling intervals (keep the signal true until the next stats update)
  useEffect(() => {
    if (!stats) return;

    setPrevStats((prev) => {
      if (prev) {
        setIsStatsIncreasing(
          stats.totalUpdates > prev.updates || stats.totalEvents > prev.events,
        );
      } else {
        setIsStatsIncreasing(false);
      }

      return { updates: stats.totalUpdates, events: stats.totalEvents };
    });
  }, [stats]);

  // Use multiple signals to detect writing activity
  const isStillWriting = useMemo(() => {
    return hasPendingWork || isStatsIncreasing || writeActivity?.isWriting || hasRecentActivity;
  }, [hasPendingWork, isStatsIncreasing, writeActivity?.isWriting, hasRecentActivity]);

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

  const migrationsInCursors = useMemo(() => {
    const set = new Set<number>();
    for (const c of allCursors) {
      if (typeof c.migration_id === "number") set.add(c.migration_id);
    }
    return Array.from(set).sort((a, b) => a - b);
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
      const cursorHasPendingWork = (cursor.pending_writes || 0) > 0 || (cursor.buffered_records || 0) > 0;

      if (cursor.complete) {
        totalProgress += cursorHasPendingWork ? 99.9 : 100;
        validCursors++;
      } else if (cursor.min_time && cursor.max_time && cursor.last_before) {
        const minTime = new Date(cursor.min_time).getTime();
        const maxTime = new Date(cursor.max_time).getTime();
        const currentTime = new Date(cursor.last_before).getTime();
        const totalRange = maxTime - minTime;
        if (totalRange > 0) {
          const progressFromMax = maxTime - currentTime;
          const computed = Math.min(100, Math.max(0, (progressFromMax / totalRange) * 100));
          totalProgress += cursorHasPendingWork ? Math.min(computed, 99.9) : computed;
          validCursors++;
        }
      }
    }

    return validCursors > 0 ? totalProgress / validCursors : 0;
  }, [allCursors]);

  // While data is still being written, avoid showing a misleading 100%
  const displayOverallProgress = useMemo(() => {
    if (!isStillWriting) return overallProgress;
    return overallProgress >= 99.95 ? 99.9 : overallProgress;
  }, [isStillWriting, overallProgress]);

  // Data auto-refreshes via react-query refetchInterval

  

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
      const response = await apiFetch<{ deleted_cursors: number; deleted_data_dir?: boolean }>("/api/backfill/purge", { method: "DELETE" });
      toast({
        title: "Purge complete",
        description: `Deleted ${response.deleted_cursors} cursor files${response.deleted_data_dir ? " and raw data directory" : ""}`,
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

        {/* Local backfill diagnostics (helps explain why a migration is missing) */}
        {(backfillDebug || (stats?.activeMigrations && migrationsInCursors.length > 0)) && (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm">Local Backfill Diagnostics</CardTitle>
            </CardHeader>
            <CardContent className="pt-0 text-sm text-muted-foreground space-y-1">
              {backfillDebug && (
                <>
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span>Cursor dir</span>
                    <span className="font-mono text-xs text-foreground/80 break-all">{backfillDebug.cursorDir}</span>
                  </div>
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span>Cursor files</span>
                    <span className="text-foreground/80">{backfillDebug.cursorFiles?.length ?? 0}</span>
                  </div>
                </>
              )}
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span>Migrations from cursor files</span>
                <span className="text-foreground/80">{migrationsInCursors.length ? migrationsInCursors.join(", ") : "None"}</span>
              </div>
              {typeof stats?.activeMigrations === "number" && (
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span>Migrations seen in data files</span>
                  <span className="text-foreground/80">{stats.activeMigrations}</span>
                </div>
              )}
              {(stats as any)?.rawFileCounts && (
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span>Raw binary files</span>
                  <span className="text-foreground/80">{(stats as any).rawFileCounts.events?.toLocaleString() || 0} events, {(stats as any).rawFileCounts.updates?.toLocaleString() || 0} updates</span>
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* Write Activity Status */}
        {writeActivity && (
          <Card className={`border-2 ${writeActivity.isWriting ? "border-blue-500/50 bg-blue-500/5" : "border-muted"}`}>
            <CardContent className="p-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  {writeActivity.isWriting ? (
                    <>
                      <Zap className="w-4 h-4 text-blue-500 animate-pulse" />
                      <span className="font-medium text-blue-500">Actively Writing Files</span>
                    </>
                  ) : (
                    <>
                      <CheckCircle2 className="w-4 h-4 text-muted-foreground" />
                      <span className="text-muted-foreground">No active writes detected</span>
                    </>
                  )}
                </div>
                <div className="text-sm text-muted-foreground">
                  {writeActivity.eventFiles.toLocaleString()} event files • {writeActivity.updateFiles.toLocaleString()} update files
                </div>
              </div>
            </CardContent>
          </Card>
        )}

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

        {/* Live Updates Status Card */}
        {cursorsByMigration.length > 0 && (
          <Card className={`border-2 ${
            cursorsByMigration.every(({ cursors }) => cursors.every(c => c.complete)) && !isStillWriting
              ? "border-green-500/50 bg-green-500/5"
              : "border-amber-500/50 bg-amber-500/5"
          }`}>
            <CardContent className="p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  {cursorsByMigration.every(({ cursors }) => cursors.every(c => c.complete)) && !isStillWriting ? (
                    <>
                      <PlayCircle className="w-5 h-5 text-green-500" />
                      <span className="font-semibold text-green-500">Live Updates Ready</span>
                    </>
                  ) : (
                    <>
                      <PauseCircle className="w-5 h-5 text-amber-500" />
                      <span className="font-semibold text-amber-500">Live Updates Pending</span>
                    </>
                  )}
                </div>
              </div>
              
              {cursorsByMigration.every(({ cursors }) => cursors.every(c => c.complete)) && !isStillWriting ? (
                <p className="text-sm text-muted-foreground">
                  All migrations complete. Live updates will start automatically.
                </p>
              ) : (
                <div className="space-y-2">
                  <p className="text-sm text-muted-foreground mb-2">
                    Migrations must complete before live updates begin:
                  </p>
                  <div className="flex flex-wrap gap-2">
                    {cursorsByMigration.map(({ migrationId, cursors: migCursors }) => {
                      const allComplete = migCursors.every(c => c.complete);
                      const hasPending = migCursors.some(c => (c.pending_writes || 0) > 0 || (c.buffered_records || 0) > 0);
                      const completedCount = migCursors.filter(c => c.complete).length;
                      
                      return (
                        <div 
                          key={migrationId}
                          className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-sm ${
                            allComplete && !hasPending
                              ? "bg-green-500/20 text-green-400"
                              : "bg-amber-500/20 text-amber-400"
                          }`}
                        >
                          {allComplete && !hasPending ? (
                            <CheckCircle2 className="w-4 h-4" />
                          ) : (
                            <Activity className="w-4 h-4 animate-pulse" />
                          )}
                          <span>Migration {migrationId}</span>
                          <span className="text-xs opacity-75">
                            ({completedCount}/{migCursors.length})
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
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

        {/* Gap Detection & Recovery */}
        <GapRecoveryPanel refreshInterval={30000} />

        {/* Data Integrity Validator */}
        <DataIntegrityValidator />

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
                  const migrationHasPendingWork = migrationCursors.some(
                    (c) => (c.pending_writes || 0) > 0 || (c.buffered_records || 0) > 0,
                  );
                  const isActive = migrationId === activeMigrationId;
                  const completedCount = migrationCursors.filter(c => c.complete).length;
                  
                  return (
                    <div key={migrationId} className="space-y-3">
                      {/* Migration Header */}
                      <div className="flex items-center justify-between border-b border-border/50 pb-2">
                        <div className="flex items-center gap-3">
                          <span className="text-lg font-semibold">Migration {migrationId}</span>
                          {allComplete && !migrationHasPendingWork ? (
                            <Badge variant="default" className="bg-green-600">Complete</Badge>
                          ) : allComplete && migrationHasPendingWork ? (
                            <Badge variant="default" className="bg-accent text-accent-foreground animate-pulse">Finalizing</Badge>
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
                          const cursorHasPendingWork = (cursor.pending_writes || 0) > 0 || (cursor.buffered_records || 0) > 0;

                          let progressPercent = 0;

                          // Compute range progress (backfill runs backwards: max_time → min_time)
                          if (cursor.min_time && cursor.max_time && cursor.last_before) {
                            const minTime = new Date(cursor.min_time).getTime();
                            const maxTime = new Date(cursor.max_time).getTime();
                            const currentTime = new Date(cursor.last_before).getTime();
                            const totalRange = maxTime - minTime;
                            const progressFromMax = maxTime - currentTime;
                            progressPercent = totalRange > 0
                              ? Math.min(100, Math.max(0, (progressFromMax / totalRange) * 100))
                              : 0;
                          } else if (cursor.complete) {
                            progressPercent = cursorHasPendingWork ? 99.9 : 100;
                          }

                          // Never show 100% unless cursor is explicitly complete.
                          // Otherwise, 100% can happen when last_before <= min_time even if the cursor wasn't finalized.
                          let displayProgressPercent = cursor.complete
                            ? (cursorHasPendingWork ? 99.9 : 100)
                            : Math.min(progressPercent, 99.9);

                          // While data is still being written, also avoid rendering a misleading 100%/near-100%.
                          if ((isStillWriting || cursorHasPendingWork) && displayProgressPercent >= 99.95) {
                            displayProgressPercent = 99.9;
                          }

                          const { eta, throughput } = calculateETA(cursor);

                          return (
                            <div key={cursor.id} className="space-y-2 p-3 rounded-lg bg-muted/50">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-3">
                                  <span className="font-mono text-sm font-medium">{cursor.cursor_name}</span>
                                  {cursor.complete && !cursorHasPendingWork ? (
                                    <Badge variant="default" className="text-xs bg-primary text-primary-foreground">Complete</Badge>
                                  ) : cursor.complete && cursorHasPendingWork ? (
                                    <Badge variant="default" className="text-xs bg-accent text-accent-foreground animate-pulse">Finalizing</Badge>
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
