import { useState, useEffect, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { supabase } from "@/integrations/supabase/client";
import { Database, Activity, Trash2, FileText, Layers, Zap } from "lucide-react";
import { formatDistanceToNow, format } from "date-fns";
import { useBackfillCursors, useBackfillStats, BackfillCursor } from "@/hooks/use-backfill-cursors";
import { useToast } from "@/hooks/use-toast";

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

        {/* Backfill Cursors Progress */}
        <Card className="bg-card/50 backdrop-blur">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="w-5 h-5" />
              Backfill Cursors
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {allCursors.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">No cursors found</div>
              ) : (
                allCursors.map((cursor) => {
                  // Calculate progress percentage based on time range
                  // Note: backfill goes backwards in time (max_time -> min_time)
                  let progressPercent = 0;
                  if (cursor.min_time && cursor.max_time && cursor.last_before) {
                    const minTime = new Date(cursor.min_time).getTime();
                    const maxTime = new Date(cursor.max_time).getTime();
                    const currentTime = new Date(cursor.last_before).getTime();
                    const totalRange = maxTime - minTime;
                    // Calculate how much we've moved backward from max_time
                    const progressFromMax = maxTime - currentTime;
                    progressPercent = Math.min(100, Math.max(0, (progressFromMax / totalRange) * 100));
                  } else if (cursor.complete) {
                    progressPercent = 100;
                  }

                  return (
                    <div key={cursor.id} className="space-y-2 p-4 rounded-lg bg-muted/50">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <span className="font-mono text-sm font-medium">{cursor.cursor_name}</span>
                          <Badge variant={cursor.complete ? "default" : "secondary"}>
                            {cursor.complete ? "Complete" : "In Progress"}
                          </Badge>
                        </div>
                        <div className="text-sm text-muted-foreground">
                          Round {cursor.last_processed_round.toLocaleString()}
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
                        </div>
                      )}
                      <div className="text-xs text-muted-foreground">
                        Updated {formatDistanceToNow(new Date(cursor.updated_at), { addSuffix: true })}
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
