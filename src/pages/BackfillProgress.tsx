import { useState, useEffect, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { supabase } from "@/integrations/supabase/client";
import { Database, Activity, Trash2, FileText, Layers, Zap } from "lucide-react";
import { formatDistanceToNow, format } from "date-fns";
import { useBackfillCursors, BackfillCursor } from "@/hooks/use-backfill-cursors";
import { useToast } from "@/hooks/use-toast";

interface BackfillStats {
  totalCursors: number;
  completedCursors: number;
  totalUpdates: number;
  totalEvents: number;
  activeMigrations: number;
}

const BackfillProgress = () => {
  const { data: cursors = [], isLoading, refetch } = useBackfillCursors();
  const [realtimeCursors, setRealtimeCursors] = useState<BackfillCursor[]>([]);
  const [isPurging, setIsPurging] = useState(false);
  const [stats, setStats] = useState<BackfillStats>({
    totalCursors: 0,
    completedCursors: 0,
    totalUpdates: 0,
    totalEvents: 0,
    activeMigrations: 0,
  });
  const { toast } = useToast();

  // Calculate merged cursors list
  const allCursors = useMemo(() => {
    return [...realtimeCursors, ...cursors.filter((c) => !realtimeCursors.some((rc) => rc.id === c.id))];
  }, [cursors, realtimeCursors]);

  // Calculate cursor stats from allCursors (derived, not in useEffect)
  const cursorStats = useMemo(() => ({
    totalCursors: allCursors.length,
    completedCursors: allCursors.filter((c) => c.last_processed_round > 0).length,
  }), [allCursors]);

  // Load initial stats on mount only
  useEffect(() => {
    let isMounted = true;
    
    const loadInitialStats = async () => {
      try {
        const [updatesCount, eventsCount, migrationsResult] = await Promise.all([
          supabase.from("ledger_updates").select("*", { count: "exact", head: true }),
          supabase.from("ledger_events").select("*", { count: "exact", head: true }),
          supabase.from("ledger_updates").select("migration_id").not("migration_id", "is", null),
        ]);

        if (!isMounted) return;

        // Count unique migration IDs
        const uniqueMigrations = new Set(migrationsResult.data?.map(row => row.migration_id) || []);

        setStats({
          totalCursors: 0,
          completedCursors: 0,
          totalUpdates: updatesCount.count || 0,
          totalEvents: eventsCount.count || 0,
          activeMigrations: uniqueMigrations.size,
        });
      } catch (error) {
        console.error("Failed to load initial stats:", error);
      }
    };

    loadInitialStats();
    return () => { isMounted = false; };
  }, []);

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
        (payload: any) => {
          setStats((prev) => ({ ...prev, totalUpdates: prev.totalUpdates + 1 }));
        },
      )
      .on(
        "postgres_changes",
        {
          event: "INSERT",
          schema: "public",
          table: "ledger_events",
        },
        (payload: any) => {
          setStats((prev) => ({ ...prev, totalEvents: prev.totalEvents + 1 }));
        },
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, []);

  

  const handlePurgeAll = async () => {
    if (
      !confirm(
        "Are you sure you want to purge ALL backfill data? This will delete all backfill cursors, ledger updates, and ledger events. This action cannot be undone.",
      )
    ) {
      return;
    }

    setIsPurging(true);
    try {
      const { data, error } = await supabase.functions.invoke("purge-backfill-data", {
        body: { purge_all: true },
      });

      if (error) throw error;

      toast({
        title: "Purge complete",
        description: `Deleted ${data.deleted_cursors} cursors, ${data.deleted_updates} updates, ${data.deleted_events} events`,
      });

      // Refresh the cursors list
      refetch();
      setRealtimeCursors([]);
      setStats({
        totalCursors: 0,
        completedCursors: 0,
        totalUpdates: 0,
        totalEvents: 0,
        activeMigrations: 0,
      });
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
              <div className="text-2xl font-bold text-blue-400">{stats.totalUpdates}</div>
            </CardContent>
          </Card>

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Layers className="w-4 h-4" />
                Events Received
              </div>
              <div className="text-2xl font-bold text-green-400">{stats.totalEvents}</div>
            </CardContent>
          </Card>

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Database className="w-4 h-4" />
                Active Migrations
              </div>
              <div className="text-2xl font-bold text-primary">{stats.activeMigrations}</div>
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
