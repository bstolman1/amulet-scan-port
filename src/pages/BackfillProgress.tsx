import { useState, useEffect, useRef, useMemo } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { supabase } from "@/integrations/supabase/client";
import { Clock, Database, Activity, Zap, Trash2, FileText, Layers } from "lucide-react";
import { formatDistanceToNow, format } from "date-fns";
import { useBackfillCursors, BackfillCursor } from "@/hooks/use-backfill-cursors";
import { useLedgerUpdates } from "@/hooks/use-ledger-updates";
import { useToast } from "@/hooks/use-toast";
import { useQuery } from "@tanstack/react-query";

interface ActivityLog {
  id: string;
  timestamp: string;
  type: "update" | "event" | "cursor_update";
  updateId?: string;
  eventId?: string;
  migrationId?: number;
  synchronizerId?: string;
  complete?: boolean;
}

interface BackfillStats {
  totalCursors: number;
  completedCursors: number;
  totalUpdates: number;
  totalEvents: number;
  activeMigrations: number;
}

const BackfillProgress = () => {
  const { data: cursors = [], isLoading, refetch } = useBackfillCursors();
  const { data: recentUpdates = [] } = useLedgerUpdates(50);
  const { data: recentEvents = [] } = useQuery({
    queryKey: ["ledgerEvents", 50],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .order("created_at", { ascending: false })
        .limit(50);
      if (error) throw error;
      return data;
    },
    staleTime: 5_000,
  });
  const [realtimeCursors, setRealtimeCursors] = useState<BackfillCursor[]>([]);
  const [isPurging, setIsPurging] = useState(false);
  const [activityLog, setActivityLog] = useState<ActivityLog[]>([]);
  const [isMonitoring, setIsMonitoring] = useState(true);
  const [activeFilters, setActiveFilters] = useState<Set<string>>(new Set(["cursor_update", "update", "event"]));
  const [stats, setStats] = useState<BackfillStats>({
    totalCursors: 0,
    completedCursors: 0,
    totalUpdates: 0,
    totalEvents: 0,
    activeMigrations: 0,
  });
  const [lastActivity, setLastActivity] = useState<string>("");
  const logEndRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();

  const toggleFilter = (type: string) => {
    setActiveFilters((prev) => {
      const newFilters = new Set(prev);
      if (newFilters.has(type)) {
        newFilters.delete(type);
      } else {
        newFilters.add(type);
      }
      return newFilters;
    });
  };

  const filteredActivityLog = activityLog.filter((log) => activeFilters.has(log.type));

  // Calculate merged cursors list
  const allCursors = useMemo(() => {
    return [...realtimeCursors, ...cursors.filter((c) => !realtimeCursors.some((rc) => rc.id === c.id))];
  }, [cursors, realtimeCursors]);

  // Load initial stats on mount
  useEffect(() => {
    const loadInitialStats = async () => {
      try {
        const [updatesCount, eventsCount] = await Promise.all([
          supabase.from("ledger_updates").select("*", { count: "exact", head: true }),
          supabase.from("ledger_events").select("*", { count: "exact", head: true }),
        ]);

        setStats((prev) => ({
          ...prev,
          totalUpdates: updatesCount.count || 0,
          totalEvents: eventsCount.count || 0,
        }));
      } catch (error) {
        console.error("Failed to load initial stats:", error);
      }
    };

    loadInitialStats();
  }, []);

  // Update cursor stats when cursors change
  useEffect(() => {
    setStats((prev) => ({
      ...prev,
      totalCursors: allCursors.length,
      completedCursors: allCursors.filter((c) => c.last_processed_round > 0).length,
    }));
  }, [allCursors]);

  // Auto-scroll to bottom of activity log
  useEffect(() => {
    if (isMonitoring) {
      logEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [activityLog, isMonitoring]);

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
            if (isMonitoring) {
              const cursor = payload.new as BackfillCursor;
              setActivityLog((prev) => [
                ...prev.slice(-99),
                {
                  id: crypto.randomUUID(),
                  timestamp: new Date().toISOString(),
                  type: "cursor_update",
                },
              ]);
              setLastActivity(`New cursor: ${cursor.cursor_name}`);
            }
          } else if (payload.eventType === "UPDATE") {
            setRealtimeCursors((prev) =>
              prev.map((c) => (c.id === payload.new.id ? (payload.new as BackfillCursor) : c)),
            );
            if (isMonitoring && payload.new.last_processed_round > 0) {
              const cursor = payload.new as BackfillCursor;
              setActivityLog((prev) => [
                ...prev.slice(-99),
                {
                  id: crypto.randomUUID(),
                  timestamp: new Date().toISOString(),
                  type: "cursor_update",
                  complete: true,
                },
              ]);
              setLastActivity(`Updated: ${cursor.cursor_name}`);
            }
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
          if (isMonitoring) {
            setStats((prev) => ({ ...prev, totalUpdates: prev.totalUpdates + 1 }));
            setActivityLog((prev) => [
              ...prev.slice(-99),
              {
                id: crypto.randomUUID(),
                timestamp: new Date().toISOString(),
                type: "update",
                updateId: payload.new.id,
              },
            ]);
            setLastActivity(`New update: ${payload.new.update_type}`);
          }
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
          if (isMonitoring) {
            setStats((prev) => ({ ...prev, totalEvents: prev.totalEvents + 1 }));
            setActivityLog((prev) => [
              ...prev.slice(-99),
              {
                id: crypto.randomUUID(),
                timestamp: new Date().toISOString(),
                type: "event",
                eventId: payload.new.id,
              },
            ]);
            setLastActivity(`New event: ${payload.new.event_type}`);
          }
        },
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, [isMonitoring]);

  

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
      setActivityLog([]);
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
              <Badge variant={isMonitoring ? "default" : "secondary"} className="gap-1">
                {isMonitoring && <Activity className="w-3 h-3 animate-pulse" />}
                {isMonitoring ? "Processing" : "Paused"}
              </Badge>
            </div>
            <p className="text-sm text-muted-foreground">{lastActivity || "Waiting for activity..."}</p>
          </div>
          <div className="flex gap-2">
            <Button onClick={() => setIsMonitoring(!isMonitoring)} variant="outline" size="sm">
              {isMonitoring ? "Pause" : "Resume"}
            </Button>
            <Button onClick={handlePurgeAll} disabled={isPurging} variant="destructive" size="sm">
              <Trash2 className={`h-4 w-4 mr-2 ${isPurging ? "animate-spin" : ""}`} />
              {isPurging ? "Purging..." : "Purge All"}
            </Button>
          </div>
        </div>

        {/* Stats Grid */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Database className="w-4 h-4" />
                Cursors Completed
              </div>
              <div className="text-2xl font-bold text-primary">{stats.completedCursors}</div>
            </CardContent>
          </Card>

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <FileText className="w-4 h-4" />
                Total Cursors
              </div>
              <div className="text-2xl font-bold text-primary">{stats.totalCursors}</div>
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

          <Card className="bg-card/50 backdrop-blur">
            <CardContent className="p-4">
              <div className="flex items-center gap-2 text-muted-foreground text-sm mb-1">
                <Activity className="w-4 h-4" />
                Activity Log
              </div>
              <div className="text-2xl font-bold text-primary">{activityLog.length}</div>
            </CardContent>
          </Card>
        </div>

        {/* Last Activity Bar */}
        {lastActivity && (
          <div className="flex items-center justify-between px-4 py-2 bg-primary/5 border border-primary/20 rounded-lg text-sm">
            <span className="text-primary font-medium">{lastActivity}</span>
            <span className="text-muted-foreground">
              {activityLog.length > 0 &&
                formatDistanceToNow(new Date(activityLog[activityLog.length - 1].timestamp), { addSuffix: true })}
            </span>
          </div>
        )}

        {/* Activity List */}
        <Card className="bg-card/50 backdrop-blur">
          <CardContent className="p-0">
            <div className="flex items-center justify-between p-4 border-b">
              <div className="flex items-center gap-4">
                <h3 className="font-semibold">Synchronizer Activity</h3>
                <div className="flex items-center gap-2">
                  <Badge
                    variant={activeFilters.has("cursor_update") ? "default" : "outline"}
                    className="cursor-pointer hover:bg-primary/80 transition-colors"
                    onClick={() => toggleFilter("cursor_update")}
                  >
                    <Database className="w-3 h-3 mr-1" />
                    Cursors
                  </Badge>
                  <Badge
                    variant={activeFilters.has("update") ? "default" : "outline"}
                    className="cursor-pointer hover:bg-primary/80 transition-colors bg-blue-500/10 text-blue-400 border-blue-500/20 hover:bg-blue-500/20"
                    onClick={() => toggleFilter("update")}
                  >
                    <Zap className="w-3 h-3 mr-1" />
                    Updates
                  </Badge>
                  <Badge
                    variant={activeFilters.has("event") ? "default" : "outline"}
                    className="cursor-pointer hover:bg-primary/80 transition-colors bg-green-500/10 text-green-400 border-green-500/20 hover:bg-green-500/20"
                    onClick={() => toggleFilter("event")}
                  >
                    <Layers className="w-3 h-3 mr-1" />
                    Events
                  </Badge>
                  <span className="text-xs text-muted-foreground ml-2">
                    {filteredActivityLog.length} / {activityLog.length}
                  </span>
                </div>
              </div>
              <Button onClick={() => setActivityLog([])} variant="ghost" size="sm">
                Clear
              </Button>
            </div>
            <div className="max-h-96 overflow-y-auto">
              {filteredActivityLog.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
                  <Activity className="w-12 h-12 mb-4 opacity-20" />
                  <p>
                    {activityLog.length === 0
                      ? "No activity yet. Waiting for backfill data..."
                      : "No activity matching current filters."}
                  </p>
                </div>
              ) : (
                <div className="divide-y">
                  {filteredActivityLog
                    .slice()
                    .reverse()
                    .map((log) => (
                      <div
                        key={log.id}
                        className="flex items-center justify-between px-4 py-3 hover:bg-muted/50 transition-colors"
                      >
                        <div className="flex-1 min-w-0">
                          <div className="font-mono text-sm truncate">
                            {log.type === "cursor_update" ? (
                              <>
                                <Badge variant={log.complete ? "default" : "secondary"} className="mr-2">
                                  {log.complete ? "Completed" : "Updated"}
                                </Badge>
                                Cursor updated
                              </>
                            ) : log.type === "update" ? (
                              <>
                                <Badge
                                  variant="secondary"
                                  className="mr-2 bg-blue-500/10 text-blue-400 border-blue-500/20"
                                >
                                  Update
                                </Badge>
                                {log.updateId}
                              </>
                            ) : (
                              <>
                                <Badge
                                  variant="secondary"
                                  className="mr-2 bg-green-500/10 text-green-400 border-green-500/20"
                                >
                                  Event
                                </Badge>
                                {log.eventId}
                              </>
                            )}
                          </div>
                          {log.migrationId && (
                            <div className="text-xs text-muted-foreground mt-1">Migration #{log.migrationId}</div>
                          )}
                        </div>
                        <div className="flex items-center gap-2 ml-4">
                          <span className="text-xs text-muted-foreground whitespace-nowrap">
                            {formatDistanceToNow(new Date(log.timestamp), { addSuffix: true })}
                          </span>
                        </div>
                      </div>
                    ))}
                </div>
              )}
              <div ref={logEndRef} />
            </div>
          </CardContent>
        </Card>

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
                allCursors.map((cursor) => (
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
                        <div className="flex items-center justify-between text-xs text-muted-foreground">
                          <span>{format(new Date(cursor.min_time), "MMM d, yyyy HH:mm")}</span>
                          <span>{format(new Date(cursor.max_time), "MMM d, yyyy HH:mm")}</span>
                        </div>
                        <Progress value={cursor.complete ? 100 : 50} className="h-2" />
                      </div>
                    )}
                    <div className="text-xs text-muted-foreground">
                      Updated {formatDistanceToNow(new Date(cursor.updated_at), { addSuffix: true })}
                    </div>
                  </div>
                ))
              )}
            </div>
          </CardContent>
        </Card>

        {/* Recent Ledger Updates */}
        <Card className="bg-card/50 backdrop-blur">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Zap className="w-5 h-5 text-blue-400" />
              Recent Ledger Updates
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Type</TableHead>
                    <TableHead>Migration ID</TableHead>
                    <TableHead>Timestamp</TableHead>
                    <TableHead>Created</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {recentUpdates.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center text-muted-foreground">
                        No updates found
                      </TableCell>
                    </TableRow>
                  ) : (
                    recentUpdates.slice(0, 20).map((update) => (
                      <TableRow key={update.id}>
                        <TableCell className="font-mono text-xs">{update.update_type}</TableCell>
                        <TableCell>{update.migration_id || "-"}</TableCell>
                        <TableCell>
                          {update.migration_id ? (
                            <Badge variant="outline" className="text-xs">
                              {update.migration_id}
                            </Badge>
                          ) : (
                            "-"
                          )}
                        </TableCell>
                        <TableCell className="text-xs">
                          {format(new Date(update.timestamp), "MMM d, HH:mm:ss")}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {formatDistanceToNow(new Date(update.created_at), { addSuffix: true })}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>

        {/* Recent Ledger Events */}
        <Card className="bg-card/50 backdrop-blur">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Layers className="w-5 h-5 text-green-400" />
              Recent Ledger Events
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Type</TableHead>
                    <TableHead>Migration ID</TableHead>
                    <TableHead>Template</TableHead>
                    <TableHead>Migration ID</TableHead>
                    <TableHead>Timestamp</TableHead>
                    <TableHead>Created</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {!recentEvents || recentEvents.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-center text-muted-foreground">
                        No events found
                      </TableCell>
                    </TableRow>
                  ) : (
                    recentEvents.slice(0, 20).map((event) => (
                      <TableRow key={event.id}>
                        <TableCell className="font-mono text-xs">{event.event_type}</TableCell>
                        <TableCell>{event.migration_id || "-"}</TableCell>
                        <TableCell className="text-xs truncate max-w-[200px]">{event.template_id || "-"}</TableCell>
                        <TableCell>
                          {event.migration_id ? (
                            <Badge variant="outline" className="text-xs">
                              {event.migration_id}
                            </Badge>
                          ) : (
                            "-"
                          )}
                        </TableCell>
                        <TableCell className="text-xs">
                          {format(new Date(event.timestamp), "MMM d, HH:mm:ss")}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {formatDistanceToNow(new Date(event.created_at), { addSuffix: true })}
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default BackfillProgress;
