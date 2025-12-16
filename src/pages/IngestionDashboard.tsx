import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { 
  Activity, Database, FileText, Radio, Circle, Clock, 
  CheckCircle2, AlertCircle, Zap, RefreshCw, Layers, Timer
} from "lucide-react";
import { formatDistanceToNow, format } from "date-fns";
import { useBackfillCursors, useBackfillStats, useWriteActivity, BackfillCursor } from "@/hooks/use-backfill-cursors";
import { getLiveStatus, type LiveStatus } from "@/lib/duckdb-api-client";
import { useToast } from "@/hooks/use-toast";

/**
 * Combined Ingestion Dashboard - Shows both backfill and live ingestion status
 */
const IngestionDashboard = () => {
  const { data: cursors = [], isLoading: cursorsLoading, refetch: refetchCursors } = useBackfillCursors();
  const { data: stats, refetch: refetchStats } = useBackfillStats();
  const { data: writeActivity } = useWriteActivity();
  const { toast } = useToast();

  // Fetch live status
  const { data: liveStatus, isLoading: liveStatusLoading, refetch: refetchLiveStatus } = useQuery({
    queryKey: ["liveStatus"],
    queryFn: getLiveStatus,
    refetchInterval: 10000,
    retry: false,
  });

  const handleRefresh = () => {
    refetchCursors();
    refetchStats();
    refetchLiveStatus();
    toast({ title: "Refreshing data..." });
  };

  // Calculate backfill progress
  const backfillProgress = useMemo(() => {
    if (cursors.length === 0) return { progress: 0, completedCount: 0, totalCount: 0 };

    let totalProgress = 0;
    let completedCount = 0;

    for (const cursor of cursors) {
      if (cursor.complete) {
        totalProgress += 100;
        completedCount++;
      } else if (cursor.min_time && cursor.max_time && cursor.last_before) {
        const minTime = new Date(cursor.min_time).getTime();
        const maxTime = new Date(cursor.max_time).getTime();
        const currentTime = new Date(cursor.last_before).getTime();
        const totalRange = maxTime - minTime;
        if (totalRange > 0) {
          totalProgress += Math.min(100, Math.max(0, ((maxTime - currentTime) / totalRange) * 100));
        }
      }
    }

    return {
      progress: cursors.length > 0 ? totalProgress / cursors.length : 0,
      completedCount,
      totalCount: cursors.length,
    };
  }, [cursors]);

  // Determine overall system status
  const systemStatus = useMemo(() => {
    const allBackfillComplete = liveStatus?.all_backfill_complete ?? backfillProgress.completedCount === backfillProgress.totalCount;
    const liveActive = liveStatus?.status === 'running';
    const backfillActive = writeActivity?.isWriting || cursors.some(c => !c.complete && c.is_recently_updated);

    if (liveActive) {
      return { status: 'live', label: 'Live Ingestion Active', color: 'text-green-500', bgColor: 'bg-green-500/10' };
    }
    if (backfillActive) {
      return { status: 'backfill', label: 'Backfill Running', color: 'text-blue-500', bgColor: 'bg-blue-500/10' };
    }
    if (allBackfillComplete && !liveStatus?.live_cursor) {
      return { status: 'ready', label: 'Ready for Live Mode', color: 'text-amber-500', bgColor: 'bg-amber-500/10' };
    }
    if (allBackfillComplete) {
      return { status: 'idle', label: 'System Idle', color: 'text-muted-foreground', bgColor: 'bg-muted/50' };
    }
    return { status: 'stopped', label: 'Ingestion Stopped', color: 'text-muted-foreground', bgColor: 'bg-muted/50' };
  }, [liveStatus, backfillProgress, writeActivity, cursors]);

  const isLoading = cursorsLoading || liveStatusLoading;

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
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold mb-2">Ingestion Dashboard</h1>
            <p className="text-muted-foreground">Combined backfill and live ingestion status</p>
          </div>
          <Button variant="outline" onClick={handleRefresh}>
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>

        {/* System Status Banner */}
        <Card className={`border-2 ${systemStatus.bgColor}`}>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                {systemStatus.status === 'live' && <Radio className={`w-5 h-5 ${systemStatus.color} animate-pulse`} />}
                {systemStatus.status === 'backfill' && <Zap className={`w-5 h-5 ${systemStatus.color} animate-pulse`} />}
                {systemStatus.status === 'ready' && <AlertCircle className={`w-5 h-5 ${systemStatus.color}`} />}
                {systemStatus.status === 'idle' && <CheckCircle2 className={`w-5 h-5 ${systemStatus.color}`} />}
                {systemStatus.status === 'stopped' && <Circle className={`w-5 h-5 ${systemStatus.color}`} />}
                <div>
                  <p className={`font-semibold ${systemStatus.color}`}>{systemStatus.label}</p>
                  {liveStatus?.suggestion && (
                    <p className="text-sm text-muted-foreground mt-1">{liveStatus.suggestion}</p>
                  )}
                </div>
              </div>
              <div className="flex items-center gap-4 text-sm text-muted-foreground">
                {liveStatus?.latest_file_write && (
                  <div className="flex items-center gap-1">
                    <FileText className="w-4 h-4" />
                    <span>Last write: {formatDistanceToNow(new Date(liveStatus.latest_file_write), { addSuffix: true })}</span>
                  </div>
                )}
                {liveStatus?.current_record_time && (
                  <div className="flex items-center gap-1">
                    <Timer className="w-4 h-4" />
                    <span>Ledger: {formatDistanceToNow(new Date(liveStatus.current_record_time), { addSuffix: true })}</span>
                  </div>
                )}
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Stats Grid */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Layers className="w-4 h-4" />
                Migrations
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-primary">{stats?.activeMigrations || cursors.length || 0}</p>
              <p className="text-xs text-muted-foreground">{backfillProgress.completedCount} complete</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Updates
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-emerald-500">{stats?.totalUpdates?.toLocaleString() || 0}</p>
              <p className="text-xs text-muted-foreground">{(stats as any)?.rawFileCounts?.updates?.toLocaleString() || 0} files</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Database className="w-4 h-4" />
                Events
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-amber-500">{stats?.totalEvents?.toLocaleString() || 0}</p>
              <p className="text-xs text-muted-foreground">{(stats as any)?.rawFileCounts?.events?.toLocaleString() || 0} files</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Clock className="w-4 h-4" />
                Progress
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-violet-500">{backfillProgress.progress.toFixed(1)}%</p>
              <p className="text-xs text-muted-foreground">Overall backfill</p>
            </CardContent>
          </Card>
        </div>

        {/* Tabs for Backfill vs Live */}
        <Tabs defaultValue="overview" className="space-y-4">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="backfill">Backfill Cursors</TabsTrigger>
            <TabsTrigger value="live">Live Status</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="space-y-4">
            {/* Backfill Progress Bar */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm flex items-center gap-2">
                  <Zap className="w-4 h-4" />
                  Backfill Progress
                </CardTitle>
              </CardHeader>
              <CardContent>
                <Progress value={backfillProgress.progress} className="h-3 mb-2" />
                <div className="flex justify-between text-xs text-muted-foreground">
                  <span>{backfillProgress.completedCount} of {backfillProgress.totalCount} migrations complete</span>
                  <span>{backfillProgress.progress.toFixed(1)}%</span>
                </div>
              </CardContent>
            </Card>

            {/* Live Cursor Status */}
            {liveStatus?.live_cursor && (
              <Card className="border-green-500/30 bg-green-500/5">
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <Radio className="w-4 h-4 text-green-500" />
                    Live Cursor
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Migration</span>
                    <span>{liveStatus.live_cursor.migration_id}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Record Time</span>
                    <span className="font-mono text-xs">{liveStatus.live_cursor.record_time?.substring(0, 19)}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Last Updated</span>
                    <span>{formatDistanceToNow(new Date(liveStatus.live_cursor.updated_at), { addSuffix: true })}</span>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Quick Migration Summary */}
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Migration Summary</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {cursors.map((cursor) => {
                    const progress = cursor.complete ? 100 : 
                      (cursor.min_time && cursor.max_time && cursor.last_before) ?
                        Math.min(100, Math.max(0, ((new Date(cursor.max_time).getTime() - new Date(cursor.last_before).getTime()) / 
                          (new Date(cursor.max_time).getTime() - new Date(cursor.min_time).getTime())) * 100)) : 0;
                    
                    return (
                      <div key={cursor.id} className="flex items-center gap-3">
                        <div className="w-24 text-sm font-medium">Migration {cursor.migration_id}</div>
                        <Progress value={progress} className="flex-1 h-2" />
                        <div className="w-20 text-right">
                          {cursor.complete ? (
                            <Badge variant="default" className="bg-green-500">Complete</Badge>
                          ) : (
                            <span className="text-sm text-muted-foreground">{progress.toFixed(0)}%</span>
                          )}
                        </div>
                      </div>
                    );
                  })}
                  {cursors.length === 0 && (
                    <p className="text-muted-foreground text-sm">No backfill cursors found</p>
                  )}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="backfill" className="space-y-4">
            {/* Detailed Cursor List */}
            {cursors.map((cursor) => (
              <Card key={cursor.id}>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-sm flex items-center gap-2">
                      <Database className="w-4 h-4" />
                      Migration {cursor.migration_id}
                    </CardTitle>
                    <Badge variant={cursor.complete ? "default" : "secondary"}>
                      {cursor.complete ? "Complete" : cursor.is_recently_updated ? "Active" : "Paused"}
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <p className="text-muted-foreground">Time Range</p>
                      <p className="font-mono text-xs">
                        {cursor.min_time?.substring(0, 10)} → {cursor.max_time?.substring(0, 10)}
                      </p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Current Position</p>
                      <p className="font-mono text-xs">{cursor.last_before?.substring(0, 19) || "N/A"}</p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Updates Processed</p>
                      <p>{cursor.total_updates?.toLocaleString() || 0}</p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Events Processed</p>
                      <p>{cursor.total_events?.toLocaleString() || 0}</p>
                    </div>
                  </div>
                  {!cursor.complete && cursor.min_time && cursor.max_time && cursor.last_before && (
                    <Progress 
                      value={Math.min(100, Math.max(0, ((new Date(cursor.max_time).getTime() - new Date(cursor.last_before).getTime()) / 
                        (new Date(cursor.max_time).getTime() - new Date(cursor.min_time).getTime())) * 100))} 
                      className="h-2" 
                    />
                  )}
                  <div className="text-xs text-muted-foreground">
                    Last updated: {cursor.updated_at ? formatDistanceToNow(new Date(cursor.updated_at), { addSuffix: true }) : "Never"}
                  </div>
                </CardContent>
              </Card>
            ))}
            {cursors.length === 0 && (
              <Card>
                <CardContent className="p-8 text-center text-muted-foreground">
                  No backfill cursors found. Start a backfill to see progress here.
                </CardContent>
              </Card>
            )}
          </TabsContent>

          <TabsContent value="live" className="space-y-4">
            {/* Live Status Card */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Radio className={`w-4 h-4 ${liveStatus?.status === 'running' ? 'text-green-500 animate-pulse' : 'text-muted-foreground'}`} />
                  Live Ingestion Status
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <p className="text-muted-foreground">Mode</p>
                    <Badge variant={liveStatus?.mode === 'live' ? 'default' : 'secondary'}>
                      {liveStatus?.mode || 'Unknown'}
                    </Badge>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Status</p>
                    <Badge variant={liveStatus?.status === 'running' ? 'default' : 'outline'}>
                      {liveStatus?.status || 'Unknown'}
                    </Badge>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Latest File Write</p>
                    <p className="font-mono text-xs">
                      {liveStatus?.latest_file_write 
                        ? format(new Date(liveStatus.latest_file_write), 'yyyy-MM-dd HH:mm:ss')
                        : 'N/A'}
                    </p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Current Record Time</p>
                    <p className="font-mono text-xs">
                      {liveStatus?.current_record_time?.substring(0, 19) || 'N/A'}
                    </p>
                  </div>
                </div>

                {liveStatus?.suggestion && (
                  <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg p-3 text-sm">
                    💡 {liveStatus.suggestion}
                  </div>
                )}

                {/* Live Cursor Details */}
                {liveStatus?.live_cursor && (
                  <div className="border rounded-lg p-4 space-y-2">
                    <h4 className="font-medium">Live Cursor Details</h4>
                    <div className="grid grid-cols-2 gap-2 text-sm">
                      <div>
                        <span className="text-muted-foreground">Migration ID:</span> {liveStatus.live_cursor.migration_id}
                      </div>
                      <div>
                        <span className="text-muted-foreground">Mode:</span> {liveStatus.live_cursor.mode}
                      </div>
                      <div className="col-span-2">
                        <span className="text-muted-foreground">Record Time:</span>{' '}
                        <span className="font-mono text-xs">{liveStatus.live_cursor.record_time}</span>
                      </div>
                      <div className="col-span-2">
                        <span className="text-muted-foreground">Last Updated:</span>{' '}
                        {formatDistanceToNow(new Date(liveStatus.live_cursor.updated_at), { addSuffix: true })}
                      </div>
                    </div>
                  </div>
                )}

                {/* Command Help */}
                <div className="border rounded-lg p-4 bg-muted/50">
                  <h4 className="font-medium mb-2">Commands</h4>
                  <div className="space-y-2 text-sm font-mono">
                    <div>
                      <span className="text-muted-foreground"># Resume from backfill cursor:</span>
                      <br />
                      <code>node scripts/ingest/fetch-updates-parquet.js</code>
                    </div>
                    <div>
                      <span className="text-muted-foreground"># Start live from current time:</span>
                      <br />
                      <code>node scripts/ingest/fetch-updates-parquet.js --live</code>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Backfill Cursors Summary for Live Tab */}
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">Backfill Cursor Status</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {(liveStatus?.backfill_cursors || []).map((cursor, idx) => (
                    <div key={idx} className="flex items-center justify-between text-sm">
                      <span>{cursor.file}</span>
                      <div className="flex items-center gap-2">
                        <span className="text-muted-foreground">Migration {cursor.migration_id}</span>
                        <Badge variant={cursor.complete ? "default" : "secondary"} className="text-xs">
                          {cursor.complete ? "Complete" : "In Progress"}
                        </Badge>
                      </div>
                    </div>
                  ))}
                  {(!liveStatus?.backfill_cursors || liveStatus.backfill_cursors.length === 0) && (
                    <p className="text-muted-foreground text-sm">No backfill cursors found</p>
                  )}
                </div>
                {liveStatus?.all_backfill_complete && (
                  <div className="mt-4 p-3 bg-green-500/10 border border-green-500/30 rounded-lg text-sm text-green-600 dark:text-green-400">
                    ✓ All backfill cursors are complete. Ready for live ingestion.
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </DashboardLayout>
  );
};

export default IngestionDashboard;
