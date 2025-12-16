import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { formatDistanceToNow } from "date-fns";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import {
  Activity,
  AlertCircle,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Circle,
  Clock,
  Copy,
  Database,
  FileText,
  Radio,
  RefreshCw,
  Search,
  Timer,
  Trash2,
  Wifi,
  WifiOff,
  Zap,
} from "lucide-react";
import { useLedgerUpdates, type LedgerUpdate } from "@/hooks/use-ledger-updates";
import { useLatestUpdates } from "@/hooks/use-latest-updates";
import { useDuckDBHealth } from "@/hooks/use-duckdb-events";
import { useDuckDBForLedger } from "@/lib/backend-config";
import { getLiveStatus, purgeLiveCursor, type LiveStatus } from "@/lib/duckdb-api-client";
import { useToast } from "@/hooks/use-toast";

// JSON Viewer Component with copy functionality
const JsonViewer = ({ data, label }: { data: any; label: string }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const { toast } = useToast();
  
  const jsonString = useMemo(() => JSON.stringify(data, null, 2), [data]);
  
  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(jsonString);
      setCopied(true);
      toast({ title: "Copied to clipboard" });
      setTimeout(() => setCopied(false), 2000);
    } catch {
      toast({ title: "Failed to copy", variant: "destructive" });
    }
  };
  
  if (!data || (typeof data === 'object' && Object.keys(data).length === 0)) {
    return null;
  }
  
  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen}>
      <CollapsibleTrigger asChild>
        <Button variant="ghost" size="sm" className="h-6 px-2 text-xs gap-1">
          {isOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
          {label}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <div className="relative mt-2">
          <Button 
            variant="ghost" 
            size="sm" 
            className="absolute top-2 right-2 h-6 px-2"
            onClick={handleCopy}
          >
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          </Button>
          <pre className="text-xs bg-muted/50 p-3 rounded-md overflow-x-auto max-h-96 overflow-y-auto font-mono">
            {jsonString}
          </pre>
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
};

const LiveUpdates = () => {
  const { data: events = [], isLoading, dataUpdatedAt, refetch } = useLedgerUpdates(100);
  const { data: rawUpdates = [], isLoading: isLoadingUpdates, refetch: refetchUpdates } = useLatestUpdates(100);
  const { data: isDuckDBAvailable } = useDuckDBHealth();
  const usingDuckDB = useDuckDBForLedger();
  const [searchTerm, setSearchTerm] = useState("");
  const [secondsSinceRefresh, setSecondsSinceRefresh] = useState(0);
  const { toast } = useToast();

  // Fetch live status from backend
  const { data: liveStatus, refetch: refetchLiveStatus } = useQuery<LiveStatus>({
    queryKey: ["liveStatus"],
    queryFn: getLiveStatus,
    refetchInterval: 10000,
    retry: false,
  });

  // Merge events and updates into unified timeline
  const unifiedTransactions = useMemo(() => {
    const eventItems = (events as any[]).map((e) => ({
      ...e,
      _source: "event" as const,
      _sortTime: new Date(e.effective_at || e.timestamp || e.record_time || 0).getTime(),
    }));
    
    const updateItems = (rawUpdates as any[]).map((u) => ({
      ...u,
      _source: "update" as const,
      _sortTime: new Date(u.record_time || u.timestamp || 0).getTime(),
    }));

    return [...eventItems, ...updateItems].sort((a, b) => b._sortTime - a._sortTime);
  }, [events, rawUpdates]);

  // Freshness indicators
  const freshness = useMemo(() => {
    const latestEventTime = events.length > 0
      ? Math.max(...(events as any[]).map((e) => new Date(e.effective_at || e.timestamp || 0).getTime()))
      : null;
    const latestUpdateTime = rawUpdates.length > 0
      ? Math.max(...(rawUpdates as any[]).map((u) => new Date(u.record_time || u.timestamp || 0).getTime()))
      : null;

    const now = Date.now();
    const fiveMinutes = 5 * 60 * 1000;

    return {
      events: {
        count: events.length,
        latestTime: latestEventTime ? new Date(latestEventTime) : null,
        isStale: latestEventTime ? (now - latestEventTime) > fiveMinutes : true,
      },
      updates: {
        count: rawUpdates.length,
        latestTime: latestUpdateTime ? new Date(latestUpdateTime) : null,
        isStale: latestUpdateTime ? (now - latestUpdateTime) > fiveMinutes : true,
      },
    };
  }, [events, rawUpdates]);

  // Update seconds since last refresh
  useEffect(() => {
    const interval = setInterval(() => {
      if (dataUpdatedAt) {
        setSecondsSinceRefresh(Math.floor((Date.now() - dataUpdatedAt) / 1000));
      }
    }, 1000);
    return () => clearInterval(interval);
  }, [dataUpdatedAt]);

  // Manual refresh handler
  const handleRefresh = () => {
    refetch();
    refetchUpdates();
    toast({ title: "Refreshing data..." });
  };

  // Purge live cursor handler
  const handlePurgeLiveCursor = async () => {
    try {
      const result = await purgeLiveCursor();
      toast({
        title: result.success ? "Live cursor purged" : "No cursor to purge",
        description: result.message,
      });
      refetchLiveStatus();
    } catch (err: any) {
      toast({
        title: "Failed to purge",
        description: err.message,
        variant: "destructive",
      });
    }
  };

  const filteredTransactions = unifiedTransactions.filter((item: any) => {
    const id = item._source === "update" ? (item.update_id || "") : item.id;
    const type = item.update_type || "";
    return (
      !searchTerm ||
      String(id).toLowerCase().includes(searchTerm.toLowerCase()) ||
      String(type).toLowerCase().includes(searchTerm.toLowerCase())
    );
  });

  // Calculate statistics
  const stats = useMemo(() => {
    const updatesByType = unifiedTransactions.reduce((acc, u) => {
      const type = u.update_type || "unknown";
      acc[type] = (acc[type] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    const templateCounts = (events as any[]).reduce((acc, u) => {
      const data = u.update_data as any;
      const evts = data?.events || data?.transaction?.events || [];
      evts.forEach((event: any) => {
        const templateId = event?.template_id || event?.templateId;
        if (templateId) {
          const shortName = templateId.split(":").pop() || templateId;
          acc[shortName] = (acc[shortName] || 0) + 1;
        }
      });
      return acc;
    }, {} as Record<string, number>);

    const totalEvents = (events as any[]).reduce((sum, u) => {
      const data = u.update_data as any;
      const evts = data?.events || data?.transaction?.events || [];
      return sum + (Array.isArray(evts) ? evts.length : 0);
    }, 0);

    const latestUpdate = unifiedTransactions.length > 0
      ? new Date(Math.max(...unifiedTransactions.map((u) => u._sortTime)))
      : null;

    const topTemplates = (Object.entries(templateCounts) as Array<[string, number]>)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 5);

    return {
      updatesByType,
      topTemplates,
      totalEvents,
      latestUpdate,
      totalTypes: Object.keys(updatesByType).length,
    };
  }, [events, unifiedTransactions]);

  // Calculate type distribution for visual breakdown
  const typeDistribution = useMemo(() => {
    const total = unifiedTransactions.length || 1;
    return (Object.entries(stats.updatesByType) as Array<[string, number]>).map(([type, count]) => ({
      type,
      count,
      percentage: Math.round((count / total) * 100),
    }));
  }, [unifiedTransactions.length, stats.updatesByType]);

  // Track whether data is advancing (helps validate live ingestion in UI)
  const lastSeenDataTimeRef = useRef<number | null>(null);
  const [dataAdvancing, setDataAdvancing] = useState<null | {
    status: "advancing" | "stalled";
    lastSeenIso: string;
    checkedAtIso: string;
  }>(null);

  useEffect(() => {
    if (!stats.latestUpdate) return;
    const latest = stats.latestUpdate.getTime();
    const prev = lastSeenDataTimeRef.current;
    lastSeenDataTimeRef.current = latest;

    setDataAdvancing({
      status: prev !== null && latest > prev ? "advancing" : "stalled",
      lastSeenIso: stats.latestUpdate.toISOString(),
      checkedAtIso: new Date().toISOString(),
    });
  }, [stats.latestUpdate?.getTime()]);

  if (isLoading || isLoadingUpdates) {
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
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div>
            <h1 className="text-3xl font-bold mb-2">Transaction History</h1>
            <p className="text-muted-foreground">Unified events & updates from {usingDuckDB ? "DuckDB API" : "Supabase"}</p>
          </div>
          {/* Freshness Indicators */}
          <div className="flex items-center gap-3">
            <div className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border ${freshness.events.isStale ? 'border-amber-500/50 bg-amber-500/10' : 'border-emerald-500/50 bg-emerald-500/10'}`}>
              <Zap className={`w-4 h-4 ${freshness.events.isStale ? 'text-amber-500' : 'text-emerald-500'}`} />
              <div className="text-xs">
                <div className="font-medium">Events ({freshness.events.count})</div>
                <div className="text-muted-foreground">
                  {freshness.events.latestTime ? formatDistanceToNow(freshness.events.latestTime, { addSuffix: true }) : 'No data'}
                </div>
              </div>
            </div>
            <div className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border ${freshness.updates.isStale ? 'border-amber-500/50 bg-amber-500/10' : 'border-emerald-500/50 bg-emerald-500/10'}`}>
              <Activity className={`w-4 h-4 ${freshness.updates.isStale ? 'text-amber-500' : 'text-emerald-500'}`} />
              <div className="text-xs">
                <div className="font-medium">Updates ({freshness.updates.count})</div>
                <div className="text-muted-foreground">
                  {freshness.updates.latestTime ? formatDistanceToNow(freshness.updates.latestTime, { addSuffix: true }) : 'No data'}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Backend Status & Refresh Indicator */}
        <div className="flex items-center justify-between flex-wrap gap-2">
          <div className="flex items-center gap-2">
            <Badge variant={usingDuckDB ? "default" : "secondary"} className="flex items-center gap-1">
              <Database className="w-3 h-3" />
              {usingDuckDB ? "DuckDB" : "Supabase"}
            </Badge>
            {usingDuckDB && (
              <Badge variant={isDuckDBAvailable ? "outline" : "destructive"} className="flex items-center gap-1">
                {isDuckDBAvailable ? <Wifi className="w-3 h-3" /> : <WifiOff className="w-3 h-3" />}
                {isDuckDBAvailable ? "Connected" : "Disconnected"}
              </Badge>
            )}
            {/* Live/Backfill Mode Indicator */}
            {liveStatus && (
              <Badge 
                variant={liveStatus.mode === 'live' ? "default" : "secondary"} 
                className={`flex items-center gap-1 ${liveStatus.mode === 'live' && liveStatus.status === 'running' ? 'animate-pulse' : ''}`}
              >
                {liveStatus.mode === 'live' ? (
                  <Radio className="w-3 h-3" />
                ) : (
                  <Circle className="w-3 h-3" />
                )}
                {liveStatus.mode === 'live' ? 'Live' : liveStatus.mode === 'backfill' ? 'Backfill' : 'Unknown'}
                {liveStatus.status === 'running' && ' (Active)'}
              </Badge>
            )}
          </div>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Button variant="outline" size="sm" onClick={handleRefresh} className="h-7 px-2">
              <RefreshCw className={`w-4 h-4 mr-1 ${secondsSinceRefresh < 3 ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
            {liveStatus?.live_cursor && (
              <Button 
                variant="outline" 
                size="sm" 
                onClick={handlePurgeLiveCursor} 
                className="h-7 px-2 text-destructive border-destructive/50 hover:bg-destructive/10"
                title="Delete live cursor to stop tracking. Script will need restart."
              >
                <Trash2 className="w-4 h-4 mr-1" />
                Purge Live Cursor
              </Button>
            )}
            <span>Updated {secondsSinceRefresh}s ago</span>
            {/* Show actual file timestamp if available */}
            {liveStatus?.latest_file_write && (
              <div className="flex items-center gap-1" title="Last file written to disk">
                <FileText className="w-4 h-4" />
                <span>File: {formatDistanceToNow(new Date(liveStatus.latest_file_write), { addSuffix: true })}</span>
              </div>
            )}
            {/* Show record time from cursor */}
            {liveStatus?.current_record_time && (
              <div className="flex items-center gap-1" title="Latest record time from ledger">
                <Timer className="w-4 h-4" />
                <span>Ledger: {formatDistanceToNow(new Date(liveStatus.current_record_time), { addSuffix: true })}</span>
              </div>
            )}
            {/* Fallback to calculated latest */}
            {!liveStatus?.current_record_time && stats.latestUpdate && (
              <div className="flex items-center gap-1" title="Effective time of latest record in dataset">
                <Timer className="w-4 h-4" />
                <span>Data from: {formatDistanceToNow(stats.latestUpdate, { addSuffix: true })}</span>
              </div>
            )}
          </div>
        </div>

        {/* Validation Pipeline */}
        {liveStatus && (
          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm flex items-center gap-2">
                <CheckCircle2 className="w-4 h-4" />
                Validation Pipeline
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              {(() => {
                const backfills = liveStatus.backfill_cursors ?? [];

                const latestBackfill = [...backfills].sort((a, b) => {
                  if (a.migration_id !== b.migration_id) return b.migration_id - a.migration_id;
                  const at = a.max_time ? new Date(a.max_time).getTime() : 0;
                  const bt = b.max_time ? new Date(b.max_time).getTime() : 0;
                  return bt - at;
                })[0];

                // Specifically show migration 4 checkpoint if it exists
                const m4Backfill = [...backfills]
                  .filter((c) => c.migration_id === 4)
                  .sort((a, b) => {
                    const at = a.max_time ? new Date(a.max_time).getTime() : 0;
                    const bt = b.max_time ? new Date(b.max_time).getTime() : 0;
                    return bt - at;
                  })[0];

                const m4Time = m4Backfill?.max_time ? new Date(m4Backfill.max_time).getTime() : null;

                const liveCursor = liveStatus.live_cursor;
                const liveCursorTime = liveCursor?.record_time ? new Date(liveCursor.record_time).getTime() : null;
                const liveBuildsOnM4 =
                  m4Time && liveCursorTime
                    ? liveCursor.migration_id > 4 || (liveCursor.migration_id === 4 && liveCursorTime >= m4Time)
                    : null;

                const latestDataTime = stats.latestUpdate ? stats.latestUpdate.getTime() : null;
                const displayedBuildsOnM4 = m4Time && latestDataTime ? latestDataTime >= m4Time : null;

                const Step = ({
                  ok,
                  title,
                  subtitle,
                }: {
                  ok: boolean | null;
                  title: string;
                  subtitle: string;
                }) => (
                  <div className="flex items-start justify-between gap-3 rounded-md border border-border bg-muted/30 p-3">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        {ok === null ? (
                          <Circle className="w-4 h-4 text-muted-foreground" />
                        ) : ok ? (
                          <CheckCircle2 className="w-4 h-4 text-emerald-500" />
                        ) : (
                          <AlertCircle className="w-4 h-4 text-amber-500" />
                        )}
                        <span className="font-medium">{title}</span>
                      </div>
                      <p className="text-xs text-muted-foreground mt-1 break-words">{subtitle}</p>
                    </div>
                  </div>
                );

                const m4Subtitle = m4Backfill
                  ? `m4 @ ${m4Backfill.max_time?.substring(0, 19) ?? "(no max_time)"}`
                  : "No migration 4 backfill checkpoint reported by local API";

                const latestBackfillSubtitle = latestBackfill
                  ? `m${latestBackfill.migration_id} @ ${latestBackfill.max_time?.substring(0, 19) ?? "(no max_time)"}`
                  : "No backfill cursor reported by local API";

                const liveSubtitle = liveCursor
                  ? `m${liveCursor.migration_id} @ ${liveCursor.record_time?.substring(0, 19) ?? "(no record_time)"}`
                  : "No live cursor detected";

                const advancingSubtitle = dataAdvancing
                  ? `${dataAdvancing.lastSeenIso} (${dataAdvancing.status}, checked ${dataAdvancing.checkedAtIso.substring(11, 19)})`
                  : "No latest data timestamp yet";

                return (
                  <div className="space-y-2">
                    <Step ok={m4Backfill ? true : null} title="Migration 4 backfill checkpoint" subtitle={m4Subtitle} />
                    <Step ok={latestBackfill ? true : null} title="Latest backfill checkpoint (any migration)" subtitle={latestBackfillSubtitle} />
                    <Step ok={liveBuildsOnM4} title="Live cursor builds on migration 4" subtitle={liveSubtitle} />
                    <Step ok={displayedBuildsOnM4} title="Displayed data is at/after migration 4" subtitle={advancingSubtitle} />
                  </div>
                );
              })()}
            </CardContent>
          </Card>
        )}

        {/* Suggestion Banner */}
        {liveStatus?.suggestion && (
          <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg p-3 text-sm text-amber-600 dark:text-amber-400">
            💡 {liveStatus.suggestion}
          </div>
        )}

        {/* Backfill→Live Gap Card */}
        {liveStatus && (
          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Clock className="w-4 h-4" />
                Backfill → Live Gap
              </CardTitle>
            </CardHeader>
            <CardContent>
              {(() => {
                const backfills = liveStatus.backfill_cursors ?? [];
                const m4Backfill = [...backfills]
                  .filter((c) => c.migration_id === 4)
                  .sort((a, b) => {
                    const at = a.max_time ? new Date(a.max_time).getTime() : 0;
                    const bt = b.max_time ? new Date(b.max_time).getTime() : 0;
                    return bt - at;
                  })[0];

                const m4Time = m4Backfill?.max_time ? new Date(m4Backfill.max_time).getTime() : null;
                const liveTime = stats.latestUpdate ? stats.latestUpdate.getTime() : null;

                if (!m4Time) {
                  return <p className="text-muted-foreground text-sm">No migration 4 checkpoint</p>;
                }
                if (!liveTime) {
                  return <p className="text-muted-foreground text-sm">No live data yet</p>;
                }

                const gapMs = liveTime - m4Time;
                const absGap = Math.abs(gapMs);

                // Format gap nicely
                const formatGap = (ms: number) => {
                  if (ms < 1000) return `${ms}ms`;
                  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
                  if (ms < 3600000) return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
                  return `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
                };

                const isCaughtUp = gapMs >= 0;
                const isClose = absGap < 60000; // within 1 minute

                return (
                  <div className="space-y-2">
                    <div className="flex items-center gap-2">
                      <span
                        className={`text-2xl font-bold ${
                          isCaughtUp
                            ? "text-emerald-500"
                            : isClose
                            ? "text-amber-500"
                            : "text-destructive"
                        }`}
                      >
                        {isCaughtUp ? "+" : "-"}{formatGap(absGap)}
                      </span>
                      <Badge
                        variant={isCaughtUp ? "default" : isClose ? "secondary" : "destructive"}
                        className="text-xs"
                      >
                        {isCaughtUp ? "Caught up" : isClose ? "Slightly behind" : "Behind"}
                      </Badge>
                    </div>
                    <div className="text-xs text-muted-foreground space-y-1">
                      <div className="flex justify-between">
                        <span>M4 checkpoint:</span>
                        <span className="font-mono">{m4Backfill.max_time?.substring(11, 23)}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Live data:</span>
                        <span className="font-mono">{stats.latestUpdate?.toISOString().substring(11, 23)}</span>
                      </div>
                    </div>
                  </div>
                );
              })()}
            </CardContent>
          </Card>
        )}

        {/* Stats Grid - Row 1 */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Total Items
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-primary">{unifiedTransactions.length}</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Zap className="w-4 h-4" />
                Total Events
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-emerald-500">{stats.totalEvents}</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Database className="w-4 h-4" />
                Update Types
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-amber-500">{stats.totalTypes}</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <FileText className="w-4 h-4" />
                Active Templates
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-violet-500">{stats.topTemplates.length}</p>
            </CardContent>
          </Card>
        </div>

        {/* Row 2 - Type Breakdown & Top Templates */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Update Type Breakdown */}
          <Card className="glass-card">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium">Update Type Distribution</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {typeDistribution.length > 0 ? (
                typeDistribution.map(({ type, count, percentage }) => (
                  <div key={type} className="space-y-1">
                    <div className="flex justify-between text-sm">
                      <span className="font-medium capitalize">{type.replace(/_/g, ' ')}</span>
                      <span className="text-muted-foreground">{count} ({percentage}%)</span>
                    </div>
                    <Progress value={percentage} className="h-2" />
                  </div>
                ))
              ) : (
                <p className="text-muted-foreground text-sm">No updates to analyze</p>
              )}
            </CardContent>
          </Card>

          {/* Top Active Templates */}
          <Card className="glass-card">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium">Top Active Templates</CardTitle>
            </CardHeader>
            <CardContent>
              {stats.topTemplates.length > 0 ? (
                <div className="space-y-2">
                  {stats.topTemplates.map(([template, count], idx) => (
                    <div key={template} className="flex items-center justify-between p-2 rounded-lg bg-muted/50">
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-bold text-muted-foreground w-5">#{idx + 1}</span>
                        <span className="font-mono text-sm truncate max-w-[200px]">{template}</span>
                      </div>
                      <Badge variant="secondary">{count}</Badge>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-muted-foreground text-sm">No template activity detected</p>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Updates List */}
        <Card className="glass-card">
          <CardHeader>
            <div className="flex flex-col md:flex-row gap-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground w-4 h-4" />
                <Input
                  placeholder="Search by update ID or type..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 max-h-[600px] overflow-y-auto">
              {filteredTransactions.map((item: any, idx: number) => {
                const isUpdate = item._source === "update";
                const rowId: string = isUpdate ? String(item.update_id || "") : String(item.id || "");
                const rowType: string = String(item.update_type || "unknown");

                const data = item.update_data as any;

                const contractIdRaw =
                  item.contract_id ??
                  data?.contract_id ??
                  data?.created_event?.contract_id ??
                  null;
                const templateIdRaw =
                  item.template_id ??
                  data?.template_id ??
                  data?.created_event?.template_id ??
                  null;

                const contractId = typeof contractIdRaw === "string" ? contractIdRaw : null;
                const templateId = typeof templateIdRaw === "string" ? templateIdRaw : null;
                const templateShort = templateId ? templateId.split(":").pop() : null;
                const signatories = data?.signatories || data?.created_event?.signatories || [];
                const payload = data?.payload || data?.create_arguments || null;

                const effectiveAtIso = item.effective_at || item.record_time || item.timestamp;
                const createdAtIso = item.created_at || item.timestamp || item.record_time || item.effective_at;

                return (
                  <div
                    key={`${item._source}-${rowId}-${idx}`}
                    className="p-3 rounded-lg border bg-card hover:bg-muted/50 transition-colors"
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="space-y-2 flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          {/* Source badge */}
                          <Badge variant={isUpdate ? "secondary" : "default"} className="text-xs">
                            {isUpdate ? "Update" : "Event"}
                          </Badge>
                          <Badge variant="outline">{rowType}</Badge>
                          <span className="font-mono text-xs text-muted-foreground">
                            Migration {item.migration_id ?? liveStatus?.live_cursor?.migration_id ?? "N/A"}
                          </span>
                          <span
                            className="font-mono text-xs text-muted-foreground truncate max-w-[200px]"
                            title={rowId}
                          >
                            {rowId ? `${rowId.substring(0, 30)}...` : "(missing id)"}
                          </span>
                        </div>

                        {(contractId || templateShort) && (
                          <div className="flex items-center gap-3 flex-wrap text-xs">
                            {contractId && (
                              <span className="font-mono text-blue-500" title={contractId}>
                                {contractId.substring(0, 24)}...
                              </span>
                            )}
                            {templateShort && (
                              <Badge variant="secondary" className="font-mono text-xs">
                                {templateShort}
                              </Badge>
                            )}
                          </div>
                        )}

                        {!isUpdate && signatories.length > 0 && (
                          <div className="flex items-center gap-1 flex-wrap text-xs text-muted-foreground">
                            {signatories.slice(0, 2).map((s: string, i: number) => (
                              <span key={i} className="font-mono truncate max-w-[150px]" title={s}>
                                {s.substring(0, 20)}...
                              </span>
                            ))}
                            {signatories.length > 2 && <span>+{signatories.length - 2} more</span>}
                          </div>
                        )}

                        <div className="text-xs text-muted-foreground">
                          <Clock className="w-3 h-3 inline mr-1" />
                          {effectiveAtIso ? new Date(effectiveAtIso).toLocaleString() : "(no time)"}
                        </div>

                        <div className="flex flex-wrap gap-2 pt-1">
                          {!isUpdate && payload && <JsonViewer data={payload} label="View Payload" />}
                          <JsonViewer data={data} label="View Data" />
                        </div>
                      </div>

                      <div className="text-xs text-muted-foreground whitespace-nowrap">
                        {createdAtIso ? formatDistanceToNow(new Date(createdAtIso), { addSuffix: true }) : ""}
                      </div>
                    </div>
                  </div>
                );
              })}

              {filteredTransactions.length === 0 && (
                <div className="text-center py-12 text-muted-foreground">
                  {searchTerm ? "No matching transactions found." : "No data yet. Start the DuckDB API server to see data."}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default LiveUpdates;