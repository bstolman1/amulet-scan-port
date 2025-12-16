import { useState, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Clock, Database, FileText, Activity, CheckCircle, XCircle, Trash2, Server, Filter, Calendar, Timer } from "lucide-react";
import { formatDistanceToNow, differenceInMinutes, differenceInSeconds } from "date-fns";
import { useToast } from "@/hooks/use-toast";
import { TriggerACSSnapshotButton } from "@/components/TriggerACSSnapshotButton";
import { checkDuckDBConnection } from "@/lib/backend-config";
import { getACSSnapshots, getACSTemplates, getACSStats, apiFetch, type ACSSnapshot as LocalACSSnapshot, type ACSTemplateStats as LocalACSTemplateStats } from "@/lib/duckdb-api-client";

interface Snapshot {
  id: string;
  round?: number;
  snapshot_data?: any;
  timestamp: string;
  created_at?: string;
  migration_id: number | null;
  record_time: string | null;
  sv_url?: string | null;
  canonical_package?: string | null;
  amulet_total?: number | null;
  locked_total?: number | null;
  circulating_supply?: number | null;
  entry_count: number | null;
  template_count?: number | null;
  status: string | null;
  error_message?: string | null;
  updated_at?: string | null;
  source?: string;
  // Computed fields
  processed_pages?: number;
  processed_events?: number;
  elapsed_time_ms?: number;
  pages_per_minute?: number;
  template_batch_updates?: number;
  last_batch_info?: any;
}

interface TemplateStats {
  id: string;
  snapshot_id: string;
  template_id: string;
  contract_count: number;
  entity_name?: string;
  module_name?: string;
  created_at?: string;
  updated_at?: string;
}

// Calculate next scheduled run based on 3-hour UTC schedule (00:00, 03:00, 06:00, etc.)
function getNextScheduledRun(): Date {
  const now = new Date();
  const scheduleHours = [0, 3, 6, 9, 12, 15, 18, 21];
  
  for (const hour of scheduleHours) {
    const nextRun = new Date(now);
    nextRun.setUTCHours(hour, 0, 0, 0);
    if (nextRun > now) {
      return nextRun;
    }
  }
  
  // Next day at 00:00 UTC
  const tomorrow = new Date(now);
  tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
  tomorrow.setUTCHours(0, 0, 0, 0);
  return tomorrow;
}

function getPreviousScheduledRun(): Date {
  const now = new Date();
  const scheduleHours = [0, 3, 6, 9, 12, 15, 18, 21];
  
  // Find the most recent scheduled time
  for (let i = scheduleHours.length - 1; i >= 0; i--) {
    const prevRun = new Date(now);
    prevRun.setUTCHours(scheduleHours[i], 0, 0, 0);
    if (prevRun <= now) {
      return prevRun;
    }
  }
  
  // Yesterday's last run (21:00 UTC)
  const yesterday = new Date(now);
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  yesterday.setUTCHours(21, 0, 0, 0);
  return yesterday;
}

// Scheduler Status Component
const SchedulerStatusCard = ({ latestSnapshotTime }: { latestSnapshotTime?: string }) => {
  const [now, setNow] = useState(new Date());
  
  // Update every second to keep countdown accurate
  useEffect(() => {
    const interval = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(interval);
  }, []);
  
  const nextRun = getNextScheduledRun();
  const prevRun = getPreviousScheduledRun();
  
  const minutesToNext = differenceInMinutes(nextRun, now);
  const secondsToNext = differenceInSeconds(nextRun, now) % 60;
  
  // Check if latest snapshot is from the previous scheduled run (within 30 min window)
  const latestSnapshot = latestSnapshotTime ? new Date(latestSnapshotTime) : null;
  const wasSnapshotTaken = latestSnapshot && 
    Math.abs(differenceInMinutes(latestSnapshot, prevRun)) < 30;
  
  return (
    <Card className="glass-card border-primary/20 bg-primary/5">
      <CardContent className="py-4">
        <div className="flex items-center gap-2 mb-3">
          <Calendar className="h-5 w-5 text-primary" />
          <span className="font-semibold">ACS Snapshot Schedule</span>
          <Badge variant="outline" className="text-xs ml-2">Every 3 hours UTC</Badge>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div>
            <div className="text-sm text-muted-foreground">Next Scheduled Run</div>
            <div className="text-lg font-bold text-primary">
              {nextRun.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </div>
            <div className="text-xs text-muted-foreground">
              {nextRun.toLocaleDateString()}
            </div>
          </div>
          <div>
            <div className="text-sm text-muted-foreground">Time Until Next</div>
            <div className="text-lg font-bold flex items-center gap-1">
              <Timer className="h-4 w-4 text-primary" />
              {minutesToNext}m {secondsToNext}s
            </div>
          </div>
          <div>
            <div className="text-sm text-muted-foreground">Previous Scheduled</div>
            <div className="text-lg font-bold">
              {prevRun.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
            </div>
            <div className="text-xs text-muted-foreground">
              {prevRun.toLocaleDateString()}
            </div>
          </div>
          <div>
            <div className="text-sm text-muted-foreground">Last Snapshot Status</div>
            <div className="flex items-center gap-2">
              {wasSnapshotTaken ? (
                <>
                  <CheckCircle className="h-4 w-4 text-green-500" />
                  <span className="text-sm text-green-500">Completed</span>
                </>
              ) : latestSnapshot ? (
                <>
                  <Clock className="h-4 w-4 text-yellow-500" />
                  <span className="text-sm text-yellow-500">
                    {formatDistanceToNow(latestSnapshot, { addSuffix: true })}
                  </span>
                </>
              ) : (
                <>
                  <XCircle className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm text-muted-foreground">No data</span>
                </>
              )}
            </div>
          </div>
        </div>
        <div className="mt-3 text-xs text-muted-foreground">
          Schedule: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 UTC
          <span className="mx-2">•</span>
          Start scheduler: <code className="bg-muted px-1 rounded">npm run acs:schedule</code>
        </div>
      </CardContent>
    </Card>
  );
};

const AUTO_REFRESH_INTERVAL = 10000; // 10 seconds

const SnapshotProgress = () => {
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [templateStats, setTemplateStats] = useState<Record<string, TemplateStats[]>>({});
  const [loading, setLoading] = useState(true);
  const [isPurging, setIsPurging] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [localStats, setLocalStats] = useState<{ total_contracts: number; total_templates: number } | null>(null);
  const [selectedMigration, setSelectedMigration] = useState<string>('all');
  const [prevContractCount, setPrevContractCount] = useState<number | null>(null);
  const [isCountIncreasing, setIsCountIncreasing] = useState(false);
  const { toast } = useToast();

  // Data is "still writing" only if the count is actively increasing between refreshes
  const isStillWriting = isCountIncreasing;

  // Get unique migrations from snapshots
  const uniqueMigrations = [...new Set(snapshots.map(s => s.migration_id))]
    .filter(m => m !== null && m !== undefined)
    .sort((a, b) => (b ?? 0) - (a ?? 0));

  // Filter snapshots by selected migration
  const filteredSnapshots = selectedMigration === 'all' 
    ? snapshots 
    : snapshots.filter(s => String(s.migration_id) === selectedMigration);

  useEffect(() => {
    const init = async () => {
      const connected = await checkDuckDBConnection();
      setIsConnected(connected);
      if (connected) {
        fetchLocalSnapshots();
      } else {
        setLoading(false);
        toast({
          title: "DuckDB not connected",
          description: "Make sure your local DuckDB server is running",
          variant: "destructive",
        });
      }
    };
    
    init();
  }, []);

  // Auto-refresh to detect if data is still being written
  useEffect(() => {
    if (!isConnected) return;

    const interval = setInterval(() => {
      fetchLocalSnapshots();
    }, AUTO_REFRESH_INTERVAL);

    return () => clearInterval(interval);
  }, [isConnected]);

  const fetchLocalSnapshots = async () => {
    try {
      const [snapshotsResponse, templatesResponse, statsResponse] = await Promise.all([
        getACSSnapshots(),
        getACSTemplates(100),
        getACSStats(),
      ]);

      const currentCount = statsResponse.data.total_contracts;
      
      // Detect if data is still being written by comparing to previous count
      if (prevContractCount !== null && currentCount > prevContractCount) {
        // Count increased - data is still being written
        setIsCountIncreasing(true);
      } else {
        // Count hasn't changed - data is stable
        setIsCountIncreasing(false);
      }
      setPrevContractCount(currentCount);

      // Transform local snapshots to match UI format
      const transformedSnapshots: Snapshot[] = snapshotsResponse.data.map((s: LocalACSSnapshot) => ({
        id: s.id,
        timestamp: s.timestamp,
        migration_id: s.migration_id,
        record_time: s.record_time,
        entry_count: s.entry_count,
        template_count: s.template_count,
        status: s.status,
        source: s.source,
      }));

      setSnapshots(transformedSnapshots);
      setLocalStats({
        total_contracts: statsResponse.data.total_contracts,
        total_templates: statsResponse.data.total_templates,
      });

      // Set template stats for the first snapshot
      if (transformedSnapshots.length > 0) {
        const templates: TemplateStats[] = templatesResponse.data.map((t: LocalACSTemplateStats, idx: number) => ({
          id: `local-template-${idx}`,
          snapshot_id: transformedSnapshots[0].id,
          template_id: t.template_id,
          contract_count: t.contract_count,
          entity_name: t.entity_name,
          module_name: t.module_name,
        }));
        setTemplateStats({ [transformedSnapshots[0].id]: templates });
      }
    } catch (error) {
      console.error("Error fetching local snapshots:", error);
      toast({
        title: "Error loading local ACS data",
        description: "Make sure your DuckDB server is running",
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  };

  const getStatusBadge = (status: string | null) => {
    switch (status) {
      case "completed":
        return (
          <Badge className="bg-green-500/10 text-green-500">
            <CheckCircle className="w-3 h-3 mr-1" />
            Completed
          </Badge>
        );
      case "processing":
        return (
          <Badge className="bg-blue-500/10 text-blue-500">
            <Activity className="w-3 h-3 mr-1 animate-spin" />
            Processing
          </Badge>
        );
      case "failed":
        return (
          <Badge className="bg-red-500/10 text-red-500">
            <XCircle className="w-3 h-3 mr-1" />
            Failed
          </Badge>
        );
      default:
        return <Badge>{status || "Unknown"}</Badge>;
    }
  };

  const formatDuration = (ms: number) => {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
      return `${hours}h ${minutes % 60}m`;
    }
    if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    }
    return `${seconds}s`;
  };

  const handlePurgeAll = async () => {
    if (
      !confirm(
        "Are you sure you want to purge ALL ACS data? This will delete all local ACS parquet files. This action cannot be undone.",
      )
    ) {
      return;
    }

    setIsPurging(true);
    try {
      const response = await apiFetch('/api/acs/purge', { method: 'POST' }) as { success?: boolean; error?: string; message?: string };
      
      if (!response.success) throw new Error(response.error || 'Purge failed');

      toast({
        title: "Purge complete",
        description: response.message || "ACS data purged successfully",
      });

      // Refresh the snapshots list
      fetchLocalSnapshots();
    } catch (error: any) {
      console.error("Purge error:", error);
      toast({ title: "Purge failed", description: error.message || "Unknown error", variant: "destructive" });
    } finally {
      setIsPurging(false);
    }
  };

  const refreshLocalData = async () => {
    setLoading(true);
    await fetchLocalSnapshots();
  };

  if (loading) {
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
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold mb-2">ACS Snapshot</h1>
            <p className="text-muted-foreground">
              <span className="flex items-center gap-2">
                <Server className={`w-4 h-4 ${isConnected ? 'text-green-500' : 'text-red-500'}`} />
                {isConnected ? 'Using local DuckDB data' : 'DuckDB not connected'}
              </span>
            </p>
          </div>
          <div className="flex gap-2">
            <Button onClick={refreshLocalData} variant="outline" size="sm" disabled={!isConnected}>
              <Activity className="h-4 w-4 mr-2" />
              Refresh
            </Button>
            <TriggerACSSnapshotButton />
            <Button onClick={handlePurgeAll} disabled={isPurging || !isConnected} variant="destructive" size="sm">
              <Trash2 className={`h-4 w-4 mr-2 ${isPurging ? "animate-spin" : ""}`} />
              {isPurging ? "Purging..." : "Purge All ACS Data"}
            </Button>
          </div>
        </div>

        {/* Stats Summary */}
        {localStats && (
          <Card className="glass-card border-green-500/20 bg-green-500/5">
            <CardContent className="py-4">
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <div className="text-sm text-muted-foreground">Total Contracts</div>
                  <div className="text-2xl font-bold text-green-500">{localStats.total_contracts.toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-sm text-muted-foreground">Unique Templates</div>
                  <div className="text-2xl font-bold text-green-500">{localStats.total_templates.toLocaleString()}</div>
                </div>
                <div>
                  <div className="text-sm text-muted-foreground">Snapshots</div>
                  <div className="text-2xl font-bold text-green-500">{snapshots.length}</div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Scheduler Status Card */}
        <SchedulerStatusCard latestSnapshotTime={snapshots[0]?.timestamp} />

        {/* Migration Filter */}
        {uniqueMigrations.length > 0 && (
          <div className="flex items-center gap-3">
            <Filter className="h-4 w-4 text-muted-foreground" />
            <span className="text-sm text-muted-foreground">Migration:</span>
            <Select value={selectedMigration} onValueChange={setSelectedMigration}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="Select migration" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Migrations ({snapshots.length})</SelectItem>
                {uniqueMigrations.map(m => (
                  <SelectItem key={m} value={String(m)}>
                    Migration #{m} ({snapshots.filter(s => s.migration_id === m).length})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        {filteredSnapshots.map((snapshot, index) => {
          // For the first (latest) snapshot, use fresh localStats data
          const isLatestSnapshot = index === 0;
          
          return (
          <Card key={snapshot.id} className="glass-card">
            <CardHeader>
              <div className="flex items-center justify-between">
                <div className="space-y-1">
                  <CardTitle className="flex items-center gap-2">
                    <Database className="w-5 h-5" />
                    {snapshot.migration_id !== null && snapshot.migration_id !== undefined 
                      ? `Migration #${snapshot.migration_id}`
                      : 'Unknown Migration'}
                    {snapshot.source === 'local' && (
                      <Badge variant="outline" className="text-green-500 border-green-500/50">
                        <Server className="w-3 h-3 mr-1" />
                        Local
                      </Badge>
                    )}
                  </CardTitle>
                  <CardDescription>
                    {snapshot.record_time 
                      ? `Record time: ${new Date(snapshot.record_time).toLocaleString()}`
                      : `Created ${formatDistanceToNow(new Date(snapshot.timestamp), { addSuffix: true })}`
                    }
                  </CardDescription>
                </div>
                {getStatusBadge(snapshot.status)}
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* Progress Bar */}
              <div className="space-y-2">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Overall Progress</span>
                  <span className="font-medium">
                    {isStillWriting ? (
                      <span className="text-blue-500">Writing data...</span>
                    ) : prevContractCount === null ? (
                      <span className="text-muted-foreground">Checking...</span>
                    ) : (
                      <span className="text-green-500">100%</span>
                    )}
                  </span>
                </div>
                <Progress 
                  value={isStillWriting ? 75 : (prevContractCount === null ? 0 : 100)} 
                  className={`h-2 ${isStillWriting ? '[&>div]:animate-pulse' : ''}`}
                />
                <div className="flex items-center justify-between text-xs text-muted-foreground">
                  <span>{(isLatestSnapshot ? localStats?.total_contracts : snapshot.entry_count)?.toLocaleString() || 0} contracts indexed</span>
                  {isStillWriting && (
                    <span className="text-blue-500">Data still being written...</span>
                  )}
                </div>
              </div>

              {/* Stats Grid */}
              <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                <div className="space-y-1">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Database className="w-4 h-4" />
                    Contracts
                  </div>
                  <p className="text-2xl font-bold">
                    {(isLatestSnapshot ? localStats?.total_contracts : snapshot.entry_count)?.toLocaleString() || 0}
                  </p>
                </div>

                <div className="space-y-1">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <FileText className="w-4 h-4" />
                    Templates
                  </div>
                  <p className="text-2xl font-bold">
                    {(isLatestSnapshot ? localStats?.total_templates : snapshot.template_count)?.toLocaleString() || 0}
                  </p>
                </div>

                <div className="space-y-1">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Clock className="w-4 h-4" />
                    Snapshot Time
                  </div>
                  <p className="text-lg font-medium">
                    {new Date(snapshot.timestamp).toLocaleDateString()}
                  </p>
                </div>
              </div>


              {/* Template Stats */}
              {templateStats[snapshot.id] && templateStats[snapshot.id].length > 0 && (
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">Top Templates</h4>
                  <div className="max-h-64 overflow-y-auto space-y-2">
                    {templateStats[snapshot.id].slice(0, 20).map((stat, idx) => {
                      const isRecent = stat.updated_at && new Date(stat.updated_at).getTime() > Date.now() - 5 * 60 * 1000;
                      return (
                        <div
                          key={stat.id || `stat-${idx}`}
                          className={`flex items-center justify-between p-2 rounded-lg text-sm ${
                            isRecent ? "bg-green-500/10 border border-green-500/20" : "bg-muted/50"
                          }`}
                        >
                          <div className="flex flex-col flex-1 min-w-0">
                            {stat.entity_name && (
                              <span className="font-medium text-foreground">{stat.entity_name}</span>
                            )}
                            <span className="font-mono text-xs truncate text-muted-foreground">
                              {stat.module_name ? `${stat.module_name}:${stat.entity_name}` : stat.template_id}
                            </span>
                          </div>
                          <div className="flex items-center gap-2">
                            <Badge variant="secondary">{stat.contract_count.toLocaleString()} contracts</Badge>
                            {stat.updated_at && (
                              <span className="text-xs text-muted-foreground">
                                {formatDistanceToNow(new Date(stat.updated_at), { addSuffix: true })}
                              </span>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Error Message */}
              {snapshot.error_message && (
                <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/20">
                  <p className="text-sm text-red-500">{snapshot.error_message}</p>
                </div>
              )}

              {/* Completion Info */}
              {snapshot.status === 'completed' && snapshot.updated_at && (
                <div className="text-sm text-muted-foreground">
                  Completed {formatDistanceToNow(new Date(snapshot.updated_at), { addSuffix: true })}
                </div>
              )}
            </CardContent>
          </Card>
          );
        })}

        {filteredSnapshots.length === 0 && (
          <Card className="glass-card">
            <CardContent className="flex flex-col items-center justify-center py-12">
              <Database className="w-12 h-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground">
                {selectedMigration !== 'all' 
                  ? `No snapshots found for Migration #${selectedMigration}`
                  : "No local ACS data found. Run fetch-acs-parquet.js to populate data."
                }
              </p>
            </CardContent>
          </Card>
        )}
      </div>
    </DashboardLayout>
  );
};

export default SnapshotProgress;
