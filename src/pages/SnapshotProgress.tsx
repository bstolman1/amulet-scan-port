import { useState, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { supabase } from "@/integrations/supabase/client";
import { Clock, Database, FileText, Activity, CheckCircle, XCircle, Trash2, Server, Filter } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { useToast } from "@/hooks/use-toast";
import { TriggerACSSnapshotButton } from "@/components/TriggerACSSnapshotButton";
import { useDuckDBForLedger, checkDuckDBConnection } from "@/lib/backend-config";
import { getACSSnapshots, getACSTemplates, getACSStats, type ACSSnapshot as LocalACSSnapshot, type ACSTemplateStats as LocalACSTemplateStats } from "@/lib/duckdb-api-client";

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

const AUTO_REFRESH_INTERVAL = 10000; // 10 seconds for local mode

const SnapshotProgress = () => {
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [templateStats, setTemplateStats] = useState<Record<string, TemplateStats[]>>({});
  const [loading, setLoading] = useState(true);
  const [isPurging, setIsPurging] = useState(false);
  const [isLocalMode, setIsLocalMode] = useState(false);
  const [localStats, setLocalStats] = useState<{ total_contracts: number; total_templates: number } | null>(null);
  const [selectedMigration, setSelectedMigration] = useState<string>('all');
  const [prevContractCount, setPrevContractCount] = useState<number | null>(null);
  const [stableCheckCount, setStableCheckCount] = useState(0); // Count consecutive checks with no change
  const { toast } = useToast();
  const useDuckDB = useDuckDBForLedger();

  // Data is "still writing" until we've seen 2 consecutive refreshes with the same count
  const isStillWriting = stableCheckCount < 2;

  // Get unique migrations from snapshots
  const uniqueMigrations = [...new Set(snapshots.map(s => s.migration_id))]
    .filter(m => m !== null && m !== undefined)
    .sort((a, b) => (b ?? 0) - (a ?? 0));

  // Filter snapshots by selected migration
  const filteredSnapshots = selectedMigration === 'all' 
    ? snapshots 
    : snapshots.filter(s => String(s.migration_id) === selectedMigration);

  useEffect(() => {
    // Check if we should use local mode
    const checkMode = async () => {
      if (useDuckDB) {
        const isConnected = await checkDuckDBConnection();
        setIsLocalMode(isConnected);
        if (isConnected) {
          fetchLocalSnapshots();
          return;
        }
      }
      // Fall back to Supabase
      fetchSnapshots();
    };
    
    checkMode();
  }, [useDuckDB]);

  // Auto-refresh for local mode to detect if data is still being written
  useEffect(() => {
    if (!isLocalMode) return;

    const interval = setInterval(() => {
      fetchLocalSnapshots();
    }, AUTO_REFRESH_INTERVAL);

    return () => clearInterval(interval);
  }, [isLocalMode]);

  // Set up Supabase realtime subscriptions only when not in local mode
  useEffect(() => {
    if (isLocalMode) return;

    // Subscribe to realtime updates for snapshots
    const snapshotChannel = supabase
      .channel("snapshot-progress")
      .on(
        "postgres_changes",
        {
          event: "*",
          schema: "public",
          table: "acs_snapshots",
        },
        (payload) => {
          console.log("Snapshot update:", payload);
          if (payload.eventType === "INSERT") {
            setSnapshots((prev) => [payload.new as Snapshot, ...prev]);
          } else if (payload.eventType === "UPDATE") {
            setSnapshots((prev) => prev.map((s) => (s.id === payload.new.id ? (payload.new as Snapshot) : s)));
          }
        },
      )
      .subscribe();

    // Subscribe to template stats updates
    const templateChannel = supabase
      .channel("template-stats")
      .on(
        "postgres_changes",
        {
          event: "INSERT",
          schema: "public",
          table: "acs_template_stats",
        },
        (payload) => {
          console.log("Template stats update:", payload);
          const newStat = payload.new as TemplateStats;
          setTemplateStats((prev) => ({
            ...prev,
            [newStat.snapshot_id]: [...(prev[newStat.snapshot_id] || []), newStat],
          }));
        },
      )
      .subscribe();

    return () => {
      supabase.removeChannel(snapshotChannel);
      supabase.removeChannel(templateChannel);
    };
  }, [isLocalMode]);

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
        // Count increased - data is still being written, reset stable counter
        setStableCheckCount(0);
      } else if (prevContractCount !== null && currentCount === prevContractCount) {
        // Count hasn't changed - increment stable counter
        setStableCheckCount(prev => prev + 1);
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

  const fetchSnapshots = async () => {
    try {
      const { data, error } = await supabase
        .from("acs_snapshots")
        .select("*")
        .order("timestamp", { ascending: false })
        .limit(10);

      if (error) throw error;
      setSnapshots(data || []);

      // Fetch template stats for each snapshot
      for (const snapshot of data || []) {
        fetchTemplateStats(snapshot.id);
      }
    } catch (error) {
      console.error("Error fetching snapshots:", error);
    } finally {
      setLoading(false);
    }
  };

  const fetchTemplateStats = async (snapshotId: string) => {
    try {
      const { data, error } = await supabase
        .from("acs_template_stats")
        .select("*")
        .eq("snapshot_id", snapshotId)
        .order("updated_at", { ascending: false });

      if (error) throw error;
      if (data) {
        setTemplateStats((prev) => ({
          ...prev,
          [snapshotId]: data,
        }));
      }
    } catch (error) {
      console.error("Error fetching template stats:", error);
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
        "Are you sure you want to purge ALL ACS data? This will delete all snapshots, template stats, and storage files. This action cannot be undone.",
      )
    ) {
      return;
    }

    setIsPurging(true);
    try {
      const { data, error } = await supabase.functions.invoke("purge-acs-storage", {
        body: { purge_all: true },
      });

      if (error) throw error;

      toast({
        title: "Purge complete",
        description: `Deleted ${data.deleted_files} files and ${data.deleted_stats} stats`,
      });

      // Refresh the snapshots list
      fetchSnapshots();
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
              {isLocalMode ? (
                <span className="flex items-center gap-2">
                  <Server className="w-4 h-4 text-green-500" />
                  Using local DuckDB data
                </span>
              ) : (
                "Monitor live ACS snapshot uploads and template processing"
              )}
            </p>
          </div>
          <div className="flex gap-2">
            {isLocalMode ? (
              <Button onClick={refreshLocalData} variant="outline" size="sm">
                <Activity className="h-4 w-4 mr-2" />
                Refresh
              </Button>
            ) : (
              <>
                <TriggerACSSnapshotButton />
                <Button onClick={handlePurgeAll} disabled={isPurging} variant="destructive" size="sm">
                  <Trash2 className={`h-4 w-4 mr-2 ${isPurging ? "animate-spin" : ""}`} />
                  {isPurging ? "Purging..." : "Purge All ACS Data"}
                </Button>
              </>
            )}
          </div>
        </div>

        {/* Local Mode Stats Summary */}
        {isLocalMode && localStats && (
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

        {filteredSnapshots.map((snapshot) => (
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
              {isLocalMode ? (
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
                    <span>{snapshot.entry_count?.toLocaleString() || 0} contracts indexed</span>
                    {isStillWriting && (
                      <span className="text-blue-500">Data still being written...</span>
                    )}
                  </div>
                </div>
              ) : snapshot.status === "completed" ? (
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-muted-foreground">Overall Progress</span>
                    <span className="font-medium">100%</span>
                  </div>
                  <Progress value={100} className="h-2" />
                </div>
              ) : null}

              {/* Stats Grid - Show different stats for local vs remote */}
              {isLocalMode ? (
                <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Database className="w-4 h-4" />
                      Contracts
                    </div>
                    <p className="text-2xl font-bold">{snapshot.entry_count?.toLocaleString() || 0}</p>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <FileText className="w-4 h-4" />
                      Templates
                    </div>
                    <p className="text-2xl font-bold">{snapshot.template_count?.toLocaleString() || 0}</p>
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
              ) : (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <FileText className="w-4 h-4" />
                      Pages Processed
                    </div>
                    <p className="text-2xl font-bold">{snapshot.processed_pages?.toLocaleString() || 0}</p>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Activity className="w-4 h-4" />
                      Events Processed
                    </div>
                    <p className="text-2xl font-bold">{snapshot.processed_events?.toLocaleString() || 0}</p>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Clock className="w-4 h-4" />
                      Elapsed Time
                    </div>
                    <p className="text-2xl font-bold">{formatDuration(snapshot.elapsed_time_ms || 0)}</p>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Activity className="w-4 h-4" />
                      Pages/Min
                    </div>
                    <p className="text-2xl font-bold">{Number(snapshot.pages_per_minute ?? 0).toFixed(1)}</p>
                  </div>
                </div>
              )}

              {/* Activity Metrics - Only for Supabase mode */}
              {!isLocalMode && (
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 p-4 rounded-lg bg-primary/5 border border-primary/10">
                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Database className="w-4 h-4" />
                      Total Contracts
                    </div>
                    <p className="text-2xl font-bold text-primary">
                      {templateStats[snapshot.id]
                        ?.reduce((sum, stat) => sum + stat.contract_count, 0)
                        ?.toLocaleString() || 0}
                    </p>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Activity className="w-4 h-4" />
                      Template Updates
                    </div>
                    <p className="text-2xl font-bold text-primary">{snapshot.template_batch_updates || 0}</p>
                  </div>

                  <div className="space-y-1">
                    <div className="flex items-center gap-2 text-sm text-muted-foreground">
                      <FileText className="w-4 h-4" />
                      Unique Templates
                    </div>
                    <p className="text-2xl font-bold text-primary">{templateStats[snapshot.id]?.length || 0}</p>
                  </div>
                </div>
              )}

              {/* Last Batch Info - Only for Supabase mode */}
              {!isLocalMode && snapshot.last_batch_info && (
                <div className="p-3 rounded-lg bg-green-500/10 border border-green-500/20">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-green-600 dark:text-green-400 font-medium">
                      Last batch: {snapshot.last_batch_info.templates_updated} templates, +
                      {snapshot.last_batch_info.contracts_added.toLocaleString()} contracts
                    </span>
                    <span className="text-muted-foreground text-xs">
                      {formatDistanceToNow(new Date(snapshot.last_batch_info.timestamp), { addSuffix: true })}
                    </span>
                  </div>
                </div>
              )}

              {/* Template Stats */}
              {templateStats[snapshot.id] && templateStats[snapshot.id].length > 0 && (
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">
                    {isLocalMode ? "Top Templates" : "Template Activity"}
                  </h4>
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
        ))}

        {filteredSnapshots.length === 0 && (
          <Card className="glass-card">
            <CardContent className="flex flex-col items-center justify-center py-12">
              <Database className="w-12 h-12 text-muted-foreground mb-4" />
              <p className="text-muted-foreground">
                {isLocalMode 
                  ? selectedMigration !== 'all' 
                    ? `No snapshots found for Migration #${selectedMigration}`
                    : "No local ACS data found. Run fetch-acs-parquet.js to populate data."
                  : "No snapshots found. Trigger a snapshot to see real-time progress."
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
