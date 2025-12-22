import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Database,
  RefreshCw,
  FileText,
  Vote,
  Layers,
  CheckCircle2,
  AlertCircle,
  Loader2,
  Clock,
  Activity,
  Play,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { useToast } from "@/hooks/use-toast";
import { getDuckDBApiUrl } from "@/lib/backend-config";
import { useState } from "react";

interface TemplateInfo {
  template_name: string;
  total_events: number;
  file_count: number;
}

// API functions
const fetchTemplateIndexStatus = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/engine/template-index/status`);
  if (!res.ok) throw new Error("Failed to fetch template index status");
  return res.json();
};

const fetchVoteRequestIndexStatus = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/events/vote-request-index/status`);
  if (!res.ok) throw new Error("Failed to fetch vote request index status");
  return res.json();
};

const fetchAggregationState = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/stats/aggregation-state`);
  if (!res.ok) throw new Error("Failed to fetch aggregation state");
  return res.json();
};

const fetchIndexedTemplates = async (): Promise<{ templates: TemplateInfo[] }> => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/engine/template-index/templates`);
  if (!res.ok) throw new Error("Failed to fetch indexed templates");
  return res.json();
};

const rebuildTemplateIndex = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/engine/template-index/build`, { 
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ force: true })
  });
  if (!res.ok) throw new Error("Failed to rebuild template index");
  return res.json();
};

const rebuildVoteRequestIndex = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/events/vote-request-index/build`, { 
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ force: true })
  });
  if (!res.ok) throw new Error("Failed to rebuild vote request index");
  return res.json();
};

interface IndexCardProps {
  title: string;
  description: string;
  icon: React.ReactNode;
  status: "ready" | "building" | "empty" | "error" | "loading";
  stats?: { label: string; value: string | number }[];
  lastUpdated?: string | null;
  onRebuild?: () => void;
  isRebuilding?: boolean;
  buildProgress?: { current: number; total: number } | null;
  onCreateTable?: () => void;
  isCreatingTable?: boolean;
}

const IndexCard = ({
  title,
  description,
  icon,
  status,
  stats = [],
  lastUpdated,
  onRebuild,
  isRebuilding,
  buildProgress,
  onCreateTable,
  isCreatingTable,
}: IndexCardProps) => {
  const statusConfig = {
    ready: { label: "Ready", variant: "default" as const, color: "text-green-500", bg: "bg-green-500/10" },
    building: { label: "Building...", variant: "secondary" as const, color: "text-blue-500", bg: "bg-blue-500/10" },
    empty: { label: "Not Built", variant: "outline" as const, color: "text-amber-500", bg: "bg-amber-500/10" },
    error: { label: "Error", variant: "destructive" as const, color: "text-destructive", bg: "bg-destructive/10" },
    loading: { label: "Loading...", variant: "secondary" as const, color: "text-muted-foreground", bg: "bg-muted" },
  };

  const config = statusConfig[status];

  return (
    <Card className={`border ${status === "building" ? "border-blue-500/30" : ""}`}>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className={`p-2 rounded-lg ${config.bg}`}>
              <div className={config.color}>{icon}</div>
            </div>
            <div>
              <CardTitle className="text-lg">{title}</CardTitle>
              <CardDescription className="text-sm">{description}</CardDescription>
            </div>
          </div>
          <Badge variant={config.variant} className={status === "building" ? "animate-pulse" : ""}>
            {status === "building" && <Loader2 className="w-3 h-3 mr-1 animate-spin" />}
            {config.label}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Stats Grid */}
        {stats.length > 0 && (
          <div className="grid grid-cols-2 gap-3">
            {stats.map((stat, i) => (
              <div key={i} className="bg-muted/50 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">{stat.label}</p>
                <p className="text-lg font-semibold">{stat.value}</p>
              </div>
            ))}
          </div>
        )}

        {/* Build Progress */}
        {buildProgress && status === "building" && (
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">Building...</span>
              <span>{buildProgress.current.toLocaleString()} / {buildProgress.total.toLocaleString()}</span>
            </div>
            <Progress value={(buildProgress.current / buildProgress.total) * 100} className="h-2" />
          </div>
        )}

        {/* Last Updated */}
        {lastUpdated && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Clock className="w-4 h-4" />
            <span>Updated {formatDistanceToNow(new Date(lastUpdated), { addSuffix: true })}</span>
          </div>
        )}

        {/* Create Table Button (for Aggregation State when table doesn't exist) */}
        {onCreateTable && status === "empty" && (
          <Button
            variant="default"
            size="sm"
            onClick={onCreateTable}
            disabled={isCreatingTable}
            className="w-full"
          >
            {isCreatingTable ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Creating...
              </>
            ) : (
              <>
                <Database className="w-4 h-4 mr-2" />
                Create Table
              </>
            )}
          </Button>
        )}

        {/* Rebuild Button */}
        {onRebuild && (
          <Button
            variant="outline"
            size="sm"
            onClick={onRebuild}
            disabled={isRebuilding || status === "building"}
            className="w-full"
          >
            {isRebuilding || status === "building" ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Building...
              </>
            ) : (
              <>
                <Play className="w-4 h-4 mr-2" />
                {status === "empty" ? "Build Index" : "Rebuild Index"}
              </>
            )}
          </Button>
        )}
      </CardContent>
    </Card>
  );
};

const IndexStatus = () => {
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const [showAllTemplates, setShowAllTemplates] = useState(false);

  // Fetch statuses
  const { data: templateIndex, isLoading: templateLoading, error: templateError } = useQuery({
    queryKey: ["templateIndexStatus"],
    queryFn: fetchTemplateIndexStatus,
    refetchInterval: 5000,
    retry: false,
  });

  const { data: voteRequestIndex, isLoading: voteLoading, error: voteError } = useQuery({
    queryKey: ["voteRequestIndexStatus"],
    queryFn: fetchVoteRequestIndexStatus,
    refetchInterval: 5000,
    retry: false,
  });

  const { data: aggregationState, isLoading: aggLoading, error: aggError } = useQuery({
    queryKey: ["aggregationState"],
    queryFn: fetchAggregationState,
    refetchInterval: 10000,
    retry: false,
  });

  const { data: templatesData, isLoading: templatesLoading } = useQuery({
    queryKey: ["indexedTemplates"],
    queryFn: fetchIndexedTemplates,
    refetchInterval: 30000,
    retry: false,
    enabled: templateIndex?.isPopulated,
  });

  const templates = templatesData?.templates || [];

  // Mutations
  const templateRebuildMutation = useMutation({
    mutationFn: rebuildTemplateIndex,
    onSuccess: (data) => {
      toast({
        title: "Template index build started",
        description: data.message || "Building in background...",
      });
      queryClient.invalidateQueries({ queryKey: ["templateIndexStatus"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Failed to build template index",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const voteRebuildMutation = useMutation({
    mutationFn: rebuildVoteRequestIndex,
    onSuccess: (data) => {
      toast({
        title: "Vote request index build started",
        description: data.message || "Building in background...",
      });
      queryClient.invalidateQueries({ queryKey: ["voteRequestIndexStatus"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Failed to build vote request index",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  // Mutation to initialize engine schema (create aggregation_state table)
  const initSchemaMutation = useMutation({
    mutationFn: async () => {
      const res = await fetch(`${getDuckDBApiUrl()}/api/stats/init-engine-schema`, { method: "POST" });
      if (!res.ok) throw new Error("Failed to initialize engine schema");
      return res.json();
    },
    onSuccess: () => {
      toast({
        title: "Engine schema initialized",
        description: "Aggregation state table created successfully",
      });
      queryClient.invalidateQueries({ queryKey: ["aggregationState"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Failed to initialize schema",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const handleRefreshAll = () => {
    queryClient.invalidateQueries({ queryKey: ["templateIndexStatus"] });
    queryClient.invalidateQueries({ queryKey: ["voteRequestIndexStatus"] });
    queryClient.invalidateQueries({ queryKey: ["aggregationState"] });
    toast({ title: "Refreshing status..." });
  };

  // Derive template index status
  const getTemplateStatus = (): IndexCardProps["status"] => {
    if (templateLoading) return "loading";
    if (templateError) return "error";
    if (templateIndex?.inProgress) return "building";
    if (templateIndex?.isPopulated) return "ready";
    return "empty";
  };

  // Derive vote request index status
  const getVoteStatus = (): IndexCardProps["status"] => {
    if (voteLoading) return "loading";
    if (voteError) return "error";
    if (voteRequestIndex?.isIndexing) return "building";
    if (voteRequestIndex?.stats?.total > 0) return "ready";
    return "empty";
  };

  // Derive aggregation status
  const getAggStatus = (): IndexCardProps["status"] => {
    if (aggLoading) return "loading";
    if (aggError) return "error";
    // If tableExists is explicitly false, show empty (not error)
    if (aggregationState?.tableExists === false) return "empty";
    if (aggregationState?.states?.length > 0) return "ready";
    return "empty";
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold mb-2">DuckDB Index Status</h1>
            <p className="text-muted-foreground">
              Monitor and manage persistent indexes for fast queries
            </p>
          </div>
          <Button variant="outline" onClick={handleRefreshAll}>
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh All
          </Button>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="glass-card">
            <CardContent className="pt-6">
              <div className="flex items-center gap-4">
                <div className="p-3 rounded-lg bg-primary/10">
                  <Database className="w-6 h-6 text-primary" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Total Indexes</p>
                  <p className="text-2xl font-bold">3</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardContent className="pt-6">
              <div className="flex items-center gap-4">
                <div className="p-3 rounded-lg bg-green-500/10">
                  <CheckCircle2 className="w-6 h-6 text-green-500" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Ready</p>
                  <p className="text-2xl font-bold">
                    {[getTemplateStatus(), getVoteStatus(), getAggStatus()].filter(s => s === "ready").length}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardContent className="pt-6">
              <div className="flex items-center gap-4">
                <div className="p-3 rounded-lg bg-amber-500/10">
                  <AlertCircle className="w-6 h-6 text-amber-500" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Need Attention</p>
                  <p className="text-2xl font-bold">
                    {[getTemplateStatus(), getVoteStatus(), getAggStatus()].filter(s => s === "empty" || s === "error").length}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Index Cards */}
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
          {/* Template File Index */}
          <IndexCard
            title="Template File Index"
            description="Maps templates to binary files for fast filtering"
            icon={<FileText className="w-5 h-5" />}
            status={getTemplateStatus()}
            stats={
              templateIndex?.isPopulated
                ? [
                    { label: "Files Indexed", value: templateIndex.totalFiles?.toLocaleString?.() || 0 },
                    { label: "Templates Found", value: templateIndex.uniqueTemplates?.toLocaleString?.() || 0 },
                    { label: "Build Duration", value: templateIndex.buildDurationSeconds ? `${Number(templateIndex.buildDurationSeconds).toFixed(1)}s` : "—" },
                    { label: "Event Mappings", value: templateIndex.totalEventMappings?.toLocaleString?.() || 0 },
                  ]
                : []
            }
            lastUpdated={templateIndex?.lastIndexedAt}
            onRebuild={() => templateRebuildMutation.mutate()}
            isRebuilding={templateRebuildMutation.isPending}
            buildProgress={
              templateIndex?.inProgress && templateIndex?.progress
                ? { current: templateIndex.progress.current || 0, total: templateIndex.progress.total || 1 }
                : null
            }
          />

          {/* Vote Request Index */}
          <IndexCard
            title="Vote Request Index"
            description="Persistent index for governance vote requests"
            icon={<Vote className="w-5 h-5" />}
            status={getVoteStatus()}
            stats={
              voteRequestIndex?.stats?.total > 0
                ? [
                    { label: "Total Requests", value: voteRequestIndex.stats.total?.toLocaleString() || 0 },
                    { label: "Active", value: voteRequestIndex.stats.active?.toLocaleString() || 0 },
                    { label: "Historical", value: voteRequestIndex.stats.historical?.toLocaleString() || 0 },
                    { label: "Closed", value: voteRequestIndex.stats.closed?.toLocaleString() || 0 },
                  ]
                : []
            }
            lastUpdated={voteRequestIndex?.lastIndexedAt}
            onRebuild={() => voteRebuildMutation.mutate()}
            isRebuilding={voteRebuildMutation.isPending}
          />

          {/* Aggregation State */}
          <IndexCard
            title="Aggregation State"
            description="Tracks incremental aggregation progress"
            icon={<Layers className="w-5 h-5" />}
            status={getAggStatus()}
            stats={
              aggregationState?.states?.length > 0
                ? aggregationState.states.slice(0, 4).map((state: any) => ({
                    label: state.agg_name || "Unknown",
                    value: `File #${state.last_file_id || 0}`,
                  }))
                : []
            }
            lastUpdated={aggregationState?.states?.[0]?.last_updated}
            onCreateTable={aggregationState?.tableExists === false ? () => initSchemaMutation.mutate() : undefined}
            isCreatingTable={initSchemaMutation.isPending}
          />
        </div>

        {/* Indexed Templates List */}
        {templateIndex?.isPopulated && templates.length > 0 && (
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <FileText className="w-5 h-5 text-primary" />
                    Indexed Templates ({templates.length})
                  </CardTitle>
                  <CardDescription>All unique templates found across binary files</CardDescription>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowAllTemplates(!showAllTemplates)}
                >
                  {showAllTemplates ? (
                    <>
                      <ChevronUp className="w-4 h-4 mr-1" />
                      Show Less
                    </>
                  ) : (
                    <>
                      <ChevronDown className="w-4 h-4 mr-1" />
                      Show All
                    </>
                  )}
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <ScrollArea className={showAllTemplates ? "h-[400px]" : "h-[200px]"}>
                <div className="space-y-1">
                  {/* Header */}
                  <div className="grid grid-cols-3 gap-4 text-xs text-muted-foreground font-medium px-3 py-2 bg-muted/50 rounded sticky top-0">
                    <span>Template Name</span>
                    <span className="text-right">Events</span>
                    <span className="text-right">Files</span>
                  </div>
                  {templates.map((t, idx) => (
                    <div
                      key={t.template_name}
                      className={`grid grid-cols-3 gap-4 text-sm py-2 px-3 rounded hover:bg-muted/50 ${
                        idx % 2 === 0 ? "bg-muted/20" : ""
                      }`}
                    >
                      <span className="font-mono text-xs truncate" title={t.template_name}>
                        {t.template_name}
                      </span>
                      <span className="text-right tabular-nums">
                        {Number(t.total_events).toLocaleString()}
                      </span>
                      <span className="text-right tabular-nums text-muted-foreground">
                        {Number(t.file_count).toLocaleString()}
                      </span>
                    </div>
                  ))}
                </div>
              </ScrollArea>
              {templatesLoading && (
                <div className="flex items-center justify-center py-4 text-muted-foreground">
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Loading templates...
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* Info Section */}
        <Card className="bg-muted/30">
          <CardContent className="pt-6">
            <div className="flex items-start gap-4">
              <Activity className="w-5 h-5 text-muted-foreground mt-0.5" />
              <div className="space-y-2 text-sm text-muted-foreground">
                <p>
                  <strong>Template File Index:</strong> Dramatically speeds up template-specific queries by scanning only
                  relevant files instead of all binary files. Build time is ~10-15 minutes for large datasets.
                </p>
                <p>
                  <strong>Vote Request Index:</strong> Enables instant historical governance queries without scanning
                  thousands of binary files. Uses the template index when available for faster builds.
                </p>
                <p>
                  <strong>Aggregation State:</strong> Tracks the last processed file for incremental aggregations,
                  avoiding reprocessing of already-computed data.
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default IndexStatus;
