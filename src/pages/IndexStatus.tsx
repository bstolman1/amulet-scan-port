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
  Coins,
  Users,
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

const clearVoteRequestIndexLock = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/events/vote-request-index/lock`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error("Failed to clear lock");
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
  const res = await fetch(`${getDuckDBApiUrl()}/api/events/vote-request-index/build?force=true`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw new Error("Failed to rebuild vote request index");
  return res.json();
};

const fetchRewardCouponIndexStatus = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/events/reward-coupon-index/status`);
  if (!res.ok) throw new Error("Failed to fetch reward coupon index status");
  return res.json();
};

const rebuildRewardCouponIndex = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/events/reward-coupon-index/build?force=true`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw new Error("Failed to rebuild reward coupon index");
  return res.json();
};

const fetchPartyIndexStatus = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/party/index/status`);
  if (!res.ok) throw new Error("Failed to fetch party index status");
  return res.json();
};

const rebuildPartyIndex = async () => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/party/index/build`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ forceRebuild: true }),
  });
  if (!res.ok) throw new Error("Failed to rebuild party index");
  return res.json();
};

interface LastSuccessfulBuild {
  build_id: string;
  started_at: string;
  completed_at: string;
  duration_seconds: number;
  total_indexed: number;
  inserted: number;
  updated: number;
  closed_count: number;
  in_progress_count: number;
  executed_count: number;
  rejected_count: number;
  expired_count: number;
}

interface IndexCardProps {
  title: string;
  description: string;
  icon: React.ReactNode;
  status: "ready" | "building" | "empty" | "error" | "loading" | "locked";
  stats?: { label: string; value: string | number }[];
  lastUpdated?: string | null;
  onRebuild?: () => void;
  isRebuilding?: boolean;
  buildProgress?: { current: number; total: number } | null;
  onCreateTable?: () => void;
  isCreatingTable?: boolean;
  lastSuccessfulBuild?: LastSuccessfulBuild | null;
  onClearLock?: () => void;
  isClearingLock?: boolean;
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
  lastSuccessfulBuild,
  onClearLock,
  isClearingLock,
}: IndexCardProps) => {
  const statusConfig = {
    ready: { label: "Ready", variant: "default" as const, color: "text-green-500", bg: "bg-green-500/10" },
    building: { label: "Building...", variant: "secondary" as const, color: "text-blue-500", bg: "bg-blue-500/10" },
    empty: { label: "Not Built", variant: "outline" as const, color: "text-amber-500", bg: "bg-amber-500/10" },
    error: { label: "Error", variant: "destructive" as const, color: "text-destructive", bg: "bg-destructive/10" },
    loading: { label: "Loading...", variant: "secondary" as const, color: "text-muted-foreground", bg: "bg-muted" },
    locked: { label: "Locked", variant: "outline" as const, color: "text-orange-500", bg: "bg-orange-500/10" },
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

        {/* Last Successful Build Summary */}
        {lastSuccessfulBuild && (
          <div className="bg-muted/30 rounded-lg p-3 space-y-2 border border-border/50">
            <div className="flex items-center gap-2 text-sm font-medium text-green-600 dark:text-green-400">
              <CheckCircle2 className="w-4 h-4" />
              <span>Last Successful Build</span>
            </div>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground">
              <span>Build ID:</span>
              <span className="font-mono truncate" title={lastSuccessfulBuild.build_id}>
                {lastSuccessfulBuild.build_id.slice(0, 20)}...
              </span>
              <span>Completed:</span>
              <span>{formatDistanceToNow(new Date(lastSuccessfulBuild.completed_at), { addSuffix: true })}</span>
              <span>Duration:</span>
              <span>{lastSuccessfulBuild.duration_seconds?.toFixed(1)}s</span>
              <span>Total Indexed:</span>
              <span>{lastSuccessfulBuild.total_indexed?.toLocaleString()}</span>
            </div>
            <div className="flex flex-wrap gap-1 pt-1">
              <Badge variant="secondary" className="text-xs">
                In Progress: {lastSuccessfulBuild.in_progress_count}
              </Badge>
              <Badge variant="secondary" className="text-xs bg-green-500/20 text-green-700 dark:text-green-300">
                Executed: {lastSuccessfulBuild.executed_count}
              </Badge>
              <Badge variant="secondary" className="text-xs bg-red-500/20 text-red-700 dark:text-red-300">
                Rejected: {lastSuccessfulBuild.rejected_count}
              </Badge>
              <Badge variant="secondary" className="text-xs bg-amber-500/20 text-amber-700 dark:text-amber-300">
                Expired: {lastSuccessfulBuild.expired_count}
              </Badge>
            </div>
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

        {/* Clear Lock Button */}
        {onClearLock && status === "locked" && (
          <Button
            variant="destructive"
            size="sm"
            onClick={onClearLock}
            disabled={isClearingLock}
            className="w-full"
          >
            {isClearingLock ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Clearing...
              </>
            ) : (
              <>
                <AlertCircle className="w-4 h-4 mr-2" />
                Clear Stale Lock
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

  const { data: rewardCouponIndex, isLoading: rewardLoading, error: rewardError } = useQuery({
    queryKey: ["rewardCouponIndexStatus"],
    queryFn: fetchRewardCouponIndexStatus,
    refetchInterval: 5000,
    retry: false,
  });

  const { data: partyIndex, isLoading: partyLoading, error: partyError } = useQuery({
    queryKey: ["partyIndexStatus"],
    queryFn: fetchPartyIndexStatus,
    refetchInterval: 5000,
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

  const clearLockMutation = useMutation({
    mutationFn: clearVoteRequestIndexLock,
    onSuccess: (data) => {
      toast({
        title: data.cleared ? "Lock cleared" : "No lock to clear",
        description: data.cleared 
          ? `Cleared stale lock from PID ${data.previousPid}` 
          : data.reason,
      });
      queryClient.invalidateQueries({ queryKey: ["voteRequestIndexStatus"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Failed to clear lock",
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

  const rewardRebuildMutation = useMutation({
    mutationFn: rebuildRewardCouponIndex,
    onSuccess: (data) => {
      toast({
        title: "Reward coupon index build started",
        description: data.message || "Building in background...",
      });
      queryClient.invalidateQueries({ queryKey: ["rewardCouponIndexStatus"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Failed to build reward coupon index",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const partyRebuildMutation = useMutation({
    mutationFn: rebuildPartyIndex,
    onSuccess: (data) => {
      toast({
        title: "Party index build started",
        description: data.message || "Building in background...",
      });
      queryClient.invalidateQueries({ queryKey: ["partyIndexStatus"] });
    },
    onError: (error: Error) => {
      toast({
        title: "Failed to build party index",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const handleRefreshAll = () => {
    queryClient.invalidateQueries({ queryKey: ["templateIndexStatus"] });
    queryClient.invalidateQueries({ queryKey: ["voteRequestIndexStatus"] });
    queryClient.invalidateQueries({ queryKey: ["aggregationState"] });
    queryClient.invalidateQueries({ queryKey: ["rewardCouponIndexStatus"] });
    queryClient.invalidateQueries({ queryKey: ["partyIndexStatus"] });
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
    if (voteRequestIndex?.lockExists) return "locked";
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

  // Derive reward coupon index status
  const getRewardStatus = (): IndexCardProps["status"] => {
    if (rewardLoading) return "loading";
    if (rewardError) return "error";
    if (rewardCouponIndex?.isIndexing) return "building";
    if (rewardCouponIndex?.stats?.total > 0) return "ready";
    return "empty";
  };

  // Derive party index status
  const getPartyStatus = (): IndexCardProps["status"] => {
    if (partyLoading) return "loading";
    if (partyError) return "error";
    if (partyIndex?.indexing) return "building";
    if (partyIndex?.isPopulated) return "ready";
    return "empty";
  };

  const allStatuses = [getTemplateStatus(), getVoteStatus(), getAggStatus(), getRewardStatus(), getPartyStatus()];

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
                  <p className="text-2xl font-bold">5</p>
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
                    {allStatuses.filter(s => s === "ready").length}
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
                    {allStatuses.filter(s => s === "empty" || s === "error").length}
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
                    { label: "In Progress", value: voteRequestIndex.stats.inProgress?.toLocaleString() || voteRequestIndex.stats.active?.toLocaleString() || 0 },
                    { label: "Executed", value: voteRequestIndex.stats.executed?.toLocaleString() || 0 },
                    { label: "Rejected", value: voteRequestIndex.stats.rejected?.toLocaleString() || 0 },
                    { label: "Expired", value: voteRequestIndex.stats.expired?.toLocaleString() || 0 },
                  ]
                : []
            }
            lastUpdated={voteRequestIndex?.lastIndexedAt}
            onRebuild={() => voteRebuildMutation.mutate()}
            isRebuilding={voteRebuildMutation.isPending}
            buildProgress={
              voteRequestIndex?.isIndexing && voteRequestIndex?.progress
                ? { current: voteRequestIndex.progress.current || 0, total: voteRequestIndex.progress.total || 1 }
                : null
            }
            lastSuccessfulBuild={voteRequestIndex?.lastSuccessfulBuild}
            onClearLock={() => clearLockMutation.mutate()}
            isClearingLock={clearLockMutation.isPending}
          />

          {/* Reward Coupon Index */}
          <IndexCard
            title="Reward Coupon Index"
            description="Pre-calculated CC rewards from App/Validator/SV coupons"
            icon={<Coins className="w-5 h-5" />}
            status={getRewardStatus()}
            stats={
              rewardCouponIndex?.stats?.total > 0
                ? [
                    { label: "Total Coupons", value: rewardCouponIndex.stats.total?.toLocaleString() || 0 },
                    { label: "App", value: rewardCouponIndex.stats.app?.toLocaleString() || 0 },
                    { label: "Validator", value: rewardCouponIndex.stats.validator?.toLocaleString() || 0 },
                    { label: "SV", value: rewardCouponIndex.stats.sv?.toLocaleString() || 0 },
                  ]
                : []
            }
            lastUpdated={rewardCouponIndex?.lastIndexedAt}
            onRebuild={() => rewardRebuildMutation.mutate()}
            isRebuilding={rewardRebuildMutation.isPending}
            buildProgress={
              rewardCouponIndex?.isIndexing && rewardCouponIndex?.progress
                ? { current: rewardCouponIndex.progress.current || 0, total: rewardCouponIndex.progress.total || 1 }
                : null
            }
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

          {/* Party Index */}
          <IndexCard
            title="Party Index"
            description="Maps party IDs to event files for instant lookups"
            icon={<Users className="w-5 h-5" />}
            status={getPartyStatus()}
            stats={
              partyIndex?.isPopulated
                ? [
                    { label: "Unique Parties", value: partyIndex.totalParties?.toLocaleString() || 0 },
                    { label: "Files Indexed", value: partyIndex.totalFiles?.toLocaleString() || 0 },
                    { label: "Files Scanned", value: partyIndex.filesIndexed?.toLocaleString() || 0 },
                  ]
                : []
            }
            lastUpdated={partyIndex?.lastIndexedAt}
            onRebuild={() => partyRebuildMutation.mutate()}
            isRebuilding={partyRebuildMutation.isPending}
            buildProgress={
              partyIndex?.indexing
                ? { current: partyIndex.indexing.filesScanned || 0, total: partyIndex.indexing.totalFiles || 1 }
                : null
            }
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
