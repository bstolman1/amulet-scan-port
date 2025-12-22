import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Database, FileText, Layers, Clock, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getDuckDBApiUrl } from "@/lib/backend-config";
import { formatDistanceToNow } from "date-fns";

interface TemplateIndexStatus {
  totalFiles: number;
  uniqueTemplates: number;
  totalMappings: number;
  lastIndexedAt: string | null;
  buildDurationSeconds: number | null;
  inProgress: boolean;
  progress?: {
    current: number;
    total: number;
    percent: string;
    filesPerSec: string;
    etaMinutes: string;
  };
}

interface TemplateInfo {
  template_name: string;
  total_events: number;
  file_count: number;
}

export function TemplateIndexStatsCard() {
  const backendUrl = getDuckDBApiUrl();

  const { data: status, isLoading: statusLoading, refetch: refetchStatus } = useQuery<TemplateIndexStatus>({
    queryKey: ["template-index-status"],
    queryFn: async () => {
      const res = await fetch(`${backendUrl}/api/engine/template-index/status`);
      if (!res.ok) throw new Error("Failed to fetch template index status");
      return res.json();
    },
    refetchInterval: 10000, // Refresh every 10s
  });

  const { data: templatesData, isLoading: templatesLoading } = useQuery<{ templates: TemplateInfo[] }>({
    queryKey: ["template-index-templates"],
    queryFn: async () => {
      const res = await fetch(`${backendUrl}/api/engine/template-index/templates`);
      if (!res.ok) throw new Error("Failed to fetch indexed templates");
      return res.json();
    },
    refetchInterval: 30000,
  });

  const templates = templatesData?.templates || [];

  if (statusLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Template File Index
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Skeleton className="h-32 w-full" />
        </CardContent>
      </Card>
    );
  }

  const formatNumber = (n: number) => n.toLocaleString();

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5 text-primary" />
            Template File Index
          </CardTitle>
          <Button variant="ghost" size="sm" onClick={() => refetchStatus()}>
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Status Banner */}
        {status?.inProgress ? (
          <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg p-3">
            <div className="flex items-center gap-2 text-amber-500 font-medium mb-2">
              <RefreshCw className="h-4 w-4 animate-spin" />
              Indexing in Progress
            </div>
            {status.progress && (
              <div className="text-sm text-muted-foreground space-y-1">
                <p>{status.progress.current} / {status.progress.total} files ({status.progress.percent}%)</p>
                <p>{status.progress.filesPerSec} files/sec • ETA: {status.progress.etaMinutes} min</p>
              </div>
            )}
          </div>
        ) : (
          <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
            Index Ready
          </Badge>
        )}

        {/* Stats Grid */}
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground text-xs">
              <FileText className="h-3.5 w-3.5" />
              Files Indexed
            </div>
            <p className="text-2xl font-bold">{formatNumber(status?.totalFiles || 0)}</p>
          </div>
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground text-xs">
              <Layers className="h-3.5 w-3.5" />
              Unique Templates
            </div>
            <p className="text-2xl font-bold">{formatNumber(status?.uniqueTemplates || 0)}</p>
          </div>
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground text-xs">
              <Database className="h-3.5 w-3.5" />
              Template Mappings
            </div>
            <p className="text-lg font-semibold">{formatNumber(status?.totalMappings || 0)}</p>
          </div>
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground text-xs">
              <Clock className="h-3.5 w-3.5" />
              Last Indexed
            </div>
            <p className="text-sm font-medium">
              {status?.lastIndexedAt 
                ? formatDistanceToNow(new Date(status.lastIndexedAt), { addSuffix: true })
                : "Never"}
            </p>
          </div>
        </div>

        {/* Build Duration */}
        {status?.buildDurationSeconds != null && (
          <p className="text-xs text-muted-foreground">
            Last build took {status.buildDurationSeconds.toFixed(1)}s
          </p>
        )}

        {/* Templates List */}
        {!templatesLoading && templates.length > 0 && (
          <div className="pt-2 border-t">
            <p className="text-sm font-medium mb-2">Templates ({templates.length})</p>
            <ScrollArea className="h-48">
              <div className="space-y-1.5">
                {templates.map((t) => (
                  <div
                    key={t.template_name}
                    className="flex items-center justify-between text-sm py-1 px-2 rounded hover:bg-muted/50"
                  >
                    <span className="font-mono text-xs truncate max-w-[180px]" title={t.template_name}>
                      {t.template_name}
                    </span>
                    <div className="flex items-center gap-2 text-xs text-muted-foreground">
                      <span>{formatNumber(t.total_events)} events</span>
                      <span className="text-muted-foreground/50">•</span>
                      <span>{t.file_count} files</span>
                    </div>
                  </div>
                ))}
              </div>
            </ScrollArea>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
