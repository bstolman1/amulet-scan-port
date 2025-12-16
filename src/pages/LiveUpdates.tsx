import { useState, useMemo, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Activity, Clock, Search, Database, Wifi, WifiOff, FileText, RefreshCw, Zap, Timer, ChevronDown, ChevronRight, Copy, Check } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { useLedgerUpdates, LedgerUpdate } from "@/hooks/use-ledger-updates";
import { useDuckDBHealth } from "@/hooks/use-duckdb-events";
import { useDuckDBForLedger } from "@/lib/backend-config";
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
  const { data: updates = [], isLoading, dataUpdatedAt, refetch } = useLedgerUpdates(100);
  const { data: isDuckDBAvailable } = useDuckDBHealth();
  const usingDuckDB = useDuckDBForLedger();
  const [searchTerm, setSearchTerm] = useState("");
  const [secondsSinceRefresh, setSecondsSinceRefresh] = useState(0);
  const { toast } = useToast();

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
    toast({ title: "Refreshing data..." });
  };

  const filteredUpdates = updates.filter((update) => {
    const matchesSearch =
      !searchTerm ||
      update.id.toLowerCase().includes(searchTerm.toLowerCase()) ||
      update.update_type?.toLowerCase().includes(searchTerm.toLowerCase());

    return matchesSearch;
  });

  // Calculate statistics
  const stats = useMemo(() => {
    const updatesByType = updates.reduce((acc, u) => {
      const type = u.update_type || "unknown";
      acc[type] = (acc[type] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    // Extract templates from update data
    const templateCounts = updates.reduce((acc, u) => {
      const data = u.update_data as any;
      // Look for template IDs in events array within update_data
      const events = data?.events || data?.transaction?.events || [];
      events.forEach((event: any) => {
        const templateId = event?.template_id || event?.templateId;
        if (templateId) {
          const shortName = templateId.split(":").pop() || templateId;
          acc[shortName] = (acc[shortName] || 0) + 1;
        }
      });
      return acc;
    }, {} as Record<string, number>);

    // Count total events
    const totalEvents = updates.reduce((sum, u) => {
      const data = u.update_data as any;
      const events = data?.events || data?.transaction?.events || [];
      return sum + (Array.isArray(events) ? events.length : 0);
    }, 0);

    // Get latest update time - prefer effective_at (actual transaction time)
    const latestUpdate = updates.length > 0 
      ? new Date(Math.max(...updates.map(u => {
          const effectiveAt = (u as any).effective_at;
          return new Date(effectiveAt || u.timestamp).getTime();
        })))
      : null;

    // Sort templates by count
    const topTemplates = Object.entries(templateCounts)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 5);

    return {
      updatesByType,
      topTemplates,
      totalEvents,
      latestUpdate,
      totalTypes: Object.keys(updatesByType).length,
    };
  }, [updates]);

  // Calculate type distribution for visual breakdown
  const typeDistribution = useMemo(() => {
    const total = updates.length || 1;
    return Object.entries(stats.updatesByType).map(([type, count]) => ({
      type,
      count,
      percentage: Math.round((count / total) * 100),
    }));
  }, [updates.length, stats.updatesByType]);

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
        <div>
          <h1 className="text-3xl font-bold mb-2">Live Ledger Updates</h1>
          <p className="text-muted-foreground">Ledger updates from {usingDuckDB ? "DuckDB API" : "Supabase"}</p>
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
          </div>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Button variant="outline" size="sm" onClick={handleRefresh} className="h-7 px-2">
              <RefreshCw className={`w-4 h-4 mr-1 ${secondsSinceRefresh < 3 ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
            <span>Updated {secondsSinceRefresh}s ago</span>
            {stats.latestUpdate && (
              <div className="flex items-center gap-1" title="Effective time of latest record in dataset">
                <Timer className="w-4 h-4" />
                <span>Data from: {formatDistanceToNow(stats.latestUpdate, { addSuffix: true })}</span>
              </div>
            )}
          </div>
        </div>

        {/* Stats Grid - Row 1 */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="glass-card">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Total Updates
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-primary">{updates.length}</p>
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
              {filteredUpdates.map((update) => {
                // Extract additional fields from update_data
                const data = update.update_data as any;
                const contractId = data?.contract_id || data?.created_event?.contract_id || null;
                const templateId = data?.template_id || data?.created_event?.template_id || null;
                const templateShort = templateId ? templateId.split(':').pop() : null;
                const signatories = data?.signatories || data?.created_event?.signatories || [];
                const payload = data?.payload || data?.create_arguments || null;
                
                return (
                  <div
                    key={update.id}
                    className="p-3 rounded-lg border bg-card hover:bg-muted/50 transition-colors"
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="space-y-2 flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <Badge variant="outline">{update.update_type}</Badge>
                          <span className="font-mono text-xs text-muted-foreground">Migration {update.migration_id || "N/A"}</span>
                          <span className="font-mono text-xs text-muted-foreground truncate max-w-[200px]" title={update.id}>{update.id.substring(0, 30)}...</span>
                        </div>

                        {/* Contract & Template Info */}
                        {(contractId || templateShort) && (
                          <div className="flex items-center gap-3 flex-wrap text-xs">
                            {contractId && (
                              <span className="font-mono text-blue-500" title={contractId}>
                                📄 {contractId.substring(0, 24)}...
                              </span>
                            )}
                            {templateShort && (
                              <Badge variant="secondary" className="font-mono text-xs">
                                {templateShort}
                              </Badge>
                            )}
                          </div>
                        )}

                        {/* Signatories */}
                        {signatories.length > 0 && (
                          <div className="flex items-center gap-1 flex-wrap text-xs text-muted-foreground">
                            <span>👤</span>
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
                          {new Date((update as any).effective_at || update.timestamp).toLocaleString()}
                          {(update as any).effective_at && (update as any).effective_at !== update.timestamp && (
                            <span className="ml-2 text-muted-foreground/60" title="When the file was written">
                              (written {new Date(update.timestamp).toLocaleString()})
                            </span>
                          )}
                        </div>

                        {/* Full JSON Data - Expandable */}
                        <div className="flex flex-wrap gap-2 pt-1">
                          <JsonViewer data={payload} label="View Payload" />
                          <JsonViewer data={data} label="View Full Data" />
                        </div>
                      </div>

                      <div className="text-xs text-muted-foreground whitespace-nowrap">
                        {formatDistanceToNow(new Date(update.created_at), { addSuffix: true })}
                      </div>
                    </div>
                  </div>
                );
              })}

              {filteredUpdates.length === 0 && (
                <div className="text-center py-12 text-muted-foreground">
                  {searchTerm ? "No matching updates found." : "No updates yet. Start the DuckDB API server to see data."}
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