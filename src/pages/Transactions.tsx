import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronRight, Database, RefreshCw, Copy, Check } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useLocalTransactions, LocalEvent } from "@/hooks/use-local-events";
import { useState } from "react";

// JSON Value Renderer Component
const JsonValue = ({ value, depth = 0 }: { value: any; depth?: number }) => {
  const [isExpanded, setIsExpanded] = useState(depth < 2);

  if (value === null) return <span className="text-orange-400">null</span>;
  if (value === undefined) return <span className="text-muted-foreground">undefined</span>;
  if (typeof value === "boolean") return <span className="text-purple-400">{value.toString()}</span>;
  if (typeof value === "number") return <span className="text-cyan-400">{value}</span>;
  if (typeof value === "string") {
    // Check if it's a timestamp
    if (/^\d{4}-\d{2}-\d{2}T/.test(value)) {
      return (
        <span className="text-green-400" title={value}>
          "{new Date(value).toLocaleString()}"
        </span>
      );
    }
    // Truncate long strings
    if (value.length > 60) {
      return (
        <span className="text-green-400" title={value}>
          "{value.slice(0, 60)}..."
        </span>
      );
    }
    return <span className="text-green-400">"{value}"</span>;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="text-muted-foreground">[]</span>;
    return (
      <span className="inline">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="text-muted-foreground hover:text-foreground inline-flex items-center"
        >
          {isExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
          <span className="text-yellow-400 ml-1">[{value.length}]</span>
        </button>
        {isExpanded && (
          <div className="ml-4 border-l border-border/50 pl-2">
            {value.map((item, i) => (
              <div key={i} className="py-0.5">
                <span className="text-muted-foreground mr-2">{i}:</span>
                <JsonValue value={item} depth={depth + 1} />
              </div>
            ))}
          </div>
        )}
      </span>
    );
  }

  if (typeof value === "object") {
    const keys = Object.keys(value);
    if (keys.length === 0) return <span className="text-muted-foreground">{"{}"}</span>;
    return (
      <span className="inline">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="text-muted-foreground hover:text-foreground inline-flex items-center"
        >
          {isExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
          <span className="text-blue-400 ml-1">{"{"}...{"}"}</span>
        </button>
        {isExpanded && (
          <div className="ml-4 border-l border-border/50 pl-2">
            {keys.map((key) => (
              <div key={key} className="py-0.5">
                <span className="text-pink-400">{key}</span>
                <span className="text-muted-foreground">: </span>
                <JsonValue value={value[key]} depth={depth + 1} />
              </div>
            ))}
          </div>
        )}
      </span>
    );
  }

  return <span>{String(value)}</span>;
};

// Copy Button Component
const CopyButton = ({ text }: { text: string }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Button variant="ghost" size="sm" onClick={handleCopy} className="h-6 px-2">
      {copied ? <Check className="h-3 w-3 text-green-400" /> : <Copy className="h-3 w-3" />}
    </Button>
  );
};

const Transactions = () => {
  const {
    data: events,
    isLoading,
    isError,
    refetch,
  } = useLocalTransactions(100);

  const getEventTypeColor = (eventType: string) => {
    if (eventType === "created") {
      return "bg-green-500/20 text-green-400 border-green-500/30";
    }
    if (eventType === "archived") {
      return "bg-red-500/20 text-red-400 border-red-500/30";
    }
    return "bg-muted text-muted-foreground border-border";
  };

  const getTemplateColor = (templateId: string | null) => {
    if (!templateId) return "bg-muted text-muted-foreground";
    if (templateId.includes("Amulet:Amulet") && !templateId.includes("Locked")) {
      return "bg-emerald-500/20 text-emerald-400";
    }
    if (templateId.includes("LockedAmulet")) return "bg-amber-500/20 text-amber-400";
    if (templateId.includes("ValidatorRewardCoupon")) return "bg-purple-500/20 text-purple-400";
    if (templateId.includes("AppRewardCoupon")) return "bg-orange-500/20 text-orange-400";
    if (templateId.includes("SvRewardCoupon")) return "bg-teal-500/20 text-teal-400";
    if (templateId.includes("Transfer")) return "bg-blue-500/20 text-blue-400";
    return "bg-slate-500/20 text-slate-400";
  };

  const extractTemplateName = (templateId: string | null) => {
    if (!templateId) return "Unknown";
    const parts = templateId.split(":");
    return parts[parts.length - 1] || templateId;
  };

  const formatTimestamp = (ts: string | null, label?: string) => {
    if (!ts) return "N/A";
    try {
      const formatted = new Date(ts).toLocaleString();
      return label ? `${label}: ${formatted}` : formatted;
    } catch {
      return ts;
    }
  };

  // Helper to get the display timestamp - prefer effective_at (actual transaction time)
  const getDisplayTimestamp = (event: LocalEvent) => {
    return event.effective_at || event.timestamp;
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold mb-2">Transaction History</h2>
            <p className="text-muted-foreground">
              Browse transactions from local binary data
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="flex items-center gap-1">
              <Database className="h-3 w-3" />
              Local DuckDB
            </Badge>
            <Button variant="outline" size="sm" onClick={() => refetch()}>
              <RefreshCw className="h-4 w-4 mr-1" />
              Refresh
            </Button>
          </div>
        </div>

        {isLoading ? (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-40 w-full" />
            ))}
          </div>
        ) : isError ? (
          <Card className="glass-card">
            <div className="h-48 flex flex-col items-center justify-center text-center space-y-3 text-muted-foreground">
              <p className="font-medium">Unable to load transactions</p>
              <p className="text-xs">Make sure the local DuckDB server is running on port 3001.</p>
              <Button variant="outline" size="sm" onClick={() => refetch()}>
                Retry
              </Button>
            </div>
          </Card>
        ) : !events?.length ? (
          <Card className="glass-card">
            <div className="h-48 flex flex-col items-center justify-center text-muted-foreground">
              <Database className="h-12 w-12 mb-4 opacity-50" />
              <p>No transactions found</p>
              <p className="text-xs mt-1">Check that binary data exists in the data/raw folder</p>
            </div>
          </Card>
        ) : (
          <div className="space-y-4">
            <p className="text-sm text-muted-foreground">
              Showing {events.length} events
            </p>
            
            {events.map((event, idx) => (
              <Card key={event.event_id || idx} className="glass-card overflow-hidden">
                <CardHeader className="pb-3 bg-muted/30">
                  <div className="flex items-center justify-between flex-wrap gap-2">
                    <div className="flex items-center gap-2 flex-wrap">
                      <Badge className={getEventTypeColor(event.event_type)}>
                        {event.event_type}
                      </Badge>
                      <Badge className={getTemplateColor(event.template_id)}>
                        {extractTemplateName(event.template_id)}
                      </Badge>
                    </div>
                    <div className="text-right">
                      <span className="text-sm text-muted-foreground">
                        {formatTimestamp(getDisplayTimestamp(event))}
                      </span>
                      {event.effective_at && event.timestamp && event.effective_at !== event.timestamp && (
                        <span className="block text-xs text-muted-foreground/60" title="When the file was written">
                          (written {formatTimestamp(event.timestamp)})
                        </span>
                      )}
                    </div>
                  </div>
                </CardHeader>
                
                <CardContent className="p-4 space-y-4">
                  {/* Key Fields */}
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 text-sm">
                    <div className="space-y-1">
                      <div className="flex items-center justify-between">
                        <span className="text-muted-foreground text-xs">Event ID</span>
                        <CopyButton text={event.event_id || ""} />
                      </div>
                      <p className="font-mono text-xs bg-muted/50 p-2 rounded break-all">
                        {event.event_id || "N/A"}
                      </p>
                    </div>
                    <div className="space-y-1">
                      <div className="flex items-center justify-between">
                        <span className="text-muted-foreground text-xs">Contract ID</span>
                        <CopyButton text={event.contract_id || ""} />
                      </div>
                      <p className="font-mono text-xs bg-muted/50 p-2 rounded break-all">
                        {event.contract_id || "N/A"}
                      </p>
                    </div>
                    {event.update_id && (
                      <div className="space-y-1">
                        <div className="flex items-center justify-between">
                          <span className="text-muted-foreground text-xs">Update ID</span>
                          <CopyButton text={event.update_id || ""} />
                        </div>
                        <p className="font-mono text-xs bg-muted/50 p-2 rounded break-all">
                          {event.update_id}
                        </p>
                      </div>
                    )}
                    {event.party && (
                      <div className="space-y-1">
                        <div className="flex items-center justify-between">
                          <span className="text-muted-foreground text-xs">Party</span>
                          <CopyButton text={event.party || ""} />
                        </div>
                        <p className="font-mono text-xs bg-muted/50 p-2 rounded break-all">
                          {event.party}
                        </p>
                      </div>
                    )}
                  </div>

                  {/* Template ID */}
                  <div className="space-y-1">
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground text-xs">Template ID</span>
                      <CopyButton text={event.template_id || ""} />
                    </div>
                    <p className="font-mono text-xs bg-muted/50 p-2 rounded break-all">
                      {event.template_id || "N/A"}
                    </p>
                  </div>

                  {/* Full Event JSON Viewer */}
                  <Collapsible defaultOpen>
                    <CollapsibleTrigger asChild>
                      <Button variant="outline" size="sm" className="w-full justify-between">
                        <span className="flex items-center">
                          <ChevronDown className="h-4 w-4 mr-2" />
                          View Full Event Data
                        </span>
                        <span className="text-xs text-muted-foreground">
                          {Object.keys(event).length} fields
                        </span>
                      </Button>
                    </CollapsibleTrigger>
                    <CollapsibleContent className="mt-3">
                      <div className="bg-muted/30 border border-border rounded-lg p-4 font-mono text-xs overflow-x-auto">
                        <JsonValue value={event} />
                      </div>
                      {/* Raw JSON Toggle */}
                      <Collapsible className="mt-2">
                        <CollapsibleTrigger asChild>
                          <Button variant="ghost" size="sm" className="text-xs">
                            <ChevronRight className="h-3 w-3 mr-1" />
                            Show Raw JSON
                          </Button>
                        </CollapsibleTrigger>
                        <CollapsibleContent className="mt-2">
                          <div className="relative">
                            <pre className="text-xs bg-black/50 text-green-400 p-4 rounded-lg overflow-auto max-h-96 border border-border">
                              {JSON.stringify(event, null, 2)}
                            </pre>
                            <div className="absolute top-2 right-2">
                              <CopyButton text={JSON.stringify(event, null, 2)} />
                            </div>
                          </div>
                        </CollapsibleContent>
                      </Collapsible>
                    </CollapsibleContent>
                  </Collapsible>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>
    </DashboardLayout>
  );
};

export default Transactions;
