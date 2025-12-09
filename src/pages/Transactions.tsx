import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ChevronDown, Database, RefreshCw } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useLocalTransactions, LocalEvent } from "@/hooks/use-local-events";

const Transactions = () => {
  const {
    data: events,
    isLoading,
    isError,
    refetch,
  } = useLocalTransactions(100);

  const getEventTypeColor = (eventType: string, templateId: string) => {
    if (templateId?.includes("Amulet:Amulet") && !templateId?.includes("Locked")) {
      return "bg-emerald-500/20 text-emerald-400 border-emerald-500/30";
    }
    if (templateId?.includes("LockedAmulet")) {
      return "bg-amber-500/20 text-amber-400 border-amber-500/30";
    }
    if (templateId?.includes("ValidatorRewardCoupon")) {
      return "bg-purple-500/20 text-purple-400 border-purple-500/30";
    }
    if (templateId?.includes("AppRewardCoupon")) {
      return "bg-orange-500/20 text-orange-400 border-orange-500/30";
    }
    if (templateId?.includes("SvRewardCoupon")) {
      return "bg-teal-500/20 text-teal-400 border-teal-500/30";
    }
    if (eventType === "created") {
      return "bg-green-500/20 text-green-400 border-green-500/30";
    }
    if (eventType === "archived") {
      return "bg-red-500/20 text-red-400 border-red-500/30";
    }
    return "bg-muted text-muted-foreground border-border";
  };

  const getEventTypeName = (event: LocalEvent) => {
    const template = event.template_id || "";
    if (template.includes("Amulet:Amulet") && !template.includes("Locked")) return "Amulet Created";
    if (template.includes("LockedAmulet")) return "Locked Amulet";
    if (template.includes("ValidatorRewardCoupon")) return "Validator Reward";
    if (template.includes("AppRewardCoupon")) return "App Reward";
    if (template.includes("SvRewardCoupon")) return "SV Reward";
    if (template.includes("Transfer")) return "Transfer";
    // Extract template name from full ID
    const parts = template.split(":");
    return parts[parts.length - 1] || event.event_type || "Event";
  };

  const extractAmount = (payload: any): string | null => {
    if (!payload) return null;
    
    // Try common paths for amounts
    const paths = [
      payload?.amount?.amount,
      payload?.amount?.initialAmount,
      payload?.initialAmount,
      payload?.amulet?.amount?.initialAmount,
    ];
    
    for (const value of paths) {
      if (value !== undefined && value !== null) {
        const num = parseFloat(value);
        if (!isNaN(num)) {
          return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 6 });
        }
      }
    }
    return null;
  };

  const formatTimestamp = (ts: string | null) => {
    if (!ts) return "N/A";
    try {
      return new Date(ts).toLocaleString();
    } catch {
      return ts;
    }
  };

  const truncateId = (id: string | null, chars = 16) => {
    if (!id) return "N/A";
    if (id.length <= chars * 2) return id;
    return `${id.slice(0, chars)}...${id.slice(-chars)}`;
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold mb-2">Transaction History</h2>
            <p className="text-muted-foreground">
              Browse transactions from local backfill data
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

        <Card className="glass-card">
          <div className="p-6">
            {isLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : isError ? (
              <div className="h-48 flex flex-col items-center justify-center text-center space-y-3 text-muted-foreground">
                <p className="font-medium">Unable to load transactions</p>
                <p className="text-xs">Make sure the local DuckDB server is running on port 3001.</p>
                <Button variant="outline" size="sm" onClick={() => refetch()}>
                  Retry
                </Button>
              </div>
            ) : !events?.length ? (
              <div className="h-48 flex flex-col items-center justify-center text-muted-foreground">
                <Database className="h-12 w-12 mb-4 opacity-50" />
                <p>No transactions found</p>
                <p className="text-xs mt-1">Check that binary data exists in the data/raw folder</p>
              </div>
            ) : (
              <div className="space-y-4">
                <p className="text-sm text-muted-foreground">
                  Showing {events.length} events
                </p>
                {events.map((event, idx) => {
                  const amount = extractAmount(event.payload);
                  const eventTypeName = getEventTypeName(event);

                  return (
                    <Card key={event.event_id || idx} className="glass-card hover:shadow-lg transition-smooth">
                      <CardHeader className="pb-3">
                        <div className="flex items-center justify-between">
                          <div className="space-y-2">
                            <div className="flex items-center gap-2">
                              <Badge className={getEventTypeColor(event.event_type, event.template_id)}>
                                {eventTypeName}
                              </Badge>
                              <Badge variant="outline" className="text-xs">
                                {event.event_type}
                              </Badge>
                            </div>
                            {amount && (
                              <CardTitle className="text-2xl font-bold">
                                <span className="font-mono text-primary">
                                  {amount} <span className="text-lg text-muted-foreground">CC</span>
                                </span>
                              </CardTitle>
                            )}
                          </div>
                          <div className="text-right">
                            <p className="text-sm text-muted-foreground">
                              {formatTimestamp(event.timestamp)}
                            </p>
                          </div>
                        </div>
                      </CardHeader>
                      <CardContent className="space-y-4">
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                          <div>
                            <p className="text-muted-foreground text-xs mb-1">Contract ID</p>
                            <p className="font-mono text-xs text-foreground">
                              {truncateId(event.contract_id)}
                            </p>
                          </div>
                          <div>
                            <p className="text-muted-foreground text-xs mb-1">Event ID</p>
                            <p className="font-mono text-xs text-foreground">
                              {truncateId(event.event_id)}
                            </p>
                          </div>
                          {event.party && (
                            <div className="md:col-span-2">
                              <p className="text-muted-foreground text-xs mb-1">Party</p>
                              <p className="font-mono text-xs text-foreground break-all">
                                {event.party}
                              </p>
                            </div>
                          )}
                          <div className="md:col-span-2">
                            <p className="text-muted-foreground text-xs mb-1">Template</p>
                            <p className="font-mono text-xs text-foreground break-all">
                              {event.template_id || "N/A"}
                            </p>
                          </div>
                        </div>

                        {/* Collapsible Raw Data */}
                        {event.payload && (
                          <Collapsible>
                            <CollapsibleTrigger asChild>
                              <Button variant="outline" size="sm" className="w-full">
                                <ChevronDown className="h-4 w-4 mr-2" />
                                View Payload
                              </Button>
                            </CollapsibleTrigger>
                            <CollapsibleContent className="mt-4">
                              <pre className="text-xs bg-muted/50 p-4 rounded-lg overflow-auto max-h-96 border border-border">
                                {JSON.stringify(event.payload, null, 2)}
                              </pre>
                            </CollapsibleContent>
                          </Collapsible>
                        )}
                      </CardContent>
                    </Card>
                  );
                })}
              </div>
            )}
          </div>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Transactions;
