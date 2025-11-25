import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ArrowRight, Code } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { format } from "date-fns";

const Transactions = () => {
  const {
    data: events,
    isLoading,
    isError,
    refetch,
  } = useQuery({
    queryKey: ["ledgerEvents", "transactions"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from("ledger_events")
        .select("*")
        .like("template_id", "%Amulet%")
        .order("created_at", { ascending: false })
        .limit(100);
      if (error) throw error;
      return data;
    },
  });

  const getStatusColor = (status: string) => {
    switch (status) {
      case "confirmed":
        return "bg-success/10 text-success border-success/20";
      case "pending":
        return "bg-warning/10 text-warning border-warning/20";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const getTypeColor = (type: string) => {
    switch (type) {
      case "transfer":
        return "bg-primary/10 text-primary border-primary/20";
      case "mint":
        return "bg-accent/10 text-accent border-accent/20";
      case "tap":
        return "bg-chart-3/10 text-chart-3 border-chart-3/20";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const formatPartyId = (partyId: string) => {
    if (!partyId) return "N/A";
    const parts = partyId.split("::");
    const name = parts[0] || partyId;
    const hash = parts[1] || "";
    return `${name}::${hash.substring(0, 8)}...`;
  };

  const parseEventPayload = (event: any) => {
    const data = event.event_data || event.payload || {};
    const createArgs = data.create_arguments?.record?.fields || [];
    const exerciseArgs = data.exercise_arguments?.record?.fields || [];
    
    // For Amulet created events (mints)
    if (event.event_type === "created_event" && event.template_id?.includes("Amulet:Amulet")) {
      const amountField = createArgs.find((f: any) => f.value?.record?.fields?.[0]?.value?.numeric);
      const amount = amountField?.value?.record?.fields?.[0]?.value?.numeric || "0";
      return {
        type: "mint",
        amount,
        from: null,
        to: data.signatories?.[1] || null,
        fee: "0",
      };
    }
    
    // For transfer/exercise events
    if (event.event_type === "exercised_event") {
      const amount = exerciseArgs.find((f: any) => f.value?.numeric)?.value?.numeric || "0";
      return {
        type: "transfer",
        amount,
        from: data.signatories?.[0] || null,
        to: data.signatories?.[1] || null,
        fee: "0",
      };
    }
    
    return {
      type: "unknown",
      amount: "0",
      from: null,
      to: null,
      fee: "0",
    };
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold mb-2">Transaction History</h2>
            <p className="text-muted-foreground">Browse recent transactions on the Canton Network</p>
          </div>
        </div>

        <Card className="glass-card">
          <div className="p-6">
            {isLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-48 w-full" />
                ))}
              </div>
            ) : isError ? (
              <div className="h-48 flex flex-col items-center justify-center text-center space-y-3 text-muted-foreground">
                <p className="font-medium">Unable to load transactions</p>
                <p className="text-xs">The API may be temporarily unavailable. Please try again.</p>
                <button
                  onClick={() => refetch()}
                  className="inline-flex items-center justify-center rounded-md px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 transition-smooth"
                >
                  Retry
                </button>
              </div>
            ) : !events?.length ? (
              <div className="h-48 flex items-center justify-center text-muted-foreground">No recent transactions</div>
            ) : (
              <div className="space-y-4">
                {events.map((event) => {
                  const tx = parseEventPayload(event);
                  return (
                    <Card
                      key={event.id}
                      className="p-6 hover:shadow-lg transition-smooth"
                    >
                      <div className="flex items-start justify-between mb-6">
                        <div className="flex items-center space-x-3">
                          <Badge className={getTypeColor(tx.type)}>{tx.type}</Badge>
                          <Badge className={getStatusColor("confirmed")}>confirmed</Badge>
                          <Badge variant="outline">{event.event_type}</Badge>
                        </div>
                        <div className="text-right">
                          <p className="text-sm text-muted-foreground">Migration</p>
                          <p className="font-mono font-semibold">{event.migration_id || "N/A"}</p>
                        </div>
                      </div>

                      {/* Primary Transaction Details */}
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                        <div className="p-3 rounded-lg bg-muted/30">
                          <p className="text-sm text-muted-foreground mb-1">Event ID</p>
                          <div className="flex items-center space-x-2">
                            <p className="font-mono text-xs break-all">{event.event_id || event.id}</p>
                          </div>
                        </div>
                        <div className="p-3 rounded-lg bg-primary/10">
                          <p className="text-sm text-muted-foreground mb-1">Amount</p>
                          <p className="font-mono font-bold text-primary text-2xl">
                            {parseFloat(tx.amount).toFixed(4)} CC
                          </p>
                        </div>
                        <div className="p-3 rounded-lg bg-muted/30">
                          <p className="text-sm text-muted-foreground mb-1">Contract ID</p>
                          <p className="font-mono text-xs break-all">{event.contract_id || "N/A"}</p>
                        </div>
                      </div>

                      {/* Template & Package Info */}
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                        <div className="p-3 rounded-lg bg-muted/30">
                          <p className="text-sm text-muted-foreground mb-1">Template ID</p>
                          <p className="font-mono text-xs break-all">{event.template_id || "N/A"}</p>
                        </div>
                        <div className="p-3 rounded-lg bg-muted/30">
                          <p className="text-sm text-muted-foreground mb-1">Package Name</p>
                          <p className="font-mono text-xs">{event.package_name || "N/A"}</p>
                        </div>
                      </div>

                      {/* Party Information */}
                      {(event.signatories?.length > 0 || event.observers?.length > 0 || (tx.from && tx.to)) && (
                        <div className="mb-6 p-4 rounded-lg bg-background/50 border border-border/30">
                          <p className="text-sm font-semibold mb-3">Parties</p>
                          
                          {tx.from && tx.to && (
                            <div className="flex items-center space-x-3 mb-3">
                              <div className="flex-1 p-2 rounded bg-muted/30">
                                <p className="text-xs text-muted-foreground mb-1">From</p>
                                <p className="font-mono text-xs break-all">{tx.from}</p>
                              </div>
                              <ArrowRight className="h-4 w-4 text-primary flex-shrink-0" />
                              <div className="flex-1 p-2 rounded bg-muted/30">
                                <p className="text-xs text-muted-foreground mb-1">To</p>
                                <p className="font-mono text-xs break-all">{tx.to}</p>
                              </div>
                            </div>
                          )}

                          {event.signatories?.length > 0 && (
                            <div className="mb-2">
                              <p className="text-xs text-muted-foreground mb-1">Signatories ({event.signatories.length})</p>
                              <div className="space-y-1">
                                {event.signatories.map((sig: string, idx: number) => (
                                  <p key={idx} className="font-mono text-xs bg-muted/30 p-2 rounded break-all">{sig}</p>
                                ))}
                              </div>
                            </div>
                          )}

                          {event.observers?.length > 0 && (
                            <div>
                              <p className="text-xs text-muted-foreground mb-1">Observers ({event.observers.length})</p>
                              <div className="space-y-1">
                                {event.observers.map((obs: string, idx: number) => (
                                  <p key={idx} className="font-mono text-xs bg-muted/30 p-2 rounded break-all">{obs}</p>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      )}

                      {/* Timestamps */}
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
                        <div className="p-2 rounded bg-muted/20">
                          <p className="text-xs text-muted-foreground">Timestamp</p>
                          <p className="text-xs font-mono">{format(new Date(event.timestamp), "MMM d, yyyy HH:mm:ss")}</p>
                        </div>
                        {event.created_at && (
                          <div className="p-2 rounded bg-muted/20">
                            <p className="text-xs text-muted-foreground">Created At</p>
                            <p className="text-xs font-mono">{format(new Date(event.created_at), "MMM d, yyyy HH:mm:ss")}</p>
                          </div>
                        )}
                        {event.created_at_ts && (
                          <div className="p-2 rounded bg-muted/20">
                            <p className="text-xs text-muted-foreground">Created At (TS)</p>
                            <p className="text-xs font-mono">{format(new Date(event.created_at_ts), "MMM d, yyyy HH:mm:ss")}</p>
                          </div>
                        )}
                      </div>

                      {/* Update ID */}
                      {event.update_id && (
                        <div className="p-3 rounded-lg bg-muted/30 mb-4">
                          <p className="text-sm text-muted-foreground mb-1">Update ID</p>
                          <p className="font-mono text-xs break-all">{event.update_id}</p>
                        </div>
                      )}

                      {/* Raw Event Data Collapsible */}
                      <Collapsible>
                        <CollapsibleTrigger asChild>
                          <Button variant="outline" size="sm" className="w-full">
                            <Code className="h-4 w-4 mr-2" />
                            View Full Event Data
                          </Button>
                        </CollapsibleTrigger>
                        <CollapsibleContent className="mt-4">
                          <div className="p-4 rounded-lg bg-background/70 border border-border/50 max-h-96 overflow-auto">
                            <p className="text-xs text-muted-foreground mb-2 font-semibold">Complete Event JSON:</p>
                            <pre className="text-xs overflow-x-auto">{JSON.stringify(event, null, 2)}</pre>
                          </div>
                        </CollapsibleContent>
                      </Collapsible>
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
