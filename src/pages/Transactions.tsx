import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Code, ChevronDown } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { parseEventData, ParsedField } from "@/lib/daml-field-mapping";

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

  const getEventTypeColor = (eventType: string) => {
    switch (eventType) {
      case "Amulet Created":
        return "bg-emerald-500/20 text-emerald-400 border-emerald-500/30";
      case "Locked Amulet":
        return "bg-amber-500/20 text-amber-400 border-amber-500/30";
      case "Transfer":
        return "bg-blue-500/20 text-blue-400 border-blue-500/30";
      case "Validator Reward":
        return "bg-purple-500/20 text-purple-400 border-purple-500/30";
      case "App Reward":
        return "bg-orange-500/20 text-orange-400 border-orange-500/30";
      case "SV Reward":
        return "bg-teal-500/20 text-teal-400 border-teal-500/30";
      default:
        return "bg-muted text-muted-foreground border-border";
    }
  };

  const groupFieldsByCategory = (fields: ParsedField[]) => {
    const groups: Record<string, ParsedField[]> = {
      amount: [],
      fee: [],
      metadata: [],
      party: [],
    };

    fields.forEach(field => {
      groups[field.category].push(field);
    });

    return groups;
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
              <div className="space-y-6">
                {events.map((event) => {
                  const parsedData = parseEventData(event.event_data, event.template_id || "");
                  const groupedFields = groupFieldsByCategory(parsedData.details);

                  return (
                    <Card key={event.id} className="glass-card hover:shadow-lg transition-smooth">
                      <CardHeader>
                        <div className="flex items-center justify-between">
                          <div className="space-y-2">
                            <div className="flex items-center gap-2">
                              <Badge className={getEventTypeColor(parsedData.eventType)}>
                                {parsedData.eventType}
                              </Badge>
                              <Badge className="bg-green-500/20 text-green-400 border-green-500/30">
                                Completed
                              </Badge>
                            </div>
                            {parsedData.primaryAmount && (
                              <CardTitle className="text-3xl font-bold">
                                <span className="font-mono text-primary">
                                  {parsedData.primaryAmount.value}{" "}
                                  <span className="text-lg text-muted-foreground">
                                    {parsedData.eventType === "SV Reward" ? "Weight" : "CC"}
                                  </span>
                                </span>
                              </CardTitle>
                            )}
                          </div>
                          <div className="text-right">
                            <p className="text-sm text-muted-foreground">
                              {new Date(event.timestamp).toLocaleString()}
                            </p>
                          </div>
                        </div>
                      </CardHeader>
                      <CardContent className="space-y-6">
                        {/* Amounts Section */}
                        {groupedFields.amount.length > 0 && (
                          <div>
                            <h4 className="text-sm font-semibold mb-3 text-foreground">Amounts</h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                              {groupedFields.amount.map((field, idx) => (
                                <div key={idx} className="flex justify-between items-center p-3 rounded-lg bg-primary/5 border border-primary/10">
                                  <span className="text-sm text-muted-foreground">{field.label}</span>
                                  <span className="text-base font-mono font-semibold text-foreground">
                                    {field.value}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Fees Section */}
                        {groupedFields.fee.length > 0 && (
                          <div>
                            <h4 className="text-sm font-semibold mb-3 text-foreground">Fees</h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                              {groupedFields.fee.map((field, idx) => (
                                <div key={idx} className="flex justify-between items-center p-3 rounded-lg bg-muted/50">
                                  <span className="text-sm text-muted-foreground">{field.label}</span>
                                  <span className="text-sm font-mono text-foreground">{field.value}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Parties Section */}
                        {groupedFields.party.length > 0 && (
                          <div>
                            <h4 className="text-sm font-semibold mb-3 text-foreground">Participants</h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                              {groupedFields.party.map((field, idx) => (
                                <div key={idx} className="p-3 rounded-lg bg-muted/30">
                                  <p className="text-xs text-muted-foreground mb-1">{field.label}</p>
                                  <p className="text-sm font-mono text-foreground break-all">{field.value}</p>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Metadata Section */}
                        {groupedFields.metadata.length > 0 && (
                          <div>
                            <h4 className="text-sm font-semibold mb-3 text-foreground">Metadata</h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                              {groupedFields.metadata.map((field, idx) => (
                                <div key={idx} className="flex justify-between items-center p-3 rounded-lg bg-muted/30">
                                  <span className="text-sm text-muted-foreground">{field.label}</span>
                                  <span className="text-sm font-mono text-foreground">{field.value}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Technical Details */}
                        <div className="grid grid-cols-2 gap-4 text-sm pt-4 border-t border-border">
                          <div>
                            <p className="text-muted-foreground text-xs mb-1">Contract ID</p>
                            <p className="font-mono text-xs truncate text-foreground">
                              {event.contract_id || "N/A"}
                            </p>
                          </div>
                          <div>
                            <p className="text-muted-foreground text-xs mb-1">Event ID</p>
                            <p className="font-mono text-xs truncate text-foreground">
                              {event.event_id || event.id}
                            </p>
                          </div>
                        </div>

                        {/* Collapsible Raw Data */}
                        <Collapsible>
                          <CollapsibleTrigger asChild>
                            <Button variant="outline" size="sm" className="w-full">
                              <ChevronDown className="h-4 w-4 mr-2" />
                              View Full Event Data
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-4">
                            <pre className="text-xs bg-muted/50 p-4 rounded-lg overflow-auto max-h-96 border border-border">
                              {JSON.stringify(event.event_data, null, 2)}
                            </pre>
                          </CollapsibleContent>
                        </Collapsible>
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
