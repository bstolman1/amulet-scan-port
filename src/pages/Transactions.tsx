import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowRight, Copy, Check } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";
import { Skeleton } from "@/components/ui/skeleton";
import { format } from "date-fns";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { useState } from "react";

const CopyButton = ({ text }: { text: string }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <button
      onClick={handleCopy}
      className="ml-2 p-1 hover:bg-muted rounded transition-colors inline-flex items-center"
      title="Copy to clipboard"
    >
      {copied ? <Check className="h-3 w-3 text-green-500" /> : <Copy className="h-3 w-3" />}
    </button>
  );
};

const JsonViewer = ({ data, title }: { data: any; title: string }) => {
  if (!data) return null;
  
  return (
    <div className="mt-4">
      <h4 className="font-semibold text-sm mb-2 flex items-center">
        {title}
        <CopyButton text={JSON.stringify(data, null, 2)} />
      </h4>
      <pre className="bg-background/80 p-4 rounded-lg text-xs overflow-x-auto max-h-96 overflow-y-auto border border-border/50 font-mono">
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
};

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

  const getTypeColor = (type: string) => {
    switch (type) {
      case "created_event":
        return "bg-accent/10 text-accent border-accent/20";
      case "exercised_event":
        return "bg-primary/10 text-primary border-primary/20";
      case "archived_event":
        return "bg-muted/50 text-muted-foreground border-muted";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const formatPartyId = (partyId: string) => {
    if (!partyId) return "N/A";
    const parts = partyId.split("::");
    const name = parts[0] || partyId;
    const hash = parts[1] || "";
    return hash ? `${name}::${hash.substring(0, 8)}...` : name;
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-3xl font-bold mb-2">Ledger Events - Complete Data</h2>
            <p className="text-muted-foreground">All fields and metadata from Canton ledger events</p>
          </div>
        </div>

        <Card className="glass-card">
          <div className="p-6">
            {isLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-24 w-full" />
                ))}
              </div>
            ) : isError ? (
              <div className="h-48 flex flex-col items-center justify-center text-center space-y-3 text-muted-foreground">
                <p className="font-medium">Unable to load events</p>
                <p className="text-xs">The API may be temporarily unavailable. Please try again.</p>
                <button
                  onClick={() => refetch()}
                  className="inline-flex items-center justify-center rounded-md px-3 py-1.5 text-sm font-medium bg-primary text-primary-foreground hover:bg-primary/90 transition-smooth"
                >
                  Retry
                </button>
              </div>
            ) : !events?.length ? (
              <div className="h-48 flex items-center justify-center text-muted-foreground">No events found</div>
            ) : (
              <Accordion type="single" collapsible className="w-full space-y-3">
                {events.map((event: any) => (
                  <AccordionItem key={event.id} value={event.id} className="border rounded-lg px-4 bg-muted/20">
                    <AccordionTrigger className="hover:no-underline py-4">
                      <div className="flex items-center justify-between w-full pr-4">
                        <div className="flex items-center gap-3">
                          <Badge className={getTypeColor(event.event_type)}>
                            {event.event_type}
                          </Badge>
                          <span className="text-sm font-mono truncate max-w-[300px]">
                            {event.template_id?.split(':').slice(-2).join(':') || 'Unknown'}
                          </span>
                        </div>
                        <span className="text-xs text-muted-foreground">
                          {format(new Date(event.created_at), "MMM d, yyyy HH:mm:ss")}
                        </span>
                      </div>
                    </AccordionTrigger>
                    <AccordionContent className="space-y-6 pt-4 pb-6">
                      {/* Core Identifiers */}
                      <div className="space-y-4">
                        <h3 className="font-bold text-base border-b pb-2">Core Identifiers</h3>
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Database ID</h4>
                            <div className="flex items-center">
                              <code className="text-xs bg-background p-2 rounded break-all flex-1 border">{event.id}</code>
                              <CopyButton text={event.id} />
                            </div>
                          </div>
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Event ID</h4>
                            <div className="flex items-center">
                              <code className="text-xs bg-background p-2 rounded break-all flex-1 border">{event.event_id || 'N/A'}</code>
                              {event.event_id && <CopyButton text={event.event_id} />}
                            </div>
                          </div>
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Contract ID</h4>
                            <div className="flex items-center">
                              <code className="text-xs bg-background p-2 rounded break-all flex-1 border">{event.contract_id || 'N/A'}</code>
                              {event.contract_id && <CopyButton text={event.contract_id} />}
                            </div>
                          </div>
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Update ID</h4>
                            <div className="flex items-center">
                              <code className="text-xs bg-background p-2 rounded break-all flex-1 border">{event.update_id || 'N/A'}</code>
                              {event.update_id && <CopyButton text={event.update_id} />}
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Template & Package Info */}
                      <div className="space-y-4">
                        <h3 className="font-bold text-base border-b pb-2">Template & Package Information</h3>
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Template ID</h4>
                            <div className="flex items-center">
                              <code className="text-xs bg-background p-2 rounded break-all flex-1 border">{event.template_id || 'N/A'}</code>
                              {event.template_id && <CopyButton text={event.template_id} />}
                            </div>
                          </div>
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Package Name</h4>
                            <div className="flex items-center">
                              <code className="text-xs bg-background p-2 rounded break-all flex-1 border">{event.package_name || 'N/A'}</code>
                              {event.package_name && <CopyButton text={event.package_name} />}
                            </div>
                          </div>
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Event Type</h4>
                            <Badge className={getTypeColor(event.event_type)}>{event.event_type}</Badge>
                          </div>
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Migration ID</h4>
                            <code className="text-xs bg-background p-2 rounded block border">{event.migration_id || 'N/A'}</code>
                          </div>
                        </div>
                      </div>

                      {/* Timestamps */}
                      <div className="space-y-4">
                        <h3 className="font-bold text-base border-b pb-2">Timestamps</h3>
                        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Event Timestamp</h4>
                            <code className="text-xs bg-background p-2 rounded block border">
                              {format(new Date(event.timestamp), "yyyy-MM-dd HH:mm:ss.SSS")}
                            </code>
                          </div>
                          <div>
                            <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Created At (DB)</h4>
                            <code className="text-xs bg-background p-2 rounded block border">
                              {format(new Date(event.created_at), "yyyy-MM-dd HH:mm:ss.SSS")}
                            </code>
                          </div>
                          {event.created_at_ts && (
                            <div>
                              <h4 className="font-semibold text-sm mb-2 text-muted-foreground">Created At TS</h4>
                              <code className="text-xs bg-background p-2 rounded block border">
                                {format(new Date(event.created_at_ts), "yyyy-MM-dd HH:mm:ss.SSS")}
                              </code>
                            </div>
                          )}
                        </div>
                      </div>

                      {/* Parties (Signatories & Observers) */}
                      {(event.signatories?.length > 0 || event.observers?.length > 0) && (
                        <div className="space-y-4">
                          <h3 className="font-bold text-base border-b pb-2">Parties</h3>
                          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                            {event.signatories && event.signatories.length > 0 && (
                              <div>
                                <h4 className="font-semibold text-sm mb-2 text-muted-foreground">
                                  Signatories ({event.signatories.length})
                                </h4>
                                <div className="space-y-2">
                                  {event.signatories.map((sig: string, idx: number) => (
                                    <div key={idx} className="flex items-center gap-2">
                                      <code className="text-xs bg-background p-2 rounded block break-all flex-1 border">
                                        {sig}
                                      </code>
                                      <CopyButton text={sig} />
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                            {event.observers && event.observers.length > 0 && (
                              <div>
                                <h4 className="font-semibold text-sm mb-2 text-muted-foreground">
                                  Observers ({event.observers.length})
                                </h4>
                                <div className="space-y-2">
                                  {event.observers.map((obs: string, idx: number) => (
                                    <div key={idx} className="flex items-center gap-2">
                                      <code className="text-xs bg-background p-2 rounded block break-all flex-1 border">
                                        {obs}
                                      </code>
                                      <CopyButton text={obs} />
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        </div>
                      )}

                      {/* JSON Data Sections */}
                      <div className="space-y-4">
                        <h3 className="font-bold text-base border-b pb-2">Raw Data & Payloads</h3>
                        <JsonViewer data={event.event_data} title="Event Data (Primary)" />
                        {event.payload && <JsonViewer data={event.payload} title="Payload" />}
                        {event.raw && <JsonViewer data={event.raw} title="Raw Event Data (Complete)" />}
                      </div>
                    </AccordionContent>
                  </AccordionItem>
                ))}
              </Accordion>
            )}
          </div>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Transactions;
