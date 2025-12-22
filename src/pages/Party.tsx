import { useParams, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { ArrowLeft, Copy, ExternalLink, Activity, FileText, Clock, Hash } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { getPartyEvents, getPartySummary, LedgerEvent } from "@/lib/duckdb-api-client";
import { format } from "date-fns";

const Party = () => {
  const { partyId } = useParams<{ partyId: string }>();
  const navigate = useNavigate();
  const { toast } = useToast();
  const decodedPartyId = partyId ? decodeURIComponent(partyId) : "";

  const { data: eventsData, isLoading: eventsLoading, error: eventsError } = useQuery({
    queryKey: ["party-events", decodedPartyId],
    queryFn: () => getPartyEvents(decodedPartyId, 500),
    enabled: !!decodedPartyId,
  });

  const { data: summaryData, isLoading: summaryLoading } = useQuery({
    queryKey: ["party-summary", decodedPartyId],
    queryFn: () => getPartySummary(decodedPartyId),
    enabled: !!decodedPartyId,
  });

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    toast({ title: "Copied to clipboard" });
  };

  const formatDate = (dateStr: string) => {
    try {
      return format(new Date(dateStr), "MMM d, yyyy HH:mm:ss");
    } catch {
      return dateStr;
    }
  };

  const getEventTypeColor = (type: string) => {
    switch (type) {
      case "created":
        return "bg-green-500/20 text-green-400 border-green-500/30";
      case "archived":
        return "bg-red-500/20 text-red-400 border-red-500/30";
      case "exercised":
        return "bg-blue-500/20 text-blue-400 border-blue-500/30";
      default:
        return "bg-muted text-muted-foreground";
    }
  };

  const getTemplateName = (templateId: string) => {
    if (!templateId) return "Unknown";
    const parts = templateId.split(":");
    return parts[parts.length - 1] || templateId;
  };

  const events = eventsData?.data || [];
  const summary = summaryData?.data || [];

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-start gap-4">
          <Button variant="ghost" size="icon" onClick={() => navigate(-1)}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div className="flex-1 min-w-0">
            <h1 className="text-2xl font-bold">Party Details</h1>
            <div className="flex items-center gap-2 mt-2">
              <code className="text-sm text-muted-foreground bg-muted/50 px-2 py-1 rounded font-mono break-all">
                {decodedPartyId}
              </code>
              <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => copyToClipboard(decodedPartyId)}>
                <Copy className="h-3 w-3" />
              </Button>
            </div>
          </div>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card className="bg-blue-500/10 border-blue-500/20">
            <CardContent className="p-4">
              <div className="flex items-center gap-2">
                <Activity className="h-4 w-4 text-blue-400" />
                <span className="text-xs text-muted-foreground">Total Events</span>
              </div>
              <div className="text-2xl font-bold text-blue-400 mt-1">
                {eventsLoading ? <Skeleton className="h-8 w-16" /> : events.length}
              </div>
            </CardContent>
          </Card>

          {summary.slice(0, 3).map((s: any) => (
            <Card key={s.event_type} className={`${getEventTypeColor(s.event_type)} bg-opacity-10`}>
              <CardContent className="p-4">
                <div className="flex items-center gap-2">
                  <FileText className="h-4 w-4" />
                  <span className="text-xs text-muted-foreground capitalize">{s.event_type}</span>
                </div>
                <div className="text-2xl font-bold mt-1">{s.count}</div>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Activity Summary */}
        {summary.length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Activity Summary</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-3">
                {summary.map((s: any) => (
                  <div key={s.event_type} className="flex items-center justify-between p-3 rounded-lg bg-muted/30">
                    <div className="flex items-center gap-3">
                      <Badge variant="outline" className={getEventTypeColor(s.event_type)}>
                        {s.event_type}
                      </Badge>
                      <span className="font-medium">{s.count} events</span>
                    </div>
                    <div className="text-xs text-muted-foreground">
                      {s.first_seen && (
                        <span>
                          {formatDate(s.first_seen)} - {formatDate(s.last_seen)}
                        </span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        {/* Events List */}
        <Card>
          <CardHeader>
            <CardTitle className="text-lg flex items-center justify-between">
              <span>Recent Events</span>
              {events.length > 0 && (
                <Badge variant="secondary">{events.length} events</Badge>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {eventsLoading ? (
              <div className="space-y-3">
                {[...Array(5)].map((_, i) => (
                  <Skeleton key={i} className="h-20 w-full" />
                ))}
              </div>
            ) : eventsError ? (
              <div className="text-center py-8 text-destructive">
                <p>Failed to load events</p>
                <p className="text-sm text-muted-foreground mt-1">
                  Make sure the DuckDB server is running locally
                </p>
              </div>
            ) : events.length === 0 ? (
              <div className="text-center py-8 text-muted-foreground">
                <p>No events found for this party</p>
              </div>
            ) : (
              <ScrollArea className="h-[600px]">
                <div className="space-y-3 pr-4">
                  {events.map((event: LedgerEvent, idx: number) => (
                    <div
                      key={event.event_id || idx}
                      className="p-4 rounded-lg border bg-card hover:bg-muted/30 transition-colors"
                    >
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            <Badge variant="outline" className={getEventTypeColor(event.event_type)}>
                              {event.event_type}
                            </Badge>
                            <span className="text-sm font-medium">
                              {getTemplateName(event.template_id)}
                            </span>
                            {event.choice && (
                              <Badge variant="secondary" className="text-xs">
                                {event.choice}
                              </Badge>
                            )}
                          </div>
                          
                          <div className="mt-2 flex items-center gap-4 text-xs text-muted-foreground">
                            <span className="flex items-center gap-1">
                              <Clock className="h-3 w-3" />
                              {formatDate(event.timestamp)}
                            </span>
                            {event.contract_id && (
                              <span className="flex items-center gap-1 font-mono">
                                <Hash className="h-3 w-3" />
                                {event.contract_id.slice(0, 16)}...
                              </span>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </ScrollArea>
            )}
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Party;
