import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Loader2, Search, FileText, CheckCircle2, XCircle, Clock, AlertTriangle } from "lucide-react";
import { getDuckDBApiUrl } from "@/lib/backend-config";

interface DebugResult {
  contractId: string;
  totalEventsFound: number;
  dedupedCount: number;
  createdEvent: {
    event_id: string;
    event_type: string;
    template_id: string;
    effective_at: string;
    timestamp: string;
    file: string | null;
  } | null;
  exercisedEvents: Array<{
    event_id: string;
    event_type: string;
    choice: string;
    template_id: string;
    effective_at: string;
    file: string | null;
    isClosingChoice: boolean;
  }>;
  closingEventUsed: {
    event_id: string;
    choice: string;
    template_id: string;
    effective_at: string;
    file: string | null;
    exerciseResult: unknown;
  } | null;
  parsedPayloadFields: {
    action: unknown;
    requester: string | null;
    reason: unknown;
    votes: unknown;
    voteBefore: string | null;
    targetEffectiveAt: string | null;
    trackingCid: string | null;
    dso: string | null;
  };
  indexedRecord: {
    event_id: string;
    status: string;
    is_closed: boolean;
    action_tag: string;
    vote_count: number;
    vote_before: string;
    requester: string;
    reason: string;
  } | null;
}

const fetchDebugInfo = async (contractId: string): Promise<DebugResult> => {
  const res = await fetch(`${getDuckDBApiUrl()}/api/events/debug-vote-request/${encodeURIComponent(contractId)}`);
  if (!res.ok) throw new Error("Failed to fetch debug info");
  return res.json();
};

export function VoteRequestDebugPanel() {
  const [contractId, setContractId] = useState("");
  const [searchId, setSearchId] = useState("");

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ["voteRequestDebug", searchId],
    queryFn: () => fetchDebugInfo(searchId),
    enabled: !!searchId,
    retry: false,
  });

  const handleSearch = () => {
    if (contractId.trim()) {
      setSearchId(contractId.trim());
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      handleSearch();
    }
  };

  return (
    <Card className="mt-6">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Search className="w-5 h-5" />
          VoteRequest Debug Panel
        </CardTitle>
        <CardDescription>
          Enter a contract_id to see raw events, closing choice, and parsed payload
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Search Input */}
        <div className="flex gap-2">
          <Input
            placeholder="Enter contract_id (e.g., 00abc123...)"
            value={contractId}
            onChange={(e) => setContractId(e.target.value)}
            onKeyDown={handleKeyDown}
            className="font-mono text-sm"
          />
          <Button onClick={handleSearch} disabled={isLoading || !contractId.trim()}>
            {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Debug"}
          </Button>
        </div>

        {/* Error State */}
        {error && (
          <div className="p-4 rounded-lg bg-destructive/10 border border-destructive/30 text-destructive">
            <p className="text-sm">{(error as Error).message}</p>
          </div>
        )}

        {/* Results */}
        {data && (
          <div className="space-y-4">
            {/* Summary Stats */}
            <div className="grid grid-cols-3 gap-3">
              <div className="bg-muted/50 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Total Events</p>
                <p className="text-lg font-semibold">{data.totalEventsFound}</p>
              </div>
              <div className="bg-muted/50 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">After Dedup</p>
                <p className="text-lg font-semibold">{data.dedupedCount}</p>
              </div>
              <div className="bg-muted/50 rounded-lg p-3">
                <p className="text-xs text-muted-foreground">Exercised Events</p>
                <p className="text-lg font-semibold">{data.exercisedEvents.length}</p>
              </div>
            </div>

            {/* Created Event */}
            <div className="space-y-2">
              <h4 className="text-sm font-medium flex items-center gap-2">
                <CheckCircle2 className="w-4 h-4 text-success" />
                Created Event
              </h4>
              {data.createdEvent ? (
                <div className="bg-success/10 border border-success/30 rounded-lg p-3 text-sm">
                  <div className="grid grid-cols-2 gap-2">
                    <span className="text-muted-foreground">Event ID:</span>
                    <span className="font-mono text-xs truncate">{data.createdEvent.event_id}</span>
                    <span className="text-muted-foreground">Template:</span>
                    <span className="font-mono text-xs">{data.createdEvent.template_id?.split(":").pop()}</span>
                    <span className="text-muted-foreground">Effective At:</span>
                    <span>{data.createdEvent.effective_at}</span>
                    {data.createdEvent.file && (
                      <>
                        <span className="text-muted-foreground">File:</span>
                        <span className="font-mono text-xs truncate">{data.createdEvent.file}</span>
                      </>
                    )}
                  </div>
                </div>
              ) : (
                <div className="text-sm text-muted-foreground italic">No created event found</div>
              )}
            </div>

            {/* Closing Event */}
            <div className="space-y-2">
              <h4 className="text-sm font-medium flex items-center gap-2">
                <XCircle className="w-4 h-4 text-destructive" />
                Closing Event Used
              </h4>
              {data.closingEventUsed ? (
                <div className="bg-destructive/10 border border-destructive/30 rounded-lg p-3 text-sm">
                  <div className="grid grid-cols-2 gap-2">
                    <span className="text-muted-foreground">Choice:</span>
                    <Badge variant="outline" className="w-fit">{data.closingEventUsed.choice}</Badge>
                    <span className="text-muted-foreground">Event ID:</span>
                    <span className="font-mono text-xs truncate">{data.closingEventUsed.event_id}</span>
                    <span className="text-muted-foreground">Effective At:</span>
                    <span>{data.closingEventUsed.effective_at}</span>
                    {data.closingEventUsed.file && (
                      <>
                        <span className="text-muted-foreground">File:</span>
                        <span className="font-mono text-xs truncate">{data.closingEventUsed.file}</span>
                      </>
                    )}
                  </div>
                </div>
              ) : (
                <div className="bg-warning/10 border border-warning/30 rounded-lg p-3 text-sm flex items-center gap-2">
                  <AlertTriangle className="w-4 h-4 text-warning" />
                  <span>No closing event detected (proposal may still be open)</span>
                </div>
              )}
            </div>

            {/* All Exercised Events */}
            <div className="space-y-2">
              <h4 className="text-sm font-medium flex items-center gap-2">
                <FileText className="w-4 h-4" />
                All Exercised Events ({data.exercisedEvents.length})
              </h4>
              <ScrollArea className="h-40 rounded-lg border">
                <div className="p-2 space-y-1">
                  {data.exercisedEvents.length === 0 ? (
                    <p className="text-sm text-muted-foreground italic p-2">No exercised events</p>
                  ) : (
                    data.exercisedEvents.map((e, i) => (
                      <div
                        key={i}
                        className={`p-2 rounded text-xs ${
                          e.isClosingChoice ? "bg-red-500/10 border border-red-500/30" : "bg-muted/50"
                        }`}
                      >
                        <div className="flex items-center justify-between">
                          <Badge variant={e.isClosingChoice ? "destructive" : "secondary"} className="text-xs">
                            {e.choice || "no choice"}
                          </Badge>
                          {e.isClosingChoice && (
                            <Badge variant="outline" className="text-xs">CLOSING</Badge>
                          )}
                        </div>
                        <p className="text-muted-foreground mt-1 font-mono truncate">{e.event_id}</p>
                      </div>
                    ))
                  )}
                </div>
              </ScrollArea>
            </div>

            {/* Parsed Payload Fields */}
            <div className="space-y-2">
              <h4 className="text-sm font-medium">Parsed Payload Fields</h4>
              <ScrollArea className="h-48 rounded-lg border">
                <pre className="p-3 text-xs font-mono overflow-x-auto">
                  {JSON.stringify(data.parsedPayloadFields, null, 2)}
                </pre>
              </ScrollArea>
            </div>

            {/* Indexed Record (from DuckDB) */}
            <div className="space-y-2">
              <h4 className="text-sm font-medium flex items-center gap-2">
                <Clock className="w-4 h-4" />
                Indexed Record (DuckDB)
              </h4>
              {data.indexedRecord ? (
                <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-3">
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <span className="text-muted-foreground">Status:</span>
                    <Badge variant="outline">{data.indexedRecord.status}</Badge>
                    <span className="text-muted-foreground">Is Closed:</span>
                    <span>{data.indexedRecord.is_closed ? "Yes" : "No"}</span>
                    <span className="text-muted-foreground">Action Tag:</span>
                    <span className="font-mono text-xs">{data.indexedRecord.action_tag || "—"}</span>
                    <span className="text-muted-foreground">Vote Count:</span>
                    <span>{data.indexedRecord.vote_count}</span>
                    <span className="text-muted-foreground">Vote Before:</span>
                    <span className="text-xs">{data.indexedRecord.vote_before || "—"}</span>
                    <span className="text-muted-foreground">Requester:</span>
                    <span className="font-mono text-xs truncate">{data.indexedRecord.requester || "—"}</span>
                  </div>
                </div>
              ) : (
                <div className="text-sm text-muted-foreground italic">Not found in index</div>
              )}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}