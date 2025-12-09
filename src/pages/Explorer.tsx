import { useState, useEffect, useMemo, useCallback } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import {
  Activity,
  Database,
  Filter,
  RefreshCw,
  FileCode,
  Search,
  Clock,
  Layers,
} from "lucide-react";
import { getDuckDBApiUrl } from "@/lib/backend-config";

type UpdateRow = {
  id: string;
  synchronizer: string;
  effective_at: string;
  recorded_at: string;
  transaction_id: string;
  command_id: string;
  workflow_id: string;
  status: string;
};

type EventRow = {
  id: string;
  update_id: string;
  type: string;
  synchronizer: string;
  effective_at: string;
  recorded_at: string;
  contract_id: string;
  party: string;
  template: string;
  payload: any;
};

type TimelinePoint = { t: string; count: number };

type ExplorerStats = {
  ledgerRoot: string;
  updateFiles: number;
  eventFiles: number;
  totalSizeMB: string;
};

const Explorer = () => {
  const [updates, setUpdates] = useState<UpdateRow[]>([]);
  const [selectedUpdate, setSelectedUpdate] = useState<UpdateRow | null>(null);
  const [events, setEvents] = useState<EventRow[]>([]);
  const [timeline, setTimeline] = useState<TimelinePoint[]>([]);
  const [stats, setStats] = useState<ExplorerStats | null>(null);
  const [selectedEvent, setSelectedEvent] = useState<EventRow | null>(null);

  const [loadingUpdates, setLoadingUpdates] = useState(false);
  const [loadingEvents, setLoadingEvents] = useState(false);
  const [loadingTimeline, setLoadingTimeline] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Filters
  const [synchronizerFilter, setSynchronizerFilter] = useState("");
  const [workflowFilter, setWorkflowFilter] = useState("");
  const [partyFilter, setPartyFilter] = useState("");
  const [contractFilter, setContractFilter] = useState("");
  const [templateFilter, setTemplateFilter] = useState("");

  const baseUrl = getDuckDBApiUrl();

  const loadUpdates = useCallback(async () => {
    setLoadingUpdates(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", "100");
      if (synchronizerFilter) params.set("synchronizer", synchronizerFilter);
      if (workflowFilter) params.set("workflow", workflowFilter);

      const res = await fetch(`${baseUrl}/api/explorer/updates?${params}`);
      const data = await res.json();
      
      if (data.error) {
        setError(data.error);
        setUpdates([]);
      } else {
        setUpdates(data.updates || []);
      }
    } catch (e: any) {
      setError(e.message || "Failed to load updates");
    } finally {
      setLoadingUpdates(false);
    }
  }, [baseUrl, synchronizerFilter, workflowFilter]);

  const loadTimeline = useCallback(async () => {
    setLoadingTimeline(true);
    try {
      const res = await fetch(`${baseUrl}/api/explorer/metrics/timeline?bucket=hour`);
      const data = await res.json();
      setTimeline(data.points || []);
    } catch {
      // ignore
    } finally {
      setLoadingTimeline(false);
    }
  }, [baseUrl]);

  const loadStats = useCallback(async () => {
    try {
      const res = await fetch(`${baseUrl}/api/explorer/stats`);
      const data = await res.json();
      setStats(data);
    } catch {
      // ignore
    }
  }, [baseUrl]);

  const loadEvents = useCallback(async (updateId: string) => {
    setLoadingEvents(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (partyFilter) params.set("party", partyFilter);
      if (contractFilter) params.set("contract", contractFilter);
      if (templateFilter) params.set("template", templateFilter);

      const res = await fetch(
        `${baseUrl}/api/explorer/updates/${encodeURIComponent(updateId)}/events?${params}`
      );
      const data = await res.json();
      setEvents(data.events || []);
      setSelectedEvent(null);
    } catch (e: any) {
      setError(e.message || "Failed to load events");
    } finally {
      setLoadingEvents(false);
    }
  }, [baseUrl, partyFilter, contractFilter, templateFilter]);

  useEffect(() => {
    loadUpdates();
    loadTimeline();
    loadStats();
  }, []);

  const onSelectUpdate = async (u: UpdateRow) => {
    setSelectedUpdate(u);
    await loadEvents(u.id);
  };

  const refreshAll = () => {
    loadUpdates();
    loadTimeline();
    loadStats();
    if (selectedUpdate) {
      loadEvents(selectedUpdate.id);
    }
  };

  const formatTimestamp = (ts: string) => {
    if (!ts) return "-";
    try {
      return new Date(ts).toLocaleString();
    } catch {
      return ts;
    }
  };

  const truncate = (str: string, len = 16) => {
    if (!str) return "-";
    if (str.length <= len) return str;
    return str.slice(0, len) + "…";
  };

  const selectedPayload = useMemo(() => {
    const ev = selectedEvent || events[0];
    if (!ev?.payload) return null;
    return ev.payload;
  }, [events, selectedEvent]);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-primary/10">
              <Database className="h-6 w-6 text-primary" />
            </div>
            <div>
              <h1 className="text-2xl font-bold">Canton Explorer</h1>
              <p className="text-sm text-muted-foreground">
                Browse .pb.zst ledger files
              </p>
            </div>
          </div>
          <Button onClick={refreshAll} variant="outline" size="sm">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card className="glass-card">
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground">Update Files</p>
                  <p className="text-2xl font-bold">{stats?.updateFiles ?? "-"}</p>
                </div>
                <Layers className="h-8 w-8 text-primary/50" />
              </div>
            </CardContent>
          </Card>
          <Card className="glass-card">
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground">Event Files</p>
                  <p className="text-2xl font-bold">{stats?.eventFiles ?? "-"}</p>
                </div>
                <Activity className="h-8 w-8 text-accent/50" />
              </div>
            </CardContent>
          </Card>
          <Card className="glass-card">
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground">Total Size</p>
                  <p className="text-2xl font-bold">{stats?.totalSizeMB ?? "-"} MB</p>
                </div>
                <Database className="h-8 w-8 text-success/50" />
              </div>
            </CardContent>
          </Card>
          <Card className="glass-card">
            <CardContent className="pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-xs text-muted-foreground">Visible Updates</p>
                  <p className="text-2xl font-bold">{updates.length}</p>
                </div>
                <Clock className="h-8 w-8 text-warning/50" />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Timeline Chart */}
        <Card className="glass-card">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm flex items-center gap-2">
              <Activity className="h-4 w-4" />
              Update Volume Over Time
              {loadingTimeline && <span className="text-xs text-muted-foreground">(loading...)</span>}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-48">
              {timeline.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={timeline}>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                    <XAxis
                      dataKey="t"
                      tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
                      interval="preserveStartEnd"
                    />
                    <YAxis
                      tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
                      width={50}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "hsl(var(--card))",
                        border: "1px solid hsl(var(--border))",
                        borderRadius: "8px",
                        fontSize: 12,
                      }}
                    />
                    <Line
                      type="monotone"
                      dataKey="count"
                      stroke="hsl(var(--primary))"
                      strokeWidth={2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex items-center justify-center h-full text-muted-foreground">
                  No timeline data available
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Main Content Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Updates Panel */}
          <Card className="glass-card lg:col-span-1">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm flex items-center gap-2">
                <Filter className="h-4 w-4" />
                Updates
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {/* Filters */}
              <div className="space-y-2">
                <Input
                  placeholder="Filter synchronizer..."
                  value={synchronizerFilter}
                  onChange={(e) => setSynchronizerFilter(e.target.value)}
                  className="h-8 text-xs"
                />
                <Input
                  placeholder="Filter workflow..."
                  value={workflowFilter}
                  onChange={(e) => setWorkflowFilter(e.target.value)}
                  className="h-8 text-xs"
                />
                <Button
                  onClick={() => loadUpdates()}
                  size="sm"
                  className="w-full"
                >
                  <Search className="h-3 w-3 mr-2" />
                  Apply Filters
                </Button>
              </div>

              {error && (
                <div className="text-xs text-destructive bg-destructive/10 p-2 rounded">
                  {error}
                </div>
              )}

              {/* Updates List */}
              <div className="max-h-96 overflow-auto space-y-1">
                {loadingUpdates ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <Skeleton key={i} className="h-16 w-full" />
                  ))
                ) : updates.length === 0 ? (
                  <div className="text-center text-muted-foreground py-8 text-sm">
                    No updates found
                  </div>
                ) : (
                  updates.map((u) => (
                    <div
                      key={u.id}
                      onClick={() => onSelectUpdate(u)}
                      className={`p-2 rounded-lg border cursor-pointer transition-colors ${
                        selectedUpdate?.id === u.id
                          ? "bg-primary/10 border-primary/50"
                          : "bg-card/50 border-border/50 hover:bg-muted/50"
                      }`}
                    >
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs font-mono text-muted-foreground">
                          {truncate(u.id, 12)}
                        </span>
                        <Badge
                          variant={u.status === "Succeeded" ? "default" : "secondary"}
                          className="text-[10px] px-1.5 py-0"
                        >
                          {u.status || "?"}
                        </Badge>
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {formatTimestamp(u.effective_at)}
                      </div>
                      {u.workflow_id && (
                        <div className="text-xs text-muted-foreground truncate">
                          WF: {truncate(u.workflow_id, 20)}
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>
            </CardContent>
          </Card>

          {/* Events Panel */}
          <Card className="glass-card lg:col-span-1">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm flex items-center gap-2">
                <Activity className="h-4 w-4" />
                Events
                {selectedUpdate && (
                  <span className="text-muted-foreground font-normal">
                    for {truncate(selectedUpdate.id, 8)}
                  </span>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {/* Event Filters */}
              <div className="grid grid-cols-3 gap-1">
                <Input
                  placeholder="Party"
                  value={partyFilter}
                  onChange={(e) => setPartyFilter(e.target.value)}
                  className="h-7 text-xs"
                />
                <Input
                  placeholder="Contract"
                  value={contractFilter}
                  onChange={(e) => setContractFilter(e.target.value)}
                  className="h-7 text-xs"
                />
                <Input
                  placeholder="Template"
                  value={templateFilter}
                  onChange={(e) => setTemplateFilter(e.target.value)}
                  className="h-7 text-xs"
                />
              </div>
              {selectedUpdate && (
                <Button
                  onClick={() => loadEvents(selectedUpdate.id)}
                  size="sm"
                  variant="outline"
                  className="w-full h-7 text-xs"
                >
                  Apply Event Filters
                </Button>
              )}

              {/* Events List */}
              <div className="max-h-80 overflow-auto">
                {loadingEvents ? (
                  Array.from({ length: 3 }).map((_, i) => (
                    <Skeleton key={i} className="h-12 w-full mb-1" />
                  ))
                ) : !selectedUpdate ? (
                  <div className="text-center text-muted-foreground py-8 text-sm">
                    Select an update to view events
                  </div>
                ) : events.length === 0 ? (
                  <div className="text-center text-muted-foreground py-8 text-sm">
                    No events for this update
                  </div>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="text-xs">Type</TableHead>
                        <TableHead className="text-xs">Template</TableHead>
                        <TableHead className="text-xs">Party</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {events.map((ev) => (
                        <TableRow
                          key={ev.id}
                          onClick={() => setSelectedEvent(ev)}
                          className={`cursor-pointer ${
                            selectedEvent?.id === ev.id ? "bg-primary/5" : ""
                          }`}
                        >
                          <TableCell className="text-xs py-1">
                            <Badge variant="outline" className="text-[10px]">
                              {ev.type}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-xs py-1 font-mono">
                            {truncate(ev.template?.split(":").pop() || "", 20)}
                          </TableCell>
                          <TableCell className="text-xs py-1">
                            {truncate(ev.party, 12)}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Payload Viewer */}
          <Card className="glass-card lg:col-span-1">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm flex items-center gap-2">
                <FileCode className="h-4 w-4" />
                Payload
                {selectedEvent && (
                  <span className="text-muted-foreground font-normal">
                    ({selectedEvent.type})
                  </span>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="max-h-[500px] overflow-auto">
                {selectedPayload ? (
                  <pre className="text-xs bg-background/50 p-3 rounded-lg border border-border/50 overflow-auto whitespace-pre-wrap break-all font-mono text-accent">
                    {JSON.stringify(selectedPayload, null, 2)}
                  </pre>
                ) : (
                  <div className="text-center text-muted-foreground py-12 text-sm">
                    {events.length > 0
                      ? "Click an event to view its payload"
                      : "Select an update and event to view payload"}
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </DashboardLayout>
  );
};

export default Explorer;
