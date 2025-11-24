import { useState, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { supabase } from "@/integrations/supabase/client";
import { Activity, Database, Clock, Search } from "lucide-react";
import { formatDistanceToNow } from "date-fns";
import { useLedgerUpdates, LedgerUpdate } from "@/hooks/use-ledger-updates";

const LiveUpdates = () => {
  const { data: updates = [], isLoading } = useLedgerUpdates(100);
  const [realtimeUpdates, setRealtimeUpdates] = useState<LedgerUpdate[]>([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedMigration, setSelectedMigration] = useState<number | null>(null);

  useEffect(() => {
    const channel = supabase
      .channel("ledger-updates-live")
      .on(
        "postgres_changes",
        {
          event: "INSERT",
          schema: "public",
          table: "ledger_updates",
        },
        (payload) => {
          console.log("New ledger update:", payload);
          setRealtimeUpdates((prev) => [payload.new as LedgerUpdate, ...prev.slice(0, 49)]);
        },
      )
      .subscribe();

    return () => {
      supabase.removeChannel(channel);
    };
  }, []);

  const allUpdates = [
    ...realtimeUpdates,
    ...updates.filter((u) => !realtimeUpdates.some((ru) => ru.update_id === u.update_id)),
  ];

  const filteredUpdates = allUpdates.filter((update) => {
    const matchesSearch =
      !searchTerm ||
      update.update_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
      update.workflow_id?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      update.kind?.toLowerCase().includes(searchTerm.toLowerCase());

    const matchesMigration = !selectedMigration || update.migration_id === selectedMigration;

    return matchesSearch && matchesMigration;
  });

  const migrations = Array.from(new Set(allUpdates.map((u) => u.migration_id).filter(Boolean)));
  const updatesByKind = allUpdates.reduce(
    (acc, u) => {
      const kind = u.kind || "unknown";
      acc[kind] = (acc[kind] || 0) + 1;
      return acc;
    },
    {} as Record<string, number>,
  );

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
          <p className="text-muted-foreground">Real-time stream of ledger updates from V2 API</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="glass-card">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">Total Updates</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-primary">{allUpdates.length}</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">Migrations</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-primary">{migrations.length}</p>
            </CardContent>
          </Card>

          <Card className="glass-card">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground">Update Types</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-bold text-primary">{Object.keys(updatesByKind).length}</p>
            </CardContent>
          </Card>
        </div>

        <Card className="glass-card">
          <CardHeader>
            <div className="flex flex-col md:flex-row gap-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground w-4 h-4" />
                <Input
                  placeholder="Search by update ID, workflow ID, or kind..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
              <div className="flex gap-2 flex-wrap">
                <Badge
                  variant={selectedMigration === null ? "default" : "outline"}
                  className="cursor-pointer"
                  onClick={() => setSelectedMigration(null)}
                >
                  All Migrations
                </Badge>
                {migrations.map((migId) => (
                  <Badge
                    key={migId}
                    variant={selectedMigration === migId ? "default" : "outline"}
                    className="cursor-pointer"
                    onClick={() => setSelectedMigration(migId as number)}
                  >
                    Migration #{migId}
                  </Badge>
                ))}
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 max-h-[600px] overflow-y-auto">
              {filteredUpdates.map((update) => (
                <div
                  key={update.update_id}
                  className="p-3 rounded-lg border bg-card hover:bg-muted/50 transition-colors"
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="space-y-2 flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <Badge variant="outline">{update.kind || "unknown"}</Badge>
                        {update.migration_id && <Badge variant="secondary">Migration #{update.migration_id}</Badge>}
                        <span className="font-mono text-xs text-muted-foreground truncate">{update.update_id}</span>
                      </div>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-xs text-muted-foreground">
                        {update.workflow_id && (
                          <div className="flex items-center gap-1 min-w-0">
                            <Database className="w-3 h-3 flex-shrink-0" />
                            <span className="truncate" title={update.workflow_id}>
                              Workflow: {update.workflow_id}
                            </span>
                          </div>
                        )}
                        {update.synchronizer_id && (
                          <div className="flex items-center gap-1 min-w-0">
                            <Activity className="w-3 h-3 flex-shrink-0" />
                            <span className="truncate" title={update.synchronizer_id}>
                              Sync: {update.synchronizer_id}
                            </span>
                          </div>
                        )}
                        {update.record_time && (
                          <div className="flex items-center gap-1">
                            <Clock className="w-3 h-3 flex-shrink-0" />
                            <span>Record: {new Date(update.record_time).toLocaleString()}</span>
                          </div>
                        )}
                        {update.offset && (
                          <div className="flex items-center gap-1">
                            <span>Offset: {update.offset}</span>
                          </div>
                        )}
                      </div>
                    </div>

                    <div className="text-xs text-muted-foreground whitespace-nowrap">
                      {formatDistanceToNow(new Date(update.created_at), { addSuffix: true })}
                    </div>
                  </div>
                </div>
              ))}

              {filteredUpdates.length === 0 && (
                <div className="text-center py-12 text-muted-foreground">
                  {searchTerm || selectedMigration ? "No matching updates found." : "No updates yet."}
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
