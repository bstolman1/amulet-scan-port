import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Search, Activity, Code } from "lucide-react";
import { useDsoInfo, useDsoSequencers } from "@/hooks/use-canton-scan-api";
import { PaginationControls } from "@/components/PaginationControls";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";

const MemberTraffic = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 50;

  const { data: dsoInfo, isLoading: dsoLoading } = useDsoInfo();
  const { data: sequencers, isLoading: sequencersLoading } = useDsoSequencers();

  const isLoading = dsoLoading || sequencersLoading;

  // Extract SV node states which contain network member info
  const svNodeStates = dsoInfo?.sv_node_states || [];

  // Flatten sequencers for display
  const allSequencers = (sequencers || []).flatMap((domain: any) =>
    (domain.sequencers || []).map((seq: any) => ({
      ...seq,
      domainId: domain.domainId,
    }))
  );

  const filteredSequencers = allSequencers.filter((seq: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    return (
      seq.svName?.toLowerCase().includes(search) ||
      seq.id?.toLowerCase().includes(search) ||
      seq.url?.toLowerCase().includes(search)
    );
  });

  const paginatedData = filteredSequencers.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-2">
            <Activity className="h-8 w-8 text-primary" />
            Network Members & Sequencers
          </h1>
          <p className="text-muted-foreground">
            View network member information and sequencer configuration from the live Canton network.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">SV Node States</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{svNodeStates.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Domain Sequencers</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{allSequencers.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Voting Threshold</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{dsoInfo?.voting_threshold || "—"}</p>
            )}
          </Card>
        </div>

        <Card className="p-6">
          <Tabs defaultValue="sequencers" className="space-y-4">
            <TabsList>
              <TabsTrigger value="sequencers">Sequencers ({allSequencers.length})</TabsTrigger>
              <TabsTrigger value="nodes">SV Nodes ({svNodeStates.length})</TabsTrigger>
            </TabsList>

            <TabsContent value="sequencers" className="space-y-4">
              <div className="mb-4">
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
                  <Input
                    type="text"
                    placeholder="Search by SV name, ID, or URL..."
                    value={searchTerm}
                    onChange={(e) => {
                      setSearchTerm(e.target.value);
                      setCurrentPage(1);
                    }}
                    className="pl-10"
                  />
                </div>
              </div>

              {isLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-20 w-full" />
                  ))}
                </div>
              ) : filteredSequencers.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <Activity className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No sequencers found</p>
                </div>
              ) : (
                <>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>SV Name</TableHead>
                        <TableHead>Migration ID</TableHead>
                        <TableHead>URL</TableHead>
                        <TableHead>Available After</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {paginatedData.map((seq: any, idx: number) => (
                        <TableRow key={idx}>
                          <TableCell className="font-medium">{seq.svName || "—"}</TableCell>
                          <TableCell>
                            <Badge variant="outline">{seq.migrationId}</Badge>
                          </TableCell>
                          <TableCell className="font-mono text-xs max-w-[300px] truncate">
                            {seq.url || "—"}
                          </TableCell>
                          <TableCell className="text-xs">
                            {seq.availableAfter ? new Date(seq.availableAfter).toLocaleString() : "—"}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>

                  {filteredSequencers.length > pageSize && (
                    <PaginationControls
                      currentPage={currentPage}
                      totalItems={filteredSequencers.length}
                      pageSize={pageSize}
                      onPageChange={setCurrentPage}
                    />
                  )}
                </>
              )}
            </TabsContent>

            <TabsContent value="nodes" className="space-y-4">
              {isLoading ? (
                <div className="space-y-3">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : svNodeStates.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  <Activity className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No SV node states found</p>
                </div>
              ) : (
                <div className="space-y-3">
                  {svNodeStates.map((node: any, idx: number) => {
                    const payload = node?.contract?.payload || node?.payload || node;
                    const svName = payload?.svName;
                    const sv = payload?.sv;

                    return (
                      <Card key={idx} className="p-4 space-y-3">
                        <div className="flex justify-between items-start">
                          <div className="flex-1 space-y-2">
                            <p className="font-semibold">{svName || "Unknown SV"}</p>
                            <div>
                              <p className="text-xs text-muted-foreground">Party</p>
                              <p className="font-mono text-xs break-all">{sv || "—"}</p>
                            </div>
                          </div>
                        </div>

                        <Collapsible className="pt-2 border-t">
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-start">
                              <Code className="h-4 w-4 mr-2" />
                              Show Raw JSON
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-2">
                            <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                              {JSON.stringify(node, null, 2)}
                            </pre>
                          </CollapsibleContent>
                        </Collapsible>
                      </Card>
                    );
                  })}
                </div>
              )}
            </TabsContent>
          </Tabs>
        </Card>

        <Card className="p-4 text-xs text-muted-foreground">
          <p>
            Data sourced from Canton Scan API <code>/v0/dso</code> and <code>/v0/dso-sequencers</code> endpoints.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default MemberTraffic;