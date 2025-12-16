import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Search, Globe, Database } from "lucide-react";
import { useState } from "react";
import { Skeleton } from "@/components/ui/skeleton";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { PaginationControls } from "@/components/PaginationControls";
import { useLocalACSAvailable } from "@/hooks/use-local-acs";

const ANS = () => {
  const [searchQuery, setSearchQuery] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 20;
  const { data: dataAvailable } = useLocalACSAvailable();

  // Fetch from updates data (no snapshot required)
  const ansEntriesQuery = useAggregatedTemplateData(undefined, "Splice:Ans:AnsEntry");
  const ansContextsQuery = useAggregatedTemplateData(undefined, "Splice:Ans:AnsEntryContext");

  const isLoading = ansEntriesQuery.isLoading || ansContextsQuery.isLoading;
  const ansEntries = ansEntriesQuery.data?.data || [];
  const ansContexts = ansContextsQuery.data?.data || [];
  const dataSource = ansEntriesQuery.data?.source || "unknown";

  const enrichedEntries = ansEntries.map((entry: any) => {
    const context = ansContexts.find(
      (ctx: any) => ctx.payload?.name === entry.payload?.name || ctx.name === entry.payload?.name,
    );
    return {
      name: entry.payload?.name || entry.name,
      user: entry.payload?.user || entry.user,
      url: entry.payload?.url || entry.url,
      description: entry.payload?.description || entry.description,
      expiresAt: entry.payload?.expiresAt || entry.expiresAt,
      reference: context?.payload?.reference || context?.reference,
      ...entry,
    };
  });

  const filteredEntries = enrichedEntries.filter(
    (entry: any) =>
      (entry.name?.toLowerCase() || "").includes(searchQuery.toLowerCase()) ||
      (entry.user?.toLowerCase() || "").includes(searchQuery.toLowerCase()) ||
      (entry.description?.toLowerCase() || "").includes(searchQuery.toLowerCase()),
  );

  const paginatedEntries = filteredEntries.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-2 mb-2">
            <Globe className="h-8 w-8 text-primary" />
            <h2 className="text-3xl font-bold">Amulet Name Service (ANS)</h2>
            {dataAvailable && (
              <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
                <Database className="h-3 w-3 mr-1" />
                {dataSource === "updates" ? "Updates" : dataSource === "acs-fallback" ? "ACS" : "Local"}
              </Badge>
            )}
          </div>
          <p className="text-muted-foreground">Human-readable names for Canton Network parties</p>
        </div>

        <Card className="glass-card p-6">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search ANS entries..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </Card>

        {isLoading && (
          <div className="space-y-4">
            {[1, 2, 3, 4, 5].map((i) => (
              <Card key={i} className="p-6">
                <Skeleton className="h-6 w-32 mb-2" />
                <Skeleton className="h-4 w-full" />
              </Card>
            ))}
          </div>
        )}
        {!isLoading && filteredEntries.length === 0 && searchQuery && (
          <Card className="p-6">
            <p className="text-muted-foreground text-center">No ANS entries found</p>
          </Card>
        )}
        {!isLoading && filteredEntries.length > 0 && (
          <>
            <div className="space-y-4">
              {paginatedEntries.map((entry: any, i: number) => (
                <Card key={i} className="p-6">
                  <h3 className="text-xl font-semibold text-primary mb-2">{entry.name}</h3>
                  {entry.expiresAt && (
                    <p className="text-sm text-muted-foreground">
                      Expires: {new Date(entry.expiresAt).toLocaleDateString()}
                    </p>
                  )}
                  <p className="text-sm">
                    <span className="text-muted-foreground">User:</span>{" "}
                    <span className="font-mono text-xs">{entry.user}</span>
                  </p>
                  {entry.url && (
                    <p className="text-sm">
                      <span className="text-muted-foreground">URL:</span>{" "}
                      <a
                        href={entry.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-primary hover:underline"
                      >
                        {entry.url}
                      </a>
                    </p>
                  )}
                  {entry.description && <p className="text-sm text-muted-foreground">{entry.description}</p>}
                </Card>
              ))}
            </div>
            <PaginationControls
              currentPage={currentPage}
              totalItems={filteredEntries.length}
              pageSize={pageSize}
              onPageChange={setCurrentPage}
            />
          </>
        )}

        <DataSourcesFooter
          snapshotId={undefined}
          templateSuffixes={["Splice:Ans:AnsEntry", "Splice:Ans:AnsEntryContext"]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default ANS;
