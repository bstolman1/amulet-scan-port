import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { Hash, ChevronDown, ChevronRight } from "lucide-react";
import { useState } from "react";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Input } from "@/components/ui/input";
import { PaginationControls } from "@/components/PaginationControls";

const TransferCounters = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [openItems, setOpenItems] = useState<Record<number, boolean>>({});
  const itemsPerPage = 20;

  const { data: latestSnapshot } = useLatestACSSnapshot();

  const countersQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:ExternalPartyAmuletRules:TransferCommandCounter",
  );

  const getField = (obj: any, fieldNames: string[]) => {
    for (const name of fieldNames) {
      if (obj?.[name] !== undefined && obj?.[name] !== null) return obj[name];
      if (obj?.payload?.[name] !== undefined && obj?.payload?.[name] !== null) return obj.payload[name];
    }
    return null;
  };

  const counters = countersQuery.data?.data || [];

  const filteredCounters = counters.filter((counter: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    const jsonString = JSON.stringify(counter).toLowerCase();
    return jsonString.includes(search);
  });

  const paginatedData = filteredCounters.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);

  const totalPages = Math.ceil(filteredCounters.length / itemsPerPage);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-bold mb-2">Transfer Command Counters</h2>
          <p className="text-muted-foreground">External party transfer command tracking and counters</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Total Counters</h3>
              <Hash className="h-5 w-5 text-primary" />
            </div>
            {countersQuery.isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-primary">{counters.length.toLocaleString()}</p>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Filtered Results</h3>
              <Hash className="h-5 w-5 text-primary" />
            </div>
            {countersQuery.isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-primary">{filteredCounters.length.toLocaleString()}</p>
            )}
          </Card>
        </div>

        <Input
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          placeholder="Search counters..."
          className="max-w-md"
        />

        {countersQuery.isLoading ? (
          <div className="grid gap-4">
            <Skeleton className="h-32 w-full" />
            <Skeleton className="h-32 w-full" />
          </div>
        ) : counters.length === 0 ? (
          <Card className="p-8 text-center">
            <Hash className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
            <p className="text-muted-foreground">No transfer command counters found in this snapshot</p>
          </Card>
        ) : (
          <div className="space-y-4">
            {paginatedData.map((counter: any, index: number) => {
              const itemKey = (currentPage - 1) * itemsPerPage + index;

              return (
                <Card key={index}>
                  <Collapsible
                    open={openItems[itemKey] || false}
                    onOpenChange={(isOpen) => setOpenItems((prev) => ({ ...prev, [itemKey]: isOpen }))}
                  >
                    <CollapsibleTrigger className="w-full">
                      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <div className="flex items-center gap-2">
                          {openItems[itemKey] ? (
                            <ChevronDown className="h-4 w-4" />
                          ) : (
                            <ChevronRight className="h-4 w-4" />
                          )}
                          <CardTitle className="text-base font-medium">
                            Counter {(currentPage - 1) * itemsPerPage + index + 1}
                          </CardTitle>
                        </div>
                        <Badge variant="secondary">View Details</Badge>
                      </CardHeader>
                    </CollapsibleTrigger>
                    <CardContent>
                      <CollapsibleContent>
                        <div className="p-4 rounded-lg bg-muted/50">
                          <p className="text-xs font-semibold mb-2">Raw JSON:</p>
                          <pre className="text-xs overflow-auto max-h-96">{JSON.stringify(counter, null, 2)}</pre>
                        </div>
                      </CollapsibleContent>
                    </CardContent>
                  </Collapsible>
                </Card>
              );
            })}
          </div>
        )}

        {totalPages > 1 && (
          <PaginationControls
            currentPage={currentPage}
            totalItems={filteredCounters.length}
            pageSize={itemsPerPage}
            onPageChange={setCurrentPage}
          />
        )}

        <DataSourcesFooter
          snapshotId={latestSnapshot?.id}
          templateSuffixes={["Splice:ExternalPartyAmuletRules:TransferCommandCounter"]}
          isProcessing={latestSnapshot?.status === "processing"}
        />
      </div>
    </DashboardLayout>
  );
};

export default TransferCounters;
