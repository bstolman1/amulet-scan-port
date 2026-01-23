import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useStateAcs } from "@/hooks/use-canton-scan-api";
import { Hash, ChevronDown, ChevronRight, Code } from "lucide-react";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { PaginationControls } from "@/components/PaginationControls";
import { Button } from "@/components/ui/button";

const TransferCounters = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [openItems, setOpenItems] = useState<Record<number, boolean>>({});
  const itemsPerPage = 20;

  // Fetch TransferCommandCounter contracts from live ACS
  const { data: countersData, isLoading } = useStateAcs([
    "Splice.ExternalPartyAmuletRules:TransferCommandCounter",
  ]);

  const counters = countersData || [];

  // Helper to extract fields
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record?.[field] !== undefined) return record[field];
      if (record?.create_arguments?.[field] !== undefined) return record.create_arguments[field];
      if (record?.payload?.[field] !== undefined) return record.payload[field];
    }
    return undefined;
  };

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
          <p className="text-muted-foreground">External party transfer command tracking from live network state</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Total Counters</h3>
              <Hash className="h-5 w-5 text-primary" />
            </div>
            {isLoading ? (
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
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-primary">{filteredCounters.length.toLocaleString()}</p>
            )}
          </Card>
        </div>

        <Input
          value={searchTerm}
          onChange={(e) => {
            setSearchTerm(e.target.value);
            setCurrentPage(1);
          }}
          placeholder="Search counters..."
          className="max-w-md"
        />

        {isLoading ? (
          <div className="grid gap-4">
            <Skeleton className="h-32 w-full" />
            <Skeleton className="h-32 w-full" />
          </div>
        ) : counters.length === 0 ? (
          <Card className="p-8 text-center">
            <Hash className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
            <p className="text-muted-foreground">No transfer command counters found in the live ACS</p>
          </Card>
        ) : (
          <div className="space-y-4">
            {paginatedData.map((counter: any, index: number) => {
              const itemKey = (currentPage - 1) * itemsPerPage + index;
              const sender = getField(counter, "sender");
              const nonce = getField(counter, "nextNonce");

              return (
                <Card key={index}>
                  <Collapsible
                    open={openItems[itemKey] || false}
                    onOpenChange={(isOpen) => setOpenItems((prev) => ({ ...prev, [itemKey]: isOpen }))}
                  >
                    <CardHeader className="pb-2">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="p-0 h-auto">
                              {openItems[itemKey] ? (
                                <ChevronDown className="h-4 w-4" />
                              ) : (
                                <ChevronRight className="h-4 w-4" />
                              )}
                            </Button>
                          </CollapsibleTrigger>
                          <CardTitle className="text-base font-medium">
                            Counter #{(currentPage - 1) * itemsPerPage + index + 1}
                          </CardTitle>
                        </div>
                        <Badge variant="secondary">Nonce: {nonce || "—"}</Badge>
                      </div>
                    </CardHeader>
                    <CardContent>
                      {sender && (
                        <div className="mb-3">
                          <p className="text-xs text-muted-foreground">Sender</p>
                          <p className="font-mono text-xs break-all">{sender}</p>
                        </div>
                      )}
                      <CollapsibleContent>
                        <div className="p-4 rounded-lg bg-muted/50 mt-2">
                          <div className="flex items-center gap-2 mb-2">
                            <Code className="h-4 w-4" />
                            <p className="text-xs font-semibold">Raw JSON</p>
                          </div>
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

        <Card className="p-4 text-xs text-muted-foreground">
          <p>
            Data sourced from Canton Scan API <code>/v0/state/acs</code> endpoint.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default TransferCounters;