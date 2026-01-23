import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Shield, ChevronDown, ChevronRight } from "lucide-react";
import { useState } from "react";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Input } from "@/components/ui/input";
import { PaginationControls } from "@/components/PaginationControls";
import { useStateAcs } from "@/hooks/use-canton-scan-api";

const ExternalPartyRules = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [openItems, setOpenItems] = useState<Record<number, boolean>>({});
  const itemsPerPage = 20;

  const { data: rulesData, isLoading } = useStateAcs(
    ["Splice.ExternalPartyAmuletRules:ExternalPartyAmuletRules"],
    1000
  );

  const rules = rulesData || [];

  const filteredRules = rules.filter((rule: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    const jsonString = JSON.stringify(rule).toLowerCase();
    return jsonString.includes(search);
  });

  const paginatedData = filteredRules.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);

  const totalPages = Math.ceil(filteredRules.length / itemsPerPage);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h2 className="text-3xl font-bold mb-2">External Party Amulet Rules</h2>
          <p className="text-muted-foreground">Rules and configurations for external party amulet interactions</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Total Rules</h3>
              <Shield className="h-5 w-5 text-primary" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-primary">{rules.length.toLocaleString()}</p>
            )}
          </Card>

          <Card className="glass-card p-6">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-muted-foreground">Filtered Results</h3>
              <Shield className="h-5 w-5 text-primary" />
            </div>
            {isLoading ? (
              <Skeleton className="h-10 w-full" />
            ) : (
              <p className="text-3xl font-bold text-primary">{filteredRules.length.toLocaleString()}</p>
            )}
          </Card>
        </div>

        <Input
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          placeholder="Search rules..."
          className="max-w-md"
        />

        {isLoading ? (
          <div className="grid gap-4">
            <Skeleton className="h-32 w-full" />
            <Skeleton className="h-32 w-full" />
          </div>
        ) : rules.length === 0 ? (
          <Card className="p-8 text-center">
            <Shield className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
            <p className="text-muted-foreground">No external party rules found</p>
          </Card>
        ) : (
          <div className="space-y-4">
            {paginatedData.map((rule: any, index: number) => {
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
                            Rule {(currentPage - 1) * itemsPerPage + index + 1}
                          </CardTitle>
                        </div>
                        <Badge variant="secondary">View Details</Badge>
                      </CardHeader>
                    </CollapsibleTrigger>
                    <CardContent>
                      <CollapsibleContent>
                        <div className="p-4 rounded-lg bg-muted/50">
                          <p className="text-xs font-semibold mb-2">Raw JSON:</p>
                          <pre className="text-xs overflow-auto max-h-96">{JSON.stringify(rule, null, 2)}</pre>
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
            totalItems={filteredRules.length}
            pageSize={itemsPerPage}
            onPageChange={setCurrentPage}
          />
        )}

        <DataSourcesFooter
          snapshotId={undefined}
          templateSuffixes={[]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default ExternalPartyRules;
