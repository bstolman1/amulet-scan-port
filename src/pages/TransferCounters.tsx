import { useState } from "react";
import DashboardLayout from "@/components/DashboardLayout";
import { useAcsSnapshots } from "@/hooks/use-acs-snapshots";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";
import SearchBar from "@/components/SearchBar";

const TransferCounters = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const { data: snapshots, isLoading } = useAcsSnapshots({ limit: 1 });

  const latestSnapshot = snapshots?.[0];
  const templates = latestSnapshot?.snapshot_data as any;
  const counters = templates?.["Splice:ExternalPartyAmuletRules:TransferCommandCounter"] || [];

  const filteredCounters = counters.filter((counter: any) => {
    const searchLower = searchTerm.toLowerCase();
    const contractId = counter.contractId || "";
    return contractId.toLowerCase().includes(searchLower);
  });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Transfer Counters</h1>
          <p className="text-muted-foreground">
            External party transfer command counters
          </p>
        </div>

        <SearchBar
          value={searchTerm}
          onChange={setSearchTerm}
          placeholder="Search transfer counters..."
        />

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-2">
            {filteredCounters.map((counter: any, index: number) => (
              <Card key={index} className="p-4">
                <div className="space-y-2">
                  <div className="font-medium break-all text-sm">
                    {counter.contractId}
                  </div>
                  <details className="text-sm">
                    <summary className="cursor-pointer text-primary">
                      View Details
                    </summary>
                    <pre className="mt-2 text-xs bg-muted p-2 rounded overflow-auto max-h-60">
                      {JSON.stringify(counter, null, 2)}
                    </pre>
                  </details>
                </div>
              </Card>
            ))}
            {filteredCounters.length === 0 && (
              <div className="text-center text-muted-foreground py-8">
                No transfer counters found
              </div>
            )}
          </div>
        )}
      </div>
    </DashboardLayout>
  );
};

export default TransferCounters;
