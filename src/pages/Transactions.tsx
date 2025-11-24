import { useState } from "react";
import DashboardLayout from "@/components/DashboardLayout";
import { useLedgerUpdates } from "@/hooks/use-ledger-updates";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";
import SearchBar from "@/components/SearchBar";
import { Button } from "@/components/ui/button";

const Transactions = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [page, setPage] = useState(0);
  const limit = 50;

  const { data: updates, isLoading } = useLedgerUpdates({
    limit,
    offset: page * limit,
  });

  const filteredUpdates = updates?.filter((update) => {
    const searchLower = searchTerm.toLowerCase();
    return (
      update.update_type.toLowerCase().includes(searchLower) ||
      update.round.toString().includes(searchLower)
    );
  });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Transactions</h1>
          <p className="text-muted-foreground">
            Browse ledger updates and contract events
          </p>
        </div>

        <SearchBar
          value={searchTerm}
          onChange={setSearchTerm}
          placeholder="Search by type or round..."
        />

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <>
            <div className="space-y-2">
              {filteredUpdates?.map((update) => (
                <Card key={update.id} className="p-4">
                  <div className="flex justify-between items-start">
                    <div className="space-y-1">
                      <div className="font-medium">{update.update_type}</div>
                      <div className="text-sm text-muted-foreground">
                        Round {update.round}
                      </div>
                    </div>
                    <div className="text-sm text-muted-foreground">
                      {new Date(update.timestamp).toLocaleString()}
                    </div>
                  </div>
                  <details className="mt-2">
                    <summary className="cursor-pointer text-sm text-primary">
                      View Details
                    </summary>
                    <pre className="mt-2 text-xs bg-muted p-2 rounded overflow-auto max-h-40">
                      {JSON.stringify(update.update_data, null, 2)}
                    </pre>
                  </details>
                </Card>
              ))}
            </div>

            <div className="flex justify-between items-center pt-4">
              <Button
                variant="outline"
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                disabled={page === 0}
              >
                Previous
              </Button>
              <span className="text-sm text-muted-foreground">Page {page + 1}</span>
              <Button
                variant="outline"
                onClick={() => setPage((p) => p + 1)}
                disabled={!updates || updates.length < limit}
              >
                Next
              </Button>
            </div>
          </>
        )}
      </div>
    </DashboardLayout>
  );
};

export default Transactions;
