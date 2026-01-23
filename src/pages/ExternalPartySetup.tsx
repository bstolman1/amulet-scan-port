import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Search, Users, Clock } from "lucide-react";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { useStateAcs } from "@/hooks/use-canton-scan-api";

const ExternalPartySetup = () => {
  const [searchTerm, setSearchTerm] = useState("");

  const { data: proposalsData, isLoading } = useStateAcs(
    ["Splice.AmuletRules:ExternalPartySetupProposal"],
    1000
  );

  const proposals = proposalsData || [];

  const formatParty = (party: string) => {
    if (!party || party.length <= 30) return party || "Unknown";
    return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
  };

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case "pending":
        return "outline";
      case "approved":
        return "default";
      case "rejected":
        return "destructive";
      default:
        return "secondary";
    }
  };

  const filteredProposals = proposals
    .filter((proposal: any) => {
      if (!searchTerm) return true;
      const party = proposal.create_arguments?.externalParty || "";
      const requester = proposal.create_arguments?.requester || "";
      return (
        party.toLowerCase().includes(searchTerm.toLowerCase()) ||
        requester.toLowerCase().includes(searchTerm.toLowerCase())
      );
    })
    .slice(0, 100);

  const pendingCount = proposals.filter(
    (p: any) => (p.create_arguments?.status || "pending").toLowerCase() === "pending",
  ).length;

  const approvedCount = proposals.filter(
    (p: any) => (p.create_arguments?.status || "pending").toLowerCase() === "approved",
  ).length;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-2">
            <Users className="h-8 w-8 text-primary" />
            External Party Setup Proposals
          </h1>
          <p className="text-muted-foreground">
            View and track proposals for external party integration on the network.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Total Proposals</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{proposals.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Pending</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold text-warning">{pendingCount}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Approved</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold text-success">{approvedCount}</p>
            )}
          </Card>
        </div>

        <Card className="p-6">
          <div className="mb-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
              <Input
                type="text"
                placeholder="Search proposals..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10"
              />
            </div>
          </div>

          {isLoading ? (
            <div className="space-y-4">
              {[1, 2, 3, 4, 5].map((i) => (
                <Skeleton key={i} className="h-32 w-full" />
              ))}
            </div>
          ) : filteredProposals.length === 0 ? (
            <p className="text-center text-muted-foreground py-8">No external party setup proposals found</p>
          ) : (
            <div className="space-y-3">
              {filteredProposals.map((proposal: any, idx: number) => {
                const externalParty = proposal.create_arguments?.externalParty;
                const requester = proposal.create_arguments?.requester;
                const status = proposal.create_arguments?.status || "pending";
                const createdAt = proposal.created_at;

                return (
                  <div key={idx} className="p-4 bg-muted/30 rounded-lg space-y-3">
                    <div className="flex justify-between items-start">
                      <div className="flex-1">
                        <p className="text-sm font-medium mb-1">External Party: {formatParty(externalParty)}</p>
                        <p className="text-xs text-muted-foreground">Requester: {formatParty(requester)}</p>
                      </div>
                      <Badge variant={getStatusColor(status)}>{status}</Badge>
                    </div>
                    {createdAt && (
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Clock className="h-3 w-3" />
                        <span>Created: {new Date(createdAt).toLocaleString()}</span>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {!isLoading && filteredProposals.length > 0 && (
            <div className="mt-4 text-xs text-muted-foreground">
              Showing {filteredProposals.length} of {proposals.length} proposals
            </div>
          )}
        </Card>

        <DataSourcesFooter
          snapshotId={undefined}
          templateSuffixes={[]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default ExternalPartySetup;
