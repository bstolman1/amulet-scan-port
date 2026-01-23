import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowRightLeft } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { PaginationControls } from "@/components/PaginationControls";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { useStateAcs } from "@/hooks/use-canton-scan-api";

const Transfers = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [preapprovalsPage, setPreapprovalsPage] = useState(1);
  const [commandsPage, setCommandsPage] = useState(1);
  const [instructionsPage, setInstructionsPage] = useState(1);
  const pageSize = 50;

  const { data: preapprovalsData, isLoading: preapprovalsLoading } = useStateAcs(
    ["Splice.AmuletRules:TransferPreapproval"],
    1000
  );

  const { data: commandsData, isLoading: commandsLoading } = useStateAcs(
    ["Splice.ExternalPartyAmuletRules:TransferCommand"],
    1000
  );

  const { data: instructionsData, isLoading: instructionsLoading } = useStateAcs(
    ["Splice.AmuletTransferInstruction:AmuletTransferInstruction"],
    1000
  );

  const preapprovals = preapprovalsData || [];
  const commands = commandsData || [];
  const instructions = instructionsData || [];
  const isLoading = preapprovalsLoading || commandsLoading || instructionsLoading;

  const formatAmount = (amount: any) => {
    if (!amount) return "0.00";
    const value = amount?.amount || amount?.initialAmount?.amount || amount;
    const numValue = typeof value === "string" ? parseFloat(value) : value;
    return (numValue || 0).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };

  const formatParty = (party: any) => {
    if (!party) return "Unknown";
    const partyStr =
      party?.party ||
      party?.provider ||
      party?.sender ||
      party?.receiver ||
      (typeof party === "string" ? party : JSON.stringify(party));
    return partyStr.length > 20
      ? `${partyStr.substring(0, 10)}...${partyStr.substring(partyStr.length - 8)}`
      : partyStr;
  };

  const filteredPreapprovals = preapprovals.filter((p: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    return (
      formatParty(p.create_arguments?.provider)
        .toLowerCase()
        .includes(search) ||
      formatParty(p.create_arguments?.consumer)
        .toLowerCase()
        .includes(search)
    );
  });

  const filteredCommands = commands.filter((c: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    return (
      formatParty(c.create_arguments?.sender)
        .toLowerCase()
        .includes(search) ||
      formatParty(c.create_arguments?.provider)
        .toLowerCase()
        .includes(search)
    );
  });

  const filteredInstructions = instructions.filter((i: any) => {
    if (!searchTerm) return true;
    const search = searchTerm.toLowerCase();
    return (
      formatParty(i.create_arguments?.transfer?.sender)
        .toLowerCase()
        .includes(search) ||
      formatParty(i.create_arguments?.transfer?.receiver?.receiver)
        .toLowerCase()
        .includes(search)
    );
  });

  const preapprovalsDataPage = filteredPreapprovals.slice((preapprovalsPage - 1) * pageSize, preapprovalsPage * pageSize);
  const commandsDataPage = filteredCommands.slice((commandsPage - 1) * pageSize, commandsPage * pageSize);
  const instructionsDataPage = filteredInstructions.slice((instructionsPage - 1) * pageSize, instructionsPage * pageSize);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-2">
            <ArrowRightLeft className="h-8 w-8 text-primary" />
            Transfer Activity
          </h1>
          <p className="text-muted-foreground">Track transfer preapprovals, commands, and instructions.</p>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Active Preapprovals</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{preapprovals.length}</p>
            )}
          </Card>
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">External Commands</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{commands.length}</p>
            )}
          </Card>
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Pending Instructions</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{instructions.length}</p>
            )}
          </Card>
        </div>

        <Card className="p-4">
          <Input
            type="text"
            placeholder="Search transfers..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </Card>

        <Card className="p-6">
          <Tabs defaultValue="preapprovals" className="w-full">
            <TabsList className="grid w-full grid-cols-3">
              <TabsTrigger value="preapprovals">
                Preapprovals ({preapprovals.length})
              </TabsTrigger>
              <TabsTrigger value="commands">Commands ({commands.length})</TabsTrigger>
              <TabsTrigger value="instructions">
                Instructions ({instructions.length})
              </TabsTrigger>
            </TabsList>

            <TabsContent value="preapprovals" className="space-y-4 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : preapprovalsDataPage.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No preapprovals found</p>
              ) : (
                <>
                  {preapprovalsDataPage.map((p: any, i: number) => (
                    <div key={i} className="p-4 bg-muted/30 rounded-lg space-y-2">
                      <div className="flex justify-between">
                        <span className="text-sm font-medium">
                          Provider: {formatParty(p.create_arguments?.provider)}
                        </span>
                        <span className="text-sm text-muted-foreground">
                          Amount: {formatAmount(p.create_arguments?.amount)}
                        </span>
                      </div>
                      <div className="text-sm text-muted-foreground">
                        Consumer: {formatParty(p.create_arguments?.consumer)}
                      </div>
                    </div>
                  ))}
                  <PaginationControls
                    currentPage={preapprovalsPage}
                    totalItems={filteredPreapprovals.length}
                    pageSize={pageSize}
                    onPageChange={setPreapprovalsPage}
                  />
                </>
              )}
            </TabsContent>

            <TabsContent value="commands" className="space-y-4 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : commandsDataPage.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No commands found</p>
              ) : (
                <>
                  {commandsDataPage.map((c: any, i: number) => (
                    <div key={i} className="p-4 bg-muted/30 rounded-lg space-y-2">
                      <div className="flex justify-between">
                        <span className="text-sm font-medium">
                          Sender: {formatParty(c.create_arguments?.sender)}
                        </span>
                        <span className="text-sm text-muted-foreground">
                          Nonce: {c.create_arguments?.nonce || "N/A"}
                        </span>
                      </div>
                      <div className="text-sm text-muted-foreground">
                        Provider: {formatParty(c.create_arguments?.provider)}
                      </div>
                    </div>
                  ))}
                  <PaginationControls
                    currentPage={commandsPage}
                    totalItems={filteredCommands.length}
                    pageSize={pageSize}
                    onPageChange={setCommandsPage}
                  />
                </>
              )}
            </TabsContent>

            <TabsContent value="instructions" className="space-y-4 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : instructionsDataPage.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No instructions found</p>
              ) : (
                <>
                  {instructionsDataPage.map((ins: any, i: number) => (
                    <div key={i} className="p-4 bg-muted/30 rounded-lg space-y-2">
                      <div className="flex justify-between">
                        <span className="text-sm font-medium">
                          Transfer ID: {(ins.contract_id || "Unknown").substring(0, 16)}...
                        </span>
                        <span className="text-sm text-muted-foreground">
                          Amount: {formatAmount(ins.create_arguments?.transfer?.amount)}
                        </span>
                      </div>
                      <div className="text-sm text-muted-foreground">
                        Sender: {formatParty(ins.create_arguments?.transfer?.sender)}
                      </div>
                      <div className="text-sm text-muted-foreground">
                        Receiver: {formatParty(ins.create_arguments?.transfer?.receiver?.receiver)}
                      </div>
                    </div>
                  ))}
                  <PaginationControls
                    currentPage={instructionsPage}
                    totalItems={filteredInstructions.length}
                    pageSize={pageSize}
                    onPageChange={setInstructionsPage}
                  />
                </>
              )}
            </TabsContent>
          </Tabs>
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

export default Transfers;
