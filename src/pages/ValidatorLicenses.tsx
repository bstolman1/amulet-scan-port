import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Search, Award, Ticket, Code, Clock, Activity } from "lucide-react";
import { PaginationControls } from "@/components/PaginationControls";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { useValidatorLicenses, useTopValidatorsByFaucets } from "@/hooks/use-canton-scan-api";

const ValidatorLicenses = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 100;

  const { data: licensesData, isLoading: licensesLoading } = useValidatorLicenses();
  const { data: faucetData, isLoading: faucetsLoading } = useTopValidatorsByFaucets(1000);

  const licenses = licensesData || [];
  const faucets = faucetData || [];
  const isLoading = licensesLoading || faucetsLoading;

  const formatParty = (party: string) => {
    if (!party || party.length <= 30) return party || "Unknown";
    return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
  };

  const filteredLicenses = licenses.filter((lic: any) => {
    if (!searchTerm) return true;
    const validator = lic.payload?.validator || lic.validator;
    const sponsor = lic.payload?.sponsor || lic.sponsor;
    return (
      (validator?.toLowerCase() || "").includes(searchTerm.toLowerCase()) ||
      (sponsor?.toLowerCase() || "").includes(searchTerm.toLowerCase())
    );
  });

  const filteredFaucets = faucets.filter((f) => {
    if (!searchTerm) return true;
    return f.validator?.toLowerCase().includes(searchTerm.toLowerCase());
  });

  const paginateData = (data: any[]) => {
    return data.slice((currentPage - 1) * pageSize, currentPage * pageSize);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <div className="flex items-center gap-2 mb-2">
            <Award className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold">Validator Licenses & Faucets</h1>
          </div>
          <p className="text-muted-foreground">View active validator licenses and faucet activity on the network.</p>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Active Licenses</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{licenses.length}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Validators with Faucets</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{faucets.length}</p>
            )}
          </Card>
        </div>

        <Card className="p-6">
          <div className="mb-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground h-4 w-4" />
              <Input
                type="text"
                placeholder="Search by validator..."
                value={searchTerm}
                onChange={(e) => {
                  setSearchTerm(e.target.value);
                  setCurrentPage(1);
                }}
                className="pl-10"
              />
            </div>
          </div>

          <Tabs defaultValue="licenses" className="w-full" onValueChange={() => setCurrentPage(1)}>
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="licenses">Licenses ({filteredLicenses.length})</TabsTrigger>
              <TabsTrigger value="faucets">Faucet Activity ({filteredFaucets.length})</TabsTrigger>
            </TabsList>

            <TabsContent value="licenses" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : filteredLicenses.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No validator licenses found</p>
              ) : (
                <>
                  {paginateData(filteredLicenses).map((license: any, idx: number) => {
                    const validator = license.payload?.validator || license.validator;
                    const sponsor = license.payload?.sponsor || license.sponsor;
                    const dso = license.payload?.dso || license.dso;
                    const faucetState = license.payload?.faucetState || license.faucetState;
                    const metadata = license.payload?.metadata || license.metadata;

                    return (
                      <Card key={idx} className="p-4 space-y-3">
                        <div className="flex justify-between items-start">
                          <div className="flex-1 space-y-2">
                            <div>
                              <p className="text-sm font-semibold text-primary">Validator</p>
                              <p className="text-xs font-mono break-all">{validator}</p>
                            </div>

                            <div className="grid grid-cols-2 gap-3 text-sm">
                              <div>
                                <p className="text-xs text-muted-foreground">Sponsor</p>
                                <p className="font-mono text-xs break-all">{formatParty(sponsor)}</p>
                              </div>
                              <div>
                                <p className="text-xs text-muted-foreground">DSO</p>
                                <p className="font-mono text-xs break-all">{formatParty(dso || "N/A")}</p>
                              </div>
                            </div>

                            {faucetState && (
                              <div className="pt-2 border-t">
                                <p className="text-xs font-semibold mb-2">Faucet State</p>
                                <div className="grid grid-cols-3 gap-3 text-xs">
                                  <div>
                                    <p className="text-muted-foreground">First Round</p>
                                    <p className="font-medium">{faucetState.firstReceivedFor?.number || "N/A"}</p>
                                  </div>
                                  <div>
                                    <p className="text-muted-foreground">Last Round</p>
                                    <p className="font-medium">{faucetState.lastReceivedFor?.number || "N/A"}</p>
                                  </div>
                                  <div>
                                    <p className="text-muted-foreground">Missed Coupons</p>
                                    <p className="font-medium">{faucetState.numCouponsMissed || "0"}</p>
                                  </div>
                                </div>
                              </div>
                            )}

                            {metadata && (
                              <div className="pt-2 border-t">
                                <p className="text-xs font-semibold mb-2">Metadata</p>
                                <div className="grid grid-cols-2 gap-3 text-xs">
                                  <div>
                                    <p className="text-muted-foreground">Version</p>
                                    <p className="font-medium">{metadata.version || "N/A"}</p>
                                  </div>
                                  <div>
                                    <p className="text-muted-foreground">Contact</p>
                                    <p className="font-medium">{metadata.contactPoint || "Not provided"}</p>
                                  </div>
                                  {metadata.lastUpdatedAt && (
                                    <div className="col-span-2">
                                      <p className="text-muted-foreground">Last Updated</p>
                                      <p className="font-medium flex items-center gap-1">
                                        <Clock className="h-3 w-3" />
                                        {new Date(metadata.lastUpdatedAt).toLocaleString()}
                                      </p>
                                    </div>
                                  )}
                                </div>
                              </div>
                            )}

                            <Collapsible className="pt-2 border-t">
                              <CollapsibleTrigger asChild>
                                <Button variant="ghost" size="sm" className="w-full justify-start">
                                  <Code className="h-4 w-4 mr-2" />
                                  Show Raw JSON
                                </Button>
                              </CollapsibleTrigger>
                              <CollapsibleContent className="mt-2">
                                <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                                  {JSON.stringify(license, null, 2)}
                                </pre>
                              </CollapsibleContent>
                            </Collapsible>
                          </div>
                          <Badge variant="default">Active</Badge>
                        </div>
                      </Card>
                    );
                  })}
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={filteredLicenses.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
                  />
                </>
              )}
            </TabsContent>

            <TabsContent value="faucets" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : filteredFaucets.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No faucet activity found</p>
              ) : (
                <>
                  {paginateData(filteredFaucets).map((faucet, idx: number) => (
                    <Card key={idx} className="p-4 space-y-2">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <Activity className="h-4 w-4 text-success" />
                          <p className="text-sm font-medium">Validator: {formatParty(faucet.validator)}</p>
                        </div>
                        <Badge variant="secondary">
                          {faucet.numRoundsCollected} rounds collected
                        </Badge>
                      </div>
                      <div className="grid grid-cols-4 gap-3 text-xs">
                        <div>
                          <p className="text-muted-foreground">First Round</p>
                          <p className="font-medium">{faucet.firstCollectedInRound}</p>
                        </div>
                        <div>
                          <p className="text-muted-foreground">Last Round</p>
                          <p className="font-medium">{faucet.lastCollectedInRound}</p>
                        </div>
                        <div>
                          <p className="text-muted-foreground">Collected</p>
                          <p className="font-medium text-success">{faucet.numRoundsCollected}</p>
                        </div>
                        <div>
                          <p className="text-muted-foreground">Missed</p>
                          <p className="font-medium text-destructive">{faucet.numRoundsMissed}</p>
                        </div>
                      </div>
                    </Card>
                  ))}
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={filteredFaucets.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
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

export default ValidatorLicenses;
