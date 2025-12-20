import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Search, Award, Ticket, Code, Clock, Activity, Database } from "lucide-react";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { PaginationControls } from "@/components/PaginationControls";
import { DataSourcesFooter } from "@/components/DataSourcesFooter";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Button } from "@/components/ui/button";
import { useACSStatus } from "@/hooks/use-local-acs";
import { ACSStatusBanner } from "@/components/ACSStatusBanner";

const ValidatorLicenses = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 100;
  const { data: acsStatus } = useACSStatus();

  const { data: latestSnapshot } = useLatestACSSnapshot();

  const licensesQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:ValidatorLicense:ValidatorLicense",
  );

  const couponsQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:ValidatorLicense:ValidatorFaucetCoupon",
  );

  const livenessQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:ValidatorLicense:ValidatorLivenessActivityRecord",
  );

  const validatorRightsQuery = useAggregatedTemplateData(
    latestSnapshot?.id,
    "Splice:Amulet:ValidatorRight",
  );

  const licensesData = licensesQuery.data?.data || [];
  const couponsData = couponsQuery.data?.data || [];
  const livenessData = livenessQuery.data?.data || [];
  const validatorRightsData = validatorRightsQuery.data?.data || [];
  const isLoading =
    licensesQuery.isLoading || couponsQuery.isLoading || livenessQuery.isLoading || validatorRightsQuery.isLoading;

  // Helper to safely extract field values from nested structure
  const getField = (record: any, ...fieldNames: string[]) => {
    for (const field of fieldNames) {
      if (record[field] !== undefined && record[field] !== null) return record[field];
      if (record.payload?.[field] !== undefined && record.payload?.[field] !== null) return record.payload[field];
    }
    return undefined;
  };

  // Debug logging for licenses data
  console.log("🔍 DEBUG ValidatorLicenses: Total licenses count:", licensesData.length);
  console.log("🔍 DEBUG ValidatorLicenses: First 3 licenses raw data:", licensesData.slice(0, 3));
  if (licensesData.length > 0) {
    console.log("🔍 DEBUG ValidatorLicenses: First license structure:", JSON.stringify(licensesData[0], null, 2));
  }

  const formatParty = (party: string) => {
    if (!party || party.length <= 30) return party || "Unknown";
    return `${party.substring(0, 15)}...${party.substring(party.length - 12)}`;
  };

  const filteredLicenses = licensesData.filter((lic: any) => {
    if (!searchTerm) return true;
    const validator = getField(lic, "validator", "validatorId");
    const sponsor = getField(lic, "sponsor", "sponsorId");
    return (
      (validator?.toLowerCase() || "").includes(searchTerm.toLowerCase()) ||
      (sponsor?.toLowerCase() || "").includes(searchTerm.toLowerCase())
    );
  });

  const filteredCoupons = couponsData.filter((coupon: any) => {
    if (!searchTerm) return true;
    const validator = getField(coupon, "validator", "validatorId");
    return (validator?.toLowerCase() || "").includes(searchTerm.toLowerCase());
  });

  const paginateData = (data: any[]) => {
    return data.slice((currentPage - 1) * pageSize, currentPage * pageSize);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <ACSStatusBanner />
        <div>
          <div className="flex items-center gap-2 mb-2">
            <Award className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold">Validator Licenses & Coupons</h1>
            {acsStatus?.available && (
              <Badge variant="outline" className="bg-green-500/10 text-green-500 border-green-500/30">
                <Database className="h-3 w-3 mr-1" />
                Local ACS
              </Badge>
            )}
          </div>
          <p className="text-muted-foreground">View active validator licenses and faucet coupons on the network.</p>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Active Licenses</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{licensesQuery.data?.totalContracts || 0}</p>
            )}
          </Card>

          <Card className="p-6">
            <h3 className="text-sm font-medium text-muted-foreground mb-2">Faucet Coupons</h3>
            {isLoading ? (
              <Skeleton className="h-8 w-24" />
            ) : (
              <p className="text-2xl font-bold">{couponsQuery.data?.totalContracts || 0}</p>
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
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="licenses">Licenses ({filteredLicenses.length})</TabsTrigger>
              <TabsTrigger value="coupons">Coupons ({filteredCoupons.length})</TabsTrigger>
              <TabsTrigger value="liveness">Liveness ({livenessData.length})</TabsTrigger>
              <TabsTrigger value="rights">Rights ({validatorRightsData.length})</TabsTrigger>
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
                    const lastActiveAt = license.payload?.lastActiveAt || license.lastActiveAt;
                    const lastActiveRound = license.payload?.lastActiveRound || license.lastActiveRound;
                    const roundNumber = typeof lastActiveRound === "object" ? lastActiveRound?.number : lastActiveRound;

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

                            {lastActiveAt && (
                              <div className="pt-2 border-t flex items-center gap-2 text-xs">
                                <Activity className="h-3 w-3 text-green-500" />
                                <span className="text-muted-foreground">Last Active:</span>
                                <span className="font-medium">{new Date(lastActiveAt).toLocaleString()}</span>
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

            <TabsContent value="coupons" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : filteredCoupons.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No faucet coupons found</p>
              ) : (
                <>
                  {paginateData(filteredCoupons).map((coupon: any, idx: number) => {
                    const validator = coupon.payload?.validator || coupon.validator;
                    const round = coupon.payload?.round || coupon.round;
                    const roundNumber = typeof round === "object" ? round?.number : round;

                    return (
                      <div key={idx} className="p-4 bg-muted/30 rounded-lg space-y-2">
                        <div className="flex justify-between items-start">
                          <div className="flex-1">
                            <div className="flex items-center gap-2 mb-1">
                              <Ticket className="h-4 w-4 text-primary" />
                              <p className="text-sm font-medium">Validator: {formatParty(validator)}</p>
                            </div>
                            {roundNumber && <p className="text-xs text-muted-foreground">Round: {roundNumber}</p>}
                          </div>
                          <Badge variant="secondary">Coupon</Badge>
                        </div>
                      </div>
                    );
                  })}
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={filteredCoupons.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
                  />
                </>
              )}
            </TabsContent>

            <TabsContent value="liveness" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : livenessData.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No liveness records found</p>
              ) : (
                <>
                  {paginateData(livenessData).map((record: any, idx: number) => {
                    const validator = getField(record, "validator", "validatorId");
                    const round = getField(record, "round");
                    const roundNumber = typeof round === "object" ? round?.number : round;
                    const activityTimestamp = getField(record, "timestamp", "activityTimestamp");

                    return (
                      <Card key={idx} className="p-4 space-y-2">
                        <div className="flex items-center gap-2">
                          <Activity className="h-4 w-4 text-success" />
                          <p className="text-sm font-medium">Validator: {formatParty(validator)}</p>
                        </div>
                        <div className="grid grid-cols-2 gap-3 text-xs">
                          <div>
                            <p className="text-muted-foreground">Round</p>
                            <p className="font-medium">{roundNumber || "N/A"}</p>
                          </div>
                          {activityTimestamp && (
                            <div>
                              <p className="text-muted-foreground">Activity Time</p>
                              <p className="font-medium">{new Date(activityTimestamp).toLocaleString()}</p>
                            </div>
                          )}
                        </div>
                        <Collapsible>
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-start">
                              <Code className="h-4 w-4 mr-2" />
                              Show Raw JSON
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-2">
                            <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                              {JSON.stringify(record, null, 2)}
                            </pre>
                          </CollapsibleContent>
                        </Collapsible>
                      </Card>
                    );
                  })}
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={livenessData.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
                  />
                </>
              )}
            </TabsContent>

            <TabsContent value="rights" className="space-y-3 mt-4">
              {isLoading ? (
                <div className="space-y-4">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-24 w-full" />
                  ))}
                </div>
              ) : validatorRightsData.length === 0 ? (
                <p className="text-center text-muted-foreground py-8">No validator rights found</p>
              ) : (
                <>
                  {paginateData(validatorRightsData).map((right: any, idx: number) => {
                    const user = getField(right, "user", "validatorUser");
                    const validator = getField(right, "validator", "validatorId");
                    const dso = getField(right, "dso");

                    return (
                      <Card key={idx} className="p-4 space-y-2">
                        <div className="flex items-center gap-2">
                          <Award className="h-4 w-4 text-primary" />
                          <p className="text-sm font-medium">Validator Right</p>
                        </div>
                        <div className="space-y-2 text-xs">
                          <div>
                            <p className="text-muted-foreground">User</p>
                            <p className="font-mono break-all">{user || "Unknown"}</p>
                          </div>
                          <div>
                            <p className="text-muted-foreground">Validator</p>
                            <p className="font-mono break-all">{formatParty(validator || "Unknown")}</p>
                          </div>
                          {dso && (
                            <div>
                              <p className="text-muted-foreground">DSO</p>
                              <p className="font-mono break-all">{formatParty(dso)}</p>
                            </div>
                          )}
                        </div>
                        <Collapsible>
                          <CollapsibleTrigger asChild>
                            <Button variant="ghost" size="sm" className="w-full justify-start">
                              <Code className="h-4 w-4 mr-2" />
                              Show Raw JSON
                            </Button>
                          </CollapsibleTrigger>
                          <CollapsibleContent className="mt-2">
                            <pre className="text-xs bg-muted p-3 rounded overflow-auto max-h-96">
                              {JSON.stringify(right, null, 2)}
                            </pre>
                          </CollapsibleContent>
                        </Collapsible>
                      </Card>
                    );
                  })}
                  <PaginationControls
                    currentPage={currentPage}
                    totalItems={validatorRightsData.length}
                    pageSize={pageSize}
                    onPageChange={setCurrentPage}
                  />
                </>
              )}
            </TabsContent>
          </Tabs>
        </Card>

        <DataSourcesFooter
          snapshotId={latestSnapshot?.id}
          templateSuffixes={[
            "Splice:ValidatorLicense:ValidatorLicense",
            "Splice:ValidatorLicense:ValidatorFaucetCoupon",
            "Splice:ValidatorLicense:ValidatorLivenessActivityRecord",
            "Splice:Amulet:ValidatorRight",
          ]}
          isProcessing={false}
        />
      </div>
    </DashboardLayout>
  );
};

export default ValidatorLicenses;
