import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useDsoInfo, useStateAcs } from "@/hooks/use-canton-scan-api";
import { Database, FileJson, ChevronRight, ChevronDown, Code } from "lucide-react";
import { useState } from "react";
import { getPagesThatUseTemplate } from "@/lib/template-page-map";
import { Button } from "@/components/ui/button";

const TemplateRow = ({
  templateId,
}: {
  templateId: string;
}) => {
  const [open, setOpen] = useState(false);
  const suffix = templateId.split(":").slice(-2).join(":");
  const pages = getPagesThatUseTemplate(templateId);

  // Only fetch data when expanded
  const { data, isLoading } = useStateAcs(open ? [templateId] : [], 3);

  return (
    <Card className="p-0 overflow-hidden">
      <Collapsible open={open} onOpenChange={setOpen}>
        <div className="flex items-center justify-between px-4 py-3">
          <div className="flex items-center gap-3">
            <button
              className="inline-flex items-center justify-center"
              onClick={() => setOpen(!open)}
              aria-label={open ? "Collapse" : "Expand"}
            >
              {open ? (
                <ChevronDown className="h-4 w-4 text-muted-foreground" />
              ) : (
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
              )}
            </button>
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <FileJson className="h-4 w-4 text-primary" />
                <code className="text-sm font-mono">{suffix}</code>
              </div>
              <div className="flex flex-wrap gap-2">
                {pages.length > 0 ? (
                  pages.map((p) => (
                    <Badge key={p} variant="outline" className="text-xs">
                      Used on: {p}
                    </Badge>
                  ))
                ) : (
                  <Badge variant="destructive" className="text-xs">
                    Not used on any page
                  </Badge>
                )}
              </div>
            </div>
          </div>
        </div>
        <CollapsibleContent>
          <div className="border-t px-4 py-3 bg-muted/30">
            {isLoading ? (
              <Skeleton className="h-16 w-full" />
            ) : (
              <div className="space-y-3">
                {data && data.length > 0 ? (
                  <>
                    <p className="text-xs text-muted-foreground">
                      Showing {Math.min(3, data.length)} sample record(s) from live network
                    </p>
                    <div className="grid gap-3 md:grid-cols-2">
                      {data.slice(0, 3).map((entry: any, idx: number) => (
                        <Card key={idx} className="p-3 overflow-auto">
                          <pre className="text-xs leading-snug">{JSON.stringify(entry, null, 2)}</pre>
                        </Card>
                      ))}
                    </div>
                  </>
                ) : (
                  <p className="text-sm text-muted-foreground">No active contracts found for this template.</p>
                )}
              </div>
            )}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  );
};

const TemplateAudit = () => {
  const { data: dsoInfo, isLoading } = useDsoInfo();
  const [query, setQuery] = useState("");

  // Common templates to audit
  const auditTemplates = [
    "Splice.AmuletRules:AmuletRules",
    "Splice.Amulet:Amulet",
    "Splice.Amulet:ValidatorRewardCoupon",
    "Splice.Amulet:SvRewardCoupon",
    "Splice.Amulet:AppRewardCoupon",
    "Splice.ValidatorLicense:ValidatorLicense",
    "Splice.Ans:AnsEntry",
    "Splice.ExternalPartyAmuletRules:TransferCommandCounter",
    "Splice.ExternalPartyAmuletRules:ExternalPartySetupProposal",
    "Splice.FeaturedAppRight:FeaturedAppRight",
    "Splice.Wallet:LockedAmulet",
    "Splice.Subscription:SubscriptionRequest",
    "Splice.Subscription:SubscriptionInitialPayment",
  ];

  const filtered = auditTemplates.filter((t) => t.toLowerCase().includes(query.toLowerCase()));

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <h1 className="text-2xl font-bold">Template Coverage Audit</h1>
            <p className="text-sm text-muted-foreground">
              Verify template data availability from the live Canton network.
            </p>
          </div>
          <Badge variant="secondary" className="gap-2">
            <Database className="h-4 w-4" />
            Live API
          </Badge>
        </div>

        {dsoInfo && (
          <Card className="p-4">
            <div className="flex items-center gap-4 text-sm">
              <div className="flex items-center gap-2">
                <span className="text-muted-foreground">Latest Round:</span>
                <code className="text-foreground font-semibold">
                  {dsoInfo.latest_mining_round?.contract?.payload?.round?.number || "—"}
                </code>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-muted-foreground">SV Nodes:</span>
                <code className="text-foreground font-semibold">{dsoInfo.sv_node_states?.length || 0}</code>
              </div>
            </div>
          </Card>
        )}

        <div className="grid gap-4">
          <div className="flex items-center gap-3">
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search templates"
              className="w-full md:w-80 rounded-md border bg-background px-3 py-2 text-sm"
            />
            <Badge variant="outline" className="text-xs">
              {filtered.length} of {auditTemplates.length} templates
            </Badge>
          </div>

          {isLoading ? (
            <div className="grid gap-3">
              <Skeleton className="h-14 w-full" />
              <Skeleton className="h-14 w-full" />
              <Skeleton className="h-14 w-full" />
            </div>
          ) : (
            <div className="grid gap-3">
              {filtered.map((templateId) => (
                <TemplateRow key={templateId} templateId={templateId} />
              ))}
            </div>
          )}
        </div>

        <Card className="p-4 text-xs text-muted-foreground">
          <p>
            Data sourced from Canton Scan API <code>/v0/state/acs</code> endpoint.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default TemplateAudit;