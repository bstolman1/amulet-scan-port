import { DashboardLayout } from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useACSTemplateData, useACSTemplates } from "@/hooks/use-acs-template-data";
import { useLatestACSSnapshot } from "@/hooks/use-acs-snapshots";
import { Database, FileJson, ChevronRight, ChevronDown } from "lucide-react";
import { useState } from "react";
import { getPagesThatUseTemplate } from "@/lib/template-page-map";

const TemplateRow = ({
  snapshotId,
  templateId,
  contractCount,
}: {
  snapshotId: string;
  templateId: string;
  contractCount: number;
}) => {
  const [open, setOpen] = useState(false);
  const suffix = templateId.split(":").slice(-3).join(":");
  const pages = getPagesThatUseTemplate(templateId);
  const { data, isLoading } = useACSTemplateData(snapshotId, templateId, open);

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
                <Badge variant="secondary" className="text-xs">
                  {contractCount} entries
                </Badge>
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
                {data?.data && data.data.length > 0 ? (
                  <>
                    <p className="text-xs text-muted-foreground">
                      Showing first {Math.min(3, data.data.length)} of {data.data.length} records
                    </p>
                    <div className="grid gap-3 md:grid-cols-2">
                      {data.data.slice(0, 3).map((entry: any, idx: number) => (
                        <Card key={idx} className="p-3 overflow-auto">
                          <pre className="text-xs leading-snug">{JSON.stringify(entry, null, 2)}</pre>
                        </Card>
                      ))}
                    </div>
                  </>
                ) : (
                  <p className="text-sm text-muted-foreground">No data found for this template.</p>
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
  const { data: latestSnapshot } = useLatestACSSnapshot();
  const { data: templates, isLoading } = useACSTemplates(latestSnapshot?.id);

  const [query, setQuery] = useState("");
  const filtered = (templates || []).filter((t: any) => t.template_id.toLowerCase().includes(query.toLowerCase()));

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <h1 className="text-2xl font-bold">Template Coverage Audit</h1>
            <p className="text-sm text-muted-foreground">
              Verify every template's data is discoverable and which pages consume it.
            </p>
          </div>
          <Badge variant="secondary" className="gap-2">
            <Database className="h-4 w-4" />
            Snapshot {latestSnapshot?.id ? `${latestSnapshot.id.substring(0, 8)}...` : "loading"}
          </Badge>
        </div>

        <div className="grid gap-4">
          <div className="flex items-center gap-3">
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search templates"
              className="w-full md:w-80 rounded-md border bg-background px-3 py-2 text-sm"
            />
            {templates && (
              <Badge variant="outline" className="text-xs">
                {filtered.length} of {templates.length} templates
              </Badge>
            )}
          </div>

          {isLoading ? (
            <div className="grid gap-3">
              <Skeleton className="h-14 w-full" />
              <Skeleton className="h-14 w-full" />
              <Skeleton className="h-14 w-full" />
            </div>
          ) : (
            <div className="grid gap-3">
              {filtered.map((t: any) => (
                <TemplateRow
                  key={t.template_id}
                  snapshotId={latestSnapshot.id}
                  templateId={t.template_id}
                  contractCount={t.contract_count}
                />
              ))}
            </div>
          )}
        </div>
      </div>
    </DashboardLayout>
  );
};

export default TemplateAudit;
