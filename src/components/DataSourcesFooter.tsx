import { Badge } from "@/components/ui/badge";
import { AlertCircle, Database } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";

interface DataSourcesFooterProps {
  snapshotId?: string;
  templateSuffixes: string[];
  isProcessing?: boolean;
}

export const DataSourcesFooter = ({ snapshotId, templateSuffixes, isProcessing = false }: DataSourcesFooterProps) => {
  return (
    <div className="mt-8 space-y-3">
      {isProcessing && (
        <Alert variant="default" className="border-yellow-500/50 bg-yellow-500/10">
          <AlertCircle className="h-4 w-4 text-yellow-500" />
          <AlertDescription className="text-sm">
            Using data from an in-progress snapshot. Data may be incomplete.
          </AlertDescription>
        </Alert>
      )}

      <div className="flex items-start gap-3 p-4 rounded-lg border bg-muted/50">
        <Database className="h-5 w-5 text-muted-foreground mt-0.5" />
        <div className="flex-1 space-y-2">
          <p className="text-sm font-medium">Data Sources</p>
          <div className="flex flex-wrap gap-2">
            {templateSuffixes.map((suffix, i) => (
              <Badge key={i} variant="secondary" className="font-mono text-xs">
                {suffix}
              </Badge>
            ))}
          </div>
          {snapshotId && (
            <p className="text-xs text-muted-foreground font-mono">Snapshot: {snapshotId.substring(0, 8)}...</p>
          )}
        </div>
      </div>
    </div>
  );
};
