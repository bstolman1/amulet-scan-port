import { AlertTriangle, RefreshCw, Database } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useACSStatus } from "@/hooks/use-local-acs";

interface ACSStatusBannerProps {
  /** If true, shows nothing when data is available (default: true) */
  hideWhenAvailable?: boolean;
}

/**
 * Banner component that shows ACS availability status.
 * Displays a warning when an ACS snapshot is in progress and no complete data is available.
 */
export function ACSStatusBanner({ hideWhenAvailable = true }: ACSStatusBannerProps) {
  const { data: status, isLoading } = useACSStatus();

  // Don't show anything while loading initial status
  if (isLoading) return null;

  // If available and we should hide, return nothing
  if (status?.available && hideWhenAvailable) return null;

  // Snapshot in progress with no complete data
  if (status?.snapshotInProgress && !status?.available) {
    return (
      <Alert variant="default" className="mb-4 border-amber-500/50 bg-amber-500/10">
        <RefreshCw className="h-4 w-4 animate-spin text-amber-500" />
        <AlertTitle className="text-amber-500">ACS Snapshot In Progress</AlertTitle>
        <AlertDescription className="text-muted-foreground">
          A new ACS snapshot is being fetched. This page will be available once the snapshot completes. 
          This typically takes a few minutes.
        </AlertDescription>
      </Alert>
    );
  }

  // Snapshot in progress but we have stale data
  if (status?.snapshotInProgress && status?.available) {
    return (
      <Alert variant="default" className="mb-4 border-blue-500/30 bg-blue-500/5">
        <RefreshCw className="h-4 w-4 animate-spin text-blue-400" />
        <AlertTitle className="text-blue-400">Updating ACS Data</AlertTitle>
        <AlertDescription className="text-muted-foreground">
          A new ACS snapshot is being fetched. Current data is from{" "}
          {status.latestComplete?.snapshotTime 
            ? new Date(status.latestComplete.snapshotTime).toLocaleString()
            : "a previous snapshot"}.
        </AlertDescription>
      </Alert>
    );
  }

  // No DuckDB data - but that's fine, we use live SCAN data
  if (!status?.available) {
    return (
      <Alert variant="default" className="mb-4 border-muted">
        <Database className="h-4 w-4 text-muted-foreground" />
        <AlertTitle className="text-muted-foreground">Using Live Network Data</AlertTitle>
        <AlertDescription>
          Data is fetched directly from the Canton Scan API.
        </AlertDescription>
      </Alert>
    );
  }

  // Data available (only shown if hideWhenAvailable is false)
  return (
    <Alert variant="default" className="mb-4 border-green-500/30 bg-green-500/5">
      <Database className="h-4 w-4 text-green-400" />
      <AlertTitle className="text-green-400">ACS Data Available</AlertTitle>
      <AlertDescription className="text-muted-foreground">
        Using snapshot from{" "}
        {status.latestComplete?.snapshotTime 
          ? new Date(status.latestComplete.snapshotTime).toLocaleString()
          : "the latest available snapshot"}.
      </AlertDescription>
    </Alert>
  );
}

/**
 * Simple wrapper to conditionally render children only when ACS is available
 */
export function ACSDataRequired({ children, fallback }: { children: React.ReactNode; fallback?: React.ReactNode }) {
  const { data: status, isLoading } = useACSStatus();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-muted-foreground">Checking ACS status...</span>
      </div>
    );
  }

  if (!status?.available) {
    if (fallback) return <>{fallback}</>;
    return <ACSStatusBanner hideWhenAvailable={false} />;
  }

  return <>{children}</>;
}