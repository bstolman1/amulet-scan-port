import { useTriggerACSSnapshot, useACSSnapshots } from "@/hooks/use-acs-snapshots";
import { Button } from "@/components/ui/button";
import { RefreshCw } from "lucide-react";

export const TriggerACSSnapshotButton = () => {
  const { mutate: triggerSnapshot, isPending } = useTriggerACSSnapshot();
  const { data: snapshots } = useACSSnapshots();

  // Check if there's already a snapshot in progress
  const hasSnapshotInProgress = snapshots?.some(s => s.status === 'processing');

  return (
    <Button
      onClick={() => triggerSnapshot()}
      disabled={isPending || hasSnapshotInProgress}
      variant="outline"
      size="sm"
    >
      <RefreshCw className={`h-4 w-4 mr-2 ${(isPending || hasSnapshotInProgress) ? 'animate-spin' : ''}`} />
      {hasSnapshotInProgress ? 'Snapshot In Progress...' : isPending ? 'Starting Snapshot...' : 'Trigger ACS Snapshot'}
    </Button>
  );
};
