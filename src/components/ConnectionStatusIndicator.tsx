import { useQuery } from "@tanstack/react-query";
import { Wifi, WifiOff } from "lucide-react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

const SCAN_API_URL = "https://scan.sv-1.global.canton.network.sync.global/api/scan";

export const ConnectionStatusIndicator = () => {
  const { data: isConnected, isLoading } = useQuery({
    queryKey: ["scan-api-health"],
    queryFn: async () => {
      try {
        const response = await fetch(`${SCAN_API_URL}/v0/round-of-latest-data`, {
          method: "GET",
          signal: AbortSignal.timeout(5000),
        });
        return response.ok;
      } catch {
        return false;
      }
    },
    refetchInterval: 30000, // Check every 30 seconds
    staleTime: 10000,
  });

  return (
    <div className="fixed bottom-4 right-4 z-50">
      <Tooltip>
        <TooltipTrigger asChild>
          <div
            className={`p-2 rounded-full shadow-lg border transition-all ${
              isLoading
                ? "bg-muted border-muted-foreground/20 animate-pulse"
                : isConnected
                ? "bg-green-500/20 border-green-500/50 text-green-500"
                : "bg-destructive/20 border-destructive/50 text-destructive"
            }`}
          >
            {isLoading ? (
              <Wifi className="h-5 w-5 text-muted-foreground animate-pulse" />
            ) : isConnected ? (
              <Wifi className="h-5 w-5" />
            ) : (
              <WifiOff className="h-5 w-5" />
            )}
          </div>
        </TooltipTrigger>
        <TooltipContent side="left">
          <p>
            {isLoading
              ? "Checking Scan API..."
              : isConnected
              ? "Connected to Scan API"
              : "Scan API Disconnected"}
          </p>
        </TooltipContent>
      </Tooltip>
    </div>
  );
};
