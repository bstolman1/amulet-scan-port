import { useQuery } from "@tanstack/react-query";
import { Wifi, WifiOff } from "lucide-react";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

// Connection status checks our backend via scan-proxy (not DuckDB)
// Rule: Browser → our API → Scan API (never browser → Scan directly)

export const ConnectionStatusIndicator = () => {
  const { data: isConnected, isLoading } = useQuery({
    queryKey: ["backend-api-health"],
    queryFn: async () => {
      try {
        // Use scan-proxy which works without DuckDB
        const response = await fetch("/api/scan-proxy/v0/dso", {
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
              ? "Checking API..."
              : isConnected
              ? "Connected to Backend API"
              : "Backend API Disconnected"}
          </p>
        </TooltipContent>
      </Tooltip>
    </div>
  );
};
