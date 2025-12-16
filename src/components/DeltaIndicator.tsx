import { Badge } from "@/components/ui/badge";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

interface DeltaIndicatorProps {
  created?: number;
  archived?: number;
  since?: string;
  compact?: boolean;
}

export function DeltaIndicator({ created = 0, archived = 0, since, compact = false }: DeltaIndicatorProps) {
  const netChange = created - archived;
  
  if (created === 0 && archived === 0) {
    return null;
  }

  const content = compact ? (
    <Badge 
      variant="outline" 
      className={`text-xs ${
        netChange > 0 
          ? "bg-success/10 text-success border-success/30" 
          : netChange < 0 
            ? "bg-destructive/10 text-destructive border-destructive/30"
            : "bg-muted text-muted-foreground"
      }`}
    >
      {netChange > 0 ? <TrendingUp className="h-3 w-3 mr-1" /> : netChange < 0 ? <TrendingDown className="h-3 w-3 mr-1" /> : <Minus className="h-3 w-3 mr-1" />}
      {netChange > 0 ? "+" : ""}{netChange}
    </Badge>
  ) : (
    <div className="flex items-center gap-1.5">
      {created > 0 && (
        <Badge variant="outline" className="text-xs bg-success/10 text-success border-success/30">
          +{created}
        </Badge>
      )}
      {archived > 0 && (
        <Badge variant="outline" className="text-xs bg-destructive/10 text-destructive border-destructive/30">
          -{archived}
        </Badge>
      )}
    </div>
  );

  if (!since) return content;

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          {content}
        </TooltipTrigger>
        <TooltipContent>
          <p className="text-xs">
            {created} created, {archived} archived since snapshot
            {since && <span className="block text-muted-foreground">{new Date(since).toLocaleString()}</span>}
          </p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
