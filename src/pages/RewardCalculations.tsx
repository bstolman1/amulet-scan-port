// Page commented out - temporarily disabled
/*
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Calculator,
  CalendarIcon,
  Search,
  Award,
  Coins,
  FileText,
  AlertCircle,
  Loader2,
} from "lucide-react";
import { format } from "date-fns";
import { cn } from "@/lib/utils";
import { getDuckDBApiUrl } from "@/lib/backend-config";
*/

import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent } from "@/components/ui/card";
import { AlertCircle } from "lucide-react";

interface RewardResult {
  partyId: string;
  totalRewards: number;
  totalWeight: number;
  rewardCount: number;
  byRound: Record<string, { count: number; amount: number; weight: number }>;
  events: Array<{
    event_id: string;
    round: number;
    amount: number;
    weight: number;
    effective_at: string;
    template_id: string;
    templateType: string;
  }>;
  hasIssuanceData: boolean;
  note: string | null;
}

// Original RewardCalculations component commented out - see above for full code
const RewardCalculations = () => {
  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2 flex items-center gap-3">
            <AlertCircle className="h-8 w-8 text-muted-foreground" />
            Reward Calculations
          </h1>
          <p className="text-muted-foreground">
            This page is temporarily disabled.
          </p>
        </div>
        <Card className="bg-muted/30">
          <CardContent className="pt-6">
            <div className="text-center text-muted-foreground py-8">
              <AlertCircle className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>Reward Calculations page has been temporarily disabled.</p>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default RewardCalculations;
