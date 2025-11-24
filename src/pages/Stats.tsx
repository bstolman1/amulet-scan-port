import DashboardLayout from "@/components/DashboardLayout";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";

const Stats = () => {
  const { data: aggregatedData, isLoading } = useAggregatedTemplateData();

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Statistics</h1>
          <p className="text-muted-foreground">
            Network-wide statistics and metrics
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {aggregatedData?.map((item) => (
              <Card key={item.template_name} className="p-6">
                <div className="space-y-2">
                  <div className="text-sm text-muted-foreground">
                    {item.template_name.split(":").pop()}
                  </div>
                  <div className="text-2xl font-bold">
                    {item.instance_count.toLocaleString()}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    Active Instances
                  </div>
                </div>
              </Card>
            ))}
          </div>
        )}
      </div>
    </DashboardLayout>
  );
};

export default Stats;
