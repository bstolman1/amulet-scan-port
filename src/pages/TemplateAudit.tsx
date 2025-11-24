import DashboardLayout from "@/components/DashboardLayout";
import { useAcsTemplateData } from "@/hooks/use-acs-template-data";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";

const TemplateAudit = () => {
  const { data: templateData, isLoading } = useAcsTemplateData({ limit: 100 });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Template Audit</h1>
          <p className="text-muted-foreground">
            Historical template statistics by round
          </p>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-2">
            {templateData?.map((stat) => (
              <Card key={stat.id} className="p-4">
                <div className="flex justify-between items-center">
                  <div>
                    <div className="font-medium">{stat.template_name}</div>
                    <div className="text-sm text-muted-foreground">
                      Round {stat.round}
                    </div>
                  </div>
                  <div className="text-lg font-semibold">
                    {stat.instance_count.toLocaleString()}
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

export default TemplateAudit;
