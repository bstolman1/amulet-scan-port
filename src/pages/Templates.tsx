import { useState } from "react";
import DashboardLayout from "@/components/DashboardLayout";
import { useAggregatedTemplateData } from "@/hooks/use-aggregated-template-data";
import { Card } from "@/components/ui/card";
import { Loader2 } from "lucide-react";
import SearchBar from "@/components/SearchBar";

const Templates = () => {
  const [searchTerm, setSearchTerm] = useState("");
  const { data: aggregatedData, isLoading } = useAggregatedTemplateData();

  const filteredTemplates = aggregatedData?.filter((template) => {
    const searchLower = searchTerm.toLowerCase();
    return template.template_name.toLowerCase().includes(searchLower);
  });

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Templates</h1>
          <p className="text-muted-foreground">
            All contract templates and their instance counts
          </p>
        </div>

        <SearchBar
          value={searchTerm}
          onChange={setSearchTerm}
          placeholder="Search templates..."
        />

        {isLoading ? (
          <div className="flex items-center justify-center min-h-[400px]">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
          </div>
        ) : (
          <div className="space-y-2">
            {filteredTemplates?.map((template) => (
              <Card key={template.template_name} className="p-4">
                <div className="flex justify-between items-center">
                  <div className="font-medium">{template.template_name}</div>
                  <div className="text-lg font-semibold">
                    {template.instance_count.toLocaleString()}
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

export default Templates;
