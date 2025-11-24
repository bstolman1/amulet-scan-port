import DashboardLayout from "@/components/DashboardLayout";
import { Card } from "@/components/ui/card";

const Admin = () => {
  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">Admin</h1>
          <p className="text-muted-foreground">
            Administrative tools and settings
          </p>
        </div>

        <Card className="p-6">
          <h3 className="text-lg font-semibold mb-4">Admin Features</h3>
          <p className="text-muted-foreground">
            Administrative functionality coming soon.
          </p>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Admin;
