import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Database, Info } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";

const Admin = () => {
  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2">Admin Panel</h1>
          <p className="text-muted-foreground">System administration and configuration</p>
        </div>

        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>DuckDB-Only Mode</AlertTitle>
          <AlertDescription>
            This dashboard now uses DuckDB exclusively for all data operations. 
            Previous Supabase-based CIP voting functionality has been removed.
          </AlertDescription>
        </Alert>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              Backend Status
            </CardTitle>
            <CardDescription>
              All ledger data is now served from the local DuckDB server
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="text-sm text-muted-foreground">
              <p>The admin features for CIP and Featured App voting have been deprecated.</p>
              <p className="mt-2">
                Governance data is now sourced directly from the Canton ledger via the DuckDB API.
              </p>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default Admin;
