import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Database, Server, Shield } from "lucide-react";

const Admin = () => {
  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2">Admin Panel</h1>
          <p className="text-muted-foreground">System administration and configuration</p>
        </div>

        <div className="grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Database className="h-5 w-5" />
                Data Backend
              </CardTitle>
              <CardDescription>
                DuckDB-powered ledger storage
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-sm text-muted-foreground space-y-2">
                <p>All ledger data is served from the local DuckDB server with binary Protobuf storage.</p>
                <p>Governance data is sourced directly from the Canton ledger via the API.</p>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Server className="h-5 w-5" />
                API Server
              </CardTitle>
              <CardDescription>
                Express + DuckDB API endpoints
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-sm text-muted-foreground space-y-2">
                <p>The API server provides endpoints for events, updates, ACS snapshots, and governance data.</p>
                <p>All queries use parameterized SQL to prevent injection attacks.</p>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Shield className="h-5 w-5" />
                Security
              </CardTitle>
              <CardDescription>
                Input validation and sanitization
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-sm text-muted-foreground space-y-2">
                <p>SQL queries use centralized sanitization utilities to prevent injection.</p>
                <p>Dangerous patterns (UNION injection, DROP statements, etc.) are rejected at input.</p>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </DashboardLayout>
  );
};

export default Admin;
