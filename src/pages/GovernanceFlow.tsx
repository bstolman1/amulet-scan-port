import { useState } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { 
  ExternalLink, 
  RefreshCw, 
  FileText, 
  Calendar,
  ArrowRight,
  AlertCircle,
} from "lucide-react";

interface Announcement {
  id: string;
  subject: string;
  date: string;
  excerpt: string;
  sourceUrl?: string;
  linkedUrls: string[];
  status: "pending" | "fetched" | "error";
}

// Placeholder data - will be replaced with actual API data
const mockAnnouncements: Announcement[] = [
  {
    id: "1",
    subject: "Governance Proposal #42 - Network Parameter Update",
    date: "2025-01-08",
    excerpt: "Proposal to adjust network parameters for improved throughput...",
    sourceUrl: "https://lists.sync.global/g/supervalidator-announce/message/42",
    linkedUrls: [
      "https://scan.sv-1.global.canton.network.sync.global/api/scan/v0/dso/sv-node-states",
    ],
    status: "pending",
  },
  {
    id: "2", 
    subject: "SV Election Results - December 2024",
    date: "2025-01-05",
    excerpt: "Results of the latest Super Validator election cycle...",
    sourceUrl: "https://lists.sync.global/g/supervalidator-announce/message/41",
    linkedUrls: [
      "https://scan.sv-1.global.canton.network.sync.global/api/scan/v0/dso/info",
    ],
    status: "fetched",
  },
];

const GovernanceFlow = () => {
  const [announcements, setAnnouncements] = useState<Announcement[]>(mockAnnouncements);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refreshAnnouncements = async () => {
    setIsLoading(true);
    setError(null);
    
    // TODO: Call local API to fetch from Groups.io
    // This will be connected to the Node.js script output
    setTimeout(() => {
      setIsLoading(false);
    }, 1500);
  };

  const fetchLinkedData = async (announcementId: string, url: string) => {
    // TODO: Fetch data from the linked URL
    console.log(`Fetching data for announcement ${announcementId} from ${url}`);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold">Governance Flow</h1>
            <p className="text-muted-foreground mt-1">
              Track SV announcements and linked governance data
            </p>
          </div>
          <Button 
            onClick={refreshAnnouncements} 
            disabled={isLoading}
            className="gap-2"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />
            Refresh Announcements
          </Button>
        </div>

        {/* Error State */}
        {error && (
          <Card className="border-destructive/50 bg-destructive/10">
            <CardContent className="flex items-center gap-3 py-4">
              <AlertCircle className="h-5 w-5 text-destructive" />
              <span className="text-destructive">{error}</span>
            </CardContent>
          </Card>
        )}

        {/* Announcements List */}
        <div className="space-y-4">
          {isLoading ? (
            // Loading skeletons
            Array.from({ length: 3 }).map((_, i) => (
              <Card key={i}>
                <CardHeader>
                  <Skeleton className="h-6 w-3/4" />
                  <Skeleton className="h-4 w-1/4 mt-2" />
                </CardHeader>
                <CardContent>
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-2/3 mt-2" />
                </CardContent>
              </Card>
            ))
          ) : announcements.length === 0 ? (
            <Card>
              <CardContent className="flex flex-col items-center justify-center py-12">
                <FileText className="h-12 w-12 text-muted-foreground mb-4" />
                <h3 className="text-lg font-medium">No Announcements</h3>
                <p className="text-muted-foreground text-sm mt-1">
                  Click refresh to fetch the latest announcements from Groups.io
                </p>
              </CardContent>
            </Card>
          ) : (
            announcements.map((announcement) => (
              <Card key={announcement.id} className="hover:border-primary/30 transition-colors">
                <CardHeader>
                  <div className="flex items-start justify-between gap-4">
                    <div className="space-y-1">
                      <CardTitle className="text-lg">{announcement.subject}</CardTitle>
                      <div className="flex items-center gap-3 text-sm text-muted-foreground">
                        <span className="flex items-center gap-1">
                          <Calendar className="h-3.5 w-3.5" />
                          {announcement.date}
                        </span>
                        <Badge 
                          variant={
                            announcement.status === "fetched" 
                              ? "default" 
                              : announcement.status === "error" 
                              ? "destructive" 
                              : "secondary"
                          }
                        >
                          {announcement.status}
                        </Badge>
                      </div>
                    </div>
                    {announcement.sourceUrl && (
                      <Button variant="outline" size="sm" asChild>
                        <a 
                          href={announcement.sourceUrl} 
                          target="_blank" 
                          rel="noopener noreferrer"
                          className="gap-1.5"
                        >
                          <ExternalLink className="h-3.5 w-3.5" />
                          View Source
                        </a>
                      </Button>
                    )}
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <p className="text-sm text-muted-foreground">{announcement.excerpt}</p>
                  
                  {/* Linked URLs */}
                  {announcement.linkedUrls.length > 0 && (
                    <div className="space-y-2">
                      <h4 className="text-sm font-medium flex items-center gap-2">
                        <ArrowRight className="h-4 w-4 text-primary" />
                        Linked Data Sources
                      </h4>
                      <div className="space-y-2 pl-6">
                        {announcement.linkedUrls.map((url, idx) => (
                          <div 
                            key={idx}
                            className="flex items-center justify-between gap-4 p-3 rounded-lg bg-muted/50 border border-border/50"
                          >
                            <code className="text-xs text-muted-foreground truncate flex-1">
                              {url}
                            </code>
                            <Button 
                              variant="ghost" 
                              size="sm"
                              onClick={() => fetchLinkedData(announcement.id, url)}
                              className="shrink-0 gap-1.5"
                            >
                              <RefreshCw className="h-3.5 w-3.5" />
                              Fetch
                            </Button>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            ))
          )}
        </div>

        {/* Info Card */}
        <Card className="bg-muted/30 border-dashed">
          <CardContent className="py-4">
            <p className="text-sm text-muted-foreground">
              <strong>Note:</strong> This page fetches announcements from the supervalidator-announce 
              Groups.io list. The linked URLs are extracted from announcement content and can be 
              used to pull relevant governance data from the Canton Network.
            </p>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default GovernanceFlow;
