import { useState, useEffect } from "react";
import { DashboardLayout } from "@/components/DashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { 
  ExternalLink, 
  RefreshCw, 
  FileText, 
  Calendar,
  User,
  AlertCircle,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { getDuckDBApiUrl } from "@/lib/backend-config";

interface Announcement {
  id: string;
  subject: string;
  date: string;
  content: string;
  excerpt: string;
  sourceUrl?: string;
  linkedUrls: string[];
  sender?: string;
  messageCount?: number;
}

const GovernanceFlow = () => {
  const [announcements, setAnnouncements] = useState<Announcement[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());

  const fetchAnnouncements = async () => {
    setIsLoading(true);
    setError(null);
    
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/announcements?limit=50`);
      
      if (!response.ok) {
        throw new Error(`Failed to fetch: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (data.error) {
        throw new Error(data.error);
      }
      
      setAnnouncements(data.announcements || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch announcements');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchAnnouncements();
  }, []);

  const toggleExpand = (id: string) => {
    setExpandedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const formatDate = (dateStr: string) => {
    try {
      return new Date(dateStr).toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
      });
    } catch {
      return dateStr;
    }
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold">Governance Flow</h1>
            <p className="text-muted-foreground mt-1">
              Track SV announcements from Groups.io ({announcements.length} found)
            </p>
          </div>
          <Button 
            onClick={fetchAnnouncements} 
            disabled={isLoading}
            className="gap-2"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />
            Refresh
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
            Array.from({ length: 5 }).map((_, i) => (
              <Card key={i}>
                <CardHeader>
                  <Skeleton className="h-6 w-3/4" />
                  <Skeleton className="h-4 w-1/4 mt-2" />
                </CardHeader>
                <CardContent>
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-full mt-2" />
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
                  {error ? 'Check API key configuration' : 'No announcements found'}
                </p>
              </CardContent>
            </Card>
          ) : (
            announcements.map((announcement) => {
              const isExpanded = expandedIds.has(announcement.id);
              const hasLongContent = announcement.content.length > 300;
              
              return (
                <Card key={announcement.id} className="hover:border-primary/30 transition-colors">
                  <CardHeader className="pb-3">
                    <div className="flex items-start justify-between gap-4">
                      <div className="space-y-2 flex-1 min-w-0">
                        <CardTitle className="text-lg leading-tight">
                          {announcement.subject}
                        </CardTitle>
                        <div className="flex flex-wrap items-center gap-3 text-sm text-muted-foreground">
                          <span className="flex items-center gap-1">
                            <Calendar className="h-3.5 w-3.5" />
                            {formatDate(announcement.date)}
                          </span>
                          {announcement.sender && (
                            <span className="flex items-center gap-1">
                              <User className="h-3.5 w-3.5" />
                              {announcement.sender}
                            </span>
                          )}
                          {announcement.messageCount && announcement.messageCount > 1 && (
                            <Badge variant="secondary">
                              {announcement.messageCount} messages
                            </Badge>
                          )}
                          {announcement.linkedUrls.length > 0 && (
                            <Badge variant="outline" className="text-primary">
                              {announcement.linkedUrls.length} linked URLs
                            </Badge>
                          )}
                        </div>
                      </div>
                      {announcement.sourceUrl && (
                        <Button variant="outline" size="sm" asChild className="shrink-0">
                          <a 
                            href={announcement.sourceUrl} 
                            target="_blank" 
                            rel="noopener noreferrer"
                            className="gap-1.5"
                          >
                            <ExternalLink className="h-3.5 w-3.5" />
                            View
                          </a>
                        </Button>
                      )}
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    {/* Content */}
                    <div className="relative">
                      <ScrollArea className={isExpanded ? "max-h-96" : ""}>
                        <div className="text-sm text-muted-foreground whitespace-pre-wrap break-words">
                          {isExpanded ? announcement.content : announcement.excerpt}
                          {!isExpanded && hasLongContent && '...'}
                        </div>
                      </ScrollArea>
                      {hasLongContent && (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => toggleExpand(announcement.id)}
                          className="mt-2 gap-1"
                        >
                          {isExpanded ? (
                            <>
                              <ChevronUp className="h-4 w-4" />
                              Show Less
                            </>
                          ) : (
                            <>
                              <ChevronDown className="h-4 w-4" />
                              Show More
                            </>
                          )}
                        </Button>
                      )}
                    </div>
                    
                    {/* Linked URLs */}
                    {announcement.linkedUrls.length > 0 && (
                      <div className="space-y-2 pt-2 border-t border-border/50">
                        <h4 className="text-sm font-medium">
                          Extracted URLs ({announcement.linkedUrls.length})
                        </h4>
                        <div className="space-y-1">
                          {announcement.linkedUrls.map((url, idx) => (
                            <a
                              key={idx}
                              href={url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="block text-xs text-primary hover:underline truncate"
                            >
                              {url}
                            </a>
                          ))}
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
              );
            })
          )}
        </div>

        {/* Info Card */}
        <Card className="bg-muted/30 border-dashed">
          <CardContent className="py-4">
            <p className="text-sm text-muted-foreground">
              <strong>Note:</strong> Fetching announcements from the supervalidator-announce 
              Groups.io mailing list. URLs related to Canton Network are automatically extracted.
            </p>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
};

export default GovernanceFlow;
