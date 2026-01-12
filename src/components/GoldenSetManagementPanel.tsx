import React, { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter, DialogDescription } from "@/components/ui/dialog";
import { useToast } from "@/hooks/use-toast";
import { getDuckDBApiUrl } from "@/lib/backend-config";
import { cn } from "@/lib/utils";
import {
  Trophy,
  Plus,
  Trash2,
  Play,
  History,
  Target,
  CheckCircle2,
  XCircle,
  TrendingUp,
  TrendingDown,
  RefreshCw,
  AlertTriangle,
  Info,
  FileText,
  Clock,
  BarChart3,
  Shield,
  Sparkles,
  Shuffle,
  Download,
} from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";

interface GoldenItem {
  id: string;
  subject: string;
  body?: string;
  trueType: string;
  addedAt: string;
  addedBy: string;
  notes?: string;
  category: 'standard' | 'edge_case' | 'boundary';
  sourceUrl?: string;
  frozen: boolean;
}

interface GoldenSetData {
  version: string;
  createdAt: string;
  lastModified: string | null;
  items: GoldenItem[];
  stats: {
    total: number;
    byType: Record<string, number>;
    byCategory: Record<string, number>;
  };
}

interface EvaluationResult {
  classifierVersion: string;
  evaluatedAt: string;
  goldenSetSize: number;
  correct: number;
  incorrect: number;
  accuracy: string;
  byType: Record<string, { total: number; correct: number; accuracy: string }>;
  byCategory: Record<string, { total: number; correct: number; accuracy: string }>;
  regressions: Array<{
    id: string;
    subject: string;
    trueType: string;
    previousPrediction: string;
    currentPrediction: string;
  }>;
  improvements: Array<{
    id: string;
    subject: string;
    trueType: string;
    previousPrediction: string;
    currentPrediction: string;
  }>;
  previousAccuracy?: string;
  accuracyDelta?: string;
  previousVersion?: string;
}

interface GoldenSetSummary {
  version: string;
  itemCount: number;
  stats: {
    total: number;
    byType: Record<string, number>;
    byCategory: Record<string, number>;
  };
  lastModified: string | null;
  lastEvaluation: {
    accuracy: string;
    classifierVersion: string;
    evaluatedAt: string;
    regressions: number;
    improvements: number;
  } | null;
  evaluationCount: number;
}

const GOVERNANCE_TYPES = [
  'election',
  'validator-onboarding',
  'validator-offboarding', 
  'featured-app',
  'cip',
  'protocol-upgrade',
  'outcome',
  'other',
];

const CATEGORY_OPTIONS = [
  { value: 'standard', label: 'Standard', description: 'Typical case' },
  { value: 'edge_case', label: 'Edge Case', description: 'Unusual but valid' },
  { value: 'boundary', label: 'Boundary', description: 'Between categories' },
];

export function GoldenSetManagementPanel() {
  const { toast } = useToast();
  const [summary, setSummary] = useState<GoldenSetSummary | null>(null);
  const [fullSet, setFullSet] = useState<GoldenSetData | null>(null);
  const [evaluationHistory, setEvaluationHistory] = useState<EvaluationResult[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isEvaluating, setIsEvaluating] = useState(false);
  const [showAddDialog, setShowAddDialog] = useState(false);
  const [showHistoryDialog, setShowHistoryDialog] = useState(false);
  const [selectedEval, setSelectedEval] = useState<EvaluationResult | null>(null);
  
  // Add item form state
  const [newItem, setNewItem] = useState({
    id: '',
    subject: '',
    body: '',
    trueType: '',
    notes: '',
    category: 'standard' as 'standard' | 'edge_case' | 'boundary',
  });
  
  // Sample from existing state
  const [showSampleDialog, setShowSampleDialog] = useState(false);
  const [sampledItems, setSampledItems] = useState<Array<{
    id: string;
    subject: string;
    excerpt?: string;
    sourceUrl?: string;
    stage?: string;
    selected: boolean;
    trueType: string;
  }>>([]);
  const [isSampling, setIsSampling] = useState(false);
  const [isAddingSamples, setIsAddingSamples] = useState(false);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setIsLoading(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      
      const [summaryRes, fullRes, historyRes] = await Promise.all([
        fetch(`${baseUrl}/api/governance-lifecycle/golden-set`),
        fetch(`${baseUrl}/api/governance-lifecycle/golden-set/full`),
        fetch(`${baseUrl}/api/governance-lifecycle/golden-set/history`),
      ]);
      
      if (summaryRes.ok) {
        setSummary(await summaryRes.json());
      }
      if (fullRes.ok) {
        setFullSet(await fullRes.json());
      }
      if (historyRes.ok) {
        const data = await historyRes.json();
        setEvaluationHistory(data.evaluations || []);
      }
    } catch (error) {
      console.error('Failed to fetch golden set data:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const runEvaluation = async () => {
    setIsEvaluating(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/golden-set/evaluate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ classifierVersion: `v${Date.now()}` }),
      });
      
      if (response.ok) {
        const result = await response.json();
        toast({
          title: `Evaluation Complete: ${result.accuracy}% Accuracy`,
          description: `${result.correct}/${result.goldenSetSize} correct. ${result.regressions?.length || 0} regressions, ${result.improvements?.length || 0} improvements.`,
          className: result.regressions?.length > 0 
            ? "bg-yellow-900/90 border-yellow-500/50 text-yellow-100"
            : "bg-green-900/90 border-green-500/50 text-green-100",
        });
        await fetchData();
      } else {
        const error = await response.json();
        toast({
          title: "Evaluation Failed",
          description: error.error || "Failed to evaluate classifier",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Evaluation Failed",
        description: error instanceof Error ? error.message : "Network error",
        variant: "destructive",
      });
    } finally {
      setIsEvaluating(false);
    }
  };

  const addItem = async () => {
    if (!newItem.id || !newItem.subject || !newItem.trueType) {
      toast({
        title: "Missing Fields",
        description: "ID, Subject, and True Type are required",
        variant: "destructive",
      });
      return;
    }

    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/golden-set`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(newItem),
      });
      
      if (response.ok) {
        toast({
          title: "Item Added",
          description: `Added "${newItem.subject.slice(0, 40)}..." to golden set`,
          className: "bg-green-900/90 border-green-500/50 text-green-100",
        });
        setShowAddDialog(false);
        setNewItem({ id: '', subject: '', body: '', trueType: '', notes: '', category: 'standard' });
        await fetchData();
      } else {
        const error = await response.json();
        toast({
          title: "Failed to Add",
          description: error.error || "Failed to add item",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Failed to Add",
        description: error instanceof Error ? error.message : "Network error",
        variant: "destructive",
      });
    }
  };

  const removeItem = async (id: string, reason: string = "Removed by user") => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/golden-set/${id}`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason }),
      });
      
      if (response.ok) {
        toast({
          title: "Item Removed",
          description: "Item removed from golden set",
        });
        await fetchData();
      }
    } catch (error) {
      toast({
        title: "Failed to Remove",
        variant: "destructive",
      });
    }
  };

  const sampleFromExisting = async (count: number = 20) => {
    setIsSampling(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      
      // Try the governance-lifecycle main endpoint which has rich items
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/`);
      
      if (!response.ok) {
        toast({
          title: "Failed to Sample",
          description: "Could not fetch governance items",
          variant: "destructive",
        });
        setIsSampling(false);
        return;
      }
      
      const data = await response.json();
      
      // The main endpoint returns lifecycleItems (nested with stages) and allTopics (flat list)
      // allTopics is the flat list with subject, type, etc - prefer this
      let items: any[] = [];
      
      // First try allTopics which is a flat list of topics with subject, type, etc
      if (Array.isArray(data.allTopics) && data.allTopics.length > 0) {
        items = data.allTopics;
      } 
      // If no allTopics, flatten lifecycleItems.stages into topics
      else if (Array.isArray(data.lifecycleItems) && data.lifecycleItems.length > 0) {
        // lifecycleItems has nested stages - extract all topics from all stages
        for (const lifecycleItem of data.lifecycleItems) {
          if (lifecycleItem.stages && typeof lifecycleItem.stages === 'object') {
            for (const stageTopics of Object.values(lifecycleItem.stages)) {
              if (Array.isArray(stageTopics)) {
                items.push(...stageTopics);
              }
            }
          }
        }
      }
      
      if (items.length === 0) {
        toast({
          title: "No Items Found",
          description: "No governance items available to sample from. Try refreshing the governance data first.",
          variant: "destructive",
        });
        setIsSampling(false);
        return;
      }
      
      // Filter out items already in golden set
      const existingIds = new Set(fullSet?.items.map(i => i.id) || []);
      const available = items.filter((item: any) => {
        const itemId = item.id || item.contractId || item.permalink;
        return !existingIds.has(itemId);
      });
      
      // Randomly sample
      const shuffled = available.sort(() => Math.random() - 0.5);
      const sampled = shuffled.slice(0, count).map((item: any) => ({
        id: item.id || item.contractId || item.permalink || `sample-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        // Topics have: subject, sourceUrl, postedStage, inferredStage, effectiveStage
        subject: item.subject || item.title || item.name || 'Unknown',
        excerpt: item.excerpt || item.content || '',
        sourceUrl: item.sourceUrl || item.url || '',
        stage: item.effectiveStage || item.postedStage || item.stage || item.type || 'unknown',
        selected: true,
        // Ground truth should be chosen by the user; do not auto-fill from stage
        trueType: '',
      }));
        
      setSampledItems(sampled);
      setShowSampleDialog(true);
    } catch (error) {
      toast({
        title: "Failed to Sample",
        description: error instanceof Error ? error.message : "Network error",
        variant: "destructive",
      });
    } finally {
      setIsSampling(false);
    }
  };

  const addSampledItems = async () => {
    const selected = sampledItems.filter(item => item.selected && item.trueType);
    if (selected.length === 0) {
      toast({
        title: "No Items Selected",
        description: "Select at least one item with a true type assigned",
        variant: "destructive",
      });
      return;
    }

    setIsAddingSamples(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      let added = 0;
      let failed = 0;

      for (const item of selected) {
        try {
          const response = await fetch(`${baseUrl}/api/governance-lifecycle/golden-set`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              id: item.id,
              subject: item.subject,
              body: item.excerpt || '',
              trueType: item.trueType,
              category: 'standard',
              notes: 'Sampled from existing governance items',
            }),
          });
          
          if (response.ok) {
            added++;
          } else {
            failed++;
          }
        } catch {
          failed++;
        }
      }

      toast({
        title: `Added ${added} Items`,
        description: failed > 0 ? `${failed} items failed to add` : 'All items added successfully',
        className: failed === 0 
          ? "bg-green-900/90 border-green-500/50 text-green-100"
          : "bg-yellow-900/90 border-yellow-500/50 text-yellow-100",
      });

      setShowSampleDialog(false);
      setSampledItems([]);
      await fetchData();
    } catch (error) {
      toast({
        title: "Failed to Add Items",
        variant: "destructive",
      });
    } finally {
      setIsAddingSamples(false);
    }
  };

  const toggleSampleItem = (index: number) => {
    setSampledItems(prev => prev.map((item, i) => 
      i === index ? { ...item, selected: !item.selected } : item
    ));
  };

  const updateSampleTrueType = (index: number, trueType: string) => {
    setSampledItems(prev => prev.map((item, i) => 
      i === index ? { ...item, trueType } : item
    ));
  };

  const selectAllSamples = (selected: boolean) => {
    setSampledItems(prev => prev.map(item => ({ ...item, selected })));
  };

  const clearGoldenSet = async () => {
    if (!fullSet?.items.length) return;
    
    const confirmed = window.confirm(`Are you sure you want to remove all ${fullSet.items.length} items from the golden set? This cannot be undone.`);
    if (!confirmed) return;

    try {
      const baseUrl = getDuckDBApiUrl();
      let removed = 0;
      
      for (const item of fullSet.items) {
        try {
          const response = await fetch(`${baseUrl}/api/governance-lifecycle/golden-set/${item.id}`, {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason: 'Bulk clear' }),
          });
          if (response.ok) removed++;
        } catch {
          // continue
        }
      }
      
      toast({
        title: "Golden Set Cleared",
        description: `Removed ${removed} items`,
      });
      await fetchData();
    } catch (error) {
      toast({
        title: "Failed to Clear",
        variant: "destructive",
      });
    }
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const getTypeColor = (type: string) => {
    const colors: Record<string, string> = {
      'election': 'bg-purple-500/20 text-purple-300 border-purple-500/30',
      'validator-onboarding': 'bg-green-500/20 text-green-300 border-green-500/30',
      'validator-offboarding': 'bg-red-500/20 text-red-300 border-red-500/30',
      'featured-app': 'bg-blue-500/20 text-blue-300 border-blue-500/30',
      'cip': 'bg-amber-500/20 text-amber-300 border-amber-500/30',
      'protocol-upgrade': 'bg-cyan-500/20 text-cyan-300 border-cyan-500/30',
      'outcome': 'bg-pink-500/20 text-pink-300 border-pink-500/30',
      'other': 'bg-gray-500/20 text-gray-300 border-gray-500/30',
    };
    return colors[type] || colors.other;
  };

  const getCategoryColor = (cat: string) => {
    const colors: Record<string, string> = {
      'standard': 'bg-slate-500/20 text-slate-300',
      'edge_case': 'bg-orange-500/20 text-orange-300',
      'boundary': 'bg-yellow-500/20 text-yellow-300',
    };
    return colors[cat] || colors.standard;
  };

  if (isLoading) {
    return (
      <Card className="border-amber-500/20 bg-card/50">
        <CardContent className="flex items-center justify-center py-8">
          <RefreshCw className="h-6 w-6 animate-spin text-muted-foreground" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-amber-500/20 bg-card/50">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Trophy className="h-5 w-5 text-amber-400" />
            <CardTitle className="text-lg">Golden Evaluation Set</CardTitle>
          </div>
          <div className="flex items-center gap-2">
            {(fullSet?.items.length || 0) > 0 && (
              <Button 
                size="sm" 
                variant="ghost" 
                className="gap-1 text-destructive hover:text-destructive hover:bg-destructive/10"
                onClick={clearGoldenSet}
              >
                <Trash2 className="h-4 w-4" />
                Clear
              </Button>
            )}
            
            <Button 
              size="sm" 
              variant="outline" 
              className="gap-1"
              onClick={() => sampleFromExisting(20)}
              disabled={isSampling}
            >
              {isSampling ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Shuffle className="h-4 w-4" />
              )}
              Sample 20
            </Button>
            
            <Dialog open={showAddDialog} onOpenChange={setShowAddDialog}>
              <DialogTrigger asChild>
                <Button size="sm" variant="outline" className="gap-1">
                  <Plus className="h-4 w-4" />
                  Add Item
                </Button>
              </DialogTrigger>
              <DialogContent className="max-w-2xl">
                <DialogHeader>
                  <DialogTitle>Add Golden Set Item</DialogTitle>
                  <DialogDescription>
                    Add a verified governance item with ground-truth classification
                  </DialogDescription>
                </DialogHeader>
                <div className="grid gap-4 py-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="id">Item ID *</Label>
                      <Input 
                        id="id" 
                        placeholder="e.g., golden-1234" 
                        value={newItem.id}
                        onChange={(e) => setNewItem({ ...newItem, id: e.target.value })}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="trueType">True Type *</Label>
                      <Select 
                        value={newItem.trueType} 
                        onValueChange={(v) => setNewItem({ ...newItem, trueType: v })}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select type..." />
                        </SelectTrigger>
                        <SelectContent>
                          {GOVERNANCE_TYPES.map((type) => (
                            <SelectItem key={type} value={type}>
                              {type}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="subject">Subject *</Label>
                    <Input 
                      id="subject" 
                      placeholder="The governance item subject line" 
                      value={newItem.subject}
                      onChange={(e) => setNewItem({ ...newItem, subject: e.target.value })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="body">Body (optional)</Label>
                    <Textarea 
                      id="body" 
                      placeholder="Full content for better classification" 
                      value={newItem.body}
                      onChange={(e) => setNewItem({ ...newItem, body: e.target.value })}
                      rows={3}
                    />
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="category">Category</Label>
                      <Select 
                        value={newItem.category} 
                        onValueChange={(v) => setNewItem({ ...newItem, category: v as any })}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {CATEGORY_OPTIONS.map((opt) => (
                            <SelectItem key={opt.value} value={opt.value}>
                              {opt.label} - {opt.description}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="notes">Notes</Label>
                      <Input 
                        id="notes" 
                        placeholder="Why this is a good test case" 
                        value={newItem.notes}
                        onChange={(e) => setNewItem({ ...newItem, notes: e.target.value })}
                      />
                    </div>
                  </div>
                </div>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setShowAddDialog(false)}>Cancel</Button>
                  <Button onClick={addItem}>Add to Golden Set</Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
            
            {/* Sample from Existing Dialog */}
            <Dialog open={showSampleDialog} onOpenChange={setShowSampleDialog}>
              <DialogContent className="max-w-4xl max-h-[80vh]">
                <DialogHeader>
                  <DialogTitle className="flex items-center gap-2">
                    <Shuffle className="h-5 w-5" />
                    Sample from Existing Items
                  </DialogTitle>
                  <DialogDescription>
                    {sampledItems.length} items sampled. Select items and assign their true type for the golden set.
                  </DialogDescription>
                </DialogHeader>
                
                <div className="flex items-center justify-between py-2 border-b">
                  <div className="flex items-center gap-2">
                    <Checkbox 
                      checked={sampledItems.every(i => i.selected)}
                      onCheckedChange={(checked) => selectAllSamples(!!checked)}
                    />
                    <span className="text-sm text-muted-foreground">
                      {sampledItems.filter(i => i.selected).length} selected
                    </span>
                  </div>
                  <Button 
                    size="sm" 
                    variant="outline" 
                    onClick={() => sampleFromExisting(20)}
                    disabled={isSampling}
                  >
                    {isSampling ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Shuffle className="h-4 w-4" />}
                    Resample
                  </Button>
                </div>
                
                <ScrollArea className="h-[400px] pr-4">
                  <div className="space-y-2">
                    {sampledItems.map((item, index) => (
                      <div 
                        key={item.id} 
                        className={cn(
                          "p-3 rounded-lg border transition-colors",
                          item.selected 
                            ? "bg-primary/5 border-primary/30" 
                            : "bg-muted/30 border-border opacity-60"
                        )}
                      >
                        <div className="flex items-start gap-3">
                          <Checkbox 
                            checked={item.selected}
                            onCheckedChange={() => toggleSampleItem(index)}
                            className="mt-1"
                          />
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium truncate">{item.subject}</p>

                            {item.sourceUrl && (
                              <a
                                href={item.sourceUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="text-xs text-muted-foreground underline underline-offset-2 block truncate mt-0.5"
                              >
                                {item.sourceUrl}
                              </a>
                            )}

                            {item.excerpt && (
                              <p className="text-xs text-muted-foreground truncate mt-0.5">
                                {item.excerpt.slice(0, 120)}...
                              </p>
                            )}

                            <div className="flex items-center gap-2 mt-2 flex-wrap">
                              <Badge variant="outline" className="text-xs">
                                Stage: {item.stage || 'unknown'}
                              </Badge>
                              <span className="text-xs text-muted-foreground">Ground truth:</span>
                              <Select 
                                value={item.trueType} 
                                onValueChange={(v) => updateSampleTrueType(index, v)}
                              >
                                <SelectTrigger className="h-7 w-44 text-xs">
                                  <SelectValue placeholder="Select type..." />
                                </SelectTrigger>
                                <SelectContent>
                                  {GOVERNANCE_TYPES.map((type) => (
                                    <SelectItem key={type} value={type} className="text-xs">
                                      {type}
                                    </SelectItem>
                                  ))}
                                </SelectContent>
                              </Select>
                            </div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </ScrollArea>
                
                <DialogFooter>
                  <div className="flex items-center gap-2 text-xs text-muted-foreground mr-auto">
                    <Info className="h-3.5 w-3.5" />
                    Items without a true type assigned will be skipped
                  </div>
                  <Button variant="outline" onClick={() => setShowSampleDialog(false)}>Cancel</Button>
                  <Button 
                    onClick={addSampledItems} 
                    disabled={isAddingSamples || sampledItems.filter(i => i.selected && i.trueType).length === 0}
                  >
                    {isAddingSamples ? (
                      <RefreshCw className="h-4 w-4 animate-spin mr-2" />
                    ) : (
                      <Download className="h-4 w-4 mr-2" />
                    )}
                    Add {sampledItems.filter(i => i.selected && i.trueType).length} Items
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
            
            <Button 
              size="sm" 
              onClick={runEvaluation} 
              disabled={isEvaluating || (summary?.itemCount || 0) === 0}
              className="gap-1 bg-amber-600 hover:bg-amber-700"
            >
              {isEvaluating ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Play className="h-4 w-4" />
              )}
              Evaluate
            </Button>
          </div>
        </div>
        <CardDescription>
          Fixed benchmark with {summary?.itemCount || 0} verified items for measuring classifier accuracy
        </CardDescription>
      </CardHeader>
      
      <CardContent className="space-y-4">
        {/* Summary Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div className="rounded-lg border bg-card/50 p-3">
            <div className="flex items-center gap-2 text-muted-foreground text-xs mb-1">
              <Target className="h-3.5 w-3.5" />
              Items
            </div>
            <div className="text-2xl font-bold">{summary?.itemCount || 0}</div>
          </div>
          
          <div className="rounded-lg border bg-card/50 p-3">
            <div className="flex items-center gap-2 text-muted-foreground text-xs mb-1">
              <BarChart3 className="h-3.5 w-3.5" />
              Evaluations
            </div>
            <div className="text-2xl font-bold">{summary?.evaluationCount || 0}</div>
          </div>
          
          <div className="rounded-lg border bg-card/50 p-3">
            <div className="flex items-center gap-2 text-muted-foreground text-xs mb-1">
              <CheckCircle2 className="h-3.5 w-3.5 text-green-400" />
              Last Accuracy
            </div>
            <div className="text-2xl font-bold">
              {summary?.lastEvaluation?.accuracy ? `${summary.lastEvaluation.accuracy}%` : '—'}
            </div>
          </div>
          
          <div className="rounded-lg border bg-card/50 p-3">
            <div className="flex items-center gap-2 text-muted-foreground text-xs mb-1">
              <Clock className="h-3.5 w-3.5" />
              Last Run
            </div>
            <div className="text-sm font-medium">
              {summary?.lastEvaluation?.evaluatedAt 
                ? formatDate(summary.lastEvaluation.evaluatedAt) 
                : 'Never'}
            </div>
          </div>
        </div>

        {/* Type Distribution */}
        {summary?.stats?.byType && Object.keys(summary.stats.byType).length > 0 && (
          <div className="space-y-2">
            <h4 className="text-sm font-medium text-muted-foreground">Distribution by Type</h4>
            <div className="flex flex-wrap gap-2">
              {Object.entries(summary.stats.byType).map(([type, count]) => (
                <Badge key={type} variant="outline" className={cn("gap-1", getTypeColor(type))}>
                  {type}: {count}
                </Badge>
              ))}
            </div>
          </div>
        )}

        {/* Evaluation History */}
        {evaluationHistory.length > 0 && (
          <Accordion type="single" collapsible className="w-full">
            <AccordionItem value="history" className="border-muted/30">
              <AccordionTrigger className="hover:no-underline py-2">
                <div className="flex items-center gap-2">
                  <History className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm font-medium">Evaluation History</span>
                  <Badge variant="secondary" className="text-xs">
                    {evaluationHistory.length} runs
                  </Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <ScrollArea className="h-[200px]">
                  <div className="space-y-2">
                    {[...evaluationHistory].reverse().slice(0, 10).map((eval_, idx) => (
                      <div 
                        key={idx}
                        className="flex items-center justify-between p-2 rounded-lg border bg-muted/20 hover:bg-muted/30 cursor-pointer"
                        onClick={() => setSelectedEval(eval_)}
                      >
                        <div className="flex items-center gap-3">
                          <div className={cn(
                            "text-lg font-bold",
                            parseFloat(eval_.accuracy) >= 90 ? "text-green-400" :
                            parseFloat(eval_.accuracy) >= 70 ? "text-yellow-400" : "text-red-400"
                          )}>
                            {eval_.accuracy}%
                          </div>
                          <div className="text-xs text-muted-foreground">
                            {eval_.correct}/{eval_.goldenSetSize} correct
                          </div>
                          {eval_.accuracyDelta && (
                            <Badge 
                              variant="outline" 
                              className={cn(
                                "text-xs",
                                parseFloat(eval_.accuracyDelta) > 0 
                                  ? "text-green-400 border-green-500/30" 
                                  : parseFloat(eval_.accuracyDelta) < 0 
                                  ? "text-red-400 border-red-500/30"
                                  : "text-muted-foreground"
                              )}
                            >
                              {parseFloat(eval_.accuracyDelta) > 0 ? '+' : ''}{eval_.accuracyDelta}%
                            </Badge>
                          )}
                        </div>
                        <div className="flex items-center gap-2">
                          {eval_.regressions.length > 0 && (
                            <Badge variant="outline" className="text-xs text-red-400 border-red-500/30">
                              <TrendingDown className="h-3 w-3 mr-1" />
                              {eval_.regressions.length}
                            </Badge>
                          )}
                          {eval_.improvements.length > 0 && (
                            <Badge variant="outline" className="text-xs text-green-400 border-green-500/30">
                              <TrendingUp className="h-3 w-3 mr-1" />
                              {eval_.improvements.length}
                            </Badge>
                          )}
                          <span className="text-xs text-muted-foreground">
                            {formatDate(eval_.evaluatedAt)}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </ScrollArea>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        )}

        {/* Selected Evaluation Details */}
        {selectedEval && (
          <Dialog open={!!selectedEval} onOpenChange={() => setSelectedEval(null)}>
            <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
              <DialogHeader>
                <DialogTitle className="flex items-center gap-2">
                  <BarChart3 className="h-5 w-5" />
                  Evaluation Details: {selectedEval.accuracy}% Accuracy
                </DialogTitle>
                <DialogDescription>
                  {formatDate(selectedEval.evaluatedAt)} • {selectedEval.correct}/{selectedEval.goldenSetSize} correct
                </DialogDescription>
              </DialogHeader>
              
              <div className="space-y-4">
                {/* By Type Breakdown */}
                <div className="space-y-2">
                  <h4 className="text-sm font-medium">Accuracy by Type</h4>
                  <div className="grid grid-cols-2 gap-2">
                    {Object.entries(selectedEval.byType).map(([type, stats]) => (
                      <div key={type} className="flex items-center justify-between p-2 rounded-lg border bg-muted/20">
                        <Badge variant="outline" className={getTypeColor(type)}>{type}</Badge>
                        <span className={cn(
                          "font-mono text-sm",
                          parseFloat(stats.accuracy) >= 80 ? "text-green-400" : "text-yellow-400"
                        )}>
                          {stats.accuracy}% ({stats.correct}/{stats.total})
                        </span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Regressions */}
                {selectedEval.regressions.length > 0 && (
                  <div className="space-y-2">
                    <h4 className="text-sm font-medium flex items-center gap-2 text-red-400">
                      <TrendingDown className="h-4 w-4" />
                      Regressions ({selectedEval.regressions.length})
                    </h4>
                    <div className="space-y-1">
                      {selectedEval.regressions.map((reg, idx) => (
                        <div key={idx} className="flex items-center gap-2 p-2 rounded-lg border border-red-500/20 bg-red-500/5 text-sm">
                          <XCircle className="h-4 w-4 text-red-400 shrink-0" />
                          <span className="truncate flex-1">{reg.subject}</span>
                          <Badge variant="outline" className="text-xs">
                            {reg.previousPrediction} → {reg.currentPrediction}
                          </Badge>
                          <Badge className={getTypeColor(reg.trueType)}>
                            Expected: {reg.trueType}
                          </Badge>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Improvements */}
                {selectedEval.improvements.length > 0 && (
                  <div className="space-y-2">
                    <h4 className="text-sm font-medium flex items-center gap-2 text-green-400">
                      <TrendingUp className="h-4 w-4" />
                      Improvements ({selectedEval.improvements.length})
                    </h4>
                    <div className="space-y-1">
                      {selectedEval.improvements.map((imp, idx) => (
                        <div key={idx} className="flex items-center gap-2 p-2 rounded-lg border border-green-500/20 bg-green-500/5 text-sm">
                          <CheckCircle2 className="h-4 w-4 text-green-400 shrink-0" />
                          <span className="truncate flex-1">{imp.subject}</span>
                          <Badge variant="outline" className="text-xs">
                            {imp.previousPrediction} → {imp.currentPrediction}
                          </Badge>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </DialogContent>
          </Dialog>
        )}

        {/* Golden Set Items */}
        {fullSet && fullSet.items.length > 0 && (
          <Accordion type="single" collapsible className="w-full">
            <AccordionItem value="items" className="border-muted/30">
              <AccordionTrigger className="hover:no-underline py-2">
                <div className="flex items-center gap-2">
                  <FileText className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm font-medium">Golden Set Items</span>
                  <Badge variant="secondary" className="text-xs">
                    {fullSet.items.length} items
                  </Badge>
                </div>
              </AccordionTrigger>
              <AccordionContent>
                <ScrollArea className="h-[300px]">
                  <div className="space-y-2">
                    {fullSet.items.map((item) => (
                      <div 
                        key={item.id}
                        className="p-3 rounded-lg border bg-muted/20 space-y-2"
                      >
                        <div className="flex items-start justify-between gap-2">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <Badge variant="outline" className={getTypeColor(item.trueType)}>
                                {item.trueType}
                              </Badge>
                              <Badge variant="outline" className={getCategoryColor(item.category)}>
                                {item.category}
                              </Badge>
                              {item.frozen && (
                                <span title="Frozen">
                                  <Shield className="h-3.5 w-3.5 text-muted-foreground" />
                                </span>
                              )}
                            </div>
                            <p className="text-sm font-medium truncate">{item.subject}</p>
                            {item.notes && (
                              <p className="text-xs text-muted-foreground mt-1 flex items-center gap-1">
                                <Info className="h-3 w-3" />
                                {item.notes}
                              </p>
                            )}
                          </div>
                          <Button 
                            size="icon" 
                            variant="ghost" 
                            className="h-8 w-8 text-muted-foreground hover:text-red-400"
                            onClick={() => removeItem(item.id)}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </div>
                        <div className="flex items-center gap-3 text-xs text-muted-foreground">
                          <span>ID: {item.id}</span>
                          <span>•</span>
                          <span>Added: {formatDate(item.addedAt)}</span>
                          {item.addedBy && (
                            <>
                              <span>•</span>
                              <span>By: {item.addedBy}</span>
                            </>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </ScrollArea>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        )}

        {/* Empty State */}
        {(!fullSet || fullSet.items.length === 0) && (
          <div className="flex flex-col items-center justify-center py-8 text-center">
            <Sparkles className="h-10 w-10 text-muted-foreground/50 mb-3" />
            <h3 className="text-sm font-medium mb-1">No Golden Set Items Yet</h3>
            <p className="text-xs text-muted-foreground max-w-sm mb-4">
              Add verified governance items with ground-truth classifications to create an objective benchmark for measuring classifier accuracy.
            </p>
            <Button size="sm" variant="outline" onClick={() => setShowAddDialog(true)}>
              <Plus className="h-4 w-4 mr-1" />
              Add First Item
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
