import React, { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Switch } from "@/components/ui/switch";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { useToast } from "@/hooks/use-toast";
import { getDuckDBApiUrl } from "@/lib/backend-config";
import { cn } from "@/lib/utils";
import {
  RefreshCw,
  CheckCircle2,
  XCircle,
  Clock,
  Lightbulb,
  Code,
  FileText,
  AlertTriangle,
  Sparkles,
  History,
  ChevronRight,
  Shield,
  TrendingUp,
  Zap,
  GitBranch,
  Target,
  Info,
} from "lucide-react";

interface ProposedImprovement {
  id: string;
  file: string;
  location: string;
  type: 'add_keyword' | 'add_regex' | 'prompt_enhancement' | 'entity_mapping';
  priority: 'high' | 'medium' | 'low';
  description: string;
  keywords?: string[];
  codeChange?: {
    target: string;
    action: string;
    values: string[];
  };
  promptAddition?: string;
  examples?: string[];
  reason: string;
  evidenceCount?: number;
  // Enhanced fields
  confidence?: {
    level: 'general' | 'contextual' | 'edge-case';
    description: string;
    sourceCount: number;
    avgKeywordMatch: number;
    uniqueEntities: number;
  };
  scope?: {
    applies: 'future_only' | 'reclassify_on_demand';
    retroactive: boolean;
    description: string;
  };
  provenance?: {
    sourceCorrections: number;
    affectedEntities: string[];
    transition: string;
    avgKeywordFrequency: number;
  };
  learningLayer?: 'pattern' | 'instructional';
  promptType?: 'example_injection' | 'definition_clarification';
}

interface LearnedPatterns {
  version: string;
  previousVersion: string | null;
  generatedAt: string;
  basedOnCorrections: number;
  learningMode: boolean;
  learningModeChangedAt?: string;
  patterns: {
    validatorKeywords: string[];
    featuredAppKeywords: string[];
    cipKeywords: string[];
    protocolUpgradeKeywords: string[];
    outcomeKeywords: string[];
    entityNameMappings: Record<string, string>;
  };
  history?: {
    version: string;
    timestamp: string;
    correctionsApplied: number;
    acceptedProposals: number | string;
  }[];
}

interface ProposalDecision {
  proposalId: string;
  decision: 'accept' | 'reject' | 'pending';
  decidedAt?: string;
}

export function LearnFromCorrectionsPanel() {
  const { toast } = useToast();
  const [isGenerating, setIsGenerating] = useState(false);
  const [isApplying, setIsApplying] = useState(false);
  const [isTogglingMode, setIsTogglingMode] = useState(false);
  const [proposals, setProposals] = useState<ProposedImprovement[]>([]);
  const [decisions, setDecisions] = useState<Record<string, ProposalDecision>>({});
  const [currentPatterns, setCurrentPatterns] = useState<LearnedPatterns | null>(null);
  const [correctionsCount, setCorrectionsCount] = useState(0);
  const [lastGenerated, setLastGenerated] = useState<string | null>(null);
  const [learningMode, setLearningMode] = useState(true);

  // Fetch current learned patterns status on mount
  useEffect(() => {
    fetchCurrentPatterns();
  }, []);

  // Fetch current learned patterns status
  const fetchCurrentPatterns = async () => {
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/learned-patterns`);
      if (response.ok) {
        const data = await response.json();
        if (data.exists) {
          setCurrentPatterns({
            version: data.version,
            previousVersion: data.previousVersion,
            generatedAt: data.generatedAt,
            basedOnCorrections: data.basedOnCorrections,
            learningMode: data.learningMode ?? true,
            learningModeChangedAt: data.learningModeChangedAt,
            patterns: data.patterns,
            history: data.history,
          });
          setLearningMode(data.learningMode ?? true);
        } else {
          setLearningMode(data.learningMode ?? true);
        }
      }
    } catch (error) {
      console.error('Failed to fetch current patterns:', error);
    }
  };

  // Toggle learning mode
  const toggleLearningMode = async () => {
    setIsTogglingMode(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/learning-mode`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: !learningMode }),
      });
      
      if (response.ok) {
        const data = await response.json();
        setLearningMode(data.learningMode);
        toast({
          title: data.learningMode ? "Learning Mode Enabled" : "Learning Mode Disabled",
          description: data.message,
        });
        await fetchCurrentPatterns();
      }
    } catch (error) {
      toast({
        title: "Failed to toggle learning mode",
        variant: "destructive",
      });
    } finally {
      setIsTogglingMode(false);
    }
  };

  // Generate proposals from corrections (dry run)
  const generateProposals = async () => {
    setIsGenerating(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      
      // First, get the improvement suggestions
      const improvementsRes = await fetch(`${baseUrl}/api/governance-lifecycle/classification-improvements`);
      if (!improvementsRes.ok) throw new Error('Failed to fetch improvements');
      const improvementsData = await improvementsRes.json();
      
      // Also do a dry run to see what patterns would be generated
      const dryRunRes = await fetch(`${baseUrl}/api/governance-lifecycle/apply-improvements`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dryRun: true }),
      });
      
      if (!dryRunRes.ok) throw new Error('Failed to generate patterns');
      const dryRunData = await dryRunRes.json();
      
      // Transform suggestions into proposals with IDs
      const newProposals: ProposedImprovement[] = (improvementsData.suggestions || []).map((s: any, i: number) => ({
        id: `prop-${i}-${Date.now()}`,
        ...s,
        evidenceCount: s.provenance?.sourceCorrections || s.transitionCount || (s.examples?.length || 0),
      }));
      
      setProposals(newProposals);
      setCorrectionsCount(dryRunData.correctionsAnalyzed || 0);
      setLastGenerated(new Date().toISOString());
      
      // Initialize all as pending
      const initialDecisions: Record<string, ProposalDecision> = {};
      newProposals.forEach(p => {
        initialDecisions[p.id] = { proposalId: p.id, decision: 'pending' };
      });
      setDecisions(initialDecisions);
      
      // Fetch current patterns for comparison
      await fetchCurrentPatterns();
      
      toast({
        title: "Proposals Generated",
        description: `Found ${newProposals.length} improvement suggestions from ${dryRunData.correctionsAnalyzed} corrections`,
      });
    } catch (error) {
      console.error('Error generating proposals:', error);
      toast({
        title: "Generation Failed",
        description: error instanceof Error ? error.message : "Unknown error",
        variant: "destructive",
      });
    } finally {
      setIsGenerating(false);
    }
  };

  // Update decision for a proposal
  const updateDecision = (proposalId: string, decision: 'accept' | 'reject') => {
    setDecisions(prev => ({
      ...prev,
      [proposalId]: {
        proposalId,
        decision,
        decidedAt: new Date().toISOString(),
      },
    }));
  };

  // Accept all high priority proposals
  const acceptAllHighPriority = () => {
    const updates: Record<string, ProposalDecision> = {};
    proposals.forEach(p => {
      if (p.priority === 'high') {
        updates[p.id] = {
          proposalId: p.id,
          decision: 'accept',
          decidedAt: new Date().toISOString(),
        };
      }
    });
    setDecisions(prev => ({ ...prev, ...updates }));
  };

  // Accept all general confidence proposals
  const acceptAllGeneral = () => {
    const updates: Record<string, ProposalDecision> = {};
    proposals.forEach(p => {
      if (p.confidence?.level === 'general') {
        updates[p.id] = {
          proposalId: p.id,
          decision: 'accept',
          decidedAt: new Date().toISOString(),
        };
      }
    });
    setDecisions(prev => ({ ...prev, ...updates }));
  };

  // Apply accepted proposals
  const applyAccepted = async () => {
    const acceptedIds = Object.entries(decisions)
      .filter(([, d]) => d.decision === 'accept')
      .map(([id]) => id);
    
    if (acceptedIds.length === 0) {
      toast({
        title: "Nothing to Apply",
        description: "Accept at least one proposal before applying",
        variant: "destructive",
      });
      return;
    }
    
    setIsApplying(true);
    try {
      const baseUrl = getDuckDBApiUrl();
      
      // Apply the improvements (not dry run)
      const response = await fetch(`${baseUrl}/api/governance-lifecycle/apply-improvements`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dryRun: false,
          acceptedProposals: acceptedIds,
        }),
      });
      
      if (!response.ok) throw new Error('Failed to apply improvements');
      const result = await response.json();
      
      // Refresh patterns
      await fetchCurrentPatterns();
      
      toast({
        title: `Patterns Applied (v${result.version})`,
        description: `Learned patterns saved from ${result.correctionsAnalyzed} corrections. Future classifications will use these patterns.`,
      });
      
      // Clear proposals after successful apply
      setProposals([]);
      setDecisions({});
    } catch (error) {
      console.error('Error applying improvements:', error);
      toast({
        title: "Apply Failed",
        description: error instanceof Error ? error.message : "Unknown error",
        variant: "destructive",
      });
    } finally {
      setIsApplying(false);
    }
  };

  const pendingCount = Object.values(decisions).filter(d => d.decision === 'pending').length;
  const acceptedCount = Object.values(decisions).filter(d => d.decision === 'accept').length;
  const rejectedCount = Object.values(decisions).filter(d => d.decision === 'reject').length;

  const priorityColor = {
    high: 'bg-red-500/20 text-red-400 border-red-500/30',
    medium: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    low: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
  };

  const confidenceColor = {
    'general': 'bg-green-500/20 text-green-400 border-green-500/30',
    'contextual': 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    'edge-case': 'bg-red-500/20 text-red-400 border-red-500/30',
  };

  const confidenceIcon = {
    'general': <TrendingUp className="h-3 w-3" />,
    'contextual': <Target className="h-3 w-3" />,
    'edge-case': <AlertTriangle className="h-3 w-3" />,
  };

  const typeIcon = {
    add_keyword: <Code className="h-4 w-4" />,
    add_regex: <FileText className="h-4 w-4" />,
    prompt_enhancement: <Sparkles className="h-4 w-4" />,
    entity_mapping: <ChevronRight className="h-4 w-4" />,
  };

  return (
    <Card className="border-border/50">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Lightbulb className="h-5 w-5 text-yellow-400" />
              Learn from Corrections
            </CardTitle>
            <CardDescription className="mt-1">
              Generate improvement proposals from manual classification corrections
            </CardDescription>
          </div>
          <div className="flex items-center gap-3">
            {/* Learning Mode Toggle */}
            <div className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">Learning Mode</span>
              <Switch
                checked={learningMode}
                onCheckedChange={toggleLearningMode}
                disabled={isTogglingMode}
              />
            </div>
            <Button
              onClick={generateProposals}
              disabled={isGenerating || !learningMode}
              variant="outline"
              className="gap-2"
            >
              {isGenerating ? (
                <>
                  <RefreshCw className="h-4 w-4 animate-spin" />
                  Analyzing...
                </>
              ) : (
                <>
                  <Lightbulb className="h-4 w-4" />
                  Generate Proposals
                </>
              )}
            </Button>
          </div>
        </div>
      </CardHeader>
      
      <CardContent className="space-y-4">
        {/* Learning Mode Disabled Banner */}
        {!learningMode && (
          <div className="p-3 bg-muted/30 border border-border/50 rounded-lg">
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Shield className="h-4 w-4" />
              <span>Learning mode is disabled. Corrections apply locally only and won't generate proposals.</span>
            </div>
          </div>
        )}

        {/* Current Patterns Status */}
        {currentPatterns && (
          <div className="p-3 bg-muted/30 rounded-lg border border-border/50">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium flex items-center gap-2">
                <GitBranch className="h-4 w-4 text-muted-foreground" />
                Current Learned Patterns
                <Badge variant="outline" className="text-xs font-mono">
                  v{currentPatterns.version}
                </Badge>
                {currentPatterns.previousVersion && (
                  <span className="text-xs text-muted-foreground">
                    (from v{currentPatterns.previousVersion})
                  </span>
                )}
              </span>
              <Badge variant="outline" className="text-xs">
                {new Date(currentPatterns.generatedAt).toLocaleDateString()}
              </Badge>
            </div>
            <div className="grid grid-cols-3 gap-2 text-xs text-muted-foreground">
              <div>Validator: {currentPatterns.patterns.validatorKeywords?.length || 0} keywords</div>
              <div>Featured App: {currentPatterns.patterns.featuredAppKeywords?.length || 0} keywords</div>
              <div>CIP: {currentPatterns.patterns.cipKeywords?.length || 0} keywords</div>
              <div>Protocol: {currentPatterns.patterns.protocolUpgradeKeywords?.length || 0} keywords</div>
              <div>Outcome: {currentPatterns.patterns.outcomeKeywords?.length || 0} keywords</div>
              <div>Entity Mappings: {Object.keys(currentPatterns.patterns.entityNameMappings || {}).length}</div>
            </div>
            
            {/* Version History */}
            {currentPatterns.history && currentPatterns.history.length > 0 && (
              <div className="mt-3 pt-3 border-t border-border/30">
                <div className="text-xs font-medium text-muted-foreground mb-1 flex items-center gap-1">
                  <History className="h-3 w-3" />
                  Recent History
                </div>
                <div className="flex gap-2 flex-wrap">
                  {currentPatterns.history.slice(-3).reverse().map((h, i) => (
                    <Badge key={i} variant="outline" className="text-[10px] font-mono">
                      v{h.version} • {h.correctionsApplied} corrections
                    </Badge>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
        
        {/* Proposals */}
        {proposals.length > 0 && (
          <>
            {/* Summary Bar */}
            <div className="flex items-center justify-between p-3 bg-muted/20 rounded-lg">
              <div className="flex items-center gap-4 text-sm">
                <span>
                  <strong>{proposals.length}</strong> proposals from <strong>{correctionsCount}</strong> corrections
                </span>
                <div className="flex items-center gap-3 text-xs">
                  <span className="flex items-center gap-1">
                    <Clock className="h-3 w-3" /> {pendingCount} pending
                  </span>
                  <span className="flex items-center gap-1 text-green-400">
                    <CheckCircle2 className="h-3 w-3" /> {acceptedCount} accepted
                  </span>
                  <span className="flex items-center gap-1 text-red-400">
                    <XCircle className="h-3 w-3" /> {rejectedCount} rejected
                  </span>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={acceptAllGeneral}
                  className="text-xs gap-1"
                  title="Accept proposals with high confidence across multiple entities"
                >
                  <TrendingUp className="h-3 w-3" />
                  Accept General
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={acceptAllHighPriority}
                  className="text-xs"
                >
                  Accept High Priority
                </Button>
                <Button
                  onClick={applyAccepted}
                  disabled={acceptedCount === 0 || isApplying}
                  size="sm"
                  className="gap-1"
                >
                  {isApplying ? (
                    <RefreshCw className="h-3 w-3 animate-spin" />
                  ) : (
                    <CheckCircle2 className="h-3 w-3" />
                  )}
                  Apply {acceptedCount} Accepted
                </Button>
              </div>
            </div>
            
            {/* Proposal List */}
            <ScrollArea className="h-[400px]">
              <Accordion type="multiple" className="space-y-2">
                {proposals.map((proposal) => {
                  const decision = decisions[proposal.id]?.decision || 'pending';
                  
                  return (
                    <AccordionItem
                      key={proposal.id}
                      value={proposal.id}
                      className={cn(
                        "border rounded-lg overflow-hidden transition-colors",
                        decision === 'accept' && "border-green-500/40 bg-green-500/5",
                        decision === 'reject' && "border-red-500/40 bg-red-500/5 opacity-60",
                        decision === 'pending' && "border-border/50"
                      )}
                    >
                      <AccordionTrigger className="px-3 py-2 hover:no-underline">
                        <div className="flex items-center gap-3 w-full">
                          <div className="flex-shrink-0">
                            {typeIcon[proposal.type] || <Code className="h-4 w-4" />}
                          </div>
                          <div className="flex-1 text-left">
                            <div className="flex items-center gap-2 flex-wrap">
                              <span className="font-medium text-sm">{proposal.description}</span>
                              <Badge variant="outline" className={cn("text-[10px]", priorityColor[proposal.priority])}>
                                {proposal.priority}
                              </Badge>
                              {proposal.confidence && (
                                <Badge 
                                  variant="outline" 
                                  className={cn("text-[10px] gap-1", confidenceColor[proposal.confidence.level])}
                                >
                                  {confidenceIcon[proposal.confidence.level]}
                                  {proposal.confidence.level}
                                </Badge>
                              )}
                              {proposal.learningLayer && (
                                <Badge variant="outline" className="text-[10px] bg-purple-500/10 text-purple-400 border-purple-500/30">
                                  {proposal.learningLayer === 'pattern' ? <Zap className="h-2.5 w-2.5 mr-1" /> : <Info className="h-2.5 w-2.5 mr-1" />}
                                  {proposal.learningLayer}
                                </Badge>
                              )}
                            </div>
                            <div className="text-xs text-muted-foreground mt-0.5">
                              {proposal.file} → {proposal.location}
                            </div>
                          </div>
                          <div className="flex items-center gap-2">
                            {proposal.evidenceCount && (
                              <Badge variant="outline" className="text-[10px]">
                                {proposal.evidenceCount} examples
                              </Badge>
                            )}
                            {decision !== 'pending' && (
                              <Badge 
                                variant="outline" 
                                className={cn(
                                  "text-[10px]",
                                  decision === 'accept' && "border-green-500/40 text-green-400",
                                  decision === 'reject' && "border-red-500/40 text-red-400"
                                )}
                              >
                                {decision === 'accept' ? '✓ Accepted' : '✗ Rejected'}
                              </Badge>
                            )}
                          </div>
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-3 pb-3">
                        <div className="space-y-3 pt-2 border-t border-border/30">
                          {/* Reason */}
                          <div>
                            <div className="text-xs font-medium text-muted-foreground mb-1">Reason</div>
                            <p className="text-sm">{proposal.reason}</p>
                          </div>
                          
                          {/* Confidence Details */}
                          {proposal.confidence && (
                            <div className="p-2 bg-muted/20 rounded-lg">
                              <div className="text-xs font-medium text-muted-foreground mb-1 flex items-center gap-1">
                                {confidenceIcon[proposal.confidence.level]}
                                Confidence Analysis
                              </div>
                              <p className="text-xs text-muted-foreground">{proposal.confidence.description}</p>
                              <div className="flex gap-3 mt-1 text-xs">
                                <span>Sources: {proposal.confidence.sourceCount}</span>
                                <span>Entities: {proposal.confidence.uniqueEntities}</span>
                                <span>Match: {(proposal.confidence.avgKeywordMatch * 100).toFixed(0)}%</span>
                              </div>
                            </div>
                          )}
                          
                          {/* Scope */}
                          {proposal.scope && (
                            <div>
                              <div className="text-xs font-medium text-muted-foreground mb-1 flex items-center gap-1">
                                <Shield className="h-3 w-3" />
                                Scope
                              </div>
                              <p className="text-xs text-muted-foreground">{proposal.scope.description}</p>
                            </div>
                          )}
                          
                          {/* Provenance */}
                          {proposal.provenance && (
                            <div>
                              <div className="text-xs font-medium text-muted-foreground mb-1 flex items-center gap-1">
                                <History className="h-3 w-3" />
                                Provenance
                              </div>
                              <div className="text-xs text-muted-foreground">
                                Derived from {proposal.provenance.sourceCorrections} corrections affecting:
                              </div>
                              <ul className="text-xs mt-1 space-y-0.5">
                                {proposal.provenance.affectedEntities.slice(0, 3).map((e, i) => (
                                  <li key={i} className="text-muted-foreground truncate">• {e}</li>
                                ))}
                                {proposal.provenance.affectedEntities.length > 3 && (
                                  <li className="text-muted-foreground">
                                    +{proposal.provenance.affectedEntities.length - 3} more
                                  </li>
                                )}
                              </ul>
                            </div>
                          )}
                          
                          {/* Keywords */}
                          {proposal.keywords && proposal.keywords.length > 0 && (
                            <div>
                              <div className="text-xs font-medium text-muted-foreground mb-1">Keywords to Add</div>
                              <div className="flex flex-wrap gap-1">
                                {proposal.keywords.slice(0, 10).map((kw, i) => (
                                  <Badge key={i} variant="secondary" className="text-xs">{kw}</Badge>
                                ))}
                                {proposal.keywords.length > 10 && (
                                  <Badge variant="outline" className="text-xs">+{proposal.keywords.length - 10} more</Badge>
                                )}
                              </div>
                            </div>
                          )}
                          
                          {/* Code Change */}
                          {proposal.codeChange && (
                            <div>
                              <div className="text-xs font-medium text-muted-foreground mb-1">Code Change</div>
                              <code className="text-xs bg-muted/50 p-2 rounded block">
                                {proposal.codeChange.target}.{proposal.codeChange.action}([{proposal.codeChange.values.slice(0, 5).map(v => `"${v}"`).join(', ')}])
                              </code>
                            </div>
                          )}
                          
                          {/* Prompt Addition */}
                          {proposal.promptAddition && (
                            <div>
                              <div className="text-xs font-medium text-muted-foreground mb-1 flex items-center gap-1">
                                <Sparkles className="h-3 w-3" />
                                LLM Prompt Enhancement
                                {proposal.promptType && (
                                  <Badge variant="outline" className="text-[10px] ml-1">
                                    {proposal.promptType === 'example_injection' ? 'Examples' : 'Definition'}
                                  </Badge>
                                )}
                              </div>
                              <pre className="text-xs bg-muted/50 p-2 rounded whitespace-pre-wrap max-h-32 overflow-auto">
                                {proposal.promptAddition}
                              </pre>
                            </div>
                          )}
                          
                          {/* Examples */}
                          {proposal.examples && proposal.examples.length > 0 && (
                            <div>
                              <div className="text-xs font-medium text-muted-foreground mb-1">Examples</div>
                              <ul className="text-xs space-y-1">
                                {proposal.examples.slice(0, 3).map((ex, i) => (
                                  <li key={i} className="text-muted-foreground truncate">• {ex}</li>
                                ))}
                              </ul>
                            </div>
                          )}
                          
                          {/* Action Buttons */}
                          <div className="flex items-center gap-2 pt-2">
                            <Button
                              size="sm"
                              variant={decision === 'accept' ? 'default' : 'outline'}
                              className={cn("gap-1", decision === 'accept' && "bg-green-600 hover:bg-green-700")}
                              onClick={(e) => {
                                e.stopPropagation();
                                updateDecision(proposal.id, 'accept');
                              }}
                            >
                              <CheckCircle2 className="h-3 w-3" />
                              Accept
                            </Button>
                            <Button
                              size="sm"
                              variant={decision === 'reject' ? 'destructive' : 'outline'}
                              className="gap-1"
                              onClick={(e) => {
                                e.stopPropagation();
                                updateDecision(proposal.id, 'reject');
                              }}
                            >
                              <XCircle className="h-3 w-3" />
                              Reject
                            </Button>
                            <span className="text-xs text-muted-foreground ml-auto">
                              ID: {proposal.id.slice(0, 12)}
                            </span>
                          </div>
                        </div>
                      </AccordionContent>
                    </AccordionItem>
                  );
                })}
              </Accordion>
            </ScrollArea>
          </>
        )}
        
        {/* Empty State */}
        {proposals.length === 0 && !isGenerating && (
          <div className="text-center py-8 text-muted-foreground">
            <Lightbulb className="h-12 w-12 mx-auto mb-3 opacity-30" />
            <p className="text-sm">Click "Generate Proposals" to analyze corrections</p>
            <p className="text-xs mt-1">
              Proposals are generated from manual type overrides and reclassifications
            </p>
          </div>
        )}
        
        {/* Info Banner */}
        <div className="p-3 bg-yellow-500/10 border border-yellow-500/30 rounded-lg">
          <div className="flex items-start gap-2">
            <AlertTriangle className="h-4 w-4 text-yellow-400 mt-0.5 shrink-0" />
            <div className="text-xs text-yellow-200/80 space-y-1">
              <p>
                <strong>How it works:</strong> Corrections generate suggestions → You review and accept/reject → 
                Accepted patterns are saved and used in future classifications.
              </p>
              <p>
                <strong>Confidence levels:</strong>{' '}
                <span className="text-green-400">🟢 General</span> (seen across multiple entities) •{' '}
                <span className="text-yellow-400">🟡 Contextual</span> (specific to certain flows) •{' '}
                <span className="text-red-400">🔴 Edge-case</span> (rare, may be brittle)
              </p>
              <p>
                <strong>Non-retroactive:</strong> Changes apply to future classifications only. Existing items unchanged.
              </p>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
