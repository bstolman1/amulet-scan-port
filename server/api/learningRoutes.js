/**
 * Pattern learning routes.
 *
 * GET  /learned-patterns            - current pattern state + history
 * POST /apply-improvements          - generate and optionally persist patterns
 * POST /learning-mode               - toggle learning on/off
 * POST /rollback                    - restore a backed-up pattern version
 * POST /impact-preview              - preview classification changes before applying
 * POST /test-proposals              - test patterns against historical data
 * POST /reinforce-survivors         - reinforce patterns that survived without correction
 * GET  /classification-improvements - human-readable suggestions
 * GET  /classification-training-data - export labelled training data
 */

import { Router } from 'express';
import {
  readLearnedPatternsFile,
  writeLearnedPatternsFile,
  backupLearnedPatterns,
  readPatternBackup,
  listPatternBackups,
} from '../repositories/fileRepository.js';
import { readCache, readOverrides, readAuditLog } from '../repositories/fileRepository.js';
import {
  getLearnedPatterns,
  invalidatePatternCache,
  calculatePatternConfidence,
  shouldArchivePattern,
  reinforceSurvivingPatterns,
} from '../services/patternCache.js';
import {
  analyzeCorrections,
  generateImprovementSuggestions,
  generateLearnedPatterns,
  simulateClassification,
} from '../services/classificationAnalyzer.js';
import { validateBody } from '../validators/requestValidators.js';
import { MIN_CONFIDENCE, MINOR_VERSION_GROWTH_RATIO, CHANGELOG_MAX_ENTRIES, HISTORY_MAX_ENTRIES } from '../utils/constants.js';

const router = Router();

// ── GET /learned-patterns ─────────────────────────────────────────────────

router.get('/', async (req, res) => {
  const data = await readLearnedPatternsFile();
  if (!data) {
    return res.json({
      exists: false,
      learningMode: true,
      message: 'No learned patterns. Use POST /apply-improvements to generate.',
    });
  }

  const patterns = data.patterns ?? {};
  const calcStats = arr => {
    if (!Array.isArray(arr)) return { active: 0, decaying: 0, archived: 0, avgConfidence: 0 };
    let active = 0, decaying = 0, archived = 0, total = 0;
    for (const p of arr) {
      const conf = typeof p === 'object' ? calculatePatternConfidence(p) : 1.0;
      total += conf;
      if (conf >= 0.7) active++;
      else if (conf >= MIN_CONFIDENCE * 2) decaying++;
      else archived++;
    }
    return { active, decaying, archived, avgConfidence: arr.length ? (total / arr.length).toFixed(2) : 0 };
  };

  const history = data.history ?? [];

  res.json({
    exists: true,
    version: data.version ?? '1.0.0',
    previousVersion: data.previousVersion ?? null,
    generatedAt: data.generatedAt,
    basedOnCorrections: data.basedOnCorrections,
    learningMode: data.learningMode ?? true,
    patterns,
    stats: {
      validatorKeywords: patterns.validatorKeywords?.length ?? 0,
      featuredAppKeywords: patterns.featuredAppKeywords?.length ?? 0,
      cipKeywords: patterns.cipKeywords?.length ?? 0,
      protocolUpgradeKeywords: patterns.protocolUpgradeKeywords?.length ?? 0,
      outcomeKeywords: patterns.outcomeKeywords?.length ?? 0,
      entityMappings: Object.keys(patterns.entityNameMappings ?? {}).length,
    },
    confidenceStats: {
      validator: calcStats(patterns.validatorKeywords),
      featuredApp: calcStats(patterns.featuredAppKeywords),
      cip: calcStats(patterns.cipKeywords),
      protocolUpgrade: calcStats(patterns.protocolUpgradeKeywords),
      outcome: calcStats(patterns.outcomeKeywords),
    },
    history,
    changelog: data.changelog ?? [],
    canRollback: history.length > 1,
    rollbackVersions: history.slice(0, -1).map(h => ({
      version: h.version,
      timestamp: h.timestamp,
      description: `${h.correctionsApplied} corrections`,
    })),
  });
});

// ── POST /apply-improvements ──────────────────────────────────────────────

router.post('/apply-improvements', validateBody('applyImprovements'), async (req, res) => {
  const { dryRun = true } = req.body;

  const [auditLog, overrides, cached, existingData] = await Promise.all([
    readAuditLog(),
    readOverrides(),
    readCache(),
    readLearnedPatternsFile(),
  ]);

  if (!existingData?.learningMode && !dryRun) {
    return res.json({ success: false, message: 'Learning mode is disabled.' });
  }

  // Collect corrections
  const corrections = [];
  const processedIds = new Set();

  for (const entry of auditLog) {
    if (!['reclassify_item', 'reclassify_topic'].includes(entry.actionType)) continue;
    if (processedIds.has(entry.targetId)) continue;
    processedIds.add(entry.targetId);
    corrections.push({
      targetId: entry.targetId,
      label: entry.targetLabel ?? entry.targetId,
      originalType: entry.originalValue,
      correctedType: entry.newValue,
    });
  }

  for (const [key, override] of Object.entries(overrides.itemOverrides ?? {})) {
    if (processedIds.has(key)) continue;
    corrections.push({ targetId: key, label: key, originalType: override.originalType, correctedType: override.type });
  }

  if (corrections.length === 0) {
    return res.json({ success: false, message: 'No corrections found to learn from' });
  }

  const newPatterns = generateLearnedPatterns(corrections, existingData?.patterns);

  // Version bump
  const [major, minor, patch] = (existingData?.version ?? '1.0.0').split('.').map(Number);
  const newCount = Object.values(newPatterns).reduce(
    (s, v) => s + (Array.isArray(v) ? v.length : Object.keys(v).length), 0,
  );
  const oldCount = existingData?.patterns
    ? Object.values(existingData.patterns).reduce(
        (s, v) => s + (Array.isArray(v) ? v.length : Object.keys(v).length), 0,
      )
    : 0;

  const newVersion = newCount > oldCount * MINOR_VERSION_GROWTH_RATIO
    ? `${major}.${minor + 1}.0`
    : `${major}.${minor}.${patch + 1}`;

  const changelogEntry = {
    timestamp: new Date().toISOString(),
    action: 'apply',
    version: newVersion,
    from: existingData?.version ?? null,
    summary: { correctionsApplied: corrections.length },
  };

  const newData = {
    version: newVersion,
    previousVersion: existingData?.version ?? null,
    generatedAt: new Date().toISOString(),
    basedOnCorrections: corrections.length,
    learningMode: true,
    patterns: newPatterns,
    history: [...(existingData?.history ?? []).slice(-HISTORY_MAX_ENTRIES + 1), {
      version: newVersion,
      timestamp: new Date().toISOString(),
      correctionsApplied: corrections.length,
    }],
    changelog: [...(existingData?.changelog ?? []).slice(-CHANGELOG_MAX_ENTRIES + 1), changelogEntry],
  };

  if (!dryRun) {
    if (existingData?.version) await backupLearnedPatterns(existingData.version);
    await writeLearnedPatternsFile(newData);
    invalidatePatternCache();
  }

  res.json({
    success: true,
    dryRun,
    message: dryRun
      ? 'Dry run — patterns not saved. Set dryRun=false to apply.'
      : `Patterns v${newVersion} saved.`,
    correctionsAnalyzed: corrections.length,
    version: newVersion,
    patternsGenerated: newPatterns,
    changelog: changelogEntry,
  });
});

// ── POST /learning-mode ───────────────────────────────────────────────────

router.post('/learning-mode', validateBody('learningMode'), async (req, res) => {
  const { enabled } = req.body;
  const data = (await readLearnedPatternsFile()) ?? { learningMode: enabled, patterns: {}, version: '1.0.0' };
  data.learningMode = enabled;
  data.learningModeChangedAt = new Date().toISOString();
  await writeLearnedPatternsFile(data);
  res.json({ success: true, learningMode: enabled });
});

// ── POST /rollback ────────────────────────────────────────────────────────

router.post('/rollback', validateBody('rollback'), async (req, res) => {
  const { targetVersion } = req.body;
  const backup = await readPatternBackup(targetVersion);

  if (!backup) {
    const available = await listPatternBackups();
    return res.status(404).json({ error: `No backup found for v${targetVersion}`, available });
  }

  const current = await readLearnedPatternsFile();
  const currentVersion = current?.version ?? '0.0.0';

  backup.changelog = backup.changelog ?? [];
  backup.changelog.push({
    timestamp: new Date().toISOString(),
    action: 'rollback',
    from: currentVersion,
    to: targetVersion,
  });

  const [maj, min, pat] = targetVersion.split('.').map(Number);
  backup.version = `${maj}.${min}.${pat + 1}-rollback`;
  backup.previousVersion = currentVersion;
  backup.rolledBackAt = new Date().toISOString();
  backup.rolledBackFrom = currentVersion;

  await writeLearnedPatternsFile(backup);
  invalidatePatternCache();

  res.json({
    success: true,
    message: `Rolled back from v${currentVersion} to v${targetVersion}`,
    newVersion: backup.version,
  });
});

// ── POST /impact-preview ──────────────────────────────────────────────────

router.post('/impact-preview', async (req, res) => {
  const { proposedPatterns } = req.body;
  const [cached, overrides, current] = await Promise.all([
    readCache(),
    readOverrides(),
    getLearnedPatterns(),
  ]);

  if (!cached?.lifecycleItems) {
    return res.json({ success: false, message: 'No historical data for impact preview' });
  }

  const testPatterns = mergePatterns(current ?? {}, proposedPatterns ?? {});
  const before = {};
  const after = {};
  const changes = [];

  for (const item of cached.lifecycleItems) {
    const subject = item.primaryId ?? '';
    const cur = simulateClassification(subject, current ?? {});
    const prop = simulateClassification(subject, testPatterns);
    before[cur] = (before[cur] ?? 0) + 1;
    after[prop] = (after[prop] ?? 0) + 1;
    if (cur !== prop) {
      const trueType = overrides.itemOverrides?.[item.primaryId]?.type;
      changes.push({ id: item.primaryId, subject: subject.slice(0, 60), before: cur, after: prop,
        isImprovement: trueType ? prop === trueType : null,
        isDegradation: trueType ? cur === trueType && prop !== trueType : false });
    }
  }

  const total = cached.lifecycleItems.length;
  res.json({
    success: true,
    total,
    changedCount: changes.length,
    changedPercent: ((changes.length / total) * 100).toFixed(1),
    before,
    after,
    improvements: changes.filter(c => c.isImprovement).length,
    degradations: changes.filter(c => c.isDegradation).length,
    sample: {
      improvements: changes.filter(c => c.isImprovement).slice(0, 5),
      degradations: changes.filter(c => c.isDegradation).slice(0, 5),
    },
  });
});

// ── POST /test-proposals ──────────────────────────────────────────────────

router.post('/test-proposals', validateBody('testProposals'), async (req, res) => {
  const { proposedPatterns, sampleSize = 50 } = req.body;
  const [cached, overrides, current] = await Promise.all([
    readCache(),
    readOverrides(),
    getLearnedPatterns(),
  ]);

  if (!cached?.lifecycleItems?.length) {
    return res.json({ success: false, message: 'No historical data for testing' });
  }

  const sample = cached.lifecycleItems.slice(0, Math.min(sampleSize, cached.lifecycleItems.length));
  const testPatterns = mergePatterns(current ?? {}, proposedPatterns ?? {});

  const results = { unchanged: [], improved: [], changed: [], degraded: [] };

  for (const item of sample) {
    const subject = item.primaryId ?? '';
    const cur = simulateClassification(subject, current ?? {});
    const prop = simulateClassification(subject, testPatterns);
    const trueType = overrides.itemOverrides?.[item.primaryId]?.type ?? item.type;

    if (cur === prop) {
      results.unchanged.push({ id: item.primaryId, subject: subject.slice(0, 80), type: cur });
    } else if (prop === trueType && cur !== trueType) {
      results.improved.push({ id: item.primaryId, subject: subject.slice(0, 80), currentType: cur, proposedType: prop, trueType });
    } else if (cur === trueType && prop !== trueType) {
      results.degraded.push({ id: item.primaryId, subject: subject.slice(0, 80), currentType: cur, proposedType: prop, trueType });
    } else {
      results.changed.push({ id: item.primaryId, subject: subject.slice(0, 80), currentType: cur, proposedType: prop });
    }
  }

  const total = sample.length;
  res.json({
    success: true,
    summary: {
      total,
      unchanged: results.unchanged.length,
      improved: results.improved.length,
      changed: results.changed.length,
      degraded: results.degraded.length,
      safeToApply: results.degraded.length === 0,
      recommendation: results.degraded.length === 0
        ? (results.improved.length > 0 ? 'Safe to apply — improvements detected' : 'Safe to apply — no regressions')
        : `Caution: ${results.degraded.length} items would regress`,
    },
    results: {
      improved: results.improved.slice(0, 10),
      degraded: results.degraded,
      changed: results.changed.slice(0, 10),
    },
  });
});

// ── POST /reinforce-survivors ─────────────────────────────────────────────

router.post('/reinforce-survivors', async (req, res) => {
  const [cached, overrides] = await Promise.all([readCache(), readOverrides()]);
  if (!cached?.lifecycleItems) {
    return res.json({ success: false, message: 'No data available' });
  }
  const correctedIds = new Set(Object.keys(overrides.itemOverrides ?? {}));
  const result = await reinforceSurvivingPatterns(cached.lifecycleItems, correctedIds);
  res.json({ success: true, ...result });
});

// ── GET /classification-improvements ─────────────────────────────────────

router.get('/classification-improvements', async (req, res) => {
  const [auditLog, overrides, current] = await Promise.all([
    readAuditLog(),
    readOverrides(),
    getLearnedPatterns(),
  ]);

  const corrections = collectCorrections(auditLog, overrides);

  if (corrections.length === 0) {
    return res.json({
      message: 'No manual corrections found.',
      suggestions: [],
      stats: { totalCorrections: 0 },
    });
  }

  const analysis = analyzeCorrections(corrections);
  const suggestions = generateImprovementSuggestions(analysis, current);

  res.json({
    stats: {
      totalCorrections: corrections.length,
      byOriginalType: analysis.byOriginalType,
      byCorrectedType: analysis.byCorrectedType,
      typeTransitions: analysis.typeTransitions,
    },
    patterns: analysis.patterns,
    suggestions,
    corrections: corrections.slice(0, 50),
  });
});

// ── GET /classification-training-data ────────────────────────────────────

router.get('/classification-training-data', async (req, res) => {
  const [auditLog, overrides, cached] = await Promise.all([
    readAuditLog(),
    readOverrides(),
    readCache(),
  ]);

  const processedIds = new Set();
  const trainingData = [];

  const addItem = (id, originalType, correctedType, label, cached) => {
    if (processedIds.has(id)) return;
    processedIds.add(id);
    let content = null;
    if (cached?.lifecycleItems) {
      for (const item of cached.lifecycleItems) {
        if (item.primaryId === id || item.id === id) {
          content = { subject: item.primaryId, body: (item.topics ?? []).map(t => t.subject).join(' | '), stage: item.currentStage };
          break;
        }
        const topic = (item.topics ?? []).find(t => String(t.id) === String(id));
        if (topic) {
          content = { subject: topic.subject, body: topic.content ?? topic.excerpt ?? '', stage: topic.stage };
          break;
        }
      }
    }
    trainingData.push({ id, originalType, correctedType, label, content });
  };

  for (const entry of auditLog) {
    if (['reclassify_item', 'reclassify_topic'].includes(entry.actionType)) {
      addItem(entry.targetId, entry.originalValue, entry.newValue, entry.targetLabel ?? entry.targetId, cached);
    }
  }
  for (const [key, override] of Object.entries(overrides.itemOverrides ?? {})) {
    addItem(key, override.originalType, override.type, key, cached);
  }

  res.json({
    count: trainingData.length,
    description: 'Training data for classification model improvement',
    data: trainingData,
    jsonl: trainingData.map(d => JSON.stringify({
      text: `${d.content?.subject ?? d.label}\n${d.content?.body ?? ''}`.trim(),
      label: d.correctedType,
      original_label: d.originalType,
    })).join('\n'),
  });
});

// ── Helpers ───────────────────────────────────────────────────────────────

function mergePatterns(current, proposed) {
  const mergeKws = (a, b) => [...new Set([
    ...(a ?? []).map(p => typeof p === 'object' ? p.keyword : p),
    ...(b ?? []),
  ])];
  return {
    validatorKeywords: mergeKws(current.validatorKeywords, proposed.validatorKeywords),
    featuredAppKeywords: mergeKws(current.featuredAppKeywords, proposed.featuredAppKeywords),
    cipKeywords: mergeKws(current.cipKeywords, proposed.cipKeywords),
    protocolUpgradeKeywords: mergeKws(current.protocolUpgradeKeywords, proposed.protocolUpgradeKeywords),
    outcomeKeywords: mergeKws(current.outcomeKeywords, proposed.outcomeKeywords),
    entityNameMappings: { ...(current.entityNameMappings ?? {}), ...(proposed.entityNameMappings ?? {}) },
  };
}

function collectCorrections(auditLog, overrides) {
  const out = [];
  const seen = new Set();
  for (const e of auditLog) {
    if (!['reclassify_item', 'reclassify_topic'].includes(e.actionType)) continue;
    if (seen.has(e.targetId)) continue;
    seen.add(e.targetId);
    out.push({ targetId: e.targetId, label: e.targetLabel ?? e.targetId, originalType: e.originalValue, correctedType: e.newValue });
  }
  for (const [key, override] of Object.entries(overrides.itemOverrides ?? {})) {
    if (seen.has(key)) continue;
    out.push({ targetId: key, label: key, originalType: override.originalType, correctedType: override.type });
  }
  return out;
}

export default router;
