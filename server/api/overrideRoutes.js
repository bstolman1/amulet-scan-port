/**
 * Override routes.
 *
 * GET    /overrides                - list all overrides
 * POST   /overrides                - set item-level type override
 * DELETE /overrides/:key           - remove item override
 * POST   /overrides/topic          - reclassify a topic
 * POST   /overrides/extract        - extract a topic to its own card
 * POST   /overrides/merge          - merge a card/topic into a CIP
 * POST   /overrides/move-topic     - move a topic to a different card
 * GET    /overrides/merge-debug    - debug merge override matching
 * GET    /overrides/analysis       - analyse patterns in existing overrides
 */

import { Router } from 'express';
import { readOverrides } from './fileRepository.js';
import { readCache } from './fileRepository.js';
import {
  setItemOverride,
  deleteItemOverride,
  setTopicOverride,
  setExtractOverride,
  setMergeOverride,
  setMoveOverride,
  findOriginalClassification,
  logOverrideAction,
} from './overrideService.js';
import { validateBody } from './requestValidators.js';
import { analyzeCorrections, generateImprovementSuggestions } from './classificationAnalyzer.js';
import { readAuditLog } from './fileRepository.js';

const router = Router();

// ── GET /overrides ────────────────────────────────────────────────────────

router.get('/', async (req, res) => {
  const overrides = await readOverrides();
  res.json(overrides);
});

// ── POST /overrides ───────────────────────────────────────────────────────

router.post('/', validateBody('setItemOverride'), async (req, res) => {
  const { itemId, primaryId, type, reason } = req.body;
  const key = primaryId ?? itemId;

  try {
    const original = await findOriginalClassification(key, 'item');
    await setItemOverride(key, type, reason, original?.type);
    await logOverrideAction({
      actionType: 'reclassify_item',
      targetId: key,
      targetLabel: original?.primaryId ?? key,
      originalValue: original?.type ?? 'unknown',
      newValue: type,
      reason: reason ?? 'Manual correction',
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /overrides/:key ────────────────────────────────────────────────

router.delete('/:key', async (req, res) => {
  // Sanitise: reject keys that look like path traversal
  const key = req.params.key;
  if (key.includes('/') || key.includes('\\') || key.includes('..')) {
    return res.status(400).json({ error: 'Invalid key' });
  }
  try {
    await deleteItemOverride(key);
    res.json({ success: true, message: `Override removed for "${key}"` });
  } catch (err) {
    res.status(err.message.startsWith('No override') ? 404 : 500).json({ error: err.message });
  }
});

// ── POST /overrides/topic ─────────────────────────────────────────────────

router.post('/topic', validateBody('setTopicOverride'), async (req, res) => {
  const { topicId, newType, reason } = req.body;
  const key = String(topicId);

  try {
    const original = await findOriginalClassification(key, 'topic');
    await setTopicOverride(key, newType, reason, original?.parentType, original?.parentId);
    await logOverrideAction({
      actionType: 'reclassify_topic',
      targetId: key,
      targetLabel: original?.subject ?? key,
      originalValue: original?.parentType ?? 'unknown',
      originalParentId: original?.parentId ?? null,
      newValue: newType,
      reason: reason ?? 'Manual topic reclassification',
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /overrides/extract ───────────────────────────────────────────────

router.post('/extract', validateBody('setExtractOverride'), async (req, res) => {
  const { topicId, customName, reason } = req.body;
  const key = String(topicId);

  try {
    const original = await findOriginalClassification(key, 'topic');
    await setExtractOverride(key, customName, reason, original?.parentId, original?.parentType);
    await logOverrideAction({
      actionType: 'extract_topic',
      targetId: key,
      targetLabel: original?.subject ?? key,
      originalValue: original?.parentId ?? 'unknown',
      newValue: customName ?? 'new card',
      reason: reason ?? 'Extracted to own card',
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /overrides/merge ─────────────────────────────────────────────────

router.post('/merge', validateBody('setMergeOverride'), async (req, res) => {
  const { sourceId, sourcePrimaryId, mergeInto, reason } = req.body;
  const key = String(sourcePrimaryId ?? sourceId);

  try {
    const originalItem = await findOriginalClassification(key, 'item');
    const originalTopic = await findOriginalClassification(key, 'topic');
    const original = originalItem ?? originalTopic;
    const targets = await setMergeOverride(
      key, mergeInto, reason, original?.parentId ?? original?.primaryId, original?.parentType ?? original?.type,
    );
    await logOverrideAction({
      actionType: 'merge',
      targetId: key,
      targetLabel: original?.subject ?? original?.primaryId ?? key,
      originalValue: original?.parentId ?? original?.primaryId ?? 'unknown',
      newValue: targets.join(', '),
      reason: reason ?? 'Manual merge',
    });
    res.json({ success: true, targets });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /overrides/move-topic ────────────────────────────────────────────

router.post('/move-topic', validateBody('setMoveOverride'), async (req, res) => {
  const { topicId, targetCardId, reason } = req.body;
  const key = String(topicId);

  try {
    const original = await findOriginalClassification(key, 'topic');
    const cached = await readCache();
    const targetCard = cached?.lifecycleItems?.find(
      i => i.id === String(targetCardId) || i.primaryId === String(targetCardId),
    );
    await setMoveOverride(key, targetCardId, reason, original?.parentId, original?.parentType, targetCard?.type);
    await logOverrideAction({
      actionType: 'move_topic',
      targetId: key,
      targetLabel: original?.subject ?? key,
      originalValue: original?.parentId ?? 'unknown',
      newValue: targetCard?.primaryId ?? String(targetCardId),
      reason: reason ?? 'Manual topic move',
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /overrides/merge-debug ────────────────────────────────────────────

router.get('/merge-debug', async (req, res) => {
  const [overrides, cached] = await Promise.all([readOverrides(), readCache()]);

  if (!cached?.lifecycleItems) {
    return res.json({ error: 'No cached data', mergeOverrides: overrides.mergeOverrides });
  }

  const allTopicIds = [];
  const allItemIds = [];

  cached.lifecycleItems.forEach(item => {
    allItemIds.push({ id: item.id, primaryId: item.primaryId, type: item.type });
    (item.topics ?? []).forEach(t => {
      allTopicIds.push({
        id: String(t.id),
        subject: t.subject?.slice(0, 80),
        stage: t.stage,
        parentPrimaryId: item.primaryId,
        parentType: item.type,
      });
    });
  });

  const matchResults = Object.entries(overrides.mergeOverrides ?? {}).map(([key, override]) => ({
    key,
    override,
    matchedTopic: allTopicIds.find(t => t.id === key || t.subject === key) ?? null,
    matchedItem: allItemIds.find(i => i.id === key || i.primaryId === key) ?? null,
    hasMatch: !!(allTopicIds.find(t => t.id === key) || allItemIds.find(i => i.id === key || i.primaryId === key)),
  }));

  res.json({
    mergeOverrideCount: Object.keys(overrides.mergeOverrides ?? {}).length,
    matchResults,
    sampleTopicIds: allTopicIds.slice(0, 20),
  });
});

// ── GET /overrides/analysis ───────────────────────────────────────────────

router.get('/analysis', async (req, res) => {
  const [overrides, auditLog, cached] = await Promise.all([
    readOverrides(),
    readAuditLog(),
    readCache(),
  ]);

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

  const analysis = analyzeCorrections(corrections);
  const suggestions = generateImprovementSuggestions(analysis);

  res.json({
    overrides: overrides.itemOverrides ?? {},
    mergeOverrides: overrides.mergeOverrides ?? {},
    analysis: corrections.slice(0, 50),
    suggestions,
    summary: {
      totalItemOverrides: Object.keys(overrides.itemOverrides ?? {}).length,
      totalMergeOverrides: Object.keys(overrides.mergeOverrides ?? {}).length,
      correctionsByType: Object.fromEntries(
        Object.entries(analysis.typeTransitions).map(([k, v]) => [k, v.count]),
      ),
    },
  });
});

export default router;
