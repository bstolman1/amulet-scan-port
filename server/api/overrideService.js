/**
 * Override service.
 *
 * Applies manual corrections (type, merge, move, extract, topic) to a
 * lifecycle dataset.  Each override type is handled by its own focused
 * function instead of a single 200-line monolith.
 *
 * All functions are pure transformations: they receive data and return
 * new data; side effects (logging) are minimal and labelled.
 */

import {
  readOverrides,
  writeOverrides,
  readAuditLog,
  writeAuditLog,
  readCache,
} from './fileRepository.js';
import { WORKFLOW_STAGES, VALID_TYPES } from './constants.js';

// ── Audit log helpers ─────────────────────────────────────────────────────

/**
 * Append a structured entry to the audit log.
 * @param {object} action
 * @returns {Promise<object>} The new entry.
 */
export async function logOverrideAction(action) {
  const entries = await readAuditLog();
  const entry = {
    id: `audit-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: new Date().toISOString(),
    ...action,
  };
  entries.push(entry);
  await writeAuditLog(entries);
  console.log(
    `📝 Audit: ${action.actionType} — ${action.targetId} ` +
    `(${action.originalValue ?? 'n/a'} → ${action.newValue ?? 'n/a'})`,
  );
  return entry;
}

// ── Source lookup helpers ─────────────────────────────────────────────────

/**
 * Find the original classification of an item or topic from cached data.
 * @param {string} targetId
 * @param {'item'|'topic'} targetType
 * @returns {Promise<object|null>}
 */
export async function findOriginalClassification(targetId, targetType = 'item') {
  const cached = await readCache();
  if (!cached?.lifecycleItems) return null;

  if (targetType === 'item') {
    const item = cached.lifecycleItems.find(
      i => i.id === targetId || i.primaryId === targetId,
    );
    return item ? { type: item.type, primaryId: item.primaryId, id: item.id } : null;
  }

  for (const item of cached.lifecycleItems) {
    const topic = (item.topics ?? []).find(t => String(t.id) === String(targetId));
    if (topic) {
      return {
        parentType: item.type,
        parentId: item.primaryId,
        stage: topic.effectiveStage ?? topic.stage,
        subject: topic.subject?.slice(0, 100),
      };
    }
  }
  return null;
}

// ── CIP normalisation ─────────────────────────────────────────────────────

function normalizeCipId(cip) {
  return cip
    .toUpperCase()
    .replace(/^CIP\s*[-#]?\s*0*/, 'CIP-')
    .replace(/CIP-(\d+)/, (_, num) => `CIP-${num.padStart(4, '0')}`);
}

// ── Individual override applicators ──────────────────────────────────────

/** Apply item-level type overrides. */
function applyItemOverrides(items, itemOverrides) {
  let count = 0;
  const result = items.map(item => {
    const override =
      itemOverrides[item.id] ?? itemOverrides[item.primaryId];
    if (!override) return item;
    count++;
    console.log(`Override type: "${item.primaryId}" → ${override.type}`);
    return {
      ...item,
      type: override.type,
      overrideApplied: true,
      overrideReason: override.reason ?? 'Manual correction',
    };
  });
  if (count) console.log(`Applied ${count} item type overrides`);
  return result;
}

/** Apply merge overrides (item-level and topic-level). */
function applyMergeOverrides(items, mergeOverrides) {
  if (!Object.keys(mergeOverrides).length) return items;

  const byPrimaryId = new Map(
    items.map(item => [item.primaryId?.toUpperCase(), item]),
  );

  /** Move a single topic into one or more target items. */
  const mergeTopicInto = (topic, targets, sourcePrimaryId) => {
    for (const targetKey of targets) {
      const target = byPrimaryId.get(targetKey.toUpperCase());
      if (!target) continue;

      // Map non-CIP stages into the CIP workflow so the topic is visible
      let stage = topic.stage;
      if (target.type === 'cip' && !WORKFLOW_STAGES.cip.includes(stage)) {
        stage = WORKFLOW_STAGES.cip[0];
      }
      const topicForTarget = stage === topic.stage ? topic : { ...topic, stage };

      if (!target.topics.find(t => t.id === topicForTarget.id)) {
        target.topics.push(topicForTarget);
      }
      target.stages[stage] ??= [];
      if (!target.stages[stage].find(t => t.id === topicForTarget.id)) {
        target.stages[stage].push(topicForTarget);
      }
      target.mergedFrom ??= [];
      if (sourcePrimaryId && !target.mergedFrom.includes(sourcePrimaryId)) {
        target.mergedFrom.push(sourcePrimaryId);
      }
    }
  };

  const mergedItemIds = new Set();
  let mergeCount = 0;

  for (const item of items) {
    // Item-level merge
    const itemMerge =
      mergeOverrides[item.id] ?? mergeOverrides[item.primaryId];
    if (itemMerge) {
      const targets = [itemMerge.mergeInto].flat();
      item.topics.forEach(t => mergeTopicInto(t, targets, item.primaryId));

      for (const targetKey of targets) {
        const target = byPrimaryId.get(targetKey.toUpperCase());
        if (!target) continue;
        if (new Date(item.firstDate) < new Date(target.firstDate)) target.firstDate = item.firstDate;
        if (new Date(item.lastDate) > new Date(target.lastDate)) target.lastDate = item.lastDate;
      }
      mergedItemIds.add(item.id);
      mergedItemIds.add(item.primaryId);
      mergeCount++;
      continue;
    }

    // Topic-level merge
    const topicsToMove = [];
    const remaining = [];

    for (const topic of item.topics ?? []) {
      const key = String(topic.id);
      const override = mergeOverrides[key] ?? mergeOverrides[topic.subject];
      if (override) {
        topicsToMove.push({ topic, targets: [override.mergeInto].flat() });
        mergeCount++;
      } else {
        remaining.push(topic);
      }
    }

    if (!topicsToMove.length) continue;

    for (const { topic, targets } of topicsToMove) {
      mergeTopicInto(topic, targets, item.primaryId);
    }

    const movedIds = new Set(topicsToMove.map(({ topic }) => String(topic.id)));
    item.topics = remaining;

    for (const stageKey of Object.keys(item.stages ?? {})) {
      item.stages[stageKey] = (item.stages[stageKey] ?? []).filter(
        t => !movedIds.has(String(t.id)),
      );
      if (!item.stages[stageKey].length) delete item.stages[stageKey];
    }

    if (item.topics.length === 0) {
      mergedItemIds.add(item.id);
    } else {
      const dates = item.topics
        .map(t => t.date)
        .filter(Boolean)
        .map(d => new Date(d))
        .filter(d => !isNaN(d));
      if (dates.length) {
        item.firstDate = new Date(Math.min(...dates)).toISOString();
        item.lastDate = new Date(Math.max(...dates)).toISOString();
      }
    }
  }

  if (mergeCount) console.log(`Applied ${mergeCount} merge overrides`);
  return items.filter(i => !mergedItemIds.has(i.id) && !mergedItemIds.has(i.primaryId));
}

/** Apply move overrides (topic to a different card). */
function applyMoveOverrides(items, moveOverrides) {
  if (!Object.keys(moveOverrides).length) return items;

  const cardById = new Map(items.map(i => [String(i.id), i]));
  const cardByPrimaryId = new Map(items.map(i => [String(i.primaryId), i]));
  const getCard = id => cardById.get(String(id)) ?? cardByPrimaryId.get(String(id));

  const movedIds = new Set();

  for (const item of items) {
    const toMove = [];
    const remaining = [];

    for (const topic of item.topics ?? []) {
      const override = moveOverrides[String(topic.id)];
      if (override?.targetCardId) {
        const target = getCard(override.targetCardId);
        if (target && target.id !== item.id) {
          toMove.push({ topic, target });
          movedIds.add(String(topic.id));
        } else {
          remaining.push(topic);
        }
      } else {
        remaining.push(topic);
      }
    }

    for (const { topic, target } of toMove) {
      console.log(
        `Moving "${topic.subject?.slice(0, 50)}" from "${item.primaryId}" to "${target.primaryId}"`,
      );
      target.topics ??= [];
      target.topics.push(topic);

      const stage = topic.effectiveStage ?? topic.stage;
      if (stage) {
        target.stages ??= {};
        target.stages[stage] ??= [];
        target.stages[stage].push(topic);
      }

      if (topic.date) {
        const d = new Date(topic.date);
        if (!isNaN(d)) {
          if (!target.firstDate || d < new Date(target.firstDate)) target.firstDate = d.toISOString();
          if (!target.lastDate || d > new Date(target.lastDate)) target.lastDate = d.toISOString();
        }
      }
    }

    item.topics = remaining;

    for (const stageKey of Object.keys(item.stages ?? {})) {
      item.stages[stageKey] = (item.stages[stageKey] ?? []).filter(
        t => !movedIds.has(String(t.id)),
      );
      if (!item.stages[stageKey].length) delete item.stages[stageKey];
    }
  }

  console.log(`Applied ${movedIds.size} move overrides`);
  return items.filter(i => i.topics?.length > 0);
}

/** Apply topic-level type overrides (reclassify a topic to a different type). */
function applyTopicTypeOverrides(items, topicOverrides) {
  if (!Object.keys(topicOverrides).length) return items;

  const toReclassify = [];

  for (const item of items) {
    const remaining = [];
    for (const topic of item.topics ?? []) {
      const override = topicOverrides[String(topic.id)];
      if (override && override.type !== item.type) {
        toReclassify.push({ topic, sourceItem: item, newType: override.type, reason: override.reason });
      } else {
        remaining.push(topic);
      }
    }
    item.topics = remaining;

    for (const stageKey of Object.keys(item.stages ?? {})) {
      item.stages[stageKey] = (item.stages[stageKey] ?? []).filter(
        t => !toReclassify.find(r => String(r.topic.id) === String(t.id)),
      );
      if (!item.stages[stageKey].length) delete item.stages[stageKey];
    }
  }

  for (const { topic, sourceItem, newType, reason } of toReclassify) {
    const newId = `topic-reclassified-${topic.id}`;
    const targetStages = WORKFLOW_STAGES[newType] ?? WORKFLOW_STAGES.other;
    const effectiveStage = targetStages.includes(topic.stage)
      ? topic.stage
      : targetStages[0];

    items.push({
      id: newId,
      primaryId: topic.subject?.slice(0, 80) ?? `Reclassified Topic ${topic.id}`,
      type: newType,
      network: sourceItem.network ?? null,
      stages: { [effectiveStage]: [{ ...topic, stage: effectiveStage }] },
      topics: [{ ...topic, stage: effectiveStage }],
      firstDate: topic.date,
      lastDate: topic.date,
      currentStage: effectiveStage,
      overrideApplied: true,
      overrideReason: reason ?? 'Topic reclassified',
    });
    console.log(`Reclassified topic "${topic.subject?.slice(0, 50)}" → ${newType}`);
  }

  return items.filter(
    i => i.topics?.length > 0 ||
      i.id.startsWith('topic-reclassified-') ||
      i.id.startsWith('topic-extracted-'),
  );
}

/** Apply extract overrides (move a topic to its own card, same type). */
function applyExtractOverrides(items, extractOverrides) {
  if (!Object.keys(extractOverrides).length) return items;

  const toExtract = [];

  for (const item of items) {
    const remaining = [];
    for (const topic of item.topics ?? []) {
      const override = extractOverrides[String(topic.id)];
      if (override) {
        toExtract.push({ topic, sourceItem: item, customName: override.customName, reason: override.reason });
      } else {
        remaining.push(topic);
      }
    }
    item.topics = remaining;

    for (const stageKey of Object.keys(item.stages ?? {})) {
      item.stages[stageKey] = (item.stages[stageKey] ?? []).filter(
        t => !toExtract.find(e => String(e.topic.id) === String(t.id)),
      );
      if (!item.stages[stageKey].length) delete item.stages[stageKey];
    }
  }

  for (const { topic, sourceItem, customName, reason } of toExtract) {
    const effectiveStage = topic.stage ?? Object.keys(sourceItem.stages ?? {})[0] ?? 'announced';
    items.push({
      id: `topic-extracted-${topic.id}`,
      primaryId: customName ?? topic.subject?.slice(0, 80) ?? `Extracted Topic ${topic.id}`,
      type: sourceItem.type,
      network: sourceItem.network ?? null,
      stages: { [effectiveStage]: [{ ...topic, stage: effectiveStage }] },
      topics: [{ ...topic, stage: effectiveStage }],
      firstDate: topic.date,
      lastDate: topic.date,
      currentStage: effectiveStage,
      overrideApplied: true,
      overrideReason: reason ?? 'Extracted to own card',
      extractedFrom: sourceItem.primaryId,
    });
    console.log(`Extracted "${topic.subject?.slice(0, 50)}" from "${sourceItem.primaryId}"`);
  }

  return items.filter(
    i => i.topics?.length > 0 ||
      i.id.startsWith('topic-reclassified-') ||
      i.id.startsWith('topic-extracted-'),
  );
}

// ── Main entry point ──────────────────────────────────────────────────────

/**
 * Apply all overrides to a lifecycle dataset.
 * @param {object} data - { lifecycleItems, ... }
 * @returns {Promise<object>} - mutated copy
 */
export async function applyOverrides(data) {
  if (!data?.lifecycleItems) return data;

  const overrides = await readOverrides();
  const hasAny = [
    overrides.itemOverrides,
    overrides.topicOverrides,
    overrides.mergeOverrides,
    overrides.extractOverrides,
    overrides.moveOverrides,
  ].some(o => Object.keys(o).length > 0);

  if (!hasAny) return data;

  let items = [...data.lifecycleItems];

  items = applyItemOverrides(items, overrides.itemOverrides);
  items = applyMergeOverrides(items, overrides.mergeOverrides);
  items = applyMoveOverrides(items, overrides.moveOverrides);
  items = applyTopicTypeOverrides(items, overrides.topicOverrides);
  items = applyExtractOverrides(items, overrides.extractOverrides);

  return { ...data, lifecycleItems: items };
}

// ── CRUD helpers used by route handlers ──────────────────────────────────

export async function setItemOverride(key, type, reason, originalType) {
  if (!VALID_TYPES.includes(type)) throw new Error(`Invalid type: ${type}`);
  const overrides = await readOverrides();
  overrides.itemOverrides[key] = {
    type,
    originalType: originalType ?? null,
    reason: reason ?? 'Manual correction',
    createdAt: new Date().toISOString(),
  };
  await writeOverrides(overrides);
}

export async function deleteItemOverride(key) {
  const overrides = await readOverrides();
  if (!overrides.itemOverrides[key]) throw new Error(`No override for "${key}"`);
  delete overrides.itemOverrides[key];
  await writeOverrides(overrides);
}

export async function setTopicOverride(topicId, type, reason, originalParentType, originalParentId) {
  if (!VALID_TYPES.includes(type)) throw new Error(`Invalid type: ${type}`);
  const overrides = await readOverrides();
  overrides.topicOverrides[String(topicId)] = {
    type,
    originalParentType: originalParentType ?? null,
    originalParentId: originalParentId ?? null,
    reason: reason ?? 'Manual topic reclassification',
    createdAt: new Date().toISOString(),
  };
  await writeOverrides(overrides);
}

export async function setExtractOverride(topicId, customName, reason, originalParentId, originalParentType) {
  const overrides = await readOverrides();
  overrides.extractOverrides[String(topicId)] = {
    customName: customName ?? null,
    originalParentId: originalParentId ?? null,
    originalParentType: originalParentType ?? null,
    reason: reason ?? 'Extracted to own card',
    createdAt: new Date().toISOString(),
  };
  await writeOverrides(overrides);
}

export async function setMergeOverride(key, mergeIntoRaw, reason, originalParentId, originalType) {
  const normalizedTargets = [mergeIntoRaw].flat().map(normalizeCipId);
  const overrides = await readOverrides();
  overrides.mergeOverrides[String(key)] = {
    mergeInto: normalizedTargets,
    originalParentId: originalParentId ?? null,
    originalType: originalType ?? null,
    reason: reason ?? 'Manual merge',
    createdAt: new Date().toISOString(),
  };
  await writeOverrides(overrides);
  return normalizedTargets;
}

export async function setMoveOverride(topicId, targetCardId, reason, originalParentId, originalParentType, targetType) {
  const overrides = await readOverrides();
  overrides.moveOverrides[String(topicId)] = {
    targetCardId: String(targetCardId),
    originalParentId: originalParentId ?? null,
    originalParentType: originalParentType ?? null,
    targetType: targetType ?? null,
    reason: reason ?? 'Manual topic move',
    createdAt: new Date().toISOString(),
  };
  await writeOverrides(overrides);
}
