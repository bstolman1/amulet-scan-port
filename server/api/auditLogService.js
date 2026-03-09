/**
 * Audit log service.
 *
 * Thin wrapper around the file repository that adds business logic:
 * backfilling historical overrides, computing stats, and filtering.
 */

import { readAuditLog, writeAuditLog, readCache } from '../repositories/fileRepository.js';
import { readOverrides } from '../repositories/fileRepository.js';
import { findOriginalClassification } from './overrideService.js';

// ── Helpers ───────────────────────────────────────────────────────────────

function makeBackfillEntry(overrideKey, override, actionType, original, extra = {}) {
  return {
    id: `backfill-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: override.createdAt ?? new Date().toISOString(),
    backfilled: true,
    actionType,
    targetId: overrideKey,
    ...extra,
    reason: override.reason ?? 'Manual correction',
  };
}

// ── Public API ────────────────────────────────────────────────────────────

/**
 * Retrieve filtered and paginated audit log entries.
 * @param {{ limit?: number, actionType?: string }} opts
 * @returns {Promise<object>}
 */
export async function getAuditLogEntries({ limit = 100, actionType } = {}) {
  const entries = await readAuditLog();
  const filtered = actionType
    ? entries.filter(e => e.actionType === actionType)
    : entries;

  const sorted = [...filtered].sort(
    (a, b) => new Date(b.timestamp) - new Date(a.timestamp),
  );

  return {
    total: entries.length,
    filtered: sorted.length,
    entries: sorted.slice(0, limit),
    actionTypes: [...new Set(entries.map(e => e.actionType))],
  };
}

/**
 * Compute summary stats over the audit log.
 * @returns {Promise<object>}
 */
export async function getAuditLogStats() {
  const entries = await readAuditLog();
  const byActionType = {};
  const byMonth = {};

  for (const e of entries) {
    byActionType[e.actionType] = (byActionType[e.actionType] ?? 0) + 1;
    const month = e.timestamp?.slice(0, 7) ?? 'unknown';
    byMonth[month] = (byMonth[month] ?? 0) + 1;
  }

  return {
    total: entries.length,
    byActionType,
    byMonth,
    oldestEntry: entries[0]?.timestamp ?? null,
    newestEntry: entries[entries.length - 1]?.timestamp ?? null,
  };
}

/**
 * Report which overrides are not yet represented in the audit log.
 * @returns {Promise<object>}
 */
export async function getPendingBackfill() {
  const [overrides, existingLog] = await Promise.all([readOverrides(), readAuditLog()]);
  const existingIds = new Set(existingLog.map(e => e.targetId));

  const pending = {
    itemOverrides: Object.keys(overrides.itemOverrides).filter(k => !existingIds.has(k)),
    topicOverrides: Object.keys(overrides.topicOverrides).filter(k => !existingIds.has(k)),
    extractOverrides: Object.keys(overrides.extractOverrides).filter(k => !existingIds.has(k)),
    mergeOverrides: Object.keys(overrides.mergeOverrides).filter(k => !existingIds.has(k)),
    moveOverrides: Object.keys(overrides.moveOverrides).filter(k => !existingIds.has(k)),
  };

  const totalPending = Object.values(pending).reduce((s, a) => s + a.length, 0);
  return { totalPending, pending, existingAuditEntries: existingLog.length };
}

/**
 * Backfill the audit log from existing override files.
 * Creates entries for any override that lacks an audit record.
 * @returns {Promise<object>}
 */
export async function backfillAuditLog() {
  const [overrides, existingLog] = await Promise.all([readOverrides(), readAuditLog()]);
  const existingIds = new Set(existingLog.map(e => e.targetId));
  const backfilled = [];

  const maybeAdd = (key, entry) => {
    if (!existingIds.has(key)) backfilled.push(entry);
  };

  // Item overrides
  for (const [key, override] of Object.entries(overrides.itemOverrides)) {
    const original = await findOriginalClassification(key, 'item');
    maybeAdd(key, makeBackfillEntry(key, override, 'reclassify_item', original, {
      targetLabel: original?.primaryId ?? key,
      originalValue: override.originalType ?? original?.type ?? 'unknown',
      newValue: override.type,
    }));
  }

  // Topic overrides
  for (const [key, override] of Object.entries(overrides.topicOverrides)) {
    const original = await findOriginalClassification(key, 'topic');
    maybeAdd(key, makeBackfillEntry(key, override, 'reclassify_topic', original, {
      targetLabel: original?.subject ?? key,
      originalValue: override.originalParentType ?? original?.parentType ?? 'unknown',
      originalParentId: override.originalParentId ?? original?.parentId ?? null,
      newValue: override.type,
    }));
  }

  // Extract overrides
  for (const [key, override] of Object.entries(overrides.extractOverrides)) {
    const original = await findOriginalClassification(key, 'topic');
    maybeAdd(key, makeBackfillEntry(key, override, 'extract_topic', original, {
      targetLabel: original?.subject ?? key,
      originalValue: override.originalParentId ?? original?.parentId ?? 'unknown',
      newValue: override.customName ?? 'new card',
    }));
  }

  // Merge overrides
  for (const [key, override] of Object.entries(overrides.mergeOverrides)) {
    const originalItem = await findOriginalClassification(key, 'item');
    const originalTopic = await findOriginalClassification(key, 'topic');
    const original = originalItem ?? originalTopic;
    maybeAdd(key, makeBackfillEntry(key, override, 'merge', original, {
      targetLabel: original?.subject ?? original?.primaryId ?? key,
      originalValue: override.originalParentId ?? original?.parentId ?? original?.primaryId ?? 'unknown',
      newValue: [override.mergeInto].flat().join(', '),
    }));
  }

  // Move overrides
  for (const [key, override] of Object.entries(overrides.moveOverrides)) {
    const original = await findOriginalClassification(key, 'topic');
    maybeAdd(key, makeBackfillEntry(key, override, 'move_topic', original, {
      targetLabel: original?.subject ?? key,
      originalValue: override.originalParentId ?? original?.parentId ?? 'unknown',
      newValue: override.targetCardId,
    }));
  }

  if (backfilled.length > 0) {
    const updated = [...backfilled, ...existingLog].sort(
      (a, b) => new Date(a.timestamp) - new Date(b.timestamp),
    );
    await writeAuditLog(updated);
    console.log(`📝 Backfilled ${backfilled.length} audit entries`);
  }

  return {
    success: true,
    backfilledCount: backfilled.length,
    backfilledEntries: backfilled,
    totalAuditEntries: existingLog.length + backfilled.length,
  };
}
