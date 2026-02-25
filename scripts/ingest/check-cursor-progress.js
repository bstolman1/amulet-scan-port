#!/usr/bin/env node
/**
 * Cursor-Based Backfill Progress CLI
 * 
 * Reads cursor files and calculates real backfill progress by comparing
 * last_before against migration time boundaries.
 * 
 * The backfill moves BACKWARD from max_time toward min_time, so progress is:
 *   (max_time - last_before) / (max_time - min_time)
 * 
 * Usage:
 *   node check-cursor-progress.js                # All migrations
 *   node check-cursor-progress.js --migration 3   # Single migration
 *   node check-cursor-progress.js --watch          # Auto-refresh every 3s
 *   node check-cursor-progress.js --json           # JSON output
 */

import { readdirSync, readFileSync, statSync } from 'fs';
import { join } from 'path';
import { getCursorDir } from './path-utils.js';

// ── CLI flags ──────────────────────────────────────────────

const args = process.argv.slice(2);
const WATCH_MODE = args.includes('--watch') || args.includes('-w');
const JSON_MODE = args.includes('--json');
const MIGRATION_FILTER = args.includes('--migration')
  ? parseInt(args[args.indexOf('--migration') + 1], 10)
  : null;
const REFRESH_INTERVAL = 3000;
const STALE_THRESHOLD_MS = 60_000; // 60s

// ── Cursor loading ─────────────────────────────────────────

function loadCursors() {
  const cursorDir = getCursorDir();
  const cursors = [];

  let files;
  try {
    files = readdirSync(cursorDir);
  } catch (e) {
    if (!JSON_MODE) {
      console.error(`❌ Cannot read cursor directory: ${cursorDir}`);
      console.error(`   ${e.message}`);
      console.error(`   Set CURSOR_DIR or DATA_DIR env vars if using a custom location.`);
    }
    return cursors;
  }

  for (const file of files) {
    if (!file.startsWith('cursor-') || !file.endsWith('.json')) continue;

    const filePath = join(cursorDir, file);
    try {
      const content = JSON.parse(readFileSync(filePath, 'utf8'));
      const fstat = statSync(filePath);
      cursors.push({ file, ...content, _file_mtime: fstat.mtime });
    } catch {
      // skip corrupt / partial files
    }
  }

  return cursors;
}

// ── Progress math ──────────────────────────────────────────

function calcProgress(cursor) {
  if (cursor.complete) return 100;
  if (!cursor.min_time || !cursor.max_time || !cursor.last_before) return 0;

  const minMs = new Date(cursor.min_time).getTime();
  const maxMs = new Date(cursor.max_time).getTime();
  const curMs = new Date(cursor.last_before).getTime();

  const totalRange = maxMs - minMs;
  if (totalRange <= 0) return 100;

  const completed = maxMs - curMs;
  const pct = (completed / totalRange) * 100;
  return Math.min(100, Math.max(0, pct));
}

function calcETA(cursor) {
  if (cursor.complete) return null;
  if (!cursor.started_at || !cursor.updated_at) return null;

  const elapsed = new Date(cursor.updated_at).getTime() - new Date(cursor.started_at).getTime();
  if (elapsed <= 0) return null;

  const pct = calcProgress(cursor);
  if (pct <= 0) return null;

  const totalEstimate = (elapsed / pct) * 100;
  const remaining = totalEstimate - elapsed;
  return remaining > 0 ? remaining : 0;
}

function formatDuration(ms) {
  if (ms == null) return 'Unknown';
  if (ms <= 0) return 'Almost done';
  const h = Math.floor(ms / 3_600_000);
  const m = Math.floor((ms % 3_600_000) / 60_000);
  return h > 0 ? `~${h}h ${m}m` : `~${m}m`;
}

function formatNum(n) {
  return (n || 0).toLocaleString();
}

function progressBar(pct, width = 20) {
  const filled = Math.round((pct / 100) * width);
  return '[' + '█'.repeat(filled) + '░'.repeat(width - filled) + ']';
}

function cursorStatus(cursor) {
  if (cursor.complete) return '✅ Complete';
  if (!cursor.last_before) return '⏳ Not started';

  const updatedAt = cursor.updated_at || cursor._file_mtime;
  if (updatedAt) {
    const age = Date.now() - new Date(updatedAt).getTime();
    if (age > STALE_THRESHOLD_MS) {
      const secsAgo = Math.round(age / 1000);
      return `⚠️  Stalled (${secsAgo}s ago)`;
    }
    const secsAgo = Math.round(age / 1000);
    return `🔄 Active (updated ${secsAgo}s ago)`;
  }
  return '🔄 Active';
}

// ── Group by migration ─────────────────────────────────────

function groupByMigration(cursors) {
  const map = new Map();

  for (const c of cursors) {
    const mid = c.migration_id ?? 'unknown';
    if (MIGRATION_FILTER !== null && mid !== MIGRATION_FILTER) continue;

    if (!map.has(mid)) {
      map.set(mid, {
        migrationId: mid,
        shards: [],
      });
    }
    map.get(mid).shards.push(c);
  }

  // Sort by migration id
  return [...map.entries()]
    .sort(([a], [b]) => (a === 'unknown' ? 1 : b === 'unknown' ? -1 : a - b))
    .map(([, v]) => v);
}

// ── Aggregate a migration group ────────────────────────────

function aggregateMigration(group) {
  const { shards } = group;
  const allComplete = shards.every(s => s.complete);
  const anyStarted = shards.some(s => s.last_before);
  const totalUpdates = shards.reduce((s, c) => s + (c.total_updates || 0), 0);
  const totalEvents = shards.reduce((s, c) => s + (c.total_events || 0), 0);

  // Weighted average progress
  const pcts = shards.map(s => calcProgress(s));
  const avgProgress = pcts.length > 0 ? pcts.reduce((a, b) => a + b, 0) / pcts.length : 0;

  // Best ETA estimate: max across shards (slowest determines completion)
  const etas = shards.map(s => calcETA(s)).filter(e => e != null);
  const etaMs = etas.length > 0 ? Math.max(...etas) : null;

  // Representative cursor position (the one furthest from completion = highest last_before)
  const activeShard = shards
    .filter(s => !s.complete && s.last_before)
    .sort((a, b) => new Date(b.last_before).getTime() - new Date(a.last_before).getTime())[0];

  // Synchronizer (should be same for all shards in a migration)
  const synchronizer = shards[0]?.synchronizer_id || 'unknown';

  // Range
  const minTime = shards[0]?.min_time;
  const maxTime = shards[0]?.max_time;

  // Status: check staleness across all active shards
  let status;
  if (allComplete) {
    status = '✅ Complete';
  } else if (!anyStarted) {
    status = '⏳ Not started';
  } else {
    // Use the most recent updated_at across shards
    const activeShards = shards.filter(s => !s.complete);
    const latestUpdate = activeShards
      .map(s => new Date(s.updated_at || s._file_mtime || 0).getTime())
      .reduce((a, b) => Math.max(a, b), 0);
    const age = Date.now() - latestUpdate;
    if (age > STALE_THRESHOLD_MS) {
      status = `⚠️  Stalled (${Math.round(age / 1000)}s ago)`;
    } else {
      status = `🔄 Active (updated ${Math.round(age / 1000)}s ago)`;
    }
  }

  return {
    migrationId: group.migrationId,
    synchronizer,
    minTime,
    maxTime,
    cursorAt: activeShard?.last_before || null,
    progress: Math.min(allComplete ? 100 : avgProgress, allComplete ? 100 : 99.9),
    totalUpdates,
    totalEvents,
    eta: etaMs,
    status,
    complete: allComplete,
    shardCount: shards.length,
    shards: shards.map(s => ({
      file: s.file,
      shard_index: s.shard_index,
      progress: calcProgress(s),
      last_before: s.last_before,
      complete: !!s.complete,
      total_updates: s.total_updates || 0,
      total_events: s.total_events || 0,
      status: cursorStatus(s),
    })),
  };
}

// ── JSON output ────────────────────────────────────────────

function outputJSON(migrations) {
  const data = migrations.map(aggregateMigration);
  const completedCount = data.filter(m => m.complete).length;
  const totalProgress = data.length > 0
    ? data.reduce((s, m) => s + m.progress, 0) / data.length
    : 0;

  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    overall: {
      completed: completedCount,
      total: data.length,
      progress: parseFloat(totalProgress.toFixed(1)),
    },
    migrations: data,
  }, null, 2));
}

// ── Pretty output ──────────────────────────────────────────

function outputPretty(migrations) {
  console.log('');
  console.log('═'.repeat(63));
  console.log('📊 BACKFILL PROGRESS (Cursor-Based)');
  console.log('═'.repeat(63));

  if (migrations.length === 0) {
    console.log('\n  No cursor files found.\n');
    console.log(`  Cursor dir: ${getCursorDir()}`);
    console.log('═'.repeat(63));
    return;
  }

  const summaries = migrations.map(aggregateMigration);
  let completedCount = 0;

  for (const m of summaries) {
    console.log('');
    console.log(`Migration ${m.migrationId}`);

    if (m.complete) {
      completedCount++;
      console.log(`  Status:         ${m.status}`);
      console.log(`  Updates:        ${formatNum(m.totalUpdates)}`);
      console.log(`  Events:         ${formatNum(m.totalEvents)}`);
      continue;
    }

    if (!m.cursorAt) {
      console.log(`  Status:         ${m.status}`);
      continue;
    }

    const syncShort = m.synchronizer.length > 40
      ? m.synchronizer.substring(0, 40) + '...'
      : m.synchronizer;

    console.log(`  Synchronizer:   ${syncShort}`);
    console.log(`  Cursor at:      ${m.cursorAt}`);
    if (m.minTime && m.maxTime) {
      console.log(`  Range:          ${m.minTime.substring(0, 10)} → ${m.maxTime.substring(0, 10)}`);
    }
    console.log(`  Progress:       ${progressBar(m.progress)} ${m.progress.toFixed(1)}%`);
    console.log(`  Updates:        ${formatNum(m.totalUpdates)}`);
    console.log(`  Events:         ${formatNum(m.totalEvents)}`);
    console.log(`  ETA:            ${formatDuration(m.eta)}`);
    console.log(`  Status:         ${m.status}`);

    // Show per-shard breakdown if multiple shards
    if (m.shardCount > 1) {
      console.log(`  Shards:         ${m.shardCount}`);
      for (const s of m.shards) {
        const label = s.shard_index != null ? `Shard ${s.shard_index}` : s.file;
        const icon = s.complete ? '✅' : '🔄';
        console.log(`    ${icon} ${label.padEnd(10)} ${progressBar(s.progress, 15)} ${s.progress.toFixed(1).padStart(5)}%  ${formatNum(s.total_updates).padStart(10)} upd`);
      }
    }
  }

  const totalProgress = summaries.length > 0
    ? summaries.reduce((s, m) => s + m.progress, 0) / summaries.length
    : 0;

  console.log('');
  console.log('─'.repeat(63));
  console.log(`Overall: ${completedCount}/${summaries.length} complete | ${totalProgress.toFixed(1)}% total`);
  console.log('─'.repeat(63));

  if (WATCH_MODE) {
    console.log(`Last refresh: ${new Date().toISOString()}  (Ctrl+C to exit)`);
  }
}

// ── Main ───────────────────────────────────────────────────

function run() {
  const cursors = loadCursors();
  const migrations = groupByMigration(cursors);

  if (JSON_MODE) {
    outputJSON(migrations);
  } else {
    outputPretty(migrations);
  }
}

if (WATCH_MODE) {
  const refresh = () => {
    process.stdout.write('\x1B[2J\x1B[H');
    run();
  };
  refresh();
  setInterval(refresh, REFRESH_INTERVAL);
} else {
  run();
}
