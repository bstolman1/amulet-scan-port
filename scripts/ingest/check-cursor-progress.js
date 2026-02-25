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
const STALE_THRESHOLD_MS = 600_000; // 10 minutes (parallel slices may not update cursor for a while)

// ── Known expected update volumes per migration ────────────
// CCView total: ~152M updates (as of 2025-02-25).
// M0-M2 confirmed from completed cursors. M3/M4 estimated by
// proportional day-count split of remaining ~135M:
//   M3: 168 days (Jun 25–Dec 10) → ~92M
//   M4:  77 days (Dec 10–Feb 25) → ~43M
const EXPECTED_UPDATES = {
  0: 2_750_000,
  1: 1_660_000,
  2: 12_570_000,
  3: 92_000_000,
  4: 43_000_000,
};

// ── Watch mode state (for rate calculation) ────────────────
let previousSnapshot = null;

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

function shardStartedAt(shards) {
  const starts = shards
    .filter(s => s.started_at)
    .map(s => new Date(s.started_at).getTime());
  return starts.length > 0 ? Math.min(...starts) : null;
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
  if (!cursor.last_before && !(cursor.total_updates > 0 || cursor.total_events > 0)) return '⏳ Not started';

  // Use file mtime as ground truth for activity (more reliable than updated_at
  // since cursor file is only rewritten when a slice completes or transaction occurs)
  const mtime = cursor._file_mtime;
  const updatedAt = cursor.updated_at;
  const latestActivity = mtime
    ? new Date(mtime).getTime()
    : updatedAt ? new Date(updatedAt).getTime() : 0;

  if (latestActivity > 0) {
    const age = Date.now() - latestActivity;
    if (age > STALE_THRESHOLD_MS) {
      const secsAgo = Math.round(age / 1000);
      return `⚠️  Stalled (last write ${secsAgo}s ago)`;
    }
    const secsAgo = Math.round(age / 1000);
    return `🔄 Active (file updated ${secsAgo}s ago)`;
  }
  return '🔄 Active';
}

/**
 * Detect if cursor is waiting for contiguous slice completion.
 * With parallel slices, the cursor only advances when slice 0..N are all done,
 * so data may be flowing (events growing) while cursor position stays at max_time.
 */
function isWaitingForSlices(cursor) {
  if (cursor.complete) return false;
  if (!cursor.max_time || !cursor.last_before) return false;

  const maxMs = new Date(cursor.max_time).getTime();
  const curMs = new Date(cursor.last_before).getTime();
  // Cursor hasn't moved from max_time (or very close to it)
  const stuck = Math.abs(curMs - maxMs) < 60_000; // within 1 minute of max
  const hasData = (cursor.total_updates || 0) > 0 || (cursor.total_events || 0) > 0;
  return stuck && hasData;
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
  const anyStarted = shards.some(s => s.last_before || (s.total_updates || 0) > 0);
  const totalUpdates = shards.reduce((s, c) => s + (c.total_updates || 0), 0);
  const totalEvents = shards.reduce((s, c) => s + (c.total_events || 0), 0);

  // Pending data (slices in-flight)
  const pendingUpdates = shards.reduce((s, c) => s + (c.pending_updates || 0), 0);
  const pendingEvents = shards.reduce((s, c) => s + (c.pending_events || 0), 0);
  const inTransaction = shards.some(s => s.in_transaction);

  // Cursor-based progress (only advances when contiguous slices complete)
  const pcts = shards.map(s => calcProgress(s));
  const avgProgress = pcts.length > 0 ? pcts.reduce((a, b) => a + b, 0) / pcts.length : 0;

  // Check if we're in the "parallel slices filling" state
  const waitingForSlices = shards.some(s => isWaitingForSlices(s));

  // Best ETA estimate: max across shards (slowest determines completion)
  const etas = shards.map(s => calcETA(s)).filter(e => e != null);
  const etaMs = etas.length > 0 ? Math.max(...etas) : null;

  // Representative cursor position (the one furthest from completion = highest last_before)
  const activeShard = shards
    .filter(s => !s.complete && s.last_before)
    .sort((a, b) => new Date(b.last_before).getTime() - new Date(a.last_before).getTime())[0];

  // Deepest pending_before across shards (shows how far parallel slices have reached)
  const pendingBefores = shards
    .filter(s => s.pending_before)
    .map(s => s.pending_before);
  const deepestPending = pendingBefores.length > 0
    ? pendingBefores.sort((a, b) => new Date(a).getTime() - new Date(b).getTime())[0]
    : null;

  // Synchronizer (should be same for all shards in a migration)
  const synchronizer = shards[0]?.synchronizer_id || 'unknown';

  // Range
  const minTime = shards[0]?.min_time;
  const maxTime = shards[0]?.max_time;

  // Status: use file mtime for activity detection
  let status;
  if (allComplete) {
    status = '✅ Complete';
  } else if (!anyStarted) {
    status = '⏳ Not started';
  } else {
    const activeShards = shards.filter(s => !s.complete);
    const latestMtime = activeShards
      .map(s => new Date(s._file_mtime || 0).getTime())
      .reduce((a, b) => Math.max(a, b), 0);
    const age = Date.now() - latestMtime;
    if (age > STALE_THRESHOLD_MS) {
      status = `⚠️  Stalled (last write ${Math.round(age / 1000)}s ago)`;
    } else {
      status = `🔄 Active (file updated ${Math.round(age / 1000)}s ago)`;
    }
  }

  return {
    migrationId: group.migrationId,
    synchronizer,
    minTime,
    maxTime,
    cursorAt: activeShard?.last_before || null,
    deepestPending,
    progress: Math.min(allComplete ? 100 : avgProgress, allComplete ? 100 : 99.9),
    totalUpdates,
    totalEvents,
    pendingUpdates,
    pendingEvents,
    inTransaction,
    waitingForSlices,
    eta: etaMs,
    status,
    complete: allComplete,
    shardCount: shards.length,
    shards: shards.map(s => ({
      file: s.file,
      shard_index: s.shard_index,
      progress: calcProgress(s),
      last_before: s.last_before,
      pending_before: s.pending_before || null,
      started_at: s.started_at || null,
      complete: !!s.complete,
      total_updates: s.total_updates || 0,
      total_events: s.total_events || 0,
      pending_updates: s.pending_updates || 0,
      in_transaction: !!s.in_transaction,
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

    if (!m.cursorAt && !m.waitingForSlices) {
      console.log(`  Status:         ${m.status}`);
      if (m.totalUpdates > 0 || m.totalEvents > 0) {
        console.log(`  Updates:        ${formatNum(m.totalUpdates)}`);
        console.log(`  Events:         ${formatNum(m.totalEvents)}`);
      }
      continue;
    }

    const syncShort = m.synchronizer.length > 40
      ? m.synchronizer.substring(0, 40) + '...'
      : m.synchronizer;

    console.log(`  Synchronizer:   ${syncShort}`);

    if (m.minTime && m.maxTime) {
      console.log(`  Range:          ${m.minTime.substring(0, 10)} → ${m.maxTime.substring(0, 10)}`);
    }

    // Volume-based progress using UPDATES (verifiable against CCView)
    const expectedUpdates = EXPECTED_UPDATES[m.migrationId];
    const volumePct = expectedUpdates ? (m.totalUpdates / expectedUpdates) * 100 : null;

    // Show cursor position progress
    if (m.progress > 0.1) {
      console.log(`  Cursor at:      ${m.cursorAt}`);
      console.log(`  Cursor prog:    ${progressBar(m.progress)} ${m.progress.toFixed(1)}%`);
    } else if (m.waitingForSlices) {
      console.log(`  Cursor at:      ${m.cursorAt}  (waiting for slices)`);
    }

    // Volume-based progress bar (the one that actually moves)
    if (volumePct != null) {
      const cappedVolPct = Math.min(volumePct, 100);
      console.log(`  Volume prog:    ${progressBar(cappedVolPct)} ${cappedVolPct.toFixed(1)}%  (${formatNum(m.totalUpdates)} / ~${formatNum(expectedUpdates)} updates)`);
    }

    // Show deepest pending position (where the farthest parallel slice has reached)
    if (m.deepestPending) {
      console.log(`  Deepest slice:  ${m.deepestPending}`);
    }

    // Volume stats
    console.log(`  Updates:        ${formatNum(m.totalUpdates)}${m.pendingUpdates > 0 ? ` (+${formatNum(m.pendingUpdates)} pending)` : ''}`);
    console.log(`  Events:         ${formatNum(m.totalEvents)}${m.pendingEvents > 0 ? ` (+${formatNum(m.pendingEvents)} pending)` : ''}`);
    if (m.inTransaction) {
      console.log(`  Transaction:    🔒 In-flight (data being written)`);
    }

    // ETA based on volume if cursor hasn't moved
    if (m.progress > 0.1) {
      console.log(`  ETA:            ${formatDuration(m.eta)}`);
    } else if (volumePct != null && volumePct > 0 && m.shards[0]?.started_at) {
      const startedAt = shardStartedAt(m.shards);
      if (startedAt) {
        const elapsed = Date.now() - startedAt;
        const totalEstimate = (elapsed / volumePct) * 100;
        const remaining = totalEstimate - elapsed;
        console.log(`  ETA:            ${formatDuration(remaining > 0 ? remaining : 0)}  (based on ${formatNum(expectedUpdates)} expected updates)`);
      } else {
        console.log(`  ETA:            Calculating...`);
      }
    } else {
      console.log(`  ETA:            Calculating...`);
    }

    console.log(`  Status:         ${m.status}`);

    // Rate calculation in watch mode
    if (WATCH_MODE && previousSnapshot) {
      const prevM = previousSnapshot.find(p => p.migrationId === m.migrationId);
      if (prevM && prevM.totalEvents < m.totalEvents) {
        const delta = m.totalEvents - prevM.totalEvents;
        const rate = Math.round(delta / (REFRESH_INTERVAL / 1000));
        console.log(`  Rate:           ~${formatNum(rate)} events/sec`);
      }
    }

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
  const summaries = migrations.map(aggregateMigration);

  if (JSON_MODE) {
    outputJSON(migrations);
  } else {
    outputPretty(migrations);
  }

  // Save snapshot for rate calculation in watch mode
  previousSnapshot = summaries;
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
