#!/usr/bin/env node
/**
 * ACS Snapshot Scheduler
 *
 * Runs fetch-acs.js every 3 hours starting at 00:00 UTC
 * Schedule: 0 0,3,6,9,12,15,18,21 * * *
 *
 * Usage:
 *   node acs-scheduler.js              # Normal mode
 *   node acs-scheduler.js --run-now    # Run immediately then schedule
 *   node acs-scheduler.js --local      # Force local disk mode (ignore GCS_BUCKET)
 */

import cron from 'node-cron';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SCRIPT_PATH = path.join(__dirname, 'fetch-acs.js');

// Maximum time a single snapshot is allowed to run before it is killed.
// A snapshot that hangs forever would keep isRunning=true permanently,
// causing every subsequent cron trigger to be silently skipped.
// Set to 2.5 hours — safely under the 3-hour schedule interval.
const SNAPSHOT_TIMEOUT_MS = 2.5 * 60 * 60 * 1000;

// After this many consecutive failures the scheduler exits non-zero so that
// a process supervisor (systemd, Docker restart policy, etc.) can alert.
const MAX_CONSECUTIVE_FAILURES = 3;

// Track if a job is currently running
let isRunning = false;
let lastRunTime = null;
let lastRunStatus = null;
let runCount = 0;
let consecutiveFailures = 0;

// FIX: module-level reference to the active child process.
// Previously `child` was declared inside runACSSnapshot() with `const`,
// making it unreachable from signal handlers. On SIGINT/SIGTERM the parent
// exited via process.exit() while the child continued running as an orphan.
let currentChild = null;
let snapshotTimeout = null;

function formatTime(date) {
  return date ? date.toISOString() : 'Never';
}

// FIX: moved above its call site (line ~114 in original).
// Was declared after the console.log that calls it. Function declarations are
// hoisted so it worked, but it's fragile: a `const` refactor would throw
// ReferenceError. Declaring before use is safer and clearer.
function getNextRunTime() {
  const now = new Date();
  const hours = [0, 3, 6, 9, 12, 15, 18, 21];

  for (const hour of hours) {
    const nextRun = new Date(now);
    nextRun.setUTCHours(hour, 0, 0, 0);
    if (nextRun > now) {
      return nextRun.toISOString();
    }
  }

  // Next day at 00:00 UTC
  const tomorrow = new Date(now);
  tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
  tomorrow.setUTCHours(0, 0, 0, 0);
  return tomorrow.toISOString();
}

function runACSSnapshot() {
  if (isRunning) {
    console.log(`[${new Date().toISOString()}] ⚠️ Skipping run - previous snapshot still in progress`);
    return;
  }

  isRunning = true;
  runCount++;
  const startTime = new Date();
  console.log(`\n${'='.repeat(80)}`);
  console.log(`[${startTime.toISOString()}] 🚀 Starting ACS Snapshot (Run #${runCount})`);
  console.log(`${'='.repeat(80)}\n`);

  // FIX: use process.execPath instead of 'node'.
  // spawn('node', ...) resolves 'node' from PATH, which may differ from the
  // binary that launched this scheduler when using nvm/volta/asdf. Using
  // process.execPath guarantees the child runs under the same Node.js binary.
  currentChild = spawn(process.execPath, [SCRIPT_PATH], {
    cwd: __dirname,
    stdio: 'inherit',
    env: { ...process.env },
  });

  // FIX: watchdog timeout — kill a hung child after SNAPSHOT_TIMEOUT_MS.
  // A deadlocked fetch-acs.js never emits 'close', keeping isRunning=true
  // forever and silently blocking all future cron triggers.
  snapshotTimeout = setTimeout(() => {
    console.error(
      `[${new Date().toISOString()}] ❌ Snapshot timeout after ${SNAPSHOT_TIMEOUT_MS / 60000} minutes — killing child process`
    );
    if (currentChild) currentChild.kill('SIGTERM');
  }, SNAPSHOT_TIMEOUT_MS);

  currentChild.on('error', (err) => {
    clearTimeout(snapshotTimeout);
    snapshotTimeout = null;
    console.error(`[${new Date().toISOString()}] ❌ Failed to start ACS snapshot:`, err.message);
    isRunning = false;
    currentChild = null;
    lastRunStatus = 'error';
    lastRunTime = new Date();
    consecutiveFailures++;
    checkConsecutiveFailures();
  });

  currentChild.on('close', (code) => {
    clearTimeout(snapshotTimeout);
    snapshotTimeout = null;
    const endTime = new Date();
    const duration = ((endTime - startTime) / 1000 / 60).toFixed(1);

    if (code === 0) {
      console.log(`\n[${endTime.toISOString()}] ✅ ACS Snapshot completed successfully (${duration} minutes)`);
      lastRunStatus = 'success';
      consecutiveFailures = 0;
    } else {
      console.log(`\n[${endTime.toISOString()}] ❌ ACS Snapshot failed with exit code ${code} (${duration} minutes)`);
      lastRunStatus = 'failed';
      consecutiveFailures++;
      checkConsecutiveFailures();
    }

    isRunning = false;
    currentChild = null;
    lastRunTime = endTime;
  });
}

// FIX: consecutive failure escalation.
// Previously a persistent failure (bad credentials, schema error, endpoint
// change) produced an infinite stream of hourly "status=failed" log lines
// with no escalation. Now after MAX_CONSECUTIVE_FAILURES the process exits
// with code 1 so a supervisor can restart or alert.
function checkConsecutiveFailures() {
  if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
    console.error(
      `[${new Date().toISOString()}] ❌ FATAL: ${consecutiveFailures} consecutive snapshot failures. ` +
      `Exiting so supervisor can restart/alert.`
    );
    task.stop();
    process.exit(1);
  }
}

// FIX: shared shutdown logic used by both SIGINT and SIGTERM.
// Previously:
//   - SIGINT  logged "Waiting..." then called process.exit(0) immediately —
//     the message was false, no waiting occurred.
//   - SIGTERM called process.exit(0) with no isRunning check at all.
//   - Neither handler killed the child, orphaning fetch-acs.js.
// Now: the child is killed first, then we wait for its 'close' event before
// exiting. If the child has already exited (currentChild=null), we exit
// immediately.
function shutdown(signal) {
  console.log(`\n[SHUTDOWN] Received ${signal}, stopping scheduler...`);
  task.stop();
  if (snapshotTimeout) clearTimeout(snapshotTimeout);

  if (currentChild) {
    console.log('[SHUTDOWN] Sending SIGTERM to active snapshot process...');
    currentChild.kill('SIGTERM');

    // Give the child up to 10 seconds to exit cleanly, then force-exit.
    const forceExit = setTimeout(() => {
      console.warn('[SHUTDOWN] Child did not exit in time — force killing.');
      currentChild?.kill('SIGKILL');
      process.exit(0);
    }, 10_000);

    currentChild.on('close', () => {
      clearTimeout(forceExit);
      console.log('[SHUTDOWN] Snapshot process exited. Scheduler stopped.');
      process.exit(0);
    });
  } else {
    process.exit(0);
  }
}

// Schedule: every 3 hours starting at 00:00 UTC
// Format: minute hour day-of-month month day-of-week
const SCHEDULE = '0 0,3,6,9,12,15,18,21 * * *';

console.log(`
╔════════════════════════════════════════════════════════════════════════════════╗
║                        ACS Snapshot Scheduler                                   ║
╠════════════════════════════════════════════════════════════════════════════════╣
║ Schedule: Every 3 hours (00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00)║
║ Timezone: UTC                                                                   ║
╚════════════════════════════════════════════════════════════════════════════════╝
`);

// Parse arguments for immediate run and local mode
const args = process.argv.slice(2);
const LOCAL_MODE = args.includes('--local') || args.includes('--local-disk');

// If --local flag is set, force local disk mode
if (LOCAL_MODE) {
  process.env.GCS_ENABLED = 'false';
  console.log('📂 Local disk mode enabled (--local flag)\n');
}

if (args.includes('--run-now') || args.includes('-r')) {
  console.log('🔄 Running immediate ACS snapshot before starting scheduler...\n');
  runACSSnapshot();
}

// Start the cron scheduler
// `task` must be declared before checkConsecutiveFailures() can call task.stop(),
// so it must be assigned before any snapshot could complete. Since --run-now
// fires runACSSnapshot() synchronously but spawn() is async, the cron is
// always scheduled before any 'close' event fires.
const task = cron.schedule(SCHEDULE, () => {
  runACSSnapshot();
}, {
  scheduled: true,
  timezone: 'UTC',
});

console.log(`[${new Date().toISOString()}] 📅 Scheduler started`);
console.log(`[${new Date().toISOString()}] Next run: ${getNextRunTime()}`);
console.log('\nPress Ctrl+C to stop the scheduler.\n');

// Status log every hour
setInterval(() => {
  console.log(
    `[${new Date().toISOString()}] 📊 Status: runs=${runCount}, ` +
    `lastRun=${formatTime(lastRunTime)}, status=${lastRunStatus || 'pending'}, ` +
    `running=${isRunning}, consecutiveFailures=${consecutiveFailures}`
  );
}, 60 * 60 * 1000);

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
