#!/usr/bin/env node
/**
 * ACS Snapshot Scheduler
 * 
 * Runs fetch-acs-parquet.js every 3 hours starting at 00:00 UTC
 * Schedule: 0 0,3,6,9,12,15,18,21 * * *
 * 
 * Usage: node acs-scheduler.js
 */

import cron from 'node-cron';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SCRIPT_PATH = path.join(__dirname, 'fetch-acs-parquet.js');

// Track if a job is currently running
let isRunning = false;
let lastRunTime = null;
let lastRunStatus = null;
let runCount = 0;

function formatTime(date) {
  return date ? date.toISOString() : 'Never';
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

  const child = spawn('node', [SCRIPT_PATH], {
    cwd: __dirname,
    stdio: 'inherit',
    env: { ...process.env }
  });

  child.on('error', (err) => {
    console.error(`[${new Date().toISOString()}] ❌ Failed to start ACS snapshot:`, err.message);
    isRunning = false;
    lastRunStatus = 'error';
    lastRunTime = new Date();
  });

  child.on('close', (code) => {
    const endTime = new Date();
    const duration = ((endTime - startTime) / 1000 / 60).toFixed(1);
    
    if (code === 0) {
      console.log(`\n[${endTime.toISOString()}] ✅ ACS Snapshot completed successfully (${duration} minutes)`);
      lastRunStatus = 'success';
    } else {
      console.log(`\n[${endTime.toISOString()}] ❌ ACS Snapshot failed with exit code ${code} (${duration} minutes)`);
      lastRunStatus = 'failed';
    }
    
    isRunning = false;
    lastRunTime = endTime;
  });
}

// Schedule: every 3 hours starting at 00:00 UTC
// Format: minute hour day-of-month month day-of-week
const SCHEDULE = '0 0,3,6,9,12,15,18,21 * * *';

console.log(`
╔════════════════════════════════════════════════════════════════════════════════╗
║                        ACS Snapshot Scheduler                                   ║
╠════════════════════════════════════════════════════════════════════════════════╣
║ Schedule: Every 3 hours (00:00, 03:00, 06:00, 09:00, 12:15, 15:00, 18:00, 21:00)║
║ Timezone: UTC                                                                   ║
╚════════════════════════════════════════════════════════════════════════════════╝
`);

// Parse arguments for immediate run
const args = process.argv.slice(2);
if (args.includes('--run-now') || args.includes('-r')) {
  console.log('🔄 Running immediate ACS snapshot before starting scheduler...\n');
  runACSSnapshot();
}

// Start the cron scheduler
const task = cron.schedule(SCHEDULE, () => {
  runACSSnapshot();
}, {
  scheduled: true,
  timezone: 'UTC'
});

console.log(`[${new Date().toISOString()}] 📅 Scheduler started`);
console.log(`[${new Date().toISOString()}] Next run: ${getNextRunTime()}`);
console.log('\nPress Ctrl+C to stop the scheduler.\n');

// Status endpoint via simple interval
setInterval(() => {
  console.log(`[${new Date().toISOString()}] 📊 Status: runs=${runCount}, lastRun=${formatTime(lastRunTime)}, status=${lastRunStatus || 'pending'}, running=${isRunning}`);
}, 60 * 60 * 1000); // Log status every hour

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

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[SHUTDOWN] Stopping scheduler...');
  task.stop();
  if (isRunning) {
    console.log('[SHUTDOWN] Waiting for current snapshot to complete...');
  }
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n[SHUTDOWN] Received SIGTERM, stopping scheduler...');
  task.stop();
  process.exit(0);
});
