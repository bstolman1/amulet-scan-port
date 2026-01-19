#!/usr/bin/env node
/**
 * Unified Canton Ledger Ingestion Script
 * 
 * Single entry point that runs backfill first, then automatically
 * transitions to live updates once all historical data is ingested.
 * 
 * Usage:
 *   node ingest-all.js              # Backfill → Live Updates (default)
 *   node ingest-all.js --live-only  # Skip backfill, start live updates immediately
 *   node ingest-all.js --backfill-only  # Run backfill only, don't start live updates
 *   node ingest-all.js --keep-raw   # Also write to .pb.zst files (passed to both scripts)
 * 
 * Environment variables:
 *   GCS_ENABLED=true/false   - Enable/disable GCS uploads
 *   GCS_BUCKET=bucket-name   - GCS bucket name (required)
 *   SCAN_URL=...             - Canton Scan API URL
 *   BATCH_SIZE=1000          - API batch size (max 1000)
 */

import { spawn } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Parse command line arguments
const args = process.argv.slice(2);
const LIVE_ONLY = args.includes('--live-only') || args.includes('-l');
const BACKFILL_ONLY = args.includes('--backfill-only') || args.includes('-b');
const KEEP_RAW = args.includes('--keep-raw') || args.includes('--raw');

// Pass-through args for child scripts
const passArgs = [];
if (KEEP_RAW) passArgs.push('--keep-raw');

/**
 * Run a script and wait for completion
 */
function runScript(scriptPath, scriptArgs = []) {
  return new Promise((resolve, reject) => {
    console.log(`\n${"═".repeat(80)}`);
    console.log(`🚀 Starting: ${scriptPath}`);
    console.log(`   Args: ${scriptArgs.join(' ') || '(none)'}`);
    console.log(`${"═".repeat(80)}\n`);

    const child = spawn('node', [scriptPath, ...scriptArgs], {
      stdio: 'inherit',
      cwd: __dirname,
      env: process.env
    });

    child.on('error', (err) => {
      reject(new Error(`Failed to start ${scriptPath}: ${err.message}`));
    });

    child.on('exit', (code) => {
      if (code === 0) {
        resolve({ success: true, code });
      } else {
        reject(new Error(`${scriptPath} exited with code ${code}`));
      }
    });
  });
}

/**
 * Main orchestration function
 */
async function main() {
  const startTime = Date.now();
  
  console.log("\n" + "═".repeat(80));
  console.log("📦 CANTON LEDGER UNIFIED INGESTION");
  console.log("═".repeat(80));
  console.log(`Mode: ${LIVE_ONLY ? 'LIVE ONLY' : BACKFILL_ONLY ? 'BACKFILL ONLY' : 'BACKFILL → LIVE'}`);
  console.log(`GCS Enabled: ${process.env.GCS_ENABLED || 'not set'}`);
  console.log(`GCS Bucket: ${process.env.GCS_BUCKET || 'not set'}`);
  console.log(`Keep Raw: ${KEEP_RAW}`);
  console.log("═".repeat(80));

  try {
    // Step 1: Run backfill (unless --live-only)
    if (!LIVE_ONLY) {
      console.log("\n📥 PHASE 1: BACKFILL");
      console.log("   Fetching all historical ledger data...\n");
      
      const backfillScript = join(__dirname, 'fetch-backfill.js');
      
      // The backfill script will automatically start live updates when complete
      // if --backfill-only is not set. But we handle it explicitly here for clarity.
      if (BACKFILL_ONLY) {
        // Run backfill and exit
        await runScript(backfillScript, passArgs);
        
        const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        console.log(`\n${"═".repeat(80)}`);
        console.log(`✅ BACKFILL COMPLETE (${duration} minutes)`);
        console.log(`   Live updates NOT started (--backfill-only mode)`);
        console.log(`${"═".repeat(80)}\n`);
        return;
      }
      
      // Run backfill - it will check completion and transition to live updates
      await runScript(backfillScript, passArgs);
      
      // If we get here, backfill exited normally (live updates may have been spawned)
    } else {
      // Step 2: Live updates only mode
      console.log("\n📡 LIVE UPDATES ONLY MODE");
      console.log("   Skipping backfill, starting live updates immediately...\n");
      
      const liveScript = join(__dirname, 'fetch-updates.js');
      await runScript(liveScript, ['--live', ...passArgs]);
    }

    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`\n${"═".repeat(80)}`);
    console.log(`✅ INGESTION COMPLETE (${duration} minutes)`);
    console.log(`${"═".repeat(80)}\n`);

  } catch (err) {
    console.error(`\n❌ INGESTION FAILED: ${err.message}`);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\n🛑 Received SIGINT - shutting down...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n\n🛑 Received SIGTERM - shutting down...');
  process.exit(0);
});

// Run
main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
