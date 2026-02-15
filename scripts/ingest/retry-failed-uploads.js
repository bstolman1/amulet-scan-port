#!/usr/bin/env node
/**
 * Retry Failed GCS Uploads
 * 
 * Reads the dead-letter file (failed-uploads.jsonl) and retries each upload.
 * Successfully retried entries are removed from the dead-letter log.
 * Now includes MD5 integrity verification after each retry (matching primary upload path).
 * 
 * Usage:
 *   node retry-failed-uploads.js              # Retry all failed uploads
 *   node retry-failed-uploads.js --dry-run    # Show what would be retried
 *   node retry-failed-uploads.js --status     # Show dead-letter stats
 */

import { readFileSync, writeFileSync, existsSync, statSync, unlinkSync } from 'fs';
import { basename, dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';
import { verifyUploadIntegrity } from './gcs-upload-queue.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const DEAD_LETTER_FILE = '/tmp/ledger_raw/failed-uploads.jsonl';

const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
const STATUS_ONLY = args.includes('--status');

/**
 * Read and parse the dead-letter file.
 */
export function readDeadLetterLog(filePath = DEAD_LETTER_FILE) {
  if (!existsSync(filePath)) {
    return [];
  }
  const content = readFileSync(filePath, 'utf8').trim();
  if (!content) return [];
  
  return content.split('\n').map((line, idx) => {
    try {
      return JSON.parse(line);
    } catch {
      console.warn(`⚠️ Skipping malformed line ${idx + 1}`);
      return null;
    }
  }).filter(Boolean);
}

/**
 * Attempt to upload a file to GCS.
 */
export function retryUpload(localPath, gcsPath, timeout = 300000) {
  if (!existsSync(localPath)) {
    return { ok: false, error: 'Local file no longer exists', recoverable: false };
  }

  try {
    execSync(`gsutil -q cp "${localPath}" "${gcsPath}"`, {
      stdio: 'pipe',
      timeout,
      encoding: 'utf8',
    });
    
    // Verify integrity after upload (same as primary upload path)
    const verification = verifyUploadIntegrity(localPath, gcsPath);
    if (!verification.ok) {
      return { ok: false, error: `Integrity check failed: ${verification.error}`, recoverable: true };
    }
    
    return { ok: true, localMD5: verification.localMD5 };
  } catch (err) {
    return { ok: false, error: err.message, recoverable: true };
  }
}

/**
 * Process the dead-letter log: retry uploads, rewrite log with remaining failures.
 */
export function processDeadLetterLog(filePath = DEAD_LETTER_FILE, dryRun = false) {
  const entries = readDeadLetterLog(filePath);
  
  if (entries.length === 0) {
    console.log('✅ No failed uploads to retry.');
    return { total: 0, retried: 0, stillFailed: 0, noFile: 0 };
  }

  console.log(`📋 Found ${entries.length} failed upload(s) to retry\n`);

  const remaining = [];
  let retried = 0;
  let noFile = 0;

  for (const entry of entries) {
    const { localPath, gcsPath, error, timestamp } = entry;
    const name = basename(localPath);

    if (dryRun) {
      const exists = existsSync(localPath);
      console.log(`  ${exists ? '📂' : '❌'} ${name} → ${gcsPath} (failed: ${timestamp})`);
      if (!exists) noFile++;
      continue;
    }

    const result = retryUpload(localPath, gcsPath);

    if (result.ok) {
      retried++;
      console.log(`  ✅ ${name} → uploaded successfully`);
      // Clean up local file after successful retry
      try {
        if (existsSync(localPath)) {
          unlinkSync(localPath);
        }
      } catch { }
    } else if (!result.recoverable) {
      noFile++;
      console.log(`  ❌ ${name} → file missing, cannot retry`);
      // Don't keep in dead-letter if file is gone
    } else {
      remaining.push({ ...entry, lastRetry: new Date().toISOString(), retryError: result.error });
      console.log(`  ⚠️ ${name} → still failing: ${result.error}`);
    }
  }

  // Rewrite dead-letter with only remaining failures
  if (!dryRun) {
    if (remaining.length > 0) {
      writeFileSync(filePath, remaining.map(e => JSON.stringify(e)).join('\n') + '\n');
    } else if (existsSync(filePath)) {
      writeFileSync(filePath, '');
    }
  }

  const stats = { total: entries.length, retried, stillFailed: remaining.length, noFile };
  console.log(`\n📊 Results: ${retried} retried, ${remaining.length} still failed, ${noFile} files missing`);
  return stats;
}

// Main execution guard
const IS_MAIN = process.argv[1] && fileURLToPath(import.meta.url).endsWith(process.argv[1].replace(/\//g, '/'));

if (IS_MAIN) {
  if (STATUS_ONLY) {
    const entries = readDeadLetterLog();
    console.log(`\n📋 Dead-letter log: ${DEAD_LETTER_FILE}`);
    console.log(`   Entries: ${entries.length}`);
    const withFiles = entries.filter(e => existsSync(e.localPath)).length;
    console.log(`   Files still on disk: ${withFiles}`);
    console.log(`   Files missing: ${entries.length - withFiles}\n`);
    if (entries.length > 0) {
      console.log('Recent failures:');
      entries.slice(-5).forEach(e => {
        console.log(`  ${basename(e.localPath)} — ${e.error} (${e.timestamp})`);
      });
    }
  } else {
    processDeadLetterLog(DEAD_LETTER_FILE, DRY_RUN);
  }
}
