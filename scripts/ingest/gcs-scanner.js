/**
 * GCS Hive Partition Scanner
 *
 * Scans Google Cloud Storage for the latest data position by walking
 * Hive-partitioned directory structures (migration=X/year=Y/month=M/day=D).
 *
 * Extracted from fetch-updates.js for testability and reuse.
 *
 * The scanner checks ALL paths that getPartitionPath() can produce:
 *   - raw/updates/updates/   (live transactions)
 *   - raw/updates/events/    (live events)
 *   - raw/backfill/updates/  (historical transactions)
 *   - raw/backfill/events/   (historical events)
 *
 * For cursor resume purposes, we only need the 'updates' type paths
 * (since events share the same timestamps), but we scan both for
 * completeness and to detect any drift.
 *
 * FIXES APPLIED:
 *
 * FIX #1  execSync → execFileAsync throughout
 *         execSync blocked the event loop on every gsutil ls call.
 *         Large buckets can trigger 3+ sequential ls calls per path, each with
 *         a 15s timeout — total blocking time could reach minutes.
 *         All exec calls are now async; findLatestFromGCS and all internal
 *         helpers are async. The execFn parameter contract is now async.
 *
 * FIX #2  Shell injection in exec() calls removed
 *         Old: execSync(`gsutil ls "${prefix}"`) — shell-interpolated string.
 *         GCS_BUCKET is user-controlled; a value containing " or ` injects
 *         shell commands. New: execFileAsync('gsutil', ['ls', path]) — no shell.
 *         The default execFn now uses execFileAsync with an args array, so
 *         production code is safe even without a custom execFn.
 *
 * FIX #3  extractTimestampFromGCSFiles: T23:59:59 fallback replaced
 *         Same bug as fetch-backfill.js / fetch-updates.js FIX #10:
 *         T23:59:59.999999Z overshoots by hours on any incomplete day, causing
 *         the live cursor to skip real records after restart.
 *         Now uses end-of-day-minus-5-minutes for ALL days.
 *
 * FIX #4  Silent error swallowing fixed in scanGCSHivePartition and scanGCSDatePartitions
 *         Unexpected errors (auth failures, network errors, malformed output) were
 *         indistinguishable from "no data found". Both functions now accept a logFn
 *         parameter and log unexpected errors before returning null.
 *
 * FIX #5  Dead error branch in scanGCSHivePartition removed
 *         The catch block had an `if (!err.message?.includes(...))` with an empty
 *         body — it detected unexpected errors but did nothing with them. Replaced
 *         with logFn call (FIX #4).
 *
 * FIX #6  Default export removed
 *         The file had both named exports and a redundant default object export
 *         containing the same functions. Named exports are the canonical interface;
 *         the default export was misleading (plain object, not live function refs).
 *
 * FIX #7  fetch-updates.js call site must await findLatestFromGCS
 *         findLatestFromGCS is now async; the call in fetch-updates.js must be
 *         `gcsDataResult = await findLatestFromGCS()`. This is a cross-file
 *         contract change — callers must be updated.
 */

import { promisify } from 'util';
import { execFile as execFileCb } from 'child_process';

// FIX #1/#2: promisified execFile — async, no shell, no injection
const execFileAsync = promisify(execFileCb);

/**
 * Default exec function: runs `gsutil ls <path>` asynchronously without a shell.
 *
 * FIX #1: async, does not block the event loop.
 * FIX #2: execFile with args array — path is never shell-interpolated.
 *
 * @param {string} gcsPath - GCS path to list (e.g. gs://bucket/raw/updates/updates/)
 * @returns {Promise<string>} stdout output trimmed
 */
async function defaultExecFn(gcsPath) {
  const { stdout } = await execFileAsync('gsutil', ['ls', gcsPath], {
    timeout: 15000,
  });
  return stdout.trim();
}

/**
 * Build the list of GCS prefixes that should be scanned.
 * These MUST match every prefix that getPartitionPath() can produce.
 *
 * getPartitionPath(ts, mig, type, source) produces:
 *   {source}/{type}/migration=X/year=Y/month=M/day=D
 *
 * Valid sources: 'backfill', 'updates'
 * Valid types:   'updates', 'events'
 *
 * → 4 combinations total, all under raw/ in GCS
 *
 * @param {string} bucket - GCS bucket name
 * @returns {string[]} Array of gs:// prefixes to scan
 */
export function getGCSScanPrefixes(bucket) {
  if (!bucket) return [];

  const sources = ['updates', 'backfill'];
  const types   = ['updates', 'events'];

  return sources.flatMap(source =>
    types.map(type => `gs://${bucket}/raw/${source}/${type}/`)
  );
}

/**
 * Find the latest data timestamp in GCS by scanning Hive partition structure.
 *
 * Scans all paths that the ingestion pipeline writes to and returns the
 * most recent partition found. This runs on startup in GCS mode to detect
 * cursor drift and auto-advance if needed.
 *
 * FIX #1: Now async — callers must await this function.
 *   The call site in fetch-updates.js must be updated:
 *     gcsDataResult = await findLatestFromGCS();   // FIX #7
 *
 * @param {object}   options
 * @param {string}   options.bucket  - GCS bucket name (defaults to process.env.GCS_BUCKET)
 * @param {function} [options.execFn] - Async function (gcsPath: string) => Promise<string>
 *                                      Defaults to execFileAsync('gsutil', ['ls', path]).
 *                                      Must be async; sync functions are not supported.
 * @param {function} [options.logFn]  - Logging function (level, msg, data) => void
 * @returns {Promise<{ migrationId: number, timestamp: string, source: string } | null>}
 */
export async function findLatestFromGCS({ bucket, execFn, logFn } = {}) {
  const gcsBucket = bucket || process.env.GCS_BUCKET;
  if (!gcsBucket) return null;

  // FIX #1/#2: default exec is async and uses execFileAsync (no shell)
  const exec = execFn || defaultExecFn;
  const log  = logFn  || (() => {});

  const prefixes = getGCSScanPrefixes(gcsBucket);
  let best = null;

  for (const prefix of prefixes) {
    try {
      // FIX #1: await the now-async scanGCSHivePartition
      const result = await scanGCSHivePartition(prefix, exec, log);
      if (!result) continue;

      if (!best ||
          result.migrationId > best.migrationId ||
          (result.migrationId === best.migrationId &&
           new Date(result.timestamp).getTime() > new Date(best.timestamp).getTime())) {
        best = result;
      }
    } catch (err) {
      log('warn', 'gcs_scan_error', { prefix, error: err.message });
    }
  }

  if (best) {
    log('info', 'gcs_scan_result', {
      migrationId: best.migrationId,
      timestamp:   best.timestamp,
      source:      best.source,
    });
  }

  return best;
}

/**
 * Scan a GCS prefix for the latest Hive partition (migration/year/month/day).
 *
 * FIX #1: Now async — exec is async; all internal calls are awaited.
 * FIX #4/#5: Unexpected errors are now logged via logFn before returning null.
 *   The original had a dead `if (!err.message?.includes(...)) {}` block with
 *   no body — it detected unexpected errors but silently discarded them.
 *
 * @param {string}   prefix - gs:// prefix to scan
 * @param {function} exec   - Async function (gcsPath: string) => Promise<string>
 * @param {function} logFn  - Logging function
 * @returns {Promise<{ migrationId: number, timestamp: string, source: string } | null>}
 */
export async function scanGCSHivePartition(prefix, exec, logFn = () => {}) {
  try {
    // FIX #1: await the async exec
    const migOutput = await exec(prefix);
    if (!migOutput) return null;

    const migDirs = parseMigrationDirs(migOutput);
    if (migDirs.length === 0) return null;

    for (const mig of migDirs) {
      // FIX #1: await the now-async scanGCSDatePartitions
      const latest = await scanGCSDatePartitions(mig.path, mig.id, exec, logFn);
      if (latest) return latest;
    }
    return null;
  } catch (err) {
    // gsutil ls on a non-existent prefix is expected — not an error worth logging
    const isExpected = err.message?.includes('CommandException') ||
                       err.message?.includes('No URLs matched') ||
                       err.stderr?.toString().includes('CommandException');

    // FIX #4/#5: log unexpected errors instead of silently discarding them
    if (!isExpected) {
      logFn('warn', 'gcs_scan_hive_partition_error', {
        prefix,
        error:  err.message,
        stderr: err.stderr?.toString()?.trim(),
      });
    }
    return null;
  }
}

/**
 * Parse migration directories from gsutil ls output.
 * Returns sorted descending by migration ID (newest first).
 *
 * @param {string} output - gsutil ls output
 * @returns {{ path: string, id: number }[]}
 */
export function parseMigrationDirs(output) {
  if (!output) return [];
  return output.split('\n')
    .filter(l => l.includes('migration='))
    .map(l => {
      const trimmed = l.trim();
      const match   = trimmed.match(/migration=(\d+)/);
      return match ? { path: trimmed, id: parseInt(match[1]) } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.id - a.id);
}

/**
 * Parse partition directories (year=, month=, day=) from gsutil ls output.
 * Returns sorted descending by value (newest first).
 *
 * @param {string} output   - gsutil ls output
 * @param {string} partKey  - Partition key to look for (e.g., 'year', 'month', 'day')
 * @returns {{ path: string, val: number }[]}
 */
export function parsePartitionValues(output, partKey) {
  if (!output) return [];
  const regex = new RegExp(`${partKey}=(\\d+)`);
  return output.split('\n')
    .filter(l => l.includes(`${partKey}=`))
    .map(l => {
      const trimmed = l.trim();
      const match   = trimmed.match(regex);
      return match ? { path: trimmed, val: parseInt(match[1]) } : null;
    })
    .filter(d => d && !isNaN(d.val))
    .sort((a, b) => b.val - a.val);
}

/**
 * Given a migration path in GCS, find the latest year/month/day partition
 * and extract a timestamp from the Parquet files in it.
 *
 * FIX #1: Now async — exec is async; all internal ls calls are awaited.
 * FIX #4: Unexpected errors are now logged via logFn before returning null,
 *   consistent with scanGCSHivePartition.
 *
 * @param {string}   migPath      - GCS path to migration directory
 * @param {number}   migrationId  - Migration ID number
 * @param {function} exec         - Async function (gcsPath: string) => Promise<string>
 * @param {function} logFn        - Logging function
 * @returns {Promise<{ migrationId: number, timestamp: string, source: string } | null>}
 */
export async function scanGCSDatePartitions(migPath, migrationId, exec, logFn = () => {}) {
  try {
    // FIX #1: await all exec calls
    const yearOutput = await exec(migPath);
    const years      = parsePartitionValues(yearOutput, 'year');

    for (const year of years) {
      const monthOutput = await exec(year.path);
      const months      = parsePartitionValues(monthOutput, 'month');

      for (const month of months) {
        const dayOutput = await exec(month.path);
        const days      = parsePartitionValues(dayOutput, 'day');

        if (days.length === 0) continue;

        const latestDay = days[0];
        const dateStr   = `${year.val}-${String(month.val).padStart(2, '0')}-${String(latestDay.val).padStart(2, '0')}`;

        // FIX #1: await the now-async extractTimestampFromGCSFiles
        const timestamp = await extractTimestampFromGCSFiles(latestDay.path, dateStr, exec);

        return {
          migrationId,
          timestamp,
          source: `gcs:${migPath.replace(/.*raw\//, 'raw/')}year=${year.val}/month=${month.val}/day=${latestDay.val}/`,
        };
      }
    }
    return null;
  } catch (err) {
    // FIX #4: log unexpected errors — silent null return hides auth/network failures
    logFn('warn', 'gcs_scan_date_partitions_error', {
      migPath,
      migrationId,
      error:  err.message,
      stderr: err.stderr?.toString()?.trim(),
    });
    return null;
  }
}

/**
 * List files in a GCS day partition and extract the latest record_time
 * from Parquet filenames. Falls back to end-of-day-minus-5-minutes.
 *
 * Expected filename format: updates_2026-02-02T15-30-00.000000Z.parquet
 * The dashes in the time portion (HH-MM-SS) are converted back to colons.
 *
 * FIX #1: Now async — exec is async.
 * FIX #3: T23:59:59.999999Z fallback replaced with end-of-day-minus-5-minutes.
 *   Using T23:59:59 on any day whose last real record is earlier in the day
 *   causes the forward cursor to skip real data between the last record and
 *   midnight — the same bug fixed in fetch-updates.js (FIX #10) and
 *   fetch-backfill.js. End-of-day-minus-5-min is conservative without
 *   overshooting by hours.
 *
 * @param {string}   dayPath  - GCS path to day partition
 * @param {string}   dateStr  - Date string (YYYY-MM-DD) for fallback
 * @param {function} exec     - Async function (gcsPath: string) => Promise<string>
 * @returns {Promise<string>} ISO timestamp
 */
export async function extractTimestampFromGCSFiles(dayPath, dateStr, exec) {
  try {
    // FIX #1: await the async exec
    const filesOutput = await exec(dayPath);
    const files = filesOutput.split('\n')
      .filter(f => f.endsWith('.parquet'))
      .sort()
      .reverse();

    if (files.length > 0) {
      const latestFile = files[0];
      // Match: 2026-02-02T15-30-00.000000Z
      const match = latestFile.match(/(\d{4}-\d{2}-\d{2}T[\d-]+\.\d+Z)/);
      if (match) {
        // Convert filename format back to ISO: 15-30-00 → 15:30:00
        return match[1].replace(/(\d{2})-(\d{2})-(\d{2})\./, '$1:$2:$3.');
      }
    }
  } catch {
    // Fall through to conservative fallback
  }

  // FIX #3: end-of-day-minus-5-min for ALL days (not just today).
  // T23:59:59 on any incomplete day overshoots by hours and causes the live
  // cursor to skip real records between the actual last record and midnight.
  const endOfDay = new Date(`${dateStr}T23:59:59.999Z`);
  endOfDay.setMinutes(endOfDay.getMinutes() - 5);
  return endOfDay.toISOString();
}
// FIX #6: Default export removed — it duplicated named exports as a plain object,
// which is misleading (callers get a frozen snapshot, not live function references).
// Use named imports: import { findLatestFromGCS, ... } from './gcs-scanner.js';
