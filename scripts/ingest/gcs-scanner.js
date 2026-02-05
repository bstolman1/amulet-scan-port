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
 */

import { execSync } from 'child_process';

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
  const types = ['updates', 'events'];
  
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
 * @param {object} options
 * @param {string} options.bucket - GCS bucket name (defaults to process.env.GCS_BUCKET)
 * @param {function} [options.execFn] - Function to execute shell commands (for testing)
 * @param {function} [options.logFn] - Logging function (for testing)
 * @returns {{ migrationId: number, timestamp: string, source: string } | null}
 */
export function findLatestFromGCS({ bucket, execFn, logFn } = {}) {
  const gcsBucket = bucket || process.env.GCS_BUCKET;
  if (!gcsBucket) return null;

  const exec = execFn || ((cmd) => execSync(cmd, { stdio: 'pipe', timeout: 15000 }).toString().trim());
  const log = logFn || (() => {});

  const prefixes = getGCSScanPrefixes(gcsBucket);
  let best = null;

  for (const prefix of prefixes) {
    try {
      const result = scanGCSHivePartition(prefix, exec);
      if (!result) continue;

      if (!best || result.migrationId > best.migrationId ||
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
      timestamp: best.timestamp, 
      source: best.source 
    });
  }

  return best;
}

/**
 * Scan a GCS prefix for the latest Hive partition (migration/year/month/day).
 * 
 * @param {string} prefix - gs:// prefix to scan
 * @param {function} exec - Function to execute shell commands
 * @returns {{ migrationId: number, timestamp: string, source: string } | null}
 */
export function scanGCSHivePartition(prefix, exec) {
  try {
    const migOutput = exec(`gsutil ls "${prefix}"`);
    if (!migOutput) return null;

    const migDirs = parseMigrationDirs(migOutput);
    if (migDirs.length === 0) return null;

    for (const mig of migDirs) {
      const latest = scanGCSDatePartitions(mig.path, mig.id, exec);
      if (latest) return latest;
    }
    return null;
  } catch (err) {
    // gsutil ls fails if prefix doesn't exist - that's fine
    if (!err.message?.includes('CommandException') && !err.message?.includes('No URLs matched')) {
      // Unexpected error, but still non-fatal
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
      const match = l.match(/migration=(\d+)/);
      return match ? { path: l.trim(), id: parseInt(match[1]) } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.id - a.id);
}

/**
 * Parse partition directories (year=, month=, day=) from gsutil ls output.
 * Returns sorted descending by value (newest first).
 * 
 * @param {string} output - gsutil ls output
 * @param {string} partKey - Partition key to look for (e.g., 'year', 'month', 'day')
 * @returns {{ path: string, val: number }[]}
 */
export function parsePartitionValues(output, partKey) {
  if (!output) return [];
  const regex = new RegExp(`${partKey}=(\\d+)`);
  return output.split('\n')
    .filter(l => l.includes(`${partKey}=`))
    .map(l => {
      const match = l.match(regex);
      return match ? { path: l.trim(), val: parseInt(match[1]) } : null;
    })
    .filter(d => d && !isNaN(d.val))
    .sort((a, b) => b.val - a.val);
}

/**
 * Given a migration path in GCS, find the latest year/month/day partition
 * and extract a timestamp from the Parquet files in it.
 * 
 * @param {string} migPath - GCS path to migration directory
 * @param {number} migrationId - Migration ID number
 * @param {function} exec - Function to execute shell commands
 * @returns {{ migrationId: number, timestamp: string, source: string } | null}
 */
export function scanGCSDatePartitions(migPath, migrationId, exec) {
  try {
    const yearOutput = exec(`gsutil ls "${migPath}"`);
    const years = parsePartitionValues(yearOutput, 'year');

    for (const year of years) {
      const monthOutput = exec(`gsutil ls "${year.path}"`);
      const months = parsePartitionValues(monthOutput, 'month');

      for (const month of months) {
        const dayOutput = exec(`gsutil ls "${month.path}"`);
        const days = parsePartitionValues(dayOutput, 'day');

        if (days.length === 0) continue;

        const latestDay = days[0];
        const dateStr = `${year.val}-${String(month.val).padStart(2, '0')}-${String(latestDay.val).padStart(2, '0')}`;

        const timestamp = extractTimestampFromGCSFiles(latestDay.path, dateStr, exec);

        return {
          migrationId,
          timestamp,
          source: `gcs:${migPath.replace(/.*raw\//, 'raw/')}year=${year.val}/month=${month.val}/day=${latestDay.val}/`,
        };
      }
    }
    return null;
  } catch (err) {
    return null;
  }
}

/**
 * List files in a GCS day partition and extract the latest record_time
 * from Parquet filenames. Falls back to end-of-day timestamp.
 * 
 * Expected filename format: updates_2026-02-02T15-30-00.000000Z.parquet
 * The dashes in the time portion (HH-MM-SS) are converted back to colons.
 * 
 * @param {string} dayPath - GCS path to day partition
 * @param {string} dateStr - Date string (YYYY-MM-DD) for fallback
 * @param {function} exec - Function to execute shell commands
 * @returns {string} ISO timestamp
 */
export function extractTimestampFromGCSFiles(dayPath, dateStr, exec) {
  try {
    const filesOutput = exec(`gsutil ls "${dayPath}"`);
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
        const ts = match[1].replace(/(\d{2})-(\d{2})-(\d{2})\./, '$1:$2:$3.');
        return ts;
      }
    }
  } catch {
    // Fall through to default
  }

  // Conservative fallback: end of that day
  return `${dateStr}T23:59:59.999999Z`;
}

export default {
  getGCSScanPrefixes,
  findLatestFromGCS,
  scanGCSHivePartition,
  scanGCSDatePartitions,
  extractTimestampFromGCSFiles,
  parseMigrationDirs,
  parsePartitionValues,
};
