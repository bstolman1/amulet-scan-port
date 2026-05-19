/**
 * GCS Hive Partition Scanner
 *
 * Scans Google Cloud Storage for the latest data position by walking
 * Hive-partitioned directory structures (migration=X/year=Y/month=M/day=D).
 *
 * Uses @google-cloud/storage SDK (ADC) instead of gsutil to avoid
 * interactive reauthentication in non-interactive environments (systemd,
 * tmux without active gcloud session).
 *
 * The scanner checks ALL paths that getPartitionPath() can produce:
 *   - raw/updates/updates/   (live transactions)
 *   - raw/updates/events/    (live events)
 *   - raw/backfill/updates/  (historical transactions)
 *   - raw/backfill/events/   (historical events)
 */

import { Storage } from '@google-cloud/storage';

let _storage = null;
function getStorage() {
  if (!_storage) _storage = new Storage();
  return _storage;
}

/**
 * List "subdirectories" under a GCS prefix using delimiter='/'.
 * Returns an array of prefix strings (e.g. ['gs://bucket/raw/updates/updates/migration=0/', ...]).
 */
async function listPrefixes(bucketName, prefix) {
  const bucket = getStorage().bucket(bucketName);
  const [, , apiResponse] = await bucket.getFiles({
    prefix,
    delimiter: '/',
    autoPaginate: false,
    maxResults: 1000,
  });
  return apiResponse.prefixes || [];
}

/**
 * Build the list of GCS prefixes that should be scanned.
 *
 * @param {string} bucket - GCS bucket name
 * @returns {string[]} Array of prefixes to scan (without gs:// — just bucket-relative)
 */
export function getGCSScanPrefixes(bucket) {
  if (!bucket) return [];
  const sources = ['updates', 'backfill'];
  const types   = ['updates', 'events'];
  return sources.flatMap(source =>
    types.map(type => `raw/${source}/${type}/`)
  );
}

/**
 * Parse a numeric partition value from a prefix string.
 * e.g. 'raw/updates/updates/migration=4/' → 4
 */
function extractPartitionValue(prefix, key) {
  const match = prefix.match(new RegExp(`${key}=(\\d+)`));
  return match ? parseInt(match[1]) : NaN;
}

/**
 * Find the latest data timestamp in GCS by scanning Hive partition structure.
 *
 * @param {object}   options
 * @param {string}   options.bucket  - GCS bucket name (defaults to process.env.GCS_BUCKET)
 * @param {function} [options.logFn]  - Logging function (level, msg, data) => void
 * @returns {Promise<{ migrationId: number, timestamp: string, source: string } | null>}
 */
export async function findLatestFromGCS({ bucket, logFn } = {}) {
  const gcsBucket = bucket || process.env.GCS_BUCKET;
  if (!gcsBucket) return null;

  const log = logFn || (() => {});
  const prefixes = getGCSScanPrefixes(gcsBucket);
  let best = null;

  for (const prefix of prefixes) {
    try {
      const result = await scanGCSHivePartition(gcsBucket, prefix, log);
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
 */
export async function scanGCSHivePartition(bucketName, prefix, logFn = () => {}) {
  try {
    const migPrefixes = await listPrefixes(bucketName, prefix);
    const migrations = migPrefixes
      .map(p => ({ path: p, id: extractPartitionValue(p, 'migration') }))
      .filter(m => !isNaN(m.id))
      .sort((a, b) => b.id - a.id);

    if (migrations.length === 0) return null;

    for (const mig of migrations) {
      const latest = await scanGCSDatePartitions(bucketName, mig.path, mig.id, logFn);
      if (latest) return latest;
    }
    return null;
  } catch (err) {
    logFn('warn', 'gcs_scan_hive_partition_error', {
      prefix,
      error: err.message,
    });
    return null;
  }
}

/**
 * Given a migration prefix in GCS, find the latest year/month/day partition
 * and extract a timestamp from the Parquet files in it.
 */
export async function scanGCSDatePartitions(bucketName, migPrefix, migrationId, logFn = () => {}) {
  try {
    const yearPrefixes = await listPrefixes(bucketName, migPrefix);
    const years = yearPrefixes
      .map(p => ({ path: p, val: extractPartitionValue(p, 'year') }))
      .filter(y => !isNaN(y.val))
      .sort((a, b) => b.val - a.val);

    for (const year of years) {
      const monthPrefixes = await listPrefixes(bucketName, year.path);
      const months = monthPrefixes
        .map(p => ({ path: p, val: extractPartitionValue(p, 'month') }))
        .filter(m => !isNaN(m.val))
        .sort((a, b) => b.val - a.val);

      for (const month of months) {
        const dayPrefixes = await listPrefixes(bucketName, month.path);
        const days = dayPrefixes
          .map(p => ({ path: p, val: extractPartitionValue(p, 'day') }))
          .filter(d => !isNaN(d.val))
          .sort((a, b) => b.val - a.val);

        if (days.length === 0) continue;

        const latestDay = days[0];
        const dateStr = `${year.val}-${String(month.val).padStart(2, '0')}-${String(latestDay.val).padStart(2, '0')}`;

        const timestamp = await extractTimestampFromGCSFiles(bucketName, latestDay.path, dateStr);

        return {
          migrationId,
          timestamp,
          source: `gcs:${migPrefix}year=${year.val}/month=${month.val}/day=${latestDay.val}/`,
        };
      }
    }
    return null;
  } catch (err) {
    logFn('warn', 'gcs_scan_date_partitions_error', {
      migPrefix,
      migrationId,
      error: err.message,
    });
    return null;
  }
}

/**
 * List files in a GCS day partition and extract the latest record_time
 * from Parquet filenames. Falls back to end-of-day-minus-5-minutes.
 */
export async function extractTimestampFromGCSFiles(bucketName, dayPrefix, dateStr) {
  try {
    const bucket = getStorage().bucket(bucketName);
    const [files] = await bucket.getFiles({ prefix: dayPrefix, maxResults: 100 });

    const parquetFiles = files
      .filter(f => f.name.endsWith('.parquet'))
      .map(f => f.name)
      .sort()
      .reverse();

    if (parquetFiles.length > 0) {
      const latestFile = parquetFiles[0];
      const match = latestFile.match(/(\d{4}-\d{2}-\d{2}T[\d-]+\.\d+Z)/);
      if (match) {
        return match[1].replace(/(\d{2})-(\d{2})-(\d{2})\./, '$1:$2:$3.');
      }
    }
  } catch {
    // Fall through to conservative fallback
  }

  const endOfDay = new Date(`${dateStr}T23:59:59.999Z`);
  endOfDay.setMinutes(endOfDay.getMinutes() - 5);
  return endOfDay.toISOString();
}

// Kept for backwards compatibility with any callers using these parse helpers
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
