#!/usr/bin/env node
/**
 * Check for duplicate update_ids in GCS Parquet data, one day at a time.
 * Downloads each day's files to a temp dir, queries with DuckDB, then cleans up.
 */
import { Storage } from '@google-cloud/storage';
import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';

const storage = new Storage();
const bucket = storage.bucket(process.env.GCS_BUCKET || 'canton-bucket');
const TMP = '/tmp/dedup-day';

const prefixes = [
  'raw/updates/updates/migration=4/year=2026/month=3/',
  'raw/updates/updates/migration=4/year=2026/month=4/',
];

function cleanup() {
  if (fs.existsSync(TMP)) {
    for (const f of fs.readdirSync(TMP)) fs.unlinkSync(path.join(TMP, f));
  }
}

for (const prefix of prefixes) {
  const [files] = await bucket.getFiles({ prefix, matchGlob: '**/*.parquet' });

  // Group by day
  const byDay = {};
  for (const f of files) {
    const m = f.name.match(/month=(\d+)\/day=(\d+)/);
    if (!m) continue;
    const key = `month=${m[1]}/day=${m[2]}`;
    if (!byDay[key]) byDay[key] = [];
    byDay[key].push(f.name);
  }

  const days = Object.keys(byDay).sort();
  for (const day of days) {
    const dayFiles = byDay[day];
    cleanup();
    fs.mkdirSync(TMP, { recursive: true });

    // Download all parquet files for this day
    for (const name of dayFiles) {
      const local = path.join(TMP, path.basename(name));
      await new Promise((resolve, reject) => {
        bucket.file(name).createReadStream()
          .pipe(fs.createWriteStream(local))
          .on('finish', resolve)
          .on('error', reject);
      });
    }

    // Query with DuckDB
    try {
      const sql = `SELECT count(*) as total, count(DISTINCT update_id) as uniq FROM read_parquet('/tmp/dedup-day/*.parquet')`;
      const result = execSync(`duckdb -csv -c "${sql}"`, { encoding: 'utf8' }).trim();
      const lines = result.split('\n');
      const [total, uniq] = lines[1].split(',').map(Number);
      const dups = total - uniq;
      const flag = dups > 0 ? '  <<< DUPS FOUND' : '';
      console.log(`${day.padEnd(18)} files=${String(dayFiles.length).padEnd(4)} total=${String(total).padEnd(10)} unique=${String(uniq).padEnd(10)} dups=${dups}${flag}`);
    } catch (e) {
      console.log(`${day.padEnd(18)} ERROR: ${e.message.slice(0, 120)}`);
    }

    cleanup();
  }
}

console.log('\nDone.');
