#!/usr/bin/env node
/**
 * Cleanup Corrupt GCS Partitions
 * 
 * Finds and removes partitions with invalid day values (day>31, day==X double-equals, etc.)
 * 
 * Usage:
 *   node cleanup-corrupt-partitions.js --dry-run    # Preview what would be deleted
 *   node cleanup-corrupt-partitions.js --delete      # Actually delete corrupt partitions
 */

import { execSync } from 'child_process';

const GCS_BUCKET = process.env.GCS_BUCKET || 'canton-bucket';
const DRY_RUN = !process.argv.includes('--delete');
const PREFIXES = [
  `gs://${GCS_BUCKET}/raw/backfill/events/`,
  `gs://${GCS_BUCKET}/raw/backfill/updates/`,
  `gs://${GCS_BUCKET}/raw/updates/events/`,
  `gs://${GCS_BUCKET}/raw/updates/updates/`,
];

console.log(`🔍 Scanning for corrupt partitions in gs://${GCS_BUCKET}/raw/`);
console.log(`   Mode: ${DRY_RUN ? 'DRY RUN (use --delete to remove)' : '⚠️  DELETE MODE'}\n`);

let totalCorrupt = 0;
let totalBytes = 0;

for (const prefix of PREFIXES) {
  console.log(`\n📂 Scanning ${prefix}...`);
  
  let listing;
  try {
    listing = execSync(`gsutil ls -r "${prefix}" 2>/dev/null`, {
      encoding: 'utf-8',
      timeout: 120000,
      maxBuffer: 50 * 1024 * 1024,
    });
  } catch (err) {
    if (err.status === 1) {
      console.log(`   (no data found)`);
      continue;
    }
    throw err;
  }
  
  const lines = listing.split('\n').filter(Boolean);
  const corruptPaths = new Set();
  
  for (const line of lines) {
    // Check for invalid day values
    const dayMatch = line.match(/day=([^/]+)/);
    if (!dayMatch) continue;
    
    const dayVal = dayMatch[1];
    const dayNum = parseInt(dayVal, 10);
    
    let corrupt = false;
    let reason = '';
    
    // Double equals (day==30)
    if (dayVal.startsWith('=')) {
      corrupt = true;
      reason = `double-equals: day=${dayVal}`;
    }
    // Day > 31 or < 1
    else if (isNaN(dayNum) || dayNum < 1 || dayNum > 31) {
      corrupt = true;
      reason = `invalid day value: ${dayVal}`;
    }
    
    if (corrupt) {
      // Extract the partition directory (up to and including day=X/)
      const partMatch = line.match(/(.*day=[^/]+\/)/);
      if (partMatch) {
        corruptPaths.add(partMatch[1]);
      }
    }
  }
  
  if (corruptPaths.size === 0) {
    console.log(`   ✅ No corrupt partitions found`);
    continue;
  }
  
  console.log(`   ❌ Found ${corruptPaths.size} corrupt partition(s):`);
  
  for (const corruptPath of corruptPaths) {
    // Count files and size
    let fileCount = 0;
    let bytes = 0;
    try {
      const duOutput = execSync(`gsutil du -s "${corruptPath}" 2>/dev/null`, {
        encoding: 'utf-8',
        timeout: 30000,
      }).trim();
      const parts = duOutput.split(/\s+/);
      bytes = parseInt(parts[0]) || 0;
      
      const lsOutput = execSync(`gsutil ls "${corruptPath}**" 2>/dev/null`, {
        encoding: 'utf-8',
        timeout: 30000,
      });
      fileCount = lsOutput.split('\n').filter(l => l.endsWith('.parquet')).length;
    } catch {}
    
    totalCorrupt++;
    totalBytes += bytes;
    
    const sizeStr = bytes > 1e9 ? `${(bytes/1e9).toFixed(2)} GB` 
                  : bytes > 1e6 ? `${(bytes/1e6).toFixed(1)} MB`
                  : `${(bytes/1e3).toFixed(1)} KB`;
    
    console.log(`      ${corruptPath} (${fileCount} files, ${sizeStr})`);
    
    if (!DRY_RUN) {
      try {
        console.log(`      🗑️  Deleting...`);
        execSync(`gsutil -m rm -r "${corruptPath}" 2>/dev/null`, {
          encoding: 'utf-8',
          timeout: 120000,
        });
        console.log(`      ✅ Deleted`);
      } catch (err) {
        console.error(`      ❌ Delete failed: ${err.message}`);
      }
    }
  }
}

const totalSizeStr = totalBytes > 1e9 ? `${(totalBytes/1e9).toFixed(2)} GB`
                   : totalBytes > 1e6 ? `${(totalBytes/1e6).toFixed(1)} MB`
                   : `${(totalBytes/1e3).toFixed(1)} KB`;

console.log(`\n${'='.repeat(60)}`);
console.log(`Total corrupt partitions: ${totalCorrupt}`);
console.log(`Total size: ${totalSizeStr}`);
if (DRY_RUN && totalCorrupt > 0) {
  console.log(`\n⚠️  Run with --delete to remove these partitions`);
}
