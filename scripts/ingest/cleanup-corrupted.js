/**
 * Scan for and remove corrupted JSONL files
 * Run with: node scripts/ingest/cleanup-corrupted.js
 */

import { readFileSync, unlinkSync, readdirSync, statSync, createReadStream } from 'fs';
import { createGunzip } from 'zlib';
import { createInterface } from 'readline';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
// Default WSL path: /home/bstolz/canton-explorer/data/raw
const WSL_DEFAULT = '/home/bstolz/canton-explorer/data/raw';
const DATA_DIR = process.env.DATA_DIR ? join(process.env.DATA_DIR, 'raw') : WSL_DEFAULT;

const DRY_RUN = process.argv.includes('--dry-run');
const VERBOSE = process.argv.includes('--verbose');

let scanned = 0;
let corrupted = 0;
let deleted = 0;
let totalBytes = 0;

/**
 * Check if a JSONL file is valid by parsing each line
 */
async function isValidJsonl(filePath) {
  return new Promise((resolve) => {
    try {
      const isGzip = filePath.endsWith('.gz');
      let stream = createReadStream(filePath);
      
      if (isGzip) {
        const gunzip = createGunzip();
        stream = stream.pipe(gunzip);
        gunzip.on('error', () => resolve(false));
      }
      
      const rl = createInterface({ input: stream, crlfDelay: Infinity });
      let lineCount = 0;
      let hasError = false;
      
      rl.on('line', (line) => {
        if (hasError) return;
        lineCount++;
        if (line.trim()) {
          try {
            JSON.parse(line);
          } catch {
            hasError = true;
            if (VERBOSE) console.log(`  ❌ Invalid JSON at line ${lineCount}`);
            rl.close();
          }
        }
      });
      
      rl.on('close', () => {
        if (hasError || lineCount === 0) {
          resolve(false);
        } else {
          resolve(true);
        }
      });
      
      rl.on('error', () => resolve(false));
      stream.on('error', () => resolve(false));
      
    } catch {
      resolve(false);
    }
  });
}

/**
 * Recursively find all JSONL files
 */
function findJsonlFiles(dir, files = []) {
  try {
    const entries = readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = join(dir, entry.name);
      if (entry.isDirectory()) {
        findJsonlFiles(fullPath, files);
      } else if (entry.name.endsWith('.jsonl') || entry.name.endsWith('.jsonl.gz')) {
        files.push(fullPath);
      }
    }
  } catch (err) {
    console.error(`Error reading ${dir}:`, err.message);
  }
  return files;
}

async function main() {
  console.log('🔍 Scanning for corrupted JSONL files...');
  console.log(`   Directory: ${DATA_DIR}`);
  if (DRY_RUN) console.log('   Mode: DRY RUN (no files will be deleted)');
  console.log('');
  
  const files = findJsonlFiles(DATA_DIR);
  console.log(`   Found ${files.length} JSONL files\n`);
  
  for (const file of files) {
    scanned++;
    const stat = statSync(file);
    
    // Quick check: empty or tiny files are likely corrupted
    if (stat.size < 10) {
      console.log(`⚠️ Empty/tiny file: ${file} (${stat.size} bytes)`);
      corrupted++;
      totalBytes += stat.size;
      if (!DRY_RUN) {
        unlinkSync(file);
        deleted++;
      }
      continue;
    }
    
    // Full validation
    const isValid = await isValidJsonl(file);
    
    if (!isValid) {
      console.log(`❌ Corrupted: ${file} (${(stat.size / 1024).toFixed(1)} KB)`);
      corrupted++;
      totalBytes += stat.size;
      if (!DRY_RUN) {
        try {
          unlinkSync(file);
          deleted++;
        } catch (err) {
          console.error(`   Failed to delete: ${err.message}`);
        }
      }
    } else if (VERBOSE) {
      console.log(`✓ Valid: ${file}`);
    }
    
    // Progress indicator
    if (scanned % 100 === 0) {
      process.stdout.write(`   Scanned ${scanned}/${files.length} files...\r`);
    }
  }
  
  console.log('\n');
  console.log('📊 Summary:');
  console.log(`   Scanned: ${scanned} files`);
  console.log(`   Corrupted: ${corrupted} files (${(totalBytes / 1024 / 1024).toFixed(2)} MB)`);
  if (DRY_RUN) {
    console.log(`   Would delete: ${corrupted} files`);
    console.log('\n   Run without --dry-run to delete corrupted files');
  } else {
    console.log(`   Deleted: ${deleted} files`);
  }
}

main().catch(console.error);
