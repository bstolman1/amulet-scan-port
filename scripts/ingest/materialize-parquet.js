/**
 * Parquet Materialization
 * 
 * Converts intermediate storage formats to Parquet for SQL analytics:
 * 1. JSONL → Parquet (direct via DuckDB)
 * 2. pb.zst → Parquet (decode → DuckDB memory → Parquet)
 * 3. Compacts small Parquet files into larger ones
 * 
 * Primary storage formats:
 * - .pb.zst (Protobuf + ZSTD) for ledger updates/events (backfill)
 * - .jsonl for ACS snapshots
 * 
 * Optional: Run this script to materialize .parquet files for 
 * faster SQL queries (vs streaming from binary).
 * 
 * Usage:
 *   node materialize-parquet.js                    # JSONL only (default)
 *   node materialize-parquet.js --include-binary   # JSONL + pb.zst
 *   node materialize-parquet.js --binary-only      # pb.zst only
 *   node materialize-parquet.js --keep-originals   # Don't delete source files
 */

import { execSync } from 'child_process';
import { readdirSync, statSync, unlinkSync, existsSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { readBinaryFile } from './read-binary.js';

// Default Windows path: C:\ledger_raw\raw
const WIN_DEFAULT = 'C:\\ledger_raw\\raw';
const DATA_DIR = process.env.DATA_DIR ? join(process.env.DATA_DIR, 'raw') : WIN_DEFAULT;
const MIN_FILE_SIZE_MB = 100;  // Minimum file size before compaction (increased for larger files)
const TARGET_FILE_SIZE_MB = 500;  // Target file size after compaction (larger = faster reads)

/**
 * Find all JSON-lines files that need conversion
 */
function findJsonlFiles(dir) {
  const files = [];
  
  function walk(currentDir) {
    if (!existsSync(currentDir)) return;
    
    const entries = readdirSync(currentDir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = join(currentDir, entry.name);
      
      if (entry.isDirectory()) {
        walk(fullPath);
      } else if (entry.name.endsWith('.jsonl') || entry.name.endsWith('.jsonl.gz')) {
        files.push(fullPath);
      }
    }
  }
  
  walk(dir);
  return files;
}

/**
 * Find all binary .pb.zst files that need conversion
 */
export function findBinaryFiles(dir) {
  const files = [];
  
  function walk(currentDir) {
    if (!existsSync(currentDir)) return;
    
    const entries = readdirSync(currentDir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = join(currentDir, entry.name);
      
      if (entry.isDirectory()) {
        walk(fullPath);
      } else if (entry.name.endsWith('.pb.zst')) {
        // Check if parquet already exists
        const parquetPath = fullPath.replace('.pb.zst', '.parquet');
        if (!existsSync(parquetPath)) {
          files.push(fullPath);
        }
      }
    }
  }
  
  walk(dir);
  return files;
}

/**
 * Convert JSON-lines to parquet using DuckDB CLI
 */
export function convertJsonlToParquet(jsonlPath, options = {}) {
  const { deleteOriginal = true } = options;
  
  // Handle both .jsonl and .jsonl.gz files
  const parquetPath = jsonlPath.replace('.jsonl.gz', '.parquet').replace('.jsonl', '.parquet');
  
  try {
    // DuckDB's read_json_auto handles gzip automatically
    const sql = `
      COPY (
        SELECT * FROM read_json_auto('${jsonlPath}')
      ) TO '${parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `;
    
    execSync(`duckdb -c "${sql}"`, { stdio: 'pipe' });
    
    // Remove original jsonl file after successful conversion
    if (deleteOriginal) {
      unlinkSync(jsonlPath);
    }
    
    console.log(`✅ Converted ${jsonlPath} -> ${parquetPath}`);
    return parquetPath;
  } catch (err) {
    console.error(`❌ Failed to convert ${jsonlPath}:`, err.message);
    return null;
  }
}

/**
 * Convert .pb.zst directly to Parquet via DuckDB
 * 
 * Process:
 * 1. Decode pb.zst to records in memory
 * 2. Write records to temp JSONL (DuckDB can't read from JS arrays directly via CLI)
 * 3. Convert temp JSONL to Parquet via DuckDB
 * 4. Clean up temp files
 */
export async function convertBinaryToParquet(pbzstPath, options = {}) {
  const { deleteOriginal = true } = options;
  const parquetPath = pbzstPath.replace('.pb.zst', '.parquet');
  const tempJsonlPath = pbzstPath.replace('.pb.zst', '.temp.jsonl');
  
  try {
    // Step 1: Decode pb.zst to records
    console.log(`  📖 Decoding ${pbzstPath}...`);
    const result = await readBinaryFile(pbzstPath);
    console.log(`  📊 Found ${result.count} ${result.type} records (${result.chunksRead} chunks)`);
    
    if (result.count === 0) {
      console.log(`  ⚠️ Skipping empty file: ${pbzstPath}`);
      return null;
    }
    
    // Step 2: Write to temp JSONL
    // Note: We use temp JSONL because DuckDB CLI can't read from JS arrays directly
    // This is still faster than keeping JSONL around because we delete it immediately
    console.log(`  📝 Writing temp JSONL...`);
    const lines = result.records.map(r => JSON.stringify(r));
    writeFileSync(tempJsonlPath, lines.join('\n') + '\n');
    
    // Step 3: Convert to Parquet via DuckDB
    console.log(`  🔄 Converting to Parquet...`);
    const sql = `
      COPY (
        SELECT * FROM read_json_auto('${tempJsonlPath}')
      ) TO '${parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `;
    
    execSync(`duckdb -c "${sql}"`, { stdio: 'pipe' });
    
    // Step 4: Clean up
    unlinkSync(tempJsonlPath);
    
    if (deleteOriginal) {
      unlinkSync(pbzstPath);
    }
    
    const parquetStats = statSync(parquetPath);
    const sizeMB = (parquetStats.size / (1024 * 1024)).toFixed(1);
    
    console.log(`✅ Converted ${pbzstPath} -> ${parquetPath} (${sizeMB} MB)`);
    return parquetPath;
  } catch (err) {
    // Clean up temp file on error
    if (existsSync(tempJsonlPath)) {
      unlinkSync(tempJsonlPath);
    }
    console.error(`❌ Failed to convert ${pbzstPath}:`, err.message);
    return null;
  }
}

/**
 * Convert all pending JSON-lines files
 */
export function convertAllJsonl(options = {}) {
  const jsonlFiles = findJsonlFiles(DATA_DIR);
  console.log(`Found ${jsonlFiles.length} JSON-lines files to convert`);
  
  const results = [];
  for (const file of jsonlFiles) {
    const result = convertJsonlToParquet(file, options);
    if (result) results.push(result);
  }
  
  return results;
}

/**
 * Convert all pending binary files
 */
export async function convertAllBinary(options = {}) {
  const binaryFiles = findBinaryFiles(DATA_DIR);
  console.log(`Found ${binaryFiles.length} binary (.pb.zst) files to convert`);
  
  const results = [];
  for (const file of binaryFiles) {
    const result = await convertBinaryToParquet(file, options);
    if (result) results.push(result);
  }
  
  return results;
}

/**
 * Find small parquet files that should be compacted
 */
function findSmallParquetFiles(dir, prefix) {
  const files = [];
  
  function walk(currentDir) {
    if (!existsSync(currentDir)) return;
    
    const entries = readdirSync(currentDir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = join(currentDir, entry.name);
      
      if (entry.isDirectory()) {
        walk(fullPath);
      } else if (entry.name.startsWith(prefix) && entry.name.endsWith('.parquet')) {
        const stats = statSync(fullPath);
        const sizeMB = stats.size / (1024 * 1024);
        
        if (sizeMB < MIN_FILE_SIZE_MB) {
          files.push({ path: fullPath, size: sizeMB });
        }
      }
    }
  }
  
  walk(dir);
  return files;
}

/**
 * Compact multiple small parquet files into one
 */
export function compactParquetFiles(inputFiles, outputPath) {
  if (inputFiles.length < 2) return null;
  
  try {
    const fileList = inputFiles.map(f => `'${f.path}'`).join(', ');
    
    const sql = `
      COPY (
        SELECT * FROM read_parquet([${fileList}])
      ) TO '${outputPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `;
    
    execSync(`duckdb -c "${sql}"`, { stdio: 'pipe' });
    
    // Remove original files after successful compaction
    for (const file of inputFiles) {
      unlinkSync(file.path);
    }
    
    console.log(`✅ Compacted ${inputFiles.length} files -> ${outputPath}`);
    return outputPath;
  } catch (err) {
    console.error(`❌ Failed to compact files:`, err.message);
    return null;
  }
}

/**
 * Run full materialization: convert jsonl + pb.zst + compact small files
 */
export async function runRotation(options = {}) {
  const { includeBinary = false, binaryOnly = false, keepOriginals = false } = options;
  
  console.log('🔄 Starting Parquet materialization...\n');
  console.log(`📁 Data directory: ${DATA_DIR}`);
  console.log(`   Options: includeBinary=${includeBinary}, binaryOnly=${binaryOnly}, keepOriginals=${keepOriginals}\n`);
  
  const conversionOptions = { deleteOriginal: !keepOriginals };
  
  // Step 0: Optionally convert pb.zst files
  if (includeBinary || binaryOnly) {
    console.log('📦 Converting binary (.pb.zst) files to Parquet...');
    const converted = await convertAllBinary(conversionOptions);
    console.log(`Converted ${converted.length} binary files\n`);
  }
  
  if (!binaryOnly) {
    // Step 1: Convert JSON-lines to parquet
    console.log('📝 Converting JSON-lines files...');
    const converted = convertAllJsonl(conversionOptions);
    console.log(`Converted ${converted.length} JSONL files\n`);
    
    // Step 2: Find and compact small files
    console.log('📦 Looking for small Parquet files to compact...');
    
    for (const prefix of ['updates', 'events']) {
      const smallFiles = findSmallParquetFiles(DATA_DIR, prefix);
      
      if (smallFiles.length >= 2) {
        // Group by partition directory
        const byDir = {};
        for (const file of smallFiles) {
          const dir = dirname(file.path);
          if (!byDir[dir]) byDir[dir] = [];
          byDir[dir].push(file);
        }
        
        for (const [dir, files] of Object.entries(byDir)) {
          if (files.length >= 2) {
            const outputPath = join(dir, `${prefix}-compacted-${Date.now()}.parquet`);
            compactParquetFiles(files, outputPath);
          }
        }
      }
    }
  }
  
  console.log('\n✅ Materialization complete');
}

// Parse CLI arguments
function parseArgs() {
  const args = process.argv.slice(2);
  return {
    includeBinary: args.includes('--include-binary'),
    binaryOnly: args.includes('--binary-only'),
    keepOriginals: args.includes('--keep-originals'),
    help: args.includes('--help') || args.includes('-h'),
  };
}

function printHelp() {
  console.log(`
Parquet Materialization - Convert storage formats to Parquet

Usage:
  node materialize-parquet.js [options]

Options:
  --include-binary   Include .pb.zst files in conversion (in addition to JSONL)
  --binary-only      Convert only .pb.zst files (skip JSONL)
  --keep-originals   Don't delete source files after conversion
  --help, -h         Show this help message

Examples:
  # Convert only JSONL files (default)
  node materialize-parquet.js

  # Convert both JSONL and pb.zst files
  node materialize-parquet.js --include-binary

  # Convert only pb.zst files, keep originals
  node materialize-parquet.js --binary-only --keep-originals
`);
}

// Run if called directly
if (process.argv[1]?.includes('materialize-parquet')) {
  const args = parseArgs();
  
  if (args.help) {
    printHelp();
    process.exit(0);
  }
  
  runRotation(args).catch(console.error);
}

export default {
  convertJsonlToParquet,
  convertBinaryToParquet,
  convertAllJsonl,
  convertAllBinary,
  compactParquetFiles,
  runRotation,
  findBinaryFiles,
  findJsonlFiles,
};
