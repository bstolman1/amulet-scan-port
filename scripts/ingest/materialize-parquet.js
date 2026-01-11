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
import { join, dirname, basename } from 'path';
import { readBinaryFile } from './read-binary.js';

// Default Windows path: C:\ledger_raw\raw
const WIN_DEFAULT = 'C:\\ledger_raw\\raw';
const DATA_DIR = process.env.DATA_DIR ? join(process.env.DATA_DIR, 'raw') : WIN_DEFAULT;
const MIN_FILE_SIZE_MB = 100;  // Minimum file size before compaction (increased for larger files)
const TARGET_FILE_SIZE_MB = 500;  // Target file size after compaction (larger = faster reads)

// ============================================================================
// Progress Tracking
// ============================================================================

/**
 * Progress tracker for batch operations
 */
class ProgressTracker {
  constructor(totalFiles, label = 'Processing') {
    this.totalFiles = totalFiles;
    this.processedFiles = 0;
    this.startTime = Date.now();
    this.fileTimes = [];
    this.label = label;
    this.lastPrintTime = 0;
  }

  /**
   * Record completion of a file and print progress
   */
  tick(fileName) {
    const now = Date.now();
    this.processedFiles++;
    
    // Track time for this file (use time since last tick or start)
    const lastTime = this.fileTimes.length > 0 
      ? this.fileTimes[this.fileTimes.length - 1].endTime 
      : this.startTime;
    this.fileTimes.push({ 
      name: fileName, 
      duration: now - lastTime,
      endTime: now 
    });
    
    // Print progress (throttle to every 500ms minimum, or always for last file)
    if (now - this.lastPrintTime > 500 || this.processedFiles === this.totalFiles) {
      this.printProgress();
      this.lastPrintTime = now;
    }
  }

  /**
   * Calculate average time per file (using recent files for better accuracy)
   */
  getAverageTimeMs() {
    if (this.fileTimes.length === 0) return 0;
    
    // Use last 10 files for rolling average (more accurate for varying file sizes)
    const recentFiles = this.fileTimes.slice(-10);
    const totalTime = recentFiles.reduce((sum, f) => sum + f.duration, 0);
    return totalTime / recentFiles.length;
  }

  /**
   * Get estimated time remaining
   */
  getEtaMs() {
    const remaining = this.totalFiles - this.processedFiles;
    if (remaining <= 0) return 0;
    
    const avgTime = this.getAverageTimeMs();
    return remaining * avgTime;
  }

  /**
   * Format milliseconds as human-readable duration
   */
  formatDuration(ms) {
    if (ms < 1000) return `${Math.round(ms)}ms`;
    
    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) return `${seconds}s`;
    
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    if (minutes < 60) return `${minutes}m ${remainingSeconds}s`;
    
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    return `${hours}h ${remainingMinutes}m`;
  }

  /**
   * Print current progress with ETA
   */
  printProgress() {
    const percent = ((this.processedFiles / this.totalFiles) * 100).toFixed(1);
    const elapsed = Date.now() - this.startTime;
    const eta = this.getEtaMs();
    
    const progressBar = this.getProgressBar(20);
    const etaStr = eta > 0 ? ` | ETA: ${this.formatDuration(eta)}` : '';
    const elapsedStr = this.formatDuration(elapsed);
    
    // Use carriage return to overwrite line (cleaner output)
    process.stdout.write(
      `\r${this.label}: ${progressBar} ${this.processedFiles}/${this.totalFiles} (${percent}%) | Elapsed: ${elapsedStr}${etaStr}   `
    );
    
    // Newline when done
    if (this.processedFiles === this.totalFiles) {
      console.log();
    }
  }

  /**
   * Generate ASCII progress bar
   */
  getProgressBar(width = 20) {
    const filled = Math.round((this.processedFiles / this.totalFiles) * width);
    const empty = width - filled;
    return `[${'█'.repeat(filled)}${'░'.repeat(empty)}]`;
  }

  /**
   * Print final summary
   */
  printSummary() {
    const totalTime = Date.now() - this.startTime;
    const avgTime = this.getAverageTimeMs();
    
    console.log(`\n📊 ${this.label} Summary:`);
    console.log(`   Files processed: ${this.processedFiles}/${this.totalFiles}`);
    console.log(`   Total time: ${this.formatDuration(totalTime)}`);
    if (this.processedFiles > 0) {
      console.log(`   Average per file: ${this.formatDuration(avgTime)}`);
    }
  }
}

// ============================================================================
// File Discovery
// ============================================================================

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

// ============================================================================
// Conversion Functions
// ============================================================================

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
    
    return parquetPath;
  } catch (err) {
    console.error(`\n❌ Failed to convert ${jsonlPath}:`, err.message);
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
  const { deleteOriginal = true, verbose = false } = options;
  const parquetPath = pbzstPath.replace('.pb.zst', '.parquet');
  const tempJsonlPath = pbzstPath.replace('.pb.zst', '.temp.jsonl');
  
  try {
    // Step 1: Decode pb.zst to records
    if (verbose) console.log(`\n  📖 Decoding ${basename(pbzstPath)}...`);
    const result = await readBinaryFile(pbzstPath);
    if (verbose) console.log(`  📊 Found ${result.count} ${result.type} records (${result.chunksRead} chunks)`);
    
    if (result.count === 0) {
      if (verbose) console.log(`  ⚠️ Skipping empty file: ${pbzstPath}`);
      return null;
    }
    
    // Step 2: Write to temp JSONL
    if (verbose) console.log(`  📝 Writing temp JSONL...`);
    const lines = result.records.map(r => JSON.stringify(r));
    writeFileSync(tempJsonlPath, lines.join('\n') + '\n');
    
    // Step 3: Convert to Parquet via DuckDB
    if (verbose) console.log(`  🔄 Converting to Parquet...`);
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
    
    return parquetPath;
  } catch (err) {
    // Clean up temp file on error
    if (existsSync(tempJsonlPath)) {
      unlinkSync(tempJsonlPath);
    }
    console.error(`\n❌ Failed to convert ${pbzstPath}:`, err.message);
    return null;
  }
}

/**
 * Convert all pending JSON-lines files with progress tracking
 */
export function convertAllJsonl(options = {}) {
  const jsonlFiles = findJsonlFiles(DATA_DIR);
  
  if (jsonlFiles.length === 0) {
    console.log('No JSON-lines files to convert');
    return [];
  }
  
  console.log(`Found ${jsonlFiles.length} JSON-lines files to convert\n`);
  
  const progress = new ProgressTracker(jsonlFiles.length, 'JSONL → Parquet');
  const results = [];
  
  for (const file of jsonlFiles) {
    const result = convertJsonlToParquet(file, options);
    if (result) results.push(result);
    progress.tick(basename(file));
  }
  
  progress.printSummary();
  return results;
}

/**
 * Convert all pending binary files with progress tracking
 */
export async function convertAllBinary(options = {}) {
  const binaryFiles = findBinaryFiles(DATA_DIR);
  
  if (binaryFiles.length === 0) {
    console.log('No binary (.pb.zst) files to convert');
    return [];
  }
  
  console.log(`Found ${binaryFiles.length} binary (.pb.zst) files to convert\n`);
  
  const progress = new ProgressTracker(binaryFiles.length, 'pb.zst → Parquet');
  const results = [];
  
  for (const file of binaryFiles) {
    const result = await convertBinaryToParquet(file, { ...options, verbose: false });
    if (result) results.push(result);
    progress.tick(basename(file));
  }
  
  progress.printSummary();
  return results;
}

// ============================================================================
// Compaction
// ============================================================================

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
    
    console.log(`✅ Compacted ${inputFiles.length} files -> ${basename(outputPath)}`);
    return outputPath;
  } catch (err) {
    console.error(`❌ Failed to compact files:`, err.message);
    return null;
  }
}

// ============================================================================
// Main Entry Point
// ============================================================================

/**
 * Run full materialization: convert jsonl + pb.zst + compact small files
 */
export async function runRotation(options = {}) {
  const { includeBinary = false, binaryOnly = false, keepOriginals = false } = options;
  
  const overallStart = Date.now();
  
  console.log('🔄 Starting Parquet materialization...\n');
  console.log(`📁 Data directory: ${DATA_DIR}`);
  console.log(`   Options: includeBinary=${includeBinary}, binaryOnly=${binaryOnly}, keepOriginals=${keepOriginals}\n`);
  
  const conversionOptions = { deleteOriginal: !keepOriginals };
  
  // Step 0: Optionally convert pb.zst files
  if (includeBinary || binaryOnly) {
    console.log('━'.repeat(60));
    console.log('📦 Phase 1: Converting binary (.pb.zst) files to Parquet...');
    console.log('━'.repeat(60));
    const converted = await convertAllBinary(conversionOptions);
    console.log(`\nConverted ${converted.length} binary files\n`);
  }
  
  if (!binaryOnly) {
    // Step 1: Convert JSON-lines to parquet
    console.log('━'.repeat(60));
    console.log('📝 Phase 2: Converting JSON-lines files to Parquet...');
    console.log('━'.repeat(60));
    const converted = convertAllJsonl(conversionOptions);
    console.log(`\nConverted ${converted.length} JSONL files\n`);
    
    // Step 2: Find and compact small files
    console.log('━'.repeat(60));
    console.log('📦 Phase 3: Compacting small Parquet files...');
    console.log('━'.repeat(60));
    
    let totalCompacted = 0;
    
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
            const result = compactParquetFiles(files, outputPath);
            if (result) totalCompacted += files.length;
          }
        }
      }
    }
    
    if (totalCompacted === 0) {
      console.log('No small files to compact');
    }
  }
  
  // Final summary
  const totalTime = Date.now() - overallStart;
  console.log('\n' + '═'.repeat(60));
  console.log(`✅ Materialization complete in ${formatDuration(totalTime)}`);
  console.log('═'.repeat(60));
}

/**
 * Format milliseconds as human-readable duration
 */
function formatDuration(ms) {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  if (minutes < 60) return `${minutes}m ${remainingSeconds}s`;
  
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
}

// ============================================================================
// CLI
// ============================================================================

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
  ProgressTracker,
};
