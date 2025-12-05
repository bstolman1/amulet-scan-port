/**
 * Parquet File Rotation and Compaction
 * 
 * This script handles:
 * 1. Converting JSON-lines files to parquet
 * 2. Compacting small parquet files into larger ones
 * 3. Cleaning up old temporary files
 */

import { execSync } from 'child_process';
import { readdirSync, statSync, unlinkSync, existsSync } from 'fs';
import { join } from 'path';

const DATA_DIR = process.env.DATA_DIR || './data/raw';
const MIN_FILE_SIZE_MB = 50;  // Minimum file size before compaction
const TARGET_FILE_SIZE_MB = 200;  // Target file size after compaction

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
      } else if (entry.name.endsWith('.jsonl')) {
        files.push(fullPath);
      }
    }
  }
  
  walk(dir);
  return files;
}

/**
 * Convert JSON-lines to parquet using DuckDB CLI
 */
export function convertJsonlToParquet(jsonlPath) {
  const parquetPath = jsonlPath.replace('.jsonl', '.parquet');
  
  try {
    const sql = `
      COPY (
        SELECT * FROM read_json_auto('${jsonlPath}')
      ) TO '${parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
    `;
    
    execSync(`duckdb -c "${sql}"`, { stdio: 'pipe' });
    
    // Remove original jsonl file after successful conversion
    unlinkSync(jsonlPath);
    
    console.log(`✅ Converted ${jsonlPath} -> ${parquetPath}`);
    return parquetPath;
  } catch (err) {
    console.error(`❌ Failed to convert ${jsonlPath}:`, err.message);
    return null;
  }
}

/**
 * Convert all pending JSON-lines files
 */
export function convertAllJsonl() {
  const jsonlFiles = findJsonlFiles(DATA_DIR);
  console.log(`Found ${jsonlFiles.length} JSON-lines files to convert`);
  
  const results = [];
  for (const file of jsonlFiles) {
    const result = convertJsonlToParquet(file);
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
      ) TO '${outputPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
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
 * Run full rotation: convert jsonl + compact small files
 */
export async function runRotation() {
  console.log('🔄 Starting file rotation...\n');
  
  // Step 1: Convert JSON-lines to parquet
  console.log('📝 Converting JSON-lines files...');
  const converted = convertAllJsonl();
  console.log(`Converted ${converted.length} files\n`);
  
  // Step 2: Find and compact small files
  console.log('📦 Looking for small files to compact...');
  
  for (const prefix of ['updates', 'events']) {
    const smallFiles = findSmallParquetFiles(DATA_DIR, prefix);
    
    if (smallFiles.length >= 2) {
      // Group by partition directory
      const byDir = {};
      for (const file of smallFiles) {
        const dir = file.path.substring(0, file.path.lastIndexOf('/'));
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
  
  console.log('\n✅ Rotation complete');
}

// Run if called directly
if (process.argv[1]?.includes('rotate-parquet')) {
  runRotation().catch(console.error);
}

export default {
  convertJsonlToParquet,
  convertAllJsonl,
  compactParquetFiles,
  runRotation,
};
