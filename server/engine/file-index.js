/**
 * File Index - Scans filesystem for .pb.zst files and indexes them
 * 
 * Tracks which files have been discovered but not yet ingested.
 */

import fs from 'fs';
import path from 'path';
import { query } from '../duckdb/connection.js';

// Match the same path resolution logic as connection.js (but avoid fileURLToPath for Vitest)
const REPO_DATA_DIR = path.join(process.cwd(), 'data');
const repoRawDir = path.join(REPO_DATA_DIR, 'raw');
const WIN_DEFAULT_DATA_DIR = 'C:\\ledger_raw';
const BASE_DATA_DIR = process.env.DATA_DIR || (fs.existsSync(repoRawDir) ? REPO_DATA_DIR : WIN_DEFAULT_DATA_DIR);
const RAW_DIR = path.join(BASE_DATA_DIR, 'raw');

/**
 * Walk directory tree and find all .pb.zst files
 */
function walkFiles(dir) {
  const out = [];
  if (!fs.existsSync(dir)) return out;

  const stack = [dir];
  while (stack.length) {
    const current = stack.pop();
    try {
      const entries = fs.readdirSync(current, { withFileTypes: true });
      for (const entry of entries) {
        const full = path.join(current, entry.name);
        if (entry.isDirectory()) {
          stack.push(full);
        } else if (entry.name.endsWith('.pb.zst')) {
          out.push(full);
        }
      }
    } catch (err) {
      console.warn(`⚠️ Cannot read directory ${current}: ${err.message}`);
    }
  }
  return out;
}

/**
 * Detect file type from path
 */
function detectFileType(filePath) {
  const basename = path.basename(filePath);
  if (basename.startsWith('events-')) return 'events';
  if (basename.startsWith('updates-')) return 'updates';
  return null;
}

/**
 * Extract migration ID from path (e.g., migration=3)
 */
function extractMigrationId(filePath) {
  const match = filePath.match(/migration[=_](\d+)/i);
  return match ? parseInt(match[1], 10) : null;
}

/**
 * Extract record date from path (year/month/day)
 */
function extractRecordDate(filePath) {
  const yearMatch = filePath.match(/year[=_](\d+)/i);
  const monthMatch = filePath.match(/month[=_](\d+)/i);
  const dayMatch = filePath.match(/day[=_](\d+)/i);
  
  if (!yearMatch || !monthMatch || !dayMatch) return null;
  
  const year = yearMatch[1];
  const month = monthMatch[1].padStart(2, '0');
  const day = dayMatch[1].padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Scan filesystem and index new files into raw_files table
 */
export async function scanAndIndexFiles() {
  const files = walkFiles(RAW_DIR);
  
  if (files.length === 0) {
    return { totalFiles: 0, newFiles: 0 };
  }

  // Get existing file paths
  const existingRows = await query('SELECT file_path FROM raw_files');
  const existingPaths = new Set(existingRows.map(r => r.file_path));

  let inserted = 0;

  for (const filePath of files) {
    // Normalize path for consistent comparison
    const normalizedPath = filePath.replace(/\\/g, '/');
    
    if (existingPaths.has(normalizedPath)) continue;

    const fileType = detectFileType(filePath);
    if (!fileType) continue;

    const migrationId = extractMigrationId(filePath);
    const recordDate = extractRecordDate(filePath);

    try {
      await query(
        `INSERT INTO raw_files (file_id, file_path, file_type, migration_id, record_date, ingested)
         VALUES (nextval('raw_files_seq'), ?, ?, ?, ?, FALSE)`,
        [normalizedPath, fileType, migrationId, recordDate]
      );
      inserted++;
    } catch (err) {
      console.warn(`⚠️ Failed to index ${filePath}: ${err.message}`);
    }
  }

  if (inserted > 0) {
    console.log(`📂 Indexed ${inserted} new files (${files.length} total on disk)`);
  }

  return { totalFiles: files.length, newFiles: inserted };
}

/**
 * Get counts of files by status
 */
export async function getFileStats() {
  const rows = await query(`
    SELECT 
      file_type,
      ingested,
      COUNT(*) as count,
      SUM(record_count) as records
    FROM raw_files
    GROUP BY file_type, ingested
    ORDER BY file_type, ingested
  `);
  
  // Convert BigInt to Number for JSON serialization
  return rows.map(r => ({
    ...r,
    count: Number(r.count || 0),
    records: Number(r.records || 0),
  }));
}

/**
 * Get pending (un-ingested) file count
 */
export async function getPendingFileCount() {
  const rows = await query(`
    SELECT COUNT(*) as count FROM raw_files WHERE ingested = FALSE
  `);
  // Convert BigInt to Number for JSON serialization
  return Number(rows[0]?.count || 0);
}
