#!/usr/bin/env node
/**
 * Shard Progress Aggregator
 * 
 * Monitors and aggregates progress across all running backfill shards.
 * Shows combined stats, per-shard progress, and estimated completion time.
 * 
 * Usage: node shard-progress.js [--watch]
 */

import { readdirSync, readFileSync, statSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Default Windows path: C:\ledger_raw\cursors
const WIN_DEFAULT = 'C:\\ledger_raw\\cursors';
const CURSOR_DIR = process.env.CURSOR_DIR || WIN_DEFAULT;
const WATCH_MODE = process.argv.includes('--watch') || process.argv.includes('-w');
const REFRESH_INTERVAL = 2000; // 2 seconds

/**
 * Parse all shard cursor files
 */
function loadAllCursors() {
  const cursors = [];
  
  try {
    const files = readdirSync(CURSOR_DIR);
    
    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      
      try {
        const filePath = join(CURSOR_DIR, file);
        const content = JSON.parse(readFileSync(filePath, 'utf8'));
        const stats = statSync(filePath);
        
        cursors.push({
          file,
          ...content,
          file_modified: stats.mtime,
        });
      } catch (e) {
        // Skip invalid files
      }
    }
  } catch (e) {
    console.error('Error reading cursor directory:', e.message);
  }
  
  return cursors;
}

/**
 * Group cursors by migration and synchronizer
 */
function groupCursors(cursors) {
  const groups = {};
  
  for (const cursor of cursors) {
    const migrationId = cursor.migration_id || 'unknown';
    const synchronizerId = cursor.synchronizer_id || 'unknown';
    const key = `${migrationId}-${synchronizerId}`;
    
    if (!groups[key]) {
      groups[key] = {
        migrationId,
        synchronizerId,
        shards: [],
        totalShards: cursor.shard_total || 1,
      };
    }
    
    groups[key].shards.push(cursor);
  }
  
  return groups;
}

/**
 * Calculate progress percentage for a shard
 */
function calculateProgress(cursor) {
  if (cursor.complete) return 100;
  if (!cursor.min_time || !cursor.max_time || !cursor.last_before) return 0;
  
  const minMs = new Date(cursor.min_time).getTime();
  const maxMs = new Date(cursor.max_time).getTime();
  const currentMs = new Date(cursor.last_before).getTime();
  
  const totalRange = maxMs - minMs;
  if (totalRange <= 0) return 100;
  
  const completed = maxMs - currentMs;
  let rawProgress = (completed / totalRange) * 100;
  
  // Cap at 99.9% if not marked complete OR has pending writes
  const hasPendingWork = (cursor.pending_writes || 0) > 0 || (cursor.buffered_records || 0) > 0;
  if ((rawProgress >= 99.5 || hasPendingWork) && !cursor.complete) {
    rawProgress = Math.min(rawProgress, 99.9);
  }
  
  return Math.min(100, Math.max(0, rawProgress));
}

/**
 * Calculate ETA based on progress rate
 */
function calculateETA(cursor) {
  if (cursor.complete) return 'Complete';
  if (!cursor.started_at || !cursor.updated_at) return 'Unknown';
  
  const startedAt = new Date(cursor.started_at).getTime();
  const updatedAt = new Date(cursor.updated_at).getTime();
  const elapsed = updatedAt - startedAt;
  
  if (elapsed <= 0) return 'Calculating...';
  
  const progress = calculateProgress(cursor);
  if (progress <= 0) return 'Calculating...';
  
  const totalEstimate = (elapsed / progress) * 100;
  const remaining = totalEstimate - elapsed;
  
  if (remaining <= 0) return 'Almost done';
  
  const hours = Math.floor(remaining / 3600000);
  const minutes = Math.floor((remaining % 3600000) / 60000);
  
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  return `${minutes}m`;
}

/**
 * Format number with commas
 */
function formatNumber(num) {
  return (num || 0).toLocaleString();
}

/**
 * Clear console and move cursor to top
 */
function clearScreen() {
  process.stdout.write('\x1B[2J\x1B[H');
}

/**
 * Print progress report
 */
function printReport(cursors) {
  const groups = groupCursors(cursors);
  
  // Calculate totals
  let totalUpdates = 0;
  let totalEvents = 0;
  let totalShards = 0;
  let completedShards = 0;
  let activeShards = 0;
  
  for (const cursor of cursors) {
    totalUpdates += cursor.total_updates || 0;
    totalEvents += cursor.total_events || 0;
    totalShards++;
    
    if (cursor.complete) {
      completedShards++;
    } else if (cursor.updated_at) {
      const lastUpdate = new Date(cursor.updated_at).getTime();
      const now = Date.now();
      if (now - lastUpdate < 60000) { // Active in last minute
        activeShards++;
      }
    }
  }
  
  const overallProgress = totalShards > 0 
    ? cursors.reduce((sum, c) => sum + calculateProgress(c), 0) / totalShards 
    : 0;
  
  // Print header
  console.log('═'.repeat(80));
  console.log('📊 SHARDED BACKFILL PROGRESS');
  console.log('═'.repeat(80));
  console.log('');
  
  // Overall stats
  console.log('📈 OVERALL STATS');
  console.log('─'.repeat(40));
  console.log(`   Total Updates:     ${formatNumber(totalUpdates)}`);
  console.log(`   Total Events:      ${formatNumber(totalEvents)}`);
  console.log(`   Overall Progress:  ${overallProgress.toFixed(1)}%`);
  console.log(`   Shards:            ${completedShards}/${totalShards} complete, ${activeShards} active`);
  console.log('');
  
  // Per-group breakdown
  for (const [key, group] of Object.entries(groups)) {
    const syncShort = group.synchronizerId.substring(0, 30);
    console.log(`📦 Migration ${group.migrationId} / ${syncShort}...`);
    console.log('─'.repeat(60));
    
    // Sort shards by index
    const sortedShards = group.shards.sort((a, b) => 
      (a.shard_index || 0) - (b.shard_index || 0)
    );
    
    for (const shard of sortedShards) {
      const shardLabel = shard.shard_index !== undefined 
        ? `Shard ${shard.shard_index}` 
        : 'Main';
      
      const progress = calculateProgress(shard);
      const eta = calculateETA(shard);
      const status = shard.complete ? '✅' : (shard.error ? '⚠️' : '🔄');
      
      const progressBar = createProgressBar(progress, 20);
      
      console.log(`   ${status} ${shardLabel.padEnd(10)} ${progressBar} ${progress.toFixed(1).padStart(5)}% | ${formatNumber(shard.total_updates || 0).padStart(10)} upd | ETA: ${eta}`);
      
      if (shard.error) {
        console.log(`      └─ Error: ${shard.error.substring(0, 50)}`);
      }
    }
    console.log('');
  }
  
  // Footer
  console.log('─'.repeat(80));
  console.log(`Last updated: ${new Date().toISOString()}`);
  if (WATCH_MODE) {
    console.log('Press Ctrl+C to exit');
  }
}

/**
 * Create ASCII progress bar
 */
function createProgressBar(percent, width) {
  const filled = Math.round((percent / 100) * width);
  const empty = width - filled;
  return '[' + '█'.repeat(filled) + '░'.repeat(empty) + ']';
}

/**
 * Main function
 */
function main() {
  if (WATCH_MODE) {
    // Watch mode - refresh every few seconds
    const refresh = () => {
      clearScreen();
      const cursors = loadAllCursors();
      printReport(cursors);
    };
    
    refresh();
    setInterval(refresh, REFRESH_INTERVAL);
  } else {
    // One-shot mode
    const cursors = loadAllCursors();
    printReport(cursors);
  }
}

main();
