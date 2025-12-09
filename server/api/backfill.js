import { Router } from 'express';
import { readFileSync, existsSync, readdirSync, unlinkSync, rmSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import db from '../duckdb/connection.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const router = Router();

// Path to cursor directory (relative to server directory)
const CURSOR_DIR = join(__dirname, '../../data/cursors');

/**
 * Read all cursor files from the cursors directory
 */
function readAllCursors() {
  if (!existsSync(CURSOR_DIR)) {
    return [];
  }

  const files = readdirSync(CURSOR_DIR).filter(f => f.startsWith('cursor-') && f.endsWith('.json'));
  const cursors = [];

  for (const file of files) {
    try {
      const filePath = join(CURSOR_DIR, file);
      const data = JSON.parse(readFileSync(filePath, 'utf-8'));
      
      // Parse shard info from synchronizer_id (e.g., "sync-id-shard2")
      const shardMatch = data.synchronizer_id?.match(/-shard(\d+)$/);
      const shardIndex = shardMatch ? parseInt(shardMatch[1], 10) : null;

      cursors.push({
        id: data.id || file.replace('.json', ''),
        cursor_name: data.cursor_name || `migration-${data.migration_id}-${data.synchronizer_id?.substring(0, 20)}`,
        migration_id: data.migration_id,
        synchronizer_id: data.synchronizer_id,
        shard_index: shardIndex,
        min_time: data.min_time,
        max_time: data.max_time,
        last_before: data.last_before,
        complete: data.complete || false,
        last_processed_round: data.last_processed_round || 0,
        updated_at: data.updated_at || new Date().toISOString(),
        started_at: data.started_at,
        total_updates: data.total_updates || 0,
        total_events: data.total_events || 0,
      });
    } catch (err) {
      console.error(`Error reading cursor file ${file}:`, err.message);
    }
  }

  return cursors;
}

// GET /api/backfill/debug - Debug endpoint to check paths
router.get('/debug', (req, res) => {
  const cursorExists = existsSync(CURSOR_DIR);
  const cursorFiles = cursorExists ? readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json')) : [];
  
  res.json({
    cursorDir: CURSOR_DIR,
    cursorDirExists: cursorExists,
    cursorFiles,
  });
});

// GET /api/backfill/cursors - Get all backfill cursors
router.get('/cursors', (req, res) => {
  try {
    const cursors = readAllCursors();
    console.log(`[backfill] Found ${cursors.length} cursors in ${CURSOR_DIR}`);
    res.json({ data: cursors, count: cursors.length });
  } catch (err) {
    console.error('Error reading cursors:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/backfill/stats - Get backfill statistics from data files
router.get('/stats', async (req, res) => {
  try {
    // Find actual files (avoids Windows glob pattern issues)
    const updateFiles = db.findDataFiles('updates');
    const eventFiles = db.findDataFiles('events');

    // Count updates and events from JSONL files
    const [updatesResult, eventsResult] = await Promise.all([
      updateFiles.length > 0 
        ? db.safeQuery(`SELECT COUNT(*) as count FROM ${db.readJsonlFiles(updateFiles)}`).catch(() => [{ count: 0 }])
        : Promise.resolve([{ count: 0 }]),
      eventFiles.length > 0
        ? db.safeQuery(`SELECT COUNT(*) as count FROM ${db.readJsonlFiles(eventFiles)}`).catch(() => [{ count: 0 }])
        : Promise.resolve([{ count: 0 }]),
    ]);

    // Get unique migrations
    let activeMigrations = 0;
    if (updateFiles.length > 0) {
      try {
        const migrationsResult = await db.safeQuery(`
          SELECT COUNT(DISTINCT migration_id) as count 
          FROM ${db.readJsonlFiles(updateFiles)}
          WHERE migration_id IS NOT NULL
        `);
        activeMigrations = Number(migrationsResult[0]?.count || 0);
      } catch (e) {
        // Data might not have migration_id column
      }
    }

    // Read cursor stats
    const cursors = readAllCursors();
    const totalCursors = cursors.length;
    const completedCursors = cursors.filter(c => c.complete).length;

    res.json({
      totalUpdates: Number(updatesResult[0]?.count || 0),
      totalEvents: Number(eventsResult[0]?.count || 0),
      activeMigrations,
      totalCursors,
      completedCursors,
    });
  } catch (err) {
    console.error('Error getting backfill stats:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/backfill/purge - Purge all backfill data (local files)
router.delete('/purge', (req, res) => {
  try {
    const dataDir = join(__dirname, '../../data');
    const rawDir = join(dataDir, 'raw');
    let deletedCursors = 0;
    let deletedDataFiles = 0;

    // Delete cursor files
    if (existsSync(CURSOR_DIR)) {
      const cursorFiles = readdirSync(CURSOR_DIR).filter(f => f.endsWith('.json'));
      for (const file of cursorFiles) {
        try {
          unlinkSync(join(CURSOR_DIR, file));
          deletedCursors++;
        } catch (err) {
          console.error(`Error deleting cursor file ${file}:`, err.message);
        }
      }
    }

    // Delete raw data directory recursively
    if (existsSync(rawDir)) {
      try {
        rmSync(rawDir, { recursive: true, force: true });
        deletedDataFiles = 1; // Directory deleted
      } catch (err) {
        console.error('Error deleting raw data directory:', err.message);
      }
    }

    console.log(`[backfill] Purged ${deletedCursors} cursors, deleted raw data: ${deletedDataFiles > 0}`);
    
    res.json({
      success: true,
      deleted_cursors: deletedCursors,
      deleted_data_dir: deletedDataFiles > 0,
    });
  } catch (err) {
    console.error('Error purging backfill data:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
