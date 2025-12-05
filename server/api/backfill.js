import { Router } from 'express';
import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import db from '../duckdb/connection.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const router = Router();

// Path to cursor file (relative to server directory)
const CURSOR_FILE = join(__dirname, '../../data/backfill-cursors.json');

// GET /api/backfill/cursors - Get all backfill cursors
router.get('/cursors', (req, res) => {
  try {
    if (!existsSync(CURSOR_FILE)) {
      return res.json({ data: [], count: 0 });
    }

    const data = JSON.parse(readFileSync(CURSOR_FILE, 'utf-8'));
    const cursors = Object.values(data).map((cursor, index) => ({
      id: cursor.id || `cursor-${index}`,
      cursor_name: cursor.cursor_name || `migration-${cursor.migration_id}-${cursor.synchronizer_id?.substring(0, 20)}`,
      migration_id: cursor.migration_id,
      synchronizer_id: cursor.synchronizer_id,
      min_time: cursor.min_time,
      max_time: cursor.max_time,
      last_before: cursor.last_before,
      complete: cursor.complete,
      last_processed_round: cursor.last_processed_round || 0,
      updated_at: cursor.updated_at || new Date().toISOString(),
    }));

    res.json({ data: cursors, count: cursors.length });
  } catch (err) {
    console.error('Error reading cursor file:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/backfill/stats - Get backfill statistics from Parquet files
router.get('/stats', async (req, res) => {
  try {
    // Count updates and events from Parquet files
    const [updatesResult, eventsResult] = await Promise.all([
      db.safeQuery(`
        SELECT COUNT(*) as count 
        FROM read_parquet('${db.DATA_PATH}/**/*updates*.parquet', union_by_name=true)
      `).catch(() => [{ count: 0 }]),
      db.safeQuery(`
        SELECT COUNT(*) as count 
        FROM read_parquet('${db.DATA_PATH}/**/*events*.parquet', union_by_name=true)
      `).catch(() => [{ count: 0 }]),
    ]);

    // Get unique migrations
    let activeMigrations = 0;
    try {
      const migrationsResult = await db.safeQuery(`
        SELECT COUNT(DISTINCT migration_id) as count 
        FROM read_parquet('${db.DATA_PATH}/**/*updates*.parquet', union_by_name=true)
        WHERE migration_id IS NOT NULL
      `);
      activeMigrations = migrationsResult[0]?.count || 0;
    } catch (e) {
      // Parquet files might not have migration_id column
    }

    // Read cursor stats
    let totalCursors = 0;
    let completedCursors = 0;
    try {
      if (existsSync(CURSOR_FILE)) {
        const data = JSON.parse(readFileSync(CURSOR_FILE, 'utf-8'));
        const cursors = Object.values(data);
        totalCursors = cursors.length;
        completedCursors = cursors.filter(c => c.complete).length;
      }
    } catch (e) {
      // Cursor file might not exist
    }

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

export default router;
