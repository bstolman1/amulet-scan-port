/**
 * Explorer API - Query .pb.zst ledger files
 * 
 * Endpoints:
 * - GET /api/explorer/updates - List updates with filters
 * - GET /api/explorer/updates/:id/events - Events for a specific update
 * - GET /api/explorer/metrics/timeline - Update volume over time
 * - GET /api/explorer/stats - File statistics
 */

import { Router } from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Import read-binary utilities from scripts/ingest
const readBinaryPath = path.resolve(__dirname, '../../scripts/ingest/read-binary.js');
let readBinaryFile, getFileStats;

try {
  const readBinary = await import(readBinaryPath);
  readBinaryFile = readBinary.readBinaryFile;
  getFileStats = readBinary.getFileStats;
} catch (err) {
  console.warn('⚠️ Could not import read-binary.js:', err.message);
}

const router = Router();

// Ledger root from env or default
const LEDGER_ROOT = process.env.LEDGER_ROOT || process.env.DATA_DIR || 'C:\\ledger_raw';

/**
 * Recursively find .pb.zst files matching pattern
 */
function findPbZstFiles(dir, prefix = '') {
  const results = [];
  
  try {
    if (!fs.existsSync(dir)) return results;
    
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory()) {
        results.push(...findPbZstFiles(fullPath, prefix));
      } else if (entry.name.startsWith(prefix) && entry.name.endsWith('.pb.zst')) {
        results.push(fullPath);
      }
    }
  } catch (err) {
    console.error(`Error scanning ${dir}:`, err.message);
  }
  
  return results;
}

/**
 * Match helper for partial string matching
 */
function matchesFilter(value, filter) {
  if (!filter) return true;
  if (!value) return false;
  return String(value).toLowerCase().includes(String(filter).toLowerCase());
}

/**
 * GET /api/explorer/health
 */
router.get('/health', (req, res) => {
  res.json({
    ok: true,
    ledgerRoot: LEDGER_ROOT,
    exists: fs.existsSync(LEDGER_ROOT),
    readBinaryAvailable: !!readBinaryFile
  });
});

/**
 * GET /api/explorer/updates
 * Query params: limit, offset, synchronizer, workflow, status, from, to
 */
router.get('/updates', async (req, res) => {
  try {
    if (!readBinaryFile) {
      return res.status(503).json({ error: 'read-binary module not available' });
    }
    
    const limit = Math.min(Number(req.query.limit) || 50, 500);
    const offset = Number(req.query.offset) || 0;
    const synchronizerFilter = req.query.synchronizer || '';
    const workflowFilter = req.query.workflow || '';
    const statusFilter = req.query.status || '';
    const fromTs = req.query.from ? Number(req.query.from) : null;
    const toTs = req.query.to ? Number(req.query.to) : null;
    
    // Find update files
    const updateFiles = findPbZstFiles(LEDGER_ROOT, 'updates-').sort().reverse();
    
    if (updateFiles.length === 0) {
      return res.json({ updates: [], count: 0, offset, totalFiles: 0 });
    }
    
    const results = [];
    let skipped = 0;
    let filesScanned = 0;
    const maxFilesToScan = 20; // Limit for performance
    
    for (const file of updateFiles) {
      if (results.length >= limit || filesScanned >= maxFilesToScan) break;
      filesScanned++;
      
      try {
        const data = await readBinaryFile(file);
        
        for (const u of data.records) {
          // Time filter
          const effAt = u.effective_at ? new Date(u.effective_at).getTime() : 0;
          if (fromTs && effAt < fromTs) continue;
          if (toTs && effAt > toTs) continue;
          
          // String filters
          if (!matchesFilter(u.synchronizer, synchronizerFilter)) continue;
          if (!matchesFilter(u.workflow_id, workflowFilter)) continue;
          if (!matchesFilter(u.status, statusFilter)) continue;
          
          if (skipped < offset) {
            skipped++;
            continue;
          }
          
          results.push(u);
          
          if (results.length >= limit) break;
        }
      } catch (err) {
        console.error(`Error reading ${file}:`, err.message);
      }
    }
    
    res.json({
      updates: results,
      count: results.length,
      offset,
      filesScanned,
      totalFiles: updateFiles.length
    });
  } catch (err) {
    console.error('Error in /api/explorer/updates:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/explorer/updates/:id/events
 * Query params: party, contract, template
 */
router.get('/updates/:id/events', async (req, res) => {
  try {
    if (!readBinaryFile) {
      return res.status(503).json({ error: 'read-binary module not available' });
    }
    
    const updateId = req.params.id;
    const partyFilter = req.query.party || '';
    const contractFilter = req.query.contract || '';
    const templateFilter = req.query.template || '';
    
    // Find event files
    const eventFiles = findPbZstFiles(LEDGER_ROOT, 'events-').sort().reverse();
    
    const events = [];
    let filesScanned = 0;
    const maxFilesToScan = 50;
    
    for (const file of eventFiles) {
      if (filesScanned >= maxFilesToScan) break;
      filesScanned++;
      
      try {
        const data = await readBinaryFile(file);
        
        for (const ev of data.records) {
          if (ev.update_id !== updateId) continue;
          if (!matchesFilter(ev.party, partyFilter)) continue;
          if (!matchesFilter(ev.contract_id, contractFilter)) continue;
          if (!matchesFilter(ev.template, templateFilter)) continue;
          
          events.push(ev);
        }
      } catch (err) {
        console.error(`Error reading ${file}:`, err.message);
      }
    }
    
    res.json({ updateId, events, filesScanned });
  } catch (err) {
    console.error('Error in /api/explorer/updates/:id/events:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/explorer/metrics/timeline
 * Query params: bucket (hour|day)
 */
router.get('/metrics/timeline', async (req, res) => {
  try {
    if (!readBinaryFile) {
      return res.status(503).json({ error: 'read-binary module not available' });
    }
    
    const bucket = req.query.bucket === 'day' ? 'day' : 'hour';
    
    // Find update files
    const updateFiles = findPbZstFiles(LEDGER_ROOT, 'updates-').sort().reverse();
    
    const buckets = new Map();
    let filesScanned = 0;
    const maxFilesToScan = 30;
    
    for (const file of updateFiles) {
      if (filesScanned >= maxFilesToScan) break;
      filesScanned++;
      
      try {
        const data = await readBinaryFile(file);
        
        for (const u of data.records) {
          const effAt = u.effective_at;
          if (!effAt) continue;
          
          const d = new Date(effAt);
          let key;
          
          if (bucket === 'day') {
            key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
          } else {
            key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')} ${String(d.getUTCHours()).padStart(2, '0')}:00`;
          }
          
          buckets.set(key, (buckets.get(key) || 0) + 1);
        }
      } catch (err) {
        console.error(`Error reading ${file}:`, err.message);
      }
    }
    
    const points = Array.from(buckets.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([t, count]) => ({ t, count }));
    
    res.json({ bucket, points, filesScanned });
  } catch (err) {
    console.error('Error in /api/explorer/metrics/timeline:', err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/explorer/stats
 * Get file statistics
 */
router.get('/stats', async (req, res) => {
  try {
    const updateFiles = findPbZstFiles(LEDGER_ROOT, 'updates-');
    const eventFiles = findPbZstFiles(LEDGER_ROOT, 'events-');
    
    // Get total size
    let totalSize = 0;
    for (const f of [...updateFiles, ...eventFiles]) {
      try {
        const stat = fs.statSync(f);
        totalSize += stat.size;
      } catch {}
    }
    
    // Get sample file stats if available
    let sampleStats = null;
    if (getFileStats && updateFiles.length > 0) {
      try {
        sampleStats = await getFileStats(updateFiles[0]);
      } catch {}
    }
    
    res.json({
      ledgerRoot: LEDGER_ROOT,
      updateFiles: updateFiles.length,
      eventFiles: eventFiles.length,
      totalSizeBytes: totalSize,
      totalSizeMB: (totalSize / (1024 * 1024)).toFixed(2),
      sampleFileStats: sampleStats
    });
  } catch (err) {
    console.error('Error in /api/explorer/stats:', err);
    res.status(500).json({ error: err.message });
  }
});

export default router;
