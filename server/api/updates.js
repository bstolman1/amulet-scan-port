import { Router } from 'express';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';

const router = Router();

const getUpdatesSource = () => {
  const hasParquet = db.hasFileType('updates', '.parquet');
  if (hasParquet) {
    return `read_parquet('${db.DATA_PATH.replace(/\\/g, '/')}/**/updates-*.parquet', union_by_name=true)`;
  }

  const hasJsonl = db.hasFileType('updates', '.jsonl');
  const hasGzip = db.hasFileType('updates', '.jsonl.gz');
  const hasZstd = db.hasFileType('updates', '.jsonl.zst');

  if (!hasJsonl && !hasGzip && !hasZstd) {
    return `(SELECT NULL::VARCHAR as update_id, NULL::VARCHAR as update_type, NULL::TIMESTAMP as record_time WHERE false)`;
  }

  const basePath = db.DATA_PATH.replace(/\\/g, '/');
  const queries = [];
  if (hasJsonl) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl', union_by_name=true, ignore_errors=true)`);
  if (hasGzip) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl.gz', union_by_name=true, ignore_errors=true)`);
  if (hasZstd) queries.push(`SELECT * FROM read_json_auto('${basePath}/**/updates-*.jsonl.zst', union_by_name=true, ignore_errors=true)`);
  // Use UNION (not UNION ALL) to prevent duplicate records
  return `(${queries.join(' UNION ')})`;
};

function getDataSources() {
  const hasBinaryUpdates = binaryReader.hasBinaryFiles(db.DATA_PATH, 'updates');
  const hasParquetUpdates = db.hasFileType('updates', '.parquet');
  return {
    hasBinaryUpdates,
    hasParquetUpdates,
    primarySource: hasBinaryUpdates ? 'binary' : hasParquetUpdates ? 'parquet' : 'jsonl',
  };
}

// GET /api/updates/latest
router.get('/latest', async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const offset = parseInt(req.query.offset) || 0;
    const sources = getDataSources();

    if (sources.primarySource === 'binary') {
      const result = await binaryReader.streamRecords(db.DATA_PATH, 'updates', {
        limit,
        offset,
        maxDays: 30,
        maxFilesToScan: 200,
        sortBy: 'record_time',
      });
      return res.json({ data: result.records, count: result.records.length, hasMore: result.hasMore, source: 'binary' });
    }

    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      ORDER BY record_time DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const rows = await db.safeQuery(sql);
    res.json({ data: rows, count: rows.length, source: sources.primarySource });
  } catch (err) {
    console.error('Error fetching latest updates:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/updates/count
router.get('/count', async (req, res) => {
  try {
    const sources = getDataSources();

    if (sources.primarySource === 'binary') {
      const fileCount = binaryReader.countBinaryFiles(db.DATA_PATH, 'updates');
      const estimated = fileCount * 100;
      return res.json({ count: estimated, estimated: true, fileCount, source: 'binary' });
    }

    const rows = await db.safeQuery(`SELECT COUNT(*) as total FROM ${getUpdatesSource()}`);
    res.json({ count: rows[0]?.total || 0, source: sources.primarySource });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
