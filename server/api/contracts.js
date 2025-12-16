import { Router } from 'express';
import db from '../duckdb/connection.js';
import binaryReader from '../duckdb/binary-reader.js';

const router = Router();

// Helper to get updates source (supports binary, parquet, and jsonl)
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
  return `(${queries.join(' UNION ALL ')})`;
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

// GET /api/contracts/:contractId - Get contract lifecycle
router.get('/:contractId', async (req, res) => {
  try {
    const { contractId } = req.params;
    
    const sql = `
      SELECT *
      FROM ${getUpdatesSource()}
      WHERE contract_id = '${contractId}'
      ORDER BY record_time ASC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows, contract_id: contractId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/contracts/active/by-template/:templateSuffix - Get active contracts by template
// Computes active contracts from updates: created - archived
router.get('/active/by-template/:templateSuffix', async (req, res) => {
  try {
    const { templateSuffix } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 100, 100000);
    const sources = getDataSources();

    // For binary source, use streaming with filter
    if (sources.primarySource === 'binary') {
      try {
        const result = await binaryReader.streamRecords(db.DATA_PATH, 'updates', {
          limit: limit * 2, // Fetch more since we'll filter
          maxDays: 365, // Look back a year for contracts
          maxFilesToScan: 1000,
          sortBy: 'record_time',
        });

        // Process records: find created contracts not yet archived
        const created = new Map();
        const archived = new Set();

        for (const record of result.records) {
          const data = record.update_data || record;
          const events = data?.events || data?.transaction?.events || [];
          
          for (const event of events) {
            const eventType = event?.event_type || event?.eventType;
            const contractId = event?.contract_id || event?.contractId;
            const templateId = event?.template_id || event?.templateId || '';
            
            if (!templateId.includes(templateSuffix)) continue;
            
            if (eventType === 'created' || eventType === 'CreatedEvent') {
              created.set(contractId, {
                contract_id: contractId,
                template_id: templateId,
                created_at: record.record_time || record.effective_at,
                payload: event?.payload || event?.create_arguments || event?.createArguments,
              });
            } else if (eventType === 'archived' || eventType === 'ArchivedEvent') {
              archived.add(contractId);
            }
          }
        }

        // Filter out archived contracts
        const active = [];
        for (const [contractId, contract] of created) {
          if (!archived.has(contractId)) {
            active.push(contract);
          }
        }

        // Sort by created_at desc and limit
        active.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
        const limited = active.slice(0, limit);

        return res.json({ data: limited, count: limited.length, source: 'binary' });
      } catch (binaryErr) {
        console.warn('Binary read failed, falling back to SQL:', binaryErr.message);
      }
    }

    // SQL-based approach for parquet/jsonl
    const sql = `
      WITH created AS (
        SELECT 
          contract_id, 
          template_id, 
          record_time as created_at, 
          update_data
        FROM ${getUpdatesSource()}
        WHERE update_type = 'transaction'
          AND template_id LIKE '%${templateSuffix}'
      ),
      archived AS (
        SELECT DISTINCT contract_id
        FROM ${getUpdatesSource()}
        WHERE update_type = 'transaction'
          AND JSON_EXTRACT_STRING(update_data, '$.events[0].event_type') = 'archived'
      )
      SELECT c.*
      FROM created c
      LEFT JOIN archived a ON c.contract_id = a.contract_id
      WHERE a.contract_id IS NULL
      ORDER BY c.created_at DESC
      LIMIT ${limit}
    `;
    
    const rows = await db.safeQuery(sql);
    
    // Transform to expected format
    const transformed = rows.map(row => ({
      contract_id: row.contract_id,
      template_id: row.template_id,
      created_at: row.created_at,
      payload: row.update_data?.payload || row.update_data?.create_arguments || row.update_data,
    }));
    
    res.json({ data: transformed, count: transformed.length, source: sources.primarySource });
  } catch (err) {
    console.error('Error fetching active contracts:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/contracts/templates - List all unique templates
router.get('/templates/list', async (req, res) => {
  try {
    const sql = `
      SELECT 
        template_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT contract_id) as contract_count
      FROM ${getUpdatesSource()}
      WHERE template_id IS NOT NULL
      GROUP BY template_id
      ORDER BY contract_count DESC
    `;
    
    const rows = await db.safeQuery(sql);
    res.json({ data: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
