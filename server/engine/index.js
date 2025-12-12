/**
 * Warehouse Engine - Main export (STREAMING-ONLY)
 * 
 * A streaming ingestion engine for .pb.zst files that:
 * - Indexes files from the backfill process
 * - Incrementally ingests data into DuckDB tables via streaming decode
 * - Updates aggregations based on new data
 * - All reads use pagination/streaming to avoid OOM
 * - Runs in the background without blocking server startup
 */

export { initEngineSchema, resetEngineSchema } from './schema.js';
export { scanAndIndexFiles, getFileStats, getPendingFileCount } from './file-index.js';
export { decodeFile, decodeFileStreaming, getFileType } from './decoder.js';
export { ingestNewFiles, getIngestionStats } from './ingest.js';
export { 
  updateAllAggregations, 
  getTotalCounts, 
  getTimeRange, 
  getTemplateEventCounts,
  streamTemplateEventCounts,
  streamEvents 
} from './aggregations.js';
export { startEngineWorker, stopEngineWorker, triggerCycle, getEngineStatus } from './worker.js';
export { default as engineRouter } from './api.js';
