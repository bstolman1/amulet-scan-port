/**
 * Warehouse Engine - Main export
 * 
 * A streaming ingestion engine for .pb.zst files that:
 * - Indexes files from the backfill process
 * - Incrementally ingests data into DuckDB tables
 * - Updates aggregations based on new data
 * - Runs in the background without blocking server startup
 */

export { initEngineSchema, resetEngineSchema } from './schema.js';
export { scanAndIndexFiles, getFileStats, getPendingFileCount } from './file-index.js';
export { decodeFile, decodeFileWithStats } from './decoder.js';
export { ingestNewFiles, getIngestionStats } from './ingest.js';
export { updateAllAggregations, getTotalCounts, getTimeRange, getTemplateEventCounts } from './aggregations.js';
export { startEngineWorker, stopEngineWorker, triggerCycle, getEngineStatus } from './worker.js';
export { default as engineRouter } from './api.js';
