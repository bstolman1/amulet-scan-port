/**
 * Engine Schema - DuckDB persistent tables for the warehouse engine
 * 
 * Tables:
 * - raw_files: metadata about ingested .pb.zst files
 * - events_raw: decoded event records
 * - updates_raw: decoded update records
 * - aggregation_state: tracks last processed file for incremental aggregations
 */

import { query } from '../duckdb/connection.js';

let schemaInitialized = false;

/**
 * Initialize all engine tables if they don't exist
 */
export async function initEngineSchema() {
  if (schemaInitialized) return;

  console.log('🔧 Initializing engine schema...');

  // Raw files metadata table
  await query(`
    CREATE TABLE IF NOT EXISTS raw_files (
      file_id       INTEGER PRIMARY KEY,
      file_path     VARCHAR NOT NULL,
      file_type     VARCHAR NOT NULL,
      migration_id  INTEGER,
      record_date   DATE,
      record_count  BIGINT DEFAULT 0,
      min_ts        TIMESTAMP,
      max_ts        TIMESTAMP,
      ingested      BOOLEAN DEFAULT FALSE,
      ingested_at   TIMESTAMP,
      created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Events table - matches existing event schema
  await query(`
    CREATE TABLE IF NOT EXISTS events_raw (
      id                  VARCHAR,
      update_id           VARCHAR,
      type                VARCHAR,
      synchronizer        VARCHAR,
      effective_at        TIMESTAMP,
      recorded_at         TIMESTAMP,
      contract_id         VARCHAR,
      party               VARCHAR,
      template            VARCHAR,
      package_name        VARCHAR,
      signatories         VARCHAR[],
      observers           VARCHAR[],
      payload             JSON,
      raw_json            JSON,
      _file_id            INTEGER
    )
  `);

  // Updates table - matches existing update schema
  await query(`
    CREATE TABLE IF NOT EXISTS updates_raw (
      id                  VARCHAR,
      synchronizer        VARCHAR,
      effective_at        TIMESTAMP,
      recorded_at         TIMESTAMP,
      type                VARCHAR,
      command_id          VARCHAR,
      workflow_id         VARCHAR,
      kind                VARCHAR,
      migration_id        INTEGER,
      offset_val          BIGINT,
      event_count         INTEGER,
      _file_id            INTEGER
    )
  `);

  // Aggregation state tracking
  await query(`
    CREATE TABLE IF NOT EXISTS aggregation_state (
      agg_name            VARCHAR PRIMARY KEY,
      last_file_id        INTEGER DEFAULT 0,
      last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Sequence for file IDs
  await query(`
    CREATE SEQUENCE IF NOT EXISTS raw_files_seq START 1
  `);

  schemaInitialized = true;
  console.log('✅ Engine schema initialized');
}

export async function resetEngineSchema() {
  console.log('🗑️ Resetting engine schema...');
  
  await query('DROP TABLE IF EXISTS raw_files');
  await query('DROP TABLE IF EXISTS events_raw');
  await query('DROP TABLE IF EXISTS updates_raw');
  await query('DROP TABLE IF EXISTS aggregation_state');
  await query('DROP SEQUENCE IF EXISTS raw_files_seq');
  
  schemaInitialized = false;
  await initEngineSchema();
}
