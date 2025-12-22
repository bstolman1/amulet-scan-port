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
  // Even if we already initialized during this process, we still need to ensure
  // compatibility migrations run against the *persistent* DuckDB file.

  console.log('🔧 Initializing engine schema...');


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

  // DuckDB requires a UNIQUE index for ON CONFLICT (agg_name)
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_aggregation_state_agg_name
    ON aggregation_state(agg_name)
  `);

  // Sequence for file IDs
  await query(`
    CREATE SEQUENCE IF NOT EXISTS raw_files_seq START 1
  `);

  // VoteRequest persistent index table
  await query(`
    CREATE TABLE IF NOT EXISTS vote_requests (
      event_id            VARCHAR PRIMARY KEY,
      stable_id           VARCHAR,
      contract_id         VARCHAR,
      template_id         VARCHAR,
      effective_at        TIMESTAMP,
      status              VARCHAR DEFAULT 'active',
      is_closed           BOOLEAN DEFAULT FALSE,
      action_tag          VARCHAR,
      action_value        VARCHAR,
      requester           VARCHAR,
      reason              VARCHAR,
      votes               VARCHAR,
      vote_count          INTEGER DEFAULT 0,
      vote_before         VARCHAR,
      target_effective_at VARCHAR,
      tracking_cid        VARCHAR,
      dso                 VARCHAR,
      payload             VARCHAR,
      created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Backward-compatible schema fixes for existing DuckDB files
  // (DuckDB CREATE TABLE IF NOT EXISTS does not modify existing tables)
  // Some older DuckDB files may have stable_id defined as NOT NULL; DuckDB
  // doesn't reliably support DROP NOT NULL across versions, so we do a safe
  // table rebuild when needed.
  try {
    await query(`ALTER TABLE vote_requests ADD COLUMN stable_id VARCHAR`);
  } catch {
    // Column may already exist
  }

  try {
    const cols = await query(`PRAGMA table_info('vote_requests')`);
    const stable = cols.find(c => c.name === 'stable_id');
    if (stable && Number(stable.notnull) === 1) {
      console.log('🔧 Migrating vote_requests.stable_id to nullable (table rebuild)...');
      await query(`
        CREATE TABLE vote_requests__tmp AS
        SELECT * FROM vote_requests
      `);

      await query(`DROP TABLE vote_requests`);

      await query(`
        CREATE TABLE vote_requests (
          event_id            VARCHAR PRIMARY KEY,
          stable_id           VARCHAR,
          contract_id         VARCHAR,
          template_id         VARCHAR,
          effective_at        TIMESTAMP,
          status              VARCHAR DEFAULT 'active',
          is_closed           BOOLEAN DEFAULT FALSE,
          action_tag          VARCHAR,
          action_value        VARCHAR,
          requester           VARCHAR,
          reason              VARCHAR,
          votes               VARCHAR,
          vote_count          INTEGER DEFAULT 0,
          vote_before         VARCHAR,
          target_effective_at VARCHAR,
          tracking_cid        VARCHAR,
          dso                 VARCHAR,
          payload             VARCHAR,
          created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `);

      await query(`
        INSERT INTO vote_requests
        SELECT * FROM vote_requests__tmp
      `);
      await query(`DROP TABLE vote_requests__tmp`);
      console.log('   ✓ vote_requests migrated');
    }
  } catch (err) {
    console.warn('⚠️ vote_requests schema migration skipped:', err?.message || err);
  }

  // DuckDB requires UNIQUE indexes for ON CONFLICT targets
  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_vote_requests_event_id
    ON vote_requests(event_id)
  `);

  // Indexes for efficient queries
  await query(`
    CREATE INDEX IF NOT EXISTS idx_vote_requests_status ON vote_requests(status)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_vote_requests_effective_at ON vote_requests(effective_at)
  `);
  await query(`
    CREATE INDEX IF NOT EXISTS idx_vote_requests_contract_id ON vote_requests(contract_id)
  `);

  // Track indexing progress for vote_requests
  await query(`
    CREATE TABLE IF NOT EXISTS vote_request_index_state (
      id                  INTEGER PRIMARY KEY DEFAULT 1,
      last_indexed_file   VARCHAR,
      last_indexed_at     TIMESTAMP,
      total_indexed       BIGINT DEFAULT 0,
      CHECK (id = 1)
    )
  `);

  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_vote_request_index_state_id
    ON vote_request_index_state(id)
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
  await query('DROP TABLE IF EXISTS vote_requests');
  await query('DROP TABLE IF EXISTS vote_request_index_state');
  await query('DROP SEQUENCE IF EXISTS raw_files_seq');
  
  schemaInitialized = false;
  await initEngineSchema();
}
