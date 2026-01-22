#!/usr/bin/env node
/**
 * GCS Schema Audit - Field Coverage Validator
 * 
 * Samples Parquet files from GCS and compares their schemas against
 * the expected LEDGER_UPDATES_SCHEMA, LEDGER_EVENTS_SCHEMA, and ACS_CONTRACTS_SCHEMA.
 * 
 * Usage:
 *   node audit-gcs-schema.js              # Audit all migrations + ACS
 *   node audit-gcs-schema.js --migration=2  # Audit specific migration
 *   node audit-gcs-schema.js --acs-only   # Audit only ACS snapshots
 *   node audit-gcs-schema.js --verbose    # Show all columns, not just missing
 * 
 * Requires:
 *   - GCS_BUCKET environment variable
 *   - gsutil installed and authenticated
 *   - duckdb CLI available
 */

import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { execSync } from 'child_process';
import { existsSync, mkdirSync, unlinkSync, readdirSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: join(__dirname, '.env') });

import { LEDGER_UPDATES_SCHEMA, LEDGER_EVENTS_SCHEMA } from './data-schema.js';
import { ACS_CONTRACTS_SCHEMA } from './acs-schema.js';

const GCS_BUCKET = process.env.GCS_BUCKET;
const TMP_DIR = '/tmp/schema-audit';

// Parse args
const args = process.argv.slice(2);
const VERBOSE = args.includes('--verbose');
const ACS_ONLY = args.includes('--acs-only');
const MIGRATION_ARG = args.find(a => a.startsWith('--migration='));
const TARGET_MIGRATION = MIGRATION_ARG ? parseInt(MIGRATION_ARG.split('=')[1]) : null;

// Results tracking
const auditResults = {
  timestamp: new Date().toISOString(),
  bucket: GCS_BUCKET,
  migrations: {},
  acs: null,
  summary: {
    totalFiles: 0,
    schemasChecked: 0,
    missingColumns: 0,
    extraColumns: 0,
  }
};

/**
 * Execute shell command with error handling
 */
function exec(cmd, options = {}) {
  try {
    return execSync(cmd, { 
      encoding: 'utf8', 
      maxBuffer: 50 * 1024 * 1024,
      stdio: ['pipe', 'pipe', 'pipe'],
      ...options 
    });
  } catch (err) {
    if (options.throwOnError !== false) {
      throw err;
    }
    return '';
  }
}

/**
 * List sample files from GCS path
 */
function listGCSFiles(prefix, limit = 5) {
  if (!GCS_BUCKET) {
    throw new Error('GCS_BUCKET not set');
  }
  
  try {
    const output = exec(
      `gsutil ls "gs://${GCS_BUCKET}/${prefix}**/*.parquet" 2>/dev/null | head -${limit}`,
      { throwOnError: false }
    );
    return output.trim().split('\n').filter(line => line.includes('.parquet'));
  } catch {
    return [];
  }
}

/**
 * Download a file from GCS to local tmp
 */
function downloadFile(gcsPath, localName) {
  const localPath = join(TMP_DIR, localName);
  exec(`gsutil cp "${gcsPath}" "${localPath}"`);
  return localPath;
}

/**
 * Get schema from a Parquet file using DuckDB
 */
function getParquetSchema(filePath) {
  try {
    const output = exec(
      `duckdb -c "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM '${filePath}')" -csv`
    );
    
    const lines = output.trim().split('\n').slice(1); // Skip header
    const schema = {};
    
    for (const line of lines) {
      const [name, type] = line.split(',').map(s => s.replace(/"/g, '').trim());
      if (name) {
        schema[name] = type;
      }
    }
    
    return schema;
  } catch (err) {
    console.error(`  ❌ Failed to read schema: ${err.message}`);
    return null;
  }
}

/**
 * Compare actual schema against expected schema
 */
function compareSchemas(actual, expected, label) {
  const actualColumns = new Set(Object.keys(actual || {}));
  const expectedColumns = new Set(Object.keys(expected));
  
  const missing = [...expectedColumns].filter(c => !actualColumns.has(c));
  const extra = [...actualColumns].filter(c => !expectedColumns.has(c));
  
  return {
    label,
    actualCount: actualColumns.size,
    expectedCount: expectedColumns.size,
    missing,
    extra,
    missingCount: missing.length,
    extraCount: extra.length,
    coverage: expectedColumns.size > 0 
      ? (((expectedColumns.size - missing.length) / expectedColumns.size) * 100).toFixed(1)
      : '100.0',
  };
}

/**
 * Audit a single migration's backfill data
 */
async function auditMigration(migrationId) {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`📊 MIGRATION ${migrationId}`);
  console.log('═'.repeat(60));
  
  const result = {
    migrationId,
    updates: null,
    events: null,
    sampleFiles: [],
  };
  
  // Find update files
  const updateFiles = listGCSFiles(`raw/backfill/migration=${migrationId}/`, 3);
  const updateFile = updateFiles.find(f => f.includes('updates-'));
  
  if (updateFile) {
    console.log(`\n📄 Updates Schema (sampled from ${updateFile.split('/').pop()})`);
    const localPath = downloadFile(updateFile, `updates-m${migrationId}.parquet`);
    const schema = getParquetSchema(localPath);
    
    if (schema) {
      result.updates = compareSchemas(schema, LEDGER_UPDATES_SCHEMA, 'updates');
      result.sampleFiles.push(updateFile);
      
      printSchemaComparison(result.updates, schema, LEDGER_UPDATES_SCHEMA);
    }
  } else {
    console.log(`\n  ⚠️ No update files found for migration ${migrationId}`);
  }
  
  // Find event files
  const eventFile = updateFiles.find(f => f.includes('events-'));
  
  if (eventFile) {
    console.log(`\n📄 Events Schema (sampled from ${eventFile.split('/').pop()})`);
    const localPath = downloadFile(eventFile, `events-m${migrationId}.parquet`);
    const schema = getParquetSchema(localPath);
    
    if (schema) {
      result.events = compareSchemas(schema, LEDGER_EVENTS_SCHEMA, 'events');
      result.sampleFiles.push(eventFile);
      
      printSchemaComparison(result.events, schema, LEDGER_EVENTS_SCHEMA);
    }
  } else {
    console.log(`\n  ⚠️ No event files found for migration ${migrationId}`);
  }
  
  return result;
}

/**
 * Audit ACS snapshot data
 */
async function auditACS() {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`📊 ACS SNAPSHOTS`);
  console.log('═'.repeat(60));
  
  const result = {
    contracts: null,
    sampleFiles: [],
  };
  
  // Find ACS files
  const acsFiles = listGCSFiles(`raw/acs/`, 5);
  const contractFile = acsFiles.find(f => f.includes('.parquet'));
  
  if (contractFile) {
    console.log(`\n📄 ACS Contracts Schema (sampled from ${contractFile.split('/').pop()})`);
    const localPath = downloadFile(contractFile, `acs-sample.parquet`);
    const schema = getParquetSchema(localPath);
    
    if (schema) {
      result.contracts = compareSchemas(schema, ACS_CONTRACTS_SCHEMA, 'acs_contracts');
      result.sampleFiles.push(contractFile);
      
      printSchemaComparison(result.contracts, schema, ACS_CONTRACTS_SCHEMA);
    }
  } else {
    console.log(`\n  ⚠️ No ACS files found in gs://${GCS_BUCKET}/raw/acs/`);
  }
  
  return result;
}

/**
 * Print schema comparison results
 */
function printSchemaComparison(comparison, actual, expected) {
  const { missing, extra, coverage, actualCount, expectedCount } = comparison;
  
  console.log(`\n   Coverage: ${coverage}% (${actualCount}/${expectedCount} columns)`);
  
  if (missing.length > 0) {
    console.log(`\n   ❌ MISSING COLUMNS (${missing.length}):`);
    for (const col of missing) {
      const expectedType = expected[col];
      console.log(`      - ${col} (expected: ${expectedType})`);
    }
    auditResults.summary.missingColumns += missing.length;
  } else {
    console.log(`\n   ✅ All expected columns present`);
  }
  
  if (extra.length > 0) {
    console.log(`\n   ➕ EXTRA COLUMNS (${extra.length}):`);
    for (const col of extra) {
      const actualType = actual[col];
      console.log(`      + ${col} (type: ${actualType})`);
    }
    auditResults.summary.extraColumns += extra.length;
  }
  
  if (VERBOSE && Object.keys(actual).length > 0) {
    console.log(`\n   📋 ALL COLUMNS:`);
    for (const [col, type] of Object.entries(actual)) {
      const status = missing.includes(col) ? '❌' : expected[col] ? '✅' : '➕';
      console.log(`      ${status} ${col}: ${type}`);
    }
  }
  
  auditResults.summary.schemasChecked++;
}

/**
 * Detect available migrations in GCS
 */
function detectMigrations() {
  try {
    const output = exec(
      `gsutil ls "gs://${GCS_BUCKET}/raw/backfill/" 2>/dev/null`,
      { throwOnError: false }
    );
    
    const migrations = [];
    for (const line of output.split('\n')) {
      const match = line.match(/migration=(\d+)/);
      if (match) {
        migrations.push(parseInt(match[1]));
      }
    }
    
    return [...new Set(migrations)].sort((a, b) => a - b);
  } catch {
    return [];
  }
}

/**
 * Cleanup temp files
 */
function cleanup() {
  try {
    if (existsSync(TMP_DIR)) {
      const files = readdirSync(TMP_DIR);
      for (const file of files) {
        unlinkSync(join(TMP_DIR, file));
      }
    }
  } catch {}
}

/**
 * Print final summary
 */
function printSummary() {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`📊 AUDIT SUMMARY`);
  console.log('═'.repeat(60));
  
  console.log(`\n   Bucket: gs://${GCS_BUCKET}`);
  console.log(`   Schemas Checked: ${auditResults.summary.schemasChecked}`);
  console.log(`   Missing Columns: ${auditResults.summary.missingColumns}`);
  console.log(`   Extra Columns: ${auditResults.summary.extraColumns}`);
  
  // Migration summary
  const migrationIds = Object.keys(auditResults.migrations);
  if (migrationIds.length > 0) {
    console.log(`\n   📁 MIGRATION COVERAGE:`);
    for (const mId of migrationIds) {
      const m = auditResults.migrations[mId];
      const updateCov = m.updates?.coverage || 'N/A';
      const eventCov = m.events?.coverage || 'N/A';
      const updateMissing = m.updates?.missingCount || 0;
      const eventMissing = m.events?.missingCount || 0;
      
      console.log(`      Migration ${mId}: Updates ${updateCov}% (${updateMissing} missing), Events ${eventCov}% (${eventMissing} missing)`);
    }
  }
  
  // ACS summary
  if (auditResults.acs?.contracts) {
    const acsCov = auditResults.acs.contracts.coverage;
    const acsMissing = auditResults.acs.contracts.missingCount;
    console.log(`\n   📁 ACS COVERAGE:`);
    console.log(`      Contracts: ${acsCov}% (${acsMissing} missing)`);
  }
  
  // Recommendations
  if (auditResults.summary.missingColumns > 0) {
    console.log(`\n   ⚠️ RECOMMENDATIONS:`);
    console.log(`      1. Missing columns will be NULL when queried with new schema`);
    console.log(`      2. Add COALESCE() wrappers in queries for graceful handling`);
    console.log(`      3. Consider re-ingesting if critical columns are missing`);
    console.log(`      4. Use BigQuery schema auto-detection for flexible loading`);
  } else {
    console.log(`\n   ✅ All schemas match expected structure!`);
  }
  
  console.log(`\n${'═'.repeat(60)}\n`);
}

/**
 * Main
 */
async function main() {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔍 GCS SCHEMA AUDIT`);
  console.log('═'.repeat(60));
  console.log(`\n   Bucket: gs://${GCS_BUCKET}`);
  console.log(`   Timestamp: ${auditResults.timestamp}`);
  console.log(`   Mode: ${ACS_ONLY ? 'ACS Only' : TARGET_MIGRATION !== null ? `Migration ${TARGET_MIGRATION}` : 'All'}`);
  
  if (!GCS_BUCKET) {
    console.error('\n❌ GCS_BUCKET environment variable not set');
    process.exit(1);
  }
  
  // Ensure tmp directory exists
  if (!existsSync(TMP_DIR)) {
    mkdirSync(TMP_DIR, { recursive: true });
  }
  
  try {
    // Audit backfill data
    if (!ACS_ONLY) {
      const migrations = TARGET_MIGRATION !== null 
        ? [TARGET_MIGRATION] 
        : detectMigrations();
      
      if (migrations.length === 0) {
        console.log('\n   ⚠️ No migrations found in GCS');
      } else {
        console.log(`\n   Found ${migrations.length} migration(s): ${migrations.join(', ')}`);
        
        for (const migrationId of migrations) {
          const result = await auditMigration(migrationId);
          auditResults.migrations[migrationId] = result;
        }
      }
    }
    
    // Audit ACS data
    if (!TARGET_MIGRATION) {
      auditResults.acs = await auditACS();
    }
    
    // Print summary
    printSummary();
    
  } finally {
    cleanup();
  }
}

main().catch(err => {
  console.error('\n❌ Audit failed:', err.message);
  process.exit(1);
});
