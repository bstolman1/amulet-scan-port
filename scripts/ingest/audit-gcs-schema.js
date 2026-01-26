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
 * Uses -noheader and -list mode for reliable parsing
 */
function getParquetSchema(filePath) {
  try {
    // Use -list mode with | separator for reliable parsing
    const output = exec(
      `duckdb -list -separator '|' -noheader -c "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM '${filePath}')"`
    );
    
    const lines = output.trim().split('\n');
    const schema = {};
    
    for (const line of lines) {
      // Skip empty lines and box-drawing characters
      if (!line || line.includes('─') || line.includes('┼') || line.includes('┤') || line.includes('├')) {
        continue;
      }
      
      // Parse pipe-separated values
      const parts = line.split('|').map(s => s.trim());
      if (parts.length >= 2) {
        const name = parts[0];
        const type = parts[1];
        // Skip header row if it slipped through
        if (name && name !== 'column_name' && !name.includes('varchar')) {
          schema[name] = type;
        }
      }
    }
    
    return schema;
  } catch (err) {
    console.error(`  ❌ Failed to read schema: ${err.message}`);
    return null;
  }
}

/**
 * Get data population stats for each column in a Parquet file
 * Returns count of non-null values for each column
 */
function getColumnPopulation(filePath, columns) {
  try {
    const results = {};
    const columnList = columns.slice(0, 20); // Limit to avoid huge queries
    
    // Build a query that counts non-null values for each column
    const countExprs = columnList.map(col => 
      `COUNT("${col}") AS "${col}_count", COUNT(*) - COUNT("${col}") AS "${col}_nulls"`
    ).join(', ');
    
    const output = exec(
      `duckdb -list -separator '|' -noheader -c "SELECT ${countExprs} FROM '${filePath}' LIMIT 10000"`,
      { throwOnError: false }
    );
    
    if (!output || output.includes('Error')) {
      // Fallback: just get row count and sample values
      const rowCountOutput = exec(
        `duckdb -list -separator '|' -noheader -c "SELECT COUNT(*) FROM '${filePath}'"`,
        { throwOnError: false }
      );
      const totalRows = parseInt(rowCountOutput?.trim() || '0');
      
      return { 
        totalRows, 
        columns: {},
        error: 'Could not get detailed column stats'
      };
    }
    
    const values = output.trim().split('|');
    let totalRows = 0;
    
    for (let i = 0; i < columnList.length; i++) {
      const countIdx = i * 2;
      const nullIdx = i * 2 + 1;
      const count = parseInt(values[countIdx] || '0');
      const nulls = parseInt(values[nullIdx] || '0');
      const total = count + nulls;
      if (total > totalRows) totalRows = total;
      
      results[columnList[i]] = {
        populated: count,
        nulls: nulls,
        populationRate: total > 0 ? ((count / total) * 100).toFixed(1) : '0.0'
      };
    }
    
    return { totalRows, columns: results };
  } catch (err) {
    return { totalRows: 0, columns: {}, error: err.message };
  }
}

/**
 * Get sample values for critical columns
 */
function getSampleValues(filePath, columns) {
  try {
    const criticalCols = columns.filter(c => 
      ['event_id', 'update_id', 'contract_id', 'template_id', 'payload', 'raw_event', 'raw'].includes(c)
    ).slice(0, 5);
    
    if (criticalCols.length === 0) return {};
    
    const selectExpr = criticalCols.map(c => `"${c}"`).join(', ');
    const output = exec(
      `duckdb -list -separator '|' -noheader -c "SELECT ${selectExpr} FROM '${filePath}' WHERE \"${criticalCols[0]}\" IS NOT NULL LIMIT 1"`,
      { throwOnError: false }
    );
    
    if (!output) return {};
    
    const values = output.trim().split('|');
    const samples = {};
    criticalCols.forEach((col, i) => {
      const val = values[i] || '';
      // Truncate long values
      samples[col] = val.length > 80 ? val.substring(0, 80) + '...' : val;
    });
    
    return samples;
  } catch {
    return {};
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
 * List files matching a specific prefix pattern
 */
function listGCSFilesWithPrefix(basePath, filePrefix, limit = 5) {
  if (!GCS_BUCKET) {
    throw new Error('GCS_BUCKET not set');
  }
  
  try {
    // Use glob pattern to search across all date partitions
    const output = exec(
      `gsutil ls "gs://${GCS_BUCKET}/${basePath}**/${filePrefix}*.parquet" 2>/dev/null | head -${limit}`,
      { throwOnError: false }
    );
    return output.trim().split('\n').filter(line => line.includes('.parquet'));
  } catch {
    return [];
  }
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
    dataPopulation: {},
    sampleFiles: [],
  };
  
  // Find update files SPECIFICALLY from the updates folder (nested under backfill/)
  const updateFileList = listGCSFilesWithPrefix(`raw/backfill/updates/migration=${migrationId}/`, 'updates-', 3);
  const updateFile = updateFileList[0];
  
  if (updateFile) {
    console.log(`\n📄 Updates Schema (sampled from ${updateFile.split('/').pop()})`);
    const localPath = downloadFile(updateFile, `updates-m${migrationId}.parquet`);
    const schema = getParquetSchema(localPath);
    
    if (schema) {
      result.updates = compareSchemas(schema, LEDGER_UPDATES_SCHEMA, 'updates');
      result.sampleFiles.push(updateFile);
      
      printSchemaComparison(result.updates, schema, LEDGER_UPDATES_SCHEMA);
      
      // Check data population
      console.log(`\n   📊 DATA POPULATION CHECK:`);
      const population = getColumnPopulation(localPath, Object.keys(schema));
      result.dataPopulation.updates = population;
      printDataPopulation(population, 'updates');
      
      // Show sample values
      const samples = getSampleValues(localPath, Object.keys(schema));
      if (Object.keys(samples).length > 0) {
        console.log(`\n   🔍 SAMPLE VALUES:`);
        for (const [col, val] of Object.entries(samples)) {
          console.log(`      ${col}: ${val || '(empty)'}`);
        }
      }
    }
  } else {
    console.log(`\n  ⚠️ No update files found for migration ${migrationId}`);
  }
  
  // Find event files SPECIFICALLY from the events folder (nested under backfill/)
  const eventFileList = listGCSFilesWithPrefix(`raw/backfill/events/migration=${migrationId}/`, 'events-', 3);
  const eventFile = eventFileList[0];
  
  if (eventFile) {
    console.log(`\n📄 Events Schema (sampled from ${eventFile.split('/').pop()})`);
    const localPath = downloadFile(eventFile, `events-m${migrationId}.parquet`);
    const schema = getParquetSchema(localPath);
    
    if (schema) {
      result.events = compareSchemas(schema, LEDGER_EVENTS_SCHEMA, 'events');
      result.sampleFiles.push(eventFile);
      
      printSchemaComparison(result.events, schema, LEDGER_EVENTS_SCHEMA);
      
      // Check data population
      console.log(`\n   📊 DATA POPULATION CHECK:`);
      const population = getColumnPopulation(localPath, Object.keys(schema));
      result.dataPopulation.events = population;
      printDataPopulation(population, 'events');
      
      // Show sample values
      const samples = getSampleValues(localPath, Object.keys(schema));
      if (Object.keys(samples).length > 0) {
        console.log(`\n   🔍 SAMPLE VALUES:`);
        for (const [col, val] of Object.entries(samples)) {
          console.log(`      ${col}: ${val || '(empty)'}`);
        }
      }
    }
  } else {
    console.log(`\n  ⚠️ No event files found for migration ${migrationId}`);
  }
  
  return result;
}

/**
 * Print data population stats
 */
function printDataPopulation(population, label) {
  if (population.error) {
    console.log(`      ⚠️ ${population.error}`);
    return;
  }
  
  console.log(`      Total rows sampled: ${population.totalRows}`);
  
  const columns = population.columns;
  const populated = [];
  const sparse = [];
  const empty = [];
  
  for (const [col, stats] of Object.entries(columns)) {
    const rate = parseFloat(stats.populationRate);
    if (rate >= 90) {
      populated.push({ col, rate });
    } else if (rate > 0) {
      sparse.push({ col, rate, nulls: stats.nulls });
    } else {
      empty.push(col);
    }
  }
  
  if (populated.length > 0) {
    console.log(`      ✅ Well-populated (>90%): ${populated.length} columns`);
  }
  
  if (sparse.length > 0) {
    console.log(`      ⚠️ SPARSE COLUMNS (has data but <90%):`);
    for (const { col, rate, nulls } of sparse.slice(0, 10)) {
      console.log(`         - ${col}: ${rate}% populated (${nulls} nulls)`);
    }
    if (sparse.length > 10) {
      console.log(`         ... and ${sparse.length - 10} more`);
    }
  }
  
  if (empty.length > 0) {
    console.log(`      ❌ EMPTY COLUMNS (0% populated):`);
    for (const col of empty.slice(0, 10)) {
      console.log(`         - ${col}`);
    }
    if (empty.length > 10) {
      console.log(`         ... and ${empty.length - 10} more`);
    }
  }
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
    dataPopulation: {},
    sampleFiles: [],
  };
  
  // Find ACS contract files specifically
  const acsFiles = listGCSFilesWithPrefix(`raw/acs/`, 'contracts-', 5);
  const contractFile = acsFiles[0];
  
  if (contractFile) {
    console.log(`\n📄 ACS Contracts Schema (sampled from ${contractFile.split('/').pop()})`);
    const localPath = downloadFile(contractFile, `acs-sample.parquet`);
    const schema = getParquetSchema(localPath);
    
    if (schema) {
      result.contracts = compareSchemas(schema, ACS_CONTRACTS_SCHEMA, 'acs_contracts');
      result.sampleFiles.push(contractFile);
      
      printSchemaComparison(result.contracts, schema, ACS_CONTRACTS_SCHEMA);
      
      // Check data population
      console.log(`\n   📊 DATA POPULATION CHECK:`);
      const population = getColumnPopulation(localPath, Object.keys(schema));
      result.dataPopulation.contracts = population;
      printDataPopulation(population, 'contracts');
      
      // Show sample values
      const samples = getSampleValues(localPath, Object.keys(schema));
      if (Object.keys(samples).length > 0) {
        console.log(`\n   🔍 SAMPLE VALUES:`);
        for (const [col, val] of Object.entries(samples)) {
          console.log(`      ${col}: ${val || '(empty)'}`);
        }
      }
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
 * Searches within both updates/ and events/ subdirectories
 */
function detectMigrations() {
  try {
    const migrations = new Set();
    
    // Check both updates and events subdirectories for migration folders
    for (const subdir of ['updates', 'events']) {
      const output = exec(
        `gsutil ls "gs://${GCS_BUCKET}/raw/backfill/${subdir}/" 2>/dev/null`,
        { throwOnError: false }
      );
      
      for (const line of output.split('\n')) {
        const match = line.match(/migration=(\d+)/);
        if (match) {
          migrations.add(parseInt(match[1]));
        }
      }
    }
    
    return [...migrations].sort((a, b) => a - b);
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
