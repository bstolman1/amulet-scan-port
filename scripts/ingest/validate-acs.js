#!/usr/bin/env node
/**
 * ACS Snapshot Validation Script
 * 
 * Validates existing ACS parquet/JSONL files without re-fetching.
 * 
 * Checks performed:
 * 1. File integrity - validates all .jsonl files can be parsed
 * 2. Completion markers - checks for _COMPLETE files
 * 3. Field validation - ensures critical/important fields are populated
 * 4. Template validation - checks expected templates are present
 * 5. Duplicate detection - identifies potential duplicate contracts
 * 
 * Usage:
 *   node validate-acs.js                    # Validate all snapshots
 *   node validate-acs.js --migration 4      # Validate specific migration
 *   node validate-acs.js --quick            # Quick scan (stats only)
 *   node validate-acs.js --verbose          # Show detailed output
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import readline from 'readline';
import { 
  CRITICAL_CONTRACT_FIELDS, 
  IMPORTANT_CONTRACT_FIELDS, 
  validateContractFields, 
  validateTemplates,
  normalizeTemplateKey 
} from './acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const BASE_DATA_DIR = process.env.DATA_DIR || '/home/bstolz/canton-explorer/data';
const RAW_DIR = process.env.RAW_DIR || path.join(BASE_DATA_DIR, 'raw');
const ACS_DIR = path.join(RAW_DIR, 'acs');

// Parse CLI args
const args = process.argv.slice(2);
const targetMigration = args.includes('--migration') 
  ? parseInt(args[args.indexOf('--migration') + 1]) 
  : null;
const quickMode = args.includes('--quick');
const verbose = args.includes('--verbose') || args.includes('-v');

/**
 * Results accumulator
 */
const results = {
  snapshotsFound: 0,
  snapshotsComplete: 0,
  snapshotsIncomplete: 0,
  filesScanned: 0,
  filesCorrupted: 0,
  totalContracts: 0,
  duplicatesFound: 0,
  errors: [],
  startTime: Date.now(),
  // Per-snapshot details
  snapshots: [],
  // Field validation
  fieldValidation: {
    total: 0,
    missingFields: {},
    sampleMissing: [],
  },
  // Template counts across all snapshots
  templateCounts: {},
};

/**
 * Find all ACS snapshot directories
 */
function findSnapshots(migrationFilter = null) {
  const snapshots = [];
  
  if (!fs.existsSync(ACS_DIR)) {
    console.log(`⚠️ No ACS directory found: ${ACS_DIR}`);
    return snapshots;
  }
  
  function scanDir(dir, pathParts = {}) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      
      if (entry.isDirectory()) {
        // Parse partition parts
        const newParts = { ...pathParts };
        
        if (entry.name.startsWith('migration=')) {
          newParts.migrationId = parseInt(entry.name.split('=')[1]);
          // Apply migration filter
          if (migrationFilter !== null && newParts.migrationId !== migrationFilter) {
            continue;
          }
        } else if (entry.name.startsWith('year=')) {
          newParts.year = entry.name.split('=')[1];
        } else if (entry.name.startsWith('month=')) {
          newParts.month = entry.name.split('=')[1];
        } else if (entry.name.startsWith('day=')) {
          newParts.day = entry.name.split('=')[1];
        } else if (entry.name.startsWith('snapshot=')) {
          newParts.snapshotId = entry.name.split('=')[1];
          
          // This is a snapshot directory
          snapshots.push({
            path: fullPath,
            ...newParts,
            hasComplete: fs.existsSync(path.join(fullPath, '_COMPLETE')),
          });
          continue; // Don't recurse into snapshot dirs
        }
        
        scanDir(fullPath, newParts);
      }
    }
  }
  
  scanDir(ACS_DIR);
  return snapshots;
}

/**
 * Read JSONL file line by line (memory-efficient)
 */
async function readJsonlFile(filePath) {
  const records = [];
  
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });
  
  for await (const line of rl) {
    if (line.trim()) {
      try {
        records.push(JSON.parse(line));
      } catch (err) {
        throw new Error(`Invalid JSON at line ${records.length + 1}: ${err.message}`);
      }
    }
  }
  
  return records;
}

/**
 * Validate a single snapshot directory
 */
async function validateSnapshot(snapshot, quickScan = false) {
  const result = {
    path: snapshot.path,
    migrationId: snapshot.migrationId,
    snapshotId: snapshot.snapshotId,
    date: `${snapshot.year}-${snapshot.month}-${snapshot.day}`,
    hasComplete: snapshot.hasComplete,
    completionStats: null,
    files: 0,
    contracts: 0,
    templateCounts: {},
    fieldIssues: { critical: 0, important: 0 },
    errors: [],
    valid: true,
  };
  
  // Read completion marker if exists
  if (snapshot.hasComplete) {
    try {
      const markerPath = path.join(snapshot.path, '_COMPLETE');
      const markerContent = await readJsonlFile(markerPath);
      if (markerContent.length > 0) {
        result.completionStats = markerContent[0];
      }
    } catch (err) {
      result.errors.push(`Failed to read completion marker: ${err.message}`);
    }
  }
  
  // Find JSONL files
  const entries = fs.readdirSync(snapshot.path, { withFileTypes: true });
  const jsonlFiles = entries
    .filter(e => e.isFile() && e.name.endsWith('.jsonl') && e.name !== '_COMPLETE')
    .map(e => path.join(snapshot.path, e.name));
  
  result.files = jsonlFiles.length;
  
  if (quickScan) {
    // Just count files, don't read contents
    return result;
  }
  
  // Validate each file
  const contractIds = new Set();
  
  for (const filePath of jsonlFiles) {
    try {
      const contracts = await readJsonlFile(filePath);
      results.filesScanned++;
      
      for (const contract of contracts) {
        result.contracts++;
        results.totalContracts++;
        
        // Check for duplicates
        const contractId = contract.contract_id;
        if (contractId) {
          if (contractIds.has(contractId)) {
            results.duplicatesFound++;
          } else {
            contractIds.add(contractId);
          }
        }
        
        // Template counting
        const templateId = contract.template_id || 'unknown';
        result.templateCounts[templateId] = (result.templateCounts[templateId] || 0) + 1;
        results.templateCounts[templateId] = (results.templateCounts[templateId] || 0) + 1;
        
        // Field validation
        results.fieldValidation.total++;
        const { missingCritical, missingImportant } = validateContractFields(contract);
        
        if (missingCritical.length > 0) {
          result.fieldIssues.critical++;
          for (const field of missingCritical) {
            results.fieldValidation.missingFields[field] = 
              (results.fieldValidation.missingFields[field] || 0) + 1;
          }
          
          // Sample missing
          if (results.fieldValidation.sampleMissing.length < 5) {
            results.fieldValidation.sampleMissing.push({
              file: path.basename(filePath),
              snapshot: snapshot.snapshotId,
              missingFields: missingCritical,
              contract: {
                contract_id: contract.contract_id,
                template_id: contract.template_id,
                migration_id: contract.migration_id,
              },
            });
          }
        }
        
        if (missingImportant.length > 0) {
          result.fieldIssues.important++;
        }
      }
    } catch (err) {
      results.filesCorrupted++;
      result.errors.push(`File ${path.basename(filePath)}: ${err.message}`);
      result.valid = false;
    }
  }
  
  return result;
}

/**
 * Format duration
 */
function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
  return `${(ms / 3600000).toFixed(1)}h`;
}

/**
 * Print validation report
 */
function printReport() {
  const elapsed = ((Date.now() - results.startTime) / 1000).toFixed(1);
  
  console.log('\n' + '═'.repeat(80));
  console.log('📊 ACS SNAPSHOT VALIDATION REPORT');
  console.log('═'.repeat(80));
  
  // Snapshot summary
  console.log('\n📁 SNAPSHOT OVERVIEW:');
  console.log(`   Snapshots found:      ${results.snapshotsFound}`);
  console.log(`   Snapshots complete:   ${results.snapshotsComplete}`);
  console.log(`   Snapshots incomplete: ${results.snapshotsIncomplete}`);
  console.log(`   Files scanned:        ${results.filesScanned}`);
  console.log(`   Files corrupted:      ${results.filesCorrupted}`);
  console.log(`   Total contracts:      ${results.totalContracts.toLocaleString()}`);
  
  // Per-snapshot details
  if (results.snapshots.length > 0) {
    console.log('\n📋 SNAPSHOT DETAILS:');
    for (const snap of results.snapshots) {
      const status = snap.hasComplete ? '✅' : '⚠️';
      console.log(`   ${status} Migration ${snap.migrationId} - ${snap.date} (${snap.snapshotId})`);
      console.log(`      Files: ${snap.files}, Contracts: ${snap.contracts.toLocaleString()}`);
      
      if (snap.fieldIssues.critical > 0) {
        console.log(`      ❌ Critical field issues: ${snap.fieldIssues.critical}`);
      }
      if (snap.errors.length > 0) {
        console.log(`      ❌ Errors: ${snap.errors.length}`);
        if (verbose) {
          for (const err of snap.errors) {
            console.log(`         • ${err}`);
          }
        }
      }
    }
  }
  
  // Field validation
  console.log('\n🔍 FIELD VALIDATION:');
  const fv = results.fieldValidation;
  const missingFields = Object.entries(fv.missingFields).sort((a, b) => b[1] - a[1]);
  
  if (missingFields.length === 0) {
    console.log('   ✅ All critical fields present');
  } else {
    console.log(`   Records validated: ${fv.total.toLocaleString()}`);
    for (const [field, count] of missingFields.slice(0, 10)) {
      const pct = fv.total > 0 ? ((count / fv.total) * 100).toFixed(2) : 0;
      const isCritical = CRITICAL_CONTRACT_FIELDS.includes(field);
      console.log(`   ${isCritical ? '❌' : '⚠️'} ${field}: ${count.toLocaleString()} missing (${pct}%)`);
    }
  }
  
  // Sample missing
  if (verbose && fv.sampleMissing.length > 0) {
    console.log('\n   Sample contracts with missing critical fields:');
    for (const sample of fv.sampleMissing) {
      console.log(`      • Snapshot: ${sample.snapshot}, File: ${sample.file}`);
      console.log(`        Missing: ${sample.missingFields.join(', ')}`);
      console.log(`        Contract: ${JSON.stringify(sample.contract)}`);
    }
  }
  
  // Template validation
  console.log('\n📋 TEMPLATE VALIDATION:');
  const templateValidation = validateTemplates(results.templateCounts);
  
  console.log(`   Found: ${templateValidation.found.length} expected templates`);
  console.log(`   Missing: ${templateValidation.missing.length} templates`);
  console.log(`   Unexpected: ${templateValidation.unexpected.length} templates`);
  
  if (templateValidation.warnings.length > 0) {
    console.log('\n   Warnings:');
    for (const w of templateValidation.warnings) {
      console.log(`      ${w}`);
    }
  }
  
  if (verbose && templateValidation.found.length > 0) {
    console.log('\n   Found templates:');
    for (const t of templateValidation.found.slice(0, 20)) {
      console.log(`      • ${t.key}: ${t.count.toLocaleString()} contracts`);
    }
  }
  
  if (templateValidation.unexpected.length > 0) {
    console.log('\n   Unexpected templates (consider adding to registry):');
    for (const t of templateValidation.unexpected.slice(0, 10)) {
      console.log(`      • ${t.key}: ${t.count.toLocaleString()} contracts`);
    }
    if (templateValidation.unexpected.length > 10) {
      console.log(`      ... and ${templateValidation.unexpected.length - 10} more`);
    }
  }
  
  // Duplicates
  console.log('\n🔄 DUPLICATES:');
  if (results.duplicatesFound === 0) {
    console.log('   ✅ No duplicates detected');
  } else {
    console.log(`   ⚠️ Found ${results.duplicatesFound} duplicate contract(s)`);
  }
  
  // Errors
  if (results.errors.length > 0) {
    console.log('\n❌ ERRORS:');
    for (const err of results.errors.slice(0, 10)) {
      console.log(`   • ${err}`);
    }
    if (results.errors.length > 10) {
      console.log(`   ... and ${results.errors.length - 10} more`);
    }
  }
  
  console.log('\n' + '─'.repeat(80));
  console.log(`⏱️ Validation completed in ${elapsed}s`);
  console.log('═'.repeat(80) + '\n');
  
  // Exit code based on issues
  const criticalMissing = CRITICAL_CONTRACT_FIELDS
    .some(f => (fv.missingFields[f] || 0) > 0);
  const requiredTemplateMissing = templateValidation.missing
    .some(t => t.required);
  
  const hasIssues = results.filesCorrupted > 0 ||
                   results.snapshotsIncomplete > 0 ||
                   criticalMissing ||
                   requiredTemplateMissing;
  
  return hasIssues ? 1 : 0;
}

/**
 * Main validation function
 */
async function runValidation() {
  console.log('\n' + '═'.repeat(80));
  console.log('🔍 ACS SNAPSHOT VALIDATION');
  console.log('═'.repeat(80));
  console.log(`   Data directory: ${BASE_DATA_DIR}`);
  console.log(`   ACS directory:  ${ACS_DIR}`);
  console.log(`   Mode:           ${quickMode ? 'Quick scan' : 'Full validation'}`);
  if (targetMigration) {
    console.log(`   Target migration: ${targetMigration}`);
  }
  console.log('═'.repeat(80) + '\n');
  
  // Find snapshots
  console.log('📁 Scanning for snapshots...');
  const snapshots = findSnapshots(targetMigration);
  results.snapshotsFound = snapshots.length;
  console.log(`   Found ${snapshots.length} snapshot(s)`);
  
  if (snapshots.length === 0) {
    console.log('\n⚠️ No snapshots found to validate');
    process.exit(0);
  }
  
  // Validate each snapshot
  console.log('\n🔄 Validating snapshots...');
  let progress = 0;
  
  for (const snapshot of snapshots) {
    const result = await validateSnapshot(snapshot, quickMode);
    results.snapshots.push(result);
    
    if (result.hasComplete) {
      results.snapshotsComplete++;
    } else {
      results.snapshotsIncomplete++;
    }
    
    progress++;
    process.stdout.write(`\r   Progress: ${progress}/${snapshots.length} snapshots`);
  }
  console.log('');
  
  // Print report
  const exitCode = printReport();
  process.exit(exitCode);
}

// Run validation
runValidation().catch(err => {
  console.error('\n❌ FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
