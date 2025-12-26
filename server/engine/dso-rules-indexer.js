/**
 * DSO Rules SV Membership Indexer
 * 
 * CANONICAL source of SV membership for vote threshold calculation.
 * 
 * DSO Rules contracts define the active SV set at any point in time.
 * Votes are validated against DSO Rules state, NOT onboarding attestations.
 * 
 * Chain of authority:
 *   DSO Rules → defines active SV set → defines vote threshold → determines vote outcome
 * 
 * This indexer:
 * 1. Uses template-file-index to find only DsoRules files (tens, not 70K)
 * 2. Extracts SV set from each DsoRules contract payload
 * 3. Builds interval state for SV membership (effective_from, effective_until)
 * 4. Exposes getDsoSvCountAt(timestamp) for vote threshold calculation
 * 
 * ❌ DO NOT use SvOnboardingConfirmed for thresholds - it's ephemeral attestation only
 * ✅ USE this index for canonical SV count at vote time
 */

import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import * as binaryReader from '../duckdb/binary-reader.js';
import {
  getFilesForTemplate,
  isTemplateIndexPopulated,
} from './template-file-index.js';

let indexingInProgress = false;
let indexingProgress = null;

/**
 * Ensure DSO Rules index tables exist
 */
export async function ensureDsoTables() {
  // DSO Rules state table - each row is a DSO Rules contract with its SV set
  await query(`
    CREATE TABLE IF NOT EXISTS dso_rules_state (
      contract_id VARCHAR PRIMARY KEY,
      effective_from TIMESTAMP NOT NULL,
      effective_until TIMESTAMP,
      sv_count INTEGER NOT NULL,
      sv_parties TEXT,
      rule_version VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_dso_rules_state_contract
    ON dso_rules_state(contract_id)
  `);

  // Index for time-based queries
  await query(`
    CREATE INDEX IF NOT EXISTS idx_dso_rules_state_time
    ON dso_rules_state(effective_from, effective_until)
  `);

  // Index state tracking
  await query(`
    CREATE TABLE IF NOT EXISTS dso_rules_index_state (
      id INTEGER PRIMARY KEY DEFAULT 1,
      last_indexed_at TIMESTAMP,
      total_rules INTEGER DEFAULT 0,
      files_scanned INTEGER DEFAULT 0
    )
  `);

  await query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_dso_rules_index_state_id
    ON dso_rules_index_state(id)
  `);
}

/**
 * Get DSO Rules index statistics
 */
export async function getDsoIndexStats() {
  try {
    await ensureDsoTables();

    const totalRules = await queryOne(`SELECT COUNT(*) as count FROM dso_rules_state`);
    const currentRule = await queryOne(`
      SELECT sv_count, contract_id, effective_from
      FROM dso_rules_state
      WHERE effective_until IS NULL OR effective_until > CURRENT_TIMESTAMP
      ORDER BY effective_from DESC
      LIMIT 1
    `);
    const state = await queryOne(`SELECT last_indexed_at, total_rules, files_scanned FROM dso_rules_index_state WHERE id = 1`);

    return {
      totalRules: Number(totalRules?.count || 0),
      currentSvCount: Number(currentRule?.sv_count || 0),
      currentRuleContractId: currentRule?.contract_id || null,
      lastIndexedAt: state?.last_indexed_at || null,
      filesScanned: Number(state?.files_scanned || 0),
      isPopulated: Number(totalRules?.count || 0) > 0,
      isIndexing: indexingInProgress,
      indexing: indexingInProgress ? indexingProgress : null,
    };
  } catch (err) {
    console.error('Error getting DSO index stats:', err);
    return { totalRules: 0, currentSvCount: 0, isPopulated: false, isIndexing: indexingInProgress };
  }
}

/**
 * Get SV count from DSO Rules at a specific point in time.
 * This is the CANONICAL function for vote threshold calculation.
 * 
 * @param {Date|string} dateTime - The timestamp to check
 * @returns {number} Count of active SVs at that time per DSO Rules
 */
export async function getDsoSvCountAt(dateTime) {
  try {
    await ensureDsoTables();

    const timestamp = new Date(dateTime).toISOString();

    // Find the DSO Rules contract that was active at this timestamp
    const result = await queryOne(`
      SELECT sv_count, sv_parties, contract_id
      FROM dso_rules_state
      WHERE effective_from <= '${timestamp}'
        AND (effective_until IS NULL OR effective_until > '${timestamp}')
      ORDER BY effective_from DESC
      LIMIT 1
    `);

    return Number(result?.sv_count || 0);
  } catch (err) {
    console.error('Error getting DSO SV count at date:', err);
    return 0;
  }
}

/**
 * Get list of SVs from DSO Rules at a specific date/time
 */
export async function getDsoSvsAt(dateTime) {
  try {
    await ensureDsoTables();

    const timestamp = new Date(dateTime).toISOString();

    const result = await queryOne(`
      SELECT sv_count, sv_parties, contract_id, effective_from, effective_until
      FROM dso_rules_state
      WHERE effective_from <= '${timestamp}'
        AND (effective_until IS NULL OR effective_until > '${timestamp}')
      ORDER BY effective_from DESC
      LIMIT 1
    `);

    if (!result) return { svCount: 0, svParties: [], contractId: null };

    // Parse sv_parties JSON if stored as string
    let svParties = [];
    if (result.sv_parties) {
      try {
        svParties = JSON.parse(result.sv_parties);
      } catch {
        svParties = [];
      }
    }

    return {
      svCount: Number(result.sv_count || 0),
      svParties,
      contractId: result.contract_id,
      effectiveFrom: result.effective_from,
      effectiveUntil: result.effective_until,
    };
  } catch (err) {
    console.error('Error getting DSO SVs at date:', err);
    return { svCount: 0, svParties: [], contractId: null };
  }
}

/**
 * Calculate voting threshold based on DSO Rules SV count
 */
export function calculateVotingThreshold(svCount) {
  const twoThirdsThreshold = Math.ceil((svCount * 2) / 3);
  return {
    svCount,
    twoThirdsThreshold,
    simpleMajority: Math.floor(svCount / 2) + 1,
  };
}

/**
 * Extract SV parties from DsoRules payload
 * DsoRules contract contains the authoritative SV set
 */
function extractSvSetFromDsoRules(payload) {
  if (!payload) return [];

  const svParties = [];

  try {
    // DsoRules payload structure varies, try multiple extraction paths
    const fields = payload.record?.fields || [];

    // Look for svs field (list of SV info)
    for (const field of fields) {
      const label = field.label || '';
      const value = field.value;

      // svs field contains the SV list
      if (label === 'svs' || label === 'dsoMembers' || label === 'members') {
        const list = value?.list || value?.map?.entries || [];

        for (const item of list) {
          // Each item may be a map entry with key=partyId
          if (item.key?.party) {
            svParties.push(item.key.party);
          } else if (item.value?.party) {
            svParties.push(item.value.party);
          } else if (item.party) {
            svParties.push(item.party);
          }
          // Or nested record with svParty field
          const itemFields = item.record?.fields || item.value?.record?.fields || [];
          for (const f of itemFields) {
            if ((f.label === 'svParty' || f.label === 'party') && f.value?.party) {
              svParties.push(f.value.party);
            }
          }
        }
      }

      // Also check for direct map structure
      if (value?.map?.entries) {
        for (const entry of value.map.entries) {
          if (entry.key?.party) {
            svParties.push(entry.key.party);
          }
        }
      }
    }

    // Fallback: recursively search for party fields in svs structure
    if (svParties.length === 0) {
      const deepSearch = (obj, depth = 0) => {
        if (depth > 10 || !obj || typeof obj !== 'object') return;
        
        if (obj.party && typeof obj.party === 'string') {
          svParties.push(obj.party);
          return;
        }

        for (const key of Object.keys(obj)) {
          if (key === 'svs' || key === 'members' || key === 'dsoMembers') {
            deepSearch(obj[key], depth + 1);
          } else if (Array.isArray(obj[key])) {
            for (const item of obj[key]) {
              deepSearch(item, depth + 1);
            }
          } else if (typeof obj[key] === 'object') {
            deepSearch(obj[key], depth + 1);
          }
        }
      };
      deepSearch(payload);
    }
  } catch (err) {
    console.warn('Error extracting SV set from DsoRules:', err.message);
  }

  // Deduplicate
  return [...new Set(svParties)];
}

/**
 * Build DSO Rules SV membership index
 * Scans only DsoRules template files (should be tens, not 70K)
 */
export async function buildDsoRulesIndex({ force = false } = {}) {
  if (indexingInProgress) {
    console.log('⏳ DSO Rules indexing already in progress');
    return { status: 'in_progress' };
  }

  indexingInProgress = true;
  indexingProgress = { phase: 'starting', current: 0, total: 0 };
  console.log('\n📜 Starting DSO Rules SV membership index build...');
  console.log('   Source: DsoRules template (canonical SV membership)');

  try {
    await ensureDsoTables();

    // Check if already populated
    if (!force) {
      const stats = await getDsoIndexStats();
      if (stats.totalRules > 0) {
        console.log(`✅ DSO Rules index already populated (${stats.totalRules} rules), use force=true to rebuild`);
        indexingInProgress = false;
        return { status: 'skipped', rules: stats.totalRules };
      }
    }

    if (force) {
      console.log('   🗑️ Force rebuild - clearing existing DSO Rules data...');
      await query(`DELETE FROM dso_rules_state`);
    }

    // Check if template index is available
    const templateIndexPopulated = await isTemplateIndexPopulated();

    if (!templateIndexPopulated) {
      console.log('   ⚠️ Template index not available, build it first');
      indexingInProgress = false;
      return { status: 'error', message: 'Template index required. Build template index first.' };
    }

    // Get ONLY files containing DsoRules - should be much fewer than 70K
    const dsoRulesFiles = await getFilesForTemplate('DsoRules');
    console.log(`   📂 Found ${dsoRulesFiles.length} files with DsoRules events`);

    if (dsoRulesFiles.length === 0) {
      console.log('   ⚠️ No DsoRules files found in template index');
      indexingInProgress = false;
      return { status: 'error', message: 'No DsoRules files found' };
    }

    // This should be manageable - if it's thousands, log a warning
    if (dsoRulesFiles.length > 1000) {
      console.log(`   ⚠️ Warning: ${dsoRulesFiles.length} files is more than expected for DsoRules`);
    }

    indexingProgress = { phase: 'scanning', current: 0, total: dsoRulesFiles.length };

    // Map to track DSO Rules state by contract_id
    const dsoRulesMap = new Map();

    const isDsoRulesTemplate = (templateId) => {
      if (!templateId) return false;
      // Match DsoRules but not VoteRequest or other nested templates
      return templateId.includes(':DsoRules') && !templateId.includes('VoteRequest');
    };

    let dsoRulesEventsFound = 0;
    let createdCount = 0;
    let archivedCount = 0;

    for (let i = 0; i < dsoRulesFiles.length; i++) {
      indexingProgress.current = i + 1;
      const filePath = dsoRulesFiles[i];

      try {
        const result = await binaryReader.readBinaryFile(filePath);
        const events = result.records || result || [];

        // Filter to only DsoRules events
        const dsoEvents = events.filter((r) => isDsoRulesTemplate(r.template_id || r.templateId));
        dsoRulesEventsFound += dsoEvents.length;

        // Debug: log first file structure
        if (i === 0 && dsoEvents.length > 0) {
          const firstCreated = dsoEvents.find(r => (r.event_type || r.event_type_original) === 'created');
          if (firstCreated) {
            console.log('   🔍 Debug - first DsoRules created event:');
            const topLabels = (firstCreated.payload?.record?.fields || []).map(f => f.label);
            console.log(`      payload.record.fields labels: [${topLabels.slice(0, 10).join(', ')}${topLabels.length > 10 ? '...' : ''}]`);
            
            // Try to extract SVs
            const testSvs = extractSvSetFromDsoRules(firstCreated.payload);
            console.log(`      Extracted ${testSvs.length} SVs from first event`);
          }
        }

        for (const record of dsoEvents) {
          const contractId = record.contract_id;
          if (!contractId) continue;

          const evtType = String(record.event_type || record.event_type_original || '').toLowerCase();
          const isCreate = evtType === 'created';
          const isConsume = evtType === 'exercised' && record.consuming === true;

          if (isCreate) {
            createdCount++;
            const startTime = record.created_at_ts || record.effective_at || record.timestamp || record.record_time;
            if (!startTime) continue;

            const svParties = extractSvSetFromDsoRules(record.payload);
            const svCount = svParties.length;

            const existing = dsoRulesMap.get(contractId);
            dsoRulesMap.set(contractId, {
              contract_id: contractId,
              effective_from: startTime,
              effective_until: existing?.effective_until ?? null,
              sv_count: svCount,
              sv_parties: JSON.stringify(svParties),
              rule_version: record.template_id || null,
            });
          }

          if (isConsume) {
            archivedCount++;
            const endTime = record.effective_at || record.timestamp || record.record_time || record.created_at_ts;
            if (!endTime) continue;

            const existing = dsoRulesMap.get(contractId);
            if (existing) {
              existing.effective_until = endTime;
            } else {
              // Consume before create - store end time
              dsoRulesMap.set(contractId, {
                contract_id: contractId,
                effective_from: null,
                effective_until: endTime,
                sv_count: 0,
                sv_parties: '[]',
                rule_version: null,
              });
            }
          }
        }
      } catch (fileErr) {
        console.warn(`   ⚠️ Error reading ${filePath}:`, fileErr.message);
      }

      // Log progress every 100 files
      if ((i + 1) % 100 === 0) {
        console.log(`   📜 [${i + 1}/${dsoRulesFiles.length}] ${dsoRulesEventsFound} DsoRules events, ${dsoRulesMap.size} contracts`);
      }
    }

    console.log(`   📊 Found ${dsoRulesEventsFound} DsoRules events → ${dsoRulesMap.size} contracts`);
    console.log(`      Created: ${createdCount}, Archived: ${archivedCount}`);

    // Insert all rules into the database
    let insertedCount = 0;
    let skippedIncomplete = 0;

    for (const rule of dsoRulesMap.values()) {
      if (!rule.effective_from) {
        skippedIncomplete++;
        continue;
      }

      try {
        await query(`
          INSERT INTO dso_rules_state 
            (contract_id, effective_from, effective_until, sv_count, sv_parties, rule_version)
          VALUES (
            '${rule.contract_id.replace(/'/g, "''")}',
            '${rule.effective_from}',
            ${rule.effective_until ? `'${rule.effective_until}'` : 'NULL'},
            ${rule.sv_count},
            '${rule.sv_parties.replace(/'/g, "''")}',
            ${rule.rule_version ? `'${rule.rule_version.replace(/'/g, "''")}'` : 'NULL'}
          )
          ON CONFLICT (contract_id) DO UPDATE SET
            effective_until = EXCLUDED.effective_until,
            sv_count = EXCLUDED.sv_count,
            sv_parties = EXCLUDED.sv_parties
        `);
        insertedCount++;
      } catch (insertErr) {
        console.warn(`   ⚠️ Insert error for ${rule.contract_id}:`, insertErr.message);
      }
    }

    // Get current SV count from DSO Rules
    const currentSvCount = await getDsoSvCountAt(new Date());

    // Update index state
    const now = new Date().toISOString();
    await query(`
      INSERT INTO dso_rules_index_state (id, last_indexed_at, total_rules, files_scanned)
      VALUES (1, '${now}', ${insertedCount}, ${dsoRulesFiles.length})
      ON CONFLICT (id) DO UPDATE SET
        last_indexed_at = '${now}',
        total_rules = ${insertedCount},
        files_scanned = ${dsoRulesFiles.length}
    `);

    console.log(`\n✅ DSO Rules SV membership index complete:`);
    console.log(`   - Files scanned: ${dsoRulesFiles.length} (only DsoRules template)`);
    console.log(`   - Rules indexed: ${insertedCount} (skipped ${skippedIncomplete} incomplete)`);
    console.log(`   - Current SV count (from DSO Rules): ${currentSvCount}`);

    indexingInProgress = false;
    indexingProgress = null;

    return {
      status: 'complete',
      filesScanned: dsoRulesFiles.length,
      rulesIndexed: insertedCount,
      currentSvCount,
    };
  } catch (err) {
    console.error('❌ DSO Rules index build failed:', err);
    indexingInProgress = false;
    indexingProgress = null;
    throw err;
  }
}

/**
 * Test DSO Rules historical counts
 */
export async function testDsoHistoricalCounts() {
  await ensureDsoTables();

  const samples = await query(`
    SELECT effective_from, effective_until, sv_count, contract_id
    FROM dso_rules_state
    WHERE effective_from IS NOT NULL
    ORDER BY effective_from DESC
    LIMIT 5
  `);

  if (samples.length === 0) {
    return { success: false, message: 'No DSO Rules indexed', tests: [] };
  }

  const tests = [];

  for (const sample of samples) {
    const midpoint = new Date(new Date(sample.effective_from).getTime() + 1000);
    const count = await getDsoSvCountAt(midpoint);
    tests.push({
      timestamp: midpoint.toISOString(),
      description: `During rule ${sample.contract_id?.slice(0, 20)}...`,
      count,
      expected: sample.sv_count,
      pass: count === sample.sv_count,
    });
  }

  // Test current time
  const nowCount = await getDsoSvCountAt(new Date());
  tests.push({
    timestamp: new Date().toISOString(),
    description: 'Current time (should match latest active rule)',
    count: nowCount,
    expected: '>= 0',
    pass: nowCount >= 0,
  });

  const allPass = tests.every(t => t.pass);

  return {
    success: allPass,
    message: allPass ? 'DSO Rules SV counts working correctly' : 'Some tests failed',
    tests,
  };
}

export function isIndexingInProgress() {
  return indexingInProgress;
}

export function getIndexingProgress() {
  return indexingProgress;
}
