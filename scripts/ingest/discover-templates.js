#!/usr/bin/env node
/**
 * Template Discovery Script
 *
 * Scans ACS data files to discover all templates in use and suggests
 * additions to EXPECTED_TEMPLATES registry in acs-schema.js
 */

import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';
import { EXPECTED_TEMPLATES, normalizeTemplateKey } from './acs-schema.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Cross-platform path handling
import { getBaseDataDir, getRawDir } from './path-utils.js';
const BASE_DATA_DIR = getBaseDataDir();
const ACS_DIR = path.join(getRawDir(), 'acs');

// Parse CLI args
const args = process.argv.slice(2);
const outputJson = args.includes('--json');
const generateCode = args.includes('--generate');
const verbose = args.includes('--verbose') || args.includes('-v');

// FIX #4: Only use \r progress updates when stdout is an interactive terminal.
// Piped output (CI, log files) would otherwise get garbled carriage-return characters.
const isTTY = process.stdout.isTTY === true;

/**
 * Find all JSONL files in ACS directory.
 *
 * FIX #1: Converted from async to sync — the function only used synchronous
 * fs calls internally, so the async wrapper was misleading and unnecessary.
 *
 * FIX #8: Added a `visitedInodes` Set to detect and skip symlink loops,
 * and a `maxDepth` guard to prevent runaway recursion on unexpectedly deep
 * or malformed directory trees.
 *
 * @param {string} dir
 * @param {Set<number>} [_visited]  - Internal: tracks visited inodes
 * @param {number}      [_depth]    - Internal: current recursion depth
 * @param {number}      [maxDepth]  - Max directory depth to traverse (default: 20)
 * @returns {string[]} Absolute paths to all .jsonl files found
 */
function findJsonlFiles(dir = ACS_DIR, _visited = new Set(), _depth = 0, maxDepth = 20) {
  const files = [];

  if (!fs.existsSync(dir)) return files;

  // FIX #8: Guard against runaway recursion on deeply nested or circular trees
  if (_depth > maxDepth) {
    console.warn(`[template-discovery] Max depth (${maxDepth}) reached at "${dir}" — skipping subtree`);
    return files;
  }

  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (err) {
    console.warn(`[template-discovery] Cannot read directory "${dir}": ${err.message}`);
    return files;
  }

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isSymbolicLink()) {
      // FIX #8: Resolve symlink and check inode to detect loops
      try {
        const real = fs.realpathSync(fullPath);
        const stat = fs.statSync(real);
        if (_visited.has(stat.ino)) {
          console.warn(`[template-discovery] Symlink loop detected at "${fullPath}" — skipping`);
          continue;
        }
        _visited.add(stat.ino);
        if (stat.isDirectory()) {
          files.push(...findJsonlFiles(real, _visited, _depth + 1, maxDepth));
        } else if (entry.name.endsWith('.jsonl')) {
          files.push(real);
        }
      } catch (err) {
        console.warn(`[template-discovery] Cannot resolve symlink "${fullPath}": ${err.message}`);
      }
      continue;
    }

    if (entry.isDirectory()) {
      // Track real inode to detect hard-linked directory loops (rare but possible)
      try {
        const stat = fs.statSync(fullPath);
        if (_visited.has(stat.ino)) continue;
        _visited.add(stat.ino);
      } catch {}
      files.push(...findJsonlFiles(fullPath, _visited, _depth + 1, maxDepth));
    } else if (entry.name.endsWith('.jsonl')) {
      files.push(fullPath);
    }
  }

  return files;
}

/**
 * Read a JSONL file and extract template IDs with contract counts.
 *
 * FIX #2: The readline interface is now closed in a finally block so it is
 * always cleaned up, even when the underlying stream throws.
 *
 * FIX #3: I/O errors are no longer swallowed. Only JSON parse errors on
 * individual lines are caught and skipped (with an optional verbose log).
 * File-level errors propagate to the caller so they are visible in output.
 *
 * @param {string} filePath
 * @returns {Promise<Map<string, number>>} normalized template key → contract count
 */
async function extractTemplatesFromFile(filePath) {
  const templates = new Map();
  let parseErrors = 0;

  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  // FIX #2: Always close rl regardless of success or failure
  try {
    for await (const line of rl) {
      if (!line.trim()) continue;
      try {
        const contract = JSON.parse(line);
        const templateId = contract.template_id;
        if (templateId) {
          const normalized = normalizeTemplateKey(templateId);
          templates.set(normalized, (templates.get(normalized) || 0) + 1);
        }
      } catch {
        // FIX #3: Only JSON parse errors are silently skipped.
        // I/O errors on the stream itself are not caught here — they propagate.
        parseErrors++;
        if (verbose) {
          console.warn(`[template-discovery] Skipping malformed JSON line in "${path.basename(filePath)}"`);
        }
      }
    }
  } finally {
    // FIX #2: Close the readline interface to release the file handle
    rl.close();
    fileStream.destroy();
  }

  if (parseErrors > 0 && !verbose) {
    console.warn(`[template-discovery] "${path.basename(filePath)}": skipped ${parseErrors} malformed line(s)`);
  }

  return templates;
}

/**
 * Merge template counts from multiple sources into target Map.
 */
function mergeTemplateCounts(target, source) {
  for (const [template, count] of source) {
    target.set(template, (target.get(template) || 0) + count);
  }
}

/**
 * Categorize a template based on its module path.
 */
function categorizeTemplate(templateKey) {
  if (templateKey.startsWith('Splice.Amulet:'))         return 'Amulet';
  if (templateKey.startsWith('Splice.ValidatorLicense:')) return 'Validator';
  if (templateKey.startsWith('Splice.DsoRules:'))        return 'DSO Rules';
  if (templateKey.startsWith('Splice.DSO.SvState:'))     return 'DSO SV State';
  if (templateKey.startsWith('Splice.DSO.AmuletPrice:')) return 'DSO Amulet Price';
  if (templateKey.startsWith('Splice.AmuletRules:'))     return 'Amulet Rules';
  if (templateKey.startsWith('Splice.Round:'))           return 'Round';
  if (templateKey.startsWith('Splice.Ans:') || templateKey.startsWith('Splice.ANS:')) return 'ANS';
  if (templateKey.startsWith('Splice.Wallet:'))          return 'Wallet';
  if (templateKey.startsWith('Splice.'))                 return 'Other Splice';
  return 'Unknown';
}

/**
 * Generate code snippet for adding new templates to acs-schema.js.
 *
 * FIX #6: Parameter renamed from Map to entries array and explicitly converted
 * to a Map internally. Previously called with an array of [k,v] tuples from
 * `missing.map(m => [m.template, m.count])`, which happened to work because
 * arrays are iterable, but misled readers into thinking a Map was expected.
 *
 * @param {Array<[string, number]>} templateEntries - Array of [templateKey, count] tuples
 * @returns {string} Code snippet
 */
function generateCodeSnippet(templateEntries) {
  // FIX #6: Normalize input to Map regardless of whether an array or Map is passed
  const newTemplates = templateEntries instanceof Map
    ? templateEntries
    : new Map(templateEntries);

  const byCategory = new Map();

  for (const [template, count] of newTemplates) {
    const category = categorizeTemplate(template);
    if (!byCategory.has(category)) byCategory.set(category, []);
    byCategory.get(category).push({ template, count });
  }

  let code = '\n// === ADD THESE TO EXPECTED_TEMPLATES ===\n';

  for (const [category, templates] of byCategory) {
    code += `\n  // ${category} templates (auto-discovered)\n`;
    for (const { template, count } of templates.sort((a, b) => a.template.localeCompare(b.template))) {
      const name = template.split(':')[1] || template;
      const description = name.replace(/([A-Z])/g, ' $1').trim().toLowerCase();
      code += `  '${template}': { required: false, description: '${description}' }, // ${count.toLocaleString()} contracts\n`;
    }
  }

  return code;
}

/**
 * Write a progress line that degrades gracefully in non-TTY environments.
 *
 * FIX #4: In TTY mode, uses \r to overwrite the current line (interactive feel).
 * In non-TTY mode (CI, pipes, log files), emits a plain newline-terminated line
 * at each interval to avoid garbled output.
 */
function writeProgress(message, done = false) {
  if (isTTY) {
    process.stdout.write(`\r   ${message}${done ? '\n' : ''}`);
  } else if (done) {
    process.stdout.write(`   ${message}\n`);
  }
  // In non-TTY mode, suppress intermediate progress lines to keep logs clean.
  // Only the final "done" line is emitted.
}

/**
 * Main discovery function.
 */
async function discoverTemplates() {
  console.log('🔍 ACS Template Discovery\n');
  console.log(`📁 Scanning: ${ACS_DIR}\n`);

  // FIX #1: findJsonlFiles is now synchronous — no await needed
  const files = findJsonlFiles();

  if (files.length === 0) {
    // FIX #5: Exit 0 with a clear warning rather than exit(1).
    // No data yet is a normal operational state (e.g. running before backfill),
    // and exit(1) would break CI pipelines and scheduled jobs unnecessarily.
    // Use --require-data flag to opt into strict mode if exit(1) is needed.
    const requireData = args.includes('--require-data');
    console.warn('⚠️  No JSONL files found in ACS directory.');
    if (requireData) {
      console.error('❌ --require-data set: treating empty directory as an error.');
      process.exit(1);
    }
    console.log('   Nothing to scan. Re-run after ACS data is available.');
    process.exit(0);
  }

  console.log(`📄 Found ${files.length} JSONL file(s) to scan\n`);

  // Extract templates from all files
  const allTemplates = new Map();
  let filesProcessed = 0;
  let fileErrors = 0;

  for (const file of files) {
    try {
      const templates = await extractTemplatesFromFile(file);
      mergeTemplateCounts(allTemplates, templates);
    } catch (err) {
      // FIX #3: File-level I/O errors are now visible rather than silently
      // returning an empty map and producing wrong totals.
      fileErrors++;
      console.warn(`[template-discovery] ⚠️  Failed to read "${path.basename(file)}": ${err.message}`);
    }

    filesProcessed++;

    if (verbose) {
      console.log(`   Processed: ${path.basename(file)}`);
    } else if (filesProcessed % 20 === 0 || filesProcessed === files.length) {
      // FIX #4: Use TTY-aware progress helper
      writeProgress(
        `Processing... ${filesProcessed}/${files.length} files${fileErrors > 0 ? ` (${fileErrors} errors)` : ''}`,
        filesProcessed === files.length
      );
    }
  }

  if (!verbose && filesProcessed % 20 !== 0) {
    // Ensure final line is always written
    writeProgress(`Processing... ${filesProcessed}/${files.length} files ✓`, true);
  }

  if (fileErrors > 0) {
    console.warn(`\n⚠️  ${fileErrors} file(s) could not be read and were excluded from results.`);
  }

  // Sort templates by count (descending)
  const sortedTemplates = [...allTemplates.entries()].sort((a, b) => b[1] - a[1]);

  // Categorize: registered vs new
  const registered = [];
  const missing = [];
  const expectedKeys = new Set(Object.keys(EXPECTED_TEMPLATES));

  for (const [template, count] of sortedTemplates) {
    if (expectedKeys.has(template)) {
      registered.push({ template, count });
    } else {
      missing.push({ template, count });
    }
  }

  // FIX #7: Guard against undefined entries in EXPECTED_TEMPLATES before
  // accessing .required, which would throw if a key maps to undefined.
  const notFound = [];
  for (const key of expectedKeys) {
    if (!allTemplates.has(key)) {
      notFound.push(key);
    }
  }

  // Output results
  if (outputJson) {
    const result = {
      totalTemplates: allTemplates.size,
      totalContracts: [...allTemplates.values()].reduce((a, b) => a + b, 0),
      fileErrors,
      registered: registered.map(r => ({ ...r })),
      missing: missing.map(m => ({ ...m, category: categorizeTemplate(m.template) })),
      notFound,
    };
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  console.log('\n' + '═'.repeat(80));
  console.log('📊 TEMPLATE DISCOVERY RESULTS');
  console.log('═'.repeat(80));

  console.log(`\n✅ REGISTERED TEMPLATES (${registered.length}):\n`);
  for (const { template, count } of registered) {
    console.log(`   • ${template}: ${count.toLocaleString()} contracts`);
  }

  if (missing.length > 0) {
    console.log(`\n⚠️  UNREGISTERED TEMPLATES (${missing.length}):\n`);
    for (const { template, count } of missing) {
      const category = categorizeTemplate(template);
      console.log(`   • ${template}: ${count.toLocaleString()} contracts [${category}]`);
    }
  } else {
    console.log('\n✅ All discovered templates are registered!');
  }

  if (notFound.length > 0) {
    console.log(`\n❓ EXPECTED BUT NOT FOUND (${notFound.length}):\n`);
    for (const template of notFound) {
      // FIX #7: Guard against undefined registry entries before accessing .required
      const info = EXPECTED_TEMPLATES[template];
      const requiredLabel = info?.required ? ' (REQUIRED)' : '';
      console.log(`   • ${template}${requiredLabel}`);
    }
  }

  // Summary
  const totalContracts = [...allTemplates.values()].reduce((a, b) => a + b, 0);
  console.log('\n' + '─'.repeat(80));
  console.log('📈 SUMMARY:');
  console.log(`   Total templates discovered: ${allTemplates.size}`);
  console.log(`   Total contracts:            ${totalContracts.toLocaleString()}`);
  console.log(`   Registered:                 ${registered.length}`);
  console.log(`   Unregistered:               ${missing.length}`);
  console.log(`   Expected but not found:     ${notFound.length}`);
  if (fileErrors > 0) {
    console.log(`   Files with read errors:     ${fileErrors}`);
  }

  // Generate code if requested
  if (generateCode && missing.length > 0) {
    console.log('\n' + '═'.repeat(80));
    console.log('💻 GENERATED CODE FOR acs-schema.js:');
    console.log('═'.repeat(80));
    // FIX #6: Pass array of [k,v] tuples — generateCodeSnippet normalizes internally
    console.log(generateCodeSnippet(missing.map(m => [m.template, m.count])));
  }

  console.log('\n' + '─'.repeat(80));
  console.log('💡 Usage:');
  console.log('   --json           Output as JSON');
  console.log('   --generate       Generate code snippet for missing templates');
  console.log('   --verbose        Show detailed progress');
  console.log('   --require-data   Exit 1 if no JSONL files found (for strict CI use)');
  console.log('─'.repeat(80) + '\n');
}

// Run
discoverTemplates().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
