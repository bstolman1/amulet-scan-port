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

/**
 * Find all JSONL files in ACS directory
 */
async function findJsonlFiles() {
  const files = [];
  
  async function scanDir(dir) {
    if (!fs.existsSync(dir)) return;
    
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await scanDir(fullPath);
      } else if (entry.name.endsWith('.jsonl')) {
        files.push(fullPath);
      }
    }
  }
  
  await scanDir(ACS_DIR);
  return files;
}

/**
 * Read JSONL file and extract template IDs
 */
async function extractTemplatesFromFile(filePath) {
  const templates = new Map();
  
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  
  for await (const line of rl) {
    if (!line.trim()) continue;
    try {
      const contract = JSON.parse(line);
      const templateId = contract.template_id;
      if (templateId) {
        const normalized = normalizeTemplateKey(templateId);
        const current = templates.get(normalized) || 0;
        templates.set(normalized, current + 1);
      }
    } catch (e) {
      // Skip malformed lines
    }
  }
  
  return templates;
}

/**
 * Merge template counts from multiple sources
 */
function mergeTemplateCounts(target, source) {
  for (const [template, count] of source) {
    const current = target.get(template) || 0;
    target.set(template, current + count);
  }
}

/**
 * Categorize a template based on its module path
 */
function categorizeTemplate(templateKey) {
  if (templateKey.startsWith('Splice.Amulet:')) return 'Amulet';
  if (templateKey.startsWith('Splice.ValidatorLicense:')) return 'Validator';
  if (templateKey.startsWith('Splice.DsoRules:')) return 'DSO Rules';
  if (templateKey.startsWith('Splice.DSO.SvState:')) return 'DSO SV State';
  if (templateKey.startsWith('Splice.DSO.AmuletPrice:')) return 'DSO Amulet Price';
  if (templateKey.startsWith('Splice.AmuletRules:')) return 'Amulet Rules';
  if (templateKey.startsWith('Splice.Round:')) return 'Round';
  if (templateKey.startsWith('Splice.Ans:') || templateKey.startsWith('Splice.ANS:')) return 'ANS';
  if (templateKey.startsWith('Splice.Wallet:')) return 'Wallet';
  if (templateKey.startsWith('Splice.')) return 'Other Splice';
  return 'Unknown';
}

/**
 * Generate code snippet for adding templates
 */
function generateCodeSnippet(newTemplates) {
  const byCategory = new Map();
  
  for (const [template, count] of newTemplates) {
    const category = categorizeTemplate(template);
    if (!byCategory.has(category)) {
      byCategory.set(category, []);
    }
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
 * Main discovery function
 */
async function discoverTemplates() {
  console.log('🔍 ACS Template Discovery\n');
  console.log(`📁 Scanning: ${ACS_DIR}\n`);
  
  // Find all JSONL files
  const files = await findJsonlFiles();
  if (files.length === 0) {
    console.log('❌ No JSONL files found in ACS directory');
    process.exit(1);
  }
  
  console.log(`📄 Found ${files.length} JSONL files to scan\n`);
  
  // Extract templates from all files
  const allTemplates = new Map();
  let filesProcessed = 0;
  
  for (const file of files) {
    const templates = await extractTemplatesFromFile(file);
    mergeTemplateCounts(allTemplates, templates);
    filesProcessed++;
    
    if (verbose) {
      console.log(`   Processed: ${path.basename(file)} (${templates.size} templates)`);
    } else if (filesProcessed % 20 === 0) {
      process.stdout.write(`\r   Processing... ${filesProcessed}/${files.length} files`);
    }
  }
  
  if (!verbose) {
    process.stdout.write(`\r   Processing... ${filesProcessed}/${files.length} files ✓\n`);
  }
  
  // Sort templates by count
  const sortedTemplates = [...allTemplates.entries()].sort((a, b) => b[1] - a[1]);
  
  // Categorize templates
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
  
  // Check for expected but not found
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
      registered: registered.map(r => ({ ...r })),
      missing: missing.map(m => ({ ...m, category: categorizeTemplate(m.template) })),
      notFound
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
      const info = EXPECTED_TEMPLATES[template];
      console.log(`   • ${template}${info.required ? ' (REQUIRED)' : ''}`);
    }
  }
  
  // Summary
  console.log('\n' + '─'.repeat(80));
  console.log('📈 SUMMARY:');
  console.log(`   Total templates discovered: ${allTemplates.size}`);
  console.log(`   Total contracts: ${[...allTemplates.values()].reduce((a, b) => a + b, 0).toLocaleString()}`);
  console.log(`   Registered: ${registered.length}`);
  console.log(`   Unregistered: ${missing.length}`);
  console.log(`   Expected but not found: ${notFound.length}`);
  
  // Generate code if requested
  if (generateCode && missing.length > 0) {
    console.log('\n' + '═'.repeat(80));
    console.log('💻 GENERATED CODE FOR acs-schema.js:');
    console.log('═'.repeat(80));
    console.log(generateCodeSnippet(missing.map(m => [m.template, m.count])));
  }
  
  console.log('\n' + '─'.repeat(80));
  console.log('💡 Usage:');
  console.log('   --json      Output as JSON');
  console.log('   --generate  Generate code snippet for missing templates');
  console.log('   --verbose   Show detailed progress');
  console.log('─'.repeat(80) + '\n');
}

// Run
discoverTemplates().catch(err => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
