#!/usr/bin/env node

/**
 * Display template index statistics and discovered templates
 * Run with: node scripts/show-template-index.js
 */

import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Import from server engine
const enginePath = path.join(__dirname, '../server/engine/template-file-index.js');
const { 
  getTemplateIndexStats,
  getIndexedTemplates,
  getTemplateIndexState,
  isTemplateIndexPopulated
} = await import(enginePath);

async function main() {
  console.log('\n🔍 Template File Index Report\n');
  console.log('='.repeat(60));

  // Check if index is populated
  const populated = await isTemplateIndexPopulated();
  if (!populated) {
    console.log('\n⚠️  Template index is empty. Run the indexing first.\n');
    process.exit(1);
  }

  // Get overall stats
  const stats = await getTemplateIndexStats();
  console.log('\n📊 Overall Statistics:\n');
  console.log(`   Total files indexed:     ${stats.totalFiles?.toLocaleString() || 0}`);
  console.log(`   Unique templates found:  ${stats.totalTemplates?.toLocaleString() || 0}`);
  console.log(`   Total events indexed:    ${stats.totalEvents?.toLocaleString() || 0}`);
  
  // Get indexing state
  const state = await getTemplateIndexState();
  if (state) {
    console.log(`\n   Last indexed:            ${state.lastIndexed || 'Unknown'}`);
    console.log(`   Duration:                ${state.duration || 'Unknown'}`);
  }

  // Get all templates with counts
  console.log('\n' + '='.repeat(60));
  console.log('\n📋 Discovered Templates:\n');
  
  const templates = await getIndexedTemplates();
  
  if (!templates || templates.length === 0) {
    console.log('   No templates found.\n');
    process.exit(0);
  }

  // Sort by event count descending
  templates.sort((a, b) => (b.eventCount || 0) - (a.eventCount || 0));

  // Print header
  console.log('   ' + '-'.repeat(80));
  console.log(`   ${'Template Name'.padEnd(50)} ${'Events'.padStart(12)} ${'Files'.padStart(8)}`);
  console.log('   ' + '-'.repeat(80));

  // Print each template
  for (const t of templates) {
    const name = (t.templateName || t.template_name || 'Unknown').substring(0, 48);
    const events = (t.eventCount || t.event_count || 0).toLocaleString();
    const files = (t.fileCount || t.file_count || 0).toLocaleString();
    console.log(`   ${name.padEnd(50)} ${events.padStart(12)} ${files.padStart(8)}`);
  }

  console.log('   ' + '-'.repeat(80));
  console.log(`\n   Total: ${templates.length} templates\n`);

  // Summary by category (extract module names)
  console.log('='.repeat(60));
  console.log('\n📦 Templates by Module:\n');

  const moduleMap = new Map();
  for (const t of templates) {
    const name = t.templateName || t.template_name || '';
    // Extract module from template name (e.g., "Splice.Amulet" from full template ID)
    const parts = name.split('.');
    const module = parts.length >= 2 ? `${parts[0]}.${parts[1]}` : parts[0] || 'Unknown';
    
    if (!moduleMap.has(module)) {
      moduleMap.set(module, { count: 0, events: 0 });
    }
    const m = moduleMap.get(module);
    m.count++;
    m.events += (t.eventCount || t.event_count || 0);
  }

  const modules = Array.from(moduleMap.entries())
    .sort((a, b) => b[1].events - a[1].events);

  for (const [module, data] of modules) {
    console.log(`   ${module.padEnd(40)} ${data.count.toString().padStart(4)} templates, ${data.events.toLocaleString().padStart(12)} events`);
  }

  console.log('\n');
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
