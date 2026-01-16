#!/usr/bin/env node
/**
 * Test Quality Report Generator
 * 
 * Analyzes test files to generate metrics on:
 * - Assertion density (assertions per test)
 * - Test coverage by file/module
 * - Code path coverage analysis
 * - Test distribution across categories
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');

// ─────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────

const TEST_PATTERNS = [
  { dir: 'src', pattern: /\.(test|spec)\.(ts|tsx)$/, type: 'frontend' },
  { dir: 'server', pattern: /\.(test|spec)\.js$/, type: 'server' },
  { dir: 'scripts/ingest/test', pattern: /\.test\.js$/, type: 'ingest' },
];

const ASSERTION_PATTERNS = [
  /expect\s*\(/g,
  /assert\s*\(/g,
  /assert\.\w+\s*\(/g,
  /\.toBe\s*\(/g,
  /\.toEqual\s*\(/g,
  /\.toMatch\s*\(/g,
  /\.toContain\s*\(/g,
  /\.toHaveLength\s*\(/g,
  /\.toBeDefined\s*\(/g,
  /\.toBeNull\s*\(/g,
  /\.toBeTruthy\s*\(/g,
  /\.toBeFalsy\s*\(/g,
  /\.toThrow\s*\(/g,
  /\.rejects\s*\./g,
  /\.resolves\s*\./g,
  /\.toHaveProperty\s*\(/g,
  /\.toBeInstanceOf\s*\(/g,
  /\.toBeGreaterThan\s*\(/g,
  /\.toBeLessThan\s*\(/g,
  /\.toBeGreaterThanOrEqual\s*\(/g,
  /\.toBeLessThanOrEqual\s*\(/g,
  /\.toBeCloseTo\s*\(/g,
  /\.toHaveBeenCalled/g,
  /\.toHaveBeenCalledWith\s*\(/g,
  /\.not\./g,
];

const TEST_PATTERNS_REGEX = [
  /\bit\s*\(\s*['"]/g,
  /\btest\s*\(\s*['"]/g,
];

// ─────────────────────────────────────────────────────────────
// File Discovery
// ─────────────────────────────────────────────────────────────

function findTestFiles(baseDir, pattern, results = []) {
  const fullPath = path.join(PROJECT_ROOT, baseDir);
  
  if (!fs.existsSync(fullPath)) return results;
  
  const entries = fs.readdirSync(fullPath, { withFileTypes: true });
  
  for (const entry of entries) {
    const entryPath = path.join(baseDir, entry.name);
    
    if (entry.isDirectory() && entry.name !== 'node_modules') {
      findTestFiles(entryPath, pattern, results);
    } else if (entry.isFile() && pattern.test(entry.name)) {
      results.push(entryPath);
    }
  }
  
  return results;
}

// ─────────────────────────────────────────────────────────────
// Analysis Functions
// ─────────────────────────────────────────────────────────────

function analyzeTestFile(filePath) {
  const fullPath = path.join(PROJECT_ROOT, filePath);
  const content = fs.readFileSync(fullPath, 'utf-8');
  const lines = content.split('\n');
  
  // Count assertions
  let assertionCount = 0;
  for (const pattern of ASSERTION_PATTERNS) {
    const matches = content.match(pattern);
    if (matches) assertionCount += matches.length;
  }
  
  // Count test cases
  let testCount = 0;
  for (const pattern of TEST_PATTERNS_REGEX) {
    const matches = content.match(pattern);
    if (matches) testCount += matches.length;
  }
  
  // Count describe blocks
  const describeMatches = content.match(/\bdescribe\s*\(\s*['"]/g);
  const describeCount = describeMatches ? describeMatches.length : 0;
  
  // Count mocks
  const mockMatches = content.match(/vi\.mock\s*\(|jest\.mock\s*\(|\.mockReturnValue|\.mockResolvedValue|\.mockRejectedValue/g);
  const mockCount = mockMatches ? mockMatches.length : 0;
  
  // Check for async tests
  const asyncTests = content.match(/async\s*\(\s*\)\s*=>\s*\{|async\s+function/g);
  const asyncCount = asyncTests ? asyncTests.length : 0;
  
  // Check for error testing
  const errorTests = content.match(/toThrow|rejects\.toThrow|error|Error|catch/gi);
  const hasErrorTesting = errorTests && errorTests.length > 2;
  
  // Check for edge case testing
  const edgeCasePatterns = /null|undefined|empty|invalid|missing|zero|negative|boundary|edge/gi;
  const edgeCaseMatches = content.match(edgeCasePatterns);
  const edgeCaseCount = edgeCaseMatches ? edgeCaseMatches.length : 0;
  
  return {
    path: filePath,
    lines: lines.length,
    assertions: assertionCount,
    tests: testCount,
    describes: describeCount,
    mocks: mockCount,
    asyncTests: asyncCount,
    hasErrorTesting,
    edgeCaseCoverage: edgeCaseCount,
    assertionDensity: testCount > 0 ? (assertionCount / testCount).toFixed(2) : 0,
  };
}

function analyzeSourceCoverage() {
  const coveragePath = path.join(PROJECT_ROOT, 'coverage', 'coverage-summary.json');
  
  if (!fs.existsSync(coveragePath)) {
    return null;
  }
  
  try {
    const coverage = JSON.parse(fs.readFileSync(coveragePath, 'utf-8'));
    return coverage;
  } catch (e) {
    return null;
  }
}

// ─────────────────────────────────────────────────────────────
// Report Generation
// ─────────────────────────────────────────────────────────────

function generateReport() {
  console.log('\n' + '═'.repeat(70));
  console.log('  📊 TEST QUALITY REPORT');
  console.log('  Generated: ' + new Date().toISOString());
  console.log('═'.repeat(70) + '\n');
  
  const allResults = [];
  const categoryStats = {};
  
  // Analyze each test category
  for (const { dir, pattern, type } of TEST_PATTERNS) {
    const files = findTestFiles(dir, pattern);
    const results = files.map(f => ({ ...analyzeTestFile(f), type }));
    allResults.push(...results);
    
    categoryStats[type] = {
      files: results.length,
      tests: results.reduce((sum, r) => sum + r.tests, 0),
      assertions: results.reduce((sum, r) => sum + r.assertions, 0),
      lines: results.reduce((sum, r) => sum + r.lines, 0),
    };
  }
  
  // ─────────────────────────────────────────────────────────────
  // Summary Statistics
  // ─────────────────────────────────────────────────────────────
  
  console.log('📈 SUMMARY STATISTICS');
  console.log('─'.repeat(70));
  
  const totalFiles = allResults.length;
  const totalTests = allResults.reduce((sum, r) => sum + r.tests, 0);
  const totalAssertions = allResults.reduce((sum, r) => sum + r.assertions, 0);
  const totalLines = allResults.reduce((sum, r) => sum + r.lines, 0);
  const avgDensity = totalTests > 0 ? (totalAssertions / totalTests).toFixed(2) : 0;
  
  console.log(`  Total Test Files:     ${totalFiles}`);
  console.log(`  Total Test Cases:     ${totalTests}`);
  console.log(`  Total Assertions:     ${totalAssertions}`);
  console.log(`  Total Test LOC:       ${totalLines}`);
  console.log(`  Avg Assertion Density: ${avgDensity} assertions/test`);
  console.log('');
  
  // ─────────────────────────────────────────────────────────────
  // Category Breakdown
  // ─────────────────────────────────────────────────────────────
  
  console.log('📂 CATEGORY BREAKDOWN');
  console.log('─'.repeat(70));
  console.log('  Category     │ Files │ Tests │ Assertions │ Density');
  console.log('  ─────────────┼───────┼───────┼────────────┼────────');
  
  for (const [type, stats] of Object.entries(categoryStats)) {
    const density = stats.tests > 0 ? (stats.assertions / stats.tests).toFixed(2) : '0.00';
    const typeStr = type.padEnd(12);
    const filesStr = String(stats.files).padStart(5);
    const testsStr = String(stats.tests).padStart(5);
    const assertStr = String(stats.assertions).padStart(10);
    console.log(`  ${typeStr} │${filesStr} │${testsStr} │${assertStr} │ ${density}`);
  }
  console.log('');
  
  // ─────────────────────────────────────────────────────────────
  // Top Files by Assertion Density
  // ─────────────────────────────────────────────────────────────
  
  console.log('🏆 TOP 10 FILES BY ASSERTION DENSITY');
  console.log('─'.repeat(70));
  
  const sortedByDensity = [...allResults]
    .filter(r => r.tests >= 3)
    .sort((a, b) => parseFloat(b.assertionDensity) - parseFloat(a.assertionDensity))
    .slice(0, 10);
  
  for (const result of sortedByDensity) {
    const shortPath = result.path.length > 50 
      ? '...' + result.path.slice(-47) 
      : result.path.padEnd(50);
    console.log(`  ${shortPath} │ ${result.assertionDensity} (${result.assertions}/${result.tests})`);
  }
  console.log('');
  
  // ─────────────────────────────────────────────────────────────
  // Files Needing Attention (Low Assertion Density)
  // ─────────────────────────────────────────────────────────────
  
  console.log('⚠️  FILES WITH LOW ASSERTION DENSITY (<2.0)');
  console.log('─'.repeat(70));
  
  const lowDensity = allResults
    .filter(r => r.tests >= 2 && parseFloat(r.assertionDensity) < 2.0)
    .sort((a, b) => parseFloat(a.assertionDensity) - parseFloat(b.assertionDensity));
  
  if (lowDensity.length === 0) {
    console.log('  ✅ All test files have adequate assertion density!');
  } else {
    for (const result of lowDensity.slice(0, 10)) {
      const shortPath = result.path.length > 50 
        ? '...' + result.path.slice(-47) 
        : result.path.padEnd(50);
      console.log(`  ${shortPath} │ ${result.assertionDensity}`);
    }
  }
  console.log('');
  
  // ─────────────────────────────────────────────────────────────
  // Test Quality Indicators
  // ─────────────────────────────────────────────────────────────
  
  console.log('🔍 TEST QUALITY INDICATORS');
  console.log('─'.repeat(70));
  
  const withErrorTesting = allResults.filter(r => r.hasErrorTesting).length;
  const withMocks = allResults.filter(r => r.mocks > 0).length;
  const withAsync = allResults.filter(r => r.asyncTests > 0).length;
  const withEdgeCases = allResults.filter(r => r.edgeCaseCoverage >= 3).length;
  
  console.log(`  Files with error testing:    ${withErrorTesting}/${totalFiles} (${((withErrorTesting/totalFiles)*100).toFixed(0)}%)`);
  console.log(`  Files with mocking:          ${withMocks}/${totalFiles} (${((withMocks/totalFiles)*100).toFixed(0)}%)`);
  console.log(`  Files with async tests:      ${withAsync}/${totalFiles} (${((withAsync/totalFiles)*100).toFixed(0)}%)`);
  console.log(`  Files with edge case tests:  ${withEdgeCases}/${totalFiles} (${((withEdgeCases/totalFiles)*100).toFixed(0)}%)`);
  console.log('');
  
  // ─────────────────────────────────────────────────────────────
  // Code Coverage Analysis
  // ─────────────────────────────────────────────────────────────
  
  const coverage = analyzeSourceCoverage();
  
  if (coverage && coverage.total) {
    console.log('📊 CODE COVERAGE SUMMARY');
    console.log('─'.repeat(70));
    
    const { lines, statements, functions, branches } = coverage.total;
    
    const formatPct = (pct) => {
      const num = parseFloat(pct) || 0;
      const bar = '█'.repeat(Math.floor(num / 5)) + '░'.repeat(20 - Math.floor(num / 5));
      return `${bar} ${num.toFixed(1)}%`;
    };
    
    console.log(`  Lines:      ${formatPct(lines?.pct)}`);
    console.log(`  Statements: ${formatPct(statements?.pct)}`);
    console.log(`  Functions:  ${formatPct(functions?.pct)}`);
    console.log(`  Branches:   ${formatPct(branches?.pct)}`);
    console.log('');
    
    // Find uncovered files
    console.log('🚨 UNCOVERED SOURCE FILES (0% coverage)');
    console.log('─'.repeat(70));
    
    const uncovered = Object.entries(coverage)
      .filter(([key, val]) => key !== 'total' && val.lines?.pct === 0)
      .map(([key]) => key)
      .slice(0, 15);
    
    if (uncovered.length === 0) {
      console.log('  ✅ All source files have some coverage!');
    } else {
      for (const file of uncovered) {
        const shortPath = file.length > 65 ? '...' + file.slice(-62) : file;
        console.log(`  ${shortPath}`);
      }
      if (Object.keys(coverage).length - 1 - uncovered.length > 15) {
        console.log(`  ... and ${Object.keys(coverage).length - 1 - 15} more`);
      }
    }
    console.log('');
  } else {
    console.log('📊 CODE COVERAGE: Not available');
    console.log('   Run: bash scripts/run-tests.sh coverage');
    console.log('');
  }
  
  // ─────────────────────────────────────────────────────────────
  // Quality Score
  // ─────────────────────────────────────────────────────────────
  
  console.log('═'.repeat(70));
  console.log('  📋 OVERALL TEST QUALITY SCORE');
  console.log('═'.repeat(70));
  
  let score = 0;
  const maxScore = 100;
  const criteria = [];
  
  // Assertion density (25 points) - high density = meaningful tests
  // 3.0+ assertions/test = full score
  const densityScore = Math.min(25, parseFloat(avgDensity) * 8.33);
  score += densityScore;
  criteria.push({ name: 'Assertion Density', score: densityScore, max: 25 });
  
  // Error testing (15 points) - 50%+ = full score
  const errorScore = Math.min(15, (withErrorTesting / totalFiles) * 30);
  score += errorScore;
  criteria.push({ name: 'Error Testing', score: errorScore, max: 15 });
  
  // Edge case coverage (15 points) - 70%+ = full score
  const edgeScore = Math.min(15, (withEdgeCases / totalFiles) * 21.4);
  score += edgeScore;
  criteria.push({ name: 'Edge Case Coverage', score: edgeScore, max: 15 });
  
  // Mock usage (10 points) - 30%+ = full score (not everything needs mocking)
  const mockScore = Math.min(10, (withMocks / totalFiles) * 33.3);
  score += mockScore;
  criteria.push({ name: 'Mock Usage', score: mockScore, max: 10 });
  
  // Async testing (10 points) - 50%+ = full score
  const asyncScore = Math.min(10, (withAsync / totalFiles) * 20);
  score += asyncScore;
  criteria.push({ name: 'Async Testing', score: asyncScore, max: 10 });
  
  // Code coverage (25 points) - only for directly-importable code
  // Note: API routes, workers execute via server runtime - validated by integration/e2e tests
  if (coverage && coverage.total) {
    // Core logic coverage - 80%+ = full score
    const covScore = Math.min(25, (coverage.total.lines?.pct || 0) / 3.2);
    score += covScore;
    criteria.push({ name: 'Code Coverage', score: covScore, max: 25, note: 'core logic only' });
  } else {
    // If no coverage file, check if we have integration/e2e tests
    const hasIntegration = allResults.some(r => 
      r.file.includes('integration') || r.file.includes('e2e')
    );
    if (hasIntegration) {
      // Give partial credit for integration tests
      const integrationTests = allResults.filter(r => 
        r.file.includes('integration') || r.file.includes('e2e')
      );
      const integrationScore = Math.min(15, integrationTests.length * 3);
      score += integrationScore;
      criteria.push({ name: 'Code Coverage', score: integrationScore, max: 25, note: 'via integration tests' });
    } else {
      criteria.push({ name: 'Code Coverage', score: 0, max: 25, note: 'Run coverage first' });
    }
  }
  
  console.log('');
  for (const c of criteria) {
    const pct = ((c.score / c.max) * 100).toFixed(0);
    const bar = '█'.repeat(Math.floor(c.score / c.max * 10)) + '░'.repeat(10 - Math.floor(c.score / c.max * 10));
    const note = c.note ? ` (${c.note})` : '';
    console.log(`  ${c.name.padEnd(20)} ${bar} ${c.score.toFixed(1)}/${c.max}${note}`);
  }
  
  console.log('');
  console.log(`  ${'─'.repeat(50)}`);
  
  const finalBar = '█'.repeat(Math.floor(score / 10)) + '░'.repeat(10 - Math.floor(score / 10));
  console.log(`  ${'TOTAL SCORE'.padEnd(20)} ${finalBar} ${score.toFixed(1)}/${maxScore}`);
  
  let grade = 'F';
  if (score >= 90) grade = 'A';
  else if (score >= 80) grade = 'B';
  else if (score >= 70) grade = 'C';
  else if (score >= 60) grade = 'D';
  
  console.log(`  ${'GRADE'.padEnd(20)} ${grade}`);
  console.log('');
  console.log('═'.repeat(70) + '\n');
  
  // Write JSON report
  const reportPath = path.join(PROJECT_ROOT, 'coverage', 'test-quality-report.json');
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  
  const jsonReport = {
    generated: new Date().toISOString(),
    summary: { totalFiles, totalTests, totalAssertions, avgDensity, score, grade },
    categories: categoryStats,
    files: allResults,
    criteria,
  };
  
  fs.writeFileSync(reportPath, JSON.stringify(jsonReport, null, 2));
  console.log(`📄 JSON report saved to: ${reportPath}`);
}

// ─────────────────────────────────────────────────────────────
// Run
// ─────────────────────────────────────────────────────────────

generateReport();
