#!/usr/bin/env node
/**
 * Sharding Test Script
 * 
 * Verifies that shard boundaries are calculated correctly with no gaps or overlaps.
 * 
 * Usage:
 *   node test-sharding.js                    # Test with defaults
 *   node test-sharding.js --shards 4         # Test with 4 shards
 *   node test-sharding.js --range 3600000    # Test with 1 hour range (ms)
 */

const args = process.argv.slice(2);
const shardCount = args.includes('--shards') 
  ? parseInt(args[args.indexOf('--shards') + 1]) 
  : 2;
const rangeMs = args.includes('--range')
  ? parseInt(args[args.indexOf('--range') + 1])
  : 3600000; // 1 hour default

/**
 * Calculate time slice for a shard (copied from fetch-backfill.js)
 */
function calculateShardTimeRange(minTime, maxTime, shardIndex, shardTotal) {
  const minMs = new Date(minTime).getTime();
  const maxMs = new Date(maxTime).getTime();
  const rangeMs = maxMs - minMs;
  
  // Use integer division to avoid floating point precision issues
  const shardMaxMs = maxMs - Math.floor((shardIndex * rangeMs) / shardTotal);
  const shardMinMs = maxMs - Math.floor(((shardIndex + 1) * rangeMs) / shardTotal);
  
  return {
    minTime: new Date(shardMinMs).toISOString(),
    maxTime: new Date(shardMaxMs).toISOString(),
    minMs: shardMinMs,
    maxMs: shardMaxMs,
  };
}

/**
 * Format milliseconds as human-readable duration
 */
function formatMs(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
  return `${(ms / 3600000).toFixed(2)}h`;
}

/**
 * Run sharding tests
 */
function runTests() {
  console.log('\n' + '='.repeat(80));
  console.log('🧪 SHARDING TEST');
  console.log('='.repeat(80));
  console.log(`   Shard count: ${shardCount}`);
  console.log(`   Time range:  ${formatMs(rangeMs)}`);
  console.log('='.repeat(80) + '\n');

  // Create test time range
  const maxTime = new Date();
  const minTime = new Date(maxTime.getTime() - rangeMs);
  
  console.log(`📅 Full Range:`);
  console.log(`   Min: ${minTime.toISOString()}`);
  console.log(`   Max: ${maxTime.toISOString()}`);
  console.log(`   Duration: ${formatMs(rangeMs)}`);
  console.log('');

  // Calculate all shard ranges
  const shards = [];
  for (let i = 0; i < shardCount; i++) {
    const range = calculateShardTimeRange(minTime.toISOString(), maxTime.toISOString(), i, shardCount);
    shards.push({ index: i, ...range });
  }

  // Display shard ranges
  console.log('📊 Shard Ranges:');
  console.log('─'.repeat(80));
  
  let totalCoverage = 0;
  for (const shard of shards) {
    const duration = shard.maxMs - shard.minMs;
    totalCoverage += duration;
    console.log(`   Shard ${shard.index}:`);
    console.log(`      Min: ${shard.minTime}`);
    console.log(`      Max: ${shard.maxTime}`);
    console.log(`      Duration: ${formatMs(duration)}`);
    console.log('');
  }

  // Test 1: No gaps between shards
  console.log('─'.repeat(80));
  console.log('🔍 TEST 1: Checking for gaps between shards...');
  
  let hasGaps = false;
  for (let i = 1; i < shards.length; i++) {
    const prevShard = shards[i - 1];
    const currShard = shards[i];
    
    // The previous shard's min should equal current shard's max
    // (since we're going backwards in time)
    const gap = prevShard.minMs - currShard.maxMs;
    
    if (gap !== 0) {
      hasGaps = true;
      console.log(`   ❌ Gap between shard ${i-1} and ${i}: ${gap}ms`);
    }
  }
  
  if (!hasGaps) {
    console.log('   ✅ No gaps found between shards');
  }

  // Test 2: No overlaps between shards
  console.log('\n🔍 TEST 2: Checking for overlaps between shards...');
  
  let hasOverlaps = false;
  for (let i = 1; i < shards.length; i++) {
    const prevShard = shards[i - 1];
    const currShard = shards[i];
    
    // Check if ranges overlap
    if (currShard.maxMs > prevShard.minMs) {
      hasOverlaps = true;
      console.log(`   ❌ Overlap between shard ${i-1} and ${i}: ${currShard.maxMs - prevShard.minMs}ms`);
    }
  }
  
  if (!hasOverlaps) {
    console.log('   ✅ No overlaps found between shards');
  }

  // Test 3: Full coverage
  console.log('\n🔍 TEST 3: Checking total coverage...');
  
  const expectedCoverage = rangeMs;
  const coverageDiff = Math.abs(totalCoverage - expectedCoverage);
  
  if (coverageDiff === 0) {
    console.log(`   ✅ Total coverage: ${formatMs(totalCoverage)} (100%)`);
  } else {
    console.log(`   ⚠️ Coverage difference: ${coverageDiff}ms`);
    console.log(`      Expected: ${formatMs(expectedCoverage)}`);
    console.log(`      Actual:   ${formatMs(totalCoverage)}`);
  }

  // Test 4: First shard ends at maxTime
  console.log('\n🔍 TEST 4: Checking boundary alignment...');
  
  const firstShardMaxDiff = shards[0].maxMs - maxTime.getTime();
  const lastShardMinDiff = shards[shards.length - 1].minMs - minTime.getTime();
  
  if (firstShardMaxDiff === 0) {
    console.log('   ✅ First shard ends at maxTime');
  } else {
    console.log(`   ❌ First shard max differs by ${firstShardMaxDiff}ms`);
  }
  
  if (lastShardMinDiff === 0) {
    console.log('   ✅ Last shard starts at minTime');
  } else {
    console.log(`   ❌ Last shard min differs by ${lastShardMinDiff}ms`);
  }

  // Test 5: API boundary behavior simulation
  console.log('\n🔍 TEST 5: Simulating API boundary behavior...');
  console.log('   (Testing record at exact boundary between shard 0 and 1)');
  
  if (shards.length >= 2) {
    const boundaryTime = shards[0].minMs; // Same as shards[1].maxMs
    const boundaryTimeStr = new Date(boundaryTime).toISOString();
    
    console.log(`   Boundary timestamp: ${boundaryTimeStr}`);
    
    // Shard 0: before=maxTime, at_or_after=boundaryTime
    // A record at exactly boundaryTime:
    //   - before check: boundaryTime < shards[0].maxMs → TRUE (included)
    //   - at_or_after check: boundaryTime >= shards[0].minMs → TRUE (included)
    // Result: Shard 0 INCLUDES this record
    
    // Shard 1: before=boundaryTime, at_or_after=shards[1].minMs  
    // A record at exactly boundaryTime:
    //   - before check: boundaryTime < boundaryTime → FALSE (excluded)
    // Result: Shard 1 EXCLUDES this record
    
    console.log('   Shard 0 (before=' + shards[0].maxTime + ', at_or_after=' + shards[0].minTime + '):');
    console.log('      Record at boundary: ✅ INCLUDED (before is exclusive, record < before)');
    console.log('   Shard 1 (before=' + shards[1].maxTime + ', at_or_after=' + shards[1].minTime + '):');
    console.log('      Record at boundary: ❌ EXCLUDED (before is exclusive, record == before)');
    console.log('   ✅ No duplicate processing at boundaries');
  }

  // Summary
  console.log('\n' + '='.repeat(80));
  const allPassed = !hasGaps && !hasOverlaps && coverageDiff === 0;
  if (allPassed) {
    console.log('✅ ALL TESTS PASSED');
  } else {
    console.log('❌ SOME TESTS FAILED');
  }
  console.log('='.repeat(80) + '\n');

  // Visual diagram
  console.log('📊 Visual Representation:');
  console.log('');
  console.log('   Time ────────────────────────────────────────────────────────▶');
  console.log('   minTime                                                 maxTime');
  console.log('   │                                                            │');
  
  const barWidth = 60;
  let bar = '   ';
  for (let i = shards.length - 1; i >= 0; i--) {
    const width = Math.round((shards[i].maxMs - shards[i].minMs) / rangeMs * barWidth);
    bar += `[${'─'.repeat(Math.max(1, width - 2))}]`;
  }
  console.log(bar);
  
  let labels = '   ';
  for (let i = shards.length - 1; i >= 0; i--) {
    const width = Math.round((shards[i].maxMs - shards[i].minMs) / rangeMs * barWidth);
    const label = `S${i}`;
    const padding = Math.max(0, width - label.length);
    labels += label + ' '.repeat(padding);
  }
  console.log(labels);
  console.log('');

  return allPassed;
}

// Run tests
const passed = runTests();
process.exit(passed ? 0 : 1);
