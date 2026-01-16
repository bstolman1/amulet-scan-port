/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
  // Package manager
  packageManager: 'npm',
  
  // Test runner
  testRunner: 'vitest',
  
  // Files to mutate (source code, not tests)
  mutate: [
    'src/lib/**/*.ts',
    'src/hooks/**/*.ts',
    '!src/**/*.test.ts',
    '!src/**/*.spec.ts',
    '!src/**/*.d.ts',
    'server/lib/**/*.js',
    'server/engine/aggregations.js',
    '!server/**/*.test.js',
  ],
  
  // Vitest configuration
  vitest: {
    configFile: 'vitest.config.ts',
  },
  
  // Reporters
  reporters: ['html', 'clear-text', 'progress', 'json'],
  htmlReporter: {
    fileName: 'coverage/mutation/index.html',
  },
  jsonReporter: {
    fileName: 'coverage/mutation/mutation-report.json',
  },
  
  // Thresholds
  thresholds: {
    high: 80,
    low: 60,
    break: null, // Don't fail CI on low mutation score (yet)
  },
  
  // Performance
  concurrency: 4,
  timeoutMS: 30000,
  timeoutFactor: 2,
  
  // Incremental mode for faster reruns
  incremental: true,
  incrementalFile: '.stryker-cache/incremental.json',
  
  // Ignore patterns (reduce noise)
  ignorers: ['regex'],
  
  // Disable specific mutators that create too much noise
  mutator: {
    excludedMutations: [
      'StringLiteral', // Don't mutate string literals (too noisy)
      'ObjectLiteral', // Don't mutate object literals
    ],
  },
  
  // Reduce scope for initial run
  // Uncomment to run on specific files only
  // mutate: ['src/lib/amount-utils.ts'],
  
  // Dashboard (optional - for CI integration)
  // dashboard: {
  //   project: 'github.com/your-org/amulet-scan',
  //   version: 'main',
  // },
};
