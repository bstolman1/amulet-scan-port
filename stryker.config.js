/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
  // Package manager
  packageManager: 'npm',
  
  // Test runner
  testRunner: 'vitest',
  
  // Start with a focused subset for initial run (faster, more stable)
  mutate: [
    'src/lib/amount-utils.ts',
    'src/lib/utils.ts',
    'server/lib/validate.js',
    'server/lib/sql-sanitize.js',
  ],
  
  // Vitest configuration
  vitest: {
    configFile: 'vitest.config.ts',
  },
  
  // Reporters
  reporters: ['clear-text', 'progress'],
  
  // Thresholds
  thresholds: {
    high: 80,
    low: 60,
    break: null,
  },
  
  // Conservative performance settings for stability
  concurrency: 1,
  timeoutMS: 60000,
  timeoutFactor: 3,
  
  // Disable incremental for clean run
  incremental: false,
  
  // Disable specific mutators that create too much noise
  mutator: {
    excludedMutations: [
      'StringLiteral',
      'ObjectLiteral',
    ],
  },
};
