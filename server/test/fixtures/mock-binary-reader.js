/**
 * Mock binary reader for testing
 */

import { mockEvents, mockGovernanceEvents } from './mock-data.js';

/**
 * Check if binary files exist
 */
export function hasBinaryFiles(dataPath, prefix) {
  // Simulate having binary files
  return true;
}

/**
 * Find binary files
 */
export function findBinaryFiles(dataPath, prefix) {
  return [
    `${dataPath}/year=2025/month=01/day=10/${prefix}-1736510400000-001.bin`,
    `${dataPath}/year=2025/month=01/day=09/${prefix}-1736424000000-001.bin`,
  ];
}

/**
 * Find binary files with fast search (limited days/files)
 */
export function findBinaryFilesFast(dataPath, prefix, options = {}) {
  return findBinaryFiles(dataPath, prefix).slice(0, options.maxFiles || 10);
}

/**
 * Count binary files
 */
export function countBinaryFiles(dataPath, prefix) {
  return 100;
}

/**
 * Read a single binary file
 */
export async function readBinaryFile(filePath) {
  return {
    records: mockEvents,
    count: mockEvents.length,
  };
}

/**
 * Load all records from binary files
 */
export async function loadAllRecords(dataPath, prefix) {
  return mockEvents;
}

/**
 * Stream records with filtering
 */
export async function streamRecords(dataPath, prefix, options = {}) {
  let records = [...mockEvents];
  
  // Apply filter if provided
  if (options.filter) {
    records = records.filter(options.filter);
  }
  
  // Apply limit
  const limit = options.limit || 100;
  const offset = options.offset || 0;
  
  const paginatedRecords = records.slice(offset, offset + limit);
  
  return {
    records: paginatedRecords,
    hasMore: offset + limit < records.length,
  };
}

export default {
  hasBinaryFiles,
  findBinaryFiles,
  findBinaryFilesFast,
  countBinaryFiles,
  readBinaryFile,
  loadAllRecords,
  streamRecords,
};
