/**
 * Party Indexer - Builds persistent index mapping Party IDs to their event files
 * 
 * Scans binary .pb.zst files and maintains a mapping:
 *   partyId → [{ file, eventCount, firstSeen, lastSeen }]
 * 
 * This enables O(1) lookup of which files contain a party's events,
 * instead of scanning the entire backfill.
 */

import { query, queryOne, DATA_PATH } from '../duckdb/connection.js';
import * as binaryReader from '../duckdb/binary-reader.js';
import fs from 'fs';
import path from 'path';

const INDEX_FILE = path.join(DATA_PATH, 'party-index.json');
const STATE_FILE = path.join(DATA_PATH, 'party-index-state.json');

let indexingInProgress = false;
let indexingProgress = null;

/**
 * Get current indexing state
 */
export function getIndexState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
    }
  } catch (err) {
    console.error('Error reading party index state:', err.message);
  }
  return { 
    lastIndexedAt: null, 
    totalFiles: 0, 
    totalParties: 0,
    filesIndexed: 0,
    status: 'not_started'
  };
}

/**
 * Save indexing state
 */
function saveIndexState(state) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (err) {
    console.error('Error saving party index state:', err.message);
  }
}

/**
 * Load the party index from disk
 */
export function loadPartyIndex() {
  try {
    if (fs.existsSync(INDEX_FILE)) {
      const data = JSON.parse(fs.readFileSync(INDEX_FILE, 'utf-8'));
      return new Map(Object.entries(data));
    }
  } catch (err) {
    console.error('Error loading party index:', err.message);
  }
  return new Map();
}

/**
 * Save the party index to disk
 */
function savePartyIndex(index) {
  try {
    const obj = Object.fromEntries(index);
    fs.writeFileSync(INDEX_FILE, JSON.stringify(obj));
  } catch (err) {
    console.error('Error saving party index:', err.message);
  }
}

/**
 * Get indexing progress
 */
export function getIndexingProgress() {
  if (!indexingInProgress) return null;
  return indexingProgress;
}

/**
 * Check if index exists and has data
 */
export function isIndexPopulated() {
  try {
    if (fs.existsSync(INDEX_FILE)) {
      const stats = fs.statSync(INDEX_FILE);
      return stats.size > 100; // More than just "{}"
    }
  } catch {}
  return false;
}

/**
 * Get party index stats
 */
export function getPartyIndexStats() {
  const state = getIndexState();
  const index = loadPartyIndex();
  
  return {
    totalParties: index.size,
    totalFiles: state.totalFiles || 0,
    filesIndexed: state.filesIndexed || 0,
    lastIndexedAt: state.lastIndexedAt,
    status: state.status || 'unknown',
    isPopulated: isIndexPopulated(),
  };
}

/**
 * Get files containing a specific party's events
 */
export function getFilesForParty(partyId) {
  const index = loadPartyIndex();
  return index.get(partyId) || [];
}

/**
 * Get events for a party using the index (fast path)
 */
export async function getPartyEventsFromIndex(partyId, limit = 100) {
  const fileInfos = getFilesForParty(partyId);
  
  if (fileInfos.length === 0) {
    return { events: [], total: 0, indexed: true };
  }
  
  // Sort files by lastSeen descending (most recent first)
  const sortedFiles = [...fileInfos].sort((a, b) => {
    const aTime = new Date(a.lastSeen || 0).getTime();
    const bTime = new Date(b.lastSeen || 0).getTime();
    return bTime - aTime;
  });
  
  const allEvents = [];
  
  for (const fileInfo of sortedFiles) {
    if (allEvents.length >= limit) break;
    
    try {
      const filePath = fileInfo.file;
      if (!fs.existsSync(filePath)) continue;
      
      const result = await binaryReader.readBinaryFile(filePath);
      
      // Filter events for this party
      const partyEvents = result.records.filter(r => 
        (r.signatories && r.signatories.includes(partyId)) ||
        (r.observers && r.observers.includes(partyId))
      );
      
      allEvents.push(...partyEvents);
    } catch (err) {
      console.warn(`Error reading file for party index: ${fileInfo.file}`, err.message);
    }
  }
  
  // Sort by timestamp descending
  allEvents.sort((a, b) => {
    const aTime = new Date(a.timestamp || 0).getTime();
    const bTime = new Date(b.timestamp || 0).getTime();
    return bTime - aTime;
  });
  
  return {
    events: allEvents.slice(0, limit),
    total: allEvents.length,
    filesScanned: Math.min(sortedFiles.length, limit),
    indexed: true,
  };
}

/**
 * Build the party index by scanning all binary files
 */
export async function buildPartyIndex(options = {}) {
  const { forceRebuild = false, onProgress = null } = options;
  
  if (indexingInProgress) {
    return { status: 'already_running', progress: indexingProgress };
  }
  
  const state = getIndexState();
  
  // Check if we need to rebuild
  if (!forceRebuild && state.status === 'completed') {
    console.log('Party index already complete, skipping rebuild');
    return { status: 'already_complete', stats: getPartyIndexStats() };
  }
  
  indexingInProgress = true;
  indexingProgress = { phase: 'scanning', filesScanned: 0, totalFiles: 0, partiesFound: 0 };
  
  console.log('🔍 Building party index...');
  
  try {
    // Find all event files
    const files = binaryReader.findBinaryFiles(DATA_PATH, 'events');
    indexingProgress.totalFiles = files.length;
    
    console.log(`   Found ${files.length} event files to scan`);
    
    // Build index: partyId → [{ file, eventCount, firstSeen, lastSeen }]
    const index = new Map();
    let filesProcessed = 0;
    
    for (const filePath of files) {
      try {
        const result = await binaryReader.readBinaryFile(filePath);
        
        // Track parties in this file
        const fileParties = new Map(); // partyId → { count, first, last }
        
        for (const event of result.records) {
          const parties = new Set([
            ...(event.signatories || []),
            ...(event.observers || []),
          ]);
          
          const eventTime = event.timestamp || event.effective_at;
          
          for (const partyId of parties) {
            if (!partyId) continue;
            
            if (!fileParties.has(partyId)) {
              fileParties.set(partyId, { count: 0, first: eventTime, last: eventTime });
            }
            
            const info = fileParties.get(partyId);
            info.count++;
            if (eventTime && (!info.first || eventTime < info.first)) info.first = eventTime;
            if (eventTime && (!info.last || eventTime > info.last)) info.last = eventTime;
          }
        }
        
        // Merge file parties into main index
        for (const [partyId, fileInfo] of fileParties) {
          if (!index.has(partyId)) {
            index.set(partyId, []);
          }
          
          index.get(partyId).push({
            file: filePath,
            eventCount: fileInfo.count,
            firstSeen: fileInfo.first,
            lastSeen: fileInfo.last,
          });
        }
        
        filesProcessed++;
        indexingProgress.filesScanned = filesProcessed;
        indexingProgress.partiesFound = index.size;
        
        if (onProgress) {
          onProgress(indexingProgress);
        }
        
        // Log progress every 100 files
        if (filesProcessed % 100 === 0) {
          console.log(`   Processed ${filesProcessed}/${files.length} files, ${index.size} parties found`);
        }
        
        // Save checkpoint every 500 files
        if (filesProcessed % 500 === 0) {
          savePartyIndex(index);
          saveIndexState({
            lastIndexedAt: new Date().toISOString(),
            totalFiles: files.length,
            filesIndexed: filesProcessed,
            totalParties: index.size,
            status: 'in_progress',
          });
        }
        
      } catch (err) {
        console.warn(`Error processing file ${filePath}:`, err.message);
      }
    }
    
    // Save final index
    savePartyIndex(index);
    saveIndexState({
      lastIndexedAt: new Date().toISOString(),
      totalFiles: files.length,
      filesIndexed: filesProcessed,
      totalParties: index.size,
      status: 'completed',
    });
    
    console.log(`✅ Party index complete: ${index.size} parties across ${filesProcessed} files`);
    
    indexingInProgress = false;
    indexingProgress = null;
    
    return { 
      status: 'completed', 
      totalParties: index.size, 
      filesProcessed,
    };
    
  } catch (err) {
    console.error('Error building party index:', err);
    indexingInProgress = false;
    indexingProgress = null;
    
    saveIndexState({
      ...getIndexState(),
      status: 'error',
      error: err.message,
    });
    
    throw err;
  }
}

/**
 * Get party summary from the index
 */
export function getPartySummaryFromIndex(partyId) {
  const fileInfos = getFilesForParty(partyId);
  
  if (fileInfos.length === 0) {
    return null;
  }
  
  let totalEvents = 0;
  let firstSeen = null;
  let lastSeen = null;
  
  for (const info of fileInfos) {
    totalEvents += info.eventCount || 0;
    
    if (info.firstSeen) {
      if (!firstSeen || info.firstSeen < firstSeen) {
        firstSeen = info.firstSeen;
      }
    }
    
    if (info.lastSeen) {
      if (!lastSeen || info.lastSeen > lastSeen) {
        lastSeen = info.lastSeen;
      }
    }
  }
  
  return {
    partyId,
    totalEvents,
    filesWithEvents: fileInfos.length,
    firstSeen,
    lastSeen,
    indexed: true,
  };
}

/**
 * Search parties by prefix
 */
export function searchPartiesByPrefix(prefix, limit = 50) {
  const index = loadPartyIndex();
  const matches = [];
  
  const lowerPrefix = prefix.toLowerCase();
  
  for (const [partyId, fileInfos] of index) {
    if (partyId.toLowerCase().startsWith(lowerPrefix)) {
      let totalEvents = 0;
      for (const info of fileInfos) {
        totalEvents += info.eventCount || 0;
      }
      
      matches.push({
        partyId,
        totalEvents,
        fileCount: fileInfos.length,
      });
      
      if (matches.length >= limit) break;
    }
  }
  
  // Sort by total events descending
  matches.sort((a, b) => b.totalEvents - a.totalEvents);
  
  return matches;
}
