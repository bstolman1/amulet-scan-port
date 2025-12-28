/**
 * Post Content Cache - Persistent storage for groups.io post content
 * 
 * Design principle: "LLMs may read raw human text exactly once per artifact;
 * results are cached and treated as authoritative governance metadata."
 * 
 * Stores:
 * - subject: Post subject line
 * - body: Full post body (plain text)
 * - author: Post author
 * - timestamp: Original post timestamp
 * - thread_id: Thread/topic ID
 * - source_url: URL to the post
 * - fetched_at: When we fetched it
 * - content_hash: SHA256 hash of subject+body for change detection
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE_DATA_DIR = process.env.DATA_DIR || path.resolve(__dirname, '../../data');
const CONTENT_CACHE_DIR = path.join(BASE_DATA_DIR, 'cache', 'post-content');
const CONTENT_INDEX_FILE = path.join(CONTENT_CACHE_DIR, 'index.json');

// Ensure cache directory exists
function ensureCacheDir() {
  if (!fs.existsSync(CONTENT_CACHE_DIR)) {
    fs.mkdirSync(CONTENT_CACHE_DIR, { recursive: true });
  }
}

/**
 * Generate a content hash for change detection
 */
export function generateContentHash(subject, body) {
  const content = `${subject || ''}\n${body || ''}`;
  return crypto.createHash('sha256').update(content).digest('hex').substring(0, 16);
}

/**
 * Read the content index
 */
export function readContentIndex() {
  try {
    if (fs.existsSync(CONTENT_INDEX_FILE)) {
      return JSON.parse(fs.readFileSync(CONTENT_INDEX_FILE, 'utf-8'));
    }
  } catch (err) {
    console.error('Error reading content index:', err.message);
  }
  return { posts: {}, stats: { totalPosts: 0, lastUpdated: null } };
}

/**
 * Write the content index
 */
function writeContentIndex(index) {
  ensureCacheDir();
  index.stats.lastUpdated = new Date().toISOString();
  fs.writeFileSync(CONTENT_INDEX_FILE, JSON.stringify(index, null, 2));
}

/**
 * Get cached content for a post by topic ID
 * @returns {object|null} Cached content or null if not found
 */
export function getCachedContent(topicId) {
  const index = readContentIndex();
  return index.posts[String(topicId)] || null;
}

/**
 * Check if content has changed (by comparing hashes)
 */
export function hasContentChanged(topicId, newSubject, newBody) {
  const cached = getCachedContent(topicId);
  if (!cached) return true; // No cache = treat as changed
  
  const newHash = generateContentHash(newSubject, newBody);
  return cached.content_hash !== newHash;
}

/**
 * Store post content in cache
 */
export function cachePostContent({
  topicId,
  subject,
  body,
  author,
  timestamp,
  sourceUrl,
  groupName,
}) {
  ensureCacheDir();
  const index = readContentIndex();
  
  const contentHash = generateContentHash(subject, body);
  const idStr = String(topicId);
  
  // Check if content actually changed
  const existing = index.posts[idStr];
  if (existing && existing.content_hash === contentHash) {
    // No change, don't update
    return existing;
  }
  
  const entry = {
    topic_id: idStr,
    subject: subject || '',
    body: body || '',
    author: author || null,
    timestamp: timestamp || null,
    source_url: sourceUrl || null,
    group_name: groupName || null,
    fetched_at: new Date().toISOString(),
    content_hash: contentHash,
  };
  
  index.posts[idStr] = entry;
  index.stats.totalPosts = Object.keys(index.posts).length;
  
  writeContentIndex(index);
  
  return entry;
}

/**
 * Bulk cache multiple posts (more efficient for batch operations)
 */
export function cachePostsBulk(posts) {
  ensureCacheDir();
  const index = readContentIndex();
  let newCount = 0;
  let updatedCount = 0;
  
  for (const post of posts) {
    const idStr = String(post.topicId);
    const contentHash = generateContentHash(post.subject, post.body);
    
    const existing = index.posts[idStr];
    if (existing && existing.content_hash === contentHash) {
      continue; // No change
    }
    
    if (existing) {
      updatedCount++;
    } else {
      newCount++;
    }
    
    index.posts[idStr] = {
      topic_id: idStr,
      subject: post.subject || '',
      body: post.body || '',
      author: post.author || null,
      timestamp: post.timestamp || null,
      source_url: post.sourceUrl || null,
      group_name: post.groupName || null,
      fetched_at: new Date().toISOString(),
      content_hash: contentHash,
    };
  }
  
  if (newCount > 0 || updatedCount > 0) {
    index.stats.totalPosts = Object.keys(index.posts).length;
    writeContentIndex(index);
    console.log(`📄 Content cache: ${newCount} new, ${updatedCount} updated, ${index.stats.totalPosts} total`);
  }
  
  return { newCount, updatedCount, total: index.stats.totalPosts };
}

/**
 * Get stats about the content cache
 */
export function getContentCacheStats() {
  const index = readContentIndex();
  return {
    totalPosts: index.stats.totalPosts || 0,
    lastUpdated: index.stats.lastUpdated || null,
    cacheLocation: CONTENT_CACHE_DIR,
  };
}

/**
 * Clear the entire content cache (for testing/maintenance)
 */
export function clearContentCache() {
  if (fs.existsSync(CONTENT_INDEX_FILE)) {
    fs.unlinkSync(CONTENT_INDEX_FILE);
  }
  console.log('🗑️ Content cache cleared');
}
