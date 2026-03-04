/**
 * File-backed repository layer.
 *
 * All I/O is async (fs.promises) so the event loop is never blocked.
 * Each exported function is narrowly scoped: one file, one concern.
 *
 * Callers receive typed results; raw FS/JSON errors are wrapped and re-thrown
 * so callers can distinguish "not found" from real I/O failures.
 */

import fs from 'fs/promises';
import { existsSync, mkdirSync } from 'fs';
import path from 'path';
import {
  CACHE_DIR,
  CACHE_FILE,
  OVERRIDES_FILE,
  LEARNED_PATTERNS_FILE,
  AUDIT_LOG_FILE,
  PATTERN_BACKUPS_DIR,
} from '../utils/constants.js';

// ── Helpers ────────────────────────────────────────────────────────────────

/** Ensure a directory exists (sync, called once at startup). */
export function ensureDirs() {
  for (const dir of [CACHE_DIR, PATTERN_BACKUPS_DIR]) {
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  }
}

/**
 * Read and JSON-parse a file.
 * @returns {Promise<T|null>} Parsed data, or null if the file does not exist.
 * @throws If the file exists but cannot be read or parsed.
 */
async function readJSON(filePath) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw new Error(`Failed to read ${path.basename(filePath)}: ${err.message}`);
  }
}

/**
 * Atomically write JSON to a file via a temp file + rename,
 * preventing partial writes from corrupting data.
 */
async function writeJSON(filePath, data) {
  const tmp = `${filePath}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(data, null, 2), 'utf8');
  await fs.rename(tmp, filePath);
}

// ── Governance cache ───────────────────────────────────────────────────────

/** @returns {Promise<object|null>} */
export async function readCache() {
  return readJSON(CACHE_FILE);
}

/** @param {object} data */
export async function writeCache(data) {
  await writeJSON(CACHE_FILE, data);
}

// ── Overrides ──────────────────────────────────────────────────────────────

const EMPTY_OVERRIDES = () => ({
  itemOverrides: {},
  topicOverrides: {},
  mergeOverrides: {},
  extractOverrides: {},
  moveOverrides: {},
});

/** @returns {Promise<OverridesShape>} */
export async function readOverrides() {
  const data = await readJSON(OVERRIDES_FILE);
  if (!data) return EMPTY_OVERRIDES();
  return {
    itemOverrides: data.itemOverrides ?? {},
    topicOverrides: data.topicOverrides ?? {},
    mergeOverrides: data.mergeOverrides ?? {},
    extractOverrides: data.extractOverrides ?? {},
    moveOverrides: data.moveOverrides ?? {},
  };
}

/** @param {OverridesShape} overrides */
export async function writeOverrides(overrides) {
  await writeJSON(OVERRIDES_FILE, overrides);
}

// ── Audit log ──────────────────────────────────────────────────────────────

/** @returns {Promise<AuditEntry[]>} */
export async function readAuditLog() {
  const data = await readJSON(AUDIT_LOG_FILE);
  return Array.isArray(data) ? data : [];
}

/** @param {AuditEntry[]} entries */
export async function writeAuditLog(entries) {
  await writeJSON(AUDIT_LOG_FILE, entries);
}

// ── Learned patterns ───────────────────────────────────────────────────────

/** @returns {Promise<LearnedPatternsFile|null>} */
export async function readLearnedPatternsFile() {
  return readJSON(LEARNED_PATTERNS_FILE);
}

/** @param {LearnedPatternsFile} data */
export async function writeLearnedPatternsFile(data) {
  await writeJSON(LEARNED_PATTERNS_FILE, data);
}

/**
 * Backup the current patterns file before overwriting.
 * @param {string} version - e.g. "1.2.3"
 */
export async function backupLearnedPatterns(version) {
  const src = LEARNED_PATTERNS_FILE;
  const dest = path.join(PATTERN_BACKUPS_DIR, `learned-patterns-v${version}.json`);
  try {
    // Only backup if the destination doesn't already exist
    await fs.access(dest);
  } catch {
    try {
      await fs.copyFile(src, dest);
      console.log(`📦 Backed up patterns v${version}`);
    } catch (err) {
      console.warn(`Could not backup patterns v${version}: ${err.message}`);
    }
  }
}

/**
 * List available backup versions.
 * @returns {Promise<string[]>} Sorted version strings (oldest first).
 */
export async function listPatternBackups() {
  try {
    const files = await fs.readdir(PATTERN_BACKUPS_DIR);
    return files
      .filter(f => f.startsWith('learned-patterns-v') && f.endsWith('.json'))
      .map(f => f.replace('learned-patterns-v', '').replace('.json', ''))
      .sort();
  } catch {
    return [];
  }
}

/**
 * Read a specific backup version.
 * @param {string} version
 * @returns {Promise<LearnedPatternsFile|null>}
 */
export async function readPatternBackup(version) {
  const filePath = path.join(PATTERN_BACKUPS_DIR, `learned-patterns-v${version}.json`);
  return readJSON(filePath);
}
