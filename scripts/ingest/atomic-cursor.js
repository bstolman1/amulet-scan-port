/**
 * Atomic Cursor Operations - Crash-Safe Cursor Management
 * 
 * CRITICAL: Cursor updates must be atomic with data writes.
 * 
 * This module provides:
 * 1. Atomic file writes (write to temp, fsync, rename, fsync parent dir)
 * 2. Transactional cursor updates (only after data confirmed)
 * 3. Recovery from partial writes
 * 
 * The cursor file is the source of truth for resume position.
 * If we crash between data write and cursor update: data duplicates (safe)
 * If we crash between cursor update and data write: data loss (UNSAFE)
 * 
 * This module ensures cursor ONLY updates AFTER data is durable.
 */

import { existsSync, readFileSync, writeFileSync, renameSync, mkdirSync, unlinkSync, statSync, openSync, fsyncSync, closeSync } from 'fs';
import { join, dirname } from 'path';

// Configuration - cross-platform path handling
import { getBaseDataDir, getCursorDir } from './path-utils.js';
const BASE_DATA_DIR = getBaseDataDir();
const CURSOR_DIR = getCursorDir();

/**
 * Sanitize string for filename
 */
function sanitize(str) {
  return str.replace(/[^a-zA-Z0-9-_]/g, '_').substring(0, 50);
}

/**
 * Get cursor file path (shard-aware)
 */
export function getCursorPath(migrationId, synchronizerId, shardIndex = null) {
  const shardSuffix = shardIndex !== null ? `-shard${shardIndex}` : '';
  return join(CURSOR_DIR, `cursor-${migrationId}-${sanitize(synchronizerId)}${shardSuffix}.json`);
}

/**
 * Atomic file write using write-to-temp-then-rename pattern
 * 
 * This ensures the cursor file is never corrupted:
 * - Write to .tmp file first
 * - fsync the temp file via a writable fd (ensures data is flushed)
 * - Atomic rename to final path
 * - fsync the parent directory (ensures rename is durable on Linux)
 * 
 * If crash happens during write: .tmp file is incomplete, original intact
 * If crash happens during rename: atomic, so either old or new is complete
 * 
 * FIX #1: fsync now uses a writable fd (r+ instead of r) so the flush is
 *         guaranteed to reach disk rather than silently no-op'ing.
 * FIX #4: Parent directory is fsynced after rename so the rename itself
 *         survives a crash on Linux (ext4, xfs, etc.).
 * FIX #7: Backup is written via temp+rename rather than a bare writeFileSync
 *         so a crash mid-backup doesn't corrupt the backup file.
 */
export function atomicWriteFile(filePath, data) {
  const tempPath = filePath + '.tmp';
  const backupPath = filePath + '.bak';
  const backupTempPath = backupPath + '.tmp';
  const dir = dirname(filePath);

  try {
    // Ensure directory exists
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    // Write content to temp file
    const content = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    writeFileSync(tempPath, content, { encoding: 'utf8', flag: 'w' });

    // FIX #1: Open with 'r+' (read-write) so fsync actually flushes write buffers.
    // Opening read-only ('r') may silently no-op on some operating systems.
    const fd = openSync(tempPath, 'r+');
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }

    // FIX #7: Write backup atomically (temp+rename) so a crash mid-backup
    // doesn't leave a corrupted .bak file.
    if (existsSync(filePath)) {
      try {
        const existing = readFileSync(filePath, 'utf8');
        JSON.parse(existing); // Only backup if current file is valid JSON
        writeFileSync(backupTempPath, existing, { encoding: 'utf8', flag: 'w' });
        renameSync(backupTempPath, backupPath);
      } catch {
        // Existing file is corrupted or backup write failed — skip backup
        try { if (existsSync(backupTempPath)) unlinkSync(backupTempPath); } catch {}
      }
    }

    // Atomic rename (this is the commit point)
    renameSync(tempPath, filePath);

    // FIX #4: fsync the parent directory so the rename is durable on Linux.
    // Without this, a crash immediately after rename may leave the directory
    // entry pointing to the old file on filesystems like ext4.
    try {
      const dirFd = openSync(dir, 'r');
      try {
        fsyncSync(dirFd);
      } finally {
        closeSync(dirFd);
      }
    } catch {
      // Directory fsync is not supported on all platforms (e.g. Windows).
      // Silently ignore — the rename is still as durable as the OS allows.
    }

    return true;
  } catch (err) {
    // Clean up temp files if they exist
    try { if (existsSync(tempPath)) unlinkSync(tempPath); } catch {}
    try { if (existsSync(backupTempPath)) unlinkSync(backupTempPath); } catch {}

    throw new Error(`Atomic write failed for ${filePath}: ${err.message}`);
  }
}

/**
 * Read cursor with recovery from corrupted state
 */
function readCursorSafe(filePath) {
  const backupPath = filePath + '.bak';

  // Try main file first
  if (existsSync(filePath)) {
    try {
      const content = readFileSync(filePath, 'utf8').trim();
      if (content) {
        return JSON.parse(content);
      }
    } catch (err) {
      console.warn(`⚠️ Cursor file corrupted: ${filePath}. Trying backup.`);
    }
  }

  // Try backup file
  if (existsSync(backupPath)) {
    try {
      const content = readFileSync(backupPath, 'utf8').trim();
      if (content) {
        const cursor = JSON.parse(content);
        console.log(`✅ Recovered cursor from backup: ${backupPath}`);
        // Restore backup to main file atomically
        atomicWriteFile(filePath, cursor);
        return cursor;
      }
    } catch (err) {
      console.warn(`⚠️ Backup cursor also corrupted: ${backupPath}`);
    }
  }

  return null;
}

/**
 * Atomic Cursor Manager
 * 
 * Tracks pending data and only commits cursor after data is confirmed written.
 * 
 * Usage pattern:
 *   cursor.beginTransaction(updates, events, timestamp);
 *   // ... write data to disk ...
 *   cursor.commit();   // only call after data is durable
 * 
 * Never call saveAtomic() while a transaction is open — it will throw.
 */
export class AtomicCursor {
  constructor(migrationId, synchronizerId, shardIndex = null, shardTotal = null) {
    this.migrationId = migrationId;
    this.synchronizerId = synchronizerId;
    this.shardIndex = shardIndex;
    this.shardTotal = shardTotal;
    this.cursorPath = getCursorPath(migrationId, synchronizerId, shardIndex);

    // Confirmed state (safe to resume from)
    this.confirmedState = {
      lastBefore: null,
      totalUpdates: 0,
      totalEvents: 0,
      minTime: null,
      maxTime: null,
      complete: false,
      // GCS-aware crash safety: separate tracking for GCS-confirmed position
      lastGCSConfirmed: null,
      gcsConfirmedUpdates: 0,
      gcsConfirmedEvents: 0,
    };

    // Pending state (not yet confirmed — never persisted as a resume point)
    this.pendingState = {
      updates: 0,
      events: 0,
      lastBefore: null,
    };

    // Transaction tracking
    this.inTransaction = false;
    this.transactionStartState = null;
  }

  /**
   * Load cursor from disk
   */
  load() {
    const data = readCursorSafe(this.cursorPath);

    if (data) {
      this.confirmedState = {
        lastBefore: data.last_confirmed_before || data.last_before || null,
        totalUpdates: data.confirmed_updates || data.total_updates || 0,
        totalEvents: data.confirmed_events || data.total_events || 0,
        minTime: data.min_time || null,
        maxTime: data.max_time || null,
        complete: data.complete || false,
        // GCS-aware fields
        lastGCSConfirmed: data.last_gcs_confirmed || null,
        gcsConfirmedUpdates: data.gcs_confirmed_updates || 0,
        gcsConfirmedEvents: data.gcs_confirmed_events || 0,
      };

      // Log recovery if there was pending data at the time of the crash.
      // We do NOT resume from pending — we resume from confirmed only.
      if (data.pending_updates > 0 || data.pending_events > 0) {
        console.log(
          `⚠️ Cursor has pending data from crash: ${data.pending_updates} updates, ${data.pending_events} events. ` +
          `Resuming from confirmed position: ${this.confirmedState.lastBefore}`
        );
      }

      // GCS crash safety: warn if local cursor is ahead of GCS-confirmed
      if (this.confirmedState.lastGCSConfirmed && this.confirmedState.lastBefore) {
        const localTime = new Date(this.confirmedState.lastBefore).getTime();
        const gcsTime = new Date(this.confirmedState.lastGCSConfirmed).getTime();
        if (localTime > gcsTime) {
          console.log(
            `⚠️ GCS gap detected: local cursor at ${this.confirmedState.lastBefore} ` +
            `but GCS only confirmed to ${this.confirmedState.lastGCSConfirmed}. ` +
            `Will re-fetch ${this.confirmedState.totalUpdates - this.confirmedState.gcsConfirmedUpdates} updates.`
          );
        }
      }
    }

    return this.confirmedState;
  }

  /**
   * Get current resume position.
   * 
   * By default, returns the GCS-confirmed position (crash-safe).
   * Pass useLocalPosition=true to get the local-disk position (faster but risky).
   */
  getResumePosition(useLocalPosition = false) {
    // GCS-aware: prefer GCS-confirmed position for crash safety
    if (!useLocalPosition && this.confirmedState.lastGCSConfirmed) {
      return {
        lastBefore: this.confirmedState.lastGCSConfirmed,
        totalUpdates: this.confirmedState.gcsConfirmedUpdates,
        totalEvents: this.confirmedState.gcsConfirmedEvents,
        isGCSConfirmed: true,
      };
    }

    // Fallback to local position (legacy behavior or no GCS checkpoint yet)
    return {
      lastBefore: this.confirmedState.lastBefore,
      totalUpdates: this.confirmedState.totalUpdates,
      totalEvents: this.confirmedState.totalEvents,
      isGCSConfirmed: false,
    };
  }

  /**
   * Get the local (unsafe) resume position - for debugging only.
   * This position may have data that never reached GCS.
   */
  getUnsafeResumePosition() {
    return {
      lastBefore: this.confirmedState.lastBefore,
      totalUpdates: this.confirmedState.totalUpdates,
      totalEvents: this.confirmedState.totalEvents,
    };
  }

  /**
   * Begin a transaction - record pending data
   * 
   * Call this BEFORE writing data to disk.
   * If we crash after this but before commit: data is lost (but cursor hasn't advanced)
   * This is safe — we'll refetch the same data on restart.
   */
  beginTransaction(updates, events, beforeTimestamp) {
    if (this.inTransaction) {
      throw new Error('Already in transaction. Call commit() or rollback() first.');
    }

    this.inTransaction = true;
    this.transactionStartState = { ...this.confirmedState };

    this.pendingState = {
      updates,
      events,
      lastBefore: beforeTimestamp,
    };

    // Write pending state to disk so crash recovery can log what was in-flight.
    // NOTE: pending state is never used as a resume point — only confirmedState is.
    this._writePendingState();
  }

  /**
   * Record additional pending data within a transaction
   */
  addPending(updates, events, beforeTimestamp) {
    if (!this.inTransaction) {
      this.beginTransaction(updates, events, beforeTimestamp);
      return;
    }

    this.pendingState.updates += updates;
    this.pendingState.events += events;
    if (beforeTimestamp && (!this.pendingState.lastBefore || beforeTimestamp < this.pendingState.lastBefore)) {
      this.pendingState.lastBefore = beforeTimestamp;
    }

    this._writePendingState();
  }

  /**
   * Commit transaction - data has been confirmed written
   * 
   * Call this ONLY AFTER data is durably on disk.
   * This advances the cursor position.
   */
  commit() {
    if (!this.inTransaction) {
      throw new Error('No transaction in progress. Call beginTransaction() first.');
    }

    // Update confirmed state
    this.confirmedState.totalUpdates += this.pendingState.updates;
    this.confirmedState.totalEvents += this.pendingState.events;
    if (this.pendingState.lastBefore) {
      this.confirmedState.lastBefore = this.pendingState.lastBefore;
    }

    // Clear pending
    this.pendingState = { updates: 0, events: 0, lastBefore: null };
    this.inTransaction = false;
    this.transactionStartState = null;

    // Write confirmed state atomically
    this._writeConfirmedState();

    return this.confirmedState;
  }

  /**
   * Rollback transaction - data write failed
   * 
   * Restores cursor to pre-transaction state.
   */
  rollback() {
    if (!this.inTransaction) {
      return; // Nothing to rollback
    }

    // Restore from transaction start
    if (this.transactionStartState) {
      this.confirmedState = { ...this.transactionStartState };
    }

    this.pendingState = { updates: 0, events: 0, lastBefore: null };
    this.inTransaction = false;
    this.transactionStartState = null;

    // Write rollback state
    this._writeConfirmedState();
  }

  /**
   * Confirm GCS checkpoint - called after GCS queue is drained.
   * 
   * This advances the GCS-confirmed position to the current local position,
   * making it safe to resume from this point even after a VM crash.
   */
  confirmGCS(timestamp = null, updates = null, events = null) {
    this.confirmedState.lastGCSConfirmed = timestamp || this.confirmedState.lastBefore;
    this.confirmedState.gcsConfirmedUpdates = updates ?? this.confirmedState.totalUpdates;
    this.confirmedState.gcsConfirmedEvents = events ?? this.confirmedState.totalEvents;

    this._writeConfirmedState();

    return {
      lastGCSConfirmed: this.confirmedState.lastGCSConfirmed,
      gcsConfirmedUpdates: this.confirmedState.gcsConfirmedUpdates,
      gcsConfirmedEvents: this.confirmedState.gcsConfirmedEvents,
    };
  }

  /**
   * Get GCS confirmation status.
   */
  getGCSStatus() {
    const hasGCSCheckpoint = !!this.confirmedState.lastGCSConfirmed;
    const localUpdates = this.confirmedState.totalUpdates;
    const gcsUpdates = this.confirmedState.gcsConfirmedUpdates;
    const pendingGCSUpdates = localUpdates - gcsUpdates;

    return {
      hasGCSCheckpoint,
      lastGCSConfirmed: this.confirmedState.lastGCSConfirmed,
      gcsConfirmedUpdates: gcsUpdates,
      localUpdates,
      pendingGCSUpdates,
      isSynced: pendingGCSUpdates === 0,
    };
  }

  /**
   * Mark cursor as complete.
   * 
   * FIX #5: Requires GCS to be fully synced before marking complete.
   * Call confirmGCS() first to drain the GCS queue, then markComplete().
   * This removes the implicit assumption that GCS is synced and makes it explicit.
   */
  markComplete() {
    if (this.inTransaction || this.pendingState.updates > 0 || this.pendingState.events > 0) {
      throw new Error('Cannot mark complete with pending data. Call commit() first.');
    }

    // FIX #5: Enforce that GCS is actually synced rather than assuming it.
    const gcsStatus = this.getGCSStatus();
    if (!gcsStatus.isSynced) {
      throw new Error(
        `Cannot mark complete: GCS is not synced. ` +
        `${gcsStatus.pendingGCSUpdates} updates not yet confirmed to GCS. ` +
        `Call confirmGCS() after draining the GCS queue first.`
      );
    }

    this.confirmedState.complete = true;
    this._writeConfirmedState();

    console.log(
      `✅ Cursor marked complete: ${this.confirmedState.totalUpdates} updates, ` +
      `${this.confirmedState.totalEvents} events`
    );
  }

  /**
   * Set time range bounds
   */
  setTimeBounds(minTime, maxTime) {
    this.confirmedState.minTime = minTime;
    this.confirmedState.maxTime = maxTime;
  }

  /**
   * Write pending state to disk (for crash recovery visibility only)
   * 
   * The pending fields are informational — they help diagnose crashes but are
   * never used as a resume point. Only confirmedState drives resumption.
   */
  _writePendingState() {
    const data = {
      id: `cursor-${this.migrationId}-${this.synchronizerId.substring(0, 20)}`,
      migration_id: this.migrationId,
      synchronizer_id: this.synchronizerId,
      shard_index: this.shardIndex,
      shard_total: this.shardTotal,

      // Confirmed state (safe resume point — this is what matters on restart)
      last_confirmed_before: this.confirmedState.lastBefore,
      confirmed_updates: this.confirmedState.totalUpdates,
      confirmed_events: this.confirmedState.totalEvents,

      // Legacy fields for compatibility
      last_before: this.confirmedState.lastBefore,
      total_updates: this.confirmedState.totalUpdates,
      total_events: this.confirmedState.totalEvents,

      // Time bounds
      min_time: this.confirmedState.minTime,
      max_time: this.confirmedState.maxTime,

      // Pending state (diagnostic only — never used for resume)
      pending_updates: this.pendingState.updates,
      pending_events: this.pendingState.events,
      pending_before: this.pendingState.lastBefore,
      in_transaction: this.inTransaction,

      // Status
      complete: false,
      updated_at: new Date().toISOString(),
    };

    atomicWriteFile(this.cursorPath, data);
  }

  /**
   * Write confirmed state to disk
   */
  _writeConfirmedState() {
    const data = {
      id: `cursor-${this.migrationId}-${this.synchronizerId.substring(0, 20)}`,
      migration_id: this.migrationId,
      synchronizer_id: this.synchronizerId,
      shard_index: this.shardIndex,
      shard_total: this.shardTotal,

      // Confirmed state (local disk)
      last_confirmed_before: this.confirmedState.lastBefore,
      confirmed_updates: this.confirmedState.totalUpdates,
      confirmed_events: this.confirmedState.totalEvents,

      // GCS-confirmed state (crash-safe resume point)
      last_gcs_confirmed: this.confirmedState.lastGCSConfirmed,
      gcs_confirmed_updates: this.confirmedState.gcsConfirmedUpdates,
      gcs_confirmed_events: this.confirmedState.gcsConfirmedEvents,

      // Legacy fields for compatibility
      last_before: this.confirmedState.lastBefore,
      total_updates: this.confirmedState.totalUpdates,
      total_events: this.confirmedState.totalEvents,

      // Time bounds
      min_time: this.confirmedState.minTime,
      max_time: this.confirmedState.maxTime,

      // No pending data
      pending_updates: 0,
      pending_events: 0,
      in_transaction: false,

      // Status
      complete: this.confirmedState.complete,
      updated_at: new Date().toISOString(),
      ...(this.confirmedState.complete ? { completed_at: new Date().toISOString() } : {}),
    };

    atomicWriteFile(this.cursorPath, data);
  }

  /**
   * Get current state for logging/UI
   */
  getState() {
    return {
      confirmed: { ...this.confirmedState },
      pending: { ...this.pendingState },
      inTransaction: this.inTransaction,
      gcsStatus: this.getGCSStatus(),
    };
  }

  /**
   * Simple atomic save - convenience wrapper for backward compatibility.
   * 
   * Use this for simple state updates without the full transaction pattern.
   * 
   * FIX #3: Throws if called while a transaction is open. Previously this
   * silently committed the transaction, which could advance the cursor even
   * if the in-flight data was never confirmed as durable.
   */
  saveAtomic(state) {
    // FIX #3: Do not silently commit an open transaction — throw instead.
    // The caller must explicitly commit() or rollback() before calling saveAtomic().
    if (this.inTransaction) {
      throw new Error(
        'saveAtomic() called while a transaction is open. ' +
        'Call commit() or rollback() first to avoid advancing the cursor on unconfirmed data.'
      );
    }

    // Update confirmed state from provided state object
    if (state.last_before !== undefined) {
      this.confirmedState.lastBefore = state.last_before;
    }
    if (state.total_updates !== undefined) {
      this.confirmedState.totalUpdates = state.total_updates;
    }
    if (state.total_events !== undefined) {
      this.confirmedState.totalEvents = state.total_events;
    }
    if (state.min_time !== undefined) {
      this.confirmedState.minTime = state.min_time;
    }
    if (state.max_time !== undefined) {
      this.confirmedState.maxTime = state.max_time;
    }
    if (state.complete !== undefined) {
      this.confirmedState.complete = state.complete;
    }

    // Write to disk atomically
    this._writeConfirmedState();

    return this.confirmedState;
  }
}

/**
 * Load cursor using legacy format (for backwards compatibility)
 */
export function loadCursorLegacy(migrationId, synchronizerId, shardIndex = null) {
  const cursorPath = getCursorPath(migrationId, synchronizerId, shardIndex);
  return readCursorSafe(cursorPath);
}

/**
 * Check if cursor exists and is complete
 */
export function isCursorComplete(migrationId, synchronizerId, shardIndex = null) {
  const cursor = loadCursorLegacy(migrationId, synchronizerId, shardIndex);
  return cursor?.complete === true;
}

export default {
  AtomicCursor,
  loadCursorLegacy,
  isCursorComplete,
  atomicWriteFile,
  getCursorPath,
  CURSOR_DIR,
};
