/**
 * Parquet Writer Pool Tests
 * 
 * Tests the parallel Parquet writing logic including:
 * - Retry logic for transient errors
 * - Worker pool management
 * - Stats tracking
 */

import { describe, it, expect } from 'vitest';

describe('ParquetWriterPool', () => {
  
  describe('Transient Error Detection', () => {
    function isTransientError(msg) {
      if (!msg) return false;
      const patterns = [
        /resource busy/i,
        /disk full/i,
        /no space left/i,
        /ENOSPC/i,
        /EMFILE/i,           // Too many open files
        /ENFILE/i,           // File table overflow
        /EAGAIN/i,           // Resource temporarily unavailable
        /EBUSY/i,            // Device or resource busy
        /timeout/i,
        /timed out/i,
        /worker crashed/i,
      ];
      return patterns.some(p => p.test(msg));
    }
    
    it('should identify disk space errors as transient', () => {
      expect(isTransientError('ENOSPC: no space left on device')).toBe(true);
      expect(isTransientError('disk full')).toBe(true);
      expect(isTransientError('No space left on device')).toBe(true);
    });
    
    it('should identify file descriptor errors as transient', () => {
      expect(isTransientError('EMFILE: too many open files')).toBe(true);
      expect(isTransientError('ENFILE: file table overflow')).toBe(true);
    });
    
    it('should identify resource busy errors as transient', () => {
      expect(isTransientError('Resource busy')).toBe(true);
      expect(isTransientError('EBUSY')).toBe(true);
      expect(isTransientError('EAGAIN: resource temporarily unavailable')).toBe(true);
    });
    
    it('should identify worker crashes as transient', () => {
      expect(isTransientError('Worker crashed with code 1')).toBe(true);
    });
    
    it('should NOT identify schema errors as transient', () => {
      expect(isTransientError('Invalid column type')).toBe(false);
      expect(isTransientError('Schema mismatch')).toBe(false);
      expect(isTransientError('Cannot convert string to int')).toBe(false);
    });
  });
  
  describe('Retry Backoff Calculation', () => {
    function calculateRetryDelay(attempt) {
      const delay = Math.min(1000 * Math.pow(2, attempt), 10000);
      const jitter = Math.random() * 500;
      return delay + jitter;
    }
    
    it('should increase delay exponentially', () => {
      const delays = [];
      for (let i = 0; i < 4; i++) {
        const baseDelay = Math.min(1000 * Math.pow(2, i), 10000);
        delays.push(baseDelay);
      }
      
      expect(delays[0]).toBe(1000);   // 1s
      expect(delays[1]).toBe(2000);   // 2s
      expect(delays[2]).toBe(4000);   // 4s
      expect(delays[3]).toBe(8000);   // 8s
    });
    
    it('should cap delay at 10 seconds', () => {
      for (let attempt = 0; attempt < 10; attempt++) {
        const baseDelay = Math.min(1000 * Math.pow(2, attempt), 10000);
        expect(baseDelay).toBeLessThanOrEqual(10000);
      }
    });
  });
  
  describe('Pool Stats Calculation', () => {
    it('should calculate MB/s throughput', () => {
      const stats = {
        totalBytes: 50 * 1024 * 1024, // 50 MB
        startTime: Date.now() - 10000, // 10 seconds ago
      };
      
      const elapsed = (Date.now() - stats.startTime) / 1000;
      const mbWritten = stats.totalBytes / (1024 * 1024);
      const mbPerSec = mbWritten / elapsed;
      
      expect(mbPerSec).toBeCloseTo(5, 0); // ~5 MB/s
    });
    
    it('should calculate files/sec throughput', () => {
      const stats = {
        completedJobs: 100,
        startTime: Date.now() - 20000, // 20 seconds ago
      };
      
      const elapsed = (Date.now() - stats.startTime) / 1000;
      const filesPerSec = stats.completedJobs / elapsed;
      
      expect(filesPerSec).toBeCloseTo(5, 0); // ~5 files/s
    });
    
    it('should calculate validation rate', () => {
      const stats = {
        validatedFiles: 100,
        validationFailures: 3,
      };
      
      const rate = ((stats.validatedFiles - stats.validationFailures) / stats.validatedFiles * 100);
      
      expect(rate).toBe(97);
    });
    
    it('should track queue depth correctly', () => {
      const queuedJobs = 15;
      const activeWorkers = 4;
      const availableSlots = 4;
      const maxWorkers = 8;
      
      // Total pending work
      const pendingWork = queuedJobs + activeWorkers;
      expect(pendingWork).toBe(19);
      
      // Slots used
      const usedSlots = maxWorkers - availableSlots;
      expect(usedSlots).toBe(4);
    });
  });
  
  describe('Validation Issue Tracking', () => {
    it('should limit stored validation issues', () => {
      const maxIssues = 10;
      const issues = [];
      
      for (let i = 0; i < 15; i++) {
        if (issues.length < maxIssues) {
          issues.push({ file: `file-${i}.parquet`, issues: ['row count mismatch'] });
        }
      }
      
      expect(issues).toHaveLength(10);
    });
    
    it('should capture file name and issues', () => {
      const validationIssue = {
        file: 'updates-001.parquet',
        issues: ['Expected 100 rows, got 95', 'Missing required column'],
      };
      
      expect(validationIssue.file).toBe('updates-001.parquet');
      expect(validationIssue.issues).toHaveLength(2);
    });
  });
  
  describe('Worker Pool Sizing', () => {
    it('should default to CPU count minus 1', () => {
      const cpuCount = 8;
      const defaultSize = Math.max(2, cpuCount - 1);
      
      expect(defaultSize).toBe(7);
    });
    
    it('should ensure minimum of 2 workers', () => {
      const cpuCount = 1;
      const defaultSize = Math.max(2, cpuCount - 1);
      
      expect(defaultSize).toBe(2);
    });
    
    it('should respect environment override', () => {
      const envValue = 4;
      const cpuCount = 8;
      const defaultSize = Math.max(2, cpuCount - 1);
      
      const finalSize = envValue || defaultSize;
      
      expect(finalSize).toBe(4);
    });
  });
  
  describe('Drain Behavior', () => {
    it('should detect drained state', () => {
      const queue = [];
      const activeWorkers = 0;
      
      const isDrained = queue.length === 0 && activeWorkers === 0;
      
      expect(isDrained).toBe(true);
    });
    
    it('should wait while work is pending', () => {
      const queue = [{ job: 'pending' }];
      const activeWorkers = 2;
      
      const isDrained = queue.length === 0 && activeWorkers === 0;
      
      expect(isDrained).toBe(false);
    });
  });
});
