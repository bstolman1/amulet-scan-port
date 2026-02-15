/**
 * Tests for graceful shutdown in ingest-all.js
 * 
 * Verifies that:
 * - Signals are forwarded to child processes
 * - SIGKILL escalation fires after 5s timeout
 * - When no child is active, process.exit is called directly
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { gracefulShutdown } from '../ingest-all.js';
import EventEmitter from 'events';

function createMockChild() {
  const child = new EventEmitter();
  child.killed = false;
  child.kill = vi.fn((signal) => {
    if (signal === 'SIGKILL') {
      child.killed = true;
    }
  });
  return child;
}

describe('gracefulShutdown', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('sends the signal to the active child process', () => {
    const child = createMockChild();
    const exitFn = vi.fn();

    gracefulShutdown('SIGTERM', child, exitFn);

    expect(child.kill).toHaveBeenCalledWith('SIGTERM');
  });

  it('calls exitFn(0) when child exits cleanly', () => {
    const child = createMockChild();
    const exitFn = vi.fn();

    gracefulShutdown('SIGINT', child, exitFn);

    // Simulate child exiting
    child.emit('exit', 0);

    expect(exitFn).toHaveBeenCalledWith(0);
  });

  it('sends SIGKILL after 5s if child does not exit', () => {
    const child = createMockChild();
    const exitFn = vi.fn();

    gracefulShutdown('SIGTERM', child, exitFn);

    // Verify SIGKILL not sent yet
    expect(child.kill).toHaveBeenCalledTimes(1);
    expect(child.kill).toHaveBeenCalledWith('SIGTERM');

    // Advance 5 seconds
    vi.advanceTimersByTime(5000);

    // Now SIGKILL should be sent
    expect(child.kill).toHaveBeenCalledWith('SIGKILL');
    expect(exitFn).toHaveBeenCalledWith(1);
  });

  it('cancels SIGKILL timeout if child exits before 5s', () => {
    const child = createMockChild();
    const exitFn = vi.fn();

    gracefulShutdown('SIGTERM', child, exitFn);

    // Child exits after 1 second
    vi.advanceTimersByTime(1000);
    child.emit('exit', 0);

    expect(exitFn).toHaveBeenCalledWith(0);

    // Advance past 5s — SIGKILL should NOT fire
    vi.advanceTimersByTime(5000);
    // Only SIGTERM was sent, not SIGKILL
    expect(child.kill).toHaveBeenCalledTimes(1);
  });

  it('calls exitFn(0) immediately when no child is active', () => {
    const exitFn = vi.fn();

    gracefulShutdown('SIGTERM', null, exitFn);

    expect(exitFn).toHaveBeenCalledWith(0);
  });

  it('calls exitFn(0) when child is already killed', () => {
    const child = createMockChild();
    child.killed = true;
    const exitFn = vi.fn();

    gracefulShutdown('SIGTERM', child, exitFn);

    expect(child.kill).not.toHaveBeenCalled();
    expect(exitFn).toHaveBeenCalledWith(0);
  });
});
