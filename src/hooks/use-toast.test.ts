/**
 * useToast Hook Tests
 * 
 * Tests for the toast notification system
 */

import { describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useToast, toast } from './use-toast';

describe('useToast', () => {
  it('should initialize with empty toasts', () => {
    const { result } = renderHook(() => useToast());
    expect(result.current.toasts).toEqual([]);
  });

  it('should add a toast via the hook', () => {
    const { result } = renderHook(() => useToast());

    act(() => {
      result.current.toast({
        title: 'Test Toast',
        description: 'This is a test',
      });
    });

    expect(result.current.toasts.length).toBe(1);
    expect(result.current.toasts[0].title).toBe('Test Toast');
    expect(result.current.toasts[0].description).toBe('This is a test');
  });

  it('should add a toast via the standalone function', () => {
    const { result } = renderHook(() => useToast());

    act(() => {
      toast({
        title: 'Standalone Toast',
      });
    });

    expect(result.current.toasts.some(t => t.title === 'Standalone Toast')).toBe(true);
  });

  it('should dismiss a toast', () => {
    const { result } = renderHook(() => useToast());

    let toastId: string;
    act(() => {
      const { id } = result.current.toast({
        title: 'Dismissable Toast',
      });
      toastId = id;
    });

    expect(result.current.toasts.length).toBe(1);

    act(() => {
      result.current.dismiss(toastId);
    });

    // Toast should be marked as dismissed (open: false)
    const dismissedToast = result.current.toasts.find(t => t.id === toastId);
    expect(dismissedToast?.open).toBe(false);
  });

  it('should support different toast variants', () => {
    const { result } = renderHook(() => useToast());

    act(() => {
      result.current.toast({
        title: 'Default Toast',
      });
      result.current.toast({
        title: 'Destructive Toast',
        variant: 'destructive',
      });
    });

    const toasts = result.current.toasts;
    expect(toasts.length).toBe(2);
    expect(toasts.find(t => t.title === 'Destructive Toast')?.variant).toBe('destructive');
  });

  it('should generate unique IDs for each toast', () => {
    const { result } = renderHook(() => useToast());

    act(() => {
      result.current.toast({ title: 'Toast 1' });
      result.current.toast({ title: 'Toast 2' });
      result.current.toast({ title: 'Toast 3' });
    });

    const ids = result.current.toasts.map(t => t.id);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(ids.length);
  });
});
