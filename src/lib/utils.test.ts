/**
 * Utils Tests
 * 
 * Tests for the cn (classnames) utility
 */

import { describe, it, expect } from 'vitest';
import { cn } from './utils';

describe('cn', () => {
  it('merges class names', () => {
    expect(cn('foo', 'bar')).toBe('foo bar');
  });

  it('handles conditional classes', () => {
    expect(cn('base', true && 'included', false && 'excluded')).toBe('base included');
  });

  it('handles arrays of classes', () => {
    expect(cn(['foo', 'bar'])).toBe('foo bar');
  });

  it('handles objects with boolean values', () => {
    expect(cn({ foo: true, bar: false, baz: true })).toBe('foo baz');
  });

  it('handles undefined and null', () => {
    expect(cn('foo', undefined, null, 'bar')).toBe('foo bar');
  });

  it('merges conflicting Tailwind classes', () => {
    // twMerge should keep the last conflicting class
    expect(cn('px-2', 'px-4')).toBe('px-4');
    expect(cn('text-red-500', 'text-blue-500')).toBe('text-blue-500');
    expect(cn('bg-black', 'bg-white')).toBe('bg-white');
  });

  it('preserves non-conflicting classes', () => {
    expect(cn('px-2', 'py-4', 'text-red-500')).toBe('px-2 py-4 text-red-500');
  });

  it('handles complex combinations', () => {
    const isActive = true;
    const isDisabled = false;
    
    expect(cn(
      'base-class',
      isActive && 'active-class',
      isDisabled && 'disabled-class',
      { 'conditional-class': true }
    )).toBe('base-class active-class conditional-class');
  });

  it('handles empty inputs', () => {
    expect(cn()).toBe('');
    expect(cn('')).toBe('');
    expect(cn('', '', '')).toBe('');
  });
});
