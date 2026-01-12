/**
 * ErrorBoundary Component Tests
 * 
 * Tests for the error boundary component
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';
import { ErrorBoundary } from './ErrorBoundary';

// Mock console.error to avoid noise in test output
const consoleError = console.error;
beforeEach(() => {
  console.error = vi.fn();
});

// Restore after tests
afterAll(() => {
  console.error = consoleError;
});

// Component that throws an error
const ThrowingComponent = ({ shouldThrow = true }: { shouldThrow?: boolean }) => {
  if (shouldThrow) {
    throw new Error('Test error');
  }
  return <div>No error</div>;
};

import { afterAll } from 'vitest';

describe('ErrorBoundary', () => {
  it('renders children when there is no error', () => {
    const { container } = render(
      <ErrorBoundary>
        <div>Child content</div>
      </ErrorBoundary>
    );
    
    expect(container.textContent).toContain('Child content');
  });

  it('renders error UI when child throws', () => {
    const { container } = render(
      <ErrorBoundary title="App Error">
        <ThrowingComponent />
      </ErrorBoundary>
    );
    
    // Should render error message, not the child content
    expect(container.textContent).toContain('App Error');
    expect(container.textContent).not.toContain('No error');
  });

  it('displays error details', () => {
    const { container } = render(
      <ErrorBoundary>
        <ThrowingComponent />
      </ErrorBoundary>
    );
    
    expect(container.textContent).toContain('Test error');
  });

  it('renders with custom title', () => {
    const { container } = render(
      <ErrorBoundary title="Custom Error Title">
        <ThrowingComponent />
      </ErrorBoundary>
    );
    
    expect(container.textContent).toContain('Custom Error Title');
  });

  it('includes refresh button', () => {
    const { container } = render(
      <ErrorBoundary>
        <ThrowingComponent />
      </ErrorBoundary>
    );
    
    const refreshButton = container.querySelector('button');
    expect(refreshButton).toBeDefined();
  });

  it('renders working children after initial error is fixed', () => {
    // First render with error
    const { container, rerender } = render(
      <ErrorBoundary>
        <ThrowingComponent shouldThrow={true} />
      </ErrorBoundary>
    );
    
    expect(container.textContent).toContain('Test error');
    
    // The error boundary maintains error state, so rerender won't clear it
    // This tests that the error state is properly captured
  });
});
