import '@testing-library/jest-dom';
import { afterEach, vi } from 'vitest';

// Only run React Testing Library cleanup in DOM-like environments
let cleanup: undefined | (() => void);
try {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  cleanup = require('@testing-library/react').cleanup;
} catch {
  cleanup = undefined;
}

afterEach(() => {
  cleanup?.();
});

// Guard browser-only globals so this setup file can run in node env too
if (typeof window !== 'undefined') {
  // Mock matchMedia for components that use media queries
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: (query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }),
  });
}

if (typeof globalThis !== 'undefined') {
  // Mock ResizeObserver (used by some UI components)
  (globalThis as any).ResizeObserver = class ResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
}

// Mock scrollIntoView for cmdk and other components that use it
if (typeof Element !== 'undefined') {
  Element.prototype.scrollIntoView = vi.fn();
}
