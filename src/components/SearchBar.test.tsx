import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { SearchBar } from './SearchBar';

// Mock the API client
vi.mock('@/lib/duckdb-api-client', () => ({
  searchAnsEntries: vi.fn(),
  getPartyEvents: vi.fn(),
}));

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Mock useToast
const mockToast = vi.fn();
vi.mock('@/hooks/use-toast', () => ({
  useToast: () => ({ toast: mockToast }),
}));

import { searchAnsEntries } from '@/lib/duckdb-api-client';

// Helper functions
const getButton = (container: HTMLElement) => container.querySelector('button');
const getInput = (container: HTMLElement) => container.querySelector('input');
const getByText = (container: HTMLElement, text: string | RegExp) => {
  const elements = container.querySelectorAll('*');
  return Array.from(elements).find(el => {
    if (typeof text === 'string') return el.textContent?.includes(text);
    return text.test(el.textContent || '');
  });
};
const getAllByText = (container: HTMLElement, text: string | RegExp) => {
  const elements = container.querySelectorAll('*');
  return Array.from(elements).filter(el => {
    if (typeof text === 'string') return el.textContent?.includes(text);
    return text.test(el.textContent || '');
  });
};
const queryByText = (container: HTMLElement, text: string | RegExp) => getByText(container, text) || null;

describe('SearchBar', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  const renderSearchBar = () => {
    return render(
      <MemoryRouter>
        <SearchBar />
      </MemoryRouter>
    );
  };

  describe('Dialog interaction', () => {
    it('renders search button', () => {
      const { container } = renderSearchBar();
      expect(getButton(container)).toBeDefined();
    });

    it('opens command dialog on button click', async () => {
      const user = userEvent.setup();
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      // After clicking, the dialog should appear with an input
      await vi.waitFor(() => {
        const input = document.querySelector('input[placeholder*="Search by party"]');
        expect(input).toBeDefined();
      });
    });
  });

  describe('Party ID detection', () => {
    it('navigates to party page for ID containing "::"', async () => {
      const user = userEvent.setup();
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      // Wait for input to appear first
      await vi.waitFor(() => {
        expect(document.querySelector('input')).not.toBeNull();
      });
      
      const input = document.querySelector('input')!;
      await user.type(input, 'validator::1220abc');
      
      // Wait for command items to appear and click search action
      await vi.waitFor(() => {
        const allItems = document.querySelectorAll('[cmdk-item]');
        expect(allItems.length).toBeGreaterThan(0);
      });
      
      const allItems = document.querySelectorAll('[cmdk-item]');
      const actionItem = Array.from(allItems).find(item => 
        item.textContent?.includes('Search for')
      );
      if (actionItem) await user.click(actionItem as HTMLElement);
      
      await vi.waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/party/validator%3A%3A1220abc');
      });
    });
  });

  describe('Event ID detection', () => {
    it('navigates to transactions page for ID starting with "#"', async () => {
      const user = userEvent.setup();
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      await vi.waitFor(async () => {
        const input = document.querySelector('input');
        if (input) {
          await user.type(input, '#1220abc');
          
          const allItems = document.querySelectorAll('[cmdk-item]');
          const actionItem = Array.from(allItems).find(item => 
            item.textContent?.includes('Search for')
          );
          if (actionItem) await user.click(actionItem);
        }
      });
      
      await vi.waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/transactions?search=%231220abc');
      });
    });
  });

  describe('ANS name search', () => {
    it('navigates to ANS page when entries found', async () => {
      const user = userEvent.setup();
      (searchAnsEntries as any).mockResolvedValue({
        data: [{ payload: { name: 'testname' } }],
      });
      
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      await vi.waitFor(async () => {
        const input = document.querySelector('input');
        if (input) {
          await user.type(input, 'testname');
          
          const allItems = document.querySelectorAll('[cmdk-item]');
          const actionItem = Array.from(allItems).find(item => 
            item.textContent?.includes('Search for')
          );
          if (actionItem) await user.click(actionItem);
        }
      });
      
      await vi.waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/ans?search=testname');
      });
    });

    it('shows "No Results" toast when no ANS entries found', async () => {
      const user = userEvent.setup();
      (searchAnsEntries as any).mockResolvedValue({ data: [] });
      
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      await vi.waitFor(async () => {
        const input = document.querySelector('input');
        if (input) {
          await user.type(input, 'nonexistent');
          
          const allItems = document.querySelectorAll('[cmdk-item]');
          const actionItem = Array.from(allItems).find(item => 
            item.textContent?.includes('Search for')
          );
          if (actionItem) await user.click(actionItem);
        }
      });
      
      await vi.waitFor(() => {
        expect(mockToast).toHaveBeenCalledWith(
          expect.objectContaining({
            title: 'No Results',
            variant: 'destructive',
          })
        );
      });
      
      expect(mockNavigate).not.toHaveBeenCalled();
    });

    it('shows error toast when ANS search fails', async () => {
      const user = userEvent.setup();
      (searchAnsEntries as any).mockRejectedValue(new Error('API error'));
      
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      await vi.waitFor(async () => {
        const input = document.querySelector('input');
        if (input) {
          await user.type(input, 'testquery');
          
          const allItems = document.querySelectorAll('[cmdk-item]');
          const actionItem = Array.from(allItems).find(item => 
            item.textContent?.includes('Search for')
          );
          if (actionItem) await user.click(actionItem);
        }
      });
      
      await vi.waitFor(() => {
        expect(mockToast).toHaveBeenCalledWith(
          expect.objectContaining({
            title: 'Search Error',
            variant: 'destructive',
          })
        );
      });
    });
  });

  describe('Empty query handling', () => {
    it('ignores whitespace-only queries', async () => {
      const user = userEvent.setup();
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      await vi.waitFor(async () => {
        const input = document.querySelector('input');
        if (input) {
          await user.type(input, '   ');
          
          const allItems = document.querySelectorAll('[cmdk-item]');
          const actionItem = Array.from(allItems).find(item => 
            item.textContent?.includes('Search for')
          );
          if (actionItem) await user.click(actionItem);
        }
      });
      
      // No navigation should happen for whitespace
      expect(mockNavigate).not.toHaveBeenCalled();
      expect(searchAnsEntries).not.toHaveBeenCalled();
    });
  });

  describe('Suggestion items', () => {
    it('shows suggestion items in dialog', async () => {
      const user = userEvent.setup();
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      await vi.waitFor(() => {
        const items = document.querySelectorAll('[cmdk-item]');
        expect(items.length).toBeGreaterThanOrEqual(3);
      });
    });

    it('clicking party ID suggestion shows tip toast', async () => {
      const user = userEvent.setup();
      const { container } = renderSearchBar();
      
      const button = getButton(container);
      if (button) await user.click(button);
      
      await vi.waitFor(async () => {
        const allItems = document.querySelectorAll('[cmdk-item]');
        const partyIdSuggestion = Array.from(allItems).find(item => 
          item.textContent?.includes('Search by Party ID')
        );
        if (partyIdSuggestion) await user.click(partyIdSuggestion);
      });
      
      await vi.waitFor(() => {
        expect(mockToast).toHaveBeenCalledWith(
          expect.objectContaining({
            title: 'Tip',
          })
        );
      });
    });
  });
});
