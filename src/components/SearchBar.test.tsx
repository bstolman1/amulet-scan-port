import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
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
      renderSearchBar();
      expect(screen.getByRole('button')).toBeInTheDocument();
    });

    it('opens command dialog on button click', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      expect(screen.getByPlaceholderText(/search by party id/i)).toBeInTheDocument();
    });
  });

  describe('Party ID detection', () => {
    it('navigates to party page for ID containing "::"', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, 'validator::1220abc');
      
      // Find and click the search action
      const searchAction = screen.getByText(/search for "validator::1220abc"/i);
      await user.click(searchAction);
      
      expect(mockNavigate).toHaveBeenCalledWith('/party/validator%3A%3A1220abc');
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: 'Loading Party',
        })
      );
    });

    it('encodes party ID with special characters correctly', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, 'test::user/with+chars');
      
      const searchAction = screen.getByText(/search for "test::user\/with\+chars"/i);
      await user.click(searchAction);
      
      expect(mockNavigate).toHaveBeenCalledWith(
        expect.stringContaining('/party/')
      );
    });
  });

  describe('Event ID detection', () => {
    it('navigates to transactions page for ID starting with "#"', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, '#1220abc');
      
      const searchAction = screen.getByText(/search for "#1220abc"/i);
      await user.click(searchAction);
      
      expect(mockNavigate).toHaveBeenCalledWith('/transactions?search=%231220abc');
    });

    it('handles event ID with special characters', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, '#event:test/id');
      
      const searchAction = screen.getByText(/search for "#event:test\/id"/i);
      await user.click(searchAction);
      
      expect(mockNavigate).toHaveBeenCalledWith(
        expect.stringContaining('/transactions?search=')
      );
    });
  });

  describe('ANS name search', () => {
    it('navigates to ANS page when entries found', async () => {
      const user = userEvent.setup();
      (searchAnsEntries as any).mockResolvedValue({
        data: [{ payload: { name: 'testname' } }],
      });
      
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, 'testname');
      
      const searchAction = screen.getByText(/search for "testname"/i);
      await user.click(searchAction);
      
      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/ans?search=testname');
      });
      
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: 'Search Results',
          description: expect.stringContaining('1 ANS entry'),
        })
      );
    });

    it('shows "No Results" toast when no ANS entries found', async () => {
      const user = userEvent.setup();
      (searchAnsEntries as any).mockResolvedValue({ data: [] });
      
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, 'nonexistent');
      
      const searchAction = screen.getByText(/search for "nonexistent"/i);
      await user.click(searchAction);
      
      await waitFor(() => {
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
      
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, 'testquery');
      
      const searchAction = screen.getByText(/search for "testquery"/i);
      await user.click(searchAction);
      
      await waitFor(() => {
        expect(mockToast).toHaveBeenCalledWith(
          expect.objectContaining({
            title: 'Search Error',
            variant: 'destructive',
          })
        );
      });
    });

    it('filters ANS results by matching name', async () => {
      const user = userEvent.setup();
      (searchAnsEntries as any).mockResolvedValue({
        data: [
          { payload: { name: 'alice.canton' } },
          { payload: { name: 'bob.canton' } },
          { payload: { name: 'alicia.other' } },
        ],
      });
      
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, 'ali');
      
      const searchAction = screen.getByText(/search for "ali"/i);
      await user.click(searchAction);
      
      await waitFor(() => {
        expect(mockToast).toHaveBeenCalledWith(
          expect.objectContaining({
            description: expect.stringContaining('2 ANS entry'),
          })
        );
      });
    });
  });

  describe('Empty query handling', () => {
    it('ignores empty search queries', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      // Try to search with empty input - the search action shouldn't appear
      expect(screen.queryByText(/search for ""/i)).not.toBeInTheDocument();
    });

    it('ignores whitespace-only queries', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const input = screen.getByPlaceholderText(/search by party id/i);
      await user.type(input, '   ');
      
      // The search action should appear but handleSearch should return early
      const searchAction = screen.getByText(/search for "   "/i);
      await user.click(searchAction);
      
      // No navigation should happen for whitespace
      expect(mockNavigate).not.toHaveBeenCalled();
      expect(searchAnsEntries).not.toHaveBeenCalled();
    });
  });

  describe('Suggestion items', () => {
    it('shows party ID suggestion', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      expect(screen.getByText(/search by party id/i)).toBeInTheDocument();
    });

    it('shows event ID suggestion', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      expect(screen.getByText(/search by event id/i)).toBeInTheDocument();
    });

    it('shows ANS name suggestion', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      expect(screen.getByText(/search by ans name/i)).toBeInTheDocument();
    });

    it('sets example party ID when suggestion clicked', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const partyIdSuggestion = screen.getByText(/search by party id/i);
      await user.click(partyIdSuggestion);
      
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: 'Tip',
          description: expect.stringContaining('Party IDs contain ::'),
        })
      );
    });

    it('sets "#" prefix when event ID suggestion clicked', async () => {
      const user = userEvent.setup();
      renderSearchBar();
      
      await user.click(screen.getByRole('button'));
      
      const eventIdSuggestion = screen.getByText(/search by event id/i);
      await user.click(eventIdSuggestion);
      
      expect(mockToast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: 'Tip',
          description: expect.stringContaining('Event IDs start with #'),
        })
      );
    });
  });
});
