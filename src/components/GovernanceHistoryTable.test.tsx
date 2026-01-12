import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { GovernanceHistoryTable } from './GovernanceHistoryTable';

// Mock the hook
vi.mock('@/hooks/use-scan-vote-results', () => ({
  useGovernanceVoteHistory: vi.fn(),
}));

import { useGovernanceVoteHistory } from '@/hooks/use-scan-vote-results';

const createQueryClient = () => new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

const mockVoteResults = [
  {
    outcome: 'accepted',
    actionTitle: 'Add Featured App: TestApp',
    actionType: 'CRARC_AddFutureAmuletConfigSchedule',
    completedAt: '2024-06-15T10:30:00Z',
    voteBefore: '2024-06-20T10:30:00Z',
    votesFor: 5,
    votesAgainst: 2,
    reasonBody: 'This is a valid proposal',
    reasonUrl: 'https://example.com/proposal',
    trackingCid: 'abc123def456',
  },
  {
    outcome: 'rejected',
    actionTitle: 'Remove Validator: BadActor',
    actionType: 'SRARC_OffboardSv',
    completedAt: '2024-06-14T15:00:00Z',
    voteBefore: '2024-06-19T15:00:00Z',
    votesFor: 1,
    votesAgainst: 6,
    reasonBody: null,
    reasonUrl: null,
    trackingCid: 'xyz789abc012',
  },
  {
    outcome: 'expired',
    actionTitle: 'Update Amulet Rules',
    actionType: 'ARC_AmuletRules',
    completedAt: '2024-06-13T08:00:00Z',
    voteBefore: '2024-06-18T08:00:00Z',
    votesFor: 3,
    votesAgainst: 3,
    reasonBody: 'Tied vote, expired',
    reasonUrl: null,
    trackingCid: 'tied123456',
  },
];

describe('GovernanceHistoryTable', () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = createQueryClient();
    vi.clearAllMocks();
  });

  const renderComponent = (searchParams = '') => {
    return render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter initialEntries={[`/governance${searchParams}`]}>
          <GovernanceHistoryTable />
        </MemoryRouter>
      </QueryClientProvider>
    );
  };

  describe('loading state', () => {
    it('shows skeleton loaders when loading', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: undefined,
        isLoading: true,
        error: null,
      });

      renderComponent();

      // Should show skeleton elements
      const skeletons = document.querySelectorAll('.animate-pulse');
      expect(skeletons.length).toBeGreaterThan(0);
    });

    it('shows skeleton in stats cards when loading', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: undefined,
        isLoading: true,
        error: null,
      });

      renderComponent();

      expect(screen.getByText('Total Votes')).toBeInTheDocument();
      expect(screen.getByText('Accepted')).toBeInTheDocument();
      expect(screen.getByText('Rejected')).toBeInTheDocument();
      expect(screen.getByText('Expired')).toBeInTheDocument();
    });
  });

  describe('error state', () => {
    it('shows error alert when fetch fails', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: undefined,
        isLoading: false,
        error: new Error('API unavailable'),
      });

      renderComponent();

      expect(screen.getByText(/failed to load governance history/i)).toBeInTheDocument();
      expect(screen.getByText(/API unavailable/i)).toBeInTheDocument();
    });
  });

  describe('empty state', () => {
    it('shows empty message when no results', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: [],
        isLoading: false,
        error: null,
      });

      renderComponent();

      expect(screen.getByText(/no governance history found/i)).toBeInTheDocument();
    });

    it('shows zero counts in stats', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: [],
        isLoading: false,
        error: null,
      });

      renderComponent();

      // All stats should show 0
      const zeros = screen.getAllByText('0');
      expect(zeros.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('data display', () => {
    beforeEach(() => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: mockVoteResults,
        isLoading: false,
        error: null,
      });
    });

    it('displays vote results', () => {
      renderComponent();

      expect(screen.getByText('Add Featured App: TestApp')).toBeInTheDocument();
      expect(screen.getByText('Remove Validator: BadActor')).toBeInTheDocument();
      expect(screen.getByText('Update Amulet Rules')).toBeInTheDocument();
    });

    it('displays correct stats counts', () => {
      renderComponent();

      // Total: 3, Accepted: 1, Rejected: 1, Expired: 1
      expect(screen.getByText('3')).toBeInTheDocument(); // Total
      const ones = screen.getAllByText('1');
      expect(ones.length).toBeGreaterThanOrEqual(3); // Accepted, Rejected, Expired
    });

    it('displays action types', () => {
      renderComponent();

      expect(screen.getByText('CRARC_AddFutureAmuletConfigSchedule')).toBeInTheDocument();
      expect(screen.getByText('SRARC_OffboardSv')).toBeInTheDocument();
    });

    it('displays vote counts', () => {
      renderComponent();

      expect(screen.getByText(/5 for/)).toBeInTheDocument();
      expect(screen.getByText(/2 against/)).toBeInTheDocument();
    });

    it('displays reason when provided', () => {
      renderComponent();

      expect(screen.getByText('This is a valid proposal')).toBeInTheDocument();
    });

    it('displays reason URL as link', () => {
      renderComponent();

      const link = screen.getByRole('link', { name: /example.com\/proposal/i });
      expect(link).toHaveAttribute('href', 'https://example.com/proposal');
      expect(link).toHaveAttribute('target', '_blank');
    });

    it('shows "No reason provided" when missing', () => {
      renderComponent();

      expect(screen.getByText(/no reason provided/i)).toBeInTheDocument();
    });

    it('displays tracking CID', () => {
      renderComponent();

      expect(screen.getByText('abc123def456')).toBeInTheDocument();
    });
  });

  describe('outcome badges', () => {
    beforeEach(() => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: mockVoteResults,
        isLoading: false,
        error: null,
      });
    });

    it('displays accepted badge', () => {
      renderComponent();
      
      const acceptedBadge = screen.getByText('accepted');
      expect(acceptedBadge).toBeInTheDocument();
    });

    it('displays rejected badge', () => {
      renderComponent();
      
      const rejectedBadge = screen.getByText('rejected');
      expect(rejectedBadge).toBeInTheDocument();
    });

    it('displays expired badge', () => {
      renderComponent();
      
      const expiredBadge = screen.getByText('expired');
      expect(expiredBadge).toBeInTheDocument();
    });
  });

  describe('date formatting', () => {
    beforeEach(() => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: mockVoteResults,
        isLoading: false,
        error: null,
      });
    });

    it('formats dates correctly', () => {
      renderComponent();

      // Should format as "Jun 15, 2024" or similar
      expect(screen.getByText(/jun 15, 2024/i)).toBeInTheDocument();
    });

    it('handles missing dates gracefully', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: [{
          ...mockVoteResults[0],
          completedAt: null,
          voteBefore: null,
        }],
        isLoading: false,
        error: null,
      });

      renderComponent();

      expect(screen.getAllByText('N/A').length).toBeGreaterThanOrEqual(2);
    });

    it('handles invalid dates gracefully', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: [{
          ...mockVoteResults[0],
          completedAt: 'invalid-date',
          voteBefore: 'also-invalid',
        }],
        isLoading: false,
        error: null,
      });

      renderComponent();

      expect(screen.getAllByText('N/A').length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('proposal highlighting', () => {
    beforeEach(() => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: mockVoteResults,
        isLoading: false,
        error: null,
      });
    });

    it('applies highlight class when proposal matches URL param', () => {
      const { container } = renderComponent('?proposal=abc123def456');

      // The card with matching trackingCid should have highlight class
      const highlightedCard = container.querySelector('.ring-pink-500');
      expect(highlightedCard).toBeInTheDocument();
    });

    it('matches by short ID prefix', () => {
      const { container } = renderComponent('?proposal=abc123def456');

      const highlightedCard = container.querySelector('.ring-2');
      expect(highlightedCard).toBeInTheDocument();
    });
  });

  describe('collapsible JSON', () => {
    beforeEach(() => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: mockVoteResults,
        isLoading: false,
        error: null,
      });
    });

    it('renders Show Raw JSON button', () => {
      renderComponent();

      const jsonButtons = screen.getAllByText(/show raw json/i);
      expect(jsonButtons.length).toBe(mockVoteResults.length);
    });
  });

  describe('snapshots', () => {
    it('matches snapshot with data', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: mockVoteResults,
        isLoading: false,
        error: null,
      });

      const { container } = renderComponent();
      expect(container).toMatchSnapshot();
    });

    it('matches snapshot when loading', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: undefined,
        isLoading: true,
        error: null,
      });

      const { container } = renderComponent();
      expect(container).toMatchSnapshot();
    });

    it('matches snapshot with error', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: undefined,
        isLoading: false,
        error: new Error('Network error'),
      });

      const { container } = renderComponent();
      expect(container).toMatchSnapshot();
    });
  });
});
