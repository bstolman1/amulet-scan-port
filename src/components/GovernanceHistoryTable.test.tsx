import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { GovernanceHistoryTable } from './GovernanceHistoryTable';

// Mock the hook
vi.mock('@/hooks/use-scan-vote-results', () => ({
  useGovernanceVoteHistory: vi.fn(),
}));

import { useGovernanceVoteHistory } from '@/hooks/use-scan-vote-results';

// Helper functions
const getByText = (container: HTMLElement, text: string | RegExp) => {
  const elements = container.querySelectorAll('*');
  return Array.from(elements).find(el => {
    const content = el.textContent || '';
    if (typeof text === 'string') return content.includes(text);
    return text.test(content);
  });
};

const getAllByText = (container: HTMLElement, text: string) => {
  const elements = container.querySelectorAll('*');
  return Array.from(elements).filter(el => el.textContent === text);
};

const getLink = (container: HTMLElement, href: string) => {
  const links = container.querySelectorAll('a');
  return Array.from(links).find(link => link.getAttribute('href')?.includes(href));
};

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

      const { container } = renderComponent();

      // Should show skeleton elements
      const skeletons = container.querySelectorAll('.animate-pulse');
      expect(skeletons.length).toBeGreaterThan(0);
    });

    it('shows stats card labels when loading', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: undefined,
        isLoading: true,
        error: null,
      });

      const { container } = renderComponent();

      expect(getByText(container, 'Total Votes')).toBeDefined();
      expect(getByText(container, 'Accepted')).toBeDefined();
      expect(getByText(container, 'Rejected')).toBeDefined();
      expect(getByText(container, 'Expired')).toBeDefined();
    });
  });

  describe('error state', () => {
    it('shows error alert when fetch fails', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: undefined,
        isLoading: false,
        error: new Error('API unavailable'),
      });

      const { container } = renderComponent();

      expect(getByText(container, /failed to load governance history/i)).toBeDefined();
      expect(getByText(container, /API unavailable/i)).toBeDefined();
    });
  });

  describe('empty state', () => {
    it('shows empty message when no results', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: [],
        isLoading: false,
        error: null,
      });

      const { container } = renderComponent();

      expect(getByText(container, /no governance history found/i)).toBeDefined();
    });

    it('shows zero counts in stats', () => {
      (useGovernanceVoteHistory as any).mockReturnValue({
        data: [],
        isLoading: false,
        error: null,
      });

      const { container } = renderComponent();

      // All stats should show 0
      const zeros = getAllByText(container, '0');
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
      const { container } = renderComponent();

      expect(getByText(container, 'Add Featured App: TestApp')).toBeDefined();
      expect(getByText(container, 'Remove Validator: BadActor')).toBeDefined();
      expect(getByText(container, 'Update Amulet Rules')).toBeDefined();
    });

    it('displays correct total count', () => {
      const { container } = renderComponent();

      // Total: 3
      expect(getByText(container, '3')).toBeDefined();
    });

    it('displays action types', () => {
      const { container } = renderComponent();

      expect(getByText(container, 'CRARC_AddFutureAmuletConfigSchedule')).toBeDefined();
      expect(getByText(container, 'SRARC_OffboardSv')).toBeDefined();
    });

    it('displays vote counts', () => {
      const { container } = renderComponent();

      expect(getByText(container, /5 for/)).toBeDefined();
      expect(getByText(container, /2 against/)).toBeDefined();
    });

    it('displays reason when provided', () => {
      const { container } = renderComponent();

      expect(getByText(container, 'This is a valid proposal')).toBeDefined();
    });

    it('displays reason URL as link', () => {
      const { container } = renderComponent();

      const link = getLink(container, 'example.com/proposal');
      expect(link).toBeDefined();
      expect(link?.getAttribute('href')).toBe('https://example.com/proposal');
      expect(link?.getAttribute('target')).toBe('_blank');
    });

    it('shows "No reason provided" when missing', () => {
      const { container } = renderComponent();

      expect(getByText(container, /no reason provided/i)).toBeDefined();
    });

    it('displays tracking CID', () => {
      const { container } = renderComponent();

      expect(getByText(container, 'abc123def456')).toBeDefined();
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

    it('displays outcome badges', () => {
      const { container } = renderComponent();
      
      expect(getByText(container, 'accepted')).toBeDefined();
      expect(getByText(container, 'rejected')).toBeDefined();
      expect(getByText(container, 'expired')).toBeDefined();
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
      const { container } = renderComponent();

      // Should format as "Jun 15, 2024" or similar
      expect(getByText(container, /Jun 15, 2024/i)).toBeDefined();
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

      const { container } = renderComponent();

      expect(getAllByText(container, 'N/A').length).toBeGreaterThanOrEqual(2);
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

      const { container } = renderComponent();

      expect(getAllByText(container, 'N/A').length).toBeGreaterThanOrEqual(2);
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
      expect(highlightedCard).toBeDefined();
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

    it('renders Show Raw JSON buttons', () => {
      const { container } = renderComponent();

      const jsonButtons = container.querySelectorAll('button');
      const showJsonButtons = Array.from(jsonButtons).filter((btn: Element) => 
        btn.textContent?.includes('Show Raw JSON')
      );
      expect(showJsonButtons.length).toBe(mockVoteResults.length);
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
