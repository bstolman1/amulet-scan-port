import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { TooltipProvider } from '@/components/ui/tooltip';
import { DashboardLayout } from './DashboardLayout';

const getByText = (container: HTMLElement, text: string | RegExp) => {
  const elements = container.querySelectorAll('*');
  return Array.from(elements).find(el => {
    const content = el.textContent || '';
    if (typeof text === 'string') return content.includes(text);
    return text.test(content);
  });
};

const getButton = (container: HTMLElement, name: string | RegExp) => {
  const buttons = container.querySelectorAll('button');
  return Array.from(buttons).find(btn => {
    const content = btn.textContent || '';
    if (typeof name === 'string') return content.includes(name);
    return name.test(content);
  });
};

const getLink = (name: string | RegExp) => {
  const links = document.querySelectorAll('a');
  return Array.from(links).find(link => {
    const content = link.textContent || '';
    if (typeof name === 'string') return content.toLowerCase().includes(name.toLowerCase());
    return name.test(content);
  });
};

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

describe('DashboardLayout', () => {
  const renderWithRouter = (initialPath = '/') => {
    const queryClient = createTestQueryClient();
    return render(
      <QueryClientProvider client={queryClient}>
        <TooltipProvider>
          <MemoryRouter initialEntries={[initialPath]}>
            <DashboardLayout>
              <div data-testid="child-content">Test Content</div>
            </DashboardLayout>
          </MemoryRouter>
        </TooltipProvider>
      </QueryClientProvider>
    );
  };

  describe('rendering', () => {
    it('renders the SCANTON logo', () => {
      const { container } = renderWithRouter();
      expect(getByText(container, 'SCANTON')).toBeDefined();
    });

    it('renders the tagline', () => {
      const { container } = renderWithRouter();
      expect(getByText(container, 'Canton Network Analytics')).toBeDefined();
    });

    it('renders children content', () => {
      const { container } = renderWithRouter();
      expect(container.querySelector('[data-testid="child-content"]')).toBeDefined();
      expect(getByText(container, 'Test Content')).toBeDefined();
    });
  });

  describe('navigation', () => {
    it('renders navigation group buttons', () => {
      const { container } = renderWithRouter();
      const expectedGroups = [
        'Overview', 'Governance', 'Burn/Mint', 'Validators',
        'Rewards', 'Exchange Data', 'Services', 'Statistics',
      ];
      for (const groupName of expectedGroups) {
        expect(getButton(container, groupName)).toBeDefined();
      }
    });

    it('shows Dashboard link when Overview dropdown is opened', async () => {
      const { container } = renderWithRouter();
      const user = userEvent.setup();
      const overviewBtn = getButton(container, 'Overview')!;
      await user.click(overviewBtn);
      expect(getLink(/Dashboard/i)).toBeDefined();
    });

    it('shows Governance link when Governance dropdown is opened', async () => {
      const { container } = renderWithRouter();
      const user = userEvent.setup();
      const govBtn = getButton(container, 'Governance')!;
      await user.click(govBtn);
      expect(getLink(/Governance/)).toBeDefined();
    });

    it('has correct href for Dashboard', async () => {
      const { container } = renderWithRouter();
      const user = userEvent.setup();
      await user.click(getButton(container, 'Overview')!);
      const link = getLink(/Dashboard/i);
      expect(link?.getAttribute('href')).toBe('/');
    });

    it('has correct href for Governance', async () => {
      const { container } = renderWithRouter();
      const user = userEvent.setup();
      await user.click(getButton(container, 'Governance')!);
      const link = getLink(/^Governance$/);
      expect(link?.getAttribute('href')).toBe('/governance');
    });
  });

  describe('active state', () => {
    it('highlights Overview group when on root path', () => {
      const { container } = renderWithRouter('/');
      const overviewBtn = getButton(container, 'Overview');
      expect(overviewBtn?.className).toContain('bg-primary/10');
    });

    it('highlights Governance group when on governance path', () => {
      const { container } = renderWithRouter('/governance');
      const govBtn = getButton(container, 'Governance');
      expect(govBtn?.className).toContain('bg-primary/10');
    });

    it('does not highlight inactive groups', () => {
      const { container } = renderWithRouter('/governance');
      const overviewBtn = getButton(container, 'Overview');
      expect(overviewBtn?.className).not.toContain('bg-primary/10');
    });
  });

  describe('layout structure', () => {
    it('renders header as sticky', () => {
      const { container } = renderWithRouter();
      const header = container.querySelector('header');
      expect(header?.className).toContain('sticky');
      expect(header?.className).toContain('top-0');
    });

    it('renders main content area', () => {
      const { container } = renderWithRouter();
      const main = container.querySelector('main');
      expect(main).toBeDefined();
      expect(main?.className).toContain('container');
    });

    it('applies glass-card style to header', () => {
      const { container } = renderWithRouter();
      const header = container.querySelector('header');
      expect(header?.className).toContain('glass-card');
    });
  });

  describe('snapshots', () => {
    it('matches snapshot for root path', () => {
      const { container } = renderWithRouter('/');
      expect(container).toMatchSnapshot();
    });

    it('matches snapshot for governance path', () => {
      const { container } = renderWithRouter('/governance');
      expect(container).toMatchSnapshot();
    });
  });
});
