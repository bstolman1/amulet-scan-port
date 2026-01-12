import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { DashboardLayout } from './DashboardLayout';

// Helper functions
const getByText = (container: HTMLElement, text: string | RegExp) => {
  const elements = container.querySelectorAll('*');
  return Array.from(elements).find(el => {
    const content = el.textContent || '';
    if (typeof text === 'string') return content.includes(text);
    return text.test(content);
  });
};

const getLink = (container: HTMLElement, name: string | RegExp) => {
  const links = container.querySelectorAll('a');
  return Array.from(links).find(link => {
    const content = link.textContent || '';
    if (typeof name === 'string') return content.toLowerCase().includes(name.toLowerCase());
    return name.test(content);
  });
};

const getAllLinks = (container: HTMLElement) => Array.from(container.querySelectorAll('a'));

describe('DashboardLayout', () => {
  const renderWithRouter = (initialPath = '/') => {
    return render(
      <MemoryRouter initialEntries={[initialPath]}>
        <DashboardLayout>
          <div data-testid="child-content">Test Content</div>
        </DashboardLayout>
      </MemoryRouter>
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
    it('renders Dashboard link', () => {
      const { container } = renderWithRouter();
      expect(getLink(container, 'Dashboard')).toBeDefined();
    });

    it('renders Supply link', () => {
      const { container } = renderWithRouter();
      expect(getLink(container, 'Supply')).toBeDefined();
    });

    it('renders Governance link', () => {
      const { container } = renderWithRouter();
      const governanceLink = getLink(container, /^Governance$/);
      expect(governanceLink).toBeDefined();
    });

    it('renders Validators/SVs link', () => {
      const { container } = renderWithRouter();
      expect(getLink(container, 'Validators/SVs')).toBeDefined();
    });

    it('renders all core navigation items', () => {
      const { container } = renderWithRouter();
      
      const expectedLinks = [
        'Dashboard',
        'Supply',
        'Rich List',
        'Transactions',
        'Transfers',
        'Governance',
        'ANS',
        'Templates',
        'Admin',
      ];

      for (const linkName of expectedLinks) {
        const link = getLink(container, linkName);
        expect(link).toBeDefined();
      }
    });

    it('has correct href for Dashboard', () => {
      const { container } = renderWithRouter();
      const dashboardLink = getLink(container, 'Dashboard');
      expect(dashboardLink?.getAttribute('href')).toBe('/');
    });

    it('has correct href for Governance', () => {
      const { container } = renderWithRouter();
      const governanceLink = getLink(container, /^Governance$/);
      expect(governanceLink?.getAttribute('href')).toBe('/governance');
    });

    it('has correct href for Templates', () => {
      const { container } = renderWithRouter();
      const templatesLink = getLink(container, 'Templates');
      expect(templatesLink?.getAttribute('href')).toBe('/templates');
    });
  });

  describe('active state', () => {
    it('highlights Dashboard link when on root path', () => {
      const { container } = renderWithRouter('/');
      const dashboardLink = getLink(container, 'Dashboard');
      expect(dashboardLink?.className).toContain('bg-primary/10');
    });

    it('highlights Governance link when on governance path', () => {
      const { container } = renderWithRouter('/governance');
      const governanceLink = getLink(container, /^Governance$/);
      expect(governanceLink?.className).toContain('bg-primary/10');
    });

    it('does not highlight inactive links', () => {
      const { container } = renderWithRouter('/governance');
      const dashboardLink = getLink(container, 'Dashboard');
      expect(dashboardLink?.className).not.toContain('bg-primary/10');
    });

    it('applies hover styles to inactive links', () => {
      const { container } = renderWithRouter('/');
      const supplyLink = getLink(container, 'Supply');
      expect(supplyLink?.className).toContain('hover:bg-muted/50');
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
