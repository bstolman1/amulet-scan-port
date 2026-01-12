import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { DashboardLayout } from './DashboardLayout';

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
      renderWithRouter();
      expect(screen.getByText('SCANTON')).toBeInTheDocument();
    });

    it('renders the tagline', () => {
      renderWithRouter();
      expect(screen.getByText('Canton Network Analytics')).toBeInTheDocument();
    });

    it('renders children content', () => {
      renderWithRouter();
      expect(screen.getByTestId('child-content')).toBeInTheDocument();
      expect(screen.getByText('Test Content')).toBeInTheDocument();
    });
  });

  describe('navigation', () => {
    it('renders Dashboard link', () => {
      renderWithRouter();
      expect(screen.getByRole('link', { name: /dashboard/i })).toBeInTheDocument();
    });

    it('renders Supply link', () => {
      renderWithRouter();
      expect(screen.getByRole('link', { name: /supply/i })).toBeInTheDocument();
    });

    it('renders Governance link', () => {
      renderWithRouter();
      expect(screen.getByRole('link', { name: /^governance$/i })).toBeInTheDocument();
    });

    it('renders Validators/SVs link', () => {
      renderWithRouter();
      expect(screen.getByRole('link', { name: /validators\/svs/i })).toBeInTheDocument();
    });

    it('renders all navigation items', () => {
      renderWithRouter();
      
      const expectedLinks = [
        'Dashboard',
        'Supply',
        'Rich List',
        'Transactions',
        'Transfers',
        'Validators/SVs',
        'Governance',
        'ANS',
        'Templates',
        'Admin',
      ];

      for (const linkName of expectedLinks) {
        const link = screen.getByRole('link', { name: new RegExp(linkName, 'i') });
        expect(link).toBeInTheDocument();
      }
    });

    it('has correct href for Dashboard', () => {
      renderWithRouter();
      const dashboardLink = screen.getByRole('link', { name: /dashboard/i });
      expect(dashboardLink).toHaveAttribute('href', '/');
    });

    it('has correct href for Governance', () => {
      renderWithRouter();
      const governanceLink = screen.getByRole('link', { name: /^governance$/i });
      expect(governanceLink).toHaveAttribute('href', '/governance');
    });

    it('has correct href for Templates', () => {
      renderWithRouter();
      const templatesLink = screen.getByRole('link', { name: /templates/i });
      expect(templatesLink).toHaveAttribute('href', '/templates');
    });
  });

  describe('active state', () => {
    it('highlights Dashboard link when on root path', () => {
      renderWithRouter('/');
      const dashboardLink = screen.getByRole('link', { name: /dashboard/i });
      expect(dashboardLink).toHaveClass('bg-primary/10');
    });

    it('highlights Governance link when on governance path', () => {
      renderWithRouter('/governance');
      const governanceLink = screen.getByRole('link', { name: /^governance$/i });
      expect(governanceLink).toHaveClass('bg-primary/10');
    });

    it('does not highlight inactive links', () => {
      renderWithRouter('/governance');
      const dashboardLink = screen.getByRole('link', { name: /dashboard/i });
      expect(dashboardLink).not.toHaveClass('bg-primary/10');
    });

    it('applies hover styles to inactive links', () => {
      renderWithRouter('/');
      const supplyLink = screen.getByRole('link', { name: /supply/i });
      expect(supplyLink).toHaveClass('hover:bg-muted/50');
    });
  });

  describe('layout structure', () => {
    it('renders header as sticky', () => {
      const { container } = renderWithRouter();
      const header = container.querySelector('header');
      expect(header).toHaveClass('sticky', 'top-0');
    });

    it('renders main content area', () => {
      const { container } = renderWithRouter();
      const main = container.querySelector('main');
      expect(main).toBeInTheDocument();
      expect(main).toHaveClass('container');
    });

    it('applies glass-card style to header', () => {
      const { container } = renderWithRouter();
      const header = container.querySelector('header');
      expect(header).toHaveClass('glass-card');
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
