/**
 * Badge Component Tests
 * 
 * Tests for the Badge UI component and its variants
 */

import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { Badge, badgeVariants } from './badge';

describe('Badge', () => {
  it('renders with default props', () => {
    const { container } = render(<Badge>New</Badge>);
    const badge = container.firstChild as HTMLElement;
    expect(badge.textContent).toBe('New');
  });

  it('applies default variant styles', () => {
    const { container } = render(<Badge>Default</Badge>);
    const badge = container.firstChild as HTMLElement;
    expect(badge.className).toContain('bg-primary');
    expect(badge.className).toContain('text-primary-foreground');
  });

  it('applies secondary variant', () => {
    const { container } = render(<Badge variant="secondary">Secondary</Badge>);
    const badge = container.firstChild as HTMLElement;
    expect(badge.className).toContain('bg-secondary');
    expect(badge.className).toContain('text-secondary-foreground');
  });

  it('applies destructive variant', () => {
    const { container } = render(<Badge variant="destructive">Error</Badge>);
    const badge = container.firstChild as HTMLElement;
    expect(badge.className).toContain('bg-destructive');
  });

  it('applies outline variant', () => {
    const { container } = render(<Badge variant="outline">Outline</Badge>);
    const badge = container.firstChild as HTMLElement;
    expect(badge.className).toContain('text-foreground');
  });

  it('merges custom className', () => {
    const { container } = render(<Badge className="my-class">Custom</Badge>);
    const badge = container.firstChild as HTMLElement;
    expect(badge.className).toContain('my-class');
  });

  it('applies common badge styles', () => {
    const { container } = render(<Badge>Badge</Badge>);
    const badge = container.firstChild as HTMLElement;
    expect(badge.className).toContain('inline-flex');
    expect(badge.className).toContain('items-center');
    expect(badge.className).toContain('rounded-md');
    expect(badge.className).toContain('border');
  });
});

describe('badgeVariants', () => {
  it('generates correct class strings for default variant', () => {
    const classes = badgeVariants({ variant: 'default' });
    expect(classes).toContain('bg-primary');
  });

  it('generates correct class strings for secondary variant', () => {
    const classes = badgeVariants({ variant: 'secondary' });
    expect(classes).toContain('bg-secondary');
  });

  it('generates correct class strings for destructive variant', () => {
    const classes = badgeVariants({ variant: 'destructive' });
    expect(classes).toContain('bg-destructive');
  });

  it('generates correct class strings for outline variant', () => {
    const classes = badgeVariants({ variant: 'outline' });
    expect(classes).toContain('text-foreground');
  });
});
