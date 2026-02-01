/**
 * Button Component Tests
 * 
 * Tests for the Button UI component and its variants
 */

import { describe, it, expect, vi } from 'vitest';
import { render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Button } from './button';

// Helper to get element by role
const getButton = (container: HTMLElement, name?: string) => {
  const buttons = container.querySelectorAll('button');
  if (name) {
    return Array.from(buttons).find(b => b.textContent?.includes(name));
  }
  return buttons[0];
};

describe('Button', () => {
  it('renders with default props', () => {
    const { container } = render(<Button>Click me</Button>);
    const button = getButton(container, 'Click me');
    expect(button).toBeDefined();
    expect(button?.textContent).toBe('Click me');
  });

  it('applies default variant classes', () => {
    const { container } = render(<Button>Default</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('bg-primary');
    expect(button?.className).toContain('text-primary-foreground');
  });

  it('applies destructive variant', () => {
    const { container } = render(<Button variant="destructive">Delete</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('bg-destructive');
  });

  it('applies outline variant', () => {
    const { container } = render(<Button variant="outline">Outline</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('border');
  });

  it('applies secondary variant', () => {
    const { container } = render(<Button variant="secondary">Secondary</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('bg-secondary');
  });

  it('applies ghost variant', () => {
    const { container } = render(<Button variant="ghost">Ghost</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('hover:bg-accent');
  });

  it('applies link variant', () => {
    const { container } = render(<Button variant="link">Link</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('underline-offset-4');
  });

  it('applies size sm', () => {
    const { container } = render(<Button size="sm">Small</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('h-9');
  });

  it('applies size lg', () => {
    const { container } = render(<Button size="lg">Large</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('h-12');
  });

  it('applies size icon', () => {
    const { container } = render(<Button size="icon">🔍</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('h-10');
    expect(button?.className).toContain('w-10');
  });

  it('handles disabled state', () => {
    const { container } = render(<Button disabled>Disabled</Button>);
    const button = getButton(container);
    expect(button?.disabled).toBe(true);
  });

  it('merges custom className', () => {
    const { container } = render(<Button className="custom-class">Custom</Button>);
    const button = getButton(container);
    expect(button?.className).toContain('custom-class');
  });

  it('renders as child when asChild is true', () => {
    const { container } = render(
      <Button asChild>
        <a href="/test">Link Button</a>
      </Button>
    );
    const link = container.querySelector('a');
    expect(link).toBeDefined();
    expect(link?.getAttribute('href')).toBe('/test');
    expect(link?.textContent).toBe('Link Button');
  });

  it('handles click events', async () => {
    const handleClick = vi.fn();
    const { container } = render(<Button onClick={handleClick}>Clickable</Button>);
    
    const button = getButton(container);
    if (button) {
      await userEvent.click(button);
    }
    
    expect(handleClick).toHaveBeenCalledTimes(1);
  });

  it('does not trigger click when disabled', async () => {
    const handleClick = vi.fn();
    const { container } = render(<Button disabled onClick={handleClick}>Disabled</Button>);
    
    const button = getButton(container);
    if (button) {
      await userEvent.click(button);
    }
    
    expect(handleClick).not.toHaveBeenCalled();
  });
});
