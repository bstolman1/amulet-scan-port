/**
 * Card Component Tests
 * 
 * Tests for the Card UI component and its subcomponents
 */

import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { 
  Card, 
  CardHeader, 
  CardTitle, 
  CardDescription, 
  CardContent, 
  CardFooter 
} from './card';

describe('Card', () => {
  it('renders with default props', () => {
    const { container } = render(<Card>Card Content</Card>);
    const card = container.firstChild as HTMLElement;
    expect(card.textContent).toBe('Card Content');
  });

  it('applies base card styles', () => {
    const { container } = render(<Card>Content</Card>);
    const card = container.firstChild as HTMLElement;
    expect(card.className).toContain('rounded-xl');
    expect(card.className).toContain('border');
    expect(card.className).toContain('bg-card');
  });

  it('merges custom className', () => {
    const { container } = render(<Card className="my-custom-class">Content</Card>);
    const card = container.firstChild as HTMLElement;
    expect(card.className).toContain('my-custom-class');
  });

  it('forwards ref', () => {
    let ref: HTMLDivElement | null = null;
    render(<Card ref={(el) => { ref = el; }}>Content</Card>);
    expect(ref).toBeInstanceOf(HTMLDivElement);
  });
});

describe('CardHeader', () => {
  it('renders children', () => {
    const { container } = render(<CardHeader>Header Content</CardHeader>);
    expect(container.textContent).toBe('Header Content');
  });

  it('applies header styles', () => {
    const { container } = render(<CardHeader>Header</CardHeader>);
    const header = container.firstChild as HTMLElement;
    expect(header.className).toContain('flex');
    expect(header.className).toContain('flex-col');
    expect(header.className).toContain('p-6');
  });
});

describe('CardTitle', () => {
  it('renders title text', () => {
    const { container } = render(<CardTitle>My Title</CardTitle>);
    expect(container.textContent).toBe('My Title');
  });

  it('applies title styles', () => {
    const { container } = render(<CardTitle>Title</CardTitle>);
    const title = container.firstChild as HTMLElement;
    expect(title.className).toContain('font-semibold');
    expect(title.className).toContain('leading-none');
  });
});

describe('CardDescription', () => {
  it('renders description text', () => {
    const { container } = render(<CardDescription>A description</CardDescription>);
    expect(container.textContent).toBe('A description');
  });

  it('applies muted foreground color', () => {
    const { container } = render(<CardDescription>Description</CardDescription>);
    const desc = container.firstChild as HTMLElement;
    expect(desc.className).toContain('text-muted-foreground');
  });
});

describe('CardContent', () => {
  it('renders content', () => {
    const { container } = render(<CardContent>Main content here</CardContent>);
    expect(container.textContent).toBe('Main content here');
  });

  it('applies content padding', () => {
    const { container } = render(<CardContent>Content</CardContent>);
    const content = container.firstChild as HTMLElement;
    expect(content.className).toContain('p-6');
    expect(content.className).toContain('pt-0');
  });
});

describe('CardFooter', () => {
  it('renders footer content', () => {
    const { container } = render(<CardFooter>Footer content</CardFooter>);
    expect(container.textContent).toBe('Footer content');
  });

  it('applies footer flex layout', () => {
    const { container } = render(<CardFooter>Footer</CardFooter>);
    const footer = container.firstChild as HTMLElement;
    expect(footer.className).toContain('flex');
    expect(footer.className).toContain('items-center');
  });
});

describe('Card composition', () => {
  it('renders complete card with all subcomponents', () => {
    const { container } = render(
      <Card>
        <CardHeader>
          <CardTitle>Card Title</CardTitle>
          <CardDescription>Card description here</CardDescription>
        </CardHeader>
        <CardContent>
          <p>Main content area</p>
        </CardContent>
        <CardFooter>
          <button>Action</button>
        </CardFooter>
      </Card>
    );

    expect(container.textContent).toContain('Card Title');
    expect(container.textContent).toContain('Card description here');
    expect(container.textContent).toContain('Main content area');
    expect(container.textContent).toContain('Action');
  });
});
