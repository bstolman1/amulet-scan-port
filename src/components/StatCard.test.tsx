import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { StatCard } from './StatCard';
import { Activity, Users } from 'lucide-react';

describe('StatCard', () => {
  it('renders title and value', () => {
    const { container } = render(<StatCard title="Total Users" value={1234} icon={Users} />);
    expect(container.textContent).toContain('Total Users');
    expect(container.textContent).toContain('1234');
  });

  it('renders string values', () => {
    const { container } = render(<StatCard title="Status" value="Active" icon={Activity} />);
    expect(container.textContent).toContain('Active');
  });

  it('renders trend value when provided', () => {
    const { container } = render(
      <StatCard title="Revenue" value="$10,000" icon={Activity} trend={{ value: '+15%', positive: true }} />
    );
    expect(container.textContent).toContain('+15%');
  });

  it('applies glow-primary class when gradient is true', () => {
    const { container } = render(<StatCard title="Featured" value={999} icon={Activity} gradient={true} />);
    expect(container.querySelector('.glow-primary')).toBeDefined();
  });

  it('displays zero correctly', () => {
    const { container } = render(<StatCard title="Empty" value={0} icon={Users} />);
    expect(container.textContent).toContain('0');
  });

  it('displays special characters in value', () => {
    const { container } = render(<StatCard title="Special" value="$1,234.56" icon={Activity} />);
    expect(container.textContent).toContain('$1,234.56');
  });
});
