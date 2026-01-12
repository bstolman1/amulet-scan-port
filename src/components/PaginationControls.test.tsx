import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
import { PaginationControls } from './PaginationControls';

describe('PaginationControls', () => {
  const defaultProps = {
    currentPage: 1,
    totalItems: 100,
    pageSize: 10,
    onPageChange: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders previous and next buttons', () => {
    const { container } = render(<PaginationControls {...defaultProps} />);
    expect(container.textContent).toContain('Previous');
    expect(container.textContent).toContain('Next');
  });

  it('displays correct page count', () => {
    const { container } = render(<PaginationControls {...defaultProps} />);
    expect(container.textContent).toContain('Page 1 of 10');
  });

  it('shows at least 1 page even with 0 items', () => {
    const { container } = render(<PaginationControls {...defaultProps} totalItems={0} />);
    expect(container.textContent).toContain('Page 1 of 1');
  });

  it('disables Previous button on first page', () => {
    const { container } = render(<PaginationControls {...defaultProps} currentPage={1} />);
    const buttons = container.querySelectorAll('button');
    expect(buttons[0].disabled).toBe(true);
  });

  it('disables Next button on last page', () => {
    const { container } = render(<PaginationControls {...defaultProps} currentPage={10} />);
    const buttons = container.querySelectorAll('button');
    expect(buttons[1].disabled).toBe(true);
  });

  it('calls onPageChange with previous page when clicking Previous', () => {
    const onPageChange = vi.fn();
    const { container } = render(
      <PaginationControls {...defaultProps} currentPage={5} onPageChange={onPageChange} />
    );
    const buttons = container.querySelectorAll('button');
    fireEvent.click(buttons[0]);
    expect(onPageChange).toHaveBeenCalledWith(4);
  });

  it('calls onPageChange with next page when clicking Next', () => {
    const onPageChange = vi.fn();
    const { container } = render(
      <PaginationControls {...defaultProps} currentPage={5} onPageChange={onPageChange} />
    );
    const buttons = container.querySelectorAll('button');
    fireEvent.click(buttons[1]);
    expect(onPageChange).toHaveBeenCalledWith(6);
  });
});
