# Testing Guide

This project uses **Vitest** as the testing framework, with **React Testing Library** for component testing.

## Running Tests

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage

# Run only server tests
npm test -- server/

# Run only frontend tests
npm test -- src/
```

## Test Structure

```
├── src/
│   ├── lib/
│   │   ├── amount-utils.test.ts      # CC amount conversion tests
│   │   ├── utils.test.ts             # Utility function tests
│   │   └── api-client.test.ts        # API client tests
│   ├── hooks/
│   │   └── use-toast.test.ts         # Toast hook tests
│   └── components/
│       ├── ErrorBoundary.test.tsx    # Error boundary tests
│       └── ui/
│           ├── button.test.tsx       # Button component tests
│           ├── card.test.tsx         # Card component tests
│           └── badge.test.tsx        # Badge component tests
└── server/
    ├── api/
    │   ├── stats.test.js             # Stats API integration tests
    │   ├── events.test.js            # Events API integration tests
    │   ├── search.test.js            # Search API integration tests
    │   ├── rewards.test.js           # Rewards API tests
    │   ├── party.test.js             # Party API tests
    │   └── governance-lifecycle.test.js  # Governance lifecycle tests
    ├── engine/
    │   ├── decoder.test.js           # Binary decoder tests
    │   ├── vote-outcome-analyzer.test.js # Vote threshold logic tests
    │   └── reward-indexer.test.js    # Reward indexing tests
    ├── lib/
    │   └── sql-sanitize.test.js      # SQL injection prevention tests
    └── test/
        └── fixtures/
            ├── mock-data.js          # Mock data fixtures
            ├── mock-db.js            # Mock database connection
            └── mock-binary-reader.js # Mock binary reader
```

## Test Categories

### 1. SQL Sanitization Tests (Critical Security)

Located in `server/lib/sql-sanitize.test.js`, these tests verify:

- Detection of SQL injection patterns (UNION, DROP, DELETE, etc.)
- **Tautology-based injection detection** (OR 1=1, 'a'='a', etc.)
- **Comment-based bypass detection** (/**/, --)
- Proper escaping of special characters in strings
- Input validation for identifiers, timestamps, contract IDs
- Safe query builder functions

**These tests are critical** - they verify the security layer that prevents SQL injection attacks.

### 2. Engine Tests (Core Business Logic)

- **decoder.test.js**: Binary file decoding, malformed input handling
- **vote-outcome-analyzer.test.js**: 2/3 majority threshold voting logic
- **reward-indexer.test.js**: Reward coupon indexing and CC calculations

### 3. API Tests

- **stats.test.js**: Statistics endpoint validation
- **events.test.js**: Event query parameter validation
- **search.test.js**: Search query sanitization
- **rewards.test.js**: Reward calculation and filtering
- **party.test.js**: Party search and event retrieval
- **governance-lifecycle.test.js**: Governance flow classification

### 4. Utility Tests

- **amount-utils.test.ts**: Tests for CC (Canton Coin) amount conversions
- **utils.test.ts**: Tests for the `cn()` class name merging utility

### 5. Hook Tests

- **use-toast.test.ts**: Tests for the toast notification system

### 6. Component Tests

- **button.test.tsx**: Button variants, sizes, and click handling
- **card.test.tsx**: Card composition and styling
- **badge.test.tsx**: Badge variants
- **ErrorBoundary.test.tsx**: Error catching and display

## Writing New Tests

### Test File Naming

- Frontend: `*.test.ts` or `*.test.tsx`
- Server: `*.test.js`

### Basic Test Template

```typescript
import { describe, it, expect } from 'vitest';
import { myFunction } from './my-module';

describe('myFunction', () => {
  it('should handle normal input', () => {
    expect(myFunction('input')).toBe('expected');
  });

  it('should handle edge cases', () => {
    expect(myFunction(null)).toBe(null);
    expect(myFunction('')).toBe('');
  });
});
```

### Component Test Template

```tsx
import { describe, it, expect, vi } from 'vitest';
import { render } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MyComponent } from './MyComponent';

describe('MyComponent', () => {
  it('renders correctly', () => {
    const { container } = render(<MyComponent>Content</MyComponent>);
    expect(container.textContent).toContain('Content');
  });

  it('handles user interactions', async () => {
    const onClick = vi.fn();
    const { container } = render(<MyComponent onClick={onClick} />);
    
    const button = container.querySelector('button');
    if (button) {
      await userEvent.click(button);
    }
    
    expect(onClick).toHaveBeenCalled();
  });
});
```

## Coverage

Run `npm run test:coverage` to generate coverage reports:

- **Text report**: Displayed in terminal
- **HTML report**: Open `coverage/index.html` in browser
- **JSON report**: `coverage/coverage-final.json`

### Coverage Targets

| Area | Target |
|------|--------|
| SQL Sanitization | 100% |
| Utility Functions | 90%+ |
| UI Components | 80%+ |
| Hooks | 80%+ |

## Mocking

### Mocking Fetch

```typescript
import { vi } from 'vitest';

const mockFetch = vi.fn();
global.fetch = mockFetch;

beforeEach(() => {
  mockFetch.mockReset();
});

it('handles API response', async () => {
  mockFetch.mockResolvedValueOnce({
    ok: true,
    json: async () => ({ data: [] }),
  });
  
  // ... test code
});
```

### Mocking Modules

```typescript
vi.mock('./my-module', () => ({
  myFunction: vi.fn(() => 'mocked'),
}));
```

## Best Practices

1. **Test behavior, not implementation**: Focus on what the code does, not how it does it

2. **Use descriptive test names**: `it('returns null when input is empty')` not `it('test1')`

3. **One assertion per test** (when practical): Makes failures easier to diagnose

4. **Test edge cases**: null, undefined, empty strings, boundary values

5. **Keep tests independent**: Each test should be able to run in isolation

6. **Mock external dependencies**: API calls, timers, random values

7. **Security tests are mandatory**: All SQL-related code must have injection tests

## CI/CD Integration

Tests run automatically on every PR via GitHub Actions (`.github/workflows/test.yml`):

1. **Test job**: Runs `vitest run --coverage` and uploads coverage artifacts
2. **Lint job**: Runs ESLint checks
3. **Build job**: Verifies the project builds successfully

### Coverage Artifacts

After each CI run, coverage reports are available as downloadable artifacts:
- Download from the Actions tab → Select workflow run → Artifacts section

### PR Coverage Comments

PRs automatically receive a comment with coverage metrics (lines, statements, functions, branches).

## API Integration Tests

The server API tests use mocked dependencies to test endpoint logic without a real database:

```javascript
// Example: Mock database in test
vi.mock('../duckdb/connection.js', () => ({
  default: {
    safeQuery: vi.fn().mockResolvedValue([{ count: 100n }]),
    hasFileType: vi.fn().mockReturnValue(false),
  },
}));
```

### Fixtures

Mock data and helpers are in `server/test/fixtures/`:

- **mock-data.js**: Sample events, governance data, stats responses
- **mock-db.js**: Stubbed database connection
- **mock-binary-reader.js**: Stubbed binary file reader

### Running API Tests

```bash
# Run all server tests
npm test -- server/

# Run specific API tests
npm test -- server/api/stats.test.js
npm test -- server/api/events.test.js
npm test -- server/api/search.test.js
```
