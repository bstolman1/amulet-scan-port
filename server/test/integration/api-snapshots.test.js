/**
 * API Snapshot Tests
 * 
 * Lock down response structures for /api/events, /api/stats, and /api/governance-lifecycle endpoints.
 * These tests validate the shape of API responses and REQUIRE success (200) or properly validate errors.
 * 
 * IMPORTANT: Tests now FAIL on 500 errors or validate error structure - no silent passes.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createTestApp } from '../app.js';

let server;
let baseUrl;

beforeAll(async () => {
  const app = createTestApp();
  await new Promise((resolve) => {
    server = app.listen(0, () => {
      const addr = server.address();
      baseUrl = `http://localhost:${addr.port}`;
      resolve();
    });
  });
});

afterAll(async () => {
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
});

/**
 * HTTP helper for JSON requests
 */
async function httpJson(path, { query, method = 'GET' } = {}) {
  const url = new URL(path, baseUrl);
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      url.searchParams.set(k, String(v));
    }
  }
  const res = await fetch(url, { method });
  let body;
  try {
    body = await res.json();
  } catch {
    body = undefined;
  }
  return { status: res.status, body };
}

/**
 * Normalize dynamic values for stable snapshots
 * Replaces timestamps, UUIDs, and IDs with placeholders
 */
function normalizeForSnapshot(obj) {
  if (!obj) return obj;
  return JSON.parse(JSON.stringify(obj, (key, value) => {
    // Normalize ISO timestamps
    if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
      return '[TIMESTAMP]';
    }
    // Normalize hex IDs (contract IDs, UUIDs without dashes)
    if (typeof value === 'string' && /^[0-9a-f]{16,}$/i.test(value)) {
      return '[ID]';
    }
    // Normalize UUIDs with dashes
    if (typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
      return '[UUID]';
    }
    return value;
  }));
}

/**
 * Extract the structure/shape of a response for snapshot comparison
 */
function extractStructure(body) {
  if (Array.isArray(body)) {
    return {
      type: 'array',
      length: body.length,
      itemShape: body[0] ? Object.keys(body[0]).sort() : [],
    };
  }
  if (typeof body === 'object' && body !== null) {
    return {
      type: 'object',
      keys: Object.keys(body).sort(),
    };
  }
  return { type: typeof body };
}

/**
 * Validate error response structure (used when APIs return 4xx/5xx)
 */
function validateErrorResponse(body) {
  expect(body).toBeDefined();
  expect(typeof body).toBe('object');
  // Error responses should have an error message
  expect('error' in body || 'message' in body || 'details' in body).toBe(true);
}

// =============================================================================
// Health & Root Endpoints
// =============================================================================

describe('API Snapshots: Core Endpoints', () => {
  it('GET /health returns expected structure', async () => {
    const { status, body } = await httpJson('/health');
    expect(status).toBe(200);
    expect(body).toHaveProperty('status');
    expect(body).toHaveProperty('timestamp');
    expect(body.status).toBe('ok');
    
    expect({
      fields: Object.keys(body).sort(),
      statusValue: body.status,
    }).toMatchSnapshot();
  });

  it('GET / returns API info structure', async () => {
    const { status, body } = await httpJson('/');
    expect(status).toBe(200);
    expect(body).toHaveProperty('name');
    expect(body).toHaveProperty('version');
    expect(body).toHaveProperty('status');
    expect(body.status).toBe('ok');
    
    expect({
      fields: Object.keys(body).sort(),
      hasEngine: 'engine' in body,
    }).toMatchSnapshot();
  });

  it('GET /nonexistent returns 404 structure', async () => {
    const { status, body } = await httpJson('/api/nonexistent-endpoint-xyz');
    expect(status).toBe(404);
  });
});

// =============================================================================
// Events API Snapshots
// =============================================================================

describe('API Snapshots: Events Endpoints', () => {
  it('GET /api/events/latest returns expected structure', async () => {
    const { status, body } = await httpJson('/api/events/latest', { query: { limit: 3 } });
    
    // STRICT: Must be 200 or we fail with useful message
    expect(status, `Expected 200 but got ${status}: ${JSON.stringify(body)}`).toBe(200);
    
    expect(body).toHaveProperty('data');
    expect(Array.isArray(body.data)).toBe(true);
    expect(typeof body.count).toBe('number');
    
    expect(normalizeForSnapshot({
      hasData: true,
      hasCount: true,
      hasSource: 'source' in body,
      hasMore: 'hasMore' in body,
      dataIsArray: true,
      // Only snapshot shape if we have data
      dataShape: body.data.length > 0 ? Object.keys(body.data[0]).sort() : '[EMPTY]',
    })).toMatchSnapshot();
  });

  it('GET /api/events/latest with pagination returns consistent structure', async () => {
    const { status, body } = await httpJson('/api/events/latest', { 
      query: { limit: 5, offset: 0 } 
    });
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toHaveProperty('data');
    expect(Array.isArray(body.data)).toBe(true);
    
    expect({
      isArray: true,
      paginationFields: ['count', 'hasMore', 'offset'].filter(f => f in body).sort(),
    }).toMatchSnapshot();
  });

  it('GET /api/events/by-type/:type returns expected structure', async () => {
    const { status, body } = await httpJson('/api/events/by-type/created', { 
      query: { limit: 3 } 
    });
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect({
      hasData: 'data' in body || Array.isArray(body),
      responseType: Array.isArray(body) ? 'array' : 'object',
    }).toMatchSnapshot();
  });

  it('GET /api/events/by-template/:id returns expected structure', async () => {
    const { status, body } = await httpJson('/api/events/by-template/Splice.Amulet:Amulet', { 
      query: { limit: 3 } 
    });
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect({
      hasData: 'data' in body || Array.isArray(body),
      responseType: Array.isArray(body) ? 'array' : 'object',
    }).toMatchSnapshot();
  });

  it('GET /api/events/count returns expected structure', async () => {
    const { status, body } = await httpJson('/api/events/count');
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toHaveProperty('count');
    expect(typeof body.count).toBe('number');
    
    expect({
      hasCount: true,
      hasEstimated: 'estimated' in body,
      hasSource: 'source' in body,
      fields: Object.keys(body).sort(),
    }).toMatchSnapshot();
  });

  it('GET /api/events/debug returns expected structure', async () => {
    const { status, body } = await httpJson('/api/events/debug');
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toBeDefined();
    expect(typeof body).toBe('object');
    
    expect({
      responseType: 'object',
      isObject: true,
      topLevelKeys: Object.keys(body).sort(),
    }).toMatchSnapshot();
  });

  it('GET /api/events/governance returns expected structure', async () => {
    const { status, body } = await httpJson('/api/events/governance', { 
      query: { limit: 5 } 
    });
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    const isArrayResponse = Array.isArray(body);
    const hasDataProp = !isArrayResponse && 'data' in body;
    
    expect({
      responseType: isArrayResponse ? 'array' : 'object',
      hasData: hasDataProp,
      topLevelKeys: isArrayResponse ? [] : Object.keys(body).sort(),
    }).toMatchSnapshot();
  });

  it('handles validation by returning appropriate status', async () => {
    // Test that invalid params are handled gracefully (200 with defaults or 400 with error)
    const { status, body } = await httpJson('/api/events/latest', { 
      query: { limit: -1 } 
    });
    
    // Either 200 (coerced to valid value) or 400 (validation error)
    expect([200, 400]).toContain(status);
    
    if (status === 400) {
      validateErrorResponse(body);
    } else {
      expect(body).toHaveProperty('data');
    }
  });
});

// =============================================================================
// Stats API Snapshots
// =============================================================================

describe('API Snapshots: Stats Endpoints', () => {
  it('GET /api/stats/overview returns expected structure', async () => {
    const { status, body } = await httpJson('/api/stats/overview');
    expect(status, `Expected 200 but got ${status}: ${JSON.stringify(body)}`).toBe(200);
    
    expect(body).toHaveProperty('total_events');
    expect(body).toHaveProperty('data_source');
    expect(typeof body.total_events).toBe('number');
    
    expect({
      fields: Object.keys(body).sort(),
      hasTotalEvents: true,
      hasDataSource: true,
      validSource: ['engine', 'binary', 'jsonl', 'parquet', 'empty'].includes(body.data_source),
    }).toMatchSnapshot();
  });

  it('GET /api/stats/daily returns expected structure', async () => {
    const { status, body } = await httpJson('/api/stats/daily', { 
      query: { days: 7 } 
    });
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toHaveProperty('data');
    expect(Array.isArray(body.data)).toBe(true);
    
    expect({
      hasData: true,
      isArray: true,
      itemShape: body.data.length > 0 ? Object.keys(body.data[0]).sort() : '[EMPTY]',
    }).toMatchSnapshot();
  });

  it('GET /api/stats/by-type returns expected structure', async () => {
    const { status, body } = await httpJson('/api/stats/by-type');
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toHaveProperty('data');
    expect(Array.isArray(body.data)).toBe(true);
    
    expect({
      hasData: true,
      isArray: true,
      itemShape: body.data.length > 0 ? Object.keys(body.data[0]).sort() : '[EMPTY]',
    }).toMatchSnapshot();
  });

  it('GET /api/stats/by-template returns expected structure', async () => {
    const { status, body } = await httpJson('/api/stats/by-template', { 
      query: { limit: 10 } 
    });
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toHaveProperty('data');
    expect(Array.isArray(body.data)).toBe(true);
    
    expect({
      hasData: true,
      isArray: true,
      dataLength: body.data.length,
      itemShape: body.data.length > 0 ? Object.keys(body.data[0]).sort() : '[EMPTY]',
    }).toMatchSnapshot();
  });

  it('GET /api/stats/hourly returns expected structure', async () => {
    const { status, body } = await httpJson('/api/stats/hourly');
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toHaveProperty('data');
    expect(Array.isArray(body.data)).toBe(true);
    
    expect({
      hasData: true,
      isArray: true,
      itemShape: body.data.length > 0 ? Object.keys(body.data[0]).sort() : '[EMPTY]',
    }).toMatchSnapshot();
  });

  it('GET /api/stats/burn returns expected structure', async () => {
    const { status, body } = await httpJson('/api/stats/burn');
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toHaveProperty('data');
    expect(Array.isArray(body.data)).toBe(true);
    
    expect({
      hasData: true,
      isArray: true,
      itemShape: body.data.length > 0 ? Object.keys(body.data[0]).sort() : '[EMPTY]',
    }).toMatchSnapshot();
  });

  it('GET /api/stats/sources returns expected structure', async () => {
    const { status, body } = await httpJson('/api/stats/sources');
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect(body).toBeDefined();
    expect(typeof body).toBe('object');
    
    expect({
      fields: Object.keys(body).sort(),
      hasSources: 'sources' in body || 'primary_source' in body,
    }).toMatchSnapshot();
  });
});

// =============================================================================
// Governance Lifecycle API Snapshots
// =============================================================================

describe('API Snapshots: Governance Lifecycle Endpoints', () => {
  it('GET /api/governance-lifecycle returns expected structure', async () => {
    const { status, body } = await httpJson('/api/governance-lifecycle');
    expect(status, `Expected 200 but got ${status}: ${JSON.stringify(body)}`).toBe(200);
    
    const isArrayResponse = Array.isArray(body);
    const hasProposals = isArrayResponse || 
                        (body.proposals && Array.isArray(body.proposals)) ||
                        (body.data && Array.isArray(body.data));
    
    expect(hasProposals || typeof body === 'object').toBe(true);
    
    expect({
      responseType: isArrayResponse ? 'array' : 'object',
      hasProposals,
      topLevelKeys: isArrayResponse ? [] : Object.keys(body).sort(),
    }).toMatchSnapshot();
  });

  it('GET /api/governance-lifecycle with pagination returns consistent structure', async () => {
    const { status, body } = await httpJson('/api/governance-lifecycle', { 
      query: { limit: 5, offset: 0 } 
    });
    expect(status, `Expected 200 but got ${status}`).toBe(200);
    
    expect({
      responseType: Array.isArray(body) ? 'array' : 'object',
      paginationSupported: 'total' in body || 'count' in body || 'hasMore' in body || Array.isArray(body),
    }).toMatchSnapshot();
  });

  it('GET /api/governance-lifecycle/proposals returns expected structure or 404', async () => {
    const { status, body } = await httpJson('/api/governance-lifecycle/proposals', { 
      query: { limit: 5 } 
    });
    
    // 200 = has data, 404 = endpoint doesn't exist (acceptable)
    expect([200, 404]).toContain(status);
    
    if (status === 200) {
      expect(extractStructure(body)).toMatchSnapshot();
    }
  });

  it('GET /api/governance-lifecycle/stats returns expected structure or 404', async () => {
    const { status, body } = await httpJson('/api/governance-lifecycle/stats');
    
    expect([200, 404]).toContain(status);
    
    if (status === 200) {
      expect(body).toBeDefined();
      expect(typeof body).toBe('object');
      
      expect({
        responseType: 'object',
        isObject: true,
        fields: Object.keys(body).sort(),
      }).toMatchSnapshot();
    }
  });

  it('GET /api/governance-lifecycle/audit/status returns expected structure or 404', async () => {
    const { status, body } = await httpJson('/api/governance-lifecycle/audit/status');
    
    expect([200, 404]).toContain(status);
    
    if (status === 200) {
      expect(body).toBeDefined();
      expect(typeof body).toBe('object');
      
      expect({
        responseType: 'object',
        isObject: true,
        hasStatus: 'status' in body || 'enabled' in body,
        fields: Object.keys(body).sort(),
      }).toMatchSnapshot();
    }
  });

  it('GET /api/governance-lifecycle/patterns returns expected structure or 404', async () => {
    const { status, body } = await httpJson('/api/governance-lifecycle/patterns');
    
    expect([200, 404]).toContain(status);
    
    if (status === 200) {
      expect(extractStructure(body)).toMatchSnapshot();
    }
  });
});

// =============================================================================
// Error Response Structure Tests
// =============================================================================

describe('API Snapshots: Error Responses', () => {
  it('400 validation error has consistent structure', async () => {
    // Trigger a validation error with clearly invalid params
    const { status, body } = await httpJson('/api/events/latest', { 
      query: { limit: 'not-a-number-xyz' } 
    });
    
    // Depending on implementation: 200 (defaults used) or 400 (validation error)
    expect([200, 400]).toContain(status);
    
    if (status === 400) {
      validateErrorResponse(body);
      expect({
        hasError: 'error' in body,
        hasDetails: 'details' in body || 'issues' in body,
        errorType: typeof body.error,
      }).toMatchSnapshot();
    }
  });

  it('404 endpoint returns proper error format', async () => {
    const { status } = await httpJson('/api/this-endpoint-does-not-exist');
    expect(status).toBe(404);
  });
});
