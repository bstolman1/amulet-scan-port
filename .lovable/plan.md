

# Fix Scan API Proxy - Method-Preserving Corrections

## Problem Summary

The scan-proxy system has **two critical bugs** that cause requests to fail or stall:

1. **Health Check Uses Wrong HTTP Method** - The `checkAllEndpoints()` function in `endpoint-rotation.js` sends `POST` requests to `/v0/dso`, but this is a **GET-only endpoint**. This causes health checks to fail, marking all endpoints as unhealthy.

2. **fetchWithFailover Forces Content-Type on GET** - The `fetchWithFailover()` function always sets `Content-Type: application/json` headers, even for GET requests. This can cause issues with GET-only endpoints.

---

## Root Cause Analysis

### Issue 1: Health Check (Lines 240-276 in endpoint-rotation.js)

```text
Current (BROKEN):
  fetch(`${endpoint.url}/v0/dso`, {
    method: 'POST',           // ❌ Wrong - /v0/dso is GET-only
    body: JSON.stringify({}), // ❌ Wrong - GET endpoints reject bodies
  })

Should be:
  fetch(`${endpoint.url}/v0/dso`, {
    method: 'GET',            // ✅ Correct
    // No body                // ✅ Correct
  })
```

### Issue 2: fetchWithFailover (Lines 149-197 in endpoint-rotation.js)

The function always injects headers for JSON regardless of method. While `scan-proxy.js` is now correct, if `fetchWithFailover` is used elsewhere, it could cause issues.

---

## Implementation Plan

### Step 1: Fix checkAllEndpoints() Health Check

**File:** `server/lib/endpoint-rotation.js`

**Change:** Update the health check to use GET method without a body.

```javascript
// BEFORE (lines 246-254):
const response = await fetch(`${endpoint.url}/v0/dso`, {
  method: 'POST',
  headers: { 
    'Content-Type': 'application/json',
    'Accept': 'application/json' 
  },
  body: JSON.stringify({}),
  signal: AbortSignal.timeout(10000),
});

// AFTER:
const response = await fetch(`${endpoint.url}/v0/dso`, {
  method: 'GET',
  headers: { 
    'Accept': 'application/json' 
  },
  signal: AbortSignal.timeout(10000),
});
```

### Step 2: Fix fetchWithFailover() to be Method-Aware

**File:** `server/lib/endpoint-rotation.js`

**Change:** Only set `Content-Type` header for methods that send bodies.

```javascript
// BEFORE (lines 158-166):
const response = await fetch(url, {
  ...options,
  headers: {
    'Content-Type': 'application/json',  // ❌ Always set
    'Accept': 'application/json',
    ...options.headers,
  },
  signal: options.signal || AbortSignal.timeout(30000),
});

// AFTER:
const method = options.method?.toUpperCase() || 'GET';
const headers = {
  'Accept': 'application/json',
  ...options.headers,
};

// Only set Content-Type for methods that have bodies
if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
  headers['Content-Type'] = 'application/json';
}

const response = await fetch(url, {
  ...options,
  method,
  headers,
  signal: options.signal || AbortSignal.timeout(30000),
});
```

### Step 3: Update Inline Comment

**File:** `server/lib/endpoint-rotation.js`

**Change:** Fix the misleading comment at line 246.

```javascript
// BEFORE:
// Scan API requires POST requests with application/json

// AFTER:
// /v0/dso is a GET-only endpoint for health checks
```

---

## Technical Details

| File | Lines Changed | Description |
|------|---------------|-------------|
| `server/lib/endpoint-rotation.js` | 246-254 | Change health check from POST to GET |
| `server/lib/endpoint-rotation.js` | 158-166 | Make fetchWithFailover method-aware |

---

## Verification Steps

After deployment, run these commands on your VM:

```bash
# 1. Restart the backend
pm2 restart all

# 2. Test GET endpoint (should return DSO state)
curl http://34.56.191.157/api/scan-proxy/v0/dso

# 3. Trigger health check and verify endpoints are healthy
curl -X POST http://34.56.191.157/api/scan-proxy/_health/check

# 4. Check health status
curl http://34.56.191.157/api/scan-proxy/_health

# 5. Test POST endpoint (should return updates)
curl -X POST http://34.56.191.157/api/scan-proxy/v2/updates \
  -H 'Content-Type: application/json' \
  -d '{"page_size": 5}'
```

**Expected results:**
- Step 2: Returns JSON with DSO/SV state data
- Step 3: Returns all endpoints as `healthy: true`
- Step 4: Shows current endpoint and health stats
- Step 5: Returns ledger updates array

