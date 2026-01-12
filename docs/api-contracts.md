# API Contracts

Formal response shape contracts for the Amulet Scan REST API. These contracts are enforced by snapshot tests in `server/test/integration/api-snapshots.test.js`.

> **Contract Philosophy**: These tests lock down response *structure*, not exact values. Dynamic data (timestamps, IDs, counts) is normalized. Breaking changes require explicit snapshot updates.

---

## Table of Contents

- [Contract Conventions](#contract-conventions)
- [Core Endpoints](#core-endpoints)
- [Events API](#events-api)
- [Stats API](#stats-api)
- [Governance Lifecycle API](#governance-lifecycle-api)
- [Error Responses](#error-responses)

---

## Contract Conventions

### Response Envelope

All paginated endpoints return a standard envelope:

```typescript
interface PaginatedResponse<T> {
  data: T[];
  count: number;
  hasMore?: boolean;
  offset?: number;
  source?: DataSource;
}

type DataSource = 'engine' | 'binary' | 'jsonl' | 'parquet' | 'empty';
```

### Empty Data Handling

When a query returns no results:
- `data` is an empty array `[]`
- `count` is `0`
- Snapshot tests record this as `itemShape: "[EMPTY]"` for explicit verification

### Normalized Values in Snapshots

Snapshot tests replace dynamic values with placeholders:

| Pattern | Placeholder | Example |
|---------|-------------|---------|
| ISO timestamps | `[TIMESTAMP]` | `2024-01-15T10:30:00.000Z` → `[TIMESTAMP]` |
| Hex IDs (16+ chars) | `[ID]` | `00abc123def456...` → `[ID]` |
| UUIDs with dashes | `[UUID]` | `a1b2c3d4-e5f6-...` → `[UUID]` |

---

## Core Endpoints

### GET /health

Health check for load balancers and monitoring.

**Contract:**
```typescript
interface HealthResponse {
  status: 'ok';
  timestamp: string; // ISO 8601
}
```

**Snapshot Assertion:**
```javascript
{
  fields: ['status', 'timestamp'],
  statusValue: 'ok'
}
```

### GET /

API information and available endpoints.

**Contract:**
```typescript
interface RootResponse {
  name: string;
  version: string;
  status: 'ok';
  engine?: 'enabled' | 'disabled';
  endpoints?: string[];
  dataPath?: string;
}
```

**Snapshot Assertion:**
```javascript
{
  fields: ['dataPath', 'endpoints', 'engine', 'name', 'status', 'version'],
  hasEngine: true
}
```

---

## Events API

### GET /api/events/latest

Most recent ledger events with pagination.

**Contract:**
```typescript
interface EventsLatestResponse {
  data: LedgerEvent[];
  count: number;
  source?: DataSource;
  hasMore?: boolean;
  offset?: number;
}

interface LedgerEvent {
  update_id: number;
  event_id: string;
  event_type: 'created' | 'archived' | 'exercised';
  contract_id: string;
  template_id: string;
  effective_at: string;
  signatories?: string[];
  observers?: string[];
  payload?: Record<string, unknown>;
  migration_id?: number;
  synchronizer_id?: string;
}
```

**Required Fields in Event:**
- `update_id` (bigint)
- `event_id` (string)
- `template_id` (string)
- `effective_at` (timestamp)
- `migration_id` (bigint)
- `synchronizer_id` (string)

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  hasCount: true,
  hasSource: true,
  hasMore: true,
  dataIsArray: true,
  dataShape: [
    'contract_id', 'effective_at', 'event_id', 'event_type',
    'migration_id', 'payload', 'signatories', 'synchronizer_id',
    'template_id', 'update_id'
  ]
}
```

### GET /api/events/by-type/:type

Events filtered by event type.

**Contract:**
```typescript
interface EventsByTypeResponse {
  data: LedgerEvent[];
  count?: number;
}
// OR direct array:
type EventsByTypeResponse = LedgerEvent[];
```

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  responseType: 'array' | 'object'
}
```

### GET /api/events/by-template/:templateId

Events filtered by template (partial match supported).

**Contract:**
```typescript
interface EventsByTemplateResponse {
  data: LedgerEvent[];
  count?: number;
}
```

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  responseType: 'object'
}
```

### GET /api/events/count

Total event count with source metadata.

**Contract:**
```typescript
interface EventCountResponse {
  count: number;
  estimated?: boolean;
  fileCount?: number;
  source: DataSource;
}
```

**Snapshot Assertion:**
```javascript
{
  hasCount: true,
  hasEstimated: true,
  hasSource: true,
  fields: ['count', 'estimated', 'fileCount', 'source']
}
```

### GET /api/events/debug

Debug information for event engine.

**Contract:**
```typescript
interface EventDebugResponse {
  engineStatus?: Record<string, unknown>;
  fileIndex?: Record<string, unknown>;
  lastCycle?: string;
  // Structure varies based on engine state
}
```

**Snapshot Assertion:**
```javascript
{
  responseType: 'object',
  isObject: true,
  topLevelKeys: [...] // Dynamic based on engine state
}
```

### GET /api/events/governance

Governance-related events (VoteRequest, Confirmation, DsoRules).

**Contract:**
```typescript
interface GovernanceEventsResponse {
  data: LedgerEvent[];
  count?: number;
}
// OR direct array for compatibility
```

**Snapshot Assertion:**
```javascript
{
  responseType: 'array' | 'object',
  hasData: boolean,
  topLevelKeys: []
}
```

---

## Stats API

### GET /api/stats/overview

Dashboard overview statistics.

**Contract:**
```typescript
interface StatsOverviewResponse {
  total_events: number;
  unique_contracts?: number;
  unique_templates?: number;
  earliest_event?: string;
  latest_event?: string;
  data_source: DataSource;
}
```

**Validation:**
- `data_source` must be one of: `'engine' | 'binary' | 'jsonl' | 'parquet' | 'empty'`

**Snapshot Assertion:**
```javascript
{
  fields: ['data_source', 'earliest_event', 'latest_event', 'total_events', 'unique_contracts', 'unique_templates'],
  hasTotalEvents: true,
  hasDataSource: true,
  validSource: true
}
```

### GET /api/stats/daily

Daily event counts for trend analysis.

**Contract:**
```typescript
interface StatsDailyResponse {
  data: DailyStats[];
}

interface DailyStats {
  date: string; // YYYY-MM-DD
  event_count: number;
  contract_count?: number;
}
```

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  isArray: true,
  itemShape: ['contract_count', 'date', 'event_count'] // or '[EMPTY]'
}
```

### GET /api/stats/by-type

Event counts grouped by event type.

**Contract:**
```typescript
interface StatsByTypeResponse {
  data: TypeStats[];
}

interface TypeStats {
  event_type: string;
  count: number;
}
```

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  isArray: true,
  itemShape: ['count', 'event_type'] // or '[EMPTY]'
}
```

### GET /api/stats/by-template

Event counts grouped by template.

**Contract:**
```typescript
interface StatsByTemplateResponse {
  data: TemplateStats[];
}

interface TemplateStats {
  template_id: string;
  event_count: number;
  contract_count?: number;
  first_seen?: string;
  last_seen?: string;
}
```

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  isArray: true,
  dataLength: number, // Actual count for observability
  itemShape: ['contract_count', 'event_count', 'first_seen', 'last_seen', 'template_id']
}
```

### GET /api/stats/hourly

Hourly activity for the last 24 hours.

**Contract:**
```typescript
interface StatsHourlyResponse {
  data: HourlyStats[];
}

interface HourlyStats {
  hour: string; // ISO timestamp
  event_count: number;
}
```

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  isArray: true,
  itemShape: ['event_count', 'hour'] // or '[EMPTY]'
}
```

### GET /api/stats/burn

Token burn statistics.

**Contract:**
```typescript
interface StatsBurnResponse {
  data: BurnStats[];
}

interface BurnStats {
  date?: string;
  burn_amount?: number;
  burn_count?: number;
}
```

**Snapshot Assertion:**
```javascript
{
  hasData: true,
  isArray: true,
  itemShape: [...] // or '[EMPTY]'
}
```

### GET /api/stats/sources

Data source information.

**Contract:**
```typescript
interface StatsSourcesResponse {
  sources?: string[];
  primary_source?: DataSource;
  // Additional metadata varies
}
```

**Snapshot Assertion:**
```javascript
{
  fields: [...],
  hasSources: true
}
```

---

## Governance Lifecycle API

### GET /api/governance-lifecycle

Governance proposal lifecycle data.

**Contract:**
```typescript
interface GovernanceLifecycleResponse {
  proposals?: Proposal[];
  data?: Proposal[];
  total?: number;
  count?: number;
  hasMore?: boolean;
}
// OR direct array for compatibility

interface Proposal {
  id: string;
  action: string;
  status: 'pending' | 'accepted' | 'rejected' | 'executed' | 'expired';
  created_at: string;
  votes?: VoteSummary;
}

interface VoteSummary {
  accept: number;
  reject: number;
}
```

**Snapshot Assertion:**
```javascript
{
  responseType: 'array' | 'object',
  hasProposals: boolean,
  topLevelKeys: ['count', 'data', 'hasMore'] // varies
}
```

### GET /api/governance-lifecycle/proposals

Direct proposals endpoint (may return 404 if not implemented).

**Contract:** Same as main endpoint, returns `200` or `404`.

### GET /api/governance-lifecycle/stats

Governance statistics summary.

**Contract:**
```typescript
interface GovernanceStatsResponse {
  total_proposals?: number;
  pending?: number;
  accepted?: number;
  rejected?: number;
  executed?: number;
  // Additional stats vary
}
```

**Snapshot Assertion (if 200):**
```javascript
{
  responseType: 'object',
  isObject: true,
  fields: [...]
}
```

### GET /api/governance-lifecycle/audit/status

Audit system status.

**Contract:**
```typescript
interface AuditStatusResponse {
  status?: string;
  enabled?: boolean;
  // Additional status fields vary
}
```

### GET /api/governance-lifecycle/patterns

Governance voting patterns analysis.

**Contract:** Returns structured pattern data or 404.

---

## Error Responses

### Standard Error Envelope

All 4xx and 5xx responses follow this structure:

```typescript
interface ErrorResponse {
  error: string;
  message?: string;
  details?: string | ValidationIssue[];
  issues?: ValidationIssue[];
}

interface ValidationIssue {
  path: string[];
  message: string;
  code?: string;
}
```

### 400 Validation Error

Returned when request parameters fail validation.

**Contract:**
```typescript
interface ValidationErrorResponse {
  error: 'Validation failed' | string;
  details?: ValidationIssue[];
  issues?: ValidationIssue[];
}
```

**Snapshot Assertion:**
```javascript
{
  hasError: true,
  hasDetails: true,
  errorType: 'string'
}
```

### 404 Not Found

Returned when endpoint or resource doesn't exist.

**Contract:**
```typescript
interface NotFoundResponse {
  error: 'Not found' | string;
  path?: string;
}
```

### 500 Internal Server Error

Returned on unexpected server errors.

**Contract:**
```typescript
interface ServerErrorResponse {
  error: string;
  message?: string;
}
```

**Note:** Snapshot tests FAIL on 500 errors unless explicitly handling known edge cases.

---

## Contract Enforcement

### Snapshot Test Behavior

1. **Strict Success**: Most endpoints require `status === 200`
2. **Descriptive Failures**: Failed assertions include status and response body
3. **Empty Data Explicit**: Empty arrays are marked as `[EMPTY]` in snapshots
4. **Valid Source Check**: `data_source` field is validated against allowed values

### Updating Contracts

When API changes are intentional:

```bash
# Update all snapshots
npx vitest -u

# Update specific test file
npx vitest -u server/test/integration/api-snapshots.test.js
```

### CI Integration

Snapshot tests run in CI. Unexpected changes will fail the build. To accept changes:

1. Review the diff carefully
2. Ensure changes are intentional
3. Update snapshots locally
4. Commit the updated snapshots

---

## Version History

| Date | Change | Migration |
|------|--------|-----------|
| 2025-01-12 | Added `validSource` to stats/overview | Update snapshots |
| 2025-01-12 | Empty arrays report `[EMPTY]` in itemShape | Update snapshots |
| 2025-01-12 | Added `dataLength` to by-template | Update snapshots |
| 2025-01-12 | Added `update_id`, `migration_id`, `synchronizer_id` to events | Update test fixtures |
