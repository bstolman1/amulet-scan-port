# Data Architecture

## Overview

This document describes the data pipeline from raw ledger ingestion through API queries. The architecture uses **Parquet files** as the sole authoritative data source, queried via DuckDB.

---

## ⚠️ DATA AUTHORITY CONTRACT

> **Parquet files produced by ledger ingestion are the sole authoritative data source.**
>
> All governance, rewards, party state, and analytics **must** be derived via DuckDB SQL queries
> over Parquet files. Legacy binary formats (JSONL, PBZST) are **deprecated** and must not
> be read by API routes or business logic.
>
> This is not documentation fluff — it's a contract with future you (and future collaborators).

### Enforcement

| Directory | Allowed Operations |
|-----------|-------------------|
| `server/api/` | DuckDB queries over Parquet **only** |
| `server/engine/` | DuckDB analytical queries **only** |
| `scripts/ingest/` | Write-only (produces Parquet) |
| `scripts/export/` | JSONL/PBZST writers (export-only, never imported by API) |

**CI enforces this** via `data-authority-check` job in `.github/workflows/test.yml`.

### Guardrail Tests

The `server/test/guardrails/data-authority.test.js` file enforces:

1. **No binary reader imports** in `server/api/` files
2. **No `.pb.zst` references** in API code
3. **No `decodeFile` or `binaryReader` usage** in query paths

Violations cause CI failures. No exceptions.

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CANTON NETWORK API                                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                     ┌────────────────┼────────────────┐
                     ▼                ▼                ▼
              ┌───────────┐    ┌───────────┐    ┌───────────┐
              │ fetch-    │    │ fetch-    │    │ fetch-    │
              │ updates   │    │ backfill  │    │ acs       │
              │ (live)    │    │ (history) │    │ (state)   │
              └───────────┘    └───────────┘    └───────────┘
                     │                │                │
                     ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SOURCE OF TRUTH: PARQUET FILES                            │
│                                                                              │
│  data/raw/                              data/acs/                            │
│  ├── migration=1/                       ├── migration=1/                     │
│  │   └── year=2024/                     │   └── year=2024/                   │
│  │       └── month=12/                  │       └── month=12/                │
│  │           └── day=15/                │           └── day=15/              │
│  │               ├── events-*.parquet   │               └── *.parquet        │
│  │               └── updates-*.parquet  └── _COMPLETE                        │
│  │                                                                           │
│  Compression: ZSTD (Parquet internal)                                        │
│  Size: ~1.8 TB compressed (optimized columnar format)                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Direct SQL Queries
                                      │ (no intermediate indexing)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           QUERY LAYER (DuckDB)                               │
│                                                                              │
│  server/duckdb/connection.js                                                 │
│  ├── safeQuery()      - Parameterized query execution                        │
│  ├── getEventsSource() - Returns Parquet glob for events                     │
│  ├── hasFileType()    - Check for Parquet/JSONL availability                 │
│  └── DATA_PATH        - Base path for data files                             │
│                                                                              │
│  Features:                                                                   │
│  • Connection pooling with retry logic                                       │
│  • Automatic BigInt → Number conversion                                      │
│  • Union by name for schema evolution                                        │
│  • Glob patterns for partitioned data                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API LAYER                                       │
│                                                                              │
│  server/api/*.js - Express routes                                            │
│  ├── events.js     - Event queries (created, archived, exercised)            │
│  ├── party.js      - Party activity and summaries                            │
│  ├── contracts.js  - Contract lifecycle queries                              │
│  ├── stats.js      - Dashboard statistics                                    │
│  ├── search.js     - Full-text and filtered search                           │
│  ├── rewards.js    - Reward calculations (DuckDB only)                       │
│  ├── backfill.js   - Backfill progress and cursors                           │
│  └── governance-lifecycle.js - Proposal tracking                             │
│                                                                              │
│  Port: 3001                                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           REACT FRONTEND                                     │
│                                                                              │
│  src/hooks/use-*.ts - React Query hooks for API calls                        │
│  src/lib/api-client.ts - Typed API client                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## DuckDB Query Patterns

### Basic Event Query

```javascript
import db from '../duckdb/connection.js';

const events = await db.safeQuery(`
  SELECT * FROM ${db.getEventsSource()}
  WHERE template_name = $1
  ORDER BY record_time DESC
  LIMIT $2
`, [templateName, limit]);
```

### Aggregation Query

```javascript
const stats = await db.safeQuery(`
  SELECT 
    template_name,
    COUNT(*) as event_count,
    MIN(record_time) as first_seen,
    MAX(record_time) as last_seen
  FROM ${db.getEventsSource()}
  GROUP BY template_name
  ORDER BY event_count DESC
`);
```

### Direct Parquet Query (when needed)

```javascript
const basePath = db.DATA_PATH.replace(/\\/g, '/');

const result = await db.safeQuery(`
  SELECT COUNT(*) as count 
  FROM read_parquet('${basePath}/**/events-*.parquet', union_by_name=true)
  WHERE migration_id = $1
`, [migrationId]);
```

---

## File Formats

### Parquet Files (Primary)

**Location**: `data/raw/` and `data/acs/`

**Format**: Apache Parquet with ZSTD compression

**Partitioning**: Hive-style (`migration=X/year=YYYY/month=MM/day=DD/`)

**Schema**: Defined by ingestion scripts, includes:
- `event_id` - Unique event identifier
- `template_name` - DAML template name
- `event_type` - created, archived, exercised
- `contract_id` - Contract identifier
- `signatories` - List of signing parties
- `observers` - List of observing parties
- `payload` - JSON payload
- `record_time` - Event timestamp
- `migration_id` - Network migration ID

**Query Pattern**:
```sql
SELECT * FROM read_parquet('data/raw/**/events-*.parquet', union_by_name=true)
```

### JSONL Files (Fallback)

**Location**: `data/jsonl/`

**Purpose**: Compatibility layer for environments without Parquet support

**Query Pattern**:
```sql
SELECT * FROM read_json_auto('data/jsonl/events-*.jsonl')
```

---

## Aggregations

### Pre-computed Statistics

The `server/engine/aggregations.js` module provides:

| Function | Description |
|----------|-------------|
| `getTotalCounts()` | Total events and updates |
| `getTimeRange()` | MIN/MAX timestamps |
| `getTemplateEventCounts()` | Events per template |
| `getEventTypeCounts()` | Events by type |

All aggregations query Parquet files directly via DuckDB.

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Cold query (first) | 100-500ms |
| Warm query (cached) | 10-50ms |
| Full table scan | 2-10s (depends on data size) |
| Template-filtered | 50-200ms |
| Party-filtered | 100-500ms |

DuckDB automatically caches metadata and uses column pruning for efficient queries.

---

## Migration from Binary Format

The codebase has been migrated from `.pb.zst` (Protobuf + Zstandard) to Parquet:

### What Changed

1. **Removed**: `server/duckdb/binary-reader.js`
2. **Removed**: `server/engine/template-file-index.js` (no longer needed)
3. **Updated**: All `server/api/*.js` to use `db.safeQuery()` with Parquet sources
4. **Added**: Guardrail tests to prevent regression

### Why Parquet?

1. **Direct SQL**: No decompression/parsing step - DuckDB reads Parquet natively
2. **Column pruning**: Only reads columns needed for query
3. **Predicate pushdown**: Filters applied at file level
4. **Schema evolution**: `union_by_name=true` handles schema changes
5. **BigQuery ready**: Parquet files upload directly to GCS/BigQuery

### Legacy Binary Files

If you have existing `.pb.zst` files, convert them using:

```bash
node scripts/ingest/materialize-parquet.js
```

The API will not read binary files - they must be converted to Parquet first.

---

## Design Decisions

### Why Parquet as Sole Source of Truth?

1. **Query performance**: Native DuckDB support, no parsing overhead
2. **Compression**: ZSTD compression built into Parquet
3. **Portability**: Industry standard, works with BigQuery, Spark, Pandas
4. **Column store**: Efficient for analytical queries
5. **Metadata**: Statistics enable query optimization

### Why Not Keep Binary Format?

1. **Complexity**: Maintaining two code paths (binary + Parquet) is error-prone
2. **Performance**: Binary requires decompression + parsing on every query
3. **Tooling**: Parquet has better ecosystem support
4. **Debugging**: Can inspect Parquet with standard tools (DuckDB CLI, Pandas)

### Data Authority Contract Benefits

1. **Single source of truth**: No ambiguity about which format is canonical
2. **CI enforcement**: Guardrail tests catch violations
3. **Future-proofing**: Clear contract for all contributors
4. **Simplified debugging**: Always know where data comes from
