# Data Architecture

## Overview

This document describes the data pipeline from raw ledger ingestion through derived indexes. The architecture uses **binary `.pb.zst` files** (Protobuf + Zstandard compression) as the source of truth, with derived indexes stored in DuckDB and JSON files.

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
│                        SOURCE OF TRUTH: .pb.zst FILES                        │
│                                                                              │
│  data/ledger_raw/                    data/acs/                               │
│  ├── migration=1/                    ├── migration=1/                        │
│  │   └── year=2024/                  │   └── year=2024/                      │
│  │       └── month=12/               │       └── month=12/                   │
│  │           └── day=15/             │           └── day=15/                 │
│  │               └── events-*.pb.zst │               └── *.parquet           │
│  └── updates-*.pb.zst                └── _COMPLETE                           │
│                                                                              │
│  Format: Protobuf-serialized events compressed with Zstandard               │
│  Size: ~1.8 TB compressed (35K+ files)                                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Index Build Process
                                      │ (scans .pb.zst files)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DERIVED INDEXES                                   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ TEMPLATE FILE INDEX                                                  │    │
│  │ Storage: DuckDB tables (template_file_index, template_file_state)   │    │
│  │ Purpose: Map template names → files containing that template        │    │
│  │ Build time: ~10-15 min parallel, ~70 min sequential                 │    │
│  │ Enables: Fast VoteRequest queries (scan 100 files vs 35K)           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ PARTY INDEX                                                          │    │
│  │ Storage: JSON file (data/party-index.json)                          │    │
│  │ Purpose: Map party IDs → files containing their events              │    │
│  │ Structure: partyId → [{ file, eventCount, firstSeen, lastSeen }]    │    │
│  │ Enables: O(1) lookup of party activity across backfill              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ AGGREGATIONS                                                         │    │
│  │ Storage: DuckDB tables (events_raw, aggregation_state)              │    │
│  │ Purpose: Pre-computed counts and summaries                          │    │
│  │ Tracks: Last processed file_id for incremental updates             │    │
│  │ Provides: Event type counts, template counts, time ranges           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           QUERY LAYER (DuckDB)                               │
│                                                                              │
│  server/duckdb/connection.js - Connection pool with retry logic              │
│  server/duckdb/binary-reader.js - Reads and decompresses .pb.zst files      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API LAYER                                       │
│                                                                              │
│  server/api/*.js - Express routes                                            │
│  Port 3001                                                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           REACT FRONTEND                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Index Details

### 1. Template File Index

**Purpose**: Dramatically speed up template-specific queries by scanning only relevant files.

**Source**: `server/engine/template-file-index.js`

**Storage Location**: DuckDB tables
- `template_file_index` - Maps (file_path, template_name) → metadata
- `template_file_index_state` - Tracks build progress and last indexed file

**Schema**:
```sql
CREATE TABLE template_file_index (
  file_path VARCHAR NOT NULL,
  template_name VARCHAR NOT NULL,
  event_count INTEGER DEFAULT 0,
  first_event_at TIMESTAMP,
  last_event_at TIMESTAMP,
  PRIMARY KEY (file_path, template_name)
);
```

**Build Process**:
1. Scan all `.pb.zst` files in `DATA_PATH`
2. Decompress and parse each file
3. Extract unique template names from events
4. Insert mappings into DuckDB with event counts and timestamps
5. Supports incremental updates (only indexes new files)

**Usage Example**:
```javascript
// Get all files containing VoteRequest events
const files = await getFilesForTemplate('VoteRequest');
// Returns: ['/data/ledger_raw/migration=1/.../events-001.pb.zst', ...]
```

---

### 2. Party Index

**Purpose**: Enable O(1) lookup of which files contain a party's events.

**Source**: `server/engine/party-indexer.js`

**Storage Location**: JSON files on disk
- `data/party-index.json` - Main index
- `data/party-index-state.json` - Build state tracking

**Structure**:
```json
{
  "party::12345abcdef...": [
    {
      "file": "/data/ledger_raw/.../events-001.pb.zst",
      "eventCount": 42,
      "firstSeen": "2024-01-15T10:30:00Z",
      "lastSeen": "2024-01-15T14:22:00Z"
    }
  ]
}
```

**Build Process**:
1. Scan all `.pb.zst` event files
2. Extract signatories and observers from each event
3. Build mapping: partyId → list of files with metadata
4. Save checkpoints every 500 files
5. Store final index as JSON

**Usage Example**:
```javascript
// Get files containing a party's events
const fileInfos = getFilesForParty('party::alice123...');
// Returns: [{ file, eventCount, firstSeen, lastSeen }, ...]
```

---

### 3. Aggregations

**Purpose**: Pre-computed statistics for fast dashboard queries.

**Source**: `server/engine/aggregations.js`

**Storage Location**: DuckDB tables
- `events_raw` - Ingested raw events
- `aggregation_state` - Tracks last processed file per aggregation

**Schema**:
```sql
CREATE TABLE aggregation_state (
  agg_name VARCHAR PRIMARY KEY,
  last_file_id INTEGER,
  last_updated TIMESTAMP
);
```

**Aggregations Computed**:
| Name | Description |
|------|-------------|
| `event_type_counts` | Count of events by type (created, archived, etc.) |
| `template_counts` | Count of events by template |
| `time_range` | MIN/MAX timestamps in dataset |
| `total_counts` | Total events and updates |

**Incremental Update Process**:
1. Check `last_file_id` for aggregation
2. Query events where `_file_id > last_file_id`
3. Compute new aggregates
4. Update `aggregation_state` with new `last_file_id`

---

## File Formats

### Binary Files (.pb.zst)

**Location**: `data/ledger_raw/`

**Format**: Protobuf messages compressed with Zstandard

**Schema**: Defined in `scripts/ingest/schema/ledger.proto`

**Reader**: `server/duckdb/binary-reader.js`

```javascript
import { readBinaryFile } from '../duckdb/binary-reader.js';

const result = await readBinaryFile('/path/to/events-001.pb.zst');
// result.records = [{ template, type, signatories, observers, payload, ... }]
```

### Parquet Files (Optional)

**Location**: `data/parquet/` and `data/acs/`

**Purpose**: 
- Analytics and ad-hoc SQL queries
- BigQuery/external tool compatibility
- ACS (Active Contract Set) snapshots

**Generation**: `scripts/ingest/materialize-parquet.js`

---

## Index Build Commands

```bash
# Build template file index (incremental by default)
curl http://localhost:3001/api/index/templates/build

# Force rebuild template index
curl http://localhost:3001/api/index/templates/build?force=true

# Build party index
curl http://localhost:3001/api/index/party/build

# Check index status
curl http://localhost:3001/api/index/status
```

---

## Performance Characteristics

| Index | Build Time | Storage Size | Query Speedup |
|-------|------------|--------------|---------------|
| Template File Index | 10-15 min (parallel) | ~50 MB in DuckDB | 100-350x for template queries |
| Party Index | 30-60 min | ~200 MB JSON | O(n) → O(1) for party lookups |
| Aggregations | Incremental (seconds) | ~10 MB | Pre-computed, instant |

---

## Design Decisions

### Why .pb.zst as Source of Truth?

1. **Compact**: Protobuf + Zstd achieves ~10:1 compression
2. **Fast writes**: Append-only, no index updates during ingestion
3. **Immutable**: Files never modified, only appended
4. **Portable**: Self-contained, easy to backup/restore

### Why Not Parquet as Primary?

1. **Write overhead**: Parquet requires sorting and statistics computation
2. **Schema evolution**: Protobuf handles schema changes more gracefully
3. **Incremental ingestion**: Binary files support faster streaming writes

### Why Derived Indexes?

1. **Query performance**: Avoid full scans of 35K+ files
2. **Separation of concerns**: Ingestion is fast; indexing can run async
3. **Flexibility**: Different indexes for different query patterns
