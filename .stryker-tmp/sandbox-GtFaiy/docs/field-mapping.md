# Complete Field Mapping: Scan API → .pb.zst Files

This document provides the complete field mapping from the Canton Scan API through to the compressed protobuf files.

> **Reference**: [Scan API Documentation](https://docs.dev.sync.global)

## Key Concepts from API Documentation

### 1. Ordering and Pagination

- **`record_time`** is the PRIMARY ordering key (monotonically increasing within migration+synchronizer)
- `effective_at` indicates when the ledger action takes effect, NOT ordering
- Use `(migration_id, record_time, synchronizer_id)` for pagination/resume
- `record_time` can overlap across migrations - always include `migration_id`

### 2. Event Tree Structure

- Each transaction has `root_event_ids` listing top-level events
- Each event may have `child_event_ids` for nested events (e.g., from exercised choices)
- Events must be traversed in **preorder** using these identifiers
- Event ID format: `<update_id>:<event_index>` - canonical, do not synthesize

### 3. Event Types (API Names)

| API Type | Our Normalized Type | Notes |
|----------|-------------------|-------|
| `created_event` | `created` | Wrapped in `created_event` object |
| `archived_event` | `archived` | Wrapped in `archived_event` object |
| `exercised_event` | `exercised` | Wrapped in `exercised_event` object |

We store both `event_type_original` (API name) and `event_type` (normalized) for compatibility.

### 4. Optional Fields

Per API docs, parsers should handle missing fields gracefully:
- `workflow_id` - may be empty
- `command_id` - may be empty
- `signatories` - only on created events
- `observers` - only on created events
- `acting_parties` - only on exercised events
- `child_event_ids` - only on exercised events
- `contract_key` - only if contract has a key
- `exercise_result` - may be null

### 5. Reassignments

- Reassignments become active during rolling upgrades
- They do NOT have verdicts
- Ordering semantics may differ from transactions
- Fields: `source`, `target`, `unassign_id`, `submitter`, `counter`

---

## Data Flow Overview

```
Scan API Response
       ↓
normalizeUpdate() / normalizeEvent()  [parquet-schema.js]
       ↓
mapUpdateRecord() / mapEventRecord()  [write-binary.js]
       ↓
Protobuf Encoding                     [encoding.js + ledger.proto]
       ↓
ZSTD Compression                      [compression-worker.js]
       ↓
.pb.zst File                          [data/raw/migration=N/year=YYYY/month=MM/day=DD/]
```

---

## UPDATE Records

### Scan API Response Structure (Transaction)
```json
{
  "migration_id": 4,
  "transaction": {
    "update_id": "string",
    "synchronizer_id": "string",
    "workflow_id": "string",         // Optional - may be empty
    "command_id": "string",          // Optional - may be empty
    "offset": "string (numeric)",
    "record_time": "ISO8601",        // PRIMARY ordering key
    "effective_at": "ISO8601",       // When action takes effect
    "root_event_ids": ["string"],    // CRITICAL for tree traversal
    "events_by_id": { "eventId": {...} },
    "trace_context": { ... }
  }
}
```

### Scan API Response Structure (Reassignment)
```json
{
  "migration_id": 4,
  "reassignment": {
    "update_id": "string",
    "source": "string (synchronizer)",
    "target": "string (synchronizer)",
    "synchronizer_id": "string",
    "unassign_id": "string",
    "submitter": "string",
    "counter": "number",
    "kind": "assign | unassign",
    "record_time": "ISO8601",
    "effective_at": "ISO8601"
  },
  "event": {
    "created_event": {...} | null,
    "archived_event": {...} | null
  }
}
```

### Normalized Update (parquet-schema.js → normalizeUpdate)

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `update_id` | string | `update.update_id` | Canonical identifier |
| `update_type` | string | Derived | `'transaction'` or `'reassignment'` |
| `migration_id` | int64 | `raw.migration_id` | Required for ordering |
| `synchronizer_id` | string | `update.synchronizer_id` | Required for ordering |
| `workflow_id` | string | `update.workflow_id` | Optional - may be empty |
| `command_id` | string | `update.command_id` | Optional - may be empty |
| `offset` | int64 | `update.offset` | |
| `record_time` | timestamp | `update.record_time` | **PRIMARY ordering key** |
| `effective_at` | timestamp | `update.effective_at` | When action takes effect |
| `recorded_at` | timestamp | Current time | When we recorded it |
| `timestamp` | timestamp | Current time | |
| `kind` | string | `update.kind` | For reassignments only |
| `root_event_ids` | string[] | `update.root_event_ids` | **CRITICAL for tree traversal** |
| `event_count` | int32 | `Object.keys(events_by_id).length` | |
| `source_synchronizer` | string | `update.source` | Reassignment only |
| `target_synchronizer` | string | `update.target` | Reassignment only |
| `unassign_id` | string | `update.unassign_id` | Reassignment only |
| `submitter` | string | `update.submitter` | Reassignment only |
| `reassignment_counter` | int64 | `update.counter` | Reassignment only |
| `trace_context` | JSON string | `JSON.stringify(update.trace_context)` | |
| `update_data` | JSON string | **`JSON.stringify(update)` - FULL ORIGINAL** | Recovery source |

---

## EVENT Records

### Scan API Event Types

#### Created Event
```json
{
  "created_event": {
    "contract_id": "string",
    "template_id": "string (package:module:template)",
    "package_name": "string",        // API provides this
    "create_arguments": { ... },
    "signatories": ["string"],
    "observers": ["string"],
    "witness_parties": ["string"],
    "contract_key": { ... } | null,
    "created_at": "ISO8601"
  }
}
```

#### Archived Event
```json
{
  "archived_event": {
    "contract_id": "string",
    "template_id": "string"
    // Note: NO signatories, NO observers
  }
}
```

#### Exercised Event
```json
{
  "exercised_event": {
    "contract_id": "string",
    "template_id": "string",
    "choice": "string",
    "choice_argument": { ... },
    "acting_parties": ["string"],
    "consuming": boolean,
    "interface_id": "string" | null,
    "child_event_ids": ["string"],   // CRITICAL for tree traversal
    "exercise_result": { ... } | null
  }
}
```

### Event ID Format

Per API documentation, event IDs follow the format:
```
<update_id>:<event_index>
```

Example: `00e9ba9e64bb0316518f9e5e0ca3d08e4fadd37a8e3f8a8f47ae08a3f920d4bb4bca1211200220c5a7e4...:0`

**IMPORTANT**: Do NOT synthesize event IDs. If the API doesn't provide one, log a warning and leave it null. The raw JSON preserves the original data.

### Normalized Event (parquet-schema.js → normalizeEvent)

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `event_id` | string | `event.event_id` | **Original API value only** |
| `update_id` | string | Parent update | |
| `event_type` | string | Derived | `'created'`, `'archived'`, `'exercised'` |
| `event_type_original` | string | API type | `'created_event'`, `'archived_event'`, etc. |
| `contract_id` | string | From inner event | |
| `template_id` | string | From inner event | |
| `package_name` | string | API-provided or extracted | Prefer API value |
| `migration_id` | int64 | From context | |
| `synchronizer_id` | string | From updateInfo | |
| `effective_at` | timestamp | `created_at` or `record_time` | |
| `recorded_at` | timestamp | Current time | |
| `timestamp` | timestamp | Current time | |
| `created_at_ts` | timestamp | Same as effective_at | |
| `signatories` | string[] | Created event only | **Optional - null for other types** |
| `observers` | string[] | Created event only | **Optional - null for other types** |
| `acting_parties` | string[] | Exercised event only | **Optional - null for other types** |
| `witness_parties` | string[] | Created event only | **Optional** |
| `payload` | JSON string | `create_arguments` or `choice_argument` | |
| `contract_key` | JSON string | Created event only | **Optional** |
| `choice` | string | Exercised event only | |
| `consuming` | boolean | Exercised event only | |
| `interface_id` | string | Exercised event only | **Optional** |
| `child_event_ids` | string[] | Exercised event only | **CRITICAL for tree traversal** |
| `exercise_result` | JSON string | Exercised event only | **Optional** |
| `source_synchronizer` | string | Reassignment only | |
| `target_synchronizer` | string | Reassignment only | |
| `unassign_id` | string | Reassignment only | |
| `submitter` | string | Reassignment only | |
| `reassignment_counter` | int64 | Reassignment only | |
| `raw` | object | **COMPLETE ORIGINAL EVENT** | Recovery source |

---

## Event Tree Traversal

The API documentation emphasizes that events form a tree structure:

```
Transaction
├── root_event_ids: ["event1", "event2"]
└── events_by_id:
    ├── "event1" (exercised)
    │   └── child_event_ids: ["event3", "event4"]
    ├── "event2" (created)
    ├── "event3" (created)
    └── "event4" (archived)
```

To traverse in correct order:
1. Start with `root_event_ids`
2. For each event, process it then recurse into `child_event_ids`
3. This gives preorder traversal

Use `flattenEventsInTreeOrder()` helper function to flatten while preserving order.

---

## Critical Data Preservation Fields

### For Updates
- **`update_data_json`**: Contains the COMPLETE original `transaction` or `reassignment` object
- Even if individual field extraction has bugs, the full original data is preserved

### For Events
- **`raw_json`**: Contains the COMPLETE original event object
- Even if individual field extraction has bugs, the full original data is preserved

---

## Cursor/Resume Logic

Per API documentation, to resume ingestion:

```javascript
// Use these for pagination (v2/updates API)
const payload = {
  page_size: 100,
  after: {
    after_migration_id: lastMigrationId,
    after_record_time: lastRecordTime
  }
};
```

**DO NOT** use only `effective_at` or `last_before` - you may skip or replay updates at migration boundaries.

---

## File Naming Convention

```
{type}-{timestamp_ms}-{random_hex}.pb.zst

Examples:
- updates-1734567890123-a1b2c3d4.pb.zst
- events-1734567890456-e5f6g7h8.pb.zst
```

## Directory Structure

```
data/raw/
├── migration=1/
│   ├── year=2024/
│   │   ├── month=06/
│   │   │   ├── day=15/
│   │   │   │   ├── updates-1718467200000-abc123.pb.zst
│   │   │   │   └── events-1718467200001-def456.pb.zst
```

Migration is included in path because `record_time` can overlap across migrations.

---

## Verification Checklist

1. **Updates have `update_data_json`** - Can recover all original data
2. **Events have `raw_json`** - Can recover all original data
3. **Event IDs are original** - Not synthesized, format is `<update_id>:<index>`
4. **Tree structure preserved** - `root_event_ids` and `child_event_ids` present
5. **record_time populated** - Primary ordering key
6. **migration_id populated** - Required for ordering across migrations
7. **Optional fields handled** - No errors on missing `workflow_id`, etc.

---

## API Evolution Compatibility

Per API documentation:
- New optional fields may be added anytime
- New templates and choices may appear
- Parsers should NOT fail on unknown fields
- Always preserve raw JSON for forward compatibility
