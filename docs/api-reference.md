# API Reference

Complete reference for the Amulet Scan REST API. Default base URL: `http://localhost:3001`

## Table of Contents

- [Health & Status](#health--status)
- [Events](#events)
- [Active Contract Set (ACS)](#active-contract-set-acs)
- [Statistics](#statistics)
- [Governance](#governance)
- [Party](#party)
- [Search](#search)
- [Engine](#engine)
- [Backfill](#backfill)

---

## Health & Status

### GET /
Returns API information and available endpoints.

**Response:**
```json
{
  "name": "Amulet Scan DuckDB API",
  "version": "1.0.0",
  "status": "ok",
  "engine": "enabled",
  "endpoints": ["GET /health", "GET /api/events/latest", ...],
  "dataPath": "/path/to/data"
}
```

### GET /health
Quick health check (no engine status).

**Response:**
```json
{
  "status": "ok",
  "timestamp": "2024-01-15T10:30:00.000Z"
}
```

### GET /health/detailed
Detailed health check with engine and cache status.

**Response:**
```json
{
  "status": "ok",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "engine": "enabled",
  "engineStatus": {
    "running": true,
    "lastCycle": "2024-01-15T10:25:00.000Z"
  },
  "cache": {
    "entries": 42
  }
}
```

---

## Events

### GET /api/events/latest
Get the most recent ledger events.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 100 | Max results (1-1000) |
| `offset` | number | 0 | Pagination offset |

**Response:**
```json
{
  "data": [
    {
      "event_id": "evt_123",
      "event_type": "created",
      "contract_id": "00abc...",
      "template_id": "Splice:Amulet:Amulet",
      "effective_at": "2024-01-15T10:30:00.000Z",
      "signatories": ["party::alice"],
      "payload": { ... }
    }
  ],
  "count": 100,
  "source": "binary"
}
```

### GET /api/events/by-type/:type
Get events by event type (created, archived, etc.).

**URL Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `type` | string | Event type (created, archived, exercised) |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 100 | Max results |
| `offset` | number | 0 | Pagination offset |

### GET /api/events/by-template/:templateId
Get events by template (partial match supported).

**URL Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `templateId` | string | Template name or partial match |

**Example:**
```
GET /api/events/by-template/VoteRequest
GET /api/events/by-template/Amulet
```

### GET /api/events/by-date
Get events within a date range.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `start` | string | Start timestamp (ISO 8601) |
| `end` | string | End timestamp (ISO 8601) |
| `limit` | number | Max results |

### GET /api/events/count
Get total event count.

**Response:**
```json
{
  "count": 15000000,
  "estimated": true,
  "fileCount": 35000,
  "source": "binary"
}
```

### GET /api/events/governance
Get governance-related events (VoteRequest, Confirmation, DsoRules, etc.).

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 200 | Max results |
| `offset` | number | 0 | Pagination offset |

### GET /api/events/vote-requests
Query VoteRequest contracts with filtering.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `status` | string | Filter: "pending", "accepted", "rejected", "all" |
| `requester` | string | Filter by requester party |
| `limit` | number | Max results |
| `verbose` | boolean | Include full payload |

**Response:**
```json
{
  "data": [
    {
      "contract_id": "00abc...",
      "action_type": "ARC_DsoRules",
      "action_name": "SetSynchronizerFeesConfig",
      "requester": "party::sv1",
      "effective_at": "2024-01-15T10:30:00.000Z",
      "expires_at": "2024-01-17T10:30:00.000Z",
      "votes": [
        { "sv": "sv1", "vote": "accept", "reason": "" }
      ]
    }
  ],
  "count": 50,
  "indexed": true
}
```

---

## Active Contract Set (ACS)

### GET /api/acs/latest
Get the latest ACS snapshot information.

**Response:**
```json
{
  "snapshot": {
    "migrationId": 1,
    "snapshotTime": "2024-01-15T12:00:00.000Z",
    "isComplete": true
  },
  "stats": {
    "totalContracts": 45000,
    "uniqueTemplates": 120
  }
}
```

### GET /api/acs/snapshots
List all available ACS snapshots.

**Response:**
```json
{
  "data": [
    {
      "migrationId": 1,
      "snapshotTime": "2024-01-15T12:00:00.000Z",
      "isComplete": true,
      "format": "parquet"
    }
  ]
}
```

### GET /api/acs/contracts
Get contracts by template from the latest ACS snapshot.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `template` | string | Template suffix (e.g., "Amulet", "LockedAmulet") |
| `limit` | number | Max results (default: 1000) |

**Response:**
```json
{
  "data": [
    {
      "contract_id": "00abc...",
      "template_id": "Splice:Amulet:Amulet",
      "payload": { ... },
      "created_at": "2024-01-10T08:00:00.000Z"
    }
  ],
  "count": 1000,
  "snapshot": {
    "migrationId": 1,
    "snapshotTime": "2024-01-15T12:00:00.000Z"
  }
}
```

### GET /api/acs/supply
Get current token supply from ACS.

**Response:**
```json
{
  "total_supply": 15000000.50,
  "locked_supply": 2500000.00,
  "circulating_supply": 12500000.50,
  "snapshot_time": "2024-01-15T12:00:00.000Z"
}
```

### GET /api/acs/rich-list
Get top token holders.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 100 | Max results |

**Response:**
```json
{
  "data": [
    {
      "party": "party::alice",
      "balance": 500000.00,
      "locked": 50000.00,
      "total": 550000.00,
      "rank": 1
    }
  ]
}
```

### GET /api/acs/templates
Get template statistics from ACS.

**Response:**
```json
{
  "data": [
    {
      "template_id": "Splice:Amulet:Amulet",
      "contract_count": 25000,
      "percentage": 55.5
    }
  ]
}
```

---

## Statistics

### GET /api/stats/overview
Get dashboard overview statistics.

**Response:**
```json
{
  "total_events": 15000000,
  "unique_contracts": 100000,
  "unique_templates": 120,
  "earliest_event": "2023-01-01T00:00:00.000Z",
  "latest_event": "2024-01-15T10:30:00.000Z",
  "data_source": "engine"
}
```

### GET /api/stats/daily
Get daily event counts.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `days` | number | 30 | Number of days (max 365) |

**Response:**
```json
{
  "data": [
    {
      "date": "2024-01-15",
      "event_count": 50000,
      "contract_count": 5000
    }
  ]
}
```

### GET /api/stats/by-template
Get event counts by template.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 50 | Max results |

**Response:**
```json
{
  "data": [
    {
      "template_id": "Splice:Amulet:Amulet",
      "event_count": 500000,
      "contract_count": 25000,
      "first_seen": "2023-01-01T00:00:00.000Z",
      "last_seen": "2024-01-15T10:30:00.000Z"
    }
  ]
}
```

### GET /api/stats/by-type
Get event counts by event type.

### GET /api/stats/hourly
Get hourly activity for the last 24 hours.

### GET /api/stats/burn
Get burn statistics.

---

## Governance

### GET /api/governance-lifecycle
Get governance proposal lifecycle data.

**Response:**
```json
{
  "proposals": [
    {
      "id": "proposal_123",
      "action": "SetSynchronizerFeesConfig",
      "status": "executed",
      "created_at": "2024-01-10T00:00:00.000Z",
      "votes": {
        "accept": 5,
        "reject": 1
      }
    }
  ]
}
```

---

## Party

### GET /api/party/:partyId
Get all events for a specific party.

**URL Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `partyId` | string | Full party identifier |

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | number | 100 | Max results |
| `index` | boolean | true | Use party index if available |

### GET /api/party/:partyId/summary
Get party activity summary.

### GET /api/party/search
Search parties by prefix.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `q` | string | Search query (prefix) |
| `limit` | number | Max results |

### GET /api/party/index/status
Get party index build status.

### POST /api/party/index/build
Trigger party index build.

---

## Search

### GET /api/search
Search across events, contracts, and parties.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `q` | string | Search query |
| `type` | string | Filter: "event", "contract", "party" |
| `limit` | number | Max results |

---

## Engine

### GET /api/engine/status
Get warehouse engine status.

**Response:**
```json
{
  "running": true,
  "lastCycle": "2024-01-15T10:25:00.000Z",
  "filesIndexed": 35000,
  "pendingFiles": 5
}
```

### GET /api/engine/stats
Get engine statistics.

### POST /api/engine/cycle
Trigger an engine processing cycle.

### GET /api/engine/templates/status
Get template file index status.

### POST /api/engine/templates/build
Build/rebuild template file index.

---

## Backfill

### GET /api/backfill/cursors
Get backfill cursor positions.

**Response:**
```json
{
  "cursors": [
    {
      "name": "events",
      "position": "2024-01-15T10:30:00.000Z",
      "recordsProcessed": 15000000
    }
  ]
}
```

### GET /api/backfill/stats
Get backfill progress statistics.

---

## Error Responses

All endpoints return errors in a consistent format:

```json
{
  "error": "Error message description"
}
```

Common HTTP status codes:
- `400` - Bad Request (invalid parameters)
- `404` - Not Found
- `500` - Internal Server Error
- `503` - Service Unavailable (index not built)

---

## Rate Limiting

The API does not currently implement rate limiting. For production use, consider adding a reverse proxy with rate limiting.

## CORS

CORS is enabled for all origins. Configure in `server/server.js` for production.
