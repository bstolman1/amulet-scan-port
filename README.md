# Amulet Scan

A high-performance ledger explorer for Canton Network, featuring real-time data ingestion, governance tracking, and comprehensive analytics.

## Overview

Amulet Scan provides a complete solution for exploring and analyzing Canton Network ledger data:

- **Real-time Updates**: Live streaming of ledger events as they occur
- **Historical Backfill**: Complete historical data ingestion with TB-scale support
- **Governance Tracking**: Monitor votes, proposals, and SV weight changes
- **ACS Snapshots**: Point-in-time Active Contract Set analysis
- **Supply Analytics**: Token minting, burning, and holder distribution

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   Canton Network    │────▶│   Ingestion Layer   │────▶│   Binary Storage    │
│      Scan API       │     │   (Node.js scripts) │     │   (.pb.zst files)   │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
                                                                  │
                                                                  ▼
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   React Frontend    │◀────│    Express API      │◀────│      DuckDB         │
│   (Vite + Tailwind) │     │    (Port 3001)      │     │   Query Engine      │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
```

## Quick Start

### Prerequisites

- Node.js 20.x or later
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/amulet-scan.git
cd amulet-scan

# Install frontend dependencies
npm install

# Install server dependencies
cd server && npm install

# Install ingestion dependencies
cd ../scripts/ingest && npm install
```

### Running Locally

```bash
# Terminal 1: Start the API server
cd server
cp .env.example .env  # Configure your paths
npm start

# Terminal 2: Start the frontend
cd ..
npm run dev

# Terminal 3: Run data ingestion (optional)
cd scripts/ingest
node fetch-updates.js  # Live updates
```

### Environment Configuration

Create `server/.env`:
```bash
PORT=3001
DATA_DIR=/path/to/ledger_data
CURSOR_DIR=/path/to/ledger_data/cursors
ENGINE_ENABLED=true
```

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture Overview](docs/architecture-overview.md) | System design and component interactions |
| [API Reference](docs/api-reference.md) | Complete API endpoint documentation |
| [Data Architecture](docs/data-architecture.md) | Storage formats and indexing strategy |
| [Setup Guide](docs/setup-guide.md) | Detailed installation and configuration |
| [Deployment Guide](docs/deployment.md) | Production deployment instructions |

## Technology Stack

### Frontend
- **React 18** with TypeScript
- **Vite** for fast development and building
- **Tailwind CSS** with custom design system
- **shadcn/ui** component library
- **TanStack Query** for data fetching
- **Recharts** for data visualization

### Backend
- **Express.js** API server
- **DuckDB** for high-performance SQL queries
- **Node.js** ingestion scripts with worker pools
- **Protobuf + Zstandard** for efficient storage

### Data Pipeline
- Binary files (.pb.zst) as source of truth
- Optional Parquet materialization for analytics
- Incremental indexing for fast queries

## Project Structure

```
├── src/                    # React frontend
│   ├── components/         # UI components
│   ├── hooks/              # Custom React hooks
│   ├── pages/              # Route pages
│   └── lib/                # Utilities
├── server/                 # Express API server
│   ├── api/                # API route handlers
│   ├── engine/             # Warehouse engine
│   ├── duckdb/             # Database connection
│   └── cache/              # Caching layer
├── scripts/ingest/         # Data ingestion
│   ├── fetch-updates.js    # Live updates
│   ├── fetch-backfill.js   # Historical data
│   └── fetch-acs.js        # ACS snapshots
├── docs/                   # Documentation
└── data/                   # Data storage (gitignored)
```

## Key Features

### Governance Dashboard
- Track VoteRequest lifecycle from creation to execution
- Monitor Super Validator weight distributions
- View proposal outcomes and voting patterns

### Supply Analytics
- Real-time minting and burning statistics
- Rich list of top token holders
- Daily supply changes visualization

### Performance
- Handles 35K+ binary files (1.8TB compressed)
- Sub-second queries via template file indexing
- Streaming decompression for memory efficiency

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is proprietary software. All rights reserved.

## Support

For questions or issues, please open a GitHub issue or contact the development team.
