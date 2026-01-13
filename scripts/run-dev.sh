#!/bin/bash
# Run both server and frontend concurrently
# Usage: ./scripts/run-dev.sh

cd "$(dirname "$0")/.."

echo "🚀 Starting Amulet Scan development servers..."
echo "   Server: http://localhost:3001"
echo "   Frontend: http://localhost:5173"
echo ""

npx concurrently -n server,frontend -c blue,green \
  "cd server && npm start" \
  "npm run dev -- --host"
