#!/usr/bin/env bash
set -euo pipefail

# ==========================================================
#  WSL2 Setup Script for Canton Ledger Backfill
#  Clones repo, installs dependencies, configures environment
# ==========================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REPO_URL="https://github.com/bstolman1/amulet-scan-port.git"
INSTALL_DIR="$HOME/canton-explorer"
DATA_DIR="/mnt/c/ledger_raw"
NODE_VERSION="20"

echo -e "${BLUE}"
echo "=========================================================="
echo "  Canton Ledger Backfill - WSL2 Setup Script"
echo "=========================================================="
echo -e "${NC}"

# -----------------------------------------------------------------------------
# Step 1: Check if running in WSL
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[1/8] Checking WSL environment...${NC}"

if ! grep -qi microsoft /proc/version 2>/dev/null; then
    echo -e "${RED}ERROR: This script must be run inside WSL (Windows Subsystem for Linux)${NC}"
    echo "Please open a WSL terminal and run this script again."
    exit 1
fi

# Check for WSL2 (recommended)
if grep -qi "WSL2" /proc/version 2>/dev/null || [ -f /proc/sys/fs/binfmt_misc/WSLInterop ]; then
    echo -e "${GREEN}✓ Running in WSL2${NC}"
else
    echo -e "${YELLOW}⚠ Running in WSL1 - WSL2 is recommended for better performance${NC}"
    echo "  To upgrade: wsl --set-version <distro-name> 2"
fi

# -----------------------------------------------------------------------------
# Step 2: Check required tools
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[2/8] Checking required tools...${NC}"

MISSING_TOOLS=()

if ! command -v git &> /dev/null; then
    MISSING_TOOLS+=("git")
fi

if ! command -v curl &> /dev/null; then
    MISSING_TOOLS+=("curl")
fi

if [ ${#MISSING_TOOLS[@]} -gt 0 ]; then
    echo -e "${YELLOW}Installing missing tools: ${MISSING_TOOLS[*]}${NC}"
    sudo apt-get update
    sudo apt-get install -y "${MISSING_TOOLS[@]}"
fi

echo -e "${GREEN}✓ Required tools available${NC}"

# -----------------------------------------------------------------------------
# Step 3: Install Node.js
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[3/8] Setting up Node.js v${NODE_VERSION}...${NC}"

if command -v node &> /dev/null; then
    CURRENT_NODE=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$CURRENT_NODE" -ge "$NODE_VERSION" ]; then
        echo -e "${GREEN}✓ Node.js $(node -v) already installed${NC}"
    else
        echo "Upgrading Node.js from v$CURRENT_NODE to v$NODE_VERSION..."
        INSTALL_NODE=true
    fi
else
    INSTALL_NODE=true
fi

if [ "${INSTALL_NODE:-false}" = true ]; then
    echo "Installing Node.js v${NODE_VERSION} LTS..."
    curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | sudo -E bash -
    sudo apt-get install -y nodejs
    echo -e "${GREEN}✓ Node.js $(node -v) installed${NC}"
fi

echo -e "${GREEN}✓ npm $(npm -v)${NC}"

# -----------------------------------------------------------------------------
# Step 4: Clone Repository
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[4/8] Cloning repository...${NC}"

if [ -d "$INSTALL_DIR" ]; then
    echo -e "${YELLOW}Directory $INSTALL_DIR already exists${NC}"
    read -p "Do you want to update it (pull latest)? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cd "$INSTALL_DIR"
        git pull
        echo -e "${GREEN}✓ Repository updated${NC}"
    else
        echo -e "${BLUE}Skipping clone, using existing directory${NC}"
    fi
else
    echo "Cloning $REPO_URL into $INSTALL_DIR..."
    git clone "$REPO_URL" "$INSTALL_DIR"
    echo -e "${GREEN}✓ Repository cloned${NC}"
fi

cd "$INSTALL_DIR"

# -----------------------------------------------------------------------------
# Step 5: Install Dependencies
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[5/8] Installing dependencies...${NC}"

echo "Installing root project dependencies..."
npm install

echo "Installing ingest script dependencies..."
cd scripts/ingest
npm install

# Ensure piscina is installed (required for worker threads)
if ! npm list piscina &> /dev/null; then
    echo "Installing piscina for worker threads..."
    npm install piscina
fi

cd "$INSTALL_DIR"

echo "Installing server dependencies..."
cd server
npm install
cd "$INSTALL_DIR"

echo -e "${GREEN}✓ All dependencies installed${NC}"

# -----------------------------------------------------------------------------
# Step 6: Configure Environment
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[6/8] Configuring environment...${NC}"

ENV_FILE="$INSTALL_DIR/scripts/ingest/.env"

# Create .env if it doesn't exist, or update DATA_DIR
if [ -f "$ENV_FILE" ]; then
    # Update or add DATA_DIR
    if grep -q "^DATA_DIR=" "$ENV_FILE"; then
        sed -i "s|^DATA_DIR=.*|DATA_DIR=$DATA_DIR|" "$ENV_FILE"
    else
        echo "DATA_DIR=$DATA_DIR" >> "$ENV_FILE"
    fi
    echo -e "${GREEN}✓ Updated existing .env file${NC}"
else
    # Create new .env with defaults
    cat > "$ENV_FILE" << EOF
# Canton Ledger Backfill Configuration
# Generated by setup-wsl.sh on $(date)

# Scan API endpoint
SCAN_URL=https://scan.sv-2.prod.canton.network

# Data directory (Windows path accessible from WSL)
DATA_DIR=$DATA_DIR

# Performance tuning
PARALLEL_FETCHES=6
MAX_WORKERS=30
MAX_ROWS_PER_FILE=15000
DECODE_WORKERS=4

# Optional: Groups.io API key for announcements
# GROUPS_IO_API_KEY=your_key_here
EOF
    echo -e "${GREEN}✓ Created .env file${NC}"
fi

echo -e "${BLUE}Environment configuration:${NC}"
grep -E "^(DATA_DIR|SCAN_URL|PARALLEL_FETCHES)=" "$ENV_FILE" || true

# -----------------------------------------------------------------------------
# Step 7: Create Data Directories
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[7/8] Creating data directories...${NC}"

# Check if Windows drive is accessible
if [ ! -d "/mnt/c" ]; then
    echo -e "${RED}ERROR: Cannot access Windows filesystem at /mnt/c${NC}"
    echo "Make sure WSL has access to Windows drives."
    exit 1
fi

# Create all required directories
mkdir -p "$DATA_DIR/raw"
mkdir -p "$DATA_DIR/acs"
mkdir -p "$DATA_DIR/cursors"
mkdir -p "$DATA_DIR/logs"
mkdir -p "$DATA_DIR/cache"

echo -e "${GREEN}✓ Data directories created:${NC}"
echo "   $DATA_DIR/raw      - Binary ledger data"
echo "   $DATA_DIR/acs      - ACS snapshot data"
echo "   $DATA_DIR/cursors  - Backfill progress cursors"
echo "   $DATA_DIR/logs     - Shard logs"
echo "   $DATA_DIR/cache    - Governance cache"

# -----------------------------------------------------------------------------
# Step 8: Make Scripts Executable
# -----------------------------------------------------------------------------
echo -e "${YELLOW}[8/8] Setting script permissions...${NC}"

chmod +x "$INSTALL_DIR/scripts/ingest/run-sharded-backfill.sh"
chmod +x "$INSTALL_DIR/scripts/setup-wsl.sh" 2>/dev/null || true

echo -e "${GREEN}✓ Scripts are executable${NC}"

# -----------------------------------------------------------------------------
# Done!
# -----------------------------------------------------------------------------
echo ""
echo -e "${GREEN}=========================================================="
echo "  ✓ Setup Complete!"
echo "==========================================================${NC}"
echo ""
echo -e "${BLUE}Quick Start Commands:${NC}"
echo ""
echo "  1. Navigate to ingest scripts:"
echo -e "     ${YELLOW}cd $INSTALL_DIR/scripts/ingest${NC}"
echo ""
echo "  2. Start sharded backfill (6 shards by default):"
echo -e "     ${YELLOW}./run-sharded-backfill.sh${NC}"
echo ""
echo "  3. Or specify shard count and migration:"
echo -e "     ${YELLOW}./run-sharded-backfill.sh 8 3${NC}"
echo ""
echo "  4. Monitor progress:"
echo -e "     ${YELLOW}node shard-progress.js${NC}"
echo ""
echo "  5. Watch logs:"
echo -e "     ${YELLOW}tail -f $DATA_DIR/logs/shard-0.log${NC}"
echo ""
echo -e "${BLUE}Windows Server:${NC}"
echo "  Data is written to C:\\ledger_raw and accessible by the Windows server."
echo "  Start the server on Windows with:"
echo -e "     ${YELLOW}cd server && npm start${NC}"
echo ""
echo -e "${GREEN}Happy backfilling! 🚀${NC}"
