#!/bin/bash
set -euo pipefail

# =============================================================================
# deploy-frontend.sh - Build and deploy Vite React SPA to nginx
# =============================================================================

DEPLOY_DIR="/var/www/html"
BACKUP_DIR="/var/www/html.backup.$(date +%Y%m%d_%H%M%S)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "  Frontend Deployment Script"
echo "=========================================="
echo ""

# Check if running from correct directory
if [ ! -f "$PROJECT_DIR/package.json" ]; then
    echo "❌ Error: package.json not found. Run from project root."
    exit 1
fi

cd "$PROJECT_DIR"

# Check for .env.production
if [ ! -f ".env.production" ]; then
    echo "⚠️  Warning: .env.production not found"
    echo "   Copy .env.production.example to .env.production and configure it"
    read -p "   Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Step 1: Install dependencies
echo "📦 Installing dependencies..."
npm ci --prefer-offline

# Step 2: Build the project
echo "🔨 Building production bundle..."
npm run build

# Verify build output
if [ ! -d "dist" ] || [ ! -f "dist/index.html" ]; then
    echo "❌ Error: Build failed - dist/index.html not found"
    exit 1
fi

echo "✅ Build successful"

# Step 3: Backup existing deployment
if [ -d "$DEPLOY_DIR" ] && [ "$(ls -A $DEPLOY_DIR 2>/dev/null)" ]; then
    echo "📁 Backing up current deployment to $BACKUP_DIR..."
    sudo cp -r "$DEPLOY_DIR" "$BACKUP_DIR"
fi

# Step 4: Deploy new build
echo "🚀 Deploying to $DEPLOY_DIR..."

# Create deploy dir if it doesn't exist
sudo mkdir -p "$DEPLOY_DIR"

# Clear old files (safely)
sudo find "$DEPLOY_DIR" -mindepth 1 -delete

# Copy new build
sudo cp -r dist/* "$DEPLOY_DIR/"

# Set permissions
sudo chown -R www-data:www-data "$DEPLOY_DIR"
sudo chmod -R 755 "$DEPLOY_DIR"

# Step 5: Verify deployment
if [ -f "$DEPLOY_DIR/index.html" ]; then
    echo ""
    echo "=========================================="
    echo "  ✅ Deployment successful!"
    echo "=========================================="
    echo ""
    echo "  Deployed to: $DEPLOY_DIR"
    echo "  Backup at:   $BACKUP_DIR"
    echo ""
    echo "  To rollback: sudo cp -r $BACKUP_DIR/* $DEPLOY_DIR/"
    echo ""
else
    echo "❌ Error: Deployment verification failed"
    exit 1
fi
