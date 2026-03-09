# Production Deployment Guide

This guide covers deploying Amulet Scan to a Linux VM (Google Cloud, AWS, Azure, etc.) for production use.

## Overview

The deployment consists of:
1. **API Server** - Express.js serving the DuckDB-powered API
2. **Ingestion Pipeline** - Node.js scripts for data collection
3. **Frontend** - Static React app (served separately or via CDN)

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Linux VM | Ubuntu 22.04 LTS recommended |
| SSH Access | For initial setup and maintenance |
| Node.js | 20.x (installed via NodeSource) |
| Git | For code deployment |
| Storage | 500GB+ SSD for full backfill |
| RAM | 16GB+ recommended |
| Network | Access to Canton Scan API |

## Quick Start

```bash
# 1. SSH into your VM
gcloud compute ssh your-vm-name --zone=us-central1-f

# 2. Install Node.js 20.x
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs git

# 3. Clone and setup
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git ~/app
cd ~/app/server && npm install
cd ~/app/scripts/ingest && npm install

# 4. Create data directories
mkdir -p ~/ledger_data/raw ~/ledger_data/cursors ~/ledger_data/logs
```

## Environment Configuration

### Server (`~/app/server/.env`)

```bash
cat > ~/app/server/.env << 'EOF'
PORT=3001
DATA_DIR=/home/YOUR_USERNAME/ledger_data
CURSOR_DIR=/home/YOUR_USERNAME/ledger_data/cursors
ENGINE_ENABLED=true
LOG_LEVEL=info

# Optional API keys
# GROUPS_IO_API_KEY=your_key
# KAIKO_API_KEY=your_key
# OPENAI_API_KEY=your_key
EOF
```

### Ingestion Scripts (`~/app/scripts/ingest/.env`)

```bash
cat > ~/app/scripts/ingest/.env << 'EOF'
SCAN_URL=https://scan.sv-1.global.canton.network.sync.global/api/scan
DATA_DIR=/home/YOUR_USERNAME/ledger_data
CURSOR_DIR=/home/YOUR_USERNAME/ledger_data/cursors
PARALLEL_FETCHES=8
MAX_WORKERS=12
BATCH_SIZE=1000
MAX_ROWS_PER_FILE=20000
EOF
```

## Process Management (Choose One)

### Option A: PM2 (Recommended)

PM2 provides auto-restart, monitoring, log management, and zero-downtime reloads.

```bash
# Install PM2 globally
npm install -g pm2

# Start the server
cd ~/app/server
pm2 start ecosystem.config.cjs --env production

# Enable auto-start on boot
pm2 startup
pm2 save

# View logs
pm2 logs duckdb-api

# Monitor in real-time
pm2 monit
```

See `server/pm2-setup.md` for detailed PM2 configuration and commands.

### Option B: Systemd

Create a service to keep the API server running:

```bash
sudo tee /etc/systemd/system/duckdb-api.service << 'EOF'
[Unit]
Description=DuckDB API Server
After=network.target

[Service]
Type=simple
User=YOUR_USERNAME
WorkingDirectory=/home/YOUR_USERNAME/app/server
ExecStart=/usr/bin/node --expose-gc --max-old-space-size=4096 server.js
Restart=always
RestartSec=10
Environment=NODE_ENV=production

# Crash recovery with backoff
StartLimitIntervalSec=300
StartLimitBurst=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable duckdb-api
sudo systemctl start duckdb-api
```

Check status:
```bash
sudo systemctl status duckdb-api
journalctl -u duckdb-api -f  # View logs
```

## Auto-Update from GitHub

Create an update script that pulls changes and restarts the server:

```bash
cat > ~/update-server.sh << 'EOF'
#!/bin/bash
cd ~/app
git fetch origin main
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "$(date): Pulling updates..."
    git pull origin main
    cd server && npm install
    cd ../scripts/ingest && npm install
    sudo systemctl restart duckdb-api
    echo "$(date): Server updated and restarted"
fi
EOF
chmod +x ~/update-server.sh
```

Add to crontab (every 5 minutes):
```bash
crontab -e
# Add this line:
*/5 * * * * /home/YOUR_USERNAME/update-server.sh >> /home/YOUR_USERNAME/update.log 2>&1
```

## Data Ingestion Cron Jobs

Set up automated data ingestion:

```bash
crontab -e
# Add these lines:

# Pull code updates every 5 minutes
*/5 * * * * /home/YOUR_USERNAME/update-server.sh >> /home/YOUR_USERNAME/update.log 2>&1

# Run live updates every 10 minutes
*/10 * * * * cd /home/YOUR_USERNAME/app/scripts/ingest && node fetch-updates.js >> /home/YOUR_USERNAME/ingest.log 2>&1

# Run ACS snapshot daily at 2am
0 2 * * * cd /home/YOUR_USERNAME/app/scripts/ingest && node fetch-acs.js >> /home/YOUR_USERNAME/acs.log 2>&1
```

## Firewall Configuration

Open port 3001 for API access:

```bash
# Google Cloud
gcloud compute firewall-rules create allow-api-3001 \
    --allow tcp:3001 \
    --source-ranges 0.0.0.0/0 \
    --description "Allow DuckDB API access"

# Or using ufw (Ubuntu)
sudo ufw allow 3001/tcp
```

## Frontend Configuration

Update `src/lib/backend-config.ts` with your VM's external IP:

```typescript
const CLOUDFLARE_TUNNEL_URL = 'http://YOUR_VM_EXTERNAL_IP:3001';
```

Get your VM's IP:
```bash
gcloud compute instances describe your-vm-name \
    --zone=us-central1-f \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

## Optional: HTTPS with Nginx

For production, add Nginx as a reverse proxy with SSL:

```bash
sudo apt install nginx certbot python3-certbot-nginx

# Configure nginx
sudo tee /etc/nginx/sites-available/duckdb-api << 'EOF'
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:3001;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/duckdb-api /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx

# Add SSL
sudo certbot --nginx -d your-domain.com
```

## Monitoring

### Log Rotation

Add log rotation to prevent disk filling:

```bash
sudo tee /etc/logrotate.d/amulet-scan << 'EOF'
/home/YOUR_USERNAME/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
}
EOF
```

### Health Check Script

Create a monitoring script:

```bash
cat > ~/check-health.sh << 'EOF'
#!/bin/bash
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3001/health)
if [ "$RESPONSE" != "200" ]; then
    echo "$(date): Health check failed with status $RESPONSE"
    sudo systemctl restart duckdb-api
    echo "$(date): Server restarted"
fi
EOF
chmod +x ~/check-health.sh
```

Add to crontab:
```bash
*/5 * * * * /home/YOUR_USERNAME/check-health.sh >> /home/YOUR_USERNAME/health.log 2>&1
```

## Troubleshooting

### Check Server Status
```bash
sudo systemctl status duckdb-api
curl http://localhost:3001/health/detailed
```

### View Logs
```bash
# Server logs (systemd)
journalctl -u duckdb-api -f

# Ingestion logs
tail -f ~/ingest.log

# Git update logs
tail -f ~/update.log
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Server won't start | Check `journalctl -u duckdb-api -e` for errors |
| Out of memory | Reduce `MAX_WORKERS` or add swap |
| Permission denied | Verify file ownership: `chown -R $USER ~/ledger_data` |
| Port in use | Check: `lsof -i :3001` and kill process |
| Disk full | Check: `df -h` and clean old logs |

### Manual Operations

```bash
# Restart server
sudo systemctl restart duckdb-api

# Check data directory
ls -la ~/ledger_data/raw/
du -sh ~/ledger_data/

# Rebuild indexes
curl -X POST http://localhost:3001/api/engine/templates/build

# Trigger aggregation refresh
curl -X POST http://localhost:3001/api/refresh-aggregations
```

## Backup Strategy

### Data Backup

```bash
# Create backup script
cat > ~/backup-data.sh << 'EOF'
#!/bin/bash
DATE=$(date +%Y%m%d)
BACKUP_DIR=~/backups
mkdir -p $BACKUP_DIR

# Backup cursors and indexes (small, critical)
tar -czf $BACKUP_DIR/cursors-$DATE.tar.gz ~/ledger_data/cursors/
tar -czf $BACKUP_DIR/indexes-$DATE.tar.gz ~/ledger_data/*.json

# Keep only last 7 backups
find $BACKUP_DIR -name "*.tar.gz" -mtime +7 -delete
EOF
chmod +x ~/backup-data.sh
```

### Upload to Cloud Storage

```bash
# Google Cloud Storage example
gsutil cp ~/backups/*.tar.gz gs://your-bucket/backups/
```

## Performance Tuning

### System Limits

```bash
# Increase file descriptor limits
echo "YOUR_USERNAME soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "YOUR_USERNAME hard nofile 65536" | sudo tee -a /etc/security/limits.conf
```

### Node.js Memory

For large datasets, increase Node.js heap:

```bash
# In systemd service file
Environment=NODE_OPTIONS="--max-old-space-size=8192"
```

### DuckDB Configuration

The connection pool in `server/duckdb/connection.js` can be tuned:
- `MAX_POOL_SIZE`: Increase for more concurrent queries
- `QUERY_TIMEOUT_MS`: Adjust for slow queries

## Security Checklist

- [ ] Firewall configured (only port 3001 or 443 open)
- [ ] HTTPS enabled with valid certificate
- [ ] API keys stored in environment variables
- [ ] Regular security updates applied
- [ ] Log monitoring configured
- [ ] Backup strategy implemented
