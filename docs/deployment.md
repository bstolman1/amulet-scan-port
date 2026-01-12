# VM Deployment Guide

Deploy the DuckDB API server and ingestion pipeline to a Linux VM (Google Cloud, AWS, etc.).

## Prerequisites

- Linux VM with SSH access
- Node.js 20.x
- Git
- Access to Canton Scan API

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

## Systemd Service

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
ExecStart=/usr/bin/node server.js
Restart=on-failure
RestartSec=10
Environment=NODE_ENV=production

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

## Troubleshooting

### Check if server is running
```bash
sudo systemctl status duckdb-api
curl http://localhost:3001/api/stats
```

### View logs
```bash
journalctl -u duckdb-api -f          # Server logs
tail -f ~/ingest.log                  # Ingestion logs
tail -f ~/update.log                  # Git update logs
```

### Manual restart
```bash
sudo systemctl restart duckdb-api
```

### Check data directory
```bash
ls -la ~/ledger_data/raw/
du -sh ~/ledger_data/
```
