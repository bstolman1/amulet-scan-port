# Deployment Guide

Deploy the Amulet Scan dashboard to an Ubuntu server with nginx.

Live site: `https://dashboard.canton.foundation/`
Staging site: `https://dashboard.canton.foundation/staging/`

## Architecture

```
Browser ─► nginx (port 80/443)
              ├── /            ─► /var/www/html/          (production frontend)
              ├── /api/        ─► localhost:3001           (backend API)
              ├── /staging/    ─► /var/www/staging/        (staging frontend)
              └── /staging/api/─► localhost:3001           (same backend)
```

| Component | Production | Staging |
|-----------|-----------|---------|
| Frontend | `/var/www/html/` | `/var/www/staging/` |
| Backend | PM2 `duckdb-api` on port 3001 | Same backend (port 3001) |
| URL | `dashboard.canton.foundation/` | `dashboard.canton.foundation/staging/` |

Staging shares the production backend — it only previews frontend changes.

## Prerequisites

- Ubuntu 20.04+ server
- Node.js 20+ installed
- nginx installed (`sudo apt install nginx`)
- Domain pointing to your server (for HTTPS)

## Quick Deploy (Recommended)

Use the deploy script for both staging and production:

```bash
cd ~/amulet-scan-port

# Deploy to staging (preview before going live)
./deploy/deploy-frontend.sh --staging

# Deploy to production
./deploy/deploy-frontend.sh
```

The script handles dependencies, build flags, backups, and permissions automatically.

### Manual Deploy

If you need to deploy manually:

**Production:**
```bash
cd ~/amulet-scan-port
git checkout main && git pull
npm install
npx vite build
sudo cp -r dist/* /var/www/html/
```

**Staging:**
```bash
cd ~/amulet-scan-port
git checkout <your-branch>
npm install
VITE_BASE_PATH=/staging npx vite build --base=/staging/
cp -r dist/* /var/www/staging/
```

### Backend

The backend only needs restarting when server-side code changes (files in `server/`):

```bash
cd ~/amulet-scan-port/server
npm install
pm2 restart duckdb-api
```

Frontend-only changes do NOT require a backend restart.

## Development Workflow

1. Make changes on a feature branch
2. Deploy to staging: `./deploy/deploy-frontend.sh --staging`
3. Preview at `https://dashboard.canton.foundation/staging/`
4. If happy, merge to main and deploy: `./deploy/deploy-frontend.sh`
5. If not, iterate on the branch and redeploy staging

## Initial Server Setup

### 1. Install nginx

```bash
sudo apt update
sudo apt install nginx -y
sudo systemctl enable nginx
```

### 2. Configure nginx

```bash
sudo cp deploy/nginx-site.conf /etc/nginx/sites-available/scanton
sudo nano /etc/nginx/sites-available/scanton  # Set server_name to your domain
sudo ln -sf /etc/nginx/sites-available/scanton /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

**Important:** The staging location block must use `root /var/www;` (not `alias`).
Using `alias` with `try_files` causes a known nginx bug where SPA fallback fails.

### 3. Create directories

```bash
sudo mkdir -p /var/www/html /var/www/staging
sudo chown -R josefin:josefin /var/www/staging
sudo chown -R www-data:www-data /var/www/html
sudo chmod -R 755 /var/www/html /var/www/staging
```

### 4. Start backend

```bash
cd ~/amulet-scan-port/server
pm2 start ecosystem.config.cjs --env production
pm2 save
pm2 startup   # enables auto-start on VM reboot
```

The `ecosystem.config.cjs` uses `--env-file=../scripts/ingest/.env` to load the correct DuckDB path.

### 5. Enable HTTPS (Recommended)

```bash
chmod +x deploy/enable-https.sh
sudo ./deploy/enable-https.sh yourdomain.com
```

## Staging Build Environment Variables

The staging frontend needs two env vars at build time:

| Variable | Purpose | Value |
|----------|---------|-------|
| `VITE_BASE_PATH` | Sets the React Router basename so routes work under `/staging/` | `/staging` |
| `--base` (CLI flag) | Sets the Vite asset base path so JS/CSS load from `/staging/assets/` | `/staging/` |

Production builds need neither (both default to `/`).

The deploy script (`./deploy/deploy-frontend.sh --staging`) sets these automatically.

## PM2 Commands

| Action | Command |
|--------|---------|
| View all processes | `pm2 list` |
| View logs | `pm2 logs duckdb-api --lines 20 --nostream` |
| Restart backend | `pm2 restart duckdb-api` |
| Stop backend | `pm2 stop duckdb-api` |
| Monitor resources | `pm2 monit` |
| Check DuckDB path | `grep -i "duckdb" ~/amulet-scan-port/server/logs/pm2-out.log \| tail -5` |

PM2 auto-starts on boot via systemd (configured with `pm2 startup` + `pm2 save`).

## Health Checks

```bash
# Backend health
curl http://localhost:3001/health

# Scan proxy endpoint status
curl http://localhost:3001/scan-proxy/_health

# SV status endpoint
curl http://localhost:3001/scan-proxy/_sv-node-status | head -c 200
```

## Rollback

### Frontend rollback

The deploy script creates timestamped backups automatically:

```bash
# Find latest backup
ls -la /var/www/ | grep backup

# Restore production (replace timestamp)
sudo rm -rf /var/www/html/*
sudo cp -r /var/www/html.backup.YYYYMMDD_HHMMSS/* /var/www/html/
```

### Backend rollback

```bash
cd ~/amulet-scan-port
git checkout main -- server/
cd server && npm install && pm2 restart duckdb-api
```

## Troubleshooting

### 502 Bad Gateway
- Backend not running: `pm2 list` to check status
- Wrong port: verify nginx proxy_pass matches PM2 port

### Blank page / JS errors
- Check browser console (F12) for errors
- Verify build completed: `ls dist/index.html`
- Check nginx logs: `sudo tail -f /var/log/nginx/error.log`

### Routes return 404
- Ensure `try_files` is configured in nginx
- For staging, verify build used `VITE_BASE_PATH=/staging` and `--base=/staging/`
- Reload nginx: `sudo systemctl reload nginx`

### Staging 404 but production works
- Verify nginx staging block uses `root /var/www;` (NOT `alias`)
- Verify build used correct env vars (use `./deploy/deploy-frontend.sh --staging`)
- Check: `grep -c "/staging/" /var/www/staging/assets/index-*.js` (should be > 0)
- Check: `grep basename /var/www/staging/assets/index-*.js` should show `/staging`
- Missing trailing slash: ensure `location = /staging { return 301 /staging/; }` exists

### Backend crash on startup
- Check logs: `pm2 logs duckdb-api --lines 20 --nostream`
- Common cause: missing dependency — run `cd server && npm install`
- Check DuckDB path: should be `/var/lib/ledger_raw/canton-explorer.duckdb`

### Port 3001 EADDRINUSE
- Another process is using the port: `sudo lsof -i :3001`
- Stop conflicting process and restart: `pm2 restart duckdb-api`

### Permission denied on /var/www/staging
- Fix ownership: `sudo chown -R josefin:josefin /var/www/staging`

## Firewall

```bash
sudo ufw allow 'Nginx Full'
sudo ufw enable
```

## DNS Setup

| Type | Name | Value |
|------|------|-------|
| A | @ | YOUR_SERVER_IP |
| A | www | YOUR_SERVER_IP |

Wait for DNS propagation (up to 48 hours) before enabling HTTPS.
