# Deployment Guide

Deploy the Amulet Scan dashboard to an Ubuntu server with nginx.

Live site: `https://dashboard.canton.foundation/`
Staging site: `https://dashboard.canton.foundation/staging/`

## Architecture

```
Browser ─► nginx (port 80/443)
              ├── /            ─► /var/www/html/          (production frontend)
              ├── /api/        ─► localhost:3001           (production backend)
              ├── /staging/    ─► /var/www/staging/        (staging frontend)
              └── /staging/api/─► localhost:3002           (staging backend)
```

| Component | Production | Staging |
|-----------|-----------|---------|
| Frontend | `/var/www/html/` | `/var/www/staging/` |
| Backend | PM2 `duckdb-api` on port 3001 | PM2 `duckdb-api-staging` on port 3002 |
| URL | `dashboard.canton.foundation/` | `dashboard.canton.foundation/staging/` |

## Prerequisites

- Ubuntu 20.04+ server
- Node.js 20+ installed
- nginx installed (`sudo apt install nginx`)
- Domain pointing to your server (for HTTPS)

## Quick Deploy

### Production

```bash
cd ~/amulet-scan-port
git checkout main
git pull

# Frontend
npm install
npx vite build
sudo cp -r dist/* /var/www/html/

# Backend
cd server && npm install && pm2 restart duckdb-api
```

### Staging

```bash
cd ~/amulet-scan-port
git checkout <your-branch>

# Frontend (note the env vars for subpath routing)
npm install
VITE_BASE_PATH=/staging VITE_BASE=/staging/ npx vite build
sudo rm -rf /var/www/staging/assets/
sudo cp -r dist/* /var/www/staging/

# Backend (independent of production)
cd server && npm install && pm2 restart duckdb-api-staging
```

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

### 3. Create directories

```bash
sudo mkdir -p /var/www/html /var/www/staging
sudo chown -R www-data:www-data /var/www/html /var/www/staging
sudo chmod -R 755 /var/www/html /var/www/staging
```

### 4. Start backend services

```bash
cd ~/amulet-scan-port/server

# Production backend
pm2 start ecosystem.config.cjs --env production

# Staging backend
pm2 start ecosystem.staging.cjs

# Save so they survive reboots
pm2 save
pm2 startup
```

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
| `VITE_BASE` | Sets the Vite asset base path so JS/CSS load from `/staging/assets/` | `/staging/` |

Production builds need neither (both default to `/`).

## PM2 Commands

| Action | Command |
|--------|---------|
| View all processes | `pm2 list` |
| Production backend logs | `pm2 logs duckdb-api --lines 20 --nostream` |
| Staging backend logs | `pm2 logs duckdb-api-staging --lines 20 --nostream` |
| Restart production | `pm2 restart duckdb-api` |
| Restart staging | `pm2 restart duckdb-api-staging` |
| Stop staging | `pm2 stop duckdb-api-staging` |
| Monitor resources | `pm2 monit` |

## Health Checks

```bash
# Production backend
curl http://localhost:3001/health

# Staging backend
curl http://localhost:3002/health

# Scan proxy endpoint status
curl http://localhost:3001/scan-proxy/_health
curl http://localhost:3002/scan-proxy/_health
```

## Rollback

### Frontend rollback

```bash
# Find latest backup
ls -la /var/www/

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
- For staging, verify build used `VITE_BASE_PATH=/staging`
- Reload nginx: `sudo systemctl reload nginx`

### Staging 404 but production works
- Missing trailing slash: `/staging` needs a redirect to `/staging/`
- Check nginx has `location = /staging { return 301 /staging/; }`

### Backend crash on startup
- Check logs: `pm2 logs duckdb-api --lines 20 --nostream`
- Common cause: missing dependency — run `cd server && npm install`
- Verify undici is in server/package.json

### Permission denied
- Fix ownership: `sudo chown -R www-data:www-data /var/www/html /var/www/staging`
- Fix permissions: `sudo chmod -R 755 /var/www/html /var/www/staging`

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
