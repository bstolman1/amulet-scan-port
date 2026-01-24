# Production Deployment Guide

Deploy the Amulet Scan frontend to an Ubuntu server with nginx.

## Prerequisites

- Ubuntu 20.04+ server
- Node.js 20+ installed
- nginx installed (`sudo apt install nginx`)
- Domain pointing to your server (for HTTPS)

## Quick Deploy

```bash
# 1. Clone/pull the repo on your server
git pull origin main

# 2. Configure environment
cp deploy/.env.production.example .env.production
nano .env.production  # Set your API URLs

# 3. Deploy
chmod +x deploy/deploy-frontend.sh
./deploy/deploy-frontend.sh
```

## Step-by-Step Setup

### 1. Install nginx

```bash
sudo apt update
sudo apt install nginx -y
sudo systemctl enable nginx
```

### 2. Configure nginx

```bash
# Copy the config
sudo cp deploy/nginx-site.conf /etc/nginx/sites-available/scanton

# Edit server_name to your domain
sudo nano /etc/nginx/sites-available/scanton

# Enable the site
sudo ln -sf /etc/nginx/sites-available/scanton /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# Test and reload
sudo nginx -t
sudo systemctl reload nginx
```

### 3. Configure Environment

```bash
cp deploy/.env.production.example .env.production
nano .env.production
```

Set these values:
- `VITE_API_BASE_URL` - Your API server URL (e.g., `https://api.scanton.io`)
- `VITE_SCAN_API_URL` - Canton Scan API (if using external)

### 4. Build and Deploy

```bash
chmod +x deploy/deploy-frontend.sh
./deploy/deploy-frontend.sh
```

### 5. Enable HTTPS (Recommended)

```bash
chmod +x deploy/enable-https.sh
sudo ./deploy/enable-https.sh yourdomain.com
```

## Rollback

If something goes wrong, restore the backup:

```bash
# Find latest backup
ls -la /var/www/

# Restore (replace timestamp)
sudo rm -rf /var/www/html/*
sudo cp -r /var/www/html.backup.YYYYMMDD_HHMMSS/* /var/www/html/
```

## Troubleshooting

### 502 Bad Gateway
- API server not running
- Check `VITE_API_BASE_URL` is correct

### Blank page / JS errors
- Check browser console for errors
- Verify build completed successfully
- Check nginx error logs: `sudo tail -f /var/log/nginx/error.log`

### Routes return 404
- Ensure `try_files` is configured in nginx
- Reload nginx: `sudo systemctl reload nginx`

### Permission denied
- Fix ownership: `sudo chown -R www-data:www-data /var/www/html`
- Fix permissions: `sudo chmod -R 755 /var/www/html`

## Updating

```bash
cd /path/to/project
git pull origin main
./deploy/deploy-frontend.sh
```

## Firewall

```bash
sudo ufw allow 'Nginx Full'
sudo ufw enable
```

## DNS Setup

Add these DNS records:

| Type | Name | Value |
|------|------|-------|
| A | @ | YOUR_SERVER_IP |
| A | www | YOUR_SERVER_IP |

Wait for DNS propagation (up to 48 hours) before enabling HTTPS.

## File Structure After Deploy

```
/var/www/html/
├── index.html
├── assets/
│   ├── index-[hash].js
│   └── index-[hash].css
├── favicon.ico
└── robots.txt
```
