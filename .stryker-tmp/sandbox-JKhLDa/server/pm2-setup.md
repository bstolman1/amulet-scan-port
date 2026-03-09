# PM2 Production Setup Guide

PM2 is a production process manager that provides auto-restart, monitoring, and log management.

## Quick Start

```bash
# Install PM2 globally
npm install -g pm2

# Start the server with PM2
cd server
pm2 start ecosystem.config.cjs --env production

# Check status
pm2 status

# View logs
pm2 logs duckdb-api

# Monitor in real-time
pm2 monit
```

## Auto-Start on Boot

```bash
# Generate startup script (run the command it outputs)
pm2 startup

# Save current process list
pm2 save
```

## Common Commands

| Command | Description |
|---------|-------------|
| `pm2 start ecosystem.config.cjs` | Start with default env |
| `pm2 start ecosystem.config.cjs --env production` | Start with production env |
| `pm2 stop duckdb-api` | Stop the server |
| `pm2 restart duckdb-api` | Restart the server |
| `pm2 reload duckdb-api` | Zero-downtime reload |
| `pm2 delete duckdb-api` | Remove from PM2 |
| `pm2 logs duckdb-api` | View logs |
| `pm2 logs duckdb-api --lines 100` | View last 100 lines |
| `pm2 monit` | Real-time monitoring dashboard |
| `pm2 status` | Show all processes |
| `pm2 info duckdb-api` | Detailed process info |
| `pm2 flush` | Clear all logs |

## Configuration Explained

The `ecosystem.config.cjs` includes:

- **Auto-restart**: Automatically restarts on crash
- **Exponential backoff**: Waits longer between restarts if crashing repeatedly
- **Memory limit**: Restarts if memory exceeds 3GB
- **GC exposure**: Enables manual garbage collection for memory management
- **Log rotation**: Logs written to `server/logs/pm2-*.log`

## Monitoring & Alerts

### View Memory Usage
```bash
pm2 monit
```

### Check Restart Count
```bash
pm2 describe duckdb-api | grep restarts
```

### Log Rotation Setup
```bash
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 50M
pm2 set pm2-logrotate:retain 10
```

## Troubleshooting

### Server keeps restarting
Check crash logs:
```bash
pm2 logs duckdb-api --err --lines 50
cat server/logs/crash.log
```

### High memory usage
```bash
# Check current memory
pm2 monit

# Force restart to clear memory
pm2 restart duckdb-api
```

### Port already in use
```bash
# Find process using port 3001
lsof -i :3001
# or on Windows
netstat -ano | findstr :3001

# Kill the process or change PORT in .env
```

## Alternative: systemd (Linux)

If you prefer systemd over PM2, see the service file in `docs/deployment.md`.
