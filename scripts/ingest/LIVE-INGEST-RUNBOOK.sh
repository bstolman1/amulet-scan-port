#!/bin/bash
# Canton Live Ingestion — Quick Reference
# ========================================
#
# Service: canton-live-ingest (systemd)
# Script:  ~/amulet-scan-port/scripts/ingest/fetch-updates.js
# Cursor:  /var/lib/ledger_raw/cursors/live-cursor.json
# Logs:    journalctl -u canton-live-ingest

# ─── Service management ─────────────────────────────────────

# Check status
sudo systemctl status canton-live-ingest

# Start / stop / restart
sudo systemctl start canton-live-ingest
sudo systemctl stop canton-live-ingest
sudo systemctl restart canton-live-ingest

# Enable/disable start on boot
sudo systemctl enable canton-live-ingest
sudo systemctl disable canton-live-ingest

# ─── Logs ────────────────────────────────────────────────────

# Live log stream
journalctl -u canton-live-ingest -f

# Recent activity
journalctl -u canton-live-ingest -n 50 --no-pager

# Batch progress only
journalctl -u canton-live-ingest -f | grep --line-buffered 'cursor_advanced\|batch_processed\|heartbeat'

# Errors only
journalctl -u canton-live-ingest --since "1 hour ago" --no-pager | grep -E 'error|FATAL|FAIL'

# Logs since a specific time
journalctl -u canton-live-ingest --since "2 hours ago" --no-pager
journalctl -u canton-live-ingest --since "2026-05-19 16:00" --no-pager

# ─── Cursor & lag ────────────────────────────────────────────

# Current cursor position
cat /var/lib/ledger_raw/cursors/live-cursor.json

# How far behind real-time?
python3 -c "
import json
from datetime import datetime, timezone
c = json.load(open('/var/lib/ledger_raw/cursors/live-cursor.json'))
cursor = datetime.fromisoformat(c['record_time'].replace('Z','+00:00'))
now = datetime.now(timezone.utc)
lag = now - cursor
print(f'Cursor: {c[\"record_time\"]}')
print(f'Lag:    {lag}')
"

# ─── Verification ────────────────────────────────────────────

# Verify a specific day against Scan API
source ~/.gcs_hmac_env && node --max-old-space-size=8192 \
  ~/amulet-scan-port/scripts/ingest/verify-scan-completeness.js \
  --migration=4 --date=YYYY-MM-DD --scope=updates \
  --output=/tmp/verify-spot.ndjson

# Check partition coverage (all migrations)
node ~/amulet-scan-port/scripts/ingest/check-partition-coverage.js

# Check file counts for a specific day
cd ~/amulet-scan-port/scripts/ingest && node -e "
import('@google-cloud/storage').then(async ({ Storage }) => {
  const bucket = new Storage().bucket('canton-bucket');
  const day = process.argv[1] || '2026-05-18';
  const [y, m, d] = day.split('-');
  for (const type of ['updates', 'events']) {
    const prefix = 'raw/updates/' + type + '/migration=4/year=' + parseInt(y) + '/month=' + parseInt(m) + '/day=' + parseInt(d) + '/';
    const [files] = await bucket.getFiles({ prefix });
    console.log(type + ': ' + files.length + ' files');
  }
});
" -- YYYY-MM-DD

# ─── Remediation (re-ingest a day) ───────────────────────────

# Re-ingest a single day (wipes and replaces existing data)
source ~/.gcs_hmac_env && node --max-old-space-size=8192 \
  ~/amulet-scan-port/scripts/ingest/reingest-updates.js \
  --start=YYYY-MM-DD --end=YYYY-MM-DD --migration=4 --clean --force

# ─── Service configuration ──────────────────────────────────

# Service file location
# /etc/systemd/system/canton-live-ingest.service
#
# Environment files:
#   ~/amulet-scan-port/scripts/ingest/.env     (Scan API config)
#   ~/.gcs_hmac_env.systemd                     (GCS HMAC keys, no 'export' prefix)
#
# After editing the service file:
sudo systemctl daemon-reload
sudo systemctl restart canton-live-ingest
