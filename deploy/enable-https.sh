#!/bin/bash
set -euo pipefail

# =============================================================================
# enable-https.sh - Enable HTTPS using Let's Encrypt + Certbot
# =============================================================================
# Usage: sudo ./enable-https.sh yourdomain.com [www.yourdomain.com]

if [ "$EUID" -ne 0 ]; then
    echo "❌ Please run as root (sudo)"
    exit 1
fi

if [ -z "${1:-}" ]; then
    echo "Usage: sudo $0 <domain> [additional-domain]"
    echo "Example: sudo $0 scanton.io www.scanton.io"
    exit 1
fi

DOMAIN="$1"
EXTRA_DOMAIN="${2:-}"

echo "=========================================="
echo "  HTTPS Setup with Let's Encrypt"
echo "=========================================="
echo ""
echo "Domain: $DOMAIN"
[ -n "$EXTRA_DOMAIN" ] && echo "Also:   $EXTRA_DOMAIN"
echo ""

# Step 1: Install certbot
echo "📦 Installing certbot..."
apt update
apt install -y certbot python3-certbot-nginx

# Step 2: Update nginx config with domain
echo "🔧 Updating nginx configuration..."
NGINX_CONF="/etc/nginx/sites-available/scanton"

if [ -f "$NGINX_CONF" ]; then
    if [ -n "$EXTRA_DOMAIN" ]; then
        sed -i "s/server_name .*;/server_name $DOMAIN $EXTRA_DOMAIN;/" "$NGINX_CONF"
    else
        sed -i "s/server_name .*;/server_name $DOMAIN;/" "$NGINX_CONF"
    fi
    nginx -t
    systemctl reload nginx
else
    echo "⚠️  nginx config not found at $NGINX_CONF"
    echo "   Run deploy first or create config manually"
fi

# Step 3: Obtain certificate
echo "🔐 Obtaining SSL certificate..."

CERTBOT_ARGS="-d $DOMAIN"
[ -n "$EXTRA_DOMAIN" ] && CERTBOT_ARGS="$CERTBOT_ARGS -d $EXTRA_DOMAIN"

certbot --nginx $CERTBOT_ARGS --non-interactive --agree-tos --redirect --email "admin@$DOMAIN"

# Step 4: Set up auto-renewal
echo "⏰ Setting up auto-renewal..."
systemctl enable certbot.timer
systemctl start certbot.timer

# Step 5: Verify
echo ""
echo "=========================================="
echo "  ✅ HTTPS enabled!"
echo "=========================================="
echo ""
echo "  Your site is now available at:"
echo "  https://$DOMAIN"
[ -n "$EXTRA_DOMAIN" ] && echo "  https://$EXTRA_DOMAIN"
echo ""
echo "  Certificate auto-renews via systemd timer."
echo "  Check renewal: sudo certbot renew --dry-run"
echo ""
