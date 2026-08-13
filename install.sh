#!/bin/bash
# ============================================================
#   MY ALGO — Angel One Edition
#   install.sh — One-time setup for Ubuntu / AWS / Local PC
#
#   First time: automatically clones from GitHub
#   Already installed: skips clone, updates dependencies
#
#   Usage:
#     bash install.sh
# ============================================================

set -e

# ── ADMIN: Set your GitHub repo URL here ─────────────────────
# Replace with your actual GitHub token and repo after creating
GITHUB_REPO="https://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@github.com/vgtradinglab/angelone-algo.git"
INSTALL_DIR="angelone-algo"
# ─────────────────────────────────────────────────────────────

clear
echo ""
echo " ============================================"
echo "   MY ALGO — Angel One Edition"
echo "   Installer — please wait..."
echo " ============================================"
echo ""

# ── Step 1: Install system dependencies ──────────────────────
echo " [1/5] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y python3 python3-pip git curl authbind -qq

# Allow port 80 without root
sudo touch /etc/authbind/byport/80
sudo chmod 500 /etc/authbind/byport/80
sudo chown ubuntu /etc/authbind/byport/80

echo " System packages ready ✓"

# Disable auto-updates during market hours
sudo systemctl disable unattended-upgrades 2>/dev/null || true
echo " Auto-updates disabled ✓"

# Setup crontab — daily cleanup + Monday 1 AM IST reboot
(crontab -l 2>/dev/null; echo "5 0 * * * /bin/bash -c 'sync && echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null
10 0 * * * find /tmp -name '*.py' -mtime +1 -delete 2>/dev/null
0 1 * * 0 sudo truncate -s 0 /var/log/btmp 2>/dev/null
30 19 * * 0 sudo reboot") | crontab -
echo " Crontab configured ✓"

# ── Step 2: Clone or update from GitHub ──────────────────────
echo ""
echo " [2/5] Setting up MY ALGO files..."

if [ -d "$INSTALL_DIR/.git" ]; then
    echo " Already installed. Pulling latest from GitHub..."
    cd "$INSTALL_DIR"
    git pull origin main --quiet 2>/dev/null || \
    git pull origin master --quiet 2>/dev/null || true
    echo " Files updated ✓"
else
    echo " Cloning MY ALGO from GitHub..."
    git clone "$GITHUB_REPO" "$INSTALL_DIR" --quiet
    cd "$INSTALL_DIR"
    echo " Files cloned ✓"
fi

# ── Step 3: Install Python dependencies ──────────────────────
echo ""
echo " [3/5] Installing Python dependencies..."
pip3 install -r requirements.txt --break-system-packages --quiet 2>&1 | tail -2
echo " Python dependencies ready ✓"

# ── Step 4: Set permissions ───────────────────────────────────
echo ""
echo " [4/5] Setting permissions..."
chmod +x start.sh install.sh
echo " Permissions set ✓"

# ── Step 5: Create config.json if not exists ─────────────────
echo ""
echo " [5/5] Checking config..."
if [ ! -f "config.json" ]; then
    echo " Creating blank config.json..."
    cat > config.json << 'EOF'
{
  "broker": "angelone",
  "broker_creds": {
    "api_key": "",
    "api_secret": "",
    "access_token": "",
    "request_token": ""
  },
  "strategies": [],
  "paper_trade": true
}
EOF
    echo " config.json created ✓"
else
    echo " config.json already exists — keeping your settings ✓"
fi

# ── Step 6: Setup systemd auto-start ─────────────────────────
echo ""
echo " [6/6] Setting up auto-start service..."
sudo bash -c "cat > /etc/systemd/system/myalgo.service << 'SVCEOF'
[Unit]
Description=MY ALGO Trading Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/angelone-algo
ExecStart=/bin/bash /home/ubuntu/angelone-algo/start.sh
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SVCEOF"
sudo systemctl daemon-reload
sudo systemctl enable myalgo --quiet
sudo systemctl start myalgo
echo " Auto-start service ready ✓"

# ── Done ─────────────────────────────────────────────────────
echo ""
IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")
echo " ============================================"
echo "   Installation complete!"
echo ""
echo "   Bot is starting automatically..."
echo "   Open your browser and go to:"
echo "     http://$IP"
echo ""
echo "   That is it! Bot will start automatically"
echo "   every day — no manual action needed."
echo " ============================================"
echo ""
