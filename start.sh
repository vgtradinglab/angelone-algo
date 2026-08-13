#!/bin/bash
# ============================================================
#   MY ALGO — Angel One Edition
#   start.sh — Start bot with once-per-day GitHub auto-update
#
#   How update works:
#   - First run of the day → checks GitHub for updates → pulls
#   - Subsequent runs same day → skips update check
#   - Next day first run → checks again
#   - config.json is NEVER touched by update (your settings safe)
# ============================================================

clear
echo ""
echo " ============================================"
echo "   MY ALGO — Angel One Edition"
echo "   Starting up, please wait..."
echo " ============================================"
echo ""

# Change to script directory
cd "$(dirname "$0")"

# ── Check Python3 ─────────────────────────────────────────────
if ! command -v python3 &> /dev/null; then
    echo " ERROR: Python3 is not installed."
    echo " Run: bash install.sh"
    echo ""
    exit 1
fi

# ── Always pull latest from GitHub on every start ─────────────────────────

_do_update() {
    echo " Checking GitHub for updates..."

    # Check if this is a git repo with a remote
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        echo " Skipping update — not a git repository."
        return
    fi

    REMOTE=$(git remote get-url origin 2>/dev/null || echo "")
    if [ -z "$REMOTE" ]; then
        echo " Skipping update — no GitHub remote configured."
        return
    fi

    # Fetch latest from GitHub (silent)
    if ! git fetch origin --quiet 2>/dev/null; then
        echo " Skipping update — could not reach GitHub (no internet?)."
        return
    fi

    LOCAL=$(git rev-parse HEAD 2>/dev/null)
    REMOTE_HEAD=$(git rev-parse origin/main 2>/dev/null || git rev-parse origin/master 2>/dev/null)

    if [ "$LOCAL" = "$REMOTE_HEAD" ]; then
        echo " Already up to date ✓"
    else
        echo " Update found! Pulling latest code..."

        # Stash any local changes (safety)
        git stash --quiet 2>/dev/null || true

        # Pull latest — NEVER touch config.json
        git pull origin main --quiet 2>/dev/null || \
        git pull origin master --quiet 2>/dev/null || true

        # Restore stashed changes if any
        git stash pop --quiet 2>/dev/null || true

        # Reinstall dependencies in case requirements.txt changed
        echo " Updating dependencies..."
        pip3 install -r requirements.txt --break-system-packages --quiet 2>&1 | tail -2

        echo " Update applied successfully ✓"
    fi

}

# Always pull latest from GitHub on every start
_do_update

echo ""

# ── Check dependencies ────────────────────────────────────────
echo " Checking dependencies..."
pip3 install -r requirements.txt --break-system-packages --quiet 2>&1 | tail -2
echo " Dependencies ready."
echo ""

# ── Start the bot ─────────────────────────────────────────────
# Detect IP for dashboard URL
IP=$(hostname -I 2>/dev/null | awk '{print $1}')
if [ -z "$IP" ]; then
    IP="localhost"
fi

echo " ============================================"
echo "   Starting MY ALGO..."
echo "   Dashboard: http://$IP"
echo "   Press Ctrl+C to stop the bot."
echo " ============================================"
echo ""

authbind --deep python3 run.py
