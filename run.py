"""
============================================================
  MY ALGO -- ENTRY POINT
  Version : 1.0  --  April 2026

  HOW TO RUN:
    Double-click START.bat
    OR: python run.py
  Then open http://localhost:5000 in browser.
============================================================

  AWS DEPLOYMENT:
    1. Copy all 4 files + logo.png to AWS server
    2. pip install -r requirements.txt
    4. screen -S myalgo
    5. python run.py
    6. Open port 5000 in AWS Security Group
    7. Access: http://<aws-ip>:5000
    8. Ctrl+A then D to detach (keeps running)
============================================================
"""

import os, json, time, threading, logging, glob
from datetime import date

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
    handlers=[
        logging.FileHandler(f"myalgo_{date.today()}.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
_log = logging.getLogger("MyAlgo")

def rotate_logs():
    try:
        files = sorted(glob.glob("myalgo_*.log"))
        for f in files[:-7]:
            try: os.remove(f)
            except: pass
    except: pass

# ── Default config (written to config.json if not exists) ─────
DEFAULT_CONFIG = {
    "dry_run"   : True,
    "port"      : 80,
    "broker"    : "angelone",
    "server_ip" : "",
    "broker_creds": {
        "api_key"       : "",
        "api_secret"    : "",
        "client_code"   : "",
        "password"      : "",
        "totp_key"      : "",
    },
    "telegram_token"      : "",
    "telegram_chat_id"    : "",
    "gmail_user"          : "",
    "gmail_app_password"  : "",
    "gmail_to"            : "",
    "strategies"          : [],
}

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

def load_config() -> dict:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                saved = json.load(f)
            # Merge with defaults to pick up any new fields
            cfg = {**DEFAULT_CONFIG, **saved}
            cfg["broker_creds"] = {**DEFAULT_CONFIG["broker_creds"], **saved.get("broker_creds",{})}
            return cfg
        except Exception as e:
            _log.warning(f"config.json read error: {e} -- using defaults")
    # First run -- write default config
    with open(CONFIG_PATH, "w") as f:
        json.dump(DEFAULT_CONFIG, f, indent=2)
    _log.info(f"Created default config.json at {CONFIG_PATH}")
    return dict(DEFAULT_CONFIG)


def main():
    _log.info("="*58)
    _log.info("  MY ALGO v1.0 -- DIY Strategy Platform")
    _log.info(f"  Date : {date.today()}")
    _log.info("="*58)

    rotate_logs()
    config = load_config()
    dry_run = config.get("dry_run", True)
    port    = int(config.get("port", 5000))
    _log.info(f"Mode : {'PAPER TRADE' if dry_run else 'LIVE TRADE'}")
    _log.info(f"Port : {port}")

    # ── Build engine and notifier ─────────────────────────────
    from engine import Engine, Notifier
    notifier = Notifier(config)
    engine   = Engine(config, notifier, dry_run)

    # ── Wire dashboard ─────────────────────────────────────────
    import dashboard
    # Start P&L thread immediately — records MTM whenever it changes
    import threading as _pt
    if not dashboard._pnl_thread_running:
        dashboard._pnl_thread_running = True
        _pt.Thread(target=dashboard.pnl_thread, name="PnlThread", daemon=True).start()
    dashboard.set_refs(engine, config, CONFIG_PATH)

    # Wire dashboard activity log into engine notifier
    # This lets engine events (ENTRY, SL, RE-ENTRY etc.) appear in the dashboard log
    notifier.log_fn = dashboard.add_log

    # ── Midnight reset thread ──────────────────────────────────
    threading.Thread(target=dashboard.midnight_reset, name="Midnight", daemon=True).start()

    # ── Start Flask dashboard ──────────────────────────────────
    threading.Thread(
        target=dashboard.start_dashboard, args=(port,),
        name="Dashboard", daemon=True).start()
    _log.info(f"Dashboard : http://localhost:{port}")

    # ── Auto-open browser ─────────────────────────────────────
    def _open_browser():
        time.sleep(2)
        try:
            import webbrowser
            webbrowser.open(f"http://localhost:{port}")
        except: pass
    threading.Thread(target=_open_browser, daemon=True).start()

    # ── Notify startup ─────────────────────────────────────────
    _broker_name = {
        "angelone": "Angel One",
    }.get(config.get("broker","angelone").lower(), "Angel One")
    _broker_hint = (
        "Check credentials in Broker Setup, then click Start Algo."
        if config.get("broker","angelone").lower() != "angelone"
        else "Check Angel One credentials in Broker Setup, then click Start Algo."
    )
    # Use public Elastic IP from config if available, else detect dynamically
    try:
        _server_ip = config.get("server_ip","").strip()
        if not _server_ip:
            import socket as _socket
            _server_ip = _socket.gethostbyname(_socket.gethostname())
            if _server_ip.startswith("127."):
                import subprocess as _sp
                _server_ip = _sp.check_output(["hostname", "-I"]).decode().split()[0]
    except Exception:
        _server_ip = "localhost"
    notifier.telegram(
        f"[{config.get('algo_name','My Algo') or 'My Algo'}] Started\n"
        f"Broker: {_broker_name}\n"
        f"Mode: {'PAPER' if dry_run else 'LIVE'}\n"
        f"Dashboard: http://{_server_ip}\n"
        f"{_broker_hint}")
    _log.info("Ready. Open browser and click Start Algo.")

    # ── Keep main thread alive ────────────────────────────────
    try:
        while True: time.sleep(60)
    except KeyboardInterrupt:
        _log.info("Shutdown signal received.")
        engine.stop_all()
        _log.info("Goodbye.")

if __name__ == "__main__":
    main()
