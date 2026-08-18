"""
============================================================
  MY ALGO -- FLASK DASHBOARD + API  v2.0
  Fully synchronised with dashboard_ui.html v2.0
  Handles: strategies, legs, protect profit, advanced
           settings, MCX futures, broker config, alerts,
           live leg view, duplicate, CSV download
============================================================
"""

import json, io, csv, threading, time, os, logging
from datetime import datetime, date, timedelta
from flask import Flask, jsonify, request, send_file, abort, Response, make_response
import auth as _auth

_log = logging.getLogger("Dashboard")

# ── refs set by run.py ───────────────────────────────────────
engine_ref   = None
config_ref   = None
config_path  = "config.json"
trade_log    = []
trade_log_lk = threading.Lock()
pnl_history  = []
pnl_hist_lk  = threading.Lock()

def set_refs(engine, config, cfg_path):
    global engine_ref, config_ref, config_path
    engine_ref  = engine
    config_ref  = config
    config_path = cfg_path
    # Restore basket SL/Target from config — persists across restarts
    global _basket_target, _basket_sl
    _basket_target = float(config.get("basket_target", 0) or 0)
    _basket_sl     = float(config.get("basket_sl",     0) or 0)

    # Clear token_date if it's from a previous day — token expired at midnight
    from datetime import timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    today_ist = datetime.now(ist).strftime("%Y-%m-%d")
    bc = config.get("broker_creds", {})
    if bc.get("token_date","") and bc.get("token_date","") != today_ist:
        _log.info(f"Token from {bc.get('token_date')} expired — clearing token_date.")
        config["broker_creds"]["token_date"] = ""
        save_config()
    # Load historical pnl from disk

    # ── Startup date check — handles local PC shutdown case ───
    today_str = datetime.now().strftime("%Y-%m-%d")
    last_run  = config.get("_last_run_date", "")
    # Reset if: new day OR first ever run (no last_run date)
    if last_run != today_str:
        _log.info(f"New day detected ({last_run or 'first run'} → {today_str}). Resetting intraday strategies to READY.")
        strats = config.get("strategies", [])
        for s in strats:
            trade_type = (s.get("logic") or {}).get("tradeType", "Intraday")
            if trade_type != "Positional":
                s["status"] = "READY"
        config["strategies"] = strats
        config["_last_run_date"] = today_str
        save_config()

    # ── Start midnight reset thread ───────────────────────────
    import threading as _th
    _th.Thread(target=midnight_reset, daemon=True, name="MidnightReset").start()
    _log.info("Midnight reset thread started.")

def save_config():
    try:
        with open(config_path, "w") as f:
            json.dump(config_ref, f, indent=2)
    except Exception as e:
        _log.error(f"save_config: {e}")

# Tags that matter -- everything else is silently dropped from the log
_IMPORTANT_TAGS = {
    "ALGO", "ENTRY", "RE-ENTRY", "RE-COST", "RE-EXECUTE", "RE-EXECUTE ENTRY",
    "LEG SL", "LEG TP", "MTM SL", "MTM TARGET", "SQUAREOFF", "MANUAL EXIT",
    "ERROR", "WARN", "RETRY OK", "RETRY FAILED", "LEG DISABLED",
    "RESTART", "SUMMARY", "BROKER", "FEED",
    "SL EXIT", "TP EXIT",
    # Additional tags wired from engine
    "WAIT&TRADE", "RB ENTRY", "RB BREAKOUT", "RB RANGE",
    "PORTFOLIO SL", "PORTFOLIO TARGET",
    "CONFIG RELOAD", "MARKET NOT READY", "WAITING",
    "ENTRY", "EXIT", "SL", "TARGET",
}

def add_log(tag, sname, body):
    # Filter: only store important tags
    tag_upper = str(tag).upper()
    if tag_upper == "SYSTEM":
        # Allow only specific SYSTEM messages (algo start/stop/restart)
        important_keywords = ["algo", "started", "stopped", "restart",
                              "broker", "error", "failed", "connected",
                              "disconnected", "margin", "exit", "retry"]
        body_lower = str(body).lower()
        if not any(kw in body_lower for kw in important_keywords):
            return  # silently drop unimportant SYSTEM messages
    elif tag_upper not in _IMPORTANT_TAGS:
        return  # drop tags not in important list

    with trade_log_lk:
        trade_log.append({
            "ts"  : datetime.now().strftime("%H:%M:%S"),
            "date": date.today().isoformat(),
            "tag" : tag, "set": sname, "body": body,
        })
        if len(trade_log) > 500:
            trade_log.pop(0)

# Shared variable — always holds latest dashboard Total MTM
# Updated by api_state() on every poll, read by snap_pnl()
_last_dashboard_pnl = 0.0

def snap_pnl():
    """Snapshot current Total MTM to pnl_history every 60 seconds.
    Only records when MTM is non-zero or curve already started.
    Prevents flat zero line before first trade.
    Stops when MTM returns to zero after trading ends.
    Resumes when next strategy starts."""
    global _last_dashboard_pnl
    try:
        if engine_ref and engine_ref.runners:
            _last_dashboard_pnl = sum(r.compute_total_pnl() for r in engine_ref.runners.values())
    except Exception:
        pass
    total = _last_dashboard_pnl
    with pnl_hist_lk:
        has_started = any(p["pnl"] != 0 for p in pnl_history) if pnl_history else False
        # Skip zeros before first trade
        if not has_started and total == 0.0:
            return
        # Skip if MTM unchanged from last recorded value — no flat lines
        last_val = pnl_history[-1]["pnl"] if pnl_history else None
        if last_val is not None and round(total, 2) == last_val:
            return
        if True:
            pnl_history.append({
                "ts" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "pnl": round(total, 2),
            })
            if len(pnl_history) > 1000:
                pnl_history.pop(0)


_pnl_thread_running = False

def pnl_thread():
    """Background thread: snapshots P&L every 60 seconds for the curve chart."""
    import time as _time
    _gc_counter = 0
    while True:
        _time.sleep(60)
        try:
            snap_pnl()
            # Run garbage collection every 5 minutes to free unused memory
            _gc_counter += 1
            if _gc_counter >= 60:
                _gc.collect()
                _gc_counter = 0
        except Exception as e:
            _log.warning(f"pnl_thread: {e}")


# ── Error Alert Manager ───────────────────────────────────────────────────
# Tracks unresolved leg errors per strategy and sends repeating Telegram
# alerts every 60 seconds, up to MAX_ERROR_ALERTS times.
# Stops automatically when the error is resolved (leg retried/skipped/fixed).
# ─────────────────────────────────────────────────────────────────────────
MAX_ERROR_ALERTS  = 3       # send max 3 alerts total then stop permanently
ERROR_ALERT_SLEEP = 60      # 1 alert per minute, 3 minutes, then stop
_error_alert_running = False

# active error alerts: sid -> {"count": int, "msg": str, "leg_ids": set}
_active_errors: dict = {}
_active_errors_lk = threading.Lock()

def _build_error_msg(runner) -> str:
    """Build a Telegram error message from a runner's failed legs."""
    failed = [ls for ls in runner.leg_states
              if ls.failed and not ls.disabled]
    if not failed:
        return ""
    lines = []
    for ls in failed:
        lines.append(f"  • {ls.opt_type} ({ls.action}) — {ls.symbol or 'no fill'}")
    mode = "PAPER" if runner.dry_run else "LIVE"
    return (
        f"⚠️ [LEG ERROR] | {runner.name}  [{mode}]\n\n"
        f"Failed legs:\n" + "\n".join(lines) +
        f"\n\nAction needed: Open dashboard → View (👁) → Retry or Skip."
    )

def error_alert_manager():
    """
    Background thread. Every ERROR_ALERT_SLEEP seconds:
    1. Scan all runners for failed legs.
    2. For new errors → send immediate Telegram + register in _active_errors.
    3. For ongoing errors → repeat Telegram until MAX_ERROR_ALERTS reached.
    4. For resolved errors → remove from _active_errors (alerts stop).
    """
    import time as _t
    while True:
        _t.sleep(ERROR_ALERT_SLEEP)
        if not engine_ref or not engine_ref.running:
            continue
        try:
            current_error_sids = set()
            for sid, runner in list(engine_ref.runners.items()):
                failed = [ls for ls in runner.leg_states
                          if ls.failed and not ls.disabled]
                if not failed:
                    # No failed legs — if we had an active error, it's resolved
                    with _active_errors_lk:
                        if sid in _active_errors:
                            del _active_errors[sid]
                            if engine_ref and engine_ref.notifier:
                                engine_ref.notifier.telegram(
                                    f"✅ [ERROR RESOLVED] | {runner.name}\n\n"
                                    f"All failed legs have been handled.\n"
                                    f"Alerts stopped.")
                    continue

                current_error_sids.add(sid)
                failed_ids = {ls.leg_id for ls in failed}

                with _active_errors_lk:
                    rec = _active_errors.get(sid)
                    if rec is None:
                        # New error — register it (immediate alert already sent
                        # by engine; this starts the repeat cycle from count=1)
                        _active_errors[sid] = {
                            "count"  : 1,
                            "leg_ids": failed_ids,
                        }
                    else:
                        # Ongoing error
                        if rec["count"] >= MAX_ERROR_ALERTS:
                            continue   # alert limit reached — stop repeating
                        rec["count"] += 1
                        rec["leg_ids"] = failed_ids
                        msg = _build_error_msg(runner)
                        if msg and engine_ref and engine_ref.notifier:
                            repeat_msg = (
                                f"{msg}\n"
                                f"(Alert {rec['count']}/{MAX_ERROR_ALERTS} — "
                                f"will stop after {MAX_ERROR_ALERTS} alerts "
                                f"or when resolved)")
                            engine_ref.notifier.telegram(repeat_msg)
                            _log.warning(f"Error alert #{rec['count']} for {runner.name}")
        except Exception as e:
            _log.warning(f"error_alert_manager: {e}")



# Session token flag: True once broker logs in, reset at midnight
_session_active = False
_session_lk     = threading.Lock()

# Basket SL / Target limits (set from dashboard)
_basket_target = 0.0
_basket_sl     = 0.0

# Index previous-close prices for change calculation
# Populated once after broker connects via Kotak quotes API
_index_prev_close = {}   # {"NIFTY": 24500.0, "SENSEX": 80500.0}

PNL_FILE = "pnl_history.json"





def midnight_reset():
    global trade_log, pnl_history, _session_active
    # Reset engine running flag on midnight reset — prevents stuck state
    if engine_ref:
        engine_ref.running = False
    while True:
        now = datetime.now()
        nxt = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
        time.sleep(max((nxt - now).total_seconds(), 1))
        # Clear today's P&L from memory — fresh curve next day
        today_str = datetime.now().strftime("%Y-%m-%d")
        with pnl_hist_lk:
            pnl_history[:] = [p for p in pnl_history
                              if not p.get("ts","").startswith(today_str)]
        # Save trimmed history to disk (no today's data)
        with trade_log_lk:
            trade_log.clear()
        with _session_lk:
            _session_active = False
        # Check for positional strategies still holding positions.
        # Those runners stay alive — their thread wakes next morning at startTime.
        # Only kill engine.running for pure intraday sessions.
        has_positional_holding = False
        if engine_ref:
            for runner in engine_ref.runners.values():
                if (runner.status == "HOLDING" and
                        runner.s.get("logic", {}).get("tradeType") == "Positional"):
                    has_positional_holding = True
                    break
        if engine_ref and not has_positional_holding:
            engine_ref.running = False
            # Clear runner state so fresh strategies can be loaded next day
            engine_ref.runners.clear()
        # Clean up old system journal logs — keep last 2 days only
        try:
            import subprocess
            subprocess.run(["journalctl", "--vacuum-time=2d"],
                          capture_output=True, timeout=30)
            _log.info("Disk cleanup: journal logs vacuumed (kept 2 days)")
        except Exception as _de:
            _log.warning(f"Disk cleanup failed: {_de}")
        # Auto cleanup — all junk, cache, temp, logs
        try:
            import glob, os as _os, shutil, subprocess as _sp
            from datetime import datetime as _dt, timedelta as _td

            # 1. Bot logs older than 7 days
            cutoff = (_dt.now() - _td(days=3)).strftime("%Y-%m-%d")
            for f in glob.glob("/home/ubuntu/angelone-algo/myalgo_*.log"):
                day = _os.path.basename(f).replace("myalgo_","").replace(".log","")
                if day < cutoff:
                    _os.remove(f)
                    _log.info(f"Cleanup: deleted {f}")

            # 2. Blackbox logs and bak files
            for f in glob.glob("/home/ubuntu/myalgo/blackbox_*.log"):
                _os.remove(f)
            for f in glob.glob("/home/ubuntu/myalgo/*.bak") + glob.glob("/home/ubuntu/myalgo/*.bak2"):
                _os.remove(f)

            # 3. Python pycache
            for d in glob.glob("/home/ubuntu/myalgo/__pycache__"):
                shutil.rmtree(d, ignore_errors=True)

            # 4. Temp files in /tmp from bot
            for f in glob.glob("/tmp/*.html") + glob.glob("/tmp/*.js"):
                try: _os.remove(f)
                except: pass

            # 5. System journal — keep 2 days
            _sp.run(["journalctl","--vacuum-time=2d"], capture_output=True, timeout=30)

            # 6. Auth logs — keep 3 days
            _sp.run(["logrotate","--force","/etc/logrotate.d/rsyslog"],
                    capture_output=True, timeout=30)

            # 7. btmp and wtmp — clear weekly (Sunday midnight)
            if _dt.now().weekday() == 6:
                try: open("/var/log/btmp","w").close()
                except: pass
                try: open("/var/log/wtmp","w").close()
                except: pass

            # 8. Git prune — monthly (1st of month)
            if _dt.now().day == 1:
                _sp.run(["git","-C","/home/ubuntu/myalgo","gc","--prune=now","--quiet"],
                        capture_output=True, timeout=60)

            # 9. APT cache — clean daily
            _sp.run(["apt-get","clean"], capture_output=True, timeout=60)

            _log.info("Midnight cleanup complete: logs, cache, temp, journal all cleaned.")
        except Exception as _ce:
            _log.warning(f"Midnight cleanup error: {_ce}")
        # Restart bot at midnight — fresh process every day
        try:
            _log.info("Midnight: restarting bot for fresh connection...")
            import subprocess as _spr
            _spr.Popen(["sudo", "systemctl", "restart", "myalgo"])
        except Exception as _re:
            _log.warning(f"Midnight restart failed: {_re}")

        # Clear auth OTP and sessions at midnight
        try:
            _auth.midnight_reset()
            _log.info("Auth reset: OTP and sessions cleared for new day.")
        except Exception as _ae:
            _log.warning(f"Auth midnight reset error: {_ae}")

        _log.info(
            "Midnight reset complete. PnL history preserved. "
            + ("Positional runners kept alive." if has_positional_holding
               else "Ready for next day."))

# ── Flask app ────────────────────────────────────────────────
app = Flask(__name__)

# ── Auth middleware ───────────────────────────────────────────
def _get_role():
    """Get current user role from cookie. Returns 'admin', 'demo', or None."""
    token = request.cookies.get('myalgo_token')
    role  = request.cookies.get('myalgo_role')
    if not token or not role:
        return None
    return _auth.check_session(token, role)

def _require_auth():
    """Return error response if not logged in."""
    role = _get_role()
    if not role:
        return jsonify({"ok": False, "msg": "Not logged in.", "auth": False}), 401
    return None

def _require_admin():
    """Return error response if not admin."""
    role = _get_role()
    if not role:
        return jsonify({"ok": False, "msg": "Not logged in.", "auth": False}), 401
    if role != 'admin':
        return jsonify({"ok": False, "msg": "Admin access required.", "auth": False}), 403
    return None

# ── Auth API routes ───────────────────────────────────────────
@app.route("/api/auth/send_otp", methods=["POST"])
def api_send_otp():
    data = request.get_json() or {}
    role = data.get("role", "admin")
    if role == "admin":
        return jsonify(_auth.send_admin_otp())
    elif role == "demo":
        return jsonify(_auth.send_demo_otp())
    return jsonify({"ok": False, "msg": "Invalid role."})

@app.route("/api/auth/login", methods=["POST"])
def api_login():
    data  = request.get_json() or {}
    role  = data.get("role", "admin")
    otp   = str(data.get("otp", "")).strip()
    result = _auth.verify_login(role, otp)
    if not result["ok"]:
        return jsonify(result)
    resp = make_response(jsonify(result))
    # Session cookie — expires at midnight
    from datetime import datetime as _dt
    now = _dt.now()
    midnight = now.replace(hour=23, minute=59, second=59)
    if role == "demo" and result.get("expiry"):
        from datetime import datetime as _dt2
        midnight = min(midnight, _dt2.strptime(result["expiry"], "%Y-%m-%d %H:%M:%S"))
    resp.set_cookie("myalgo_token", result["token"],
                    expires=midnight, httponly=True, samesite="Lax")
    resp.set_cookie("myalgo_role", role,
                    expires=midnight, httponly=True, samesite="Lax")
    return resp

@app.route("/api/auth/logout", methods=["POST"])
def api_logout():
    resp = make_response(jsonify({"ok": True}))
    resp.delete_cookie("myalgo_token")
    resp.delete_cookie("myalgo_role")
    return resp

@app.route("/api/auth/status", methods=["GET"])
def api_auth_status():
    role = _get_role()
    if not role:
        demo_status = _auth.get_demo_status()
        return jsonify({"ok": False, "auth": False,
                        "demo_enabled": demo_status["demo_enabled"]})
    demo_status = _auth.get_demo_status() if role == "admin" else {}
    return jsonify({"ok": True, "auth": True, "role": role,
                    "demo_status": demo_status})

@app.route("/api/auth/demo_settings", methods=["POST"])
def api_demo_settings():
    err = _require_admin()
    if err: return err
    data = request.get_json() or {}
    enabled = data.get("enabled", False)
    hours   = data.get("validity_hours", 1)
    return jsonify(_auth.set_demo_settings(enabled, hours))

@app.route("/api/auth/reset_demo", methods=["POST"])
def api_reset_demo():
    err = _require_admin()
    if err: return err
    return jsonify(_auth.reset_demo_otp())

@app.route("/api/auth/demo_status", methods=["GET"])
def api_demo_status():
    err = _require_admin()
    if err: return err
    return jsonify(_auth.get_demo_status())

# ── Gzip compression for all responses ───────────────────────
import gzip as _gzip
import gc as _gc

@app.after_request
def compress_response(response):
    # Only compress text responses larger than 1KB
    if (response.status_code < 200 or response.status_code >= 300
            or response.direct_passthrough):
        return response
    content_type = response.content_type or ""
    if not any(t in content_type for t in ("text/", "application/json", "application/javascript")):
        return response
    if len(response.get_data()) < 1024:
        return response
    accept_encoding = request.headers.get("Accept-Encoding", "")
    if "gzip" not in accept_encoding.lower():
        return response
    compressed = _gzip.compress(response.get_data(), compresslevel=6)
    response.set_data(compressed)
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = len(compressed)
    return response

# Load dashboard HTML from same folder as this file
_ui_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_ui.html")

@app.route("/")
def index():
    if os.path.exists(_ui_path):
        return open(_ui_path, encoding="utf-8").read(), 200, {"Content-Type": "text/html; charset=utf-8"}
    return "<h2>dashboard_ui.html not found. Place it in the same folder as dashboard.py</h2>", 404

@app.route("/logo")
def logo():
    for name in ["logo.png","logo.jpg","logo.jpeg","logo.svg"]:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
        if os.path.exists(p):
            return send_file(p)
    abort(404)

@app.route("/manifest.json")
def manifest():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manifest.json")
    if os.path.exists(p):
        return send_file(p, mimetype="application/json")
    abort(404)

@app.route("/icon-192.png")
def icon192():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon-192.png")
    if os.path.exists(p):
        return send_file(p)
    abort(404)

@app.route("/icon-512.png")
def icon512():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icon-512.png")
    if os.path.exists(p):
        return send_file(p)
    abort(404)

# ── /api/state ───────────────────────────────────────────────
def _fetch_index_prev_close():
    """
    Fetch previous day close for Nifty and Sensex from Kotak quotes API.
    Called once after broker login. Enables accurate change/% display.
    Kotak quotes with quote_type="ohlc" returns pdc (previous day close).
    """
    global _index_prev_close
    if not engine_ref or not engine_ref.broker:
        return
    # Prev-close fetch uses Kotak-specific quotes() API. Skip silently for
    # AngelOne (spot prices still
    # tick via WS so % change just won't render until we wire historical().)
    if not hasattr(engine_ref.broker, "_client"):
        return
    try:
        from engine import INSTRUMENTS
        tokens = []
        idx_map = {}
        for name in ("NIFTY", "SENSEX"):
            info = INSTRUMENTS.get(name, {})
            tok  = info.get("index_token", "")
            exch = info.get("index_exch", "nse_cm")
            if tok:
                tokens.append({"instrument_token": tok, "exchange_segment": exch})
                idx_map[tok] = name
        if not tokens:
            return
        # Call Kotak quotes API with ohlc type
        result = engine_ref.broker._client.quotes(
            instrument_tokens=tokens,
            quote_type="ohlc"
        )
        data = result.get("data", []) if isinstance(result, dict) else (result or [])
        for item in data:
            tok = str(item.get("tk","") or item.get("instrument_token",""))
            # Kotak returns previous day close as "pdc" or "close"
            pdc = float(item.get("pdc") or item.get("c") or item.get("close") or 0)
            if pdc > 0 and tok in idx_map:
                _index_prev_close[idx_map[tok]] = pdc
                _log.info(f"Prev close {idx_map[tok]}: {pdc}")
    except Exception as e:
        _log.warning(f"_fetch_index_prev_close: {e}")


@app.route("/api/instruments")
def api_instruments():
    """
    Return the live instrument list built from AngelOne instrument master.
    Used by the UI to populate the INDEX dropdown dynamically.
    Returns a sorted list of instrument names with their key properties.
    """
    from engine import INSTRUMENTS
    _ALLOWED_MCX = {"CRUDEOIL","CRUDEOILM","NATURALGAS","NATGASMINI"}
    result = []
    for name, info in sorted(INSTRUMENTS.items()):
        # Filter MCX — only show allowed instruments
        if info.get("is_mcx") and name not in _ALLOWED_MCX:
            continue
        result.append({
            "name"        : name,
            "exchange"    : info.get("exchange",""),
            "lot"         : info.get("lot", 1),
            "is_mcx"      : info.get("is_mcx", False),
            "has_options" : info.get("has_options", False),
            "has_weekly"  : info.get("has_weekly", False),
        })
    return jsonify({"ok": True, "instruments": result})


# ── State cache ──────────────────────────────────────────────
_state_cache     = {}
_state_cache_ts  = 0.0
_STATE_CACHE_TTL = 0.8

@app.route("/api/state")
def api_state():
    global _state_cache, _state_cache_ts
    now = time.time()
    if (now - _state_cache_ts) > _STATE_CACHE_TTL:
        _state_cache_ts = now
        _state_cache = engine_ref.get_state() if engine_ref else {
            "running": False, "feed_count": 0, "feed_ts": 0, "strategies": []
        }
    engine_state = _state_cache
    with trade_log_lk: logs = list(trade_log[-80:])
    with pnl_hist_lk:  pnl  = list(pnl_history[-300:])
    # Use live runner compute for freshest MTM (avoids stale get_state snapshot)
    if engine_ref:
        try:
            total_pnl = sum(r.compute_total_pnl() for r in engine_ref.runners.values())
        except Exception:
            total_pnl = sum(s.get("total_pnl", 0) for s in engine_state.get("strategies", []))
    else:
        total_pnl = sum(s.get("total_pnl", 0) for s in engine_state.get("strategies", []))
    # Always keep snap_pnl in sync with dashboard Total MTM
    global _last_dashboard_pnl
    _last_dashboard_pnl = total_pnl

    # Spot prices from engine price_store (index tokens)
    nifty_price  = 0.0; nifty_chg  = 0.0; nifty_pct  = 0.0
    sensex_price = 0.0; sensex_chg = 0.0; sensex_pct = 0.0
    try:
        from engine import price_store, INSTRUMENTS
        np = price_store.get(INSTRUMENTS.get("NIFTY",{}).get("index_ws_key","Nifty 50")) or \
               price_store.get(INSTRUMENTS.get("NIFTY",{}).get("index_token","26000"))
        sp = price_store.get(INSTRUMENTS.get("SENSEX",{}).get("index_ws_key","SENSEX")) or \
               price_store.get(INSTRUMENTS.get("SENSEX",{}).get("index_token","1"))
        if np > 0:
            nifty_price = round(np, 2)
            # Use tracked prev-close for change calculation
            npc = _index_prev_close.get("NIFTY", 0.0)
            if npc > 0:
                nifty_chg = round(nifty_price - npc, 2)
                nifty_pct = round((nifty_chg / npc) * 100, 2)
        if sp > 0:
            sensex_price = round(sp, 2)
            spc = _index_prev_close.get("SENSEX", 0.0)
            if spc > 0:
                sensex_chg = round(sensex_price - spc, 2)
                sensex_pct = round((sensex_chg / spc) * 100, 2)
    except Exception:
        pass

    # Return saved credentials for pre-filling modal (mask MPIN)
    bc = config_ref.get("broker_creds", {}) if config_ref else {}

    # ── Feed health (watchdog) ─────────────────────────────
    feed_healthy = True
    feed_age     = 0.0
    if engine_ref and engine_ref.broker:
        if hasattr(engine_ref.broker, "is_feed_healthy"):
            try:
                feed_healthy = engine_ref.broker.is_feed_healthy()
                feed_age     = engine_ref.broker.feed_age_seconds()
                # If watchdog says unhealthy but price_store has live tokens → treat as healthy
                if not feed_healthy and engine_state.get("feed_count", 0) > 0:
                    feed_healthy = True
            except Exception:
                pass

    # When algo not running — show READY for all intraday strategies
    algo_running  = engine_state.get("running", False)
    raw_strats    = config_ref.get("strategies", []) if config_ref else []
    if not algo_running:
        config_strats = []
        for s in raw_strats:
            s2 = dict(s)
            trade_type = (s2.get("logic") or {}).get("tradeType", "Intraday")
            if trade_type != "Positional":
                s2["status"] = "READY"
            config_strats.append(s2)
    else:
        config_strats = raw_strats

    return jsonify({
        "algo_started"      : engine_state.get("running", False),
        "session_active"    : _session_active,
        "dry_run"           : config_ref.get("dry_run", True) if config_ref else True,
        "feed_count"        : engine_state.get("feed_count", 0),
        "feed_ts"           : engine_state.get("feed_ts", 0),
        "feed_healthy"      : bool(_session_active and engine_state.get("feed_count",0) > 0 and
                                  (time.time() - engine_state.get("feed_ts",0)) < 60),
        "feed_age_seconds"  : int(time.time() - engine_state.get("feed_ts",0)) if engine_state.get("feed_ts",0) else 9999,
        "feed_healthy"      : feed_healthy,
        "feed_age_seconds"  : round(feed_age, 1),
        "total_pnl"         : round(total_pnl, 2),
        "strategies"        : engine_state.get("strategies", []),
        "config_strategies" : config_strats,
        "trade_log"         : logs,
        "pnl_history"       : pnl,
        "broker"            : config_ref.get("broker", "kotak") if config_ref else "kotak",
        # spot prices
        "nifty_price"       : nifty_price,
        "nifty_change"      : nifty_chg,
        "nifty_pct"         : nifty_pct,
        "sensex_price"      : sensex_price,
        "sensex_change"     : sensex_chg,
        "sensex_pct"        : sensex_pct,
        "nifty_atm"         : (round(nifty_price/50)*50) if nifty_price>0 else 0,
        "sensex_atm"        : (round(sensex_price/100)*100) if sensex_price>0 else 0,
        # saved config for pre-filling modals
        "saved_broker"      : {
            "broker"       : config_ref.get("broker","angelone") if config_ref else "angelone",
            "api_key"      : bc.get("api_key",""),
            "client_code"  : bc.get("client_code",""),
            "password"     : bc.get("password",""),
            "totp_key"     : bc.get("totp_key",""),
            "static_ip"    : bc.get("static_ip",""),
            "algo_name"    : config_ref.get("algo_name","My Algo") if config_ref else "My Algo",
            "has_token"    : False,
        },
        "basket_target"     : _basket_target,
        "basket_sl"         : _basket_sl,
        "saved_alerts"      : {
            "telegram_token"       : config_ref.get("telegram_token","") if config_ref else "",
            "telegram_chat_id"     : config_ref.get("telegram_chat_id","") if config_ref else "",
            "gmail_user"           : config_ref.get("gmail_user","") if config_ref else "",
            "gmail_to"             : config_ref.get("gmail_to","") if config_ref else "",
            "important_alerts_only": config_ref.get("important_alerts_only",False) if config_ref else False,
        },
    })

# ── /api/live_legs/<sid> ─────────────────────────────────────
@app.route("/api/live_legs/<int:sid>")
def api_live_legs(sid):
    if not engine_ref:
        return jsonify({"legs": [], "total_pnl": 0})
    runner = engine_ref.runners.get(sid)
    if not runner:
        return jsonify({"legs": [], "total_pnl": 0})
    return jsonify({
        "legs"     : runner.get_live_legs(),
        "total_pnl": round(runner.compute_total_pnl(), 2),
    })

# ── /api/start_algo ──────────────────────────────────────────

def _is_market_open() -> tuple:
    """
    Returns (is_open: bool, market: str, reason: str).
    NOTE: Day-of-week check removed — unreliable (special sessions on Sat/Sun,
    Budget Day etc.). Holiday check is handled by engine.py.
    kite.holidays() API after login. This function only checks time window.
    """
    from datetime import datetime, time
    now = datetime.now()
    t   = now.time()

    nse_open  = time(9, 15)
    nse_close = time(15, 30)
    if nse_open <= t <= nse_close:
        return True, "NSE/BSE", "Market open"

    mcx_open  = time(9, 0)
    mcx_close = time(23, 30)
    if mcx_open <= t <= mcx_close:
        return True, "MCX", "MCX market open"

    if t < nse_open:
        return False, "NSE/BSE", f"Pre-market (NSE opens 09:15)"
    return False, "NSE/BSE", f"Post-market (NSE closed at 15:30)"

@app.route("/api/start_algo", methods=["POST"])
def api_start_algo():
    global _session_active
    if not engine_ref:
        return jsonify({"ok": False, "msg": "Engine not initialised."})
    # Reset stuck running flag if engine threads are dead
    if engine_ref.running:
        import threading as _th
        alive = any(t.name.startswith("strategy_") for t in _th.enumerate())
        if not alive:
            engine_ref.running = False
        else:
            return jsonify({"ok": False, "msg": "Already running."})
    # Send Algo Turned On telegram immediately on manual start
    try:
        import requests as _req
        _tc = config_ref or {}
        _tok = _tc.get("telegram_token","")
        _cid = _tc.get("telegram_chat_id","")
        dry  = _tc.get("dry_run", True)
        mode = "PAPER" if dry else "LIVE"
        if _tok and "YOUR" not in _tok:
            _aname = (_tc.get("algo_name","") or "My Algo").strip()
            _req.post(f"https://api.telegram.org/bot{_tok}/sendMessage",
                      json={"chat_id": _cid,
                            "text": f"✅ {_aname} Turned On | MAIN  [{mode}]"},
                      timeout=5)
    except Exception:
        pass
    def _start():
        global _session_active
        if engine_ref and config_ref:
            engine_ref.config["broker_creds"] = dict(config_ref.get("broker_creds", {}))
            engine_ref.config["broker"] = config_ref.get("broker", "kotak")
        ok, msg = engine_ref.start()
        if not ok:
            # Get actual broker name for error message
            _broker = (config_ref.get("broker","kotak") if config_ref else "kotak").title()
            _broker_name = {
                "kotak"  : "Kotak Neo",
                "angelone": "AngelOne",
                "dhan"   : "Dhan",
                "upstox" : "Upstox",
                "angel"  : "Angel One",
            }.get(config_ref.get("broker","kotak").lower() if config_ref else "kotak", _broker)

            add_log("ERROR", "MAIN", f"Start failed: {msg}")
            if engine_ref and engine_ref.notifier:
                if "login" in msg.lower() or "credential" in msg.lower() or "token" in msg.lower():
                    engine_ref.notifier.telegram(
                        f"[LOGIN FAILED] | MAIN\n\n"
                        f"⚠️ {_broker_name} login failed.\n"
                        f"Please check credentials in Broker Setup.\n\n"
                        f"Reason: {msg}")
                else:
                    engine_ref.notifier.telegram(
                        f"[START FAILED] | MAIN\n\n"
                        f"⚠️ Bot failed to start.\n{msg}")
            return
        with _session_lk:
            _session_active = True
        # Start P&L snapshot thread for the curve chart (only once)
        global _pnl_thread_running, _error_alert_running
        if not _pnl_thread_running:
            _pnl_thread_running = True
            import threading as _thr
            _thr.Thread(target=pnl_thread, name="PnlThread", daemon=True).start()
        if not _error_alert_running:
            _error_alert_running = True
            import threading as _thr2
            _thr2.Thread(target=error_alert_manager,
                         name="ErrorAlertMgr", daemon=True).start()

        # Fetch index prev-close prices for change display
        try:
            _fetch_index_prev_close()
        except Exception as _e:
            _log.warning(f"Prev close fetch: {_e}")

        # Refresh INSTRUMENTS from live AngelOne data
        try:
            from engine import refresh_instruments_from_broker
            refresh_instruments_from_broker(engine_ref.broker)
        except Exception as _e:
            _log.warning(f"refresh_instruments_from_broker: {_e}")

        # Load all enabled strategies
        strats  = config_ref.get("strategies", [])
        enabled = [s for s in strats if s.get("enabled", True)]

        # Clear stale runners from previous session
        if engine_ref.runners:
            _log.info("Clearing stale runners from previous session.")
            for runner in engine_ref.runners.values():
                runner.stopped = True
            engine_ref.runners.clear()

        if enabled:
            engine_ref.load_strategies(enabled)
        else:
            _log.info("No enabled strategies — ready for dynamic strategy creation.")

        # AngelOne — tokens managed internally by adapter
        save_config()

        add_log("ALGO", "MAIN",
                f"Algo started | {len(enabled)} strategies | "
                f"Mode: {'PAPER' if config_ref.get('dry_run', True) else 'LIVE'}")
    threading.Thread(target=_start, daemon=True).start()
    return jsonify({"ok": True})

# ── /api/stop_algo ───────────────────────────────────────────
@app.route("/api/stop_algo", methods=["POST"])
def api_stop_algo():
    if engine_ref:
        engine_ref.stop_all()
        # Mark engine as not running so dashboard shows "Stopped" state.
        # session_active stays True so Reconnect button remains visible.
        engine_ref.running = False
    add_log("ALGO", "MAIN", "Algo stopped from dashboard.")
    return jsonify({"ok": True})

# ── /api/exit_strategy/<sid> ─────────────────────────────────
@app.route("/api/exit_strategy/<int:sid>", methods=["POST"])
def api_exit_strategy(sid):
    if engine_ref:
        engine_ref.stop_strategy(sid)
    return jsonify({"status": f"Exit triggered for strategy {sid}"})

# ── /api/exit_all ────────────────────────────────────────────
@app.route("/api/exit_all", methods=["POST"])
def api_exit_all():
    # EXIT ALL: exits positions ONLY — engine keeps running
    if engine_ref:
        engine_ref.exit_all_positions()
    data   = request.get_json(silent=True) or {}
    reason = data.get("reason", "")
    if reason:
        add_log("EXIT", "MAIN", reason)
    else:
        add_log("MANUAL EXIT", "MAIN", "Dashboard EXIT ALL pressed.")
    return jsonify({"status": "Exit triggered for all strategies"})

# ── /api/strategies/reorder ─────────────────────────────────
@app.route("/api/strategies/reorder", methods=["POST"])
def api_reorder_strategy():
    """Drag-drop reorder: move src_id to position of tgt_id."""
    data   = request.get_json() or {}
    src_id = data.get("src_id")
    tgt_id = data.get("tgt_id")
    # Also support simple up/down for button fallback
    sid       = data.get("id")
    direction = data.get("direction", "up")
    strats = config_ref.get("strategies", [])
    if src_id is not None and tgt_id is not None:
        src_idx = next((i for i,s in enumerate(strats) if s["id"]==src_id), None)
        tgt_idx = next((i for i,s in enumerate(strats) if s["id"]==tgt_id), None)
        if src_idx is None or tgt_idx is None:
            return jsonify({"ok": False, "msg": "Strategy not found"})
        moved = strats.pop(src_idx)
        strats.insert(tgt_idx, moved)
    elif sid is not None:
        idx = next((i for i,s in enumerate(strats) if s["id"]==sid), None)
        if idx is None:
            return jsonify({"ok": False, "msg": "Strategy not found"})
        if direction == "up" and idx > 0:
            strats[idx], strats[idx-1] = strats[idx-1], strats[idx]
        elif direction == "down" and idx < len(strats)-1:
            strats[idx], strats[idx+1] = strats[idx+1], strats[idx]
    config_ref["strategies"] = strats
    save_config()
    return jsonify({"ok": True})

# ── /api/retry_leg/<sid>/<lid> ───────────────────────────────
@app.route("/api/retry_leg/<int:sid>/<int:lid>", methods=["POST"])
def api_retry_leg(sid, lid):
    if engine_ref and sid in engine_ref.runners:
        engine_ref.runners[sid].request_retry(lid)
        return jsonify({"status": "Retry queued"})
    return jsonify({"status": "Strategy not active"})

# ── /api/retry_exit/<sid>/<lid> ──────────────────────────────
@app.route("/api/retry_exit/<int:sid>/<int:lid>", methods=["POST"])
def api_retry_exit(sid, lid):
    """Retry a failed exit order — user clicked Retry Exit in dashboard."""
    if engine_ref and sid in engine_ref.runners:
        engine_ref.runners[sid].request_action(lid, "retry_exit")
        return jsonify({"status": "Exit retry queued"})
    return jsonify({"status": "Strategy not active"})


# ── /api/disable_leg/<sid>/<lid> ─────────────────────────────
@app.route("/api/disable_leg/<int:sid>/<int:lid>", methods=["POST"])
def api_disable_leg(sid, lid):
    if engine_ref and sid in engine_ref.runners:
        engine_ref.runners[sid].request_disable_leg(lid)
        return jsonify({"status": "Leg disabled"})
    return jsonify({"status": "Strategy not active"})

# ── /api/strategies/<id>/mult  POST ─────────────────────────
@app.route("/api/strategies/<int:sid>/mult", methods=["POST"])
def api_set_mult(sid):
    data = request.get_json()
    mult = str(data.get("mult", "1X")).upper().strip()
    if not mult.endswith("X"):
        mult = mult + "X"
    strats = config_ref.get("strategies", [])
    for s in strats:
        if s.get("id") == sid:
            s["mult"] = mult
            config_ref["strategies"] = strats
            save_config()
            # Update running runner immediately
            if engine_ref and sid in engine_ref.runners:
                engine_ref.runners[sid].s["mult"] = mult
            return jsonify({"ok": True, "mult": mult})
    return jsonify({"ok": False, "msg": "Strategy not found"}), 404


def api_get_strategies():
    return jsonify(config_ref.get("strategies", []) if config_ref else [])

# ── /api/strategies  POST (create or update) ─────────────────
@app.route("/api/strategies", methods=["POST"])
def api_save_strategy():
    s = request.get_json()
    if not s:
        return jsonify({"ok": False, "msg": "No data received"}), 400

    # Validate required fields
    if not s.get("name"):
        return jsonify({"ok": False, "msg": "Strategy name is required"}), 400

    # Ensure all expected fields exist with defaults
    s.setdefault("segment",  "Options")
    s.setdefault("mult",     "1X")
    s.setdefault("sqOffMode","one")
    s.setdefault("status",   "READY")
    s.setdefault("enabled",  True)
    s.setdefault("mtm",      0)
    s.setdefault("openLegs", 0)
    s.setdefault("days",     [True,True,True,True,True,False,False])
    s.setdefault("legs",     [])
    s.setdefault("logic",    {})
    s.setdefault("protect",  {})
    s.setdefault("advanced", {})

    # Validate and normalise logic block
    logic = s["logic"]
    logic.setdefault("underlying",  "Spot")
    logic.setdefault("tradeType",   "Intraday")
    logic.setdefault("startTime",   "09:20:00")
    # Default exit time depends on exchange:
    # MCX: 23:00:00 (commodity market closes at 23:30)
    # NSE/BSE: 15:15:00 (equity market closes at 15:30)
    from engine import INSTRUMENTS as _INSTR
    _is_mcx = _INSTR.get(s.get("idx",""), {}).get("is_mcx", False)
    logic.setdefault("endTime", "23:00:00" if _is_mcx else "15:15:00")
    logic.setdefault("flags",       [])
    logic.setdefault("mtmTarget",   0)
    logic.setdefault("mtmSL",       0)
    logic.setdefault("mtmTType",    "Amount (₹)")
    logic.setdefault("mtmSLType",   "Amount (₹)")

    # Validate and normalise each leg
    for leg in s.get("legs", []):
        leg.setdefault("action",  "SELL")
        leg.setdefault("prod",    "MIS")
        leg.setdefault("type",    "CE")
        leg.setdefault("lots",    1)
        leg.setdefault("baseLots",1)
        leg.setdefault("stType",  "Strike Type")
        leg.setdefault("strike",  "ATM")
        leg.setdefault("premVal", 0)
        leg.setdefault("tp",      40)
        leg.setdefault("tpU",     "%")
        leg.setdefault("sl",      20)
        leg.setdefault("slU",     "%")
        leg.setdefault("tslConfig", None)
        leg.setdefault("resl",    1)
        leg.setdefault("expiry",  "Weekly")
        # Ensure slU preserves UL modes — do not override with default
        if leg.get("slU") not in ("%", "Pts", "UL %", "UL Pts"):
            leg["slU"] = "%"
        # Force leg type to FUT only when the strategy segment is Futures.
        # MCX instruments with options (CRUDEOIL, NATURALGAS, GOLD etc.) can
        # have CE/PE legs — do NOT override those to FUT.
        if s.get("segment") == "Futures":
            leg["type"] = "FUT"

    # Validate protect profit block
    protect = s.get("protect", {})
    protect.setdefault("mode",       "")
    protect.setdefault("lockReach",  0)
    protect.setdefault("lockAt",     0)
    protect.setdefault("trailReach", 0)
    protect.setdefault("trailBy",    0)
    s["protect"] = protect

    # Validate advanced settings block
    adv = s.get("advanced", {})
    adv.setdefault("entryOrderType", "Limit / SL-Limit")
    adv.setdefault("entryBufferIn",  "Percentage")
    adv.setdefault("entryBufferVal", 10)
    adv.setdefault("exitOrderType",  "Limit / SL-Limit")
    adv.setdefault("exitBufferIn",   "Percentage")
    adv.setdefault("exitBufferVal",  10)
    adv.setdefault("autoHandleSL",   True)
    adv.setdefault("autoExitMargin", False)
    s["advanced"] = adv

    strats = config_ref.get("strategies", [])
    idx    = next((i for i, x in enumerate(strats) if x["id"] == s["id"]), None)
    if idx is not None:
        strats[idx] = s
        msg = f"Strategy '{s['name']}' updated."
    else:
        strats.append(s)
        msg = f"Strategy '{s['name']}' created."

    config_ref["strategies"] = strats
    save_config()
    # Do NOT log strategy created/updated -- these clutter the activity log
    # Only important trading events are logged (errors, trades, algo start/stop)

    # Hot-reload: update running strategy if live, or start new runner
    if engine_ref and engine_ref.running:
        if s["id"] in engine_ref.runners:
            runner_existing = engine_ref.runners[s["id"]]
            old_idx = runner_existing.s.get("idx", "")
            new_idx = s.get("idx", "")
            # If instrument changed while running — cannot hot-reload.
            # The runner already has old chain + old WebSocket subscriptions.
            # User must stop and restart for instrument change to take effect.
            if old_idx and new_idx and old_idx != new_idx:
                _log.warning(
                    f"Config save: instrument changed {old_idx} → {new_idx} "
                    f"while strategy '{s.get('name','')}' is running. "
                    f"Config saved to disk. Stop and restart strategy for change to take effect.")
                # Save config to disk (already done above) but do NOT update runner
                # The runner keeps running with old instrument until restarted
                pass
            else:
                # Same instrument — safe to hot-reload config in-place
                runner_existing.s = s
                # Signal runner that config changed so it resets stale state
                # (rangeBreak high/low/window-done counters etc.) on next tick
                runner_existing._config_changed = True
        elif not (idx is not None) and s.get("enabled", False):
            # NEW strategy added while algo is running -- start it immediately
            # Only if enabled=True (user toggled ON or created with enabled flag)
            try:
                from engine import StrategyRunner, OrderManager
                chain = engine_ref._option_chains.get(s.get("idx","NIFTY"), [])
                if not chain:
                    # fetch chain for this instrument
                    from engine import nearest_expiry_from_broker, nearest_expiry, expiry_fmt, INSTRUMENTS
                    info   = INSTRUMENTS.get(s.get("idx","NIFTY"), {})
                    # Collect all unique expiry types from legs
                    _leg_expiries = set()
                    for _lg in s.get("legs", []):
                        _et = str(_lg.get("expiry","Weekly")).lower().replace(" ","_")
                        _emap = {"weekly":"weekly","next_weekly":"next_weekly",
                                 "monthly":"monthly","next_month":"next_month"}
                        _et_m = _emap.get(_et, "weekly")
                        if info.get("is_mcx") and _et_m in ("weekly","next_weekly"):
                            _et_m = "monthly"
                        _leg_expiries.add(_et_m)
                    _combined_chain = []
                    for _et_m in _leg_expiries:
                        exp_str = nearest_expiry_from_broker(engine_ref.broker, s.get("idx","NIFTY"), _et_m)
                        if not exp_str:
                            exp_str = expiry_fmt(nearest_expiry(s.get("idx","NIFTY"), _et_m))
                        if exp_str:
                            _c = engine_ref.broker.get_option_chain(s.get("idx","NIFTY"), exp_str)
                            _combined_chain.extend(_c)
                    chain = _combined_chain
                    engine_ref._option_chains[s.get("idx","NIFTY")] = chain
                om = OrderManager(engine_ref.broker, engine_ref.dry_run,
                                  config_ref, notifier=engine_ref.notifier)
                runner = StrategyRunner(s, engine_ref.broker, om,
                                        engine_ref.notifier, engine_ref.dry_run)
                engine_ref.runners[s["id"]] = runner
                # ── Subscribe WS BEFORE starting thread (fix race condition) ──
                # pSymbol is the correct Kotak token field (not pToken)
                try:
                    from engine import price_store
                    import time as _time
                    opt_toks = []; idx_toks = []
                    for ch in engine_ref._option_chains.values():
                        for item in ch:
                            ptoken = str(item.get("instrument_token","") or item.get("pSymbol","") or "")
                            exch   = str(item.get("exchange_segment","") or item.get("exchange","") or item.get("pExchSeg","NFO") or "NFO")
                            if not ptoken: continue
                            tok_entry = {"instrument_token": ptoken, "exchange_segment": exch}
                            if exch in ("nse_cm","bse_cm"):
                                idx_toks.append(tok_entry)
                            else:
                                opt_toks.append(tok_entry)
                    if opt_toks:
                        engine_ref.broker.subscribe_feed(
                            opt_toks, lambda tok, ltp: price_store.update(tok, ltp))
                        _log.info(f"[WS] Hot-start: subscribed {len(opt_toks)} option tokens")
                    if idx_toks and hasattr(engine_ref.broker, "subscribe_index_feed"):
                        engine_ref.broker.subscribe_index_feed(idx_toks)
                        _log.info(f"[WS] Hot-start: subscribed {len(idx_toks)} index tokens")
                    # Wait briefly for prices to arrive before strategy enters
                    _time.sleep(2)
                except Exception as _ws_e:
                    _log.warning(f"Hot-start WS re-subscribe: {_ws_e}")
                import threading
                t = threading.Thread(target=runner.run, args=(chain,),
                                     name=f"Strat_{s['id']}", daemon=True)
                engine_ref._threads[s["id"]] = t
                t.start()
                add_log("ALGO", s["name"], f"Strategy auto-started (added while algo running)")
            except Exception as _e:
                _log.warning(f"Auto-start new strategy: {_e}")

    return jsonify({"ok": True, "msg": msg})

# ── /api/strategies/<sid>  DELETE ────────────────────────────
@app.route("/api/strategies/<int:sid>", methods=["DELETE"])
def api_delete_strategy(sid):
    strats = config_ref.get("strategies", [])
    config_ref["strategies"] = [x for x in strats if x["id"] != sid]
    if engine_ref and sid in engine_ref.runners:
        engine_ref.stop_strategy(sid)
    save_config()
    return jsonify({"ok": True})

# ── /api/strategies/<sid>/toggle ─────────────────────────────
@app.route("/api/strategies/<int:sid>/toggle", methods=["POST"])
def api_toggle_strategy(sid):
    strats = config_ref.get("strategies", [])
    for s in strats:
        if s["id"] == sid:
            was_enabled = s.get("enabled", True)
            s["enabled"] = not was_enabled

            # ── Holiday check when turning ON ─────────────────────────
            if s["enabled"] and not was_enabled:
                # Reset MTM SL breach flag so fresh alert fires on next breach
                if engine_ref and sid in engine_ref.runners:
                    engine_ref.runners[sid]._mtm_sl_breach_warned = False
                try:
                    from engine import INSTRUMENTS
                    from datetime import date as _date
                    import requests as _req

                    instr_chk  = s.get("idx","NIFTY")
                    info_chk   = INSTRUMENTS.get(instr_chk, {})
                    is_mcx_chk = info_chk.get("is_mcx", False)
                    s_exch_chk = "MCX" if is_mcx_chk else ("BSE" if info_chk.get("exchange","") in ("BSE","BFO") else "NSE")
                    today_chk  = _date.today().isoformat()

                    # AngelOne — no kite object needed
                    if engine_ref and engine_ref.broker:
                        pass

                    if False:  # kite.holidays() not available — placeholder for future holiday check
                        pass
                    if False:  # AngelOne — no kite object
                        hname = ""
                        if False:
                                # Send Telegram alert
                                _tc  = config_ref or {}
                                _tok = _tc.get("telegram_token","")
                                _cid = _tc.get("telegram_chat_id","")
                                if _tok and "YOUR" not in _tok:
                                    _req.post(
                                        f"https://api.telegram.org/bot{_tok}/sendMessage",
                                        json={"chat_id": _cid,
                                              "text": f"🏖 Market Holiday\n\n"
                                                      f"{s_exch_chk} is closed today ({hname}).\n"
                                                      f"Strategy '{s.get('name','')}' not started."},
                                        timeout=5)
                                # Revert toggle — don't enable on holiday
                                s["enabled"] = False
                                save_config()
                                return jsonify({"ok": True})
                    else:
                        # Not logged in yet — no alert needed.
                        # User toggled strategy ON themselves — no notification required.
                        pass
                except Exception as _he:
                    _log.warning(f"Toggle holiday check error: {_he}")
            # ── End holiday check ──
            if not s["enabled"] and engine_ref and sid in engine_ref.runners:
                # Turning OFF: stop the runner
                engine_ref.stop_strategy(sid)
            elif s["enabled"] and not was_enabled:
                # Turning ON: hot-start if algo is running and not already running
                if engine_ref and engine_ref.running and sid not in engine_ref.runners:
                    def _hotstart_on_toggle(strat=s):
                        try:
                            from engine import StrategyRunner, OrderManager, price_store, INSTRUMENTS
                            import time as _t
                            from datetime import date as _date

                            # ── Holiday check before starting toggled strategy ──
                            instr_chk  = strat.get("idx","NIFTY")
                            info_chk   = INSTRUMENTS.get(instr_chk, {})
                            is_mcx_chk = info_chk.get("is_mcx", False)
                            s_exch_chk = "MCX" if is_mcx_chk else ("BSE" if info_chk.get("exchange","") in ("BSE","BFO") else "NSE")
                            today_chk  = _date.today().isoformat()

                            # Get kite object from broker (available after login)
                            _kite = None
                            try:
                                _kite = engine_ref.broker._kite if hasattr(engine_ref.broker,'_kite') else None
                                if _kite is None:
                                    _kite = engine_ref.broker.kite if hasattr(engine_ref.broker,'kite') else None
                            except Exception:
                                pass

                            _is_holiday = False
                            _hname = ""
                            # holiday check not available — placeholder
                            # Holiday detection will be implemented after testing on actual holiday.

                            if _is_holiday:
                                _log.warning(f"Toggle ON blocked — {s_exch_chk} holiday: {_hname}")
                                # Send Telegram directly
                                try:
                                    import requests as _req
                                    _tc  = config_ref or {}
                                    _tok = _tc.get("telegram_token","")
                                    _cid = _tc.get("telegram_chat_id","")
                                    if _tok and "YOUR" not in _tok:
                                        _req.post(
                                            f"https://api.telegram.org/bot{_tok}/sendMessage",
                                            json={"chat_id": _cid,
                                                  "text": f"🏖 Market Holiday\n\n"
                                                          f"{s_exch_chk} is closed today ({_hname}).\n"
                                                          f"Strategy '{strat.get('name','')}' not started."},
                                            timeout=5)
                                except Exception:
                                    pass
                                return  # block strategy start
                            # ── End holiday check ──
                            instr = strat.get("idx","NIFTY")
                            chain = engine_ref._option_chains.get(instr, [])
                            if not chain:
                                from engine import nearest_expiry_from_broker, nearest_expiry, expiry_fmt
                                info = INSTRUMENTS.get(instr,{})
                                et   = "monthly" if info.get("is_mcx") else "weekly"
                                exp  = nearest_expiry_from_broker(engine_ref.broker, instr, et)
                                if not exp: exp = expiry_fmt(nearest_expiry(instr, et))
                                chain = engine_ref.broker.get_option_chain(instr, exp)
                                engine_ref._option_chains[instr] = chain
                            # Subscribe tokens first
                            opt_toks=[]; idx_toks=[]
                            for item in chain:
                                pt = str(item.get("pSymbol","") or "")
                                ex = str(item.get("pExchSeg","nse_fo") or "nse_fo")
                                if not pt: continue
                                if ex in ("nse_cm","bse_cm"): idx_toks.append({"instrument_token":pt,"exchange_segment":ex})
                                else: opt_toks.append({"instrument_token":pt,"exchange_segment":ex})
                            if opt_toks: engine_ref.broker.subscribe_feed(opt_toks, lambda tok,ltp: price_store.update(tok,ltp))
                            if idx_toks and hasattr(engine_ref.broker,"subscribe_index_feed"): engine_ref.broker.subscribe_index_feed(idx_toks)
                            _t.sleep(2)  # wait for prices
                            om = OrderManager(engine_ref.broker, engine_ref.dry_run, config_ref, notifier=engine_ref.notifier)
                            runner = StrategyRunner(strat, engine_ref.broker, om, engine_ref.notifier, engine_ref.dry_run)
                            engine_ref.runners[strat["id"]] = runner
                            import threading as _thr
                            _thr.Thread(target=runner.run, args=(chain,), name=f"Strat_{strat['id']}", daemon=True).start()
                            add_log("ALGO", strat["name"], "Strategy started (toggle ON)")
                        except Exception as _e:
                            _log.warning(f"Toggle-ON hot-start: {_e}")
                    import threading as _th
                    _th.Thread(target=_hotstart_on_toggle, daemon=True).start()
            break
    save_config()
    return jsonify({"ok": True})

# ── /api/restart_strategy/<sid> ───────────────────────────────
@app.route("/api/restart_strategy/<int:sid>", methods=["POST"])
def api_restart_strategy(sid):
    """
    Re-start a strategy that is EXITED or CLOSED.
    Only works if algo is running (engine active).
    Creates a fresh StrategyRunner for the same strategy and starts it.
    """
    if not engine_ref or not engine_ref.running:
        return jsonify({"ok": False, "msg": "Algo not running. Start Algo first."})

    strats = config_ref.get("strategies", [])
    s = next((x for x in strats if x["id"] == sid), None)
    if not s:
        return jsonify({"ok": False, "msg": "Strategy not found."})

    # Check current runner status
    runner = engine_ref.runners.get(sid)
    if runner and runner.status not in ("EXITED", "CLOSED", "DISABLED", "ERROR"):
        return jsonify({"ok": False, "msg": f"Strategy is {runner.status}. Can only restart EXITED/CLOSED/ERROR strategies."})

    def _restart():
        try:
            from engine import StrategyRunner, OrderManager
            instr = s.get("idx","NIFTY")
            # Always rebuild chain fresh from current leg expiry settings
            from engine import INSTRUMENTS, nearest_expiry, expiry_fmt, nearest_expiry_from_broker
            info = INSTRUMENTS.get(instr, {})
            _leg_expiries = set()
            for _lg in s.get("legs", []):
                _et = str(_lg.get("expiry","Weekly")).lower().replace(" ","_")
                _emap = {"weekly":"weekly","next_weekly":"next_weekly",
                         "monthly":"monthly","next_month":"next_month"}
                _et_m = _emap.get(_et, "weekly")
                if info.get("is_mcx") and _et_m in ("weekly","next_weekly"):
                    _et_m = "monthly"
                _leg_expiries.add(_et_m)
            _combined_chain = []
            for _et_m in _leg_expiries:
                _exp = nearest_expiry_from_broker(engine_ref.broker, instr, _et_m)
                if not _exp:
                    _exp = expiry_fmt(nearest_expiry(instr, _et_m))
                if _exp:
                    _c = engine_ref.broker.get_option_chain(instr, _exp)
                    _combined_chain.extend(_c)
            chain = _combined_chain if _combined_chain else engine_ref._option_chains.get(instr, [])
            engine_ref._option_chains[instr] = chain
            om = OrderManager(engine_ref.broker, engine_ref.dry_run, config_ref)
            new_runner = StrategyRunner(s, engine_ref.broker, om,
                                        engine_ref.notifier, engine_ref.dry_run)
            engine_ref.runners[sid] = new_runner
            import threading
            t = threading.Thread(target=new_runner.run, args=(chain,),
                                 name=f"Restart_{sid}", daemon=True)
            engine_ref._threads[sid] = t
            t.start()
            # Re-subscribe WS after restart so leg tokens are live
            try:
                from engine import price_store
                all_toks = []
                for ch in engine_ref._option_chains.values():
                    for item in ch:
                        ptoken = str(item.get("pToken","") or item.get("token","") or "")
                        if ptoken: all_toks.append({"instrument_token": ptoken,
                                                    "exchange_segment": item.get("pExchSeg","nse_fo")})
                if all_toks:
                    engine_ref.broker.subscribe_feed(
                        all_toks, lambda tok, ltp: price_store.update(tok, ltp))
            except Exception as _e2:
                _log.warning(f"Restart WS re-subscribe: {_e2}")
            add_log("RESTART", "MAIN", f"Strategy '{s['name']}' restarted by user.")
        except Exception as e:
            add_log("ERROR", "MAIN", f"Restart failed for '{s.get('name',sid)}': {e}")

    import threading as _th
    _th.Thread(target=_restart, daemon=True).start()
    return jsonify({"ok": True, "msg": f"Restart triggered for '{s['name']}'"})

# ── /api/config/broker ───────────────────────────────────────
@app.route("/api/config/broker", methods=["POST"])
def api_save_broker():
    err = _require_admin()
    if err: return err
    data = request.get_json() or {}
    # broker selection
    if "broker" in data:
        config_ref["broker"] = data["broker"]
    # credentials
    bc = config_ref.setdefault("broker_creds", {})
    ALL_BROKER_FIELDS = [
        "api_key","client_code","password","totp_key",
        "angel_api_key","angel_client_id","angel_password","angel_totp_key",
        "totp","pin","app_id","app_secret","resecret",
    ]
    for k in ALL_BROKER_FIELDS:
        if k in data and str(data.get(k,"")).strip():
            bc[k] = str(data[k]).strip()
    new_mpin = str(data.get("mpin","")).strip()
    if new_mpin and not all(c=="•" for c in new_mpin):
        bc["mpin"] = new_mpin
    if "static_ip" in data and data["static_ip"]:
        bc["static_ip"] = data["static_ip"]
        config_ref["server_ip"] = data["static_ip"]
    if "server_ip" in data and data["server_ip"]:
        config_ref["server_ip"] = data["server_ip"]
    if "broker" in data and data["broker"]:
        config_ref["broker"] = data["broker"]
    if "algo_name" in data and str(data.get("algo_name","")).strip():
        config_ref["algo_name"] = str(data["algo_name"]).strip()
    save_config()
    if engine_ref:
        engine_ref.config["broker_creds"] = dict(bc)
        engine_ref.config["broker"] = config_ref.get("broker","angelone")
    # hot-reload credentials into running engine
    if engine_ref and engine_ref.notifier:
        engine_ref.notifier.cfg.update(config_ref)
    return jsonify({"ok": True, "msg": "Broker config saved."})

# ── /api/zerodha/save_token — NOT USED IN ANGELONE ──────────
@app.route("/api/zerodha/save_token_disabled", methods=["POST"])
def api_save_token():
    err = _require_admin()
    if err: return err
    data = request.get_json() or {}
    req_token = data.get("request_token","").strip()
    if not req_token:
        return jsonify({"ok": False, "msg": "No request_token provided."})
    try:
        pass  # AngelOne — not used
        if engine_ref and engine_ref.broker:
            engine_ref.broker._access_token = session_data["access_token"]
            engine_ref.broker._kite.set_access_token(session_data["access_token"])
        add_log("BROKER", "MAIN", "Access token saved successfully via First Time Setup.")
        return jsonify({"ok": True, "msg": "Token saved! Click Start Algo to begin."})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

# ── /api/config/alerts ───────────────────────────────────────
@app.route("/api/config/alerts", methods=["POST"])
def api_save_alerts():
    data = request.get_json() or {}
    for k in ["telegram_token","telegram_chat_id",
              "gmail_user","gmail_app_password","gmail_to"]:
        if k in data and data[k]:
            config_ref[k] = str(data[k])
    # Save important_alerts_only toggle (boolean)
    config_ref["important_alerts_only"] = bool(data.get("important_alerts_only", False))
    save_config()
    # hot-reload into notifier
    if engine_ref and engine_ref.notifier:
        engine_ref.notifier.cfg.update(config_ref)
        _an = (config_ref.get("algo_name","") or "My Algo").strip()
        engine_ref.notifier.telegram(f"[{_an}] Alert config saved & tested.")
    return jsonify({"ok": True, "msg": "Alert config saved."})

# ── /api/config/mode ─────────────────────────────────────────
@app.route("/api/config/mode", methods=["POST"])
def api_toggle_mode():
    data = request.get_json() or {}
    if "dry_run" in data:
        new_dry_run = bool(data["dry_run"])
        config_ref["dry_run"] = new_dry_run
        save_config()

        # ── Critical: update engine and ALL running runners ──────
        # Without this, mode toggle only changes config but
        # existing runners keep trading in old mode ❌
        if engine_ref is not None:
            engine_ref.dry_run = new_dry_run
            # Update all active strategy runners
            for runner in engine_ref.runners.values():
                runner.dry_run = new_dry_run
                # Update their OrderManager too
                if hasattr(runner, 'om') and runner.om is not None:
                    runner.om.dry_run = new_dry_run
            _log.info(
                f"Mode switched to {'PAPER' if new_dry_run else 'LIVE'} — "
                f"updated {len(engine_ref.runners)} active runners.")
            # Notify via Telegram
            if engine_ref.notifier:
                mode_str = "PAPER TRADE" if new_dry_run else "LIVE TRADE"
                _an2 = (config_ref.get("algo_name","") or "My Algo").strip()
                engine_ref.notifier.telegram(
                    "[" + _an2 + "] Mode switched to " + mode_str + "\n"
                    "All strategies now running in " + mode_str + " mode.")

    return jsonify({"ok": True, "dry_run": config_ref.get("dry_run", True)})

# ── /api/download_log ────────────────────────────────────────

@app.route("/api/basket_limits", methods=["POST"])
def api_basket_limits():
    """Save basket SL and target from dashboard to backend state."""
    global _basket_target, _basket_sl
    data = request.get_json() or {}
    _basket_target = float(data.get("basket_target", 0) or 0)
    _basket_sl     = float(data.get("basket_sl",     0) or 0)
    _log.info(f"Basket limits updated: target=Rs {_basket_target:.0f}  SL=Rs {_basket_sl:.0f}")
    # Save permanently to config.json — survives restart and basket hits
    if config_ref is not None:
        config_ref["basket_target"] = _basket_target
        config_ref["basket_sl"]     = _basket_sl
        save_config()
    # Push into engine config so StrategyRunner.run() can read them
    if engine_ref:
        engine_ref.config["basket_sl"]     = _basket_sl
        engine_ref.config["basket_target"] = _basket_target
    return jsonify({"ok": True, "basket_target": _basket_target, "basket_sl": _basket_sl})

@app.route("/api/basket_alert", methods=["POST"])
def api_basket_alert():
    data = request.get_json() or {}
    alert_type = data.get("type", "sl")
    mtm   = data.get("mtm", 0)
    limit = data.get("limit", 0)
    if alert_type == "target":
        msg = f"[BASKET TARGET] | PORTFOLIO\n\nBasket Profit Target Hit!\nCombined MTM : Rs {mtm:.0f}\nTarget Set   : Rs {limit:.0f}\nAll strategies exited."
    else:
        msg = f"[BASKET SL] | PORTFOLIO\n\nBasket Stop Loss Hit!\nCombined MTM : Rs {mtm:.0f}\nSL Set       : Rs {limit:.0f}\nAll strategies exited."
    add_log("BASKET", "PORTFOLIO", msg)
    if engine_ref and engine_ref.notifier:
        engine_ref.notifier.telegram(msg)
    return jsonify({"ok": True})

# ── /zerodha/callback — NOT USED IN ANGELONE ───────────────
@app.route("/zerodha/callback_disabled")
def zerodha_callback():
    request_token = request.args.get("request_token", "")
    status        = request.args.get("status", "")
    if status != "success" or not request_token:
        return "<h2>Login failed. Please try again.</h2>", 400
    if config_ref:
        from datetime import date, timezone, timedelta
        ist = timezone(timedelta(hours=5, minutes=30))
        config_ref.setdefault("broker_creds", {})["request_token"] = request_token
        config_ref["broker_creds"]["access_token"] = ""
        config_ref["broker_creds"]["token_date"] = datetime.now(ist).strftime("%Y-%m-%d")
        save_config()
    return f"""<!DOCTYPE html><html><head><title>Login Success</title>
    <style>body{{font-family:sans-serif;background:#0d1117;color:#f1f5fb;
    display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
    .box{{background:#161b22;border:1px solid #2d3748;border-radius:12px;
    padding:40px;text-align:center;max-width:420px}}
    h2{{color:#10b981}}p{{color:#b8cce0;line-height:1.6}}
    .token{{font-family:monospace;font-size:11px;background:#0d1117;
    padding:8px;border-radius:6px;word-break:break-all;color:#f59e0b;margin:12px 0}}
    .btn{{background:#10b981;color:#fff;border:none;padding:10px 24px;
    border-radius:8px;font-size:14px;font-weight:700;cursor:pointer}}
    </style></head><body><div class="box">
    <h2>&#10003; AngelOne Login Successful</h2>
    <p>Token saved. Return to dashboard and click <b>Start Algo</b>.</p>
    <div class="token">{request_token}</div>
    <button class="btn" onclick="window.close()">Close Window</button>
    <script>setTimeout(()=>window.close(),3000)</script>
    </div></body></html>"""

@app.route("/api/zerodha/login_url_disabled")
def zerodha_login_url():
    err = _require_admin()
    if err: return err
    bc = config_ref.get("broker_creds",{}) if config_ref else {}
    api_key = bc.get("api_key","")
    if not api_key:
        return jsonify({"ok":False,"url":"","msg":"Enter API Key in Broker Setup first"})
    try:
        url = ""  # AngelOne — not used
        return jsonify({"ok":True,"url":url})
    except Exception as e:
        return jsonify({"ok":False,"url":"","msg":str(e)})

@app.route("/api/zerodha/connect_disabled", methods=["POST"])
def zerodha_connect():
    data = request.get_json() or {}
    request_token = data.get("request_token","").strip()
    if not request_token:
        return jsonify({"ok":False,"msg":"No request token provided"})
    if config_ref:
        from datetime import timezone, timedelta as _td
        _ist = timezone(_td(hours=5, minutes=30))
        bc = config_ref.setdefault("broker_creds",{})
        bc["request_token"] = request_token
        bc["access_token"]  = ""
        bc["token_date"]    = datetime.now(_ist).strftime("%Y-%m-%d")
        save_config()
    add_log("BROKER","MAIN","AngelOne token saved. Click Start Algo.")
    return jsonify({"ok":True,"msg":"Token saved. Click Start Algo to begin."})


@app.route("/api/download_log")
def api_download_log():
    """
    Download activity log as CSV in Tradetron format.
    Log body format (from engine.py):
      Script: NIFTY | Options | Strike: 24000 CE | Type: CE | Txn: SELL |
      Cond: ENTRY | Time: 09:17:00 | Qty: 75 | Price: Rs 120.50
    Each field is a SEPARATE cell — never combined into one cell.
    """
    with trade_log_lk: logs = list(trade_log)
    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow([
        "Sl No", "Strategy Name", "Underlying", "Strike", "Option Type",
        "Txn Type", "Condition Type", "Date & time",
        "Quantity", "Price", "Amount"
    ])
    sno = 1
    for e in logs:
        tag   = e.get("tag", "")
        body  = e.get("body", "")
        sname = e.get("set", "")
        ts    = e.get("ts", "")
        dt    = e.get("date", "")

        is_entry = tag in ("ENTRY", "RE-ENTRY", "RE-COST", "RE-EXECUTE ENTRY")
        is_exit  = tag in ("LEG SL", "LEG TP", "SQUAREOFF", "MANUAL EXIT",
                           "SL EXIT", "TP EXIT", "MTM SL", "MTM TARGET")
        if not (is_entry or is_exit):
            continue

        # ── Parse each field individually from the pipe-separated body ──
        # Body example:
        # "Script: NIFTY | Options | Strike: 24000 CE | Type: CE | Txn: SELL |
        #  Cond: ENTRY | Time: 09:17:00 | Qty: 75 | Price: Rs 120.50"

        def _get(key):
            """Extract value for key from pipe-separated log body."""
            import re as _re
            m = _re.search(r'(?:^|\|)\s*' + _re.escape(key) + r'\s*:\s*([^|]+)', body, _re.IGNORECASE)
            return m.group(1).strip() if m else ""

        # Underlying — Script field e.g. "NIFTY", "CrudeOil"
        raw_script = _get("Script")
        underly    = _clean_index_name(raw_script or sname)

        # Strike — e.g. "24000 CE" or "NIFTY26MAY24000CE" → extract number only
        raw_strike = _get("Strike")
        strike_num = _extract_strike_number(raw_strike)

        # Option Type — CE / PE / FUT
        opt = _get("Type").upper()
        if not opt:
            opt = "FUT" if "Futures" in body else ""

        # Txn Type — SELL / BUY (strip extra text like "(exit)")
        raw_txn = _get("Txn").upper()
        if "SELL" in raw_txn:
            txn = "SELL"
        elif "BUY" in raw_txn:
            txn = "BUY"
        else:
            txn = "SELL" if is_entry else "BUY"

        # Condition — Entry or Exit
        raw_cond = _get("Cond")
        if raw_cond:
            cond = raw_cond
        else:
            cond = "Entry" if is_entry else "Exit"

        # Time from body (most accurate), fallback to log timestamp
        body_time = _get("Time")
        datetime_str = f"{dt} {body_time}".strip() if body_time else f"{dt} {ts}".strip()

        # Quantity
        qty_str = _get("Qty")
        try:
            qty_val = int(str(qty_str).replace(",", "").strip())
            # Buy = positive, Sell = negative
            if txn == "SELL":
                qty_val = -abs(qty_val)
            else:
                qty_val = abs(qty_val)
        except (ValueError, TypeError):
            qty_val = qty_str or ""

        # Price — strip "Rs" prefix
        raw_price = _get("Price")
        try:
            price_val = float(str(raw_price).replace("Rs", "").replace(",", "").strip())
        except (ValueError, TypeError):
            price_val = raw_price or ""

        # Amount = Price × Quantity
        try:
            amount = round(float(price_val) * float(qty_val), 2)
        except (ValueError, TypeError):
            amount = ""

        w.writerow([
            sno, sname, underly, strike_num, opt,
            txn, cond, datetime_str,
            qty_val, price_val, amount
        ])
        sno += 1

    out.seek(0)
    return Response(out.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition":
                 f"attachment; filename=myalgo_trades_{date.today()}.csv"})

def _extract(body, key, default=""):
    """Extract value from log body text."""
    import re as _re
    m = _re.search(key+r'\s*:\s*([^\n]+)', body, _re.IGNORECASE)
    return m.group(1).strip() if m else default

def _extract_strike_number(symbol: str) -> str:
    """
    Extract strike number only from any symbol format.
    "260 CE"            -> 260   (fmt_sym short form)
    "NIFTY26MAY24000CE" -> 24000 (NSE/BSE full symbol)
    "CRUDEOIL16MAY266800CE" -> 6800 (MCX full symbol)
    "NIFTY26MAYFUT"     -> FUT
    """
    if not symbol: return ""
    import re as _re
    s = str(symbol).strip()
    su = s.upper()
    if su.endswith("FUT") or su == "FUT": return "FUT"
    # Already clean short form: "260 CE" or "24000 PE"
    m = _re.match(r'^([0-9]+(?:[.][0-9]+)?)\s*(?:CE|PE)$', su)
    if m: return m.group(1).split(".")[0]
    # NSE/BSE full: NIFTY26MAY24000CE
    m = _re.match(r'^[A-Z]+[0-9]{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)([0-9]+)(?:CE|PE)$', su)
    if m: return m.group(1)
    # MCX full: CRUDEOIL16MAY266800CE
    m = _re.match(r'^[A-Z]+[0-9]{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[0-9]{2}([0-9]+)(?:CE|PE)$', su)
    if m: return m.group(1)
    # Last resort: any digits before CE/PE
    m = _re.search(r'([0-9]+)(?:CE|PE)$', su)
    if m: return m.group(1)
    return s

def _clean_index_name(raw: str) -> str:
    """Return clean index name: Nifty, BankNifty, Sensex, FinNifty etc."""
    if not raw: return ""
    r = str(raw).upper().strip()
    _map = {
        "NIFTY":     "Nifty",
        "BANKNIFTY": "BankNifty",
        "FINNIFTY":  "FinNifty",
        "MIDCPNIFTY":"MidcapNifty",
        "SENSEX":    "Sensex",
        "BANKEX":    "Bankex",
        "GOLD":      "Gold",
        "SILVER":    "Silver",
        "CRUDEOIL":  "CrudeOil",
        "NATURALGAS":"NaturalGas",
    }
    for k, v in _map.items():
        if k in r:
            return v
    return raw.title()

# ── start Flask ──────────────────────────────────────────────
def start_dashboard(port: int):
    import logging as lg
    lg.getLogger("werkzeug").setLevel(lg.ERROR)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)

# ── /api/setup/status ────────────────────────────────────────
@app.route("/api/setup/status", methods=["GET"])
def api_setup_status():
    """Check if first-time setup is complete."""
    bc = config_ref.get("broker_creds", {}) if config_ref else {}
    angelone_ok = all([
        bc.get("api_key","").strip(),
        bc.get("client_code","").strip(),
        bc.get("password","").strip(),
        bc.get("totp_key","").strip(),
    ])
    telegram_ok = all([
        config_ref.get("telegram_token","").strip() if config_ref else "",
        config_ref.get("telegram_chat_id","").strip() if config_ref else "",
    ])
    return jsonify({
        "setup_complete": angelone_ok and telegram_ok,
        "angelone_ok": angelone_ok,
        "telegram_ok": telegram_ok,
    })

# ── /api/setup/save ──────────────────────────────────────────
@app.route("/api/setup/save", methods=["POST"])
def api_setup_save():
    """Save first-time setup credentials."""
    data = request.get_json() or {}
    bc = config_ref.setdefault("broker_creds", {})
    # AngelOne fields
    for k in ["api_key","client_code","password","totp_key"]:
        if data.get(k,"").strip():
            bc[k] = data[k].strip()
    # Telegram fields
    for k in ["telegram_token","telegram_chat_id"]:
        if data.get(k,"").strip():
            config_ref[k] = data[k].strip()
    save_config()
    # Check if complete
    angelone_ok = all([bc.get(k,"").strip() for k in ["api_key","client_code","password","totp_key"]])
    telegram_ok = all([config_ref.get(k,"").strip() for k in ["telegram_token","telegram_chat_id"]])
    if not angelone_ok:
        return jsonify({"ok": False, "msg": "Please complete AngelOne setup"})
    if not telegram_ok:
        return jsonify({"ok": False, "msg": "Please complete Telegram setup"})
    return jsonify({"ok": True, "msg": "Setup complete! Redirecting to login..."})
