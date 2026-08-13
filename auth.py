"""
auth.py — MY ALGO Login System
OTP-based login via Telegram. Two roles: Admin and Demo.
Admin OTP: valid full day till midnight.
Demo OTP: valid for admin-set hours, clears at midnight.
No database — simple JSON state file.
"""
import json, os, random, threading, time
from datetime import datetime, timedelta

AUTH_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auth_state.json")
_lock = threading.Lock()

# ── Default state ─────────────────────────────────────────────
DEFAULT = {
    "admin_otp"          : None,
    "admin_otp_date"     : None,
    "admin_resend_count" : 0,
    "demo_enabled"       : False,
    "demo_otp"           : None,
    "demo_otp_expiry"    : None,
    "demo_validity_hours": 1,
    "demo_resend_count"  : 0,
    "admin_sessions"     : [],
    "demo_sessions"      : [],
}

def _load():
    try:
        if os.path.exists(AUTH_FILE):
            with open(AUTH_FILE) as f:
                data = json.load(f)
            # Fill missing keys with defaults
            for k, v in DEFAULT.items():
                if k not in data:
                    data[k] = v
            return data
    except Exception:
        pass
    return dict(DEFAULT)

def _save(state):
    try:
        with open(AUTH_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass

def _gen_otp():
    return str(random.randint(1000, 9999))

def _today():
    return datetime.now().strftime("%Y-%m-%d")

def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _send_telegram(token, chat_id, msg):
    """Send message via Telegram bot."""
    try:
        import requests
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
        return True
    except Exception:
        return False

def _get_algo_name() -> str:
    """Read algo_name from config.json."""
    try:
        import json as _json, os as _os
        _cfg_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "config.json")
        with open(_cfg_path) as _f:
            return _json.load(_f).get("algo_name", "MY ALGO")
    except Exception:
        return "MY ALGO"

def _get_telegram_creds():
    """Read telegram token and chat_id from config.json."""
    try:
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(cfg_path) as f:
            cfg = json.load(f)
        token   = cfg.get("telegram_token","").strip()
        chat_id = cfg.get("telegram_chat_id","").strip()
        return token, chat_id
    except Exception:
        return "", ""

# ── Midnight reset — called from dashboard.py midnight_reset() ─
def midnight_reset():
    """Clear all OTP and session data at midnight."""
    with _lock:
        state = _load()
        state["admin_otp"]           = None
        state["admin_otp_date"]      = None
        state["admin_resend_count"]  = 0
        state["demo_otp"]            = None
        state["demo_otp_expiry"]     = None
        state["demo_resend_count"]   = 0
        state["admin_sessions"]      = []
        state["demo_sessions"]       = []
        # Keep demo_enabled and demo_validity_hours — admin preference
        _save(state)

def _get_server_ip():
    """Read server IP from config.json for OTP messages."""
    try:
        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        with open(cfg_path) as f:
            cfg = json.load(f)
        ip = cfg.get("server_ip","").strip() or cfg.get("static_ip","").strip()
        return ip if ip else "YOUR_SERVER_IP"
    except Exception:
        return "YOUR_SERVER_IP"

# ── OTP: Admin ────────────────────────────────────────────────
def send_admin_otp():
    """Generate or resend admin OTP. Max 3 sends per day."""
    with _lock:
        state = _load()
        today = _today()
        # Generate new OTP if first time today
        if state["admin_otp"] is None or state["admin_otp_date"] != today:
            state["admin_otp"]          = _gen_otp()
            state["admin_otp_date"]     = today
            state["admin_resend_count"] = 0
        # Check resend limit
        if state["admin_resend_count"] >= 5:
            return {"ok": False, "msg": "Maximum 5 OTP sends reached for today. Use existing OTP."}
        # Send via Telegram
        token, chat_id = _get_telegram_creds()
        if not token or not chat_id:
            return {"ok": False, "msg": "Telegram not configured. Set up in Alert Setup first."}
        midnight = datetime.now().replace(hour=23, minute=59, second=59)
        server_ip = _get_server_ip()
        msg = (f"🔐 {_get_algo_name()} — Admin OTP\n\n"
               f"Access Link: http://{server_ip}\n"
               f"OTP: {state['admin_otp']}\n"
               f"User: Admin\n"
               f"Valid till: 11:59 PM today\n"
               f"Attempt: {state['admin_resend_count']+1}/5")
        ok = _send_telegram(token, chat_id, msg)
        if not ok:
            return {"ok": False, "msg": "Telegram send failed. Check token and chat ID."}
        state["admin_resend_count"] += 1
        _save(state)
        return {"ok": True, "msg": f"OTP sent to your Telegram. ({state['admin_resend_count']}/5 sends used)"}

# ── OTP: Demo ─────────────────────────────────────────────────
def send_demo_otp():
    """Generate or resend demo OTP. Sent to admin Telegram only."""
    with _lock:
        state = _load()
        if not state["demo_enabled"]:
            return {"ok": False, "msg": "Demo access is disabled by admin."}
        now = datetime.now()
        # Check if existing OTP still valid
        if state["demo_otp"] and state["demo_otp_expiry"]:
            expiry = datetime.strptime(state["demo_otp_expiry"], "%Y-%m-%d %H:%M:%S")
            if now < expiry:
                # Reuse existing OTP — check resend limit
                if state["demo_resend_count"] >= 3:
                    return {"ok": False, "msg": "Maximum 3 OTP sends reached. Admin must reset demo access."}
            else:
                # Expired — generate new OTP
                state["demo_otp"]          = _gen_otp()
                hrs = int(state["demo_validity_hours"])
                state["demo_otp_expiry"]   = (now + timedelta(hours=hrs)).strftime("%Y-%m-%d %H:%M:%S")
                state["demo_resend_count"] = 0
                state["demo_sessions"]     = []
        else:
            # First time — generate new OTP
            state["demo_otp"]          = _gen_otp()
            hrs = int(state["demo_validity_hours"])
            state["demo_otp_expiry"]   = (now + timedelta(hours=hrs)).strftime("%Y-%m-%d %H:%M:%S")
            state["demo_resend_count"] = 0
            state["demo_sessions"]     = []
        # Send to admin Telegram only
        token, chat_id = _get_telegram_creds()
        if not token or not chat_id:
            return {"ok": False, "msg": "Telegram not configured."}
        expiry_str = datetime.strptime(state["demo_otp_expiry"], "%Y-%m-%d %H:%M:%S").strftime("%I:%M %p")
        server_ip = _get_server_ip()
        msg = (f"🔐 {_get_algo_name()} — Demo OTP\n\n"
               f"Access Link: http://{server_ip}\n"
               f"OTP: {state['demo_otp']}\n"
               f"User: Demo\n"
               f"Valid for: {state['demo_validity_hours']} hour(s)\n"
               f"Expires at: {expiry_str}\n\n"
               f"Please check the demo before it expires.")
        ok = _send_telegram(token, chat_id, msg)
        if not ok:
            return {"ok": False, "msg": "Telegram send failed."}
        state["demo_resend_count"] += 1
        _save(state)
        return {"ok": True, "msg": f"Demo OTP sent to your Telegram. Share manually with demo user."}

# ── Login ─────────────────────────────────────────────────────
def verify_login(role, otp):
    """Verify OTP and create session. Returns session token or error."""
    import secrets
    with _lock:
        state = _load()
        now   = datetime.now()
        today = _today()
        if role == "admin":
            if not state["admin_otp"] or state["admin_otp_date"] != today:
                return {"ok": False, "msg": "No OTP generated yet. Click Send OTP first."}
            if otp != state["admin_otp"]:
                return {"ok": False, "msg": "Wrong OTP. Please try again."}
            # Valid — create session token
            token = secrets.token_hex(32)
            state["admin_sessions"].append(token)
            _save(state)
            return {"ok": True, "role": "admin", "token": token}
        elif role == "demo":
            if not state["demo_enabled"]:
                return {"ok": False, "msg": "Demo access disabled."}
            if not state["demo_otp"] or not state["demo_otp_expiry"]:
                return {"ok": False, "msg": "No demo OTP generated yet. Contact admin."}
            expiry = datetime.strptime(state["demo_otp_expiry"], "%Y-%m-%d %H:%M:%S")
            if now >= expiry:
                return {"ok": False, "msg": "Demo OTP expired. Contact admin for new access."}
            if otp != state["demo_otp"]:
                return {"ok": False, "msg": "Wrong OTP. Please try again."}
            token = secrets.token_hex(32)
            state["demo_sessions"].append(token)
            _save(state)
            return {"ok": True, "role": "demo", "token": token,
                    "expiry": state["demo_otp_expiry"]}
        return {"ok": False, "msg": "Invalid role."}

# ── Session check ─────────────────────────────────────────────
def check_session(token, role):
    """Check if session token is valid. Returns role or None."""
    with _lock:
        state = _load()
        now   = datetime.now()
        today = _today()
        if role == "admin":
            if state["admin_otp_date"] != today:
                return None   # midnight reset
            if token in state["admin_sessions"]:
                return "admin"
        elif role == "demo":
            if not state["demo_enabled"]:
                return None
            if not state["demo_otp_expiry"]:
                return None
            expiry = datetime.strptime(state["demo_otp_expiry"], "%Y-%m-%d %H:%M:%S")
            if now >= expiry:
                return None
            if token in state["demo_sessions"]:
                return "demo"
        return None

# ── Demo settings (admin only) ────────────────────────────────
def get_demo_status():
    """Get current demo access status for admin panel."""
    with _lock:
        state = _load()
        now   = datetime.now()
        remaining = None
        if state["demo_otp_expiry"]:
            try:
                expiry = datetime.strptime(state["demo_otp_expiry"], "%Y-%m-%d %H:%M:%S")
                diff   = expiry - now
                if diff.total_seconds() > 0:
                    mins = int(diff.total_seconds() // 60)
                    remaining = f"{mins // 60}h {mins % 60}m remaining"
                else:
                    remaining = "Expired"
            except Exception:
                remaining = None
        return {
            "demo_enabled"       : state["demo_enabled"],
            "demo_validity_hours": state["demo_validity_hours"],
            "demo_otp_expiry"    : state["demo_otp_expiry"],
            "remaining"          : remaining,
            "demo_active"        : len(state["demo_sessions"]) > 0,
        }

def set_demo_settings(enabled, validity_hours):
    """Admin sets demo enabled/disabled and validity hours."""
    with _lock:
        state = _load()
        state["demo_enabled"] = bool(enabled)
        state["demo_validity_hours"] = int(validity_hours)
        if not enabled:
            # Revoke all demo sessions immediately
            state["demo_sessions"]     = []
            state["demo_otp"]          = None
            state["demo_otp_expiry"]   = None
            state["demo_resend_count"] = 0
        _save(state)
        return {"ok": True}

def reset_demo_otp():
    """Admin resets demo OTP — kills current session, forces new OTP next send."""
    with _lock:
        state = _load()
        state["demo_otp"]          = None
        state["demo_otp_expiry"]   = None
        state["demo_resend_count"] = 0
        state["demo_sessions"]     = []
        _save(state)
        return {"ok": True, "msg": "Demo access reset. New OTP will be generated on next Send OTP."}
