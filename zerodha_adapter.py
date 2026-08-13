"""
============================================================
  ZERODHA KITE CONNECT ADAPTER
  Implements BrokerBase for Zerodha Kite Connect API.

  Authentication Flow (Option A — Manual Daily Token):
    1. User clicks "Generate Login URL" in Broker Setup
    2. Opens Zerodha login in browser
    3. After login, browser redirects to:
       http://127.0.0.1:5000/zerodha/callback?request_token=xxx
    4. User copies token from URL, pastes in dashboard
    5. Bot exchanges request_token → access_token
    6. access_token saved in config, valid until midnight

  Instrument Data:
    - Downloads NSE/BSE instrument CSV from Zerodha on startup
    - Filters locally for option chains — no API call per chain
    - Accurate, fast, no mixed-instrument issues

  WebSocket:
    - Kite Ticker (KiteTicker) — stable, standard fields
    - tick.last_price — works for both options and indices
    - No isIndex=True quirks, no 'iv' field issues
============================================================
"""

import logging
import threading
import time
import csv
import io
import re
import requests
import queue
from abc import ABC
from datetime import date, datetime
from kiteconnect import KiteConnect, KiteTicker

_log = logging.getLogger("ZerodhaAdapter")

# ── Zerodha instrument CSV URL ─────────────────────────────
NSE_INSTRUMENTS_URL = "https://api.kite.trade/instruments/NSE"
NFO_INSTRUMENTS_URL = "https://api.kite.trade/instruments/NFO"
BSE_INSTRUMENTS_URL = "https://api.kite.trade/instruments/BSE"
BFO_INSTRUMENTS_URL = "https://api.kite.trade/instruments/BFO"
MCX_INSTRUMENTS_URL = "https://api.kite.trade/instruments/MCX"


class ZerodhaAdapter:
    """
    Zerodha Kite Connect adapter implementing the BrokerBase interface.
    Drop-in replacement for KotakNeoAdapter.
    """
    name = "Zerodha"

    def __init__(self):
        self._kite        = None
        self._ticker      = None
        self._api_key     = ""
        self._api_secret  = ""
        self._access_token= ""
        self._on_tick_cb  = None
        self._sub_tokens  = []
        self._instruments = {}   # cache: exchange -> list of instrument dicts
        self._connected   = False
        self._notifier    = None
        self._lock        = threading.Lock()
        # ── Feed health (watchdog) ────────────────────────────
        self._last_tick_ts   = 0.0   # epoch seconds of most recent tick
        self._feed_healthy   = False # True when ticks flowing within timeout
        self._feed_was_alerted = False  # avoid spamming telegram
        self._stall_alerted  = False    # 5-min stall alert sent flag
        self._watchdog_started = False
        self._watchdog_stop  = False
        self._reconnect_attempts = 0
        self._ws_connected = threading.Event()  # set when on_connect fires   # for backoff on manual reconnects
        # ── Tick queue (community best practice) ───────────────
        # on_ticks must NOT do any heavy work — Zerodha drops the WS if it
        # blocks. Queue + worker pattern: on_ticks just enqueues, worker
        # thread does price_store.update() and notifier calls.
        self._tick_queue = queue.Queue(maxsize=10000)
        self._worker_started = False
        self._worker_stop = False

    # ── Login ──────────────────────────────────────────────
    def login(self, api_key="", api_secret="", access_token="",
              request_token="", user_id="", password="", totp_key="",
              **kwargs) -> bool:
        """
        Login using access_token (already exchanged) or request_token.
        access_token is stored in config after first exchange.
        """
        try:
            self._api_key    = api_key.strip()
            self._api_secret = api_secret.strip()
            self._kite = KiteConnect(api_key=self._api_key)

            # Method 1: TOTP Auto-Login
            if password and totp_key and user_id and api_secret:
                _log.info(f"[Zerodha] TOTP auto-login for {user_id}...")
                try:
                    import pyotp, requests as _req
                    session = _req.Session()
                    session.headers.update({"User-Agent": "Mozilla/5.0"})
                    r1 = session.post("https://kite.zerodha.com/api/login",
                        data={"user_id": user_id.strip(), "password": password.strip()})
                    d1 = r1.json()
                    if d1.get("status") != "success":
                        raise Exception(f"Login failed: {d1.get('message','')}")
                    request_id = d1["data"]["request_id"]
                    _log.info(f"[Zerodha] Step 1 OK. request_id={request_id}")
                    totp_code = pyotp.TOTP(totp_key.strip()).now()
                    r2 = session.post("https://kite.zerodha.com/api/twofa",
                        data={"user_id": user_id.strip(), "request_id": request_id,
                              "twofa_value": totp_code, "twofa_type": "totp"})
                    d2 = r2.json()
                    if d2.get("status") != "success":
                        raise Exception(f"2FA failed: {d2.get('message','')}")
                    _log.info("[Zerodha] Step 2 OK. 2FA passed.")
                    login_url = self._kite.login_url()
                    from urllib.parse import urlparse, parse_qs
                    import re as _re
                    req_token = None
                    try:
                        r3 = session.get(login_url, allow_redirects=True)
                    except Exception as _e3:
                        # Extract request_token from connection error URL
                        _em = str(_e3)
                        _m = _re.search(r'request_token=([A-Za-z0-9]+)', _em)
                        if _m:
                            req_token = _m.group(1)
                            _log.info(f"[Zerodha] Step 3 OK. request_token extracted from redirect URL.")
                        r3 = type('R', (), {'url': '', 'headers': {}, 'history': []})()
                    parsed = urlparse(r3.url)
                    params = parse_qs(parsed.query)
                    req_token = params.get("request_token", [None])[0]
                    if not req_token:
                        for resp in r3.history:
                            loc = resp.headers.get("location", "")
                            if "request_token" in loc:
                                p2 = urlparse(loc)
                                q2 = parse_qs(p2.query)
                                req_token = q2.get("request_token", [None])[0]
                                if req_token: break
                    if not req_token:
                        # Step 4: Handle authorize consent page
                        # Zerodha redirects to /connect/authorize when app needs consent
                        parsed3 = urlparse(r3.url)
                        params3 = parse_qs(parsed3.query)
                        sess_id = params3.get("sess_id", [None])[0]
                        if sess_id:
                            _log.info(f"[Zerodha] Step 4: authorize consent page, sess_id={sess_id[:10]}...")
                            # Try GET first (some Zerodha accounts use GET for consent)
                            r4 = session.get(f"https://kite.zerodha.com/connect/authorize?api_key={self._api_key}&sess_id={sess_id}",
                                allow_redirects=True)
                            parsed4 = urlparse(r4.url)
                            params4 = parse_qs(parsed4.query)
                            req_token = params4.get("request_token", [None])[0]
                            if not req_token:
                                # Try POST as fallback
                                r4 = session.post("https://kite.zerodha.com/connect/authorize",
                                    data={"api_key": self._api_key, "sess_id": sess_id},
                                    allow_redirects=True)
                            parsed4 = urlparse(r4.url)
                            params4 = parse_qs(parsed4.query)
                            req_token = params4.get("request_token", [None])[0]
                            if not req_token:
                                for resp in r4.history:
                                    loc = resp.headers.get("location", "")
                                    if "request_token" in loc:
                                        p4 = urlparse(loc)
                                        q4 = parse_qs(p4.query)
                                        req_token = q4.get("request_token", [None])[0]
                                        if req_token: break
                        if not req_token:
                            raise Exception("Could not extract request_token")
                    _log.info("[Zerodha] Step 3 OK. request_token obtained.")
                    data = self._kite.generate_session(req_token, api_secret=self._api_secret)
                    self._access_token = data["access_token"]
                    self._kite.set_access_token(self._access_token)
                    profile = self._kite.profile()
                    _log.info(f"[Zerodha] TOTP login OK — {profile.get('user_name','')} ({profile.get('user_id','')})")
                    self._connected = True
                    self._download_instruments()
                    return True
                except Exception as totp_err:
                    _log.warning(f"[Zerodha] TOTP failed: {totp_err}. Trying saved token...")

            # Method 2: Saved access_token
            if access_token and access_token.strip():
                self._access_token = access_token.strip()
                self._kite.set_access_token(self._access_token)
                # Verify token is still valid
                try:
                    profile = self._kite.profile()
                    _log.info(f"Zerodha login OK — {profile.get('user_name','')} ({profile.get('user_id','')})")
                    self._connected = True
                    self._download_instruments()
                    return True
                except Exception as e:
                    _log.warning(f"Saved access_token expired: {e}. Need fresh request_token.")
                    self._access_token = ""

            # Exchange request_token for access_token
            if request_token and request_token.strip():
                data = self._kite.generate_session(
                    request_token.strip(), api_secret=self._api_secret)
                self._access_token = data["access_token"]
                self._kite.set_access_token(self._access_token)
                profile = self._kite.profile()
                _log.info(f"Zerodha login OK — {profile.get('user_name','')} ({profile.get('user_id','')})")
                self._connected = True
                self._download_instruments()
                return True

            _log.error("Zerodha login: no access_token or request_token provided.")
            return False

        except Exception as e:
            _log.error(f"Zerodha login failed: {e}")
            return False

    def get_access_token(self) -> str:
        """Return access token so dashboard can save it to config."""
        return self._access_token

    def get_login_url(self) -> str:
        """Return the Zerodha OAuth login URL for the user to open."""
        if not self._api_key:
            return ""
        kite = KiteConnect(api_key=self._api_key)
        return kite.login_url()

    # ── Instrument CSV ─────────────────────────────────────
    def _download_instruments(self):
        """Download and cache instrument CSVs from Zerodha."""
        urls = {
            "NFO": NFO_INSTRUMENTS_URL,   # NSE F&O (NIFTY, BANKNIFTY etc.)
            "BFO": BFO_INSTRUMENTS_URL,   # BSE F&O (SENSEX, BANKEX)
            "MCX": MCX_INSTRUMENTS_URL,   # MCX commodities
            "NSE": NSE_INSTRUMENTS_URL,   # NSE cash (for index tokens)
            "BSE": BSE_INSTRUMENTS_URL,   # BSE cash
        }
        headers = {"Authorization": f"token {self._api_key}:{self._access_token}"}
        for exch, url in urls.items():
            try:
                r = requests.get(url, headers=headers, timeout=30)
                if r.status_code == 200:
                    reader = csv.DictReader(io.StringIO(r.text))
                    self._instruments[exch] = list(reader)
                    _log.info(f"Instruments {exch}: {len(self._instruments[exch])} contracts loaded")
                else:
                    _log.warning(f"Instruments {exch}: HTTP {r.status_code}")
            except Exception as e:
                _log.warning(f"Instruments {exch} download failed: {e}")

    def get_all_fo_instruments(self) -> list:
        """
        Return all F&O and MCX derivative contracts across NFO, BFO, MCX.
        Used by engine.refresh_instruments_from_broker() to build the live
        INSTRUMENTS dict without any hardcoding.
        """
        rows = []
        for exch in ("NFO", "BFO", "MCX"):
            rows.extend(self._get_instruments(exch))
        return rows

    def _get_instruments(self, exchange: str) -> list:
        """Return cached instruments for exchange, download if missing."""
        if exchange not in self._instruments:
            self._download_instruments()
        return self._instruments.get(exchange, [])

    # ── Option Chain ───────────────────────────────────────
    def get_option_chain(self, instrument: str, expiry_str: str) -> list:
        """
        Build option chain from Zerodha instrument CSV.

        KEY FIX — MCX options vs FUT expiry offset:
        For MCX commodities, FUT and options often expire on DIFFERENT dates
        within the same month. Examples:
          CRUDEOIL May: FUT=2026-05-18, CE/PE=2026-05-16 (2 days earlier)
          NATURALGAS:   FUT and options may differ by several days

        Strategy:
          - FUT contracts: exact expiry_str date match only
          - MCX CE/PE:     accept any contract in the same calendar month
            (so passing the FUT expiry still finds the options)
          - NFO/BFO CE/PE: exact date match (options share FUT expiry on NSE/BSE)

        Zerodha CSV fields:
          instrument_token, tradingsymbol, name, expiry, strike,
          lot_size, instrument_type (CE/PE/FUT/EQ),
          exchange (NFO/BFO/MCX)
        """
        from engine import INSTRUMENTS as INSTR_MAP
        info  = INSTR_MAP.get(instrument, {})
        exch_map = {
            "nse_fo": "NFO", "NFO": "NFO",
            "bse_fo": "BFO", "BFO": "BFO",
            "mcx_fo": "MCX", "MCX": "MCX",
        }
        z_exchange = exch_map.get(info.get("exchange","nse_fo"), "NFO")
        is_mcx = (z_exchange == "MCX")

        # Parse expiry_str "18MAY2026" → date object + "2026-05-18" string
        try:
            exp_date_obj = datetime.strptime(expiry_str, "%d%b%Y").date()
            exp_date     = exp_date_obj.strftime("%Y-%m-%d")
            target_year  = exp_date_obj.year
            target_month = exp_date_obj.month
        except Exception:
            exp_date     = expiry_str
            exp_date_obj = None
            target_year  = None
            target_month = None

        instruments = self._get_instruments(z_exchange)
        chain = []
        for row in instruments:
            row_name = row.get("name","").upper().strip()
            row_sym  = row.get("tradingsymbol","").upper().strip()

            # MCX CSV often has empty name field — use tradingsymbol prefix as primary
            # NSE/BSE have name field populated — use both as fallback
            if is_mcx:
                # For MCX: match purely by tradingsymbol prefix
                # e.g. GOLDM26MAYFUT, GOLDM26MAY9000CE all start with "GOLDM"
                if not row_sym.startswith(instrument.upper()):
                    continue
            else:
                # For NSE/BSE: match by name OR tradingsymbol prefix
                from engine import ZERODHA_NAME_MAP as _znm
                if row_name and row_name != instrument.upper():
                    zerodha_name = _znm.get(instrument.upper(), instrument.upper())
                    if row_name != zerodha_name:
                        if not row_sym.startswith(instrument.upper()):
                            continue
                elif not row_name:
                    if not row_sym.startswith(instrument.upper()):
                        continue

            inst_type = row.get("instrument_type","").upper()
            if inst_type not in ("CE","PE","FUT"):
                continue

            row_exp = row.get("expiry","")

            if inst_type == "FUT":
                if is_mcx and target_year and target_month:
                    # MCX FUT: same calendar month (FUT and options may differ by days)
                    try:
                        row_d = datetime.strptime(row_exp, "%Y-%m-%d").date()
                        if row_d.year != target_year or row_d.month != target_month:
                            continue
                    except Exception:
                        if row_exp != exp_date:
                            continue
                else:
                    # NSE/BSE FUT: nearest monthly — match same calendar month
                    try:
                        row_d = datetime.strptime(row_exp, "%Y-%m-%d").date()
                        if row_d.year != target_year or row_d.month != target_month:
                            continue
                    except Exception:
                        if row_exp != exp_date:
                            continue
            elif is_mcx and target_year and target_month:
                # MCX options: same calendar month
                try:
                    row_d = datetime.strptime(row_exp, "%Y-%m-%d").date()
                    if row_d.year != target_year or row_d.month != target_month:
                        continue
                except Exception:
                    if row_exp != exp_date:
                        continue
            else:
                # NSE/BSE options: exact date
                if row_exp != exp_date:
                    continue

            chain.append({
                "instrument_token": row.get("instrument_token",""),
                "tradingsymbol"   : row.get("tradingsymbol",""),
                "strike"          : float(row.get("strike",0) or 0),
                "instrument_type" : inst_type,
                "expiry"          : row_exp,
                "lot_size"        : row.get("lot_size",""),
                "pSymbol"         : row.get("instrument_token",""),
                "pTrdSymbol"      : row.get("tradingsymbol",""),
                "pOptionType"     : inst_type,
                "pSymbolName"     : instrument.upper(),
                "pExchSeg"        : info.get("exchange","nse_fo"),
                "dStrikePrice;"   : float(row.get("strike",0) or 0) * 100,
                "exchange"        : z_exchange,
            })

        _log.info(f"get_option_chain({instrument}): {len(chain)} contracts expiry={expiry_str}")
        return chain

    def get_fut_chain(self, instrument: str, expiry_str: str) -> list:
        """Return futures contracts for instrument.
        Uses get_option_chain with the given expiry first.
        If no FUT found (expiry mismatch — e.g. GOLDM FUT vs options have
        different expiry dates), falls back to searching ALL contracts
        for the instrument and returns the nearest-expiry FUT contract.
        This handles all MCX instruments universally."""
        # First try with given expiry
        chain = self.get_option_chain(instrument, expiry_str)
        futs = [c for c in chain if c.get("instrument_type") == "FUT"]
        if futs:
            return futs
        # Fallback: search ALL MCX contracts for this instrument
        # to find FUT regardless of expiry date
        from engine import INSTRUMENTS as INSTR_MAP
        info = INSTR_MAP.get(instrument, {})
        exch_map = {"nse_fo":"NFO","NFO":"NFO","bse_fo":"BFO","BFO":"BFO","mcx_fo":"MCX","MCX":"MCX"}
        z_exchange = exch_map.get(info.get("exchange","MCX"), "MCX")
        all_rows = self._get_instruments(z_exchange)
        from datetime import datetime, date
        today = date.today()
        fut_rows = []
        for row in all_rows:
            sym = row.get("tradingsymbol","").upper()
            if not sym.startswith(instrument.upper()):
                continue
            if row.get("instrument_type","").upper() != "FUT":
                continue
            try:
                exp_d = datetime.strptime(row.get("expiry",""), "%Y-%m-%d").date()
                if exp_d >= today:
                    fut_rows.append((exp_d, row))
            except Exception:
                continue
        if not fut_rows:
            return []
        # Return nearest expiry FUT
        fut_rows.sort(key=lambda x: x[0])
        nearest = fut_rows[0][1]
        _log.info(f"get_fut_chain({instrument}): fallback found FUT {nearest.get('tradingsymbol')} expiry={nearest.get('expiry')}")
        return [{
            "instrument_token": nearest.get("instrument_token",""),
            "tradingsymbol"   : nearest.get("tradingsymbol",""),
            "strike"          : 0.0,
            "instrument_type" : "FUT",
            "expiry"          : nearest.get("expiry",""),
            "lot_size"        : nearest.get("lot_size",""),
            "pSymbol"         : nearest.get("instrument_token",""),
            "pTrdSymbol"      : nearest.get("tradingsymbol",""),
            "pOptionType"     : "FUT",
            "pSymbolName"     : instrument.upper(),
            "pExchSeg"        : info.get("exchange","mcx_fo"),
            "exchange"        : z_exchange,
        }]

    def get_available_expiries(self, instrument: str) -> list:
        """
        Return sorted list of expiry dates for instrument.
        Format: DDMMMYYYY e.g. ["24APR2026","29APR2026","25APR2026"]
        """
        from engine import INSTRUMENTS as INSTR_MAP
        info  = INSTR_MAP.get(instrument, {})
        exch_map = {
            "nse_fo": "NFO", "NFO": "NFO",
            "bse_fo": "BFO", "BFO": "BFO",
            "mcx_fo": "MCX", "MCX": "MCX",
        }
        z_exchange = exch_map.get(info.get("exchange","nse_fo"), "NFO")

        instruments = self._get_instruments(z_exchange)
        today = date.today()
        expiries = set()
        for row in instruments:
            row_name = row.get("name","").upper().strip()
            row_sym  = row.get("tradingsymbol","").upper().strip()
            is_mcx_exp = (z_exchange == "MCX")
            if is_mcx_exp:
                if not row_sym.startswith(instrument.upper()):
                    continue
            else:
                if row_name and row_name != instrument.upper():
                    from engine import ZERODHA_NAME_MAP as _znm
                    zerodha_name = _znm.get(instrument.upper(), instrument.upper())
                    if row_name != zerodha_name:
                        if not row_sym.startswith(instrument.upper()):
                            continue
                elif not row_name:
                    if not row_sym.startswith(instrument.upper()):
                        continue
            exp_str = row.get("expiry","")
            if not exp_str: continue
            try:
                exp_d = datetime.strptime(exp_str, "%Y-%m-%d").date()
                if exp_d >= today:
                    expiries.add(exp_d.strftime("%d%b%Y").upper())
            except Exception:
                pass
        return sorted(expiries, key=lambda s: datetime.strptime(s, "%d%b%Y"))

    # ── WebSocket Feed ─────────────────────────────────────
    def subscribe_feed(self, tokens: list, on_tick: callable):
        """
        Subscribe to live price feed using KiteTicker.

        IMPORTANT: KiteTicker uses Twisted, which has a hard rule:
        only ONE reactor per Python process, EVER. Once stopped, it
        cannot be restarted (twisted.internet.error.ReactorNotRestartable).

        So we start the ticker ONCE on the first call. Subsequent calls
        (e.g. when a new strategy adds NIFTY/MCX tokens) ADD tokens to
        the live ticker via ws.subscribe() instead of spawning a new one.
        """
        self._on_tick_cb = on_tick

        # De-dupe and merge into saved sub_tokens
        existing_int = set()
        for t in self._sub_tokens:
            try: existing_int.add(int(t.get("instrument_token","")))
            except (ValueError, TypeError): pass

        new_int_tokens = []
        for t in tokens:
            tok = t.get("instrument_token","")
            try:
                it = int(tok)
                if it not in existing_int:
                    new_int_tokens.append(it)
                    existing_int.add(it)
                    self._sub_tokens.append(t)
            except (ValueError, TypeError):
                pass

        if not new_int_tokens:
            _log.info("[WS] No new tokens to subscribe")
            return

        # First time: start ticker. Subsequent times: add to live ticker.
        if self._ticker is None:
            self._start_ticker(new_int_tokens)
        else:
            try:
                self._ticker.subscribe(new_int_tokens)
                self._ticker.set_mode(self._ticker.MODE_LTP, new_int_tokens)
                _log.info(f"[WS] Added {len(new_int_tokens)} tokens to live ticker "
                          f"(total: {len(self._sub_tokens)})")
                self._ws_connected.set()  # tokens added — signal feed check
            except Exception as e:
                _log.warning(f"[WS] Add-to-live-ticker failed: {e}. "
                             f"Tokens stored — on_connect will subscribe them automatically.")
    def subscribe_index_feed(self, index_tokens: list):
        """
        Add index tokens (NIFTY 50, SENSEX etc.) to the existing ticker.
        Zerodha treats indices the same as F&O tokens — just route through
        the same socket. Calls subscribe_feed() with no callback re-bind.
        """
        if not index_tokens:
            return
        # Reuse subscribe_feed which handles dedupe + live add
        cb = self._on_tick_cb or (lambda tok, ltp: None)
        self.subscribe_feed(index_tokens, cb)

    def _start_ticker(self, int_tokens: list):
        """Start KiteTicker WebSocket. Can only be called ONCE per process
        (Twisted reactor limitation). Subsequent token additions go through
        ws.subscribe() in subscribe_feed()."""
        if self._ticker is not None:
            _log.warning("[WS] _start_ticker called but ticker already exists. "
                         "Adding tokens to live ticker instead.")
            try:
                self._ticker.subscribe(int_tokens)
                self._ticker.set_mode(self._ticker.MODE_LTP, int_tokens)
            except Exception as e:
                _log.warning(f"[WS] subscribe to live ticker failed: {e}")
            return
        def _on_ticks(ws, ticks):
            # CRITICAL: this callback must be ultra-light. Zerodha drops the
            # WS connection (1006) if on_ticks blocks for too long.
            # Per Zerodha staff: "issue minimal work like copying the tick
            # elsewhere ... worry about handling that data elsewhere".
            # We just timestamp + drop into queue. Worker thread does the rest.
            self._last_tick_ts = time.time()
            try:
                # Non-blocking put; drop ticks if queue full (better than blocking WS)
                self._tick_queue.put_nowait(ticks)
            except queue.Full:
                # Queue overflow — should never happen with maxsize=10000.
                # If it does, worker is stalled; drop tick to keep WS alive.
                pass

        def _on_connect(ws, response):
            # Always subscribe ALL tokens from self._sub_tokens
            # This covers: first connect, reconnect, and any tokens
            # added after initial connection (strategy option tokens)
            all_int = []
            seen = set()
            for t in self._sub_tokens:
                try:
                    it = int(t.get("instrument_token",""))
                    if it not in seen:
                        seen.add(it)
                        all_int.append(it)
                except (ValueError, TypeError):
                    pass
            if not all_int:
                all_int = int_tokens
            _log.info(f"[WS] on_connect: subscribing {len(all_int)} tokens (all accumulated).")
            ws.subscribe(all_int)
            ws.set_mode(ws.MODE_LTP, all_int)
            self._reconnect_attempts = 0
            self._ws_connected.set()  # signal feed check that subscription is done

        def _on_close(ws, code, reason):
            _log.warning(f"[WS] Ticker disconnected: {code} {reason}")
            # Mark feed as down; watchdog will detect stall and restart if needed.
            # NOTE: do NOT call ws.stop() here — it disables auto-reconnect.
            self._feed_healthy = False

        def _on_error(ws, code, reason):
            _log.error(f"[WS] Ticker error: {code} {reason}")
            self._feed_healthy = False

        def _on_reconnect(ws, attempts_count):
            _log.info(f"[WS] Reconnecting... attempt {attempts_count}")

        def _on_noreconnect(ws):
            _log.error("[WS] Max reconnect attempts reached -- watchdog will force restart.")
            # Don't give up: watchdog thread will call _force_restart_ticker()

        try:
            if self._ticker:
                try: self._ticker.stop()
                except: pass
            self._ws_connected.clear()  # reset before new connection — prevents stale Event from yesterday

            # Tunable reconnect params — community best practice:
            #   reconnect_max_delay=5  → fastest allowed (default 60 = too slow)
            #   reconnect_max_tries=300 → max allowed (default 50 = gives up early)
            self._ticker = KiteTicker(
                self._api_key, self._access_token,
                reconnect=True,
                reconnect_max_delay=5,
                reconnect_max_tries=300,
            )
            self._ticker.on_ticks      = _on_ticks
            self._ticker.on_connect    = _on_connect
            self._ticker.on_close      = _on_close
            self._ticker.on_error      = _on_error
            self._ticker.on_reconnect  = _on_reconnect
            self._ticker.on_noreconnect= _on_noreconnect

            # Run in background thread
            threading.Thread(
                target=self._ticker.connect,
                kwargs={"threaded": True},
                daemon=True, name="ZerodhaTicker"
            ).start()
            _log.info(f"[WS] Ticker started with {len(int_tokens)} tokens "
                      f"(reconnect_max_delay=5s, reconnect_max_tries=300)")

            # Start tick worker (only once per adapter instance)
            if not self._worker_started:
                self._worker_started = True
                self._worker_stop = False
                threading.Thread(
                    target=self._tick_worker,
                    daemon=True, name="TickWorker"
                ).start()

            # Start watchdog (only once per adapter instance)
            if not self._watchdog_started:
                self._watchdog_started = True
                self._watchdog_stop = False
                threading.Thread(
                    target=self._feed_watchdog,
                    daemon=True, name="FeedWatchdog"
                ).start()
        except Exception as e:
            _log.error(f"[WS] Ticker start failed: {e}")

    # ── Tick worker (community best practice) ────────────────
    # Drains the tick queue and dispatches to price_store + handles
    # feed-restored alerts. Runs in its own thread so on_ticks stays fast.
    def _tick_worker(self):
        """Worker thread: drains tick queue, updates price store, sends restored alert."""
        _log.info("[TickWorker] Started.")
        while not self._worker_stop:
            try:
                ticks = self._tick_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            # Mark feed as healthy on first tick after stall
            if not self._feed_healthy:
                self._feed_healthy = True
                if self._feed_was_alerted and self._notifier:
                    from datetime import datetime as _dt
                    _now_t = _dt.now().strftime("%H:%M")
                    _in_market_hours = "09:00" <= _now_t <= "23:30"
                    if _in_market_hours and getattr(self._notifier, 'has_active_strategies', lambda: True)():
                        try:
                            self._notifier.telegram(
                                "[FEED RESTORED] | MAIN\n\n"
                                "Live price feed is healthy again.\n"
                                "Resuming SL / TP / entry checks.")
                        except Exception:
                            pass
                    self._feed_was_alerted = False
                self._stall_alerted = False
            # Dispatch ticks to price store
            if self._on_tick_cb:
                for tick in ticks:
                    token = str(tick.get("instrument_token",""))
                    ltp   = float(tick.get("last_price", 0) or 0)
                    if token and ltp > 0:
                        try:
                            self._on_tick_cb(token, ltp)
                        except Exception as e:
                            _log.warning(f"[TickWorker] callback error: {e}")

    # ── Feed watchdog ─────────────────────────────────────
    # Background thread: every 30s, check if a tick arrived in the last 60s.
    # If not, alert via Telegram and force-restart the WebSocket. After 5 min
    # of continuous stall, send escalation alert. The is_feed_healthy() method
    # is read by the engine to skip SL/TP/entry checks while feed is stale.
    def _feed_watchdog(self):
        """Background thread that monitors feed health and force-restarts WS on stall."""
        TICK_TIMEOUT  = 60     # seconds without a tick = stalled
        CHECK_EVERY   = 30     # check interval
        STALL_ESCALATE = 300   # 5 min stall = escalation alert

        # Wait for first tick before starting checks (avoid false alarm at boot)
        boot_wait = 0
        while boot_wait < 60 and self._last_tick_ts == 0:
            time.sleep(2)
            boot_wait += 2
            if self._watchdog_stop:
                return

        _log.info("[Watchdog] Feed monitor started.")
        stall_started_at = 0.0

        while not self._watchdog_stop:
            time.sleep(CHECK_EVERY)
            if self._watchdog_stop: break

            now = time.time()
            silence = now - self._last_tick_ts if self._last_tick_ts > 0 else 0

            if silence > TICK_TIMEOUT:
                # Feed is stalled
                if stall_started_at == 0.0:
                    stall_started_at = now
                stall_duration = int(now - stall_started_at)

                # First-time stall detection -> force restart always.
                # Alert only during market hours (09:00-23:30 IST).
                # Outside market hours: reconnect silently, no Telegram noise.
                from datetime import datetime as _dt
                _now_t = _dt.now().strftime("%H:%M")
                _in_market_hours = "09:00" <= _now_t <= "23:30"

                if not self._feed_was_alerted:
                    _log.warning(
                        f"[Watchdog] Feed stalled ({silence:.0f}s no ticks). "
                        f"Forcing WebSocket restart...")
                    if self._notifier and _in_market_hours and getattr(self._notifier, 'has_active_strategies', lambda: True)():
                        try:
                            self._notifier.telegram(
                                "[FEED STALLED] | MAIN\n\n"
                                f"No price ticks for {silence:.0f} seconds.\n"
                                "Forcing WebSocket reconnect now.\n"
                                "SL / TP / entry checks paused until feed restores.")
                        except Exception: pass
                    self._feed_was_alerted = True
                    self._force_restart_ticker()

                # 5-min escalation — only during market hours
                elif stall_duration > STALL_ESCALATE and not self._stall_alerted:
                    _log.error(f"[Watchdog] Feed stalled >{STALL_ESCALATE}s, manual intervention may be needed.")
                    if self._notifier and _in_market_hours and getattr(self._notifier, 'has_active_strategies', lambda: True)():
                        try:
                            self._notifier.telegram(
                                "[FEED STALL — ATTENTION] | MAIN\n\n"
                                f"Feed has been down for {stall_duration//60} minutes.\n"
                                "Auto-reconnect attempts not succeeding.\n"
                                "Please check internet / Zerodha status.\n"
                                "Bot is paused (no trades on stale prices).")
                        except Exception: pass
                    self._stall_alerted = True
            else:
                # Feed healthy -- reset stall tracker
                stall_started_at = 0.0

    def wait_for_connection(self, timeout=30):
        """Wait for on_connect to fire — guarantees tokens are subscribed.
        Called by engine before feed check to avoid stale price false positive."""
        import time as _t
        waited = 0
        interval = 5
        max_wait = 300  # wait up to 5 minutes total
        while waited < max_wait:
            connected = self._ws_connected.wait(timeout=interval)
            if connected:
                _log.info("[WS] on_connect confirmed — feed check can proceed.")
                return True
            waited += interval
            _log.info(f"[WS] Waiting for on_connect... {waited}s elapsed. Retrying.")
        _log.warning(f"[WS] wait_for_connection gave up after {max_wait}s.")
        return False

    def _force_restart_ticker(self):
        """
        Force-drop the WebSocket connection so KiteTicker auto-reconnect fires.
        Uses _close() — NOT close() — because close() calls stop_retry() which
        permanently kills auto-reconnect. _close() just drops the connection
        and lets the existing reconnect loop handle it automatically.
        Running strategies are NOT affected — only the feed reconnects.
        """
        try:
            if self._ticker is not None:
                _log.info("[Watchdog] Dropping WebSocket via _close() to trigger auto-reconnect...")
                try:
                    self._ticker._close()
                    _log.info("[Watchdog] WebSocket dropped — auto-reconnect will fire in ~5s.")
                except Exception as ce:
                    _log.warning(f"[Watchdog] _close() failed: {ce} — reconnect may self-recover.")
            else:
                _log.warning("[Watchdog] No ticker to restart.")
        except Exception as e:
            _log.error(f"[Watchdog] _force_restart_ticker: {e}")

    def is_feed_healthy(self) -> bool:
        """
        Return True when ticks arrived within last 90s.
        90s (not 60s) to tolerate brief MCX auction pauses and
        normal quiet periods between trades on illiquid contracts.
        Engine reads this to pause SL/TP/entry checks during stalls.
        """
        if self._last_tick_ts == 0:
            return False  # never received any tick
        return (time.time() - self._last_tick_ts) < 90

    def feed_age_seconds(self) -> float:
        """Seconds since last tick. 0 if no tick ever received."""
        if self._last_tick_ts == 0:
            return 0.0
        return time.time() - self._last_tick_ts

    def unsubscribe_feed(self, tokens: list):
        """Unsubscribe tokens."""
        int_tokens = []
        for t in tokens:
            try: int_tokens.append(int(t.get("instrument_token","")))
            except: pass
        if self._ticker and int_tokens:
            try: self._ticker.unsubscribe(int_tokens)
            except: pass

    # ── Order Placement ────────────────────────────────────
    def place_order(self, exchange: str, symbol: str, qty: int,
                    side: str, price: float, order_type: str,
                    product: str, tag: str = "") -> str:
        """
        Place order on Zerodha.
        Returns order_id string or "" on failure.
        On failure, also stores the reason in self._last_order_error
        so OrderManager can classify and surface it.

        exchange   : "NSE", "BSE", "NFO", "BFO", "MCX"
        symbol     : tradingsymbol e.g. "NIFTY29APR2524000CE"
        qty        : quantity (lots * lot_size)
        side       : "BUY" or "SELL"
        price      : 0 for MARKET, else limit price
        order_type : "MARKET" or "LIMIT"
        product    : "MIS" (intraday) or "NRML" (positional)
        """
        self._last_order_error = ""
        try:
            # Map exchange names
            exch_map = {
                "nse_fo": "NFO", "bse_fo": "BFO", "mcx_fo": "MCX",
                "nse_cm": "NSE", "bse_cm": "BSE",
                "NFO":"NFO","BFO":"BFO","MCX":"MCX","NSE":"NSE","BSE":"BSE"
            }
            z_exchange = exch_map.get(exchange, exchange.upper())

            # Map order type
            z_order_type = (
                self._kite.ORDER_TYPE_MARKET if order_type.upper() == "MARKET"
                else self._kite.ORDER_TYPE_LIMIT
            )

            # Map product
            z_product = (
                self._kite.PRODUCT_MIS if product.upper() == "MIS"
                else self._kite.PRODUCT_NRML
            )

            # Map transaction type
            # Handle both "B"/"S" and "BUY"/"SELL" formats
            z_txn = (
                self._kite.TRANSACTION_TYPE_BUY if side.upper() in ("BUY", "B")
                else self._kite.TRANSACTION_TYPE_SELL
            )

            params = dict(
                variety          = self._kite.VARIETY_REGULAR,
                exchange         = z_exchange,
                tradingsymbol    = symbol,
                transaction_type = z_txn,
                quantity         = int(qty),
                product          = z_product,
                order_type       = z_order_type,
                price            = round(price, 2) if price else None,
                tag              = tag[:20] if tag else "",
            )
            if z_order_type == self._kite.ORDER_TYPE_MARKET:
                params.pop("price", None)

            order_id = self._kite.place_order(**params)
            _log.info(f"Order placed: {side} {qty} {symbol} @ {price} → {order_id}")
            return str(order_id)

        except Exception as e:
            err_str = str(e)
            self._last_order_error = err_str   # expose to OrderManager
            _log.error(f"place_order failed: {err_str}")
            return ""

    def get_order_status(self, order_id: str) -> dict:
        """
        Returns:
          {'status': 'COMPLETE'|'REJECTED'|'PENDING',
           'fill_price': float,
           'reason': str}   ← Zerodha's status_message on rejection
        """
        try:
            # Use order_history for specific order — more reliable than
            # scanning all orders (avoids confusion with other orders)
            try:
                history = self._kite.order_history(order_id=order_id)
                # Last entry is most recent status
                if history:
                    o = history[-1]
                    status = str(o.get("status","")).upper()
                    fill   = float(o.get("average_price", 0) or 0)
                    reason = str(o.get("status_message","") or
                                 o.get("status_message_raw","") or "")
                    if status == "COMPLETE":
                        return {"status":"COMPLETE", "fill_price": fill, "reason": ""}
                    elif status in ("REJECTED","CANCELLED"):
                        return {"status":"REJECTED", "fill_price": 0.0, "reason": reason}
                    else:
                        return {"status":"PENDING",  "fill_price": 0.0, "reason": ""}
            except Exception:
                pass  # Fall back to orders() scan

            # Fallback: scan all orders
            orders = self._kite.orders()
            for o in orders:
                if str(o.get("order_id","")) == str(order_id):
                    status = str(o.get("status","")).upper()
                    fill   = float(o.get("average_price", 0) or 0)
                    reason = str(o.get("status_message","") or
                                 o.get("status_message_raw","") or "")
                    if status == "COMPLETE":
                        return {"status":"COMPLETE", "fill_price": fill, "reason": ""}
                    elif status in ("REJECTED","CANCELLED"):
                        return {"status":"REJECTED", "fill_price": 0.0, "reason": reason}
                    else:
                        return {"status":"PENDING",  "fill_price": 0.0, "reason": ""}
            return {"status":"PENDING", "fill_price": 0.0, "reason": ""}
        except Exception as e:
            err_str = str(e)
            _log.error(f"get_order_status: {err_str}")
            return {"status":"PENDING", "fill_price": 0.0, "reason": err_str}

    def get_last_order_error(self) -> str:
        """Return the last order placement error string (set by place_order on failure)."""
        return getattr(self, "_last_order_error", "")

    def cancel_order(self, order_id: str):
        """Cancel a pending order."""
        try:
            self._kite.cancel_order(
                variety=self._kite.VARIETY_REGULAR,
                order_id=order_id)
            _log.info(f"Order cancelled: {order_id}")
        except Exception as e:
            _log.error(f"cancel_order: {e}")
        """Cancel a pending order."""
        try:
            self._kite.cancel_order(
                variety=self._kite.VARIETY_REGULAR,
                order_id=order_id)
            _log.info(f"Order cancelled: {order_id}")
        except Exception as e:
            _log.error(f"cancel_order: {e}")

    def modify_order(self, order_id: str, new_price: float) -> bool:
        """
        Modify an existing pending order price.
        More efficient than cancel + new order:
        - Keeps queue priority at exchange ✓
        - No duplicate orders ✓
        - Industry standard (Quantiply/AlgoTest approach) ✓
        """
        try:
            self._kite.modify_order(
                variety   = self._kite.VARIETY_REGULAR,
                order_id  = order_id,
                price     = round(new_price, 2))
            _log.info(f"Order modified: {order_id} → Rs {new_price:.2f}")
            return True
        except Exception as e:
            _log.warning(f"modify_order failed: {e}")
            return False

    def get_candle_high_low(self, instrument_token: str, from_time, to_time) -> dict:
        """
        Fetch the High and Low of a specific time window using Zerodha's
        historical data API. Used by Range Breakout to reconstruct the range
        when the bot starts AFTER the window has already closed.

        Returns: {'high': float, 'low': float, 'ok': bool}
        """
        try:
            from datetime import datetime as _dt
            # Zerodha expects datetime objects
            if isinstance(from_time, str):
                from_time = _dt.strptime(from_time, "%H:%M:%S").replace(
                    year=_dt.now().year, month=_dt.now().month, day=_dt.now().day)
            if isinstance(to_time, str):
                to_time = _dt.strptime(to_time, "%H:%M:%S").replace(
                    year=_dt.now().year, month=_dt.now().month, day=_dt.now().day)

            candles = self._kite.historical_data(
                instrument_token=int(instrument_token),
                from_date=from_time,
                to_date=to_time,
                interval="minute",
                continuous=False,
                oi=False
            )
            if not candles:
                _log.warning(f"get_candle_high_low: no candles returned for token {instrument_token}")
                return {"high": 0.0, "low": 0.0, "ok": False}

            high = max(c["high"] for c in candles)
            low  = min(c["low"]  for c in candles)
            _log.info(f"get_candle_high_low({instrument_token}): H={high} L={low} "
                      f"from {from_time.strftime('%H:%M')} to {to_time.strftime('%H:%M')} "
                      f"({len(candles)} candles)")
            return {"high": float(high), "low": float(low), "ok": True}

        except Exception as e:
            _log.error(f"get_candle_high_low: {e}")
            return {"high": 0.0, "low": 0.0, "ok": False}

    def supports_mcx_options(self) -> bool:
        return True

    def set_notifier(self, notifier):
        self._notifier = notifier
