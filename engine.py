"""
============================================================
  MY ALGO -- EXECUTION ENGINE
  Version : 1.0  --  April 2026
  Broker adapter pattern + multi-instrument strategy runner
============================================================
"""

import time, threading, logging, re, json, requests
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Optional

_log = logging.getLogger("Engine")

# ============================================================
#   INSTRUMENT REGISTRY
#   All lot sizes, segments, tick sizes, expiry formats
# ============================================================

# ── Expiry facts (verified from NSE/BSE circulars, effective Sep 2025) ──────
# NSE shifted ALL expiries from Thursday → Tuesday effective Sep 1, 2025.
# NIFTY 50  : weekly + monthly, expiry day = Tuesday (weekday 1)
# BANKNIFTY : monthly only (weekly discontinued Nov 2024), expiry = last Tuesday
# ── Static index token map ────────────────────────────────────────────────
# ONLY fields that CANNOT be derived from Zerodha's instrument CSVs are kept
# here. Everything else (lot size, exchange, is_mcx, has_options, has_weekly)
# is derived dynamically from live Zerodha data via get_instrument_info().
#
# index_token  : Zerodha token for the underlying SPOT price feed
#                (NSE/BSE cash segment). Used for ATM calculation and UL SL.
#                Not available in F&O CSV — must be hardcoded.
# index_exch   : Cash exchange segment for spot subscription (nse_cm / bse_cm)
# index_ws_key : Human-readable key for logging
#
# MCX commodities have no separate spot token (MCX futures ARE the price).
# MCX lot sizes — Zerodha CSV stores lot_size=1 for all MCX contracts.
# These are the ACTUAL trading units per lot as defined by MCX.
# Source: Dhan MCX lot size chart (verified April 2026)
# Used by refresh_instruments_from_broker to override the CSV value.
MCX_LOT_SIZES = {
    # Gold variants
    "GOLD"      : 100,    # 100 grams (1 kg)
    "GOLDM"     : 10,     # 10 grams (Gold Mini)
    "GOLDTEN"   : 10,     # 10 grams (Gold Ten)
    "GOLDPETAL" : 1,      # 1 gram (Gold Petal)
    "GOLDGUINEA": 8,      # 8 grams (Gold Guinea)
    # Silver variants
    "SILVER"    : 30,     # 30 kg
    "SILVERM"   : 5,      # 5 kg (Silver Mini)
    "SILVERMIC" : 1,      # 1 kg (Silver Micro)
    # Crude Oil variants
    "CRUDEOIL"  : 100,    # 100 barrels
    "CRUDEOILM" : 10,     # 10 barrels (Crude Mini)
    # Natural Gas variants
    "NATURALGAS": 1250,   # 1250 mmBtu
    "NATGASMINI": 250,    # 250 mmBtu (NG Mini)
    # Base Metals
    "COPPER"    : 2500,   # 2500 kg
    "COPPERM"   : 250,    # 250 kg (Copper Mini)
    "ZINC"      : 5000,   # 5 MT = 5000 kg
    "ZINCMINI"  : 1000,   # 1 MT = 1000 kg (Zinc Mini)
    "ALUMINIUM" : 5000,   # 5 MT = 5000 kg
    "ALUMINI"   : 1000,   # 1 MT = 1000 kg (Aluminium Mini)
    "LEAD"      : 5000,   # 5 MT = 5000 kg
    "LEADMINI"  : 1000,   # 1 MT = 1000 kg (Lead Mini)
    "NICKEL"    : 1500,   # 1.5 MT = 1500 kg
    "NICKELM"   : 100,    # 100 kg (Nickel Mini)
}

INDEX_TOKEN_MAP = {
    "NIFTY"      : {"index_token":"26000", "index_exch":"NSE","index_ws_key":"NSE:NIFTY 50"},
    "BANKNIFTY"  : {"index_token":"26009", "index_exch":"NSE","index_ws_key":"NSE:NIFTY BANK"},
    "FINNIFTY"   : {"index_token":"26037", "index_exch":"NSE","index_ws_key":"NSE:NIFTY FIN SERVICE"},
    "MIDCPNIFTY" : {"index_token":"26074", "index_exch":"NSE","index_ws_key":"NSE:NIFTY MID SELECT"},
    "SENSEX"     : {"index_token":"1",     "index_exch":"BSE","index_ws_key":"BSE:SENSEX"},
    "BANKEX"     : {"index_token":"99919003","index_exch":"BSE","index_ws_key":"BSE:BANKEX"},
}

# Zerodha CSV 'name' field for F&O contracts — may differ from our instrument key.
# Our key       → Zerodha CSV name field (as seen in NFO/BFO instrument list)
# These are used in resolve_strike() name filter and get_atm_strike() fallback.
ZERODHA_NAME_MAP = {
    "NIFTY"      : "NIFTY",               # NFO CSV: name=NIFTY
    "BANKNIFTY"  : "NIFTY BANK",          # NFO CSV: name=NIFTY BANK
    "FINNIFTY"   : "NIFTY FIN SERVICE",   # NFO CSV: name=NIFTY FIN SERVICE
    "MIDCPNIFTY" : "NIFTY MID SELECT",    # NFO CSV: name=NIFTY MID SELECT
    "SENSEX"     : "SENSEX",              # BFO CSV: name=SENSEX
    "BANKEX"     : "BANKEX",              # BFO CSV: name=BANKEX
}

# Legacy alias kept for backward compatibility with any code that still
# does INSTRUMENTS.get(). Populated dynamically once broker connects.
# Before broker connects, falls back to a minimal static dict.
INSTRUMENTS: dict = {}

def _build_instruments_static() -> dict:
    """
    Minimal static fallback used BEFORE broker connects (dry-run startup,
    offline testing). Contains only the NSE/BSE index derivatives and a
    handful of common MCX instruments. All values are best-known static data.
    Once the broker connects, get_instrument_info() replaces these with live
    data from Zerodha's CSV.
    """
    static = {}
    # NSE index derivatives
    for name, itok in INDEX_TOKEN_MAP.items():
        if itok["index_exch"] == "NSE":
            static[name] = {"exchange":"NFO","lot":25,"tick":0.05,
                            "is_mcx":False,"has_options":True,"has_weekly":True,
                            **itok}
        elif itok["index_exch"] == "BSE":
            static[name] = {"exchange":"BFO","lot":20,"tick":0.05,
                            "is_mcx":False,"has_options":True,"has_weekly":True,
                            **itok}
    # A handful of MCX instruments as fallback — broker replaces with live data
    _mcx_fallback = {
        "GOLD":100,"GOLDM":10,"GOLDTEN":10,"GOLDPETAL":1,"GOLDGUINEA":8,
        "SILVER":30,"SILVERM":5,"SILVERMIC":1,
        "CRUDEOIL":100,"CRUDEOILM":10,
        "NATURALGAS":1250,"NATGASMINI":250,
        "COPPER":2500,"COPPERM":250,
        "ZINC":5000,"ZINCMINI":1000,
        "ALUMINIUM":5000,"ALUMINI":1000,
        "LEAD":5000,"LEADMINI":1000,
        "NICKEL":1500,"NICKELM":100,
    }
    for name, lot in _mcx_fallback.items():
        static[name] = {"exchange":"MCX","lot":lot,"tick":1.0,
                        "is_mcx":True,"has_options":False,"has_weekly":False,
                        "index_token":"","index_exch":"MCX","index_ws_key":""}
    return static

INSTRUMENTS = _build_instruments_static()

def refresh_instruments_from_broker(broker) -> None:
    """
    Called after broker connects. Builds the live INSTRUMENTS dict from
    Zerodha's instrument CSVs so all fields are accurate and up to date.

    Derived fields (no hardcoding needed):
      exchange     — from Zerodha CSV 'exchange' column per contract
      lot          — from Zerodha CSV 'lot_size' column
      is_mcx       — exchange == "MCX"
      has_options  — instrument has CE or PE contracts in the CSV
      has_weekly   — instrument has >1 distinct expiry month in current month
      tick         — from Zerodha CSV 'tick_size' column

    Static fields (kept from INDEX_TOKEN_MAP):
      index_token, index_exch, index_ws_key
    """
    global INSTRUMENTS
    from collections import defaultdict
    from datetime import date as _date

    try:
        # Ask broker for all F&O instruments across all exchanges
        all_rows = (broker.get_all_fo_instruments()
                    if hasattr(broker, "get_all_fo_instruments")
                    else [])
        if not all_rows:
            _log.warning("refresh_instruments_from_broker: broker returned no rows; "
                         "keeping static fallback.")
            return

        # Group rows by instrument name
        by_name: dict = defaultdict(list)
        # Build reverse map: Zerodha CSV name → our instrument key
        # e.g. "NIFTY BANK" → "BANKNIFTY", "NIFTY FIN SERVICE" → "FINNIFTY"
        _ZERODHA_TO_KEY = {v: k for k, v in ZERODHA_NAME_MAP.items()}

        for row in all_rows:
            raw_name = str(row.get("name","")).upper().strip()
            # Translate Zerodha CSV name to our instrument key
            name = _ZERODHA_TO_KEY.get(raw_name, raw_name)
            if name:
                by_name[name].append(row)

        today = _date.today()
        new_instruments = {}

        # Index instruments supported by this platform.
        # F&O stocks (RELIANCE, TCS, INFY etc.) are intentionally excluded —
        # this platform trades index options and MCX commodities only.
        # MCX instruments are identified by exchange=="MCX" (no whitelist needed).
        _NSE_INDICES = {
            "NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY",
        }
        _BSE_INDICES = {
            "SENSEX","BANKEX",
        }
        _ALLOWED_INDICES = _NSE_INDICES | _BSE_INDICES
        _ALLOWED_MCX = {"CRUDEOIL","CRUDEOILM","NATURALGAS","NATGASMINI"}

        for name, rows in by_name.items():
            # Skip equity rows (EQ segment) — we only want derivatives
            types = {str(r.get("instrument_type","")).upper() for r in rows}
            if types == {"EQ"} or not (types & {"CE","PE","FUT"}):
                continue

            # Exchange from first row
            exch = str(rows[0].get("exchange","")).upper()
            is_mcx = (exch == "MCX")

            # Only allow: MCX instruments (all) + known NSE/BSE index instruments.
            # This prevents F&O stocks (RELIANCE, TCS, HDFC etc.) from appearing
            # in the instrument dropdown — this platform only trades indices + MCX.
            if is_mcx and name not in _ALLOWED_MCX:
                continue
            if not is_mcx and name not in _ALLOWED_INDICES:
                continue

            # Lot size: use most common non-zero value across contracts
            # EXCEPTION: MCX CSV stores lot_size=1 (one contract) for all instruments.
            # The actual trading unit (e.g. 1250 MMBtu for NATURALGAS, 100 for CRUDEOIL)
            # must come from our static table — NOT the CSV.
            if is_mcx:
                # Always use static MCX lot sizes — CSV value is unreliable (usually 1)
                lot = MCX_LOT_SIZES.get(name, 1)
            else:
                lot_sizes = [int(r.get("lot_size",0) or 0) for r in rows if r.get("lot_size")]
                lot = max(set(lot_sizes), key=lot_sizes.count) if lot_sizes else 1

            # Tick size
            ticks = [float(r.get("tick_size",0) or 0) for r in rows if r.get("tick_size")]
            tick = ticks[0] if ticks else 0.05

            # has_options: instrument has CE or PE contracts
            has_options = bool(types & {"CE","PE"})

            # has_weekly: check if there are multiple distinct expiry dates
            # within the SAME calendar month (implies weekly contracts exist)
            from datetime import datetime as _dt
            expiry_months = set()
            expiry_dates = set()
            for r in rows:
                exp_str = r.get("expiry","")
                if not exp_str: continue
                try:
                    exp_d = _dt.strptime(exp_str, "%Y-%m-%d").date()
                    if exp_d >= today:
                        expiry_months.add((exp_d.year, exp_d.month))
                        expiry_dates.add(exp_d)
                except Exception:
                    pass
            # If a single month has more than one expiry date → weekly contracts
            from collections import Counter
            month_counts = Counter((exp_d.year, exp_d.month)
                                   for exp_d in expiry_dates)
            has_weekly = any(v > 1 for v in month_counts.values())

            # Known MCX instruments that have options — used as a floor.
            # On expiry day, Zerodha's CSV may only show the expiring FUT
            # and not yet list next-month CE/PE contracts (uploaded after
            # market close). So has_options could be False from the CSV even
            # though options DO exist and will be tradeable tomorrow.
            # Solution: if our static fallback says has_options=True for this
            # instrument, never downgrade it to False from the live CSV.
            _static_fallback = INSTRUMENTS.get(name, {})
            if _static_fallback.get("has_options") and not has_options:
                has_options = True   # preserve known capability; CSV is just incomplete today

            info = {
                "exchange"   : exch,
                "lot"        : lot,
                "tick"       : tick,
                "is_mcx"     : is_mcx,
                "has_options": has_options,
                "has_weekly" : has_weekly,
                "index_token": "",
                "index_exch" : "MCX" if is_mcx else ("NSE" if exch == "NFO" else "BSE"),
                "index_ws_key": "",
            }
            # Merge static index token info if available
            if name in INDEX_TOKEN_MAP:
                info.update(INDEX_TOKEN_MAP[name])

            new_instruments[name] = info

        if new_instruments:
            INSTRUMENTS.clear()
            INSTRUMENTS.update(new_instruments)
            _log.info(f"refresh_instruments_from_broker: loaded {len(INSTRUMENTS)} "
                      f"instruments from live Zerodha data.")
        else:
            _log.warning("refresh_instruments_from_broker: derived 0 instruments; "
                         "keeping static fallback.")
    except Exception as e:
        _log.error(f"refresh_instruments_from_broker failed: {e}. "
                   f"Keeping static fallback.")



# Minimum valid option strike per instrument.
# Used to reject cross-listed tokens (e.g. Kotak returns SENSEX-prefixed
# items where pTrdSymbol says SENSEX but strike is in NIFTY range ~25000).
# Set conservatively — well below any real-world level the index can reach.
STRIKE_MIN = {
    "NIFTY":       5000, "BANKNIFTY":  15000, "FINNIFTY":    5000,
    "MIDCPNIFTY":  2000, "SENSEX":     60000, "BANKEX":      50000,
    "GOLD": 10000, "SILVER": 10000, "CRUDEOIL": 1000,
    "NATURALGAS": 50, "COPPER": 100, "ZINC": 50,
}
STRIKE_MAX = {
    "NIFTY":      50000, "BANKNIFTY": 120000, "FINNIFTY":   60000,
    "MIDCPNIFTY": 30000, "SENSEX":    200000, "BANKEX":    120000,
    "GOLD": 200000, "SILVER": 300000, "CRUDEOIL": 30000,
    "NATURALGAS": 1000,  "COPPER": 1200, "ZINC": 500,
}

# ============================================================
#   BROKER ABSTRACTION LAYER
# ============================================================

class BrokerBase(ABC):
    """
    Abstract broker interface.
    To add a new broker: subclass BrokerBase, implement all
    abstract methods. The execution engine calls only these
    methods and never knows which broker is underneath.
    """
    name = "BasebrokerBase"

    @abstractmethod
    def login(self, **kwargs) -> bool:
        """Authenticate. Returns True on success."""

    @abstractmethod
    def subscribe_feed(self, tokens: list, on_tick: callable):
        """Subscribe to live price feed. on_tick(token, ltp) called per tick."""

    @abstractmethod
    def unsubscribe_feed(self, tokens: list):
        """Unsubscribe tokens."""

    @abstractmethod
    def get_option_chain(self, instrument: str, expiry_str: str) -> list:
        """Return list of option contracts for instrument+expiry."""

    def get_fut_chain(self, instrument: str, expiry_str: str) -> list:
        """Return list of futures contracts. Default: empty (subclass overrides)."""
        return []

    @abstractmethod
    def place_order(self, exchange: str, symbol: str, qty: int,
                    side: str, price: float, order_type: str,
                    product: str, tag: str = "") -> str:
        """Place order. Returns order_id or '' on failure."""

    @abstractmethod
    def get_order_status(self, order_id: str) -> dict:
        """Returns {'status': 'COMPLETE'|'REJECTED'|'PENDING', 'fill_price': float}"""

    @abstractmethod
    def cancel_order(self, order_id: str):
        """Cancel a pending order."""

    def get_available_expiries(self, instrument: str) -> list:
        """
        Return sorted list of available expiry date strings from broker.
        Each string is in DDMMMYYYY format, e.g. ["24APR2026","29APR2026"].
        Default: empty list (subclass overrides if broker supports it).
        """
        return []

    def supports_mcx_options(self) -> bool:
        """Override to True in brokers that support MCX options."""
        return False



class DhanAdapter(BrokerBase):
    name = "Dhan"
    def login(self, **kw): raise NotImplementedError("Dhan adapter coming soon")
    def subscribe_feed(self, t, cb): raise NotImplementedError
    def unsubscribe_feed(self, t): raise NotImplementedError
    def get_option_chain(self, i, e): raise NotImplementedError
    def place_order(self, *a, **k): raise NotImplementedError
    def get_order_status(self, o): raise NotImplementedError
    def cancel_order(self, o): raise NotImplementedError
    def supports_mcx_options(self): return True

class AngelOneAdapter(BrokerBase):
    name = "Angel One"
    def login(self, **kw): raise NotImplementedError("Angel One adapter coming soon")
    def subscribe_feed(self, t, cb): raise NotImplementedError
    def unsubscribe_feed(self, t): raise NotImplementedError
    def get_option_chain(self, i, e): raise NotImplementedError
    def place_order(self, *a, **k): raise NotImplementedError
    def get_order_status(self, o): raise NotImplementedError
    def cancel_order(self, o): raise NotImplementedError
    def supports_mcx_options(self): return True

class UpstoxAdapter(BrokerBase):
    name = "Upstox"
    def login(self, **kw): raise NotImplementedError("Upstox adapter coming soon")
    def subscribe_feed(self, t, cb): raise NotImplementedError
    def unsubscribe_feed(self, t): raise NotImplementedError
    def get_option_chain(self, i, e): raise NotImplementedError
    def place_order(self, *a, **k): raise NotImplementedError
    def get_order_status(self, o): raise NotImplementedError
    def cancel_order(self, o): raise NotImplementedError

class GrowwAdapter(BrokerBase):
    name = "Groww"
    def login(self, **kw): raise NotImplementedError("Groww adapter coming soon")
    def subscribe_feed(self, t, cb): raise NotImplementedError
    def unsubscribe_feed(self, t): raise NotImplementedError
    def get_option_chain(self, i, e): raise NotImplementedError
    def place_order(self, *a, **k): raise NotImplementedError
    def get_order_status(self, o): raise NotImplementedError
    def cancel_order(self, o): raise NotImplementedError

from angelone_adapter import AngelOneAdapter

BROKER_MAP = {
    "angelone" : AngelOneAdapter,
}


# ============================================================
#   LIVE PRICE STORE (shared across all strategies)
# ============================================================

class PriceStore:
    def __init__(self):
        self._prices = {}
        self._lock   = threading.Lock()
        self._ts     = 0.0

    def update(self, token: str, ltp: float):
        with self._lock:
            self._prices[str(token)] = ltp
            self._ts = time.time()

    def get(self, token: str) -> float:
        with self._lock:
            return self._prices.get(str(token), 0.0)

    def count(self) -> int:
        with self._lock:
            return sum(1 for v in self._prices.values() if v > 0)

    def last_ts(self) -> float:
        return self._ts

    def all(self) -> dict:
        with self._lock:
            return dict(self._prices)


# Global price store shared by all strategies
price_store     = PriceStore()
_kite_obj       = None   # not used in Angel One — kept for compatibility
_rest_ltp_cache = {}     # {token: (ltp, timestamp)} — throttled REST LTP for illiquid tokens


# ============================================================
#   EXPIRY UTILITIES
# ============================================================

def nearest_expiry_from_broker(broker, instrument: str,
                               expiry_type: str = "weekly") -> str:
    """
    PERMANENT SOLUTION: Fetch real expiry dates directly from the broker API.

    This function replaces all hardcoded expiry logic. It asks the broker
    which contracts actually exist today and selects the correct one based
    on expiry_type. No weekday calculations. No hardcoded Tuesday/Thursday.
    No need to update code when SEBI/NSE/BSE change expiry days.

    Parameters
    ──────────
    broker       : BrokerBase instance (must be logged in)
    instrument   : e.g. "NIFTY", "SENSEX", "NATURALGAS"
    expiry_type  : "weekly" | "next_weekly" | "monthly" | "next_month"

    Returns
    ───────
    Expiry string in DDMMMYYYY format e.g. "29APR2026"
    Returns "" if no expiries available (caller must handle).
    """
    from datetime import datetime as _dt

    def parse_exp(s):
        try:   return _dt.strptime(s, "%d%b%Y").date()
        except: return None

    today     = date.today()
    # Get all available expiry dates from broker live data
    _is_mcx = INSTRUMENTS.get(instrument,{}).get("is_mcx",False)
    all_exps_str  = broker.get_available_expiries(instrument, opt_only=_is_mcx) if hasattr(broker,"get_available_expiries") and "opt_only" in broker.get_available_expiries.__code__.co_varnames else broker.get_available_expiries(instrument)
    all_exps_date = [(s, parse_exp(s)) for s in all_exps_str]
    all_exps_date = [(s, d) for s, d in all_exps_date if d is not None]
    all_exps_date.sort(key=lambda x: x[1])       # sort oldest → newest

    if not all_exps_date:
        _log.warning(
            f"No expiries returned by broker for {instrument}. "
            f"Check broker connection and instrument symbol.")
        return ""

    # All future expiry dates (today or later)
    future = [(s, d) for s, d in all_exps_date if d >= today]
    if not future:
        _log.warning(f"No future expiries for {instrument}. All expired today?")
        return all_exps_date[-1][0]   # return last known

    if expiry_type == "weekly":
        # Nearest expiry from Angel One data — no day calculation
        return future[0][0]

    elif expiry_type == "next_weekly":
        # Second nearest expiry from Angel One data
        # If second expiry is in same month as first — it is a genuine next weekly
        # If not — fallback to monthly (last of current month)
        if len(future) >= 2:
            first_exp  = future[0][1]
            second_exp = future[1][1]
            if second_exp.month == first_exp.month:
                return future[1][0]  # genuine next weekly — same month
            else:
                # No next weekly in same month — fallback to monthly
                this_month = [(s,d) for s,d in future
                              if d.year==today.year and d.month==today.month]
                if this_month:
                    return this_month[-1][0]
        return future[0][0]  # absolute fallback

    elif expiry_type == "monthly":
        # The expiry whose date is the LAST one in the current calendar month.
        # This naturally picks the monthly contract whether weekly contracts
        # exist or not — it's just the latest-dated contract this month.
        this_month = [(s, d) for s, d in future
                      if d.year == today.year and d.month == today.month]
        if this_month:
            return this_month[-1][0]   # last expiry of current month = monthly
        # If current month has no future expiries, roll to next month
        next_m = today.month + 1 if today.month < 12 else 1
        next_y = today.year if today.month < 12 else today.year + 1
        next_month = [(s, d) for s, d in future
                      if d.year == next_y and d.month == next_m]
        if next_month:
            return next_month[-1][0]
        return future[-1][0]   # fallback

    elif expiry_type == "next_month":
        # Last expiry of next calendar month
        nm = today.month + 1 if today.month < 12 else 1
        ny = today.year if today.month < 12 else today.year + 1
        nxt = [(s, d) for s, d in future
               if d.year == ny and d.month == nm]
        if nxt:
            return nxt[-1][0]
        # Fallback: next-month contracts may not yet be listed by the broker
        # (common on the last trading day of the current contract — broker
        # uploads next-month instruments after market close or next morning).
        # Return empty string so the caller can skip/warn rather than trading
        # the wrong contract.
        _log.warning(
            f"{instrument}: next_month ({ny}-{nm:02d}) contracts not yet "
            f"available in broker instrument list. "
            f"This is normal on the last trading day of the current contract. "
            f"Retry after broker refreshes (usually next morning).")
        return ""

    return future[0][0]   # default fallback


def nearest_expiry(instrument: str, expiry_type: str = "weekly") -> date:
    """
    FALLBACK ONLY — used when broker is not yet connected (e.g. dry-run
    before login). Returns a calculated date using the last-known exchange
    rules. Once broker is connected, nearest_expiry_from_broker() is used
    instead and this function is never called in live trading.
    """
    import calendar as _cal
    today     = date.today()
    info      = INSTRUMENTS.get(instrument, {})
    is_mcx    = info.get("is_mcx", False)
    exp_day   = info.get("expiry_day", 1)     # 1=Tue (NSE), 3=Thu (BSE)
    has_weekly= info.get("has_weekly", False)

    def last_weekday_of_month(yr, mo, wday):
        last = date(yr, mo, _cal.monthrange(yr, mo)[1])
        diff = (last.weekday() - wday) % 7
        return last - timedelta(days=diff)

    def last_working_day_of_month(yr, mo):
        last = date(yr, mo, _cal.monthrange(yr, mo)[1])
        while last.weekday() > 4: last -= timedelta(days=1)
        return last

    def get_monthly(yr, mo):
        if is_mcx: return last_working_day_of_month(yr, mo)
        return last_weekday_of_month(yr, mo, exp_day)

    def next_mo(yr, mo): return (yr, mo+1) if mo<12 else (yr+1, 1)

    if expiry_type in ("monthly","next_month"):
        exp = get_monthly(today.year, today.month)
        if expiry_type=="next_month" or exp<=today:
            exp = get_monthly(*next_mo(today.year, today.month))
        return exp
    if not has_weekly:
        return get_monthly(today.year, today.month)
    days_until = (exp_day - today.weekday()) % 7
    if days_until == 0: days_until = 7
    nearest_weekly = today + timedelta(days=days_until)
    if expiry_type == "next_weekly": return nearest_weekly + timedelta(weeks=1)
    return nearest_weekly



def expiry_fmt(d: date) -> str:
    return d.strftime("%d%b%Y").upper()

def current_dte(instrument: str) -> int:
    exp = nearest_expiry(instrument)
    return (exp - date.today()).days

def should_trade_today() -> bool:
    dte = current_dte("NIFTY")
    return dte in {0, 1, 2, 3}


# ============================================================
#   STRIKE SELECTOR
# ============================================================

def fmt_sym(raw: str) -> str:
    """
    Convert Kotak trading symbol to clean display format.
    Handles all instruments: NSE/BSE options/futures + MCX futures.

    Kotak format: INSTRUMENT + DDMMMYY + STRIKE + (CE|PE)   (options)
                  INSTRUMENT + DDMMMYY + FUT                 (futures)

    Examples (all verified):
      NIFTY26APR2579900CE      -> 79900 CE
      BANKNIFTY26APR2556845CE  -> 56845 CE
      SENSEX26APR2579900PE     -> 79900 PE
      BANKNIFTY26APR25FUT      -> BANKNIFTY FUT
      GOLDM26APR25FUT          -> GOLDM FUT
      379900CE  (short form)   -> 79900 CE
      2579900CE (no prefix)    -> 79900 CE
    """
    if not raw:
        return "--"
    s = str(raw).strip()
    u = s.upper()

    # Already in clean short form: "23500 CE" / "BANKNIFTY FUT"
    if len(s) <= 16 and re.match(r'^[\w.]+\s+(CE|PE|FUT)$', u):
        return s

    # Raw numeric token (from price store)
    if re.match(r'^\d{1,10}$', s):
        return s

    # Kotak uses 2-digit year: 26APR25 (DDMMMYY)
    MNTHS = r'(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)'
    D2    = r'\d{2}' + MNTHS + r'\d{2}'

    # FUT: INSTRUMENT + DATE + FUT
    mf = re.match(r'^([A-Z]+)(' + D2 + r')FUT$', u)
    if mf:
        return mf.group(1) + ' FUT'

    # Options primary: INSTRUMENT + DATE + 4-6 digit STRIKE + CE/PE
    mo = re.match(r'^([A-Z]+)(' + D2 + r')(\d{2,6}(?:\.\d+)?)(CE|PE)$', u)
    if mo:
        try:    strike = str(int(float(mo.group(3))))
        except: strike = mo.group(3)
        return strike + ' ' + mo.group(4)

    # Options fallback: find CE/PE at end, then extract strike from preceding digits.
    # Handles short forms like "379900CE" or "2579900CE" where the full Kotak
    # name isn't present. Takes the last digit sequence and trims leading
    # year-bleed digits (e.g. "25" prefix from DDMMMYY).
    m_type = re.search(r'(CE|PE)$', u)
    if m_type:
        opt    = m_type.group(1)
        prefix = u[:m_type.start()]
        nums   = re.findall(r'\d+', prefix)
        if nums:
            last_num = nums[-1]
            # If 7+ digits the last 5 are the strike (NSE/BSE strikes are 5 digits)
            if len(last_num) > 6:
                last_num = last_num[-5:]
            elif len(last_num) == 6:
                # 6-digit: first digit might be year bleed -- if last 5 is valid use them
                cand = last_num[-5:]
                try:
                    if 1000 <= int(cand) <= 999999:
                        last_num = cand
                except Exception:
                    pass
            try:    last_num = str(int(last_num))
            except: pass
            return last_num + ' ' + opt

    # FUT fallback: letters + digits + FUT
    ff = re.match(r'^([A-Z]+)\d+FUT$', u)
    if ff:
        return ff.group(1) + ' FUT'

    return s


def resolve_strike(leg: dict, instrument: str, option_chain: list) -> tuple:
    """
    Resolve leg strike selection → (token, trading_symbol, ref_ltp).
    Modes: ATM / ATM+N / ATM-N / OTM1..N / ITM1..N / Closest Premium /
           Premium <= / Premium >= / % of ATM / Specific Strike / FUT
    Supports Zerodha CSV fields AND Kotak field aliases.
    """
    opt_type = str(leg.get("type", "CE")).upper()
    _raw_st  = leg.get("stType", "ATM") or "ATM"
    st_type  = _raw_st if _raw_st not in ("Strike Type", "", None) else "ATM"
    prem_val = float(leg.get("premVal") or 0)
    strike   = leg.get("strike", "ATM") or "ATM"

    # ── FUTURES ─────────────────────────────────────────────────
    if opt_type == "FUT":
        fut_candidates = []
        for item in option_chain:
            sym    = str(item.get("tradingsymbol") or item.get("pTrdSymbol") or "").strip()
            tok    = str(item.get("instrument_token") or item.get("pSymbol") or "").strip()
            sym_up = sym.upper()
            opt_field = str(item.get("instrument_type") or item.get("pOptionType") or
                           item.get("prcTyp") or "").upper().strip()
            is_fut = (opt_field == "FUT" or sym_up.endswith("FUT") or
                      (opt_field in ("XX","F","") and
                       not sym_up.endswith("CE") and not sym_up.endswith("PE")))
            if not is_fut or not tok:
                continue
            # Try both str and int token lookup — price_store may use int keys
            _ltp = price_store.get(tok) or price_store.get(int(tok) if str(tok).isdigit() else tok) or 0.0
            fut_candidates.append({"tok": tok, "sym": sym, "ltp": _ltp})
        if not fut_candidates:
            _log.warning(f"FUT resolve: no contracts found ({len(option_chain)} items)")
            return "", "", 0.0
        valid = sorted([c for c in fut_candidates if c["ltp"] > 0], key=lambda c: len(c["sym"]))
        best  = valid[0] if valid else sorted(fut_candidates, key=lambda c: len(c["sym"]))[0]
        return best["tok"], best["sym"], best["ltp"] if best["ltp"] > 0 else 0.001

    # ── OPTIONS: build candidate list ───────────────────────────
    instr_upper = instrument.upper()
    instr_exch  = INSTRUMENTS.get(instrument, {}).get("exchange", "")

    # Normalise instrument exchange to a canonical lowercase form so we can
    # compare against what's on the chain item (which may be "NFO"/"BFO"/"MCX"
    # uppercase from Zerodha or "nse_fo"/"bse_fo"/"mcx_fo" lowercase legacy).
    _canon = {
        "nfo": "nse_fo", "NFO": "nse_fo", "nse_fo": "nse_fo",
        "bfo": "bse_fo", "BFO": "bse_fo", "bse_fo": "bse_fo",
        "mcx": "mcx_fo", "MCX": "mcx_fo", "mcx_fo": "mcx_fo",
    }
    instr_canon = _canon.get(instr_exch, instr_exch.lower())

    candidates  = []

    # Compute spot price ONCE before the loop for dynamic strike filter.
    # Called once per resolve_strike() call — not per contract.
    _spot_for_filter = get_atm_strike(instrument, option_chain)

    for item in option_chain:
        # Exchange filter — compare canonical forms on both sides
        item_exch_raw = str(item.get("exchange") or item.get("pExchSeg") or "").strip()
        item_canon    = _canon.get(item_exch_raw, item_exch_raw.lower())
        if item_canon and instr_canon and item_canon != instr_canon:
            continue
        # Name filter — use ZERODHA_NAME_MAP to handle instruments where
        # our key differs from Zerodha CSV name field:
        #   BANKNIFTY → "NIFTY BANK", FINNIFTY → "NIFTY FIN SERVICE" etc.
        sym_name   = str(item.get("name") or item.get("pSymbolName") or "").upper().strip()
        tradingsym = str(item.get("tradingsymbol") or item.get("pTrdSymbol") or "").upper().strip()
        zerodha_name = ZERODHA_NAME_MAP.get(instrument, instr_upper)
        if sym_name:
            if sym_name != instr_upper and sym_name != zerodha_name:
                # Neither key nor mapped name matches — try tradingsymbol prefix
                if not tradingsym.startswith(instr_upper):
                    continue
        # Option type
        item_ot = str(item.get("instrument_type") or item.get("pOptionType") or
                      item.get("prcTyp") or "").upper().strip()
        if not item_ot:
            sym_t = str(item.get("tradingsymbol") or item.get("pTrdSymbol") or "").upper()
            item_ot = "CE" if sym_t.endswith("CE") else ("PE" if sym_t.endswith("PE") else "")
        if item_ot != opt_type:
            continue
        tok = str(item.get("instrument_token") or item.get("pSymbol") or "")
        sym = str(item.get("tradingsymbol") or item.get("pTrdSymbol") or tok)
        # Strike
        sp = 0.0
        if item.get("strike") is not None:
            try: sp = float(item["strike"])
            except: pass
        if sp <= 0 and item.get("dStrikePrice;") is not None:
            try: sp = float(item["dStrikePrice;"]) / 100.0
            except: pass
        if sp <= 0:
            fb = re.findall(r"\d+", re.sub(r"(CE|PE)$","",sym.upper()))
            if fb:
                try:
                    c2 = fb[-1]; c2 = c2[-5:] if len(c2) > 6 else c2; sp = float(c2)
                except: pass
        # Strike range validation — DYNAMIC, based on current spot price
        # Industry standard (Quantiply/AlgoTest): only keep strikes within
        # a reasonable range of current ATM. This works for any index at any
        # price level without hardcoding — SENSEX at 77000 or 200000, same logic.
        if sp > 0 and _spot_for_filter > 0:
            _lo = _spot_for_filter * 0.70
            _hi = _spot_for_filter * 1.30
            if sp < _lo or sp > _hi:
                continue
        candidates.append({"tok":tok,"sym":sym,"ltp":price_store.get(tok),"strike":sp})

    if not candidates:
        # Check if chain has contracts but all are FUT (no CE/PE)
        # This happens on expiry day when next-month options not yet listed by broker
        total_in_chain = len(option_chain)
        if total_in_chain > 0:
            all_types = {str(i.get("instrument_type","")).upper() for i in option_chain}
            if "FUT" in all_types and not ({"CE","PE"} & all_types):
                _log.warning(
                    f"resolve_strike({instrument}): chain has {total_in_chain} FUT contract(s) "
                    f"but NO {opt_type} options. Zerodha has not yet uploaded next-month "
                    f"option contracts (common on expiry day). Retry tomorrow morning.")
            else:
                _log.warning(
                    f"resolve_strike({instrument}): no {opt_type} candidates in chain "
                    f"of {total_in_chain} contracts.")
        return "", "", 0.0

    live = [c for c in candidates if c["ltp"] > 0]

    # ── Closest Premium ──────────────────────────────────────────
    if st_type == "Closest Premium":
        pool = live if live else candidates
        best = min(pool, key=lambda c: abs(c["ltp"] - prem_val))
        return best["tok"], best["sym"], best["ltp"]

    # ── Premium <= (lower than) ──────────────────────────────────
    elif st_type == "Premium <=":
        within = [c for c in live if c["ltp"] <= prem_val]
        if within:
            best = max(within, key=lambda c: c["ltp"])
            return best["tok"], best["sym"], best["ltp"]
        if live:
            _log.warning(f"Premium <=: no strike with LTP<={prem_val} for {instrument}")
            return "", "", 0.0
        return candidates[0]["tok"], candidates[0]["sym"], 0.0

    # ── Premium >= (higher than) ─────────────────────────────────
    elif st_type == "Premium >=":
        within = [c for c in live if c["ltp"] >= prem_val]
        if within:
            best = min(within, key=lambda c: c["ltp"])  # lowest LTP at or above limit
            return best["tok"], best["sym"], best["ltp"]
        if live:
            _log.warning(f"Premium >=: no strike with LTP>={prem_val} for {instrument}")
            return "", "", 0.0
        return candidates[0]["tok"], candidates[0]["sym"], 0.0

    # ── % of ATM ─────────────────────────────────────────────────
    # Per Quantiply spec: target = (ATM CE LTP + ATM PE LTP) / 2 * pct/100
    # Picks strike of the SAME opt_type whose LTP is closest to that target.
    # NOTE: previously this multiplied the ATM strike NUMBER by pct (e.g.
    # 24500 * 50% = 12250) which produced an impossible target far from any
    # real strike's premium.
    elif st_type == "% of ATM":
        atm = get_atm_strike(instrument, option_chain)
        if atm <= 0: return "", "", 0.0
        # Find ATM CE & PE LTPs from chain to compute the reference premium
        atm_ce_ltp = 0.0
        atm_pe_ltp = 0.0
        for c in candidates + [c2 for c2 in option_chain if c2 not in candidates]:
            try: sp_c = float(c.get("strike", 0))
            except: continue
            if abs(sp_c - atm) > 0.01: continue  # only ATM strike
            tok_c = str(c.get("instrument_token") or c.get("pSymbol") or "")
            ltp_c = price_store.get(tok_c) if tok_c else 0.0
            if ltp_c <= 0: continue
            ot_c = str(c.get("instrument_type") or c.get("pOptionType") or "").upper().strip()
            if not ot_c:
                sym_c = str(c.get("tradingsymbol") or c.get("pTrdSymbol") or "").upper()
                ot_c = "CE" if sym_c.endswith("CE") else ("PE" if sym_c.endswith("PE") else "")
            if   ot_c == "CE": atm_ce_ltp = max(atm_ce_ltp, ltp_c)
            elif ot_c == "PE": atm_pe_ltp = max(atm_pe_ltp, ltp_c)
        # Use straddle midpoint as ATM premium reference
        atm_premium = (atm_ce_ltp + atm_pe_ltp) / 2.0 if (atm_ce_ltp > 0 and atm_pe_ltp > 0) \
                      else max(atm_ce_ltp, atm_pe_ltp)
        if atm_premium <= 0:
            _log.warning(f"% of ATM: ATM premium unknown for {instrument}, cannot resolve.")
            return "", "", 0.0
        target_ltp = atm_premium * prem_val / 100.0
        _log.info(f"% of ATM({instrument}): atm_prem={atm_premium:.2f} pct={prem_val}% "
                  f"target_ltp={target_ltp:.2f}")
        pool = live if live else candidates
        best = min(pool, key=lambda c: abs(c["ltp"] - target_ltp))
        return best["tok"], best["sym"], best["ltp"]

    # ── Specific Strike ──────────────────────────────────────────
    elif st_type == "Specific Strike":
        try: target_sp = float(strike)
        except: return "", "", 0.0
        exact = [c for c in candidates if c["strike"] == target_sp]
        pool  = exact if exact else sorted(candidates, key=lambda c: abs(c["strike"]-target_sp))
        return (pool[0]["tok"], pool[0]["sym"], pool[0]["ltp"]) if pool else ("","",0.0)

    # ── ATM offset: ATM / OTM1..N / ITM1..N / ATM+pts / ATM-pts ─
    else:
        atm = get_atm_strike(instrument, option_chain)
        if atm <= 0: return "", "", 0.0
        interval = {"NIFTY":50,"BANKNIFTY":100,"FINNIFTY":50,"MIDCPNIFTY":25,
                    "SENSEX":100,"BANKEX":100,"GOLD":100,"SILVER":100,
                    "CRUDEOIL":100,"NATURALGAS":5,"COPPER":50,"ZINC":50,
                    }.get(instrument, 100)
        offset = 0.0
        s = str(strike).upper().strip()
        if s.startswith("OTM"):
            try: n = float(s[3:]); offset = n*interval if opt_type=="CE" else -n*interval
            except: pass
        elif s.startswith("ITM"):
            try: n = float(s[3:]); offset = -n*interval if opt_type=="CE" else n*interval
            except: pass
        elif "+" in s:
            # ATM+1000 means "ATM strike + 1000 points" (absolute).
            # Do NOT multiply by interval — user typed the points directly.
            try: offset = float(s.split("+")[1])
            except: pass
        elif "-" in s and s != "ATM":
            # ATM-1000 means "ATM strike - 1000 points" (absolute).
            try: offset = -float(s.split("-")[1])
            except: pass
        target_sp = atm + offset
        pool = live if live else candidates
        if not pool: return "", "", 0.0
        # First try exact match — prefer correct strike even with LTP=0
        # over a wrong strike that happens to have a live price
        exact = [c for c in candidates if c["strike"] == target_sp]
        if exact:
            exact_live = [c for c in exact if c["ltp"] > 0]
            best = exact_live[0] if exact_live else exact[0]
        else:
            best = min(pool, key=lambda c: abs(c["strike"] - target_sp))

        # If best candidate has no WebSocket price yet, try REST LTP
        if best["ltp"] <= 0 and hasattr(self.broker, "get_rest_ltp"):
            try:
                instr_info = INSTRUMENTS.get(instrument, {})
                exch = instr_info.get("exchange", "NFO")
                ltp_val = self.broker.get_rest_ltp(exch, best["sym"], best["tok"])
                if ltp_val > 0:
                    price_store.update(best["tok"], ltp_val)
                    best = dict(best); best["ltp"] = ltp_val
                    _log.info(f"resolve_strike: LTP fetched via REST for {best['sym']}: Rs {ltp_val:.2f}")
            except Exception as e:
                _log.warning(f"resolve_strike REST LTP fallback: {e}")

        return best["tok"], best["sym"], best["ltp"]


def get_atm_strike(instrument: str, chain: list = None) -> float:
    """
    ATM Strike = round(spot_price / interval) * interval

    Interval per instrument:
      NIFTY/FINNIFTY/MIDCPNIFTY → 50 / 25 / 25
      BANKNIFTY/SENSEX/BANKEX   → 100
      MCX: NATURALGAS → 5, others → 100

    Spot price sourced from Zerodha index token subscription.
    Fallback: min straddle from live chain if spot not available.
    """
    info     = INSTRUMENTS.get(instrument, {})
    interval = {
        "NIFTY":50, "BANKNIFTY":100, "FINNIFTY":50, "MIDCPNIFTY":25,
        "SENSEX":100, "BANKEX":100,
        "GOLD":100, "SILVER":100, "CRUDEOIL":100, "NATURALGAS":5,
        "COPPER":50, "ZINC":50, "NICKEL":50, "GOLDM":100,
        "SILVERM":100, "CRUDEOILM":100, "NATGASMINI":5,
    }.get(instrument, 100)

    # ── Method 1: Spot price from WebSocket price_store ────────
    idx_tok = info.get("index_token", "")
    ws_key  = info.get("index_ws_key", "")   # e.g. "NSE:NIFTY 50", "BSE:SENSEX"
    spot    = price_store.get(idx_tok) if idx_tok else 0.0

    if spot > 0:
        atm = round(spot / interval) * interval
        _log.info(f"ATM({instrument}): spot={spot:.0f} interval={interval} → ATM={atm:.0f}")
        return float(atm)

    # ── Method 2: MCX — use Futures LTP from WebSocket ────────
    # MCX options are options-on-futures. ATM = nearest strike to FUT price.
    # FUT tokens ARE subscribed (part of option chain subscription).
    # This is how Tradetron/Quantiply handle MCX ATM by default.
    if info.get("is_mcx") and chain:
        instr_upper = instrument.upper()
        for c in chain:
            sym_t = str(c.get("tradingsymbol") or c.get("pTrdSymbol") or "").upper()
            opt   = str(c.get("instrument_type") or c.get("pOptionType") or "").upper().strip()
            is_fut = (opt == "FUT" or sym_t.endswith("FUT"))
            if not is_fut: continue
            if not sym_t.startswith(instr_upper): continue
            tok = str(c.get("instrument_token") or c.get("pSymbol") or "")
            fut_ltp = price_store.get(tok) if tok else 0.0
            if fut_ltp > 0:
                atm = round(fut_ltp / interval) * interval
                _log.info(f"ATM({instrument}): futures LTP={fut_ltp:.0f} "
                          f"interval={interval} → ATM={atm:.0f}")
                return float(atm)

    # ── Method 3 (NSE/BSE): REST LTP fallback ───────────────
    try:
        _b = broker
    except NameError:
        _b = None
    if ws_key and _b and hasattr(_b, "get_rest_ltp"):
        try:
            spot = _b.get_rest_ltp("NSE", ws_key.split(":")[-1], idx_tok or "")
            if spot > 0:
                if idx_tok: price_store.update(idx_tok, spot)
                atm = round(spot / interval) * interval
                _log.info(f"ATM({instrument}): spot={spot:.0f} (REST) interval={interval} → ATM={atm:.0f}")
                return float(atm)
        except Exception as e:
            _log.warning(f"ATM({instrument}): REST LTP failed: {e}")

    # ── Method 4: Fallback — min CE-PE diff from live chain ────
    # Used only if index spot token not yet ticking.
    # Strategy: collect all strikes that have BOTH live CE and PE prices,
    # then pick the one where CE ≈ PE (min difference) — that's true ATM.
    # This is more reliable than min-straddle which can be skewed by
    # illiquid far OTM strikes.
    if chain:
        instr_upper = instrument.upper()
        straddle: dict = {}

        for c in chain:
            sym_name = str(c.get("name") or c.get("pSymbolName") or "").upper().strip()
            tradingsym_fb = str(c.get("tradingsymbol") or c.get("pTrdSymbol") or "").upper().strip()
            zerodha_name_fb = ZERODHA_NAME_MAP.get(instrument, instr_upper)
            if sym_name:
                if sym_name != instr_upper and sym_name != zerodha_name_fb:
                    if not tradingsym_fb.startswith(instr_upper):
                        continue
            opt = str(c.get("instrument_type") or c.get("pOptionType") or "").upper().strip()
            if opt not in ("CE", "PE"):
                sym_t = str(c.get("tradingsymbol") or c.get("pTrdSymbol") or "").upper()
                opt = "CE" if sym_t.endswith("CE") else ("PE" if sym_t.endswith("PE") else "")
            if opt not in ("CE", "PE"):
                continue
            sp = 0.0
            z_strike = c.get("strike")
            if z_strike is not None:
                try: sp = float(z_strike)
                except: pass
            if sp <= 0:
                raw_sp = c.get("dStrikePrice;")
                if raw_sp is not None:
                    try: sp = float(raw_sp) / 100.0
                    except: pass
            if sp <= 0:
                continue
            tok = str(c.get("instrument_token") or c.get("pSymbol") or "")
            ltp = price_store.get(tok) if tok else 0.0
            if ltp <= 0:
                continue
            if sp not in straddle:
                straddle[sp] = {}
            if ltp > straddle[sp].get(opt, 0):
                straddle[sp][opt] = ltp

        # Strikes with both CE and PE live prices
        pairs = [(abs(d["CE"] - d["PE"]), d["CE"] + d["PE"], sp)
                 for sp, d in straddle.items()
                 if "CE" in d and "PE" in d]

        if pairs:
            # Primary: pick strike where CE ≈ PE (min diff) = true ATM
            pairs.sort(key=lambda x: (x[0], x[1]))
            _, _, atm_raw = pairs[0]
            atm = round(atm_raw / interval) * interval
            _log.info(f"ATM({instrument}): fallback chain (min CE-PE diff) → spot≈{atm_raw:.0f} ATM={atm:.0f}")
            return float(atm)

    _log.warning(f"ATM({instrument}): cannot determine ATM (no spot, no chain prices)")
    return 0.0



# ============================================================
#   LEG STATE
# ============================================================

@dataclass
class LegState:
    leg_id      : int
    opt_type    : str    # CE / PE / FUT
    action      : str    # SELL / BUY
    token       : str
    symbol      : str
    qty         : int
    entry_price : float  = 0.0   # fill price of current entry
    avg_entry   : float  = 0.0
    first_entry_price: float = 0.0  # frozen at very first fill (Re-Cost target)
    entry_time  : str    = ""    # HH:MM:SS of first entry
    exit_time   : str    = ""    # HH:MM:SS of exit
    exit_price  : float  = 0.0   # price at which leg was exited
    sl_price    : float  = 0.0
    tp_price    : float  = 0.0
    tsl_config  : dict   = field(default_factory=dict)
    tsl_high_profit: float = 0.0  # peak profit seen (for trailing SL)
    status      : str    = "PENDING"   # PENDING/OPEN/CLOSED/FAILED
    realised_pnl: float  = 0.0
    reentry_count: int   = 0
    max_reentry : int    = 0
    reentry_type: str    = "Re-Cost"  # "Re-Cost" (LTP) or "Re-Entry" (candle close)
    waiting_reentry: bool = False
    reentry_target: float = 0.0
    # Protect Profit state
    pp_lock_hit  : bool  = False
    pp_trail_hit : bool  = False
    pp_sl_price  : float = 0.0
    failed      : bool   = False
    disabled    : bool   = False
    exit_failed : bool   = False  # True when exit order failed after all retries
    # Wait & Trade fields
    wnt_active     : bool  = False   # True if this leg is waiting for W&T trigger
    wnt_ref_price  : float = 0.0    # reference price captured at strategy start time
    wnt_entry_price: float = 0.0    # calculated entry trigger price
    wnt_triggered  : bool  = False  # True once W&T condition fired
    wnt_mode       : str   = ""     # W&T mode e.g. "Points ↑", "UL Pts ↓", "UL % ↑"
    wnt_val        : float = 0.0    # W&T value entered by user
    wnt_ref_time   : str   = ""     # HH:MM:SS when reference price was captured at strategy start
    # Underlying-based SL fields
    # sl_unit "UL %" or "UL Pts" → SL is tracked against index SPOT, not option LTP.
    # ul_entry_spot: underlying spot captured at the moment the entry order fills.
    # ul_sl_price  : calculated UL spot level that triggers the exit.
    ul_entry_spot  : float = 0.0    # underlying spot at entry (for UL SL)
    ul_sl_price    : float = 0.0    # UL spot SL threshold (0 = not UL mode)

    def live_ltp(self) -> float:
        ltp = price_store.get(self.token)
        if ltp > 0:
            return ltp
        # WebSocket silent — fetch via REST for illiquid tokens (throttled)
        import time as _t
        _cache = _rest_ltp_cache
        cache_entry = _cache.get(self.token, (0.0, 0.0))
        try:
            _b2 = broker
        except NameError:
            _b2 = None
        if (_t.time() - cache_entry[1]) > 30 and _b2 and hasattr(_b2, "get_rest_ltp"):
            try:
                sym = self.symbol
                for exch in ("MCX","NFO","BFO","NSE","BSE"):
                    ltp = _b2.get_rest_ltp(exch, sym, self.token)
                    if ltp > 0:
                        price_store.update(self.token, ltp)
                        _cache[self.token] = (ltp, _t.time())
                        return ltp
            except Exception:
                pass
            _cache[self.token] = (0.0, _t.time())  # mark attempted
        return cache_entry[0] if cache_entry[0] > 0 else ltp

    def live_pnl(self) -> float:
        if self.status != "OPEN": return self.realised_pnl
        ltp = self.live_ltp()
        if ltp <= 0:
            # Feed not yet arrived for this token — use avg_entry so MTM shows
            # 0 from cost basis rather than hiding the leg entirely
            ltp = self.avg_entry
        if ltp <= 0: return self.realised_pnl  # entry price also unknown yet
        # SELL position: profit when price goes down
        if self.action == "SELL":
            return (self.avg_entry - ltp) * self.qty + self.realised_pnl
        else:
            return (ltp - self.avg_entry) * self.qty + self.realised_pnl

    def to_dict(self) -> dict:
        ltp = self.live_ltp()
        return {
            "leg_id"    : self.leg_id,
            "opt_type"  : self.opt_type,
            "action"    : self.action,
            "symbol"    : fmt_sym(self.symbol),
            "symbol_raw": self.symbol,
            "qty"       : self.qty,
            "entry"     : round(self.entry_price, 2),
            "avg_entry" : round(self.avg_entry, 2),
            "ltp"       : round(ltp, 2),
            "sl"        : round(self.sl_price, 2),
            "tp"        : round(self.tp_price, 2),
            "pnl"       : round(self.live_pnl(), 2),
            "status"    : self.status,
            "failed"     : self.failed,
            "disabled"   : self.disabled,
            "exit_failed": self.exit_failed,
            "waiting_reentry": self.waiting_reentry,
            "reentry_count"  : self.reentry_count,
            "entry_time"     : self.entry_time,
            "exit_time"      : self.exit_time,
            "exit_price"     : self.exit_price,
            "wnt_active"     : self.wnt_active,
            "wnt_ref"        : round(self.wnt_ref_price, 2),
            "wnt_entry_at"   : round(self.wnt_entry_price, 2),
            "wnt_mode"       : self.wnt_mode,
            "wnt_val"        : self.wnt_val,
            "wnt_ref_time"   : self.wnt_ref_time,
            "ul_entry_spot"  : round(self.ul_entry_spot, 2),
            "ul_sl_price"    : round(self.ul_sl_price, 2),
        }


# ============================================================
#   ORDER MANAGER
# ============================================================

class OrderManager:
    """
    Best-price limit order execution engine.

    Strategy
    ────────
    Each attempt fetches fresh LTP and places a limit order at the most
    favourable price that still satisfies the order condition:

      SELL entry  → limit price starts just below LTP, steps DOWN each retry
                    (we want to SELL, so lower is better for us;
                     but the price must stay below the condition cap)

      BUY  entry  → limit price starts just above LTP, steps UP each retry
                    (we want to BUY, so higher gives better fill probability)

      SELL exit   → (closing a BUY) limit price steps DOWN — sell any price
      BUY  exit   → (closing a SELL — buying back) limit price steps UP

    The step size is `buf_pct` (from strategy Advanced Settings or global
    config default).  Each unfilled attempt moves the price by one step in
    the fill-friendly direction, capped at the worst-allowable price so the
    condition is never violated.

    Condition cap
    ─────────────
    `condition_price` is the boundary the user defined (e.g. "Premium ≤ 250").
    For a SELL entry the limit price never exceeds condition_price.
    Pass None / 0 to skip the cap (used for exits and ATM-offset entries).

    Failure handling
    ────────────────
    After all retries are exhausted:
      • Logs an error internally
      • Sends a Telegram alert with symbol, side, attempts and last price tried
      • Returns 0.0 so the caller can mark the leg FAILED and surface
        the Retry button in the dashboard
    """

    def __init__(self, broker: BrokerBase, dry_run: bool, cfg: dict,
                 notifier=None):
        self.broker   = broker
        self.dry_run  = dry_run
        self.cfg      = cfg
        self.notifier = notifier          # Notifier instance for alerts
        self.log      = logging.getLogger("OrderMgr")

    # ── Root cause classifier ────────────────────────────────────────
    @staticmethod
    def _classify_error(reason: str) -> tuple:
        """
        Classify an order rejection/failure reason into a human-readable
        category and a retry recommendation.

        Returns:
          (category: str, emoji: str, should_retry: bool)

        Categories aligned with Zerodha's actual rejection messages:
          MARGIN    — insufficient funds / margin block
          LIQUIDITY — no buyers/sellers, circuit limit, wide spread
          EXCHANGE  — exchange-side rejection, market halted
          BROKER    — Zerodha API or RMS system error
          DATA      — LTP unavailable, feed issue
          UNKNOWN   — anything else
        """
        r = reason.lower()

        # Tick size — DO NOT retry (price won't change to fix tick)
        if any(k in r for k in ("tick", "tick size", "multiple of",
                                 "not a multiple", "price multiple")):
            return ("TICK_SIZE",
                    "❌ Price not multiple of tick size — fixed in next order",
                    False)   # ← no retry, need to fix rounding

        # Margin / funds — DO NOT retry (funds won't appear between retries)
        if any(k in r for k in ("insufficient", "margin", "funds",
                                 "balance", "collateral", "span",
                                 "exposure", "blocked", "credit")):
            return ("MARGIN",
                    "💰 Insufficient margin/funds",
                    False)   # ← no retry, money won't appear

        # Liquidity / spread — RETRY aggressively with wider price
        if any(k in r for k in ("no buyer", "no seller", "liquidity",
                                 "bid", "ask", "spread", "circuit",
                                 "limit", "freeze", "not traded",
                                 "illiquid", "depth")):
            return ("LIQUIDITY",
                    "📊 Bid/Ask not available (low liquidity)",
                    True)   # ← retry with better price

        # Exchange-side issues — retry a few times
        if any(k in r for k in ("exchange", "nse", "bse", "mcx",
                                 "exchange error", "oc limit",
                                 "market closed", "market not open",
                                 "segment", "order restriction")):
            return ("EXCHANGE",
                    "🏦 Exchange-side rejection",
                    True)

        # Broker/RMS/API — retry a few times
        if any(k in r for k in ("rms", "risk", "rejected", "error",
                                 "api", "timeout", "connection",
                                 "network", "server", "gateway",
                                 "throttle", "rate limit")):
            return ("BROKER",
                    "🔌 Broker API / RMS error",
                    True)

        # Feed/data issue — retry
        if any(k in r for k in ("ltp", "price", "data", "feed",
                                 "zero", "0.0", "stale", "not found")):
            return ("DATA",
                    "📡 Data/feed issue",
                    True)

        # Unknown
        return ("UNKNOWN",
                "❓ Unknown rejection",
                True)

    def execute(self, token: str, symbol: str, qty: int, side: str,
                ref_ltp: float, exchange: str, product: str, tag: str,
                condition_price: float = 0.0,
                buf_pct: float = 0.0) -> float:
        """
        Place a best-price limit order with structured retry and root-cause
        classification.

        Execution priority:
          1. Try with live LTP + buffer (always read fresh LTP each attempt)
          2. On rejection/failure:
               MARGIN  → stop immediately (no point retrying; alert user)
               OTHERS  → retry up to max_retries with progressively wider buffer
          3. Only after all retries fail → mark failed, send classified alert

        Parameters
        ──────────
        token            : instrument token for live LTP lookup
        symbol           : trading symbol sent to broker
        qty              : order quantity (lots × lot_size)
        side             : "S" = sell  |  "B" = buy
        ref_ltp          : reference price at time of trigger
        exchange         : exchange segment string (nse_fo / bse_fo / mcx_fo)
        product          : MIS / NRML
        tag              : short label for order book identification
        condition_price  : the user-defined cap (e.g. 250 for "Premium ≤ 250")
                           Pass 0 to skip cap enforcement.
        buf_pct          : buffer % per retry. Defaults to Advanced Settings.
        """
        if self.dry_run:
            ltp = price_store.get(token) or ref_ltp
            action = "SELL" if side == "S" else "BUY"
            self.log.info(
                f"[DRY-RUN {action}] {fmt_sym(symbol)} @ Rs {ltp:.2f}  qty={qty}")
            return ltp if ltp > 0 else ref_ltp

        retries = int(self.cfg.get("order_retry_max", 10))
        if buf_pct <= 0:
            buf_pct = float(self.cfg.get("order_buffer_pct", 0.5))

        last_price  = 0.0
        fail_reason = ""
        fail_cat    = "UNKNOWN"
        fail_emoji  = "❓"

        for attempt in range(1, retries + 1):
            # Always use CURRENT LIVE LTP — re-read every attempt
            ltp = price_store.get(token) or ref_ltp
            if ltp <= 0:
                fail_reason = "LTP=0 — feed not available for this instrument"
                fail_cat, fail_emoji, _ = self._classify_error(fail_reason)
                self.log.warning(f"LTP=0 on attempt {attempt}, waiting 1s...")
                time.sleep(1.0)
                continue

            # ── Price logic — aligned with SEBI limit order standard ──────
            # Industry standard (AlgoTest / QuantMan / Quantiply):
            #
            # SELL entry:  place at LTP + buffer%
            #   → Limit price ABOVE current LTP means your order sits above
            #     the best bid. Exchange matching engine fills you immediately
            #     against resting bids. This is the standard for options SELL.
            #   → condition_price cap still enforced (user's "Premium ≤ X").
            #
            # BUY exit:    place at LTP + buffer%
            #   → Limit price ABOVE current ask ensures immediate fill when
            #     buying back a sold option.
            #
            # Buffer strategy:
            #   Attempt 1: full buffer (e.g. 10%) → most fills happen here.
            #   If rejected for liquidity: retry at same full buffer with
            #   fresh LTP (market may have moved, improving fill chance).
            #   This matches what all major platforms do.

            buf_abs = ltp * (buf_pct / 100.0)

            # ── Universal price logic (Quantiply/AlgoTest standard) ──
            # BUY  (entry or exit): LTP + buffer
            #   → Willing to pay MORE → fills against resting asks ✓
            #   → Works for: BUY entry, BUY exit (close SELL position)
            #
            # SELL (entry or exit): LTP - buffer
            #   → Willing to accept LESS → fills against resting bids ✓
            #   → Works for: SELL entry, SELL exit (close BUY position)
            #   → No fresh margin issue on exit ✓
            #
            # Applied universally across:
            #   NFO (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY)
            #   BFO (SENSEX, BANKEX)
            #   MCX (GOLD, SILVER, CRUDEOIL, NATURALGAS etc.)
            #   All segments: Options + Futures
            #   All order types: Entry, Exit, SL, Target, Square-off

            if side == "S":
                # SELL — always LTP - buffer (accept less for faster fill)
                price = max(ltp - buf_abs, 0.05)
                # Premium cap for Premium<= strike type
                if condition_price > 0:
                    price = min(price, condition_price)
            else:
                # BUY — always LTP + buffer (pay more for faster fill)
                price = ltp + buf_abs

            # Round to instrument tick size (NFO=0.05, MCX=1.0 etc.)
            # Zerodha rejects orders where price is not a tick size multiple
            _tick = 0.05  # default NFO/BSE tick
            try:
                # Look up by instrument name (e.g. NIFTY, GOLD, CRUDEOIL)
                # NOT by exchange (MCX/NFO) — exchange is segment not name
                _instr_name = symbol.split("2")[0] if symbol else ""
                _iinfo = INSTRUMENTS.get(_instr_name, {})
                if not _iinfo:
                    # Fallback by exchange segment:
                    # MCX: commodities tick = 1.0 (Gold, Silver, Crude etc.)
                    # NFO: equity F&O tick  = 0.05 (Nifty, BankNifty etc.)
                    # BFO: BSE F&O tick     = 0.05 (Sensex, Bankex)
                    # NSE/BSE: equity tick  = 0.05
                    if exchange in ("MCX",):
                        _tick = 1.0
                    elif exchange in ("NFO", "BFO", "NSE", "BSE"):
                        _tick = 0.05
                    else:
                        _tick = 0.05
                else:
                    _tick = float(_iinfo.get("tick", 0.05) or 0.05)
            except Exception:
                pass
            if _tick > 0:
                import math
                price = math.ceil(price / _tick) * _tick
                price = round(price, 10)  # remove float precision errors
            price = max(round(price, 2), _tick)
            last_price = price

            self.log.info(
                f"[ORDER {attempt}/{retries}] {side} {fmt_sym(symbol)}"
                f"  live_ltp=Rs {ltp:.2f}  limit=Rs {price:.2f}"
                + (f"  cap=Rs {condition_price:.2f}" if condition_price > 0 else ""))

            # ── Attempt 1: place order ────────────────────────────
            oid = self.broker.place_order(
                exchange=exchange, symbol=symbol, qty=qty,
                side=side, price=price,
                order_type="L", product=product, tag=tag)

            if not oid:
                # Broker returned empty order ID — usually API/network issue
                api_err = (getattr(self.broker, "get_last_order_error", lambda: "")()
                           or "Broker returned no order ID")
                fail_cat, fail_emoji, should_retry = self._classify_error(api_err)
                fail_reason = api_err
                self.log.warning(
                    f"[{fail_cat}] Attempt {attempt}: no OID — {api_err}")
                if not should_retry:
                    self.log.warning(f"Non-retryable error ({fail_cat}). Stopping.")
                    break
                time.sleep(0.5)
                continue

            # ── Poll for fill / rejection ─────────────────────────
            result = self._poll_until_filled(oid)
            if result["status"] == "COMPLETE":
                fill = result["fill_price"]
                self.log.info(
                    f"[FILLED] {fmt_sym(symbol)} @ Rs {fill:.2f}  attempt={attempt}")
                return fill

            if result["status"] == "REJECTED":
                rej_reason = result.get("reason", "")
                fail_cat, fail_emoji, should_retry = self._classify_error(rej_reason)
                fail_reason = rej_reason or f"Order rejected (attempt {attempt})"
                self.log.warning(
                    f"[REJECTED/{fail_cat}] Attempt {attempt}: {fail_reason}")
                if not should_retry:
                    # Hard stop — no point retrying (e.g. margin issue)
                    self.log.warning(
                        f"Non-retryable rejection ({fail_cat}). Stopping retries.")
                    break
                time.sleep(0.3)
                continue

            # Timed out (PENDING after poll window)
            # Use MODIFY instead of cancel+new (Quantiply/AlgoTest standard):
            # - Keeps order queue priority at exchange ✓
            # - No duplicate orders ✓
            # - More likely to fill ✓
            ltp_now = price_store.get(token) or ref_ltp
            buf_now = buf_pct + (attempt * 1.0)  # widen buffer each retry
            buf_abs_now = ltp_now * (buf_now / 100.0)
            new_price = ltp_now + buf_abs_now
            # Round to tick size
            _tick2 = 0.05
            try:
                import math as _math
                new_price = _math.ceil(new_price / _tick2) * _tick2
            except Exception:
                pass
            new_price = max(round(new_price, 2), 0.05)
            modified = False
            if hasattr(self.broker, 'modify_order'):
                modified = self.broker.modify_order(oid, new_price)
            if not modified:
                # Fallback to cancel if modify not available
                self.broker.cancel_order(oid)
            fail_reason = f"Order pending — modified price to Rs {new_price:.2f} (attempt {attempt})"
            fail_cat    = "LIQUIDITY"
            fail_emoji  = "📊"
            time.sleep(0.5)

        # ── All retries exhausted or hard-stop ────────────────────
        action_str = "SELL" if side == "S" else "BUY"
        err_msg = (
            f"⚠️ [ORDER FAILED] | {tag}\n\n"
            f"Symbol  : {fmt_sym(symbol)}\n"
            f"Side    : {action_str}\n"
            f"Qty     : {qty}\n"
            f"Price   : Rs {last_price:.2f} (last attempt)\n"
            f"Retries : {retries}\n\n"
            f"Root Cause : {fail_emoji}\n"
            f"Detail     : {fail_reason or 'No specific reason from broker'}\n\n"
            f"Action: Open dashboard → View (👁) → Retry or Skip."
        )
        self.log.error(err_msg)
        if self.notifier:
            self.notifier.telegram(err_msg)
            self.notifier.email("Order Failed", err_msg)
        return 0.0

    # ── poll broker until filled, rejected, or timeout ────────────
    def _poll_until_filled(self, oid: str, timeout: int = 60) -> dict:
        """
        Poll order status. Returns dict:
          {'status': 'COMPLETE'|'REJECTED'|'PENDING', 'fill_price': float, 'reason': str}
        Wait up to 60 seconds (industry standard — Quantiply/AlgoTest use 30-60s).
        Options need time to fill — 10s is too short for illiquid strikes.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(1)
            result = self.broker.get_order_status(oid)
            status = result.get("status", "PENDING")
            if status == "COMPLETE":
                return {"status": "COMPLETE",
                        "fill_price": float(result.get("fill_price", 0)),
                        "reason": ""}
            if status == "REJECTED":
                return {"status": "REJECTED",
                        "fill_price": 0.0,
                        "reason": result.get("reason", "")}
        return {"status": "PENDING", "fill_price": 0.0, "reason": ""}


# ============================================================
#   STRATEGY RUNNER
# ============================================================

class StrategyRunner:
    """
    Universal strategy runner -- executes any strategy JSON.
    One instance per strategy, runs in its own thread.
    """

    def __init__(self, strategy: dict, broker: BrokerBase,
                 order_mgr: OrderManager, notifier, dry_run: bool,
                 config: dict = None, siblings: dict = None):
        self.s        = strategy          # full strategy dict from config
        self.sid      = strategy["id"]
        self.name     = strategy["name"]
        self.broker   = broker
        self.om       = order_mgr
        self.notify   = notifier          # Notifier instance
        self.dry_run  = dry_run
        self.config   = config or {}      # engine-level config (basket SL etc.)
        self._siblings = siblings         # dict of all runners {sid: runner}
        self.log      = logging.getLogger(f"Strat.{self.name[:12]}")
        self.leg_states: list[LegState] = []
        self.stopped   = False
        self.status    = "READY"
        self._option_chain: list = []
        self._lock     = threading.Lock()
        self._retry_q: list = []   # [(leg_id, action)]
        self._restart_requested = False   # set True when user clicks Restart
        self._config_changed    = False   # set True by dashboard when strategy edited
                                          # mid-run; main loop resets stale state next tick
        # Move SL to Cost sticky-side: once a side (CE or PE) has moved to cost,
        # it stays the locked side for the rest of this trade group. New
        # SL hits on the same locked side do NOT move the other side.
        # Reset on MTM-level re-execute (fresh group).
        self._sl_cost_locked_side = None  # None | "CE" | "PE"

    def compute_total_pnl(self) -> float:
        # If strategy is CLOSED/EXITED and we have a saved final MTM, return it
        if self.status in ("CLOSED", "EXITED") and hasattr(self, "_closed_mtm"):
            return self._closed_mtm
        # Match exactly what get_live_legs() shows — no duplicate re-entry legs
        history = getattr(self, "_closed_legs_history", [])
        history_leg_ids = set(ls.leg_id for ls in history)
        hist_pnl = sum(ls.realised_pnl for ls in history)
        live_pnl = sum(
            ls.live_pnl() for ls in self.leg_states
            if ls.status in ("OPEN","PENDING","WAIT")
            or (ls.status in ("CLOSED","EXITED") and ls.leg_id not in history_leg_ids)
        )
        total = hist_pnl + live_pnl
        # Auto-snapshot: whenever strategy is CLOSED/EXITED, freeze the MTM
        if self.status in ("CLOSED", "EXITED"):
            self._closed_mtm = total
        return total

    def get_live_legs(self) -> list:
        # Show history rows first (each = one complete entry-exit cycle)
        # From leg_states show only OPEN/PENDING/WAIT legs (not closed — already in history)
        history = getattr(self, "_closed_legs_history", [])
        hist_dicts = [ls.to_dict() for ls in history]
        # Collect leg_ids already shown via history
        history_leg_ids = set(ls.leg_id for ls in history)
        # Show active legs — if leg is closed and already in history, skip to avoid duplicate
        live = []
        for ls in self.leg_states:
            if ls.status in ("OPEN", "PENDING", "WAIT", "FAILED"):
                live.append(ls.to_dict())
            elif ls.status in ("CLOSED", "EXITED") and ls.leg_id not in history_leg_ids:
                # Closed leg not yet in history (edge case) — show it
                live.append(ls.to_dict())
        return hist_dicts + live

    def _get_sibling_runners(self) -> list:
        """Return all StrategyRunner instances including self (for portfolio MTM)."""
        if self._siblings:
            return list(self._siblings.values())
        return [self]

    def request_retry(self, leg_id: int):
        with self._lock:
            self._retry_q.append(("retry", leg_id))

    def request_action(self, leg_id: int, action: str):
        """Generic action queue — retry_exit and other leg actions."""
        with self._lock:
            self._retry_q.append((action, leg_id))

    def request_disable_leg(self, leg_id: int):
        with self._lock:
            self._retry_q.append(("disable", leg_id))

    def request_restart(self):
        """Called when user clicks Restart after EXITED/CLOSED status."""
        # Clear ALL positional state files for this strategy matched by SID or name
        import os, glob
        state_dir = os.path.dirname(os.path.abspath(__file__))
        for f in glob.glob(os.path.join(state_dir, "positional_*.json")):
            try:
                import json as _j
                d = _j.load(open(f))
                if str(d.get("strategy_id")) == str(self.sid) or d.get("strategy_name") == self.name:
                    os.remove(f)
                    self.log.info(f"[Restart] Cleared old positional state: {f}")
            except: pass
        # Clear all old state so restart is fully fresh
        if hasattr(self, "_closed_mtm"):
            del self._closed_mtm
        self._closed_legs_history = []
        self._holding_alerted = False
        # Clear cached option chain and expiry map so new leg settings take effect
        self._option_chain = []
        if hasattr(self, '_instr_expiry_map'):
            del self._instr_expiry_map
        # Clear engine-level chain cache for this instrument so it rebuilds fresh
        instr = self.s.get("idx", "NIFTY")
        if hasattr(self, '_option_chains') and instr in self._option_chains:
            del self._option_chains[instr]
        # Fetch fresh chain based on current leg expiry settings
        try:
            from engine import nearest_expiry_from_broker, nearest_expiry, expiry_fmt, INSTRUMENTS
            _info = INSTRUMENTS.get(instr, {})
            _leg_expiries = set()
            for _lg in self.s.get("legs", []):
                _et = str(_lg.get("expiry","Weekly")).lower().replace(" ","_")
                _emap = {"weekly":"weekly","next_weekly":"next_weekly",
                         "monthly":"monthly","next_month":"next_month"}
                _et_m = _emap.get(_et, "weekly")
                if _info.get("is_mcx") and _et_m in ("weekly","next_weekly"):
                    _et_m = "monthly"
                _leg_expiries.add(_et_m)
            _combined_chain = []
            for _et_m in _leg_expiries:
                _exp = nearest_expiry_from_broker(self.broker, instr, _et_m)
                if not _exp:
                    _exp = expiry_fmt(nearest_expiry(instr, _et_m))
                if _exp:
                    _c = self.broker.get_option_chain(instr, _exp)
                    _has_fut = any(
                        str(lg.get("type","")).upper() == "FUT"
                        for lg in self.s.get("legs", [])
                    )
                    if _has_fut:
                        _fc = self.broker.get_fut_chain(instr, _exp)
                        _c = _c + _fc
                    _combined_chain.extend(_c)
                    if not hasattr(self, '_instr_expiry_map'): self._instr_expiry_map = {}
                    if instr not in self._instr_expiry_map: self._instr_expiry_map[instr] = {}
                    self._instr_expiry_map[instr][_et_m] = _exp
            if _combined_chain:
                self._option_chain = _combined_chain
                self.log.info(f"[Restart] Fresh chain fetched: {len(_combined_chain)} contracts for {instr} expiries={_leg_expiries}")
        except Exception as _e:
            self.log.warning(f"[Restart] Could not pre-fetch chain: {_e}")
        self._restart_requested = True
        self.stopped = False
        self.status  = "READY"
        self._mtm_peak = 0.0  # reset peak for fresh day


    def _run_indicator(self, option_chain: list):
        """
        Indicator based strategy runner.
        Waits for indicator signal on candle close then executes legs.
        Same leg execution, SL, MTM logic as time based strategy.
        """
        import sys, os
        sys.path.insert(0, '/home/ubuntu/angelone-algo')
        from datetime import datetime as _dt
        import pytz as _pytz
        import time as _time

        logic      = self.s.get("logic", {})
        instr      = self.s.get("idx", "NIFTY")
        start_t    = logic.get("startTime", "09:20:00")
        end_t      = logic.get("endTime", "15:15:00")
        days       = self.s.get("days", [True]*5+[False,False])
        ind_cfg    = self.s.get("indicator_config", {})
        ind_type   = ind_cfg.get("type", "EMA")
        timeframe  = ind_cfg.get("timeframe", "5 Min")
        IST        = _pytz.timezone("Asia/Kolkata")

        # Get exchange for candle fetch
        info       = INSTRUMENTS.get(instr, {})
        is_mcx     = info.get("is_mcx", False)
        exchange   = "MCX" if is_mcx else ("BSE" if info.get("exchange","NSE") in ("BSE","BFO") else "NSE")

        self.log.info(f"{self.name}: Indicator strategy started — {ind_type} {timeframe}")
        self._update_status("READY")

        last_signal_candle = None  # track last candle we acted on — avoid duplicate signals

        while True:
            now     = _dt.now(IST)
            weekday = now.weekday()

            # Check if today is active day
            if weekday >= len(days) or not days[weekday]:
                _time.sleep(60)
                continue

            now_s = now.strftime("%H:%M:%S")

            # Before start time — wait
            if now_s < start_t:
                _time.sleep(10)
                continue

            # After end time — exit and stop
            if now_s >= end_t:
                self.log.info(f"{self.name}: End time reached — stopping indicator strategy.")
                self._update_status("EXITED")
                break

            # Check if status is still running
            if self.s.get("status") in ("EXITED","CLOSED","DISABLED"):
                break

            try:
                # Get all indicator configs — support multiple indicators
                ind_panels = ind_cfg if isinstance(ind_cfg, list) else [ind_cfg]

                def _get_signal_for_panel(p):
                    """Get signal for one indicator panel."""
                    ptype = p.get("type","EMA")
                    ptf   = p.get("timeframe","5 Min")
                    psig  = p.get("signal","ABOVE")
                    raw_signal = None
                    if ptype == "EMA":
                        from indicators.ema import get_signal
                        raw_signal, _, _ = get_signal(self.broker, exchange, instr, ptf, length=int(p.get("length",9)))
                    elif ptype == "SuperTrend":
                        from indicators.supertrend import get_signal
                        raw_signal, _ = get_signal(self.broker, exchange, instr, ptf, length=int(p.get("length",10)), factor=float(p.get("factor",3)))
                    elif ptype == "RSI":
                        from indicators.rsi import get_signal
                        raw_signal, _ = get_signal(self.broker, exchange, instr, ptf, length=int(p.get("length",14)), upper=float(p.get("upper",70)), lower=float(p.get("lower",30)), use_middle=bool(p.get("use_middle",False)), middle=float(p.get("middle",50)))
                    elif ptype == "CPR":
                        from indicators.cpr import get_signal
                        raw_signal, _, _, _ = get_signal(self.broker, exchange, instr, ptf)
                    elif ptype == "VWAP":
                        from indicators.vwap import get_signal
                        raw_signal, _ = get_signal(self.broker, exchange, instr, ptf)
                    # Map raw signal to ABOVE/BELOW
                    if raw_signal == "BUY": raw_signal = "ABOVE"
                    elif raw_signal == "SELL": raw_signal = "BELOW"
                    # Check if matches required signal
                    return raw_signal == psig

                # Check all panels — ALL must confirm
                all_confirmed = all(_get_signal_for_panel(p) for p in ind_panels)
                signal = "ABOVE" if all_confirmed else None

                # Get candle timestamp from first panel
                from indicators.base import INTERVAL_MINUTES
                first_tf  = ind_panels[0].get("timeframe","5 Min")
                mins      = INTERVAL_MINUTES.get(first_tf, 5)
                candle_ts = (now.minute // mins) * mins

                if signal and candle_ts != last_signal_candle:
                    last_signal_candle = candle_ts
                    self.log.info(f"{self.name}: Signal={signal} Value={val} — executing legs")
                    self.status = "RUNNING"
                    # Execute legs same as time based strategy
                    from concurrent.futures import ThreadPoolExecutor, as_completed
                    results = []
                    legs = self.s.get("legs", [])
                    if legs:
                        with ThreadPoolExecutor(max_workers=min(len(legs),4)) as ex:
                            futs = {ex.submit(self._enter_leg, leg, option_chain): leg
                                    for leg in legs}
                            for fut in as_completed(futs):
                                ls = fut.result()
                                if ls:
                                    results.append(ls)
                        self.leg_states = results
                        self.log.info(f"{self.name}: {len(results)} legs entered")

            except Exception as e:
                self.log.error(f"{self.name}: Indicator runner error: {e}")

            # Wait for next candle close check — every 30 seconds
            _time.sleep(30)

    def _positional_state_path(self) -> str:
        import os
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"positional_{self.sid}.json")

    def _save_positional_state(self):
        """Persist open leg states to disk for overnight carry."""
        import json, os
        path = self._positional_state_path()
        try:
            data = {
                "strategy_id"   : self.sid,
                "strategy_name" : self.name,
                "saved_at"      : datetime.now().isoformat(),
                "legs": [
                    {
                        "leg_id"          : ls.leg_id,
                        "opt_type"        : ls.opt_type,
                        "action"          : ls.action,
                        "token"           : ls.token,
                        "symbol"          : ls.symbol,
                        "qty"             : ls.qty,
                        "entry_price"     : ls.entry_price,
                        "avg_entry"       : ls.avg_entry,
                        "first_entry_price": ls.first_entry_price,
                        "sl_price"        : ls.sl_price,
                        "tp_price"        : ls.tp_price,
                        "tsl_config"      : ls.tsl_config,
                        "status"          : ls.status,
                        "realised_pnl"    : ls.realised_pnl,
                        "reentry_count"   : ls.reentry_count,
                        "max_reentry"     : ls.max_reentry,
                        "reentry_type"    : ls.reentry_type,
                        "reentry_target"  : ls.reentry_target,
                        "entry_time"      : ls.entry_time,
                    }
                    for ls in self.leg_states if ls.status == "OPEN"
                ]
            }
            data["closed_legs_history"] = [
                {
                    "leg_id"       : ls.leg_id,
                    "opt_type"     : ls.opt_type,
                    "action"       : ls.action,
                    "symbol"       : ls.symbol,
                    "qty"          : ls.qty,
                    "entry_price"  : ls.entry_price,
                    "avg_entry"    : ls.avg_entry,
                    "sl_price"     : ls.sl_price,
                    "tp_price"     : ls.tp_price,
                    "status"       : ls.status,
                    "realised_pnl" : ls.realised_pnl,
                    "entry_time"   : ls.entry_time,
                    "exit_time"    : ls.exit_time,
                }
                for ls in getattr(self, "_closed_legs_history", [])
            ]
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            self.log.info(f"[Positional] State saved to {path} "
                          f"({len(data['legs'])} open legs)")
        except Exception as e:
            self.log.error(f"[Positional] Failed to save state: {e}")

    def load_positional_state(self) -> bool:
        """
        Load persisted positional state from disk.
        Called at startup for positional strategies — restores open legs
        so monitoring can resume without re-entering.
        Returns True if state was loaded, False if no saved state found.
        """
        import json, os
        path = self._positional_state_path()
        if not os.path.exists(path):
            return False
        try:
            with open(path) as f:
                data = json.load(f)
            if data.get("strategy_id") != self.sid:
                return False
            loaded_legs = []
            for d in data.get("legs", []):
                ls = LegState(
                    leg_id=d["leg_id"],
                    opt_type=d["opt_type"],
                    action=d["action"],
                    token=d["token"],
                    symbol=d["symbol"],
                    qty=d["qty"],
                    entry_price=d["entry_price"],
                    avg_entry=d["avg_entry"],
                    first_entry_price=d.get("first_entry_price", d["entry_price"]),
                    sl_price=d["sl_price"],
                    tp_price=d["tp_price"],
                    tsl_config=d.get("tsl_config") or {},
                    status=d["status"],
                    realised_pnl=d.get("realised_pnl", 0.0),
                    reentry_count=d.get("reentry_count", 0),
                    max_reentry=d.get("max_reentry", 0),
                    reentry_type=d.get("reentry_type", "Re-Cost"),
                    reentry_target=d.get("reentry_target", 0.0),
                    entry_time=d.get("entry_time", ""),
                )
                loaded_legs.append(ls)
            if loaded_legs:
                self.leg_states = loaded_legs
                # Restore closed legs history
                import copy
                hist_data = data.get("closed_legs_history", [])
                self._closed_legs_history = []
                for d in hist_data:
                    from dataclasses import fields
                    ls = LegState(leg_id=d.get("leg_id",0), opt_type=d.get("opt_type",""), action=d.get("action","SELL"), token=d.get("token",""), symbol=d.get("symbol",""), qty=d.get("qty",0), entry_price=d.get("entry_price",0), avg_entry=d.get("avg_entry",0), sl_price=d.get("sl_price",0), tp_price=d.get("tp_price",0))
                    ls.status = d.get("status","CLOSED")
                    ls.realised_pnl = d.get("realised_pnl",0)
                    ls.entry_time = d.get("entry_time","")
                    ls.exit_time = d.get("exit_time","")
                    self._closed_legs_history.append(ls)
                self.status = "RUNNING"
                saved_at = data.get("saved_at", "")
                self.log.info(
                    f"[Positional] Restored {len(loaded_legs)} legs from {path} "
                    f"(saved {saved_at})")
                self.notify.telegram(
                    f"[POSITIONAL RESTORE] | {self.name}\n\n"
                    f"Restored {len(loaded_legs)} open position(s) from previous session.\n"
                    f"Saved at: {saved_at}\n"
                    f"Monitoring now active.")
                return True
        except Exception as e:
            self.log.error(f"[Positional] Failed to load state: {e}")
        return False

    def clear_positional_state(self):
        """Delete saved state file when all positions are closed."""
        import os
        path = self._positional_state_path()
        try:
            if os.path.exists(path):
                os.remove(path)
                self.log.info(f"[Positional] State cleared: {path}")
        except Exception as e:
            self.log.warning(f"[Positional] Clear state: {e}")

    def force_exit(self):
        self._exit_all("Dashboard manual exit")
        self.stopped = True
        self.status  = "EXITED"
        self.notify.telegram(f"[MANUAL EXIT] | {self.name}\nForced exit from dashboard.")

    def _instr_info(self):
        return INSTRUMENTS.get(self.s.get("idx","NIFTY"), INSTRUMENTS["NIFTY"])

    def _qty(self, leg: dict) -> int:
        """Quantity for P&L calculation — uses actual lot size."""
        base_lots = max(1, int(leg.get("lots", 1) or 1))
        mult_str  = str(self.s.get("mult","1X")).upper().replace("X","")
        mult      = max(1, int(mult_str) if mult_str.isdigit() else 1)
        lot_size  = self._instr_info()["lot"]
        return base_lots * mult * lot_size

    def _order_qty_from_ls(self, ls) -> int:
        """Order qty for exit — MCX=1 per lot, NSE/BSE=lot_size per lot."""
        info = self._instr_info()
        if info.get("is_mcx"):
            # MCX: ls.qty = lots * lot_size (for P&L), but exchange needs lots only
            mcx_lot = info.get("lot", 1) or 1
            return max(1, ls.qty // mcx_lot)
        return ls.qty

    def _order_qty(self, leg: dict) -> int:
        """Quantity to send to Zerodha API.
        MCX: qty=1 per lot (Zerodha instruments CSV lot_size=1 for MCX).
        NSE/BSE: same as _qty() — lot_size from Zerodha CSV.
        """
        base_lots = max(1, int(leg.get("lots", 1) or 1))
        mult_str  = str(self.s.get("mult","1X")).upper().replace("X","")
        mult      = max(1, int(mult_str) if mult_str.isdigit() else 1)
        info      = self._instr_info()
        lot_size  = 1 if info.get("is_mcx") else info["lot"]
        return base_lots * mult * lot_size

    def _compute_sl_price(self, leg: dict, entry: float) -> float:
        """
        Compute the option-LTP SL price for normal % / Pts modes.
        For UL % / UL Pts modes, this returns 0.0 (no option-LTP SL is set).
        UL-based SL is stored in LegState.ul_sl_price and checked in
        _should_exit_sl() by reading the live underlying spot price.
        """
        sl_val  = float(leg.get("sl", 0) or 0)
        if sl_val <= 0:
            return 0.0
        sl_unit = str(leg.get("slU", "%"))
        if sl_unit in ("UL %", "UL Pts"):
            # UL-based SL — computed separately in _compute_ul_sl_price().
            # Option LTP SL does not apply.
            return 0.0
        if sl_unit == "%":
            if leg.get("action","SELL") == "SELL":
                return round(entry * (1 + sl_val/100), 2)
            else:
                return round(entry * (1 - sl_val/100), 2)
        else:  # Points
            if leg.get("action","SELL") == "SELL":
                return round(entry + sl_val, 2)
            else:
                return round(entry - sl_val, 2)

    def _compute_ul_sl_price(self, leg: dict, ul_spot: float) -> float:
        """
        Compute the underlying spot price level at which the UL-based SL exits.

        UL % :  SELL CE → exit when spot rises by X% from entry spot
                           ul_sl = ul_spot × (1 + sl_val/100)
                SELL PE → exit when spot falls by X% from entry spot
                           ul_sl = ul_spot × (1 - sl_val/100)
                BUY CE  → exit when spot falls by X% (against the position)
                BUY PE  → exit when spot rises by X%
        UL Pts: same logic but in absolute points instead of %.

        Returns 0.0 if this leg doesn't use UL-based SL.
        """
        sl_val  = float(leg.get("sl", 0) or 0)
        if sl_val <= 0 or ul_spot <= 0:
            return 0.0
        sl_unit = str(leg.get("slU", "%"))
        if sl_unit not in ("UL %", "UL Pts"):
            return 0.0
        action = leg.get("action","SELL")
        opt_type = leg.get("type","CE").upper()
        # Direction: SL fires when underlying moves AGAINST the position.
        # SELL CE: bearish on vol / neutral-bullish → loss if spot rises sharply → SL on rise
        # SELL PE: bearish on vol / neutral-bullish → loss if spot falls sharply → SL on fall
        # BUY CE: bullish → loss if spot falls → SL on fall
        # BUY PE: bearish → loss if spot rises → SL on rise
        if action == "SELL":
            adverse_up = (opt_type == "CE")   # CE sold → adverse if spot rises
        else:
            adverse_up = (opt_type == "CE")   # CE bought → adverse if spot falls → SL on fall → adverse_up=False
            adverse_up = not adverse_up        # flip for BUY

        if sl_unit == "UL %":
            if adverse_up:
                return round(ul_spot * (1 + sl_val / 100), 2)
            else:
                return round(ul_spot * (1 - sl_val / 100), 2)
        else:  # UL Pts
            if adverse_up:
                return round(ul_spot + sl_val, 2)
            else:
                return round(ul_spot - sl_val, 2)

    def _compute_tp_price(self, leg: dict, entry: float) -> float:
        tp_val = float(leg.get("tp", 0))
        # If user left TP at 0 → no target, return 0 so _should_exit_tp never fires
        if tp_val <= 0:
            return 0.0
        tp_unit = str(leg.get("tpU", "%"))
        if tp_unit == "%":
            if leg.get("action","SELL") == "SELL":
                return round(entry * (1 - tp_val/100), 2)
            else:
                return round(entry * (1 + tp_val/100), 2)
        else:
            if leg.get("action","SELL") == "SELL":
                return round(entry - tp_val, 2)
            else:
                return round(entry + tp_val, 2)

    def _should_exit_sl(self, ls: LegState) -> bool:
        """Check if this leg's SL has been hit.
        Normal mode: compare option LTP against sl_price.
        UL mode:     compare live underlying spot against ul_sl_price.
        """
        # ── UL-based SL ─────────────────────────────────────────
        if ls.ul_sl_price > 0:
            info   = self._instr_info()
            idx_tok = info.get("index_token", "")
            spot   = price_store.get(idx_tok) if idx_tok else 0.0
            # MCX: no index_token — use FUT price as underlying spot
            if spot <= 0 and info.get("is_mcx"):
                spot = self._rb_get_price({"priceOf": "Underlying"})
            if spot <= 0:
                return False  # no spot price yet — don't fire
            if ls.action == "SELL":
                if ls.opt_type == "CE":
                    return spot >= ls.ul_sl_price  # SELL CE: exit if spot rises to UL SL
                else:
                    return spot <= ls.ul_sl_price  # SELL PE: exit if spot falls to UL SL
            else:
                if ls.opt_type == "CE":
                    return spot <= ls.ul_sl_price  # BUY CE: exit if spot falls to UL SL
                else:
                    return spot >= ls.ul_sl_price  # BUY PE: exit if spot rises to UL SL
        # ── Normal option-LTP SL ─────────────────────────────────
        ltp = ls.live_ltp()
        if ltp <= 0 or ls.sl_price <= 0: return False
        if ls.action == "SELL": return ltp >= ls.sl_price
        else:                   return ltp <= ls.sl_price

    def _should_exit_tp(self, ls: LegState) -> bool:
        ltp = ls.live_ltp()
        if ltp <= 0 or ls.tp_price <= 0: return False
        if ls.action == "SELL": return ltp <= ls.tp_price
        else:                   return ltp >= ls.tp_price

    def _update_tsl(self, ls: LegState):
        """Update trailing stop loss if configured."""
        if not ls.tsl_config: return
        ltp     = ls.live_ltp()
        if ltp <= 0: return
        x_val   = float(ls.tsl_config.get("x", 0))
        x_unit  = ls.tsl_config.get("xU", "%")
        y_val   = float(ls.tsl_config.get("y", 0))
        y_unit  = ls.tsl_config.get("yU", "%")
        if x_val <= 0 or y_val <= 0: return

        # Current profit
        if ls.action == "SELL":
            cur_profit = ls.avg_entry - ltp
        else:
            cur_profit = ltp - ls.avg_entry

        if cur_profit <= 0: return

        # Has profit moved X beyond previous high?
        if x_unit == "%":
            x_pts = ls.avg_entry * x_val / 100
        else:
            x_pts = x_val

        if cur_profit - ls.tsl_high_profit >= x_pts:
            ls.tsl_high_profit = cur_profit
            # Move SL by Y
            if y_unit == "%":
                y_pts = ls.avg_entry * y_val / 100
            else:
                y_pts = y_val
            if ls.action == "SELL":
                new_sl = round(ltp + y_pts, 2)
                if new_sl < ls.sl_price:
                    ls.sl_price = new_sl
                    self.log.info(f"TSL moved SL to {new_sl:.2f} for {ls.opt_type}")
            else:
                new_sl = round(ltp - y_pts, 2)
                if new_sl > ls.sl_price:
                    ls.sl_price = new_sl
                    self.log.info(f"TSL moved SL to {new_sl:.2f} for {ls.opt_type}")

    def _move_sl_to_cost(self, exited_ls: LegState):
        """
        Move SL to cost per Quantiply logic:
        - Only when 'slCost' flag is set
        - Only when 2 opposite side legs (CE & PE) are open simultaneously
        - When CE side hits SL → move PE side legs to cost (not CE legs)
        - When PE side hits SL → move CE side legs to cost (not PE legs)
        - BUY leg SL is NOT revised when the same-side SELL leg hits SL
        - Sticky-side rule: once a side has been moved to cost in this trade
          group, that side remains the protected/locked side for the whole
          group. If the originally-exited (locking) side hits SL again later,
          the other side does NOT get moved to cost again.
          Reset happens at MTM-level re-execute (fresh group).
        """
        flags = self.s.get("logic", {}).get("flags", [])
        if "slCost" not in flags:
            return
        exited_ot = exited_ls.opt_type  # CE or PE
        opposite_ot = "PE" if exited_ot == "CE" else "CE"
        # Sticky-side: if a side is already locked and exited side equals
        # the previously-locked side again, do nothing.
        if self._sl_cost_locked_side is not None:
            if exited_ot == self._sl_cost_locked_side:
                # Re-hit on already-locked side -- do not re-move opposite
                self.log.info(
                    f"SL to cost: {exited_ot} already locked previously. "
                    f"Sticky-side rule: opposite leg keeps original SL.")
                return
        # Check if opposite side has open legs
        opposite_open = [ls for ls in self.leg_states
                         if ls.status == "OPEN" and ls.opt_type == opposite_ot]
        if not opposite_open:
            return  # no opposite side open — SL to cost does not apply
        # Move SL of opposite side legs to their entry price
        for ls in opposite_open:
            ls.sl_price = ls.avg_entry
            self.log.info(
                f"SL to cost: {ls.opt_type} {ls.action} SL moved to entry "
                f"{ls.avg_entry:.2f} (triggered by {exited_ot} exit)")
        # Mark the exited (locking) side as the locked side for this group
        self._sl_cost_locked_side = exited_ot

    def _enter_leg(self, leg: dict, chain: list) -> Optional[LegState]:
        """Select strike and place entry order for one leg."""
        instr  = self.s.get("idx","NIFTY")
        info   = self._instr_info()
        exch   = info["exchange"]
        prod   = leg.get("prod","MIS")
        action = leg.get("action","SELL")
        side   = "S" if action == "SELL" else "B"
        st_type = str(leg.get("stType","")).strip()
        leg_type = str(leg.get("type","CE")).upper()

        # ── MCX options product-type guard ────────────────────────────
        # Zerodha only allows MIS for MCX ENERGY options (Natural Gas, NG Mini,
        # Crude Oil, Crude Oil M). For ALL other MCX options (Gold, Silver,
        # Copper, Zinc etc.) MIS is blocked — must use NRML.
        # Source: https://support.zerodha.com/category/trading-and-markets/
        #         trading-faqs/general/articles/how-can-i-trade-commodity-options
        if info.get("is_mcx") and leg_type in ("CE","PE") and prod == "MIS":
            _MCX_ENERGY = {"NATURALGAS","NATGASMINI","CRUDEOIL","CRUDEOILM"}
            if instr.upper() not in _MCX_ENERGY:
                self.log.warning(
                    f"{self.name}: {instr} is a non-energy MCX option. "
                    f"Zerodha blocks MIS for these — auto-switching to NRML. "
                    f"Please set Product=NRML in your strategy leg settings.")
                prod = "NRML"

        # ── Strike resolution with smart retry ────────────────────────
        if leg.get("_locked_symbol") and leg.get("_locked_token"):
            # Re-Entry: use exact same strike, skip resolve_strike entirely
            tok     = str(leg["_locked_token"])
            sym     = str(leg["_locked_symbol"])
            ref_ltp = price_store.get(tok) or 0.0
            if ref_ltp <= 0:
                time.sleep(1)
                ref_ltp = price_store.get(tok) or 0.0
            self.log.info(f"Re-Entry locked strike: {fmt_sym(sym)} @ Rs {ref_ltp:.2f}")
        else:
            # Normal strike resolution with retry logic
            tok = sym = ""; ref_ltp = 0.0
            _is_premium_cond = st_type in ("Premium <=", "Premium >=", "Closest Premium")
            _max_feed_retries  = 10
            _max_prem_retries  = 60
            _prem_retry_sleep  = 5
            _prem_attempt      = 0

            # ── Filter chain by leg's expiry type ──────────────────────
            # If chain has multiple expiries (weekly+monthly), filter to only
            # contracts matching this leg's configured expiry type.
            _leg_et = str(leg.get("expiry","Weekly")).lower().replace(" ","_").replace("-","_")
            _emap = {
                "weekly":            "weekly",
                "next_weekly":       "next_weekly",
                "monthly":           "monthly",
                "next_month":        "next_month",
                "next_weekly_expiry":"next_weekly",
                "next_month_expiry": "next_month",
            }
            _leg_et_mapped = _emap.get(_leg_et, "weekly")
            # MCX/FUT: force monthly
            if info.get("is_mcx") and _leg_et_mapped in ("weekly","next_weekly"):
                _leg_et_mapped = "monthly"
            if leg_type == "FUT" and not info.get("is_mcx") and _leg_et_mapped in ("weekly","next_weekly"):
                _leg_et_mapped = "monthly"
            # Get target expiry string from engine's expiry map
            _target_exp = None
            if hasattr(self, '_option_chains'):
                _exp_map = getattr(self, '_instr_expiry_map', {}).get(instr, {})
                _target_exp = _exp_map.get(_leg_et_mapped)
            # Filter chain to only matching expiry contracts
            if _target_exp:
                _filtered_chain = [c for c in chain
                    if str(c.get("expiry","")).replace(" ","").replace("-","")[:10]
                    == str(_target_exp).replace(" ","").replace("-","")[:10]]
                if _filtered_chain:
                    chain = _filtered_chain
                    self.log.info(f"{self.name}: leg expiry filter → {_leg_et_mapped} ({_target_exp}): {len(chain)} contracts")
                else:
                    self.log.warning(f"{self.name}: expiry filter for {_leg_et_mapped} ({_target_exp}) returned 0 — using full chain")

            for _attempt in range(_max_feed_retries):
                tok, sym, ref_ltp = resolve_strike(leg, instr, chain)

                if tok and ref_ltp > 0:
                    break  # ✅ got a contract with live price

                if tok and ref_ltp == 0:
                    # Case A: contract found but no live LTP yet
                    if _attempt < _max_feed_retries - 1:
                        self.log.warning(f"Strike found but LTP=0 for {fmt_sym(sym)}, "
                                         f"waiting 3s for feed (attempt {_attempt+1}/{_max_feed_retries})")
                        time.sleep(3)
                        live = price_store.get(tok)
                        if live > 0:
                            ref_ltp = live
                            break

                elif not tok:
                    # Case B: premium condition not yet met — wait and keep checking
                    if _is_premium_cond:
                        _prem_attempt += 1
                        logic   = self.s.get("logic", {})
                        _def_end = "23:00:00" if INSTRUMENTS.get(instr,{}).get("is_mcx") else "15:15:00"
                        end_t   = logic.get("endTime", _def_end)
                        now_s   = datetime.now().strftime("%H:%M:%S")
                        prem_val = float(leg.get("premVal") or 0)

                        if now_s >= end_t:
                            self.log.warning(
                                f"Premium condition ({st_type} {prem_val}) not met "
                                f"and endTime {end_t} reached — giving up.")
                            break

                        if _prem_attempt > _max_prem_retries:
                            self.log.warning(
                                f"Premium condition ({st_type} {prem_val}) not met "
                                f"after {_max_prem_retries} retries — giving up.")
                            break

                        if _prem_attempt == 1:
                            self.notify.telegram(
                                f"[WAITING] | {self.name}\n\n"
                                f"{leg.get('type')} {st_type} {prem_val}: "
                                f"No strike currently meets condition.\n"
                                f"Checking every {_prem_retry_sleep}s until market "
                                f"offers a qualifying premium (or until {end_t}).")
                            self.log.info(
                                f"Premium condition not met — waiting for market. "
                                f"Will retry every {_prem_retry_sleep}s.")

                        self.log.info(
                            f"Premium condition ({st_type} {prem_val}) not met "
                            f"(attempt {_prem_attempt}/{_max_prem_retries}), "
                            f"retrying in {_prem_retry_sleep}s...")
                        time.sleep(_prem_retry_sleep)
                        continue  # retry same _attempt

                    else:
                        # Case C: structural failure (wrong symbol, expired chain)
                        if _attempt < _max_feed_retries - 1:
                            self.log.warning(f"No strike found for {leg.get('type')} "
                                             f"({st_type}), retrying in 3s "
                                             f"(attempt {_attempt+1}/{_max_feed_retries})")
                            time.sleep(3)

        if not tok:
            # Detect expiry day gap: chain has FUT only, no CE/PE
            all_types_chain = {str(i.get("instrument_type","")).upper() for i in chain}
            if "FUT" in all_types_chain and not ({"CE","PE"} & all_types_chain):
                self.notify.telegram(
                    f"[MARKET NOT READY] | {self.name}\n\n"
                    f"{leg.get('type')} options not yet available for {instr}.\n"
                    f"Zerodha has not yet uploaded next-month option contracts.\n"
                    f"This is normal on MCX expiry day — contracts are uploaded\n"
                    f"after market close (~23:30 IST) or next morning.\n\n"
                    f"Please restart the strategy tomorrow morning.")
            else:
                self.notify.telegram(
                    f"[ERROR] | {self.name}\nNo strike found for {leg.get('type')} "
                    f"({leg.get('stType')})\nCheck premium range and live feed.")
            return None
        if ref_ltp <= 0:
            # WebSocket hasn't ticked for this token yet.
            # Use kite.ltp() REST call to get actual last traded price.
            ref_ltp = price_store.get(tok)
            if ref_ltp <= 0 and hasattr(self.broker, "get_rest_ltp"):
                try:
                    instr_info = self._instr_info()
                    exch = instr_info.get("exchange", "NFO")
                    ref_ltp = self.broker.get_rest_ltp(exch, sym, tok)
                    if ref_ltp > 0:
                        price_store.update(tok, ref_ltp)
                        self.log.info(f"LTP fetched via REST for {fmt_sym(sym)}: Rs {ref_ltp:.2f}")
                except Exception as e:
                    self.log.warning(f"REST LTP fallback failed for {fmt_sym(sym)}: {e}")
            if ref_ltp <= 0:
                self.log.warning(f"Cannot enter {fmt_sym(sym)} — LTP=0 after all retries. "
                                 f"Market may be closed or feed unavailable. Skipping entry.")
                return None, None, 0.0  # ← abort entry cleanly

        pnl_qty   = self._qty(leg)        # for P&L calculation (actual lot size)
        order_qty = self._order_qty(leg)   # for Zerodha API (MCX=1, NSE/BSE=lot_size)
        qty = pnl_qty                      # ls.qty uses pnl_qty for correct P&L display
        if order_qty <= 0:
            self.log.warning(f"Leg {leg.get('type')} qty=0 -- skipping (check lots setting)")
            return None
        # Per-strategy buffer from Advanced Settings (falls back to global cfg)
        adv        = self.s.get("advanced", {})
        entry_buf  = float(adv.get("entryBufferVal", 0))

        # Industry best practice (AlgoTest / QuantMan):
        #   Options: 8–10% buffer (illiquid, wide spreads)
        #   Futures: ~1% buffer (liquid, tight spreads — 10% would cause huge slippage)
        # Auto-cap FUT buffer to 1% regardless of Advanced Settings.
        leg_type = str(leg.get("type","CE")).upper()
        if leg_type == "FUT" and entry_buf > 1.0:
            entry_buf = 1.0
            self.log.info(
                f"FUT order: buffer auto-capped at 1% (user set {adv.get('entryBufferVal')}%). "
                f"Futures are liquid — large buffer causes unnecessary slippage.")

        # Condition price cap: only applies to premium-based strike selection
        st_type    = leg.get("stType", "")
        prem_val   = float(leg.get("premVal") or 0)
        cond_price = 0.0
        if st_type in ("Premium <=", "Closest Premium") and prem_val > 0:
            cond_price = prem_val    # never sell above this cap

        # W&T check BEFORE placing order — prevent live order before condition met
        wnt_cfg_pre = leg.get("wntConfig") or {}
        wnt_mode_pre = str(wnt_cfg_pre.get("mode","")).strip()
        wnt_val_pre = float(wnt_cfg_pre.get("value", 0) or 0)
        wnt_is_active_pre = bool(wnt_mode_pre and wnt_mode_pre != "Immediate" and wnt_val_pre > 0)
        if wnt_is_active_pre:
            fill = ref_ltp  # reference price only — no real order placed
        else:
            fill = self.om.execute(tok, sym, order_qty, side, ref_ltp, exch, prod,
                                   f"{self.name[:8]}_{action[0]}",
                                   condition_price=cond_price,
                                   buf_pct=entry_buf)
        if not wnt_is_active_pre and fill <= 0:
            ls = LegState(leg_id=leg["id"], opt_type=leg.get("type","CE"),
                          action=action, token=tok, symbol=sym, qty=qty,
                          status="FAILED", failed=True,
                          max_reentry=int(leg.get("resl",0)))
            return ls
        wnt_cfg = leg.get("wntConfig") or {}
        wnt_mode = str(wnt_cfg.get("mode","")).strip()
        wnt_val  = float(wnt_cfg.get("value", 0) or 0)
        wnt_is_active = bool(wnt_mode and wnt_mode != "Immediate" and wnt_val > 0)

        # Reference price: Instrument LTP or Underlying spot (for UL modes)
        wnt_entry_at = 0.0
        # Capture reference price AND time at strategy start moment.
        # This is the exact time the strategy reached start_t and called _enter_leg.
        # For W&T: ref price = live LTP at this moment (option LTP for Points/%,
        # FUT price for UL modes). ref time = now (HH:MM:SS).
        _wnt_ref_time = datetime.now().strftime("%H:%M:%S")
        live_ltp_now = price_store.get(tok)
        wnt_ref_used = live_ltp_now if live_ltp_now > 0 else (fill if fill > 0 else ref_ltp)
        if wnt_is_active:
            is_ul_mode = "UL" in wnt_mode.upper()
            if is_ul_mode:
                info_wnt  = self._instr_info()
                ul_tok_r  = info_wnt.get("index_token", "")
                ul_price  = price_store.get(ul_tok_r) if ul_tok_r else 0.0
                # MCX: no index spot token — FUT IS the underlying price.
                # _rb_get_price("Underlying") handles all MCX instruments
                # via WebSocket price_store + REST fallback. Universal fix.
                if ul_price <= 0 and info_wnt.get("is_mcx"):
                    ul_price = self._rb_get_price({"priceOf": "Underlying"})
                wnt_ref_used = ul_price if ul_price > 0 else wnt_ref_used
            is_up_wnt  = "\u2191" in wnt_mode or "Up" in wnt_mode
            is_pct_wnt = "%" in wnt_mode
            if is_up_wnt:
                wnt_entry_at = round(wnt_ref_used * (1 + wnt_val/100), 2) if is_pct_wnt else round(wnt_ref_used + wnt_val, 2)
            else:
                wnt_entry_at = round(wnt_ref_used * (1 - wnt_val/100), 2) if is_pct_wnt else round(wnt_ref_used - wnt_val, 2)
            if wnt_entry_at <= 0:
                self.notify.telegram(
                    "[WARN] | " + self.name + "\n"
                    "W&T: entry price <= 0 (" + str(wnt_entry_at) + ") for " + fmt_sym(sym) + ".\n"
                    "Adjust W&T value. W&T disabled for this leg.")
                wnt_entry_at = 0.0
                wnt_is_active = False


        # Capture underlying spot at entry time for UL-based SL calculation
        _info_ul = self._instr_info()
        _ul_tok  = _info_ul.get("index_token", "")
        _ul_spot = price_store.get(_ul_tok) if _ul_tok else 0.0
        # MCX: no index_token — use FUT price as underlying spot (same fix as W&T UL mode)
        if _ul_spot <= 0 and _info_ul.get("is_mcx"):
            _ul_spot = self._rb_get_price({"priceOf": "Underlying"})
        _sl_unit = str(leg.get("slU", "%"))
        _ul_sl   = self._compute_ul_sl_price(leg, _ul_spot) if not wnt_is_active else 0.0

        ls = LegState(
            leg_id=leg["id"], opt_type=leg.get("type","CE"),
            action=action, token=tok, symbol=sym, qty=qty,
            # For W&T legs, we haven't entered yet -- fill is just the reference price
            # Status stays PENDING until W&T triggers
            entry_price=0.0 if wnt_is_active else fill,
            avg_entry=0.0  if wnt_is_active else fill,
            first_entry_price=0.0 if wnt_is_active else fill,
            sl_price=0.0   if wnt_is_active else self._compute_sl_price(leg, fill),
            tp_price=0.0   if wnt_is_active else self._compute_tp_price(leg, fill),
            tsl_config=leg.get("tslConfig") or {},
            status="PENDING" if wnt_is_active else "OPEN",
            max_reentry=int(leg.get("resl",0)),
            reentry_type=str(leg.get("rentryType","Re-Cost")),
            entry_time="" if wnt_is_active else datetime.now().strftime("%H:%M:%S"),
            wnt_active=wnt_is_active,
            wnt_ref_price=wnt_ref_used if wnt_is_active else fill,  # UL mode: FUT price; Pts/%: option LTP
            wnt_entry_price=wnt_entry_at,
            wnt_mode=wnt_mode if wnt_is_active else "",
            wnt_val=wnt_val if wnt_is_active else 0.0,
            wnt_ref_time=_wnt_ref_time if wnt_is_active else "",
            ul_entry_spot=_ul_spot if not wnt_is_active else 0.0,
            ul_sl_price=_ul_sl,
        )
        if not wnt_is_active and _ul_sl > 0:
            self.log.info(
                f"UL SL ({_sl_unit}): underlying at entry={_ul_spot:.0f} "
                f"→ UL SL threshold={_ul_sl:.0f}")
        if wnt_is_active:
            self.notify.log("WAIT&TRADE", self.name,
                f"Script: {self.s.get('idx','NIFTY')} | "
                f"Strike: {fmt_sym(sym)} | "
                f"Ref Price: Rs {fill:.2f} | Entry at: Rs {wnt_entry_at:.2f} | "
                f"Mode: {wnt_mode} {wnt_val}")
        return ls

    def _exit_leg(self, ls: LegState, reason: str) -> float:
        """Exit one leg. Returns fill price."""
        info = self._instr_info()
        exch = info["exchange"]
        # Reverse side
        side = "B" if ls.action == "SELL" else "S"
        leg  = next((l for l in self.s["legs"] if l["id"]==ls.leg_id), {})
        prod = leg.get("prod","MIS")
        adv      = self.s.get("advanced", {})
        exit_buf = float(adv.get("exitBufferVal", 0))
        # FUT exits also get 1% cap (same reasoning as entry — tight spread)
        if ls.opt_type == "FUT" and exit_buf > 1.0:
            exit_buf = 1.0

        # ── Exit order retry — attempt up to 10 times ──────────
        fill = 0.0
        _exit_attempts = 10  # more attempts for exit (critical to close)

        # Get current live LTP for exit reference — prefer WebSocket,
        # fall back to kite.ltp() REST for illiquid tokens with no ticks
        _exit_ref_ltp = price_store.get(ls.token) or 0.0
        if _exit_ref_ltp <= 0 and hasattr(self.broker, "get_rest_ltp"):
            try:
                _exit_ref_ltp = self.broker.get_rest_ltp(exch, ls.symbol, ls.token)
                if _exit_ref_ltp > 0:
                    price_store.update(ls.token, _exit_ref_ltp)
                    self.log.info(f"Exit LTP via REST for {fmt_sym(ls.symbol)}: Rs {_exit_ref_ltp:.2f}")
            except Exception as _e:
                self.log.warning(f"Exit REST LTP failed: {_e}")
        # Final fallback: use avg_entry (at least a valid price)
        if _exit_ref_ltp <= 0:
            _exit_ref_ltp = ls.avg_entry if ls.avg_entry > 0 else ls.sl_price

        for _ex_att in range(_exit_attempts):
            # Refresh LTP on each retry
            _fresh_ltp = price_store.get(ls.token) or _exit_ref_ltp
            if _fresh_ltp > 0:
                _exit_ref_ltp = _fresh_ltp

            _buf = exit_buf if _ex_att == 0 else min(exit_buf + (_ex_att * 2.0), 15.0)
            # Exit: use _order_qty (MCX=1, NSE/BSE=lot_size) not ls.qty (P&L qty)
            _exit_order_qty = self._order_qty_from_ls(ls)
            fill = self.om.execute(ls.token, ls.symbol, _exit_order_qty, side,
                                   _exit_ref_ltp, exch, prod, f"{self.name[:8]}_X",
                                   condition_price=0.0,
                                   buf_pct=_buf)
            if fill > 0:
                break  # exit succeeded
            # Exit failed — log and retry
            err = self.broker.get_last_order_error() if hasattr(self.broker, "get_last_order_error") else ""
            self.log.warning(
                f"Exit order failed for {ls.opt_type} {fmt_sym(ls.symbol)} "
                f"(attempt {_ex_att+1}/{_exit_attempts}): {err}")
            if _ex_att < _exit_attempts - 1:
                time.sleep(1)

        if fill <= 0:
            ltp_now = price_store.get(ls.token) or ls.avg_entry
            ls.exit_failed = True   # flag for dashboard retry button
            ls.status = "OPEN"      # keep as OPEN so monitoring continues
            self.log.error(
                f"EXIT FAILED after {_exit_attempts} attempts for "
                f"{ls.opt_type} {fmt_sym(ls.symbol)} — position may still be open!")
            self.notify.telegram(
                f"[EXIT FAILED] | {self.name}\n\n"
                f"⚠️ Could not exit {ls.opt_type} {fmt_sym(ls.symbol)}\n"
                f"after {_exit_attempts} attempts.\n"
                f"Reason: {self.broker.get_last_order_error() if hasattr(self.broker,'get_last_order_error') else 'Unknown'}\n\n"
                f"Dashboard: Open View → Retry Exit or Mark Manual.\n"
                f"Or wait — bot will retry automatically on next SL check.")
            fill = ltp_now
        ltp = fill if fill > 0 else price_store.get(ls.token) or ls.sl_price
        if ls.action == "SELL":
            ls.realised_pnl += (ls.avg_entry - ltp) * ls.qty
        else:
            ls.realised_pnl += (ltp - ls.avg_entry) * ls.qty
        ls.status    = "CLOSED"
        ls.exit_time = datetime.now().strftime("%H:%M:%S")
        ls.exit_price = ltp
        ls.waiting_reentry = False
        # Track closed leg history for view modal (one row per re-entry cycle)
        if not hasattr(self, "_closed_legs_history"):
            self._closed_legs_history = []
        import copy
        _snap = copy.copy(ls)
        self._closed_legs_history.append(_snap)
        self.log.info(f"{ls.opt_type} exited @ {ltp:.2f} | {reason} | P&L Rs {ls.realised_pnl:.0f}")
        return ltp

    def _exit_all(self, reason: str):
        for ls in self.leg_states:
            if ls.status == "OPEN":
                self._exit_leg(ls, reason)

    def _process_action_queue(self):
        with self._lock:
            q = list(self._retry_q)
            self._retry_q.clear()
        for action, leg_id in q:
            ls = next((x for x in self.leg_states if x.leg_id == leg_id), None)
            if not ls: continue
            if action == "disable":
                ls.disabled = True; ls.status = "CLOSED"; ls.failed = False
                self.notify.telegram(f"[LEG DISABLED] | {self.name}\n{ls.opt_type} leg skipped.")
                # Clear from error alert manager if no more failed legs
                self._clear_error_if_resolved()
            elif action == "retry" and ls.failed:
                leg = next((l for l in self.s["legs"] if l["id"]==leg_id), None)
                if not leg: continue
                new_ls = self._enter_leg(leg, self._option_chain)
                if new_ls and not new_ls.failed:
                    idx = next((i for i,x in enumerate(self.leg_states) if x.leg_id==leg_id), None)
                    if idx is not None: self.leg_states[idx] = new_ls
                    self.notify.telegram(
                        f"[RETRY OK] | {self.name}\n{new_ls.opt_type} filled @ Rs {new_ls.entry_price:.0f}")
                    self._clear_error_if_resolved()
                else:
                    self.notify.telegram(
                        f"[RETRY FAILED] | {self.name}\n{ls.opt_type} retry failed again. Try again or Skip.")

            elif action == "retry_exit" and ls.exit_failed:
                # User clicked "Retry Exit" in dashboard — attempt exit again
                ls.exit_failed = False
                fill = self._exit_leg(ls, "Manual retry exit")
                if fill > 0:
                    self.notify.telegram(
                        f"[EXIT OK] | {self.name}\n"
                        f"{ls.opt_type} {fmt_sym(ls.symbol)} exited @ Rs {fill:.0f} (manual retry)")
                else:
                    ls.exit_failed = True
                    self.notify.telegram(
                        f"[EXIT FAILED] | {self.name}\n"
                        f"{ls.opt_type} still could not exit. Try again or use Mark Manual.")

    def _check_range_breakout(self) -> bool:
        """Range breakout: placeholder, real logic handled in run()."""
        return False

    def _clear_error_if_resolved(self):
        """Remove this strategy from the error alert manager if no failed legs remain."""
        remaining = [ls for ls in self.leg_states if ls.failed and not ls.disabled]
        if remaining:
            return  # still have failures — keep alerting
        try:
            from dashboard import _active_errors, _active_errors_lk
            with _active_errors_lk:
                if self.sid in _active_errors:
                    del _active_errors[self.sid]
        except Exception:
            pass

    def _check_protect_profit(self, ls: "LegState") -> None:
        """
        Protect Profit (DEPRECATED at per-leg level).

        Protect Profit is now handled at the PORTFOLIO level inside the main
        run() loop, unified with MTM SL/Target. This method is intentionally
        a no-op to avoid double-firing when both this and the unified check
        operate on the same MTM. See "Compute effective MTM SL" block in run().
        """
        return

    def _rb_get_price(self, rb_cfg: dict) -> float:
        """Get the tracking price for range breakout.

        Price source by instrument type:
        ┌─────────────────────────┬──────────────────────────────────────────┐
        │ NSE/BSE (options + FUT) │ Index spot token (WebSocket)             │
        │                         │ e.g. NIFTY→256265, SENSEX→265            │
        ├─────────────────────────┼──────────────────────────────────────────┤
        │ MCX (options + FUT)     │ FUT token LTP (WebSocket)                │
        │                         │ No spot exists — FUT IS the price        │
        └─────────────────────────┴──────────────────────────────────────────┘
        priceOf="Instrument": tracks first open leg LTP (any instrument).
        """
        price_of = rb_cfg.get("priceOf", "Underlying")

        if price_of != "Underlying":
            # Track first open leg's LTP directly
            for ls in self.leg_states:
                if ls.status in ("OPEN", "PENDING"):
                    ltp = ls.live_ltp()
                    if ltp > 0:
                        return ltp
            return 0.0

        info  = self._instr_info()
        instr = self.s.get("idx", "").upper()

        # ── NSE / BSE ── index spot token subscribed to WebSocket
        tok = info.get("index_token", "")
        if tok:
            price = price_store.get(tok) or 0.0
            if price > 0:
                return price
            # Fallback: REST LTP if WebSocket hasn't ticked yet
            ws_key = info.get("index_ws_key", "")
            if ws_key and hasattr(self.broker, "get_rest_ltp"):
                try:
                    _rp = self.broker.get_rest_ltp("NSE", ws_key.split(":")[-1], "")
                    if _rp > 0: return _rp
                except Exception:
                    pass
            return 0.0

        # ── MCX ── no spot token; use FUT LTP from subscribed chain
        chain = getattr(self, '_cached_chain', [])
        for c in chain:
            sym_t = str(c.get("tradingsymbol") or "").upper()
            opt   = str(c.get("instrument_type") or "").upper().strip()
            if opt != "FUT":
                continue
            if not sym_t.startswith(instr):
                continue
            ftok = str(c.get("instrument_token") or "")
            if not ftok:
                continue
            fltp = price_store.get(ftok) or 0.0
            if fltp > 0:
                return fltp
            # REST fallback for illiquid MCX FUT
            if hasattr(self.broker, "get_rest_ltp"):
                try:
                    _rp = self.broker.get_rest_ltp("MCX", sym_t, ftok)
                    if _rp > 0: return _rp
                except Exception:
                    pass

        return 0.0

    def run(self, option_chain: list):
        """Main strategy execution loop."""
        # ── Route to indicator runner if stratType == indicator ──
        if self.s.get("stratType","time") == "indicator":
            self._run_indicator(option_chain)
            return
        self._cached_chain = option_chain  # used by _rb_get_price for MCX FUT price
        logic     = self.s.get("logic", {})
        instr     = self.s.get("idx","NIFTY")
        start_t   = logic.get("startTime","09:20:00")
        end_t     = logic.get("endTime","23:00:00" if INSTRUMENTS.get(instr,{}).get("is_mcx") else "15:15:00")

        # ── Multiplier scaling ────────────────────────────────────
        # MTM SL, MTM Target and Protect Profit values are entered
        # at 1x in the dashboard. Multiplier scales them proportionally.
        # e.g. MTM SL = Rs 4000 at 1x → Rs 24000 at 6x
        # This matches Quantiply/AlgoTest behavior exactly.
        def _get_mult() -> int:
            mult_str = str(self.s.get("mult","1X")).upper().replace("X","")
            return max(1, int(mult_str) if mult_str.isdigit() else 1)

        def _scaled_mtm(key, type_key, default=0):
            """Scale MTM value by multiplier — Amount (₹) type only.
            Percentage type is not scaled — it applies to actual P&L
            which is already proportional to multiplier automatically."""
            base  = float(logic.get(key, default) or default)
            mtype = str(logic.get(type_key, "Amount (₹)"))
            if "%" in mtype:
                return base  # no scaling — auto-proportional
            return base * _get_mult()  # Amount — scale by multiplier

        def _scaled_pp(val: float) -> float:
            """Scale a protect profit value by multiplier."""
            return val * _get_mult()

        mtm_sl    = _scaled_mtm("mtmSL",     "mtmSLType")
        mtm_tgt   = _scaled_mtm("mtmTarget", "mtmTType")
        sq_mode        = "one" if self.s.get("sqOffMode","one") == "one" else "all"
        re_execute_max = int(self.s.get("reExecuteMax", 0)) if sq_mode == "all" else 0
        re_execute_count = 0   # counts how many full re-executes have happened
        days      = self.s.get("days",[True]*5+[False,False])
        is_positional = logic.get("tradeType","Intraday") == "Positional"

        self._option_chain = option_chain

        # Market segment constants for hours guard
        # Use _instr_info() — same lookup used everywhere else in the runner.
        # MCX = any instrument with "exchange":"MCX" in the INSTRUMENTS table.
        _info_run     = self._instr_info()
        _is_mcx_strategy = (_info_run.get("exchange","").upper() == "MCX" or
                             bool(_info_run.get("is_mcx", False)))
        self.log.info(f"{self.name}: exchange={_info_run.get('exchange','?')} "
                      f"is_mcx={_is_mcx_strategy} idx={instr}")
        # NSE/BSE market hours (IST): 09:15-15:30. MCX: 09:00-23:30.
        NSE_OPEN  = '09:15:00'
        NSE_CLOSE = '15:30:00'
        MCX_OPEN  = '09:00:00'
        MCX_CLOSE = '23:30:00'

        # Range Breakout state -- shared across loop iterations
        _rb_high       = 0.0     # highest price seen during range window
        _rb_low        = float('inf')  # lowest price seen during range window
        _rb_window_done = False   # True after window closes
        _rb_triggered  = False    # True once breakout entry fires
        _rb_first_done = False    # tracks "first only" setting

        # Day filter
        today_idx = date.today().weekday()  # 0=Mon ... 6=Sun
        if today_idx < len(days) and not days[today_idx]:
            self.log.info(f"{self.name}: skipped -- not scheduled for today.")
            self.status = "CLOSED"
            return

        # ── Positional: try restoring overnight positions ──────────────
        # If saved state exists from a previous session, reload legs and
        # skip entry logic for today (already in the market).
        _restored_from_disk = False
        if is_positional:
            _restored_from_disk = self.load_positional_state()
            if _restored_from_disk:
                # Positions loaded. Check BTST next-day exit conditions.
                is_btst_r = self.s.get("logic",{}).get("btst",{}).get("type","") in ("BTST","STBT")
                btst_cfg_r = self.s.get("logic",{}).get("btst",{}) if is_btst_r else {}
                nd_end_t_r = btst_cfg_r.get("nextDayEndTime", end_t)
                hold_till_expiry_r = btst_cfg_r.get("holdTillExpiry", False)
                chk_after_r = btst_cfg_r.get("checkAfter", "09:16:00")
                _now_r = datetime.now().strftime("%H:%M:%S")
                _saved_date = self._positional_state_path()
                import json as _json
                try:
                    _sd = _json.load(open(self._positional_state_path())).get("saved_at","")
                    from datetime import date as _date
                    _saved_day = _sd[:10] if _sd else str(_date.today())
                    _is_next_day = str(_date.today()) > _saved_day
                except: _is_next_day = False
                # If BTST and already past next-day exit time → exit immediately
                if is_btst_r and _is_next_day and not hold_till_expiry_r and _now_r >= nd_end_t_r:
                    self.log.info(f"{self.name}: [BTST] Restored on next day, past exit time {nd_end_t_r}. Exiting now.")
                    self.notify.telegram(f"[SQUAREOFF] | {self.name}\n\nBTST: Next day exit time {nd_end_t_r} already passed.\nSquaring off restored positions now.")
                    self.status = "RUNNING"
                    self._exit_all("BTST next day exit — restored late")
                    self.status = "CLOSED"
                    return
                # Wait for start_t or chk_after on next day
                _wait_t = chk_after_r if (is_btst_r and _is_next_day) else start_t
                self.log.info(f"{self.name}: [Positional] Restored positions, waiting for {_wait_t} to resume monitoring.")
                self.status = "READY"
                while datetime.now().strftime("%H:%M:%S") < _wait_t:
                    if self.stopped: return
                    time.sleep(0.5)
                # If BTST next day — set end_t to nd_end_t
                # If holdTillExpiry — always push end_t to midnight regardless of BTST type
                if hold_till_expiry_r:
                    end_t = "23:59:59"
                elif is_btst_r and _is_next_day:
                    end_t = nd_end_t_r
                self.status = "RUNNING"
                # Fall through to the monitor loop block below.

        _rb_mode = False  # default: set below only if not restoring
        if not _restored_from_disk:
            self.log.info(f"{self.name}: waiting for {start_t}...")
            self.status = "READY"

        if not _restored_from_disk:
            while datetime.now().strftime("%H:%M:%S") < start_t:
                # Re-read start_t and end_t from live config every tick.
                # If user changes end time while strategy is in READY/waiting state,
                # the new value takes effect immediately — no restart needed.
                logic   = self.s.get("logic", {})
                start_t = logic.get("startTime", start_t)
                end_t   = logic.get("endTime",   end_t)
                if datetime.now().strftime("%H:%M:%S") >= end_t:
                    self.log.info(f"{self.name}: past end time before start -- skip today.")
                    self.status = "CLOSED"; return
                if not self.s.get("enabled", True):
                    self.status = "DISABLED"; return
                time.sleep(0.5)

        # ── Market hours gate (paper + live) ─────────────────────────
        # Check BEFORE attempting entry. Works in all modes including dry-run
        # because paper trades should also respect market hours for realism.
        # NSE/BSE F&O: 09:15–15:30 IST, Mon–Fri
        # MCX:         09:00–23:30 IST, Mon–Fri
        # If market is closed at the time entry would happen, skip the day
        # entirely and send a clear Telegram alert.
        if not _restored_from_disk:
            _now_check2 = datetime.now()
            _wday2      = _now_check2.weekday()   # 0=Mon … 6=Sun
            _now_s2     = _now_check2.strftime("%H:%M:%S")
            _is_weekend = _wday2 >= 5
            if _is_mcx_strategy:
                _mkt_closed_now = _is_weekend or (_now_s2 > MCX_CLOSE)
                _mkt_name = "MCX"
            else:
                _mkt_closed_now = _is_weekend or (_now_s2 > NSE_CLOSE)
                _mkt_name = "NSE/BSE"

            if _mkt_closed_now:
                _day_names = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
                _reason = (f"Weekend ({_day_names[_wday2]})" if _is_weekend
                           else f"Outside trading hours ({_now_s2} IST)")
                self.log.warning(
                    f"{self.name}: {_mkt_name} market closed — {_reason}. "
                    f"Skipping strategy for today.")
                self.notify.telegram(
                    f"[MARKET CLOSED] | {self.name}\n\n"
                    f"Exchange  : {_mkt_name}\n"
                    f"Reason    : {_reason}\n"
                    f"Trading hours:\n"
                    + (f"  MCX: 09:00–23:30 IST, Mon–Fri" if _is_mcx_strategy
                       else f"  NSE/BSE F&O: 09:15–15:30 IST, Mon–Fri") +
                    f"\nStrategy skipped for today. Restart during market hours.")
                self.status = "CLOSED"
                return

        # ── Entry block: skip if restoring positional state ─────────
        # If we loaded overnight positions, skip fresh entry entirely.
        # The monitor loop below resumes watching those positions.
        if not _restored_from_disk:
            # STRICT TIME CHECK: if current time is already past end_t
            # do NOT take entry — but keep waiting in case user updates end time.
            # Re-read end_t from live config every 0.5s so changes take effect immediately.
            while True:
                logic   = self.s.get("logic", {})
                end_t   = logic.get("endTime", end_t)
                start_t = logic.get("startTime", start_t)
                _now_entry_check = datetime.now().strftime("%H:%M:%S")
                if _now_entry_check < end_t:
                    break  # end time is in future — proceed to entry
                if not self.s.get("enabled", True):
                    self.status = "DISABLED"; return
                if self.stopped:
                    return
                # Still past end time — log once and keep waiting
                if not getattr(self, "_past_end_logged", False):
                    self.log.info(
                        f"{self.name}: current time {_now_entry_check} past "
                        f"end time {end_t} — waiting for end time update...")
                    self._past_end_logged = True
                self.status = "READY"
                time.sleep(0.5)
            self._past_end_logged = False  # reset for next time

            # Check if rangeBreak is enabled -- if so, skip immediate entry
            # (legs will be entered when breakout triggers after the range window)
            _rb_mode = "rangeBreak" in self.s.get("logic", {}).get("flags", [])

        if _rb_mode:
            # ── Window already closed before bot started ──────────────────
            # Instead of killing the strategy, try to reconstruct the range
            # from Zerodha's historical data API. This allows the user to:
            #   - Start the strategy at any time during the day
            #   - Set an earlier RB window (e.g. 11:00–11:15)
            #   - Bot will fetch that candle's H/L and monitor breakout live
            rb_cfg_check   = (self.s.get("logic") or {}).get("rangeBreak") or {}
            rb_from_check  = rb_cfg_check.get("windowStart","09:15:00")
            rb_until_check = rb_cfg_check.get("windowEnd","09:30:00")
            now_check      = datetime.now().strftime("%H:%M:%S")

            if now_check > rb_until_check:
                # Window already closed — fetch historical H/L
                self.log.info(
                    f"{self.name}: RB window {rb_from_check}-{rb_until_check} "
                    f"already closed (now {now_check}). "
                    f"Fetching historical candle to reconstruct range...")

                # Determine which token to use (Underlying or Instrument)
                price_of = rb_cfg_check.get("priceOf","Underlying")
                if price_of == "Underlying":
                    hist_token = self._instr_info().get("index_token","")
                else:
                    # Use first leg's token from option chain
                    hist_token = ""
                    for leg in self.s.get("legs",[]):
                        tok, _, _ = resolve_strike(leg, instr, option_chain)
                        if tok:
                            hist_token = tok
                            break

                if hist_token and hasattr(self.broker, "get_candle_high_low"):
                    result = self.broker.get_candle_high_low(
                        hist_token, rb_from_check, rb_until_check)
                    if result.get("ok") and result["high"] > 0:
                        _rb_high        = result["high"]
                        _rb_low         = result["low"]
                        _rb_window_done = True   # skip live recording phase
                        mode_rb = "PAPER" if self.dry_run else "LIVE"
                        rb_range = round(_rb_high - _rb_low, 2)
                        self.notify.telegram(
                            f"[RB RANGE] | {self.name}  [{mode_rb}]\n\n"
                            f"Window     : {rb_from_check} → {rb_until_check} (historical)\n"
                            f"Range High : Rs {round(_rb_high,2)}\n"
                            f"Range Low  : Rs {round(_rb_low,2)}\n"
                            f"Range Pts  : {rb_range} pts\n"
                            f"Entry at   : {rb_cfg_check.get('entryAt','High')}\n"
                            f"Watching for breakout now...")
                        self.log.info(
                            f"RB historical range loaded: H={_rb_high} L={_rb_low}")
                    else:
                        # Historical fetch failed — inform user but keep running
                        self.notify.telegram(
                            f"[RB WARNING] | {self.name}\n\n"
                            f"Could not fetch historical data for "
                            f"{rb_from_check}–{rb_until_check}.\n"
                            f"Range Breakout will start recording from NOW.\n"
                            f"Consider editing the window time to a future window.")
                        self.log.warning(
                            f"{self.name}: historical fetch failed, "
                            f"RB will record live from current time.")
                else:
                    self.notify.telegram(
                        f"[RB WARNING] | {self.name}\n\n"
                        f"Window {rb_from_check}–{rb_until_check} already closed.\n"
                        f"Historical data not available in paper/dry-run mode.\n"
                        f"Monitoring will start from current price levels.")
                    self.log.warning(
                        f"{self.name}: no historical API available (paper mode?), "
                        f"range will be recorded live from current time.")

        if _rb_mode:
            # Range breakout: create PENDING placeholder leg states now so
            # the dashboard shows the strategy is running and watching
            self.status = "RUNNING"
            results = []
            for leg in self.s["legs"]:
                instr = self.s.get("idx","NIFTY")
                info  = self._instr_info()
                # Resolve the strike now (to show it in dashboard) but don't enter
                tok, sym, ref_ltp = resolve_strike(leg, instr, option_chain)
                if tok:
                    ls_pend = LegState(
                        leg_id=leg["id"], opt_type=leg.get("type","CE"),
                        action=leg.get("action","SELL"), token=tok, symbol=sym,
                        qty=self._qty(leg), status="PENDING",
                        max_reentry=int(leg.get("resl",0)),
                        reentry_type=str(leg.get("rentryType","Re-Cost")),
                        wnt_active=False,
                    )
                    results.append(ls_pend)
            self.leg_states = results
            rb_cfg0 = (self.s.get("logic") or {}).get("rangeBreak") or {}
            mode0 = "PAPER" if self.dry_run else "LIVE"
            self.notify.telegram(
                "[RB WAITING] | " + self.name + "  [" + mode0 + "]\n\n"
                + "Range window : " + rb_cfg0.get("windowStart","09:15:00") + " to " + rb_cfg0.get("windowEnd","09:30:00") + "\n"
                + "Entry at     : " + rb_cfg0.get("entryAt","High") + "\n"
                + "Tracking     : " + rb_cfg0.get("priceOf","Underlying") + "\n"
                + "Recording range now...")
        else:
            # Normal immediate entry
            self.status = "RUNNING"
            results = []
            with ThreadPoolExecutor(max_workers=min(len(self.s["legs"]),4)) as ex:
                futs = {ex.submit(self._enter_leg, leg, option_chain): leg
                        for leg in self.s["legs"]}
                for fut in as_completed(futs):
                    leg_cfg = futs[fut]
                    ls = fut.result()
                    if ls:
                        results.append(ls)
                    else:
                        # _enter_leg returned None — create a failed placeholder
                        # so the leg appears in the View modal with a Retry button
                        failed_ls = LegState(
                            leg_id=leg_cfg["id"],
                            opt_type=leg_cfg.get("type","CE"),
                            action=leg_cfg.get("action","SELL"),
                            token="", symbol="",
                            qty=self._qty(leg_cfg),
                            status="FAILED", failed=True,
                            max_reentry=int(leg_cfg.get("resl",0)),
                            reentry_type=str(leg_cfg.get("rentryType","Re-Cost")),
                        )
                        results.append(failed_ls)

            if not results:
                self.notify.telegram(
                    f"[ERROR] | {self.name}\nAll legs failed to enter.\n"
                    f"Use Retry button in View modal to retry.")
                pass

            self.leg_states = results
        # Notify entry
        ok_legs  = [ls for ls in self.leg_states if not ls.failed]
        fail_legs= [ls for ls in self.leg_states if ls.failed]
        if ok_legs:
            mode     = "PAPER" if self.dry_run else "LIVE"
            instr    = self.s.get("idx","NIFTY")
            qty_line = ok_legs[0].qty if ok_legs else 0
            leg_lines = "\n".join(
                f"{ls.opt_type}      : {fmt_sym(ls.symbol)} @ Rs {ls.entry_price:.0f}"
                for ls in ok_legs)
            tg_body = (
                f"Time     : {datetime.now().strftime('%H:%M:%S')}\n"
                f"Type     : INITIAL ENTRY\n\n"
                f"{leg_lines}\n"
                f"Qty      : {qty_line}\n"
                f"MTM      : Rs 0")
            self.notify.telegram(f"[ENTRY] | {self.name}  [{mode}]\n\n{tg_body}")
            # Activity log -- structured entry per leg
            for ls in ok_legs:
                is_opt = ls.opt_type in ("CE","PE")
                log_body = (
                    f"Script: {instr} | {'Options' if is_opt else 'Futures'} | "
                    f"Strike: {fmt_sym(ls.symbol)} | "
                    f"Type: {ls.opt_type} | Txn: {ls.action} | "
                    f"Cond: ENTRY | "
                    f"Time: {ls.entry_time or datetime.now().strftime('%H:%M:%S')} | "
                    f"Qty: {ls.qty} | Price: Rs {ls.entry_price:.2f}")
                self.notify.log("ENTRY", self.name, log_body)
        if 'fail_legs' in dir() and fail_legs:
            # Set status to ERROR so dashboard shows error state
            if not ('ok_legs' in dir() and ok_legs):
                self.status = "ERROR"
            mode_e = "PAPER" if self.dry_run else "LIVE"
            lines_e = "\n".join(
                f"  • {ls.opt_type} ({ls.action}) — no fill"
                for ls in fail_legs)
            first_alert = (
                f"⚠️ [LEG ERROR] | {self.name}  [{mode_e}]\n\n"
                f"Failed legs:\n{lines_e}\n\n"
                f"Action needed: Open dashboard → View (👁) → Retry or Skip.\n"
                f"Repeat alerts will follow every 60s (up to 10 times).")
            self.notify.telegram(first_alert)
            # Register with error alert manager so repeat alerts fire
            try:
                from dashboard import _active_errors, _active_errors_lk
                with _active_errors_lk:
                    _active_errors[self.sid] = {
                        "count"  : 1,
                        "leg_ids": {ls.leg_id for ls in fail_legs},
                    }
            except Exception:
                pass  # dashboard not imported (test mode) — skip

        # ── end of entry block ───────────────────────────────────────
        # (if _restored_from_disk was True, execution joins here directly)

        zero_ct = {}

        # Monitor loop — wrapped in try/except so any unhandled bug
        # logs clearly and attempts to exit positions rather than
        # silently dying and leaving open positions unprotected.
        # Safe defaults for protect profit — prevents crash if config
        # reload triggers before monitor loop initializes these variables.
        # Each strategy has its own independent copy of these variables.
        pp_mode     = ""
        lock_reach  = 0.0
        lock_at     = 0.0
        trail_reach = 0.0
        trail_by    = 0.0
        while not self.stopped:
          try:
            if not self.s.get("enabled", True):
                self._exit_all("Dashboard disable")
                self.status = "DISABLED"; self.stopped = True; break

            # Define now_s early so config-changed block can use it
            now_s = datetime.now().strftime("%H:%M:%S")

            # ── Config-changed: reset stale state ─────────────
            if self._config_changed:
                self._config_changed = False
                logic     = self.s.get("logic", {})
                start_t   = logic.get("startTime", start_t)
                end_t     = logic.get("endTime",   end_t)
                mtm_sl    = _scaled_mtm("mtmSL",     "mtmSLType")
                mtm_tgt   = _scaled_mtm("mtmTarget", "mtmTType")
                # RB state: only reset if range window hasn't closed yet.
                # If window already done and range captured → PRESERVE it.
                # Resetting after window closes wipes the captured range
                # and breakout can never trigger (window already passed).
                _rb_cfg_check  = (self.s.get("logic") or {}).get("rangeBreak") or {}
                _rb_until_chk  = _rb_cfg_check.get("windowEnd","09:30:00")
                _window_passed = now_s > _rb_until_chk
                if not _window_passed:
                    # Window not yet closed — safe to reset everything
                    _rb_high       = 0.0
                    _rb_low        = float('inf')
                    _rb_window_done = False
                    _rb_triggered  = False
                    _rb_first_done = False
                    self.log.info(f"{self.name}: config reloaded; rangeBreak state reset.")
                else:
                    # Window already closed — preserve captured range
                    # Only reset if not yet triggered (allow re-trigger on new config)
                    self.log.info(f"{self.name}: config reloaded; rangeBreak range preserved "
                                  f"(H={_rb_high:.2f} L={_rb_low:.2f}).")

                # ── Live parameter update on open legs ──────────
                # When user changes SL%, Target%, TSL on a running strategy,
                # recompute sl_price/tp_price on all currently OPEN legs
                # so the new values take effect immediately without re-entry.
                updated_legs = []
                for ls in self.leg_states:
                    if ls.status != "OPEN": continue
                    leg_cfg = next((l for l in self.s.get("legs",[]) if l["id"]==ls.leg_id), None)
                    if not leg_cfg: continue
                    new_sl = self._compute_sl_price(leg_cfg, ls.avg_entry)
                    new_tp = self._compute_tp_price(leg_cfg, ls.avg_entry)
                    new_qty = self._qty(leg_cfg)
                    changes = []
                    if new_sl != ls.sl_price or new_tp != ls.tp_price:
                        ls.sl_price = new_sl
                        ls.tp_price = new_tp
                        ls.tsl_config = leg_cfg.get("tslConfig")
                        changes.append(f"SL→{new_sl:.1f} TP→{new_tp:.1f}")
                    if new_qty != ls.qty:
                        ls.qty = new_qty
                        changes.append(f"qty→{new_qty}")
                    if changes:
                        updated_legs.append(f"{ls.opt_type} {' '.join(changes)}")
                if updated_legs:
                    self.log.info(f"{self.name}: live param update: {', '.join(updated_legs)}")

                # ── Refresh protect profit settings on config change ──
                pp_new        = self.s.get("protect", {})
                pp_mode_new   = str(pp_new.get("mode", "")).strip().lower()
                lock_reach_new  = _scaled_pp(float(pp_new.get("lockReach",  0) or 0))
                lock_at_new     = _scaled_pp(float(pp_new.get("lockAt",     0) or 0))
                trail_reach_new = _scaled_pp(float(pp_new.get("trailReach", 0) or 0))
                trail_by_new    = _scaled_pp(float(pp_new.get("trailBy",    0) or 0))
                # Only reset peak if protect mode changed — preserve peak otherwise
                if pp_mode_new != pp_mode or lock_reach_new != lock_reach:
                    self._mtm_peak = max(0.0, self.compute_total_pnl())
                # Only reset peak if protect mode changed — preserve peak otherwise
                if pp_mode_new != pp_mode or lock_reach_new != lock_reach:
                    self._mtm_peak = max(0.0, self.compute_total_pnl())
                self.log.info(f"{self.name}: config reloaded; rangeBreak state reset.")
                self.notify.telegram(
                    f"[CONFIG RELOAD] | {self.name}\n\n"
                    f"Strategy edited via dashboard.\n"
                    f"New SL/Target/MTM settings applied to open legs immediately.\n"
                    f"MTM SL: Rs {mtm_sl:.0f} | MTM Target: Rs {mtm_tgt:.0f}"
                    + (f"\nLegs updated: {', '.join(updated_legs)}" if updated_legs else ""))

            now_s = datetime.now().strftime("%H:%M:%S")

            # Market hours guard: skip monitoring when exchange is closed
            # NSE/BSE strategies: active only 09:15-15:30
            # MCX strategies: active 09:00-23:30
            if _is_mcx_strategy:
                _mkt_open = MCX_OPEN <= now_s <= MCX_CLOSE
            else:
                _mkt_open = NSE_OPEN <= now_s <= NSE_CLOSE
            if not _mkt_open and now_s < end_t:
                # Market closed but strategy end time not yet reached
                # (handles pre-market waiting and post-market inactivity)
                time.sleep(10)  # sleep longer when market is closed
                continue

            if now_s >= end_t:
                total_pnl = self.compute_total_pnl()
                trade_type = logic.get("tradeType", "Intraday")
                is_positional = (trade_type == "Positional")
                btst_cfg    = (logic.get("btst") or {}) if is_positional else {}
                hold_expiry = bool(btst_cfg.get("holdTillExpiry", False) or btst_cfg.get("holdExpiry", False))
                is_btst     = (not hold_expiry) and bool(btst_cfg.get("type"))
                nd_end_t    = btst_cfg.get("nextDayEndTime") or end_t
                chk_after   = btst_cfg.get("checkAfter", "09:16:00")

                if is_positional:
                    open_legs = [ls for ls in self.leg_states if ls.status == "OPEN"]
                    if open_legs:
                        self.status = "HOLDING"
                        mode = "PAPER" if self.dry_run else "LIVE"
                        if hold_expiry:
                            carry_msg = "Hold Till Expiry: position carries until expiry date."
                        elif is_btst:
                            carry_msg = f"BTST/STBT: Square off tomorrow at {nd_end_t}"
                        else:
                            carry_msg = f"Monitoring resumes at {start_t} tomorrow."
                        # Send HOLDING telegram ONCE only
                        if not getattr(self, "_holding_notified", False):
                            self.notify.telegram(
                                f"[HOLDING] | {self.name}  [{mode}]\n\n"
                                f"End time reached. Carrying {len(open_legs)} open position(s) overnight.\n"
                                f"{carry_msg}\n"
                                f"MTM: Rs {total_pnl:.0f}")
                            self._holding_notified = True
                        self._resume_notified = False
                        self._save_positional_state()
                        _entry_date = date.today()
                        while not self.stopped:
                            time.sleep(60)
                            now2 = datetime.now().strftime("%H:%M:%S")
                            today_days = self.s.get("days", [True]*5+[False,False])
                            today_idx  = date.today().weekday()
                            is_new_day = (date.today() > _entry_date)
                            if hold_expiry:
                                if is_new_day and now2 >= start_t:
                                    self.status = "RUNNING"
                                    self._holding_notified = False
                                    if not getattr(self, "_resume_notified", False):
                                        self.notify.telegram(
                                            f"[RESUME] | {self.name}  [{mode}]\n\n"
                                            f"Resuming monitoring of {len(open_legs)} position(s).\n"
                                            f"Hold Till Expiry active — no auto square off.\n"
                                            f"MTM: Rs {self.compute_total_pnl():.0f}")
                                        self._resume_notified = True
                                    break
                            elif is_btst:
                                if today_days[today_idx] if today_idx < len(today_days) else True:
                                    _resume_t = chk_after if is_new_day else start_t
                                    if is_new_day:
                                        end_t = nd_end_t
                                    if now2 >= _resume_t:
                                        self.status = "RUNNING"
                                        self._holding_notified = False
                                        if not getattr(self, "_resume_notified", False):
                                            self.notify.telegram(
                                                f"[RESUME] | {self.name}  [{mode}]\n\n"
                                                f"Resuming monitoring of {len(open_legs)} position(s).\n"
                                                f"Next day exit at: {end_t}\n"
                                                f"MTM: Rs {self.compute_total_pnl():.0f}")
                                            self._resume_notified = True
                                        break
                            else:
                                if today_days[today_idx] if today_idx < len(today_days) else True:
                                    if is_new_day and now2 >= start_t:
                                        self.status = "RUNNING"
                                        self._holding_notified = False
                                        if not getattr(self, "_resume_notified", False):
                                            self.notify.telegram(
                                                f"[RESUME] | {self.name}  [{mode}]\n\n"
                                                f"Resuming monitoring of {len(open_legs)} position(s).\n"
                                                f"MTM: Rs {self.compute_total_pnl():.0f}")
                                            self._resume_notified = True
                                        break
                        self.notify.telegram(
                            f"[SQUAREOFF] | {self.name}  [Positional/{mode if 'mode' in dir() else 'PAPER'}]\n\n"
                            f"End time reached, no open positions.\nMTM: Rs {total_pnl:.0f}")
                        break
                else:
                    # Intraday: exit everything at endTime
                    self._exit_all("Square off time")
                    self.status = "CLOSED"; self.stopped = True
                    self._closed_mtm = round(self.compute_total_pnl(), 2)
                    mode = "PAPER" if self.dry_run else "LIVE"
                    self.notify.log("SQUAREOFF", self.name,
                        f"End time {end_t} | MTM: Rs {total_pnl:.0f}")
                    self.notify.telegram(
                        f"[SQUAREOFF] | {self.name}  [{mode}]\n\n"
                        f"Reason   : Square off time\n"
                        f"MTM      : Rs {total_pnl:.0f}")
                    break

            # Process retry/disable from dashboard
            self._process_action_queue()

            # ── MTM SL / Target + Protect Profit — ALWAYS runs, feed or no feed ──
            total_pnl = self.compute_total_pnl()
            if not hasattr(self, "_mtm_peak"): self._mtm_peak = 0.0
            if total_pnl > self._mtm_peak: self._mtm_peak = total_pnl

            effective_floor = -mtm_sl if mtm_sl > 0 else None
            pp = self.s.get("protect", {})
            pp_mode     = str(pp.get("mode", "")).strip().lower()
            lock_reach  = _scaled_pp(float(pp.get("lockReach",  0) or 0))
            lock_at     = _scaled_pp(float(pp.get("lockAt",     0) or 0))
            trail_reach = _scaled_pp(float(pp.get("trailReach", 0) or 0))
            trail_by    = _scaled_pp(float(pp.get("trailBy",    0) or 0))
            if pp_mode in ("lock","lock_trail","lock minimum profit","lock and trail") and lock_reach > 0:
                if self._mtm_peak >= lock_reach:
                    if effective_floor is None or lock_at > effective_floor:
                        effective_floor = lock_at
            if pp_mode in ("trail","lock_trail","trail profit","lock and trail") and trail_reach > 0 and trail_by > 0:
                if self._mtm_peak >= trail_reach:
                    steps = int((self._mtm_peak - trail_reach) / trail_by)
                    trail_floor = trail_reach - trail_by + (steps * trail_by)
                    if effective_floor is None or trail_floor > effective_floor:
                        effective_floor = trail_floor

            # ── MTM SL breach warning (one-shot) ──────────────────────
            # Fires ONLY if: MTM SL is set, total_pnl has breached it,
            # AND the strategy is still RUNNING (exit did not happen yet).
            # Amount-based only — no % logic.
            if (mtm_sl > 0
                    and total_pnl < -mtm_sl
                    and self.status == "RUNNING"
                    and not getattr(self, "_mtm_sl_breach_warned", False)):
                self._mtm_sl_breach_warned = True
                mode = "PAPER" if self.dry_run else "LIVE"
                self.notify.telegram(
                    f"[MTM SL BREACH] | {self.name}  [{mode}]\n\n"
                    f"⚠️ MTM SL breached due to sudden market movement.\n"
                    f"Bot is actively monitoring and will auto-exit at the next available price.\n"
                    f"Please monitor your positions.\n\n"
                    f"MTM SL Set : Rs -{mtm_sl:.0f}\n"
                    f"Current MTM: Rs {total_pnl:.0f}")

            if effective_floor is not None and total_pnl <= effective_floor:
                reason_tag = "MTM SL" if effective_floor < 0 else "PROTECT EXIT"
                reason     = "Hard MTM SL hit" if effective_floor < 0 else "Protect-Profit floor hit"
                self._mtm_sl_breach_warned = False  # reset on successful exit
                self._exit_all(reason)
                self.status = "CLOSED"; self.stopped = True
                mode = "PAPER" if self.dry_run else "LIVE"
                legs_info = "\n".join(f"{ls.opt_type}: {fmt_sym(ls.symbol)}" for ls in self.leg_states if ls.symbol)
                self.notify.log(reason_tag, self.name, f"MTM: Rs {total_pnl:.0f} | Floor: Rs {effective_floor:.0f} | EXIT ALL")
                self.notify.telegram(f"[{reason_tag}] | {self.name}  [{mode}]\n\n{legs_info}\nMTM: Rs {total_pnl:.0f}\nFloor: Rs {effective_floor:.0f}\nACTION: EXIT ALL")
                break

            if mtm_tgt > 0 and total_pnl >= mtm_tgt:
                self._exit_all("MTM Target hit")
                self.status = "CLOSED"; self.stopped = True
                self.notify.log("MTM TARGET", self.name, f"MTM: Rs {total_pnl:.0f} | Target: Rs {mtm_tgt:.0f}")
                self.notify.telegram(f"[MTM TARGET] | {self.name}\nTarget: Rs {mtm_tgt:.0f}\nP&L: Rs {total_pnl:.0f}")
                break

            # ── Feed health gate — AFTER MTM SL check ────────────
            # Skip rangeBreak / leg SL / entry when feed stalled.
            # MTM SL already enforced above — never skipped.
            if hasattr(self.broker, "is_feed_healthy") and not self.broker.is_feed_healthy():
                time.sleep(2)
                continue

            # ── Range Breakout: track high/low during window, trigger after ──
            logic_rb = self.s.get("logic") or {}
            if ("rangeBreak" in (logic_rb.get("flags") or [])) and not _rb_triggered:
                rb_cfg        = logic_rb.get("rangeBreak") or {}
                rb_from       = rb_cfg.get("windowStart", "09:15:00")
                rb_until      = rb_cfg.get("windowEnd",   "09:30:00")
                rb_entry_at   = rb_cfg.get("entryAt",  "High")
                rb_first_only = rb_cfg.get("firstOnly", False)
                rb_price      = self._rb_get_price(rb_cfg)

                if rb_price > 0:
                    if rb_from <= now_s <= rb_until:
                        # ── PHASE 1: recording window — track live H/L ──
                        if rb_price > _rb_high: _rb_high = rb_price
                        if rb_price < _rb_low:  _rb_low  = rb_price

                    elif now_s > rb_until:
                        # ── PHASE 2: after window — watch for breakout ──
                        # Send range notification once when window first closes
                        if _rb_high > 0 and not _rb_window_done:
                            _rb_window_done = True
                            rb_range = round(_rb_high - _rb_low, 2)
                            mode_rb  = "PAPER" if self.dry_run else "LIVE"
                            msg = "[RB RANGE] | " + self.name + "  [" + mode_rb + "]\n\n"
                            msg += "Range High : Rs " + str(round(_rb_high, 2)) + "\n"
                            msg += "Range Low  : Rs " + str(round(_rb_low,  2)) + "\n"
                            msg += "Range Pts  : " + str(rb_range) + " pts\n"
                            msg += "Entry at   : " + rb_entry_at + "\n"
                            msg += "Watching for breakout until " + end_t + "..."
                            self.notify.telegram(msg)
                            self.log.info("RB window closed H=%.2f L=%.2f range=%.2f",
                                          _rb_high, _rb_low, rb_range)

                        # Monitor breakout continuously until endTime
                        # Strategy does NOT close — it keeps watching
                        if _rb_window_done or _rb_high > 0:
                            breakout = False; trigger_price = 0.0
                            if rb_entry_at == "High" and rb_price >= _rb_high:
                                breakout = True; trigger_price = _rb_high
                            elif rb_entry_at == "Low" and rb_price <= _rb_low:
                                breakout = True; trigger_price = _rb_low
                            if breakout:
                                _rb_triggered = True
                                mode_rb  = "PAPER" if self.dry_run else "LIVE"
                                rb_range = round(_rb_high - _rb_low, 2)
                                msg2 = "[RB BREAKOUT] | " + self.name + "  [" + mode_rb + "]\n\n"
                                msg2 += "Breakout at   : Rs " + str(round(rb_price, 2)) + "\n"
                                msg2 += "Trigger level : Rs " + str(round(trigger_price, 2)) + " (" + rb_entry_at + ")\n"
                                msg2 += "ORB Range     : " + str(rb_range) + " pts\n"
                                msg2 += "Entering legs now..."
                                self.notify.telegram(msg2)
                                new_rb = []
                                with ThreadPoolExecutor(max_workers=min(len(self.s["legs"]),4)) as ex_rb:
                                    frbs = {ex_rb.submit(self._enter_leg, leg, option_chain): leg
                                            for leg in self.s["legs"]}
                                    for frb in as_completed(frbs):
                                        l_rb = frb.result()
                                        if l_rb:
                                            new_rb.append(l_rb)
                                            if rb_first_only and not l_rb.failed and not _rb_first_done:
                                                _rb_first_done = True; break
                                if new_rb:
                                    self.leg_states = new_rb
                                    ok_rb = [l for l in new_rb if not l.failed]
                                    if ok_rb:
                                        lines_rb = ", ".join(
                                            fmt_sym(l.symbol)+" @ Rs "+str(round(l.entry_price,0))
                                            for l in ok_rb)
                                        self.notify.telegram(
                                            "[RB ENTRY] | "+self.name+"  ["+mode_rb+"]\n\n"+lines_rb)
                                else:
                                    self.notify.telegram(
                                        "[ERROR] | "+self.name+"\nRB entry failed.")

            # ── Check if all legs failed — pause and wait for retry
            active_legs = [ls for ls in self.leg_states
                           if not ls.disabled and not ls.failed]
            all_failed = len(self.leg_states) > 0 and len(active_legs) == 0
            if all_failed:
                # All legs failed — stay in ERROR state, wait for user retry
                # Do not exit — dashboard shows ERROR badge with Retry button
                time.sleep(2)
                continue

            # ── Per-leg monitoring
            for ls in list(self.leg_states):
                if ls.disabled or ls.failed: continue
                if ls.status == "CLOSED" and not ls.waiting_reentry: continue

                ltp = ls.live_ltp()
                ot  = ls.opt_type

                if ltp <= 0:
                    zero_ct[ot] = zero_ct.get(ot,0) + 1
                    if zero_ct[ot] == 10:
                        self.notify.telegram(f"[ERROR] | {self.name}\n{ot} LTP=0 for 10 ticks. Feed issue?")
                    continue
                zero_ct[ot] = 0

                # ── Wait & Trade: check trigger condition ─────────────
                if ls.status == "PENDING" and ls.wnt_active and not ls.wnt_triggered:
                    wnt_cfg = next(
                        (l.get("wntConfig",{}) for l in self.s["legs"] if l["id"]==ls.leg_id),
                        {})
                    wnt_mode = str(wnt_cfg.get("mode",""))
                    triggered = False
                    if ls.wnt_entry_price > 0:
                        is_ul_mon   = "UL" in wnt_mode.upper()
                        is_up_mon   = "\u2191" in wnt_mode or "Up" in wnt_mode
                        if is_ul_mon:
                            info_m = self._instr_info()
                            ult_m  = info_m.get("index_token","")
                            cmp_p  = price_store.get(ult_m) if ult_m else 0.0
                            # MCX: no index token — get FUT price same way as entry
                            if cmp_p <= 0 and info_m.get("is_mcx"):
                                cmp_p = self._rb_get_price({"priceOf": "Underlying"})
                            if cmp_p <= 0:
                                cmp_p = ltp  # last resort: option LTP
                        else:
                            cmp_p = ltp
                        if is_up_mon:
                            triggered = cmp_p >= ls.wnt_entry_price
                        else:
                            triggered = cmp_p <= ls.wnt_entry_price

                    if triggered:
                        # Place the actual entry order now
                        leg_cfg = next((l for l in self.s["legs"] if l["id"]==ls.leg_id), None)
                        if leg_cfg:
                            # Temporarily remove wntConfig to avoid recursion
                            leg_no_wnt = dict(leg_cfg); leg_no_wnt.pop("wntConfig",None)
                            new_ls = self._enter_leg(leg_no_wnt, option_chain)
                            if new_ls and not new_ls.failed:
                                new_ls.wnt_triggered = True
                                new_ls.wnt_ref_price  = ls.wnt_ref_price
                                new_ls.wnt_entry_price= ls.wnt_entry_price
                                pidx = next((i for i,x in enumerate(self.leg_states)
                                             if x.leg_id==ls.leg_id), None)
                                if pidx is not None:
                                    self.leg_states[pidx] = new_ls
                                mode = "PAPER" if self.dry_run else "LIVE"
                                self.notify.telegram(
                                    f"[W&T ENTRY] | {self.name}  [{mode}]\n\n"
                                    f"W&T triggered\n"
                                    f"Strike   : {fmt_sym(new_ls.symbol)}\n"
                                    f"Ref Price: Rs {ls.wnt_ref_price:.0f}\n"
                                    f"Entry At : Rs {ls.wnt_entry_price:.0f}\n"
                                    f"Fill     : Rs {new_ls.entry_price:.0f}")
                    continue  # PENDING legs skip SL/TP checks

                if ls.status == "OPEN":
                    # Update TSL
                    self._update_tsl(ls)

                    # Check Protect Profit
                    self._check_protect_profit(ls)

                    # Check SL
                    if self._should_exit_sl(ls):
                        fill = self._exit_leg(ls, f"{ot} SL")
                        mode  = "PAPER" if self.dry_run else "LIVE"
                        mtm   = self.compute_total_pnl()
                        instr = self.s.get("idx","NIFTY")
                        self.notify.telegram(
                            f"[LEG SL] | {self.name}  [{mode}]\n\n"
                            f"{ot} SL HIT\n"
                            f"Strike   : {fmt_sym(ls.symbol)}\n"
                            f"Entry    : Rs {ls.avg_entry:.0f}\n"
                            f"Exit     : Rs {fill:.0f}\n\n"
                            f"MTM      : Rs {mtm:.0f}")
                        is_opt = ot in ("CE","PE")
                        self.notify.log("LEG SL", self.name,
                            f"Script: {instr} | {'Options' if is_opt else 'Futures'} | "
                            f"Strike: {fmt_sym(ls.symbol)} | "
                            f"Type: {ot} | Txn: BUY (exit) | "
                            f"Cond: SL EXIT | "
                            f"Time: {ls.exit_time or datetime.now().strftime('%H:%M:%S')} | "
                            f"Qty: {ls.qty} | Price: Rs {fill:.2f} | "
                            f"Entry was: Rs {ls.avg_entry:.2f} | MTM: Rs {mtm:.0f}")
                        self._move_sl_to_cost(ls)
                        # Square off mode
                        if sq_mode == "all":
                            self._exit_all(f"SL hit on {ot} -- all legs exit")
                            if re_execute_count < re_execute_max:
                                re_execute_count += 1
                                # Reset sticky-side: fresh group, any side can lock
                                self._sl_cost_locked_side = None
                                self.notify.telegram(
                                    f"[RE-EXECUTE] | {self.name}\n"                                    f"All legs exited (SL). Re-executing #{re_execute_count}/{re_execute_max}...\n"                                    f"New strikes will be selected now.")
                                # Fresh entry -- select new strikes immediately (LTP basis)
                                new_results = []
                                with ThreadPoolExecutor(max_workers=min(len(self.s["legs"]),4)) as ex:
                                    futs2 = {ex.submit(self._enter_leg, leg, option_chain): leg
                                             for leg in self.s["legs"]}
                                    for fut2 in as_completed(futs2):
                                        ls2 = fut2.result()
                                        if ls2: new_results.append(ls2)
                                if new_results:
                                    self.leg_states = new_results
                                    ok2 = [l for l in new_results if not l.failed]
                                    if ok2:
                                        lines2 = "\n".join(
                                            f"{l.opt_type} {l.action}: {fmt_sym(l.symbol)} @ Rs {l.entry_price:.0f}"
                                            for l in ok2)
                                        self.notify.telegram(
                                            f"[RE-EXECUTE ENTRY] | {self.name}\n{lines2}")
                                    break  # break inner for-loop, continue while-loop
                                else:
                                    self.notify.telegram(f"[ERROR] | {self.name}\nRe-execute failed -- no fills.")
                                    self.status = "CLOSED"; self.stopped = True; break
                            else:
                                self.status = "CLOSED"; self.stopped = True; break
                        # Re-Execute: immediate re-run of entry logic (may pick new strike).
                        # Distinct from Re-Entry/Re-Cost which keep same symbol & wait for price.
                        # Method LTP: re-execute now. Method Candle Close: wait until end of minute.
                        rentry_mode = ls.reentry_type
                        if rentry_mode == "Re-Execute" and ls.reentry_count < ls.max_reentry:
                            adv_re = self.s.get("advanced", {})
                            re_method = str(adv_re.get("reExecuteMethod", "LTP")).strip()
                            if re_method == "Candle Close":
                                # Defer: mark waiting; the waiting_reentry path with is_reexec
                                # will handle candle-close re-execute trigger
                                ls.waiting_reentry = True
                                ls.reentry_target  = ls.entry_price  # retrigger condition
                                continue
                            # LTP method: re-execute now
                            leg = next((l for l in self.s["legs"] if l["id"]==ls.leg_id), None)
                            if leg:
                                new_ls = self._enter_leg(leg, option_chain)
                                if new_ls and not new_ls.failed:
                                    new_ls.reentry_count     = ls.reentry_count + 1
                                    new_ls.max_reentry       = ls.max_reentry
                                    new_ls.first_entry_price = ls.first_entry_price
                                    new_ls.reentry_type      = ls.reentry_type
                                    new_ls.realised_pnl      = 0.0  # each leg P&L is independent
                                    new_ls.reentry_target    = new_ls.entry_price
                                    pidx = next((i for i,x in enumerate(self.leg_states)
                                                 if x.leg_id==ls.leg_id), None)
                                    if pidx is not None:
                                        self.leg_states[pidx] = new_ls
                                    self.notify.telegram(
                                        f"[RE-EXECUTE] | {self.name}\n\n"
                                        f"{ot} re-executed (#{new_ls.reentry_count}/{ls.max_reentry})\n"
                                        f"New strike: {fmt_sym(new_ls.symbol)}\n"
                                        f"Entry: Rs {new_ls.entry_price:.2f}")
                                else:
                                    ls.failed = True
                                    self.notify.telegram(
                                        f"[ERROR] | {self.name}\n{ot} Re-Execute failed.")
                            continue

                        # Re-entry / Re-cost: set waiting if re-entries remain
                        if ls.reentry_count < ls.max_reentry:
                            ls.waiting_reentry = True
                            ls.reentry_target  = ls.entry_price
                        else:
                            # Re-entries exhausted -- this leg is fully done
                            # Check if ALL legs are now done → close strategy
                            all_done = all(
                                l.status == "CLOSED" and not l.waiting_reentry
                                for l in self.leg_states
                                if not l.disabled and not l.failed
                            )
                            if all_done:
                                self.status  = "CLOSED"
                                self.stopped = True
                        continue

                    # Check TP
                    if self._should_exit_tp(ls):
                        fill = self._exit_leg(ls, f"{ot} TP")
                        mode = "PAPER" if self.dry_run else "LIVE"
                        mtm  = self.compute_total_pnl()
                        self.notify.telegram(
                            f"[LEG TP] | {self.name}  [{mode}]\n\n"
                            f"{ot} TARGET HIT\n"
                            f"Strike   : {fmt_sym(ls.symbol)}\n"
                            f"Entry    : Rs {ls.avg_entry:.0f}\n"
                            f"Exit     : Rs {fill:.0f}\n\n"
                            f"MTM      : Rs {mtm:.0f}")
                        if sq_mode == "all":
                            self._exit_all(f"TP hit on {ot} -- all legs exit")
                            if re_execute_count < re_execute_max:
                                re_execute_count += 1
                                # Reset sticky-side: fresh group
                                self._sl_cost_locked_side = None
                                self.notify.telegram(
                                    f"[RE-EXECUTE] | {self.name}\n"                                    f"All legs exited (TP). Re-executing #{re_execute_count}/{re_execute_max}...")
                                new_results = []
                                with ThreadPoolExecutor(max_workers=min(len(self.s["legs"]),4)) as ex:
                                    futs2 = {ex.submit(self._enter_leg, leg, option_chain): leg
                                             for leg in self.s["legs"]}
                                    for fut2 in as_completed(futs2):
                                        ls2 = fut2.result()
                                        if ls2: new_results.append(ls2)
                                if new_results:
                                    self.leg_states = new_results
                                    ok2 = [l for l in new_results if not l.failed]
                                    if ok2:
                                        lines2 = "\n".join(
                                            f"{l.opt_type} {l.action}: {fmt_sym(l.symbol)} @ Rs {l.entry_price:.0f}"
                                            for l in ok2)
                                        self.notify.telegram(
                                            f"[RE-EXECUTE ENTRY] | {self.name}\n{lines2}")
                                    break
                                else:
                                    self.notify.telegram(f"[ERROR] | {self.name}\nRe-execute failed.")
                                    self.status = "CLOSED"; self.stopped = True; break
                            else:
                                self.status = "CLOSED"; self.stopped = True; break
                        else:
                            # sq_mode=one: check if all legs done after TP
                            all_done = all(
                                l.status == "CLOSED" and not l.waiting_reentry
                                for l in self.leg_states
                                if not l.disabled and not l.failed
                            )
                            if all_done:
                                self.status  = "CLOSED"
                                self.stopped = True
                        continue

                elif ls.waiting_reentry:
                    # Determine if condition is met based on reentry_type
                    is_recost   = ls.reentry_type == "Re-Cost"
                    is_reexec   = ls.reentry_type == "Re-Execute"
                    # Re-Cost:    trigger immediately when LTP <= target (LTP basis)
                    # Re-Entry:   trigger at end-of-minute (candle close) <= target
                    # Re-Execute: trigger handled in SL block above (immediate on exit
                    #             with optional candle-close per Re-Execute Method);
                    #             waiting_reentry path here is not used for Re-Execute.
                    now_dt = datetime.now()
                    # "Candle close" = LAST 2 seconds of the current minute (58, 59).
                    # Per Quantiply spec: check the price at minute close, fire re-entry
                    # if close price <= previous entry price.
                    is_candle_close = now_dt.second >= 58
                    condition_met = False
                    if is_recost:
                        condition_met = (ls.reentry_target > 0 and ltp <= ls.reentry_target)
                    elif is_reexec:
                        # Re-Execute waiting path: shouldn't typically reach here
                        # (handled in SL exit block). If it does, behave like Re-Cost.
                        condition_met = (ls.reentry_target > 0 and ltp <= ls.reentry_target)
                    else:
                        # Re-Entry: candle-close check
                        condition_met = (is_candle_close and ls.reentry_target > 0
                                         and ltp <= ls.reentry_target)

                    if condition_met:
                        leg = next((l for l in self.s["legs"] if l["id"]==ls.leg_id), None)
                        if leg:
                            try:
                                leg_override = dict(leg)
                                if ls.symbol and not is_reexec:
                                    leg_override["_locked_symbol"] = ls.symbol
                                    leg_override["_locked_token"]  = ls.token
                                new_ls = self._enter_leg(leg_override, option_chain)
                            except Exception as _re_err:
                                self.log.error(
                                    f"Re-entry failed for {ls.opt_type} — "
                                    f"strategy monitoring continues: {_re_err}")
                                new_ls = None
                            if new_ls and not new_ls.failed:
                                prev_count = ls.reentry_count
                                new_ls.reentry_count     = prev_count + 1
                                new_ls.max_reentry       = ls.max_reentry
                                new_ls.first_entry_price = ls.first_entry_price
                                new_ls.reentry_type      = ls.reentry_type
                                new_ls.realised_pnl      = 0.0  # each leg P&L is independent
                                # Re-Entry: next trigger = this entry price (candle close logic)
                                # Re-Cost:  next trigger = first_entry_price (unchanged)
                                if ls.reentry_type == 'Re-Cost':
                                    new_ls.reentry_target = ls.first_entry_price
                                else:
                                    new_ls.reentry_target = new_ls.entry_price
                                pidx = next((i for i,x in enumerate(self.leg_states) if x.leg_id==ls.leg_id), None)
                                if pidx is not None:
                                    self.leg_states[pidx] = new_ls
                                tag   = "RE-COST" if is_recost else ("RE-EXECUTE" if is_reexec else "RE-ENTRY")
                                mode  = "PAPER" if self.dry_run else "LIVE"
                                mtm   = self.compute_total_pnl()
                                instr = self.s.get("idx","NIFTY")
                                self.notify.telegram(
                                    f"[{tag}] | {self.name}  [{mode}]\n\n"
                                    f"Type     : {ot} {tag} {prev_count+1}/{ls.max_reentry}\n"
                                    f"Strike   : {fmt_sym(new_ls.symbol)}\n"
                                    f"Price    : Rs {new_ls.entry_price:.0f}\n"
                                    f"MTM      : Rs {mtm:.0f}")
                                is_opt = ot in ("CE","PE")
                                self.notify.log(tag, self.name,
                                    f"Script: {instr} | {'Options' if is_opt else 'Futures'} | "
                                    f"Strike: {fmt_sym(new_ls.symbol)} | "
                                    f"Type: {ot} | Txn: SELL (re-entry) | "
                                    f"Cond: {tag} #{prev_count+1}/{ls.max_reentry} | "
                                    f"Time: {new_ls.entry_time} | "
                                    f"Qty: {new_ls.qty} | Price: Rs {new_ls.entry_price:.2f} | "
                                    f"MTM: Rs {mtm:.0f}")
                            else:
                                ls.failed = True; ls.waiting_reentry = False
                                self.notify.telegram(
                                    f"[ERROR] | {self.name}\n{ot} re-entry failed. Use Retry on dashboard.")

            # ── Portfolio (basket) SL & Target — cross-strategy check ──
            # These are set via the Portfolio SL & Target panel in the dashboard.
            # If the combined MTM of ALL strategies crosses the basket limit,
            # THIS strategy exits. Every runner checks independently; the net
            # effect is all strategies exit when portfolio limit is breached.
            basket_sl  = float(self.config.get("basket_sl",  0) or 0)
            basket_tgt = float(self.config.get("basket_target", 0) or 0)
            if basket_sl > 0 or basket_tgt > 0:
                # Sum MTM across all running strategies
                portfolio_pnl = sum(
                    r.compute_total_pnl()
                    for r in self._get_sibling_runners()
                )
                # ── Portfolio SL breach warning (one-shot) ──────────
                # Fires if portfolio_pnl breached basket_sl but exit has not
                # happened yet. Amount-based only — no % logic.
                _pfired_key = "_portfolio_exit_fired"
                _pbreach_key = "_portfolio_breach_warned"
                if (basket_sl > 0
                        and portfolio_pnl < -basket_sl
                        and not self.config.get(_pfired_key, False)
                        and not self.config.get(_pbreach_key, False)):
                    self.config[_pbreach_key] = True
                    mode = "PAPER" if self.dry_run else "LIVE"
                    self.notify.telegram(
                        f"[PORTFOLIO SL BREACH] | {self.name}  [{mode}]\n\n"
                        f"⚠️ Portfolio SL breached due to sudden market movement.\n"
                        f"Bot is actively monitoring and will auto-exit at the next available price.\n"
                        f"Please monitor your positions.\n\n"
                        f"Portfolio SL Set : Rs -{basket_sl:.0f}\n"
                        f"Current MTM      : Rs {portfolio_pnl:.0f}")

                if basket_sl > 0 and portfolio_pnl <= -basket_sl:
                    self.config[_pbreach_key] = False  # reset on successful exit
                    self._exit_all("Portfolio SL hit")
                    self.status = "CLOSED"; self.stopped = True
                    mode = "PAPER" if self.dry_run else "LIVE"
                    self.notify.telegram(
                        f"[PORTFOLIO SL] | {self.name}  [{mode}]\n\n"
                        f"Portfolio MTM : Rs {portfolio_pnl:.0f}\n"
                        f"Portfolio SL  : Rs -{basket_sl:.0f}\n"
                        f"ACTION        : EXIT ALL legs in this strategy")
                    break
                if basket_tgt > 0 and portfolio_pnl >= basket_tgt:
                    self._exit_all("Portfolio Target hit")
                    self.status = "CLOSED"; self.stopped = True
                    mode = "PAPER" if self.dry_run else "LIVE"
                    self.notify.telegram(
                        f"[PORTFOLIO TARGET] | {self.name}  [{mode}]\n\n"
                        f"Portfolio MTM    : Rs {portfolio_pnl:.0f}\n"
                        f"Portfolio Target : Rs {basket_tgt:.0f}\n"
                        f"ACTION           : EXIT ALL legs in this strategy")
                    break

            if self.stopped: break
            time.sleep(1)

          except Exception as _loop_err:
            # Safety net: any unhandled exception in the monitor loop
            # logs the error and attempts emergency exit of all positions.
            # This prevents the thread from dying silently with open positions.
            self.log.error(
                f"[EMERGENCY] {self.name}: unhandled error in monitor loop — "
                f"attempting emergency exit. Error: {_loop_err}",
                exc_info=True)
            self.notify.telegram(
                f"[EMERGENCY EXIT] | {self.name}\n\n"
                f"Unhandled error in strategy monitor:\n{_loop_err}\n\n"
                f"Attempting to exit all open positions now.")
            try:
                self._exit_all("Emergency: monitor loop error")
            except Exception as _exit_err:
                self.log.error(f"Emergency exit also failed: {_exit_err}")
            self.status = "CLOSED"; self.stopped = True
            break
        total = sum(ls.realised_pnl for ls in self.leg_states)
        self._final_pnl = total  # store final P&L for dashboard display
        mode  = "PAPER" if self.dry_run else "LIVE"
        leg_sum_lines = []
        for ls in self.leg_states:
            re_str = f"Re: {ls.reentry_count}" if ls.reentry_count > 0 else "Re: 0"
            leg_sum_lines.append(
                f"{ls.opt_type}      : {fmt_sym(ls.symbol)}\n"
                f"  Entry: Rs {ls.entry_price:.0f}  P&L: Rs {ls.realised_pnl:.0f}  {re_str}"
            )
        leg_block = "\n".join(leg_sum_lines)
        mtm_sl_hit = total <= -(float(self.s.get("logic",{}).get("mtmSL",0)) or 999999)
        # SUMMARY telegram removed — not needed
        self.log.info(f"{self.name} done. Total P&L: Rs {total:.0f}")
        # Positional: clear saved state file since all positions are now closed
        if logic.get("tradeType","Intraday") == "Positional":
            self.clear_positional_state()


# ============================================================
#   NOTIFIER (Telegram + Email)
# ============================================================

class Notifier:
    def __init__(self, cfg: dict, log_fn=None):
        self.cfg       = cfg
        self.log_fn    = log_fn
        self._sent     = {}   # {msg_hash: (timestamp, count)}
        self._lock     = __import__('threading').Lock()
        # Callable that returns True if any strategy is actively running.
        # Set by Engine after runners are created. Default allows all alerts.
        self.has_active_strategies = lambda: True

    def log(self, tag: str, sname: str, body: str):
        """Write to dashboard activity log if log_fn is available."""
        if self.log_fn:
            try:
                self.log_fn(tag, sname, body)
            except Exception:
                pass

    def telegram(self, text: str, is_error: bool = False):
        tok = self.cfg.get("telegram_token","")
        cid = self.cfg.get("telegram_chat_id","")
        if not tok or "YOUR" in tok: return
        # Important-only filter
        if self.cfg.get("important_alerts_only", False):
            _important = (
                "[ERROR]","[WARN]","[EXIT FAILED]","[RETRY FAILED]",
                "[MARKET CLOSED]","[SQUAREOFF]","[MTM SL BREACH]","[LEG DISABLED]",
                "[LOGIN FAILED]","[START FAILED]","[ERROR RESOLVED]",
                "[BASKET SL]","Mode switched"
            )
            if not any(k in text for k in _important):
                return

        import time, hashlib

        # Auto-detect error messages by content
        _error_keywords = ("[ERROR]","[EXIT FAILED]","[EMERGENCY]","FAILED","failed",
                           "LTP=0","issue?","cannot","Feed issue","retry failed")
        if not is_error:
            is_error = any(k in text for k in _error_keywords)

        # Trading events — deduplicate within 5 seconds
        # Prevents duplicate alerts for same event fired twice
        # Different events have different price/time so hash is always unique
        import hashlib, time as _time
        _trading_events = ("[RE-ENTRY]","[RE-COST]","[RE-EXECUTE]","[RB ENTRY]",
                           "[RB BREAKOUT]","[ENTRY]","[EXIT]","[LEG SL]","[LEG TP]",
                           "[MTM SL]","[MTM TARGET]","[SQUAREOFF]","[HOLDING]","[RESUME]")
        if any(k in text for k in _trading_events):
            msg_hash = hashlib.md5(text.encode()).hexdigest()
            now = _time.time()
            with self._lock:
                last_ts, count = self._sent.get(msg_hash, (0, 0))
                if now - last_ts < 5:
                    return  # duplicate within 5 seconds — suppress
                self._sent[msg_hash] = (now, 1)
            try:
                requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                              json={"chat_id": cid, "text": text,
                                    "parse_mode": "HTML"}, timeout=10)
            except Exception as e:
                _log.warning(f"Telegram: {e}")
            return

        msg_hash = hashlib.md5(text.encode()).hexdigest()
        now = time.time()

        # Error messages: max 5 per day
        # Normal messages: max 1 per day
        max_count = 3      if is_error else 1
        cooldown  = 86400  # 24 hours for both — no repeat after limit

        with self._lock:
            entry = self._sent.get(msg_hash, (0, 0))
            last_ts, count = entry
            if now - last_ts > cooldown:
                count = 0
            if count >= max_count:
                _log.debug(f"Telegram suppressed ({'error limit 5' if is_error else 'already sent today'}): {text[:60]}")
                return
            self._sent[msg_hash] = (now, count + 1)
            self._sent = {k:v for k,v in self._sent.items() if now - v[0] < 86400}

        try:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": cid, "text": text,
                                "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            _log.warning(f"Telegram: {e}")

    def email(self, subject: str, body: str):
        cfg = self.cfg
        if not cfg.get("gmail_user") or not cfg.get("gmail_app_password"): return
        try:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(body)
            msg["Subject"] = f"[BlackBox Pro] {subject}"
            msg["From"]    = cfg["gmail_user"]
            msg["To"]      = cfg.get("gmail_to", cfg["gmail_user"])
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
                s.login(cfg["gmail_user"], cfg["gmail_app_password"])
                s.send_message(msg)
        except Exception as e:
            _log.warning(f"Email: {e}")


# ============================================================
#   MAIN ENGINE (coordinates everything)
# ============================================================

class Engine:
    """
    Top-level coordinator.
    - Manages broker connection
    - Manages live price feed
    - Starts/stops strategy runners
    """

    def __init__(self, config: dict, notifier: Notifier, dry_run: bool):
        self.config   = config
        self.notifier = notifier
        self.dry_run  = dry_run
        self.broker   = None
        self.running  = False
        self.runners  : dict[int, StrategyRunner] = {}  # strategy_id -> runner
        self._threads : dict[int, threading.Thread] = {}
        self._option_chains: dict[str, list] = {}  # instrument -> chain
        self._sub_tokens: list = []
        self.log      = logging.getLogger("Engine")

    def start(self) -> tuple[bool, str]:
        """Login, subscribe feed, mark ready. Returns (ok, message)."""
        broker_name = self.config.get("broker","angelone").lower()
        BrokerClass = BROKER_MAP.get(broker_name, AngelOneAdapter)
        self.broker = BrokerClass()

        bc = self.config.get("broker_creds",{})
        ok = self.broker.login(
            api_key      = bc.get("api_key",""),
            client_code  = bc.get("client_code",""),
            password     = bc.get("password",""),
            totp_key     = bc.get("totp_key",""),
        )

        if not ok:
            return False, "Broker login failed. Check credentials in Broker Setup."

        # Angel One — no kite object needed, WebSocket handles all prices

        # Angel One — tokens managed internally by adapter

        _default_index_tokens = []
        for _instr, _info in INSTRUMENTS.items():
            _itok = _info.get("index_token","")
            _iexch = _info.get("index_exch","")
            if _itok and _iexch:
                _default_index_tokens.append({
                    "instrument_token": _itok,
                    "exchange": _iexch,
                    "tradingsymbol": _instr,
                })
        # Skip early index-only subscription for AngelOne — full subscription
        # (index + options) happens together at line below after chain is built.
        # Creating two WebSocket connections kills the first one via 429.
        _log.info(f"[Engine] Index tokens ready: {len(_default_index_tokens)} — will subscribe with options.")
        # Wire notifier into broker so WS alerts can fire Telegram
        if hasattr(self.broker, 'set_notifier'):
            self.broker.set_notifier(self.notifier)
        self.config.pop("_portfolio_exit_fired", None)
        self.config.pop("_portfolio_breach_warned", None)
        self.running = True
        return True, "Engine started successfully."

    def load_strategies(self, strategies: list):
        """Load option chains and start strategy runner threads."""

        # ── Per-exchange holiday check ─────────────────────────────────
        # Fetch holiday list for each exchange separately.
        # NSE/BSE holiday does NOT block MCX strategies and vice versa.
        # e.g. May Day: NSE/BSE closed, MCX open → MCX strategies run normally
        today_str = date.today().isoformat()
        _holiday_exchanges = set()  # exchanges that are holiday today
        # Holiday check via Zerodha API is not available (kite.holidays() does not exist).
        # Will be implemented after testing what API returns on actual holiday.
        # For now _holiday_exchanges stays empty — all strategies run.

        # Filter strategies — skip those whose exchange is on holiday
        # MCX strategies → check MCX holiday only
        # NSE strategies → check NSE holiday
        # BSE strategies → check BSE holiday
        active_strategies = []
        skipped_strategies = []
        for s in strategies:
            instr    = s.get("idx","NIFTY")
            info     = INSTRUMENTS.get(instr, {})
            is_mcx   = info.get("is_mcx", False)
            s_exch   = "MCX" if is_mcx else ("BSE" if info.get("exchange","NSE")=="BFO" else "NSE")
            if s_exch in _holiday_exchanges:
                skipped_strategies.append((s.get("name","?"), s_exch))
            else:
                active_strategies.append(s)

        if skipped_strategies:
            skipped_names = ", ".join(f"{n} ({e})" for n,e in skipped_strategies)
            _log.warning(f"Holiday today — skipping strategies: {skipped_names}")
            self.notifier.telegram(
                f"🏖 Market Holiday — Partial\n\n"
                f"Date: {today_str}\n"
                f"Holiday exchanges: {', '.join(_holiday_exchanges)}\n\n"
                f"Skipped: {skipped_names}\n"
                f"Active strategies continue normally.")

        if not active_strategies:
            self.notifier.telegram(
                f"🏖 Market Holiday Today\n\n"
                f"Date: {today_str}\n"
                f"All exchanges closed: {', '.join(_holiday_exchanges)}")
            return

        # Replace strategies list with only active (non-holiday) ones
        strategies = active_strategies

        # Fetch chains for all unique instruments
        instruments = set(s.get("idx","NIFTY") for s in strategies)
        all_tokens  = []

        # Collect all expiry types needed per instrument.
        # Key insight: a single instrument can have legs with DIFFERENT expiries
        # (e.g. Leg1=Weekly, Leg2=Monthly). We must fetch a separate chain for
        # each unique expiry type so every leg gets the correct contract.
        instr_expiry_types = {}
        for s in strategies:
            instr = s.get("idx","NIFTY")
            for leg in s.get("legs",[]):
                et = str(leg.get("expiry","Weekly")).lower().replace(" ","_").replace("-","_")
                emap = {
                    "weekly":            "weekly",
                    "next_weekly":       "next_weekly",
                    "monthly":           "monthly",
                    "next_month":        "next_month",
                    "next_weekly_expiry":"next_weekly",
                    "next_month_expiry": "next_month",
                }
                et_mapped = emap.get(et, "weekly")
                info_check = INSTRUMENTS.get(instr, {})
                # MCX has no weekly contracts — map weekly/next_weekly to monthly
                if info_check.get("is_mcx"):
                    if et_mapped in ("weekly", "next_weekly"):
                        et_mapped = "monthly"
                    # next_month stays as-is — MCX has next_month contracts
                # NSE/BSE FUT legs: only monthly/next_month
                leg_type = leg.get("type","CE").upper()
                if leg_type == "FUT" and not info_check.get("is_mcx"):
                    if et_mapped in ("weekly", "next_weekly"):
                        et_mapped = "monthly"
                instr_expiry_types.setdefault(instr, set()).add(et_mapped)

        for instr in instruments:
            info   = INSTRUMENTS.get(instr, {})
            expiry_types = instr_expiry_types.get(instr, {"weekly"})

            # Fetch one chain per unique expiry type needed for this instrument.
            # This ensures Leg1=Weekly and Leg2=Monthly both get correct contracts.
            has_fut_leg = any(
                any(lg.get("type","CE").upper() == "FUT"
                    for lg in s.get("legs",[]))
                for s in strategies
                if s.get("idx","NIFTY") == instr
            )
            has_mcx_options = info.get("has_options", False)

            instr_chains = []   # combined chain rows from all expiry types
            instr_expiry_map = {}  # et -> exp_str (for logging)
            if not hasattr(self, '_instr_expiry_map'): self._instr_expiry_map = {}

            for et in sorted(expiry_types):   # sorted for deterministic logging
                exp_str = nearest_expiry_from_broker(self.broker, instr, et)

                if exp_str:
                    self.log.info(f"[EXPIRY] {instr} {et} -> {exp_str} (from broker API)")
                else:
                    exp_calc = nearest_expiry(instr, et)
                    exp_str  = expiry_fmt(exp_calc)
                    self.log.warning(
                        f"[EXPIRY] {instr} {et}: broker returned no expiries. "
                        f"Using calculated fallback: {exp_str}")

                instr_expiry_map[et] = exp_str
                if not hasattr(self, '_instr_expiry_map'): self._instr_expiry_map = {}
                if instr not in self._instr_expiry_map: self._instr_expiry_map[instr] = {}
                self._instr_expiry_map[instr][et] = exp_str

                # Fetch chain for this specific expiry
                # Universal rule:
                # - Has FUT leg → always fetch both FUT + options chain and combine
                #   This works for ALL instruments regardless of has_options flag
                #   (GOLDM, CRUDEOIL, GOLD, SILVER, NSE/BSE futures etc.)
                # - Options only → fetch options chain only
                if has_fut_leg or info.get("is_mcx"):
                    opt_chain = self.broker.get_option_chain(instr, exp_str)
                    # For MCX: futures use different expiry than options
                    _fut_exp = exp_str
                    if info.get("is_mcx"):
                        _all_fut_exps = [e for e in (self.broker.get_available_expiries(instr) or [])
                                         if self.broker.get_fut_chain(instr, e)]
                        if _all_fut_exps: _fut_exp = _all_fut_exps[0]
                    fut_chain = self.broker.get_fut_chain(instr, _fut_exp)
                    # Combine — deduplicate by instrument_token
                    seen_toks = set()
                    chain_et  = []
                    for c in opt_chain + fut_chain:
                        tok = str(c.get("instrument_token") or c.get("pSymbol") or "")
                        if tok and tok not in seen_toks:
                            seen_toks.add(tok)
                            chain_et.append(c)
                else:
                    chain_et = self.broker.get_option_chain(instr, exp_str)

                if chain_et:
                    instr_chains.extend(chain_et)
                    self.log.info(
                        f"Chain {instr} [{et}]: {len(chain_et)} contracts  expiry={exp_str}")
                elif not exp_str:
                    # next_month returned "" — contracts not yet listed by broker
                    self.log.warning(
                        f"Chain {instr} [{et}]: next-month contracts not yet available. "
                        f"Start again tomorrow morning after broker refreshes.")
                    self.notifier.telegram(
                        f"[MARKET NOT READY] | {instr}\n\n"
                        f"Next Month contracts not yet listed by Zerodha.\n"
                        f"This is normal on the last trading day of the current contract.\n"
                        f"Zerodha publishes next-month instruments after market close.\n"
                        f"Please start the strategy again tomorrow morning.")
                else:
                    self.log.warning(
                        f"Chain {instr} [{et}]: no contracts for expiry={exp_str}")

            # Deduplicate by instrument_token (same contract may appear across chains)
            seen_tokens = set()
            chain = []
            for row in instr_chains:
                tok = str(row.get("instrument_token") or row.get("pSymbol",""))
                if tok and tok not in seen_tokens:
                    seen_tokens.add(tok)
                    chain.append(row)

            if not chain:
                self.notifier.telegram(
                    f"[WARN] | {instr}\n"
                    f"No contracts found for any expiry.\n"
                    f"Check instrument symbol and broker connection.")

            self._option_chains[instr] = chain
            self.log.info(f"Chain {instr}: {len(chain)} contracts total "
                          f"across {len(expiry_types)} expiry type(s)")
            # ── MCX: filter to ATM ± 20 strikes for WS subscription ──
            # Full chain kept in _option_chains for strike resolution,
            # but only subscribe ATM±20 tokens to WebSocket to save memory.
            sub_chain = chain
            if info.get("is_mcx") and chain:
                _atm = get_atm_strike(instr, chain)
                # If ATM not yet available, get futures LTP via REST
                if _atm <= 0:
                    for _c in chain:
                        if str(_c.get("instrument_type","")).upper() == "FUT":
                            _ftok = str(_c.get("instrument_token",""))
                            _fsym = str(_c.get("tradingsymbol",""))
                            if _ftok and hasattr(self.broker,"get_rest_ltp"):
                                try:
                                    _fltp = self.broker.get_rest_ltp("MCX",_fsym,_ftok)
                                    if _fltp > 0:
                                        price_store.update(_ftok, _fltp)
                                        _atm = get_atm_strike(instr, chain)
                                        self.log.info(f"[MCX] {instr} REST futures LTP={_fltp:.0f} ATM={_atm:.0f}")
                                        break
                                except Exception as _re:
                                    self.log.warning(f"[MCX] REST LTP failed: {_re}")
                if _atm > 0:
                    _tick = info.get("tick", 1.0)
                    # Get sorted unique strikes
                    _strikes = sorted(set(
                        float(c.get("strike", 0) or 0)
                        for c in chain if c.get("strike")
                    ))
                    # Find ATM index and slice ±20
                    _atm_idx = min(range(len(_strikes)),
                                   key=lambda i: abs(_strikes[i] - _atm))
                    _lo_idx  = max(0, _atm_idx - 20)
                    _hi_idx  = min(len(_strikes) - 1, _atm_idx + 20)
                    _lo_strike = _strikes[_lo_idx]
                    _hi_strike = _strikes[_hi_idx]
                    sub_chain = [c for c in chain
                                 if c.get("instrument_type","").upper() == "FUT"
                                 or (_lo_strike <= float(c.get("strike",0) or 0) <= _hi_strike)]
                    self.log.info(f"Chain {instr}: MCX WS filter ATM={_atm:.0f} "
                                  f"strikes {_lo_strike:.0f}-{_hi_strike:.0f} "
                                  f"→ {len(sub_chain)}/{len(chain)} tokens subscribed")
            for item in sub_chain:
                # Zerodha uses "instrument_token"; fallback to pSymbol (Kotak)
                tok = str(item.get("instrument_token", item.get("pSymbol","")))
                if tok:
                    all_tokens.append({"instrument_token": tok,
                                       "exchange_segment": info.get("exchange","NFO")})
            # Also subscribe index token for ATM calculation
            idx_tok = info.get("index_token","")
            if idx_tok:
                all_tokens.append({"instrument_token": idx_tok,
                                   "exchange_segment": info.get("index_exch","nse_cm")})

        if all_tokens:
            # Combine index tokens + option tokens — subscribe all at once
            # This avoids the race condition where options are added while
            # WebSocket is still connecting (sendMessage=None error)
            index_toks = []
            seen_idx   = set()
            for instr_name in instruments:
                itm = INDEX_TOKEN_MAP.get(instr_name, {})
                tok = itm.get("index_token", "")
                if tok and tok not in seen_idx:
                    index_toks.append({"instrument_token": tok,
                                       "exchange_segment": itm.get("index_exch","NSE")})
                    seen_idx.add(tok)

            self.log.info(f"Index spot tokens to subscribe: "
                          f"{[t['instrument_token'] for t in index_toks]}")

            # All tokens in one list — index first, then options
            combined = index_toks + [t for t in all_tokens
                                     if t.get("instrument_token") not in seen_idx]

            # Reset dedup and subscribe everything in one call
            self.broker._sub_tokens = []
            self.broker.subscribe_feed(combined,
                                       lambda tok, ltp: price_store.update(tok, ltp))
            self.log.info(f"Subscribed {len(index_toks)} index + "
                          f"{len(combined)-len(index_toks)} option tokens "
                          f"({len(combined)} total).")
            self._sub_tokens = list(combined)
            # Wait for on_connect to fire before feed check
            # This guarantees tokens are subscribed before checking prices
            if hasattr(self.broker, 'wait_for_connection'):
                self.broker.wait_for_connection(timeout=30)
            # Wait until live prices arrive -- retry up to 45 seconds
            _waited = 0
            while _waited < 45:
                time.sleep(2)
                _waited += 2
                live_count = price_store.count()
                self.log.info(f"Feed check: {live_count}/{len(all_tokens)} tokens live after {_waited}s")
                # Need at least 10% of tokens OR 500 tokens to have prices
                if live_count >= max(1, min(500, len(all_tokens) // 10)):
                    break
            self.log.info(f"Feed ready: {price_store.count()} tokens with live prices")
            # ── Market-open resubscribe: flush Zerodha LVC stale cache ──
            # Zerodha sends last cached price before market opens (LVC).
            # Resubscribe at 09:00 (MCX open) and 09:15 (NSE/BSE open)
            # to flush stale prices and get fresh live ticks.
            from datetime import datetime as _dt
            import pytz as _pytz
            _IST = _pytz.timezone("Asia/Kolkata")
            def _resub_all():
                self.log.info("[Engine] Resubscribing all tokens for fresh live ticks.")
                self.broker._ws_connected.clear()
                self.broker._sub_tokens = []
                # Force fresh WebSocket on resubscribe — clears stale LVC cache
                try:
                    if self.broker._ws: self.broker._ws.close_connection()
                except: pass
                self.broker._ws = None
                self.broker.subscribe_feed(combined, lambda tok, ltp: price_store.update(tok, ltp))
                if hasattr(self.broker, "wait_for_connection"):
                    self.broker.wait_for_connection(timeout=30)
                self.log.info("[Engine] Resubscribe complete — live ticks flowing.")
            if not getattr(self, "_market_open_resub_done", False):
                _now_s = _dt.now(_IST).strftime("%H:%M:%S")
                if _now_s < "09:00:00":
                    self.log.info("[Engine] Pre-market start — waiting for 09:00 MCX open.")
                    while _dt.now(_IST).strftime("%H:%M:%S") < "09:00:00":
                        time.sleep(5)
                    _resub_all()
                    self.log.info("[Engine] Waiting for 09:15 NSE/BSE open.")
                    while _dt.now(_IST).strftime("%H:%M:%S") < "09:15:00":
                        time.sleep(5)
                    _resub_all()
                elif _now_s < "09:15:00":
                    self.log.info("[Engine] Started between 09:00-09:15 — waiting for 09:15 NSE/BSE open.")
                    while _dt.now(_IST).strftime("%H:%M:%S") < "09:15:00":
                        time.sleep(5)
                    _resub_all()
                else:
                    self.log.info("[Engine] Started after 09:15 — resubscribing immediately.")
                    _resub_all()
                self._market_open_resub_done = True
            # ── MCX: re-filter tokens to ATM±20 now that futures LTP is live ──
            # During chain build, ATM was 0 (no feed yet). Now we can filter properly.
            _mcx_unsub = []
            for instr_name, chain in self._option_chains.items():
                _instr_info2 = INSTRUMENTS.get(instr_name, {})
                if not _instr_info2.get("is_mcx"): continue
                _atm2 = get_atm_strike(instr_name, chain)
                if _atm2 <= 0: continue
                _strikes2 = sorted(set(
                    float(c.get("strike", 0) or 0)
                    for c in chain if c.get("strike")
                ))
                if not _strikes2: continue
                _atm_idx2 = min(range(len(_strikes2)),
                                key=lambda i: abs(_strikes2[i] - _atm2))
                _lo_idx2  = max(0, _atm_idx2 - 20)
                _hi_idx2  = min(len(_strikes2) - 1, _atm_idx2 + 20)
                _lo_strike2 = _strikes2[_lo_idx2]
                _hi_strike2 = _strikes2[_hi_idx2]
                # Find tokens outside ATM±20 range (exclude FUT)
                for c in chain:
                    if c.get("instrument_type","").upper() == "FUT": continue
                    sp = float(c.get("strike", 0) or 0)
                    if sp < _lo_strike2 or sp > _hi_strike2:
                        tok2 = str(c.get("instrument_token",""))
                        if tok2: _mcx_unsub.append(tok2)
                self.log.info(f"[MCX Filter] {instr_name}: ATM={_atm2:.0f} "
                              f"keeping strikes {_lo_strike2:.0f}-{_hi_strike2:.0f} "
                              f"unsubscribing {len(_mcx_unsub)} OTM tokens")
            if _mcx_unsub:
                try:
                    _mcx_unsub_int = [int(t) for t in _mcx_unsub if t]
                    if hasattr(self.broker, '_ticker') and self.broker._ticker:
                        self.broker._ticker.unsubscribe(_mcx_unsub_int)
                        self.log.info(f"[MCX Filter] Unsubscribed {len(_mcx_unsub_int)} far OTM tokens")
                    else:
                        self.log.warning("[MCX Filter] Ticker not available for unsubscribe")
                except Exception as _ue:
                    self.log.warning(f"[MCX Filter] Unsubscribe failed: {_ue}")

        om = OrderManager(self.broker, self.dry_run, self.config,
                          notifier=self.notifier)

        for s in strategies:
            if not s.get("enabled", True): continue
            sid   = s["id"]
            chain = self._option_chains.get(s.get("idx","NIFTY"), [])
            # Deep copy so each StrategyRunner has its own isolated config dict.
            # Without this, duplicate strategies or mutating self.s in one runner
            # affects all other runners sharing the same dict reference.
            import copy
            s_copy = copy.deepcopy(s)
            runner = StrategyRunner(s_copy, self.broker, om, self.notifier,
                                    self.dry_run,
                                    config=self.config,
                                    siblings=self.runners)
            self.runners[sid] = runner

            def _safe_run(r=runner, ch=chain):
                """Wrap runner.run() with crash recovery.
                If the strategy thread crashes for any reason:
                  1. Log the error
                  2. Exit all open positions immediately (safety net)
                  3. Send Telegram alert
                """
                try:
                    r.run(ch)
                except Exception as _exc:
                    import traceback
                    err_msg = traceback.format_exc()
                    r.log.error(f"STRATEGY THREAD CRASHED: {_exc}\n{err_msg}")
                    # Emergency exit: close any remaining open positions
                    try:
                        open_legs = [ls for ls in r.leg_states if ls.status == "OPEN"]
                        if open_legs:
                            r.log.error(f"EMERGENCY EXIT: {len(open_legs)} open legs after crash")
                            r._exit_all("Strategy crash — emergency exit")
                            r.notify.telegram(
                                f"🚨 [CRASH RECOVERY] | {r.name}\n\n"
                                f"Strategy thread crashed unexpectedly.\n"
                                f"Error: {str(_exc)[:200]}\n\n"
                                f"Emergency exit executed for {len(open_legs)} open leg(s).\n"
                                f"Please check positions and restart strategy.")
                        else:
                            r.notify.telegram(
                                f"⚠️ [STRATEGY ERROR] | {r.name}\n\n"
                                f"Strategy thread ended with error:\n{str(_exc)[:300]}\n"
                                f"No open positions at time of crash.")
                    except Exception as _ex2:
                        r.log.error(f"Emergency exit also failed: {_ex2}")
                    r.status = "CLOSED"
                    r.stopped = True

            t = threading.Thread(
                target=_safe_run,
                name=f"Strat_{sid}", daemon=True)
            self._threads[sid] = t
            t.start()
            self.log.info(f"Strategy started: {s['name']}")
        # Tell notifier how to check active runners — used for feed alerts
        def _any_active():
            try:
                return any(
                    r.status in ("OPEN","PENDING","HOLDING")
                    for r in self.runners.values()
                )
            except Exception:
                return False
        self.notifier.has_active_strategies = _any_active

    def stop_strategy(self, sid: int):
        if sid in self.runners:
            self.runners[sid].force_exit()

    def exit_all_positions(self):
        """Exit all open positions without stopping the engine."""
        for r in self.runners.values():
            r.force_exit()

    def stop_all(self):
        for r in self.runners.values():
            r.force_exit()
        self.running = False

    def get_state(self) -> dict:
        """Return full engine state for dashboard API."""
        strats = []
        for sid, runner in self.runners.items():
            legs      = runner.get_live_legs()
            failed_ct = sum(1 for l in legs if l.get("failed") and not l.get("disabled"))
            strats.append({
                "id"          : sid,
                "name"        : runner.name,
                "status"      : runner.status,
                "total_pnl"   : round(runner.compute_total_pnl(), 2),
                "mtm"         : round(runner.compute_total_pnl(), 2),
                "legs"        : legs,
                "enabled"     : runner.s.get("enabled", True),
                "idx"         : runner.s.get("idx", ""),
                "mult"        : runner.s.get("mult", "1X"),
                # Error state — used by dashboard badge + alert manager
                "has_error"   : failed_ct > 0,
                "failed_legs" : failed_ct,
            })
        return {
            "running"     : self.running,
            "feed_count"  : price_store.count(),
            "feed_ts"     : price_store.last_ts(),
            "strategies"  : strats,
        }
