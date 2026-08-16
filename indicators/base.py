"""
Indicator Base — Common candle fetch and candle close detection.
All indicators use this to get candle data from Angel One.
"""
import logging
import time
from datetime import datetime, timedelta

_log = logging.getLogger("Indicators")

# Angel One interval mapping
INTERVAL_MAP = {
    "1 Min"  : "ONE_MINUTE",
    "5 Min"  : "FIVE_MINUTE",
    "15 Min" : "FIFTEEN_MINUTE",
    "30 Min" : "THIRTY_MINUTE",
    "1 Hour" : "ONE_HOUR",
}

# Candle duration in minutes
INTERVAL_MINUTES = {
    "1 Min"  : 1,
    "5 Min"  : 5,
    "15 Min" : 15,
    "30 Min" : 30,
    "1 Hour" : 60,
}

def get_candles(broker, exchange, symbol, timeframe, num_candles=50):
    """
    Fetch last N closed candles from Angel One.
    Waits 60 seconds after candle close before fetching.
    Returns list of [timestamp, open, high, low, close, volume]
    """
    try:
        interval    = INTERVAL_MAP.get(timeframe, "FIVE_MINUTE")
        mins        = INTERVAL_MINUTES.get(timeframe, 5)
        now         = datetime.now()
        from_date   = (now - timedelta(minutes=mins * (num_candles + 5))).strftime("%Y-%m-%d %H:%M")
        to_date     = now.strftime("%Y-%m-%d %H:%M")
        candles     = broker.get_candles(exchange, symbol, interval, from_date, to_date)
        if not candles:
            _log.warning(f"[Indicators] No candles returned for {symbol} {timeframe}")
            return []
        # Remove last candle if still forming
        now_minute  = now.minute
        candle_min  = mins
        if (now_minute % candle_min) != 0:
            candles = candles[:-1]
        return candles
    except Exception as e:
        _log.error(f"[Indicators] get_candles error: {e}")
        return []

def seconds_to_next_candle_close(timeframe):
    """Returns seconds until next candle closes."""
    mins    = INTERVAL_MINUTES.get(timeframe, 5)
    now     = datetime.now()
    elapsed = (now.minute % mins) * 60 + now.second
    remain  = (mins * 60) - elapsed
    return remain

def is_candle_just_closed(timeframe):
    """Returns True if a candle just closed (within last 90 seconds)."""
    mins    = INTERVAL_MINUTES.get(timeframe, 5)
    now     = datetime.now()
    seconds_in_candle = (now.minute % mins) * 60 + now.second
    return seconds_in_candle <= 90
