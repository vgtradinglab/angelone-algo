"""
CPR — Central Pivot Range Indicator
Signal: Price crosses TC or BC on candle close
"""
import logging
from datetime import datetime, timedelta
from .base import get_candles

_log = logging.getLogger("CPR")

def calculate_cpr(broker, exchange, symbol):
    """
    Calculate CPR using previous day OHLC.
    Returns (pivot, tc, bc) or (None, None, None)
    """
    try:
        now       = datetime.now()
        prev_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        # Calculate previous day OHLC from 5-min candle cache
        import time as _time
        candles_5m = broker.get_candles(exchange, symbol, "FIVE_MINUTE", "", "")
        if candles_5m:
            # Get today's date start timestamp
            from datetime import date
            today_start = datetime.combine(date.today(), datetime.min.time()).timestamp()
            # Filter yesterday's candles (before today)
            prev_candles = [c for c in candles_5m if c[0] < today_start]
            if prev_candles:
                prev_high  = max(c[2] for c in prev_candles)
                prev_low   = min(c[3] for c in prev_candles)
                prev_close = prev_candles[-1][4]
                pivot  = (prev_high + prev_low + prev_close) / 3
                bc     = (prev_high + prev_low) / 2
                tc     = pivot + (pivot - bc)
                _log.info(f"[CPR] {symbol} Pivot={pivot:.2f} TC={tc:.2f} BC={bc:.2f}")
                return round(pivot,2), round(tc,2), round(bc,2)
        return None, None, None
    except Exception as e:
        _log.error(f"[CPR] calculate_cpr error: {e}")
        return None, None, None

def get_signal(broker, exchange, symbol, timeframe):
    """
    Returns signal: 'BUY', 'SELL', or None
    BUY  — Price closes above TC
    SELL — Price closes below BC
    """
    try:
        pivot, tc, bc = calculate_cpr(broker, exchange, symbol)
        if tc is None:
            return None, None, None, None
        candles = get_candles(broker, exchange, symbol, timeframe, num_candles=5)
        if len(candles) < 2:
            return None, pivot, tc, bc
        curr_close = candles[-1][4]
        prev_close = candles[-2][4]
        signal = None
        if prev_close <= tc and curr_close > tc:
            signal = 'BUY'
        elif prev_close >= bc and curr_close < bc:
            signal = 'SELL'
        _log.info(f"[CPR] {symbol} {timeframe} Price={curr_close} TC={tc} BC={bc} → {signal or 'WAIT'}")
        return signal, pivot, tc, bc
    except Exception as e:
        _log.error(f"[CPR] get_signal error: {e}")
        return None, None, None, None
